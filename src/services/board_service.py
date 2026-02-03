"""Unified Board Service — combines BoardMappingService and TushareBoardService.

Consolidates board mapping construction, concept sync, and query operations
into a single service with consistent dependency injection.

Replaces:
- src/services/board_mapping_service.py
- src/services/tushare_board_service.py
"""

from __future__ import annotations

import csv
import logging
import random
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from src.config import Settings, get_settings
from src.models import BoardMapping, SymbolMetadata
from src.repositories.board_mapping_repository import BoardMappingRepository
from src.repositories.symbol_repository import SymbolRepository
from src.services.tushare_client import TushareClient
from src.utils.logging import LOGGER
from src.utils.ticker_utils import TickerNormalizer

logger = logging.getLogger(__name__)


class BoardService:
    """Unified board service for industry and concept board management.

    Capabilities:
    1. Build industry/concept board → stock mappings (with retry & checkpoint resume)
    2. Sync concept boards from THS via Tushare
    3. Query stock → concepts, board → constituents
    4. Verify board composition changes
    5. Update symbol_metadata.concepts and super_category
    """

    def __init__(
        self,
        board_repo: BoardMappingRepository,
        symbol_repo: SymbolRepository,
        settings: Settings | None = None,
    ):
        self.board_repo = board_repo
        self.symbol_repo = symbol_repo
        self.settings = settings or get_settings()

        # Rate limiting for THS API
        self.rate_limit_delay = 10
        self.random_jitter = 5
        self.max_retries = 3

        self.client = TushareClient(
            token=self.settings.tushare_token,
            points=self.settings.tushare_points,
            delay=self.settings.tushare_delay,
            max_retries=self.settings.tushare_max_retries,
        )

        self._industry_boards_cache: Optional[pd.DataFrame] = None
        self._concept_boards_cache: Optional[pd.DataFrame] = None
        self._super_category_map = self._load_super_category_map()

        logger.info("BoardService initialized")

    @classmethod
    def create_with_session(
        cls, session: Session, settings: Settings | None = None
    ) -> "BoardService":
        """Factory: create service from an existing SQLAlchemy session."""
        board_repo = BoardMappingRepository(session)
        symbol_repo = SymbolRepository(session)
        return cls(board_repo=board_repo, symbol_repo=symbol_repo, settings=settings)

    # ── Public API: Build & Sync ─────────────────────────────────────

    def build_all_mappings(self, board_types: List[str] | None = None) -> Dict[str, int]:
        """Build board → stock mappings.

        Args:
            board_types: List of types to build. Default: ['industry'].
                         Include 'concept' for full concept sync (slow).

        Returns:
            Stats dict, e.g. {'industry': 90, 'concept': 0}
        """
        if board_types is None:
            board_types = ["industry"]

        stats: Dict[str, int] = {}

        if "industry" in board_types:
            stats["industry"] = self._build_industry_mappings()
        if "concept" in board_types:
            stats["concept"] = self._build_concept_mappings()

        self._update_symbol_concepts()
        return stats

    def sync_concept_boards(self) -> int:
        """Sync THS concept boards (delete-and-replace).

        Returns:
            Number of boards synced.
        """
        if not self.settings.enable_concept_boards:
            logger.info("Concept boards disabled, skipping sync")
            return 0

        logger.info("Syncing THS concept boards...")

        concept_boards_df = self.client.fetch_ths_index(exchange="A", type="N")
        if concept_boards_df.empty:
            logger.warning("No concept board data returned")
            return 0

        logger.info(f"Found {len(concept_boards_df)} concept boards")

        # Delete old concept boards
        stmt = delete(BoardMapping).where(BoardMapping.board_type == "concept")
        result = self.board_repo.session.execute(stmt)
        logger.info(f"Deleted {result.rowcount} old concept board records")

        synced_count = 0
        for idx, row in concept_boards_df.iterrows():
            board_code = row["ts_code"]
            board_name = row["name"]

            try:
                members_df = self.client.fetch_ths_member(ts_code=board_code)
                if members_df.empty:
                    logger.warning(f"Board {board_name} has no constituents")
                    continue

                code_field = "con_code" if "con_code" in members_df.columns else "code"
                constituents = TickerNormalizer.normalize_batch(
                    [
                        self.client.denormalize_ts_code(code)
                        for code in members_df[code_field].dropna().tolist()
                    ]
                )

                if not constituents:
                    continue

                board_mapping = BoardMapping(
                    board_name=board_name,
                    board_type="concept",
                    board_code=board_code,
                    constituents=constituents,
                    last_updated=datetime.now(timezone.utc),
                )
                self.board_repo.upsert(board_mapping)
                synced_count += 1

            except Exception as e:
                logger.error(f"Failed to sync board {board_name}: {e}")
                continue

        self.board_repo.session.commit()
        logger.info(f"Concept board sync complete: {synced_count} boards")
        return synced_count

    # ── Public API: Query ────────────────────────────────────────────

    def get_stock_concepts(self, ticker: str) -> List[str]:
        """Get concept boards that a stock belongs to."""
        ticker = TickerNormalizer.normalize(ticker)

        # Try symbol_metadata first (fast path)
        symbol = self.symbol_repo.find_by_ticker(ticker)
        if symbol and symbol.concepts:
            return symbol.concepts

        # Fallback: scan board mappings
        boards = self.board_repo.find_by_type("concept")
        return [b.board_name for b in boards if ticker in b.constituents]

    def get_industry_boards(self) -> List[Dict[str, Any]]:
        """Get all industry boards with metadata."""
        return self._boards_to_dicts("industry")

    def get_concept_boards(self) -> List[Dict[str, Any]]:
        """Get all concept boards with metadata."""
        return self._boards_to_dicts("concept")

    def get_board_constituents(
        self, board_name: str, board_type: str = "industry"
    ) -> List[str]:
        """Get stock tickers in a board."""
        board = self.board_repo.find_by_name_and_type(board_name, board_type)
        if not board:
            logger.warning(f"Board not found: name={board_name}, type={board_type}")
            return []
        return board.constituents or []

    def verify_changes(self, board_name: str, board_type: str) -> Dict[str, Any]:
        """Check if a board's constituents have changed vs. database."""
        mapping = self.board_repo.find_by_name_and_type(board_name, board_type)
        old_constituents = set(mapping.constituents) if mapping else set()
        board_code = mapping.board_code if mapping else None

        if board_code is None:
            board_code = self._resolve_board_code(board_name, board_type)
            if board_code is None:
                return {
                    "has_changes": False,
                    "error": f"Board code not found for {board_name}",
                    "added": [],
                    "removed": [],
                    "current_count": 0,
                    "previous_count": len(old_constituents),
                }

        try:
            new_constituents = set(self._fetch_board_constituents(board_code))
        except Exception as e:
            LOGGER.error(f"Failed to fetch {board_type} '{board_name}': {e}")
            return {"has_changes": False, "error": str(e)}

        added = new_constituents - old_constituents
        removed = old_constituents - new_constituents

        return {
            "has_changes": len(added) > 0 or len(removed) > 0,
            "added": list(added),
            "removed": list(removed),
            "current_count": len(new_constituents),
            "previous_count": len(old_constituents),
        }

    def update_stock_concepts(self, ticker: str) -> List[str]:
        """Refresh a single stock's concept list in symbol_metadata."""
        ticker = TickerNormalizer.normalize(ticker)
        concepts = self.get_stock_concepts(ticker)

        stmt = select(SymbolMetadata).where(SymbolMetadata.ticker == ticker)
        result = self.board_repo.session.execute(stmt)
        symbol = result.scalar_one_or_none()

        if symbol:
            symbol.concepts = concepts
            self.board_repo.session.commit()

        return concepts

    # ── Internal: Build helpers ──────────────────────────────────────

    def _build_industry_mappings(self) -> int:
        """Build all industry board mappings with retry and checkpoint resume."""
        LOGGER.info("Building industry board mappings...")

        boards_df = self._get_industry_boards()
        if boards_df.empty:
            LOGGER.error("Failed to fetch industry boards from Tushare")
            return 0

        total_boards = len(boards_df)
        LOGGER.info(f"Found {total_boards} industry boards")

        completed_boards = self.board_repo.find_by_type("industry")
        completed_board_names = {
            b.board_name for b in completed_boards if b.constituents
        }
        LOGGER.info(f"{len(completed_board_names)} already completed, will skip")

        count = 0
        for idx, row in enumerate(boards_df.itertuples(index=False), start=1):
            board_name = row.industry
            board_code = row.ts_code

            if board_name in completed_board_names:
                LOGGER.info(f"[{idx}/{total_boards}] Skipping '{board_name}' (completed)")
                continue

            constituents = self._fetch_with_retry(board_code, board_name)

            if constituents is not None:
                mapping = BoardMapping(
                    board_name=board_name,
                    board_type="industry",
                    board_code=board_code,
                    constituents=constituents,
                )
                self.board_repo.upsert(mapping)
                count += 1
                LOGGER.info(
                    f"[{idx}/{total_boards}] ✓ Saved '{board_name}': {len(constituents)} stocks"
                )
            else:
                LOGGER.warning(f"[{idx}/{total_boards}] ✗ Skipped '{board_name}'")

            # Rate limiting with jitter
            delay = self.rate_limit_delay + random.randint(
                -self.random_jitter, self.random_jitter
            )
            delay = max(30, delay)
            if idx < total_boards:
                time.sleep(delay)

        return count

    def _build_concept_mappings(self) -> int:
        """Build all concept board mappings (slow: 400+ boards)."""
        LOGGER.warning("Building concept board mappings... This may take 1-2 hours!")

        boards_df = self._get_concept_boards()
        if boards_df.empty:
            LOGGER.error("Failed to fetch concept boards from Tushare")
            return 0

        count = 0
        for row in boards_df.itertuples(index=False):
            board_name = row.name
            board_code = row.ts_code

            try:
                constituents = self._fetch_board_constituents(board_code)
                mapping = BoardMapping(
                    board_name=board_name,
                    board_type="concept",
                    board_code=board_code,
                    constituents=constituents,
                )
                self.board_repo.upsert(mapping)
                count += 1
                LOGGER.info(
                    f"[{count}/{len(boards_df)}] Saved concept '{board_name}': {len(constituents)} stocks"
                )
                time.sleep(self.rate_limit_delay)

            except Exception as e:
                LOGGER.warning(f"Failed to process concept '{board_name}': {e}")
                continue

        return count

    def _fetch_with_retry(self, board_code: str, board_name: str) -> Optional[List[str]]:
        """Fetch board constituents with exponential backoff retry."""
        for retry in range(self.max_retries):
            try:
                return self._fetch_board_constituents(board_code)
            except Exception as e:
                wait_time = (2 ** retry) * 30
                if retry < self.max_retries - 1:
                    LOGGER.warning(
                        f"Retry {retry + 1}/{self.max_retries} for '{board_name}' "
                        f"after {wait_time}s: {e}"
                    )
                    time.sleep(wait_time)
                else:
                    LOGGER.error(
                        f"Failed '{board_name}' after {self.max_retries} retries: {e}"
                    )
        return None

    def _fetch_board_constituents(self, board_code: str) -> List[str]:
        """Fetch board constituents from THS via Tushare."""
        df = self.client.fetch_ths_member(ts_code=board_code)
        if df.empty:
            return []

        code_field = "con_code" if "con_code" in df.columns else "code"
        tickers = [
            self.client.denormalize_ts_code(code)
            for code in df[code_field].dropna().tolist()
        ]
        return TickerNormalizer.normalize_batch(tickers)

    def _update_symbol_concepts(self) -> None:
        """Rebuild reverse index: stock → concepts + super_category."""
        LOGGER.info("Updating symbol concepts from board mappings...")

        ticker_to_concepts: Dict[str, List[str]] = defaultdict(list)
        concept_mappings = self.board_repo.find_by_type("concept")

        for mapping in concept_mappings:
            for ticker in mapping.constituents:
                ticker_to_concepts[ticker].append(mapping.board_name)

        # Update symbols with concepts
        for ticker, concepts in ticker_to_concepts.items():
            symbol = self.symbol_repo.find_by_ticker(ticker)
            if symbol:
                symbol.concepts = concepts
                if symbol.industry_lv1:
                    symbol.super_category = self._super_category_map.get(
                        symbol.industry_lv1
                    )

        # Ensure super_category for all remaining symbols
        all_tickers = self.symbol_repo.get_all_tickers()
        for ticker in all_tickers:
            if ticker not in ticker_to_concepts:
                symbol = self.symbol_repo.find_by_ticker(ticker)
                if symbol and symbol.industry_lv1:
                    symbol.super_category = self._super_category_map.get(
                        symbol.industry_lv1
                    )

        self.symbol_repo.session.commit()
        LOGGER.info(f"Updated concepts for {len(ticker_to_concepts)} stocks")

    # ── Internal: Cache & helpers ────────────────────────────────────

    def _get_industry_boards(self) -> pd.DataFrame:
        if self._industry_boards_cache is not None:
            return self._industry_boards_cache

        trade_date = self.client.get_latest_trade_date()
        df = self.client.fetch_ths_industry_moneyflow(trade_date=trade_date)
        if not df.empty:
            df = df[["ts_code", "industry"]].drop_duplicates(subset="ts_code")
        self._industry_boards_cache = df
        return df

    def _get_concept_boards(self) -> pd.DataFrame:
        if self._concept_boards_cache is not None:
            return self._concept_boards_cache

        df = self.client.fetch_ths_index(exchange="A", type="N")
        if not df.empty:
            df = df[["ts_code", "name", "count"]].drop_duplicates(subset="ts_code")
        self._concept_boards_cache = df
        return df

    def _resolve_board_code(
        self, board_name: str, board_type: str
    ) -> Optional[str]:
        if board_type == "industry":
            boards = self._get_industry_boards()
            match = boards[boards["industry"] == board_name]
            if not match.empty:
                return match.iloc[0]["ts_code"]
        elif board_type == "concept":
            boards = self._get_concept_boards()
            match = boards[boards["name"] == board_name]
            if not match.empty:
                return match.iloc[0]["ts_code"]
        return None

    def _load_super_category_map(self) -> dict[str, str]:
        mapping_path = (
            Path(__file__).parent.parent.parent / "data" / "super_category_mapping.csv"
        )
        if not mapping_path.exists():
            LOGGER.warning(
                "Super category mapping file not found; super_category will remain empty"
            )
            return {}

        lookup: dict[str, str] = {}
        with mapping_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                industry = row.get("行业名称")
                super_category = row.get("超级行业组")
                if industry and super_category:
                    lookup[industry] = super_category
        return lookup

    def _boards_to_dicts(self, board_type: str) -> List[Dict[str, Any]]:
        boards = self.board_repo.find_by_type(board_type)
        return [
            {
                "board_name": b.board_name,
                "board_code": b.board_code,
                "constituents": b.constituents,
                "count": len(b.constituents) if b.constituents else 0,
                "last_updated": b.last_updated,
            }
            for b in boards
        ]


# Backward-compatible aliases
BoardMappingService = BoardService
TushareBoardService = BoardService
