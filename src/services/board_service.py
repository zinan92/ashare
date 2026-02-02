"""Unified Board Service — single entry point for industry/concept board operations.

Merges the former BoardMappingService (mapping build, verify, reverse-index)
and TushareBoardService (sync, query) into one cohesive service.

Key capabilities:
  - Build & rebuild board → constituent mappings (industry / concept)
  - Sync concept boards from Tushare (同花顺)
  - Query: stock → concepts, board → constituents
  - Verify changes (added/removed stocks)
  - Retry with exponential back-off & checkpoint resume
  - Super-category enrichment via CSV lookup

Session lifecycle is controlled by the caller (dependency-injected repos).
"""

from __future__ import annotations

import csv
import random
import time
import logging
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
    """Unified board service — replaces BoardMappingService + TushareBoardService."""

    def __init__(
        self,
        board_repo: BoardMappingRepository,
        symbol_repo: SymbolRepository,
        settings: Settings | None = None,
    ):
        self.board_repo = board_repo
        self.symbol_repo = symbol_repo
        self.settings = settings or get_settings()

        # Rate-limit / retry knobs
        self.rate_limit_delay = 10
        self.random_jitter = 5
        self.max_retries = 3

        self.client = TushareClient(
            token=self.settings.tushare_token,
            points=self.settings.tushare_points,
            delay=self.settings.tushare_delay,
            max_retries=self.settings.tushare_max_retries,
        )

        # In-memory caches (per-instance lifecycle)
        self._industry_boards_cache: Optional[pd.DataFrame] = None
        self._concept_boards_cache: Optional[pd.DataFrame] = None
        self._super_category_map = self._load_super_category_map()

    # -------------------------------------------------------------- #
    # Factory helpers
    # -------------------------------------------------------------- #

    @classmethod
    def create_with_session(
        cls, session: Session, settings: Settings | None = None
    ) -> "BoardService":
        """Convenience factory when you have a raw SQLAlchemy session."""
        board_repo = BoardMappingRepository(session)
        symbol_repo = SymbolRepository(session)
        return cls(board_repo=board_repo, symbol_repo=symbol_repo, settings=settings)

    # ================================================================ #
    #  PUBLIC API — Build / Sync
    # ================================================================ #

    def build_all_mappings(self, board_types: List[str] | None = None) -> Dict[str, int]:
        """Build board → constituent mappings.

        Args:
            board_types: ``['industry']``, ``['concept']``, or both.
                         Defaults to ``['industry']`` only.

        Returns:
            ``{'industry': N, 'concept': M}`` counts.
        """
        if board_types is None:
            board_types = ["industry"]

        stats: Dict[str, int] = {}
        if "industry" in board_types:
            stats["industry"] = self._build_industry_mappings()
        if "concept" in board_types:
            stats["concept"] = self._build_concept_mappings()

        # Refresh reverse index (stock → concepts + super_category)
        self._update_symbol_concepts()
        return stats

    def sync_concept_boards(self) -> int:
        """Full re-sync of 同花顺 concept boards (delete-then-insert).

        Returns:
            Number of boards successfully synced.
        """
        if not self.settings.enable_concept_boards:
            logger.info("Concept boards disabled — skipping sync")
            return 0

        logger.info("Starting concept board sync …")
        concept_boards_df = self.client.fetch_ths_index(exchange="A", type="N")
        if concept_boards_df.empty:
            logger.warning("No concept boards returned from Tushare")
            return 0

        logger.info("Fetched %d concept boards", len(concept_boards_df))

        # Wipe old concept data
        stmt = delete(BoardMapping).where(BoardMapping.board_type == "concept")
        result = self.board_repo.session.execute(stmt)
        logger.info("Deleted %d old concept board records", result.rowcount)

        synced = 0
        for idx, row in concept_boards_df.iterrows():
            board_code = row["ts_code"]
            board_name = row["name"]

            try:
                members_df = self.client.fetch_ths_member(ts_code=board_code)
                if members_df.empty:
                    logger.warning("Board '%s' has no constituents", board_name)
                    continue

                code_field = "con_code" if "con_code" in members_df.columns else "code"
                constituents = TickerNormalizer.normalize_batch(
                    [
                        self.client.denormalize_ts_code(c)
                        for c in members_df[code_field].dropna().tolist()
                    ]
                )
                if not constituents:
                    continue

                mapping = BoardMapping(
                    board_name=board_name,
                    board_type="concept",
                    board_code=board_code,
                    constituents=constituents,
                    last_updated=datetime.now(timezone.utc),
                )
                self.board_repo.upsert(mapping)
                synced += 1
                logger.debug("Synced '%s' — %d stocks", board_name, len(constituents))

            except Exception as e:
                logger.error("Failed to sync concept '%s': %s", board_name, e)

        self.board_repo.session.commit()
        logger.info("Concept sync complete — %d boards", synced)
        return synced

    # ================================================================ #
    #  PUBLIC API — Query
    # ================================================================ #

    def get_stock_concepts(self, ticker: str) -> List[str]:
        """Return concept board names a stock belongs to."""
        ticker = TickerNormalizer.normalize(ticker)

        # Fast path: check symbol_metadata first
        symbol = self.symbol_repo.find_by_ticker(ticker)
        if symbol and symbol.concepts:
            return symbol.concepts

        # Slow path: scan all concept mappings
        boards = self.board_repo.find_by_type("concept")
        return [b.board_name for b in boards if ticker in (b.constituents or [])]

    def get_industry_boards(self) -> List[Dict[str, Any]]:
        """Return all industry boards with metadata."""
        return self._boards_as_dicts("industry")

    def get_concept_boards(self) -> List[Dict[str, Any]]:
        """Return all concept boards with metadata."""
        return self._boards_as_dicts("concept")

    def get_board_constituents(
        self, board_name: str, board_type: str = "industry"
    ) -> List[str]:
        """Return ticker list for a given board."""
        board = self.board_repo.find_by_name_and_type(board_name, board_type)
        if not board:
            logger.warning("Board not found: name=%s type=%s", board_name, board_type)
            return []
        return board.constituents or []

    def verify_changes(self, board_name: str, board_type: str) -> Dict[str, Any]:
        """Check whether a board's constituents changed vs DB snapshot."""
        mapping = self.board_repo.find_by_name_and_type(board_name, board_type)
        old_constituents: set = set()
        board_code: Optional[str] = None

        if mapping:
            old_constituents = set(mapping.constituents or [])
            board_code = mapping.board_code

        if board_code is None:
            board_code = self._resolve_board_code(board_name, board_type)
            if board_code is None:
                LOGGER.error("Cannot resolve board code for %s (%s)", board_name, board_type)
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
            LOGGER.error("Failed to fetch %s '%s': %s", board_type, board_name, e)
            return {"has_changes": False, "error": str(e)}

        added = new_constituents - old_constituents
        removed = old_constituents - new_constituents
        return {
            "has_changes": bool(added or removed),
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

    # ================================================================ #
    #  INTERNAL — Build helpers
    # ================================================================ #

    def _build_industry_mappings(self) -> int:
        """Build all industry board mappings (with retry + checkpoint resume)."""
        LOGGER.info("Building industry board mappings …")
        boards_df = self._get_industry_boards()
        if boards_df.empty:
            LOGGER.error("Failed to fetch industry boards from Tushare")
            return 0

        total = len(boards_df)
        LOGGER.info("Found %d industry boards", total)

        completed = {
            b.board_name
            for b in self.board_repo.find_by_type("industry")
            if b.constituents
        }
        LOGGER.info("Skipping %d already-completed boards", len(completed))

        count = 0
        for idx, row in enumerate(boards_df.itertuples(index=False), 1):
            board_name = row.industry
            board_code = row.ts_code

            if board_name in completed:
                LOGGER.info("[%d/%d] Skip '%s' (done)", idx, total, board_name)
                continue

            constituents = self._fetch_with_retry(board_name, board_code)
            if constituents is not None:
                mapping = BoardMapping(
                    board_name=board_name,
                    board_type="industry",
                    board_code=board_code,
                    constituents=constituents,
                )
                self.board_repo.upsert(mapping)
                count += 1
                LOGGER.info("[%d/%d] ✓ '%s': %d stocks", idx, total, board_name, len(constituents))
            else:
                LOGGER.warning("[%d/%d] ✗ Skipped '%s'", idx, total, board_name)

            delay = max(30, self.rate_limit_delay + random.randint(-self.random_jitter, self.random_jitter))
            if idx < total:
                time.sleep(delay)

        return count

    def _build_concept_mappings(self) -> int:
        """Build all concept board mappings (warning: slow — 442 boards)."""
        LOGGER.warning("Building concept mappings — may take 1-2 hours!")
        boards_df = self._get_concept_boards()
        if boards_df.empty:
            LOGGER.error("Failed to fetch concept boards from Tushare")
            return 0

        count = 0
        total = len(boards_df)
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
                LOGGER.info("[%d/%d] Saved concept '%s': %d stocks", count, total, board_name, len(constituents))
                time.sleep(self.rate_limit_delay)
            except Exception as e:
                LOGGER.warning("Failed concept '%s': %s", board_name, e)

        return count

    def _fetch_with_retry(self, board_name: str, board_code: str) -> Optional[List[str]]:
        """Fetch constituents with exponential back-off."""
        for attempt in range(self.max_retries):
            try:
                return self._fetch_board_constituents(board_code)
            except Exception as e:
                wait = (2 ** attempt) * 30
                if attempt < self.max_retries - 1:
                    LOGGER.warning("Retry %d/%d for '%s' in %ds: %s", attempt + 1, self.max_retries, board_name, wait, e)
                    time.sleep(wait)
                else:
                    LOGGER.error("Failed '%s' after %d retries: %s", board_name, self.max_retries, e)
        return None

    def _fetch_board_constituents(self, board_code: str) -> List[str]:
        """Fetch member tickers for a board (同花顺 via Tushare)."""
        df = self.client.fetch_ths_member(ts_code=board_code)
        if df.empty:
            return []
        code_field = "con_code" if "con_code" in df.columns else "code"
        tickers = [self.client.denormalize_ts_code(c) for c in df[code_field].dropna().tolist()]
        return TickerNormalizer.normalize_batch(tickers)

    def _update_symbol_concepts(self) -> None:
        """Reverse-index: refresh every symbol's concept list + super_category."""
        LOGGER.info("Updating symbol concepts from board mappings …")

        ticker_to_concepts: Dict[str, List[str]] = defaultdict(list)
        for mapping in self.board_repo.find_by_type("concept"):
            for ticker in (mapping.constituents or []):
                ticker_to_concepts[ticker].append(mapping.board_name)

        for ticker, concepts in ticker_to_concepts.items():
            symbol = self.symbol_repo.find_by_ticker(ticker)
            if symbol:
                symbol.concepts = concepts
                if symbol.industry_lv1:
                    symbol.super_category = self._super_category_map.get(symbol.industry_lv1)

        # Also refresh super_category for stocks with no concept changes
        for ticker in self.symbol_repo.get_all_tickers():
            if ticker not in ticker_to_concepts:
                symbol = self.symbol_repo.find_by_ticker(ticker)
                if symbol and symbol.industry_lv1:
                    symbol.super_category = self._super_category_map.get(symbol.industry_lv1)

        self.symbol_repo.session.commit()
        LOGGER.info("Updated concepts for %d stocks", len(ticker_to_concepts))

    # ================================================================ #
    #  INTERNAL — Helpers / Caches
    # ================================================================ #

    def _boards_as_dicts(self, board_type: str) -> List[Dict[str, Any]]:
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

    def _resolve_board_code(self, board_name: str, board_type: str) -> Optional[str]:
        if board_type == "industry":
            boards = self._get_industry_boards()
            match = boards[boards["industry"] == board_name]
            return match.iloc[0]["ts_code"] if not match.empty else None
        elif board_type == "concept":
            boards = self._get_concept_boards()
            match = boards[boards["name"] == board_name]
            return match.iloc[0]["ts_code"] if not match.empty else None
        return None

    def _load_super_category_map(self) -> dict[str, str]:
        mapping_path = Path(__file__).parent.parent.parent / "data" / "super_category_mapping.csv"
        if not mapping_path.exists():
            LOGGER.warning("super_category_mapping.csv not found — super_category will remain empty")
            return {}
        lookup: dict[str, str] = {}
        with mapping_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                industry = row.get("行业名称")
                super_cat = row.get("超级行业组")
                if industry and super_cat:
                    lookup[industry] = super_cat
        return lookup
