"""板块映射服务 - 负责构建和更新股票与板块的映射关系

重构说明:
- 强制依赖注入 BoardMappingRepository 和 SymbolRepository
- Session 生命周期由调用者控制
- 不再自动创建 Session
"""

from __future__ import annotations

import random
import time
import csv
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from src.config import get_settings, Settings
from src.models import BoardMapping, SymbolMetadata
from src.repositories.board_mapping_repository import BoardMappingRepository
from src.repositories.symbol_repository import SymbolRepository
from src.services.tushare_client import TushareClient
from src.utils.logging import LOGGER
from src.utils.ticker_utils import TickerNormalizer


class BoardMappingService:
    """
    板块映射服务

    功能:
    1. 一次性构建板块→股票的映射表
    2. 反向索引：快速查询股票→概念列表
    3. 增量验证：只检查变化，不重新遍历
    4. 重试机制：失败后自动重试，指数退避
    5. 断点续跑：跳过已成功的板块，从失败处继续

    重构后支持:
    - 强制依赖注入 BoardMappingRepository 和 SymbolRepository
    - Session 生命周期由调用者控制
    """

    def __init__(
        self,
        board_repo: BoardMappingRepository,
        symbol_repo: SymbolRepository,
        settings: Settings | None = None,
    ):
        """
        初始化板块映射服务

        Args:
            board_repo: 板块映射数据仓库（必需）
            symbol_repo: 标的数据仓库（必需）
            settings: 配置对象（可选）
        """
        self.board_repo = board_repo
        self.symbol_repo = symbol_repo
        self.settings = settings or get_settings()
        self.rate_limit_delay = 10   # 同花顺接口 15000积分足够，但仍保留轻微延迟
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

    @classmethod
    def create_with_session(cls, session: Session, settings: Settings | None = None) -> "BoardMappingService":
        """使用现有session创建服务的工厂方法"""
        board_repo = BoardMappingRepository(session)
        symbol_repo = SymbolRepository(session)
        return cls(board_repo=board_repo, symbol_repo=symbol_repo, settings=settings)

    # ----------------------------------------------------------------- #
    # 公共API
    # ----------------------------------------------------------------- #

    def build_all_mappings(self, board_types: List[str] = None) -> Dict[str, int]:
        """
        一次性构建所有板块映射

        Args:
            board_types: 要构建的板块类型列表 ['industry', 'concept']
                        默认只构建 industry (概念板块太多，按需构建)

        Returns:
            统计信息 {'industry': 90, 'concept': 0}
        """
        if board_types is None:
            board_types = ['industry']  # 默认只构建行业板块

        stats = {}

        if 'industry' in board_types:
            stats['industry'] = self._build_industry_mappings()

        if 'concept' in board_types:
            stats['concept'] = self._build_concept_mappings()

        # 构建反向索引：更新 symbol_metadata.concepts + super_category
        self._update_symbol_concepts()

        return stats

    def verify_changes(self, board_name: str, board_type: str) -> Dict[str, any]:
        """
        验证单个板块的成分股是否有变化

        Args:
            board_name: 板块名称 (如 "银行")
            board_type: 板块类型 ('industry' 或 'concept')

        Returns:
            {
                'has_changes': True/False,
                'added': ['000001', ...],
                'removed': ['600036', ...],
                'current_count': 42,
                'previous_count': 40
            }
        """
        # 使用 repository 获取当前数据库中的成分股
        board_code: Optional[str] = None
        mapping = self.board_repo.find_by_name_and_type(board_name, board_type)
        if mapping:
            old_constituents = set(mapping.constituents)
            board_code = mapping.board_code
        else:
            old_constituents = set()

        if board_code is None:
            board_code = self._resolve_board_code(board_name, board_type)
            if board_code is None:
                LOGGER.error("Unable to resolve board code for %s (%s)", board_name, board_type)
                return {
                    'has_changes': False,
                    'error': f"Board code not found for {board_name}",
                    'added': [],
                    'removed': [],
                    'current_count': 0,
                    'previous_count': len(old_constituents)
                }

        # 获取最新的成分股
        try:
            new_constituents = set(self._fetch_board_constituents(board_code))
        except Exception as e:
            LOGGER.error(f"Failed to fetch {board_type} '{board_name}': {e}")
            return {'has_changes': False, 'error': str(e)}

        added = new_constituents - old_constituents
        removed = old_constituents - new_constituents

        return {
            'has_changes': len(added) > 0 or len(removed) > 0,
            'added': list(added),
            'removed': list(removed),
            'current_count': len(new_constituents),
            'previous_count': len(old_constituents)
        }

    def get_stock_concepts(self, ticker: str) -> List[str]:
        """
        查询某只股票所属的概念板块列表

        Args:
            ticker: 股票代码

        Returns:
            概念板块名称列表
        """
        # 使用 repository 查询
        symbol = self.symbol_repo.find_by_ticker(ticker)
        return symbol.concepts if symbol and symbol.concepts else []

    # ----------------------------------------------------------------- #
    # 内部方法
    # ----------------------------------------------------------------- #

    def _build_industry_mappings(self) -> int:
        """构建所有行业板块映射（带重试和断点续跑）"""
        LOGGER.info("Building industry board mappings...")

        # 获取所有行业板块列表
        boards_df = self._get_industry_boards()
        if boards_df.empty:
            LOGGER.error("Failed to fetch industry boards from Tushare")
            return 0

        total_boards = len(boards_df)
        LOGGER.info(f"Found {total_boards} industry boards")

        # 获取已成功构建的板块（断点续跑）
        completed_boards = self.board_repo.find_by_type('industry')
        completed_board_names = {
            board.board_name for board in completed_boards if board.constituents
        }

        LOGGER.info(f"Found {len(completed_board_names)} already completed boards, will skip them")

        count = 0
        total_estimated_time = (total_boards - len(completed_board_names)) * (self.rate_limit_delay + self.random_jitter // 2) / 60
        LOGGER.info(f"Estimated completion time: {total_estimated_time:.0f} minutes")

        for idx, row in enumerate(boards_df.itertuples(index=False), start=1):
            board_name = row.industry
            board_code = row.ts_code

            # 断点续跑：跳过已完成的板块
            if board_name in completed_board_names:
                LOGGER.info(f"[{idx}/{total_boards}] Skipping '{board_name}' (already completed)")
                continue

            # 带重试的获取逻辑
            constituents = None
            for retry in range(self.max_retries):
                try:
                    constituents = self._fetch_board_constituents(board_code)
                    break  # 成功则退出重试循环
                except Exception as e:
                    wait_time = (2 ** retry) * 30  # 指数退避：30秒、60秒、120秒
                    if retry < self.max_retries - 1:
                        LOGGER.warning(f"Retry {retry + 1}/{self.max_retries} for '{board_name}' after {wait_time}s: {e}")
                        time.sleep(wait_time)
                    else:
                        LOGGER.error(f"Failed to fetch '{board_name}' after {self.max_retries} retries: {e}")
                        constituents = None

            if constituents is not None:
                # 使用 repository 保存到数据库
                mapping = BoardMapping(
                    board_name=board_name,
                    board_type='industry',
                    board_code=board_code,
                    constituents=constituents,
                )
                self.board_repo.upsert(mapping)

                count += 1
                LOGGER.info(f"[{idx}/{total_boards}] ✓ Saved '{board_name}': {len(constituents)} stocks")
            else:
                LOGGER.warning(f"[{idx}/{total_boards}] ✗ Skipped '{board_name}' due to errors")

            # 限流延迟（添加随机抖动）
            delay = self.rate_limit_delay + random.randint(-self.random_jitter, self.random_jitter)
            delay = max(30, delay)  # 最少30秒
            if idx < total_boards:
                LOGGER.debug(f"Waiting {delay}s before next request...")
                time.sleep(delay)

        return count

    def _build_concept_mappings(self) -> int:
        """构建所有概念板块映射（警告：442个概念，耗时很长）"""
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

                # 使用 repository 保存
                mapping = BoardMapping(
                    board_name=board_name,
                    board_type='concept',
                    board_code=board_code,
                    constituents=constituents,
                )
                self.board_repo.upsert(mapping)

                count += 1
                LOGGER.info(f"[{count}/{len(boards_df)}] Saved concept '{board_name}': {len(constituents)} stocks")

                time.sleep(self.rate_limit_delay)

            except Exception as e:
                LOGGER.warning(f"Failed to process concept '{board_name}': {e}")
                continue

        return count

    def _fetch_board_constituents(self, board_code: str) -> List[str]:
        """
        获取板块成分股列表（使用同花顺数据）

        Returns:
            股票代码列表 ['000001', '600036', ...]（标准化为6位）
        """
        df = self.client.fetch_ths_member(ts_code=board_code)
        if df.empty:
            return []

        code_field = 'con_code' if 'con_code' in df.columns else 'code'
        tickers = [
            self.client.denormalize_ts_code(code)
            for code in df[code_field].dropna().tolist()
        ]

        return TickerNormalizer.normalize_batch(tickers)

    def _update_symbol_concepts(self) -> None:
        """根据板块映射，更新每只股票的概念列表，并刷新超级行业组"""
        LOGGER.info("Updating symbol concepts from board mappings...")

        # 构建反向索引: ticker → [concept1, concept2, ...]
        ticker_to_concepts = defaultdict(list)

        # 使用 repository 获取所有概念板块
        concept_mappings = self.board_repo.find_by_type('concept')

        for mapping in concept_mappings:
            for ticker in mapping.constituents:
                ticker_to_concepts[ticker].append(mapping.board_name)

        # 批量更新 symbol_metadata
        symbols_to_update = []
        for ticker, concepts in ticker_to_concepts.items():
            symbol = self.symbol_repo.find_by_ticker(ticker)
            if symbol:
                symbol.concepts = concepts
                if symbol.industry_lv1:
                    symbol.super_category = self._super_category_map.get(symbol.industry_lv1)
                symbols_to_update.append(symbol)

        # 确保所有股票都写入超级行业组，即便没有概念变化
        all_tickers = self.symbol_repo.get_all_tickers()
        for ticker in all_tickers:
            symbol = self.symbol_repo.find_by_ticker(ticker)
            if symbol and symbol.industry_lv1 and ticker not in ticker_to_concepts:
                symbol.super_category = self._super_category_map.get(symbol.industry_lv1)
                symbols_to_update.append(symbol)

        # 使用 repository 批量更新（这会触发 session.commit()）
        if symbols_to_update:
            self.symbol_repo.session.commit()

        LOGGER.info(f"Updated concepts for {len(ticker_to_concepts)} stocks")

    # ----------------------------------------------------------------- #
    # Helpers
    # ----------------------------------------------------------------- #

    def _get_industry_boards(self) -> pd.DataFrame:
        """Load latest industry boards from Tushare once per service lifecycle."""
        if self._industry_boards_cache is not None:
            return self._industry_boards_cache

        trade_date = self.client.get_latest_trade_date()
        df = self.client.fetch_ths_industry_moneyflow(trade_date=trade_date)
        if not df.empty:
            df = df[['ts_code', 'industry']].drop_duplicates(subset='ts_code')
        self._industry_boards_cache = df
        return df

    def _get_concept_boards(self) -> pd.DataFrame:
        """Load concept boards list from Tushare."""
        if self._concept_boards_cache is not None:
            return self._concept_boards_cache

        df = self.client.fetch_ths_index(exchange='A', type='N')
        if not df.empty:
            df = df[['ts_code', 'name', 'count']].drop_duplicates(subset='ts_code')
        self._concept_boards_cache = df
        return df

    def _resolve_board_code(self, board_name: str, board_type: str) -> Optional[str]:
        """Resolve board code from caches when DB record does not have one."""
        if board_type == 'industry':
            boards = self._get_industry_boards()
            match = boards[boards['industry'] == board_name]
            if not match.empty:
                return match.iloc[0]['ts_code']
        elif board_type == 'concept':
            boards = self._get_concept_boards()
            match = boards[boards['name'] == board_name]
            if not match.empty:
                return match.iloc[0]['ts_code']
        return None

    def _load_super_category_map(self) -> dict[str, str]:
        """行业 → 超级行业组映射（本地CSV，可为空）"""
        mapping_path = Path(__file__).parent.parent.parent / "data" / "super_category_mapping.csv"
        if not mapping_path.exists():
            LOGGER.warning("Super category mapping file not found; super_category will remain empty")
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
