from __future__ import annotations

import csv
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from src.config import Settings, get_settings
from src.repositories.symbol_repository import SymbolRepository
from src.schemas import SymbolMeta
from src.services.tushare_data_provider import TushareDataProvider
from src.utils.logging import LOGGER
from src.utils.ticker_utils import TickerNormalizer


class MarketDataService:
    """
    市场数据服务 - 协调外部数据获取和数据库持久化

    重构说明:
    - 使用 SymbolRepository 替代直接的 SQLAlchemy 查询
    - 强制使用依赖注入，不再自动创建 Session
    - Session 生命周期由调用者控制
    """

    # 类级别缓存，用于交易日期缓存
    _trade_date_cache: Optional[str] = None
    _trade_date_cache_time: Optional[datetime] = None
    _trade_date_cache_ttl = timedelta(minutes=5)  # 缓存5分钟

    def __init__(
        self,
        symbol_repo: SymbolRepository,
        provider: TushareDataProvider | None = None,
        settings: Settings | None = None,
    ) -> None:
        """
        初始化市场数据服务

        Args:
            symbol_repo: 标的数据仓库（必需）
            provider: Tushare数据提供者（可选）
            settings: 配置对象（可选）
        """
        self.symbol_repo = symbol_repo
        self.provider = provider or TushareDataProvider()
        self.settings = settings or get_settings()
        self._super_category_map = self._load_super_category_map()

    @classmethod
    def create_with_session(
        cls, session: Session, settings: Settings | None = None
    ) -> "MarketDataService":
        """使用现有session创建服务的工厂方法"""
        symbol_repo = SymbolRepository(session)
        return cls(symbol_repo=symbol_repo, settings=settings)

    def _get_latest_trade_date_cached(self) -> Optional[str]:
        """获取最新交易日期（带缓存，5分钟TTL）"""
        now = datetime.now(timezone.utc)

        # 检查缓存是否有效
        if (
            MarketDataService._trade_date_cache is not None
            and MarketDataService._trade_date_cache_time is not None
            and now - MarketDataService._trade_date_cache_time < self._trade_date_cache_ttl
        ):
            return MarketDataService._trade_date_cache

        # 缓存失效，重新获取
        try:
            trade_date = self.provider.client.get_latest_trade_date()
            MarketDataService._trade_date_cache = trade_date
            MarketDataService._trade_date_cache_time = now
            return trade_date
        except Exception as exc:
            LOGGER.warning("Failed to get latest trade date | %s", exc)
            # 如果获取失败但有旧缓存，返回旧缓存
            return MarketDataService._trade_date_cache

    def _load_super_category_map(self) -> dict[str, str]:
        """
        行业 → 超级行业组映射（如果缺失文件则返回空字典）
        """
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

    # --------------------------------------------------------------------- #
    # Public API
    # --------------------------------------------------------------------- #
    def refresh_metadata(self, tickers: list[str]) -> None:
        """刷新股票元数据"""
        tickers = TickerNormalizer.normalize_batch(list(tickers))
        if not tickers:
            LOGGER.info("No valid tickers provided for refresh; skipping.")
            return

        LOGGER.info("Begin metadata refresh | tickers=%s", len(tickers))

        try:
            metadata_df = self.provider.fetch_symbol_metadata(tickers)
        except Exception as exc:
            LOGGER.exception("Metadata fetch failed | error=%s", exc)
            metadata_df = None

        if metadata_df is not None:
            # 使用 repository 的批量更新方法
            self.symbol_repo.bulk_upsert_from_dataframe(
                metadata_df, self._super_category_map
            )
            self.symbol_repo.session.commit()

    def list_symbols(self) -> list[SymbolMeta]:
        """获取所有标的列表"""
        # 使用 repository 查询
        symbols = self.symbol_repo.find_all_ordered_by_market_value()
        return [SymbolMeta.model_validate(row) for row in symbols]

    def last_refresh_time(self) -> datetime | None:
        """获取最后刷新时间"""
        # 使用 repository 查询
        return self.symbol_repo.get_last_sync_time()

