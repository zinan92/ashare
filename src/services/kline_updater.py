"""
K线数据更新器 - 协调器
定时从各数据源获取最新K线数据并保存到 klines 表

重构说明:
- 拆分为4个专用更新器: IndexUpdater, ConceptUpdater, StockUpdater, CalendarUpdater
- 本模块作为协调器，提供统一的公共API
- 支持依赖注入 KlineRepository 和 SymbolRepository（用于测试）
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import DataUpdateLog, DataUpdateStatus
from src.repositories.kline_repository import KlineRepository
from src.repositories.symbol_repository import SymbolRepository
from src.services.calendar_updater import CalendarUpdater
from src.services.concept_updater import ConceptUpdater
from src.services.index_updater import IndexUpdater
from src.services.stock_updater import StockUpdater
from src.utils.logging import get_logger

logger = get_logger(__name__)


class KlineUpdater:
    """
    K线数据更新器 (协调器)

    委托给专用更新器:
    - IndexUpdater: 指数K线 (新浪)
    - ConceptUpdater: 概念板块K线 (同花顺)
    - StockUpdater: 股票K线 (东方财富 + 新浪)
    - CalendarUpdater: 交易日历 (Tushare) + 数据清理
    """

    def __init__(
        self,
        kline_repo: KlineRepository,
        symbol_repo: SymbolRepository,
    ):
        self.settings = get_settings()
        self.kline_repo = kline_repo
        self.symbol_repo = symbol_repo

        # 初始化专用更新器
        self._index_updater = IndexUpdater(kline_repo, symbol_repo)
        self._concept_updater = ConceptUpdater(kline_repo, symbol_repo)
        self._stock_updater = StockUpdater(kline_repo, symbol_repo)
        self._calendar_updater = CalendarUpdater(kline_repo)

    @classmethod
    def create_with_session(cls, session: Session) -> "KlineUpdater":
        """
        使用现有session创建KlineUpdater实例（工厂方法）
        """
        kline_repo = KlineRepository(session)
        symbol_repo = SymbolRepository(session)
        return cls(kline_repo, symbol_repo)

    def _log_update(
        self,
        update_type: str,
        status: DataUpdateStatus,
        records_count: int = 0,
        error_message: str = None,
    ):
        """记录更新日志"""
        now = datetime.now(timezone.utc)
        log = DataUpdateLog(
            update_type=update_type,
            status=status,
            records_updated=records_count,
            error_message=error_message,
            started_at=now,
            completed_at=now if status == DataUpdateStatus.COMPLETED else None,
        )
        self.kline_repo.session.add(log)
        self.kline_repo.session.commit()

    # ==================== 指数更新 ====================

    async def update_index_daily(self) -> int:
        """更新指数日线数据 (新浪API)"""
        try:
            count = await self._index_updater.update_daily()
            self._log_update("index_daily", DataUpdateStatus.COMPLETED, count)
            return count
        except Exception as e:
            self._log_update("index_daily", DataUpdateStatus.FAILED, error_message=str(e))
            return 0

    async def update_index_30m(self) -> int:
        """更新指数30分钟数据 (新浪API)"""
        try:
            count = await self._index_updater.update_30m()
            self._log_update("index_30m", DataUpdateStatus.COMPLETED, count)
            return count
        except Exception as e:
            self._log_update("index_30m", DataUpdateStatus.FAILED, error_message=str(e))
            return 0

    # ==================== 概念板块更新 ====================

    async def update_concept_daily(self) -> int:
        """更新概念日线数据 (同花顺)"""
        try:
            count = await self._concept_updater.update_daily()
            self._log_update("concept_daily", DataUpdateStatus.COMPLETED, count)
            return count
        except Exception as e:
            self._log_update("concept_daily", DataUpdateStatus.FAILED, error_message=str(e))
            return 0

    async def update_concept_30m(self) -> int:
        """更新概念30分钟数据 (同花顺)"""
        try:
            count = await self._concept_updater.update_30m()
            self._log_update("concept_30m", DataUpdateStatus.COMPLETED, count)
            return count
        except Exception as e:
            self._log_update("concept_30m", DataUpdateStatus.FAILED, error_message=str(e))
            return 0

    # ==================== 自选股更新 ====================

    async def update_stock_daily(self) -> int:
        """更新自选股日线数据"""
        try:
            count = await self._stock_updater.update_watchlist_daily()
            self._log_update("stock_daily", DataUpdateStatus.COMPLETED, count)
            return count
        except Exception as e:
            self._log_update("stock_daily", DataUpdateStatus.FAILED, error_message=str(e))
            return 0

    async def update_stock_30m(self) -> int:
        """更新自选股30分钟K线数据 (新浪财经)"""
        try:
            count = await self._stock_updater.update_watchlist_30m()
            self._log_update("stock_30m", DataUpdateStatus.COMPLETED, count)
            return count
        except Exception as e:
            self._log_update("stock_30m", DataUpdateStatus.FAILED, error_message=str(e))
            return 0

    async def update_all_stock_daily(self) -> int:
        """更新全市场股票日线数据"""
        try:
            count = await self._stock_updater.update_all_daily()
            self._log_update("all_stock_daily", DataUpdateStatus.COMPLETED, count)
            return count
        except Exception as e:
            self._log_update("all_stock_daily", DataUpdateStatus.FAILED, error_message=str(e))
            return 0

    async def update_single_stock_klines(self, ticker: str) -> dict:
        """更新单只股票的日线和30分钟数据"""
        return await self._stock_updater.update_single(ticker)

    # ==================== 交易日历更新 ====================

    def update_trade_calendar(self) -> int:
        """更新交易日历 (Tushare)"""
        return self._calendar_updater.update_trade_calendar()

    # ==================== 数据清理 ====================

    def cleanup_old_klines(self, days: int = 365) -> int:
        """清理过期K线数据"""
        return self._calendar_updater.cleanup_old_klines(days)


# ==================== 便捷函数 ====================

async def run_daily_update():
    """执行每日更新任务"""
    from src.database import SessionLocal

    session = SessionLocal()
    try:
        updater = KlineUpdater.create_with_session(session)

        # 并发更新指数日线和概念日线
        await asyncio.gather(
            updater.update_index_daily(),
            updater.update_concept_daily(),
        )

        logger.info("每日更新任务完成")
    finally:
        session.close()


async def run_30m_update():
    """执行30分钟更新任务"""
    from src.database import SessionLocal

    session = SessionLocal()
    try:
        updater = KlineUpdater.create_with_session(session)

        # 并发更新指数和概念30分钟线
        await asyncio.gather(
            updater.update_index_30m(),
            updater.update_concept_30m(),
        )

        logger.info("30分钟更新任务完成")
    finally:
        session.close()
