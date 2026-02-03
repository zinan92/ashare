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
<<<<<<< HEAD
        """更新自选股日线数据 (东方财富)"""
        try:
            count = await self._stock_updater.update_watchlist_daily()
            self._log_update("stock_daily", DataUpdateStatus.COMPLETED, count)
            return count
=======
        """
        更新自选股日线数据 (TuShare)
        """
        logger.info("开始更新自选股日线数据...")
        total_updated = 0

        tickers = self._get_watchlist_tickers()
        if not tickers:
            logger.info("自选股列表为空，跳过更新")
            return 0

        logger.info(f"共 {len(tickers)} 只自选股需要更新")
        kline_service = KlineService(self.kline_repo, self.symbol_repo)

        try:
            for ticker in tickers:
                try:
                    ts_code = self.tushare_client.normalize_ts_code(ticker)
                    df = self.tushare_client.fetch_daily(ts_code=ts_code)
                    if df is None or df.empty:
                        logger.debug(f"{ticker} 无日线数据")
                        continue

                    # 转换为klines格式
                    klines = []
                    for _, row in df.head(120).iterrows():
                        klines.append({
                            "datetime": row["trade_date"],
                            "open": row["open"],
                            "high": row["high"],
                            "low": row["low"],
                            "close": row["close"],
                            "volume": row["vol"],
                            "amount": row.get("amount", 0),
                        })

                    # 保存到数据库
                    count = kline_service.save_klines(
                        symbol_type=SymbolType.STOCK,
                        symbol_code=ticker,
                        symbol_name=None,  # 暂不保存名称
                        timeframe=KlineTimeframe.DAY,
                        klines=klines,
                    )
                    total_updated += count
                    logger.debug(f"{ticker} 日线: {count} 条")

                except Exception as e:
                    logger.warning(f"{ticker} 日线更新失败: {e}")
                    continue

            self._log_update(
                self.kline_repo.session, "stock_daily", DataUpdateStatus.COMPLETED, total_updated
            )
            logger.info(f"自选股日线更新完成，共 {total_updated} 条")

>>>>>>> ccf1351 (refactor: Perception Layer Phase 0+1 — cleanup + core interfaces (#40 #41))
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
<<<<<<< HEAD
        """更新全市场股票日线数据 (东方财富)"""
        try:
            count = await self._stock_updater.update_all_daily()
            self._log_update("all_stock_daily", DataUpdateStatus.COMPLETED, count)
            return count
=======
        """
        更新全市场股票日线数据 (TuShare)

        每只股票只获取最近20条日线，用于每日增量更新
        预计耗时: 5450只 × 0.1秒 ≈ 9分钟
        """
        logger.info("=" * 50)
        logger.info("开始更新全市场股票日线数据...")
        logger.info("=" * 50)
        total_updated = 0
        success_count = 0
        fail_count = 0
        kline_service = KlineService(self.kline_repo, self.symbol_repo)

        try:
            # 获取所有股票代码
            from src.models import SymbolMetadata
            all_tickers = self.kline_repo.session.query(SymbolMetadata.ticker).all()
            tickers = [t[0] for t in all_tickers]
            total = len(tickers)

            logger.info(f"共 {total} 只股票需要更新")
            start_time = time.time()

            for i, ticker in enumerate(tickers):
                try:
                    # 只获取最近20条日线用于增量更新
                    ts_code = self.tushare_client.normalize_ts_code(ticker)
                    df = self.tushare_client.fetch_daily(ts_code=ts_code)
                    if df is None or df.empty:
                        fail_count += 1
                        continue

                    # 转换为klines格式
                    klines = []
                    for _, row in df.head(20).iterrows():
                        klines.append({
                            "datetime": row["trade_date"],
                            "open": row["open"],
                            "high": row["high"],
                            "low": row["low"],
                            "close": row["close"],
                            "volume": row["vol"],
                            "amount": row.get("amount", 0),
                        })

                    # 保存到数据库
                    count = kline_service.save_klines(
                        symbol_type=SymbolType.STOCK,
                        symbol_code=ticker,
                        symbol_name=None,
                        timeframe=KlineTimeframe.DAY,
                        klines=klines,
                    )
                    total_updated += count
                    success_count += 1

                except Exception as e:
                    fail_count += 1
                    logger.debug(f"{ticker} 更新失败: {e}")
                    continue

                # 每500只股票打印一次进度
                if (i + 1) % 500 == 0:
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed
                    remaining = (total - i - 1) / rate if rate > 0 else 0
                    logger.info(
                        f"进度: {i + 1}/{total} ({(i+1)/total*100:.1f}%) | "
                        f"成功: {success_count} | 失败: {fail_count} | "
                        f"预计剩余: {remaining/60:.1f}分钟"
                    )

            elapsed = time.time() - start_time
            self._log_update(
                self.kline_repo.session, "all_stock_daily", DataUpdateStatus.COMPLETED, total_updated
            )
            logger.info("=" * 50)
            logger.info(
                f"全市场日线更新完成 | 耗时: {elapsed/60:.1f}分钟 | "
                f"成功: {success_count} | 失败: {fail_count} | 共 {total_updated} 条"
            )
            logger.info("=" * 50)

>>>>>>> ccf1351 (refactor: Perception Layer Phase 0+1 — cleanup + core interfaces (#40 #41))
        except Exception as e:
            self._log_update("all_stock_daily", DataUpdateStatus.FAILED, error_message=str(e))
            return 0

    async def update_single_stock_klines(self, ticker: str) -> dict:
<<<<<<< HEAD
        """更新单只股票的日线和30分钟数据"""
        return await self._stock_updater.update_single(ticker)
=======
        """
        更新单只股票的日线和30分钟数据
        用于添加自选股时立即获取数据

        Args:
            ticker: 股票代码 (6位)

        Returns:
            {"daily": 条数, "mins30": 条数}
        """
        from src.services.sina_kline_provider import SinaKlineProvider

        result = {"daily": 0, "mins30": 0}
        logger.info(f"开始更新单股 {ticker} 的K线数据...")

        # 1. 更新日线 (TuShare)
        try:
            ts_code = self.tushare_client.normalize_ts_code(ticker)
            daily_df = self.tushare_client.fetch_daily(ts_code=ts_code)

            if daily_df is not None and not daily_df.empty:
                # 转换DataFrame为KlineService期望的格式 (timestamp -> datetime)
                if 'timestamp' in daily_df.columns:
                    daily_df = daily_df.rename(columns={'timestamp': 'datetime'})
                daily_klines = daily_df.to_dict('records')
                service = KlineService(self.kline_repo, self.symbol_repo)
                count = service.save_klines(
                    symbol_type=SymbolType.STOCK,
                    symbol_code=ticker,
                    symbol_name=None,
                    timeframe=KlineTimeframe.DAY,
                    klines=daily_klines,
                )
                self.kline_repo.session.commit()
                result["daily"] = count
                logger.info(f"{ticker} 日线更新: {count} 条")

        except Exception as e:
            logger.warning(f"{ticker} 日线更新失败: {e}")

        # 2. 更新30分钟 (新浪财经)
        try:
            sina = SinaKlineProvider()
            mins30_df = sina.fetch_kline(ticker, period="30m", limit=80)

            if mins30_df is not None and not mins30_df.empty:
                # 转换DataFrame为KlineService期望的格式 (timestamp -> datetime)
                if 'timestamp' in mins30_df.columns:
                    mins30_df = mins30_df.rename(columns={'timestamp': 'datetime'})
                mins30_klines = mins30_df.to_dict('records')
                service = KlineService(self.kline_repo, self.symbol_repo)
                count = service.save_klines(
                    symbol_type=SymbolType.STOCK,
                    symbol_code=ticker,
                    symbol_name=None,
                    timeframe=KlineTimeframe.MINS_30,
                    klines=mins30_klines,
                )
                self.kline_repo.session.commit()
                result["mins30"] = count
                logger.info(f"{ticker} 30分钟更新: {count} 条")

        except Exception as e:
            logger.warning(f"{ticker} 30分钟更新失败: {e}")

        logger.info(f"单股 {ticker} 更新完成: 日线 {result['daily']} 条, 30分钟 {result['mins30']} 条")
        return result
>>>>>>> ccf1351 (refactor: Perception Layer Phase 0+1 — cleanup + core interfaces (#40 #41))

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
