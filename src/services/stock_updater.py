"""
股票K线更新器
从东方财富和新浪获取股票日线和30分钟数据
"""

import time
from typing import TYPE_CHECKING

from src.models import KlineTimeframe, SymbolType, Watchlist
from src.services.kline_service import KlineService
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.repositories.kline_repository import KlineRepository
    from src.repositories.symbol_repository import SymbolRepository

logger = get_logger(__name__)


class StockUpdater:
    """股票K线更新器"""

    def __init__(
        self,
        kline_repo: "KlineRepository",
        symbol_repo: "SymbolRepository",
    ):
        self.kline_repo = kline_repo
        self.symbol_repo = symbol_repo

    def _get_watchlist_tickers(self) -> list[str]:
        """获取自选股代码列表"""
        tickers = self.kline_repo.session.query(Watchlist.ticker).all()
        return [t[0] for t in tickers]

    async def update_watchlist_daily(self) -> int:
        """更新自选股日线数据 (东方财富)"""
        from src.services.eastmoney_kline_provider import EastMoneyKlineProvider

        logger.info("开始更新自选股日线数据...")
        total_updated = 0

        tickers = self._get_watchlist_tickers()
        if not tickers:
            logger.info("自选股列表为空，跳过更新")
            return 0

        logger.info(f"共 {len(tickers)} 只自选股需要更新")
        provider = EastMoneyKlineProvider(delay=0.1)
        kline_service = KlineService(self.kline_repo, self.symbol_repo)

        try:
            for ticker in tickers:
                try:
                    df = provider.fetch_kline(ticker, period="day", limit=120)
                    if df is None or df.empty:
                        logger.debug(f"{ticker} 无日线数据")
                        continue

                    klines = []
                    for _, row in df.iterrows():
                        klines.append({
                            "datetime": row["timestamp"].strftime("%Y-%m-%d"),
                            "open": row["open"],
                            "high": row["high"],
                            "low": row["low"],
                            "close": row["close"],
                            "volume": row["volume"],
                            "amount": 0,
                        })

                    count = kline_service.save_klines(
                        symbol_type=SymbolType.STOCK,
                        symbol_code=ticker,
                        symbol_name=None,
                        timeframe=KlineTimeframe.DAY,
                        klines=klines,
                    )
                    total_updated += count
                    logger.debug(f"{ticker} 日线: {count} 条")

                except Exception as e:
                    logger.warning(f"{ticker} 日线更新失败: {e}")
                    continue

            logger.info(f"自选股日线更新完成，共 {total_updated} 条")

        except Exception as e:
            logger.exception("自选股日线更新失败")
            raise

        return total_updated

    async def update_watchlist_30m(self) -> int:
        """更新自选股30分钟K线数据 (新浪财经)"""
        from src.services.sina_kline_provider import SinaKlineProvider

        logger.info("开始更新自选股30分钟数据...")
        total_updated = 0

        tickers = self._get_watchlist_tickers()
        if not tickers:
            logger.info("自选股列表为空，跳过更新")
            return 0

        logger.info(f"共 {len(tickers)} 只自选股需要更新")
        provider = SinaKlineProvider(delay=0.5)
        kline_service = KlineService(self.kline_repo, self.symbol_repo)

        try:
            for ticker in tickers:
                try:
                    df = provider.fetch_kline(ticker, period="30m", limit=500)
                    if df is None or df.empty:
                        logger.debug(f"{ticker} 无30分钟数据")
                        continue

                    klines = []
                    for _, row in df.iterrows():
                        klines.append({
                            "datetime": row["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                            "open": row["open"],
                            "high": row["high"],
                            "low": row["low"],
                            "close": row["close"],
                            "volume": row["volume"],
                            "amount": 0,
                        })

                    count = kline_service.save_klines(
                        symbol_type=SymbolType.STOCK,
                        symbol_code=ticker,
                        symbol_name=None,
                        timeframe=KlineTimeframe.MINS_30,
                        klines=klines,
                    )
                    total_updated += count
                    logger.debug(f"{ticker} 30分钟: {count} 条")

                except Exception as e:
                    logger.warning(f"{ticker} 30分钟更新失败: {e}")
                    continue

            logger.info(f"自选股30分钟更新完成，共 {total_updated} 条")

        except Exception as e:
            logger.exception("自选股30分钟更新失败")
            raise

        return total_updated

    async def update_all_daily(self) -> int:
        """
        更新全市场股票日线数据 (东方财富)

        每只股票只获取最近20条日线，用于每日增量更新
        预计耗时: 5450只 × 0.1秒 ≈ 9分钟
        """
        from src.models import SymbolMetadata
        from src.services.eastmoney_kline_provider import EastMoneyKlineProvider

        logger.info("=" * 50)
        logger.info("开始更新全市场股票日线数据...")
        logger.info("=" * 50)
        total_updated = 0
        success_count = 0
        fail_count = 0
        provider = EastMoneyKlineProvider(delay=0.1)
        kline_service = KlineService(self.kline_repo, self.symbol_repo)

        try:
            all_tickers = self.kline_repo.session.query(SymbolMetadata.ticker).all()
            tickers = [t[0] for t in all_tickers]
            total = len(tickers)

            logger.info(f"共 {total} 只股票需要更新")
            start_time = time.time()

            for i, ticker in enumerate(tickers):
                try:
                    df = provider.fetch_kline(ticker, period="day", limit=20)
                    if df is None or df.empty:
                        fail_count += 1
                        continue

                    klines = []
                    for _, row in df.iterrows():
                        klines.append({
                            "datetime": row["timestamp"].strftime("%Y-%m-%d"),
                            "open": row["open"],
                            "high": row["high"],
                            "low": row["low"],
                            "close": row["close"],
                            "volume": row["volume"],
                            "amount": 0,
                        })

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
            logger.info("=" * 50)
            logger.info(
                f"全市场日线更新完成 | 耗时: {elapsed/60:.1f}分钟 | "
                f"成功: {success_count} | 失败: {fail_count} | 共 {total_updated} 条"
            )
            logger.info("=" * 50)

        except Exception as e:
            logger.exception("全市场日线更新失败")
            raise

        return total_updated

    async def update_single(self, ticker: str) -> dict:
        """
        更新单只股票的日线和30分钟数据
        用于添加自选股时立即获取数据

        Args:
            ticker: 股票代码 (6位)

        Returns:
            {"daily": 条数, "mins30": 条数}
        """
        from src.services.eastmoney_kline_provider import EastMoneyKlineProvider
        from src.services.sina_kline_provider import SinaKlineProvider

        result = {"daily": 0, "mins30": 0}
        logger.info(f"开始更新单股 {ticker} 的K线数据...")

        # 1. 更新日线 (东方财富)
        try:
            eastmoney = EastMoneyKlineProvider()
            daily_df = eastmoney.fetch_kline(ticker, period="day", limit=120)

            if daily_df is not None and not daily_df.empty:
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

        logger.info(
            f"单股 {ticker} 更新完成: 日线 {result['daily']} 条, "
            f"30分钟 {result['mins30']} 条"
        )
        return result
