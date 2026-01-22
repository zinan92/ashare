"""
K线数据更新器
定时从各数据源获取最新K线数据并保存到 klines 表

重构说明:
- 支持依赖注入 KlineRepository 和 SymbolRepository（用于测试）
- 保持向后兼容：无参数调用时自动创建 repositories
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
import pandas as pd
from sqlalchemy.orm import Session

from src.config import get_settings
from src.database import SessionLocal
from src.models import (
    DataUpdateLog,
    DataUpdateStatus,
    Kline,
    KlineTimeframe,
    SymbolType,
    TradeCalendar,
)
from src.repositories.kline_repository import KlineRepository
from src.repositories.symbol_repository import SymbolRepository
from src.schemas.normalized import NormalizedDate, NormalizedDateTime, NormalizedTicker
from src.services.kline_service import KlineService, calculate_macd
from src.services.tushare_client import TushareClient
from src.utils.logging import get_logger

logger = get_logger(__name__)

# 同花顺 API 配置
THS_BASE_URL = "http://d.10jqka.com.cn/v4"
THS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "http://q.10jqka.com.cn/",
}

# Sina API 配置
SINA_HEADERS = {
    "Referer": "http://finance.sina.com.cn/",
    "User-Agent": "Mozilla/5.0",
}

# 指数列表
INDEX_LIST = [
    ("000001.SH", "上证指数"),
    ("399001.SZ", "深证成指"),
    ("399006.SZ", "创业板指"),
    ("000688.SH", "科创50"),
    ("899050.BJ", "北证50"),
]


class KlineUpdater:
    """
    K线数据更新器

    支持两种初始化方式:
    1. 无参数（向后兼容）: KlineUpdater()
    2. 依赖注入（用于测试）: KlineUpdater(kline_repo, symbol_repo)
    """

    def __init__(
        self,
        kline_repo: Optional[KlineRepository] = None,
        symbol_repo: Optional[SymbolRepository] = None,
    ):
        """
        初始化K线更新器

        Args:
            kline_repo: K线数据仓库（可选，用于依赖注入/测试）
            symbol_repo: 标的数据仓库（可选，用于依赖注入/测试）

        注意: 如果不提供 repositories，方法内部会自动创建 SessionLocal()
        """
        self.settings = get_settings()
        self._tushare_client: Optional[TushareClient] = None

        # 可选的 repository 注入（用于测试）
        self.kline_repo = kline_repo
        self.symbol_repo = symbol_repo

    @property
    def tushare_client(self) -> TushareClient:
        if self._tushare_client is None:
            self._tushare_client = TushareClient(
                token=self.settings.tushare_token,
                points=self.settings.tushare_points,
                delay=self.settings.tushare_delay,
                max_retries=self.settings.tushare_max_retries,
            )
        return self._tushare_client

    def _log_update(
        self,
        session,
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
        session.add(log)
        session.commit()

    # ==================== 指数更新 ====================

    async def update_index_daily(self) -> int:
        """更新指数日线数据 (新浪API - 实时数据)"""
        logger.info("开始更新指数日线数据...")
        total_updated = 0

        session = SessionLocal()
        try:
            async def fetch_daily(ts_code: str, name: str) -> list[dict]:
                """异步获取日线K线"""
                code, market = ts_code.split(".")
                if market == "SH":
                    sina_code = f"sh{code}"
                elif market == "SZ":
                    sina_code = f"sz{code}"
                elif market == "BJ":
                    sina_code = f"bj{code}"
                else:
                    return []

                # scale=240 表示日线，获取最近60条
                url = f"https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData?symbol={sina_code}&scale=240&datalen=60"

                try:
                    async with httpx.AsyncClient() as client:
                        resp = await client.get(url, headers=SINA_HEADERS, timeout=15.0)
                        resp.raise_for_status()

                        data = resp.json()
                        if not data:
                            return []

                        klines = []
                        for k in data:
                            # 日线格式: "2026-01-12"
                            trade_date = k["day"].split(" ")[0]
                            klines.append({
                                "datetime": trade_date,
                                "open": float(k["open"]),
                                "high": float(k["high"]),
                                "low": float(k["low"]),
                                "close": float(k["close"]),
                                "volume": int(float(k["volume"])),
                                "amount": float(k.get("amount", 0)),
                            })

                        return klines
                except Exception as e:
                    logger.error(f"获取 {name} 日线数据失败: {e}")
                    return []

            # 并发获取所有指数数据
            tasks = [fetch_daily(ts_code, name) for ts_code, name in INDEX_LIST]
            results = await asyncio.gather(*tasks)

            for (ts_code, name), klines in zip(INDEX_LIST, results):
                if klines:
                    service = KlineService.create_with_session(session)
                    count = service.save_klines(
                    symbol_type=SymbolType.INDEX,
                    symbol_code=ts_code,
                    symbol_name=name,
                    timeframe=KlineTimeframe.DAY,
                    klines=klines,
                    )
                    total_updated += count
                    logger.info(f"  {name}: {count} 条")

            self._log_update(
                session, "index_daily", DataUpdateStatus.COMPLETED, total_updated
            )
            logger.info(f"指数日线更新完成，共 {total_updated} 条")

        except Exception as e:
            logger.exception("指数日线更新失败")
            self._log_update(
                session, "index_daily", DataUpdateStatus.FAILED, error_message=str(e)
            )
        finally:
            session.close()

        return total_updated

    async def update_index_30m(self) -> int:
        """更新指数30分钟数据 (Sina API)"""
        logger.info("开始更新指数30分钟数据...")
        total_updated = 0

        session = SessionLocal()
        try:
            async def fetch_30m(ts_code: str, name: str) -> list[dict]:
                """异步获取30分钟K线"""
                code, market = ts_code.split(".")
                if market == "SH":
                    sina_code = f"sh{code}"
                elif market == "SZ":
                    sina_code = f"sz{code}"
                elif market == "BJ":
                    sina_code = f"bj{code}"
                else:
                    return []

                # 只获取最近60条 (约2天数据)
                url = f"https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData?symbol={sina_code}&scale=30&datalen=60"

                try:
                    async with httpx.AsyncClient() as client:
                        resp = await client.get(url, headers=SINA_HEADERS, timeout=15.0)
                        resp.raise_for_status()

                        data = resp.json()
                        if not data:
                            return []

                        klines = []
                        for k in data:
                            dt = datetime.strptime(k["day"], "%Y-%m-%d %H:%M:%S")
                            klines.append({
                                "datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
                                "open": float(k["open"]),
                                "high": float(k["high"]),
                                "low": float(k["low"]),
                                "close": float(k["close"]),
                                "volume": int(float(k["volume"])),
                                "amount": float(k["amount"]),
                            })

                        return klines
                except Exception as e:
                    logger.error(f"获取 {name} 30分钟数据失败: {e}")
                    return []

            # 并发获取所有指数数据
            tasks = [fetch_30m(ts_code, name) for ts_code, name in INDEX_LIST]
            results = await asyncio.gather(*tasks)

            for (ts_code, name), klines in zip(INDEX_LIST, results):
                if klines:
                    service = KlineService.create_with_session(session)
                    count = service.save_klines(
                        symbol_type=SymbolType.INDEX,
                        symbol_code=ts_code,
                        symbol_name=name,
                        timeframe=KlineTimeframe.MINS_30,
                        klines=klines,
                    )
                    total_updated += count
                    logger.info(f"  {name}: {count} 条")

            self._log_update(
                session, "index_30m", DataUpdateStatus.COMPLETED, total_updated
            )
            logger.info(f"指数30分钟更新完成，共 {total_updated} 条")

        except Exception as e:
            logger.exception("指数30分钟更新失败")
            self._log_update(
                session, "index_30m", DataUpdateStatus.FAILED, error_message=str(e)
            )
        finally:
            session.close()

        return total_updated

    # ==================== 概念板块更新 ====================

    def _load_hot_concepts(self) -> list[tuple[str, str]]:
        """加载热门概念列表 (code, name)"""
        from pathlib import Path

        data_dir = Path(__file__).parent.parent.parent / "data"

        # 加载概念映射
        concept_file = data_dir / "concept_to_tickers.csv"
        hot_file = data_dir / "hot_concept_categories.csv"

        if not concept_file.exists() or not hot_file.exists():
            logger.warning("概念映射文件不存在")
            return []

        # 加载概念代码映射
        df_mapping = pd.read_csv(concept_file)
        code_map = {}
        for _, row in df_mapping.iterrows():
            code = row["板块代码"].replace(".TI", "")
            name = row["板块名称"]
            code_map[name] = code

        # 加载热门概念
        df_hot = pd.read_csv(hot_file)
        hot_concepts = df_hot["概念名称"].tolist()

        result = []
        for name in hot_concepts:
            if name in code_map:
                result.append((code_map[name], name))

        return result

    async def _fetch_ths_kline(
        self, code: str, period: str = "01"
    ) -> tuple[str, list[dict]]:
        """
        获取同花顺K线数据

        Args:
            code: 板块代码 (如 885556)
            period: "01"=日线, "30"=30分钟

        Returns:
            (name, klines)
        """
        import json
        import re

        url = f"{THS_BASE_URL}/line/bk_{code}/{period}/last.js"

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(url, headers=THS_HEADERS, timeout=10.0)
                resp.raise_for_status()

                # 解析 JSONP 响应
                text = resp.text
                match = re.search(r"\((\{.*\})\)", text, re.DOTALL)
                if not match:
                    return "", []

                data = json.loads(match.group(1))
                name = data.get("name", "")
                data_str = data.get("data", "")

                if not data_str:
                    return name, []

                klines = []
                for item in data_str.split(";"):
                    parts = item.split(",")
                    if len(parts) >= 7 and parts[1]:
                        try:
                            raw_time = parts[0]
                            # 使用标准化模型解析时间
                            # 日线格式: YYYYMMDD, 30分钟格式: YYYYMMDDHHMM
                            if period == "01":
                                trade_time = NormalizedDate(value=raw_time).to_iso()
                            else:
                                trade_time = NormalizedDateTime(value=raw_time).to_iso()

                            klines.append({
                                "datetime": trade_time,
                                "open": float(parts[1]),
                                "high": float(parts[2]),
                                "low": float(parts[3]),
                                "close": float(parts[4]),
                                "volume": int(parts[5]),
                                "amount": float(parts[6]),
                            })
                        except (ValueError, IndexError) as e:
                            logger.debug(f"解析K线数据失败: {e}")
                            continue

                return name, klines

        except Exception as e:
            logger.error(f"获取概念 {code} K线失败: {e}")
            return "", []

    async def update_concept_daily(self) -> int:
        """更新概念日线数据 (同花顺)"""
        logger.info("开始更新概念日线数据...")
        total_updated = 0

        concepts = self._load_hot_concepts()
        if not concepts:
            logger.warning("未找到热门概念列表")
            return 0

        session = SessionLocal()
        try:
            # 批量获取数据
            async def fetch_one(code: str, expected_name: str):
                name, klines = await self._fetch_ths_kline(code, "01")
                return code, name or expected_name, klines

            # 分批处理，避免并发过多
            batch_size = 10
            for i in range(0, len(concepts), batch_size):
                batch = concepts[i : i + batch_size]
                tasks = [fetch_one(code, name) for code, name in batch]
                results = await asyncio.gather(*tasks)

                for code, name, klines in results:
                    if klines:
                        service = KlineService.create_with_session(session)
                        count = service.save_klines(
                            symbol_type=SymbolType.CONCEPT,
                            symbol_code=code,
                            symbol_name=name,
                            timeframe=KlineTimeframe.DAY,
                            klines=klines,
                        )
                        total_updated += count

                # 批次间延迟
                await asyncio.sleep(0.5)

            self._log_update(
                session, "concept_daily", DataUpdateStatus.COMPLETED, total_updated
            )
            logger.info(f"概念日线更新完成，共 {total_updated} 条")

        except Exception as e:
            logger.exception("概念日线更新失败")
            self._log_update(
                session, "concept_daily", DataUpdateStatus.FAILED, error_message=str(e)
            )
        finally:
            session.close()

        return total_updated

    async def update_concept_30m(self) -> int:
        """更新概念30分钟数据 (同花顺)"""
        logger.info("开始更新概念30分钟数据...")
        total_updated = 0

        concepts = self._load_hot_concepts()
        if not concepts:
            logger.warning("未找到热门概念列表")
            return 0

        session = SessionLocal()
        try:
            async def fetch_one(code: str, expected_name: str):
                name, klines = await self._fetch_ths_kline(code, "30")
                return code, name or expected_name, klines

            batch_size = 10
            for i in range(0, len(concepts), batch_size):
                batch = concepts[i : i + batch_size]
                tasks = [fetch_one(code, name) for code, name in batch]
                results = await asyncio.gather(*tasks)

                for code, name, klines in results:
                    if klines:
                        service = KlineService.create_with_session(session)
                        count = service.save_klines(
                            symbol_type=SymbolType.CONCEPT,
                            symbol_code=code,
                            symbol_name=name,
                            timeframe=KlineTimeframe.MINS_30,
                            klines=klines,
                        )
                        total_updated += count

                await asyncio.sleep(0.5)

            self._log_update(
                session, "concept_30m", DataUpdateStatus.COMPLETED, total_updated
            )
            logger.info(f"概念30分钟更新完成，共 {total_updated} 条")

        except Exception as e:
            logger.exception("概念30分钟更新失败")
            self._log_update(
                session, "concept_30m", DataUpdateStatus.FAILED, error_message=str(e)
            )
        finally:
            session.close()

        return total_updated

    # ==================== 自选股更新 ====================

    def _get_watchlist_tickers(self) -> list[str]:
        """获取自选股代码列表"""
        from src.models import Watchlist

        # 如果注入了 symbol_repo，优先使用（但 Watchlist 不在 SymbolRepository 中）
        # 这里保持原有逻辑，未来可以添加 WatchlistRepository
        session = SessionLocal()
        try:
            tickers = session.query(Watchlist.ticker).all()
            return [t[0] for t in tickers]
        finally:
            session.close()

    async def update_stock_daily(self) -> int:
        """
        更新自选股日线数据 (东方财富)
        """
        from src.services.eastmoney_kline_provider import EastMoneyKlineProvider

        logger.info("开始更新自选股日线数据...")
        total_updated = 0

        tickers = self._get_watchlist_tickers()
        if not tickers:
            logger.info("自选股列表为空，跳过更新")
            return 0

        logger.info(f"共 {len(tickers)} 只自选股需要更新")

        session = SessionLocal()
        provider = EastMoneyKlineProvider(delay=0.1)
        kline_service = KlineService.create_with_session(session)

        try:
            for ticker in tickers:
                try:
                    df = provider.fetch_kline(ticker, period="day", limit=120)
                    if df is None or df.empty:
                        logger.debug(f"{ticker} 无日线数据")
                        continue

                    # 转换为klines格式
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
                session, "stock_daily", DataUpdateStatus.COMPLETED, total_updated
            )
            logger.info(f"自选股日线更新完成，共 {total_updated} 条")

        except Exception as e:
            logger.exception("自选股日线更新失败")
            self._log_update(
                session, "stock_daily", DataUpdateStatus.FAILED, error_message=str(e)
            )
        finally:
            session.close()

        return total_updated

    async def update_stock_30m(self) -> int:
        """
        更新自选股30分钟K线数据 (新浪财经)
        """
        from src.services.sina_kline_provider import SinaKlineProvider

        logger.info("开始更新自选股30分钟数据...")
        total_updated = 0

        tickers = self._get_watchlist_tickers()
        if not tickers:
            logger.info("自选股列表为空，跳过更新")
            return 0

        logger.info(f"共 {len(tickers)} 只自选股需要更新")

        session = SessionLocal()
        provider = SinaKlineProvider(delay=0.5)
        kline_service = KlineService.create_with_session(session)

        try:
            for ticker in tickers:
                try:
                    df = provider.fetch_kline(ticker, period="30m", limit=500)
                    if df is None or df.empty:
                        logger.debug(f"{ticker} 无30分钟数据")
                        continue

                    # 转换为klines格式
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

                    # 保存到数据库
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

            self._log_update(
                session, "stock_30m", DataUpdateStatus.COMPLETED, total_updated
            )
            logger.info(f"自选股30分钟更新完成，共 {total_updated} 条")

        except Exception as e:
            logger.exception("自选股30分钟更新失败")
            self._log_update(
                session, "stock_30m", DataUpdateStatus.FAILED, error_message=str(e)
            )
        finally:
            session.close()

        return total_updated

    async def update_all_stock_daily(self) -> int:
        """
        更新全市场股票日线数据 (东方财富)

        每只股票只获取最近20条日线，用于每日增量更新
        预计耗时: 5450只 × 0.1秒 ≈ 9分钟
        """
        from src.services.eastmoney_kline_provider import EastMoneyKlineProvider

        logger.info("=" * 50)
        logger.info("开始更新全市场股票日线数据...")
        logger.info("=" * 50)
        total_updated = 0
        success_count = 0
        fail_count = 0

        session = SessionLocal()
        provider = EastMoneyKlineProvider(delay=0.1)
        kline_service = KlineService.create_with_session(session)

        try:
            # 获取所有股票代码
            from src.models import SymbolMetadata
            all_tickers = session.query(SymbolMetadata.ticker).all()
            tickers = [t[0] for t in all_tickers]
            total = len(tickers)

            logger.info(f"共 {total} 只股票需要更新")
            start_time = time.time()

            for i, ticker in enumerate(tickers):
                try:
                    # 只获取最近20条日线用于增量更新
                    df = provider.fetch_kline(ticker, period="day", limit=20)
                    if df is None or df.empty:
                        fail_count += 1
                        continue

                    # 转换为klines格式
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
                session, "all_stock_daily", DataUpdateStatus.COMPLETED, total_updated
            )
            logger.info("=" * 50)
            logger.info(
                f"全市场日线更新完成 | 耗时: {elapsed/60:.1f}分钟 | "
                f"成功: {success_count} | 失败: {fail_count} | 共 {total_updated} 条"
            )
            logger.info("=" * 50)

        except Exception as e:
            logger.exception("全市场日线更新失败")
            self._log_update(
                session, "all_stock_daily", DataUpdateStatus.FAILED, error_message=str(e)
            )
        finally:
            session.close()

        return total_updated

    # ==================== 单股更新 (添加自选时触发) ====================

    async def update_single_stock_klines(self, ticker: str) -> dict:
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
                # 转换DataFrame为KlineService期望的格式 (timestamp -> datetime)
                if 'timestamp' in daily_df.columns:
                    daily_df = daily_df.rename(columns={'timestamp': 'datetime'})
                daily_klines = daily_df.to_dict('records')
                session = SessionLocal()
                try:
                    service = KlineService.create_with_session(session)
                    count = service.save_klines(
                        symbol_type=SymbolType.STOCK,
                        symbol_code=ticker,
                        symbol_name=None,
                        timeframe=KlineTimeframe.DAY,
                        klines=daily_klines,
                    )
                    session.commit()
                    result["daily"] = count
                    logger.info(f"{ticker} 日线更新: {count} 条")
                finally:
                    session.close()
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
                session = SessionLocal()
                try:
                    service = KlineService.create_with_session(session)
                    count = service.save_klines(
                        symbol_type=SymbolType.STOCK,
                        symbol_code=ticker,
                        symbol_name=None,
                        timeframe=KlineTimeframe.MINS_30,
                        klines=mins30_klines,
                    )
                    session.commit()
                    result["mins30"] = count
                    logger.info(f"{ticker} 30分钟更新: {count} 条")
                finally:
                    session.close()
        except Exception as e:
            logger.warning(f"{ticker} 30分钟更新失败: {e}")

        logger.info(f"单股 {ticker} 更新完成: 日线 {result['daily']} 条, 30分钟 {result['mins30']} 条")
        return result

    # ==================== 交易日历更新 ====================

    def update_trade_calendar(self) -> int:
        """更新交易日历 (Tushare)"""
        logger.info("更新交易日历...")

        session = SessionLocal()
        try:
            # 获取今年和明年的交易日历
            current_year = datetime.now().year
            start_date = f"{current_year}0101"
            end_date = f"{current_year + 1}1231"

            df = self.tushare_client.fetch_trade_calendar(
                start_date=start_date, end_date=end_date
            )

            if df.empty:
                logger.warning("未获取到交易日历数据")
                return 0

            count = 0
            for _, row in df.iterrows():
                # 使用标准化模型转换日期
                trade_date = NormalizedDate(value=str(row["cal_date"])).to_iso()
                is_open = row["is_open"] == 1

                # Upsert
                existing = (
                    session.query(TradeCalendar)
                    .filter(TradeCalendar.date == trade_date)
                    .first()
                )
                if existing:
                    existing.is_trading_day = is_open
                else:
                    session.add(
                        TradeCalendar(date=trade_date, is_trading_day=is_open)
                    )
                count += 1

            session.commit()
            self._log_update(
                session, "trade_calendar", DataUpdateStatus.COMPLETED, count
            )
            logger.info(f"交易日历更新完成，共 {count} 条")
            return count

        except Exception as e:
            logger.exception("交易日历更新失败")
            self._log_update(
                session,
                "trade_calendar",
                DataUpdateStatus.FAILED,
                error_message=str(e),
            )
            return 0
        finally:
            session.close()

    # ==================== 数据清理 ====================

    def cleanup_old_klines(self, days: int = 365) -> int:
        """
        清理过期K线数据

        Args:
            days: 保留最近N天的数据

        Returns:
            删除的记录数
        """
        logger.info(f"开始清理 {days} 天前的K线数据...")
        total_deleted = 0

        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        session = SessionLocal()
        try:
            # 清理30分钟线 (只保留90天)
            mins_cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
            deleted = (
                session.query(Kline)
                .filter(
                    Kline.timeframe == KlineTimeframe.MINS_30,
                    Kline.trade_time < mins_cutoff,
                )
                .delete()
            )
            total_deleted += deleted
            logger.info(f"  30分钟线: 删除 {deleted} 条")

            # 清理日线 (保留1年)
            deleted = (
                session.query(Kline)
                .filter(
                    Kline.timeframe == KlineTimeframe.DAY,
                    Kline.trade_time < cutoff_date,
                )
                .delete()
            )
            total_deleted += deleted
            logger.info(f"  日线: 删除 {deleted} 条")

            session.commit()
            self._log_update(
                session, "cleanup", DataUpdateStatus.COMPLETED, total_deleted
            )
            logger.info(f"数据清理完成，共删除 {total_deleted} 条")

        except Exception as e:
            logger.exception("数据清理失败")
            session.rollback()
        finally:
            session.close()

        return total_deleted


# 便捷函数
async def run_daily_update():
    """执行每日更新任务"""
    updater = KlineUpdater()

    # 并发更新指数日线和概念日线
    await asyncio.gather(
        updater.update_index_daily(),
        updater.update_concept_daily(),
    )

    logger.info("每日更新任务完成")


async def run_30m_update():
    """执行30分钟更新任务"""
    updater = KlineUpdater()

    # 并发更新指数和概念30分钟线
    await asyncio.gather(
        updater.update_index_30m(),
        updater.update_concept_30m(),
    )

    logger.info("30分钟更新任务完成")
