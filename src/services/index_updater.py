"""
指数K线更新器
从新浪财经获取指数日线和30分钟数据
"""

import asyncio
from typing import TYPE_CHECKING

import httpx

from src.models import KlineTimeframe, SymbolType
from src.services.kline_service import KlineService
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.repositories.kline_repository import KlineRepository
    from src.repositories.symbol_repository import SymbolRepository

logger = get_logger(__name__)

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


class IndexUpdater:
    """指数K线更新器"""

    def __init__(
        self,
        kline_repo: "KlineRepository",
        symbol_repo: "SymbolRepository",
    ):
        self.kline_repo = kline_repo
        self.symbol_repo = symbol_repo

    def _get_sina_code(self, ts_code: str) -> str | None:
        """将ts_code转换为新浪代码格式"""
        code, market = ts_code.split(".")
        if market == "SH":
            return f"sh{code}"
        elif market == "SZ":
            return f"sz{code}"
        elif market == "BJ":
            return f"bj{code}"
        return None

    async def _fetch_kline(
        self, ts_code: str, name: str, scale: int
    ) -> list[dict]:
        """
        从新浪获取K线数据

        Args:
            ts_code: 标的代码 (如 000001.SH)
            name: 标的名称
            scale: K线周期 (240=日线, 30=30分钟)

        Returns:
            K线数据列表
        """
        sina_code = self._get_sina_code(ts_code)
        if not sina_code:
            return []

        url = (
            f"https://quotes.sina.cn/cn/api/json_v2.php/"
            f"CN_MarketDataService.getKLineData?"
            f"symbol={sina_code}&scale={scale}&datalen=60"
        )

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(url, headers=SINA_HEADERS, timeout=15.0)
                resp.raise_for_status()

                data = resp.json()
                if not data:
                    return []

                klines = []
                for k in data:
                    # 日线格式: "2026-01-12", 分钟线: "2026-01-12 10:30:00"
                    if scale == 240:
                        trade_time = k["day"].split(" ")[0]
                    else:
                        trade_time = k["day"]

                    klines.append({
                        "datetime": trade_time,
                        "open": float(k["open"]),
                        "high": float(k["high"]),
                        "low": float(k["low"]),
                        "close": float(k["close"]),
                        "volume": int(float(k["volume"])),
                        "amount": float(k.get("amount", 0)),
                    })

                return klines
        except Exception as e:
            logger.error(f"获取 {name} K线数据失败: {e}")
            return []

    async def update_daily(self) -> int:
        """更新指数日线数据"""
        logger.info("开始更新指数日线数据...")
        total_updated = 0

        try:
            tasks = [
                self._fetch_kline(ts_code, name, scale=240)
                for ts_code, name in INDEX_LIST
            ]
            results = await asyncio.gather(*tasks)

            for (ts_code, name), klines in zip(INDEX_LIST, results):
                if klines:
                    service = KlineService(self.kline_repo, self.symbol_repo)
                    count = service.save_klines(
                        symbol_type=SymbolType.INDEX,
                        symbol_code=ts_code,
                        symbol_name=name,
                        timeframe=KlineTimeframe.DAY,
                        klines=klines,
                    )
                    total_updated += count
                    logger.info(f"  {name}: {count} 条")

            logger.info(f"指数日线更新完成，共 {total_updated} 条")

        except Exception as e:
            logger.exception("指数日线更新失败")
            raise

        return total_updated

    async def update_30m(self) -> int:
        """更新指数30分钟数据"""
        logger.info("开始更新指数30分钟数据...")
        total_updated = 0

        try:
            tasks = [
                self._fetch_kline(ts_code, name, scale=30)
                for ts_code, name in INDEX_LIST
            ]
            results = await asyncio.gather(*tasks)

            for (ts_code, name), klines in zip(INDEX_LIST, results):
                if klines:
                    service = KlineService(self.kline_repo, self.symbol_repo)
                    count = service.save_klines(
                        symbol_type=SymbolType.INDEX,
                        symbol_code=ts_code,
                        symbol_name=name,
                        timeframe=KlineTimeframe.MINS_30,
                        klines=klines,
                    )
                    total_updated += count
                    logger.info(f"  {name}: {count} 条")

            logger.info(f"指数30分钟更新完成，共 {total_updated} 条")

        except Exception as e:
            logger.exception("指数30分钟更新失败")
            raise

        return total_updated
