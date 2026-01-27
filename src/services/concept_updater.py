"""
概念板块K线更新器
从同花顺获取概念板块日线和30分钟数据
"""

import asyncio
import json
import re
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pandas as pd

from src.models import KlineTimeframe, SymbolType
from src.schemas.normalized import NormalizedDate, NormalizedDateTime
from src.services.kline_service import KlineService
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.repositories.kline_repository import KlineRepository
    from src.repositories.symbol_repository import SymbolRepository

logger = get_logger(__name__)

# 同花顺 API 配置
THS_BASE_URL = "http://d.10jqka.com.cn/v4"
THS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "http://q.10jqka.com.cn/",
}


class ConceptUpdater:
    """概念板块K线更新器"""

    def __init__(
        self,
        kline_repo: "KlineRepository",
        symbol_repo: "SymbolRepository",
    ):
        self.kline_repo = kline_repo
        self.symbol_repo = symbol_repo

    def _load_hot_concepts(self) -> list[tuple[str, str]]:
        """加载热门概念列表 (code, name)"""
        data_dir = Path(__file__).parent.parent.parent / "data"

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

    async def update_daily(self) -> int:
        """更新概念日线数据"""
        logger.info("开始更新概念日线数据...")
        total_updated = 0

        concepts = self._load_hot_concepts()
        if not concepts:
            logger.warning("未找到热门概念列表")
            return 0

        try:
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
                        service = KlineService(self.kline_repo, self.symbol_repo)
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

            logger.info(f"概念日线更新完成，共 {total_updated} 条")

        except Exception as e:
            logger.exception("概念日线更新失败")
            raise

        return total_updated

    async def update_30m(self) -> int:
        """更新概念30分钟数据"""
        logger.info("开始更新概念30分钟数据...")
        total_updated = 0

        concepts = self._load_hot_concepts()
        if not concepts:
            logger.warning("未找到热门概念列表")
            return 0

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
                        service = KlineService(self.kline_repo, self.symbol_repo)
                        count = service.save_klines(
                            symbol_type=SymbolType.CONCEPT,
                            symbol_code=code,
                            symbol_name=name,
                            timeframe=KlineTimeframe.MINS_30,
                            klines=klines,
                        )
                        total_updated += count

                await asyncio.sleep(0.5)

            logger.info(f"概念30分钟更新完成，共 {total_updated} 条")

        except Exception as e:
            logger.exception("概念30分钟更新失败")
            raise

        return total_updated
