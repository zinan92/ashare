"""
数据一致性验证服务
在收盘后验证：实时价格、日线收盘价、30分钟收盘价是否一致

重构说明:
- 使用 KlineRepository 替代 session_scope()
- 强制依赖注入，不再自动创建 Session
- Session 生命周期由调用者控制
"""
import logging
from datetime import datetime, time
from typing import List, Dict, Any, Optional
from sqlalchemy import desc
from sqlalchemy.orm import Session

from src.models import Kline, KlineTimeframe, SymbolType
from src.repositories.kline_repository import KlineRepository
from src.services.kline_updater import KlineUpdater

logger = logging.getLogger(__name__)


class DataConsistencyValidator:
    """
    数据一致性验证器

    重构后支持:
    - 强制依赖注入 KlineRepository
    - Session 生命周期由调用者控制
    """

    def __init__(
        self,
        kline_repo: KlineRepository,
        updater: Optional[KlineUpdater] = None,
        tolerance: float = 0.01
    ):
        """
        初始化验证器

        Args:
            kline_repo: K线数据仓库（必需）
            updater: KlineUpdater实例（可选）
            tolerance: 价格差异容忍度（百分比），默认0.01%
        """
        self.kline_repo = kline_repo
        self.updater = updater
        self.tolerance = tolerance

    @classmethod
    def create_with_session(cls, session: Session, tolerance: float = 0.01) -> "DataConsistencyValidator":
        """
        工厂方法：使用session创建DataConsistencyValidator

        Args:
            session: SQLAlchemy session
            tolerance: 价格差异容忍度（百分比），默认0.01%
        """
        kline_repo = KlineRepository(session)
        updater = KlineUpdater.create_with_session(session)
        return cls(kline_repo=kline_repo, updater=updater, tolerance=tolerance)

    async def validate_all(self) -> Dict[str, Any]:
        """
        验证所有数据的一致性

        Returns:
            验证结果字典，包含：
            - summary: 总结信息
            - indexes: 指数验证结果
            - concepts: 概念验证结果
            - inconsistencies: 不一致的项目列表
        """
        logger.info("=" * 60)
        logger.info("开始执行数据一致性验证")
        logger.info("=" * 60)

        results = {
            "timestamp": datetime.now().isoformat(),
            "summary": {},
            "indexes": [],
            "concepts": [],
            "inconsistencies": []
        }

        # 验证指数
        index_results = await self._validate_indexes()
        results["indexes"] = index_results["items"]
        results["inconsistencies"].extend(index_results["inconsistencies"])

        # 验证概念
        concept_results = await self._validate_concepts()
        results["concepts"] = concept_results["items"]
        results["inconsistencies"].extend(concept_results["inconsistencies"])

        # 生成总结
        total_items = len(results["indexes"]) + len(results["concepts"])
        total_inconsistencies = len(results["inconsistencies"])

        results["summary"] = {
            "total_validated": total_items,
            "total_inconsistencies": total_inconsistencies,
            "consistency_rate": (
                (total_items - total_inconsistencies) / total_items * 100
                if total_items > 0 else 0
            ),
            "is_healthy": total_inconsistencies == 0
        }

        # 记录结果
        self._log_results(results)

        return results

    async def _validate_indexes(self) -> Dict[str, Any]:
        """验证指数数据一致性"""
        logger.info("开始验证指数数据...")

        items = []
        inconsistencies = []

        # 使用 repository 获取所有指数的去重列表
        from sqlalchemy import select

        stmt = (
            select(Kline.symbol_code, Kline.symbol_name)
            .filter(
                Kline.symbol_type == SymbolType.INDEX,
                Kline.timeframe == KlineTimeframe.DAY
            )
            .distinct()
        )
        result_set = self.kline_repo.session.execute(stmt)
        indexes = result_set.all()

        for symbol_code, symbol_name in indexes:
            result = await self._validate_symbol(
                symbol_code, symbol_name, SymbolType.INDEX
            )
            items.append(result)
            if not result["is_consistent"]:
                inconsistencies.append(result)

        logger.info(f"指数验证完成：{len(items)}个，{len(inconsistencies)}个不一致")
        return {"items": items, "inconsistencies": inconsistencies}

    async def _validate_concepts(self) -> Dict[str, Any]:
        """验证概念数据一致性"""
        logger.info("开始验证概念数据...")

        items = []
        inconsistencies = []

        # 使用 repository 获取所有概念的去重列表
        from sqlalchemy import select

        stmt = (
            select(Kline.symbol_code, Kline.symbol_name)
            .filter(
                Kline.symbol_type == SymbolType.CONCEPT,
                Kline.timeframe == KlineTimeframe.DAY
            )
            .distinct()
        )
        result_set = self.kline_repo.session.execute(stmt)
        concepts = result_set.all()

        for symbol_code, symbol_name in concepts:
            result = await self._validate_symbol(
                symbol_code, symbol_name, SymbolType.CONCEPT
            )
            items.append(result)
            if not result["is_consistent"]:
                inconsistencies.append(result)

        logger.info(f"概念验证完成：{len(items)}个，{len(inconsistencies)}个不一致")
        return {"items": items, "inconsistencies": inconsistencies}

    async def _validate_symbol(
        self, symbol_code: str, symbol_name: str, symbol_type: SymbolType
    ) -> Dict[str, Any]:
        """
        验证单个标的的数据一致性

        检查：
        1. 日线最后收盘价
        2. 30分钟最后收盘价
        3. 实时API返回的价格（如果可用）

        Returns:
            验证结果字典
        """
        result = {
            "symbol_code": symbol_code,
            "symbol_name": symbol_name,
            "symbol_type": symbol_type.value,
            "daily_close": None,
            "mins30_close": None,
            "realtime_price": None,
            "is_consistent": True,
            "inconsistency_details": []
        }

        try:
            # 1. 获取日线最后收盘价
            from sqlalchemy import select

            stmt = (
                select(Kline)
                .filter(
                    Kline.symbol_code == symbol_code,
                    Kline.symbol_type == symbol_type,
                    Kline.timeframe == KlineTimeframe.DAY
                )
                .order_by(desc(Kline.trade_time))
                .limit(1)
            )
            daily_kline = self.kline_repo.session.execute(stmt).scalar_one_or_none()

            if daily_kline:
                result["daily_close"] = float(daily_kline.close)
                # trade_time可能是datetime或str
                trade_time = daily_kline.trade_time
                if isinstance(trade_time, str):
                    result["daily_date"] = trade_time[:10]
                else:
                    result["daily_date"] = trade_time.strftime("%Y-%m-%d")
            else:
                result["inconsistency_details"].append("缺少日线数据")
                result["is_consistent"] = False
                return result

            # 2. 获取30分钟最后收盘价
            stmt = (
                select(Kline)
                .filter(
                    Kline.symbol_code == symbol_code,
                    Kline.symbol_type == symbol_type,
                    Kline.timeframe == KlineTimeframe.MINS_30
                )
                .order_by(desc(Kline.trade_time))
                .limit(1)
            )
            mins30_kline = self.kline_repo.session.execute(stmt).scalar_one_or_none()

            if mins30_kline:
                result["mins30_close"] = float(mins30_kline.close)
                # trade_time可能是datetime或str
                trade_time = mins30_kline.trade_time
                if isinstance(trade_time, str):
                    result["mins30_datetime"] = trade_time[:16]
                else:
                    result["mins30_datetime"] = trade_time.strftime("%Y-%m-%d %H:%M")
            else:
                result["inconsistency_details"].append("缺少30分钟线数据")
                result["is_consistent"] = False

            # 3. 获取实时价格（仅用于对比，不作为必需）
            # 注意：收盘后可能无法获取实时价格，这不算不一致
            try:
                realtime_price = await self._fetch_realtime_price(
                    symbol_code, symbol_type
                )
                if realtime_price:
                    result["realtime_price"] = realtime_price
            except Exception as e:
                logger.debug(f"无法获取 {symbol_code} 实时价格: {e}")

            # 4. 比较价格一致性
            if result["daily_close"] and result["mins30_close"]:
                diff_pct = abs(
                    result["daily_close"] - result["mins30_close"]
                ) / result["daily_close"] * 100

                if diff_pct > self.tolerance:
                    result["is_consistent"] = False
                    result["inconsistency_details"].append(
                        f"日线收盘价({result['daily_close']:.2f}) "
                        f"与30分钟收盘价({result['mins30_close']:.2f}) "
                        f"差异 {diff_pct:.2f}%"
                    )

            # 5. 如果有实时价格，也进行对比（仅警告）
            if result["realtime_price"] and result["daily_close"]:
                diff_pct = abs(
                    result["daily_close"] - result["realtime_price"]
                ) / result["daily_close"] * 100

                if diff_pct > self.tolerance:
                    # 实时价格差异不算严重错误，仅记录
                    result["inconsistency_details"].append(
                        f"[警告] 实时价格({result['realtime_price']:.2f}) "
                        f"与日线收盘价差异 {diff_pct:.2f}%"
                    )

        except Exception as e:
            logger.exception(f"验证 {symbol_code} 时出错: {e}")
            result["is_consistent"] = False
            result["inconsistency_details"].append(f"验证出错: {str(e)}")

        return result

    async def _fetch_realtime_price(
        self, symbol_code: str, symbol_type: SymbolType
    ) -> Optional[float]:
        """
        获取实时价格（用于验证）

        注意：收盘后可能无法获取，这是正常的
        """
        import httpx
        import re
        import json

        if symbol_type == SymbolType.CONCEPT:
            # 概念板块
            url = f"http://d.10jqka.com.cn/v4/time/bk_{symbol_code}/last.js"
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.get(url, headers={
                        "User-Agent": "Mozilla/5.0",
                        "Referer": "http://q.10jqka.com.cn/"
                    })
                    text = resp.text
                    match = re.search(r'\((\{.*\})\)', text, re.DOTALL)
                    if match:
                        data = json.loads(match.group(1))
                        inner_key = f"bk_{symbol_code}"
                        if inner_key in data:
                            time_data = data[inner_key].get('data', '')
                            if time_data:
                                items = [item for item in time_data.split(';') if item.strip()]
                                if items:
                                    last_item = items[-1].split(',')
                                    if len(last_item) >= 2 and last_item[1]:
                                        return float(last_item[1])
            except Exception:
                pass

        elif symbol_type == SymbolType.INDEX:
            # 指数（这里可以添加指数实时API，暂时跳过）
            pass

        return None

    def _log_results(self, results: Dict[str, Any]) -> None:
        """记录验证结果"""
        summary = results["summary"]

        logger.info("=" * 60)
        logger.info("数据一致性验证完成")
        logger.info("=" * 60)
        logger.info(f"验证总数: {summary['total_validated']}")
        logger.info(f"不一致数: {summary['total_inconsistencies']}")
        logger.info(f"一致性: {summary['consistency_rate']:.2f}%")
        logger.info(f"健康状态: {'✅ 正常' if summary['is_healthy'] else '❌ 异常'}")

        if results["inconsistencies"]:
            logger.warning(f"发现 {len(results['inconsistencies'])} 个不一致项:")
            for item in results["inconsistencies"]:
                logger.warning(
                    f"  - {item['symbol_name']} ({item['symbol_code']}): "
                    f"{', '.join(item['inconsistency_details'])}"
                )

        logger.info("=" * 60)

    async def validate_and_report(self) -> bool:
        """
        执行验证并返回是否健康

        Returns:
            True 如果所有数据一致，False 如果发现不一致
        """
        results = await self.validate_all()
        return results["summary"]["is_healthy"]
