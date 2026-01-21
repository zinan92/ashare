"""
KlineRepository - K线数据访问层

封装所有K线相关的数据库操作。
"""

from datetime import datetime
from typing import List, Optional

from sqlalchemy import and_, delete, desc, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from src.models import Kline, KlineTimeframe, SymbolType
from src.repositories.base_repository import BaseRepository
from src.utils.logging import get_logger

logger = get_logger(__name__)


class KlineRepository(BaseRepository[Kline]):
    """K线数据Repository"""

    def __init__(self, session: Session):
        """初始化KlineRepository"""
        super().__init__(session, Kline)

    def find_by_symbol(
        self,
        symbol_code: str,
        symbol_type: SymbolType,
        timeframe: KlineTimeframe,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[Kline]:
        """
        按标的查询K线数据

        Args:
            symbol_code: 标的代码
            symbol_type: 标的类型（stock/index/concept）
            timeframe: 时间周期
            limit: 限制返回数量
            offset: 偏移量

        Returns:
            K线数据列表（按时间倒序）
        """
        stmt = (
            select(Kline)
            .filter(
                Kline.symbol_code == symbol_code,
                Kline.symbol_type == symbol_type,
                Kline.timeframe == timeframe,
            )
            .order_by(desc(Kline.trade_time))
            .offset(offset)
        )

        if limit:
            stmt = stmt.limit(limit)

        result = self.session.execute(stmt)
        return list(result.scalars().all())

    def find_by_symbol_and_date_range(
        self,
        symbol_code: str,
        symbol_type: SymbolType,
        timeframe: KlineTimeframe,
        start_date: datetime,
        end_date: datetime,
    ) -> List[Kline]:
        """
        按标的和日期范围查询K线数据

        Args:
            symbol_code: 标的代码
            symbol_type: 标的类型
            timeframe: 时间周期
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            K线数据列表（按时间正序）
        """
        stmt = (
            select(Kline)
            .filter(
                Kline.symbol_code == symbol_code,
                Kline.symbol_type == symbol_type,
                Kline.timeframe == timeframe,
                Kline.trade_time >= start_date,
                Kline.trade_time <= end_date,
            )
            .order_by(Kline.trade_time)
        )

        result = self.session.execute(stmt)
        return list(result.scalars().all())

    def find_latest_by_symbol(
        self,
        symbol_code: str,
        symbol_type: SymbolType,
        timeframe: KlineTimeframe,
    ) -> Optional[Kline]:
        """
        查询标的的最新K线数据

        Args:
            symbol_code: 标的代码
            symbol_type: 标的类型
            timeframe: 时间周期

        Returns:
            最新的K线数据或None
        """
        stmt = (
            select(Kline)
            .filter(
                Kline.symbol_code == symbol_code,
                Kline.symbol_type == symbol_type,
                Kline.timeframe == timeframe,
            )
            .order_by(desc(Kline.trade_time))
            .limit(1)
        )

        result = self.session.execute(stmt)
        return result.scalar_one_or_none()

    def find_by_symbols(
        self,
        symbol_codes: List[str],
        symbol_type: SymbolType,
        timeframe: KlineTimeframe,
        limit_per_symbol: int = 100,
    ) -> List[Kline]:
        """
        批量查询多个标的的K线数据

        Args:
            symbol_codes: 标的代码列表
            symbol_type: 标的类型
            timeframe: 时间周期
            limit_per_symbol: 每个标的的数量限制

        Returns:
            K线数据列表
        """
        stmt = (
            select(Kline)
            .filter(
                Kline.symbol_code.in_(symbol_codes),
                Kline.symbol_type == symbol_type,
                Kline.timeframe == timeframe,
            )
            .order_by(Kline.symbol_code, desc(Kline.trade_time))
        )

        result = self.session.execute(stmt)
        all_klines = list(result.scalars().all())

        # 对每个标的限制数量
        klines_by_symbol = {}
        for kline in all_klines:
            if kline.symbol_code not in klines_by_symbol:
                klines_by_symbol[kline.symbol_code] = []
            if len(klines_by_symbol[kline.symbol_code]) < limit_per_symbol:
                klines_by_symbol[kline.symbol_code].append(kline)

        # 展平结果
        return [kline for klines in klines_by_symbol.values() for kline in klines]

    def upsert_batch(self, klines: List[Kline]) -> int:
        """
        批量插入或更新K线数据（使用SQLite的INSERT OR REPLACE）

        Args:
            klines: K线数据列表

        Returns:
            影响的行数
        """
        if not klines:
            return 0

        # 转换为字典列表
        kline_dicts = [
            {
                "symbol_code": k.symbol_code,
                "symbol_type": k.symbol_type,
                "symbol_name": k.symbol_name,
                "timeframe": k.timeframe,
                "trade_time": k.trade_time,
                "open": k.open,
                "high": k.high,
                "low": k.low,
                "close": k.close,
                "volume": k.volume,
                "amount": k.amount,
                "updated_at": k.updated_at,
            }
            for k in klines
        ]

        # SQLite的upsert语法
        stmt = sqlite_insert(Kline).values(kline_dicts)
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol_code", "symbol_type", "timeframe", "trade_time"],
            set_={
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "volume": stmt.excluded.volume,
                "amount": stmt.excluded.amount,
                "updated_at": stmt.excluded.updated_at,
            },
        )

        result = self.session.execute(stmt)
        self.session.flush()

        logger.info(f"Upserted {len(klines)} klines")
        return result.rowcount

    def delete_by_symbol(
        self,
        symbol_code: str,
        symbol_type: SymbolType,
        timeframe: KlineTimeframe,
    ) -> int:
        """
        删除指定标的的所有K线数据

        Args:
            symbol_code: 标的代码
            symbol_type: 标的类型
            timeframe: 时间周期

        Returns:
            删除的记录数
        """
        stmt = delete(Kline).where(
            and_(
                Kline.symbol_code == symbol_code,
                Kline.symbol_type == symbol_type,
                Kline.timeframe == timeframe,
            )
        )

        result = self.session.execute(stmt)
        self.session.flush()

        logger.info(
            f"Deleted {result.rowcount} klines for {symbol_code} ({symbol_type}, {timeframe})"
        )
        return result.rowcount

    def delete_by_date_range(
        self,
        symbol_code: str,
        symbol_type: SymbolType,
        timeframe: KlineTimeframe,
        start_date: datetime,
        end_date: datetime,
    ) -> int:
        """
        删除指定日期范围内的K线数据

        Args:
            symbol_code: 标的代码
            symbol_type: 标的类型
            timeframe: 时间周期
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            删除的记录数
        """
        stmt = delete(Kline).where(
            and_(
                Kline.symbol_code == symbol_code,
                Kline.symbol_type == symbol_type,
                Kline.timeframe == timeframe,
                Kline.trade_time >= start_date,
                Kline.trade_time <= end_date,
            )
        )

        result = self.session.execute(stmt)
        self.session.flush()

        return result.rowcount

    def count_by_symbol(
        self,
        symbol_code: str,
        symbol_type: SymbolType,
        timeframe: KlineTimeframe,
    ) -> int:
        """
        统计指定标的的K线数量

        Args:
            symbol_code: 标的代码
            symbol_type: 标的类型
            timeframe: 时间周期

        Returns:
            K线数量
        """
        stmt = select(func.count()).where(
            and_(
                Kline.symbol_code == symbol_code,
                Kline.symbol_type == symbol_type,
                Kline.timeframe == timeframe,
            )
        )

        result = self.session.execute(stmt)
        return result.scalar_one()

    def find_symbols_with_data(
        self,
        symbol_type: SymbolType,
        timeframe: KlineTimeframe,
    ) -> List[str]:
        """
        查询有数据的标的代码列表

        Args:
            symbol_type: 标的类型
            timeframe: 时间周期

        Returns:
            标的代码列表
        """
        stmt = (
            select(Kline.symbol_code)
            .distinct()
            .filter(
                Kline.symbol_type == symbol_type,
                Kline.timeframe == timeframe,
            )
        )

        result = self.session.execute(stmt)
        return list(result.scalars().all())
