"""
IndustryDailyRepository - 行业日线数据访问层

封装 IndustryDaily 模型的数据库操作。
"""

from typing import List, Optional

from sqlalchemy import and_, desc, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from src.models import IndustryDaily
from src.repositories.base_repository import BaseRepository
from src.utils.logging import get_logger

logger = get_logger(__name__)


class IndustryDailyRepository(BaseRepository[IndustryDaily]):
    """行业日线数据Repository"""

    def __init__(self, session: Session):
        """初始化IndustryDailyRepository"""
        super().__init__(session, IndustryDaily)

    def find_by_code_and_date(
        self, ts_code: str, trade_date: str
    ) -> Optional[IndustryDaily]:
        """
        查询行业日线数据

        Args:
            ts_code: 行业代码
            trade_date: 交易日期（YYYYMMDD）

        Returns:
            行业日线数据或None
        """
        stmt = select(IndustryDaily).filter(
            and_(
                IndustryDaily.ts_code == ts_code,
                IndustryDaily.trade_date == trade_date,
            )
        )
        result = self.session.execute(stmt)
        return result.scalar_one_or_none()

    def find_by_code(
        self, ts_code: str, limit: int = 100
    ) -> List[IndustryDaily]:
        """
        查询行业的历史日线数据

        Args:
            ts_code: 行业代码
            limit: 返回数量限制

        Returns:
            行业日线数据列表（按日期倒序）
        """
        stmt = (
            select(IndustryDaily)
            .filter(IndustryDaily.ts_code == ts_code)
            .order_by(desc(IndustryDaily.trade_date))
            .limit(limit)
        )
        result = self.session.execute(stmt)
        return list(result.scalars().all())

    def find_by_date(
        self, trade_date: str
    ) -> List[IndustryDaily]:
        """
        查询指定日期的所有行业数据

        Args:
            trade_date: 交易日期（YYYYMMDD）

        Returns:
            行业日线数据列表
        """
        stmt = select(IndustryDaily).filter(
            IndustryDaily.trade_date == trade_date
        )
        result = self.session.execute(stmt)
        return list(result.scalars().all())

    def upsert_batch(
        self, industry_dailies: List[IndustryDaily]
    ) -> int:
        """
        批量插入或更新行业日线数据

        Args:
            industry_dailies: 行业日线数据列表

        Returns:
            影响的行数
        """
        if not industry_dailies:
            return 0

        industry_dicts = [
            {
                "ts_code": ind.ts_code,
                "trade_date": ind.trade_date,
                "industry": ind.industry,
                "close": ind.close,
                "pct_change": ind.pct_change,
                "company_num": ind.company_num,
                "up_count": getattr(ind, "up_count", None),
                "down_count": getattr(ind, "down_count", None),
                "lead_stock": getattr(ind, "lead_stock", None),
                "lead_stock_code": getattr(ind, "lead_stock_code", None),
                "pct_change_stock": getattr(ind, "pct_change_stock", None),
                "close_price": getattr(ind, "close_price", None),
                "net_buy_amount": getattr(ind, "net_buy_amount", None),
                "net_sell_amount": getattr(ind, "net_sell_amount", None),
                "net_amount": getattr(ind, "net_amount", None),
                "industry_pe": getattr(ind, "industry_pe", None),
                "pe_median": getattr(ind, "pe_median", None),
                "total_mv": getattr(ind, "total_mv", None),
            }
            for ind in industry_dailies
        ]

        stmt = sqlite_insert(IndustryDaily).values(industry_dicts)
        stmt = stmt.on_conflict_do_update(
            index_elements=["ts_code", "trade_date"],
            set_={
                "industry": stmt.excluded.industry,
                "close": stmt.excluded.close,
                "pct_change": stmt.excluded.pct_change,
                "company_num": stmt.excluded.company_num,
                "up_count": stmt.excluded.up_count,
                "down_count": stmt.excluded.down_count,
                "lead_stock": stmt.excluded.lead_stock,
                "lead_stock_code": stmt.excluded.lead_stock_code,
                "pct_change_stock": stmt.excluded.pct_change_stock,
                "close_price": stmt.excluded.close_price,
                "net_buy_amount": stmt.excluded.net_buy_amount,
                "net_sell_amount": stmt.excluded.net_sell_amount,
                "net_amount": stmt.excluded.net_amount,
                "industry_pe": stmt.excluded.industry_pe,
                "pe_median": stmt.excluded.pe_median,
                "total_mv": stmt.excluded.total_mv,
            },
        )

        result = self.session.execute(stmt)
        self.session.flush()

        logger.info(f"Upserted {len(industry_dailies)} industry daily records")
        return result.rowcount

    def get_all_codes(self) -> List[str]:
        """
        获取所有行业代码

        Returns:
            行业代码列表
        """
        stmt = select(IndustryDaily.ts_code).distinct()
        result = self.session.execute(stmt)
        return list(result.scalars().all())
