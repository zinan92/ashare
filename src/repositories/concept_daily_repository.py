"""
ConceptDailyRepository - 概念日线数据访问层

封装 ConceptDaily 模型的数据库操作。
"""

from datetime import datetime
from typing import List, Optional

from sqlalchemy import and_, desc, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from src.models import ConceptDaily
from src.repositories.base_repository import BaseRepository
from src.utils.logging import get_logger

logger = get_logger(__name__)


class ConceptDailyRepository(BaseRepository[ConceptDaily]):
    """概念日线数据Repository"""

    def __init__(self, session: Session):
        """初始化ConceptDailyRepository"""
        super().__init__(session, ConceptDaily)

    def find_by_code_and_date(
        self, code: str, trade_date: str
    ) -> Optional[ConceptDaily]:
        """
        查询概念日线数据

        Args:
            code: 概念代码
            trade_date: 交易日期（YYYYMMDD）

        Returns:
            概念日线数据或None
        """
        stmt = select(ConceptDaily).filter(
            and_(
                ConceptDaily.code == code,
                ConceptDaily.trade_date == trade_date,
            )
        )
        result = self.session.execute(stmt)
        return result.scalar_one_or_none()

    def find_by_code(
        self, code: str, limit: int = 100
    ) -> List[ConceptDaily]:
        """
        查询概念的历史日线数据

        Args:
            code: 概念代码
            limit: 返回数量限制

        Returns:
            概念日线数据列表（按日期倒序）
        """
        stmt = (
            select(ConceptDaily)
            .filter(ConceptDaily.code == code)
            .order_by(desc(ConceptDaily.trade_date))
            .limit(limit)
        )
        result = self.session.execute(stmt)
        return list(result.scalars().all())

    def find_by_date(
        self, trade_date: str
    ) -> List[ConceptDaily]:
        """
        查询指定日期的所有概念数据

        Args:
            trade_date: 交易日期（YYYYMMDD）

        Returns:
            概念日线数据列表
        """
        stmt = select(ConceptDaily).filter(
            ConceptDaily.trade_date == trade_date
        )
        result = self.session.execute(stmt)
        return list(result.scalars().all())

    def upsert_batch(
        self, concept_dailies: List[ConceptDaily]
    ) -> int:
        """
        批量插入或更新概念日线数据

        Args:
            concept_dailies: 概念日线数据列表

        Returns:
            影响的行数
        """
        if not concept_dailies:
            return 0

        concept_dicts = []
        for con in concept_dailies:
            # Ensure timestamps are always set
            created_at = getattr(con, "created_at", None) or datetime.utcnow()
            updated_at = getattr(con, "updated_at", None) or datetime.utcnow()

            concept_dicts.append({
                "code": con.code,
                "trade_date": con.trade_date,
                "name": con.name,
                "close": con.close,
                "pct_change": con.pct_change,
                "volume": getattr(con, "volume", None),
                "amount": getattr(con, "amount", None),
                "leader_symbol": getattr(con, "leader_symbol", None),
                "leader_name": getattr(con, "leader_name", None),
                "leader_pct_change": getattr(con, "leader_pct_change", None),
                "up_count": getattr(con, "up_count", None),
                "down_count": getattr(con, "down_count", None),
                "created_at": created_at,
                "updated_at": updated_at,
            })

        stmt = sqlite_insert(ConceptDaily).values(concept_dicts)
        stmt = stmt.on_conflict_do_update(
            index_elements=["code", "trade_date"],
            set_={
                "name": stmt.excluded.name,
                "close": stmt.excluded.close,
                "pct_change": stmt.excluded.pct_change,
                "volume": stmt.excluded.volume,
                "amount": stmt.excluded.amount,
                "leader_symbol": stmt.excluded.leader_symbol,
                "leader_name": stmt.excluded.leader_name,
                "leader_pct_change": stmt.excluded.leader_pct_change,
                "up_count": stmt.excluded.up_count,
                "down_count": stmt.excluded.down_count,
                "updated_at": stmt.excluded.updated_at,
            },
        )

        result = self.session.execute(stmt)
        self.session.flush()

        logger.info(f"Upserted {len(concept_dailies)} concept daily records")
        return result.rowcount

    def get_all_codes(self) -> List[str]:
        """
        获取所有概念代码

        Returns:
            概念代码列表
        """
        stmt = select(ConceptDaily.code).distinct()
        result = self.session.execute(stmt)
        return list(result.scalars().all())
