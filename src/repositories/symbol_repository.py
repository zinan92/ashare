"""
SymbolRepository - 标的元数据访问层

封装标的（股票、指数、概念）元数据的数据库操作。
"""

from typing import List, Optional

from sqlalchemy import and_, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from src.models import SymbolMetadata
from src.repositories.base_repository import BaseRepository
from src.utils.logging import get_logger

logger = get_logger(__name__)


class SymbolRepository(BaseRepository[SymbolMetadata]):
    """标的元数据Repository"""

    def __init__(self, session: Session):
        """初始化SymbolRepository"""
        super().__init__(session, SymbolMetadata)

    def find_by_ticker(self, ticker: str) -> Optional[SymbolMetadata]:
        """
        根据ticker代码查询标的

        Args:
            ticker: 标的代码

        Returns:
            标的元数据或None
        """
        return self.find_by_id(ticker)

    def find_by_tickers(self, tickers: List[str]) -> List[SymbolMetadata]:
        """
        批量查询标的

        Args:
            tickers: 标的代码列表

        Returns:
            标的元数据列表
        """
        stmt = select(SymbolMetadata).filter(SymbolMetadata.ticker.in_(tickers))
        result = self.session.execute(stmt)
        return list(result.scalars().all())

    def find_by_name(self, name: str) -> Optional[SymbolMetadata]:
        """
        根据名称查询标的（精确匹配）

        Args:
            name: 标的名称

        Returns:
            标的元数据或None
        """
        stmt = select(SymbolMetadata).filter(SymbolMetadata.name == name)
        result = self.session.execute(stmt)
        return result.scalar_one_or_none()

    def search_by_name(self, keyword: str, limit: int = 20) -> List[SymbolMetadata]:
        """
        根据名称关键词搜索标的（模糊匹配）

        Args:
            keyword: 搜索关键词
            limit: 返回数量限制

        Returns:
            标的元数据列表
        """
        stmt = (
            select(SymbolMetadata)
            .filter(SymbolMetadata.name.like(f"%{keyword}%"))
            .limit(limit)
        )
        result = self.session.execute(stmt)
        return list(result.scalars().all())

    def find_by_industry(
        self, industry_lv1: Optional[str] = None, industry_lv2: Optional[str] = None
    ) -> List[SymbolMetadata]:
        """
        根据行业查询标的

        Args:
            industry_lv1: 一级行业
            industry_lv2: 二级行业

        Returns:
            标的元数据列表
        """
        conditions = []
        if industry_lv1:
            conditions.append(SymbolMetadata.industry_lv1 == industry_lv1)
        if industry_lv2:
            conditions.append(SymbolMetadata.industry_lv2 == industry_lv2)

        stmt = select(SymbolMetadata).filter(and_(*conditions))
        result = self.session.execute(stmt)
        return list(result.scalars().all())

    def find_by_concept(self, concept: str) -> List[SymbolMetadata]:
        """
        根据概念查询标的

        Args:
            concept: 概念名称

        Returns:
            标的元数据列表
        """
        # JSON字段查询（SQLite需要使用json_each）
        stmt = select(SymbolMetadata).filter(
            SymbolMetadata.concepts.contains(concept)
        )
        result = self.session.execute(stmt)
        return list(result.scalars().all())

    def find_by_market_value_range(
        self, min_mv: Optional[float] = None, max_mv: Optional[float] = None
    ) -> List[SymbolMetadata]:
        """
        根据市值范围查询标的

        Args:
            min_mv: 最小市值（万元）
            max_mv: 最大市值（万元）

        Returns:
            标的元数据列表
        """
        conditions = []
        if min_mv is not None:
            conditions.append(SymbolMetadata.total_mv >= min_mv)
        if max_mv is not None:
            conditions.append(SymbolMetadata.total_mv <= max_mv)

        stmt = select(SymbolMetadata).filter(and_(*conditions))
        result = self.session.execute(stmt)
        return list(result.scalars().all())

    def upsert(self, symbol: SymbolMetadata) -> SymbolMetadata:
        """
        插入或更新标的元数据

        Args:
            symbol: 标的元数据

        Returns:
            保存后的标的元数据
        """
        stmt = sqlite_insert(SymbolMetadata).values(
            ticker=symbol.ticker,
            name=symbol.name,
            total_mv=symbol.total_mv,
            circ_mv=symbol.circ_mv,
            pe_ttm=symbol.pe_ttm,
            pb=symbol.pb,
            list_date=symbol.list_date,
            introduction=symbol.introduction,
            main_business=symbol.main_business,
            business_scope=symbol.business_scope,
            chairman=symbol.chairman,
            manager=symbol.manager,
            reg_capital=symbol.reg_capital,
            setup_date=symbol.setup_date,
            province=symbol.province,
            city=symbol.city,
            employees=symbol.employees,
            website=symbol.website,
            industry_lv1=symbol.industry_lv1,
            industry_lv2=symbol.industry_lv2,
            industry_lv3=symbol.industry_lv3,
            super_category=symbol.super_category,
            concepts=symbol.concepts,
            last_sync=symbol.last_sync,
        )

        stmt = stmt.on_conflict_do_update(
            index_elements=["ticker"],
            set_={
                "name": stmt.excluded.name,
                "total_mv": stmt.excluded.total_mv,
                "circ_mv": stmt.excluded.circ_mv,
                "pe_ttm": stmt.excluded.pe_ttm,
                "pb": stmt.excluded.pb,
                "list_date": stmt.excluded.list_date,
                "introduction": stmt.excluded.introduction,
                "main_business": stmt.excluded.main_business,
                "business_scope": stmt.excluded.business_scope,
                "chairman": stmt.excluded.chairman,
                "manager": stmt.excluded.manager,
                "reg_capital": stmt.excluded.reg_capital,
                "setup_date": stmt.excluded.setup_date,
                "province": stmt.excluded.province,
                "city": stmt.excluded.city,
                "employees": stmt.excluded.employees,
                "website": stmt.excluded.website,
                "industry_lv1": stmt.excluded.industry_lv1,
                "industry_lv2": stmt.excluded.industry_lv2,
                "industry_lv3": stmt.excluded.industry_lv3,
                "super_category": stmt.excluded.super_category,
                "concepts": stmt.excluded.concepts,
                "last_sync": stmt.excluded.last_sync,
            },
        )

        self.session.execute(stmt)
        self.session.flush()

        return self.find_by_ticker(symbol.ticker)

    def upsert_batch(self, symbols: List[SymbolMetadata]) -> int:
        """
        批量插入或更新标的元数据

        Args:
            symbols: 标的元数据列表

        Returns:
            影响的行数
        """
        if not symbols:
            return 0

        # 转换为字典列表
        symbol_dicts = [
            {
                "ticker": s.ticker,
                "name": s.name,
                "total_mv": s.total_mv,
                "circ_mv": s.circ_mv,
                "pe_ttm": s.pe_ttm,
                "pb": s.pb,
                "list_date": s.list_date,
                "introduction": s.introduction,
                "main_business": s.main_business,
                "business_scope": s.business_scope,
                "chairman": s.chairman,
                "manager": s.manager,
                "reg_capital": s.reg_capital,
                "setup_date": s.setup_date,
                "province": s.province,
                "city": s.city,
                "employees": s.employees,
                "website": s.website,
                "industry_lv1": s.industry_lv1,
                "industry_lv2": s.industry_lv2,
                "industry_lv3": s.industry_lv3,
                "super_category": s.super_category,
                "concepts": s.concepts,
                "last_sync": s.last_sync,
            }
            for s in symbols
        ]

        stmt = sqlite_insert(SymbolMetadata).values(symbol_dicts)
        stmt = stmt.on_conflict_do_update(
            index_elements=["ticker"],
            set_={
                "name": stmt.excluded.name,
                "total_mv": stmt.excluded.total_mv,
                "circ_mv": stmt.excluded.circ_mv,
                "pe_ttm": stmt.excluded.pe_ttm,
                "pb": stmt.excluded.pb,
                "list_date": stmt.excluded.list_date,
                "introduction": stmt.excluded.introduction,
                "main_business": stmt.excluded.main_business,
                "business_scope": stmt.excluded.business_scope,
                "chairman": stmt.excluded.chairman,
                "manager": stmt.excluded.manager,
                "reg_capital": stmt.excluded.reg_capital,
                "setup_date": stmt.excluded.setup_date,
                "province": stmt.excluded.province,
                "city": stmt.excluded.city,
                "employees": stmt.excluded.employees,
                "website": stmt.excluded.website,
                "industry_lv1": stmt.excluded.industry_lv1,
                "industry_lv2": stmt.excluded.industry_lv2,
                "industry_lv3": stmt.excluded.industry_lv3,
                "super_category": stmt.excluded.super_category,
                "concepts": stmt.excluded.concepts,
                "last_sync": stmt.excluded.last_sync,
            },
        )

        result = self.session.execute(stmt)
        self.session.flush()

        logger.info(f"Upserted {len(symbols)} symbols")
        return result.rowcount

    def get_all_tickers(self) -> List[str]:
        """
        获取所有标的代码

        Returns:
            标的代码列表
        """
        stmt = select(SymbolMetadata.ticker)
        result = self.session.execute(stmt)
        return list(result.scalars().all())

    def get_statistics(self) -> dict:
        """
        获取标的统计信息

        Returns:
            统计信息字典
        """
        total_count = self.count()

        return {
            "total": total_count,
            # 可以添加更多统计信息
        }

    def find_all_ordered_by_market_value(self) -> List[SymbolMetadata]:
        """
        获取所有标的，按市值倒序、ticker正序排列

        Returns:
            标的元数据列表
        """
        stmt = select(SymbolMetadata).order_by(
            SymbolMetadata.total_mv.desc(),
            SymbolMetadata.ticker.asc(),
        )
        result = self.session.execute(stmt)
        return list(result.scalars().all())

    def get_last_sync_time(self) -> Optional[any]:
        """
        获取最后同步时间

        Returns:
            最后同步时间或None
        """
        from sqlalchemy import func

        stmt = select(func.max(SymbolMetadata.last_sync))
        result = self.session.execute(stmt)
        return result.scalar()

    def bulk_upsert_from_dataframe(
        self, dataframe, super_category_map: dict[str, str]
    ) -> None:
        """
        从 DataFrame 批量插入或更新标的元数据
        优化版：使用分块查询和批量操作

        Args:
            dataframe: pandas DataFrame 包含标的数据
            super_category_map: 行业到超级行业组的映射
        """
        from datetime import datetime, timezone

        if dataframe is None or dataframe.empty:
            logger.warning("Metadata dataframe empty; skipping persist.")
            return

        logger.info(f"Persist metadata | rows={len(dataframe)}")

        # OPTIMIZATION: Pre-load all existing tickers (chunk to avoid SQLite 999-param limit)
        tickers_in_df = dataframe["ticker"].tolist()
        existing_records = {}

        CHUNK_SIZE = 500
        for i in range(0, len(tickers_in_df), CHUNK_SIZE):
            ticker_chunk = tickers_in_df[i : i + CHUNK_SIZE]
            chunk_stmt = select(SymbolMetadata).where(
                SymbolMetadata.ticker.in_(ticker_chunk)
            )
            for rec in self.session.scalars(chunk_stmt).all():
                existing_records[rec.ticker] = rec

        logger.debug(f"Found {len(existing_records)} existing records")

        # Split into inserts and updates
        insert_rows = []
        update_rows = []

        for row in dataframe.itertuples(index=False):
            if row.ticker in existing_records:
                update_rows.append(row)
            else:
                insert_rows.append(row)

        # OPTIMIZATION: Bulk insert new records
        if insert_rows:
            insert_records = []
            for row in insert_rows:
                insert_records.append(
                    {
                        "ticker": row.ticker,
                        "name": row.name,
                        "total_mv": getattr(row, "total_mv", None),
                        "circ_mv": getattr(row, "circ_mv", None),
                        "pe_ttm": getattr(row, "pe_ttm", None),
                        "pb": getattr(row, "pb", None),
                        "list_date": getattr(row, "list_date", None),
                        "industry_lv1": getattr(row, "industry_lv1", None),
                        "industry_lv2": getattr(row, "industry_lv2", None),
                        "industry_lv3": getattr(row, "industry_lv3", None),
                        "super_category": super_category_map.get(
                            getattr(row, "industry_lv1", None)
                        ),
                        "concepts": getattr(row, "concepts", []),
                        "last_sync": getattr(
                            row, "last_sync", datetime.now(timezone.utc)
                        ),
                    }
                )
            self.session.bulk_insert_mappings(SymbolMetadata, insert_records)
            logger.debug(f"Bulk inserted {len(insert_records)} new records")

        # Update existing records
        if update_rows:
            for row in update_rows:
                instance = existing_records[row.ticker]
                instance.name = row.name
                instance.total_mv = getattr(row, "total_mv", None)
                instance.circ_mv = getattr(row, "circ_mv", None)
                instance.pe_ttm = getattr(row, "pe_ttm", None)
                instance.pb = getattr(row, "pb", None)
                instance.list_date = getattr(row, "list_date", None)
                # 注意: 不再覆盖 industry_lv1/lv2/lv3
                # 这些字段由 update_industry_daily.py 从同花顺成分股关系写入
                instance.concepts = getattr(row, "concepts", [])
                instance.last_sync = getattr(
                    row, "last_sync", datetime.now(timezone.utc)
                )
            logger.debug(f"Updated {len(update_rows)} existing records")
