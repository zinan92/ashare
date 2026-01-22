"""ConceptDailyRepository单元测试

测试覆盖:
1. ConceptDaily CRUD操作
2. 边界情况和错误处理
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models import Base, ConceptDaily
from src.repositories.concept_daily_repository import ConceptDailyRepository


@pytest.fixture(scope="function")
def test_db():
    """创建内存数据库用于测试"""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture
def concept_daily_repo(test_db: Session):
    """创建ConceptDailyRepository实例"""
    return ConceptDailyRepository(test_db)


# ==================== ConceptDaily 测试 ====================


class TestConceptDailyOperations:
    """ConceptDaily CRUD操作测试"""

    def test_upsert_batch(self, concept_daily_repo: ConceptDailyRepository, test_db: Session):
        """测试批量插入概念日线数据"""
        concepts = [
            ConceptDaily(
                code="885728",
                name="人工智能",
                trade_date="20260122",
                close=1530.33,
                pct_change=2.45,
                amount=987654321.0,
                leader_symbol="300750.SZ",
                leader_name="宁德时代",
            ),
            ConceptDaily(
                code="886100",
                name="华为概念",
                trade_date="20260122",
                close=1245.67,
                pct_change=1.23,
                amount=876543210.0,
                leader_symbol="000063.SZ",
                leader_name="中兴通讯",
            ),
        ]

        count = concept_daily_repo.upsert_batch(concepts)
        test_db.commit()

        assert count == 2

    def test_find_by_code_and_date(self, concept_daily_repo: ConceptDailyRepository, test_db: Session):
        """测试查询概念日线数据"""
        # 插入测试数据
        concept = ConceptDaily(
            code="885728",
            name="人工智能",
            trade_date="20260122",
            close=1530.33,
            pct_change=2.45,
            amount=987654321.0,
        )
        test_db.add(concept)
        test_db.commit()

        # 查询
        result = concept_daily_repo.find_by_code_and_date("885728", "20260122")

        assert result is not None
        assert result.code == "885728"
        assert result.name == "人工智能"
        assert result.close == 1530.33

    def test_find_by_code(self, concept_daily_repo: ConceptDailyRepository, test_db: Session):
        """测试按代码查询概念日线"""
        # 插入多天数据
        for i in range(3):
            concept = ConceptDaily(
                code="885728",
                name="人工智能",
                trade_date=f"2026012{i}",
                close=1530.33 + i,
                pct_change=2.45,
                amount=987654321.0,
            )
            test_db.add(concept)
        test_db.commit()

        # 查询最近2天
        results = concept_daily_repo.find_by_code("885728", limit=2)

        assert len(results) == 2
        # 应该按日期倒序
        assert results[0].trade_date > results[1].trade_date

    def test_find_by_date(self, concept_daily_repo: ConceptDailyRepository, test_db: Session):
        """测试按日期查询概念日线"""
        # 插入多个概念的同一天数据
        concepts = [
            ConceptDaily(
                code="885728",
                name="人工智能",
                trade_date="20260122",
                close=1530.33,
                pct_change=2.45,
            ),
            ConceptDaily(
                code="886100",
                name="华为概念",
                trade_date="20260122",
                close=1245.67,
                pct_change=1.23,
            ),
        ]
        test_db.add_all(concepts)
        test_db.commit()

        # 查询
        results = concept_daily_repo.find_by_date("20260122")

        assert len(results) == 2

    def test_get_all_codes(self, concept_daily_repo: ConceptDailyRepository, test_db: Session):
        """测试获取所有概念代码"""
        # 插入数据
        concepts = [
            ConceptDaily(
                code="885728",
                name="人工智能",
                trade_date="20260122",
                close=1530.33,
                pct_change=2.45,
            ),
            ConceptDaily(
                code="886100",
                name="华为概念",
                trade_date="20260122",
                close=1245.67,
                pct_change=1.23,
            ),
        ]
        test_db.add_all(concepts)
        test_db.commit()

        # 查询
        codes = concept_daily_repo.get_all_codes()

        assert len(codes) == 2
        assert "885728" in codes
        assert "886100" in codes


# ==================== 边界情况测试 ====================


class TestConceptDailyEdgeCases:
    """边界情况和错误处理测试"""

    def test_find_by_code_and_date_not_found(self, concept_daily_repo: ConceptDailyRepository):
        """测试查询不存在的概念日线"""
        result = concept_daily_repo.find_by_code_and_date("999999", "20260122")
        assert result is None

    def test_upsert_batch_empty(self, concept_daily_repo: ConceptDailyRepository):
        """测试批量插入空列表"""
        count = concept_daily_repo.upsert_batch([])
        assert count == 0

    def test_get_all_codes_empty(self, concept_daily_repo: ConceptDailyRepository):
        """测试获取空的概念代码列表"""
        codes = concept_daily_repo.get_all_codes()
        assert len(codes) == 0

    def test_find_by_code_empty(self, concept_daily_repo: ConceptDailyRepository):
        """测试查询不存在代码的概念日线"""
        results = concept_daily_repo.find_by_code("999999")
        assert len(results) == 0

    def test_find_by_date_empty(self, concept_daily_repo: ConceptDailyRepository):
        """测试查询不存在日期的概念数据"""
        results = concept_daily_repo.find_by_date("20990101")
        assert len(results) == 0
