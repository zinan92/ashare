"""IndustryDailyRepository单元测试

测试覆盖:
1. IndustryDaily CRUD操作
2. 边界情况和错误处理
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models import Base, IndustryDaily
from src.repositories.industry_daily_repository import IndustryDailyRepository


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
def industry_daily_repo(test_db: Session):
    """创建IndustryDailyRepository实例"""
    return IndustryDailyRepository(test_db)


# ==================== IndustryDaily 测试 ====================


class TestIndustryDailyOperations:
    """IndustryDaily CRUD操作测试"""

    def test_upsert_batch(self, industry_daily_repo: IndustryDailyRepository, test_db: Session):
        """测试批量插入行业日线数据"""
        industries = [
            IndustryDaily(
                ts_code="801010",
                industry="农林牧渔",
                trade_date="20260122",
                close=1234.56,
                pct_change=1.23,
                company_num=50,
            ),
            IndustryDaily(
                ts_code="801020",
                industry="采掘",
                trade_date="20260122",
                close=2345.67,
                pct_change=-0.45,
                company_num=30,
            ),
        ]

        count = industry_daily_repo.upsert_batch(industries)
        test_db.commit()

        assert count == 2

    def test_find_by_code_and_date(self, industry_daily_repo: IndustryDailyRepository, test_db: Session):
        """测试查询行业日线数据"""
        # 插入测试数据
        industry = IndustryDaily(
            ts_code="801010",
            industry="农林牧渔",
            trade_date="20260122",
            close=1234.56,
            pct_change=1.23,
            company_num=50,
        )
        test_db.add(industry)
        test_db.commit()

        # 查询
        result = industry_daily_repo.find_by_code_and_date("801010", "20260122")

        assert result is not None
        assert result.ts_code == "801010"
        assert result.industry == "农林牧渔"
        assert result.close == 1234.56

    def test_find_by_code(self, industry_daily_repo: IndustryDailyRepository, test_db: Session):
        """测试按代码查询行业日线"""
        # 插入多天数据
        for i in range(3):
            industry = IndustryDaily(
                ts_code="801010",
                industry="农林牧渔",
                trade_date=f"2026012{i}",
                close=1234.56 + i,
                pct_change=1.23,
                company_num=50,
            )
            test_db.add(industry)
        test_db.commit()

        # 查询最近2天
        results = industry_daily_repo.find_by_code("801010", limit=2)

        assert len(results) == 2
        # 应该按日期倒序
        assert results[0].trade_date > results[1].trade_date

    def test_find_by_date(self, industry_daily_repo: IndustryDailyRepository, test_db: Session):
        """测试按日期查询行业日线"""
        # 插入多个行业的同一天数据
        industries = [
            IndustryDaily(
                ts_code="801010",
                industry="农林牧渔",
                trade_date="20260122",
                close=1234.56,
                pct_change=1.23,
                company_num=50,
            ),
            IndustryDaily(
                ts_code="801020",
                industry="采掘",
                trade_date="20260122",
                close=2345.67,
                pct_change=-0.45,
                company_num=30,
            ),
        ]
        test_db.add_all(industries)
        test_db.commit()

        # 查询
        results = industry_daily_repo.find_by_date("20260122")

        assert len(results) == 2

    def test_get_all_codes(self, industry_daily_repo: IndustryDailyRepository, test_db: Session):
        """测试获取所有行业代码"""
        # 插入数据
        industries = [
            IndustryDaily(
                ts_code="801010",
                industry="农林牧渔",
                trade_date="20260122",
                close=1234.56,
                pct_change=1.23,
                company_num=50,
            ),
            IndustryDaily(
                ts_code="801020",
                industry="采掘",
                trade_date="20260122",
                close=2345.67,
                pct_change=-0.45,
                company_num=30,
            ),
        ]
        test_db.add_all(industries)
        test_db.commit()

        # 查询
        codes = industry_daily_repo.get_all_codes()

        assert len(codes) == 2
        assert "801010" in codes
        assert "801020" in codes


# ==================== 边界情况测试 ====================


class TestIndustryDailyEdgeCases:
    """边界情况和错误处理测试"""

    def test_find_by_code_and_date_not_found(self, industry_daily_repo: IndustryDailyRepository):
        """测试查询不存在的行业日线"""
        result = industry_daily_repo.find_by_code_and_date("999999", "20260122")
        assert result is None

    def test_upsert_batch_empty(self, industry_daily_repo: IndustryDailyRepository):
        """测试批量插入空列表"""
        count = industry_daily_repo.upsert_batch([])
        assert count == 0

    def test_get_all_codes_empty(self, industry_daily_repo: IndustryDailyRepository):
        """测试获取空的行业代码列表"""
        codes = industry_daily_repo.get_all_codes()
        assert len(codes) == 0

    def test_find_by_code_empty(self, industry_daily_repo: IndustryDailyRepository):
        """测试查询不存在代码的行业日线"""
        results = industry_daily_repo.find_by_code("999999")
        assert len(results) == 0

    def test_find_by_date_empty(self, industry_daily_repo: IndustryDailyRepository):
        """测试查询不存在日期的行业数据"""
        results = industry_daily_repo.find_by_date("20990101")
        assert len(results) == 0
