"""BoardMappingRepository单元测试

测试覆盖:
1. BoardMapping CRUD操作
2. 边界情况和错误处理
"""

import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models import Base, BoardMapping
from src.repositories.board_mapping_repository import BoardMappingRepository


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
def board_mapping_repo(test_db: Session):
    """创建BoardMappingRepository实例"""
    return BoardMappingRepository(test_db)


# ==================== BoardMapping 测试 ====================


class TestBoardMappingOperations:
    """BoardMapping CRUD操作测试"""

    def test_upsert_board_mapping(self, board_mapping_repo: BoardMappingRepository, test_db: Session):
        """测试插入板块映射"""
        mapping = BoardMapping(
            board_name="人工智能",
            board_type="concept",
            board_code="885728",
            constituents=["000001.SZ", "600000.SH"],
            last_updated=datetime.utcnow(),
        )

        result = board_mapping_repo.upsert(mapping)
        test_db.commit()

        assert result.id is not None
        assert result.board_name == "人工智能"
        assert result.board_code == "885728"
        assert len(result.constituents) == 2

    def test_find_by_name_and_type(self, board_mapping_repo: BoardMappingRepository, test_db: Session):
        """测试按名称和类型查询板块"""
        # 插入测试数据
        mapping = BoardMapping(
            board_name="人工智能",
            board_type="concept",
            board_code="885728",
            constituents=["000001.SZ", "600000.SH"],
            last_updated=datetime.utcnow(),
        )
        test_db.add(mapping)
        test_db.commit()

        # 查询
        result = board_mapping_repo.find_by_name_and_type("人工智能", "concept")

        assert result is not None
        assert result.board_name == "人工智能"
        assert result.board_type == "concept"
        assert result.board_code == "885728"

    def test_find_by_type(self, board_mapping_repo: BoardMappingRepository, test_db: Session):
        """测试按类型查询板块"""
        # 插入不同类型的板块
        concept = BoardMapping(
            board_name="人工智能",
            board_type="concept",
            board_code="885728",
            constituents=["000001.SZ"],
            last_updated=datetime.utcnow(),
        )
        industry = BoardMapping(
            board_name="银行",
            board_type="industry",
            board_code="801010",
            constituents=["000001.SZ"],
            last_updated=datetime.utcnow(),
        )
        test_db.add_all([concept, industry])
        test_db.commit()

        # 只查询concept类型
        results = board_mapping_repo.find_by_type("concept")

        assert len(results) == 1
        assert results[0].board_type == "concept"
        assert results[0].board_name == "人工智能"

    def test_upsert_board_mapping_update_existing(self, board_mapping_repo: BoardMappingRepository, test_db: Session):
        """测试更新已存在的板块映射"""
        # 首次插入
        mapping1 = BoardMapping(
            board_name="人工智能",
            board_type="concept",
            board_code="885728",
            constituents=["000001.SZ"],
            last_updated=datetime.utcnow(),
        )
        test_db.add(mapping1)
        test_db.commit()

        # 更新（增加成分股）
        mapping2 = BoardMapping(
            board_name="人工智能",
            board_type="concept",
            board_code="885728",
            constituents=["000001.SZ", "600000.SH"],
            last_updated=datetime.utcnow(),
        )
        result = board_mapping_repo.upsert(mapping2)
        test_db.commit()

        # 验证更新
        assert len(result.constituents) == 2
        assert "600000.SH" in result.constituents


# ==================== 边界情况测试 ====================


class TestBoardMappingEdgeCases:
    """边界情况和错误处理测试"""

    def test_find_by_name_and_type_not_found(self, board_mapping_repo: BoardMappingRepository):
        """测试查询不存在的板块"""
        result = board_mapping_repo.find_by_name_and_type("不存在的板块", "concept")
        assert result is None

    def test_find_by_type_empty(self, board_mapping_repo: BoardMappingRepository):
        """测试查询空的板块类型"""
        results = board_mapping_repo.find_by_type("concept")
        assert len(results) == 0
