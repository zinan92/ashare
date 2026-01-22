"""
BoardMappingRepository - 板块映射数据访问层

封装 BoardMapping 模型的数据库操作。
"""

from typing import List, Optional

from sqlalchemy import and_, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from src.models import BoardMapping
from src.repositories.base_repository import BaseRepository
from src.utils.logging import get_logger

logger = get_logger(__name__)


class BoardMappingRepository(BaseRepository[BoardMapping]):
    """板块映射Repository"""

    def __init__(self, session: Session):
        """初始化BoardMappingRepository"""
        super().__init__(session, BoardMapping)

    def find_by_name_and_type(
        self, board_name: str, board_type: str
    ) -> Optional[BoardMapping]:
        """
        根据板块名称和类型查询板块映射

        Args:
            board_name: 板块名称
            board_type: 板块类型（industry/concept）

        Returns:
            板块映射或None
        """
        stmt = select(BoardMapping).filter(
            and_(
                BoardMapping.board_name == board_name,
                BoardMapping.board_type == board_type,
            )
        )
        result = self.session.execute(stmt)
        return result.scalar_one_or_none()

    def find_by_type(self, board_type: str) -> List[BoardMapping]:
        """
        根据板块类型查询所有板块

        Args:
            board_type: 板块类型（industry/concept）

        Returns:
            板块映射列表
        """
        stmt = select(BoardMapping).filter(BoardMapping.board_type == board_type)
        result = self.session.execute(stmt)
        return list(result.scalars().all())

    def upsert(self, board_mapping: BoardMapping) -> BoardMapping:
        """
        插入或更新板块映射

        Args:
            board_mapping: 板块映射

        Returns:
            保存后的板块映射
        """
        stmt = sqlite_insert(BoardMapping).values(
            board_name=board_mapping.board_name,
            board_type=board_mapping.board_type,
            board_code=board_mapping.board_code,
            constituents=board_mapping.constituents,
            last_updated=board_mapping.last_updated,
        )

        stmt = stmt.on_conflict_do_update(
            index_elements=["board_name", "board_type"],
            set_={
                "board_code": stmt.excluded.board_code,
                "constituents": stmt.excluded.constituents,
                "last_updated": stmt.excluded.last_updated,
            },
        )

        self.session.execute(stmt)
        self.session.flush()

        return self.find_by_name_and_type(
            board_mapping.board_name, board_mapping.board_type
        )
