"""
Stub for tonghuashun_service — placeholder until full implementation.
"""
import logging

logger = logging.getLogger(__name__)


class TonghuashunService:
    """Stub service for Tonghuashun board data."""

    def get_industry_boards(self):
        return []

    def get_concept_boards(self):
        return []

    def get_board_stocks(self, code: str):
        return []

    def refresh(self):
        logger.info("Tonghuashun service refresh (stub)")


tonghuashun_service = TonghuashunService()
