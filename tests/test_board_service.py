"""
Tests for the unified BoardService (merged from BoardMappingService + TushareBoardService).
"""
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime, timezone

from src.services.board_service import BoardService


class FakeBoardMapping:
    """Lightweight stand-in for the BoardMapping ORM model."""

    def __init__(self, board_name, board_type, board_code, constituents=None, last_updated=None):
        self.board_name = board_name
        self.board_type = board_type
        self.board_code = board_code
        self.constituents = constituents or []
        self.last_updated = last_updated or datetime.now(timezone.utc)


class FakeSymbol:
    """Lightweight stand-in for SymbolMetadata."""

    def __init__(self, ticker, concepts=None, industry_lv1=None, super_category=None):
        self.ticker = ticker
        self.concepts = concepts or []
        self.industry_lv1 = industry_lv1
        self.super_category = super_category


@pytest.fixture
def board_repo():
    return MagicMock()


@pytest.fixture
def symbol_repo():
    return MagicMock()


@pytest.fixture
def settings():
    s = MagicMock()
    s.tushare_token = "test_token"
    s.tushare_points = 2000
    s.tushare_delay = 0.0
    s.tushare_max_retries = 1
    s.enable_concept_boards = True
    return s


@pytest.fixture
def service(board_repo, symbol_repo, settings):
    with patch("src.services.board_service.TushareClient"):
        svc = BoardService(
            board_repo=board_repo,
            symbol_repo=symbol_repo,
            settings=settings,
        )
    return svc


class TestBoardServiceInit:
    """Verify construction and factory."""

    def test_init(self, service):
        assert service is not None
        assert service.max_retries == 3

    def test_create_with_session(self, settings):
        session = MagicMock()
        with patch("src.services.board_service.TushareClient"):
            with patch("src.services.board_service.BoardMappingRepository") as mock_br:
                with patch("src.services.board_service.SymbolRepository") as mock_sr:
                    svc = BoardService.create_with_session(session, settings)
        assert svc is not None
        mock_br.assert_called_once_with(session)
        mock_sr.assert_called_once_with(session)


class TestBoardServiceQuery:
    """Query methods that wrap repository calls."""

    def test_get_stock_concepts_from_symbol(self, service, symbol_repo):
        """Fast path: concepts already in symbol_metadata."""
        symbol_repo.find_by_ticker.return_value = FakeSymbol("000001", concepts=["AI", "金融科技"])
        result = service.get_stock_concepts("000001")
        assert result == ["AI", "金融科技"]

    def test_get_stock_concepts_fallback_scan(self, service, board_repo, symbol_repo):
        """Slow path: scan board mappings when symbol has no concepts."""
        symbol_repo.find_by_ticker.return_value = FakeSymbol("000001", concepts=[])
        board_repo.find_by_type.return_value = [
            FakeBoardMapping("AI", "concept", "C001", constituents=["000001", "600000"]),
            FakeBoardMapping("银行", "concept", "C002", constituents=["601398"]),
        ]
        result = service.get_stock_concepts("000001")
        assert result == ["AI"]

    def test_get_industry_boards(self, service, board_repo):
        board_repo.find_by_type.return_value = [
            FakeBoardMapping("银行", "industry", "I001", constituents=["601398", "000001"]),
        ]
        result = service.get_industry_boards()
        assert len(result) == 1
        assert result[0]["board_name"] == "银行"
        assert result[0]["count"] == 2

    def test_get_concept_boards(self, service, board_repo):
        board_repo.find_by_type.return_value = []
        result = service.get_concept_boards()
        assert result == []

    def test_get_board_constituents(self, service, board_repo):
        board_repo.find_by_name_and_type.return_value = FakeBoardMapping(
            "银行", "industry", "I001", constituents=["601398", "000001"]
        )
        result = service.get_board_constituents("银行", "industry")
        assert "601398" in result

    def test_get_board_constituents_not_found(self, service, board_repo):
        board_repo.find_by_name_and_type.return_value = None
        result = service.get_board_constituents("不存在", "industry")
        assert result == []


class TestBoardServiceVerify:
    """Change verification."""

    def test_verify_no_changes(self, service, board_repo):
        board_repo.find_by_name_and_type.return_value = FakeBoardMapping(
            "银行", "industry", "I001", constituents=["601398", "000001"]
        )
        service._fetch_board_constituents = MagicMock(return_value=["601398", "000001"])
        result = service.verify_changes("银行", "industry")
        assert result["has_changes"] is False

    def test_verify_with_changes(self, service, board_repo):
        board_repo.find_by_name_and_type.return_value = FakeBoardMapping(
            "银行", "industry", "I001", constituents=["601398", "000001"]
        )
        service._fetch_board_constituents = MagicMock(return_value=["601398", "000001", "600036"])
        result = service.verify_changes("银行", "industry")
        assert result["has_changes"] is True
        assert "600036" in result["added"]


class TestBoardServiceBoardsAsDicts:
    """Helper that converts ORM objects to plain dicts."""

    def test_boards_as_dicts(self, service, board_repo):
        board_repo.find_by_type.return_value = [
            FakeBoardMapping("AI", "concept", "C001", constituents=["000001"]),
        ]
        dicts = service._boards_as_dicts("concept")
        assert len(dicts) == 1
        assert dicts[0]["count"] == 1
        assert dicts[0]["board_code"] == "C001"
