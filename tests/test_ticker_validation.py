"""Tests for ticker parameter validation."""

import pytest
from fastapi.testclient import TestClient


def test_valid_ticker_format():
    """Test that valid 6-digit tickers are accepted."""
    from src.main import app

    client = TestClient(app)

    # Valid tickers
    valid_tickers = ["000001", "600519", "300750"]

    for ticker in valid_tickers:
        response = client.get(f"/api/candles/{ticker}?timeframe=day")
        # Should return 200 or 404, but not 422 (validation error)
        assert response.status_code in [200, 404], f"Ticker {ticker} should be valid"


def test_invalid_ticker_format():
    """Test that invalid ticker formats are rejected."""
    from src.main import app

    client = TestClient(app)

    # Invalid tickers
    invalid_tickers = [
        ("abc123", 422),   # Contains letters - validation error
        ("12345", 422),    # Too short - validation error
        ("1234567", 422),  # Too long - validation error
        ("00000a", 422),   # Contains letter - validation error
        ("../etc", 404),   # Path traversal - FastAPI routing error (safe)
    ]

    for ticker, expected_status in invalid_tickers:
        response = client.get(f"/api/candles/{ticker}?timeframe=day")
        # Should return either 422 (validation error) or 404 (routing error)
        assert response.status_code == expected_status, \
            f"Ticker {ticker} should return {expected_status}, got {response.status_code}"

        # Only check pattern error for validation failures
        if expected_status == 422:
            assert "pattern" in response.json()["detail"][0]["type"]


def test_ticker_validation_error_message():
    """Test that validation errors provide clear messages."""
    from src.main import app

    client = TestClient(app)

    response = client.get("/api/candles/abc123?timeframe=day")
    assert response.status_code == 422

    error_detail = response.json()["detail"][0]
    assert error_detail["type"] == "string_pattern_mismatch"
    assert error_detail["loc"] == ["path", "ticker"]
    # Check that the pattern requires 6 digits (with optional market suffix)
    assert "^[0-9]{6}" in error_detail["ctx"]["pattern"]
