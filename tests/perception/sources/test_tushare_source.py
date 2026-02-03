"""Comprehensive tests for TuShareSource — Perception Layer adapter.

Tests cover:
- Configuration (TuShareSourceConfig defaults and overrides)
- Lifecycle (connect / disconnect / idempotency)
- Polling (daily kline, daily_basic, index_daily)
- Event conversion (DataFrame rows → RawMarketEvent)
- Health reporting (status transitions, metrics)
- Error handling (connection failures, API errors, consecutive failures)
- Edge cases (empty DataFrames, NaN values, missing fields)

All TuShare API calls are mocked — no network or token needed.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import List
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.perception.events import (
    EventSource,
    EventType,
    MarketScope,
    RawMarketEvent,
)
from src.perception.health import HealthStatus
from src.perception.sources.base import SourceType
from src.perception.sources.tushare_source import (
    TuShareSource,
    TuShareSourceConfig,
    _safe_float,
)


# ── Fixtures ─────────────────────────────────────────────────────────


def _make_config(**overrides) -> TuShareSourceConfig:
    """Create a TuShareSourceConfig with sensible test defaults."""
    defaults = dict(
        token="test-token-123",
        points=15000,
        delay=0.0,  # no delay in tests
        max_retries=1,
        poll_symbols=["000001", "600519"],
        poll_indices=["000001.SH", "399001.SZ"],
        include_daily_basic=True,
        lookback_days=5,
    )
    defaults.update(overrides)
    return TuShareSourceConfig(**defaults)


def _make_daily_df(symbol: str = "000001.SZ", trade_date: str = "20250710") -> pd.DataFrame:
    """Create a mock TuShare daily() DataFrame."""
    return pd.DataFrame(
        [
            {
                "ts_code": symbol,
                "trade_date": trade_date,
                "open": 15.20,
                "high": 15.80,
                "low": 15.10,
                "close": 15.50,
                "pre_close": 15.00,
                "change": 0.50,
                "pct_chg": 3.33,
                "vol": 1234567.0,
                "amount": 189000.0,
            }
        ]
    )


def _make_daily_basic_df(trade_date: str = "20250710") -> pd.DataFrame:
    """Create a mock TuShare daily_basic() DataFrame."""
    return pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": trade_date,
                "close": 15.50,
                "turnover_rate": 1.23,
                "volume_ratio": 0.98,
                "pe": 8.5,
                "pe_ttm": 9.0,
                "pb": 1.2,
                "ps": 2.1,
                "ps_ttm": 2.3,
                "total_mv": 3000000.0,
                "circ_mv": 2500000.0,
            },
            {
                "ts_code": "600519.SH",
                "trade_date": trade_date,
                "close": 1800.0,
                "turnover_rate": 0.45,
                "volume_ratio": 1.10,
                "pe": 35.0,
                "pe_ttm": 34.0,
                "pb": 12.0,
                "ps": 15.0,
                "ps_ttm": 14.5,
                "total_mv": 22000000.0,
                "circ_mv": 22000000.0,
            },
            {
                # A stock NOT in our poll_symbols — should be filtered out
                "ts_code": "601318.SH",
                "trade_date": trade_date,
                "close": 55.0,
                "turnover_rate": 0.60,
                "volume_ratio": 1.05,
                "pe": 10.0,
                "pe_ttm": 10.5,
                "pb": 1.5,
                "ps": 3.0,
                "ps_ttm": 3.2,
                "total_mv": 1000000.0,
                "circ_mv": 900000.0,
            },
        ]
    )


def _make_index_daily_df(
    ts_code: str = "000001.SH", trade_date: str = "20250710"
) -> pd.DataFrame:
    """Create a mock TuShare index_daily() DataFrame."""
    return pd.DataFrame(
        [
            {
                "ts_code": ts_code,
                "trade_date": trade_date,
                "open": 3200.0,
                "high": 3280.0,
                "low": 3190.0,
                "close": 3260.0,
                "pre_close": 3210.0,
                "change": 50.0,
                "pct_chg": 1.56,
                "vol": 266000000.0,
                "amount": 288000000.0,
            }
        ]
    )


def _mock_tushare_client():
    """Create a fully-mocked TushareClient."""
    client = MagicMock()
    client.get_latest_trade_date.return_value = "20250710"
    client.normalize_ts_code.side_effect = lambda s: (
        f"{s}.SH" if s.startswith("6") else f"{s}.SZ"
    )
    client.denormalize_ts_code.side_effect = lambda s: s.split(".")[0]
    client.fetch_daily.return_value = _make_daily_df()
    client.fetch_daily_basic.return_value = _make_daily_basic_df()
    client.fetch_index_daily.return_value = _make_index_daily_df()
    return client


# ── Config Tests ─────────────────────────────────────────────────────


class TestTuShareSourceConfig:
    def test_defaults(self):
        cfg = TuShareSourceConfig(token="abc")
        assert cfg.token == "abc"
        assert cfg.points == 15000
        assert cfg.delay == 0.3
        assert cfg.max_retries == 3
        assert cfg.poll_symbols == []
        assert len(cfg.poll_indices) == 3  # default indices
        assert cfg.include_daily_basic is True
        assert cfg.lookback_days == 5

    def test_custom(self):
        cfg = TuShareSourceConfig(
            token="xyz",
            points=5000,
            delay=0.5,
            poll_symbols=["000001"],
            poll_indices=[],
            include_daily_basic=False,
        )
        assert cfg.points == 5000
        assert cfg.poll_symbols == ["000001"]
        assert cfg.poll_indices == []
        assert cfg.include_daily_basic is False

    def test_none_poll_indices_uses_defaults(self):
        cfg = TuShareSourceConfig(token="t", poll_indices=None)
        assert len(cfg.poll_indices) == 3
        assert "000001.SH" in cfg.poll_indices


# ── Lifecycle Tests ──────────────────────────────────────────────────


class TestTuShareSourceLifecycle:
    def test_name_and_type(self):
        src = TuShareSource(_make_config())
        assert src.name == "tushare"
        assert src.source_type == SourceType.POLLING

    @pytest.mark.asyncio
    async def test_connect_creates_client(self):
        src = TuShareSource(_make_config())
        with patch(
            "src.services.tushare_client.TushareClient"
        ) as MockClient:
            MockClient.return_value = _mock_tushare_client()
            await src.connect()
            assert src._connected is True
            assert src._client is not None
            MockClient.assert_called_once_with(
                token="test-token-123",
                points=15000,
                delay=0.0,
                max_retries=1,
            )

    @pytest.mark.asyncio
    async def test_connect_idempotent(self):
        src = TuShareSource(_make_config())
        with patch(
            "src.services.tushare_client.TushareClient"
        ) as MockClient:
            MockClient.return_value = _mock_tushare_client()
            await src.connect()
            await src.connect()  # second call should be a no-op
            MockClient.assert_called_once()

    @pytest.mark.asyncio
    async def test_disconnect(self):
        src = TuShareSource(_make_config())
        with patch(
            "src.services.tushare_client.TushareClient"
        ) as MockClient:
            MockClient.return_value = _mock_tushare_client()
            await src.connect()
            await src.disconnect()
            assert src._connected is False
            assert src._client is None

    @pytest.mark.asyncio
    async def test_poll_before_connect_raises(self):
        src = TuShareSource(_make_config())
        with pytest.raises(RuntimeError, match="not connected"):
            await src.poll()


# ── Poll Tests ───────────────────────────────────────────────────────


class TestTuShareSourcePoll:
    """Tests for poll() — the main data-fetching entry point."""

    async def _connected_source(self) -> TuShareSource:
        """Helper: return a connected source with mocked client."""
        src = TuShareSource(_make_config())
        src._client = _mock_tushare_client()
        src._connected = True
        src._connect_time = datetime.now(timezone.utc)
        return src

    @pytest.mark.asyncio
    async def test_poll_returns_events(self):
        src = await self._connected_source()
        events = await src.poll()
        assert len(events) > 0
        assert all(isinstance(e, RawMarketEvent) for e in events)

    @pytest.mark.asyncio
    async def test_poll_kline_events(self):
        src = await self._connected_source()
        events = await src.poll()
        kline_events = [e for e in events if e.event_type == EventType.KLINE.value]
        # 2 symbols → at least 2 kline events
        assert len(kline_events) >= 2
        for ev in kline_events:
            assert ev.source == EventSource.TUSHARE.value
            assert ev.market == MarketScope.CN_STOCK.value
            assert "open" in ev.data
            assert "close" in ev.data
            assert "volume" in ev.data

    @pytest.mark.asyncio
    async def test_poll_fundamental_events(self):
        src = await self._connected_source()
        events = await src.poll()
        earnings_events = [
            e for e in events if e.event_type == EventType.EARNINGS.value
        ]
        # Should get 2 (our 2 configured symbols), NOT 3 (filtered out 601318)
        assert len(earnings_events) == 2
        for ev in earnings_events:
            assert ev.source == EventSource.TUSHARE.value
            assert "pe" in ev.data or "pe_ttm" in ev.data

    @pytest.mark.asyncio
    async def test_poll_index_events(self):
        src = await self._connected_source()
        events = await src.poll()
        index_events = [
            e for e in events if e.event_type == EventType.INDEX_UPDATE.value
        ]
        # 2 configured indices
        assert len(index_events) >= 2
        for ev in index_events:
            assert ev.market == MarketScope.CN_INDEX.value

    @pytest.mark.asyncio
    async def test_poll_no_symbols_no_kline(self):
        """If poll_symbols is empty, skip kline + daily_basic."""
        cfg = _make_config(poll_symbols=[], include_daily_basic=True)
        src = TuShareSource(cfg)
        src._client = _mock_tushare_client()
        src._connected = True
        src._connect_time = datetime.now(timezone.utc)

        events = await src.poll()
        kline_events = [e for e in events if e.event_type == EventType.KLINE.value]
        earnings_events = [e for e in events if e.event_type == EventType.EARNINGS.value]
        assert len(kline_events) == 0
        assert len(earnings_events) == 0

    @pytest.mark.asyncio
    async def test_poll_no_indices(self):
        """If poll_indices is empty, skip index fetching."""
        cfg = _make_config(poll_indices=[])
        src = TuShareSource(cfg)
        src._client = _mock_tushare_client()
        src._connected = True
        src._connect_time = datetime.now(timezone.utc)

        events = await src.poll()
        index_events = [
            e for e in events if e.event_type == EventType.INDEX_UPDATE.value
        ]
        assert len(index_events) == 0

    @pytest.mark.asyncio
    async def test_poll_without_daily_basic(self):
        """include_daily_basic=False should skip fundamentals."""
        cfg = _make_config(include_daily_basic=False)
        src = TuShareSource(cfg)
        src._client = _mock_tushare_client()
        src._connected = True
        src._connect_time = datetime.now(timezone.utc)

        events = await src.poll()
        earnings_events = [
            e for e in events if e.event_type == EventType.EARNINGS.value
        ]
        assert len(earnings_events) == 0

    @pytest.mark.asyncio
    async def test_poll_updates_metrics(self):
        src = await self._connected_source()
        assert src._total_polls == 0
        assert src._total_events == 0

        events = await src.poll()

        assert src._total_polls == 1
        assert src._total_events == len(events)
        assert src._consecutive_failures == 0
        assert src._last_success is not None
        assert src._last_latency_ms is not None
        assert src._last_latency_ms >= 0


# ── Event Conversion Tests ───────────────────────────────────────────


class TestEventConversion:
    """Test row-to-event conversion helpers."""

    def _source(self) -> TuShareSource:
        src = TuShareSource(_make_config())
        src._client = _mock_tushare_client()
        return src

    def test_kline_event_fields(self):
        src = self._source()
        row = _make_daily_df().iloc[0]
        ev = src._row_to_kline_event(row, "000001")

        assert ev.source == EventSource.TUSHARE.value
        assert ev.event_type == EventType.KLINE.value
        assert ev.market == MarketScope.CN_STOCK.value
        assert ev.symbol == "000001"
        assert ev.data["open"] == 15.20
        assert ev.data["high"] == 15.80
        assert ev.data["low"] == 15.10
        assert ev.data["close"] == 15.50
        assert ev.data["volume"] == 1234567.0
        assert ev.data["amount"] == 189000.0
        # Optional fields
        assert ev.data["pre_close"] == 15.0
        assert ev.data["change"] == 0.50
        assert ev.data["pct_chg"] == 3.33

    def test_kline_event_timestamp(self):
        src = self._source()
        row = _make_daily_df(trade_date="20250710").iloc[0]
        ev = src._row_to_kline_event(row, "000001")
        assert ev.timestamp.year == 2025
        assert ev.timestamp.month == 7
        assert ev.timestamp.day == 10
        assert ev.timestamp.tzinfo == timezone.utc

    def test_fundamental_event_fields(self):
        src = self._source()
        row = _make_daily_basic_df().iloc[0]
        ev = src._row_to_fundamental_event(row, "000001")

        assert ev.event_type == EventType.EARNINGS.value
        assert ev.data["pe"] == 8.5
        assert ev.data["pe_ttm"] == 9.0
        assert ev.data["pb"] == 1.2
        assert ev.data["total_mv"] == 3000000.0

    def test_index_event_fields(self):
        src = self._source()
        row = _make_index_daily_df().iloc[0]
        ev = src._row_to_index_event(row, "000001.SH")

        assert ev.event_type == EventType.INDEX_UPDATE.value
        assert ev.market == MarketScope.CN_INDEX.value
        assert ev.symbol == "000001.SH"
        assert ev.data["close"] == 3260.0
        assert ev.data["pct_chg"] == 1.56

    def test_nan_values_become_none(self):
        """NaN values in DataFrame should convert to None."""
        src = self._source()
        df = _make_daily_df()
        df.loc[0, "vol"] = float("nan")
        df.loc[0, "amount"] = None
        row = df.iloc[0]
        ev = src._row_to_kline_event(row, "000001")
        assert ev.data["volume"] is None
        assert ev.data["amount"] is None


# ── Health Tests ─────────────────────────────────────────────────────


class TestTuShareSourceHealth:
    def test_health_unknown_when_not_connected(self):
        src = TuShareSource(_make_config())
        h = src.health()
        assert h.source_name == "tushare"
        assert h.status == HealthStatus.UNKNOWN.value
        assert h.total_polls == 0

    @pytest.mark.asyncio
    async def test_health_healthy_after_success(self):
        src = TuShareSource(_make_config())
        src._client = _mock_tushare_client()
        src._connected = True
        src._connect_time = datetime.now(timezone.utc)

        await src.poll()

        h = src.health()
        assert h.status == HealthStatus.HEALTHY.value
        assert h.total_polls == 1
        assert h.total_events > 0
        assert h.consecutive_failures == 0
        assert h.last_success is not None
        assert h.uptime_seconds is not None
        assert h.uptime_seconds >= 0

    def test_health_degraded_on_failures(self):
        src = TuShareSource(_make_config())
        src._connected = True
        src._connect_time = datetime.now(timezone.utc)
        src._consecutive_failures = 3  # ≥2, <5 → DEGRADED
        src._total_polls = 5

        h = src.health()
        assert h.status == HealthStatus.DEGRADED.value

    def test_health_unhealthy_on_many_failures(self):
        src = TuShareSource(_make_config())
        src._connected = True
        src._connect_time = datetime.now(timezone.utc)
        src._consecutive_failures = 5  # ≥5 → UNHEALTHY
        src._total_polls = 10

        h = src.health()
        assert h.status == HealthStatus.UNHEALTHY.value

    def test_health_error_rate(self):
        src = TuShareSource(_make_config())
        src._connected = True
        src._connect_time = datetime.now(timezone.utc)
        src._total_polls = 10
        src._consecutive_failures = 3

        h = src.health()
        assert h.error_rate == pytest.approx(0.3)


# ── Error Handling Tests ─────────────────────────────────────────────


class TestTuShareSourceErrors:
    @pytest.mark.asyncio
    async def test_kline_failure_logged_not_fatal(self):
        """A single symbol failure shouldn't crash the whole poll."""
        src = TuShareSource(_make_config())
        client = _mock_tushare_client()
        # Make fetch_daily fail for first call, succeed for second
        call_count = 0
        original_return = _make_daily_df()

        def side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("API error for symbol")
            return original_return

        client.fetch_daily.side_effect = side_effect
        src._client = client
        src._connected = True
        src._connect_time = datetime.now(timezone.utc)

        events = await src.poll()
        # Should still have events from the second symbol + indices + basics
        assert len(events) > 0

    @pytest.mark.asyncio
    async def test_daily_basic_failure_non_fatal(self):
        """daily_basic failure shouldn't prevent other events."""
        src = TuShareSource(_make_config())
        client = _mock_tushare_client()
        client.fetch_daily_basic.side_effect = Exception("daily_basic API down")
        src._client = client
        src._connected = True
        src._connect_time = datetime.now(timezone.utc)

        events = await src.poll()
        # Should still have kline + index events
        kline_events = [e for e in events if e.event_type == EventType.KLINE.value]
        assert len(kline_events) > 0

    @pytest.mark.asyncio
    async def test_index_failure_non_fatal(self):
        """Index API failure shouldn't prevent other events."""
        src = TuShareSource(_make_config())
        client = _mock_tushare_client()
        client.fetch_index_daily.side_effect = Exception("Index API error")
        src._client = client
        src._connected = True
        src._connect_time = datetime.now(timezone.utc)

        events = await src.poll()
        kline_events = [e for e in events if e.event_type == EventType.KLINE.value]
        assert len(kline_events) > 0

    @pytest.mark.asyncio
    async def test_total_failure_records_metrics(self):
        """If get_latest_trade_date itself fails, poll raises and records failure."""
        src = TuShareSource(_make_config())
        client = _mock_tushare_client()
        client.get_latest_trade_date.side_effect = Exception("Network down")
        src._client = client
        src._connected = True
        src._connect_time = datetime.now(timezone.utc)

        with pytest.raises(Exception, match="Network down"):
            await src.poll()

        assert src._consecutive_failures == 1
        assert src._last_error is not None
        assert src._last_error_message == "Network down"


# ── Empty / Edge Cases ───────────────────────────────────────────────


class TestTuShareSourceEdgeCases:
    @pytest.mark.asyncio
    async def test_empty_daily_returns_no_kline_events(self):
        src = TuShareSource(_make_config())
        client = _mock_tushare_client()
        client.fetch_daily.return_value = pd.DataFrame()
        src._client = client
        src._connected = True
        src._connect_time = datetime.now(timezone.utc)

        events = await src.poll()
        kline_events = [e for e in events if e.event_type == EventType.KLINE.value]
        assert len(kline_events) == 0

    @pytest.mark.asyncio
    async def test_empty_daily_basic_returns_no_fundamental_events(self):
        src = TuShareSource(_make_config())
        client = _mock_tushare_client()
        client.fetch_daily_basic.return_value = pd.DataFrame()
        src._client = client
        src._connected = True
        src._connect_time = datetime.now(timezone.utc)

        events = await src.poll()
        earnings_events = [
            e for e in events if e.event_type == EventType.EARNINGS.value
        ]
        assert len(earnings_events) == 0

    @pytest.mark.asyncio
    async def test_empty_index_daily_returns_no_index_events(self):
        src = TuShareSource(_make_config())
        client = _mock_tushare_client()
        client.fetch_index_daily.return_value = pd.DataFrame()
        src._client = client
        src._connected = True
        src._connect_time = datetime.now(timezone.utc)

        events = await src.poll()
        index_events = [
            e for e in events if e.event_type == EventType.INDEX_UPDATE.value
        ]
        assert len(index_events) == 0


# ── _safe_float Tests ────────────────────────────────────────────────


class TestSafeFloat:
    def test_normal_float(self):
        assert _safe_float(3.14) == 3.14

    def test_int_to_float(self):
        assert _safe_float(42) == 42.0

    def test_string_float(self):
        assert _safe_float("1.23") == 1.23

    def test_none_returns_none(self):
        assert _safe_float(None) is None

    def test_nan_returns_none(self):
        assert _safe_float(float("nan")) is None

    def test_invalid_string_returns_none(self):
        assert _safe_float("not-a-number") is None

    def test_empty_string_returns_none(self):
        assert _safe_float("") is None


# ── Registry Integration ─────────────────────────────────────────────


class TestTuShareSourceRegistry:
    """Test that TuShareSource works with SourceRegistry."""

    def test_register_and_lookup(self):
        from src.perception.sources.registry import SourceRegistry

        registry = SourceRegistry()
        src = TuShareSource(_make_config())
        registry.register(src)

        assert "tushare" in registry
        assert registry.get("tushare") is src

    def test_health_report(self):
        from src.perception.sources.registry import SourceRegistry

        registry = SourceRegistry()
        src = TuShareSource(_make_config())
        registry.register(src)

        report = registry.health_report()
        assert "tushare" in report
        assert report["tushare"].source_name == "tushare"
        assert report["tushare"].status == HealthStatus.UNKNOWN.value
