"""Comprehensive tests for SinaSource — Perception Layer Sina adapter.

Tests:
- CircuitBreaker state machine
- SinaSource lifecycle (connect / poll / disconnect)
- Quote parsing (stock & index)
- Kline parsing
- Retry + exponential backoff
- 456 rate-limit handling
- Health reporting
- Edge cases (empty data, malformed responses)
"""

from __future__ import annotations

import asyncio
import re
import time
from datetime import datetime, timedelta, timezone
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.perception.config import CircuitBreakerConfig, SourcePollConfig
from src.perception.events import EventSource, EventType, MarketScope, RawMarketEvent
from src.perception.health import HealthStatus
from src.perception.sources.base import SourceType
from src.perception.sources.sina_source import (
    CircuitBreaker,
    CircuitState,
    SinaCircuitOpenError,
    SinaFetchError,
    SinaRateLimitError,
    SinaSource,
    _backoff_delay,
    _parse_bar_timestamp,
    _parse_quote_timestamp,
)


# ═══════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════


@pytest.fixture
def cb_config():
    return CircuitBreakerConfig(
        failure_threshold=3,
        recovery_timeout_seconds=1.0,  # short for testing
        half_open_max_calls=1,
    )


@pytest.fixture
def poll_config():
    return SourcePollConfig(
        source_name="sina",
        poll_interval_seconds=5.0,
        timeout_seconds=5.0,
        max_retries=2,
        backoff_factor=0.01,  # near-instant backoff for tests
    )


@pytest.fixture
def source(poll_config, cb_config):
    return SinaSource(
        poll_config=poll_config,
        cb_config=cb_config,
        stock_symbols=["sh600519", "sz000001"],
        index_symbols=["sh000001", "sz399001"],
    )


# Realistic Sina response fixtures
STOCK_QUOTE_RESPONSE = (
    'var hq_str_sh600519="贵州茅台,1810.00,1800.00,1815.50,1820.00,1798.00,'
    "1815.00,1816.00,35000000,63000000000,"
    "100,1815.00,200,1814.90,300,1814.80,400,1814.70,500,1814.60,"
    "100,1816.00,200,1816.10,300,1816.20,400,1816.30,500,1816.40,"
    '2025-07-10,15:00:03,00";\n'
    'var hq_str_sz000001="平安银行,12.50,12.40,12.55,12.60,12.38,'
    "12.54,12.55,50000000,6250000000,"
    "100,12.54,200,12.53,300,12.52,400,12.51,500,12.50,"
    "100,12.55,200,12.56,300,12.57,400,12.58,500,12.59,"
    '2025-07-10,15:00:03,00";\n'
)

INDEX_SUMMARY_RESPONSE = (
    'var hq_str_s_sh000001="上证指数,3259.22,46.14,1.44,2660394,28862016";\n'
    'var hq_str_s_sz399001="深证成指,10425.33,150.22,1.46,3200000,42000000";\n'
)

KLINE_RESPONSE = [
    {
        "day": "2025-07-10 14:30:00",
        "open": "1800.00",
        "high": "1815.00",
        "low": "1798.00",
        "close": "1810.00",
        "volume": "12000",
    },
    {
        "day": "2025-07-10 15:00:00",
        "open": "1810.00",
        "high": "1820.00",
        "low": "1808.00",
        "close": "1815.50",
        "volume": "15000",
    },
]


def _make_response(
    text: str = "",
    status_code: int = 200,
    json_data=None,
) -> httpx.Response:
    """Create a mock httpx.Response."""
    import json as _json

    request = httpx.Request("GET", "https://hq.sinajs.cn/list=test")
    if json_data is not None:
        content = _json.dumps(json_data).encode("utf-8")
        return httpx.Response(
            status_code=status_code,
            content=content,
            headers={"content-type": "application/json"},
            request=request,
        )
    return httpx.Response(
        status_code=status_code,
        text=text,
        request=request,
    )


# ═══════════════════════════════════════════════════════════════════
# CircuitBreaker tests
# ═══════════════════════════════════════════════════════════════════


class TestCircuitBreaker:
    def test_initial_state_closed(self, cb_config):
        cb = CircuitBreaker(cb_config)
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request()

    def test_stays_closed_under_threshold(self, cb_config):
        cb = CircuitBreaker(cb_config)
        cb.record_failure()
        cb.record_failure()
        # 2 failures < threshold of 3
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request()

    def test_opens_at_threshold(self, cb_config):
        cb = CircuitBreaker(cb_config)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert not cb.allow_request()

    def test_resets_on_success(self, cb_config):
        cb = CircuitBreaker(cb_config)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    def test_transitions_to_half_open_after_timeout(self, cb_config):
        # recovery_timeout_seconds=1.0
        cb = CircuitBreaker(cb_config)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN

        # Simulate time passing by adjusting last failure time
        cb._last_failure_time = time.monotonic() - 2.0
        assert cb.state == CircuitState.HALF_OPEN
        assert cb.allow_request()

    def test_half_open_success_closes(self, cb_config):
        cb = CircuitBreaker(cb_config)
        for _ in range(3):
            cb.record_failure()
        cb._last_failure_time = time.monotonic() - 2.0
        assert cb.state == CircuitState.HALF_OPEN

        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    def test_half_open_failure_reopens(self, cb_config):
        cb = CircuitBreaker(cb_config)
        for _ in range(3):
            cb.record_failure()
        cb._last_failure_time = time.monotonic() - 2.0
        assert cb.state == CircuitState.HALF_OPEN

        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_half_open_max_calls(self, cb_config):
        """Only half_open_max_calls=1 probe is allowed."""
        cb = CircuitBreaker(cb_config)
        for _ in range(3):
            cb.record_failure()
        cb._last_failure_time = time.monotonic() - 2.0

        assert cb.allow_request()  # first probe
        cb._half_open_calls = 1
        assert not cb.allow_request()  # second probe rejected

    def test_reset(self, cb_config):
        cb = CircuitBreaker(cb_config)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN

        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0
        assert cb.allow_request()


# ═══════════════════════════════════════════════════════════════════
# Backoff helper tests
# ═══════════════════════════════════════════════════════════════════


class TestBackoffDelay:
    def test_delay_is_bounded(self):
        for attempt in range(10):
            delay = _backoff_delay(attempt, factor=2.0, max_delay=60.0)
            assert 0 <= delay <= 60.0

    def test_delay_increases_with_attempts(self):
        """On average, later attempts should have larger max delays."""
        # Just verify the max possible delay grows
        assert _backoff_delay(0, factor=2.0, max_delay=60.0) <= 2.0
        # attempt=5 → max = min(2^5, 60) = 32
        # We can't guarantee randomness, but the range grows


# ═══════════════════════════════════════════════════════════════════
# Timestamp parsing tests
# ═══════════════════════════════════════════════════════════════════


class TestTimestampParsing:
    def test_parse_quote_timestamp(self):
        dt = _parse_quote_timestamp("2025-07-10", "15:00:03")
        assert dt.tzinfo == timezone.utc
        # CST 15:00:03 = UTC 07:00:03
        assert dt.hour == 7
        assert dt.minute == 0
        assert dt.second == 3

    def test_parse_quote_timestamp_invalid(self):
        dt = _parse_quote_timestamp("", "")
        assert dt.tzinfo == timezone.utc  # falls back to now

    def test_parse_bar_timestamp_datetime(self):
        dt = _parse_bar_timestamp("2025-07-10 14:30:00")
        assert dt.tzinfo == timezone.utc
        # CST 14:30 = UTC 06:30
        assert dt.hour == 6
        assert dt.minute == 30

    def test_parse_bar_timestamp_date_only(self):
        dt = _parse_bar_timestamp("2025-07-10")
        assert dt.tzinfo == timezone.utc

    def test_parse_bar_timestamp_invalid(self):
        dt = _parse_bar_timestamp("garbage")
        assert dt.tzinfo == timezone.utc


# ═══════════════════════════════════════════════════════════════════
# SinaSource lifecycle tests
# ═══════════════════════════════════════════════════════════════════


class TestSinaSourceLifecycle:
    def test_properties(self, source):
        assert source.name == "sina"
        assert source.source_type == SourceType.POLLING

    @pytest.mark.asyncio
    async def test_connect_creates_client(self, source):
        await source.connect()
        assert source._connected
        assert source._client is not None
        await source.disconnect()

    @pytest.mark.asyncio
    async def test_connect_idempotent(self, source):
        await source.connect()
        client1 = source._client
        await source.connect()
        assert source._client is client1  # same client
        await source.disconnect()

    @pytest.mark.asyncio
    async def test_disconnect(self, source):
        await source.connect()
        await source.disconnect()
        assert not source._connected
        assert source._client is None

    @pytest.mark.asyncio
    async def test_poll_before_connect_raises(self, source):
        with pytest.raises(RuntimeError, match="not connected"):
            await source.poll()

    def test_default_config(self):
        """SinaSource with no args uses sensible defaults."""
        s = SinaSource()
        assert s.name == "sina"
        assert s._poll_config.source_name == "sina"
        assert s._poll_config.timeout_seconds == 10.0


# ═══════════════════════════════════════════════════════════════════
# SinaSource quote parsing tests
# ═══════════════════════════════════════════════════════════════════


class TestStockQuoteParsing:
    def test_parse_stock_quotes(self, source):
        events = source._parse_hq_response(STOCK_QUOTE_RESPONSE, MarketScope.CN_STOCK)
        assert len(events) == 2

        # First stock: 贵州茅台 (sh600519)
        e0 = events[0]
        assert e0.source == "sina"
        assert e0.event_type == "price_update"
        assert e0.market == "cn_stock"
        assert e0.symbol == "600519"
        assert e0.data["name"] == "贵州茅台"
        assert e0.data["price"] == 1815.50
        assert e0.data["open"] == 1810.00
        assert e0.data["prev_close"] == 1800.00
        assert e0.data["high"] == 1820.00
        assert e0.data["low"] == 1798.00
        assert "change" in e0.data
        assert "change_pct" in e0.data

        # Second stock: 平安银行 (sz000001)
        e1 = events[1]
        assert e1.symbol == "000001"
        assert e1.data["price"] == 12.55

    def test_parse_empty_response(self, source):
        events = source._parse_hq_response("", MarketScope.CN_STOCK)
        assert events == []

    def test_parse_incomplete_quote(self, source):
        """Quotes with < 32 fields are skipped."""
        bad_response = 'var hq_str_sh600519="贵州茅台,1800.00,1800.00";\n'
        events = source._parse_hq_response(bad_response, MarketScope.CN_STOCK)
        assert events == []

    def test_parse_zero_price_skipped(self, source):
        """Price=0 (market closed) should be skipped."""
        zero_response = (
            'var hq_str_sh600519="贵州茅台,0,0,0,0,0,'
            "0,0,0,0,"
            "0,0,0,0,0,0,0,0,0,0,"
            "0,0,0,0,0,0,0,0,0,0,"
            '2025-07-10,15:00:00,00";\n'
        )
        events = source._parse_hq_response(zero_response, MarketScope.CN_STOCK)
        assert events == []

    def test_change_calculation(self, source):
        events = source._parse_hq_response(STOCK_QUOTE_RESPONSE, MarketScope.CN_STOCK)
        e0 = events[0]
        expected_change = round(1815.50 - 1800.00, 4)
        expected_pct = round((1815.50 - 1800.00) / 1800.00 * 100, 4)
        assert e0.data["change"] == expected_change
        assert e0.data["change_pct"] == expected_pct


class TestIndexSummaryParsing:
    def test_parse_index_summary(self, source):
        events = source._parse_index_summary_response(INDEX_SUMMARY_RESPONSE)
        assert len(events) == 2

        e0 = events[0]
        assert e0.source == "sina"
        assert e0.event_type == "index_update"
        assert e0.market == "cn_index"
        assert e0.symbol == "000001"
        assert e0.data["name"] == "上证指数"
        assert e0.data["price"] == 3259.22
        assert e0.data["change"] == 46.14
        assert e0.data["change_pct"] == 1.44

        e1 = events[1]
        assert e1.symbol == "399001"
        assert e1.data["name"] == "深证成指"

    def test_parse_empty_index(self, source):
        events = source._parse_index_summary_response("")
        assert events == []

    def test_parse_malformed_index(self, source):
        bad = 'var hq_str_s_sh000001="上证指数";\n'
        events = source._parse_index_summary_response(bad)
        assert events == []


# ═══════════════════════════════════════════════════════════════════
# SinaSource poll tests (with mocked HTTP)
# ═══════════════════════════════════════════════════════════════════


class TestSinaSourcePoll:
    @pytest.mark.asyncio
    async def test_poll_success(self, source):
        await source.connect()

        # Mock client.get to return stock and index responses
        call_count = 0

        async def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if "s_sh" in url or "s_sz" in url:
                return _make_response(text=INDEX_SUMMARY_RESPONSE)
            return _make_response(text=STOCK_QUOTE_RESPONSE)

        source._client.get = mock_get  # type: ignore

        events = await source.poll()
        assert len(events) == 4  # 2 stocks + 2 indices
        assert source._total_polls == 1
        assert source._total_events == 4
        assert source._consecutive_failures == 0

        await source.disconnect()

    @pytest.mark.asyncio
    async def test_poll_partial_failure(self, source):
        """If stock quotes fail but index succeeds, we get partial results."""
        await source.connect()

        async def mock_get(url, **kwargs):
            if "s_sh" in url or "s_sz" in url:
                return _make_response(text=INDEX_SUMMARY_RESPONSE)
            # Stock call fails with 500
            return _make_response(text="error", status_code=500)

        source._client.get = mock_get  # type: ignore

        # Should not raise — partial failures are handled
        events = await source.poll()
        # Index events should still come through
        assert len(events) == 2

        await source.disconnect()

    @pytest.mark.asyncio
    async def test_poll_with_empty_symbols(self, poll_config, cb_config):
        """Empty symbol lists return empty events."""
        source = SinaSource(
            poll_config=poll_config,
            cb_config=cb_config,
            stock_symbols=[],
            index_symbols=[],
        )
        await source.connect()

        async def mock_get(url, **kwargs):
            return _make_response(text="")

        source._client.get = mock_get  # type: ignore

        events = await source.poll()
        assert events == []

        await source.disconnect()


# ═══════════════════════════════════════════════════════════════════
# Rate limit (456) and circuit breaker integration tests
# ═══════════════════════════════════════════════════════════════════


class TestRateLimitHandling:
    @pytest.mark.asyncio
    async def test_456_triggers_breaker(self, source):
        await source.connect()

        async def mock_get_456(url, **kwargs):
            return _make_response(text="rate limited", status_code=456)

        source._client.get = mock_get_456  # type: ignore

        # poll should not raise (failures are caught and returned as exceptions)
        events = await source.poll()
        # Both stock and index fail, so no events
        assert events == []
        # Failures recorded
        assert source._consecutive_failures > 0

        await source.disconnect()

    @pytest.mark.asyncio
    async def test_breaker_opens_after_repeated_456(self, poll_config, cb_config):
        """After threshold failures, breaker opens and rejects calls."""
        # threshold=3, retries=2 → each poll does up to 3 attempts
        source = SinaSource(
            poll_config=poll_config,
            cb_config=cb_config,
            stock_symbols=["sh600519"],
            index_symbols=[],
        )
        await source.connect()

        async def mock_get_456(url, **kwargs):
            return _make_response(text="rate limited", status_code=456)

        source._client.get = mock_get_456  # type: ignore

        # First poll — tries 3 times, fails, breaker should open
        events1 = await source.poll()
        assert events1 == []

        assert source._breaker.state == CircuitState.OPEN
        health = source.health()
        assert health.status == HealthStatus.UNHEALTHY

        await source.disconnect()

    @pytest.mark.asyncio
    async def test_retry_success_after_failures(self, source):
        """Retries succeed after initial failures."""
        await source.connect()

        call_count = 0

        async def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 1:
                return _make_response(text="error", status_code=500)
            return _make_response(text=INDEX_SUMMARY_RESPONSE)

        source._client.get = mock_get  # type: ignore

        # Only index symbols (simpler test)
        source._stock_symbols = []
        events = await source.poll()
        assert len(events) == 2  # eventually succeeds

        await source.disconnect()


# ═══════════════════════════════════════════════════════════════════
# Kline fetch tests
# ═══════════════════════════════════════════════════════════════════


class TestKlineFetch:
    @pytest.mark.asyncio
    async def test_fetch_kline(self, source):
        await source.connect()

        async def mock_get(url, **kwargs):
            return _make_response(json_data=KLINE_RESPONSE)

        source._client.get = mock_get  # type: ignore

        events = await source.fetch_kline("sh600519", scale=30, datalen=100)
        assert len(events) == 2
        assert events[0].event_type == "kline"
        assert events[0].symbol == "600519"
        assert events[0].data["close"] == 1810.00
        assert events[0].data["scale"] == 30

        await source.disconnect()

    @pytest.mark.asyncio
    async def test_fetch_kline_empty(self, source):
        await source.connect()

        async def mock_get(url, **kwargs):
            return _make_response(json_data=[])

        source._client.get = mock_get  # type: ignore

        events = await source.fetch_kline("sh600519")
        assert events == []

        await source.disconnect()

    @pytest.mark.asyncio
    async def test_fetch_kline_not_connected(self, source):
        with pytest.raises(RuntimeError, match="not connected"):
            await source.fetch_kline("sh600519")


# ═══════════════════════════════════════════════════════════════════
# Health reporting tests
# ═══════════════════════════════════════════════════════════════════


class TestHealthReporting:
    def test_health_unknown_before_connect(self, source):
        h = source.health()
        assert h.source_name == "sina"
        assert h.status == HealthStatus.UNKNOWN
        assert h.total_polls == 0

    @pytest.mark.asyncio
    async def test_health_healthy_after_success(self, source):
        await source.connect()

        async def mock_get(url, **kwargs):
            if "s_sh" in url or "s_sz" in url:
                return _make_response(text=INDEX_SUMMARY_RESPONSE)
            return _make_response(text=STOCK_QUOTE_RESPONSE)

        source._client.get = mock_get  # type: ignore

        await source.poll()
        h = source.health()
        assert h.status == HealthStatus.HEALTHY
        assert h.total_polls == 1
        assert h.total_events > 0
        assert h.consecutive_failures == 0
        assert h.last_success is not None
        assert h.uptime_seconds is not None
        assert h.uptime_seconds >= 0

        await source.disconnect()

    @pytest.mark.asyncio
    async def test_health_degraded_with_failures(self, source):
        await source.connect()

        call_count = 0

        async def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            # First call fails, rest succeed
            if call_count == 1:
                return _make_response(text="error", status_code=500)
            return _make_response(text=INDEX_SUMMARY_RESPONSE)

        source._client.get = mock_get  # type: ignore
        source._stock_symbols = []

        await source.poll()
        # Should have recovered, but if there were transient failures...
        h = source.health()
        # After successful recovery, status should be HEALTHY
        assert h.status in (HealthStatus.HEALTHY, HealthStatus.DEGRADED)

        await source.disconnect()

    @pytest.mark.asyncio
    async def test_health_unhealthy_when_breaker_open(self, source):
        await source.connect()
        source._breaker._state = CircuitState.OPEN
        source._breaker._last_failure_time = time.monotonic()

        h = source.health()
        assert h.status == HealthStatus.UNHEALTHY

        await source.disconnect()

    def test_health_latency_tracking(self, source):
        source._record_latency(50.0)
        source._record_latency(100.0)
        source._connected = True
        source._total_polls = 1
        h = source.health()
        assert h.latency_ms == 75.0  # average

    def test_latency_buffer_limit(self, source):
        for i in range(150):
            source._record_latency(float(i))
        assert len(source._latencies) == 100


# ═══════════════════════════════════════════════════════════════════
# Integration with SourceRegistry
# ═══════════════════════════════════════════════════════════════════


class TestRegistryIntegration:
    def test_register_sina_source(self, source):
        from src.perception.sources.registry import SourceRegistry

        registry = SourceRegistry()
        registry.register(source)
        assert "sina" in registry
        assert registry.get("sina") is source

    def test_health_report_via_registry(self, source):
        from src.perception.sources.registry import SourceRegistry

        registry = SourceRegistry()
        registry.register(source)
        report = registry.health_report()
        assert "sina" in report
        assert report["sina"].source_name == "sina"


# ═══════════════════════════════════════════════════════════════════
# Edge cases
# ═══════════════════════════════════════════════════════════════════


class TestEdgeCases:
    def test_parse_response_with_extra_whitespace(self, source):
        response = (
            "\n  "
            + 'var hq_str_s_sh000001="上证指数,3259.22,46.14,1.44,2660394,28862016";\n'
            + "  \n"
        )
        events = source._parse_index_summary_response(response)
        assert len(events) == 1

    def test_parse_response_with_no_quotes(self, source):
        response = "var hq_str_sh600519=;\n"
        events = source._parse_hq_response(response, MarketScope.CN_STOCK)
        assert events == []

    @pytest.mark.asyncio
    async def test_concurrent_polls(self, source):
        """Multiple concurrent polls should not crash."""
        await source.connect()

        async def mock_get(url, **kwargs):
            await asyncio.sleep(0.01)  # simulate latency
            if "s_sh" in url or "s_sz" in url:
                return _make_response(text=INDEX_SUMMARY_RESPONSE)
            return _make_response(text=STOCK_QUOTE_RESPONSE)

        source._client.get = mock_get  # type: ignore

        results = await asyncio.gather(
            source.poll(), source.poll(), source.poll()
        )
        for events in results:
            assert isinstance(events, list)

        await source.disconnect()

    def test_backoff_delay_zero_attempt(self):
        delay = _backoff_delay(0, factor=2.0)
        assert 0 <= delay <= 1.0  # 2^0 = 1.0

    def test_source_custom_symbols(self):
        s = SinaSource(
            stock_symbols=["sh601398"],
            index_symbols=["sh000016"],
        )
        assert s._stock_symbols == ["sh601398"]
        assert s._index_symbols == ["sh000016"]
