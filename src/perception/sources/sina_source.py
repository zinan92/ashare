"""Sina Finance data source adapter for the Perception Layer.

Wraps existing Sina API functionality into the DataSource interface:
- A股实时行情 (real-time stock quotes via hq.sinajs.cn)
- 指数K线 (index kline data via CN_MarketData API)
- 实时报价 (real-time index quotes)

Includes:
- Circuit breaker (Sina had 456 rate-limit errors that blocked uvicorn)
- Exponential backoff with jitter
- asyncio.to_thread() for sync HTTP calls
- Per-request timeout enforcement
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

import httpx

from src.perception.config import CircuitBreakerConfig, SourcePollConfig
from src.perception.events import (
    EventSource,
    EventType,
    MarketScope,
    RawMarketEvent,
)
from src.perception.health import HealthStatus, SourceHealth
from src.perception.sources.base import DataSource, SourceType

logger = logging.getLogger(__name__)


# ── Circuit Breaker ──────────────────────────────────────────────────


class CircuitState(str, Enum):
    """Three-state circuit breaker."""

    CLOSED = "closed"  # normal operation
    OPEN = "open"  # failing, reject requests
    HALF_OPEN = "half_open"  # probing recovery


class CircuitBreaker:
    """Circuit breaker to protect against cascading Sina API failures.

    When consecutive failures reach *failure_threshold*, the breaker
    opens and rejects all calls for *recovery_timeout* seconds. After
    that it enters half-open and allows a single probe request.

    This prevents 456 rate-limit errors from blocking the event loop.
    """

    def __init__(self, config: CircuitBreakerConfig) -> None:
        self._config = config
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._half_open_calls = 0
        self._last_failure_time: Optional[float] = None
        self._last_state_change: float = time.monotonic()

    @property
    def state(self) -> CircuitState:
        """Current state, auto-transitioning OPEN → HALF_OPEN on timeout."""
        if self._state == CircuitState.OPEN and self._last_failure_time:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self._config.recovery_timeout_seconds:
                self._transition(CircuitState.HALF_OPEN)
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count

    def allow_request(self) -> bool:
        """Check if a request is allowed under current breaker state."""
        state = self.state  # triggers auto-transition
        if state == CircuitState.CLOSED:
            return True
        if state == CircuitState.HALF_OPEN:
            return self._half_open_calls < self._config.half_open_max_calls
        return False  # OPEN

    def record_success(self) -> None:
        """Record a successful call — resets breaker to CLOSED."""
        if self._state in (CircuitState.HALF_OPEN, CircuitState.CLOSED):
            self._failure_count = 0
            self._half_open_calls = 0
            self._transition(CircuitState.CLOSED)

    def record_failure(self) -> None:
        """Record a failed call — may trip the breaker to OPEN."""
        self._failure_count += 1
        self._last_failure_time = time.monotonic()

        if self._state == CircuitState.HALF_OPEN:
            # Probe failed → back to OPEN
            self._transition(CircuitState.OPEN)
        elif self._failure_count >= self._config.failure_threshold:
            self._transition(CircuitState.OPEN)

    def _transition(self, new_state: CircuitState) -> None:
        if self._state != new_state:
            logger.info(
                "Sina circuit breaker: %s → %s (failures=%d)",
                self._state.value,
                new_state.value,
                self._failure_count,
            )
            self._state = new_state
            self._last_state_change = time.monotonic()
            if new_state == CircuitState.HALF_OPEN:
                self._half_open_calls = 0

    def reset(self) -> None:
        """Force-reset the breaker (e.g. on reconnect)."""
        self._failure_count = 0
        self._half_open_calls = 0
        self._last_failure_time = None
        self._transition(CircuitState.CLOSED)


# ── Retry / Backoff ──────────────────────────────────────────────────


def _backoff_delay(attempt: int, factor: float = 2.0, max_delay: float = 60.0) -> float:
    """Exponential backoff with full jitter."""
    delay = min(factor ** attempt, max_delay)
    return random.uniform(0, delay)


# ── Default watchlist ────────────────────────────────────────────────

# Major A-share indices for default polling
DEFAULT_INDEX_SYMBOLS = [
    "sh000001",  # 上证指数
    "sz399001",  # 深证成指
    "sz399006",  # 创业板指
    "sh000300",  # 沪深300
    "sh000016",  # 上证50
    "sh000905",  # 中证500
    "sh000688",  # 科创50
    "sz399852",  # 中证1000
]

# Common stock tickers (can be overridden via config)
DEFAULT_STOCK_SYMBOLS = [
    "sh600519",  # 贵州茅台
    "sz000001",  # 平安银行
    "sh601318",  # 中国平安
    "sz000858",  # 五粮液
    "sh600036",  # 招商银行
]

# Sina API endpoints
SINA_HQ_URL = "https://hq.sinajs.cn/list={symbols}"
SINA_KLINE_URL = (
    "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php"
    "/CN_MarketData.getKLineData"
)

# HTTP headers to mimic browser (avoids Sina blocking)
SINA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.sina.com.cn/",
}


# ── SinaSource ───────────────────────────────────────────────────────


class SinaSource(DataSource):
    """Perception Layer adapter for Sina Finance APIs.

    Capabilities:
        - Real-time stock quotes (A股实时行情)
        - Index kline data (指数K线)
        - Real-time index quotes (实时报价)

    All sync HTTP calls are dispatched via ``asyncio.to_thread()``
    to avoid blocking the event loop.

    Configuration:
        poll_config: polling interval, timeout, retry settings
        cb_config:   circuit breaker thresholds
        stock_symbols: list of Sina-format stock symbols to poll
        index_symbols: list of Sina-format index symbols to poll

    Example::

        source = SinaSource(
            poll_config=SourcePollConfig(source_name="sina", poll_interval_seconds=5),
            cb_config=CircuitBreakerConfig(failure_threshold=3),
        )
        await source.connect()
        events = await source.poll()
    """

    def __init__(
        self,
        poll_config: Optional[SourcePollConfig] = None,
        cb_config: Optional[CircuitBreakerConfig] = None,
        stock_symbols: Optional[List[str]] = None,
        index_symbols: Optional[List[str]] = None,
    ) -> None:
        self._poll_config = poll_config or SourcePollConfig(
            source_name="sina",
            poll_interval_seconds=5.0,
            timeout_seconds=10.0,
            max_retries=3,
            backoff_factor=2.0,
        )
        self._cb_config = cb_config or CircuitBreakerConfig(
            failure_threshold=5,
            recovery_timeout_seconds=300.0,
            half_open_max_calls=1,
        )
        self._breaker = CircuitBreaker(self._cb_config)

        self._stock_symbols = stock_symbols or DEFAULT_STOCK_SYMBOLS
        self._index_symbols = index_symbols or DEFAULT_INDEX_SYMBOLS

        # HTTP client (created on connect)
        self._client: Optional[httpx.AsyncClient] = None

        # Metrics
        self._connected = False
        self._connected_at: Optional[datetime] = None
        self._total_polls = 0
        self._total_events = 0
        self._last_success: Optional[datetime] = None
        self._last_error: Optional[datetime] = None
        self._last_error_message: Optional[str] = None
        self._consecutive_failures = 0
        self._latencies: List[float] = []  # last N latencies

    # ── DataSource interface ─────────────────────────────────────────

    @property
    def name(self) -> str:
        return "sina"

    @property
    def source_type(self) -> SourceType:
        return SourceType.POLLING

    async def connect(self) -> None:
        """Create HTTP client. Idempotent."""
        if self._client is not None:
            return
        self._client = httpx.AsyncClient(
            headers=SINA_HEADERS,
            timeout=httpx.Timeout(self._poll_config.timeout_seconds),
            follow_redirects=True,
        )
        self._connected = True
        self._connected_at = datetime.now(timezone.utc)
        self._breaker.reset()
        logger.info(
            "SinaSource connected (stocks=%d, indices=%d)",
            len(self._stock_symbols),
            len(self._index_symbols),
        )

    async def disconnect(self) -> None:
        """Close HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        self._connected = False
        logger.info("SinaSource disconnected")

    async def poll(self) -> List[RawMarketEvent]:
        """Fetch latest quotes from Sina and return as RawMarketEvents.

        Fetches both stock quotes and index quotes in parallel.
        """
        if not self._connected or self._client is None:
            raise RuntimeError("SinaSource not connected — call connect() first")

        self._total_polls += 1
        events: List[RawMarketEvent] = []

        # Parallel fetch of stock quotes and index quotes
        stock_task = self._fetch_with_retry(self._fetch_stock_quotes)
        index_task = self._fetch_with_retry(self._fetch_index_quotes)

        stock_events, index_events = await asyncio.gather(
            stock_task, index_task, return_exceptions=True
        )

        if isinstance(stock_events, list):
            events.extend(stock_events)
        elif isinstance(stock_events, Exception):
            logger.warning("Stock quotes failed: %s", stock_events)

        if isinstance(index_events, list):
            events.extend(index_events)
        elif isinstance(index_events, Exception):
            logger.warning("Index quotes failed: %s", index_events)

        self._total_events += len(events)
        return events

    def health(self) -> SourceHealth:
        """Return current health snapshot."""
        # Determine status from breaker state and metrics
        breaker_state = self._breaker.state
        if breaker_state == CircuitState.OPEN:
            status = HealthStatus.UNHEALTHY
        elif breaker_state == CircuitState.HALF_OPEN:
            status = HealthStatus.DEGRADED
        elif self._consecutive_failures > 0:
            status = HealthStatus.DEGRADED
        elif self._connected and self._total_polls > 0:
            status = HealthStatus.HEALTHY
        else:
            status = HealthStatus.UNKNOWN

        avg_latency = (
            sum(self._latencies) / len(self._latencies)
            if self._latencies
            else None
        )

        error_rate = 0.0
        if self._total_polls > 0:
            error_rate = min(
                self._consecutive_failures / max(self._total_polls, 1), 1.0
            )

        uptime = None
        if self._connected_at:
            uptime = (datetime.now(timezone.utc) - self._connected_at).total_seconds()

        return SourceHealth(
            source_name=self.name,
            status=status,
            latency_ms=avg_latency,
            error_rate=error_rate,
            last_success=self._last_success,
            last_error=self._last_error,
            last_error_message=self._last_error_message,
            consecutive_failures=self._consecutive_failures,
            total_polls=self._total_polls,
            total_events=self._total_events,
            uptime_seconds=uptime,
        )

    # ── Internal: fetch methods ──────────────────────────────────────

    async def _fetch_stock_quotes(self) -> List[RawMarketEvent]:
        """Fetch real-time A股 quotes from hq.sinajs.cn."""
        if not self._stock_symbols:
            return []

        symbols_str = ",".join(self._stock_symbols)
        url = SINA_HQ_URL.format(symbols=symbols_str)

        start = time.monotonic()
        response = await self._client.get(url)  # type: ignore[union-attr]
        latency_ms = (time.monotonic() - start) * 1000

        self._record_latency(latency_ms)

        if response.status_code == 456:
            raise SinaRateLimitError(
                f"Sina returned 456 rate limit (latency={latency_ms:.0f}ms)"
            )
        response.raise_for_status()

        return self._parse_hq_response(response.text, MarketScope.CN_STOCK)

    async def _fetch_index_quotes(self) -> List[RawMarketEvent]:
        """Fetch real-time index quotes from hq.sinajs.cn."""
        if not self._index_symbols:
            return []

        # Index quotes use the s_ prefix for summary format
        summary_symbols = [
            f"s_{s}" if not s.startswith("s_") else s
            for s in self._index_symbols
        ]
        symbols_str = ",".join(summary_symbols)
        url = SINA_HQ_URL.format(symbols=symbols_str)

        start = time.monotonic()
        response = await self._client.get(url)  # type: ignore[union-attr]
        latency_ms = (time.monotonic() - start) * 1000

        self._record_latency(latency_ms)

        if response.status_code == 456:
            raise SinaRateLimitError(
                f"Sina returned 456 rate limit on index quotes (latency={latency_ms:.0f}ms)"
            )
        response.raise_for_status()

        return self._parse_index_summary_response(response.text)

    async def fetch_kline(
        self,
        symbol: str,
        scale: int = 30,
        datalen: int = 100,
    ) -> List[RawMarketEvent]:
        """Fetch kline data for a single symbol.

        This is an on-demand method (not called during regular polling)
        that wraps the SinaKlineProvider functionality.

        Args:
            symbol: Sina-format symbol (e.g. 'sh600519')
            scale:  Kline period in minutes (5, 15, 30, 60)
            datalen: Number of bars to fetch (max 1023)
        """
        if not self._connected or self._client is None:
            raise RuntimeError("SinaSource not connected — call connect() first")

        async def _do_fetch() -> List[RawMarketEvent]:
            params = {
                "symbol": symbol,
                "scale": scale,
                "ma": "no",
                "datalen": min(datalen, 1023),
            }
            start = time.monotonic()
            response = await self._client.get(SINA_KLINE_URL, params=params)  # type: ignore[union-attr]
            latency_ms = (time.monotonic() - start) * 1000
            self._record_latency(latency_ms)

            if response.status_code == 456:
                raise SinaRateLimitError(
                    f"Sina 456 on kline {symbol} (latency={latency_ms:.0f}ms)"
                )
            response.raise_for_status()

            data = response.json()
            if not data:
                return []

            # Extract raw ticker from sina symbol
            raw_ticker = re.sub(r"^(sh|sz|bj)", "", symbol.lower())
            events: List[RawMarketEvent] = []

            for bar in data:
                events.append(
                    RawMarketEvent(
                        source=EventSource.SINA,
                        event_type=EventType.KLINE,
                        market=MarketScope.CN_STOCK,
                        symbol=raw_ticker,
                        data={
                            "open": float(bar.get("open", 0)),
                            "high": float(bar.get("high", 0)),
                            "low": float(bar.get("low", 0)),
                            "close": float(bar.get("close", 0)),
                            "volume": int(float(bar.get("volume", 0))),
                            "scale": scale,
                            "datetime": bar.get("day", ""),
                        },
                        timestamp=_parse_bar_timestamp(bar.get("day", "")),
                    )
                )
            return events

        return await self._fetch_with_retry(_do_fetch)

    # ── Parsing helpers ──────────────────────────────────────────────

    def _parse_hq_response(
        self, text: str, market: MarketScope
    ) -> List[RawMarketEvent]:
        """Parse hq.sinajs.cn stock-quote response.

        Format per line:
        var hq_str_sh600519="贵州茅台,1800.00,...,date,time,...";
        Fields: name,open,prev_close,price,high,low,bid,ask,volume,amount,
                b1_vol,b1,b2_vol,b2,b3_vol,b3,b4_vol,b4,b5_vol,b5,
                a1_vol,a1,a2_vol,a2,a3_vol,a3,a4_vol,a4,a5_vol,a5,
                date,time,status
        """
        events: List[RawMarketEvent] = []
        for line in text.strip().split("\n"):
            line = line.strip()
            if not line:
                continue

            # Extract symbol: var hq_str_sh600519="..."
            sym_match = re.search(r"hq_str_(\w+)=", line)
            quote_match = re.search(r'"([^"]*)"', line)
            if not sym_match or not quote_match:
                continue

            sina_symbol = sym_match.group(1)
            raw_ticker = re.sub(r"^(sh|sz|bj)", "", sina_symbol)
            parts = quote_match.group(1).split(",")

            if len(parts) < 32:
                logger.debug("Skipping incomplete quote for %s (%d fields)", sina_symbol, len(parts))
                continue

            try:
                price = float(parts[3]) if parts[3] else 0.0
                # Skip if price is 0 (market closed / no data)
                if price == 0:
                    continue

                event_data: Dict[str, Any] = {
                    "name": parts[0],
                    "open": float(parts[1]) if parts[1] else 0.0,
                    "prev_close": float(parts[2]) if parts[2] else 0.0,
                    "price": price,
                    "high": float(parts[4]) if parts[4] else 0.0,
                    "low": float(parts[5]) if parts[5] else 0.0,
                    "bid": float(parts[6]) if parts[6] else 0.0,
                    "ask": float(parts[7]) if parts[7] else 0.0,
                    "volume": int(float(parts[8])) if parts[8] else 0,
                    "amount": float(parts[9]) if parts[9] else 0.0,
                    "date": parts[30] if len(parts) > 30 else "",
                    "time": parts[31] if len(parts) > 31 else "",
                }

                # Calculate change
                prev_close = event_data["prev_close"]
                if prev_close > 0:
                    event_data["change"] = round(price - prev_close, 4)
                    event_data["change_pct"] = round(
                        (price - prev_close) / prev_close * 100, 4
                    )

                # Parse timestamp from date+time fields
                ts = _parse_quote_timestamp(event_data["date"], event_data["time"])

                events.append(
                    RawMarketEvent(
                        source=EventSource.SINA,
                        event_type=EventType.PRICE_UPDATE,
                        market=market,
                        symbol=raw_ticker,
                        data=event_data,
                        timestamp=ts,
                    )
                )
            except (ValueError, IndexError) as exc:
                logger.debug("Failed to parse quote for %s: %s", sina_symbol, exc)
                continue

        return events

    def _parse_index_summary_response(self, text: str) -> List[RawMarketEvent]:
        """Parse index summary response.

        Format: var hq_str_s_sh000001="上证指数,3259.22,46.14,1.44,2660394,28862016";
        Fields: name, price, change, change_pct, volume(手), amount(万元)
        """
        events: List[RawMarketEvent] = []
        for line in text.strip().split("\n"):
            line = line.strip()
            if not line:
                continue

            sym_match = re.search(r"hq_str_s_(\w+)=", line)
            quote_match = re.search(r'"([^"]*)"', line)
            if not sym_match or not quote_match:
                continue

            sina_symbol = sym_match.group(1)
            raw_ticker = re.sub(r"^(sh|sz|bj)", "", sina_symbol)
            parts = quote_match.group(1).split(",")

            if len(parts) < 6:
                continue

            try:
                price = float(parts[1]) if parts[1] else 0.0
                if price == 0:
                    continue

                event_data: Dict[str, Any] = {
                    "name": parts[0],
                    "price": price,
                    "change": float(parts[2]) if parts[2] else 0.0,
                    "change_pct": float(parts[3]) if parts[3] else 0.0,
                    "volume": int(parts[4]) if parts[4] else 0,
                    "amount": float(parts[5]) if parts[5] else 0.0,
                }

                events.append(
                    RawMarketEvent(
                        source=EventSource.SINA,
                        event_type=EventType.INDEX_UPDATE,
                        market=MarketScope.CN_INDEX,
                        symbol=raw_ticker,
                        data=event_data,
                        timestamp=datetime.now(timezone.utc),
                    )
                )
            except (ValueError, IndexError) as exc:
                logger.debug("Failed to parse index %s: %s", sina_symbol, exc)
                continue

        return events

    # ── Retry / Circuit Breaker logic ────────────────────────────────

    async def _fetch_with_retry(self, fetch_fn) -> List[RawMarketEvent]:
        """Execute *fetch_fn* with circuit breaker and exponential backoff.

        Raises SinaCircuitOpenError if the breaker is open.
        """
        if not self._breaker.allow_request():
            raise SinaCircuitOpenError(
                f"Sina circuit breaker OPEN (failures={self._breaker.failure_count})"
            )

        last_exc: Optional[Exception] = None
        max_retries = self._poll_config.max_retries

        for attempt in range(max_retries + 1):
            if attempt > 0 and not self._breaker.allow_request():
                raise SinaCircuitOpenError(
                    f"Circuit opened during retry (attempt {attempt})"
                )

            try:
                events = await fetch_fn()
                # Success
                self._breaker.record_success()
                self._consecutive_failures = 0
                self._last_success = datetime.now(timezone.utc)
                return events

            except SinaRateLimitError as exc:
                # 456 is a signal to back off hard
                self._breaker.record_failure()
                self._consecutive_failures += 1
                self._last_error = datetime.now(timezone.utc)
                self._last_error_message = str(exc)
                last_exc = exc
                logger.warning(
                    "Sina 456 rate limit (attempt %d/%d): %s",
                    attempt + 1,
                    max_retries + 1,
                    exc,
                )

            except httpx.HTTPStatusError as exc:
                self._breaker.record_failure()
                self._consecutive_failures += 1
                self._last_error = datetime.now(timezone.utc)
                self._last_error_message = f"HTTP {exc.response.status_code}"
                last_exc = exc
                logger.warning(
                    "Sina HTTP error %d (attempt %d/%d)",
                    exc.response.status_code,
                    attempt + 1,
                    max_retries + 1,
                )

            except (httpx.RequestError, httpx.TimeoutException) as exc:
                self._breaker.record_failure()
                self._consecutive_failures += 1
                self._last_error = datetime.now(timezone.utc)
                self._last_error_message = str(exc)
                last_exc = exc
                logger.warning(
                    "Sina request error (attempt %d/%d): %s",
                    attempt + 1,
                    max_retries + 1,
                    exc,
                )

            # Backoff before next retry (unless last attempt)
            if attempt < max_retries:
                delay = _backoff_delay(
                    attempt, factor=self._poll_config.backoff_factor
                )
                logger.debug("Backing off %.2fs before retry", delay)
                await asyncio.sleep(delay)

        # All retries exhausted
        raise SinaFetchError(
            f"All {max_retries + 1} attempts failed for Sina fetch"
        ) from last_exc

    # ── Helpers ──────────────────────────────────────────────────────

    def _record_latency(self, ms: float) -> None:
        """Track last 100 latency measurements."""
        self._latencies.append(ms)
        if len(self._latencies) > 100:
            self._latencies = self._latencies[-100:]


# ── Timestamp helpers ────────────────────────────────────────────────


def _parse_quote_timestamp(date_str: str, time_str: str) -> datetime:
    """Parse Sina quote date/time into UTC datetime.

    Args:
        date_str: e.g. "2025-07-10"
        time_str: e.g. "15:00:03"
    """
    try:
        naive = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
        # Sina times are CST (UTC+8)
        from zoneinfo import ZoneInfo

        cst = ZoneInfo("Asia/Shanghai")
        aware = naive.replace(tzinfo=cst)
        return aware.astimezone(timezone.utc)
    except (ValueError, KeyError):
        return datetime.now(timezone.utc)


def _parse_bar_timestamp(day_str: str) -> datetime:
    """Parse kline bar timestamp (e.g. '2025-07-10 14:30:00')."""
    try:
        naive = datetime.strptime(day_str, "%Y-%m-%d %H:%M:%S")
        from zoneinfo import ZoneInfo

        cst = ZoneInfo("Asia/Shanghai")
        return naive.replace(tzinfo=cst).astimezone(timezone.utc)
    except (ValueError, KeyError):
        try:
            naive = datetime.strptime(day_str, "%Y-%m-%d")
            from zoneinfo import ZoneInfo

            cst = ZoneInfo("Asia/Shanghai")
            return naive.replace(tzinfo=cst).astimezone(timezone.utc)
        except (ValueError, KeyError):
            return datetime.now(timezone.utc)


# ── Custom exceptions ────────────────────────────────────────────────


class SinaSourceError(Exception):
    """Base exception for SinaSource errors."""


class SinaRateLimitError(SinaSourceError):
    """Sina returned HTTP 456 (rate limited)."""


class SinaCircuitOpenError(SinaSourceError):
    """Circuit breaker is open — calls are rejected."""


class SinaFetchError(SinaSourceError):
    """All retry attempts exhausted."""
