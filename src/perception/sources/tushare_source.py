"""TuShare data source adapter for the Perception Layer.

Wraps the existing ``TushareClient`` behind the ``DataSource`` interface,
converting TuShare DataFrames into ``RawMarketEvent`` objects that the
perception engine can route to detectors.

Capabilities
~~~~~~~~~~~~
- **A股日线K线** — daily OHLCV via ``tushare.pro.daily``
- **每日基本面指标** — PE / PB / market-cap via ``tushare.pro.daily_basic``
- **指数日线** — major indices via ``tushare.pro.index_daily``

Rate-limiting is delegated to ``TushareClient.RateLimiter``; this adapter
adds circuit-breaker awareness through the ``SourceHealth`` model.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from src.perception.events import (
    EventSource,
    EventType,
    MarketScope,
    RawMarketEvent,
)
from src.perception.health import HealthStatus, SourceHealth
from src.perception.sources.base import DataSource, SourceType

logger = logging.getLogger(__name__)

# ── Defaults ─────────────────────────────────────────────────────────

_DEFAULT_INDICES = [
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
]


class TuShareSourceConfig:
    """Plain configuration object for TuShareSource.

    Keeps the source decoupled from ``src.config.Settings`` — callers
    pass in whatever they need.
    """

    def __init__(
        self,
        token: str,
        *,
        points: int = 15000,
        delay: float = 0.3,
        max_retries: int = 3,
        poll_symbols: Optional[List[str]] = None,
        poll_indices: Optional[List[str]] = None,
        include_daily_basic: bool = True,
        lookback_days: int = 5,
    ) -> None:
        self.token = token
        self.points = points
        self.delay = delay
        self.max_retries = max_retries
        self.poll_symbols: List[str] = poll_symbols if poll_symbols is not None else []
        self.poll_indices: List[str] = (
            poll_indices if poll_indices is not None else list(_DEFAULT_INDICES)
        )
        self.include_daily_basic = include_daily_basic
        self.lookback_days = lookback_days


class TuShareSource(DataSource):
    """Perception-layer adapter for the TuShare Pro API.

    This adapter reuses the battle-tested ``TushareClient`` from
    ``src.services.tushare_client`` and converts its pandas output
    into ``RawMarketEvent`` instances.

    Usage::

        cfg = TuShareSourceConfig(token="...", poll_symbols=["000001", "600519"])
        source = TuShareSource(cfg)
        await source.connect()
        events = await source.poll()
        print(source.health())
        await source.disconnect()
    """

    def __init__(self, config: TuShareSourceConfig) -> None:
        self._config = config
        self._client: Any = None  # TushareClient — set in connect()
        self._connected = False

        # Health tracking
        self._total_polls = 0
        self._total_events = 0
        self._consecutive_failures = 0
        self._last_success: Optional[datetime] = None
        self._last_error: Optional[datetime] = None
        self._last_error_message: Optional[str] = None
        self._last_latency_ms: Optional[float] = None
        self._connect_time: Optional[datetime] = None

    # ── DataSource interface ─────────────────────────────────────────

    @property
    def name(self) -> str:
        return "tushare"

    @property
    def source_type(self) -> SourceType:
        return SourceType.POLLING

    async def connect(self) -> None:
        """Initialize the TushareClient (idempotent)."""
        if self._connected:
            logger.debug("TuShareSource already connected — skipping")
            return

        from src.services.tushare_client import TushareClient

        logger.info(
            "Connecting TuShareSource (points=%d, delay=%.1fs)",
            self._config.points,
            self._config.delay,
        )

        # TushareClient init is synchronous — run in executor to avoid
        # blocking the event loop.
        loop = asyncio.get_running_loop()
        self._client = await loop.run_in_executor(
            None,
            lambda: TushareClient(
                token=self._config.token,
                points=self._config.points,
                delay=self._config.delay,
                max_retries=self._config.max_retries,
            ),
        )

        self._connected = True
        self._connect_time = datetime.now(timezone.utc)
        logger.info("TuShareSource connected")

    async def poll(self) -> List[RawMarketEvent]:
        """Fetch latest data and return as RawMarketEvents.

        Polls three categories in order:
        1. Daily K-line for configured symbols
        2. Daily basic (fundamentals) for configured symbols
        3. Index daily for configured indices
        """
        if not self._connected or self._client is None:
            raise RuntimeError("TuShareSource is not connected — call connect() first")

        self._total_polls += 1
        start = time.monotonic()

        try:
            events: List[RawMarketEvent] = []

            trade_date = await self._get_latest_trade_date()

            # 1) Daily K-line
            if self._config.poll_symbols:
                kline_events = await self._poll_daily_kline(trade_date)
                events.extend(kline_events)

            # 2) Daily basic (fundamentals)
            if self._config.include_daily_basic and self._config.poll_symbols:
                basic_events = await self._poll_daily_basic(trade_date)
                events.extend(basic_events)

            # 3) Index daily
            if self._config.poll_indices:
                index_events = await self._poll_index_daily(trade_date)
                events.extend(index_events)

            elapsed_ms = (time.monotonic() - start) * 1000
            self._last_latency_ms = elapsed_ms
            self._total_events += len(events)
            self._consecutive_failures = 0
            self._last_success = datetime.now(timezone.utc)

            logger.info(
                "TuShareSource poll complete: %d events in %.0fms",
                len(events),
                elapsed_ms,
            )
            return events

        except Exception as exc:
            self._consecutive_failures += 1
            self._last_error = datetime.now(timezone.utc)
            self._last_error_message = str(exc)
            logger.error(
                "TuShareSource poll failed (consecutive=%d): %s",
                self._consecutive_failures,
                exc,
            )
            raise

    async def disconnect(self) -> None:
        """Release resources."""
        logger.info("Disconnecting TuShareSource")
        self._client = None
        self._connected = False

    def health(self) -> SourceHealth:
        """Return current health snapshot."""
        if not self._connected:
            status = HealthStatus.UNKNOWN
        elif self._consecutive_failures >= 5:
            status = HealthStatus.UNHEALTHY
        elif self._consecutive_failures >= 2:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.HEALTHY

        uptime: Optional[float] = None
        if self._connect_time is not None:
            uptime = (datetime.now(timezone.utc) - self._connect_time).total_seconds()

        error_rate = 0.0
        if self._total_polls > 0:
            # Approximate: consecutive failures / recent window
            error_rate = min(self._consecutive_failures / max(self._total_polls, 1), 1.0)

        return SourceHealth(
            source_name=self.name,
            status=status,
            latency_ms=self._last_latency_ms,
            error_rate=error_rate,
            last_success=self._last_success,
            last_error=self._last_error,
            last_error_message=self._last_error_message,
            consecutive_failures=self._consecutive_failures,
            total_polls=self._total_polls,
            total_events=self._total_events,
            uptime_seconds=uptime,
        )

    # ── Internal polling helpers ─────────────────────────────────────

    async def _get_latest_trade_date(self) -> str:
        """Get the latest trading date from TuShare (offloaded to executor)."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._client.get_latest_trade_date
        )

    async def _poll_daily_kline(self, trade_date: str) -> List[RawMarketEvent]:
        """Fetch daily K-line for each configured symbol."""
        events: List[RawMarketEvent] = []
        loop = asyncio.get_running_loop()

        for symbol in self._config.poll_symbols:
            try:
                ts_code = self._client.normalize_ts_code(symbol)
                df: pd.DataFrame = await loop.run_in_executor(
                    None,
                    lambda code=ts_code: self._client.fetch_daily(
                        ts_code=code, trade_date=trade_date
                    ),
                )

                for _, row in df.iterrows():
                    events.append(self._row_to_kline_event(row, symbol))

            except Exception as exc:
                logger.warning("Failed to fetch kline for %s: %s", symbol, exc)

        return events

    async def _poll_daily_basic(self, trade_date: str) -> List[RawMarketEvent]:
        """Fetch daily basic (fundamentals) in one batch call."""
        events: List[RawMarketEvent] = []
        loop = asyncio.get_running_loop()

        try:
            df: pd.DataFrame = await loop.run_in_executor(
                None,
                lambda: self._client.fetch_daily_basic(trade_date=trade_date),
            )

            if df.empty:
                return events

            # Filter to only our configured symbols
            symbol_set = {
                self._client.normalize_ts_code(s)
                for s in self._config.poll_symbols
            }
            df_filtered = df[df["ts_code"].isin(symbol_set)]

            for _, row in df_filtered.iterrows():
                symbol = self._client.denormalize_ts_code(row["ts_code"])
                events.append(self._row_to_fundamental_event(row, symbol))

        except Exception as exc:
            logger.warning("Failed to fetch daily_basic: %s", exc)

        return events

    async def _poll_index_daily(self, trade_date: str) -> List[RawMarketEvent]:
        """Fetch daily data for configured indices."""
        events: List[RawMarketEvent] = []
        loop = asyncio.get_running_loop()

        for index_code in self._config.poll_indices:
            try:
                df: pd.DataFrame = await loop.run_in_executor(
                    None,
                    lambda code=index_code: self._client.fetch_index_daily(
                        ts_code=code,
                        start_date=trade_date,
                        end_date=trade_date,
                    ),
                )

                for _, row in df.iterrows():
                    events.append(self._row_to_index_event(row, index_code))

            except Exception as exc:
                logger.warning("Failed to fetch index %s: %s", index_code, exc)

        return events

    # ── Row → Event converters ───────────────────────────────────────

    @staticmethod
    def _parse_trade_date(trade_date_str: str) -> datetime:
        """Parse Tushare YYYYMMDD date string to UTC datetime."""
        return datetime.strptime(str(trade_date_str), "%Y%m%d").replace(
            tzinfo=timezone.utc
        )

    def _row_to_kline_event(
        self, row: pd.Series, symbol: str
    ) -> RawMarketEvent:
        """Convert a daily K-line DataFrame row to a RawMarketEvent."""
        data: Dict[str, Any] = {
            "open": _safe_float(row.get("open")),
            "high": _safe_float(row.get("high")),
            "low": _safe_float(row.get("low")),
            "close": _safe_float(row.get("close")),
            "volume": _safe_float(row.get("vol")),
            "amount": _safe_float(row.get("amount")),
        }
        # Include optional fields if present
        for field in ("pre_close", "change", "pct_chg"):
            val = row.get(field)
            if val is not None and pd.notna(val):
                data[field] = float(val)

        return RawMarketEvent(
            source=EventSource.TUSHARE,
            event_type=EventType.KLINE,
            market=MarketScope.CN_STOCK,
            symbol=symbol,
            data=data,
            timestamp=self._parse_trade_date(row["trade_date"]),
        )

    def _row_to_fundamental_event(
        self, row: pd.Series, symbol: str
    ) -> RawMarketEvent:
        """Convert a daily_basic row to a RawMarketEvent."""
        data: Dict[str, Any] = {}
        for field in (
            "close",
            "turnover_rate",
            "volume_ratio",
            "pe",
            "pe_ttm",
            "pb",
            "ps",
            "ps_ttm",
            "total_mv",
            "circ_mv",
        ):
            val = row.get(field)
            if val is not None and pd.notna(val):
                data[field] = float(val)

        return RawMarketEvent(
            source=EventSource.TUSHARE,
            event_type=EventType.EARNINGS,
            market=MarketScope.CN_STOCK,
            symbol=symbol,
            data=data,
            timestamp=self._parse_trade_date(row["trade_date"]),
        )

    def _row_to_index_event(
        self, row: pd.Series, index_code: str
    ) -> RawMarketEvent:
        """Convert an index_daily row to a RawMarketEvent."""
        data: Dict[str, Any] = {
            "open": _safe_float(row.get("open")),
            "high": _safe_float(row.get("high")),
            "low": _safe_float(row.get("low")),
            "close": _safe_float(row.get("close")),
            "volume": _safe_float(row.get("vol")),
            "amount": _safe_float(row.get("amount")),
        }
        for field in ("pre_close", "change", "pct_chg"):
            val = row.get(field)
            if val is not None and pd.notna(val):
                data[field] = float(val)

        return RawMarketEvent(
            source=EventSource.TUSHARE,
            event_type=EventType.INDEX_UPDATE,
            market=MarketScope.CN_INDEX,
            symbol=index_code,
            data=data,
            timestamp=self._parse_trade_date(row["trade_date"]),
        )


# ── Helpers ──────────────────────────────────────────────────────────


def _safe_float(val: Any) -> Optional[float]:
    """Convert a value to float, returning None for NaN / None."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (ValueError, TypeError):
        return None
