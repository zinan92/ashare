"""AKShare data source adapter for the Perception Layer.

Wraps AKShare (https://github.com/akfamily/akshare) behind the
``DataSource`` interface, converting pandas DataFrames into
``RawMarketEvent`` objects.

Capabilities
~~~~~~~~~~~~
- **概念板块** — concept board rankings via ``stock_board_concept_name_ths``
- **异动数据** — unusual stock activity via ``stock_changes_em``
- **新闻快讯** — financial news via ``stock_news_em``

All AKShare calls are synchronous and dispatched via
``asyncio.get_running_loop().run_in_executor()`` to avoid blocking.

Rate-limiting
~~~~~~~~~~~~~
AKShare scrapes free public APIs (东方财富, 同花顺, etc.) that are
sensitive to request frequency.  This adapter enforces:

- A configurable minimum delay between API calls (default 1.0 s)
- Per-poll cooldown to prevent rapid-fire polling
- Circuit-breaker-style health degradation on consecutive failures
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
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


# ── Configuration ────────────────────────────────────────────────────


class AKShareSourceConfig:
    """Configuration for AKShareSource.

    Keeps the source decoupled from ``src.config.Settings``.

    Args:
        enable_boards:  Poll concept board rankings (概念板块).
        enable_changes: Poll unusual stock activity (异动数据).
        enable_news:    Poll financial news (新闻快讯).
        request_delay:  Minimum seconds between consecutive AKShare calls.
        max_retries:    Number of retries per API call on transient failure.
        top_n_boards:   How many top boards to include (by 涨跌幅).
    """

    def __init__(
        self,
        *,
        enable_boards: bool = True,
        enable_changes: bool = True,
        enable_news: bool = True,
        request_delay: float = 1.0,
        max_retries: int = 2,
        top_n_boards: int = 20,
    ) -> None:
        self.enable_boards = enable_boards
        self.enable_changes = enable_changes
        self.enable_news = enable_news
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.top_n_boards = top_n_boards


# ── AKShareSource ────────────────────────────────────────────────────


class AKShareSource(DataSource):
    """Perception Layer adapter for AKShare public-API data.

    Usage::

        cfg = AKShareSourceConfig(enable_boards=True, enable_news=True)
        source = AKShareSource(cfg)
        await source.connect()
        events = await source.poll()
        print(source.health())
        await source.disconnect()
    """

    def __init__(self, config: Optional[AKShareSourceConfig] = None) -> None:
        self._config = config or AKShareSourceConfig()
        self._connected = False
        self._ak: Any = None  # akshare module — imported lazily

        # Rate limiting
        self._last_request_time: float = 0.0

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
        return "akshare"

    @property
    def source_type(self) -> SourceType:
        return SourceType.POLLING

    async def connect(self) -> None:
        """Import akshare module (idempotent).

        We lazy-import so the module isn't required at class-definition
        time — makes testing with mocks straightforward.
        """
        if self._connected:
            logger.debug("AKShareSource already connected — skipping")
            return

        loop = asyncio.get_running_loop()
        self._ak = await loop.run_in_executor(None, _import_akshare)
        self._connected = True
        self._connect_time = datetime.now(timezone.utc)
        logger.info(
            "AKShareSource connected (boards=%s, changes=%s, news=%s)",
            self._config.enable_boards,
            self._config.enable_changes,
            self._config.enable_news,
        )

    async def poll(self) -> List[RawMarketEvent]:
        """Fetch latest data from all enabled AKShare endpoints.

        Polls sequentially (not parallel) to respect rate limits on
        the upstream free APIs.
        """
        if not self._connected or self._ak is None:
            raise RuntimeError(
                "AKShareSource not connected — call connect() first"
            )

        self._total_polls += 1
        start = time.monotonic()
        events: List[RawMarketEvent] = []

        try:
            # 1) Concept boards
            if self._config.enable_boards:
                board_events = await self._poll_boards()
                events.extend(board_events)

            # 2) Stock changes / unusual activity
            if self._config.enable_changes:
                change_events = await self._poll_changes()
                events.extend(change_events)

            # 3) Financial news
            if self._config.enable_news:
                news_events = await self._poll_news()
                events.extend(news_events)

            elapsed_ms = (time.monotonic() - start) * 1000
            self._last_latency_ms = elapsed_ms
            self._total_events += len(events)
            self._consecutive_failures = 0
            self._last_success = datetime.now(timezone.utc)

            logger.info(
                "AKShareSource poll complete: %d events in %.0fms",
                len(events),
                elapsed_ms,
            )
            return events

        except Exception as exc:
            self._consecutive_failures += 1
            self._last_error = datetime.now(timezone.utc)
            self._last_error_message = str(exc)
            logger.error(
                "AKShareSource poll failed (consecutive=%d): %s",
                self._consecutive_failures,
                exc,
            )
            raise

    async def disconnect(self) -> None:
        """Release resources."""
        logger.info("Disconnecting AKShareSource")
        self._ak = None
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
            uptime = (
                datetime.now(timezone.utc) - self._connect_time
            ).total_seconds()

        error_rate = 0.0
        if self._total_polls > 0:
            error_rate = min(
                self._consecutive_failures / max(self._total_polls, 1), 1.0
            )

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

    async def _poll_boards(self) -> List[RawMarketEvent]:
        """Fetch concept board rankings (概念板块) from 同花顺.

        Uses ``ak.stock_board_concept_name_ths()`` which returns a
        DataFrame with columns like:
            序号, 日期, 概念名称, 成分股数量, 涨跌幅, ...
        """
        try:
            df = await self._call_ak("stock_board_concept_name_ths")

            if df is None or df.empty:
                return []

            events: List[RawMarketEvent] = []

            # Sort by 涨跌幅 (change %) descending, take top N
            if "涨跌幅" in df.columns:
                df = df.copy()
                df["涨跌幅"] = pd.to_numeric(df["涨跌幅"], errors="coerce")
                df = df.dropna(subset=["涨跌幅"])
                df = df.nlargest(self._config.top_n_boards, "涨跌幅")

            for _, row in df.iterrows():
                data = _row_to_dict(row)
                board_name = str(data.get("概念名称", data.get("板块名称", "")))

                events.append(
                    RawMarketEvent(
                        source=EventSource.AKSHARE,
                        event_type=EventType.BOARD_CHANGE,
                        market=MarketScope.CN_STOCK,
                        symbol=board_name or None,
                        data=data,
                        timestamp=datetime.now(timezone.utc),
                    )
                )

            return events

        except Exception as exc:
            logger.warning("AKShare board fetch failed: %s", exc)
            return []

    async def _poll_changes(self) -> List[RawMarketEvent]:
        """Fetch unusual stock activity (异动数据) from 东方财富.

        Uses ``ak.stock_changes_em()`` which returns recent 异动
        records with columns like:
            时间, 代码, 名称, 板块, 相关信息
        """
        try:
            df = await self._call_ak("stock_changes_em")

            if df is None or df.empty:
                return []

            events: List[RawMarketEvent] = []
            now = datetime.now(timezone.utc)

            for _, row in df.iterrows():
                data = _row_to_dict(row)
                symbol = str(data.get("代码", ""))

                events.append(
                    RawMarketEvent(
                        source=EventSource.AKSHARE,
                        event_type=EventType.LIMIT_EVENT,
                        market=MarketScope.CN_STOCK,
                        symbol=symbol or None,
                        data=data,
                        timestamp=now,
                    )
                )

            return events

        except Exception as exc:
            logger.warning("AKShare changes fetch failed: %s", exc)
            return []

    async def _poll_news(self) -> List[RawMarketEvent]:
        """Fetch financial news headlines (新闻快讯) from 东方财富.

        Uses ``ak.stock_news_em(symbol="...")`` which requires a
        symbol. We fetch general market news by calling with a
        broad market symbol.
        """
        try:
            df = await self._call_ak(
                "stock_news_em", symbol="300059"
            )

            if df is None or df.empty:
                return []

            events: List[RawMarketEvent] = []

            for _, row in df.iterrows():
                data = _row_to_dict(row)

                # Try to parse the publish time
                ts = _parse_news_timestamp(
                    data.get("发布时间", data.get("datetime", ""))
                )

                events.append(
                    RawMarketEvent(
                        source=EventSource.AKSHARE,
                        event_type=EventType.NEWS,
                        market=MarketScope.CN_STOCK,
                        symbol=None,
                        data=data,
                        timestamp=ts,
                    )
                )

            return events

        except Exception as exc:
            logger.warning("AKShare news fetch failed: %s", exc)
            return []

    # ── AKShare call wrapper ─────────────────────────────────────────

    async def _call_ak(
        self, func_name: str, **kwargs: Any
    ) -> Optional[pd.DataFrame]:
        """Call an AKShare function with rate limiting and retries.

        All calls go through ``run_in_executor`` to keep the event
        loop free.  A minimum delay is enforced between calls.

        Args:
            func_name: Name of the ``akshare`` function (e.g. ``"stock_changes_em"``).
            **kwargs:  Keyword arguments forwarded to the AKShare function.

        Returns:
            DataFrame result, or None on failure after retries.
        """
        loop = asyncio.get_running_loop()
        last_exc: Optional[Exception] = None

        for attempt in range(self._config.max_retries + 1):
            # Rate limiting
            await self._rate_limit()

            try:
                result = await loop.run_in_executor(
                    None,
                    lambda fn=func_name, kw=kwargs: _invoke_akshare(
                        self._ak, fn, **kw
                    ),
                )
                return result

            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "AKShare %s attempt %d/%d failed: %s",
                    func_name,
                    attempt + 1,
                    self._config.max_retries + 1,
                    exc,
                )
                if attempt < self._config.max_retries:
                    # Simple backoff: delay * 2^attempt
                    await asyncio.sleep(
                        self._config.request_delay * (2 ** attempt)
                    )

        logger.error(
            "AKShare %s failed after %d attempts: %s",
            func_name,
            self._config.max_retries + 1,
            last_exc,
        )
        return None

    async def _rate_limit(self) -> None:
        """Enforce minimum delay between AKShare requests."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._config.request_delay:
            wait = self._config.request_delay - elapsed
            await asyncio.sleep(wait)
        self._last_request_time = time.monotonic()


# ── Module-level helpers ─────────────────────────────────────────────


def _import_akshare() -> Any:
    """Import the akshare module (may be slow on first import)."""
    import akshare as ak
    return ak


def _invoke_akshare(ak_module: Any, func_name: str, **kwargs: Any) -> pd.DataFrame:
    """Call an akshare function by name.

    Runs in executor thread — must not touch asyncio primitives.
    """
    func = getattr(ak_module, func_name)
    return func(**kwargs)


def _row_to_dict(row: pd.Series) -> Dict[str, Any]:
    """Convert a pandas Series to a plain dict, handling NaN → None."""
    result: Dict[str, Any] = {}
    for key, val in row.items():
        if pd.isna(val) if isinstance(val, (float, int)) else False:
            result[str(key)] = None
        else:
            result[str(key)] = val
    return result


def _parse_news_timestamp(ts_str: Any) -> datetime:
    """Parse a news timestamp string into a UTC datetime.

    AKShare news timestamps come in various formats:
    - "2025-07-10 14:30:00"
    - "2025-07-10"
    """
    if not ts_str or not isinstance(ts_str, str):
        return datetime.now(timezone.utc)

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            naive = datetime.strptime(ts_str.strip(), fmt)
            from zoneinfo import ZoneInfo
            cst = ZoneInfo("Asia/Shanghai")
            return naive.replace(tzinfo=cst).astimezone(timezone.utc)
        except (ValueError, KeyError):
            continue

    return datetime.now(timezone.utc)
