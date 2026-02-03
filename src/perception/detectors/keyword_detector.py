"""Keyword-based event detector.

Scans news / social RawMarketEvents for keyword matches and emits
SENTIMENT-typed UnifiedSignals.  Supports:

* Priority-tiered keyword rules (urgent / high / normal)
* Chinese + English keywords
* Watchlist stock ticker/name matching (higher confidence)
* Sector keyword association
* Dynamic rule reconfiguration at runtime (no restart)
* Merging multiple hits from the same event into a single signal
  (highest priority wins)
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import IntEnum
from typing import Any, Dict, List, Optional, Set, Tuple

from src.perception.detectors.base import Detector
from src.perception.events import EventType, RawMarketEvent
from src.perception.signals import Direction, Market, SignalType, UnifiedSignal


# ── Priority ─────────────────────────────────────────────────────────


class Priority(IntEnum):
    """Keyword rule priority (higher value → stronger signal)."""

    NORMAL = 1
    HIGH = 2
    URGENT = 3


# Mapping: priority → signal strength
PRIORITY_STRENGTH: Dict[Priority, float] = {
    Priority.NORMAL: 0.4,
    Priority.HIGH: 0.7,
    Priority.URGENT: 0.9,
}


# ── Keyword rule ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class KeywordRule:
    """A single keyword matching rule.

    Attributes:
        keyword:   The literal string to search for (case-insensitive for
                   ASCII; exact for CJK since they have no case).
        priority:  Determines signal strength on match.
        sector:    Optional sector label attached to the match.
    """

    keyword: str
    priority: Priority = Priority.NORMAL
    sector: Optional[str] = None


# ── Default rules ────────────────────────────────────────────────────

_DEFAULT_RULES: List[KeywordRule] = [
    # Urgent — AI / frontier tech
    KeywordRule("DeepSeek", Priority.URGENT, "AI"),
    KeywordRule("OpenAI", Priority.URGENT, "AI"),
    KeywordRule("ChatGPT", Priority.URGENT, "AI"),
    KeywordRule("Claude", Priority.URGENT, "AI"),
    KeywordRule("Anthropic", Priority.URGENT, "AI"),
    KeywordRule("大模型", Priority.URGENT, "AI"),
    KeywordRule("人工智能", Priority.URGENT, "AI"),
    # High — semiconductors
    KeywordRule("半导体", Priority.HIGH, "半导体"),
    KeywordRule("芯片", Priority.HIGH, "半导体"),
    KeywordRule("NVIDIA", Priority.HIGH, "半导体"),
    KeywordRule("ASML", Priority.HIGH, "半导体"),
    KeywordRule("光刻机", Priority.HIGH, "半导体"),
    KeywordRule("GPU", Priority.HIGH, "半导体"),
    # High — policy / macro
    KeywordRule("政策", Priority.HIGH, "政策"),
    KeywordRule("央行", Priority.HIGH, "政策"),
    KeywordRule("降息", Priority.HIGH, "政策"),
    KeywordRule("降准", Priority.HIGH, "政策"),
    KeywordRule("证监会", Priority.HIGH, "政策"),
    KeywordRule("国务院", Priority.HIGH, "政策"),
    # Normal — new energy
    KeywordRule("新能源", Priority.NORMAL, "新能源"),
    KeywordRule("特斯拉", Priority.NORMAL, "新能源"),
    KeywordRule("Tesla", Priority.NORMAL, "新能源"),
    KeywordRule("宁德时代", Priority.NORMAL, "新能源"),
    KeywordRule("比亚迪", Priority.NORMAL, "新能源"),
    KeywordRule("光伏", Priority.NORMAL, "新能源"),
    KeywordRule("锂电", Priority.NORMAL, "新能源"),
    # Normal — precious metals
    KeywordRule("贵金属", Priority.NORMAL, "贵金属"),
    KeywordRule("黄金", Priority.NORMAL, "贵金属"),
    KeywordRule("白银", Priority.NORMAL, "贵金属"),
    KeywordRule("Gold", Priority.NORMAL, "贵金属"),
]


# ── Watchlist entry ──────────────────────────────────────────────────


@dataclass(frozen=True)
class WatchlistEntry:
    """A stock in the user's watchlist."""

    ticker: str  # e.g. "600519"
    name: str  # e.g. "贵州茅台"


# ── Detector ─────────────────────────────────────────────────────────

# Confidence levels
_BASE_CONFIDENCE = 0.5
_WATCHLIST_CONFIDENCE_BOOST = 0.3  # watchlist hits get higher confidence
_MULTI_HIT_CONFIDENCE_BOOST = 0.1  # each additional keyword hit


class KeywordDetector(Detector):
    """Keyword-matching detector for news & social events.

    Thread-safe: rules and watchlist can be updated at runtime via
    :meth:`update_rules` and :meth:`update_watchlist`.
    """

    _ACCEPTED_TYPES = [EventType.NEWS]

    # ── Detector protocol ────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "keyword"

    @property
    def accepts(self) -> List[EventType]:
        return self._ACCEPTED_TYPES

    # ── Init ─────────────────────────────────────────────────────────

    def __init__(
        self,
        rules: Optional[List[KeywordRule]] = None,
        watchlist: Optional[List[WatchlistEntry]] = None,
    ) -> None:
        self._lock = threading.RLock()
        self._rules: List[KeywordRule] = list(rules or _DEFAULT_RULES)
        self._watchlist: List[WatchlistEntry] = list(watchlist or [])
        # pre-compile for faster matching
        self._compile()

    # ── Dynamic config ───────────────────────────────────────────────

    def update_rules(self, rules: List[KeywordRule]) -> None:
        """Replace keyword rules at runtime (thread-safe)."""
        with self._lock:
            self._rules = list(rules)
            self._compile()

    def update_watchlist(self, watchlist: List[WatchlistEntry]) -> None:
        """Replace watchlist at runtime (thread-safe)."""
        with self._lock:
            self._watchlist = list(watchlist)

    def add_rule(self, rule: KeywordRule) -> None:
        """Append a single rule (thread-safe)."""
        with self._lock:
            self._rules.append(rule)
            self._compile()

    def get_rules(self) -> List[KeywordRule]:
        """Return a snapshot of current rules."""
        with self._lock:
            return list(self._rules)

    # ── Core detection ───────────────────────────────────────────────

    def detect(self, event: RawMarketEvent) -> List[UnifiedSignal]:
        """Scan *event* text for keyword & watchlist matches.

        Returns at most **one** :class:`UnifiedSignal` per event (merged
        from all hits, taking the highest priority).
        """
        if not self._should_process(event):
            return []

        text = self._extract_text(event)
        if not text:
            return []

        with self._lock:
            rules = list(self._rules)
            watchlist = list(self._watchlist)

        # ── keyword matching ─────────────────────────────────────────
        hits: List[Tuple[KeywordRule, str]] = []  # (rule, matched_keyword)
        text_lower = text.lower()

        for rule in rules:
            # CJK keywords: exact substring.  ASCII: case-insensitive.
            if _is_ascii(rule.keyword):
                if rule.keyword.lower() in text_lower:
                    hits.append((rule, rule.keyword))
            else:
                if rule.keyword in text:
                    hits.append((rule, rule.keyword))

        # ── watchlist matching ───────────────────────────────────────
        watchlist_matches: List[WatchlistEntry] = []
        for entry in watchlist:
            if entry.ticker in text or entry.name in text:
                watchlist_matches.append(entry)

        if not hits and not watchlist_matches:
            return []

        # ── merge into single signal ─────────────────────────────────
        return [self._build_signal(event, hits, watchlist_matches)]

    # ── Internal helpers ─────────────────────────────────────────────

    def _should_process(self, event: RawMarketEvent) -> bool:
        """Return True if event type is one we handle.

        Accepts EventType.NEWS as well as raw string variants
        ``"news"``, ``"social"``, ``"NEWS"`` that may arrive from
        loosely-typed sources.
        """
        etype = event.event_type
        if isinstance(etype, EventType):
            return etype == EventType.NEWS
        # Fallback: raw string comparison
        return str(etype).lower() in {"news", "social"}

    @staticmethod
    def _extract_text(event: RawMarketEvent) -> str:
        """Pull searchable text from the event payload."""
        data = event.data or {}
        parts: List[str] = []
        for key in ("title", "content", "summary", "text", "headline"):
            val = data.get(key)
            if val and isinstance(val, str):
                parts.append(val)
        # Also include the top-level symbol if present
        if event.symbol:
            parts.append(event.symbol)
        return " ".join(parts)

    def _build_signal(
        self,
        event: RawMarketEvent,
        hits: List[Tuple[KeywordRule, str]],
        watchlist_matches: List[WatchlistEntry],
    ) -> UnifiedSignal:
        """Merge all keyword + watchlist hits into one signal."""
        # Determine highest priority
        if hits:
            best_priority = max(h[0].priority for h in hits)
        else:
            best_priority = Priority.NORMAL

        strength = PRIORITY_STRENGTH.get(best_priority, 0.4)

        # Confidence: base + boosts
        confidence = _BASE_CONFIDENCE
        extra_hits = max(0, len(hits) - 1)
        confidence += extra_hits * _MULTI_HIT_CONFIDENCE_BOOST
        if watchlist_matches:
            confidence += _WATCHLIST_CONFIDENCE_BOOST
        confidence = min(confidence, 1.0)

        # Collect sectors
        sectors: List[str] = list(
            dict.fromkeys(h[0].sector for h in hits if h[0].sector)
        )

        # Collect matched keywords
        matched_keywords: List[str] = list(
            dict.fromkeys(h[1] for h in hits)
        )

        # Determine asset — prefer watchlist ticker, else first sector, else "MARKET"
        if watchlist_matches:
            asset = watchlist_matches[0].ticker
        elif event.symbol:
            asset = event.symbol
        else:
            asset = "MARKET"

        # Direction heuristic: keyword hits are informational (LONG bias
        # for watchlist/sector news — the *strategy* layer decides action).
        direction = Direction.LONG

        # Market mapping
        market = self._resolve_market(event)

        return UnifiedSignal(
            market=market,
            asset=asset,
            direction=direction,
            strength=strength,
            confidence=confidence,
            signal_type=SignalType.SENTIMENT,
            source=f"detector:{self.name}",
            timestamp=event.timestamp,
            metadata={
                "event_id": event.event_id,
                "matched_keywords": matched_keywords,
                "sectors": sectors,
                "watchlist_tickers": [w.ticker for w in watchlist_matches],
                "watchlist_names": [w.name for w in watchlist_matches],
                "priority": best_priority.name if isinstance(best_priority, Priority) else str(best_priority),
                "hit_count": len(hits),
            },
        )

    @staticmethod
    def _resolve_market(event: RawMarketEvent) -> Market:
        """Map event's MarketScope to signal Market enum."""
        scope = str(event.market).lower()
        if "cn" in scope or "a_share" in scope:
            return Market.A_SHARE
        if "us" in scope:
            return Market.US_STOCK
        if "crypto" in scope:
            return Market.CRYPTO
        if "commodity" in scope:
            return Market.COMMODITY
        return Market.A_SHARE  # default for ashare project

    def _compile(self) -> None:
        """Pre-processing hook (reserved for future regex compilation)."""
        # Currently a no-op; kept so callers don't need changing if we
        # add compiled regex patterns later.
        pass


# ── Utility ──────────────────────────────────────────────────────────


def _is_ascii(s: str) -> bool:
    """Return True if *s* contains only ASCII characters."""
    try:
        s.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False
