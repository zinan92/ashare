"""Comprehensive tests for KeywordDetector.

All external I/O is mocked — tests are fully self-contained.
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import List

import pytest

from src.perception.detectors.keyword_detector import (
    KeywordDetector,
    KeywordRule,
    Priority,
    WatchlistEntry,
    PRIORITY_STRENGTH,
    _BASE_CONFIDENCE,
    _MULTI_HIT_CONFIDENCE_BOOST,
    _WATCHLIST_CONFIDENCE_BOOST,
    _is_ascii,
)
from src.perception.events import EventSource, EventType, MarketScope, RawMarketEvent
from src.perception.signals import Direction, Market, SignalType, UnifiedSignal


# ── Helpers ──────────────────────────────────────────────────────────


def _make_event(
    title: str = "",
    content: str = "",
    event_type: EventType = EventType.NEWS,
    symbol: str | None = None,
    market: MarketScope = MarketScope.CN_STOCK,
    **extra_data,
) -> RawMarketEvent:
    """Factory for RawMarketEvent with sensible defaults."""
    data = {}
    if title:
        data["title"] = title
    if content:
        data["content"] = content
    data.update(extra_data)
    return RawMarketEvent(
        source=EventSource.CLS,
        event_type=event_type,
        market=market,
        symbol=symbol,
        data=data,
        timestamp=datetime.now(timezone.utc),
    )


# ── Detector protocol tests ─────────────────────────────────────────


class TestDetectorProtocol:
    """Verify KeywordDetector satisfies the Detector ABC."""

    def test_name(self):
        d = KeywordDetector()
        assert d.name == "keyword"

    def test_accepts(self):
        d = KeywordDetector()
        assert EventType.NEWS in d.accepts

    def test_detect_returns_list(self):
        d = KeywordDetector()
        result = d.detect(_make_event(title="nothing special"))
        assert isinstance(result, list)


# ── Event type filtering ─────────────────────────────────────────────


class TestEventTypeFiltering:
    """Only news / social events should be processed."""

    def test_news_event_accepted(self):
        d = KeywordDetector(rules=[KeywordRule("DeepSeek", Priority.URGENT, "AI")])
        signals = d.detect(_make_event(title="DeepSeek releases V3"))
        assert len(signals) == 1

    def test_price_update_rejected(self):
        d = KeywordDetector(rules=[KeywordRule("DeepSeek", Priority.URGENT)])
        event = _make_event(title="DeepSeek", event_type=EventType.PRICE_UPDATE)
        assert d.detect(event) == []

    def test_kline_event_rejected(self):
        d = KeywordDetector()
        event = _make_event(title="AI大模型突破", event_type=EventType.KLINE)
        assert d.detect(event) == []

    def test_flow_event_rejected(self):
        d = KeywordDetector()
        event = _make_event(title="DeepSeek", event_type=EventType.FLOW)
        assert d.detect(event) == []


# ── Keyword matching ─────────────────────────────────────────────────


class TestKeywordMatching:
    """Core keyword detection logic."""

    def test_urgent_keyword_deepseek(self):
        d = KeywordDetector()
        signals = d.detect(_make_event(title="DeepSeek发布新模型"))
        assert len(signals) == 1
        s = signals[0]
        assert s.signal_type == SignalType.SENTIMENT
        assert s.strength == PRIORITY_STRENGTH[Priority.URGENT]
        assert "DeepSeek" in s.metadata["matched_keywords"]
        assert s.metadata["priority"] == "URGENT"

    def test_high_keyword_chinese(self):
        d = KeywordDetector()
        signals = d.detect(_make_event(title="央行宣布降准50个基点"))
        assert len(signals) == 1
        s = signals[0]
        assert s.strength == PRIORITY_STRENGTH[Priority.HIGH]
        assert "央行" in s.metadata["matched_keywords"]
        assert "降准" in s.metadata["matched_keywords"]

    def test_normal_keyword(self):
        d = KeywordDetector()
        signals = d.detect(_make_event(title="宁德时代发布新电池"))
        assert len(signals) == 1
        assert signals[0].strength == PRIORITY_STRENGTH[Priority.NORMAL]

    def test_english_case_insensitive(self):
        d = KeywordDetector(rules=[KeywordRule("nvidia", Priority.HIGH, "半导体")])
        signals = d.detect(_make_event(title="NVIDIA stock surges"))
        assert len(signals) == 1

    def test_default_nvidia_uppercase(self):
        """Default rules have 'NVIDIA' — should match case-insensitively."""
        d = KeywordDetector()
        signals = d.detect(_make_event(title="nvidia announces new GPU"))
        assert len(signals) == 1
        assert "NVIDIA" in signals[0].metadata["matched_keywords"]

    def test_no_match_returns_empty(self):
        d = KeywordDetector()
        signals = d.detect(_make_event(title="今天天气不错"))
        assert signals == []

    def test_empty_text_returns_empty(self):
        d = KeywordDetector()
        event = RawMarketEvent(
            source=EventSource.CLS,
            event_type=EventType.NEWS,
            market=MarketScope.CN_STOCK,
            data={},
            timestamp=datetime.now(timezone.utc),
        )
        assert d.detect(event) == []


# ── Priority merging ─────────────────────────────────────────────────


class TestPriorityMerging:
    """Multiple keyword hits from one event should merge into one signal."""

    def test_single_signal_from_multiple_hits(self):
        d = KeywordDetector()
        # Contains urgent (DeepSeek) + normal (新能源) keywords
        signals = d.detect(
            _make_event(title="DeepSeek助力新能源行业智能化")
        )
        assert len(signals) == 1, "Should merge into exactly one signal"

    def test_highest_priority_wins(self):
        d = KeywordDetector()
        # urgent (DeepSeek) + high (芯片) + normal (新能源)
        signals = d.detect(
            _make_event(title="DeepSeek芯片新能源全面布局")
        )
        s = signals[0]
        assert s.strength == PRIORITY_STRENGTH[Priority.URGENT]
        assert s.metadata["priority"] == "URGENT"

    def test_multi_hit_confidence_boost(self):
        d = KeywordDetector()
        # Two keyword hits → base + 1 extra hit boost
        signals = d.detect(_make_event(title="央行降息政策出台"))
        s = signals[0]
        # 央行 + 降息 + 政策 = 3 hits, 2 extra
        expected_conf = min(_BASE_CONFIDENCE + 2 * _MULTI_HIT_CONFIDENCE_BOOST, 1.0)
        assert abs(s.confidence - expected_conf) < 1e-6

    def test_sectors_collected(self):
        d = KeywordDetector()
        signals = d.detect(_make_event(title="DeepSeek + 半导体产业链"))
        s = signals[0]
        assert "AI" in s.metadata["sectors"]
        assert "半导体" in s.metadata["sectors"]


# ── Watchlist matching ───────────────────────────────────────────────


class TestWatchlistMatching:
    """Watchlist stock ticker/name matching."""

    def test_watchlist_ticker_match(self):
        d = KeywordDetector(
            rules=[],
            watchlist=[WatchlistEntry("600519", "贵州茅台")],
        )
        signals = d.detect(_make_event(title="600519今日大涨"))
        assert len(signals) == 1
        s = signals[0]
        assert s.asset == "600519"
        assert "600519" in s.metadata["watchlist_tickers"]

    def test_watchlist_name_match(self):
        d = KeywordDetector(
            rules=[],
            watchlist=[WatchlistEntry("600519", "贵州茅台")],
        )
        signals = d.detect(_make_event(title="贵州茅台公布年报"))
        assert len(signals) == 1
        assert signals[0].metadata["watchlist_names"] == ["贵州茅台"]

    def test_watchlist_confidence_boost(self):
        d = KeywordDetector(
            rules=[KeywordRule("大涨", Priority.NORMAL)],
            watchlist=[WatchlistEntry("600519", "贵州茅台")],
        )
        signals = d.detect(_make_event(title="贵州茅台大涨"))
        s = signals[0]
        expected = min(
            _BASE_CONFIDENCE + _WATCHLIST_CONFIDENCE_BOOST, 1.0
        )
        assert s.confidence >= expected

    def test_watchlist_only_no_keyword(self):
        """Watchlist match alone should still produce a signal."""
        d = KeywordDetector(
            rules=[],
            watchlist=[WatchlistEntry("000001", "平安银行")],
        )
        signals = d.detect(_make_event(title="平安银行发布公告"))
        assert len(signals) == 1
        assert signals[0].asset == "000001"

    def test_watchlist_no_match(self):
        d = KeywordDetector(
            rules=[],
            watchlist=[WatchlistEntry("600519", "贵州茅台")],
        )
        signals = d.detect(_make_event(title="今天天气不错"))
        assert signals == []


# ── Signal output format ─────────────────────────────────────────────


class TestSignalOutput:
    """Verify output signal fields are correct."""

    def test_signal_type_is_sentiment(self):
        d = KeywordDetector()
        signals = d.detect(_make_event(title="DeepSeek发布"))
        assert signals[0].signal_type == SignalType.SENTIMENT

    def test_source_field(self):
        d = KeywordDetector()
        signals = d.detect(_make_event(title="DeepSeek"))
        assert signals[0].source == "detector:keyword"

    def test_market_cn_stock(self):
        d = KeywordDetector()
        signals = d.detect(
            _make_event(title="DeepSeek", market=MarketScope.CN_STOCK)
        )
        assert signals[0].market == Market.A_SHARE

    def test_market_us_stock(self):
        d = KeywordDetector()
        signals = d.detect(
            _make_event(title="OpenAI", market=MarketScope.US_STOCK)
        )
        assert signals[0].market == Market.US_STOCK

    def test_market_commodity(self):
        d = KeywordDetector()
        signals = d.detect(
            _make_event(title="黄金价格飙升", market=MarketScope.COMMODITY)
        )
        assert signals[0].market == Market.COMMODITY

    def test_timestamp_from_event(self):
        ts = datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        event = RawMarketEvent(
            source=EventSource.CLS,
            event_type=EventType.NEWS,
            market=MarketScope.CN_STOCK,
            data={"title": "DeepSeek"},
            timestamp=ts,
        )
        d = KeywordDetector()
        signals = d.detect(event)
        assert signals[0].timestamp == ts

    def test_asset_defaults_to_market(self):
        """No watchlist, no symbol → asset = 'MARKET'."""
        d = KeywordDetector()
        signals = d.detect(_make_event(title="DeepSeek"))
        assert signals[0].asset == "MARKET"

    def test_asset_from_symbol(self):
        d = KeywordDetector()
        signals = d.detect(_make_event(title="DeepSeek", symbol="300750.SZ"))
        assert signals[0].asset == "300750.SZ"

    def test_direction_is_long(self):
        d = KeywordDetector()
        signals = d.detect(_make_event(title="DeepSeek"))
        assert signals[0].direction == Direction.LONG

    def test_strength_bounds(self):
        d = KeywordDetector()
        signals = d.detect(_make_event(title="DeepSeek"))
        s = signals[0]
        assert 0.0 <= s.strength <= 1.0

    def test_confidence_bounds(self):
        d = KeywordDetector()
        signals = d.detect(_make_event(title="DeepSeek"))
        s = signals[0]
        assert 0.0 <= s.confidence <= 1.0

    def test_metadata_has_event_id(self):
        event = _make_event(title="DeepSeek")
        d = KeywordDetector()
        signals = d.detect(event)
        assert signals[0].metadata["event_id"] == event.event_id

    def test_hit_count_in_metadata(self):
        d = KeywordDetector()
        signals = d.detect(_make_event(title="央行降息"))
        assert signals[0].metadata["hit_count"] >= 2


# ── Dynamic configuration ────────────────────────────────────────────


class TestDynamicConfig:
    """Rules and watchlist can be updated at runtime."""

    def test_update_rules(self):
        d = KeywordDetector(rules=[])
        # Initially no rules → no match
        assert d.detect(_make_event(title="FooBar")) == []
        # Add a rule dynamically
        d.update_rules([KeywordRule("FooBar", Priority.HIGH)])
        signals = d.detect(_make_event(title="FooBar"))
        assert len(signals) == 1
        assert signals[0].strength == PRIORITY_STRENGTH[Priority.HIGH]

    def test_add_rule(self):
        d = KeywordDetector(rules=[KeywordRule("Existing", Priority.NORMAL)])
        assert len(d.get_rules()) == 1
        d.add_rule(KeywordRule("TestKW", Priority.NORMAL))
        assert len(d.get_rules()) == 2
        signals = d.detect(_make_event(title="TestKW detected"))
        assert len(signals) == 1

    def test_update_watchlist(self):
        d = KeywordDetector(rules=[])
        assert d.detect(_make_event(title="贵州茅台")) == []
        d.update_watchlist([WatchlistEntry("600519", "贵州茅台")])
        signals = d.detect(_make_event(title="贵州茅台"))
        assert len(signals) == 1

    def test_get_rules_snapshot(self):
        d = KeywordDetector(rules=[KeywordRule("X", Priority.NORMAL)])
        rules = d.get_rules()
        assert len(rules) == 1
        # Mutating the snapshot shouldn't affect the detector
        rules.clear()
        assert len(d.get_rules()) == 1

    def test_clear_rules(self):
        d = KeywordDetector()
        assert len(d.get_rules()) > 0
        d.update_rules([])
        assert d.get_rules() == []
        assert d.detect(_make_event(title="DeepSeek")) == []


# ── Text extraction ──────────────────────────────────────────────────


class TestTextExtraction:
    """Detector should pull text from various data fields."""

    def test_title_field(self):
        d = KeywordDetector()
        signals = d.detect(_make_event(title="DeepSeek"))
        assert len(signals) == 1

    def test_content_field(self):
        d = KeywordDetector()
        signals = d.detect(_make_event(content="DeepSeek发布新版本"))
        assert len(signals) == 1

    def test_summary_field(self):
        d = KeywordDetector()
        event = _make_event()
        event.data["summary"] = "OpenAI new release"
        signals = d.detect(event)
        assert len(signals) == 1

    def test_headline_field(self):
        d = KeywordDetector()
        event = _make_event()
        event.data["headline"] = "央行最新政策"
        signals = d.detect(event)
        assert len(signals) == 1

    def test_text_field(self):
        d = KeywordDetector()
        event = _make_event()
        event.data["text"] = "NVIDIA stock price"
        signals = d.detect(event)
        assert len(signals) == 1


# ── Thread safety ────────────────────────────────────────────────────


class TestThreadSafety:
    """Concurrent detect calls and config updates should not crash."""

    def test_concurrent_detect(self):
        d = KeywordDetector()
        results: List[List[UnifiedSignal]] = []
        errors: List[Exception] = []

        def worker():
            try:
                for _ in range(50):
                    r = d.detect(_make_event(title="DeepSeek芯片"))
                    results.append(r)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert all(len(r) == 1 for r in results)

    def test_concurrent_update_and_detect(self):
        d = KeywordDetector()
        errors: List[Exception] = []

        def updater():
            try:
                for i in range(50):
                    d.update_rules([KeywordRule(f"KW{i}", Priority.NORMAL)])
            except Exception as e:
                errors.append(e)

        def detector():
            try:
                for _ in range(50):
                    d.detect(_make_event(title="KW25 detected"))
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=updater)
        t2 = threading.Thread(target=detector)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert not errors


# ── Edge cases ───────────────────────────────────────────────────────


class TestEdgeCases:
    """Boundary conditions and unusual inputs."""

    def test_empty_rules_and_watchlist(self):
        d = KeywordDetector(rules=[], watchlist=[])
        assert d.detect(_make_event(title="anything")) == []

    def test_very_long_text(self):
        d = KeywordDetector()
        long_text = "blah " * 10_000 + "DeepSeek" + " blah" * 10_000
        signals = d.detect(_make_event(title=long_text))
        assert len(signals) == 1

    def test_special_characters_in_text(self):
        d = KeywordDetector()
        signals = d.detect(_make_event(title="【重磅】DeepSeek™ 发布！🚀"))
        assert len(signals) == 1

    def test_duplicate_keywords_deduplicated_in_metadata(self):
        """Same keyword appearing twice in text shouldn't double-count."""
        d = KeywordDetector(
            rules=[KeywordRule("DeepSeek", Priority.URGENT, "AI")]
        )
        signals = d.detect(
            _make_event(title="DeepSeek and DeepSeek again")
        )
        assert len(signals) == 1
        # Keyword appears once in matched list (deduplicated)
        assert signals[0].metadata["matched_keywords"].count("DeepSeek") == 1

    def test_confidence_capped_at_1(self):
        """Many hits should not push confidence above 1.0."""
        rules = [
            KeywordRule(f"kw{i}", Priority.URGENT) for i in range(20)
        ]
        d = KeywordDetector(
            rules=rules,
            watchlist=[WatchlistEntry("000001", "test")],
        )
        text = " ".join(f"kw{i}" for i in range(20)) + " 000001"
        signals = d.detect(_make_event(title=text))
        assert signals[0].confidence <= 1.0

    def test_sector_dedup_preserves_order(self):
        d = KeywordDetector(
            rules=[
                KeywordRule("A", Priority.HIGH, "SectorX"),
                KeywordRule("B", Priority.NORMAL, "SectorX"),
                KeywordRule("C", Priority.NORMAL, "SectorY"),
            ]
        )
        signals = d.detect(_make_event(title="A B C"))
        sectors = signals[0].metadata["sectors"]
        assert sectors == ["SectorX", "SectorY"]


# ── Utility function tests ───────────────────────────────────────────


class TestUtilities:
    def test_is_ascii_true(self):
        assert _is_ascii("hello") is True
        assert _is_ascii("NVIDIA") is True

    def test_is_ascii_false(self):
        assert _is_ascii("芯片") is False
        assert _is_ascii("DeepSeek大模型") is False

    def test_is_ascii_empty(self):
        assert _is_ascii("") is True
