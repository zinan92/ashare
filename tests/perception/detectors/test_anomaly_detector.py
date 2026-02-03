"""Tests for AnomalyDetector.

All external dependencies are mocked — tests run fully offline.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

import pytest

from src.perception.detectors.anomaly_detector import AnomalyDetector
from src.perception.events import (
    EventSource,
    EventType,
    MarketScope,
    RawMarketEvent,
)
from src.perception.signals import Direction, Market, SignalType, UnifiedSignal


# ── Helpers ──────────────────────────────────────────────────────────


def _make_event(
    event_type: EventType = EventType.ANOMALY,
    symbol: str | None = "000001",
    data: Dict[str, Any] | None = None,
    market: MarketScope = MarketScope.CN_STOCK,
) -> RawMarketEvent:
    return RawMarketEvent(
        source=EventSource.SINA,
        event_type=event_type,
        market=market,
        symbol=symbol,
        data=data or {},
        timestamp=datetime.now(timezone.utc),
    )


# ── Detector Interface ───────────────────────────────────────────────


class TestDetectorInterface:
    """Verify AnomalyDetector satisfies the Detector ABC."""

    def test_name(self):
        d = AnomalyDetector()
        assert d.name == "anomaly"

    def test_accepts_event_types(self):
        d = AnomalyDetector()
        accepted = d.accepts
        assert EventType.ANOMALY in accepted
        assert EventType.PRICE_UPDATE in accepted
        assert EventType.LIMIT_EVENT in accepted
        assert EventType.BOARD_CHANGE in accepted

    def test_detect_returns_list(self):
        d = AnomalyDetector()
        result = d.detect(_make_event(data={}))
        assert isinstance(result, list)

    def test_detect_empty_for_irrelevant_data(self):
        d = AnomalyDetector()
        event = _make_event(data={"unrelated": True})
        assert d.detect(event) == []


# ── 涨停/跌停 Wave Detection ────────────────────────────────────────


class TestLimitWaveDetection:
    """Test limit-up / limit-down surge detection."""

    def test_limit_up_surge_triggers(self):
        d = AnomalyDetector({"limit_up_count_threshold": 10})
        event = _make_event(
            event_type=EventType.LIMIT_EVENT,
            data={"limit_up_count": 15},
        )
        signals = d.detect(event)
        assert len(signals) >= 1
        s = signals[0]
        assert s.direction == "long"
        assert s.signal_type == "flow"
        assert s.metadata["detector"] == "limit_wave"
        assert s.metadata["type"] == "limit_up_surge"
        assert s.metadata["limit_up_count"] == 15

    def test_limit_down_surge_triggers(self):
        d = AnomalyDetector({"limit_down_count_threshold": 10})
        event = _make_event(
            event_type=EventType.LIMIT_EVENT,
            data={"limit_down_count": 20},
        )
        signals = d.detect(event)
        assert len(signals) >= 1
        s = signals[0]
        assert s.direction == "short"
        assert s.signal_type == "flow"
        assert s.metadata["type"] == "limit_down_surge"

    def test_below_threshold_no_signal(self):
        d = AnomalyDetector({"limit_up_count_threshold": 10})
        event = _make_event(
            event_type=EventType.LIMIT_EVENT,
            data={"limit_up_count": 5},
        )
        assert d.detect(event) == []

    def test_both_limits_fire_simultaneously(self):
        d = AnomalyDetector({
            "limit_up_count_threshold": 5,
            "limit_down_count_threshold": 5,
        })
        event = _make_event(
            event_type=EventType.LIMIT_EVENT,
            data={"limit_up_count": 10, "limit_down_count": 10},
        )
        signals = d.detect(event)
        assert len(signals) == 2
        directions = {s.direction for s in signals}
        assert directions == {"long", "short"}

    def test_limit_up_strength_scales_with_count(self):
        d = AnomalyDetector({"limit_up_count_threshold": 10})
        weak = _make_event(
            event_type=EventType.LIMIT_EVENT,
            data={"limit_up_count": 10},
        )
        strong = _make_event(
            event_type=EventType.LIMIT_EVENT,
            data={"limit_up_count": 30},
        )
        weak_s = d.detect(weak)[0]
        strong_s = d.detect(strong)[0]
        assert strong_s.strength > weak_s.strength

    def test_limit_event_with_anomaly_type(self):
        """ANOMALY event type should also route to limit wave."""
        d = AnomalyDetector({"limit_up_count_threshold": 5})
        event = _make_event(
            event_type=EventType.ANOMALY,
            data={"limit_up_count": 10},
        )
        signals = d.detect(event)
        assert len(signals) >= 1
        assert signals[0].metadata["detector"] == "limit_wave"

    def test_no_symbol_uses_market(self):
        d = AnomalyDetector({"limit_up_count_threshold": 5})
        event = _make_event(
            event_type=EventType.LIMIT_EVENT,
            symbol=None,
            data={"limit_up_count": 10},
        )
        signals = d.detect(event)
        assert signals[0].asset == "MARKET"


# ── 大笔买入/卖出 Detection ─────────────────────────────────────────


class TestLargeOrderDetection:
    """Test large block trade detection."""

    def test_large_buy_triggers(self):
        d = AnomalyDetector({"large_order_amount_threshold": 1_000_000})
        event = _make_event(
            event_type=EventType.ANOMALY,
            symbol="600519",
            data={"order_amount": 5_000_000, "order_side": "buy"},
        )
        signals = d.detect(event)
        # May also have other signals; find the large_order one
        lo_signals = [s for s in signals if s.metadata.get("detector") == "large_order"]
        assert len(lo_signals) == 1
        s = lo_signals[0]
        assert s.direction == "long"
        assert s.signal_type == "flow"
        assert s.metadata["type"] == "large_buy"

    def test_large_sell_triggers(self):
        d = AnomalyDetector({"large_order_amount_threshold": 1_000_000})
        event = _make_event(
            event_type=EventType.ANOMALY,
            symbol="000858",
            data={"order_amount": 2_000_000, "order_side": "sell"},
        )
        signals = d.detect(event)
        lo_signals = [s for s in signals if s.metadata.get("detector") == "large_order"]
        assert len(lo_signals) == 1
        assert lo_signals[0].direction == "short"
        assert lo_signals[0].metadata["type"] == "large_sell"

    def test_below_threshold_no_signal(self):
        d = AnomalyDetector({"large_order_amount_threshold": 5_000_000})
        event = _make_event(
            event_type=EventType.ANOMALY,
            data={"order_amount": 1_000_000, "order_side": "buy"},
        )
        signals = d.detect(event)
        lo_signals = [s for s in signals if s.metadata.get("detector") == "large_order"]
        assert lo_signals == []

    def test_unknown_side_defaults_long_lower_confidence(self):
        d = AnomalyDetector({"large_order_amount_threshold": 1_000_000})
        event = _make_event(
            event_type=EventType.ANOMALY,
            data={"order_amount": 5_000_000, "order_side": ""},
        )
        signals = d.detect(event)
        lo_signals = [s for s in signals if s.metadata.get("detector") == "large_order"]
        assert len(lo_signals) == 1
        s = lo_signals[0]
        assert s.direction == "long"
        assert s.metadata["type"] == "large_order_unknown"
        # Confidence should be reduced
        assert s.confidence < 0.60

    def test_strength_scales_with_amount(self):
        d = AnomalyDetector({"large_order_amount_threshold": 1_000_000})
        small = _make_event(
            event_type=EventType.ANOMALY,
            data={"order_amount": 1_000_000, "order_side": "buy"},
        )
        big = _make_event(
            event_type=EventType.ANOMALY,
            data={"order_amount": 10_000_000, "order_side": "buy"},
        )
        s_small = [s for s in d.detect(small) if s.metadata.get("detector") == "large_order"][0]
        s_big = [s for s in d.detect(big) if s.metadata.get("detector") == "large_order"][0]
        assert s_big.strength > s_small.strength


# ── 自选股异动 Detection ─────────────────────────────────────────────


class TestWatchlistMoveDetection:
    """Test watchlist stock large-move detection."""

    def test_big_move_triggers(self):
        d = AnomalyDetector({"watchlist_move_pct_threshold": 5.0})
        event = _make_event(
            event_type=EventType.PRICE_UPDATE,
            symbol="000001",
            data={"change_pct": 7.5},
        )
        signals = d.detect(event)
        wl_signals = [s for s in signals if s.metadata.get("detector") == "watchlist_move"]
        assert len(wl_signals) == 1
        s = wl_signals[0]
        assert s.direction == "long"
        assert s.signal_type == "technical"
        assert s.metadata["change_pct"] == 7.5

    def test_negative_move_triggers_short(self):
        d = AnomalyDetector({"watchlist_move_pct_threshold": 5.0})
        event = _make_event(
            event_type=EventType.PRICE_UPDATE,
            symbol="600519",
            data={"change_pct": -6.0},
        )
        signals = d.detect(event)
        wl_signals = [s for s in signals if s.metadata.get("detector") == "watchlist_move"]
        assert len(wl_signals) == 1
        assert wl_signals[0].direction == "short"

    def test_small_move_no_signal(self):
        d = AnomalyDetector({"watchlist_move_pct_threshold": 5.0})
        event = _make_event(
            event_type=EventType.PRICE_UPDATE,
            data={"change_pct": 2.0},
        )
        signals = d.detect(event)
        wl_signals = [s for s in signals if s.metadata.get("detector") == "watchlist_move"]
        assert wl_signals == []

    def test_watchlist_filter_includes(self):
        d = AnomalyDetector({
            "watchlist_symbols": ["000001", "600519"],
            "watchlist_move_pct_threshold": 5.0,
        })
        event = _make_event(
            event_type=EventType.PRICE_UPDATE,
            symbol="000001",
            data={"change_pct": 8.0},
        )
        signals = d.detect(event)
        wl_signals = [s for s in signals if s.metadata.get("detector") == "watchlist_move"]
        assert len(wl_signals) == 1

    def test_watchlist_filter_excludes(self):
        d = AnomalyDetector({
            "watchlist_symbols": ["000001"],
            "watchlist_move_pct_threshold": 5.0,
        })
        event = _make_event(
            event_type=EventType.PRICE_UPDATE,
            symbol="999999",
            data={"change_pct": 8.0},
        )
        signals = d.detect(event)
        wl_signals = [s for s in signals if s.metadata.get("detector") == "watchlist_move"]
        assert wl_signals == []

    def test_no_symbol_no_signal(self):
        d = AnomalyDetector({"watchlist_move_pct_threshold": 5.0})
        event = _make_event(
            event_type=EventType.PRICE_UPDATE,
            symbol=None,
            data={"change_pct": 10.0},
        )
        signals = d.detect(event)
        wl_signals = [s for s in signals if s.metadata.get("detector") == "watchlist_move"]
        assert wl_signals == []

    def test_strength_scales_with_magnitude(self):
        d = AnomalyDetector({"watchlist_move_pct_threshold": 5.0})
        small = _make_event(
            event_type=EventType.PRICE_UPDATE,
            data={"change_pct": 5.5},
        )
        big = _make_event(
            event_type=EventType.PRICE_UPDATE,
            data={"change_pct": 15.0},
        )
        s_small = [s for s in d.detect(small) if s.metadata.get("detector") == "watchlist_move"][0]
        s_big = [s for s in d.detect(big) if s.metadata.get("detector") == "watchlist_move"][0]
        assert s_big.strength > s_small.strength


# ── Volume Spike Detection ───────────────────────────────────────────


class TestVolumeSpikeDetection:
    """Test abnormal volume detection."""

    def test_spike_triggers(self):
        d = AnomalyDetector({"volume_spike_ratio": 3.0})
        event = _make_event(
            event_type=EventType.PRICE_UPDATE,
            symbol="000001",
            data={"volume": 3_000_000, "avg_volume": 500_000, "change_pct": 2.0},
        )
        signals = d.detect(event)
        vs_signals = [s for s in signals if s.metadata.get("detector") == "volume_spike"]
        assert len(vs_signals) == 1
        s = vs_signals[0]
        assert s.signal_type == "flow"
        assert s.direction == "long"
        assert s.metadata["volume_ratio"] == 6.0

    def test_spike_negative_change_short(self):
        d = AnomalyDetector({"volume_spike_ratio": 3.0})
        event = _make_event(
            event_type=EventType.PRICE_UPDATE,
            data={"volume": 5_000_000, "avg_volume": 1_000_000, "change_pct": -3.0},
        )
        signals = d.detect(event)
        vs_signals = [s for s in signals if s.metadata.get("detector") == "volume_spike"]
        assert len(vs_signals) == 1
        assert vs_signals[0].direction == "short"

    def test_no_spike_below_threshold(self):
        d = AnomalyDetector({"volume_spike_ratio": 3.0})
        event = _make_event(
            event_type=EventType.PRICE_UPDATE,
            data={"volume": 1_000_000, "avg_volume": 500_000},
        )
        signals = d.detect(event)
        vs_signals = [s for s in signals if s.metadata.get("detector") == "volume_spike"]
        assert vs_signals == []

    def test_zero_avg_volume_no_signal(self):
        d = AnomalyDetector({"volume_spike_ratio": 3.0})
        event = _make_event(
            event_type=EventType.PRICE_UPDATE,
            data={"volume": 1_000_000, "avg_volume": 0},
        )
        signals = d.detect(event)
        vs_signals = [s for s in signals if s.metadata.get("detector") == "volume_spike"]
        assert vs_signals == []

    def test_board_change_event_routes_to_volume(self):
        d = AnomalyDetector({"volume_spike_ratio": 2.0})
        event = _make_event(
            event_type=EventType.BOARD_CHANGE,
            data={"volume": 10_000_000, "avg_volume": 1_000_000, "change_pct": 1.0},
        )
        signals = d.detect(event)
        vs_signals = [s for s in signals if s.metadata.get("detector") == "volume_spike"]
        assert len(vs_signals) == 1


# ── Configuration ────────────────────────────────────────────────────


class TestConfiguration:
    """Test configurable thresholds."""

    def test_default_config(self):
        d = AnomalyDetector()
        assert d._config["limit_up_count_threshold"] == 10
        assert d._config["large_order_amount_threshold"] == 5_000_000
        assert d._config["watchlist_move_pct_threshold"] == 5.0
        assert d._config["volume_spike_ratio"] == 3.0

    def test_custom_config_overrides(self):
        d = AnomalyDetector({
            "limit_up_count_threshold": 20,
            "volume_spike_ratio": 5.0,
        })
        assert d._config["limit_up_count_threshold"] == 20
        assert d._config["volume_spike_ratio"] == 5.0
        # Defaults preserved for non-overridden keys
        assert d._config["large_order_amount_threshold"] == 5_000_000

    def test_min_confidence_filter(self):
        d = AnomalyDetector({
            "min_confidence": 0.70,
            "watchlist_move_pct_threshold": 5.0,
            "watchlist_confidence": 0.50,  # below filter
        })
        event = _make_event(
            event_type=EventType.PRICE_UPDATE,
            data={"change_pct": 8.0},
        )
        signals = d.detect(event)
        wl_signals = [s for s in signals if s.metadata.get("detector") == "watchlist_move"]
        assert wl_signals == []

    def test_custom_watchlist(self):
        d = AnomalyDetector({
            "watchlist_symbols": ["600519"],
            "watchlist_move_pct_threshold": 3.0,
        })
        # 600519 is on watchlist — should trigger
        event1 = _make_event(
            event_type=EventType.PRICE_UPDATE,
            symbol="600519",
            data={"change_pct": 5.0},
        )
        s1 = [s for s in d.detect(event1) if s.metadata.get("detector") == "watchlist_move"]
        assert len(s1) == 1

        # 000001 not on watchlist — filtered out
        event2 = _make_event(
            event_type=EventType.PRICE_UPDATE,
            symbol="000001",
            data={"change_pct": 5.0},
        )
        s2 = [s for s in d.detect(event2) if s.metadata.get("detector") == "watchlist_move"]
        assert s2 == []


# ── Signal Quality ───────────────────────────────────────────────────


class TestSignalQuality:
    """Verify signal fields are properly bounded and typed."""

    def test_strength_bounded_0_1(self):
        d = AnomalyDetector({"limit_up_count_threshold": 1})
        event = _make_event(
            event_type=EventType.LIMIT_EVENT,
            data={"limit_up_count": 1000},  # extreme
        )
        signals = d.detect(event)
        for s in signals:
            assert 0.0 <= s.strength <= 1.0

    def test_confidence_bounded_0_1(self):
        d = AnomalyDetector({"limit_up_count_threshold": 1})
        event = _make_event(
            event_type=EventType.LIMIT_EVENT,
            data={"limit_up_count": 1000},
        )
        signals = d.detect(event)
        for s in signals:
            assert 0.0 <= s.confidence <= 1.0

    def test_signal_source_is_detector_name(self):
        d = AnomalyDetector({"limit_up_count_threshold": 5})
        event = _make_event(
            event_type=EventType.LIMIT_EVENT,
            data={"limit_up_count": 10},
        )
        signals = d.detect(event)
        assert all(s.source == "anomaly" for s in signals)

    def test_signal_market_from_event(self):
        d = AnomalyDetector({"limit_up_count_threshold": 5})
        event = _make_event(
            event_type=EventType.LIMIT_EVENT,
            market=MarketScope.CN_STOCK,
            data={"limit_up_count": 10},
        )
        signals = d.detect(event)
        assert all(s.market == "a_share" for s in signals)

    def test_unified_signal_serializable(self):
        d = AnomalyDetector({"limit_up_count_threshold": 5})
        event = _make_event(
            event_type=EventType.LIMIT_EVENT,
            data={"limit_up_count": 10},
        )
        signals = d.detect(event)
        for s in signals:
            d_dict = s.to_dict()
            assert isinstance(d_dict, dict)
            roundtrip = UnifiedSignal.from_dict(d_dict)
            assert roundtrip.signal_id == s.signal_id


# ── Import / Export ──────────────────────────────────────────────────


class TestImportExport:
    """Verify the detector is properly exported from the package."""

    def test_import_from_detectors_package(self):
        from src.perception.detectors import AnomalyDetector as AD
        assert AD is AnomalyDetector

    def test_in_all(self):
        from src.perception.detectors import __all__
        assert "AnomalyDetector" in __all__

    def test_anomaly_event_type_exists(self):
        assert hasattr(EventType, "ANOMALY")
        assert EventType.ANOMALY.value == "anomaly"
