"""Comprehensive tests for FlowDetector.

All external dependencies (database, tushare) are mocked.
Tests cover:
- Detector interface compliance
- Sector net inflow / outflow anomaly detection
- Northbound capital large-move detection
- Sector rotation signal generation
- Custom tracked-sector anomaly detection
- Configurable thresholds
- Edge cases and error handling
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest

from src.perception.detectors.flow_detector import (
    DEFAULT_TRACKED_SECTORS,
    FlowDetector,
    FlowDetectorConfig,
)
from src.perception.detectors.base import Detector
from src.perception.events import (
    EventSource,
    EventType,
    MarketScope,
    RawMarketEvent,
)
from src.perception.signals import (
    AShareSignal,
    Direction,
    Market,
    SignalType,
    UnifiedSignal,
)


# -- Helpers --

NOW = datetime.now(timezone.utc)


def _make_event(
    event_type: EventType = EventType.BOARD_CHANGE,
    data: Dict[str, Any] | None = None,
    **kwargs,
) -> RawMarketEvent:
    """Build a RawMarketEvent with sensible defaults."""
    defaults = dict(
        source=EventSource.TUSHARE,
        event_type=event_type,
        market=MarketScope.CN_STOCK,
        timestamp=NOW,
        data=data or {},
    )
    defaults.update(kwargs)
    return RawMarketEvent(**defaults)


def _sector(
    name: str,
    net_amount: float,
    pct_change: float = 0.0,
    ts_code: str = "",
    rank: int | None = None,
    prev_rank: int | None = None,
    up_count: int = 0,
    down_count: int = 0,
) -> Dict[str, Any]:
    """Build a sector dict matching the expected event.data.sectors format."""
    d: Dict[str, Any] = {
        "name": name,
        "net_amount": net_amount,
        "pct_change": pct_change,
        "ts_code": ts_code or f"{name}.THS",
        "up_count": up_count,
        "down_count": down_count,
    }
    if rank is not None:
        d["rank"] = rank
    if prev_rank is not None:
        d["prev_rank"] = prev_rank
    return d


# -- Interface Compliance --


class TestFlowDetectorInterface:
    """Verify FlowDetector satisfies the Detector ABC."""

    def test_is_detector_subclass(self):
        assert issubclass(FlowDetector, Detector)

    def test_name_property(self):
        d = FlowDetector()
        assert d.name == "flow"

    def test_accepts_property(self):
        d = FlowDetector()
        assert EventType.BOARD_CHANGE in d.accepts
        assert EventType.FLOW in d.accepts

    def test_detect_returns_list(self):
        d = FlowDetector()
        result = d.detect(_make_event())
        assert isinstance(result, list)

    def test_detect_empty_event_returns_empty(self):
        d = FlowDetector()
        result = d.detect(_make_event(data={}))
        assert result == []


# -- Sector Net Inflow / Outflow Anomalies --


class TestSectorAnomalies:
    """Tests for _detect_sector_anomalies."""

    def test_large_inflow_generates_long_signal(self):
        d = FlowDetector()
        event = _make_event(data={
            "sectors": [_sector("半导体", 10.0, pct_change=2.5)]
        })
        signals = d.detect(event)
        flow_sigs = [s for s in signals if s.metadata.get("sub_type") == "sector_anomaly"]
        assert len(flow_sigs) >= 1
        sig = flow_sigs[0]
        assert isinstance(sig, AShareSignal)
        assert sig.direction == "long"
        assert sig.signal_type == "flow"
        assert sig.source == "flow/sector_anomaly"
        assert sig.metadata["sector_name"] == "半导体"
        assert sig.metadata["net_amount"] == 10.0

    def test_large_outflow_generates_short_signal(self):
        d = FlowDetector()
        event = _make_event(data={
            "sectors": [_sector("房地产", -8.0, pct_change=-1.5)]
        })
        signals = d.detect(event)
        flow_sigs = [s for s in signals if s.metadata.get("sub_type") == "sector_anomaly"]
        assert len(flow_sigs) >= 1
        sig = flow_sigs[0]
        assert sig.direction == "short"

    def test_below_threshold_no_signal(self):
        d = FlowDetector()
        event = _make_event(data={
            "sectors": [_sector("银行", 2.0, pct_change=0.3)]
        })
        signals = d.detect(event)
        anomaly_sigs = [s for s in signals if s.metadata.get("sub_type") == "sector_anomaly"]
        assert anomaly_sigs == []

    def test_custom_threshold(self):
        cfg = FlowDetectorConfig(net_inflow_threshold=1.0)
        d = FlowDetector(config=cfg)
        event = _make_event(data={
            "sectors": [_sector("银行", 2.0, pct_change=0.3)]
        })
        signals = d.detect(event)
        anomaly_sigs = [s for s in signals if s.metadata.get("sub_type") == "sector_anomaly"]
        assert len(anomaly_sigs) >= 1

    def test_strength_scales_with_flow(self):
        d = FlowDetector()
        small = _make_event(data={"sectors": [_sector("A", 6.0)]})
        large = _make_event(data={"sectors": [_sector("B", 25.0)]})
        s_small = [s for s in d.detect(small) if s.metadata.get("sub_type") == "sector_anomaly"]
        s_large = [s for s in d.detect(large) if s.metadata.get("sub_type") == "sector_anomaly"]
        assert len(s_small) >= 1 and len(s_large) >= 1
        assert s_large[0].strength > s_small[0].strength

    def test_strength_capped_at_one(self):
        d = FlowDetector()
        event = _make_event(data={"sectors": [_sector("X", 100.0)]})
        signals = [s for s in d.detect(event) if s.metadata.get("sub_type") == "sector_anomaly"]
        assert signals[0].strength <= 1.0

    def test_confidence_boosted_when_price_aligns(self):
        d = FlowDetector()
        aligned = _make_event(data={
            "sectors": [_sector("A", 10.0, pct_change=2.0)]
        })
        misaligned = _make_event(data={
            "sectors": [_sector("A", 10.0, pct_change=-2.0)]
        })
        s_aligned = [s for s in d.detect(aligned) if s.metadata.get("sub_type") == "sector_anomaly"]
        s_misaligned = [s for s in d.detect(misaligned) if s.metadata.get("sub_type") == "sector_anomaly"]
        assert s_aligned[0].confidence > s_misaligned[0].confidence

    def test_multiple_sectors_multiple_signals(self):
        d = FlowDetector()
        event = _make_event(data={
            "sectors": [
                _sector("半导体", 15.0),
                _sector("银行", -12.0),
                _sector("传媒", 1.0),  # below threshold
            ]
        })
        signals = [s for s in d.detect(event) if s.metadata.get("sub_type") == "sector_anomaly"]
        assert len(signals) == 2

    def test_net_inflow_field_alias(self):
        """Sectors using net_inflow instead of net_amount should still work."""
        d = FlowDetector()
        event = _make_event(data={
            "sectors": [{"name": "X", "net_inflow": 10.0, "ts_code": "X.THS", "pct_change": 0}]
        })
        signals = [s for s in d.detect(event) if s.metadata.get("sub_type") == "sector_anomaly"]
        assert len(signals) >= 1

    def test_missing_sectors_key(self):
        d = FlowDetector()
        event = _make_event(data={"something_else": True})
        signals = d.detect(event)
        assert isinstance(signals, list)

    def test_empty_sectors_list(self):
        d = FlowDetector()
        event = _make_event(data={"sectors": []})
        assert d.detect(event) == []


# -- Northbound Capital --


class TestNorthboundDetection:
    """Tests for _detect_northbound."""

    def test_large_northbound_inflow(self):
        d = FlowDetector()
        event = _make_event(
            event_type=EventType.FLOW,
            data={"kind": "northbound", "north_money": 80.0, "sh_money": 50.0, "sz_money": 30.0},
        )
        signals = d.detect(event)
        assert len(signals) == 1
        sig = signals[0]
        assert isinstance(sig, AShareSignal)
        assert sig.direction == "long"
        assert sig.asset == "NORTHBOUND"
        assert sig.source == "flow/northbound"
        assert sig.north_flow == 80.0
        assert sig.metadata["sub_type"] == "northbound"
        assert sig.metadata["sh_money"] == 50.0

    def test_large_northbound_outflow(self):
        d = FlowDetector()
        event = _make_event(
            event_type=EventType.FLOW,
            data={"kind": "northbound", "north_money": -70.0},
        )
        signals = d.detect(event)
        assert len(signals) == 1
        assert signals[0].direction == "short"

    def test_northbound_below_threshold(self):
        d = FlowDetector()
        event = _make_event(
            event_type=EventType.FLOW,
            data={"kind": "northbound", "north_money": 20.0},
        )
        signals = d.detect(event)
        assert signals == []

    def test_northbound_custom_threshold(self):
        cfg = FlowDetectorConfig(northbound_threshold=10.0)
        d = FlowDetector(config=cfg)
        event = _make_event(
            event_type=EventType.FLOW,
            data={"kind": "northbound", "north_money": 15.0},
        )
        signals = d.detect(event)
        assert len(signals) == 1

    def test_northbound_strength_scales(self):
        d = FlowDetector()
        small_ev = _make_event(
            event_type=EventType.FLOW,
            data={"kind": "northbound", "north_money": 55.0},
        )
        large_ev = _make_event(
            event_type=EventType.FLOW,
            data={"kind": "northbound", "north_money": 150.0},
        )
        s_small = d.detect(small_ev)
        s_large = d.detect(large_ev)
        assert s_large[0].strength > s_small[0].strength

    def test_northbound_missing_data(self):
        d = FlowDetector()
        event = _make_event(
            event_type=EventType.FLOW,
            data={"kind": "northbound"},
        )
        assert d.detect(event) == []

    def test_northbound_preserves_trade_date(self):
        d = FlowDetector()
        event = _make_event(
            event_type=EventType.FLOW,
            data={"kind": "northbound", "north_money": 100.0, "trade_date": "20250101"},
        )
        signals = d.detect(event)
        assert signals[0].metadata["trade_date"] == "20250101"


# -- Sector Rotation --


class TestSectorRotation:
    """Tests for _detect_sector_rotation."""

    def test_basic_rotation_signal(self):
        d = FlowDetector()
        event = _make_event(data={
            "kind": "sector_snapshot",
            "sectors": [
                _sector("房地产", -8.0, pct_change=-2.0),
                _sector("半导体", 12.0, pct_change=3.0),
            ],
        })
        signals = d.detect(event)
        rotation_sigs = [s for s in signals if s.metadata.get("sub_type") == "sector_rotation"]
        assert len(rotation_sigs) >= 1
        sig = rotation_sigs[0]
        assert sig.direction == "long"
        assert sig.metadata["from_sector"] == "房地产"
        assert sig.metadata["to_sector"] == "半导体"
        assert sig.source == "flow/sector_rotation"

    def test_no_rotation_when_only_inflows(self):
        d = FlowDetector()
        event = _make_event(data={
            "kind": "sector_snapshot",
            "sectors": [
                _sector("A", 10.0),
                _sector("B", 8.0),
            ],
        })
        signals = d.detect(event)
        rotation_sigs = [s for s in signals if s.metadata.get("sub_type") == "sector_rotation"]
        assert rotation_sigs == []

    def test_no_rotation_when_only_outflows(self):
        d = FlowDetector()
        event = _make_event(data={
            "kind": "sector_snapshot",
            "sectors": [
                _sector("A", -10.0),
                _sector("B", -8.0),
            ],
        })
        signals = d.detect(event)
        rotation_sigs = [s for s in signals if s.metadata.get("sub_type") == "sector_rotation"]
        assert rotation_sigs == []

    def test_rotation_max_three_pairs(self):
        d = FlowDetector()
        event = _make_event(data={
            "kind": "sector_snapshot",
            "sectors": [
                _sector("Out1", -10.0),
                _sector("Out2", -8.0),
                _sector("Out3", -6.0),
                _sector("Out4", -5.0),
                _sector("In1", 15.0),
                _sector("In2", 12.0),
                _sector("In3", 10.0),
                _sector("In4", 8.0),
            ],
        })
        signals = d.detect(event)
        rotation_sigs = [s for s in signals if s.metadata.get("sub_type") == "sector_rotation"]
        assert len(rotation_sigs) == 3

    def test_rotation_pairs_strongest(self):
        """Strongest outflow pairs with strongest inflow."""
        d = FlowDetector()
        event = _make_event(data={
            "kind": "sector_snapshot",
            "sectors": [
                _sector("WeakOut", -4.0),
                _sector("StrongOut", -20.0),
                _sector("WeakIn", 4.0),
                _sector("StrongIn", 25.0),
            ],
        })
        signals = d.detect(event)
        rotation_sigs = [s for s in signals if s.metadata.get("sub_type") == "sector_rotation"]
        assert len(rotation_sigs) >= 1
        first = rotation_sigs[0]
        assert first.metadata["from_sector"] == "StrongOut"
        assert first.metadata["to_sector"] == "StrongIn"

    def test_rotation_below_threshold(self):
        cfg = FlowDetectorConfig(rotation_outflow_min=20.0, rotation_inflow_min=20.0)
        d = FlowDetector(config=cfg)
        event = _make_event(data={
            "kind": "sector_snapshot",
            "sectors": [
                _sector("A", -10.0),
                _sector("B", 10.0),
            ],
        })
        signals = d.detect(event)
        rotation_sigs = [s for s in signals if s.metadata.get("sub_type") == "sector_rotation"]
        assert rotation_sigs == []

    def test_single_sector_no_rotation(self):
        d = FlowDetector()
        event = _make_event(data={
            "kind": "sector_snapshot",
            "sectors": [_sector("Alone", -10.0)],
        })
        signals = d.detect(event)
        rotation_sigs = [s for s in signals if s.metadata.get("sub_type") == "sector_rotation"]
        assert rotation_sigs == []


# -- Tracked Sector Anomalies --


class TestTrackedSectorAnomalies:
    """Tests for _detect_tracked_sector_anomalies."""

    def test_tracked_sector_lower_threshold(self):
        """Tracked sectors trigger at tracked_sector_threshold, not net_inflow_threshold."""
        cfg = FlowDetectorConfig(
            net_inflow_threshold=10.0,
            tracked_sector_threshold=3.0,
            tracked_sectors=["半导体"],
        )
        d = FlowDetector(config=cfg)
        event = _make_event(data={
            "sectors": [_sector("半导体", 4.0, pct_change=1.0)]
        })
        signals = d.detect(event)
        tracked = [s for s in signals if "tracked" in s.metadata.get("sub_type", "")]
        assert len(tracked) >= 1
        assert tracked[0].metadata["sector_name"] == "半导体"

    def test_tracked_sector_skips_when_above_main_threshold(self):
        """If net_amount >= net_inflow_threshold, generic detector handles it."""
        cfg = FlowDetectorConfig(
            net_inflow_threshold=5.0,
            tracked_sector_threshold=3.0,
            tracked_sectors=["半导体"],
        )
        d = FlowDetector(config=cfg)
        event = _make_event(data={
            "sectors": [_sector("半导体", 8.0, pct_change=2.0)]
        })
        signals = d.detect(event)
        tracked = [s for s in signals if "tracked" in s.metadata.get("sub_type", "")]
        assert tracked == []

    def test_non_tracked_sector_ignored(self):
        cfg = FlowDetectorConfig(
            tracked_sectors=["半导体"],
            tracked_sector_threshold=1.0,
            net_inflow_threshold=100.0,
        )
        d = FlowDetector(config=cfg)
        event = _make_event(data={
            "sectors": [_sector("农业", 3.5)]
        })
        signals = d.detect(event)
        tracked = [s for s in signals if "tracked" in s.metadata.get("sub_type", "")]
        assert tracked == []

    def test_rank_change_detection(self):
        cfg = FlowDetectorConfig(
            tracked_sectors=["新能源"],
            rank_change_threshold=10,
            net_inflow_threshold=100.0,
            tracked_sector_threshold=0.5,
        )
        d = FlowDetector(config=cfg)
        event = _make_event(data={
            "sectors": [_sector("新能源", 1.0, rank=5, prev_rank=20)]
        })
        signals = d.detect(event)
        rank_sigs = [s for s in signals if s.metadata.get("sub_type") == "tracked_sector_rank"]
        assert len(rank_sigs) >= 1
        assert rank_sigs[0].direction == "long"
        assert rank_sigs[0].metadata["rank_change"] == 15

    def test_rank_deterioration_is_short(self):
        cfg = FlowDetectorConfig(
            tracked_sectors=["银行"],
            rank_change_threshold=10,
            net_inflow_threshold=100.0,
            tracked_sector_threshold=0.5,
        )
        d = FlowDetector(config=cfg)
        event = _make_event(data={
            "sectors": [_sector("银行", -1.0, rank=50, prev_rank=35)]
        })
        signals = d.detect(event)
        rank_sigs = [s for s in signals if s.metadata.get("sub_type") == "tracked_sector_rank"]
        assert len(rank_sigs) >= 1
        assert rank_sigs[0].direction == "short"

    def test_rank_change_below_threshold(self):
        cfg = FlowDetectorConfig(
            tracked_sectors=["银行"],
            rank_change_threshold=10,
            net_inflow_threshold=100.0,
            tracked_sector_threshold=100.0,
        )
        d = FlowDetector(config=cfg)
        event = _make_event(data={
            "sectors": [_sector("银行", 0.5, rank=10, prev_rank=12)]
        })
        signals = d.detect(event)
        tracked = [s for s in signals if "tracked" in s.metadata.get("sub_type", "")]
        assert tracked == []

    def test_default_tracked_sectors_count(self):
        assert len(DEFAULT_TRACKED_SECTORS) == 16

    def test_config_default_tracked_sectors(self):
        cfg = FlowDetectorConfig()
        assert cfg.tracked_sectors == list(DEFAULT_TRACKED_SECTORS)


# -- Event Kind Routing --


class TestEventKindRouting:
    """Test that data[kind] properly routes to sub-detectors."""

    def test_northbound_kind_only_runs_northbound(self):
        d = FlowDetector()
        event = _make_event(
            data={
                "kind": "northbound",
                "north_money": 100.0,
                "sectors": [_sector("半导体", 20.0)],
            },
        )
        signals = d.detect(event)
        assert all(s.metadata.get("sub_type") == "northbound" for s in signals)

    def test_sector_snapshot_kind(self):
        d = FlowDetector()
        event = _make_event(data={
            "kind": "sector_snapshot",
            "sectors": [
                _sector("半导体", 10.0),
                _sector("房地产", -8.0),
            ],
        })
        signals = d.detect(event)
        subtypes = {s.metadata.get("sub_type") for s in signals}
        assert "sector_anomaly" in subtypes

    def test_generic_kind_runs_all(self):
        d = FlowDetector()
        event = _make_event(data={
            "sectors": [
                _sector("半导体", 10.0),
                _sector("房地产", -8.0),
            ],
            "north_money": 100.0,
        })
        signals = d.detect(event)
        subtypes = {s.metadata.get("sub_type") for s in signals}
        assert "sector_anomaly" in subtypes
        assert "northbound" in subtypes


# -- Signal Output Quality --


class TestSignalQuality:
    """Verify signals conform to the AShareSignal schema."""

    def test_signal_is_ashare_signal(self):
        d = FlowDetector()
        event = _make_event(data={"sectors": [_sector("X", 10.0)]})
        signals = d.detect(event)
        for sig in signals:
            assert isinstance(sig, AShareSignal)

    def test_signal_market_is_a_share(self):
        d = FlowDetector()
        event = _make_event(data={"sectors": [_sector("X", 10.0)]})
        for sig in d.detect(event):
            assert sig.market == "a_share"

    def test_signal_type_is_flow(self):
        d = FlowDetector()
        event = _make_event(data={"sectors": [_sector("X", 10.0)]})
        for sig in d.detect(event):
            assert sig.signal_type == "flow"

    def test_strength_in_range(self):
        d = FlowDetector()
        event = _make_event(data={"sectors": [_sector("X", 50.0)]})
        for sig in d.detect(event):
            assert 0.0 <= sig.strength <= 1.0

    def test_confidence_in_range(self):
        d = FlowDetector()
        event = _make_event(data={"sectors": [_sector("X", 10.0)]})
        for sig in d.detect(event):
            assert 0.0 <= sig.confidence <= 1.0

    def test_timestamp_propagated(self):
        ts = datetime(2025, 6, 1, 12, 0, tzinfo=timezone.utc)
        d = FlowDetector()
        event = _make_event(data={"sectors": [_sector("X", 10.0)]}, timestamp=ts)
        for sig in d.detect(event):
            assert sig.timestamp == ts

    def test_signal_serialization(self):
        """Signals should serialize/deserialize cleanly."""
        d = FlowDetector()
        event = _make_event(data={"sectors": [_sector("X", 10.0)]})
        for sig in d.detect(event):
            json_str = sig.to_json()
            restored = AShareSignal.from_json(json_str)
            assert restored.asset == sig.asset
            assert restored.direction == sig.direction


# -- Config --


class TestFlowDetectorConfig:
    """Test configuration and threshold customisation."""

    def test_default_config(self):
        cfg = FlowDetectorConfig()
        assert cfg.net_inflow_threshold == 5.0
        assert cfg.northbound_threshold == 50.0
        assert cfg.rank_change_threshold == 10
        assert cfg.rotation_outflow_min == 3.0
        assert cfg.rotation_inflow_min == 3.0
        assert cfg.tracked_sector_threshold == 3.0
        assert cfg.base_confidence == 0.7

    def test_custom_config(self):
        cfg = FlowDetectorConfig(
            net_inflow_threshold=1.0,
            northbound_threshold=10.0,
            tracked_sectors=["A", "B"],
        )
        assert cfg.net_inflow_threshold == 1.0
        assert cfg.tracked_sectors == ["A", "B"]

    def test_detector_uses_config(self):
        cfg = FlowDetectorConfig(net_inflow_threshold=1.0)
        d = FlowDetector(config=cfg)
        assert d._config.net_inflow_threshold == 1.0


# -- Error Handling --


class TestErrorHandling:
    """Ensure detector is resilient to bad data."""

    def test_none_data_values(self):
        d = FlowDetector()
        event = _make_event(data={
            "sectors": [{"name": "X", "net_amount": None, "pct_change": None, "ts_code": "X"}]
        })
        signals = d.detect(event)
        assert isinstance(signals, list)

    def test_missing_name_field(self):
        d = FlowDetector()
        event = _make_event(data={
            "sectors": [{"net_amount": 10.0, "ts_code": "X"}]
        })
        signals = d.detect(event)
        assert isinstance(signals, list)

    def test_corrupt_sector_data(self):
        """Even if one sector dict is malformed, others should process."""
        d = FlowDetector()
        event = _make_event(data={
            "sectors": [
                "not_a_dict",
                _sector("Good", 10.0),
            ]
        })
        signals = d.detect(event)
        assert isinstance(signals, list)

    def test_northbound_non_numeric(self):
        d = FlowDetector()
        event = _make_event(
            event_type=EventType.FLOW,
            data={"kind": "northbound", "north_money": "not_a_number"},
        )
        signals = d.detect(event)
        assert isinstance(signals, list)


# -- Integration-like scenarios --


class TestEndToEndScenarios:
    """Full event-to-signal scenarios mimicking real market data."""

    def test_full_market_snapshot(self):
        """Simulate a real industry_daily snapshot with mixed sectors."""
        d = FlowDetector()
        sectors = [
            _sector("半导体", 15.5, pct_change=3.2, ts_code="885566.TI"),
            _sector("新能源", 8.0, pct_change=1.5, ts_code="885567.TI"),
            _sector("银行", -12.0, pct_change=-1.8, ts_code="885568.TI"),
            _sector("房地产", -6.5, pct_change=-2.1, ts_code="885569.TI"),
            _sector("食品饮料", 2.0, pct_change=0.5, ts_code="885570.TI"),
            _sector("计算机", 0.3, pct_change=0.1, ts_code="885571.TI"),
        ]
        event = _make_event(data={"kind": "sector_snapshot", "sectors": sectors})
        signals = d.detect(event)

        anomaly_sigs = [s for s in signals if s.metadata.get("sub_type") == "sector_anomaly"]
        assert len(anomaly_sigs) >= 3

        rotation_sigs = [s for s in signals if s.metadata.get("sub_type") == "sector_rotation"]
        assert len(rotation_sigs) >= 1

    def test_northbound_panic_sell(self):
        """Simulate a northbound panic selling day."""
        d = FlowDetector()
        event = _make_event(
            event_type=EventType.FLOW,
            data={
                "kind": "northbound",
                "north_money": -180.0,
                "sh_money": -120.0,
                "sz_money": -60.0,
                "trade_date": "20250601",
            },
        )
        signals = d.detect(event)
        assert len(signals) == 1
        sig = signals[0]
        assert sig.direction == "short"
        assert sig.strength > 0.5
        assert sig.north_flow == -180.0

    def test_tracked_sector_early_warning(self):
        """A tracked sector with moderate flow that wouldn't trigger generic detection."""
        cfg = FlowDetectorConfig(
            net_inflow_threshold=10.0,
            tracked_sector_threshold=3.0,
            tracked_sectors=["军工"],
        )
        d = FlowDetector(config=cfg)
        event = _make_event(data={
            "sectors": [_sector("军工", 4.5, pct_change=1.2, ts_code="885572.TI")]
        })
        signals = d.detect(event)
        tracked = [s for s in signals if "tracked" in s.metadata.get("sub_type", "")]
        assert len(tracked) == 1
        assert tracked[0].metadata["sector_name"] == "军工"

    def test_combined_northbound_and_sectors(self):
        """Generic event with both northbound and sector data."""
        d = FlowDetector()
        event = _make_event(data={
            "north_money": 100.0,
            "sectors": [_sector("半导体", 10.0)],
        })
        signals = d.detect(event)
        subtypes = {s.metadata.get("sub_type") for s in signals}
        assert "northbound" in subtypes
        assert "sector_anomaly" in subtypes
