"""Tests for Perception Layer core interfaces.

Covers:
- RawMarketEvent model
- EventSource / EventType / MarketScope enums
- SourceHealth / HealthMonitor / HealthStatus
- UnifiedSignal and market-specific subclasses
- DataSource ABC contract
- Detector ABC contract
- SourceRegistry
- PerceptionConfig and sub-configs
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import List

import pytest
from pydantic import ValidationError

# ── Events ───────────────────────────────────────────────────────────

from src.perception.events import (
    EventSource,
    EventType,
    MarketScope,
    RawMarketEvent,
)


class TestRawMarketEvent:
    def _make_event(self, **overrides):
        defaults = dict(
            source=EventSource.TUSHARE,
            event_type=EventType.KLINE,
            market=MarketScope.CN_STOCK,
            symbol="000001",
            data={"close": 15.5},
            timestamp=datetime.now(timezone.utc),
        )
        defaults.update(overrides)
        return RawMarketEvent(**defaults)

    def test_create_minimal(self):
        ev = self._make_event()
        assert ev.source == "tushare"
        assert ev.event_type == "kline"
        assert ev.market == "cn_stock"
        assert ev.symbol == "000001"
        assert ev.data == {"close": 15.5}
        assert ev.event_id  # auto-generated

    def test_event_id_auto_generated(self):
        a = self._make_event()
        b = self._make_event()
        assert a.event_id != b.event_id

    def test_received_at_auto(self):
        ev = self._make_event()
        assert ev.received_at is not None

    def test_latency_ms(self):
        ts = datetime.now(timezone.utc) - timedelta(seconds=1)
        ev = self._make_event(timestamp=ts)
        assert ev.latency_ms >= 900  # at least ~900ms

    def test_symbol_optional(self):
        ev = self._make_event(symbol=None)
        assert ev.symbol is None

    def test_all_event_sources(self):
        for src in EventSource:
            ev = self._make_event(source=src)
            assert ev.source == src.value

    def test_all_event_types(self):
        for et in EventType:
            ev = self._make_event(event_type=et)
            assert ev.event_type == et.value


# ── Health ───────────────────────────────────────────────────────────

from src.perception.health import HealthMonitor, HealthStatus, SourceHealth


class TestSourceHealth:
    def test_defaults(self):
        h = SourceHealth(source_name="test")
        assert h.status == "unknown"
        assert h.error_rate == 0.0
        assert h.consecutive_failures == 0

    def test_healthy_snapshot(self):
        h = SourceHealth(
            source_name="tushare",
            status=HealthStatus.HEALTHY,
            latency_ms=42.0,
            error_rate=0.01,
            total_polls=100,
            total_events=950,
        )
        assert h.status == "healthy"
        assert h.latency_ms == 42.0

    def test_error_rate_bounds(self):
        with pytest.raises(ValidationError):
            SourceHealth(source_name="x", error_rate=1.5)
        with pytest.raises(ValidationError):
            SourceHealth(source_name="x", error_rate=-0.1)


class TestHealthMonitor:
    def test_empty_report(self):
        m = HealthMonitor()
        assert m.report() == {}
        assert not m.all_healthy

    def test_update_and_get(self):
        m = HealthMonitor()
        h = SourceHealth(source_name="sina", status=HealthStatus.HEALTHY)
        m.update("sina", h)
        assert m.get("sina") is h
        assert m.get("missing") is None

    def test_all_healthy(self):
        m = HealthMonitor()
        m.update("a", SourceHealth(source_name="a", status=HealthStatus.HEALTHY))
        m.update("b", SourceHealth(source_name="b", status=HealthStatus.HEALTHY))
        assert m.all_healthy

    def test_unhealthy_sources(self):
        m = HealthMonitor()
        m.update("a", SourceHealth(source_name="a", status=HealthStatus.HEALTHY))
        m.update("b", SourceHealth(source_name="b", status=HealthStatus.UNHEALTHY))
        assert not m.all_healthy
        assert m.unhealthy_sources == ["b"]


# ── Signals ──────────────────────────────────────────────────────────

from src.perception.signals import (
    AShareSignal,
    CommoditySignal,
    CryptoSignal,
    Direction,
    Market,
    SignalType,
    UnifiedSignal,
    USStockSignal,
)


class TestUnifiedSignal:
    def _make_signal(self, **overrides):
        defaults = dict(
            market=Market.A_SHARE,
            asset="000001",
            direction=Direction.LONG,
            strength=0.8,
            confidence=0.7,
            signal_type=SignalType.TECHNICAL,
            source="test_detector",
        )
        defaults.update(overrides)
        return UnifiedSignal(**defaults)

    def test_create(self):
        s = self._make_signal()
        assert s.market == "a_share"
        assert s.direction == "long"
        assert s.signal_id

    def test_strength_bounds(self):
        with pytest.raises(ValidationError):
            self._make_signal(strength=1.5)
        with pytest.raises(ValidationError):
            self._make_signal(strength=-0.1)

    def test_confidence_bounds(self):
        with pytest.raises(ValidationError):
            self._make_signal(confidence=2.0)

    def test_expires_at_must_be_after_timestamp(self):
        ts = datetime.now(timezone.utc)
        with pytest.raises(ValidationError):
            self._make_signal(
                timestamp=ts,
                expires_at=ts - timedelta(hours=1),
            )

    def test_is_expired(self):
        s = self._make_signal(
            expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
            timestamp=datetime.now(timezone.utc) - timedelta(hours=1),
        )
        assert s.is_expired

    def test_not_expired_when_none(self):
        s = self._make_signal()
        assert not s.is_expired

    def test_to_dict_roundtrip(self):
        s = self._make_signal()
        d = s.to_dict()
        assert isinstance(d, dict)
        s2 = UnifiedSignal.from_dict(d)
        assert s2.signal_id == s.signal_id

    def test_to_json_roundtrip(self):
        s = self._make_signal()
        j = s.to_json()
        s2 = UnifiedSignal.from_json(j)
        assert s2.asset == s.asset


class TestMarketSignals:
    def test_ashare_signal(self):
        s = AShareSignal(
            asset="600519",
            direction=Direction.LONG,
            strength=0.9,
            confidence=0.85,
            signal_type=SignalType.FLOW,
            source="north_flow_detector",
            north_flow=12.5,
            concept_codes=["AI", "半导体"],
        )
        assert s.market == "a_share"
        assert s.north_flow == 12.5
        assert "AI" in s.concept_codes

    def test_crypto_signal(self):
        s = CryptoSignal(
            asset="BTC-PERP",
            direction=Direction.SHORT,
            strength=0.6,
            confidence=0.5,
            signal_type=SignalType.TECHNICAL,
            source="btc_detector",
            funding_rate=-0.001,
        )
        assert s.market == "crypto"
        assert s.funding_rate == -0.001

    def test_us_stock_signal(self):
        s = USStockSignal(
            asset="AAPL",
            direction=Direction.LONG,
            strength=0.7,
            confidence=0.8,
            signal_type=SignalType.FUNDAMENTAL,
            source="earnings",
            earnings_surprise=5.2,
        )
        assert s.market == "us_stock"

    def test_commodity_signal(self):
        s = CommoditySignal(
            asset="CL",
            direction=Direction.SHORT,
            strength=0.5,
            confidence=0.6,
            signal_type=SignalType.SENTIMENT,
            source="oil_detector",
            contract_month="2025-08",
        )
        assert s.market == "commodity"


# ── DataSource ABC ───────────────────────────────────────────────────

from src.perception.sources.base import DataSource, SourceType


class _DummySource(DataSource):
    """Minimal concrete implementation for testing the ABC."""

    def __init__(self, healthy: bool = True):
        self._healthy = healthy
        self._connected = False

    @property
    def name(self) -> str:
        return "dummy"

    @property
    def source_type(self) -> SourceType:
        return SourceType.POLLING

    async def connect(self) -> None:
        self._connected = True

    async def poll(self) -> List[RawMarketEvent]:
        return [
            RawMarketEvent(
                source=EventSource.MANUAL,
                event_type=EventType.PRICE_UPDATE,
                market=MarketScope.CN_STOCK,
                symbol="000001",
                data={"price": 10.0},
                timestamp=datetime.now(timezone.utc),
            )
        ]

    async def disconnect(self) -> None:
        self._connected = False

    def health(self) -> SourceHealth:
        return SourceHealth(
            source_name=self.name,
            status=HealthStatus.HEALTHY if self._healthy else HealthStatus.UNHEALTHY,
        )


class TestDataSourceABC:
    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError):
            DataSource()  # type: ignore[abstract]

    def test_concrete_source(self):
        src = _DummySource()
        assert src.name == "dummy"
        assert src.source_type == SourceType.POLLING

    def test_poll(self):
        src = _DummySource()
        events = asyncio.run(src.poll())
        assert len(events) == 1
        assert events[0].symbol == "000001"

    def test_connect_disconnect(self):
        src = _DummySource()
        asyncio.run(src.connect())
        assert src._connected
        asyncio.run(src.disconnect())
        assert not src._connected

    def test_health(self):
        src = _DummySource(healthy=True)
        h = src.health()
        assert h.status == "healthy"


# ── Detector ABC ─────────────────────────────────────────────────────

from src.perception.detectors.base import Detector


class _DummyDetector(Detector):
    @property
    def name(self) -> str:
        return "dummy_detector"

    @property
    def accepts(self) -> List[EventType]:
        return [EventType.KLINE, EventType.PRICE_UPDATE]

    def detect(self, event: RawMarketEvent) -> List[UnifiedSignal]:
        if event.event_type == EventType.PRICE_UPDATE.value:
            return [
                UnifiedSignal(
                    market=Market.A_SHARE,
                    asset=event.symbol or "UNKNOWN",
                    direction=Direction.LONG,
                    strength=0.5,
                    confidence=0.5,
                    signal_type=SignalType.TECHNICAL,
                    source=self.name,
                )
            ]
        return []


class TestDetectorABC:
    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError):
            Detector()  # type: ignore[abstract]

    def test_concrete_detector(self):
        d = _DummyDetector()
        assert d.name == "dummy_detector"
        assert EventType.KLINE in d.accepts

    def test_detect_returns_signals(self):
        d = _DummyDetector()
        ev = RawMarketEvent(
            source=EventSource.SINA,
            event_type=EventType.PRICE_UPDATE,
            market=MarketScope.CN_STOCK,
            symbol="600519",
            data={"price": 1800},
            timestamp=datetime.now(timezone.utc),
        )
        signals = d.detect(ev)
        assert len(signals) == 1
        assert signals[0].asset == "600519"

    def test_detect_returns_empty_for_non_matching(self):
        d = _DummyDetector()
        ev = RawMarketEvent(
            source=EventSource.CLS,
            event_type=EventType.NEWS,
            market=MarketScope.CN_STOCK,
            data={"headline": "test"},
            timestamp=datetime.now(timezone.utc),
        )
        assert d.detect(ev) == []


# ── SourceRegistry ───────────────────────────────────────────────────

from src.perception.sources.registry import SourceRegistry


class TestSourceRegistry:
    def test_empty(self):
        reg = SourceRegistry()
        assert len(reg) == 0
        assert reg.all() == []

    def test_register_and_get(self):
        reg = SourceRegistry()
        src = _DummySource()
        reg.register(src)
        assert "dummy" in reg
        assert reg.get("dummy") is src
        assert reg.names == ["dummy"]

    def test_unregister(self):
        reg = SourceRegistry()
        src = _DummySource()
        reg.register(src)
        removed = reg.unregister("dummy")
        assert removed is src
        assert len(reg) == 0

    def test_unregister_missing(self):
        reg = SourceRegistry()
        assert reg.unregister("nope") is None

    def test_health_report(self):
        reg = SourceRegistry()
        reg.register(_DummySource(healthy=True))
        report = reg.health_report()
        assert "dummy" in report
        assert report["dummy"].status == "healthy"


# ── Config ───────────────────────────────────────────────────────────

from src.perception.config import (
    CircuitBreakerConfig,
    DetectorConfig,
    PerceptionConfig,
    SourcePollConfig,
)


class TestPerceptionConfig:
    def test_defaults(self):
        cfg = PerceptionConfig()
        assert cfg.sources == []
        assert cfg.detectors == []
        assert cfg.circuit_breaker.failure_threshold == 5

    def test_source_poll_config(self):
        spc = SourcePollConfig(source_name="tushare", poll_interval_seconds=30)
        assert spc.enabled
        assert spc.poll_interval_seconds == 30

    def test_circuit_breaker_config(self):
        cb = CircuitBreakerConfig(failure_threshold=10, recovery_timeout_seconds=600)
        assert cb.failure_threshold == 10

    def test_detector_config(self):
        dc = DetectorConfig(detector_name="momentum", min_confidence=0.5)
        assert dc.min_confidence == 0.5

    def test_full_config(self):
        cfg = PerceptionConfig(
            sources=[
                SourcePollConfig(source_name="tushare", poll_interval_seconds=60),
                SourcePollConfig(source_name="sina", poll_interval_seconds=5),
            ],
            detectors=[
                DetectorConfig(detector_name="momentum", min_confidence=0.4),
            ],
            circuit_breaker=CircuitBreakerConfig(failure_threshold=3),
            max_event_age_seconds=120,
        )
        assert len(cfg.sources) == 2
        assert len(cfg.detectors) == 1
        assert cfg.max_event_age_seconds == 120

    def test_invalid_poll_interval(self):
        with pytest.raises(ValidationError):
            SourcePollConfig(source_name="x", poll_interval_seconds=0)

    def test_invalid_min_confidence(self):
        with pytest.raises(ValidationError):
            DetectorConfig(detector_name="x", min_confidence=1.5)
