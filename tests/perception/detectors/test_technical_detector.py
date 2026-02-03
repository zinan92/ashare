"""Tests for TechnicalDetector.

Uses synthetic bar data to verify MA crossover, RSI, MACD, and
volume breakout detection.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

import pytest

from src.perception.detectors.technical_detector import (
    TechnicalDetector,
    _ema,
    _macd,
    _rsi,
    _sma,
)
from src.perception.events import (
    EventSource,
    EventType,
    MarketScope,
    RawMarketEvent,
)
from src.perception.signals import Direction, Market, SignalType


# ── Fixtures ─────────────────────────────────────────────────────────

TS = datetime(2025, 1, 15, 9, 30, tzinfo=timezone.utc)


def _make_event(
    bars: List[Dict[str, Any]],
    symbol: str = "600519",
    event_type: EventType = EventType.KLINE,
) -> RawMarketEvent:
    return RawMarketEvent(
        source=EventSource.TUSHARE,
        event_type=event_type,
        market=MarketScope.CN_STOCK,
        symbol=symbol,
        data={"bars": bars},
        timestamp=TS,
    )


def _bars_from_closes(closes: List[float], volume: float = 1000.0) -> List[Dict]:
    return [{"close": c, "volume": volume} for c in closes]


# ── Interface compliance ─────────────────────────────────────────────


class TestInterface:
    def test_name(self):
        d = TechnicalDetector()
        assert d.name == "technical"

    def test_accepts(self):
        d = TechnicalDetector()
        assert EventType.KLINE in d.accepts
        assert EventType.PRICE_UPDATE in d.accepts

    def test_empty_event(self):
        d = TechnicalDetector()
        event = _make_event([])
        assert d.detect(event) == []

    def test_no_bars_key(self):
        d = TechnicalDetector()
        event = RawMarketEvent(
            source=EventSource.TUSHARE,
            event_type=EventType.KLINE,
            market=MarketScope.CN_STOCK,
            symbol="000001",
            data={},
            timestamp=TS,
        )
        assert d.detect(event) == []


# ── Helper functions ─────────────────────────────────────────────────


class TestHelpers:
    def test_sma_basic(self):
        assert _sma([1, 2, 3, 4, 5], 3) == pytest.approx(4.0)

    def test_sma_insufficient(self):
        assert _sma([1, 2], 5) is None

    def test_ema_basic(self):
        vals = [10.0] * 10
        assert _ema(vals, 5) == pytest.approx(10.0)

    def test_ema_insufficient(self):
        assert _ema([1.0], 5) is None

    def test_rsi_all_gains(self):
        closes = list(range(1, 20))  # 1..19, all gains
        r = _rsi(closes, 14)
        assert r is not None
        assert r == pytest.approx(100.0)

    def test_rsi_all_losses(self):
        closes = list(range(20, 1, -1))  # 20..2, all losses
        r = _rsi(closes, 14)
        assert r is not None
        assert r == pytest.approx(0.0, abs=0.5)

    def test_rsi_insufficient(self):
        assert _rsi([1, 2, 3], 14) is None


# ── MA Crossover ─────────────────────────────────────────────────────


class TestMACross:
    def test_golden_cross_ma5_ma10(self):
        """MA5 crosses above MA10 on the last bar."""
        d = TechnicalDetector()
        # 10 bars where MA5 < MA10, then one bar that flips it
        closes = [10.0] * 10 + [9.0] * 5 + [10.0, 10.0, 10.5, 11.0, 11.5, 12.0]
        bars = _bars_from_closes(closes)
        event = _make_event(bars)
        signals = d.detect(event)
        ma_sigs = [s for s in signals if s.metadata.get("sub_type") == "ma_cross"]
        golden = [s for s in ma_sigs if s.metadata.get("cross") == "golden_cross"]
        assert len(golden) > 0
        assert golden[0].direction == Direction.LONG
        assert golden[0].signal_type == SignalType.TECHNICAL

    def test_death_cross_ma5_ma10(self):
        """MA5 crosses below MA10 on the last bar."""
        d = TechnicalDetector()
        closes = [10.0] * 10 + [11.0] * 5 + [10.0, 10.0, 9.5, 9.0, 8.5, 8.0]
        bars = _bars_from_closes(closes)
        event = _make_event(bars)
        signals = d.detect(event)
        ma_sigs = [s for s in signals if s.metadata.get("sub_type") == "ma_cross"]
        death = [s for s in ma_sigs if s.metadata.get("cross") == "death_cross"]
        assert len(death) > 0
        assert death[0].direction == Direction.SHORT

    def test_no_cross_when_flat(self):
        """No cross when prices are flat."""
        d = TechnicalDetector()
        closes = [10.0] * 25
        bars = _bars_from_closes(closes)
        event = _make_event(bars)
        signals = d.detect(event)
        ma_sigs = [s for s in signals if s.metadata.get("sub_type") == "ma_cross"]
        assert len(ma_sigs) == 0

    def test_insufficient_data(self):
        d = TechnicalDetector()
        closes = [10.0] * 5
        bars = _bars_from_closes(closes)
        event = _make_event(bars)
        signals = d.detect(event)
        ma_sigs = [s for s in signals if s.metadata.get("sub_type") == "ma_cross"]
        assert len(ma_sigs) == 0


# ── RSI ──────────────────────────────────────────────────────────────


class TestRSI:
    def test_overbought(self):
        d = TechnicalDetector()
        # Steady climb → RSI > 70
        closes = [50 + i * 0.5 for i in range(30)]
        bars = _bars_from_closes(closes)
        event = _make_event(bars)
        signals = d.detect(event)
        rsi_sigs = [s for s in signals if s.metadata.get("sub_type") == "rsi"]
        assert len(rsi_sigs) == 1
        assert rsi_sigs[0].metadata["condition"] == "overbought"
        assert rsi_sigs[0].direction == Direction.SHORT

    def test_oversold(self):
        d = TechnicalDetector()
        # Steady decline → RSI < 30
        closes = [50 - i * 0.5 for i in range(30)]
        bars = _bars_from_closes(closes)
        event = _make_event(bars)
        signals = d.detect(event)
        rsi_sigs = [s for s in signals if s.metadata.get("sub_type") == "rsi"]
        assert len(rsi_sigs) == 1
        assert rsi_sigs[0].metadata["condition"] == "oversold"
        assert rsi_sigs[0].direction == Direction.LONG

    def test_neutral_no_signal(self):
        d = TechnicalDetector()
        # Alternating up/down → RSI near 50
        closes = [50 + (0.1 if i % 2 == 0 else -0.1) for i in range(30)]
        bars = _bars_from_closes(closes)
        event = _make_event(bars)
        signals = d.detect(event)
        rsi_sigs = [s for s in signals if s.metadata.get("sub_type") == "rsi"]
        assert len(rsi_sigs) == 0


# ── MACD ─────────────────────────────────────────────────────────────


class TestMACD:
    def test_golden_cross(self):
        """Single-jump synthetic data: flat bars then one big up bar → hist flips positive."""
        d = TechnicalDetector()
        # 50 flat bars at 10.0 → hist ≈ 0 / slightly negative
        # then jump to 15.0 on the last bar
        flat = [10.0] * 50
        bars_prev = _bars_from_closes(flat)
        event_prev = _make_event(bars_prev)
        prev_signals = d.detect(event_prev)
        prev_macd = [s for s in prev_signals if s.metadata.get("sub_type") == "macd"]
        # flat data should give no MACD cross
        assert len(prev_macd) == 0

        # Now add the jump bar
        jumped = flat + [15.0]
        bars = _bars_from_closes(jumped)
        event = _make_event(bars)
        signals = d.detect(event)
        macd_sigs = [s for s in signals if s.metadata.get("sub_type") == "macd"]
        assert len(macd_sigs) == 1
        assert macd_sigs[0].direction == Direction.LONG
        assert macd_sigs[0].metadata["cross"] == "golden_cross"
        assert macd_sigs[0].metadata["hist"] > 0

    def test_death_cross(self):
        """Single-jump synthetic data: flat bars then one big down bar → hist flips negative."""
        d = TechnicalDetector()
        flat = [10.0] * 50
        dropped = flat + [5.0]
        bars = _bars_from_closes(dropped)
        event = _make_event(bars)
        signals = d.detect(event)
        macd_sigs = [s for s in signals if s.metadata.get("sub_type") == "macd"]
        assert len(macd_sigs) == 1
        assert macd_sigs[0].direction == Direction.SHORT
        assert macd_sigs[0].metadata["cross"] == "death_cross"
        assert macd_sigs[0].metadata["hist"] < 0

    def test_insufficient_data(self):
        d = TechnicalDetector()
        closes = [10.0] * 20
        bars = _bars_from_closes(closes)
        event = _make_event(bars)
        signals = d.detect(event)
        macd_sigs = [s for s in signals if s.metadata.get("sub_type") == "macd"]
        assert len(macd_sigs) == 0


# ── Volume Breakout ──────────────────────────────────────────────────


class TestVolumeBreakout:
    def test_breakout(self):
        d = TechnicalDetector()
        # 20 normal bars + 1 spike bar at 3× average
        closes = [10.0] * 21
        bars = [{"close": 10.0, "volume": 1000.0} for _ in range(20)]
        bars.append({"close": 10.0, "volume": 3000.0})
        event = _make_event(bars)
        signals = d.detect(event)
        vol_sigs = [s for s in signals if s.metadata.get("sub_type") == "volume_breakout"]
        assert len(vol_sigs) == 1
        assert vol_sigs[0].direction == Direction.LONG
        assert vol_sigs[0].metadata["ratio"] == pytest.approx(3.0)

    def test_no_breakout(self):
        d = TechnicalDetector()
        bars = [{"close": 10.0, "volume": 1000.0} for _ in range(21)]
        event = _make_event(bars)
        signals = d.detect(event)
        vol_sigs = [s for s in signals if s.metadata.get("sub_type") == "volume_breakout"]
        assert len(vol_sigs) == 0

    def test_insufficient_volume_data(self):
        d = TechnicalDetector()
        bars = [{"close": 10.0, "volume": 1000.0} for _ in range(5)]
        event = _make_event(bars)
        signals = d.detect(event)
        vol_sigs = [s for s in signals if s.metadata.get("sub_type") == "volume_breakout"]
        assert len(vol_sigs) == 0

    def test_zero_average_volume(self):
        d = TechnicalDetector()
        bars = [{"close": 10.0, "volume": 0.0} for _ in range(20)]
        bars.append({"close": 10.0, "volume": 5000.0})
        event = _make_event(bars)
        signals = d.detect(event)
        vol_sigs = [s for s in signals if s.metadata.get("sub_type") == "volume_breakout"]
        assert len(vol_sigs) == 0


# ── Signal metadata & market resolution ──────────────────────────────


class TestMetadata:
    def test_signal_source_prefix(self):
        d = TechnicalDetector()
        closes = [50 + i * 0.5 for i in range(30)]
        bars = _bars_from_closes(closes)
        event = _make_event(bars)
        signals = d.detect(event)
        for s in signals:
            assert s.source.startswith("technical/")

    def test_market_resolution_us(self):
        d = TechnicalDetector()
        closes = [50 + i * 0.5 for i in range(30)]
        bars = _bars_from_closes(closes)
        event = _make_event(bars, symbol="AAPL")
        event.market = MarketScope.US_STOCK
        signals = d.detect(event)
        for s in signals:
            assert s.market == Market.US_STOCK

    def test_price_update_accepted(self):
        d = TechnicalDetector()
        closes = [50 + i * 0.5 for i in range(30)]
        bars = _bars_from_closes(closes)
        event = _make_event(bars, event_type=EventType.PRICE_UPDATE)
        signals = d.detect(event)
        assert len(signals) > 0

    def test_strength_clamped(self):
        d = TechnicalDetector()
        closes = [50 + i * 0.5 for i in range(30)]
        bars = _bars_from_closes(closes)
        event = _make_event(bars)
        signals = d.detect(event)
        for s in signals:
            assert 0.0 <= s.strength <= 1.0

    def test_confidence_in_range(self):
        d = TechnicalDetector()
        closes = [50 + i * 0.5 for i in range(30)]
        bars = _bars_from_closes(closes)
        event = _make_event(bars)
        signals = d.detect(event)
        for s in signals:
            assert 0.0 <= s.confidence <= 1.0
