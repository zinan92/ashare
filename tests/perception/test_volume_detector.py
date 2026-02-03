"""Tests for VolumeDetector."""

import pytest
from datetime import datetime, timezone

from src.perception.detectors.volume_detector import VolumeDetector, VolumeDetectorConfig
from src.perception.events import EventType, MarketScope, RawMarketEvent
from src.perception.signals import Direction, Market


def _make_event(bars, symbol="TEST.SH"):
    return RawMarketEvent(
        source="tushare",
        event_type=EventType.KLINE,
        market=MarketScope.CN_STOCK,
        symbol=symbol,
        data={"bars": bars},
        timestamp=datetime.now(timezone.utc),
    )


def _make_bars(closes, volumes, opens=None, highs=None, lows=None):
    n = len(closes)
    opens = opens or closes
    highs = highs or [c + 0.5 for c in closes]
    lows = lows or [c - 0.5 for c in closes]
    return [
        {"open": o, "high": h, "low": l, "close": c, "volume": v}
        for o, h, l, c, v in zip(opens, highs, lows, closes, volumes)
    ]


class TestVolumeDetectorBasic:
    def test_empty_bars(self):
        d = VolumeDetector()
        event = _make_event([])
        assert d.detect(event) == []

    def test_name_and_accepts(self):
        d = VolumeDetector()
        assert d.name == "volume"
        assert EventType.KLINE in d.accepts


class TestVolumeSurge:
    def test_basic_surge(self):
        """Volume 3x average → surge signal."""
        closes = [10.0] * 22
        closes[-1] = 10.5  # price up slightly
        volumes = [1_000_000] * 21 + [3_000_000]  # 3x spike
        bars = _make_bars(closes, volumes)
        event = _make_event(bars)

        d = VolumeDetector(VolumeDetectorConfig(surge_ratio=2.0))
        signals = d.detect(event)

        surges = [s for s in signals if "surge" in s.source]
        assert len(surges) > 0
        assert surges[0].metadata.get("volume_ratio", 0) >= 2.0

    def test_extreme_surge(self):
        """Volume 5x average → extreme surge."""
        closes = [10.0] * 22
        closes[-1] = 10.5
        volumes = [1_000_000] * 21 + [5_000_000]
        bars = _make_bars(closes, volumes)
        event = _make_event(bars)

        d = VolumeDetector(VolumeDetectorConfig(surge_ratio=2.0, extreme_surge_ratio=4.0))
        signals = d.detect(event)

        extreme = [s for s in signals if "extreme" in s.source]
        assert len(extreme) > 0

    def test_no_surge_normal_volume(self):
        """Normal volume → no surge signal."""
        closes = [10.0] * 22
        volumes = [1_000_000] * 22
        bars = _make_bars(closes, volumes)
        event = _make_event(bars)

        d = VolumeDetector(VolumeDetectorConfig(surge_ratio=2.0))
        signals = d.detect(event)

        surges = [s for s in signals if "surge" in s.source]
        assert len(surges) == 0


class TestVolumePriceDivergence:
    def test_bearish_divergence(self):
        """Price up + volume shrink → bearish divergence."""
        closes = [10.0] * 21 + [10.5]  # price up
        volumes = [1_000_000] * 21 + [400_000]  # volume only 40% of avg
        bars = _make_bars(closes, volumes)
        event = _make_event(bars)

        d = VolumeDetector(VolumeDetectorConfig(
            divergence_price_threshold=1.0,
            divergence_vol_ratio_low=0.6,
        ))
        signals = d.detect(event)

        diverge = [s for s in signals if "divergence" in s.source]
        assert len(diverge) > 0
        assert diverge[0].direction == Direction.SHORT

    def test_selling_exhaustion(self):
        """Price down + volume shrink → selling exhaustion (bullish)."""
        closes = [10.0] * 21 + [9.5]  # price down
        volumes = [1_000_000] * 21 + [400_000]  # volume only 40%
        bars = _make_bars(closes, volumes)
        event = _make_event(bars)

        d = VolumeDetector(VolumeDetectorConfig(
            divergence_price_threshold=1.0,
            divergence_vol_ratio_low=0.6,
        ))
        signals = d.detect(event)

        exhaustion = [s for s in signals if "exhaustion" in s.source]
        assert len(exhaustion) > 0
        assert exhaustion[0].direction == Direction.LONG

    def test_panic_selling(self):
        """Price down + volume surge → panic selling."""
        closes = [10.0] * 21 + [9.3]  # big drop
        volumes = [1_000_000] * 21 + [2_000_000]  # volume 2x
        bars = _make_bars(closes, volumes)
        event = _make_event(bars)

        d = VolumeDetector(VolumeDetectorConfig(
            divergence_price_threshold=1.0,
            divergence_vol_ratio_high=1.5,
        ))
        signals = d.detect(event)

        panic = [s for s in signals if "panic" in s.source]
        assert len(panic) > 0
        assert panic[0].direction == Direction.SHORT


class TestVolumeClimax:
    def test_bearish_climax(self):
        """Extreme volume + long upper wick → bearish climax."""
        closes = [10.0] * 21 + [10.3]  # price up
        volumes = [1_000_000] * 21 + [4_000_000]  # 4x avg
        opens = [10.0] * 21 + [10.0]
        highs = [10.5] * 21 + [11.0]   # long upper wick (high=11, close=10.3)
        lows = [9.5] * 21 + [10.0]
        bars = _make_bars(closes, volumes, opens=opens, highs=highs, lows=lows)
        event = _make_event(bars)

        d = VolumeDetector(VolumeDetectorConfig(climax_vol_ratio=3.0))
        signals = d.detect(event)

        climax = [s for s in signals if "climax" in s.source]
        assert len(climax) > 0
        assert climax[0].direction == Direction.SHORT

    def test_bullish_climax(self):
        """Extreme volume + long lower wick → bullish climax."""
        closes = [10.0] * 21 + [9.8]   # price down slightly
        volumes = [1_000_000] * 21 + [4_000_000]
        opens = [10.0] * 21 + [10.0]
        highs = [10.5] * 21 + [10.1]
        lows = [9.5] * 21 + [9.0]      # long lower wick
        bars = _make_bars(closes, volumes, opens=opens, highs=highs, lows=lows)
        event = _make_event(bars)

        d = VolumeDetector(VolumeDetectorConfig(climax_vol_ratio=3.0))
        signals = d.detect(event)

        climax = [s for s in signals if "climax" in s.source]
        assert len(climax) > 0
        assert climax[0].direction == Direction.LONG


class TestShrinkage:
    def test_extreme_shrinkage(self):
        """Volume at 20% of average → extreme shrinkage."""
        closes = [10.0] * 22
        volumes = [1_000_000] * 21 + [200_000]
        bars = _make_bars(closes, volumes)
        event = _make_event(bars)

        d = VolumeDetector(VolumeDetectorConfig(
            shrinkage_ratio=0.5,
            extreme_shrinkage_ratio=0.3,
        ))
        signals = d.detect(event)

        shrink = [s for s in signals if "shrinkage" in s.source]
        assert len(shrink) > 0
        assert shrink[0].metadata.get("is_extreme", False) is True


class TestVolumeTrend:
    def test_progressive_expansion(self):
        """Each bar's volume > 130% of prior → expansion trend."""
        # Need avg_period+1 bars for surge/shrinkage checks, plus trend_lookback+1
        # Use 25 bars: 19 filler + 6 progressive expansion bars
        closes = [10.0] * 25
        filler_vols = [1_000_000] * 19
        # Last 6 bars: each > 1.3x prior
        expanding = [1_000_000, 1_400_000, 1_960_000, 2_744_000, 3_841_600, 5_378_240]
        volumes = filler_vols + expanding
        bars = _make_bars(closes, volumes)
        event = _make_event(bars)

        d = VolumeDetector(VolumeDetectorConfig(trend_lookback=5, trend_min_ratio=1.3))
        signals = d.detect(event)

        expansion = [s for s in signals if "expansion" in s.source]
        assert len(expansion) > 0
