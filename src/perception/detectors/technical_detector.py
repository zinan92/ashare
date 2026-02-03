"""TechnicalDetector -- price/volume technical indicator detection.

Detects:
1. MA crossovers   (MA5/MA10/MA20 golden/death cross)
2. RSI extremes    (overbought >70, oversold <30)
3. MACD crosses    (golden cross / death cross)
4. Volume breakout (current volume > 2× rolling average)

Accepts event types: KLINE, PRICE_UPDATE

The detector is *stateless* -- each event must carry enough history
in ``event.data["bars"]`` (list of OHLCV dicts) for the indicators
to be computed.  Bars should be ordered oldest-first.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from src.perception.detectors.base import Detector
from src.perception.events import EventType, RawMarketEvent
from src.perception.signals import (
    Direction,
    Market,
    SignalType,
    UnifiedSignal,
)

logger = logging.getLogger(__name__)

# ── Helpers ──────────────────────────────────────────────────────────


def _sma(values: List[float], period: int) -> Optional[float]:
    """Simple moving average of the last *period* values, or None."""
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def _ema(values: List[float], period: int) -> Optional[float]:
    """Exponential moving average over all *values* with given period.

    Returns None if fewer values than *period*.
    """
    if len(values) < period:
        return None
    k = 2.0 / (period + 1)
    # Seed with SMA of first *period* values
    ema_val = sum(values[:period]) / period
    for v in values[period:]:
        ema_val = v * k + ema_val * (1 - k)
    return ema_val


def _rsi(closes: List[float], period: int = 14) -> Optional[float]:
    """Wilder-style RSI.  Returns None when not enough data."""
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def _macd(
    closes: List[float],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> Optional[Dict[str, float]]:
    """Return {macd, signal, hist} or None if not enough data."""
    if len(closes) < slow + signal:
        return None
    fast_ema = _ema(closes, fast)
    slow_ema = _ema(closes, slow)
    if fast_ema is None or slow_ema is None:
        return None
    # We need a MACD series long enough to compute the signal line.
    # Build MACD values for the last (slow+signal) bars.
    macd_series: List[float] = []
    for end in range(slow, len(closes) + 1):
        f = _ema(closes[:end], fast)
        s = _ema(closes[:end], slow)
        if f is not None and s is not None:
            macd_series.append(f - s)
    if len(macd_series) < signal:
        return None
    sig = _ema(macd_series, signal)
    if sig is None:
        return None
    m = macd_series[-1]
    return {"macd": m, "signal": sig, "hist": m - sig}


# ── Detector ─────────────────────────────────────────────────────────


class TechnicalDetector(Detector):
    """Stateless technical-indicator detector.

    Each event must carry ``data["bars"]`` — a list of bar dicts with
    at least ``close`` (and ``volume`` for volume breakout).  Bars are
    ordered oldest → newest.
    """

    _ACCEPTED = [EventType.KLINE, EventType.PRICE_UPDATE]

    @property
    def name(self) -> str:
        return "technical"

    @property
    def accepts(self) -> List[EventType]:
        return list(self._ACCEPTED)

    # ── public API ───────────────────────────────────────────────────

    def detect(self, event: RawMarketEvent) -> List[UnifiedSignal]:
        bars: List[Dict[str, Any]] = (event.data or {}).get("bars", [])
        if not bars:
            return []

        closes = [float(b["close"]) for b in bars if "close" in b]
        volumes = [float(b["volume"]) for b in bars if "volume" in b]

        asset = event.symbol or "UNKNOWN"
        market = self._resolve_market(event)
        ts = event.timestamp

        signals: List[UnifiedSignal] = []

        try:
            signals.extend(self._detect_ma_cross(closes, asset, market, ts))
        except Exception:
            logger.exception("MA detection error")

        try:
            signals.extend(self._detect_rsi(closes, asset, market, ts))
        except Exception:
            logger.exception("RSI detection error")

        try:
            signals.extend(self._detect_macd(closes, asset, market, ts))
        except Exception:
            logger.exception("MACD detection error")

        try:
            signals.extend(self._detect_volume_breakout(volumes, asset, market, ts))
        except Exception:
            logger.exception("Volume detection error")

        return signals

    # ── Sub-detectors ────────────────────────────────────────────────

    def _detect_ma_cross(
        self,
        closes: List[float],
        asset: str,
        market: Market,
        ts,
    ) -> List[UnifiedSignal]:
        """Check for MA5/MA10/MA20 crossovers on the last two bars."""
        signals: List[UnifiedSignal] = []
        pairs = [(5, 10), (5, 20), (10, 20)]

        for short_p, long_p in pairs:
            if len(closes) < long_p + 1:
                continue
            cur_short = _sma(closes, short_p)
            cur_long = _sma(closes, long_p)
            prev_short = _sma(closes[:-1], short_p)
            prev_long = _sma(closes[:-1], long_p)
            if None in (cur_short, cur_long, prev_short, prev_long):
                continue

            cross_up = prev_short <= prev_long and cur_short > cur_long
            cross_down = prev_short >= prev_long and cur_short < cur_long

            if cross_up or cross_down:
                direction = Direction.LONG if cross_up else Direction.SHORT
                label = "golden_cross" if cross_up else "death_cross"
                spread = abs(cur_short - cur_long) / cur_long if cur_long else 0
                strength = min(1.0, spread * 100)  # 1% spread → 1.0
                signals.append(
                    UnifiedSignal(
                        market=market,
                        asset=asset,
                        direction=direction,
                        strength=round(max(0.1, strength), 4),
                        confidence=0.65,
                        signal_type=SignalType.TECHNICAL,
                        source=f"technical/ma_cross",
                        timestamp=ts,
                        metadata={
                            "detector": "technical",
                            "sub_type": "ma_cross",
                            "cross": label,
                            "fast_period": short_p,
                            "slow_period": long_p,
                            "fast_ma": round(cur_short, 4),
                            "slow_ma": round(cur_long, 4),
                        },
                    )
                )
        return signals

    def _detect_rsi(
        self,
        closes: List[float],
        asset: str,
        market: Market,
        ts,
    ) -> List[UnifiedSignal]:
        rsi_val = _rsi(closes, 14)
        if rsi_val is None:
            return []

        if rsi_val > 70:
            direction = Direction.SHORT
            label = "overbought"
            strength = min(1.0, (rsi_val - 70) / 30)
        elif rsi_val < 30:
            direction = Direction.LONG
            label = "oversold"
            strength = min(1.0, (30 - rsi_val) / 30)
        else:
            return []

        return [
            UnifiedSignal(
                market=market,
                asset=asset,
                direction=direction,
                strength=round(max(0.1, strength), 4),
                confidence=0.6,
                signal_type=SignalType.TECHNICAL,
                source="technical/rsi",
                timestamp=ts,
                metadata={
                    "detector": "technical",
                    "sub_type": "rsi",
                    "condition": label,
                    "rsi": round(rsi_val, 2),
                },
            )
        ]

    def _detect_macd(
        self,
        closes: List[float],
        asset: str,
        market: Market,
        ts,
    ) -> List[UnifiedSignal]:
        if len(closes) < 36:  # need 26+9+1
            return []
        cur = _macd(closes)
        prev = _macd(closes[:-1])
        if cur is None or prev is None:
            return []

        cross_up = prev["hist"] <= 0 and cur["hist"] > 0
        cross_down = prev["hist"] >= 0 and cur["hist"] < 0

        if not cross_up and not cross_down:
            return []

        direction = Direction.LONG if cross_up else Direction.SHORT
        label = "golden_cross" if cross_up else "death_cross"
        strength = min(1.0, abs(cur["hist"]) * 10)

        return [
            UnifiedSignal(
                market=market,
                asset=asset,
                direction=direction,
                strength=round(max(0.1, strength), 4),
                confidence=0.7,
                signal_type=SignalType.TECHNICAL,
                source="technical/macd",
                timestamp=ts,
                metadata={
                    "detector": "technical",
                    "sub_type": "macd",
                    "cross": label,
                    "macd": round(cur["macd"], 4),
                    "signal": round(cur["signal"], 4),
                    "hist": round(cur["hist"], 4),
                },
            )
        ]

    def _detect_volume_breakout(
        self,
        volumes: List[float],
        asset: str,
        market: Market,
        ts,
    ) -> List[UnifiedSignal]:
        period = 20
        if len(volumes) < period + 1:
            return []
        avg = sum(volumes[-period - 1 : -1]) / period
        if avg <= 0:
            return []
        ratio = volumes[-1] / avg
        if ratio < 2.0:
            return []

        strength = min(1.0, (ratio - 2.0) / 3.0 + 0.3)

        return [
            UnifiedSignal(
                market=market,
                asset=asset,
                direction=Direction.LONG,
                strength=round(max(0.1, strength), 4),
                confidence=0.55,
                signal_type=SignalType.TECHNICAL,
                source="technical/volume_breakout",
                timestamp=ts,
                metadata={
                    "detector": "technical",
                    "sub_type": "volume_breakout",
                    "current_volume": volumes[-1],
                    "avg_volume": round(avg, 2),
                    "ratio": round(ratio, 2),
                },
            )
        ]

    # ── helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _resolve_market(event: RawMarketEvent) -> Market:
        scope = str(event.market).lower()
        if "cn" in scope or "a_share" in scope:
            return Market.A_SHARE
        if "us" in scope:
            return Market.US_STOCK
        if "crypto" in scope:
            return Market.CRYPTO
        if "commodity" in scope:
            return Market.COMMODITY
        return Market.A_SHARE
