"""Anomaly Detector for the Perception Layer.

Detects market anomalies specific to A-share trading:
- 涨停/跌停 count surges (limit-up / limit-down waves)
- 大笔买入/卖出 (large block trades)
- 自选股异动 (watchlist stock moves > threshold %)
- Volume spikes (abnormal volume vs rolling average)

Signal mapping:
    涨停 → FLOW / LONG
    跌停 → FLOW / SHORT
    大单买入 → FLOW / LONG
    大单卖出 → FLOW / SHORT
    自选股异动 → TECHNICAL / direction from price change
    Volume spike → FLOW / direction from price change
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from src.perception.detectors.base import Detector
from src.perception.events import EventType, MarketScope, RawMarketEvent
from src.perception.signals import (
    Direction,
    Market,
    SignalType,
    UnifiedSignal,
)

# ── Default thresholds ────────────────────────────────────────────────

_DEFAULT_CONFIG: Dict[str, Any] = {
    # 涨停/跌停 thresholds
    "limit_up_count_threshold": 10,      # >= N stocks 涨停 triggers signal
    "limit_down_count_threshold": 10,    # >= N stocks 跌停 triggers signal
    "limit_up_strength_base": 0.6,       # base strength for limit-up wave
    "limit_down_strength_base": 0.6,     # base strength for limit-down wave
    # 大单 thresholds
    "large_order_amount_threshold": 5_000_000,  # ¥5M single order
    "large_order_strength": 0.65,
    "large_order_confidence": 0.60,
    # 自选股异动
    "watchlist_move_pct_threshold": 5.0,  # > 5% move
    "watchlist_symbols": [],              # user-configurable watchlist
    "watchlist_strength_base": 0.55,
    "watchlist_confidence": 0.65,
    # Volume spike
    "volume_spike_ratio": 3.0,           # current / avg >= N
    "volume_avg_period": 20,
    "volume_spike_strength_base": 0.60,
    "volume_spike_confidence": 0.55,
    # General
    "min_confidence": 0.0,
}

_SCOPE_TO_MARKET: Dict[str, Market] = {
    MarketScope.CN_STOCK.value: Market.A_SHARE,
    MarketScope.CN_INDEX.value: Market.A_SHARE,
    MarketScope.CN_ETF.value: Market.A_SHARE,
    MarketScope.HK_STOCK.value: Market.A_SHARE,
    MarketScope.US_STOCK.value: Market.US_STOCK,
    MarketScope.CRYPTO.value: Market.CRYPTO,
    MarketScope.COMMODITY.value: Market.COMMODITY,
}


class AnomalyDetector(Detector):
    """Stateless anomaly detector for A-share market events.

    Parameters
    ----------
    config : dict, optional
        Override any key from ``_DEFAULT_CONFIG``.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self._config: Dict[str, Any] = {**_DEFAULT_CONFIG}
        if config:
            self._config.update(config)

    # ── Detector interface ────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "anomaly"

    @property
    def accepts(self) -> List[EventType]:
        return [
            EventType.ANOMALY,
            EventType.PRICE_UPDATE,
            EventType.LIMIT_EVENT,
            EventType.BOARD_CHANGE,
        ]

    def detect(self, event: RawMarketEvent) -> List[UnifiedSignal]:
        """Route an event through all anomaly sub-detectors."""
        etype = event.event_type
        etype_lower = etype.lower() if isinstance(etype, str) else etype.value.lower()

        market = _SCOPE_TO_MARKET.get(
            event.market if isinstance(event.market, str) else event.market.value,
            Market.A_SHARE,
        )
        ts = event.timestamp

        signals: List[UnifiedSignal] = []

        # Route to sub-detectors based on event type and data content
        if etype_lower in ("limit_event", "anomaly"):
            signals.extend(self._detect_limit_wave(event, market, ts))

        if etype_lower in ("anomaly", "flow"):
            signals.extend(self._detect_large_orders(event, market, ts))

        if etype_lower in ("price_update", "anomaly"):
            signals.extend(self._detect_watchlist_move(event, market, ts))

        if etype_lower in ("price_update", "anomaly", "board_change"):
            signals.extend(self._detect_volume_spike(event, market, ts))

        min_conf = self._config.get("min_confidence", 0.0)
        return [s for s in signals if s.confidence >= min_conf]

    # ── 涨停/跌停 wave detection ─────────────────────────────────────

    def _detect_limit_wave(
        self,
        event: RawMarketEvent,
        market: Market,
        ts: datetime,
    ) -> List[UnifiedSignal]:
        """Detect surges in limit-up or limit-down counts."""
        data = event.data
        signals: List[UnifiedSignal] = []

        limit_up_count = data.get("limit_up_count", 0)
        limit_down_count = data.get("limit_down_count", 0)
        asset = event.symbol or "MARKET"

        up_threshold = self._config["limit_up_count_threshold"]
        down_threshold = self._config["limit_down_count_threshold"]

        if limit_up_count >= up_threshold:
            # More stocks hitting limit-up → stronger signal
            ratio = limit_up_count / max(up_threshold, 1)
            strength = min(1.0, self._config["limit_up_strength_base"] + 0.1 * (ratio - 1))
            confidence = min(0.95, 0.65 + 0.05 * (ratio - 1))
            signals.append(self._make_signal(
                asset=asset,
                market=market,
                direction=Direction.LONG,
                signal_type=SignalType.FLOW,
                strength=strength,
                confidence=confidence,
                ts=ts,
                meta={
                    "detector": "limit_wave",
                    "type": "limit_up_surge",
                    "limit_up_count": limit_up_count,
                    "threshold": up_threshold,
                },
            ))

        if limit_down_count >= down_threshold:
            ratio = limit_down_count / max(down_threshold, 1)
            strength = min(1.0, self._config["limit_down_strength_base"] + 0.1 * (ratio - 1))
            confidence = min(0.95, 0.65 + 0.05 * (ratio - 1))
            signals.append(self._make_signal(
                asset=asset,
                market=market,
                direction=Direction.SHORT,
                signal_type=SignalType.FLOW,
                strength=strength,
                confidence=confidence,
                ts=ts,
                meta={
                    "detector": "limit_wave",
                    "type": "limit_down_surge",
                    "limit_down_count": limit_down_count,
                    "threshold": down_threshold,
                },
            ))

        return signals

    # ── 大笔买入/卖出 detection ──────────────────────────────────────

    def _detect_large_orders(
        self,
        event: RawMarketEvent,
        market: Market,
        ts: datetime,
    ) -> List[UnifiedSignal]:
        """Detect unusually large buy/sell orders."""
        data = event.data
        signals: List[UnifiedSignal] = []

        order_amount = data.get("order_amount", 0)
        order_side = data.get("order_side", "").lower()  # "buy" or "sell"
        asset = event.symbol or "UNKNOWN"

        threshold = self._config["large_order_amount_threshold"]
        if order_amount < threshold:
            return []

        ratio = order_amount / max(threshold, 1)
        strength = min(1.0, self._config["large_order_strength"] + 0.05 * (ratio - 1))
        confidence = self._config["large_order_confidence"]

        if order_side == "buy":
            direction = Direction.LONG
            order_type = "large_buy"
        elif order_side == "sell":
            direction = Direction.SHORT
            order_type = "large_sell"
        else:
            # Unknown side — default to LONG with lower confidence
            direction = Direction.LONG
            order_type = "large_order_unknown"
            confidence *= 0.8

        signals.append(self._make_signal(
            asset=asset,
            market=market,
            direction=direction,
            signal_type=SignalType.FLOW,
            strength=strength,
            confidence=confidence,
            ts=ts,
            meta={
                "detector": "large_order",
                "type": order_type,
                "order_amount": order_amount,
                "order_side": order_side,
                "threshold": threshold,
            },
        ))

        return signals

    # ── 自选股异动 detection ──────────────────────────────────────────

    def _detect_watchlist_move(
        self,
        event: RawMarketEvent,
        market: Market,
        ts: datetime,
    ) -> List[UnifiedSignal]:
        """Detect large price moves in watchlist stocks."""
        data = event.data
        asset = event.symbol
        if not asset:
            return []

        watchlist = self._config.get("watchlist_symbols", [])
        if watchlist and asset not in watchlist:
            return []

        change_pct = data.get("change_pct", 0.0)
        threshold = self._config["watchlist_move_pct_threshold"]

        if abs(change_pct) < threshold:
            return []

        direction = Direction.LONG if change_pct > 0 else Direction.SHORT
        # Larger move → stronger signal
        magnitude = abs(change_pct) / max(threshold, 0.01)
        strength = min(1.0, self._config["watchlist_strength_base"] + 0.1 * (magnitude - 1))
        confidence = self._config["watchlist_confidence"]

        return [self._make_signal(
            asset=asset,
            market=market,
            direction=direction,
            signal_type=SignalType.TECHNICAL,
            strength=strength,
            confidence=confidence,
            ts=ts,
            meta={
                "detector": "watchlist_move",
                "type": "watchlist_anomaly",
                "change_pct": change_pct,
                "threshold": threshold,
                "on_watchlist": asset in watchlist if watchlist else True,
            },
        )]

    # ── Volume spike detection ────────────────────────────────────────

    def _detect_volume_spike(
        self,
        event: RawMarketEvent,
        market: Market,
        ts: datetime,
    ) -> List[UnifiedSignal]:
        """Detect abnormal volume relative to rolling average."""
        data = event.data
        asset = event.symbol or "UNKNOWN"

        current_volume = data.get("volume", 0)
        avg_volume = data.get("avg_volume", 0)

        if avg_volume <= 0 or current_volume <= 0:
            return []

        ratio = current_volume / avg_volume
        spike_threshold = self._config["volume_spike_ratio"]

        if ratio < spike_threshold:
            return []

        # Direction from price change if available, else LONG
        change_pct = data.get("change_pct", 0.0)
        direction = Direction.LONG if change_pct >= 0 else Direction.SHORT

        strength = min(1.0, self._config["volume_spike_strength_base"] + 0.08 * (ratio - spike_threshold))
        confidence = self._config["volume_spike_confidence"]

        return [self._make_signal(
            asset=asset,
            market=market,
            direction=direction,
            signal_type=SignalType.FLOW,
            strength=strength,
            confidence=confidence,
            ts=ts,
            meta={
                "detector": "volume_spike",
                "type": "volume_anomaly",
                "volume_ratio": round(ratio, 2),
                "current_volume": current_volume,
                "avg_volume": avg_volume,
                "spike_threshold": spike_threshold,
            },
        )]

    # ── Signal builder ────────────────────────────────────────────────

    def _make_signal(
        self,
        asset: str,
        market: Market,
        direction: Direction,
        signal_type: SignalType,
        strength: float,
        confidence: float,
        ts: datetime,
        meta: Dict[str, Any],
    ) -> UnifiedSignal:
        return UnifiedSignal(
            market=market,
            asset=asset,
            direction=direction,
            strength=round(min(1.0, max(0.0, strength)), 4),
            confidence=round(min(1.0, max(0.0, confidence)), 4),
            signal_type=signal_type,
            source=self.name,
            timestamp=ts,
            metadata=meta,
        )
