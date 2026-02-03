"""FlowDetector -- capital flow anomaly detection for A-share sectors.

Detects:
1. Sector fund net inflow / outflow anomalies (industry_daily data)
2. Northbound capital large moves (moneyflow_hsgt)
3. Sector rotation signals (money flowing from sector A to B)
4. Custom-tracked sector fund anomalies (configurable 16-sector watchlist)

Accepts event types: BOARD_CHANGE, FLOW
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.perception.detectors.base import Detector
from src.perception.events import EventType, RawMarketEvent
from src.perception.signals import (
    AShareSignal,
    Direction,
    Market,
    SignalType,
    UnifiedSignal,
)

logger = logging.getLogger(__name__)

# -- Default tracked sectors (16 key industries) --

DEFAULT_TRACKED_SECTORS: List[str] = [
    "半导体",
    "新能源",
    "医药生物",
    "食品饮料",
    "银行",
    "房地产",
    "计算机",
    "电子",
    "汽车",
    "军工",
    "通信",
    "传媒",
    "有色金属",
    "化工",
    "机械设备",
    "电力设备",
]


# -- Configuration --


@dataclass
class FlowDetectorConfig:
    """Tuneable thresholds for the FlowDetector.

    All monetary amounts are in 亿元 (hundred-million CNY) to match
    the unit used in IndustryDaily.net_amount.
    """

    # Sector net-inflow anomaly: trigger when |net_amount| > threshold
    net_inflow_threshold: float = 5.0

    # Northbound capital: trigger when single-day net buy > threshold
    northbound_threshold: float = 50.0

    # Rank change threshold
    rank_change_threshold: int = 10

    # Sector rotation: min outflow from source and min inflow to target
    rotation_outflow_min: float = 3.0
    rotation_inflow_min: float = 3.0

    # Custom tracked sectors
    tracked_sectors: List[str] = field(
        default_factory=lambda: list(DEFAULT_TRACKED_SECTORS)
    )

    # Tracked-sector anomaly: lower threshold for watched sectors
    tracked_sector_threshold: float = 3.0

    # Signal strength scaling
    max_inflow_for_strength: float = 30.0
    base_confidence: float = 0.7


# -- Detector --


class FlowDetector(Detector):
    """Capital-flow anomaly detector for A-share sectors."""

    def __init__(self, config: Optional[FlowDetectorConfig] = None) -> None:
        self._config = config or FlowDetectorConfig()

    @property
    def name(self) -> str:
        return "flow"

    @property
    def accepts(self) -> List[EventType]:
        return [EventType.BOARD_CHANGE, EventType.FLOW]

    def detect(self, event: RawMarketEvent) -> List[UnifiedSignal]:
        """Route an event to the appropriate sub-detector(s)."""
        signals: List[UnifiedSignal] = []

        data = event.data or {}
        kind = data.get("kind", "")

        try:
            if kind == "northbound":
                signals.extend(self._detect_northbound(event))
            elif kind == "sector_snapshot":
                signals.extend(self._detect_sector_anomalies(event))
                signals.extend(self._detect_sector_rotation(event))
                signals.extend(self._detect_tracked_sector_anomalies(event))
            else:
                # Generic: try all applicable sub-detectors
                signals.extend(self._detect_sector_anomalies(event))
                signals.extend(self._detect_sector_rotation(event))
                signals.extend(self._detect_tracked_sector_anomalies(event))
                signals.extend(self._detect_northbound(event))
        except Exception:
            logger.exception("FlowDetector error processing event %s", event.event_id)

        return signals

    # -- Sub-detectors --

    def _detect_sector_anomalies(self, event: RawMarketEvent) -> List[AShareSignal]:
        """Detect large net inflow/outflow in any sector."""
        sectors: List[Dict[str, Any]] = event.data.get("sectors", [])
        if not sectors:
            return []

        cfg = self._config
        signals: List[AShareSignal] = []

        for sec in sectors:
            net = sec.get("net_amount") or sec.get("net_inflow") or 0.0
            abs_net = abs(net)

            if abs_net < cfg.net_inflow_threshold:
                continue

            direction = Direction.LONG if net > 0 else Direction.SHORT
            strength = min(1.0, abs_net / cfg.max_inflow_for_strength)
            pct = sec.get("pct_change", 0.0) or 0.0
            confidence = cfg.base_confidence
            if (net > 0 and pct > 0) or (net < 0 and pct < 0):
                confidence = min(1.0, confidence + 0.15)

            signals.append(
                AShareSignal(
                    asset=sec.get("ts_code") or sec.get("name", "UNKNOWN"),
                    direction=direction,
                    strength=round(strength, 4),
                    confidence=round(confidence, 4),
                    signal_type=SignalType.FLOW,
                    source="flow/sector_anomaly",
                    timestamp=event.timestamp,
                    metadata={
                        "detector": self.name,
                        "sub_type": "sector_anomaly",
                        "sector_name": sec.get("name"),
                        "net_amount": net,
                        "pct_change": pct,
                        "up_count": sec.get("up_count"),
                        "down_count": sec.get("down_count"),
                    },
                )
            )

        return signals

    def _detect_northbound(self, event: RawMarketEvent) -> List[AShareSignal]:
        """Detect unusually large northbound capital moves."""
        data = event.data
        north = data.get("north_money")
        if north is None:
            return []

        cfg = self._config
        if abs(north) < cfg.northbound_threshold:
            return []

        direction = Direction.LONG if north > 0 else Direction.SHORT
        strength = min(1.0, abs(north) / (cfg.northbound_threshold * 4))
        confidence = min(1.0, cfg.base_confidence + 0.1)

        return [
            AShareSignal(
                asset="NORTHBOUND",
                direction=direction,
                strength=round(strength, 4),
                confidence=round(confidence, 4),
                signal_type=SignalType.FLOW,
                source="flow/northbound",
                north_flow=north,
                timestamp=event.timestamp,
                metadata={
                    "detector": self.name,
                    "sub_type": "northbound",
                    "north_money": north,
                    "sh_money": data.get("sh_money"),
                    "sz_money": data.get("sz_money"),
                    "trade_date": data.get("trade_date"),
                },
            )
        ]

    def _detect_sector_rotation(self, event: RawMarketEvent) -> List[AShareSignal]:
        """Detect money rotating from one sector to another."""
        sectors: List[Dict[str, Any]] = event.data.get("sectors", [])
        if len(sectors) < 2:
            return []

        cfg = self._config
        outflows: List[Dict[str, Any]] = []
        inflows: List[Dict[str, Any]] = []

        for sec in sectors:
            net = sec.get("net_amount") or sec.get("net_inflow") or 0.0
            if net <= -cfg.rotation_outflow_min:
                outflows.append(sec)
            elif net >= cfg.rotation_inflow_min:
                inflows.append(sec)

        outflows.sort(key=lambda s: s.get("net_amount") or s.get("net_inflow") or 0.0)
        inflows.sort(
            key=lambda s: s.get("net_amount") or s.get("net_inflow") or 0.0,
            reverse=True,
        )

        signals: List[AShareSignal] = []

        for i in range(min(3, len(outflows), len(inflows))):
            src_sec = outflows[i]
            dst_sec = inflows[i]
            src_net = abs(src_sec.get("net_amount") or src_sec.get("net_inflow") or 0.0)
            dst_net = abs(dst_sec.get("net_amount") or dst_sec.get("net_inflow") or 0.0)
            avg_flow = (src_net + dst_net) / 2
            strength = min(1.0, avg_flow / cfg.max_inflow_for_strength)

            signals.append(
                AShareSignal(
                    asset=dst_sec.get("ts_code") or dst_sec.get("name", "UNKNOWN"),
                    direction=Direction.LONG,
                    strength=round(strength, 4),
                    confidence=round(cfg.base_confidence, 4),
                    signal_type=SignalType.FLOW,
                    source="flow/sector_rotation",
                    timestamp=event.timestamp,
                    metadata={
                        "detector": self.name,
                        "sub_type": "sector_rotation",
                        "from_sector": src_sec.get("name"),
                        "to_sector": dst_sec.get("name"),
                        "from_net_amount": src_sec.get("net_amount")
                        or src_sec.get("net_inflow"),
                        "to_net_amount": dst_sec.get("net_amount")
                        or dst_sec.get("net_inflow"),
                    },
                )
            )

        return signals

    def _detect_tracked_sector_anomalies(
        self, event: RawMarketEvent
    ) -> List[AShareSignal]:
        """Detect anomalies in the custom-tracked sector watchlist."""
        sectors: List[Dict[str, Any]] = event.data.get("sectors", [])
        if not sectors:
            return []

        cfg = self._config
        tracked_names = set(cfg.tracked_sectors)
        signals: List[AShareSignal] = []

        for sec in sectors:
            name = sec.get("name", "")
            if name not in tracked_names:
                if sec.get("industry", "") not in tracked_names:
                    continue

            net = sec.get("net_amount") or sec.get("net_inflow") or 0.0
            abs_net = abs(net)

            # Already covered by generic anomaly detector
            if abs_net >= cfg.net_inflow_threshold:
                continue

            rank_change = 0
            prev_rank = sec.get("prev_rank")
            curr_rank = sec.get("rank")
            if prev_rank is not None and curr_rank is not None:
                rank_change = prev_rank - curr_rank

            flow_anomaly = abs_net >= cfg.tracked_sector_threshold
            rank_anomaly = abs(rank_change) >= cfg.rank_change_threshold

            if not flow_anomaly and not rank_anomaly:
                continue

            direction = Direction.LONG if net >= 0 else Direction.SHORT
            if rank_anomaly and rank_change > 0:
                direction = Direction.LONG
            elif rank_anomaly and rank_change < 0:
                direction = Direction.SHORT

            strength = min(1.0, abs_net / cfg.max_inflow_for_strength)
            if rank_anomaly:
                strength = min(1.0, strength + abs(rank_change) / 50)

            confidence = cfg.base_confidence - 0.05

            sub_type = "tracked_sector"
            if rank_anomaly:
                sub_type = "tracked_sector_rank"

            signals.append(
                AShareSignal(
                    asset=sec.get("ts_code") or sec.get("name", "UNKNOWN"),
                    direction=direction,
                    strength=round(strength, 4),
                    confidence=round(max(0.0, confidence), 4),
                    signal_type=SignalType.FLOW,
                    source=f"flow/{sub_type}",
                    timestamp=event.timestamp,
                    metadata={
                        "detector": self.name,
                        "sub_type": sub_type,
                        "sector_name": name,
                        "net_amount": net,
                        "pct_change": sec.get("pct_change", 0.0),
                        "rank": curr_rank,
                        "prev_rank": prev_rank,
                        "rank_change": rank_change,
                    },
                )
            )

        return signals
