"""Raw market event model and supporting enums.

Every piece of data entering the Perception Layer is wrapped in a
RawMarketEvent before being routed to detectors.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class EventSource(str, Enum):
    """Origin of a market event."""

    TUSHARE = "tushare"
    SINA = "sina"
    AKSHARE = "akshare"
    YAHOO = "yahoo"  # Yahoo Finance (US stocks, ADRs, crypto, global indices)
    CLS = "cls"  # 财联社
    THS = "ths"  # 同花顺
    EXCHANGE = "exchange"
    MANUAL = "manual"


class EventType(str, Enum):
    """Category of market event."""

    PRICE_UPDATE = "price_update"
    KLINE = "kline"
    TICK = "tick"
    NEWS = "news"
    ANNOUNCEMENT = "announcement"
    FLOW = "flow"  # capital flow (north-bound, main-force, etc.)
    BOARD_CHANGE = "board_change"  # board composition change
    LIMIT_EVENT = "limit_event"  # limit-up / limit-down
    INDEX_UPDATE = "index_update"
    ETF_FLOW = "etf_flow"
    EARNINGS = "earnings"
    SENTIMENT = "sentiment"


class MarketScope(str, Enum):
    """Market scope for the event."""

    CN_STOCK = "cn_stock"  # A-shares
    CN_INDEX = "cn_index"
    CN_ETF = "cn_etf"
    CN_BOND = "cn_bond"
    HK_STOCK = "hk_stock"
    US_STOCK = "us_stock"
    CRYPTO = "crypto"
    COMMODITY = "commodity"
    GLOBAL = "global"  # cross-market


class RawMarketEvent(BaseModel):
    """A single raw event from any data source.

    This is the universal envelope — every source adapter converts its
    native payload into one or more of these before handing off to
    detectors.
    """

    event_id: str = Field(
        default_factory=lambda: uuid.uuid4().hex,
        description="Unique event identifier",
    )
    source: EventSource = Field(..., description="Which data source produced this")
    event_type: EventType = Field(..., description="What kind of event")
    market: MarketScope = Field(..., description="Which market scope")
    symbol: Optional[str] = Field(
        default=None,
        description="Ticker / symbol (None for market-wide events)",
    )
    data: Dict[str, Any] = Field(
        default_factory=dict,
        description="Source-specific payload",
    )
    timestamp: datetime = Field(
        ...,
        description="When the event occurred at source",
    )
    received_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When we received the event",
    )

    model_config = {"use_enum_values": True}

    @property
    def latency_ms(self) -> float:
        """Milliseconds between event timestamp and reception."""
        delta = self.received_at - self.timestamp
        return delta.total_seconds() * 1000
