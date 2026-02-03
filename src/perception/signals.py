"""Unified Signal Schema — multi-market signal representation.

Copied from trading-agents/src/signals/schema.py and kept in sync.
Provides a single, normalized signal format that works across:
- A-shares (CN equities)
- US stocks
- Crypto (perps, spot)
- Commodities

The base UnifiedSignal carries all fields common to every market.
Market-specific subclasses add extra fields without breaking the
shared interface.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


# ── Enums ────────────────────────────────────────────────────────────


class Market(str, Enum):
    A_SHARE = "a_share"
    US_STOCK = "us_stock"
    CRYPTO = "crypto"
    COMMODITY = "commodity"


class Direction(str, Enum):
    LONG = "long"
    SHORT = "short"


class SignalType(str, Enum):
    TECHNICAL = "technical"
    FUNDAMENTAL = "fundamental"
    SENTIMENT = "sentiment"
    FLOW = "flow"
    COMPOSITE = "composite"


# ── Base Signal ──────────────────────────────────────────────────────


class UnifiedSignal(BaseModel):
    """Market-agnostic trading signal.

    Every signal — regardless of its origin market — can be expressed
    through this schema.  Market-specific subclasses extend it with
    extra optional fields.
    """

    signal_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    market: Market
    asset: str = Field(..., min_length=1, description="Ticker / symbol")
    direction: Direction
    strength: float = Field(..., ge=0.0, le=1.0, description="Normalised 0-1")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Normalised 0-1")
    signal_type: SignalType
    source: str = Field(..., min_length=1, description="Origin system / strategy")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"use_enum_values": True}

    # ── validators ───────────────────────────────────────────────────

    @field_validator("expires_at")
    @classmethod
    def _expires_must_be_future_or_none(
        cls, v: Optional[datetime], info
    ) -> Optional[datetime]:
        """expires_at, when set, must be after timestamp."""
        if v is not None:
            ts = info.data.get("timestamp")
            if ts is not None and v <= ts:
                raise ValueError("expires_at must be after timestamp")
        return v

    # ── helpers ──────────────────────────────────────────────────────

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) >= self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a plain dict (datetimes → ISO strings)."""
        return json.loads(self.model_dump_json())

    def to_json(self, **kwargs) -> str:
        return self.model_dump_json(**kwargs)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UnifiedSignal":
        return cls.model_validate(data)

    @classmethod
    def from_json(cls, raw: str) -> "UnifiedSignal":
        return cls.model_validate_json(raw)


# ── Market-specific extensions ───────────────────────────────────────


class CryptoSignal(UnifiedSignal):
    """Crypto-specific fields on top of the unified schema."""

    market: Market = Market.CRYPTO  # type: ignore[assignment]
    funding_rate: Optional[float] = None
    volume_spike_ratio: Optional[float] = None
    chain_data: Dict[str, Any] = Field(default_factory=dict)


class AShareSignal(UnifiedSignal):
    """A-share (CN equity) specific fields."""

    market: Market = Market.A_SHARE  # type: ignore[assignment]
    limit_up_count: Optional[int] = Field(default=None, ge=0)
    concept_codes: List[str] = Field(default_factory=list)
    north_flow: Optional[float] = None  # northbound capital flow (亿元)


class USStockSignal(UnifiedSignal):
    """US equity specific fields."""

    market: Market = Market.US_STOCK  # type: ignore[assignment]
    options_flow: Optional[Dict[str, Any]] = None
    earnings_surprise: Optional[float] = None  # pct beat/miss


class CommoditySignal(UnifiedSignal):
    """Commodity-specific fields."""

    market: Market = Market.COMMODITY  # type: ignore[assignment]
    contract_month: Optional[str] = None
    inventory_change: Optional[float] = None
