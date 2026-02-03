"""Perception Layer — standardized market data ingestion and event detection.

The Perception Layer is responsible for:
1. Connecting to heterogeneous data sources (Tushare, Sina, CLS, etc.)
2. Normalizing raw data into RawMarketEvents
3. Routing events to Detectors that emit UnifiedSignals
4. Monitoring source health and managing circuit breakers
"""

from src.perception.events import (
    EventSource,
    EventType,
    MarketScope,
    RawMarketEvent,
)
from src.perception.health import HealthMonitor, HealthStatus, SourceHealth
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
from src.perception.sources.base import DataSource, SourceType
from src.perception.sources.registry import SourceRegistry
from src.perception.detectors.base import Detector

__all__ = [
    # Events
    "EventSource",
    "EventType",
    "MarketScope",
    "RawMarketEvent",
    # Health
    "HealthMonitor",
    "HealthStatus",
    "SourceHealth",
    # Signals
    "AShareSignal",
    "CommoditySignal",
    "CryptoSignal",
    "Direction",
    "Market",
    "SignalType",
    "UnifiedSignal",
    "USStockSignal",
    # Sources
    "DataSource",
    "SourceType",
    "SourceRegistry",
    # Detectors
    "Detector",
]
