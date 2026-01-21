"""
Models package - Unified exports for all models

This module provides backward-compatible imports from the modular model structure.
All models can be imported from src.models as before:

    from src.models import Kline, SymbolMetadata, KlineTimeframe, SymbolType
"""

# Base and utilities
from src.models.base import Base, utcnow

# Enums
from src.models.enums import (
    DataUpdateStatus,
    KlineTimeframe,
    SymbolType,
    Timeframe,
    TradeType,
)

# Models
from src.models.board import (
    BoardMapping,
    ConceptDaily,
    IndustryDaily,
    SuperCategoryDaily,
)
from src.models.kline import DataUpdateLog, Kline
from src.models.simulated import SimulatedAccount, SimulatedPosition, SimulatedTrade
from src.models.symbol import SymbolMetadata
from src.models.trade_calendar import TradeCalendar
from src.models.user import KlineEvaluation, Watchlist

__all__ = [
    # Base
    "Base",
    "utcnow",
    # Enums
    "Timeframe",
    "SymbolType",
    "KlineTimeframe",
    "DataUpdateStatus",
    "TradeType",
    # K-line models
    "Kline",
    "DataUpdateLog",
    # Symbol models
    "SymbolMetadata",
    # Board models
    "BoardMapping",
    "IndustryDaily",
    "ConceptDaily",
    "SuperCategoryDaily",
    # Calendar
    "TradeCalendar",
    # User models
    "Watchlist",
    "KlineEvaluation",
    # Simulated trading
    "SimulatedAccount",
    "SimulatedTrade",
    "SimulatedPosition",
]
