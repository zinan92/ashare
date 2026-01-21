"""
Enum definitions for models
"""
from enum import Enum


class Timeframe(str, Enum):
    """旧的 Timeframe 枚举，保留用于 API 响应兼容"""
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    MINS_30 = "30m"


class SymbolType(str, Enum):
    """标的类型"""
    STOCK = "stock"      # 个股
    INDEX = "index"      # 指数
    CONCEPT = "concept"  # 概念板块


class KlineTimeframe(str, Enum):
    """K线时间周期"""
    DAY = "day"
    MINS_30 = "30m"
    MINS_5 = "5m"
    MINS_1 = "1m"


class DataUpdateStatus(str, Enum):
    """数据更新状态"""
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"


class TradeType(str, Enum):
    """交易类型"""
    BUY = "buy"
    SELL = "sell"


__all__ = [
    "Timeframe",
    "SymbolType",
    "KlineTimeframe",
    "DataUpdateStatus",
    "TradeType",
]
