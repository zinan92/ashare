"""
K-line data models
"""
from datetime import datetime

from sqlalchemy import (
    DateTime,
    Enum as SqlEnum,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base, utcnow
from src.models.enums import DataUpdateStatus, KlineTimeframe, SymbolType


class Kline(Base):
    """
    统一K线数据表
    存储所有类型标的(个股/指数/概念)的K线数据
    """

    __tablename__ = "klines"
    __table_args__ = (
        UniqueConstraint("symbol_type", "symbol_code", "timeframe", "trade_time"),
        Index("ix_klines_symbol", "symbol_type", "symbol_code", "timeframe"),
        Index("ix_klines_trade_time", "trade_time"),
        Index("ix_klines_lookup", "symbol_type", "symbol_code", "timeframe", "trade_time"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # 标的信息
    symbol_type: Mapped[SymbolType] = mapped_column(
        SqlEnum(SymbolType), index=True
    )  # 'stock', 'index', 'concept'
    symbol_code: Mapped[str] = mapped_column(String(16), index=True)  # 代码
    symbol_name: Mapped[str | None] = mapped_column(String(64), nullable=True)  # 名称

    # 时间周期
    timeframe: Mapped[KlineTimeframe] = mapped_column(
        SqlEnum(KlineTimeframe), index=True
    )

    # K线数据
    trade_time: Mapped[str] = mapped_column(String(32))  # ISO格式: 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float, default=0)  # 成交量
    amount: Mapped[float] = mapped_column(Float, default=0)  # 成交额

    # 技术指标 (可选)
    dif: Mapped[float | None] = mapped_column(Float, nullable=True)  # MACD DIF
    dea: Mapped[float | None] = mapped_column(Float, nullable=True)  # MACD DEA
    macd: Mapped[float | None] = mapped_column(Float, nullable=True)  # MACD 柱

    # 元数据
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class DataUpdateLog(Base):
    """
    数据更新日志表
    记录每次K线数据更新的状态
    """

    __tablename__ = "data_update_log"
    __table_args__ = (
        Index("ix_update_log_type", "update_type", "status"),
        Index("ix_update_log_time", "started_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    update_type: Mapped[str] = mapped_column(String(32))  # 'stock_day', 'index_30m', etc.
    symbol_type: Mapped[str | None] = mapped_column(String(16), nullable=True)  # 'stock', 'index', 'concept', 'all'
    timeframe: Mapped[str | None] = mapped_column(String(10), nullable=True)  # 'day', '30m'

    status: Mapped[DataUpdateStatus] = mapped_column(SqlEnum(DataUpdateStatus))
    records_updated: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )


__all__ = ["Kline", "DataUpdateLog"]
