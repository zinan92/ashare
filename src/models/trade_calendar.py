"""
Trade calendar model
"""
from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base, utcnow


class TradeCalendar(Base):
    """
    交易日历表
    存储交易日信息，用于判断是否需要更新数据
    """

    __tablename__ = "trade_calendar"
    __table_args__ = (
        Index("ix_calendar_trading", "is_trading_day", "date"),
    )

    date: Mapped[str] = mapped_column(String(10), primary_key=True)  # 'YYYY-MM-DD'
    is_trading_day: Mapped[bool] = mapped_column(Integer)  # SQLite 用 Integer 存 Boolean
    exchange: Mapped[str] = mapped_column(String(8), default="SSE")  # 'SSE', 'SZSE'

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )


__all__ = ["TradeCalendar"]
