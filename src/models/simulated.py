"""
Simulated trading models
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
)
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base, utcnow
from src.models.enums import TradeType


class SimulatedAccount(Base):
    """
    模拟账户表
    存储模拟交易的初始资金配置
    """

    __tablename__ = "simulated_account"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    initial_capital: Mapped[float] = mapped_column(
        Float, default=10000000, comment="初始资金，默认1000万"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )


class SimulatedTrade(Base):
    """
    模拟交易记录表
    记录每一笔模拟买入/卖出操作
    """

    __tablename__ = "simulated_trades"
    __table_args__ = (
        Index("ix_sim_trade_ticker", "ticker"),
        Index("ix_sim_trade_date", "trade_date"),
        Index("ix_sim_trade_type", "trade_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(16), index=True, comment="股票代码")
    stock_name: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="股票名称")
    trade_type: Mapped[TradeType] = mapped_column(
        SqlEnum(TradeType), index=True, comment="交易类型: buy/sell"
    )
    trade_date: Mapped[str] = mapped_column(String(10), index=True, comment="交易日期 YYYY-MM-DD")
    trade_price: Mapped[float] = mapped_column(Float, comment="成交价格")
    shares: Mapped[int] = mapped_column(Integer, comment="交易股数")
    amount: Mapped[float] = mapped_column(Float, comment="交易金额")
    position_pct: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="仓位百分比（买入时记录）"
    )
    note: Mapped[str | None] = mapped_column(Text, nullable=True, comment="交易备注")

    # 卖出时的盈亏信息
    realized_pnl: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="实现盈亏金额"
    )
    realized_pnl_pct: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="实现盈亏百分比"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )


class SimulatedPosition(Base):
    """
    模拟持仓表
    记录当前持有的股票仓位
    """

    __tablename__ = "simulated_positions"
    __table_args__ = (
        Index("ix_sim_position_ticker", "ticker"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(
        String(16), unique=True, index=True, comment="股票代码"
    )
    stock_name: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="股票名称")
    shares: Mapped[int] = mapped_column(Integer, comment="持仓股数")
    cost_price: Mapped[float] = mapped_column(Float, comment="成本价（加权平均）")
    cost_amount: Mapped[float] = mapped_column(Float, comment="成本金额")
    first_buy_date: Mapped[str] = mapped_column(String(10), comment="首次买入日期")
    last_trade_date: Mapped[str] = mapped_column(String(10), comment="最后交易日期")

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


__all__ = ["SimulatedAccount", "SimulatedTrade", "SimulatedPosition"]
