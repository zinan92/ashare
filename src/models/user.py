"""
User-related models (watchlist, evaluations)
"""
from datetime import datetime

from sqlalchemy import DateTime, Float, Index, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base, utcnow


class Watchlist(Base):
    """用户自选股列表"""
    __tablename__ = "watchlist"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(
        String(16),
        unique=True,
        index=True,
        comment="股票代码"
    )
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        comment="添加时间"
    )
    purchase_price: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        comment="买入价格（虚拟投资组合）"
    )
    purchase_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="买入日期（虚拟投资组合）"
    )
    shares: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        comment="持有股数（基于10000元投资计算）"
    )
    category: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        default="未分类",
        comment="分类标签（如：AI应用、半导体、新能源等）"
    )
    is_focus: Mapped[bool] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="是否重点关注"
    )


class KlineEvaluation(Base):
    """K线标注评估表 - 用于收集训练数据"""

    __tablename__ = "kline_evaluations"
    __table_args__ = (
        Index("ix_eval_ticker", "ticker"),
        Index("ix_eval_score", "score"),
        Index("ix_eval_date", "eval_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(16), index=True)  # 股票代码
    stock_name: Mapped[str | None] = mapped_column(String(64), nullable=True)  # 股票名称
    timeframe: Mapped[str] = mapped_column(String(10), default="day")  # 周期: day/30m

    # 评估时间信息
    eval_date: Mapped[str] = mapped_column(String(10), index=True)  # 评估日期 YYYY-MM-DD
    kline_end_date: Mapped[str] = mapped_column(String(10))  # K线截止日期 YYYY-MM-DD

    # 用户标注
    description: Mapped[str | None] = mapped_column(Text, nullable=True)  # 文字描述
    score: Mapped[int] = mapped_column(Integer)  # 评分 0-10
    tags: Mapped[list | None] = mapped_column(JSON, nullable=True)  # 自动提取的标签

    # 截图和K线数据
    screenshot_path: Mapped[str | None] = mapped_column(String(255), nullable=True)  # 截图路径
    kline_data: Mapped[dict | None] = mapped_column(JSON, nullable=True)  # 120根K线数据+指标

    # 验证字段 (仅score>=8时使用)
    price_at_eval: Mapped[float | None] = mapped_column(Float, nullable=True)  # 评估时收盘价
    price_1d: Mapped[float | None] = mapped_column(Float, nullable=True)  # 1日后价格
    price_5d: Mapped[float | None] = mapped_column(Float, nullable=True)  # 5日后价格
    return_1d: Mapped[float | None] = mapped_column(Float, nullable=True)  # 1日收益率%
    return_5d: Mapped[float | None] = mapped_column(Float, nullable=True)  # 5日收益率%
    verified: Mapped[bool] = mapped_column(Integer, default=False)  # 是否已验证

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


__all__ = ["Watchlist", "KlineEvaluation"]
