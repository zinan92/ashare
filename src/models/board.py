"""
Board and sector models
"""
from datetime import datetime

from sqlalchemy import DateTime, Float, Index, Integer, JSON, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base, utcnow


class BoardMapping(Base):
    """板块成分股映射缓存表 - 用于存储板块与股票的映射关系"""

    __tablename__ = "board_mapping"
    __table_args__ = (
        UniqueConstraint("board_name", "board_type"),
        Index("ix_board_lookup", "board_name", "board_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    board_name: Mapped[str] = mapped_column(String(64), index=True)  # 板块名称
    board_type: Mapped[str] = mapped_column(String(16), index=True)  # industry / concept
    board_code: Mapped[str | None] = mapped_column(String(16), nullable=True)  # 板块代码
    constituents: Mapped[list] = mapped_column(JSON)  # 成分股列表 ["000001", "600519", ...]
    last_updated: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class IndustryDaily(Base):
    """同花顺行业板块每日数据表 - 存储90个行业的每日行情和资金流向数据"""

    __tablename__ = "industry_daily"
    __table_args__ = (
        UniqueConstraint("ts_code", "trade_date"),
        Index("ix_industry_trade_date", "trade_date"),
        Index("ix_industry_code", "ts_code"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_date: Mapped[str] = mapped_column(String(8), index=True)  # 交易日期 YYYYMMDD
    ts_code: Mapped[str] = mapped_column(String(16), index=True)  # 板块代码
    industry: Mapped[str] = mapped_column(String(64))  # 板块名称

    # 行情数据
    close: Mapped[float] = mapped_column(Float)  # 收盘指数
    pct_change: Mapped[float] = mapped_column(Float)  # 指数涨跌幅

    # 成分股统计
    company_num: Mapped[int] = mapped_column(Integer)  # 公司数量
    up_count: Mapped[int | None] = mapped_column(Integer, nullable=True)  # 上涨家数
    down_count: Mapped[int | None] = mapped_column(Integer, nullable=True)  # 下跌家数

    # 领涨股信息
    lead_stock: Mapped[str | None] = mapped_column(String(64), nullable=True)  # 领涨股票名称
    lead_stock_code: Mapped[str | None] = mapped_column(String(16), nullable=True)  # 领涨股代码
    pct_change_stock: Mapped[float | None] = mapped_column(Float, nullable=True)  # 领涨股涨跌幅
    close_price: Mapped[float | None] = mapped_column(Float, nullable=True)  # 领涨股最新价

    # 资金流向数据
    net_buy_amount: Mapped[float | None] = mapped_column(Float, nullable=True)  # 流入资金(亿元)
    net_sell_amount: Mapped[float | None] = mapped_column(Float, nullable=True)  # 流出资金(亿元)
    net_amount: Mapped[float | None] = mapped_column(Float, nullable=True)  # 净额(亿元)

    # 估值数据
    industry_pe: Mapped[float | None] = mapped_column(Float, nullable=True)  # 行业PE（市值加权）
    pe_median: Mapped[float | None] = mapped_column(Float, nullable=True)  # PE中位数
    total_mv: Mapped[float | None] = mapped_column(Float, nullable=True)  # 总市值（万元）

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class ConceptDaily(Base):
    """同花顺概念板块每日数据表 - 存储热门概念的每日行情数据"""

    __tablename__ = "concept_daily"
    __table_args__ = (
        UniqueConstraint("code", "trade_date"),
        Index("ix_concept_trade_date", "trade_date"),
        Index("ix_concept_code", "code"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_date: Mapped[str] = mapped_column(String(8), index=True)  # 交易日期 YYYYMMDD
    code: Mapped[str] = mapped_column(String(16), index=True)  # 概念代码
    name: Mapped[str] = mapped_column(String(64))  # 概念名称

    # 行情数据
    close: Mapped[float] = mapped_column(Float)  # 收盘指数
    pct_change: Mapped[float] = mapped_column(Float)  # 指数涨跌幅

    # 成交量数据
    volume: Mapped[float | None] = mapped_column(Float, nullable=True)  # 成交量
    amount: Mapped[float | None] = mapped_column(Float, nullable=True)  # 成交额

    # 龙头股信息
    leader_symbol: Mapped[str | None] = mapped_column(String(16), nullable=True)  # 龙头股代码
    leader_name: Mapped[str | None] = mapped_column(String(64), nullable=True)  # 龙头股名称
    leader_pct_change: Mapped[float | None] = mapped_column(Float, nullable=True)  # 龙头股涨跌幅

    # 成分股统计
    up_count: Mapped[int | None] = mapped_column(Integer, nullable=True)  # 上涨家数
    down_count: Mapped[int | None] = mapped_column(Integer, nullable=True)  # 下跌家数

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class SuperCategoryDaily(Base):
    """超级行业组每日数据表 - 存储14个超级行业组的每日市值和涨跌幅"""

    __tablename__ = "super_category_daily"
    __table_args__ = (
        UniqueConstraint("super_category_name", "trade_date"),
        Index("ix_super_category_trade_date", "trade_date"),
        Index("ix_super_category_name", "super_category_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    super_category_name: Mapped[str] = mapped_column(String(64), index=True)  # 超级行业组名称
    score: Mapped[int] = mapped_column(Integer)  # 进攻性评分 (10-95)
    trade_date: Mapped[str] = mapped_column(String(8), index=True)  # 交易日期 YYYYMMDD

    # 市值数据
    total_mv: Mapped[float] = mapped_column(Float)  # 总市值（万元）
    pct_change: Mapped[float | None] = mapped_column(Float, nullable=True)  # 涨跌幅（相比前一交易日）

    # 成分行业统计
    industry_count: Mapped[int] = mapped_column(Integer)  # 行业总数
    up_count: Mapped[int] = mapped_column(Integer, default=0)  # 上涨行业数
    down_count: Mapped[int] = mapped_column(Integer, default=0)  # 下跌行业数

    # 可选统计指标
    avg_pe: Mapped[float | None] = mapped_column(Float, nullable=True)  # 平均PE
    leading_industry: Mapped[str | None] = mapped_column(String(64), nullable=True)  # 涨幅最大的行业

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


__all__ = ["BoardMapping", "IndustryDaily", "ConceptDaily", "SuperCategoryDaily"]
