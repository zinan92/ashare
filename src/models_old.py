from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import (
    DateTime,
    Enum as SqlEnum,
    Float,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# 旧的 Timeframe 枚举，保留用于 API 响应兼容
class Timeframe(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    MINS_30 = "30m"


class SymbolMetadata(Base):
    __tablename__ = "symbol_metadata"

    ticker: Mapped[str] = mapped_column(String(16), primary_key=True)
    name: Mapped[str] = mapped_column(String(64))

    # Market value fields (from Tushare daily_basic, unit: 万元)
    total_mv: Mapped[float | None] = mapped_column(Float, nullable=True)  # 总市值
    circ_mv: Mapped[float | None] = mapped_column(Float, nullable=True)   # 流通市值

    # Valuation metrics (from Tushare daily_basic)
    pe_ttm: Mapped[float | None] = mapped_column(Float, nullable=True)    # 市盈率TTM
    pb: Mapped[float | None] = mapped_column(Float, nullable=True)        # 市净率

    # Basic info
    list_date: Mapped[str | None] = mapped_column(String(8), nullable=True)  # 上市日期 YYYYMMDD

    # Company information (from Tushare stock_company)
    introduction: Mapped[str | None] = mapped_column(Text, nullable=True)  # 公司介绍
    main_business: Mapped[str | None] = mapped_column(Text, nullable=True)  # 主要业务及产品
    business_scope: Mapped[str | None] = mapped_column(Text, nullable=True)  # 经营范围
    chairman: Mapped[str | None] = mapped_column(String(64), nullable=True)  # 法人代表
    manager: Mapped[str | None] = mapped_column(String(64), nullable=True)  # 总经理
    reg_capital: Mapped[float | None] = mapped_column(Float, nullable=True)  # 注册资本(万元)
    setup_date: Mapped[str | None] = mapped_column(String(10), nullable=True)  # 成立日期
    province: Mapped[str | None] = mapped_column(String(32), nullable=True)  # 所在省份
    city: Mapped[str | None] = mapped_column(String(32), nullable=True)  # 所在城市
    employees: Mapped[int | None] = mapped_column(Integer, nullable=True)  # 员工人数
    website: Mapped[str | None] = mapped_column(String(128), nullable=True)  # 公司网站

    # Industry and concept classifications
    industry_lv1: Mapped[str | None] = mapped_column(String(64), nullable=True)
    industry_lv2: Mapped[str | None] = mapped_column(String(64), nullable=True)
    industry_lv3: Mapped[str | None] = mapped_column(String(64), nullable=True)
    super_category: Mapped[str | None] = mapped_column(String(64), nullable=True)  # 超级行业组（14个大类）
    concepts: Mapped[list | None] = mapped_column(JSON, nullable=True)  # 概念板块列表

    last_sync: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )


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


# ============================================================================
# K线数据统一化相关模型 (PRD: K线数据统一化重构)
# ============================================================================

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


class DataUpdateStatus(str, Enum):
    """数据更新状态"""
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"


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


# ============================================================================
# 模拟交易系统相关模型
# ============================================================================

class TradeType(str, Enum):
    """交易类型"""
    BUY = "buy"
    SELL = "sell"


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
