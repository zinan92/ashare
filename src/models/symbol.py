"""
Symbol metadata model
"""
from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base, utcnow


class SymbolMetadata(Base):
    """股票元数据表"""
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


__all__ = ["SymbolMetadata"]
