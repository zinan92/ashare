"""
Data models for daily review system.

These Pydantic models define the structured data format for A-share market
daily reviews, following the principle: Structured Evidence → Attribution Analysis → Narrative Generation.
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import datetime


class IndexSnapshot(BaseModel):
    """
    Major market index snapshot with pattern analysis.

    Includes K-line data, pattern labels, volume trends, and technical positions.
    """
    name: str = Field(..., description="Index name (e.g., '上证指数')")
    code: str = Field(..., description="Index ticker (e.g., '000001.SH')")

    # OHLCV Data
    open: float = Field(..., description="Opening price")
    close: float = Field(..., description="Closing price")
    high: float = Field(..., description="Highest price")
    low: float = Field(..., description="Lowest price")
    change_pct: float = Field(..., description="Change percentage")
    volume: float = Field(..., description="Trading volume")
    amount: float = Field(..., description="Trading amount")

    # Pattern Analysis
    pattern: str = Field(..., description="K-line pattern label (e.g., '大阳线', '小阴线带上影')")
    body_ratio: float = Field(..., description="Body/total range ratio")
    upper_shadow_ratio: float = Field(..., description="Upper shadow/total range ratio")
    lower_shadow_ratio: float = Field(..., description="Lower shadow/total range ratio")

    # Volume Analysis
    volume_vs_5d: float = Field(..., description="Volume vs 5-day average ratio")
    volume_vs_10d: float = Field(..., description="Volume vs 10-day average ratio")
    volume_trend: str = Field(..., description="Volume trend label (e.g., '放量', '缩量')")

    # Technical Position
    ma5: Optional[float] = Field(None, description="5-day moving average")
    ma10: Optional[float] = Field(None, description="10-day moving average")
    ma20: Optional[float] = Field(None, description="20-day moving average")
    ma_position: Optional[str] = Field(None, description="MA position label (e.g., 'MA10上方运行')")


class SectorSnapshot(BaseModel):
    """
    Industry or concept sector snapshot with money flow analysis.

    Includes performance metrics, money flow, constituent statistics, and strength assessment.
    """
    sector_name: str = Field(..., description="Sector name (e.g., '银行', '人工智能')")
    sector_code: str = Field(..., description="Sector code (e.g., '886001.TI')")
    sector_type: str = Field(..., description="Sector type: 'industry' or 'concept'")

    # Performance
    change_pct: float = Field(..., description="Sector change percentage")

    # Money Flow
    net_inflow: float = Field(..., description="Net money inflow in 亿元 (100M yuan)")
    net_buy_amount: float = Field(..., description="Total buy amount in 亿元")
    net_sell_amount: float = Field(..., description="Total sell amount in 亿元")
    flow_trend: str = Field(..., description="Money flow label (e.g., '主力流入', '大幅流出')")

    # Constituent Statistics
    up_count: int = Field(..., description="Number of stocks up")
    down_count: int = Field(..., description="Number of stocks down")
    flat_count: int = Field(0, description="Number of stocks flat")
    limit_up: int = Field(0, description="Number of limit-up stocks")
    limit_down: int = Field(0, description="Number of limit-down stocks")
    strength: str = Field(..., description="Sector strength label (e.g., '强势', '偏弱')")

    # Valuation (for industries)
    pe_valuation: Optional[float] = Field(None, description="Average P/E ratio")
    valuation_position: Optional[str] = Field(None, description="Valuation position label (e.g., '历史低位')")

    # Leadership
    leader_symbol: Optional[str] = Field(None, description="Leading stock ticker")
    leader_name: Optional[str] = Field(None, description="Leading stock name")


class SampleStock(BaseModel):
    """
    Representative sample stock with role classification.

    Selected to illustrate sector performance with specific examples.
    Roles: 龙头 (leader), 高位核心 (high core), 补涨 (catch-up), 回撤 (pullback).
    """
    ticker: str = Field(..., description="Stock ticker (e.g., '688012.SH')")
    name: str = Field(..., description="Stock name (e.g., '中微公司')")
    role: str = Field(..., description="Stock role: '龙头', '高位核心', '补涨', or '回撤'")
    market_cap_rank: int = Field(..., description="Market cap rank within sector")

    # Today's Performance
    open: float = Field(..., description="Opening price")
    close: float = Field(..., description="Closing price")
    high: float = Field(..., description="Highest price")
    low: float = Field(..., description="Lowest price")
    change_pct: float = Field(..., description="Change percentage")
    pattern: str = Field(..., description="K-line pattern label")
    volume_ratio: float = Field(..., description="Volume vs 5-day average")

    # Recent Performance
    days_5_change: float = Field(..., description="5-day cumulative change %")
    days_10_change: float = Field(..., description="10-day cumulative change %")
    position: str = Field(..., description="Position label (e.g., '高位回调', '低位企稳')")

    # Technical
    ma10_break: bool = Field(..., description="Whether broke MA10 today")
    ma_position: str = Field(..., description="MA position label")


class MarketSentiment(BaseModel):
    """
    Overall market sentiment indicators.

    Aggregates breadth indicators: advance/decline ratio, limit boards, and turnover.
    """
    # Advance/Decline
    up_count: int = Field(..., description="Total stocks up")
    down_count: int = Field(..., description="Total stocks down")
    flat_count: int = Field(..., description="Total stocks flat")
    up_down_ratio: float = Field(..., description="Up/down ratio")
    ad_sentiment: str = Field(..., description="A/D sentiment label (e.g., '偏多')")

    # Limit Boards
    limit_up: int = Field(..., description="Total limit-up stocks")
    limit_down: int = Field(..., description="Total limit-down stocks")
    first_board_success_rate: float = Field(..., description="First-board seal success rate")
    limit_sentiment: str = Field(..., description="Limit board sentiment label (e.g., '活跃')")

    # Turnover
    total_amount: float = Field(..., description="Total market turnover (yuan)")
    vs_yesterday: float = Field(..., description="Turnover vs yesterday ratio")
    vs_5d_avg: float = Field(..., description="Turnover vs 5-day average ratio")
    activity: str = Field(..., description="Activity label (e.g., '缩量', '放量')")

    # Overall Sentiment
    sentiment_score: int = Field(..., description="Overall sentiment score (-5 to +5)")
    sentiment_label: str = Field(..., description="Overall sentiment label (e.g., '强势', '震荡')")


class FundamentalAlert(BaseModel):
    """
    Fundamental analysis alert for stocks with price-fundamental divergence.
    """
    ticker: str = Field(..., description="Stock ticker")
    name: str = Field(..., description="Stock name")
    sector: str = Field(..., description="Sector name")
    warning: str = Field(..., description="Warning message")
    divergence_level: str = Field(..., description="Divergence level: '严重', '中等', '轻微'")
    details: Dict = Field(..., description="Detailed analysis data")


class QualityStock(BaseModel):
    """
    Quality stock with strong fundamentals (top 20% in industry).
    """
    ticker: str = Field(..., description="Stock ticker")
    name: str = Field(..., description="Stock name")
    sector: str = Field(..., description="Sector name")
    industry: str = Field(..., description="Industry name")
    roe: Optional[float] = Field(None, description="ROE value")
    rank: int = Field(..., description="Rank within industry")
    percentile: float = Field(..., description="Percentile (0-100)")
    profit_yoy: Optional[float] = Field(None, description="Net profit YoY growth %")


class RiskStock(BaseModel):
    """
    Risk stock with poor fundamentals but rising prices.
    """
    ticker: str = Field(..., description="Stock ticker")
    name: str = Field(..., description="Stock name")
    sector: str = Field(..., description="Sector name")
    warning: str = Field(..., description="Risk warning message")
    roe: Optional[float] = Field(None, description="ROE value")
    profit_yoy: Optional[float] = Field(None, description="Net profit YoY growth %")


class FundamentalAnalysis(BaseModel):
    """
    Comprehensive fundamental analysis results.
    """
    divergence_alerts: List[FundamentalAlert] = Field(
        default_factory=list,
        description="Stocks with price-fundamental divergence"
    )
    quality_stocks: List[QualityStock] = Field(
        default_factory=list,
        description="Stocks with strong fundamentals (top 20% in industry)"
    )
    risk_stocks: List[RiskStock] = Field(
        default_factory=list,
        description="Stocks with poor fundamentals but rising prices"
    )


class DailyReviewSnapshot(BaseModel):
    """
    Complete daily review data snapshot.

    Top-level container for all structured market data needed for daily review generation.
    This snapshot follows the principle: provide labeled conclusions rather than raw numbers,
    so AI can reliably narrate without fabrication.
    """
    trade_date: str = Field(..., description="Trade date in YYYYMMDD format")

    # Core Data
    indices: List[IndexSnapshot] = Field(..., description="Major market indices")
    sectors: List[SectorSnapshot] = Field(..., description="Industry sectors")
    concepts: List[SectorSnapshot] = Field(..., description="Concept/theme sectors")
    sentiment: MarketSentiment = Field(..., description="Market sentiment indicators")

    # Sample Stocks (by sector name)
    sample_stocks: Dict[str, List[SampleStock]] = Field(
        ...,
        description="Representative stocks grouped by sector name"
    )

    # Fundamental Analysis
    fundamental_analysis: Optional[Dict] = Field(
        None,
        description="Fundamental analysis results including divergence alerts, quality stocks, and risk stocks"
    )

    # Optional Technical Data
    technical_positions: Optional[Dict] = Field(
        None,
        description="Additional technical position data by index"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "trade_date": "20250126",
                "indices": [
                    {
                        "name": "上证指数",
                        "code": "000001.SH",
                        "close": 3415.2,
                        "change_pct": -0.15,
                        "pattern": "小阴线带上影",
                        "volume_trend": "缩量",
                        "ma_position": "MA10下方运行"
                    }
                ],
                "sentiment": {
                    "up_down_ratio": 1.08,
                    "limit_up": 45,
                    "sentiment_label": "偏多"
                }
            }
        }


class DailyReviewMetadata(BaseModel):
    """
    Metadata for generated daily reviews.

    Used to track review generation history and parameters.
    """
    trade_date: str = Field(..., description="Trade date in YYYYMMDD format")
    generated_at: datetime = Field(default_factory=datetime.now, description="Generation timestamp")
    model: str = Field(..., description="AI model used for generation")
    temperature: float = Field(..., description="Generation temperature")
    snapshot_path: str = Field(..., description="Path to source snapshot file")
    review_path: str = Field(..., description="Path to generated review file")
    word_count: Optional[int] = Field(None, description="Approximate word count")

    class Config:
        json_schema_extra = {
            "example": {
                "trade_date": "20250126",
                "generated_at": "2025-01-26T16:00:00",
                "model": "claude-sonnet-4-5-20250929",
                "temperature": 0.3,
                "snapshot_path": "docs/daily_review/snapshots/20250126.json",
                "review_path": "docs/daily_review/reviews/20250126.md",
                "word_count": 1050
            }
        }
