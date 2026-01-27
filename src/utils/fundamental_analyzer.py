"""
Fundamental Analysis Utilities

基本面分析工具，用于：
1. 检测价格与基本面背离
2. 行业内横向对比排名
3. 识别股价新高但利润未新高的情况
"""

from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc
import tushare as ts
from dotenv import load_dotenv
import os

from src.models.kline import Kline
from src.models.symbol import SymbolMetadata
from src.models.board import IndustryDaily


class FundamentalAnalyzer:
    """基本面分析器"""

    def __init__(self, session: Session):
        self.session = session
        self._pro = None

    @property
    def pro(self):
        """延迟初始化Tushare Pro API"""
        if self._pro is None:
            load_dotenv()
            token = os.getenv("TUSHARE_TOKEN", "")
            ts.set_token(token)
            self._pro = ts.pro_api()
        return self._pro

    def get_52w_high_low(self, ticker: str, trade_date: str) -> Tuple[float, float]:
        """
        获取股票52周最高价和最低价

        Args:
            ticker: 股票代码 (不带后缀)
            trade_date: 当前交易日 YYYYMMDD

        Returns:
            (52周最高价, 52周最低价)
        """
        # 计算52周前的日期
        current_date = datetime.strptime(trade_date, "%Y%m%d")
        weeks_52_ago = current_date - timedelta(days=364)  # 52周约364天
        start_date = weeks_52_ago.strftime("%Y-%m-%d")

        # 查询52周内的K线数据
        klines = self.session.query(Kline).filter(
            and_(
                Kline.symbol_code == ticker,
                Kline.symbol_type == 'stock',
                Kline.timeframe == 'DAY',
                Kline.trade_time >= start_date
            )
        ).all()

        if not klines:
            return (0.0, 0.0)

        high_52w = max(k.high for k in klines)
        low_52w = min(k.low for k in klines)

        return (high_52w, low_52w)

    def get_financial_indicators(self, ticker: str, periods: int = 8) -> Optional[List[Dict]]:
        """
        获取股票的财务指标数据

        Args:
            ticker: 股票代码 (不带后缀)
            periods: 获取最近N个季度的数据

        Returns:
            财务指标列表，按报告期倒序排列
        """
        # 转换为Tushare格式
        ts_code = f"{ticker}.SZ" if ticker.startswith(("0", "3")) else f"{ticker}.SH"

        try:
            df = self.pro.fina_indicator(
                ts_code=ts_code,
                fields='ts_code,ann_date,end_date,eps,dt_eps,roe,roe_dt,roa,grossprofit_margin,netprofit_margin,netprofit_yoy,or_yoy,q_netprofit_yoy,q_sales_yoy,debt_to_assets,current_ratio,quick_ratio'
            )

            if df is None or len(df) == 0:
                return None

            # 转换为字典列表，按报告期排序
            indicators = []
            for _, row in df.iterrows():
                indicators.append({
                    'end_date': str(row.get('end_date', '')),
                    'ann_date': str(row.get('ann_date', '')),
                    'eps': float(row.get('eps', 0)) if row.get('eps') is not None else None,
                    'roe': float(row.get('roe', 0)) if row.get('roe') is not None else None,
                    'roa': float(row.get('roa', 0)) if row.get('roa') is not None else None,
                    'gross_margin': float(row.get('grossprofit_margin', 0)) if row.get('grossprofit_margin') is not None else None,
                    'net_margin': float(row.get('netprofit_margin', 0)) if row.get('netprofit_margin') is not None else None,
                    'netprofit_yoy': float(row.get('netprofit_yoy', 0)) if row.get('netprofit_yoy') is not None else None,
                    'revenue_yoy': float(row.get('or_yoy', 0)) if row.get('or_yoy') is not None else None,
                    'q_netprofit_yoy': float(row.get('q_netprofit_yoy', 0)) if row.get('q_netprofit_yoy') is not None else None,
                    'q_sales_yoy': float(row.get('q_sales_yoy', 0)) if row.get('q_sales_yoy') is not None else None,
                })

            # 按报告期降序排序
            indicators.sort(key=lambda x: x['end_date'], reverse=True)

            return indicators[:periods]

        except Exception as e:
            print(f"获取{ticker}财务指标失败: {e}")
            return None

    def analyze_price_fundamental_divergence(
        self,
        ticker: str,
        current_price: float,
        price_change_pct: float,
        trade_date: str
    ) -> Dict:
        """
        分析价格与基本面背离

        Args:
            ticker: 股票代码
            current_price: 当前价格
            price_change_pct: 近期涨幅
            trade_date: 交易日期

        Returns:
            {
                "is_divergent": bool,
                "price_vs_52w_high": float,  # 距52周高点的百分比
                "latest_profit_yoy": float,  # 最新净利润同比
                "profit_trend": str,  # "增长"/"下降"/"亏损"
                "divergence_level": str,  # "严重"/"中等"/"轻微"/"无"
                "warning": str  # 警告信息
            }
        """
        # 1. 获取52周高点
        high_52w, low_52w = self.get_52w_high_low(ticker, trade_date)

        if high_52w == 0:
            return {
                "is_divergent": False,
                "warning": "数据不足"
            }

        # 计算当前价格距52周高点的距离
        price_vs_high = (current_price / high_52w * 100) if high_52w > 0 else 0

        # 2. 获取财务指标
        indicators = self.get_financial_indicators(ticker, periods=8)

        if not indicators or len(indicators) == 0:
            return {
                "is_divergent": False,
                "price_vs_52w_high": price_vs_high,
                "warning": "无财务数据"
            }

        latest = indicators[0]
        latest_profit_yoy = latest.get('netprofit_yoy')
        latest_roe = latest.get('roe')

        # 判断利润趋势
        if latest_profit_yoy is None:
            profit_trend = "未知"
        elif latest_profit_yoy > 20:
            profit_trend = "高速增长"
        elif latest_profit_yoy > 0:
            profit_trend = "增长"
        elif latest_profit_yoy > -10:
            profit_trend = "微降"
        else:
            profit_trend = "下降"

        # 判断是否为亏损股
        if latest_roe is not None and latest_roe < 0:
            profit_trend = "亏损"

        # 3. 判断背离程度
        is_near_high = price_vs_high >= 95  # 距52周高点5%以内
        is_divergent = False
        divergence_level = "无"
        warning = ""

        if is_near_high:
            # 股价接近新高
            if profit_trend in ["亏损", "下降"]:
                is_divergent = True
                divergence_level = "严重"
                warning = f"⚠️ 股价接近新高({price_vs_high:.1f}%)，但公司{profit_trend}"
            elif profit_trend == "微降" or (latest_profit_yoy is not None and latest_profit_yoy < 10):
                is_divergent = True
                divergence_level = "中等"
                warning = f"⚠️ 股价接近新高({price_vs_high:.1f}%)，但业绩增长缓慢({latest_profit_yoy:.1f}%)"
            elif price_change_pct > 30 and (latest_profit_yoy is not None and latest_profit_yoy < price_change_pct / 2):
                # 股价涨幅远超业绩增长
                is_divergent = True
                divergence_level = "轻微"
                warning = f"⚠️ 股价涨幅({price_change_pct:.1f}%)超过业绩增长({latest_profit_yoy:.1f}%)"

        return {
            "is_divergent": is_divergent,
            "price_vs_52w_high": round(price_vs_high, 2),
            "latest_profit_yoy": latest_profit_yoy,
            "profit_trend": profit_trend,
            "divergence_level": divergence_level,
            "warning": warning,
            "roe": latest_roe,
            "gross_margin": latest.get('gross_margin'),
            "net_margin": latest.get('net_margin')
        }

    def get_industry_ranking(
        self,
        ticker: str,
        industry: str,
        metric: str = 'roe'
    ) -> Dict:
        """
        获取股票在行业内的排名

        Args:
            ticker: 股票代码
            industry: 行业名称
            metric: 排名指标 ('roe', 'profit_yoy', 'gross_margin')

        Returns:
            {
                "industry": str,
                "metric": str,
                "value": float,
                "rank": int,
                "total_count": int,
                "percentile": float,  # 百分位数
                "is_top20": bool
            }
        """
        # 1. 获取同行业所有股票
        stocks = self.session.query(SymbolMetadata).filter(
            SymbolMetadata.industry_lv1 == industry
        ).all()

        if not stocks or len(stocks) == 0:
            return {
                "industry": industry,
                "error": "未找到同行业股票"
            }

        # 2. 获取所有股票的财务指标
        stock_metrics = []

        for stock in stocks:
            indicators = self.get_financial_indicators(stock.ticker, periods=1)

            if not indicators or len(indicators) == 0:
                continue

            latest = indicators[0]

            # 根据metric选择指标值
            if metric == 'roe':
                value = latest.get('roe')
            elif metric == 'profit_yoy':
                value = latest.get('netprofit_yoy')
            elif metric == 'gross_margin':
                value = latest.get('gross_margin')
            else:
                value = None

            if value is not None:
                stock_metrics.append({
                    'ticker': stock.ticker,
                    'name': stock.name,
                    'value': value
                })

        if not stock_metrics:
            return {
                "industry": industry,
                "metric": metric,
                "error": "未获取到有效数据"
            }

        # 3. 排序并计算排名
        stock_metrics.sort(key=lambda x: x['value'], reverse=True)

        # 找到目标股票的排名
        target_rank = None
        target_value = None

        for i, item in enumerate(stock_metrics, 1):
            if item['ticker'] == ticker:
                target_rank = i
                target_value = item['value']
                break

        if target_rank is None:
            return {
                "industry": industry,
                "metric": metric,
                "error": f"未找到{ticker}的数据"
            }

        total_count = len(stock_metrics)
        percentile = (1 - (target_rank - 1) / total_count) * 100
        is_top20 = percentile >= 80

        return {
            "industry": industry,
            "metric": metric,
            "value": round(target_value, 2),
            "rank": target_rank,
            "total_count": total_count,
            "percentile": round(percentile, 1),
            "is_top20": is_top20,
            "metric_name": {
                'roe': 'ROE',
                'profit_yoy': '净利润增长',
                'gross_margin': '毛利率'
            }.get(metric, metric)
        }

    def batch_analyze_fundamentals(
        self,
        stocks: List[Dict],  # [{"ticker": "300077", "name": "国民技术", "current_price": 24.69, ...}]
        trade_date: str
    ) -> Dict:
        """
        批量分析股票基本面

        Args:
            stocks: 股票列表
            trade_date: 交易日期

        Returns:
            {
                "divergence_alerts": [...],  # 价格脱离基本面的警报
                "quality_stocks": [...],  # 基本面优质的股票
                "risk_stocks": [...]  # 高风险股票
            }
        """
        divergence_alerts = []
        quality_stocks = []
        risk_stocks = []

        for stock in stocks:
            ticker = stock['ticker']

            # 1. 价格与基本面背离分析
            divergence = self.analyze_price_fundamental_divergence(
                ticker=ticker,
                current_price=stock.get('current_price', 0),
                price_change_pct=stock.get('change_pct', 0),
                trade_date=trade_date
            )

            if divergence.get('is_divergent'):
                divergence_alerts.append({
                    "ticker": ticker,
                    "name": stock.get('name'),
                    "sector": stock.get('sector'),
                    "warning": divergence.get('warning'),
                    "divergence_level": divergence.get('divergence_level'),
                    "details": divergence
                })

            # 2. 行业排名分析
            industry = stock.get('industry')
            if industry:
                ranking = self.get_industry_ranking(ticker, industry, 'roe')

                # 基本面优质：ROE行业Top 20%
                if ranking.get('is_top20'):
                    quality_stocks.append({
                        "ticker": ticker,
                        "name": stock.get('name'),
                        "sector": stock.get('sector'),
                        "industry": industry,
                        "roe": ranking.get('value'),
                        "rank": ranking.get('rank'),
                        "percentile": ranking.get('percentile'),
                        "profit_yoy": divergence.get('latest_profit_yoy')
                    })

                # 高风险：业绩差且股价涨幅大
                if divergence.get('profit_trend') in ['亏损', '下降'] and stock.get('change_pct', 0) > 5:
                    risk_stocks.append({
                        "ticker": ticker,
                        "name": stock.get('name'),
                        "sector": stock.get('sector'),
                        "warning": f"业绩{divergence.get('profit_trend')}但股价上涨{stock.get('change_pct', 0):.1f}%",
                        "roe": divergence.get('roe'),
                        "profit_yoy": divergence.get('latest_profit_yoy')
                    })

        return {
            "divergence_alerts": divergence_alerts,
            "quality_stocks": quality_stocks,
            "risk_stocks": risk_stocks
        }


__all__ = ["FundamentalAnalyzer"]
