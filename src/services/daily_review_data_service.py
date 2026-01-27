"""
Daily Review Data Service

Main service for collecting and structuring all data needed for daily market review.
Orchestrates data collection from multiple sources and applies labeling algorithms.
"""

import asyncio
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func

from src.models.kline import Kline
from src.models.board import IndustryDaily, ConceptDaily, BoardMapping
from src.models.symbol import SymbolMetadata
from src.repositories.kline_repository import KlineRepository
from src.repositories.industry_daily_repository import IndustryDailyRepository
from src.repositories.concept_daily_repository import ConceptDailyRepository
from src.repositories.symbol_repository import SymbolRepository
from src.utils.kline_analyzer import KlinePatternAnalyzer
from src.utils.market_sentiment_analyzer import MarketSentimentAnalyzer
from src.utils.fundamental_analyzer import FundamentalAnalyzer
from src.utils.indicators import calculate_ma
from src.schemas.daily_review import (
    DailyReviewSnapshot,
    IndexSnapshot,
    SectorSnapshot,
    SampleStock,
    MarketSentiment
)


class DailyReviewDataService:
    """
    Collect and structure all daily review data.

    This service follows the principle: Structured Evidence → Attribution Analysis → Narrative Generation.
    It provides labeled conclusions rather than raw numbers for reliable AI interpretation.
    """

    # Major indices to track
    TRACKED_INDICES = [
        "000001.SH",  # 上证指数
        "399001.SZ",  # 深证成指
        "399006.SZ",  # 创业板指
        "000688.SH",  # 科创50
        "899050.BJ",  # 北证50
        "000852.SH",  # 中证1000
    ]

    def __init__(self, session: Session):
        """
        Initialize service with database session.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session
        self.kline_repo = KlineRepository(session)
        self.industry_repo = IndustryDailyRepository(session)
        self.concept_repo = ConceptDailyRepository(session)
        self.symbol_repo = SymbolRepository(session)
        self.pattern_analyzer = KlinePatternAnalyzer()
        self.sentiment_analyzer = MarketSentimentAnalyzer()
        self.fundamental_analyzer = FundamentalAnalyzer(session)

    async def collect_review_data(self, trade_date: str) -> DailyReviewSnapshot:
        """
        Collect all data for daily review.

        Args:
            trade_date: Date in YYYYMMDD format

        Returns:
            Complete DailyReviewSnapshot with structured data

        Raises:
            ValueError: If no data exists for the given date
        """
        # Validate date format
        try:
            datetime.strptime(trade_date, "%Y%m%d")
        except ValueError:
            raise ValueError(f"Invalid date format: {trade_date}, expected YYYYMMDD")

        # 1. Index data with pattern analysis
        indices = await self._collect_index_data(trade_date)
        if not indices:
            raise ValueError(f"No index data found for {trade_date}")

        # 2. Sector performance with money flow
        sectors = await self._collect_sector_data(trade_date)

        # 3. Hot concepts with leaders
        concepts = await self._collect_concept_data(trade_date)

        # 4. Market sentiment indicators
        sentiment = await self._calculate_market_sentiment(trade_date)

        # 5. Representative sample stocks
        samples = await self._select_sample_stocks(trade_date, sectors, concepts)

        # 6. Fundamental analysis
        fundamental_analysis = await self._analyze_fundamentals(trade_date, samples)

        return DailyReviewSnapshot(
            trade_date=trade_date,
            indices=indices,
            sectors=sectors,
            concepts=concepts,
            sentiment=sentiment,
            sample_stocks=samples,
            fundamental_analysis=fundamental_analysis
        )

    async def _collect_index_data(self, trade_date: str) -> List[IndexSnapshot]:
        """
        Collect major index data with K-line pattern analysis.

        Args:
            trade_date: Date in YYYYMMDD format

        Returns:
            List of IndexSnapshot objects
        """
        indices = []

        # Convert to YYYY-MM-DD format for database queries
        formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"

        for symbol in self.TRACKED_INDICES:
            # Get historical data for MA calculation (need 20+ days)
            end_date = datetime.strptime(trade_date, "%Y%m%d")
            start_date = (end_date - timedelta(days=35)).strftime("%Y-%m-%d")

            history = self.session.query(Kline).filter(
                Kline.symbol_code == symbol,
                Kline.symbol_type == 'index',
                Kline.timeframe == 'DAY',
                Kline.trade_time >= start_date,
                Kline.trade_time <= formatted_date
            ).order_by(Kline.trade_time).all()

            if len(history) < 2:
                continue

            kline = history[-1]  # Current day
            prev_close = history[-2].close if len(history) >= 2 else kline.open

            if len(history) < 5:
                continue

            # Calculate MAs
            closes = [k.close for k in history]
            ma5 = calculate_ma(closes, 5)[-1] if len(closes) >= 5 else None
            ma10 = calculate_ma(closes, 10)[-1] if len(closes) >= 10 else None
            ma20 = calculate_ma(closes, 20)[-1] if len(closes) >= 20 else None

            # Analyze pattern
            pattern, pattern_details = self.pattern_analyzer.analyze_pattern(kline)

            # Calculate volume averages
            volumes = [k.volume for k in history[-10:]]
            avg_5d = sum(volumes[-5:]) / 5 if len(volumes) >= 5 else kline.volume
            avg_10d = sum(volumes) / len(volumes) if volumes else kline.volume

            # Get volume trend
            volume_trend, volume_ratios = self.pattern_analyzer.get_volume_trend_label(
                kline.volume, avg_5d, avg_10d
            )

            # Get MA position
            ma_position = None
            if ma5 and ma10 and ma20:
                ma_position, _ = self.pattern_analyzer.analyze_ma_position(
                    kline.close,
                    {'ma5': ma5, 'ma10': ma10, 'ma20': ma20}
                )

            # Get symbol info for name
            symbol_info = self.session.query(SymbolMetadata).filter(SymbolMetadata.ticker == symbol).first()
            name = symbol_info.name if symbol_info else symbol

            indices.append(IndexSnapshot(
                name=name,
                code=symbol,
                open=kline.open,
                close=kline.close,
                high=kline.high,
                low=kline.low,
                change_pct=round((kline.close - prev_close) / prev_close * 100, 2)
                          if prev_close > 0 else 0,
                volume=kline.volume,
                amount=kline.amount,
                pattern=pattern,
                body_ratio=pattern_details['body_ratio'],
                upper_shadow_ratio=pattern_details['upper_shadow_ratio'],
                lower_shadow_ratio=pattern_details['lower_shadow_ratio'],
                volume_vs_5d=volume_ratios['vs_5d'],
                volume_vs_10d=volume_ratios['vs_10d'],
                volume_trend=volume_trend,
                ma5=ma5,
                ma10=ma10,
                ma20=ma20,
                ma_position=ma_position
            ))

        return indices

    async def _collect_sector_data(self, trade_date: str) -> List[SectorSnapshot]:
        """
        Collect industry sector data with money flow.

        Args:
            trade_date: Date in YYYYMMDD format

        Returns:
            List of SectorSnapshot objects for industries
        """
        sectors = []

        # Query all industry data for the date
        industry_data = self.industry_repo.find_by_date(trade_date)

        for industry in industry_data:
            # Calculate money flow label
            flow_label = self.sentiment_analyzer.get_money_flow_label(
                industry.net_amount or 0
            )

            # Get constituent statistics from board mapping
            up_count, down_count, flat_count = await self._get_constituent_stats(
                industry.ts_code, trade_date
            )

            # Calculate strength
            strength_label, _ = self.sentiment_analyzer.get_sector_strength_label(
                up_count, down_count, industry.pct_change or 0
            )

            sectors.append(SectorSnapshot(
                sector_name=industry.industry,
                sector_code=industry.ts_code,
                sector_type="industry",
                change_pct=industry.pct_change or 0,
                net_inflow=industry.net_amount or 0,
                net_buy_amount=industry.net_buy_amount or 0,
                net_sell_amount=industry.net_sell_amount or 0,
                flow_trend=flow_label,
                up_count=up_count,
                down_count=down_count,
                flat_count=flat_count,
                limit_up=0,  # Not tracked in IndustryDaily
                limit_down=0,
                strength=strength_label,
                pe_valuation=None,  # Could add later
                valuation_position=None,
                leader_symbol=None,  # Not in IndustryDaily
                leader_name=None
            ))

        return sorted(sectors, key=lambda x: x.net_inflow, reverse=True)

    async def _collect_concept_data(self, trade_date: str) -> List[SectorSnapshot]:
        """
        Collect concept data with leader identification.

        Args:
            trade_date: Date in YYYYMMDD format

        Returns:
            List of SectorSnapshot objects for concepts
        """
        concepts = []

        # Query all concept data for the date
        concept_data = self.concept_repo.find_by_date(trade_date)

        for concept in concept_data:
            # Calculate money flow label (concepts don't have money flow data typically)
            flow_label = "数据缺失"

            # Get constituent statistics
            up_count, down_count, flat_count = await self._get_constituent_stats(
                concept.code, trade_date
            )

            # Calculate strength
            strength_label, _ = self.sentiment_analyzer.get_sector_strength_label(
                up_count, down_count, concept.pct_change or 0
            )

            # Get leader info
            leader_name = None
            if concept.leader_symbol:
                leader_info = self.session.query(SymbolMetadata).filter(SymbolMetadata.ticker == concept.leader_symbol).first()
                leader_name = leader_info.name if leader_info else None

            concepts.append(SectorSnapshot(
                sector_name=concept.name,
                sector_code=concept.code,
                sector_type="concept",
                change_pct=concept.pct_change or 0,
                net_inflow=0,  # Concepts don't have money flow data
                net_buy_amount=0,
                net_sell_amount=0,
                flow_trend=flow_label,
                up_count=up_count,
                down_count=down_count,
                flat_count=flat_count,
                limit_up=0,
                limit_down=0,
                strength=strength_label,
                leader_symbol=concept.leader_symbol,
                leader_name=leader_name
            ))

        return sorted(concepts, key=lambda x: x.net_inflow, reverse=True)[:20]  # Top 20 concepts

    async def _calculate_market_sentiment(self, trade_date: str) -> MarketSentiment:
        """
        Calculate market sentiment from breadth indicators.

        Args:
            trade_date: Date in YYYYMMDD format

        Returns:
            MarketSentiment object
        """
        # Convert YYYYMMDD to YYYY-MM-DD format
        formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"

        # Query all stock K-lines for the date
        all_stocks = self.session.query(Kline).filter(
            Kline.trade_time == formatted_date,
            Kline.symbol_type == 'stock',
            Kline.timeframe == 'DAY'
        ).all()

        if not all_stocks:
            raise ValueError(f"No stock data found for {trade_date}")

        # Get previous day for comparison
        prev_date = (datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        prev_stocks = {k.symbol_code: k.close for k in self.session.query(Kline).filter(
            Kline.trade_time == prev_date,
            Kline.symbol_type == 'stock',
            Kline.timeframe == 'DAY'
        ).all()}

        # Count advance/decline
        up_count = 0
        down_count = 0
        flat_count = 0
        limit_up_count = 0
        limit_down_count = 0
        total_amount = 0

        for stock in all_stocks:
            prev_close = prev_stocks.get(stock.symbol_code, stock.open)
            change_pct = ((stock.close - prev_close) / prev_close * 100
                         if prev_close > 0 else 0)

            if change_pct > 0.01:
                up_count += 1
            elif change_pct < -0.01:
                down_count += 1
            else:
                flat_count += 1

            # Check for limit boards (approximately ±10% for A-shares, ±20% for ST)
            if change_pct >= 9.9:
                limit_up_count += 1
            elif change_pct <= -9.9:
                limit_down_count += 1

            total_amount += stock.amount

        # Calculate ratios
        up_down_ratio = up_count / down_count if down_count > 0 else 5.0

        # Get yesterday's total amount for comparison
        yesterday = (datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        yesterday_stocks = self.session.query(func.sum(Kline.amount)).filter(
            Kline.trade_time == yesterday,
            Kline.symbol_type == 'stock',
            Kline.timeframe == 'DAY'
        ).scalar() or total_amount

        vs_yesterday = total_amount / yesterday_stocks if yesterday_stocks > 0 else 1.0

        # Get 5-day average
        recent_amounts = []
        for i in range(1, 8):  # Last 7 days
            check_date = (datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=i)).strftime("%Y-%m-%d")
            day_amount = self.session.query(func.sum(Kline.amount)).filter(
                Kline.trade_time == check_date,
                Kline.symbol_type == 'stock',
                Kline.timeframe == 'DAY'
            ).scalar()
            if day_amount:
                recent_amounts.append(day_amount)

        avg_5d = sum(recent_amounts[:5]) / len(recent_amounts[:5]) if recent_amounts else total_amount
        vs_5d_avg = total_amount / avg_5d if avg_5d > 0 else 1.0

        # Calculate overall sentiment
        sentiment_label, sentiment_score = self.sentiment_analyzer.calculate_sentiment(
            up_count, down_count, limit_up_count, vs_5d_avg
        )

        # Analyze limit boards
        limit_analysis = self.sentiment_analyzer.analyze_limit_boards(
            limit_up_count, limit_down_count,
            limit_up_count, limit_up_count  # Simplified: assume all sealed
        )

        # Determine activity label
        if vs_5d_avg > 1.2:
            activity = "放量"
        elif vs_5d_avg < 0.8:
            activity = "缩量"
        else:
            activity = "持平"

        return MarketSentiment(
            up_count=up_count,
            down_count=down_count,
            flat_count=flat_count,
            up_down_ratio=round(up_down_ratio, 2),
            ad_sentiment=sentiment_label,
            limit_up=limit_up_count,
            limit_down=limit_down_count,
            first_board_success_rate=limit_analysis['seal_rate'],
            limit_sentiment=limit_analysis['heat'],
            total_amount=total_amount,
            vs_yesterday=round(vs_yesterday, 2),
            vs_5d_avg=round(vs_5d_avg, 2),
            activity=activity,
            sentiment_score=sentiment_score,
            sentiment_label=sentiment_label
        )

    async def _select_sample_stocks(self,
                                   trade_date: str,
                                   sectors: List[SectorSnapshot],
                                   concepts: List[SectorSnapshot]) -> Dict[str, List[SampleStock]]:
        """
        Select representative sample stocks for each sector.

        Strategy:
        - For top 5 strong sectors: select 5 samples (leader + high cores + catch-ups)
        - For top 3 weak sectors: select 2 samples (pullbacks)

        Args:
            trade_date: Date in YYYYMMDD format
            sectors: List of sector snapshots
            concepts: List of concept snapshots

        Returns:
            Dict mapping sector name to list of SampleStock objects
        """
        result = {}

        # Combine top sectors and concepts
        all_sectors = sectors + concepts
        top_sectors = sorted(all_sectors, key=lambda x: x.net_inflow, reverse=True)[:5]
        weak_sectors = sorted(all_sectors, key=lambda x: x.change_pct)[:3]

        # Process strong sectors
        for sector in top_sectors:
            samples = await self._select_sector_samples(
                sector, trade_date, sample_type="strong"
            )
            if samples:
                result[sector.sector_name] = samples

        # Process weak sectors
        for sector in weak_sectors:
            samples = await self._select_sector_samples(
                sector, trade_date, sample_type="weak"
            )
            if samples:
                result[sector.sector_name] = samples

        return result

    async def _select_sector_samples(self,
                                    sector: SectorSnapshot,
                                    trade_date: str,
                                    sample_type: str) -> List[SampleStock]:
        """
        Select sample stocks for a specific sector.

        Args:
            sector: Sector snapshot
            trade_date: Date in YYYYMMDD format
            sample_type: "strong" or "weak"

        Returns:
            List of SampleStock objects
        """
        # Get constituent tickers from board mapping
        constituents = await self._get_board_constituents(sector.sector_code)
        if not constituents:
            return []

        # Get detailed stock data
        stocks_data = []
        for ticker in constituents[:50]:  # Limit to avoid too many queries
            stock_data = await self._get_stock_detailed_data(ticker, trade_date)
            if stock_data:
                stocks_data.append(stock_data)

        if not stocks_data:
            return []

        # Select based on type
        samples = []
        if sample_type == "strong":
            # Leader
            if sector.leader_symbol:
                leader = next((s for s in stocks_data if s['ticker'] == sector.leader_symbol), None)
                if leader:
                    samples.append(self._to_sample_stock(leader, "龙头"))

            # High-position cores (5d change > 10%, top 3 by market cap)
            high_cores = [s for s in stocks_data
                         if s['days_5_change'] > 10 and s['market_cap_rank'] <= 3]
            high_cores = sorted(high_cores, key=lambda x: x['market_cap_rank'])[:2]
            samples.extend([self._to_sample_stock(s, "高位核心") for s in high_cores])

            # Catch-ups (today's change > sector avg, lower market cap)
            catch_ups = [s for s in stocks_data
                        if s['change_pct'] > sector.change_pct and s['market_cap_rank'] > 3]
            catch_ups = sorted(catch_ups, key=lambda x: x['change_pct'], reverse=True)[:2]
            samples.extend([self._to_sample_stock(s, "补涨") for s in catch_ups])

        else:  # weak
            # Select stocks with largest declines
            weak_stocks = sorted(stocks_data, key=lambda x: x['change_pct'])[:2]
            samples.extend([self._to_sample_stock(s, "回撤") for s in weak_stocks])

        return samples[:5]  # Max 5 per sector

    async def _get_board_constituents(self, board_code: str) -> List[str]:
        """
        Get constituent tickers from board mapping.

        Args:
            board_code: Board/sector code

        Returns:
            List of constituent tickers
        """
        board = self.session.query(BoardMapping).filter(
            BoardMapping.board_code == board_code
        ).first()

        if board and board.constituents:
            # constituents is a JSON array of tickers
            if isinstance(board.constituents, list):
                return board.constituents
            # If it's stored as string, parse it
            import json
            try:
                return json.loads(board.constituents)
            except:
                return []

        return []

    async def _get_constituent_stats(self, board_code: str, trade_date: str) -> Tuple[int, int, int]:
        """
        Get up/down/flat counts for board constituents.

        Args:
            board_code: Board/sector code
            trade_date: Date in YYYYMMDD format

        Returns:
            Tuple of (up_count, down_count, flat_count)
        """
        constituents = await self._get_board_constituents(board_code)
        if not constituents:
            return (0, 0, 0)

        # Convert dates
        formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
        prev_date = (datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y-%m-%d")

        up_count = 0
        down_count = 0
        flat_count = 0

        # Get today's and yesterday's data in batch
        today_data = {k.symbol_code: k for k in self.session.query(Kline).filter(
            Kline.symbol_code.in_(constituents),
            Kline.trade_time == formatted_date,
            Kline.timeframe == 'DAY'
        ).all()}

        prev_data = {k.symbol_code: k.close for k in self.session.query(Kline).filter(
            Kline.symbol_code.in_(constituents),
            Kline.trade_time == prev_date,
            Kline.timeframe == 'DAY'
        ).all()}

        for ticker in constituents:
            if ticker not in today_data:
                continue

            kline = today_data[ticker]
            prev_close = prev_data.get(ticker, kline.open)

            change_pct = ((kline.close - prev_close) / prev_close * 100
                         if prev_close > 0 else 0)

            if change_pct > 0.01:
                up_count += 1
            elif change_pct < -0.01:
                down_count += 1
            else:
                flat_count += 1

        return (up_count, down_count, flat_count)

    async def _get_stock_detailed_data(self, ticker: str, trade_date: str) -> Optional[Dict]:
        """
        Get detailed stock data for sample selection.

        Args:
            ticker: Stock ticker
            trade_date: Date in YYYYMMDD format

        Returns:
            Dict with stock details or None if not available
        """
        # Convert dates
        formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
        start_date = (datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=20)).strftime("%Y-%m-%d")

        # Get historical data for recent performance
        history = self.session.query(Kline).filter(
            Kline.symbol_code == ticker,
            Kline.symbol_type == 'stock',
            Kline.timeframe == 'DAY',
            Kline.trade_time >= start_date,
            Kline.trade_time <= formatted_date
        ).order_by(Kline.trade_time).all()

        if len(history) < 2:
            return None

        kline = history[-1]  # Current day
        prev_close = history[-2].close if len(history) >= 2 else kline.open

        # Calculate recent changes
        closes = [k.close for k in history]
        days_5_change = ((closes[-1] - closes[-6]) / closes[-6] * 100
                        if len(closes) >= 6 else 0)
        days_10_change = ((closes[-1] - closes[-11]) / closes[-11] * 100
                         if len(closes) >= 11 else 0)

        # Get symbol info
        symbol_info = self.symbol_repo.find_by_ticker(ticker)
        market_cap_rank = 999  # Default if not available

        # Pattern analysis
        pattern, _ = self.pattern_analyzer.analyze_pattern(kline)

        # Volume ratio
        volumes = [k.volume for k in history[-6:]]
        avg_5d = sum(volumes[-5:]) / 5 if len(volumes) >= 5 else kline.volume
        volume_ratio = kline.volume / avg_5d if avg_5d > 0 else 1.0

        # MA analysis
        ma10 = calculate_ma(closes, 10)[-1] if len(closes) >= 10 else kline.close
        ma10_break = kline.close < ma10 if len(history) >= 2 else False

        ma_position = "均线附近"
        if len(closes) >= 10:
            ma5 = calculate_ma(closes, 5)[-1]
            ma20 = calculate_ma(closes, 20)[-1] if len(closes) >= 20 else ma10
            ma_position, _ = self.pattern_analyzer.analyze_ma_position(
                kline.close,
                {'ma5': ma5, 'ma10': ma10, 'ma20': ma20}
            )

        # Position label
        change_pct = ((kline.close - prev_close) / prev_close * 100
                     if prev_close > 0 else 0)
        position = self.pattern_analyzer.get_position_label(
            days_5_change, days_10_change, change_pct
        )

        return {
            'ticker': ticker,
            'name': symbol_info.name if symbol_info else ticker,
            'market_cap_rank': market_cap_rank,
            'open': kline.open,
            'close': kline.close,
            'high': kline.high,
            'low': kline.low,
            'change_pct': round(change_pct, 2),
            'pattern': pattern,
            'volume_ratio': round(volume_ratio, 2),
            'days_5_change': round(days_5_change, 2),
            'days_10_change': round(days_10_change, 2),
            'position': position,
            'ma10_break': ma10_break,
            'ma_position': ma_position
        }

    def _to_sample_stock(self, stock_data: Dict, role: str) -> SampleStock:
        """
        Convert stock data dict to SampleStock object.

        Args:
            stock_data: Stock data dictionary
            role: Stock role label

        Returns:
            SampleStock object
        """
        return SampleStock(
            ticker=stock_data['ticker'],
            name=stock_data['name'],
            role=role,
            market_cap_rank=stock_data['market_cap_rank'],
            open=stock_data['open'],
            close=stock_data['close'],
            high=stock_data['high'],
            low=stock_data['low'],
            change_pct=stock_data['change_pct'],
            pattern=stock_data['pattern'],
            volume_ratio=stock_data['volume_ratio'],
            days_5_change=stock_data['days_5_change'],
            days_10_change=stock_data['days_10_change'],
            position=stock_data['position'],
            ma10_break=stock_data['ma10_break'],
            ma_position=stock_data['ma_position']
        )

    async def _analyze_fundamentals(
        self,
        trade_date: str,
        sample_stocks: Dict[str, List[SampleStock]]
    ) -> Optional[Dict]:
        """
        Analyze fundamentals for sample stocks.

        Args:
            trade_date: Trading date in YYYYMMDD format
            sample_stocks: Dictionary mapping sector names to sample stocks

        Returns:
            Fundamental analysis results including:
            - divergence_alerts: Stocks with price-fundamental divergence
            - quality_stocks: Stocks with strong fundamentals (top 20% in industry)
            - risk_stocks: Stocks with poor fundamentals but rising prices
        """
        all_stocks_for_analysis = []

        # Prepare stock data for batch analysis
        for sector_name, stocks in sample_stocks.items():
            for stock in stocks:
                # Get symbol metadata for industry info
                symbol_info = self.session.query(SymbolMetadata).filter(
                    SymbolMetadata.ticker == stock.ticker
                ).first()

                all_stocks_for_analysis.append({
                    'ticker': stock.ticker,
                    'name': stock.name,
                    'sector': sector_name,
                    'industry': symbol_info.industry_lv1 if symbol_info else None,
                    'current_price': stock.close,
                    'change_pct': stock.change_pct
                })

        if not all_stocks_for_analysis:
            return None

        # Perform batch fundamental analysis
        try:
            results = self.fundamental_analyzer.batch_analyze_fundamentals(
                all_stocks_for_analysis,
                trade_date
            )

            return {
                "divergence_alerts": results.get('divergence_alerts', []),
                "quality_stocks": results.get('quality_stocks', []),
                "risk_stocks": results.get('risk_stocks', [])
            }

        except Exception as e:
            print(f"基本面分析失败: {e}")
            import traceback
            traceback.print_exc()
            return None
