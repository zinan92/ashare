"""
股票K线API
带懒加载功能：数据库无数据或过期时自动从API获取并保存
"""
from datetime import datetime, time, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.orm import Session
from typing import Annotated, Optional

from src.api.dependencies import get_db
from src.models import KlineTimeframe, SymbolType, Timeframe, TradeCalendar
from src.schemas import CandleBatchResponse, CandlePoint
from src.services.kline_service import KlineService
from src.utils.logging import get_logger

logger = get_logger(__name__)
router = APIRouter()


# Timeframe映射 (API参数 -> 数据库枚举)
KLINE_TIMEFRAME_MAP = {
    "day": KlineTimeframe.DAY,
    "30m": KlineTimeframe.MINS_30,
}

# Timeframe映射 (API参数 -> 响应枚举)
RESPONSE_TIMEFRAME_MAP = {
    "day": Timeframe.DAY,
    "30m": Timeframe.MINS_30,
}


# ==================== 懒加载辅助函数 ====================

def _get_latest_trade_date(db: Session) -> Optional[str]:
    """
    获取最近一个交易日的日期 (YYYY-MM-DD)
    从 trade_calendar 表查询

    Args:
        db: 数据库会话
    """
    today = datetime.now().strftime("%Y-%m-%d")
    # 查找今天或之前最近的交易日
    cal = db.query(TradeCalendar).filter(
        TradeCalendar.date <= today,
        TradeCalendar.is_trading_day == True
    ).order_by(TradeCalendar.date.desc()).first()

    if cal:
        return cal.date
    return None


def _is_trading_time() -> bool:
    """判断当前是否在交易时间内"""
    now = datetime.now()
    current_time = now.time()

    # 上午: 09:30 - 11:30
    morning_start = time(9, 30)
    morning_end = time(11, 30)

    # 下午: 13:00 - 15:00
    afternoon_start = time(13, 0)
    afternoon_end = time(15, 0)

    return (morning_start <= current_time <= morning_end) or \
           (afternoon_start <= current_time <= afternoon_end)


def _is_data_stale(
    db: Session,
    latest_time: Optional[str],
    timeframe: str,
) -> bool:
    """
    判断数据是否过期需要更新

    Args:
        db: 数据库会话
        latest_time: 数据库中最新数据的时间 (日线: YYYY-MM-DD, 30m: YYYY-MM-DD HH:MM:SS)
        timeframe: 时间周期 (day/30m)

    Returns:
        True 表示数据过期需要更新
    """
    if latest_time is None:
        return True  # 无数据，需要获取

    now = datetime.now()

    if timeframe == "day":
        # 日线：检查是否是最近交易日的数据
        latest_trade_date = _get_latest_trade_date(db)
        if latest_trade_date is None:
            return False  # 无法判断，不更新

        # 提取日期部分
        data_date = latest_time[:10]  # YYYY-MM-DD

        # 如果数据日期 < 最近交易日，需要更新
        # 但如果现在是交易日且收盘前，不需要更新到今天
        if data_date < latest_trade_date:
            return True

        # 如果是今天且已收盘(15:30后)，检查是否需要更新
        today = now.strftime("%Y-%m-%d")
        if data_date < today and now.time() > time(15, 30):
            # 检查今天是否是交易日
            cal = db.query(TradeCalendar).filter(
                TradeCalendar.date == today
            ).first()
            if cal and cal.is_trading_day:
                return True

        return False

    else:  # 30m
        # 30分钟线：如果在交易时间内，检查数据是否超过35分钟
        if not _is_trading_time():
            # 非交易时间，检查是否有最近交易日的收盘数据
            latest_trade_date = _get_latest_trade_date(db)
            if latest_trade_date:
                expected_last_time = f"{latest_trade_date} 15:00:00"
                if latest_time < expected_last_time:
                    return True
            return False

        # 交易时间内，检查数据是否过期（超过35分钟）
        try:
            latest_dt = datetime.strptime(latest_time[:19], "%Y-%m-%d %H:%M:%S")
            if now - latest_dt > timedelta(minutes=35):
                return True
        except ValueError:
            return True

        return False


def _fetch_and_save_klines(
    db: Session,
    ticker: str,
    timeframe: str,
    limit: int = 120,
) -> int:
    """
    从API获取K线数据并保存到数据库

    Args:
        db: 数据库会话
        ticker: 6位股票代码
        timeframe: 时间周期 (day/30m)
        limit: 获取数量

    Returns:
        保存的记录数
    """
    from src.services.sina_kline_provider import SinaKlineProvider
    from src.services.tushare_client import TushareClient
    from src.config import get_settings

    logger.info(f"懒加载: 获取 {ticker} {timeframe} K线数据...")

    try:
        if timeframe == "day":
<<<<<<< HEAD
            # 日线用 Tushare Pro
            from src.services.tushare_data_provider import TushareDataProvider
            from src.models import Timeframe as TF
            provider = TushareDataProvider()
            ts_df = provider.fetch_candles(ticker, TF.DAY, limit)
            # TushareDataProvider returns timestamp,open,high,low,close,volume,turnover
            if ts_df is not None and not ts_df.empty:
                df = ts_df.rename(columns={"timestamp": "timestamp"})
            else:
                df = None
=======
            # 日线用TuShare
            settings = get_settings()
            client = TushareClient(
                token=settings.tushare_token,
                points=settings.tushare_points,
                delay=settings.tushare_delay,
                max_retries=settings.tushare_max_retries,
            )
            ts_code = client.normalize_ts_code(ticker)
            df = client.fetch_daily(ts_code=ts_code)
>>>>>>> ccf1351 (refactor: Perception Layer Phase 0+1 — cleanup + core interfaces (#40 #41))
        else:
            # 30分钟用新浪
            provider = SinaKlineProvider(delay=0.1)
            df = provider.fetch_kline(ticker, period="30m", limit=limit)

        if df is None or df.empty:
            logger.warning(f"懒加载: {ticker} {timeframe} 无数据返回")
            return 0

        # 转换为 klines 格式
        klines = []
        if timeframe == "day":
            for _, row in df.head(limit).iterrows():
                klines.append({
                    "datetime": row["trade_date"],
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row["vol"]) if row["vol"] else 0,
                    "amount": float(row.get("amount", 0)),
                })
        else:
            for _, row in df.iterrows():
                dt_str = row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
                klines.append({
                    "datetime": dt_str,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row["volume"]) if row["volume"] else 0,
                    "amount": 0,
                })

        # 保存到数据库
        kline_timeframe = KlineTimeframe.DAY if timeframe == "day" else KlineTimeframe.MINS_30

        service = KlineService.create_with_session(db)
        count = service.save_klines(
            symbol_type=SymbolType.STOCK,
            symbol_code=ticker,
            symbol_name=None,
            timeframe=kline_timeframe,
            klines=klines,
        )
        db.commit()

        logger.info(f"懒加载: {ticker} {timeframe} 保存 {count} 条数据")
        return count

    except Exception as e:
        logger.error(f"懒加载失败: {ticker} {timeframe} - {e}")
        db.rollback()
        return 0


@router.get("/{ticker}", response_model=CandleBatchResponse)
def get_candles(
    ticker: Annotated[str, Path(
        pattern=r"^[0-9]{6}(\.[A-Z]{2})?$",
        description="Stock ticker (e.g., 000001, 600519, 002402.SZ)",
        examples=["000001", "600519", "002402.SZ"]
    )],
    timeframe: str = Query("day", description="Timeframe: day/30m"),
    limit: int = Query(120, ge=1, le=500, description="Number of candles to return"),
    db: Session = Depends(get_db),
) -> CandleBatchResponse:
    """
    Return most recent candles for the ticker/timeframe.

    带懒加载功能：
    1. 先检查数据库是否有数据，以及数据是否过期
    2. 如果无数据或过期，从API获取新数据并保存
    3. 返回数据库中的数据

    Args:
        ticker: Stock code (e.g., 000001, 600519, or with suffix like 002402.SZ)
        timeframe: Time period (day/30m)
        limit: Number of candles to return
        db: 数据库会话（依赖注入）

    Returns:
        CandleBatchResponse containing historical candles

    Raises:
        HTTPException 404: No candles found for ticker
    """
    # 去掉后缀，只保留6位数字代码
    ticker_code = ticker.split('.')[0]

    # 映射timeframe
    kline_timeframe = KLINE_TIMEFRAME_MAP.get(timeframe, KlineTimeframe.DAY)
    response_timeframe = RESPONSE_TIMEFRAME_MAP.get(timeframe, Timeframe.DAY)

    # Step 1: 检查数据库中的最新数据时间
    service = KlineService.create_with_session(db)
    latest_time = service.get_latest_trade_time(
        symbol_type=SymbolType.STOCK,
        symbol_code=ticker_code,
        timeframe=kline_timeframe,
    )

    # Step 2: 判断是否需要懒加载更新
    if _is_data_stale(db, latest_time, timeframe):
        logger.info(f"数据过期或不存在: {ticker_code} {timeframe}, latest={latest_time}")
        _fetch_and_save_klines(db, ticker_code, timeframe, limit=limit)

    # Step 3: 从数据库读取数据
    klines = service.get_klines(
        symbol_type=SymbolType.STOCK,
        symbol_code=ticker_code,
        timeframe=kline_timeframe,
        limit=limit,
    )

    if not klines:
        raise HTTPException(
            status_code=404,
            detail=f"No candles available for ticker {ticker}. Failed to fetch from API."
        )

    # 转换为CandlePoint格式
    candle_points = []
    for k in klines:
        # 解析datetime字符串
        dt_str = k["datetime"]
        if len(dt_str) == 10:  # YYYY-MM-DD
            dt = datetime.strptime(dt_str, "%Y-%m-%d")
        else:  # YYYY-MM-DD HH:MM:SS
            dt = datetime.strptime(dt_str[:19], "%Y-%m-%d %H:%M:%S")

        candle_points.append(CandlePoint(
            timestamp=dt,
            open=k["open"],
            high=k["high"],
            low=k["low"],
            close=k["close"],
            volume=k["volume"],
            turnover=k.get("amount"),
            ma5=None,
            ma10=None,
            ma20=None,
            ma50=None,
        ))

    return CandleBatchResponse(
        ticker=ticker_code,
        timeframe=response_timeframe,
        candles=candle_points,
    )
