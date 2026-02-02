"""
Crypto WebSocket 实时数据 API 路由
提供低延迟的实时价格查询和WebSocket状态管理
"""
from typing import List, Optional
from fastapi import APIRouter, Query, HTTPException, Path
from pydantic import BaseModel

from src.services.crypto_ws import get_crypto_ws_manager

router = APIRouter()


# ── 数据模型 ──

class RealtimeTickerItem(BaseModel):
    symbol: str
    pair: str
    price: float
    change_24h: float
    change_pct_24h: float
    high_24h: float
    low_24h: float
    volume_24h: float
    quote_volume_24h: float
    open_price: float
    trades_count: int
    last_update: str
    is_stale: bool
    source: str


class RealtimeTickersResponse(BaseModel):
    count: int
    source: str
    tickers: List[RealtimeTickerItem]


class RealtimeKlineItem(BaseModel):
    symbol: str
    pair: str
    interval: str
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool


class WebSocketStatusResponse(BaseModel):
    running: bool
    connected: bool
    symbols_count: int
    tickers_cached: int
    stale_tickers: int
    kline_intervals: List[str]
    message_count: int
    uptime_seconds: float
    last_message: Optional[str]
    reconnect_delay: float


# ── 实时价格 (from WebSocket cache) ──

@router.get("/realtime", response_model=RealtimeTickersResponse)
async def get_realtime_prices():
    """
    获取所有加密货币实时价格 (WebSocket缓存)
    延迟: <100ms (内存缓存) vs REST API的 1-3s
    """
    manager = get_crypto_ws_manager()

    if not manager.is_connected:
        raise HTTPException(
            status_code=503,
            detail="WebSocket not connected. Data may be unavailable.",
        )

    tickers = manager.get_all_tickers_list()
    return RealtimeTickersResponse(
        count=len(tickers),
        source="binance_websocket",
        tickers=tickers,
    )


@router.get("/realtime/{symbol}", response_model=RealtimeTickerItem)
async def get_realtime_price(
    symbol: str = Path(..., description="加密货币代码 (如 BTC, ETH, SOL)")
):
    """获取单个加密货币实时价格 (WebSocket缓存)"""
    manager = get_crypto_ws_manager()

    ticker = manager.get_ticker(symbol.upper())
    if not ticker:
        # Fallback hint
        available = [t.base_symbol for t in manager.get_all_tickers().values()]
        raise HTTPException(
            status_code=404,
            detail=f"No realtime data for {symbol}. Available: {', '.join(sorted(available))}",
        )

    return ticker.to_dict()


# ── 实时K线 ──

@router.get("/realtime/kline/{symbol}")
async def get_realtime_kline(
    symbol: str = Path(..., description="加密货币代码"),
    interval: str = Query("1m", description="K线间隔 (需在WS订阅中)"),
):
    """获取实时K线数据 (当前未关闭的K线)"""
    manager = get_crypto_ws_manager()

    kline = manager.get_kline(symbol.upper(), interval)
    if not kline:
        raise HTTPException(
            status_code=404,
            detail=f"No realtime kline for {symbol}/{interval}. "
            f"Available intervals: {manager.kline_intervals}",
        )

    return kline.to_dict()


# ── WebSocket 状态 ──

@router.get("/ws/status", response_model=WebSocketStatusResponse)
async def get_ws_status():
    """获取WebSocket连接状态和统计"""
    manager = get_crypto_ws_manager()
    return manager.get_status()


@router.post("/ws/restart")
async def restart_ws():
    """重启WebSocket连接"""
    manager = get_crypto_ws_manager()

    if manager.is_running:
        await manager.stop()

    await manager.start()

    return {
        "status": "restarted",
        "message": "WebSocket manager restarted",
    }
