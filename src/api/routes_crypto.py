"""
加密货币 API 路由
涵盖：实时价格、K线数据、市场概览、资金费率、单币详情
"""
from typing import List, Optional
from fastapi import APIRouter, Query, HTTPException, Path
from pydantic import BaseModel

from src.services.crypto_service import get_crypto_service

router = APIRouter()


# ── 数据模型 ──

class CryptoPriceItem(BaseModel):
    symbol: str
    name: str
    price: float
    change_24h: float
    volume_24h: float
    market_cap: float
    last_update: str


class CryptoPricesResponse(BaseModel):
    count: int
    prices: List[CryptoPriceItem]


class CryptoKlineItem(BaseModel):
    time: str
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class CryptoKlineResponse(BaseModel):
    symbol: str
    interval: str
    count: int
    klines: List[CryptoKlineItem]


class CryptoQuoteResponse(BaseModel):
    symbol: str
    name: str
    price: float
    change_24h: float
    change_7d: float
    volume_24h: float
    market_cap: float
    market_cap_rank: int
    circulating_supply: float
    total_supply: float
    ath: float
    ath_change_percentage: float
    atl: float
    atl_change_percentage: float
    last_update: str


class FundingRateItem(BaseModel):
    symbol: str
    funding_rate: float
    funding_time: str
    mark_price: float
    last_update: str


class FundingRatesResponse(BaseModel):
    count: int
    funding_rates: List[FundingRateItem]


class MarketOverviewResponse(BaseModel):
    total_market_cap_usd: float
    total_volume_24h_usd: float
    bitcoin_dominance: float
    ethereum_dominance: float
    active_cryptocurrencies: int
    markets: int
    market_cap_change_24h: float
    last_update: str


# ── 实时价格 ──

@router.get("/prices", response_model=CryptoPricesResponse)
async def get_crypto_prices():
    """获取主要加密货币实时价格"""
    service = get_crypto_service()
    prices = await service.get_prices()
    return CryptoPricesResponse(count=len(prices), prices=prices)


# ── 单币详情报价 ──

@router.get("/quote/{symbol}", response_model=CryptoQuoteResponse)
async def get_crypto_quote(
    symbol: str = Path(..., description="加密货币代码 (如 BTC, ETH, SOL)")
):
    """获取单个加密货币详细报价"""
    service = get_crypto_service()
    quote = await service.get_quote(symbol.upper())
    if not quote:
        raise HTTPException(
            status_code=404, 
            detail=f"Quote not found for {symbol}. Supported: {', '.join(service.MAJOR_CRYPTOS.values())}"
        )
    return quote


# ── K线数据 ──

@router.get("/kline/{symbol}", response_model=CryptoKlineResponse)
async def get_crypto_klines(
    symbol: str = Path(..., description="加密货币代码 (如 BTC, ETH, SOL)"),
    interval: str = Query("1h", description="时间间隔 (1m/5m/15m/1h/4h/1d)"),
    limit: int = Query(100, ge=1, le=1000, description="返回条数 (最大1000)")
):
    """获取加密货币K线数据 (OHLCV)"""
    service = get_crypto_service()
    
    # 验证交易对是否支持
    if symbol.upper() not in service.BINANCE_PAIRS:
        raise HTTPException(
            status_code=404,
            detail=f"Symbol {symbol} not supported. Available: {', '.join(service.BINANCE_PAIRS.keys())}"
        )
    
    klines = await service.get_klines(symbol.upper(), interval, limit)
    
    if not klines:
        raise HTTPException(
            status_code=404, 
            detail=f"Kline data not found for {symbol} with interval {interval}"
        )
    
    return CryptoKlineResponse(
        symbol=symbol.upper(),
        interval=interval,
        count=len(klines),
        klines=klines
    )


# ── 资金费率 ──

@router.get("/funding-rates", response_model=FundingRatesResponse)
async def get_funding_rates():
    """获取主要加密货币永续合约资金费率"""
    service = get_crypto_service()
    rates = await service.get_funding_rates()
    return FundingRatesResponse(count=len(rates), funding_rates=rates)


# ── 市场概览 ──

@router.get("/market-overview", response_model=MarketOverviewResponse)
async def get_market_overview():
    """获取加密货币市场概览 (总市值、主导率、活跃币种等)"""
    service = get_crypto_service()
    overview = await service.get_market_overview()
    
    if not overview:
        raise HTTPException(
            status_code=503,
            detail="Market overview data temporarily unavailable"
        )
    
    return overview


# ── 支持的币种列表 ──

@router.get("/supported-symbols")
async def get_supported_symbols():
    """获取支持的加密货币列表"""
    service = get_crypto_service()
    
    return {
        "count": len(service.MAJOR_CRYPTOS),
        "symbols": [
            {
                "symbol": symbol,
                "name": crypto_id.replace('-', ' ').title(),
                "binance_pair": service.BINANCE_PAIRS.get(symbol, ""),
                "supports_klines": symbol in service.BINANCE_PAIRS,
                "supports_funding_rates": service.BINANCE_PAIRS.get(symbol, "").endswith("USDT")
            }
            for crypto_id, symbol in service.MAJOR_CRYPTOS.items()
        ]
    }


# ── 健康检查 ──

@router.get("/health")
async def crypto_health_check():
    """加密货币模块健康检查"""
    service = get_crypto_service()
    
    try:
        # 尝试获取 BTC 价格作为健康检查
        btc_quote = await service.get_quote("BTC")
        
        status = "healthy" if btc_quote else "degraded"
        
        return {
            "status": status,
            "message": "Crypto module is operational" if btc_quote else "External APIs may be slow",
            "timestamp": btc_quote.get("last_update") if btc_quote else None,
            "supported_symbols_count": len(service.MAJOR_CRYPTOS)
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "message": f"Health check failed: {str(e)}",
            "timestamp": None,
            "supported_symbols_count": len(service.MAJOR_CRYPTOS)
        }