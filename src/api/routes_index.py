"""
上证指数等指数数据API
提供K线、成交量、MACD技术指标和基本面指标
数据来源: SQLite klines 表 (统一K线存储)
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api.dependencies import get_db
from src.config import get_settings
from src.models import KlineTimeframe, SymbolType
from src.services.kline_service import KlineService
from src.services.tushare_client import TushareClient
from src.utils.indicators import calculate_macd
from src.utils.logging import get_logger

router = APIRouter()
logger = get_logger(__name__)


def get_tushare_client() -> TushareClient:
    """获取Tushare客户端实例"""
    settings = get_settings()
    return TushareClient(
        token=settings.tushare_token,
        points=settings.tushare_points,
        delay=settings.tushare_delay,
        max_retries=settings.tushare_max_retries
    )


@router.get("/kline/{ts_code}")
def get_index_kline(
    ts_code: str = "000001.SH",
    limit: int = Query(default=120, ge=10, le=500, description="K线数量"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    获取指数日线K线数据 (从 klines 表)

    Args:
        ts_code: 指数代码，支持 tushare (000001.SH) 或 sina (sh000001) 格式
        limit: K线数量

    Returns:
        包含K线、成交量、MACD的数据
    """
    # 标准化输入：前端可能传 sh000001
    ts_code = normalize_index_code(ts_code)
    try:
        service = KlineService.create_with_session(db)
        result = service.get_klines_with_meta(
            symbol_type=SymbolType.INDEX,
            symbol_code=ts_code,
            timeframe=KlineTimeframe.DAY,
            limit=limit,
        )

        if not result["klines"]:
            raise HTTPException(status_code=404, detail=f"未找到指数数据: {ts_code}")

        klines = []
        for k in result["klines"]:
            # 转换日期格式: "2025-01-02 00:00:00" -> "20250102"
            dt_str = k["datetime"]
            if " " in dt_str:
                dt_str = dt_str.split(" ")[0].replace("-", "")
            else:
                dt_str = dt_str.replace("-", "")

            klines.append({
                "date": dt_str,
                "open": k["open"],
                "high": k["high"],
                "low": k["low"],
                "close": k["close"],
                "volume": k["volume"],
                "amount": k["amount"],
                "dif": k.get("dif"),
                "dea": k.get("dea"),
                "macd": k.get("macd"),
            })

        # 获取最新一条数据的基本信息
        latest = klines[-1] if klines else None
        prev = klines[-2] if len(klines) > 1 else latest

        if latest and prev:
            change = latest["close"] - prev["close"]
            change_pct = (change / prev["close"]) * 100 if prev["close"] > 0 else 0
        else:
            change = 0
            change_pct = 0

        return {
            "ts_code": ts_code,
            "name": result["symbol_name"] or get_index_name(ts_code),
            "count": len(klines),
            "latest": {
                "date": latest["date"] if latest else "",
                "close": latest["close"] if latest else 0,
                "open": latest["open"] if latest else 0,
                "high": latest["high"] if latest else 0,
                "low": latest["low"] if latest else 0,
                "change": round(change, 2),
                "change_pct": round(change_pct, 2),
                "volume": latest["volume"] if latest else 0,
                "amount": latest["amount"] if latest else 0,
            } if latest else {},
            "klines": klines
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"获取指数K线失败: {ts_code}")
        raise HTTPException(status_code=500, detail=f"获取指数数据失败: {str(e)}")


@router.get("/quote/{ts_code}")
def get_index_quote(ts_code: str = "000001.SH") -> Dict[str, Any]:
    """
    获取指数实时行情和指标数据

    Args:
        ts_code: 指数代码，支持 tushare (000001.SH) 或 sina (sh000001) 格式

    Returns:
        包含价格、成交、市值、PE等指标的数据
    """
    # 标准化输入：前端可能传 sh000001
    ts_code = normalize_index_code(ts_code)
    try:
        client = get_tushare_client()

        # 获取最新交易日
        trade_date = client.get_latest_trade_date()

        # 获取日线数据 (最近2天用于计算涨跌)
        start_date = (datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=10)).strftime("%Y%m%d")
        df_daily = client.fetch_index_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=trade_date
        )

        if df_daily.empty:
            raise HTTPException(status_code=404, detail=f"未找到指数数据: {ts_code}")

        df_daily = df_daily.sort_values("trade_date", ascending=False)
        latest = df_daily.iloc[0]
        prev = df_daily.iloc[1] if len(df_daily) > 1 else latest

        # 获取指标数据
        df_basic = client.fetch_index_dailybasic(ts_code=ts_code, trade_date=trade_date)

        basic_data = {}
        if not df_basic.empty:
            basic = df_basic.iloc[0]
            basic_data = {
                "total_mv": float(basic["total_mv"]) / 100000000 if pd.notna(basic["total_mv"]) else None,  # 转为万亿
                "float_mv": float(basic["float_mv"]) / 100000000 if pd.notna(basic["float_mv"]) else None,
                "pe": float(basic["pe"]) if pd.notna(basic["pe"]) else None,
                "pe_ttm": float(basic["pe_ttm"]) if pd.notna(basic["pe_ttm"]) else None,
                "pb": float(basic["pb"]) if pd.notna(basic["pb"]) else None,
                "turnover_rate": float(basic["turnover_rate"]) if pd.notna(basic["turnover_rate"]) else None,
            }

        # 获取全市场涨跌平家数
        market_stats = get_market_stats(client, trade_date)

        change = float(latest["close"] - prev["close"])
        change_pct = (change / prev["close"]) * 100 if prev["close"] > 0 else 0
        amplitude = ((float(latest["high"]) - float(latest["low"])) / prev["close"]) * 100 if prev["close"] > 0 else 0

        return {
            "ts_code": ts_code,
            "name": get_index_name(ts_code),
            "trade_date": str(latest["trade_date"]),
            "price": {
                "close": float(latest["close"]),
                "open": float(latest["open"]),
                "high": float(latest["high"]),
                "low": float(latest["low"]),
                "prev_close": float(prev["close"]),
                "change": round(change, 2),
                "change_pct": round(change_pct, 2),
                "amplitude": round(amplitude, 2),
            },
            "volume": {
                "vol": float(latest["vol"]) if pd.notna(latest["vol"]) else 0,  # 手
                "amount": float(latest["amount"]) / 10000 if pd.notna(latest["amount"]) else 0,  # 亿元
            },
            "indicators": basic_data,
            "market_stats": market_stats
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"获取指数行情失败: {ts_code}")
        raise HTTPException(status_code=500, detail=f"获取指数行情失败: {str(e)}")


def get_market_stats(client: TushareClient, trade_date: str) -> Dict[str, int]:
    """
    获取全市场涨跌平家数

    Args:
        client: Tushare客户端
        trade_date: 交易日期

    Returns:
        包含 up_count, down_count, flat_count 的字典
    """
    try:
        df = client.fetch_daily(trade_date=trade_date)
        if df.empty:
            return {"up_count": 0, "down_count": 0, "flat_count": 0}

        up_count = len(df[df["pct_chg"] > 0])
        down_count = len(df[df["pct_chg"] < 0])
        flat_count = len(df[df["pct_chg"] == 0])

        return {
            "up_count": up_count,
            "down_count": down_count,
            "flat_count": flat_count
        }
    except Exception as e:
        logger.warning(f"获取市场统计失败: {e}")
        return {"up_count": 0, "down_count": 0, "flat_count": 0}


def get_index_name(ts_code: str) -> str:
    """根据指数代码返回名称"""
    names = {
        "000001.SH": "上证指数",
        "399001.SZ": "深证成指",
        "399006.SZ": "创业板指",
        "000688.SH": "科创50",
        "000300.SH": "沪深300",
        "000016.SH": "上证50",
        "000905.SH": "中证500",
        "000852.SH": "中证1000",
    }
    return names.get(ts_code, ts_code)


def ts_code_to_sina(ts_code: str) -> str:
    """将Tushare代码或Sina代码转换为Sina代码格式

    支持: 000001.SH → sh000001, sh000001 → sh000001
    """
    # Already in sina format (sh000001, sz399006, etc.)
    if ts_code.startswith(("sh", "sz", "bj")):
        return ts_code
    # Tushare format (000001.SH)
    if "." in ts_code:
        code, market = ts_code.split(".")
        if market == "SH":
            return f"sh{code}"
        elif market == "SZ":
            return f"sz{code}"
        elif market == "BJ":
            return f"bj{code}"
    return ts_code


def sina_to_ts_code(sina_code: str) -> str:
    """将Sina代码转换为Tushare代码格式（反向解析）

    支持: sh000001 → 000001.SH, 000001.SH → 000001.SH
    前端可能传入 sh000001 格式，后端 DB/服务层期望 000001.SH 格式。
    """
    # Already in tushare format (000001.SH)
    if "." in sina_code:
        return sina_code
    # Sina format: sh000001, sz399006, bj830799
    if sina_code.startswith("sh"):
        return f"{sina_code[2:]}.SH"
    elif sina_code.startswith("sz"):
        return f"{sina_code[2:]}.SZ"
    elif sina_code.startswith("bj"):
        return f"{sina_code[2:]}.BJ"
    return sina_code


def normalize_index_code(ts_code: str) -> str:
    """标准化指数代码：接受 sina 或 tushare 格式，统一返回 tushare 格式

    前端可能传入 sh000001 或 000001.SH，统一转为 000001.SH。
    """
    return sina_to_ts_code(ts_code)


@router.get("/realtime/{ts_code}")
async def get_index_realtime(ts_code: str = "000001.SH"):
    """
    获取指数实时行情（使用Sina API）

    Args:
        ts_code: 指数代码，支持 tushare (000001.SH) 或 sina (sh000001) 格式

    Returns:
        实时价格、涨跌幅等数据
    """
    import httpx
    import re

    # 标准化输入：前端可能传 sh000001，统一转为 000001.SH 再转 sina
    ts_code = normalize_index_code(ts_code)
    sina_code = ts_code_to_sina(ts_code)
    url = f"http://hq.sinajs.cn/list=s_{sina_code}"

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers={
                "Referer": "http://finance.sina.com.cn/",
                "User-Agent": "Mozilla/5.0"
            }, timeout=10.0)
            resp.raise_for_status()

            # 解析响应: var hq_str_s_sh000001="上证指数,3259.22,46.14,1.44,2660394,28862016";
            text = resp.text
            match = re.search(r'"([^"]+)"', text)
            if not match:
                raise HTTPException(status_code=404, detail="无法解析指数数据")

            parts = match.group(1).split(",")
            if len(parts) < 6:
                raise HTTPException(status_code=404, detail="指数数据格式错误")

            name = parts[0]
            price = float(parts[1]) if parts[1] else 0
            change = float(parts[2]) if parts[2] else 0
            change_pct = float(parts[3]) if parts[3] else 0
            volume = int(parts[4]) if parts[4] else 0
            amount = float(parts[5]) if parts[5] else 0

            return {
                "ts_code": ts_code,
                "name": name,
                "price": price,
                "change": change,
                "change_pct": change_pct,
                "volume": volume,
                "amount": amount,
                "last_update": datetime.now().strftime("%H:%M:%S")
            }

    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"请求失败: {e}")
    except Exception as e:
        logger.exception(f"获取指数实时行情失败: {ts_code}")
        raise HTTPException(status_code=500, detail=f"获取实时数据失败: {e}")


@router.get("/kline30m/{ts_code}")
def get_index_kline_30m(
    ts_code: str = "000001.SH",
    limit: int = Query(default=120, ge=10, le=500, description="K线数量"),
    db: Session = Depends(get_db),
):
    """
    获取指数30分钟K线数据 (从 klines 表)

    Args:
        ts_code: 指数代码，支持 tushare (000001.SH) 或 sina (sh000001) 格式
        limit: K线数量

    Returns:
        30分钟K线数据，包含MACD指标
    """
    # 标准化输入：前端可能传 sh000001
    ts_code = normalize_index_code(ts_code)
    try:
        service = KlineService.create_with_session(db)
        result = service.get_klines_with_meta(
            symbol_type=SymbolType.INDEX,
            symbol_code=ts_code,
            timeframe=KlineTimeframe.MINS_30,
            limit=limit,
        )

        if not result["klines"]:
            raise HTTPException(status_code=404, detail=f"未找到指数30分钟K线: {ts_code}")

        klines = []
        for k in result["klines"]:
            # 转换时间为 Unix timestamp
            dt_str = k["datetime"]
            try:
                dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                dt = datetime.strptime(dt_str, "%Y-%m-%d")
            timestamp = int(dt.timestamp())

            klines.append({
                "datetime": timestamp,
                "open": k["open"],
                "high": k["high"],
                "low": k["low"],
                "close": k["close"],
                "volume": int(k["volume"]),
                "amount": k["amount"],
                "dif": k.get("dif"),
                "dea": k.get("dea"),
                "macd": k.get("macd"),
            })

        return {
            "ts_code": ts_code,
            "name": result["symbol_name"] or get_index_name(ts_code),
            "count": len(klines),
            "klines": klines
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"获取指数30分钟K线失败: {ts_code}")
        raise HTTPException(status_code=500, detail=f"获取K线数据失败: {e}")
