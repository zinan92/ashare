"""
同花顺概念板块 API
数据来源: SQLite klines 表 (统一K线存储)
"""

import pandas as pd
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import List, Optional
from functools import lru_cache

from src.api.dependencies import get_db
from src.models import KlineTimeframe, SymbolType
from src.schemas.normalized import NormalizedTicker
from src.services.kline_service import KlineService
from src.utils.logging import get_logger

router = APIRouter()
logger = get_logger(__name__)

DATA_DIR = Path(__file__).parent.parent.parent / "data"


@lru_cache(maxsize=1)
def load_hot_concepts() -> pd.DataFrame:
    """加载热门概念分类"""
    file_path = DATA_DIR / "hot_concept_categories.csv"
    if not file_path.exists():
        return pd.DataFrame(columns=['概念名称', '大类', '股票数量'])
    return pd.read_csv(file_path)


@lru_cache(maxsize=1)
def load_concept_mapping() -> dict:
    """加载概念名称到板块代码的映射"""
    file_path = DATA_DIR / "concept_to_tickers.csv"
    if not file_path.exists():
        return {}
    df = pd.read_csv(file_path)
    mapping = {}
    for _, row in df.iterrows():
        code = row['板块代码'].replace('.TI', '')
        name = row['板块名称']
        mapping[name] = {
            'code': code,
            'stocks': row['股票代码列表'].split(',') if pd.notna(row['股票代码列表']) else [],
            'stock_count': row['股票数量']
        }
    return mapping


class ConceptInfo(BaseModel):
    name: str
    code: str
    category: str
    stock_count: int
    change_pct: Optional[float] = None  # 涨跌幅


class ConceptListResponse(BaseModel):
    concepts: List[ConceptInfo]
    total: int


def get_concept_change_pcts(db: Session) -> dict:
    """获取所有概念板块的涨跌幅 (从 klines 表)"""
    from sqlalchemy import and_, distinct
    from src.models import Kline

    change_map = {}
    try:
        # 获取所有概念代码
        codes = db.query(distinct(Kline.symbol_code)).filter(
            and_(
                Kline.symbol_type == SymbolType.CONCEPT,
                Kline.timeframe == KlineTimeframe.DAY,
            )
        ).all()

        for (code,) in codes:
            # 获取最近两条日线
            klines = db.query(Kline).filter(
                and_(
                    Kline.symbol_type == SymbolType.CONCEPT,
                    Kline.symbol_code == code,
                    Kline.timeframe == KlineTimeframe.DAY,
                )
            ).order_by(Kline.trade_time.desc()).limit(2).all()

            if len(klines) >= 2:
                last_close = klines[0].close
                prev_close = klines[1].close
                if prev_close > 0:
                    change_pct = ((last_close - prev_close) / prev_close) * 100
                    change_map[str(code)] = round(change_pct, 2)
    except Exception as e:
        logger.exception("获取概念涨跌幅失败")
        raise

    return change_map


class KlineBar(BaseModel):
    datetime: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float


class ConceptKlineResponse(BaseModel):
    code: str
    name: str
    klines: List[KlineBar]


@router.get("", response_model=ConceptListResponse)
def list_concepts(
    db: Session = Depends(get_db),
):
    """获取所有热门概念板块列表

    当 hot_concept_categories.csv 不存在时，返回空数组而不是 500 错误。
    """
    hot_df = load_hot_concepts()
    if hot_df.empty:
        return ConceptListResponse(concepts=[], total=0)

    mapping = load_concept_mapping()

    # 获取涨跌幅，失败时不影响主逻辑
    try:
        change_map = get_concept_change_pcts(db)
    except Exception:
        logger.warning("获取概念涨跌幅失败，使用空数据继续")
        change_map = {}

    concepts = []
    for _, row in hot_df.iterrows():
        name = row['概念名称']
        if name in mapping:
            info = mapping[name]
            code = info['code']
            concepts.append(ConceptInfo(
                name=name,
                code=code,
                category=row['大类'],
                stock_count=info['stock_count'],
                change_pct=change_map.get(code)
            ))

    return ConceptListResponse(concepts=concepts, total=len(concepts))


@router.get("/categories")
def list_categories():
    """获取概念分类列表

    当 hot_concept_categories.csv 不存在时，返回空数组而不是 500 错误。
    """
    hot_df = load_hot_concepts()
    if hot_df.empty:
        return []

    categories = hot_df.groupby('大类').agg({
        '概念名称': list,
        '股票数量': 'sum'
    }).reset_index()

    result = []
    for _, row in categories.iterrows():
        result.append({
            'name': row['大类'],
            'concepts': row['概念名称'],
            'total_stocks': int(row['股票数量'])
        })

    return result


@router.get("/realtime/{code}")
async def get_concept_realtime(code: str):
    """获取概念板块实时涨跌幅"""
    import httpx
    import re
    import json

    BASE_URL = "http://d.10jqka.com.cn/v4"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Referer": "http://q.10jqka.com.cn/",
    }

    try:
        # 获取分时数据
        url = f"{BASE_URL}/time/bk_{code}/last.js"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=HEADERS, timeout=10.0)
            resp.raise_for_status()

            # 解析JSONP响应
            text = resp.text
            match = re.search(r'\((\{.*\})\)', text, re.DOTALL)
            if not match:
                raise HTTPException(status_code=404, detail="无法解析数据")

            outer_data = json.loads(match.group(1))

            # 获取内层数据 (结构: {"bk_886047": {...}})
            inner_key = f"bk_{code}"
            if inner_key not in outer_data:
                raise HTTPException(status_code=404, detail=f"板块 {code} 数据不存在")

            data = outer_data[inner_key]

            # 获取关键数据
            name = data.get('name', '')
            pre_close = float(data.get('pre', 0))  # 昨收

            # 从分时数据获取最新价格
            time_data = data.get('data', '')
            if time_data:
                # 格式: "时间,价格,成交额,涨跌幅,成交量;..."
                items = [item for item in time_data.split(';') if item.strip()]
                if items:
                    last_item = items[-1].split(',')
                    if len(last_item) >= 2 and last_item[1]:
                        current_price = float(last_item[1])
                    else:
                        current_price = pre_close
                else:
                    current_price = pre_close
            else:
                current_price = pre_close

            # 计算涨跌幅
            if pre_close > 0:
                change_pct = ((current_price - pre_close) / pre_close) * 100
            else:
                change_pct = 0

            return {
                'code': code,
                'name': name,
                'price': current_price,
                'pre_close': pre_close,
                'change_pct': round(change_pct, 2),
                'last_update': data.get('update', '')
            }
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"请求失败: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取实时数据失败: {e}")


@router.get("/realtime-batch")
async def get_concepts_realtime_batch(codes: str = Query(..., description="逗号分隔的板块代码")):
    """批量获取概念板块实时涨跌幅"""
    import asyncio

    code_list = [c.strip() for c in codes.split(',') if c.strip()]
    if not code_list:
        return {"data": []}

    # 并行获取所有概念的实时数据
    async def fetch_one(code: str):
        try:
            return await get_concept_realtime(code)
        except:
            return None

    results = await asyncio.gather(*[fetch_one(c) for c in code_list])
    return {"data": [r for r in results if r is not None]}


def _format_concept_datetime(dt_str: str, is_daily: bool) -> str:
    """
    转换日期时间格式为前端期望的格式
    - 日线: "YYYY-MM-DD" -> "YYYYMMDD"
    - 30分钟: "YYYY-MM-DD HH:MM:SS" -> "YYYYMMDDHHMM"
    """
    if is_daily:
        # 日线: "2025-01-02" -> "20250102"
        return dt_str.replace("-", "").split(" ")[0]
    else:
        # 30分钟: "2025-01-02 10:00:00" -> "202501021000"
        parts = dt_str.split(" ")
        date_part = parts[0].replace("-", "")
        if len(parts) > 1:
            time_part = parts[1].replace(":", "")[:4]  # 只取 HHMM
        else:
            time_part = "0000"
        return date_part + time_part


@router.get("/kline/{code}")
def get_concept_kline(
    code: str,
    period: str = Query("30min", regex="^(30min|daily)$"),
    limit: int = Query(120, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """获取概念板块K线数据 (从 klines 表)"""
    # 转换 period 到 timeframe
    timeframe = KlineTimeframe.DAY if period == "daily" else KlineTimeframe.MINS_30
    is_daily = period == "daily"

    try:
        service = KlineService.create_with_session(db)
        result = service.get_klines_with_meta(
            symbol_type=SymbolType.CONCEPT,
            symbol_code=code,
            timeframe=timeframe,
            limit=limit,
        )

        if not result["klines"]:
            raise HTTPException(status_code=404, detail=f"概念 {code} K线数据不存在")

        # 转换为前端期望的格式
        klines = []
        for k in result["klines"]:
            klines.append({
                'datetime': _format_concept_datetime(k['datetime'], is_daily),
                'open': k['open'],
                'high': k['high'],
                'low': k['low'],
                'close': k['close'],
                'volume': int(k['volume']),
                'amount': k['amount']
            })

        return {
            'code': code,
            'name': result["symbol_name"],
            'klines': klines
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"获取概念K线失败: {code}")
        raise HTTPException(status_code=500, detail=f"获取K线数据失败: {str(e)}")


@lru_cache(maxsize=1)
def load_ticker_to_concepts() -> dict:
    """加载股票到概念的映射"""
    file_path = DATA_DIR / "ticker_to_concepts.csv"
    if not file_path.exists():
        return {}
    df = pd.read_csv(file_path)
    mapping = {}
    for _, row in df.iterrows():
        ticker = str(row['股票代码']).zfill(6)
        concepts = row['概念列表'].split(';') if pd.notna(row['概念列表']) else []
        mapping[ticker] = concepts
    return mapping


@router.get("/by-ticker/{ticker}")
def get_concepts_by_ticker(ticker: str):
    """获取指定股票的所有概念板块"""
    # 使用标准化模型，支持任意格式输入
    try:
        normalized = NormalizedTicker(raw=ticker)
        ticker_code = normalized.raw
    except ValueError:
        ticker_code = ticker.split('.')[0].zfill(6)

    mapping = load_ticker_to_concepts()
    concepts = mapping.get(ticker_code, [])

    return {
        "ticker": ticker_code,
        "concepts": concepts,
        "count": len(concepts)
    }


@router.get("/{concept_name}/stocks")
def get_concept_stocks(concept_name: str):
    """获取概念板块的成分股列表"""
    from sqlalchemy import select
    from ..database import session_scope
    from ..models import SymbolMetadata

    mapping = load_concept_mapping()

    if concept_name not in mapping:
        raise HTTPException(status_code=404, detail=f"概念 {concept_name} 不存在")

    info = mapping[concept_name]
    stock_codes = info['stocks']

    # 使用标准化模型转换为带后缀的ticker格式
    def code_to_ticker(code: str) -> str:
        try:
            return NormalizedTicker(raw=code).to_tushare()
        except ValueError:
            return code

    tickers = [code_to_ticker(code) for code in stock_codes]

    # 从数据库获取股票元数据
    # 注意：数据库中的ticker格式是6位代码（无后缀），需要用原始stock_codes查询
    meta_map = {}
    with session_scope() as session:
        if stock_codes:
            metas = session.execute(
                select(SymbolMetadata).where(
                    SymbolMetadata.ticker.in_(stock_codes)
                )
            ).scalars().all()

            for meta in metas:
                # 将数据库返回的6位代码映射到带后缀的ticker
                ticker_with_suffix = code_to_ticker(meta.ticker)
                meta_map[ticker_with_suffix] = {
                    "ticker": ticker_with_suffix,
                    "name": meta.name,
                    "industryLv1": meta.industry_lv1,
                    "totalMv": meta.total_mv,
                    "circMv": meta.circ_mv,
                    "peTtm": meta.pe_ttm,
                    "pb": meta.pb,
                }

    # 返回所有股票，有元数据的用元数据，没有的用基本信息
    result = []
    for ticker in tickers:
        if ticker in meta_map:
            result.append(meta_map[ticker])
        else:
            result.append({
                "ticker": ticker,
                "name": ticker.split('.')[0],  # 用代码作为名称占位
                "industryLv1": None,
                "totalMv": None,
                "circMv": None,
                "peTtm": None,
                "pb": None,
            })

    # 按市值排序（有市值的在前，无市值的在后）
    result.sort(key=lambda x: x.get("totalMv") or 0, reverse=True)

    return {
        'concept': concept_name,
        'code': info['code'],
        'stocks': result,
        'total': len(result)
    }
