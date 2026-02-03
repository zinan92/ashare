from typing import List, Dict, Any

from fastapi import APIRouter, Depends

from src.api.dependencies import get_data_service
from src.schemas import SymbolMeta
from src.services.data_pipeline import MarketDataService

router = APIRouter()

@router.get("", response_model=List[SymbolMeta])
def list_symbols(service: MarketDataService = Depends(get_data_service)) -> List[SymbolMeta]:
    """Return watchlist metadata sorted by market cap."""
    return service.list_symbols()


@router.get("/search")
def search_symbols(q: str):
    """搜索全部A股股票（5000+只），标注是否已在自选中"""
    from src.database import session_scope
    from src.models import SymbolMetadata, Watchlist
    import sqlite3

    with session_scope() as session:
        # 先搜自选股（有完整元数据）
        watchlist_tickers = {
            w.ticker for w in session.query(Watchlist).all()
        }

        # 搜全量 stock_basic 表（raw SQL，因为没有 ORM model）
        from src.config import get_settings
        db_url = get_settings().database_url
        db_path = db_url.replace("sqlite:///", "")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """SELECT symbol, name, industry, market
               FROM stock_basic
               WHERE symbol LIKE ? OR name LIKE ?
               ORDER BY CASE WHEN symbol = ? THEN 0
                             WHEN symbol LIKE ? THEN 1
                             ELSE 2 END,
                        symbol
               LIMIT 30""",
            (f"%{q}%", f"%{q}%", q, f"{q}%")
        )
        rows = c.fetchall()
        conn.close()

        # 对已在自选中的股票，补充完整元数据
        result_tickers = [r["symbol"] for r in rows]
        meta_map = {}
        if result_tickers:
            metas = session.query(SymbolMetadata).filter(
                SymbolMetadata.ticker.in_(result_tickers)
            ).all()
            meta_map = {m.ticker: m for m in metas}

        results = []
        for row in rows:
            ticker = row["symbol"]
            in_watchlist = ticker in watchlist_tickers
            meta = meta_map.get(ticker)

            results.append({
                "ticker": ticker,
                "name": row["name"],
                "industry": row["industry"] or "",
                "market": row["market"] or "",
                "inWatchlist": in_watchlist,
                "totalMv": meta.total_mv if meta else None,
                "peTtm": meta.pe_ttm if meta else None,
            })

        return results


@router.get("/industries")
def list_industries(service: MarketDataService = Depends(get_data_service)) -> Dict[str, Any]:
    """Return list of industries from database (Tushare 90 industries with OHLC data)."""
    from src.database import session_scope
    from src.models import IndustryDaily
    from sqlalchemy import func

    with session_scope() as session:
        # 获取最新交易日的数据
        latest_date = session.query(
            func.max(IndustryDaily.trade_date)
        ).scalar()

        # 查询最新交易日的所有行业数据
        industries = session.query(IndustryDaily).filter(
            IndustryDaily.trade_date == latest_date
        ).all()

        # 构建结果
        result = []
        for ind in industries:
            result.append({
                "板块名称": ind.industry,
                "板块代码": ind.ts_code,
                "股票数量": ind.company_num,
                "总市值": ind.total_mv if ind.total_mv else 0,
                "涨跌幅": float(ind.pct_change),
                "上涨家数": ind.up_count if ind.up_count is not None else 0,
                "下跌家数": ind.down_count if ind.down_count is not None else 0,
                "行业PE": ind.industry_pe,
                "收盘指数": float(ind.close),
                "领涨股": ind.lead_stock,
                "领涨股涨跌幅": float(ind.pct_change_stock) if ind.pct_change_stock else None,
                "净流入资金": float(ind.net_amount) if ind.net_amount else None,
                "交易日期": ind.trade_date
            })

        # 按涨跌幅降序排序
        result.sort(key=lambda x: x["涨跌幅"], reverse=True)

        return {
            "data": result,
            "last_update_time": latest_date
        }
