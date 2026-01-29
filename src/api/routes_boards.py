"""板块映射相关API端点"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import csv
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func

from src.api.dependencies import get_data_service
from src.services.board_mapping_service import BoardMappingService
from src.services.data_pipeline import MarketDataService
from src.database import session_scope
from src.models import IndustryDaily, SymbolMetadata, BoardMapping, SuperCategoryDaily
from src.schemas import SymbolMeta

router = APIRouter()


class BuildMappingsRequest(BaseModel):
    board_types: Optional[List[str]] = ["industry"]  # ['industry', 'concept']


class BuildMappingsResponse(BaseModel):
    status: str
    stats: dict
    message: str


class VerifyChangesRequest(BaseModel):
    board_name: str
    board_type: str = "industry"  # 'industry' or 'concept'


class VerifyChangesResponse(BaseModel):
    board_name: str
    board_type: str
    has_changes: bool
    added: List[str]
    removed: List[str]
    current_count: int
    previous_count: int
    error: Optional[str] = None


class StockConceptsResponse(BaseModel):
    ticker: str
    concepts: List[str]


@router.post("/build", response_model=BuildMappingsResponse)
def build_board_mappings(
    payload: BuildMappingsRequest,
) -> BuildMappingsResponse:
    """
    一次性构建板块映射

    WARNING: 如果包含 'concept'，可能需要1-2小时！
    建议只构建 'industry'（90个板块，约15-20分钟）
    """
    service = BoardMappingService()

    try:
        stats = service.build_all_mappings(board_types=payload.board_types)

        total = sum(stats.values())
        message = f"Successfully built {total} board mappings"

        if 'industry' in stats:
            message += f" (Industry: {stats['industry']})"
        if 'concept' in stats:
            message += f" (Concept: {stats['concept']})"

        return BuildMappingsResponse(
            status="success",
            stats=stats,
            message=message
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to build board mappings: {str(e)}"
        )


@router.post("/verify", response_model=VerifyChangesResponse)
def verify_board_changes(
    payload: VerifyChangesRequest,
) -> VerifyChangesResponse:
    """
    验证单个板块的成分股是否有变化

    快速检查，不重新遍历所有板块
    """
    service = BoardMappingService()

    try:
        result = service.verify_changes(
            board_name=payload.board_name,
            board_type=payload.board_type
        )

        return VerifyChangesResponse(
            board_name=payload.board_name,
            board_type=payload.board_type,
            **result
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to verify board changes: {str(e)}"
        )


@router.get("/concepts/{ticker}", response_model=StockConceptsResponse)
def get_stock_concepts(
    ticker: str,
) -> StockConceptsResponse:
    """
    获取某只股票所属的概念板块列表
    """
    service = BoardMappingService()

    try:
        concepts = service.get_stock_concepts(ticker)
        return StockConceptsResponse(
            ticker=ticker,
            concepts=concepts
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get stock concepts: {str(e)}"
        )


@router.get("/list")
def list_board_mappings(
    board_type: Optional[str] = None,
) -> dict:
    """
    列出所有板块映射

    Args:
        board_type: 筛选类型 ('industry', 'concept', 或 None 表示全部)
    """
    from sqlalchemy import select

    with session_scope() as session:
        stmt = select(BoardMapping)
        if board_type:
            stmt = stmt.where(BoardMapping.board_type == board_type)

        mappings = session.scalars(stmt).all()

        return {
            "total": len(mappings),
            "boards": [
                {
                    "name": m.board_name,
                    "type": m.board_type,
                    "code": m.board_code,
                    "stock_count": len(m.constituents),
                    "last_updated": m.last_updated.isoformat() if m.last_updated else None
                }
                for m in mappings
            ]
        }


def calculate_market_cap_growth(session, board_name: str) -> Dict[str, float | None]:
    """计算板块市值增长率"""
    # 首先从 IndustryDaily 获取板块代码
    latest_date_subq = session.query(
        func.max(IndustryDaily.trade_date)
    ).scalar_subquery()

    board_info = session.query(IndustryDaily).filter(
        IndustryDaily.industry == board_name,
        IndustryDaily.trade_date == latest_date_subq
    ).first()

    if not board_info:
        return {
            "5d": None, "2w": None, "30d": None, "3m": None, "6m": None
        }

    ts_code = board_info.ts_code
    current_mv = board_info.total_mv

    if not current_mv:
        return {
            "5d": None, "2w": None, "30d": None, "3m": None, "6m": None
        }

    # 获取所有历史数据，按日期降序排序
    history = session.query(IndustryDaily).filter(
        IndustryDaily.ts_code == ts_code
    ).order_by(IndustryDaily.trade_date.desc()).limit(150).all()

    # 转换为字典方便查询
    mv_by_date = {rec.trade_date: rec.total_mv for rec in history if rec.total_mv}
    dates = sorted(mv_by_date.keys(), reverse=True)

    if not dates:
        return {
            "5d": None, "2w": None, "30d": None, "3m": None, "6m": None
        }

    latest_date_str = dates[0]
    latest_date = datetime.strptime(latest_date_str, "%Y%m%d")

    def get_growth(days: int) -> float | None:
        target_date = latest_date - timedelta(days=days)
        target_date_str = target_date.strftime("%Y%m%d")

        # 找到最接近目标日期的交易日
        closest_date = None
        for date_str in reversed(dates):
            if date_str <= target_date_str:
                closest_date = date_str
                break

        if closest_date and closest_date in mv_by_date:
            past_mv = mv_by_date[closest_date]
            if past_mv and past_mv > 0:
                return ((current_mv - past_mv) / past_mv) * 100

        return None

    return {
        "5d": get_growth(5),
        "2w": get_growth(14),
        "30d": get_growth(30),
        "3m": get_growth(90),
        "6m": get_growth(180)
    }


# Helper to load symbols for a board (industry priority)
def _load_board_symbols(session, board_name: str) -> list[SymbolMetadata]:
    """
    获取板块成分股

    优先级:
    1. BoardMapping 表（如果有数据）
    2. 从 IndustryDaily 获取 ts_code，然后调用 Tushare API 获取同花顺成分股
    3. 回退到 industry_lv1（现在存储的是同花顺行业，由 update_industry_daily.py 维护）
    """
    # 1. 尝试从 BoardMapping 获取
    board_mapping = session.query(BoardMapping).filter(
        BoardMapping.board_name == board_name,
        BoardMapping.board_type == "industry"
    ).first()

    if board_mapping and board_mapping.constituents:
        return session.query(SymbolMetadata).filter(
            SymbolMetadata.ticker.in_(board_mapping.constituents)
        ).all()

    # 2. 从同花顺 API 获取成分股（使用 IndustryDaily 中的 ts_code）
    industry_daily = session.query(IndustryDaily).filter(
        IndustryDaily.industry == board_name
    ).first()

    if industry_daily and industry_daily.ts_code:
        try:
            from src.services.tushare_client import TushareClient
            from src.config import get_settings

            settings = get_settings()
            client = TushareClient(
                token=settings.tushare_token,
                points=settings.tushare_points
            )

            # 获取同花顺成分股
            members_df = client.fetch_ths_member(ts_code=industry_daily.ts_code)

            if not members_df.empty:
                code_field = 'con_code' if 'con_code' in members_df.columns else 'code'
                tickers = set()
                for stock_code in members_df[code_field].dropna():
                    # 标准化股票代码（去掉交易所后缀）
                    if '.' in str(stock_code):
                        ticker = str(stock_code).split('.')[0]
                    else:
                        ticker = str(stock_code)
                    tickers.add(ticker)

                if tickers:
                    return session.query(SymbolMetadata).filter(
                        SymbolMetadata.ticker.in_(list(tickers))
                    ).all()
        except Exception as e:
            import logging
            logging.warning(f"Failed to fetch THS members for {board_name}: {e}")

    # 3. 回退到 industry_lv1（现在存储的是同花顺行业）
    symbols = session.query(SymbolMetadata).filter(
        SymbolMetadata.industry_lv1 == board_name
    ).all()

    if not symbols:
        symbols = session.query(SymbolMetadata).filter(
            SymbolMetadata.industry_lv2 == board_name
        ).all()

    if not symbols:
        symbols = session.query(SymbolMetadata).filter(
            SymbolMetadata.industry_lv3 == board_name
        ).all()

    return symbols


@router.get("/{board_name}/stats")
def get_board_stats(board_name: str) -> Dict[str, Any]:
    """获取板块详细统计信息"""
    with session_scope() as session:
        # 获取最新交易日的板块数据
        latest_date_subq = session.query(
            func.max(IndustryDaily.trade_date)
        ).scalar_subquery()

        board_daily = session.query(IndustryDaily).filter(
            IndustryDaily.industry == board_name,
            IndustryDaily.trade_date == latest_date_subq
        ).first()

        if not board_daily:
            raise HTTPException(status_code=404, detail=f"板块 '{board_name}' 不存在")

        symbols = _load_board_symbols(session, board_name)

        # 计算 PE 中位数
        pe_values = [s.pe_ttm for s in symbols if s.pe_ttm is not None and s.pe_ttm > 0]
        pe_median = None
        if pe_values:
            pe_values.sort()
            n = len(pe_values)
            if n % 2 == 0:
                pe_median = (pe_values[n//2 - 1] + pe_values[n//2]) / 2
            else:
                pe_median = pe_values[n//2]

        # 找到龙头公司（按市值）
        leading_company = None
        if symbols:
            symbols_with_mv = [s for s in symbols if s.total_mv is not None]
            if symbols_with_mv:
                symbols_with_mv.sort(key=lambda x: x.total_mv, reverse=True)
                leader = symbols_with_mv[0]
                leading_company = {
                    "ticker": leader.ticker,
                    "name": leader.name,
                    "market_cap": leader.total_mv
                }

        # 计算市值增长
        mv_growth = calculate_market_cap_growth(session, board_name)

        # 构建响应
        return {
            "板块名称": board_name,
            "板块代码": board_daily.ts_code,
            "股票数量": board_daily.company_num,
            "上涨家数": board_daily.up_count or 0,
            "下跌家数": board_daily.down_count or 0,
            "平仓家数": board_daily.company_num - (board_daily.up_count or 0) - (board_daily.down_count or 0),
            "加权平均PE": board_daily.industry_pe,
            "PE中位数": round(pe_median, 2) if pe_median else None,
            "总市值": board_daily.total_mv,
            "涨跌幅": board_daily.pct_change,
            "龙头公司": leading_company,
            "市值增长": {
                "5天": round(mv_growth["5d"], 2) if mv_growth["5d"] is not None else None,
                "2周": round(mv_growth["2w"], 2) if mv_growth["2w"] is not None else None,
                "30天": round(mv_growth["30d"], 2) if mv_growth["30d"] is not None else None,
                "3个月": round(mv_growth["3m"], 2) if mv_growth["3m"] is not None else None,
                "6个月": round(mv_growth["6m"], 2) if mv_growth["6m"] is not None else None
            },
            "交易日期": board_daily.trade_date
        }


@router.get("/{board_name}/symbols", response_model=list[SymbolMeta])
def list_board_symbols(board_name: str) -> list[SymbolMeta]:
    """获取板块成分股（优先使用映射表，其次 industry_lv1/2/3）"""
    with session_scope() as session:
        symbols = _load_board_symbols(session, board_name)
        return [SymbolMeta.model_validate(s) for s in symbols]



