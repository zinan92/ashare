"""
板块轮动分析 API
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Dict, List, Optional
from pydantic import BaseModel

router = APIRouter(prefix="/rotation", tags=["rotation"])


class SectorData(BaseModel):
    name: str
    date: str
    pct_change: Optional[float] = None
    net_inflow: Optional[float] = None
    up_count: Optional[int] = None
    down_count: Optional[int] = None
    up_down_ratio: Optional[float] = None
    rank: Optional[int] = None
    rotation_signal: str
    signal_strength: float


class RotationSummary(BaseModel):
    total_analyzed: int
    inflow_count: int
    outflow_count: int


class RotationResponse(BaseModel):
    inflow_accelerating: List[SectorData]
    outflow_accelerating: List[SectorData]
    summary: RotationSummary


@router.get("/signals", response_model=RotationResponse)
async def get_rotation_signals():
    """
    获取板块轮动信号
    
    返回:
    - inflow_accelerating: 资金流入的板块
    - outflow_accelerating: 资金流出的板块
    """
    try:
        from src.services.sector_rotation import get_rotation_analysis
        result = get_rotation_analysis()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-inflow")
async def get_top_inflow(limit: int = Query(default=20, le=50)):
    """获取资金净流入 TOP 板块"""
    try:
        from src.services.sector_rotation import get_top_inflow
        return get_top_inflow(limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/heatmap")
async def get_rotation_heatmap():
    """获取轮动热力图数据"""
    try:
        from src.services.sector_rotation import SectorRotationService
        service = SectorRotationService()
        try:
            results = service.get_top_inflow_sectors(100)
            
            heatmap_data = [{
                'name': r['name'],
                'x': r.get('pct_change', 0),
                'y': r.get('net_inflow', 0),
                'signal': r['rotation_signal']
            } for r in results]
            
            return {'data': heatmap_data, 'total': len(heatmap_data)}
            
        finally:
            service.close()
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
