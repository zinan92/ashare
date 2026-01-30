"""
同花顺概念板块监控API - 优化版本
读取独立进程生成的JSON文件，不阻塞FastAPI
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import json
from pathlib import Path
from datetime import datetime

router = APIRouter()

# JSON缓存文件路径
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
CACHE_FILE = DATA_DIR / "monitor" / "latest.json"
SIGNALS_FILE = DATA_DIR / "monitor" / "momentum_signals.json"


class ConceptData(BaseModel):
    rank: int
    name: str
    code: str
    changePct: float
    changeValue: float
    mainVolume: Optional[float] = None  # Optional: may not be in all data sources
    moneyInflow: float
    volumeRatio: float
    upCount: int
    downCount: int
    limitUp: int
    totalStocks: int
    turnover: float
    volume: float
    day5Change: float
    day10Change: float
    day20Change: float


class ConceptListResponse(BaseModel):
    success: bool
    timestamp: str
    total: int
    data: list[ConceptData]


def read_cache_file():
    """读取缓存的JSON文件

    无论文件缺失还是解析失败，都返回空数据结构而不是抛出异常。
    监控脚本未运行时，前端应显示"暂无数据"而不是错误页面。
    """
    empty_response = {"success": True, "timestamp": "", "total": 0, "data": []}

    if not CACHE_FILE.exists():
        return empty_response

    try:
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception:
        # 文件损坏或格式错误时，返回空数据而非 500
        return empty_response


@router.get("/top", response_model=ConceptListResponse)
async def get_top_concepts(n: int = 20):
    """
    获取涨幅前N的概念板块

    - n: 返回前N个板块（默认20）

    注意：此接口读取独立进程生成的JSON文件，响应速度极快
    """
    cache_data = read_cache_file()

    top_concepts = cache_data.get('topConcepts', {}).get('data', [])[:n]
    timestamp = cache_data.get('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    data = []
    for idx, concept in enumerate(top_concepts):
        data.append(ConceptData(
            rank=idx + 1,
            name=concept['name'],
            code=concept['code'],
            changePct=concept['changePct'],
            changeValue=concept['changeValue'],
            mainVolume=concept.get('mainVolume'),  # Optional field
            moneyInflow=concept['moneyInflow'],
            volumeRatio=concept['volumeRatio'],
            upCount=concept['upCount'],
            downCount=concept['downCount'],
            limitUp=concept['limitUp'],
            totalStocks=concept['totalStocks'],
            turnover=concept['turnover'],
            volume=concept['volume'],
            day5Change=concept['day5Change'],
            day10Change=concept['day10Change'],
            day20Change=concept['day20Change']
        ))

    return ConceptListResponse(
        success=True,
        timestamp=timestamp,
        total=len(data),
        data=data
    )


@router.get("/watch", response_model=ConceptListResponse)
async def get_watch_concepts():
    """
    获取自选热门概念板块
    """
    cache_data = read_cache_file()

    watch_concepts = cache_data.get('watchConcepts', {}).get('data', [])
    timestamp = cache_data.get('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    data = []
    for idx, concept in enumerate(watch_concepts):
        data.append(ConceptData(
            rank=idx + 1,
            name=concept['name'],
            code=concept['code'],
            changePct=concept['changePct'],
            changeValue=concept['changeValue'],
            mainVolume=concept.get('mainVolume'),  # Optional field
            moneyInflow=concept['moneyInflow'],
            volumeRatio=concept['volumeRatio'],
            upCount=concept['upCount'],
            downCount=concept['downCount'],
            limitUp=concept['limitUp'],
            totalStocks=concept['totalStocks'],
            turnover=concept['turnover'],
            volume=concept['volume'],
            day5Change=concept['day5Change'],
            day10Change=concept['day10Change'],
            day20Change=concept['day20Change']
        ))

    return ConceptListResponse(
        success=True,
        timestamp=timestamp,
        total=len(data),
        data=data
    )


@router.get("/status")
async def get_status():
    """
    获取监控状态
    """
    if not CACHE_FILE.exists():
        return {
            "is_ready": False,
            "last_update": None,
            "cache_file": str(CACHE_FILE),
            "message": "缓存文件不存在，请运行: python3 scripts/monitor_no_flask.py --once"
        }

    try:
        cache_data = read_cache_file()

        return {
            "is_ready": True,
            "last_update": cache_data.get('timestamp'),
            "cache_file": str(CACHE_FILE),
            "top_concepts_count": len(cache_data.get('topConcepts', {}).get('data', [])),
            "watch_concepts_count": len(cache_data.get('watchConcepts', {}).get('data', [])),
            "message": "数据就绪"
        }
    except Exception as e:
        return {
            "is_ready": False,
            "last_update": None,
            "cache_file": str(CACHE_FILE),
            "error": str(e),
            "message": "读取缓存失败"
        }


class MomentumSignal(BaseModel):
    concept_name: str
    concept_code: str
    signal_type: str
    total_stocks: int
    timestamp: str
    details: str
    # Surge-specific fields (optional)
    prev_up_count: Optional[int] = None
    current_up_count: Optional[int] = None
    delta_up_count: Optional[int] = None
    threshold: Optional[int] = None
    board_type: Optional[str] = None
    # Kline-specific fields (optional)
    current_change_pct: Optional[float] = None
    kline_info: Optional[dict] = None


class MomentumSignalsResponse(BaseModel):
    success: bool
    timestamp: str
    total_signals: int
    surge_signals_count: int
    kline_signals_count: int
    signals: list[MomentumSignal]


@router.get("/momentum-signals", response_model=MomentumSignalsResponse)
async def get_momentum_signals():
    """
    获取动量信号

    包含两类信号:
    1. 上涨激增信号: 60秒内上涨家数激增
    2. K线形态信号: 30分钟K线为阳线无上影线
    """
    if not SIGNALS_FILE.exists():
        return MomentumSignalsResponse(
            success=False,
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            total_signals=0,
            surge_signals_count=0,
            kline_signals_count=0,
            signals=[]
        )

    try:
        with open(SIGNALS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        signals = []
        for signal_data in data.get('signals', []):
            signals.append(MomentumSignal(**signal_data))

        return MomentumSignalsResponse(
            success=True,
            timestamp=data.get('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            total_signals=data.get('total_signals', 0),
            surge_signals_count=data.get('surge_signals_count', 0),
            kline_signals_count=data.get('kline_signals_count', 0),
            signals=signals
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"读取动量信号失败: {str(e)}"
        )


@router.post("/momentum-signals/refresh")
async def refresh_momentum_signals():
    """
    强制刷新动量信号数据
    触发后台更新脚本运行一次
    """
    try:
        import subprocess
        import os

        # 获取项目根目录
        project_root = Path(__file__).parent.parent.parent

        # 运行更新脚本
        subprocess.Popen(
            ["python", str(project_root / "scripts" / "force_update_monitor.py")],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=str(project_root)
        )

        return {
            "success": True,
            "message": "后台更新已触发，请等待5-10秒后刷新页面查看新数据"
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"触发刷新失败: {str(e)}"
        )
