"""
系统状态 API
提供数据刷新时间、数据源新鲜度检查等功能
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends
from sqlalchemy import func, and_
from sqlalchemy.orm import Session

from src.api.dependencies import get_data_service, get_db
from src.models import Kline, KlineTimeframe, SymbolType
from src.services.data_pipeline import MarketDataService
from src.utils.logging import get_logger

router = APIRouter()
logger = get_logger(__name__)

DATA_DIR = Path(__file__).parent.parent.parent / "data"


@router.get("", response_model=dict[str, Optional[datetime]])
def get_status(
    service: MarketDataService = Depends(get_data_service),
) -> dict[str, Optional[datetime]]:
    """Expose last refresh timestamp used for UI badges."""
    return {"last_refreshed": service.last_refresh_time()}


@router.get("/data-freshness")
def get_data_freshness(db: Session = Depends(get_db)) -> dict[str, Any]:
    """返回各数据源的最后更新时间，用于数据一致性监控。

    检查项目:
    - klines 表各类型 (stock/index/concept) 的最新 trade_time
    - 概念分类 CSV 文件是否存在及修改时间
    - 监控缓存文件是否存在及修改时间
    - 整体数据健康状态
    """
    sources: dict[str, Any] = {}
    issues: list[str] = []
    now = datetime.now()

    # 1. 检查 klines 表各类型数据的最新时间
    for symbol_type in [SymbolType.STOCK, SymbolType.INDEX, SymbolType.CONCEPT]:
        for timeframe in [KlineTimeframe.DAY, KlineTimeframe.MINS_30]:
            key = f"klines_{symbol_type.value}_{timeframe.value}"
            try:
                result = db.query(
                    func.max(Kline.trade_time),
                    func.count(Kline.id),
                ).filter(
                    and_(
                        Kline.symbol_type == symbol_type,
                        Kline.timeframe == timeframe,
                    )
                ).first()

                last_time = result[0] if result else None
                count = result[1] if result else 0

                sources[key] = {
                    "last_update": last_time.isoformat() if last_time else None,
                    "record_count": count,
                    "status": "ok" if last_time else "no_data",
                }

                if not last_time:
                    issues.append(f"{key}: 无数据")
            except Exception as e:
                sources[key] = {
                    "last_update": None,
                    "record_count": 0,
                    "status": "error",
                    "error": str(e),
                }
                issues.append(f"{key}: 查询失败 - {e}")

    # 2. 检查数据文件
    data_files = {
        "hot_concept_categories": DATA_DIR / "hot_concept_categories.csv",
        "concept_to_tickers": DATA_DIR / "concept_to_tickers.csv",
        "ticker_to_concepts": DATA_DIR / "ticker_to_concepts.csv",
        "monitor_cache": DATA_DIR / "monitor" / "latest.json",
        "momentum_signals": DATA_DIR / "monitor" / "momentum_signals.json",
    }

    for name, path in data_files.items():
        if path.exists():
            stat = path.stat()
            sources[f"file_{name}"] = {
                "exists": True,
                "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "size_bytes": stat.st_size,
                "status": "ok",
            }
        else:
            sources[f"file_{name}"] = {
                "exists": False,
                "last_modified": None,
                "size_bytes": 0,
                "status": "missing",
            }
            issues.append(f"file_{name}: 文件不存在 ({path})")

    # 3. 整体健康状态
    healthy = len(issues) == 0
    return {
        "healthy": healthy,
        "checked_at": now.isoformat(),
        "sources": sources,
        "issues": issues,
        "issue_count": len(issues),
    }
