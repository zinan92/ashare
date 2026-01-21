"""
统一K线数据API - 重构版本
提供统一的K线数据访问接口，使用依赖注入
"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.database import SessionLocal
from src.models import KlineTimeframe, SymbolType
from src.services.kline_service import KlineService
from src.utils.logging import get_logger

router = APIRouter()
logger = get_logger(__name__)


# ==================== 依赖注入 ====================


def get_db() -> Session:
    """
    获取数据库Session（依赖注入）

    Yields:
        SQLAlchemy Session
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_kline_service(db: Session = Depends(get_db)) -> KlineService:
    """
    获取KlineService实例（依赖注入）

    Args:
        db: 数据库Session

    Returns:
        KlineService实例
    """
    return KlineService.create_with_session(db)


# ==================== 辅助函数 ====================


def _parse_symbol_type(symbol_type: str) -> SymbolType:
    """解析标的类型"""
    try:
        return SymbolType(symbol_type)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"无效的标的类型: {symbol_type}，支持: stock, index, concept",
        )


def _parse_timeframe(timeframe: str) -> KlineTimeframe:
    """解析时间周期"""
    tf_map = {
        "day": KlineTimeframe.DAY,
        "30m": KlineTimeframe.MINS_30,
        "5m": KlineTimeframe.MINS_5,
        "1m": KlineTimeframe.MINS_1,
    }
    tf = tf_map.get(timeframe)
    if tf is None:
        raise HTTPException(
            status_code=400,
            detail=f"无效的时间周期: {timeframe}，支持: day, 30m, 5m, 1m",
        )
    return tf


# ==================== API 端点 ====================


@router.get("/{symbol_type}/{symbol_code}")
def get_klines(
    symbol_type: str,
    symbol_code: str,
    timeframe: str = Query(default="day", description="时间周期: day, 30m"),
    limit: int = Query(default=120, ge=10, le=500, description="K线数量"),
    start_date: Optional[str] = Query(default=None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="结束日期 YYYY-MM-DD"),
    service: KlineService = Depends(get_kline_service),
) -> Dict[str, Any]:
    """
    获取K线数据

    Args:
        symbol_type: 标的类型 (stock/index/concept)
        symbol_code: 标的代码
        timeframe: 时间周期 (day/30m)
        limit: 返回数量
        start_date: 开始日期（可选）
        end_date: 结束日期（可选）
        service: KlineService实例（依赖注入）

    Returns:
        K线数据字典，包含:
        - symbol_type: 标的类型
        - symbol_code: 标的代码
        - symbol_name: 标的名称
        - timeframe: 时间周期
        - count: 数据数量
        - klines: K线数据列表

    Raises:
        HTTPException: 400 - 参数错误
        HTTPException: 404 - 数据不存在
        HTTPException: 500 - 服务器错误
    """
    # 解析参数
    sym_type = _parse_symbol_type(symbol_type)
    tf = _parse_timeframe(timeframe)

    try:
        # 使用注入的Service获取数据
        result = service.get_klines_with_meta(
            symbol_type=sym_type,
            symbol_code=symbol_code,
            timeframe=tf,
            limit=limit,
            include_indicators=True,  # 包含技术指标
        )

        # 检查是否有数据
        if not result["klines"]:
            raise HTTPException(
                status_code=404,
                detail=f"未找到K线数据: {symbol_type}/{symbol_code}",
            )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"获取K线数据失败: {symbol_type}/{symbol_code}")
        raise HTTPException(status_code=500, detail=f"获取K线数据失败: {str(e)}")


@router.get("/{symbol_type}/{symbol_code}/latest")
def get_latest_kline(
    symbol_type: str,
    symbol_code: str,
    timeframe: str = Query(default="day", description="时间周期: day, 30m"),
    service: KlineService = Depends(get_kline_service),
) -> Dict[str, Any]:
    """
    获取最新的K线数据

    Args:
        symbol_type: 标的类型 (stock/index/concept)
        symbol_code: 标的代码
        timeframe: 时间周期 (day/30m)
        service: KlineService实例（依赖注入）

    Returns:
        最新的K线数据

    Raises:
        HTTPException: 400 - 参数错误
        HTTPException: 404 - 数据不存在
        HTTPException: 500 - 服务器错误
    """
    sym_type = _parse_symbol_type(symbol_type)
    tf = _parse_timeframe(timeframe)

    try:
        kline = service.get_latest_kline(
            symbol_type=sym_type,
            symbol_code=symbol_code,
            timeframe=tf,
        )

        if not kline:
            raise HTTPException(
                status_code=404,
                detail=f"未找到K线数据: {symbol_type}/{symbol_code}",
            )

        return {
            "symbol_type": sym_type.value,
            "symbol_code": symbol_code,
            "timeframe": tf.value,
            "kline": kline,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"获取最新K线失败: {symbol_type}/{symbol_code}")
        raise HTTPException(status_code=500, detail=f"获取最新K线失败: {str(e)}")


@router.get("/{symbol_type}/{symbol_code}/count")
def get_klines_count(
    symbol_type: str,
    symbol_code: str,
    timeframe: str = Query(default="day", description="时间周期: day, 30m"),
    service: KlineService = Depends(get_kline_service),
) -> Dict[str, Any]:
    """
    获取K线数据数量

    Args:
        symbol_type: 标的类型 (stock/index/concept)
        symbol_code: 标的代码
        timeframe: 时间周期 (day/30m)
        service: KlineService实例（依赖注入）

    Returns:
        K线数量信息

    Raises:
        HTTPException: 400 - 参数错误
        HTTPException: 500 - 服务器错误
    """
    sym_type = _parse_symbol_type(symbol_type)
    tf = _parse_timeframe(timeframe)

    try:
        count = service.get_klines_count(
            symbol_type=sym_type,
            symbol_code=symbol_code,
            timeframe=tf,
        )

        return {
            "symbol_type": sym_type.value,
            "symbol_code": symbol_code,
            "timeframe": tf.value,
            "count": count,
        }

    except Exception as e:
        logger.exception(f"获取K线数量失败: {symbol_type}/{symbol_code}")
        raise HTTPException(status_code=500, detail=f"获取K线数量失败: {str(e)}")
