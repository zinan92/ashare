"""
统一K线数据服务 - 重构版本

提供统一的K线数据访问接口，使用Repository模式。
业务逻辑层，专注于指标计算和数据组装。
"""

from datetime import datetime
from typing import Optional

import numpy as np
from sqlalchemy.orm import Session

from src.models import KlineTimeframe, SymbolType
from src.repositories.kline_repository import KlineRepository
from src.repositories.symbol_repository import SymbolRepository
from src.schemas.normalized import NormalizedDate, NormalizedTicker
from src.utils.logging import get_logger

logger = get_logger(__name__)


def calculate_macd(
    close_prices: list[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> dict[str, list[float | None]]:
    """
    计算MACD指标

    Args:
        close_prices: 收盘价列表
        fast_period: 快线周期
        slow_period: 慢线周期
        signal_period: 信号线周期

    Returns:
        包含dif, dea, macd的字典
    """
    if len(close_prices) < slow_period:
        return {
            "dif": [None] * len(close_prices),
            "dea": [None] * len(close_prices),
            "macd": [None] * len(close_prices),
        }

    closes = np.array(close_prices, dtype=float)

    def ema(data: np.ndarray, period: int) -> np.ndarray:
        """计算指数移动平均"""
        result = np.zeros(len(data))
        multiplier = 2 / (period + 1)
        result[0] = data[0]
        for i in range(1, len(data)):
            result[i] = (data[i] - result[i - 1]) * multiplier + result[i - 1]
        return result

    ema_fast = ema(closes, fast_period)
    ema_slow = ema(closes, slow_period)
    dif = ema_fast - ema_slow
    dea = ema(dif, signal_period)
    macd_bar = (dif - dea) * 2

    return {
        "dif": [round(v, 4) for v in dif.tolist()],
        "dea": [round(v, 4) for v in dea.tolist()],
        "macd": [round(v, 4) for v in macd_bar.tolist()],
    }


class KlineService:
    """
    K线数据业务服务

    职责:
    - 查询K线数据（委托给Repository）
    - 计算技术指标（MACD等）
    - 组装返回数据格式
    """

    def __init__(
        self,
        kline_repo: KlineRepository,
        symbol_repo: Optional[SymbolRepository] = None,
    ):
        """
        初始化KlineService

        Args:
            kline_repo: K线数据Repository
            symbol_repo: 标的数据Repository（可选）
        """
        self.kline_repo = kline_repo
        self.symbol_repo = symbol_repo

    @classmethod
    def create_with_session(cls, session: Session) -> "KlineService":
        """
        使用Session创建KlineService实例（工厂方法）

        Args:
            session: SQLAlchemy Session

        Returns:
            KlineService实例
        """
        kline_repo = KlineRepository(session)
        symbol_repo = SymbolRepository(session)
        return cls(kline_repo, symbol_repo)

    def get_klines(
        self,
        symbol_type: SymbolType,
        symbol_code: str,
        timeframe: KlineTimeframe = KlineTimeframe.DAY,
        limit: int = 120,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list[dict]:
        """
        获取K线数据

        Args:
            symbol_type: 标的类型 (stock/index/concept)
            symbol_code: 标的代码 (支持任意格式，会自动标准化)
            timeframe: 时间周期 (day/30m)
            limit: 返回数量
            start_date: 开始日期 (可选，支持任意格式)
            end_date: 结束日期 (可选，支持任意格式)

        Returns:
            K线数据列表，日期格式为ISO标准 (YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS)
        """
        # 标准化symbol_code（个股用6位代码，指数/概念保持原样）
        if symbol_type == SymbolType.STOCK:
            try:
                symbol_code = NormalizedTicker(raw=symbol_code).raw
            except ValueError:
                pass  # 保持原值

        # 标准化日期参数
        start_datetime = None
        end_datetime = None

        if start_date:
            try:
                start_datetime = datetime.fromisoformat(
                    NormalizedDate(value=start_date).to_iso()
                )
            except ValueError:
                pass

        if end_date:
            try:
                end_datetime = datetime.fromisoformat(
                    NormalizedDate(value=end_date).to_iso()
                )
            except ValueError:
                pass

        # 根据是否有日期范围选择不同的查询方法
        if start_datetime and end_datetime:
            klines = self.kline_repo.find_by_symbol_and_date_range(
                symbol_code=symbol_code,
                symbol_type=symbol_type,
                timeframe=timeframe,
                start_date=start_datetime,
                end_date=end_datetime,
            )
        else:
            klines = self.kline_repo.find_by_symbol(
                symbol_code=symbol_code,
                symbol_type=symbol_type,
                timeframe=timeframe,
                limit=limit,
            )
            # Repository返回的是倒序，需要反转
            klines = list(reversed(klines))

        # 转换为字典格式
        return [
            {
                "datetime": k.trade_time,  # Return as 'datetime' for API backward compatibility
                "open": k.open,
                "high": k.high,
                "low": k.low,
                "close": k.close,
                "volume": k.volume,
                "amount": k.amount,
            }
            for k in klines
        ]

    def get_klines_with_indicators(
        self,
        symbol_type: SymbolType,
        symbol_code: str,
        timeframe: KlineTimeframe = KlineTimeframe.DAY,
        limit: int = 120,
        include_macd: bool = True,
    ) -> list[dict]:
        """
        获取带技术指标的K线数据

        Args:
            symbol_type: 标的类型
            symbol_code: 标的代码
            timeframe: 时间周期
            limit: 返回数量
            include_macd: 是否包含MACD指标

        Returns:
            包含技术指标的K线数据列表
        """
        klines = self.get_klines(symbol_type, symbol_code, timeframe, limit)

        if not klines:
            return []

        # 计算MACD指标
        if include_macd:
            close_prices = [k["close"] for k in klines if k["close"] is not None]
            if close_prices:
                macd_data = calculate_macd(close_prices)

                # 将指标添加到K线数据中
                for i, kline in enumerate(klines):
                    if i < len(macd_data["dif"]):
                        kline["dif"] = macd_data["dif"][i]
                        kline["dea"] = macd_data["dea"][i]
                        kline["macd"] = macd_data["macd"][i]

        return klines

    def get_klines_with_meta(
        self,
        symbol_type: SymbolType,
        symbol_code: str,
        timeframe: KlineTimeframe = KlineTimeframe.DAY,
        limit: int = 120,
        include_indicators: bool = True,
    ) -> dict:
        """
        获取K线数据及元信息

        Args:
            symbol_type: 标的类型
            symbol_code: 标的代码
            timeframe: 时间周期
            limit: 返回数量
            include_indicators: 是否包含技术指标

        Returns:
            包含 symbol_type, symbol_code, symbol_name, timeframe, count, klines 的字典
        """
        # 获取K线数据
        if include_indicators:
            klines = self.get_klines_with_indicators(
                symbol_type, symbol_code, timeframe, limit
            )
        else:
            klines = self.get_klines(symbol_type, symbol_code, timeframe, limit)

        # 获取标的名称
        symbol_name = None
        if klines:
            # 从第一条K线获取名称
            first_kline = self.kline_repo.find_by_symbol(
                symbol_code=symbol_code,
                symbol_type=symbol_type,
                timeframe=timeframe,
                limit=1,
            )
            if first_kline:
                symbol_name = first_kline[0].symbol_name

        return {
            "symbol_type": symbol_type.value,
            "symbol_code": symbol_code,
            "symbol_name": symbol_name,
            "timeframe": timeframe.value,
            "count": len(klines),
            "klines": klines,
        }

    def get_latest_kline(
        self,
        symbol_type: SymbolType,
        symbol_code: str,
        timeframe: KlineTimeframe = KlineTimeframe.DAY,
    ) -> Optional[dict]:
        """
        获取最新的K线数据

        Args:
            symbol_type: 标的类型
            symbol_code: 标的代码
            timeframe: 时间周期

        Returns:
            最新K线数据字典或None
        """
        kline = self.kline_repo.find_latest_by_symbol(
            symbol_code=symbol_code,
            symbol_type=symbol_type,
            timeframe=timeframe,
        )

        if not kline:
            return None

        return {
            "datetime": kline.trade_time,  # Return as 'datetime' for API backward compatibility
            "open": kline.open,
            "high": kline.high,
            "low": kline.low,
            "close": kline.close,
            "volume": kline.volume,
            "amount": kline.amount,
        }

    def get_latest_trade_time(
        self,
        symbol_type: SymbolType,
        symbol_code: str,
        timeframe: KlineTimeframe = KlineTimeframe.DAY,
    ) -> Optional[str]:
        """
        获取最新K线时间

        Args:
            symbol_type: 标的类型
            symbol_code: 标的代码
            timeframe: 时间周期

        Returns:
            最新交易时间的ISO字符串或None
        """
        kline = self.kline_repo.find_latest_by_symbol(
            symbol_code, symbol_type, timeframe
        )

        if kline and kline.trade_time:
            return kline.trade_time

        return None

    def get_klines_count(
        self,
        symbol_type: SymbolType,
        symbol_code: str,
        timeframe: KlineTimeframe = KlineTimeframe.DAY,
    ) -> int:
        """
        获取K线数据数量

        Args:
            symbol_type: 标的类型
            symbol_code: 标的代码
            timeframe: 时间周期

        Returns:
            K线数量
        """
        return self.kline_repo.count_by_symbol(symbol_code, symbol_type, timeframe)

    def get_symbols_with_kline_data(
        self,
        symbol_type: SymbolType,
        timeframe: KlineTimeframe = KlineTimeframe.DAY,
    ) -> list[str]:
        """
        获取有K线数据的标的列表

        Args:
            symbol_type: 标的类型
            timeframe: 时间周期

        Returns:
            标的代码列表
        """
        return self.kline_repo.find_symbols_with_data(symbol_type, timeframe)
