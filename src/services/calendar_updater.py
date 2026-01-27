"""
交易日历更新器
从Tushare获取交易日历数据
"""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Optional

from src.config import get_settings
from src.models import DataUpdateLog, DataUpdateStatus, Kline, KlineTimeframe, TradeCalendar
from src.schemas.normalized import NormalizedDate
from src.services.tushare_client import TushareClient
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.repositories.kline_repository import KlineRepository

logger = get_logger(__name__)


class CalendarUpdater:
    """交易日历和数据清理"""

    def __init__(self, kline_repo: "KlineRepository"):
        self.kline_repo = kline_repo
        self.settings = get_settings()
        self._tushare_client: Optional[TushareClient] = None

    @property
    def tushare_client(self) -> TushareClient:
        if self._tushare_client is None:
            self._tushare_client = TushareClient(
                token=self.settings.tushare_token,
                points=self.settings.tushare_points,
                delay=self.settings.tushare_delay,
                max_retries=self.settings.tushare_max_retries,
            )
        return self._tushare_client

    def _log_update(
        self,
        update_type: str,
        status: DataUpdateStatus,
        records_count: int = 0,
        error_message: str = None,
    ):
        """记录更新日志"""
        now = datetime.now(timezone.utc)
        log = DataUpdateLog(
            update_type=update_type,
            status=status,
            records_updated=records_count,
            error_message=error_message,
            started_at=now,
            completed_at=now if status == DataUpdateStatus.COMPLETED else None,
        )
        self.kline_repo.session.add(log)
        self.kline_repo.session.commit()

    def update_trade_calendar(self) -> int:
        """更新交易日历 (Tushare)"""
        logger.info("更新交易日历...")
        try:
            current_year = datetime.now().year
            start_date = f"{current_year}0101"
            end_date = f"{current_year + 1}1231"

            df = self.tushare_client.fetch_trade_calendar(
                start_date=start_date, end_date=end_date
            )

            if df.empty:
                logger.warning("未获取到交易日历数据")
                return 0

            count = 0
            for _, row in df.iterrows():
                trade_date = NormalizedDate(value=str(row["cal_date"])).to_iso()
                is_open = row["is_open"] == 1

                existing = (
                    self.kline_repo.session.query(TradeCalendar)
                    .filter(TradeCalendar.date == trade_date)
                    .first()
                )
                if existing:
                    existing.is_trading_day = is_open
                else:
                    self.kline_repo.session.add(
                        TradeCalendar(date=trade_date, is_trading_day=is_open)
                    )
                count += 1

            self.kline_repo.session.commit()
            self._log_update("trade_calendar", DataUpdateStatus.COMPLETED, count)
            logger.info(f"交易日历更新完成，共 {count} 条")
            return count

        except Exception as e:
            logger.exception("交易日历更新失败")
            self._log_update(
                "trade_calendar",
                DataUpdateStatus.FAILED,
                error_message=str(e),
            )
            return 0

    def cleanup_old_klines(self, days: int = 365) -> int:
        """
        清理过期K线数据

        Args:
            days: 保留最近N天的数据

        Returns:
            删除的记录数
        """
        logger.info(f"开始清理 {days} 天前的K线数据...")
        total_deleted = 0

        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        try:
            # 清理30分钟线 (只保留90天)
            mins_cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
            deleted = (
                self.kline_repo.session.query(Kline)
                .filter(
                    Kline.timeframe == KlineTimeframe.MINS_30,
                    Kline.trade_time < mins_cutoff,
                )
                .delete()
            )
            total_deleted += deleted
            logger.info(f"  30分钟线: 删除 {deleted} 条")

            # 清理日线 (保留1年)
            deleted = (
                self.kline_repo.session.query(Kline)
                .filter(
                    Kline.timeframe == KlineTimeframe.DAY,
                    Kline.trade_time < cutoff_date,
                )
                .delete()
            )
            total_deleted += deleted
            logger.info(f"  日线: 删除 {deleted} 条")

            self.kline_repo.session.commit()
            self._log_update("cleanup", DataUpdateStatus.COMPLETED, total_deleted)
            logger.info(f"数据清理完成，共删除 {total_deleted} 条")

        except Exception as e:
            logger.exception("数据清理失败")
            self.kline_repo.session.rollback()

        return total_deleted
