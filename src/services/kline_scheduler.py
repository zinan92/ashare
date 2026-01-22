"""
K线数据定时调度器
使用 APScheduler 定时执行K线数据更新任务

重构说明:
- 使用 Repository 模式替代 SessionLocal()
- 强制依赖注入，不再自动创建 Session
- Session 生命周期由调用者控制
"""

import asyncio
from datetime import datetime, time
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session

from src.models import TradeCalendar
from src.services.kline_updater import KlineUpdater
from src.services.data_consistency_validator import DataConsistencyValidator
from src.utils.logging import get_logger

logger = get_logger(__name__)


class KlineScheduler:
    """
    K线数据定时调度器

    重构后支持:
    - 强制依赖注入 Session
    - Session 生命周期由调用者控制
    """

    def __init__(self, session: Session):
        """
        初始化调度器

        Args:
            session: 数据库会话（必需）
        """
        self.session = session
        self.scheduler = AsyncIOScheduler()

        # 使用工厂方法创建服务
        self.updater = KlineUpdater.create_with_session(self.session)
        self.validator = DataConsistencyValidator.create_with_session(self.session)
        self._is_running = False

    @classmethod
    def create_with_session(cls, session: Session) -> "KlineScheduler":
        """使用现有session创建调度器的工厂方法"""
        return cls(session=session)

    def is_trading_day(self, date: datetime = None) -> bool:
        """
        判断是否为交易日

        Args:
            date: 日期，默认为今天

        Returns:
            是否为交易日
        """
        if date is None:
            date = datetime.now()

        trade_date = date.strftime("%Y-%m-%d")

        # 使用实例的 session 而不是创建新的
        cal = (
            self.session.query(TradeCalendar)
            .filter(TradeCalendar.date == trade_date)
            .first()
        )
        if cal:
            return cal.is_trading_day
        else:
            # 如果没有数据，按周末判断
            return date.weekday() < 5

    def is_trading_time(self, dt: datetime = None) -> bool:
        """
        判断是否为交易时间

        Args:
            dt: 时间，默认为当前时间

        Returns:
            是否为交易时间
        """
        if dt is None:
            dt = datetime.now()

        if not self.is_trading_day(dt):
            return False

        current_time = dt.time()

        # 上午: 09:30 - 11:30
        morning_start = time(9, 30)
        morning_end = time(11, 30)

        # 下午: 13:00 - 15:00
        afternoon_start = time(13, 0)
        afternoon_end = time(15, 0)

        return (morning_start <= current_time <= morning_end) or (
            afternoon_start <= current_time <= afternoon_end
        )

    # ==================== 任务函数 ====================

    async def _job_daily_update(self):
        """每日更新任务 (15:30 执行)"""
        if not self.is_trading_day():
            logger.info("非交易日，跳过日线更新")
            return

        logger.info("=" * 50)
        logger.info("开始执行每日K线更新任务")
        logger.info("=" * 50)

        try:
            # 更新指数日线
            await self.updater.update_index_daily()

            # 更新概念日线
            await self.updater.update_concept_daily()

            # 更新自选股日线
            await self.updater.update_stock_daily()

            logger.info("每日更新任务完成")

        except Exception as e:
            logger.exception(f"每日更新任务失败: {e}")

    async def _job_30m_update(self):
        """30分钟更新任务"""
        if not self.is_trading_day():
            logger.debug("非交易日，跳过30分钟更新")
            return

        # 检查是否在交易时间附近 (交易时间结束后15分钟内)
        now = datetime.now()
        current_time = now.time()

        # 只在特定时间点执行: 10:00, 10:30, 11:00, 11:30, 13:30, 14:00, 14:30, 15:00
        valid_minutes = [0, 30]
        valid_hours_am = [10, 11]
        valid_hours_pm = [13, 14, 15]

        if current_time.minute not in valid_minutes:
            return

        hour = current_time.hour
        if hour not in valid_hours_am and hour not in valid_hours_pm:
            return

        # 15:00 后不再更新30分钟线 (日线更新会处理)
        if hour == 15 and current_time.minute > 0:
            return

        logger.info(f"开始执行30分钟K线更新 ({now.strftime('%H:%M')})")

        try:
            await asyncio.gather(
                self.updater.update_index_30m(),
                self.updater.update_concept_30m(),
                self.updater.update_stock_30m(),
            )
            logger.info("30分钟更新任务完成")

        except Exception as e:
            logger.exception(f"30分钟更新任务失败: {e}")

    async def _job_calendar_update(self):
        """每日更新交易日历 (00:01 执行)"""
        logger.info("开始更新交易日历...")
        try:
            self.updater.update_trade_calendar()
        except Exception as e:
            logger.exception(f"交易日历更新失败: {e}")

    async def _job_cleanup(self):
        """每周清理旧数据 (周日 00:00 执行)"""
        logger.info("开始清理旧K线数据...")
        try:
            self.updater.cleanup_old_klines(days=365)
        except Exception as e:
            logger.exception(f"数据清理失败: {e}")

    async def _job_stock_daily(self):
        """自选股日线更新任务 (手动触发)"""
        logger.info("开始更新自选股日线数据...")
        try:
            await self.updater.update_stock_daily()
        except Exception as e:
            logger.exception(f"自选股日线更新失败: {e}")

    async def _job_stock_30m(self):
        """自选股30分钟更新任务 (手动触发)"""
        logger.info("开始更新自选股30分钟数据...")
        try:
            await self.updater.update_stock_30m()
        except Exception as e:
            logger.exception(f"自选股30分钟更新失败: {e}")

    async def _job_all_stock_daily(self):
        """全市场日线更新任务 (手动触发或定时22:00执行)"""
        if not self.is_trading_day():
            logger.info("非交易日，跳过全市场日线更新")
            return

        logger.info("开始更新全市场日线数据...")
        try:
            await self.updater.update_all_stock_daily()
        except Exception as e:
            logger.exception(f"全市场日线更新失败: {e}")

    async def _job_data_validation(self):
        """数据一致性验证任务 (交易日 15:45 执行)"""
        if not self.is_trading_day():
            logger.info("非交易日，跳过数据一致性验证")
            return

        logger.info("开始执行数据一致性验证...")
        try:
            is_healthy = await self.validator.validate_and_report()
            if not is_healthy:
                logger.warning("数据一致性验证发现异常，请检查日志")
            else:
                logger.info("数据一致性验证通过 ✅")
        except Exception as e:
            logger.exception(f"数据一致性验证失败: {e}")

    # ==================== 调度器控制 ====================

    def start(self):
        """启动调度器"""
        if self._is_running:
            logger.warning("调度器已在运行")
            return

        logger.info("正在启动K线数据调度器...")

        # 1. 每日更新任务 (交易日 15:30)
        self.scheduler.add_job(
            self._job_daily_update,
            CronTrigger(hour=15, minute=30),
            id="daily_update",
            name="每日K线更新",
            replace_existing=True,
        )

        # 2. 30分钟更新任务 (交易时间每30分钟)
        # 使用 cron 在整点和半点触发，任务内部判断是否执行
        self.scheduler.add_job(
            self._job_30m_update,
            CronTrigger(minute="0,30"),
            id="30m_update",
            name="30分钟K线更新",
            replace_existing=True,
        )

        # 3. 交易日历更新 (每天 00:01)
        self.scheduler.add_job(
            self._job_calendar_update,
            CronTrigger(hour=0, minute=1),
            id="calendar_update",
            name="交易日历更新",
            replace_existing=True,
        )

        # 4. 数据清理任务 (每周日 00:00)
        self.scheduler.add_job(
            self._job_cleanup,
            CronTrigger(day_of_week="sun", hour=0, minute=0),
            id="cleanup",
            name="旧数据清理",
            replace_existing=True,
        )

        # 5. 全市场日线更新任务 (交易日 16:00)
        self.scheduler.add_job(
            self._job_all_stock_daily,
            CronTrigger(hour=16, minute=0),
            id="all_stock_daily",
            name="全市场日线更新",
            replace_existing=True,
        )

        # 6. 数据一致性验证任务 (交易日 15:45)
        self.scheduler.add_job(
            self._job_data_validation,
            CronTrigger(hour=15, minute=45),
            id="data_validation",
            name="数据一致性验证",
            replace_existing=True,
        )

        self.scheduler.start()
        self._is_running = True

        logger.info("K线数据调度器已启动")
        logger.info("已注册任务:")
        for job in self.scheduler.get_jobs():
            logger.info(f"  - {job.name} (ID: {job.id})")

    def stop(self):
        """停止调度器"""
        if not self._is_running:
            return

        logger.info("正在停止K线数据调度器...")
        self.scheduler.shutdown(wait=False)
        self._is_running = False
        logger.info("K线数据调度器已停止")

    def get_jobs(self) -> list[dict]:
        """获取所有任务信息"""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger),
            })
        return jobs

    async def run_job_now(self, job_id: str) -> bool:
        """
        立即执行指定任务

        Args:
            job_id: 任务ID

        Returns:
            是否成功触发
        """
        job_map = {
            "daily_update": self._job_daily_update,
            "30m_update": self._job_30m_update,
            "calendar_update": self._job_calendar_update,
            "cleanup": self._job_cleanup,
            "stock_daily": self._job_stock_daily,
            "stock_30m": self._job_stock_30m,
            "all_stock_daily": self._job_all_stock_daily,
        }

        if job_id not in job_map:
            logger.warning(f"未知任务: {job_id}")
            return False

        logger.info(f"手动触发任务: {job_id}")
        await job_map[job_id]()
        return True


# 全局调度器实例
_scheduler: Optional[KlineScheduler] = None


def get_scheduler() -> KlineScheduler:
    """获取调度器单例"""
    global _scheduler
    if _scheduler is None:
        _scheduler = KlineScheduler()
    return _scheduler


def start_scheduler():
    """启动调度器"""
    scheduler = get_scheduler()
    scheduler.start()


def stop_scheduler():
    """停止调度器"""
    global _scheduler
    if _scheduler is not None:
        _scheduler.stop()
        _scheduler = None
