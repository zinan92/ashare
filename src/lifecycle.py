from fastapi import FastAPI

from src.config import get_settings
from src.database import init_db
from src.tasks.scheduler import SchedulerManager
from src.services.kline_scheduler import get_scheduler, stop_scheduler
from src.services.crypto_ws import start_crypto_ws, stop_crypto_ws
from src.utils.logging import LOGGER

_scheduler_manager: SchedulerManager | None = None


def register_startup_shutdown(app: FastAPI) -> None:
    @app.on_event("startup")
    async def _startup() -> None:
        LOGGER.info("Application startup - using Tushare Pro")

        # Tushare does not require patches like AkShare did

        init_db()
        settings = get_settings()
        if settings.scheduler:
            global _scheduler_manager
            _scheduler_manager = SchedulerManager()
            _scheduler_manager.start()

            # K线数据调度器 — 日线用Tushare Pro，30分钟用新浪
            # 不再有新浪限流问题（日线已切Tushare）
            kline_scheduler = get_scheduler()
            kline_scheduler.start()
            LOGGER.info("K-line scheduler STARTED (daily=Tushare, 30m=Sina)")

        # Crypto WebSocket 实时数据流
        try:
            await start_crypto_ws()
            LOGGER.info("Crypto WebSocket STARTED (Binance realtime)")
        except Exception as e:
            LOGGER.warning(f"Crypto WebSocket failed to start: {e} (non-fatal)")

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        LOGGER.info("Application shutdown")
        if _scheduler_manager:
            _scheduler_manager.shutdown()

        # 停止K线数据调度器
        stop_scheduler()

        # 停止Crypto WebSocket
        try:
            await stop_crypto_ws()
        except Exception as e:
            LOGGER.warning(f"Crypto WS shutdown error: {e}")
