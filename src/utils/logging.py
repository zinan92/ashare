import logging
from pathlib import Path

from src.config import get_settings

LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | %(funcName)s:%(lineno)d | %(message)s"
)

# 全局标志，防止重复配置
_configured = False


def configure_logging() -> None:
    """
    配置日志系统（幂等操作）

    该函数可以被多次调用，但只会在第一次调用时执行配置。
    这避免了重复配置导致的警告和性能问题。
    """
    global _configured

    # 如果已经配置过，直接返回
    if _configured:
        return

    settings = get_settings()
    log_dir: Path = settings.logs_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_dir / "service.log", encoding="utf-8"),
        ],
    )

    _configured = True


def reset_logging_config() -> None:
    """
    重置日志配置状态（主要用于测试）

    该函数会：
    1. 重置配置标志，允许重新配置
    2. 清除所有已注册的 handlers

    注意：这个函数主要用于测试环境，生产环境不应调用。
    """
    global _configured

    # 重置标志
    _configured = False

    # 清除 root logger 的所有 handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        handler.close()
        root_logger.removeHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for the given name.

    Best practice: Use __name__ as the logger name in each module.
    Example: logger = get_logger(__name__)

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


configure_logging()

# Backward compatibility: Keep LOGGER for existing code
LOGGER = logging.getLogger("a_share_monitor")
