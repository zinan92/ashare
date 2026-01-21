"""
Base classes and utilities for models
"""
from datetime import datetime, timezone

from src.database import Base


def utcnow() -> datetime:
    """Return current UTC datetime"""
    return datetime.now(timezone.utc)


__all__ = ["Base", "utcnow"]
