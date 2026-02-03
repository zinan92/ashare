"""Source health monitoring models.

Tracks per-source health metrics (latency, error rate, uptime) and
provides a HealthMonitor that aggregates across all registered sources.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class HealthStatus(str, Enum):
    """Traffic-light health indicator."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class SourceHealth(BaseModel):
    """Point-in-time health snapshot for a single data source."""

    source_name: str
    status: HealthStatus = HealthStatus.UNKNOWN
    latency_ms: Optional[float] = Field(
        default=None, description="Average latency in ms"
    )
    error_rate: float = Field(
        default=0.0, ge=0.0, le=1.0, description="Error rate 0-1"
    )
    last_success: Optional[datetime] = None
    last_error: Optional[datetime] = None
    last_error_message: Optional[str] = None
    consecutive_failures: int = Field(default=0, ge=0)
    total_polls: int = Field(default=0, ge=0)
    total_events: int = Field(default=0, ge=0)
    uptime_seconds: Optional[float] = None
    checked_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    model_config = {"use_enum_values": True}


class HealthMonitor:
    """Aggregates health reports from multiple data sources.

    Usage::

        monitor = HealthMonitor()
        monitor.update("tushare", source_health)
        report = monitor.report()
    """

    def __init__(self) -> None:
        self._snapshots: Dict[str, SourceHealth] = {}

    def update(self, source_name: str, health: SourceHealth) -> None:
        """Record a health snapshot for *source_name*."""
        self._snapshots[source_name] = health

    def get(self, source_name: str) -> Optional[SourceHealth]:
        """Get latest health snapshot for a source."""
        return self._snapshots.get(source_name)

    def report(self) -> Dict[str, SourceHealth]:
        """Return all current health snapshots."""
        return dict(self._snapshots)

    @property
    def all_healthy(self) -> bool:
        if not self._snapshots:
            return False
        return all(
            h.status == HealthStatus.HEALTHY for h in self._snapshots.values()
        )

    @property
    def unhealthy_sources(self) -> List[str]:
        return [
            name
            for name, h in self._snapshots.items()
            if h.status == HealthStatus.UNHEALTHY
        ]
