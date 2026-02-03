"""Perception Layer configuration models.

Pydantic settings for source polling, detector thresholds, and
circuit-breaker behaviour.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class SourcePollConfig(BaseModel):
    """Per-source polling configuration."""

    source_name: str
    enabled: bool = True
    poll_interval_seconds: float = Field(
        default=60.0, gt=0, description="How often to poll this source"
    )
    timeout_seconds: float = Field(
        default=30.0, gt=0, description="HTTP / connection timeout"
    )
    max_retries: int = Field(default=3, ge=0)
    backoff_factor: float = Field(
        default=2.0, gt=0, description="Exponential backoff multiplier"
    )


class CircuitBreakerConfig(BaseModel):
    """Circuit breaker thresholds for a data source."""

    failure_threshold: int = Field(
        default=5, ge=1, description="Consecutive failures before opening"
    )
    recovery_timeout_seconds: float = Field(
        default=300.0, gt=0, description="Seconds before half-open probe"
    )
    half_open_max_calls: int = Field(
        default=1, ge=1, description="Probes allowed in half-open state"
    )


class DetectorConfig(BaseModel):
    """Configuration for a single Detector."""

    detector_name: str
    enabled: bool = True
    min_confidence: float = Field(
        default=0.3, ge=0.0, le=1.0, description="Drop signals below this"
    )
    params: Dict[str, float] = Field(
        default_factory=dict, description="Detector-specific tuning knobs"
    )


class PerceptionConfig(BaseModel):
    """Top-level perception layer configuration."""

    sources: List[SourcePollConfig] = Field(default_factory=list)
    detectors: List[DetectorConfig] = Field(default_factory=list)
    circuit_breaker: CircuitBreakerConfig = Field(
        default_factory=CircuitBreakerConfig
    )
    max_event_age_seconds: float = Field(
        default=300.0,
        gt=0,
        description="Discard events older than this",
    )
    health_check_interval_seconds: float = Field(
        default=60.0, gt=0, description="How often to run health checks"
    )
