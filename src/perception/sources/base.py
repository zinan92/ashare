"""Abstract base class for all Perception Layer data sources.

Every external data feed (Tushare, Sina, CLS, ...) must implement
this interface to participate in the Perception Layer.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import List

from src.perception.events import RawMarketEvent
from src.perception.health import SourceHealth


class SourceType(str, Enum):
    """Classification of data source capabilities."""

    POLLING = "polling"  # periodic pull
    STREAMING = "streaming"  # push / websocket
    ON_DEMAND = "on_demand"  # called explicitly


class DataSource(ABC):
    """Contract every data source adapter must fulfil.

    Lifecycle::

        source = MySource(config)
        await source.connect()
        events = await source.poll()
        health = source.health()
        await source.disconnect()
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable source name (e.g. 'tushare', 'sina')."""
        ...

    @property
    @abstractmethod
    def source_type(self) -> SourceType:
        """Whether this source polls, streams, or is on-demand."""
        ...

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection / authenticate.

        Called once at startup.  Implementations should be idempotent.
        """
        ...

    @abstractmethod
    async def poll(self) -> List[RawMarketEvent]:
        """Fetch the latest batch of events.

        For streaming sources this returns buffered events since last call.
        For polling sources this triggers a new fetch.
        """
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Tear down connections / release resources."""
        ...

    @abstractmethod
    def health(self) -> SourceHealth:
        """Return a current health snapshot."""
        ...
