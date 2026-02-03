"""Abstract base class for event detectors.

Detectors consume RawMarketEvents and produce UnifiedSignals.
Each detector declares which EventTypes it accepts, allowing
the perception engine to route efficiently.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

from src.perception.events import EventType, RawMarketEvent
from src.perception.signals import UnifiedSignal


class Detector(ABC):
    """Contract for all perception detectors.

    A detector is a stateless (or lightly-stateful) transform::

        events → signals

    Example::

        class MomentumDetector(Detector):
            name = "momentum"
            accepts = [EventType.KLINE, EventType.PRICE_UPDATE]

            def detect(self, event):
                # analyse event.data, return signals
                ...
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable detector name."""
        ...

    @property
    @abstractmethod
    def accepts(self) -> List[EventType]:
        """Event types this detector can process."""
        ...

    @abstractmethod
    def detect(self, event: RawMarketEvent) -> List[UnifiedSignal]:
        """Analyse a single event and return zero or more signals.

        Implementations MUST be safe to call concurrently.
        Returning an empty list means "nothing interesting detected".
        """
        ...
