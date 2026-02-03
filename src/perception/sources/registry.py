"""Source registry — central catalogue of all data sources.

The registry owns the lifecycle of sources and provides a single
place to query health across the entire perception layer.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from src.perception.health import SourceHealth
from src.perception.sources.base import DataSource


class SourceRegistry:
    """Thread-safe registry of DataSource instances.

    Usage::

        registry = SourceRegistry()
        registry.register(my_tushare_source)
        registry.register(my_sina_source)

        for source in registry.all():
            events = await source.poll()

        report = registry.health_report()
    """

    def __init__(self) -> None:
        self._sources: Dict[str, DataSource] = {}

    def register(self, source: DataSource) -> None:
        """Register a data source. Overwrites if name already exists."""
        self._sources[source.name] = source

    def unregister(self, name: str) -> Optional[DataSource]:
        """Remove and return a source by name, or None."""
        return self._sources.pop(name, None)

    def get(self, name: str) -> Optional[DataSource]:
        """Look up a source by name."""
        return self._sources.get(name)

    def all(self) -> List[DataSource]:
        """Return all registered sources."""
        return list(self._sources.values())

    @property
    def names(self) -> List[str]:
        """Registered source names."""
        return list(self._sources.keys())

    def health_report(self) -> Dict[str, SourceHealth]:
        """Collect health snapshots from every registered source."""
        return {name: src.health() for name, src in self._sources.items()}

    def __len__(self) -> int:
        return len(self._sources)

    def __contains__(self, name: str) -> bool:
        return name in self._sources
