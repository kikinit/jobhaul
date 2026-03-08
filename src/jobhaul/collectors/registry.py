"""Collector registry."""

from __future__ import annotations

from jobhaul.collectors.base import Collector

_registry: dict[str, type[Collector]] = {}


def register(cls: type[Collector]) -> type[Collector]:
    """Register a collector class."""
    _registry[cls.name] = cls
    return cls


def get_collector(name: str) -> Collector:
    """Get a collector instance by name."""
    if name not in _registry:
        raise ValueError(f"Unknown collector: {name}. Available: {list(_registry.keys())}")
    return _registry[name]()


def get_all_collectors() -> list[Collector]:
    """Get instances of all registered collectors."""
    return [cls() for cls in _registry.values()]
