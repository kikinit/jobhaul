"""Central registry that maps collector names to their classes.

Collector modules use the ``@register`` decorator to add themselves to this
registry at import time.  The rest of the application can then look up
collectors by name or retrieve all registered collectors at once.
"""

from __future__ import annotations

from jobhaul.collectors.base import Collector

_registry: dict[str, type[Collector]] = {}


def register(cls: type[Collector]) -> type[Collector]:
    """Class decorator that registers a collector in the global registry.

    The collector's ``name`` class attribute is used as the registry key.
    This decorator is meant to be applied to every concrete ``Collector``
    subclass so it can be discovered by ``get_collector`` and
    ``get_all_collectors``.

    Args:
        cls: A ``Collector`` subclass with a ``name`` class attribute.

    Returns:
        The same class, unchanged (this is a pass-through decorator).
    """
    _registry[cls.name] = cls
    return cls


def get_collector(name: str) -> Collector:
    """Look up a collector by name and return a fresh instance.

    Args:
        name: The registered name of the collector (e.g. ``"platsbanken"``).

    Returns:
        A new instance of the matching collector class.

    Raises:
        ValueError: If no collector with the given name has been registered.
    """
    if name not in _registry:
        raise ValueError(f"Unknown collector: {name}. Available: {list(_registry.keys())}")
    return _registry[name]()


def get_all_collectors() -> list[Collector]:
    """Create and return a new instance of every registered collector.

    Returns:
        A list of ``Collector`` instances, one per registered class.
    """
    return [cls() for cls in _registry.values()]
