"""Package that aggregates job listings from multiple external sources.

Each collector module in this package implements a source-specific strategy
for fetching job listings (e.g., Platsbanken API, Jooble API, RemoteOK feed).
Collectors self-register via the ``@register`` decorator when their modules
are imported.
"""

from __future__ import annotations


def ensure_collectors_registered():
    """Import all collector modules so their ``@register`` decorators execute.

    Collectors use a decorator-based registration pattern: each module
    decorates its collector class with ``@register``, which adds the class
    to a central registry.  Those decorators only run when the module is
    imported, so this function forces the imports.

    Collectors that depend on optional third-party packages (LinkedIn,
    Indeed) are imported inside a try/except so the application still
    works if those packages are not installed.
    """
    import jobhaul.collectors.jooble  # noqa: F401
    import jobhaul.collectors.platsbanken  # noqa: F401
    import jobhaul.collectors.remoteok  # noqa: F401

    for optional in ("jobhaul.collectors.linkedin", "jobhaul.collectors.indeed"):
        try:
            __import__(optional)
        except ImportError:
            pass
