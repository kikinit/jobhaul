"""Collectors package."""

from __future__ import annotations


def ensure_collectors_registered():
    """Import all collector modules to trigger @register decorators."""
    import jobhaul.collectors.jooble  # noqa: F401
    import jobhaul.collectors.platsbanken  # noqa: F401
    import jobhaul.collectors.remoteok  # noqa: F401

    for optional in ("jobhaul.collectors.linkedin", "jobhaul.collectors.indeed"):
        try:
            __import__(optional)
        except ImportError:
            pass
