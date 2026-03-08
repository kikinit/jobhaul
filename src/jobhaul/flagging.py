"""Flag matching: boost, warn, exclude against listing content."""

from __future__ import annotations

import re

from jobhaul.models import Flags, JobListing


def _match_flags(text: str, terms: list[str]) -> list[str]:
    """Return which terms match in text using case-insensitive word-boundary matching."""
    matched = []
    for term in terms:
        # Use word boundaries to avoid partial matches (e.g. "AI" matching "EMAIL")
        pattern = r"\b" + re.escape(term) + r"\b"
        if re.search(pattern, text, re.IGNORECASE):
            matched.append(term)
    return matched


def flag_listing(listing: JobListing, flags: Flags) -> dict:
    """Check a listing against flag rules.

    Returns {"boost": [...], "warn": [...], "excluded": bool}
    """
    text = " ".join(
        filter(None, [listing.title, listing.company, listing.description])
    )

    boost_matches = _match_flags(text, flags.boost)
    warn_matches = _match_flags(text, flags.warn)
    exclude_matches = _match_flags(text, flags.exclude)

    return {
        "boost": boost_matches,
        "warn": warn_matches,
        "excluded": len(exclude_matches) > 0,
    }
