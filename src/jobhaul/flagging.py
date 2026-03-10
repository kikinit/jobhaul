"""Keyword flag matching for job listings.

Compares listing text (title, company, description) against user-defined
boost, warn, and exclude keyword lists to surface, caution, or filter out
listings during collection and display.
"""

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
    """Check a listing's text against the user's flag rules.

    Concatenates the listing's title, company, and description, then
    searches for each term in the boost, warn, and exclude lists.

    Args:
        listing: The job listing to evaluate.
        flags: The keyword flag rules from the user's profile.

    Returns:
        A dict with keys ``"boost"`` (list of matched boost terms),
        ``"warn"`` (list of matched warn terms), and ``"excluded"``
        (``True`` if any exclude term matched).
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
