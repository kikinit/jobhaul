"""Abstract Collector interface, remote detection, and rate-limit handling."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod

from jobhaul.log import get_logger
from jobhaul.models import CollectorResult, Profile

logger = get_logger(__name__)

REMOTE_PATTERNS = re.compile(
    r"\b(remote|distans|hemma|hybrid|fjärr|på distans|jobba hemifrån)\b",
    re.IGNORECASE,
)


def detect_remote(title: str, description: str) -> bool:
    """Detect remote/hybrid jobs from title and description text."""
    return bool(REMOTE_PATTERNS.search(f"{title} {description}"))


def handle_rate_limit(response, collector_name: str) -> int:
    """Extract wait time from a 429 response.

    Returns wait time in seconds (from Retry-After header, default 60).
    """
    retry_after = response.headers.get("retry-after", "")
    try:
        wait = int(retry_after)
    except (ValueError, TypeError):
        wait = 60
    logger.warning(
        "%s: rate limited (429), waiting %ds before retry", collector_name, wait
    )
    return wait


class Collector(ABC):
    name: str

    @abstractmethod
    async def collect(self, profile: Profile) -> CollectorResult: ...
