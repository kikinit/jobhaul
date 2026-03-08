"""Abstract Collector interface and remote detection."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod

from jobhaul.models import CollectorResult, Profile

REMOTE_PATTERNS = re.compile(
    r"\b(remote|distans|hemma|hybrid|fjärr|på distans|jobba hemifrån)\b",
    re.IGNORECASE,
)


def detect_remote(title: str, description: str) -> bool:
    """Detect remote/hybrid jobs from title and description text."""
    return bool(REMOTE_PATTERNS.search(f"{title} {description}"))


class Collector(ABC):
    name: str

    @abstractmethod
    async def collect(self, profile: Profile) -> CollectorResult: ...
