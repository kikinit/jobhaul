"""Abstract Collector interface, remote detection, and rate-limit handling."""

from __future__ import annotations

import asyncio
import re
from abc import ABC, abstractmethod

import httpx

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


class ApifyCollectorMixin:
    """Shared Apify polling and result-fetching logic for LinkedIn/Indeed collectors."""

    POLL_INTERVAL: int = 10
    TIMEOUT: int = 300  # 5 minutes
    APIFY_DATASET_URL: str = "https://api.apify.com/v2/datasets"

    async def _start_apify_run(
        self, client: httpx.AsyncClient, token: str, run_url: str, body: dict,
    ) -> tuple[str, str]:
        """Start an Apify actor run and return (run_id, dataset_id)."""
        resp = await client.post(f"{run_url}?token={token}", json=body)
        resp.raise_for_status()
        data = resp.json()["data"]
        return data["id"], data["defaultDatasetId"]

    async def _poll_until_done(
        self, client: httpx.AsyncClient, token: str, run_id: str,
    ) -> None:
        """Poll an Apify run until it completes or fails."""
        url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={token}"
        elapsed = 0
        while elapsed < self.TIMEOUT:
            resp = await client.get(url)
            resp.raise_for_status()
            status = resp.json()["data"]["status"]
            if status == "SUCCEEDED":
                return
            if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                raise RuntimeError(f"Apify actor run {status}")
            await asyncio.sleep(self.POLL_INTERVAL)
            elapsed += self.POLL_INTERVAL
        raise TimeoutError("Apify actor run timed out after 5 minutes")

    async def _fetch_apify_results(
        self, client: httpx.AsyncClient, token: str, dataset_id: str,
    ) -> list[dict]:
        """Fetch results from an Apify dataset."""
        url = f"{self.APIFY_DATASET_URL}/{dataset_id}/items?token={token}"
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()
