"""Base classes and shared utilities used by all job-listing collectors.

This module defines the abstract ``Collector`` interface that every source
must implement, the ``ApifyCollectorMixin`` for collectors that use the
Apify web-scraping platform, and helper functions for remote-job detection,
rate-limit handling, and HTTP request retries.
"""

from __future__ import annotations

import asyncio
import re
from abc import ABC, abstractmethod

import httpx

from jobhaul.constants import APIFY_POLL_INTERVAL_SECS, APIFY_TIMEOUT_SECS
from jobhaul.log import get_logger
from jobhaul.models import CollectorResult, Profile

logger = get_logger(__name__)

REMOTE_PATTERNS = re.compile(
    r"\b(remote|distans|hemma|hybrid|fjärr|på distans|jobba hemifrån)\b",
    re.IGNORECASE,
)


def detect_remote(title: str, description: str) -> bool:
    """Check whether a job listing appears to be remote or hybrid.

    Scans the title and description for keywords in English and Swedish
    (e.g. "remote", "hybrid", "distans", "hemma") and returns ``True`` if
    any match is found.

    Args:
        title: The job listing title.
        description: The full or partial job description text.

    Returns:
        ``True`` if at least one remote/hybrid keyword is found.
    """
    return bool(REMOTE_PATTERNS.search(f"{title} {description}"))


def handle_rate_limit(response, collector_name: str) -> int:
    """Extract the recommended wait time from an HTTP 429 (Too Many Requests) response.

    Reads the ``Retry-After`` header to determine how long to wait before
    retrying.  Falls back to 60 seconds if the header is missing or
    unparseable.

    Args:
        response: The HTTP response object (expected to have a ``.headers``
            mapping).
        collector_name: Name of the collector, used only for log messages.

    Returns:
        The number of seconds the caller should wait before retrying.
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


async def request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    retries: int = 3,
    backoff: list[float] | None = None,
    **kwargs,
) -> httpx.Response:
    """Make an HTTP request with automatic retry on failure.

    Retries on HTTP status errors and transport errors with exponential backoff.

    Args:
        client: The httpx async client to use.
        method: HTTP method (e.g. "GET", "POST").
        url: The URL to request.
        retries: Number of attempts before giving up.
        backoff: List of wait times in seconds for each retry. Defaults to
            exponential backoff (1, 2, 4, ...).
        **kwargs: Extra keyword arguments passed to client.request().

    Returns:
        The successful httpx.Response.

    Raises:
        httpx.HTTPStatusError: If all retries are exhausted on HTTP errors.
        httpx.TransportError: If all retries are exhausted on transport errors.
    """
    if backoff is None:
        backoff = [float(2**i) for i in range(retries - 1)]

    for attempt in range(retries):
        try:
            resp = await client.request(method, url, **kwargs)
            resp.raise_for_status()
            return resp
        except (httpx.HTTPStatusError, httpx.TransportError) as e:
            if attempt == retries - 1:
                raise
            wait = backoff[attempt] if attempt < len(backoff) else backoff[-1]
            logger.warning("Retry %d/%d after error: %s", attempt + 1, retries, e)
            await asyncio.sleep(wait)
    raise RuntimeError("Unreachable")


class Collector(ABC):
    """Abstract base class for all job-listing collectors.

    Every concrete collector must set a ``name`` class attribute (a short
    string like ``"platsbanken"`` or ``"jooble"``) and implement the
    ``collect`` method, which fetches listings from a single external
    source based on the user's search profile.
    """

    name: str

    @abstractmethod
    async def collect(self, profile: Profile) -> CollectorResult:
        """Fetch job listings from this collector's external source.

        Args:
            profile: The user's search profile containing search terms,
                location, skills, and per-source configuration.

        Returns:
            A ``CollectorResult`` containing the fetched listings and any
            errors that occurred during collection.
        """
        ...


class ApifyCollectorMixin:
    """Mixin that provides shared Apify platform integration logic.

    LinkedIn and Indeed collectors delegate the actual web scraping to
    Apify actors (cloud-based scrapers).  This mixin encapsulates the
    three-step workflow that is common to both:

    1. Start an actor run via the Apify REST API.
    2. Poll the run status until it succeeds, fails, or times out.
    3. Fetch the resulting dataset items.

    Subclasses inherit these helper methods so they only need to build
    the actor-specific request body and map the raw results to
    ``RawListing`` objects.
    """

    POLL_INTERVAL: int = APIFY_POLL_INTERVAL_SECS
    TIMEOUT: int = APIFY_TIMEOUT_SECS
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
