"""Collector for Jooble, an international job-search aggregator.

Uses the Jooble REST API (requires an API key configured in the user's
profile) to search for listings by keyword and location.  Results are
de-duplicated by external ID within each scan.
"""

from __future__ import annotations

import asyncio

import httpx

from jobhaul.collectors.base import Collector, detect_remote, request_with_retry
from jobhaul.collectors.registry import register
from jobhaul.constants import MAX_DESCRIPTION_CHARS
from jobhaul.log import get_logger
from jobhaul.models import CollectorResult, Profile, RawListing

logger = get_logger(__name__)

API_URL_TEMPLATE = "https://jooble.org/api/{api_key}"


@register
class JoobleCollector(Collector):
    """Fetches job listings from the Jooble REST API.

    Sends a POST request per search term with the keyword and location,
    then de-duplicates results by external ID.  Requires a Jooble API key
    in the source configuration.
    """

    name = "jooble"

    async def collect(self, profile: Profile) -> CollectorResult:
        """Collect job listings from Jooble for all search terms.

        Args:
            profile: The user's search profile.  The ``jooble`` source
                config must be present, enabled, and have a valid ``api_key``.

        Returns:
            A ``CollectorResult`` with de-duplicated listings and any errors.
        """
        source_config = profile.sources.get("jooble")
        if not source_config or not source_config.enabled:
            return CollectorResult(source=self.name)

        api_key = source_config.api_key
        if not api_key:
            return CollectorResult(
                source=self.name, errors=["Jooble API key not configured"]
            )

        url = API_URL_TEMPLATE.format(api_key=api_key)
        listings: list[RawListing] = []
        errors: list[str] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            for term in profile.search_terms:
                try:
                    term_listings = await self._search_term(
                        client, url, term, profile.location
                    )
                    listings.extend(term_listings)
                except Exception as e:
                    msg = f"Jooble error searching '{term}': {e}"
                    logger.warning(msg)
                    errors.append(msg)
                await asyncio.sleep(0.5)

        # Deduplicate by external_id within this batch
        seen: set[str] = set()
        unique: list[RawListing] = []
        for listing in listings:
            if listing.external_id not in seen:
                seen.add(listing.external_id)
                unique.append(listing)

        logger.info("Jooble: collected %d listings", len(unique))
        return CollectorResult(source=self.name, listings=unique, errors=errors)

    async def _search_term(
        self,
        client: httpx.AsyncClient,
        url: str,
        term: str,
        location: str,
    ) -> list[RawListing]:
        body = {"keywords": term, "location": location}
        resp = await request_with_retry(client, "POST", url, json=body)
        data = resp.json()
        jobs = data.get("jobs", [])

        listings: list[RawListing] = []
        for job in jobs:
            title = job.get("title", "")
            desc = job.get("snippet", "") or ""

            listings.append(
                RawListing(
                    title=title,
                    company=job.get("company"),
                    location=job.get("location"),
                    description=desc[:MAX_DESCRIPTION_CHARS],
                    url=job.get("link"),
                    published_at=job.get("updated"),
                    is_remote=detect_remote(title, desc),
                    employment_type=job.get("type"),
                    source=self.name,
                    external_id=str(job.get("id", "")),
                    source_url=job.get("link"),
                )
            )
        return listings

