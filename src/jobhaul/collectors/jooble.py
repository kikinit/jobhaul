"""Jooble API collector."""

from __future__ import annotations

import asyncio

import httpx

from jobhaul.collectors.base import Collector, detect_remote, handle_rate_limit
from jobhaul.collectors.registry import register
from jobhaul.log import get_logger
from jobhaul.models import CollectorResult, Profile, RawListing

logger = get_logger(__name__)

API_URL_TEMPLATE = "https://jooble.org/api/{api_key}"


@register
class JoobleCollector(Collector):
    name = "jooble"

    async def collect(self, profile: Profile) -> CollectorResult:
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
        resp = await self._request_with_retry(client, url, body)
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
                    description=desc[:5000],
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

    async def _request_with_retry(
        self, client: httpx.AsyncClient, url: str, body: dict, retries: int = 3
    ) -> httpx.Response:
        rate_limit_hits = 0
        attempt = 0
        while attempt < retries:
            try:
                resp = await client.post(url, json=body)
                if resp.status_code == 429:
                    rate_limit_hits += 1
                    if rate_limit_hits >= 3:
                        raise RuntimeError(
                            "Jooble: rate limited 3 times, aborting to preserve quota"
                        )
                    wait = handle_rate_limit(resp, "Jooble")
                    await asyncio.sleep(wait)
                    continue  # Don't count toward normal retries
                resp.raise_for_status()
                return resp
            except httpx.HTTPStatusError as e:
                if attempt == retries - 1:
                    raise
                wait = 2**attempt
                logger.warning("Jooble retry %d after error: %s", attempt + 1, e)
                await asyncio.sleep(wait)
                attempt += 1
            except httpx.TransportError as e:
                if attempt == retries - 1:
                    raise
                wait = 2**attempt
                logger.warning("Jooble retry %d after error: %s", attempt + 1, e)
                await asyncio.sleep(wait)
                attempt += 1
        raise RuntimeError("Unreachable")
