"""Arbetsförmedlingen JobStream API collector."""

from __future__ import annotations

import asyncio

import httpx

from jobhaul.collectors.base import Collector, detect_remote
from jobhaul.collectors.registry import register
from jobhaul.log import get_logger
from jobhaul.models import CollectorResult, Profile, RawListing

logger = get_logger(__name__)

API_URL = "https://jobsearch.api.jobtechdev.se/search"
PAGE_SIZE = 100
MAX_PAGES = 5
RATE_LIMIT_DELAY = 0.2  # 200ms between requests


@register
class PlatsbankenCollector(Collector):
    name = "platsbanken"

    async def collect(self, profile: Profile) -> CollectorResult:
        source_config = profile.sources.get("platsbanken")
        if not source_config or not source_config.enabled:
            return CollectorResult(source=self.name)

        listings: list[RawListing] = []
        errors: list[str] = []
        seen_ids: set[str] = set()

        async with httpx.AsyncClient(timeout=30.0) as client:
            for term in profile.search_terms:
                try:
                    term_listings = await self._search_term(
                        client, term, source_config.region, seen_ids
                    )
                    listings.extend(term_listings)
                except Exception as e:
                    msg = f"Error searching '{term}': {e}"
                    logger.warning(msg)
                    errors.append(msg)
                await asyncio.sleep(RATE_LIMIT_DELAY)

        logger.info("Platsbanken: collected %d listings", len(listings))
        return CollectorResult(source=self.name, listings=listings, errors=errors)

    async def _search_term(
        self,
        client: httpx.AsyncClient,
        term: str,
        region: str,
        seen_ids: set[str],
    ) -> list[RawListing]:
        listings: list[RawListing] = []

        for page in range(MAX_PAGES):
            params: dict = {
                "q": term,
                "offset": page * PAGE_SIZE,
                "limit": PAGE_SIZE,
            }
            if region:
                params["region"] = region

            resp = await self._request_with_retry(client, params)
            data = resp.json()
            hits = data.get("hits", [])

            if not hits:
                break

            for hit in hits:
                ext_id = str(hit.get("id", ""))
                if ext_id in seen_ids:
                    continue
                seen_ids.add(ext_id)

                title = hit.get("headline", "")
                desc = hit.get("description", {}).get("text", "") or ""
                company_name = hit.get("employer", {}).get("name")
                workplace = hit.get("workplace_address", {})
                location = workplace.get("municipality") or workplace.get("region")

                listings.append(
                    RawListing(
                        title=title,
                        company=company_name,
                        location=location,
                        description=desc[:5000],
                        url=hit.get("webpage_url"),
                        published_at=hit.get("publication_date"),
                        is_remote=detect_remote(title, desc),
                        employment_type=hit.get("employment_type", {}).get("label"),
                        source=self.name,
                        external_id=ext_id,
                        source_url=hit.get("webpage_url"),
                    )
                )

            if len(hits) < PAGE_SIZE:
                break

            await asyncio.sleep(RATE_LIMIT_DELAY)

        return listings

    async def _request_with_retry(
        self, client: httpx.AsyncClient, params: dict, retries: int = 3
    ) -> httpx.Response:
        for attempt in range(retries):
            try:
                resp = await client.get(API_URL, params=params)
                resp.raise_for_status()
                return resp
            except (httpx.HTTPStatusError, httpx.TransportError) as e:
                if attempt == retries - 1:
                    raise
                wait = 2**attempt
                logger.warning("Platsbanken retry %d after error: %s", attempt + 1, e)
                await asyncio.sleep(wait)
        raise RuntimeError("Unreachable")
