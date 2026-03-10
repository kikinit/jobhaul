"""Collector for Indeed job listings via the Apify scraping platform.

Delegates the actual Indeed scraping to an Apify actor, polls for
completion, and maps the returned dataset items to ``RawListing`` objects.
Requires an Apify API token configured in the user's profile.
"""

from __future__ import annotations

import hashlib

import httpx

from jobhaul.collectors.base import ApifyCollectorMixin, Collector, detect_remote
from jobhaul.collectors.registry import register
from jobhaul.constants import APIFY_MAX_ITEMS
from jobhaul.log import get_logger
from jobhaul.models import CollectorResult, Profile, RawListing

logger = get_logger(__name__)

ACTOR_ID = "misceres~indeed-scraper"
APIFY_RUN_URL = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs"


@register
class IndeedCollector(ApifyCollectorMixin, Collector):
    """Scrapes Indeed job listings through the Apify cloud platform.

    Submits one Apify actor run per search term (with position, location,
    and country), waits for each to complete, and converts the raw results
    into ``RawListing`` objects.
    """

    name = "indeed"

    async def collect(self, profile: Profile) -> CollectorResult:
        """Collect job listings from Indeed via Apify actor runs.

        Args:
            profile: The user's search profile.  The ``indeed`` source
                config must be present, enabled, and include an
                ``apify_token``.

        Returns:
            A ``CollectorResult`` with de-duplicated listings and any errors.
        """
        source_config = profile.sources.get("indeed")
        if not source_config or not source_config.enabled:
            return CollectorResult(source=self.name)

        token = source_config.apify_token
        if not token:
            return CollectorResult(
                source=self.name,
                errors=["Indeed Apify token not configured"],
            )

        all_items: list[dict] = []
        errors: list[str] = []
        country = (source_config.region or "SE").upper()

        async with httpx.AsyncClient(timeout=30.0) as client:
            for term in profile.search_terms:
                try:
                    body = {
                        "position": term,
                        "location": profile.location,
                        "country": country,
                        "maxItems": APIFY_MAX_ITEMS,
                    }
                    run_id, dataset_id = await self._start_apify_run(
                        client, token, APIFY_RUN_URL, body,
                    )
                    await self._poll_until_done(client, token, run_id)
                    items = await self._fetch_apify_results(client, token, dataset_id)
                    all_items.extend(items)
                except Exception as e:
                    msg = f"Indeed Apify error: {e}"
                    logger.warning(msg)
                    errors.append(msg)

        listings = self._map_results(all_items)
        logger.info("Indeed: collected %d listings", len(listings))
        return CollectorResult(source=self.name, listings=listings, errors=errors)

    def _map_results(self, items: list[dict]) -> list[RawListing]:
        listings: list[RawListing] = []
        seen: set[str] = set()
        for item in items:
            url = item.get("link") or ""
            if not url:
                continue
            ext_id = hashlib.sha256(url.encode()).hexdigest()[:16]
            if ext_id in seen:
                continue
            seen.add(ext_id)

            title = item.get("positionName") or ""
            location = item.get("formattedLocation") or ""
            listings.append(
                RawListing(
                    title=title,
                    company=item.get("company"),
                    location=location,
                    description=item.get("description") or None,
                    url=url,
                    published_at=item.get("pubDate"),
                    salary=item.get("salary"),
                    employment_type=(item.get("jobTypes") or [None])[0],
                    is_remote=detect_remote(title, location),
                    source=self.name,
                    external_id=ext_id,
                    source_url=url,
                )
            )
        return listings
