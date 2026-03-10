"""Collector for LinkedIn job listings via the Apify scraping platform.

Delegates the actual LinkedIn scraping to an Apify actor, polls for
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

ACTOR_ID = "curious_coder~linkedin-jobs-scraper"
APIFY_RUN_URL = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs"


@register
class LinkedInCollector(ApifyCollectorMixin, Collector):
    """Scrapes LinkedIn job listings through the Apify cloud platform.

    Builds LinkedIn search URLs from the user's search terms and location,
    submits them to an Apify actor, waits for the scraping run to finish,
    and converts the raw results into ``RawListing`` objects.
    """

    name = "linkedin"

    async def collect(self, profile: Profile) -> CollectorResult:
        """Collect job listings from LinkedIn via an Apify actor run.

        Args:
            profile: The user's search profile.  The ``linkedin`` source
                config must be present, enabled, and include an
                ``apify_token``.

        Returns:
            A ``CollectorResult`` with de-duplicated listings and any errors.
        """
        source_config = profile.sources.get("linkedin")
        if not source_config or not source_config.enabled:
            return CollectorResult(source=self.name)

        token = source_config.apify_token
        if not token:
            return CollectorResult(
                source=self.name,
                errors=["LinkedIn Apify token not configured"],
            )

        listings: list[RawListing] = []
        errors: list[str] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                body = self._build_run_body(profile.search_terms, profile.location)
                run_id, dataset_id = await self._start_apify_run(
                    client, token, APIFY_RUN_URL, body,
                )
                await self._poll_until_done(client, token, run_id)
                items = await self._fetch_apify_results(client, token, dataset_id)
                listings = self._map_results(items)
            except Exception as e:
                msg = f"LinkedIn Apify error: {e}"
                logger.warning(msg)
                errors.append(msg)

        logger.info("LinkedIn: collected %d listings", len(listings))
        return CollectorResult(source=self.name, listings=listings, errors=errors)

    def _build_run_body(self, search_terms: list[str], location: str) -> dict:
        start_urls = []
        for term in search_terms:
            params = (
                f"keywords={term}&location={location}"
                "&f_TPR=r604800&f_JT=F&f_E=1,2"
            )
            start_urls.append(
                {"url": f"https://www.linkedin.com/jobs/search/?{params}"}
            )
        return {"urls": start_urls, "maxItems": APIFY_MAX_ITEMS}

    def _map_results(self, items: list[dict]) -> list[RawListing]:
        listings: list[RawListing] = []
        seen: set[str] = set()
        for item in items:
            job_url = item.get("link") or ""
            if not job_url:
                continue
            ext_id = item.get("id") or hashlib.sha256(
                job_url.encode()
            ).hexdigest()[:16]
            ext_id = str(ext_id)
            if ext_id in seen:
                continue
            seen.add(ext_id)

            title = item.get("title") or ""
            location = item.get("location") or ""
            description = item.get("descriptionText") or item.get("descriptionHtml") or ""
            listings.append(
                RawListing(
                    title=title,
                    company=item.get("companyName"),
                    location=location,
                    description=description or None,
                    url=job_url,
                    published_at=item.get("postedAt"),
                    is_remote=detect_remote(title, location),
                    source=self.name,
                    external_id=ext_id,
                    source_url=job_url,
                    seniority_level=item.get("seniorityLevel"),
                    employment_type=item.get("employmentType"),
                    salary=item.get("salary"),
                )
            )
        return listings
