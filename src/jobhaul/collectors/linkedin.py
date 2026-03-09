"""LinkedIn job listings collector using Apify API."""

from __future__ import annotations

import asyncio
import hashlib

import httpx

from jobhaul.collectors.base import Collector, detect_remote
from jobhaul.collectors.registry import register
from jobhaul.log import get_logger
from jobhaul.models import CollectorResult, Profile, RawListing

logger = get_logger(__name__)

ACTOR_ID = "curious_coder~linkedin-jobs-scraper"
APIFY_RUN_URL = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs"
APIFY_DATASET_URL = "https://api.apify.com/v2/datasets"
POLL_INTERVAL = 10
TIMEOUT = 300  # 5 minutes


@register
class LinkedInCollector(Collector):
    name = "linkedin"

    async def collect(self, profile: Profile) -> CollectorResult:
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
                run_id, dataset_id = await self._start_run(
                    client, token, profile.search_terms, profile.location
                )
                await self._poll_until_done(client, token, run_id)
                items = await self._fetch_results(client, token, dataset_id)
                listings = self._map_results(items)
            except Exception as e:
                msg = f"LinkedIn Apify error: {e}"
                logger.warning(msg)
                errors.append(msg)

        logger.info("LinkedIn: collected %d listings", len(listings))
        return CollectorResult(source=self.name, listings=listings, errors=errors)

    async def _start_run(
        self,
        client: httpx.AsyncClient,
        token: str,
        search_terms: list[str],
        location: str,
    ) -> tuple[str, str]:
        start_urls = []
        for term in search_terms:
            params = (
                f"keywords={term}&location={location}"
                "&f_TPR=r604800&f_JT=F&f_E=1,2"
            )
            start_urls.append(
                {"url": f"https://www.linkedin.com/jobs/search/?{params}"}
            )
        body = {"startUrls": start_urls, "maxItems": 50}
        resp = await client.post(
            f"{APIFY_RUN_URL}?token={token}", json=body
        )
        resp.raise_for_status()
        data = resp.json()["data"]
        return data["id"], data["defaultDatasetId"]

    async def _poll_until_done(
        self, client: httpx.AsyncClient, token: str, run_id: str
    ) -> None:
        url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={token}"
        elapsed = 0
        while elapsed < TIMEOUT:
            resp = await client.get(url)
            resp.raise_for_status()
            status = resp.json()["data"]["status"]
            if status == "SUCCEEDED":
                return
            if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                raise RuntimeError(f"Apify actor run {status}")
            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
        raise TimeoutError("Apify actor run timed out after 5 minutes")

    async def _fetch_results(
        self, client: httpx.AsyncClient, token: str, dataset_id: str
    ) -> list[dict]:
        url = f"{APIFY_DATASET_URL}/{dataset_id}/items?token={token}"
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()

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
            listings.append(
                RawListing(
                    title=title,
                    company=item.get("companyName"),
                    location=location,
                    description=item.get("descriptionText"),
                    url=job_url,
                    published_at=item.get("publishedAt"),
                    is_remote=detect_remote(title, location),
                    source=self.name,
                    external_id=ext_id,
                    source_url=job_url,
                )
            )
        return listings
