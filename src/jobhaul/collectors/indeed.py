"""Indeed job listings collector using Apify API."""

from __future__ import annotations

import asyncio
import hashlib

import httpx

from jobhaul.collectors.base import Collector, detect_remote
from jobhaul.collectors.registry import register
from jobhaul.log import get_logger
from jobhaul.models import CollectorResult, Profile, RawListing

logger = get_logger(__name__)

ACTOR_ID = "misceres~indeed-scraper"
APIFY_RUN_URL = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs"
APIFY_DATASET_URL = "https://api.apify.com/v2/datasets"
POLL_INTERVAL = 10
TIMEOUT = 300  # 5 minutes


@register
class IndeedCollector(Collector):
    name = "indeed"

    async def collect(self, profile: Profile) -> CollectorResult:
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
                    run_id, dataset_id = await self._start_run(
                        client, token, term, profile.location, country
                    )
                    await self._poll_until_done(client, token, run_id)
                    items = await self._fetch_results(client, token, dataset_id)
                    all_items.extend(items)
                except Exception as e:
                    msg = f"Indeed Apify error: {e}"
                    logger.warning(msg)
                    errors.append(msg)

        listings = self._map_results(all_items)
        logger.info("Indeed: collected %d listings", len(listings))
        return CollectorResult(source=self.name, listings=listings, errors=errors)

    async def _start_run(
        self,
        client: httpx.AsyncClient,
        token: str,
        position: str,
        location: str,
        country: str,
    ) -> tuple[str, str]:
        body = {
            "position": position,
            "location": location,
            "country": country,
            "maxItems": 50,
        }
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
