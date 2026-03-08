"""RemoteOK JSON feed collector."""

from __future__ import annotations

import httpx

from jobhaul.collectors.base import Collector
from jobhaul.collectors.registry import register
from jobhaul.log import get_logger
from jobhaul.models import CollectorResult, Profile, RawListing

logger = get_logger(__name__)

API_URL = "https://remoteok.com/api"


@register
class RemoteOKCollector(Collector):
    name = "remoteok"

    async def collect(self, profile: Profile) -> CollectorResult:
        source_config = profile.sources.get("remoteok")
        if not source_config or not source_config.enabled:
            return CollectorResult(source=self.name)

        errors: list[str] = []
        skills_lower = {s.lower() for s in profile.skills}

        try:
            data = await self._fetch(profile)
        except Exception as e:
            msg = f"RemoteOK fetch error: {e}"
            logger.warning(msg)
            return CollectorResult(source=self.name, errors=[msg])

        # First element is metadata — skip it
        jobs = data[1:] if len(data) > 1 else []

        listings: list[RawListing] = []
        for job in jobs:
            tags = [t.lower() for t in job.get("tags", [])]
            if not any(skill in tags for skill in skills_lower):
                continue

            title = job.get("position", "")
            company = job.get("company")
            desc = job.get("description", "") or ""

            listings.append(
                RawListing(
                    title=title,
                    company=company,
                    location=job.get("location"),
                    description=desc[:5000],
                    url=job.get("url"),
                    published_at=job.get("date"),
                    is_remote=True,  # RemoteOK listings are inherently remote
                    employment_type=None,
                    source=self.name,
                    external_id=str(job.get("id", "")),
                    source_url=job.get("url"),
                )
            )

        logger.info("RemoteOK: collected %d listings", len(listings))
        return CollectorResult(source=self.name, listings=listings, errors=errors)

    async def _fetch(self, profile: Profile) -> list:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await self._request_with_retry(client)
            return resp.json()

    async def _request_with_retry(
        self, client: httpx.AsyncClient, retries: int = 3
    ) -> httpx.Response:
        import asyncio

        for attempt in range(retries):
            try:
                resp = await client.get(
                    API_URL, headers={"User-Agent": "Jobhaul/0.1 (job search aggregator)"}
                )
                resp.raise_for_status()
                return resp
            except (httpx.HTTPStatusError, httpx.TransportError) as e:
                if attempt == retries - 1:
                    raise
                wait = 2**attempt
                logger.warning("RemoteOK retry %d after error: %s", attempt + 1, e)
                await asyncio.sleep(wait)
        raise RuntimeError("Unreachable")
