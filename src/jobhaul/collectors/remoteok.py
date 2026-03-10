"""Collector for RemoteOK, a remote-only job board.

Fetches the public JSON feed at remoteok.com/api and filters listings to
those whose tags match the user's configured skills.  All listings from
this source are marked as remote since RemoteOK only lists remote positions.
"""

from __future__ import annotations

import httpx

from jobhaul.collectors.base import Collector, request_with_retry
from jobhaul.collectors.registry import register
from jobhaul.constants import MAX_DESCRIPTION_CHARS
from jobhaul.log import get_logger
from jobhaul.models import CollectorResult, Profile, RawListing

logger = get_logger(__name__)

API_URL = "https://remoteok.com/api"


@register
class RemoteOKCollector(Collector):
    """Fetches remote job listings from the RemoteOK public JSON feed.

    Downloads the full feed once, then filters it to listings whose tags
    overlap with the user's skills list.  Every listing is marked as
    remote because RemoteOK is a remote-only job board.
    """

    name = "remoteok"

    async def collect(self, profile: Profile) -> CollectorResult:
        """Collect job listings from RemoteOK, filtered by the user's skills.

        Args:
            profile: The user's search profile.  The ``remoteok`` source
                config must be present and enabled.  Listings are filtered
                against ``profile.skills``.

        Returns:
            A ``CollectorResult`` with matched listings and any errors.
        """
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
                    description=desc[:MAX_DESCRIPTION_CHARS],
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
            resp = await request_with_retry(
                client, "GET", API_URL,
                headers={"User-Agent": "Jobhaul/0.1 (job search aggregator)"},
            )
            return resp.json()
