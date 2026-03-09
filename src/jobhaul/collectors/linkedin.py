"""LinkedIn public job listings scraper using Playwright."""

from __future__ import annotations

import re

from jobhaul.collectors.base import Collector, detect_remote, handle_rate_limit
from jobhaul.collectors.registry import register
from jobhaul.collectors.stealth import (
    CircuitBreaker,
    RequestCounter,
    apply_stealth,
    create_stealth_context,
    random_delay,
)
from jobhaul.log import get_logger
from jobhaul.models import CollectorResult, Profile, RawListing

logger = get_logger(__name__)

SEARCH_URL = "https://www.linkedin.com/jobs/search/"
MAX_PAGES = 3
RESULTS_PER_PAGE = 25


@register
class LinkedInCollector(Collector):
    name = "linkedin"

    async def collect(self, profile: Profile) -> CollectorResult:
        source_config = profile.sources.get("linkedin")
        if not source_config or not source_config.enabled:
            return CollectorResult(source=self.name)

        try:
            from playwright.async_api import async_playwright
        except ImportError:
            return CollectorResult(
                source=self.name,
                errors=["Playwright not installed. Run: pip install playwright && playwright install chromium"],
            )

        scraping = profile.scraping
        listings: list[RawListing] = []
        errors: list[str] = []
        seen_ids: set[str] = set()
        circuit_breaker = CircuitBreaker()
        request_counter = RequestCounter(scraping.max_requests_per_run)
        rate_limit_hits = 0

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)

                for term in profile.search_terms:
                    if circuit_breaker.is_open:
                        msg = "LinkedIn: circuit breaker open, aborting remaining searches"
                        logger.warning(msg)
                        errors.append(msg)
                        break
                    if request_counter.limit_reached:
                        msg = "LinkedIn: request limit reached, stopping"
                        logger.warning(msg)
                        errors.append(msg)
                        break
                    if rate_limit_hits >= 3:
                        msg = "LinkedIn: rate limited 3 times, aborting to preserve quota"
                        logger.warning(msg)
                        errors.append(msg)
                        break

                    # New context per search term for session isolation
                    context = await create_stealth_context(browser, scraping)
                    page = await context.new_page()
                    await apply_stealth(page)

                    try:
                        term_listings = await self._search_term(
                            page, term, profile.location, seen_ids,
                            scraping, circuit_breaker, request_counter,
                        )
                        listings.extend(term_listings)
                    except Exception as e:
                        msg = f"LinkedIn error searching '{term}': {e}"
                        logger.warning(msg)
                        errors.append(msg)
                        circuit_breaker.record_failure()

                    await context.close()
                    await random_delay(scraping.delay_min, scraping.delay_max)

                await browser.close()

        except Exception as e:
            msg = f"LinkedIn browser error: {e}"
            logger.warning(msg)
            errors.append(msg)

        logger.info("LinkedIn: collected %d listings", len(listings))
        return CollectorResult(source=self.name, listings=listings, errors=errors)

    async def _search_term(
        self,
        page,
        term: str,
        location: str,
        seen_ids: set[str],
        scraping,
        circuit_breaker: CircuitBreaker,
        request_counter: RequestCounter,
    ) -> list[RawListing]:
        listings: list[RawListing] = []

        for page_num in range(MAX_PAGES):
            if circuit_breaker.is_open or request_counter.limit_reached:
                break

            start = page_num * RESULTS_PER_PAGE
            url = f"{SEARCH_URL}?keywords={term}&location={location}&start={start}"

            try:
                request_counter.increment()
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(2000)  # wait for JS rendering
                circuit_breaker.record_success()
            except Exception as e:
                logger.warning("LinkedIn page load failed: %s", e)
                circuit_breaker.record_failure()
                break

            job_cards = await page.query_selector_all(".base-card")
            if not job_cards:
                break

            for card in job_cards:
                try:
                    listing = await self._parse_card(card, seen_ids)
                    if listing:
                        listings.append(listing)
                except Exception as e:
                    logger.debug("Failed to parse LinkedIn card: %s", e)
                    continue

            if len(job_cards) < RESULTS_PER_PAGE:
                break

            await random_delay(scraping.delay_min, scraping.delay_max)

        return listings

    async def _parse_card(self, card, seen_ids: set[str]) -> RawListing | None:
        title_el = await card.query_selector(".base-search-card__title")
        company_el = await card.query_selector(".base-search-card__subtitle a")
        location_el = await card.query_selector(".job-search-card__location")
        link_el = await card.query_selector("a.base-card__full-link")
        date_el = await card.query_selector("time")

        title = (await title_el.inner_text()).strip() if title_el else None
        if not title:
            return None

        company = (await company_el.inner_text()).strip() if company_el else None
        location = (await location_el.inner_text()).strip() if location_el else None
        url = await link_el.get_attribute("href") if link_el else None
        posted = await date_el.get_attribute("datetime") if date_el else None

        # Extract job ID from URL
        ext_id = ""
        if url:
            match = re.search(r"/view/[^/]+-(\d+)", url)
            if match:
                ext_id = match.group(1)
            else:
                ext_id = url.split("?")[0].rstrip("/").split("/")[-1]

        if ext_id in seen_ids:
            return None
        if ext_id:
            seen_ids.add(ext_id)

        return RawListing(
            title=title,
            company=company,
            location=location,
            description=f"LinkedIn job posting: {title} at {company or 'Unknown'}",
            url=url,
            published_at=posted,
            is_remote=detect_remote(title, location or ""),
            source=self.name,
            external_id=ext_id or title,
            source_url=url,
        )
