"""Indeed job listings scraper using Playwright."""

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

BASE_URL_TEMPLATE = "https://{country}.indeed.com/jobs"
MAX_PAGES = 3
RESULTS_PER_PAGE = 10  # Indeed shows ~15 per page, pagination uses increments of 10


@register
class IndeedCollector(Collector):
    name = "indeed"

    async def collect(self, profile: Profile) -> CollectorResult:
        source_config = profile.sources.get("indeed")
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
        country = source_config.region or "se"
        base_url = BASE_URL_TEMPLATE.format(country=country)
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
                        msg = "Indeed: circuit breaker open, aborting remaining searches"
                        logger.warning(msg)
                        errors.append(msg)
                        break
                    if request_counter.limit_reached:
                        msg = "Indeed: request limit reached, stopping"
                        logger.warning(msg)
                        errors.append(msg)
                        break
                    if rate_limit_hits >= 3:
                        msg = "Indeed: rate limited 3 times, aborting to preserve quota"
                        logger.warning(msg)
                        errors.append(msg)
                        break

                    # New context per search term for session isolation
                    context = await create_stealth_context(browser, scraping)
                    page = await context.new_page()
                    await apply_stealth(page)

                    try:
                        term_listings = await self._search_term(
                            page, base_url, term, profile.location, seen_ids,
                            scraping, circuit_breaker, request_counter,
                        )
                        listings.extend(term_listings)
                    except Exception as e:
                        msg = f"Indeed error searching '{term}': {e}"
                        logger.warning(msg)
                        errors.append(msg)
                        circuit_breaker.record_failure()

                    await context.close()
                    await random_delay(scraping.delay_min, scraping.delay_max)

                await browser.close()

        except Exception as e:
            msg = f"Indeed browser error: {e}"
            logger.warning(msg)
            errors.append(msg)

        logger.info("Indeed: collected %d listings", len(listings))
        return CollectorResult(source=self.name, listings=listings, errors=errors)

    async def _search_term(
        self,
        page,
        base_url: str,
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
            url = f"{base_url}?q={term}&l={location}&start={start}"

            try:
                request_counter.increment()
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(2000)
                circuit_breaker.record_success()
            except Exception as e:
                logger.warning("Indeed page load failed: %s", e)
                circuit_breaker.record_failure()
                break

            job_cards = await page.query_selector_all(".job_seen_beacon, .jobsearch-ResultsList .result")
            if not job_cards:
                # Try alternative selector
                job_cards = await page.query_selector_all("[data-jk]")

            if not job_cards:
                break

            for card in job_cards:
                try:
                    listing = await self._parse_card(card, base_url, seen_ids)
                    if listing:
                        listings.append(listing)
                except Exception as e:
                    logger.debug("Failed to parse Indeed card: %s", e)
                    continue

            if len(job_cards) < RESULTS_PER_PAGE:
                break

            await random_delay(scraping.delay_min, scraping.delay_max)

        return listings

    async def _parse_card(self, card, base_url: str, seen_ids: set[str]) -> RawListing | None:
        # Try to get the job key from data attribute
        ext_id = await card.get_attribute("data-jk") or ""

        title_el = await card.query_selector("h2.jobTitle a, .jobTitle > a, a[data-jk]")
        company_el = await card.query_selector("[data-testid='company-name'], .companyName, .company")
        location_el = await card.query_selector("[data-testid='text-location'], .companyLocation, .location")
        salary_el = await card.query_selector("[data-testid='attribute_snippet_testid'], .salary-snippet-container")
        snippet_el = await card.query_selector(".job-snippet, .underShelfFooter, [class*='job-snippet']")

        title = (await title_el.inner_text()).strip() if title_el else None
        if not title:
            return None

        # Get URL from the title link
        url = None
        if title_el:
            href = await title_el.get_attribute("href")
            if href:
                if href.startswith("/"):
                    # Build absolute URL from the base domain
                    domain = "/".join(base_url.split("/")[:3])
                    url = domain + href
                else:
                    url = href

        if not ext_id and title_el:
            ext_id = await title_el.get_attribute("data-jk") or ""

        if not ext_id and url:
            match = re.search(r"jk=([a-f0-9]+)", url)
            if match:
                ext_id = match.group(1)

        if ext_id in seen_ids:
            return None
        if ext_id:
            seen_ids.add(ext_id)

        company = (await company_el.inner_text()).strip() if company_el else None
        location = (await location_el.inner_text()).strip() if location_el else None
        salary = (await salary_el.inner_text()).strip() if salary_el else None
        snippet = (await snippet_el.inner_text()).strip() if snippet_el else ""

        desc_parts = []
        if snippet:
            desc_parts.append(snippet)
        if salary:
            desc_parts.append(f"Salary: {salary}")
        description = "\n".join(desc_parts) if desc_parts else f"Indeed job: {title}"

        return RawListing(
            title=title,
            company=company,
            location=location,
            description=description[:5000],
            url=url,
            is_remote=detect_remote(title, f"{location or ''} {snippet}"),
            source=self.name,
            external_id=ext_id or title,
            source_url=url,
        )
