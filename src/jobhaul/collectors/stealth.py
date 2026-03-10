"""Stealth utilities that help browser-based scrapers avoid bot detection.

Provides randomized user agents, viewport sizes, and request delays, as
well as Playwright page patches that mask common automation fingerprints.
"""

from __future__ import annotations

import asyncio
import random

from jobhaul.log import get_logger

logger = get_logger(__name__)

# Pool of recent real Chrome/Firefox user agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]


def get_random_user_agent() -> str:
    """Return a random user agent string from the pool."""
    return random.choice(USER_AGENTS)


# Alias for the interface requested in Issue #5
random_user_agent = get_random_user_agent


def get_random_viewport() -> dict:
    """Return a random viewport size within reasonable bounds."""
    return {
        "width": random.randint(1200, 1920),
        "height": random.randint(800, 1080),
    }


async def random_delay(min_s: float = 2.0, max_s: float = 5.0) -> None:
    """Sleep for a random duration between min_s and max_s seconds."""
    delay = random.uniform(min_s, max_s)
    await asyncio.sleep(delay)


_MANUAL_STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
window.chrome = {runtime: {}};
"""


async def apply_stealth(page) -> None:
    """Apply stealth patches to a Playwright page.

    Uses playwright-stealth if available, otherwise falls back to manual
    init-script patches (removing navigator.webdriver, etc.).
    """
    try:
        from playwright_stealth import stealth_async

        await stealth_async(page)
        logger.debug("Applied playwright-stealth patches to page")
    except ImportError:
        try:
            await page.add_init_script(_MANUAL_STEALTH_JS)
            logger.debug("Applied manual stealth patches to page")
        except Exception as e:
            logger.warning("Failed to apply manual stealth patches: %s", e)
    except Exception as e:
        logger.warning("Failed to apply playwright-stealth patches: %s", e)


async def create_stealth_context(browser, scraping_config=None):
    """Create a browser context with stealth settings applied.

    Args:
        browser: Playwright browser instance
        scraping_config: Optional ScrapingConfig with proxy settings
    """
    ua = get_random_user_agent()
    viewport = get_random_viewport()

    context_kwargs = {
        "user_agent": ua,
        "viewport": viewport,
    }

    if scraping_config and scraping_config.proxy:
        context_kwargs["proxy"] = {"server": scraping_config.proxy}

    context = await browser.new_context(**context_kwargs)

    # Apply playwright-stealth patches if available
    try:
        from playwright_stealth import stealth_async

        await stealth_async(context)
        logger.debug("Applied playwright-stealth patches")
    except ImportError:
        logger.debug("playwright-stealth not installed, skipping stealth patches")

    return context
