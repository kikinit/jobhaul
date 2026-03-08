"""Tests for scraping stealth utilities (Issue #5)."""

from __future__ import annotations

import asyncio

import pytest

from jobhaul.collectors.stealth import (
    CircuitBreaker,
    RequestCounter,
    get_random_user_agent,
    get_random_viewport,
    random_delay,
)


class TestGetRandomUserAgent:
    def test_returns_string(self):
        ua = get_random_user_agent()
        assert isinstance(ua, str)
        assert len(ua) > 20

    def test_contains_browser_name(self):
        ua = get_random_user_agent()
        assert "Mozilla" in ua or "Chrome" in ua or "Firefox" in ua

    def test_randomness(self):
        """Multiple calls should return different values (statistically)."""
        agents = {get_random_user_agent() for _ in range(50)}
        assert len(agents) > 1


class TestGetRandomViewport:
    def test_returns_dict(self):
        vp = get_random_viewport()
        assert "width" in vp
        assert "height" in vp

    def test_within_range(self):
        for _ in range(20):
            vp = get_random_viewport()
            assert 1200 <= vp["width"] <= 1920
            assert 800 <= vp["height"] <= 1080


class TestRandomDelay:
    @pytest.mark.asyncio
    async def test_delay_within_range(self):
        import time

        start = time.monotonic()
        await random_delay(0.01, 0.05)
        elapsed = time.monotonic() - start
        assert 0.01 <= elapsed < 0.2  # Allow some slack


class TestCircuitBreaker:
    def test_closed_initially(self):
        cb = CircuitBreaker(max_failures=3)
        assert cb.is_open is False

    def test_opens_after_threshold(self):
        cb = CircuitBreaker(max_failures=3)
        cb.record_failure()
        cb.record_failure()
        assert cb.is_open is False
        cb.record_failure()
        assert cb.is_open is True

    def test_resets_on_success(self):
        cb = CircuitBreaker(max_failures=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        assert cb.is_open is False
        # Need 3 consecutive failures again
        cb.record_failure()
        assert cb.is_open is False


class TestRequestCounter:
    def test_starts_at_zero(self):
        rc = RequestCounter(max_requests=5)
        assert rc.limit_reached is False

    def test_reaches_limit(self):
        rc = RequestCounter(max_requests=3)
        rc.increment()
        rc.increment()
        assert rc.limit_reached is False
        rc.increment()
        assert rc.limit_reached is True

    def test_default_limit(self):
        rc = RequestCounter()
        assert rc.max_requests == 50


class TestCreateStealthContext:
    @pytest.mark.asyncio
    async def test_creates_context_without_stealth_package(self):
        """Test context creation when playwright-stealth is not installed."""
        from unittest.mock import AsyncMock, MagicMock

        from jobhaul.collectors.stealth import create_stealth_context
        from jobhaul.models import ScrapingConfig

        browser = AsyncMock()
        mock_context = AsyncMock()
        browser.new_context.return_value = mock_context

        config = ScrapingConfig()
        context = await create_stealth_context(browser, config)

        assert context == mock_context
        browser.new_context.assert_called_once()

        # Check that user_agent and viewport were passed
        call_kwargs = browser.new_context.call_args.kwargs
        assert "user_agent" in call_kwargs
        assert "viewport" in call_kwargs
        assert 1200 <= call_kwargs["viewport"]["width"] <= 1920

    @pytest.mark.asyncio
    async def test_creates_context_with_proxy(self):
        from unittest.mock import AsyncMock

        from jobhaul.collectors.stealth import create_stealth_context
        from jobhaul.models import ScrapingConfig

        browser = AsyncMock()
        mock_context = AsyncMock()
        browser.new_context.return_value = mock_context

        config = ScrapingConfig(proxy="socks5://localhost:1080")
        await create_stealth_context(browser, config)

        call_kwargs = browser.new_context.call_args.kwargs
        assert call_kwargs["proxy"] == {"server": "socks5://localhost:1080"}
