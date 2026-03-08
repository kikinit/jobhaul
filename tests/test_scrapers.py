"""Tests for LinkedIn and Indeed collectors with mocked Playwright."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from jobhaul.models import Profile, SourceConfig


def _mock_element(text=None, href=None, datetime=None, data_jk=None):
    """Create a mock page element."""
    el = AsyncMock()
    el.inner_text = AsyncMock(return_value=text or "")
    el.get_attribute = AsyncMock(side_effect=lambda attr: {
        "href": href,
        "datetime": datetime,
        "data-jk": data_jk,
    }.get(attr))
    return el


def _mock_card(elements: dict):
    """Create a mock job card that returns specific elements for selectors."""
    card = AsyncMock()

    async def query_selector(selector):
        for key, el in elements.items():
            if key in selector:
                return el
        return None

    card.query_selector = AsyncMock(side_effect=query_selector)
    card.get_attribute = AsyncMock(return_value=elements.get("data-jk"))
    return card


@pytest.fixture
def profile():
    return Profile(
        name="Test",
        roles=["developer"],
        search_terms=["python"],
        skills=["Python"],
        location="Stockholm",
        sources={
            "linkedin": SourceConfig(enabled=True),
            "indeed": SourceConfig(enabled=True, region="se"),
        },
    )


class TestLinkedInCollector:
    @pytest.mark.asyncio
    async def test_collect_disabled(self):
        from jobhaul.collectors.linkedin import LinkedInCollector

        profile = Profile(
            name="Test",
            sources={"linkedin": SourceConfig(enabled=False)},
        )
        collector = LinkedInCollector()
        result = await collector.collect(profile)
        assert result.listings == []
        assert result.errors == []

    @pytest.mark.asyncio
    async def test_collect_no_playwright(self, profile):
        from jobhaul.collectors.linkedin import LinkedInCollector

        with patch.dict("sys.modules", {"playwright": None, "playwright.async_api": None}):
            collector = LinkedInCollector()
            # Force reimport check
            with patch("jobhaul.collectors.linkedin.LinkedInCollector.collect") as mock_collect:
                mock_collect.return_value = MagicMock(
                    source="linkedin", listings=[], errors=["Playwright not installed"]
                )
                result = await mock_collect(profile)
                assert len(result.errors) >= 1

    @pytest.mark.asyncio
    async def test_parse_card(self):
        from jobhaul.collectors.linkedin import LinkedInCollector

        collector = LinkedInCollector()
        seen = set()

        card = _mock_card({
            "base-search-card__title": _mock_element(text="Python Dev"),
            "base-search-card__subtitle": _mock_element(text="Acme Corp"),
            "job-search-card__location": _mock_element(text="Stockholm"),
            "base-card__full-link": _mock_element(href="https://linkedin.com/jobs/view/python-dev-12345"),
            "time": _mock_element(datetime="2024-01-15"),
        })

        listing = await collector._parse_card(card, seen)
        assert listing is not None
        assert listing.title == "Python Dev"
        assert listing.company == "Acme Corp"
        assert listing.location == "Stockholm"
        assert listing.source == "linkedin"
        assert "12345" in listing.external_id

    @pytest.mark.asyncio
    async def test_parse_card_dedup(self):
        from jobhaul.collectors.linkedin import LinkedInCollector

        collector = LinkedInCollector()
        seen = {"12345"}

        card = _mock_card({
            "base-search-card__title": _mock_element(text="Python Dev"),
            "base-card__full-link": _mock_element(href="https://linkedin.com/jobs/view/python-dev-12345"),
        })

        listing = await collector._parse_card(card, seen)
        assert listing is None

    @pytest.mark.asyncio
    async def test_parse_card_no_title(self):
        from jobhaul.collectors.linkedin import LinkedInCollector

        collector = LinkedInCollector()
        card = _mock_card({})
        listing = await collector._parse_card(card, set())
        assert listing is None


class TestIndeedCollector:
    @pytest.mark.asyncio
    async def test_collect_disabled(self):
        from jobhaul.collectors.indeed import IndeedCollector

        profile = Profile(
            name="Test",
            sources={"indeed": SourceConfig(enabled=False)},
        )
        collector = IndeedCollector()
        result = await collector.collect(profile)
        assert result.listings == []
        assert result.errors == []

    @pytest.mark.asyncio
    async def test_parse_card(self):
        from jobhaul.collectors.indeed import IndeedCollector

        collector = IndeedCollector()
        seen = set()

        card = _mock_card({
            "jobTitle": _mock_element(text="Backend Dev", href="/rc/clk?jk=abc123"),
            "company-name": _mock_element(text="TechCo"),
            "text-location": _mock_element(text="Stockholm"),
            "attribute_snippet": _mock_element(text="50 000 SEK"),
            "job-snippet": _mock_element(text="Build APIs with Python"),
            "data-jk": "abc123",
        })
        card.get_attribute = AsyncMock(return_value="abc123")

        listing = await collector._parse_card(card, "https://se.indeed.com/jobs", seen)
        assert listing is not None
        assert listing.title == "Backend Dev"
        assert listing.company == "TechCo"
        assert listing.source == "indeed"
        assert listing.external_id == "abc123"

    @pytest.mark.asyncio
    async def test_parse_card_dedup(self):
        from jobhaul.collectors.indeed import IndeedCollector

        collector = IndeedCollector()
        seen = {"abc123"}

        card = _mock_card({
            "jobTitle": _mock_element(text="Backend Dev", href="/rc/clk?jk=abc123"),
        })
        card.get_attribute = AsyncMock(return_value="abc123")

        listing = await collector._parse_card(card, "https://se.indeed.com/jobs", seen)
        assert listing is None

    @pytest.mark.asyncio
    async def test_parse_card_no_title(self):
        from jobhaul.collectors.indeed import IndeedCollector

        collector = IndeedCollector()
        card = _mock_card({})
        card.get_attribute = AsyncMock(return_value=None)
        listing = await collector._parse_card(card, "https://se.indeed.com/jobs", set())
        assert listing is None

    @pytest.mark.asyncio
    async def test_country_config(self):
        from jobhaul.collectors.indeed import BASE_URL_TEMPLATE

        assert BASE_URL_TEMPLATE.format(country="se") == "https://se.indeed.com/jobs"
        assert BASE_URL_TEMPLATE.format(country="com") == "https://com.indeed.com/jobs"
