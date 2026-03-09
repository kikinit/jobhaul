"""Tests for LinkedIn and Indeed Apify collectors — unit tests for mapping logic."""

from __future__ import annotations

import hashlib

import pytest

from jobhaul.models import Profile, SourceConfig


@pytest.fixture
def profile():
    return Profile(
        name="Test",
        roles=["developer"],
        search_terms=["python"],
        skills=["Python"],
        location="Stockholm",
        sources={
            "linkedin": SourceConfig(enabled=True, apify_token="test-token"),
            "indeed": SourceConfig(enabled=True, apify_token="test-token", region="SE"),
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
    async def test_collect_no_token(self):
        from jobhaul.collectors.linkedin import LinkedInCollector

        profile = Profile(
            name="Test",
            search_terms=["python"],
            sources={"linkedin": SourceConfig(enabled=True, apify_token="")},
        )
        collector = LinkedInCollector()
        result = await collector.collect(profile)
        assert len(result.errors) == 1
        assert "token" in result.errors[0].lower()

    def test_map_results(self):
        from jobhaul.collectors.linkedin import LinkedInCollector

        collector = LinkedInCollector()
        items = [
            {
                "title": "Python Dev",
                "companyName": "Acme Corp",
                "location": "Stockholm",
                "description": "Build stuff",
                "jobUrl": "https://linkedin.com/jobs/view/123",
                "publishedAt": "2024-01-15",
            },
        ]
        listings = collector._map_results(items)
        assert len(listings) == 1
        assert listings[0].title == "Python Dev"
        assert listings[0].company == "Acme Corp"
        assert listings[0].location == "Stockholm"
        assert listings[0].source == "linkedin"
        expected_id = hashlib.sha256(b"https://linkedin.com/jobs/view/123").hexdigest()[:16]
        assert listings[0].external_id == expected_id

    def test_map_results_dedup(self):
        from jobhaul.collectors.linkedin import LinkedInCollector

        collector = LinkedInCollector()
        items = [
            {"title": "Dev", "jobUrl": "https://linkedin.com/jobs/view/123"},
            {"title": "Dev", "jobUrl": "https://linkedin.com/jobs/view/123"},
        ]
        listings = collector._map_results(items)
        assert len(listings) == 1

    def test_map_results_no_url(self):
        from jobhaul.collectors.linkedin import LinkedInCollector

        collector = LinkedInCollector()
        items = [{"title": "Dev"}]
        listings = collector._map_results(items)
        assert len(listings) == 0

    def test_map_results_remote_detection(self):
        from jobhaul.collectors.linkedin import LinkedInCollector

        collector = LinkedInCollector()
        items = [
            {
                "title": "Remote Python Dev",
                "location": "Remote",
                "jobUrl": "https://linkedin.com/jobs/view/789",
            },
        ]
        listings = collector._map_results(items)
        assert len(listings) == 1
        assert listings[0].is_remote is True


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
    async def test_collect_no_token(self):
        from jobhaul.collectors.indeed import IndeedCollector

        profile = Profile(
            name="Test",
            search_terms=["python"],
            sources={"indeed": SourceConfig(enabled=True, apify_token="")},
        )
        collector = IndeedCollector()
        result = await collector.collect(profile)
        assert len(result.errors) == 1
        assert "token" in result.errors[0].lower()

    def test_map_results(self):
        from jobhaul.collectors.indeed import IndeedCollector

        collector = IndeedCollector()
        items = [
            {
                "positionName": "Backend Dev",
                "company": "TechCo",
                "location": "Stockholm",
                "description": "Build APIs",
                "url": "https://indeed.com/viewjob?jk=abc123",
                "datePosted": "2024-02-01",
            },
        ]
        listings = collector._map_results(items)
        assert len(listings) == 1
        assert listings[0].title == "Backend Dev"
        assert listings[0].company == "TechCo"
        assert listings[0].source == "indeed"
        expected_id = hashlib.sha256(b"https://indeed.com/viewjob?jk=abc123").hexdigest()[:16]
        assert listings[0].external_id == expected_id

    def test_map_results_dedup(self):
        from jobhaul.collectors.indeed import IndeedCollector

        collector = IndeedCollector()
        items = [
            {"positionName": "Dev", "url": "https://indeed.com/viewjob?jk=abc"},
            {"positionName": "Dev", "url": "https://indeed.com/viewjob?jk=abc"},
        ]
        listings = collector._map_results(items)
        assert len(listings) == 1

    def test_map_results_no_url(self):
        from jobhaul.collectors.indeed import IndeedCollector

        collector = IndeedCollector()
        items = [{"positionName": "Dev"}]
        listings = collector._map_results(items)
        assert len(listings) == 0

    def test_actor_id(self):
        from jobhaul.collectors.indeed import ACTOR_ID

        assert ACTOR_ID == "apify~indeed-scraper"

    def test_linkedin_actor_id(self):
        from jobhaul.collectors.linkedin import ACTOR_ID

        assert ACTOR_ID == "fetchclub~linkedin-jobs-scraper"
