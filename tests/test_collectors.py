"""Tests for collectors with mocked HTTP responses."""

from __future__ import annotations

import hashlib
import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import respx

from jobhaul.collectors.base import detect_remote
from jobhaul.collectors.indeed import IndeedCollector
from jobhaul.collectors.jooble import JoobleCollector
from jobhaul.collectors.linkedin import LinkedInCollector
from jobhaul.collectors.platsbanken import PlatsbankenCollector
from jobhaul.collectors.remoteok import RemoteOKCollector
from jobhaul.models import Profile, SourceConfig


@pytest.fixture
def profile():
    return Profile(
        name="Test",
        roles=["developer"],
        search_terms=["python"],
        skills=["Python", "JavaScript", "react"],
        location="Stockholm",
        sources={
            "platsbanken": SourceConfig(enabled=True, region="abc"),
            "remoteok": SourceConfig(enabled=True),
            "jooble": SourceConfig(enabled=True, api_key="test-key"),
        },
    )


@pytest.fixture
def linkedin_profile():
    return Profile(
        name="Test",
        roles=["developer"],
        search_terms=["junior developer"],
        skills=["Python"],
        location="Stockholm",
        sources={
            "linkedin": SourceConfig(enabled=True, apify_token="test-token"),
        },
    )


@pytest.fixture
def indeed_profile():
    return Profile(
        name="Test",
        roles=["developer"],
        search_terms=["junior developer"],
        skills=["Python"],
        location="Sweden",
        sources={
            "indeed": SourceConfig(enabled=True, apify_token="test-token", region="SE"),
        },
    )


# --- Remote detection tests ---


class TestRemoteDetection:
    def test_english_remote(self):
        assert detect_remote("Remote Developer", "") is True

    def test_english_hybrid(self):
        assert detect_remote("", "This is a hybrid position") is True

    def test_swedish_distans(self):
        assert detect_remote("", "Arbeta på distans") is True

    def test_swedish_hemma(self):
        assert detect_remote("", "jobba hemifrån möjlighet") is True

    def test_no_remote(self):
        assert detect_remote("Developer", "Office in Stockholm") is False

    def test_case_insensitive(self):
        assert detect_remote("REMOTE DEVELOPER", "") is True

    def test_fjärr(self):
        assert detect_remote("Fjärrjobb", "fjärr arbete") is True


# --- Platsbanken tests ---


class TestPlatsbanken:
    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_success(self, profile):
        respx.get("https://jobsearch.api.jobtechdev.se/search").mock(
            return_value=httpx.Response(
                200,
                json={
                    "hits": [
                        {
                            "id": "123",
                            "headline": "Python Developer",
                            "description": {"text": "Work with Python"},
                            "employer": {"name": "Acme Corp"},
                            "workplace_address": {"municipality": "Stockholm"},
                            "webpage_url": "https://example.com/123",
                            "publication_date": "2024-01-01",
                            "employment_type": {"label": "Full-time"},
                        }
                    ]
                },
            )
        )

        collector = PlatsbankenCollector()
        result = await collector.collect(profile)

        assert result.source == "platsbanken"
        assert len(result.listings) == 1
        assert result.listings[0].title == "Python Developer"
        assert result.listings[0].company == "Acme Corp"
        assert result.listings[0].external_id == "123"

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_disabled(self):
        profile = Profile(
            name="Test",
            search_terms=["python"],
            sources={"platsbanken": SourceConfig(enabled=False)},
        )
        collector = PlatsbankenCollector()
        result = await collector.collect(profile)
        assert result.listings == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_deduplicates_within_batch(self, profile):
        hit = {
            "id": "123",
            "headline": "Python Developer",
            "description": {"text": "Work with Python"},
            "employer": {"name": "Acme Corp"},
            "workplace_address": {},
            "webpage_url": "https://example.com/123",
        }
        respx.get("https://jobsearch.api.jobtechdev.se/search").mock(
            return_value=httpx.Response(200, json={"hits": [hit, hit]})
        )

        collector = PlatsbankenCollector()
        result = await collector.collect(profile)
        assert len(result.listings) == 1


# --- RemoteOK tests ---


class TestRemoteOK:
    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_success(self, profile):
        respx.get("https://remoteok.com/api").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"legal": "metadata"},
                    {
                        "id": "456",
                        "position": "React Developer",
                        "company": "Remote Inc",
                        "location": "Worldwide",
                        "description": "Build React apps",
                        "url": "https://remoteok.com/456",
                        "date": "2024-01-01",
                        "tags": ["react", "javascript"],
                    },
                ],
            )
        )

        collector = RemoteOKCollector()
        result = await collector.collect(profile)

        assert result.source == "remoteok"
        assert len(result.listings) == 1
        assert result.listings[0].title == "React Developer"
        assert result.listings[0].is_remote is True

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_filters_by_skills(self, profile):
        respx.get("https://remoteok.com/api").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"legal": "metadata"},
                    {
                        "id": "789",
                        "position": "Rust Developer",
                        "company": "LowLevel Inc",
                        "tags": ["rust", "c++"],
                    },
                ],
            )
        )

        collector = RemoteOKCollector()
        result = await collector.collect(profile)
        assert len(result.listings) == 0

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_disabled(self):
        profile = Profile(
            name="Test",
            sources={"remoteok": SourceConfig(enabled=False)},
        )
        collector = RemoteOKCollector()
        result = await collector.collect(profile)
        assert result.listings == []


# --- Jooble tests ---


class TestJooble:
    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_success(self, profile):
        respx.post("https://jooble.org/api/test-key").mock(
            return_value=httpx.Response(
                200,
                json={
                    "jobs": [
                        {
                            "id": "j1",
                            "title": "Backend Developer",
                            "company": "TechCo",
                            "location": "Stockholm",
                            "snippet": "Work with APIs",
                            "link": "https://jooble.org/j1",
                            "updated": "2024-01-01",
                            "type": "Full-time",
                        }
                    ]
                },
            )
        )

        collector = JoobleCollector()
        result = await collector.collect(profile)

        assert result.source == "jooble"
        assert len(result.listings) == 1
        assert result.listings[0].title == "Backend Developer"
        assert result.listings[0].company == "TechCo"

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_no_api_key(self):
        profile = Profile(
            name="Test",
            search_terms=["python"],
            sources={"jooble": SourceConfig(enabled=True, api_key="")},
        )
        collector = JoobleCollector()
        result = await collector.collect(profile)
        assert len(result.errors) == 1
        assert "API key" in result.errors[0]

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_disabled(self):
        profile = Profile(
            name="Test",
            sources={"jooble": SourceConfig(enabled=False)},
        )
        collector = JoobleCollector()
        result = await collector.collect(profile)
        assert result.listings == []


# --- LinkedIn Apify tests ---


def _apify_run_response(run_id="run-1", dataset_id="ds-1", status="RUNNING"):
    return {
        "data": {
            "id": run_id,
            "defaultDatasetId": dataset_id,
            "status": status,
        }
    }


def _apify_status_response(status="SUCCEEDED"):
    return {"data": {"status": status}}


LINKEDIN_ITEMS = [
    {
        "id": "123",
        "title": "Python Developer",
        "companyName": "Acme Corp",
        "location": "Stockholm, Sweden",
        "descriptionText": "Build Python apps",
        "link": "https://www.linkedin.com/jobs/view/123",
        "postedAt": "2024-01-15",
        "seniorityLevel": "Mid-Senior level",
        "employmentType": "Full-time",
        "salary": "50000-70000 SEK/month",
    },
    {
        "id": "456",
        "title": "Remote React Dev",
        "companyName": "Remote Inc",
        "location": "Remote",
        "descriptionText": "Build React apps",
        "link": "https://www.linkedin.com/jobs/view/456",
        "postedAt": "2024-01-16",
    },
]

INDEED_ITEMS = [
    {
        "positionName": "Backend Engineer",
        "company": "TechCo",
        "location": "Gothenburg",
        "description": "Work on backend systems",
        "url": "https://indeed.com/viewjob?jk=abc123",
        "datePosted": "2024-02-01",
    },
]


class TestLinkedIn:
    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_success(self, linkedin_profile):
        # Mock start run
        respx.post(
            "https://api.apify.com/v2/acts/curious_coder~linkedin-jobs-scraper/runs",
            params={"token": "test-token"},
        ).mock(
            return_value=httpx.Response(200, json=_apify_run_response())
        )
        # Mock poll status
        respx.get(
            "https://api.apify.com/v2/actor-runs/run-1",
            params={"token": "test-token"},
        ).mock(
            return_value=httpx.Response(200, json=_apify_status_response("SUCCEEDED"))
        )
        # Mock fetch results
        respx.get(
            "https://api.apify.com/v2/datasets/ds-1/items",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(200, json=LINKEDIN_ITEMS))

        collector = LinkedInCollector()
        result = await collector.collect(linkedin_profile)

        assert result.source == "linkedin"
        assert len(result.listings) == 2
        assert result.listings[0].title == "Python Developer"
        assert result.listings[0].company == "Acme Corp"
        assert result.listings[0].location == "Stockholm, Sweden"
        assert result.listings[0].url == "https://www.linkedin.com/jobs/view/123"
        assert result.listings[0].published_at == "2024-01-15"
        assert result.listings[0].external_id == "123"
        assert result.listings[1].is_remote is True
        assert result.errors == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_maps_new_fields(self, linkedin_profile):
        """Test that seniorityLevel, employmentType, and salary are mapped."""
        respx.post(
            "https://api.apify.com/v2/acts/curious_coder~linkedin-jobs-scraper/runs",
            params={"token": "test-token"},
        ).mock(
            return_value=httpx.Response(200, json=_apify_run_response())
        )
        respx.get(
            "https://api.apify.com/v2/actor-runs/run-1",
            params={"token": "test-token"},
        ).mock(
            return_value=httpx.Response(200, json=_apify_status_response("SUCCEEDED"))
        )
        respx.get(
            "https://api.apify.com/v2/datasets/ds-1/items",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(200, json=LINKEDIN_ITEMS))

        collector = LinkedInCollector()
        result = await collector.collect(linkedin_profile)

        # First item has all new fields
        assert result.listings[0].seniority_level == "Mid-Senior level"
        assert result.listings[0].employment_type == "Full-time"
        assert result.listings[0].salary == "50000-70000 SEK/month"
        # Second item has no new fields -> None
        assert result.listings[1].seniority_level is None
        assert result.listings[1].employment_type is None
        assert result.listings[1].salary is None

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_prefers_description_text(self, linkedin_profile):
        """Test descriptionText is preferred over descriptionHtml."""
        items = [
            {
                "id": "789",
                "title": "Dev",
                "companyName": "Co",
                "location": "Here",
                "descriptionText": "Plain text desc",
                "descriptionHtml": "<p>HTML desc</p>",
                "link": "https://www.linkedin.com/jobs/view/789",
            },
            {
                "id": "790",
                "title": "Dev2",
                "companyName": "Co2",
                "location": "There",
                "descriptionText": "",
                "descriptionHtml": "<p>Fallback HTML</p>",
                "link": "https://www.linkedin.com/jobs/view/790",
            },
        ]
        respx.post(
            "https://api.apify.com/v2/acts/curious_coder~linkedin-jobs-scraper/runs",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(200, json=_apify_run_response()))
        respx.get(
            "https://api.apify.com/v2/actor-runs/run-1",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(200, json=_apify_status_response("SUCCEEDED")))
        respx.get(
            "https://api.apify.com/v2/datasets/ds-1/items",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(200, json=items))

        collector = LinkedInCollector()
        result = await collector.collect(linkedin_profile)

        assert result.listings[0].description == "Plain text desc"
        assert result.listings[1].description == "<p>Fallback HTML</p>"

    @pytest.mark.asyncio
    async def test_collect_missing_token(self):
        profile = Profile(
            name="Test",
            search_terms=["python"],
            sources={"linkedin": SourceConfig(enabled=True, apify_token="")},
        )
        collector = LinkedInCollector()
        result = await collector.collect(profile)
        assert len(result.errors) == 1
        assert "token" in result.errors[0].lower()
        assert result.listings == []

    @pytest.mark.asyncio
    async def test_collect_disabled(self):
        profile = Profile(
            name="Test",
            sources={"linkedin": SourceConfig(enabled=False)},
        )
        collector = LinkedInCollector()
        result = await collector.collect(profile)
        assert result.listings == []
        assert result.errors == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_http_error(self, linkedin_profile):
        respx.post(
            "https://api.apify.com/v2/acts/curious_coder~linkedin-jobs-scraper/runs",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(500))

        collector = LinkedInCollector()
        result = await collector.collect(linkedin_profile)

        assert len(result.errors) == 1
        assert "LinkedIn Apify error" in result.errors[0]
        assert result.listings == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_actor_timeout(self, linkedin_profile):
        respx.post(
            "https://api.apify.com/v2/acts/curious_coder~linkedin-jobs-scraper/runs",
            params={"token": "test-token"},
        ).mock(
            return_value=httpx.Response(200, json=_apify_run_response())
        )
        # Always return RUNNING status
        respx.get(
            "https://api.apify.com/v2/actor-runs/run-1",
            params={"token": "test-token"},
        ).mock(
            return_value=httpx.Response(200, json=_apify_status_response("RUNNING"))
        )

        collector = LinkedInCollector()
        # Patch sleep and timeout to avoid waiting 5 min in test
        with patch("jobhaul.collectors.linkedin.POLL_INTERVAL", 0), \
             patch("jobhaul.collectors.linkedin.TIMEOUT", 0):
            result = await collector.collect(linkedin_profile)

        assert len(result.errors) == 1
        assert "timed out" in result.errors[0].lower()
        assert result.listings == []


# --- Indeed Apify tests ---


class TestIndeed:
    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_success(self, indeed_profile):
        respx.post(
            "https://api.apify.com/v2/acts/apify~indeed-scraper/runs",
            params={"token": "test-token"},
        ).mock(
            return_value=httpx.Response(200, json=_apify_run_response())
        )
        respx.get(
            "https://api.apify.com/v2/actor-runs/run-1",
            params={"token": "test-token"},
        ).mock(
            return_value=httpx.Response(200, json=_apify_status_response("SUCCEEDED"))
        )
        respx.get(
            "https://api.apify.com/v2/datasets/ds-1/items",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(200, json=INDEED_ITEMS))

        collector = IndeedCollector()
        result = await collector.collect(indeed_profile)

        assert result.source == "indeed"
        assert len(result.listings) == 1
        assert result.listings[0].title == "Backend Engineer"
        assert result.listings[0].company == "TechCo"
        assert result.listings[0].url == "https://indeed.com/viewjob?jk=abc123"
        assert result.listings[0].published_at == "2024-02-01"
        expected_id = hashlib.sha256(b"https://indeed.com/viewjob?jk=abc123").hexdigest()[:16]
        assert result.listings[0].external_id == expected_id
        assert result.errors == []

    @pytest.mark.asyncio
    async def test_collect_missing_token(self):
        profile = Profile(
            name="Test",
            search_terms=["python"],
            sources={"indeed": SourceConfig(enabled=True, apify_token="")},
        )
        collector = IndeedCollector()
        result = await collector.collect(profile)
        assert len(result.errors) == 1
        assert "token" in result.errors[0].lower()
        assert result.listings == []

    @pytest.mark.asyncio
    async def test_collect_disabled(self):
        profile = Profile(
            name="Test",
            sources={"indeed": SourceConfig(enabled=False)},
        )
        collector = IndeedCollector()
        result = await collector.collect(profile)
        assert result.listings == []
        assert result.errors == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_http_error(self, indeed_profile):
        respx.post(
            "https://api.apify.com/v2/acts/apify~indeed-scraper/runs",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(500))

        collector = IndeedCollector()
        result = await collector.collect(indeed_profile)

        assert len(result.errors) == 1
        assert "Indeed Apify error" in result.errors[0]
        assert result.listings == []
