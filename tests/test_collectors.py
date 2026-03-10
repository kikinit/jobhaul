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
        "formattedLocation": "Gothenburg",
        "description": "Work on backend systems",
        "link": "https://indeed.com/viewjob?jk=abc123",
        "pubDate": "2026-03-09",
        "salary": "30 000 kr/mån",
        "jobTypes": ["Full-time", "Permanent"],
    },
    {
        "positionName": "Remote Frontend Dev",
        "company": "WebCorp",
        "formattedLocation": "Remote",
        "description": "Build UIs",
        "link": "https://indeed.com/viewjob?jk=def456",
        "pubDate": "2026-03-08",
        "salary": None,
        "jobTypes": ["Contract"],
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
    """Tests for Indeed collector using misceres~indeed-scraper Apify actor."""

    def _mock_indeed_run(self, run_id="run-1", dataset_id="ds-1"):
        """Set up mocks for a successful Indeed actor run."""
        respx.post(
            "https://api.apify.com/v2/acts/misceres~indeed-scraper/runs",
            params={"token": "test-token"},
        ).mock(
            return_value=httpx.Response(
                200, json=_apify_run_response(run_id, dataset_id)
            )
        )
        respx.get(
            "https://api.apify.com/v2/actor-runs/" + run_id,
            params={"token": "test-token"},
        ).mock(
            return_value=httpx.Response(
                200, json=_apify_status_response("SUCCEEDED")
            )
        )

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_success_field_mapping(self, indeed_profile):
        """Success case -- verify all output fields are mapped correctly."""
        self._mock_indeed_run()
        respx.get(
            "https://api.apify.com/v2/datasets/ds-1/items",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(200, json=INDEED_ITEMS))

        collector = IndeedCollector()
        result = await collector.collect(indeed_profile)

        assert result.source == "indeed"
        assert len(result.listings) == 2
        assert result.errors == []

        # First item -- full field mapping
        l0 = result.listings[0]
        assert l0.title == "Backend Engineer"
        assert l0.company == "TechCo"
        assert l0.location == "Gothenburg"
        assert l0.description == "Work on backend systems"
        assert l0.url == "https://indeed.com/viewjob?jk=abc123"
        assert l0.published_at == "2026-03-09"
        assert l0.salary == "30 000 kr/mån"
        assert l0.employment_type == "Full-time"
        expected_id = hashlib.sha256(b"https://indeed.com/viewjob?jk=abc123").hexdigest()[:16]
        assert l0.external_id == expected_id

        # Second item -- remote detection and different fields
        l1 = result.listings[1]
        assert l1.title == "Remote Frontend Dev"
        assert l1.is_remote is True
        assert l1.salary is None
        assert l1.employment_type == "Contract"

    @pytest.mark.asyncio
    async def test_collect_missing_token(self):
        """Missing token returns empty CollectorResult with error message."""
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
        """Disabled source returns empty CollectorResult, no API calls."""
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
        """HTTP error on run start -- collector returns error, no crash."""
        respx.post(
            "https://api.apify.com/v2/acts/misceres~indeed-scraper/runs",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(500))

        collector = IndeedCollector()
        result = await collector.collect(indeed_profile)

        assert len(result.errors) == 1
        assert "Indeed Apify error" in result.errors[0]
        assert result.listings == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_actor_run_failed(self, indeed_profile):
        """Actor run FAILED status -- collector returns error."""
        respx.post(
            "https://api.apify.com/v2/acts/misceres~indeed-scraper/runs",
            params={"token": "test-token"},
        ).mock(
            return_value=httpx.Response(200, json=_apify_run_response())
        )
        respx.get(
            "https://api.apify.com/v2/actor-runs/run-1",
            params={"token": "test-token"},
        ).mock(
            return_value=httpx.Response(200, json=_apify_status_response("FAILED"))
        )

        collector = IndeedCollector()
        result = await collector.collect(indeed_profile)

        assert len(result.errors) == 1
        assert "FAILED" in result.errors[0]
        assert result.listings == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_actor_timeout(self, indeed_profile):
        """Actor run timeout -- poll never succeeds, returns TimeoutError."""
        respx.post(
            "https://api.apify.com/v2/acts/misceres~indeed-scraper/runs",
            params={"token": "test-token"},
        ).mock(
            return_value=httpx.Response(200, json=_apify_run_response())
        )
        respx.get(
            "https://api.apify.com/v2/actor-runs/run-1",
            params={"token": "test-token"},
        ).mock(
            return_value=httpx.Response(200, json=_apify_status_response("RUNNING"))
        )

        collector = IndeedCollector()
        with patch("jobhaul.collectors.indeed.POLL_INTERVAL", 0), \
             patch("jobhaul.collectors.indeed.TIMEOUT", 0):
            result = await collector.collect(indeed_profile)

        assert len(result.errors) == 1
        assert "timed out" in result.errors[0].lower()
        assert result.listings == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_deduplication_across_terms(self):
        """Two search terms return overlapping results -- only unique listings kept."""
        profile = Profile(
            name="Test",
            search_terms=["python", "backend"],
            skills=["Python"],
            location="Sweden",
            sources={
                "indeed": SourceConfig(enabled=True, apify_token="test-token", region="SE"),
            },
        )
        shared_item = {
            "positionName": "Python Dev",
            "company": "Overlap Corp",
            "formattedLocation": "Stockholm",
            "description": "Overlapping job",
            "link": "https://indeed.com/viewjob?jk=same123",
            "pubDate": "2026-03-09",
        }
        unique_item = {
            "positionName": "Backend Dev",
            "company": "Unique Corp",
            "formattedLocation": "Malmö",
            "description": "Unique job",
            "link": "https://indeed.com/viewjob?jk=unique456",
            "pubDate": "2026-03-08",
        }

        # Both search terms trigger separate runs
        run_route = respx.post(
            "https://api.apify.com/v2/acts/misceres~indeed-scraper/runs",
            params={"token": "test-token"},
        )
        run_route.side_effect = [
            httpx.Response(200, json=_apify_run_response("run-a", "ds-a")),
            httpx.Response(200, json=_apify_run_response("run-b", "ds-b")),
        ]
        # Poll routes
        respx.get(
            "https://api.apify.com/v2/actor-runs/run-a",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(200, json=_apify_status_response("SUCCEEDED")))
        respx.get(
            "https://api.apify.com/v2/actor-runs/run-b",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(200, json=_apify_status_response("SUCCEEDED")))
        # Dataset routes -- shared_item appears in both
        respx.get(
            "https://api.apify.com/v2/datasets/ds-a/items",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(200, json=[shared_item, unique_item]))
        respx.get(
            "https://api.apify.com/v2/datasets/ds-b/items",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(200, json=[shared_item]))

        collector = IndeedCollector()
        result = await collector.collect(profile)

        # shared_item appears twice in combined items but should be deduplicated
        assert len(result.listings) == 2
        urls = {l.url for l in result.listings}
        assert "https://indeed.com/viewjob?jk=same123" in urls
        assert "https://indeed.com/viewjob?jk=unique456" in urls

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_empty_results(self, indeed_profile):
        """Actor returns 0 items -- collector returns empty listings, no crash."""
        self._mock_indeed_run()
        respx.get(
            "https://api.apify.com/v2/datasets/ds-1/items",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(200, json=[]))

        collector = IndeedCollector()
        result = await collector.collect(indeed_profile)

        assert result.listings == []
        assert result.errors == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_collect_missing_fields_skips_gracefully(self, indeed_profile):
        """Items missing link are skipped; missing optional fields handled."""
        items = [
            {
                "positionName": "No Link Job",
                "company": "Ghost Corp",
            },
            {
                "link": "https://indeed.com/viewjob?jk=valid",
            },
        ]
        self._mock_indeed_run()
        respx.get(
            "https://api.apify.com/v2/datasets/ds-1/items",
            params={"token": "test-token"},
        ).mock(return_value=httpx.Response(200, json=items))

        collector = IndeedCollector()
        result = await collector.collect(indeed_profile)

        # First item skipped (no link), second kept with defaults
        assert len(result.listings) == 1
        assert result.listings[0].url == "https://indeed.com/viewjob?jk=valid"
        assert result.listings[0].title == ""
        assert result.listings[0].company is None
        assert result.listings[0].location == ""
        assert result.listings[0].description is None
