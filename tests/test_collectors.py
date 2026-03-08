"""Tests for collectors with mocked HTTP responses."""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from jobhaul.collectors.base import detect_remote
from jobhaul.collectors.jooble import JoobleCollector
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
