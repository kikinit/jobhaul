"""Tests for analysis matcher with mocked LLM adapter."""

from __future__ import annotations

import json

import pytest

from jobhaul.analysis.adapter import LLMAdapter
from jobhaul.analysis.matcher import (
    analyze_listing,
    build_prompt,
    compute_profile_hash,
    parse_llm_response,
)
from jobhaul.models import JobListing, Profile


class MockAdapter(LLMAdapter):
    def __init__(self, response: str):
        self.response = response
        self.last_prompt = None

    async def analyze(self, prompt: str) -> str:
        self.last_prompt = prompt
        return self.response


@pytest.fixture
def profile():
    return Profile(
        name="Test User",
        roles=["developer"],
        skills=["Python", "JavaScript"],
        location="Stockholm",
        seniority="junior",
    )


@pytest.fixture
def listing():
    return JobListing(
        id=1,
        title="Python Developer",
        company="Acme Corp",
        location="Stockholm",
        description="Build amazing Python applications",
        is_remote=False,
        sources=["platsbanken"],
        created_at="2024-01-01",
    )


class TestProfileHash:
    def test_hash_is_stable(self, profile):
        h1 = compute_profile_hash(profile)
        h2 = compute_profile_hash(profile)
        assert h1 == h2

    def test_hash_changes_with_skills(self, profile):
        h1 = compute_profile_hash(profile)
        profile.skills.append("Rust")
        h2 = compute_profile_hash(profile)
        assert h1 != h2

    def test_hash_ignores_sources(self, profile):
        h1 = compute_profile_hash(profile)
        from jobhaul.models import SourceConfig

        profile.sources["test"] = SourceConfig(enabled=True)
        h2 = compute_profile_hash(profile)
        assert h1 == h2


class TestBuildPrompt:
    def test_includes_listing_info(self, listing, profile):
        prompt = build_prompt(listing, profile)
        assert "Python Developer" in prompt
        assert "Acme Corp" in prompt
        assert "Stockholm" in prompt

    def test_includes_profile_info(self, listing, profile):
        prompt = build_prompt(listing, profile)
        assert "Test User" in prompt
        assert "Python" in prompt
        assert "JavaScript" in prompt

    def test_includes_json_instruction(self, listing, profile):
        prompt = build_prompt(listing, profile)
        assert "JSON" in prompt
        assert "match_score" in prompt


class TestParseLLMResponse:
    def test_parse_plain_json(self):
        data = {"match_score": 85, "summary": "Great fit"}
        result = parse_llm_response(json.dumps(data))
        assert result["match_score"] == 85

    def test_parse_markdown_fenced_json(self):
        response = '```json\n{"match_score": 75, "summary": "OK"}\n```'
        result = parse_llm_response(response)
        assert result["match_score"] == 75

    def test_parse_plain_fenced_json(self):
        response = '```\n{"match_score": 60}\n```'
        result = parse_llm_response(response)
        assert result["match_score"] == 60

    def test_parse_invalid_json(self):
        with pytest.raises(json.JSONDecodeError):
            parse_llm_response("not json at all")


class TestAnalyzeListing:
    @pytest.mark.asyncio
    async def test_successful_analysis(self, listing, profile):
        response_data = {
            "match_score": 85,
            "match_reasons": ["Strong Python skills", "Location match"],
            "missing_skills": ["Docker"],
            "strengths": ["Location match"],
            "concerns": ["Junior level"],
            "summary": "Good fit overall",
            "application_notes": "Highlight Python projects",
        }
        adapter = MockAdapter(json.dumps(response_data))

        result = await analyze_listing(listing, profile, adapter)

        assert result.listing_id == 1
        assert result.match_score == 85
        assert result.match_reasons == ["Strong Python skills", "Location match"]
        assert result.profile_hash == compute_profile_hash(profile)

    @pytest.mark.asyncio
    async def test_string_fields_coerced_to_lists(self, listing, profile):
        response_data = {
            "match_score": 70,
            "match_reasons": "Single string reason",
            "missing_skills": "Docker",
            "strengths": "Good fit",
            "concerns": "Junior",
            "summary": "OK match",
        }
        adapter = MockAdapter(json.dumps(response_data))

        result = await analyze_listing(listing, profile, adapter)

        assert result.match_reasons == ["Single string reason"]
        assert result.missing_skills == ["Docker"]

    @pytest.mark.asyncio
    async def test_invalid_llm_response(self, listing, profile):
        adapter = MockAdapter("This is not valid JSON at all")

        result = await analyze_listing(listing, profile, adapter)

        assert result.match_score == 0
        assert "failed" in result.summary.lower()

    @pytest.mark.asyncio
    async def test_score_clamped_to_range(self, listing, profile):
        adapter = MockAdapter(json.dumps({"match_score": 150}))
        result = await analyze_listing(listing, profile, adapter)
        assert result.match_score == 100

        adapter = MockAdapter(json.dumps({"match_score": -10}))
        result = await analyze_listing(listing, profile, adapter)
        assert result.match_score == 0
