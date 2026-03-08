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
    pre_screen,
)
from jobhaul.models import JobListing, Profile


class MockAdapter(LLMAdapter):
    def __init__(self, response: str):
        self.response = response
        self.last_prompt = None
        self.call_count = 0

    async def analyze(self, prompt: str) -> str:
        self.last_prompt = prompt
        self.call_count += 1
        return self.response


class RetryMockAdapter(LLMAdapter):
    """Returns invalid response first, then valid on retry."""

    def __init__(self, first: str, second: str):
        self.responses = [first, second]
        self.call_count = 0

    async def analyze(self, prompt: str) -> str:
        idx = min(self.call_count, len(self.responses) - 1)
        self.call_count += 1
        return self.responses[idx]


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

    def test_prompt_contains_dealbreaker_senior_cap(self, listing, profile):
        prompt = build_prompt(listing, profile)
        assert "Senior/Lead/Staff/Principal" in prompt
        assert "MAX SCORE 10" in prompt

    def test_prompt_contains_dealbreaker_3yr_cap(self, listing, profile):
        prompt = build_prompt(listing, profile)
        assert "3+ years" in prompt
        assert "MAX SCORE 25" in prompt

    def test_prompt_contains_dealbreaker_5yr_cap(self, listing, profile):
        prompt = build_prompt(listing, profile)
        assert "5+ years" in prompt

    def test_prompt_contains_dealbreaker_wrong_tech(self, listing, profile):
        prompt = build_prompt(listing, profile)
        assert "Primary tech the candidate does not know" in prompt
        assert "MAX SCORE 20" in prompt

    def test_prompt_contains_junior_boost(self, listing, profile):
        prompt = build_prompt(listing, profile)
        assert "junior/graduate/entry-level" in prompt
        assert "BOOST" in prompt

    def test_prompt_contains_skill_depth_context(self, listing, profile):
        prompt = build_prompt(listing, profile)
        assert "FOUNDATIONAL/ACADEMIC" in prompt
        assert "LESS THAN 1 YEAR" in prompt

    def test_prompt_contains_neutral_factors(self, listing, profile):
        prompt = build_prompt(listing, profile)
        assert "Employment type" in prompt or "employment type" in prompt
        assert "do NOT penalize" in prompt or "do not reduce score" in prompt

    def test_prompt_contains_scoring_ranges(self, listing, profile):
        prompt = build_prompt(listing, profile)
        assert "80-100" in prompt
        assert "60-79" in prompt
        assert "40-59" in prompt
        assert "20-39" in prompt
        assert "5-15" in prompt
        assert "0-4" in prompt


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

    def test_parse_empty_response(self):
        """Issue #4: Handle empty responses gracefully."""
        with pytest.raises(json.JSONDecodeError):
            parse_llm_response("")

    def test_parse_whitespace_only(self):
        with pytest.raises(json.JSONDecodeError):
            parse_llm_response("   \n  ")

    def test_parse_json_in_mixed_text(self):
        """Issue #4: Extract JSON from mixed text."""
        response = 'Here is my analysis:\n{"match_score": 42, "summary": "OK"}\nThanks!'
        result = parse_llm_response(response)
        assert result["match_score"] == 42

    def test_parse_json_with_preamble(self):
        response = 'Sure! Here is the JSON:\n\n{"match_score": 55}'
        result = parse_llm_response(response)
        assert result["match_score"] == 55


class TestPreScreen:
    """Tests for pre-screening before LLM analysis (Issue #9)."""

    def test_high_match(self, profile):
        listing = JobListing(
            id=1,
            title="Python Developer",
            description="JavaScript and Python developer needed for Stockholm team",
            sources=["platsbanken"],
            created_at="2024-01-01",
        )
        score = pre_screen(listing, profile)
        assert score > 0.0

    def test_no_match(self, profile):
        listing = JobListing(
            id=1,
            title="Tandläkare",
            description="Vi söker en erfaren tandläkare till vår klinik",
            sources=["platsbanken"],
            created_at="2024-01-01",
        )
        score = pre_screen(listing, profile)
        assert score == 0.0

    def test_partial_match(self, profile):
        listing = JobListing(
            id=1,
            title="Python Backend Developer",
            description="Building APIs with Python and Django",
            sources=["platsbanken"],
            created_at="2024-01-01",
        )
        score = pre_screen(listing, profile)
        assert 0.0 < score < 1.0

    def test_no_terms_passes_everything(self):
        profile = Profile(name="Test")
        listing = JobListing(
            id=1,
            title="Anything",
            description="Whatever",
            sources=["platsbanken"],
            created_at="2024-01-01",
        )
        score = pre_screen(listing, profile)
        assert score == 1.0

    def test_case_insensitive(self, profile):
        listing = JobListing(
            id=1,
            title="PYTHON DEVELOPER",
            description="",
            sources=["platsbanken"],
            created_at="2024-01-01",
        )
        score = pre_screen(listing, profile)
        assert score > 0.0


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

    @pytest.mark.asyncio
    async def test_retry_on_parse_failure(self, listing, profile):
        """Issue #4: Retry once with clearer prompt on parse failure."""
        valid_data = json.dumps({"match_score": 77, "summary": "OK"})
        adapter = RetryMockAdapter("not json", valid_data)

        result = await analyze_listing(listing, profile, adapter)

        assert adapter.call_count == 2
        assert result.match_score == 77

    @pytest.mark.asyncio
    async def test_retry_also_fails(self, listing, profile):
        """If both attempts fail, return a zero-score result."""
        adapter = RetryMockAdapter("not json", "still not json")

        result = await analyze_listing(listing, profile, adapter)

        assert adapter.call_count == 2
        assert result.match_score == 0
        assert "failed" in result.summary.lower()
