"""Tests for flag matching logic (Issues #2 and #10)."""

from __future__ import annotations

import pytest

from jobhaul.flagging import _match_flags, flag_listing
from jobhaul.models import Flags, JobListing, Profile


@pytest.fixture
def flags():
    return Flags(
        boost=["AI", "startup", "remote", "SvelteKit"],
        warn=["defense", "gambling", "bemanningsföretag"],
        exclude=["tobacco"],
    )


def _make_listing(**kwargs) -> JobListing:
    defaults = {
        "id": 1,
        "title": "Software Developer",
        "company": "TechCo",
        "description": "Build software",
        "sources": ["platsbanken"],
        "created_at": "2024-01-01",
    }
    defaults.update(kwargs)
    return JobListing(**defaults)


class TestMatchFlags:
    def test_case_insensitive(self):
        matches = _match_flags("This is an AI startup", ["AI", "startup"])
        assert sorted(matches) == ["AI", "startup"]

    def test_word_boundary_prevents_partial(self):
        """AI should not match EMAIL."""
        matches = _match_flags("Send us an EMAIL", ["AI"])
        assert matches == []

    def test_word_boundary_matches_whole_word(self):
        matches = _match_flags("We use AI extensively", ["AI"])
        assert matches == ["AI"]

    def test_no_matches(self):
        matches = _match_flags("Build web apps", ["AI", "ML"])
        assert matches == []

    def test_special_characters_in_term(self):
        matches = _match_flags("We use SvelteKit for frontend", ["SvelteKit"])
        assert matches == ["SvelteKit"]


class TestFlagListing:
    def test_boost_match(self, flags):
        listing = _make_listing(description="AI startup building tools")
        result = flag_listing(listing, flags)
        assert "AI" in result["boost"]
        assert "startup" in result["boost"]
        assert result["excluded"] is False

    def test_warn_match(self, flags):
        listing = _make_listing(company="Defense Corp", description="Military tech")
        result = flag_listing(listing, flags)
        assert "defense" in result["warn"]
        assert result["excluded"] is False

    def test_exclude_match(self, flags):
        listing = _make_listing(description="Leading tobacco company")
        result = flag_listing(listing, flags)
        assert result["excluded"] is True

    def test_no_flags(self, flags):
        listing = _make_listing(description="Regular company doing things")
        result = flag_listing(listing, flags)
        assert result["boost"] == []
        assert result["warn"] == []
        assert result["excluded"] is False

    def test_matches_against_title(self, flags):
        listing = _make_listing(title="AI Engineer")
        result = flag_listing(listing, flags)
        assert "AI" in result["boost"]

    def test_matches_against_company(self, flags):
        listing = _make_listing(company="AI Startup Inc")
        result = flag_listing(listing, flags)
        assert "AI" in result["boost"]
        assert "startup" in result["boost"]

    def test_empty_flags(self):
        listing = _make_listing()
        result = flag_listing(listing, Flags())
        assert result["boost"] == []
        assert result["warn"] == []
        assert result["excluded"] is False


class TestProfileBackwardCompat:
    def test_exclusions_treated_as_warn(self):
        """Old exclusions field should be treated as flags.warn (Issue #2)."""
        profile = Profile(name="Test", exclusions=["gambling", "defense"])
        flags = profile.get_effective_flags()
        assert flags.warn == ["gambling", "defense"]
        assert flags.boost == []
        assert flags.exclude == []

    def test_explicit_flags_override_exclusions(self):
        """If flags.warn is explicitly set, don't use exclusions."""
        profile = Profile(
            name="Test",
            exclusions=["old-excluded"],
            flags=Flags(warn=["new-warn"], boost=["AI"]),
        )
        flags = profile.get_effective_flags()
        assert flags.warn == ["new-warn"]
        assert flags.boost == ["AI"]

    def test_no_exclusions_no_flags(self):
        profile = Profile(name="Test")
        flags = profile.get_effective_flags()
        assert flags.warn == []
        assert flags.boost == []
        assert flags.exclude == []
