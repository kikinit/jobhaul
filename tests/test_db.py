"""Tests for database operations."""

from __future__ import annotations

import pytest

from jobhaul.db.queries import (
    find_duplicate,
    get_analysis,
    get_listing,
    get_stats,
    get_unanalyzed_listings,
    list_listings,
    save_analysis,
    upsert_listing,
)
from jobhaul.db.schema import init_db
from jobhaul.models import AnalysisResult, RawListing


@pytest.fixture
def conn(tmp_path):
    db_path = str(tmp_path / "test.db")
    connection = init_db(db_path)
    yield connection
    connection.close()


@pytest.fixture
def sample_listing():
    return RawListing(
        title="Python Developer",
        company="Acme Corp",
        location="Stockholm",
        description="Build things with Python",
        url="https://example.com/1",
        published_at="2024-01-01",
        is_remote=False,
        employment_type="Full-time",
        source="platsbanken",
        external_id="ext-1",
        source_url="https://example.com/1",
    )


class TestUpsertListing:
    def test_insert_new_listing(self, conn, sample_listing):
        listing_id = upsert_listing(conn, sample_listing)
        assert listing_id == 1

        listing = get_listing(conn, listing_id)
        assert listing.title == "Python Developer"
        assert listing.company == "Acme Corp"
        assert "platsbanken" in listing.sources

    def test_dedup_same_title_company(self, conn, sample_listing):
        id1 = upsert_listing(conn, sample_listing)

        # Same job from different source
        dup = sample_listing.model_copy(
            update={
                "source": "jooble",
                "external_id": "j-1",
                "source_url": "https://jooble.org/1",
            }
        )
        id2 = upsert_listing(conn, dup)

        assert id1 == id2

        listing = get_listing(conn, id1)
        assert sorted(listing.sources) == ["jooble", "platsbanken"]

    def test_dedup_case_insensitive(self, conn, sample_listing):
        id1 = upsert_listing(conn, sample_listing)

        dup = sample_listing.model_copy(
            update={
                "title": "  PYTHON DEVELOPER  ",
                "company": "  acme corp  ",
                "source": "remoteok",
                "external_id": "r-1",
            }
        )
        id2 = upsert_listing(conn, dup)

        assert id1 == id2

    def test_different_title_creates_new(self, conn, sample_listing):
        id1 = upsert_listing(conn, sample_listing)

        different = sample_listing.model_copy(
            update={
                "title": "JavaScript Developer",
                "source": "platsbanken",
                "external_id": "ext-2",
            }
        )
        id2 = upsert_listing(conn, different)

        assert id1 != id2


class TestFindDuplicate:
    def test_finds_existing(self, conn, sample_listing):
        upsert_listing(conn, sample_listing)
        result = find_duplicate(conn, "Python Developer", "Acme Corp")
        assert result is not None

    def test_not_found(self, conn):
        result = find_duplicate(conn, "Nonexistent Job", "No Company")
        assert result is None


class TestListListings:
    def test_list_all(self, conn, sample_listing):
        upsert_listing(conn, sample_listing)

        other = sample_listing.model_copy(
            update={
                "title": "JS Dev",
                "source": "remoteok",
                "external_id": "r-2",
            }
        )
        upsert_listing(conn, other)

        listings = list_listings(conn)
        assert len(listings) == 2

    def test_list_with_source_filter(self, conn, sample_listing):
        upsert_listing(conn, sample_listing)

        listings = list_listings(conn, source="platsbanken")
        assert len(listings) == 1

        listings = list_listings(conn, source="jooble")
        assert len(listings) == 0

    def test_list_with_limit(self, conn, sample_listing):
        for i in range(5):
            listing = sample_listing.model_copy(
                update={"title": f"Job {i}", "external_id": f"ext-{i}"}
            )
            upsert_listing(conn, listing)

        listings = list_listings(conn, limit=3)
        assert len(listings) == 3


class TestAnalysis:
    def test_save_and_get(self, conn, sample_listing):
        listing_id = upsert_listing(conn, sample_listing)

        analysis = AnalysisResult(
            listing_id=listing_id,
            match_score=85,
            match_reasons=["Good fit", "Skills match"],
            missing_skills=["Docker"],
            strengths=["Python skills"],
            concerns=["Junior"],
            summary="Solid match",
            application_notes="Apply now",
            profile_hash="abc123",
        )
        save_analysis(conn, analysis)

        result = get_analysis(conn, listing_id)
        assert result is not None
        assert result.match_score == 85
        assert result.match_reasons == ["Good fit", "Skills match"]
        assert result.missing_skills == ["Docker"]
        assert result.strengths == ["Python skills"]
        assert result.concerns == ["Junior"]
        assert result.profile_hash == "abc123"

    def test_upsert_analysis(self, conn, sample_listing):
        listing_id = upsert_listing(conn, sample_listing)

        a1 = AnalysisResult(
            listing_id=listing_id, match_score=50, profile_hash="hash1"
        )
        save_analysis(conn, a1)

        a2 = AnalysisResult(
            listing_id=listing_id, match_score=90, profile_hash="hash1"
        )
        save_analysis(conn, a2)

        result = get_analysis(conn, listing_id)
        assert result.match_score == 90

    def test_get_unanalyzed(self, conn, sample_listing):
        upsert_listing(conn, sample_listing)
        other = sample_listing.model_copy(
            update={"title": "Other Job", "external_id": "ext-99"}
        )
        listing_id2 = upsert_listing(conn, other)

        analysis = AnalysisResult(
            listing_id=listing_id2, match_score=50, profile_hash="hash1"
        )
        save_analysis(conn, analysis)

        unanalyzed = get_unanalyzed_listings(conn, "hash1")
        assert len(unanalyzed) == 1
        assert unanalyzed[0].title == "Python Developer"


class TestAnalysisListSerialization:
    """Test that list fields survive the DB round-trip."""

    def test_empty_lists_round_trip(self, conn, sample_listing):
        listing_id = upsert_listing(conn, sample_listing)
        analysis = AnalysisResult(
            listing_id=listing_id,
            match_score=50,
            match_reasons=[],
            missing_skills=[],
            strengths=[],
            concerns=[],
            profile_hash="hash1",
        )
        save_analysis(conn, analysis)
        result = get_analysis(conn, listing_id)
        assert result.match_reasons == []
        assert result.missing_skills == []
        assert result.strengths == []
        assert result.concerns == []

    def test_multi_item_lists_round_trip(self, conn, sample_listing):
        listing_id = upsert_listing(conn, sample_listing)
        reasons = ["Skill match", "Location match", "Experience level"]
        analysis = AnalysisResult(
            listing_id=listing_id,
            match_score=90,
            match_reasons=reasons,
            missing_skills=["Docker", "Kubernetes"],
            strengths=["Python", "JS"],
            concerns=["Junior", "No CI/CD experience"],
            profile_hash="hash2",
        )
        save_analysis(conn, analysis)
        result = get_analysis(conn, listing_id)
        assert result.match_reasons == reasons
        assert result.missing_skills == ["Docker", "Kubernetes"]
        assert result.strengths == ["Python", "JS"]
        assert result.concerns == ["Junior", "No CI/CD experience"]


class TestStats:
    def test_basic_stats(self, conn, sample_listing):
        id1 = upsert_listing(conn, sample_listing)

        dup = sample_listing.model_copy(
            update={"source": "remoteok", "external_id": "r-1"}
        )
        upsert_listing(conn, dup)

        analysis = AnalysisResult(
            listing_id=id1, match_score=80, profile_hash="hash1"
        )
        save_analysis(conn, analysis)

        stats = get_stats(conn)
        assert stats["total_listings"] == 1
        assert stats["total_source_entries"] == 2
        assert stats["dedup_savings"] == 1
        assert stats["total_analyses"] == 1
        assert stats["avg_score"] == 80.0
        assert stats["source_counts"]["platsbanken"] == 1
        assert stats["source_counts"]["remoteok"] == 1
