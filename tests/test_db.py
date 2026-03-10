"""Tests for database operations."""

from __future__ import annotations

import sqlite3

import pytest

from jobhaul.db.queries import (
    _compute_dedup_key,
    _normalize_for_dedup,
    find_duplicate,
    get_analysis,
    get_failed_listings,
    get_listing,
    get_stats,
    get_unanalyzed_listings,
    list_listings,
    merge_existing_duplicates,
    save_analysis,
    upsert_listing,
)
from jobhaul.db.schema import SCHEMA_VERSION, init_db, run_migrations
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


class TestNormalizeForDedup:
    """Test improved dedup normalization (Issue #1)."""

    def test_trailing_space(self):
        assert _normalize_for_dedup("Python Developer  ") == "python developer"

    def test_leading_space(self):
        assert _normalize_for_dedup("  Python Developer") == "python developer"

    def test_mixed_case(self):
        assert _normalize_for_dedup("PYTHON Developer") == "python developer"

    def test_non_breaking_space(self):
        assert _normalize_for_dedup("Python\u00a0Developer") == "python developer"

    def test_tabs_and_newlines(self):
        assert _normalize_for_dedup("Python\t\nDeveloper") == "python developer"

    def test_multiple_spaces_collapsed(self):
        assert _normalize_for_dedup("Python    Developer") == "python developer"

    def test_empty_string(self):
        assert _normalize_for_dedup("") == ""

    def test_none(self):
        assert _normalize_for_dedup(None) == ""

    def test_unicode_whitespace(self):
        # \u2003 is em space, \u200b is zero-width space
        assert _normalize_for_dedup("Python\u2003Developer\u200b") == "python developer"


class TestDedupWithWhitespace:
    """Test that dedup catches whitespace/case variations (Issue #1)."""

    def test_dedup_trailing_space(self, conn, sample_listing):
        id1 = upsert_listing(conn, sample_listing)
        dup = sample_listing.model_copy(
            update={
                "title": "Python Developer ",
                "source": "jooble",
                "external_id": "j-1",
            }
        )
        id2 = upsert_listing(conn, dup)
        assert id1 == id2

    def test_dedup_non_breaking_space(self, conn, sample_listing):
        id1 = upsert_listing(conn, sample_listing)
        dup = sample_listing.model_copy(
            update={
                "title": "Python\u00a0Developer",
                "source": "jooble",
                "external_id": "j-2",
            }
        )
        id2 = upsert_listing(conn, dup)
        assert id1 == id2

    def test_dedup_tabs(self, conn, sample_listing):
        id1 = upsert_listing(conn, sample_listing)
        dup = sample_listing.model_copy(
            update={
                "title": "Python\tDeveloper",
                "source": "remoteok",
                "external_id": "r-3",
            }
        )
        id2 = upsert_listing(conn, dup)
        assert id1 == id2


class TestMergeExistingDuplicates:
    """Test duplicate merge on DB init (Issue #1)."""

    def test_merge_duplicates(self, tmp_path):
        import sqlite3

        from jobhaul.db.schema import SCHEMA_SQL

        db_path = str(tmp_path / "test_merge.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.executescript(SCHEMA_SQL)
        conn.commit()

        # Insert two listings that should be duplicates
        conn.execute(
            "INSERT INTO listings (id, title, company, created_at) VALUES (1, 'Python Dev', 'Acme', datetime('now'))"
        )
        conn.execute(
            "INSERT INTO listings (id, title, company, created_at) VALUES (2, 'python dev', 'acme', datetime('now'))"
        )
        conn.execute(
            "INSERT INTO listing_sources (listing_id, source, external_id) VALUES (1, 'platsbanken', 'e1')"
        )
        conn.execute(
            "INSERT INTO listing_sources (listing_id, source, external_id) VALUES (2, 'jooble', 'e2')"
        )
        conn.commit()

        merged = merge_existing_duplicates(conn)
        assert merged == 1

        # Only one listing should remain
        count = conn.execute("SELECT COUNT(*) as c FROM listings").fetchone()["c"]
        assert count == 1

        # Both sources should be on the remaining listing
        sources = conn.execute(
            "SELECT source FROM listing_sources WHERE listing_id = 1"
        ).fetchall()
        source_names = {r["source"] for r in sources}
        assert source_names == {"platsbanken", "jooble"}

        conn.close()


class TestListSortByScore:
    """Test sort_by_score in list_listings (Issue #8)."""

    def test_sort_by_score_desc(self, conn, sample_listing):
        # Insert three listings with different scores
        id1 = upsert_listing(conn, sample_listing)
        id2 = upsert_listing(
            conn,
            sample_listing.model_copy(
                update={"title": "React Dev", "external_id": "ext-2"}
            ),
        )
        id3 = upsert_listing(
            conn,
            sample_listing.model_copy(
                update={"title": "Go Dev", "external_id": "ext-3"}
            ),
        )

        save_analysis(conn, AnalysisResult(listing_id=id1, match_score=50, profile_hash="h"))
        save_analysis(conn, AnalysisResult(listing_id=id2, match_score=90, profile_hash="h"))
        save_analysis(conn, AnalysisResult(listing_id=id3, match_score=70, profile_hash="h"))

        listings = list_listings(conn, sort_by_score=True)
        scores = [
            get_analysis(conn, l.id).match_score for l in listings
        ]
        assert scores == [90, 70, 50]

    def test_no_analysis_at_bottom(self, conn, sample_listing):
        id1 = upsert_listing(conn, sample_listing)
        id2 = upsert_listing(
            conn,
            sample_listing.model_copy(
                update={"title": "React Dev", "external_id": "ext-2"}
            ),
        )

        # Only analyze listing 2
        save_analysis(conn, AnalysisResult(listing_id=id2, match_score=80, profile_hash="h"))

        listings = list_listings(conn, sort_by_score=True)
        assert listings[0].id == id2  # Analyzed listing first
        assert listings[1].id == id1  # No analysis at bottom


class TestListListingsSources:
    """Test that sources are properly populated (Issue #3)."""

    def test_sources_populated_in_list(self, conn, sample_listing):
        upsert_listing(conn, sample_listing)
        dup = sample_listing.model_copy(
            update={"source": "remoteok", "external_id": "r-1"}
        )
        upsert_listing(conn, dup)

        listings = list_listings(conn)
        assert len(listings) == 1
        assert sorted(listings[0].sources) == ["platsbanken", "remoteok"]

    def test_single_source(self, conn, sample_listing):
        upsert_listing(conn, sample_listing)
        listings = list_listings(conn)
        assert len(listings) == 1
        assert listings[0].sources == ["platsbanken"]


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
        assert stats.total_listings == 1
        assert stats.total_source_entries == 2
        assert stats.dedup_savings == 1
        assert stats.total_analyses == 1
        assert stats.avg_score == 80.0
        assert stats.source_counts["platsbanken"] == 1
        assert stats.source_counts["remoteok"] == 1

    def test_stats_is_pydantic_model(self, conn, sample_listing):
        """Stats return value is a typed Pydantic model, not a plain dict."""
        from jobhaul.models import Stats

        upsert_listing(conn, sample_listing)
        stats = get_stats(conn)
        assert isinstance(stats, Stats)
        # Verify it can be serialized to dict/JSON
        d = stats.model_dump()
        assert "total_listings" in d
        assert "source_counts" in d

    def test_stats_empty_db(self, conn):
        """Stats work on an empty database."""
        stats = get_stats(conn)
        assert stats.total_listings == 0
        assert stats.total_analyses == 0
        assert stats.avg_score == 0
        assert stats.source_counts == {}


# -- Schema migration tests (Issue #16) ----------------------------------------

# V1 schema: analyses table WITHOUT analysis_error column
V1_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    company TEXT,
    location TEXT,
    description TEXT,
    url TEXT,
    published_at TEXT,
    is_remote INTEGER DEFAULT 0,
    employment_type TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS listing_sources (
    listing_id INTEGER NOT NULL REFERENCES listings(id),
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    source_url TEXT,
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source, external_id)
);

CREATE TABLE IF NOT EXISTS analyses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id INTEGER NOT NULL REFERENCES listings(id),
    match_score INTEGER NOT NULL,
    match_reasons TEXT,
    missing_skills TEXT,
    strengths TEXT,
    concerns TEXT,
    summary TEXT,
    application_notes TEXT,
    profile_hash TEXT NOT NULL,
    analyzed_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(listing_id, profile_hash)
);

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TEXT DEFAULT (datetime('now'))
);
"""


def _make_v1_db(db_path: str) -> sqlite3.Connection:
    """Create a DB with V1 schema (no analysis_error column)."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(V1_SCHEMA_SQL)
    conn.commit()
    return conn


class TestMigrations:
    """Test schema migration mechanism (Issue #16)."""

    def test_fresh_db_at_current_version(self, tmp_path):
        db_path = str(tmp_path / "fresh.db")
        conn = init_db(db_path)
        version = conn.execute(
            "SELECT COALESCE(MAX(version), 1) FROM schema_version"
        ).fetchone()[0]
        assert version == SCHEMA_VERSION
        conn.close()

    def test_migrate_v1_adds_analysis_error(self, tmp_path):
        db_path = str(tmp_path / "v1.db")
        conn = _make_v1_db(db_path)

        # Confirm column does NOT exist yet
        cols = [
            r[1] for r in conn.execute("PRAGMA table_info(analyses)").fetchall()
        ]
        assert "analysis_error" not in cols

        run_migrations(conn)

        # Column should now exist
        cols = [
            r[1] for r in conn.execute("PRAGMA table_info(analyses)").fetchall()
        ]
        assert "analysis_error" in cols

        # Version should be bumped
        version = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        assert version == SCHEMA_VERSION
        conn.close()

    def test_migrate_v1_adds_fail_count(self, tmp_path):
        db_path = str(tmp_path / "v1_fc.db")
        conn = _make_v1_db(db_path)

        run_migrations(conn)

        cols = [
            r[1] for r in conn.execute("PRAGMA table_info(analyses)").fetchall()
        ]
        assert "fail_count" in cols
        conn.close()

    def test_migrations_idempotent(self, tmp_path):
        db_path = str(tmp_path / "idem.db")
        conn = _make_v1_db(db_path)

        run_migrations(conn)
        run_migrations(conn)  # second run should be a no-op

        cols = [
            r[1] for r in conn.execute("PRAGMA table_info(analyses)").fetchall()
        ]
        assert cols.count("analysis_error") == 1
        assert cols.count("fail_count") == 1

        version = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        assert version == SCHEMA_VERSION
        conn.close()

    def test_save_analysis_after_migration_error_none(self, tmp_path):
        db_path = str(tmp_path / "migrated.db")
        conn = _make_v1_db(db_path)
        run_migrations(conn)

        conn.execute(
            "INSERT INTO listings (title, company, created_at) VALUES ('Dev', 'Co', datetime('now'))"
        )
        conn.commit()
        listing_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        result = AnalysisResult(
            listing_id=listing_id, match_score=75, profile_hash="h1",
            analysis_error=None,
        )
        save_analysis(conn, result)

        row = get_analysis(conn, listing_id)
        assert row is not None
        assert row.analysis_error is None
        conn.close()

    def test_save_analysis_after_migration_error_set(self, tmp_path):
        db_path = str(tmp_path / "migrated2.db")
        conn = _make_v1_db(db_path)
        run_migrations(conn)

        conn.execute(
            "INSERT INTO listings (title, company, created_at) VALUES ('Dev', 'Co', datetime('now'))"
        )
        conn.commit()
        listing_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        result = AnalysisResult(
            listing_id=listing_id, match_score=0, profile_hash="h2",
            analysis_error="LLM timeout after 60s",
        )
        save_analysis(conn, result)

        row = get_analysis(conn, listing_id)
        assert row is not None
        assert row.analysis_error == "LLM timeout after 60s"
        conn.close()


# V2 schema: analyses table WITH analysis_error but WITHOUT fail_count
V2_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    company TEXT,
    location TEXT,
    description TEXT,
    url TEXT,
    published_at TEXT,
    is_remote INTEGER DEFAULT 0,
    employment_type TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS listing_sources (
    listing_id INTEGER NOT NULL REFERENCES listings(id),
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    source_url TEXT,
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source, external_id)
);

CREATE TABLE IF NOT EXISTS analyses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id INTEGER NOT NULL REFERENCES listings(id),
    match_score INTEGER NOT NULL,
    match_reasons TEXT,
    missing_skills TEXT,
    strengths TEXT,
    concerns TEXT,
    summary TEXT,
    application_notes TEXT,
    analysis_error TEXT,
    profile_hash TEXT NOT NULL,
    analyzed_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(listing_id, profile_hash)
);

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TEXT DEFAULT (datetime('now'))
);
"""


def _make_v2_db(db_path: str) -> sqlite3.Connection:
    """Create a DB with V2 schema (has analysis_error, no fail_count)."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(V2_SCHEMA_SQL)
    conn.execute("INSERT INTO schema_version (version) VALUES (2)")
    conn.commit()
    return conn


class TestMigrateV2ToV3:
    """Test v2 -> v3 migration adds fail_count column."""

    def test_migrate_v2_adds_fail_count(self, tmp_path):
        db_path = str(tmp_path / "v2.db")
        conn = _make_v2_db(db_path)

        cols = [r[1] for r in conn.execute("PRAGMA table_info(analyses)").fetchall()]
        assert "analysis_error" in cols
        assert "fail_count" not in cols

        run_migrations(conn)

        cols = [r[1] for r in conn.execute("PRAGMA table_info(analyses)").fetchall()]
        assert "fail_count" in cols

        version = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        assert version == SCHEMA_VERSION
        conn.close()


# -- Retry failed analyses tests (Issue #17) ------------------------------------


class TestRetryFailedAnalyses:
    """Test that failed analyses are re-queued and fail_count tracks correctly."""

    def test_unanalyzed_does_not_return_successful(self, conn, sample_listing):
        """Listings with successful analysis should NOT appear in unanalyzed."""
        listing_id = upsert_listing(conn, sample_listing)
        save_analysis(conn, AnalysisResult(
            listing_id=listing_id, match_score=85, profile_hash="h1",
        ))
        unanalyzed = get_unanalyzed_listings(conn, "h1")
        assert len(unanalyzed) == 0

    def test_unanalyzed_returns_failed(self, conn, sample_listing):
        """Listings with analysis_error set SHOULD appear in unanalyzed."""
        listing_id = upsert_listing(conn, sample_listing)
        save_analysis(conn, AnalysisResult(
            listing_id=listing_id, match_score=0, profile_hash="h1",
            analysis_error="LLM timeout",
        ))
        unanalyzed = get_unanalyzed_listings(conn, "h1")
        assert len(unanalyzed) == 1
        assert unanalyzed[0].id == listing_id

    def test_unanalyzed_excludes_permanently_failed(self, conn, sample_listing):
        """Listings with fail_count >= 5 should NOT appear in unanalyzed."""
        listing_id = upsert_listing(conn, sample_listing)
        # Simulate 5 failures by saving 5 times with error
        for i in range(5):
            save_analysis(conn, AnalysisResult(
                listing_id=listing_id, match_score=0, profile_hash="h1",
                analysis_error=f"LLM timeout attempt {i+1}",
            ))
        # Verify fail_count is 5
        analysis = get_analysis(conn, listing_id)
        assert analysis.fail_count == 5

        unanalyzed = get_unanalyzed_listings(conn, "h1")
        assert len(unanalyzed) == 0

    def test_fail_count_increments(self, conn, sample_listing):
        """fail_count should increment on each failed save."""
        listing_id = upsert_listing(conn, sample_listing)

        # First failure
        save_analysis(conn, AnalysisResult(
            listing_id=listing_id, match_score=0, profile_hash="h1",
            analysis_error="timeout 1",
        ))
        assert get_analysis(conn, listing_id).fail_count == 1

        # Second failure
        save_analysis(conn, AnalysisResult(
            listing_id=listing_id, match_score=0, profile_hash="h1",
            analysis_error="timeout 2",
        ))
        assert get_analysis(conn, listing_id).fail_count == 2

        # Third failure
        save_analysis(conn, AnalysisResult(
            listing_id=listing_id, match_score=0, profile_hash="h1",
            analysis_error="timeout 3",
        ))
        assert get_analysis(conn, listing_id).fail_count == 3

    def test_successful_retry_clears_error(self, conn, sample_listing):
        """A successful analysis after failures should clear the error."""
        listing_id = upsert_listing(conn, sample_listing)

        # First: failure
        save_analysis(conn, AnalysisResult(
            listing_id=listing_id, match_score=0, profile_hash="h1",
            analysis_error="timeout",
        ))
        assert get_analysis(conn, listing_id).fail_count == 1
        assert get_analysis(conn, listing_id).analysis_error == "timeout"

        # Second: success (no analysis_error)
        save_analysis(conn, AnalysisResult(
            listing_id=listing_id, match_score=75, profile_hash="h1",
            analysis_error=None,
        ))
        result = get_analysis(conn, listing_id)
        assert result.analysis_error is None
        assert result.match_score == 75
        # fail_count is preserved from the model default (0) since no error
        assert result.fail_count == 0

    def test_get_failed_listings_returns_only_failed(self, conn, sample_listing):
        """get_failed_listings should only return listings with analysis_error."""
        id1 = upsert_listing(conn, sample_listing)
        id2 = upsert_listing(conn, sample_listing.model_copy(
            update={"title": "Other Job", "external_id": "ext-2"}
        ))
        id3 = upsert_listing(conn, sample_listing.model_copy(
            update={"title": "Third Job", "external_id": "ext-3"}
        ))

        # id1: successful
        save_analysis(conn, AnalysisResult(
            listing_id=id1, match_score=80, profile_hash="h1",
        ))
        # id2: failed
        save_analysis(conn, AnalysisResult(
            listing_id=id2, match_score=0, profile_hash="h1",
            analysis_error="API overloaded",
        ))
        # id3: no analysis at all

        failed = get_failed_listings(conn, "h1")
        assert len(failed) == 1
        assert failed[0].id == id2

    def test_get_failed_listings_excludes_permanently_failed(self, conn, sample_listing):
        """get_failed_listings should not return listings with fail_count >= 5."""
        listing_id = upsert_listing(conn, sample_listing)
        for i in range(5):
            save_analysis(conn, AnalysisResult(
                listing_id=listing_id, match_score=0, profile_hash="h1",
                analysis_error=f"timeout {i+1}",
            ))
        failed = get_failed_listings(conn, "h1")
        assert len(failed) == 0


# -- Deadline tracking tests (Issue #14) ----------------------------------------


class TestDeadlineTracking:
    """Test application_deadline and listing_status columns."""

    def test_upsert_stores_deadline(self, conn, sample_listing):
        """upsert_listing() should store application_deadline correctly."""
        listing_with_deadline = sample_listing.model_copy(
            update={"application_deadline": "2025-06-15"}
        )
        listing_id = upsert_listing(conn, listing_with_deadline)
        listing = get_listing(conn, listing_id)
        assert listing.application_deadline == "2025-06-15"

    def test_upsert_stores_status(self, conn, sample_listing):
        """upsert_listing() should store listing_status correctly."""
        listing_with_status = sample_listing.model_copy(
            update={"listing_status": "active"}
        )
        listing_id = upsert_listing(conn, listing_with_status)
        listing = get_listing(conn, listing_id)
        assert listing.listing_status == "active"

    def test_mark_likely_expired(self, conn, sample_listing):
        """mark_likely_expired() should update status."""
        from jobhaul.db.queries import mark_likely_expired

        listing_id = upsert_listing(conn, sample_listing)
        mark_likely_expired(conn, listing_id)
        listing = get_listing(conn, listing_id)
        assert listing.listing_status == "likely_expired"

    def test_mark_confirmed_expired(self, conn, sample_listing):
        """mark_confirmed_expired() should update status."""
        from jobhaul.db.queries import mark_confirmed_expired

        listing_id = upsert_listing(conn, sample_listing)
        mark_confirmed_expired(conn, listing_id)
        listing = get_listing(conn, listing_id)
        assert listing.listing_status == "confirmed_expired"

    def test_list_excludes_expired_by_default(self, conn, sample_listing):
        """list_listings() should exclude expired by default."""
        from jobhaul.db.queries import mark_likely_expired

        id1 = upsert_listing(conn, sample_listing)
        id2 = upsert_listing(conn, sample_listing.model_copy(
            update={"title": "Active Job", "external_id": "ext-active"}
        ))
        mark_likely_expired(conn, id1)

        listings = list_listings(conn)
        assert len(listings) == 1
        assert listings[0].id == id2

    def test_list_includes_expired_when_requested(self, conn, sample_listing):
        """list_listings(include_expired=True) should include expired."""
        from jobhaul.db.queries import mark_likely_expired

        id1 = upsert_listing(conn, sample_listing)
        id2 = upsert_listing(conn, sample_listing.model_copy(
            update={"title": "Active Job", "external_id": "ext-active"}
        ))
        mark_likely_expired(conn, id1)

        listings = list_listings(conn, include_expired=True)
        assert len(listings) == 2

    def test_deadline_none_by_default(self, conn, sample_listing):
        """Listings without deadline should have None."""
        listing_id = upsert_listing(conn, sample_listing)
        listing = get_listing(conn, listing_id)
        assert listing.application_deadline is None

    def test_default_status_is_active(self, conn, sample_listing):
        """Default listing_status should be 'active'."""
        listing_id = upsert_listing(conn, sample_listing)
        listing = get_listing(conn, listing_id)
        assert listing.listing_status == "active"


# V3 schema: analyses with analysis_error and fail_count, but listings WITHOUT deadline columns
V3_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    company TEXT,
    location TEXT,
    description TEXT,
    url TEXT,
    published_at TEXT,
    is_remote INTEGER DEFAULT 0,
    employment_type TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS listing_sources (
    listing_id INTEGER NOT NULL REFERENCES listings(id),
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    source_url TEXT,
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source, external_id)
);

CREATE TABLE IF NOT EXISTS analyses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id INTEGER NOT NULL REFERENCES listings(id),
    match_score INTEGER NOT NULL,
    match_reasons TEXT,
    missing_skills TEXT,
    strengths TEXT,
    concerns TEXT,
    summary TEXT,
    application_notes TEXT,
    analysis_error TEXT,
    fail_count INTEGER NOT NULL DEFAULT 0,
    profile_hash TEXT NOT NULL,
    analyzed_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(listing_id, profile_hash)
);

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TEXT DEFAULT (datetime('now'))
);
"""


def _make_v3_db(db_path: str) -> sqlite3.Connection:
    """Create a DB with V3 schema (no deadline columns)."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(V3_SCHEMA_SQL)
    conn.execute("INSERT INTO schema_version (version) VALUES (3)")
    conn.commit()
    return conn


class TestComputeDedupKey:
    """Test _compute_dedup_key() correctness."""

    def test_deterministic(self):
        key1 = _compute_dedup_key("Python Developer", "Acme Corp")
        key2 = _compute_dedup_key("Python Developer", "Acme Corp")
        assert key1 == key2

    def test_case_insensitive(self):
        key1 = _compute_dedup_key("Python Developer", "Acme Corp")
        key2 = _compute_dedup_key("PYTHON DEVELOPER", "ACME CORP")
        assert key1 == key2

    def test_whitespace_insensitive(self):
        key1 = _compute_dedup_key("Python Developer", "Acme Corp")
        key2 = _compute_dedup_key("  Python  Developer  ", "  Acme  Corp  ")
        assert key1 == key2

    def test_non_breaking_space(self):
        key1 = _compute_dedup_key("Python Developer", "Acme Corp")
        key2 = _compute_dedup_key("Python\u00a0Developer", "Acme\u00a0Corp")
        assert key1 == key2

    def test_different_titles_differ(self):
        key1 = _compute_dedup_key("Python Developer", "Acme Corp")
        key2 = _compute_dedup_key("JavaScript Developer", "Acme Corp")
        assert key1 != key2

    def test_different_companies_differ(self):
        key1 = _compute_dedup_key("Python Developer", "Acme Corp")
        key2 = _compute_dedup_key("Python Developer", "Beta Inc")
        assert key1 != key2

    def test_none_company(self):
        key1 = _compute_dedup_key("Python Developer", None)
        key2 = _compute_dedup_key("Python Developer", None)
        assert key1 == key2

    def test_none_vs_empty_company(self):
        key1 = _compute_dedup_key("Python Developer", None)
        key2 = _compute_dedup_key("Python Developer", "")
        assert key1 == key2

    def test_returns_hex_string(self):
        key = _compute_dedup_key("Test", "Co")
        assert len(key) == 64  # SHA-256 hex digest
        assert all(c in "0123456789abcdef" for c in key)


class TestMigrateV3ToV5:
    """Test v3 -> v5 migration adds deadline columns."""

    def test_migrate_v3_adds_deadline_columns(self, tmp_path):
        db_path = str(tmp_path / "v3.db")
        conn = _make_v3_db(db_path)

        cols = [r[1] for r in conn.execute("PRAGMA table_info(listings)").fetchall()]
        assert "application_deadline" not in cols
        assert "listing_status" not in cols

        run_migrations(conn)

        cols = [r[1] for r in conn.execute("PRAGMA table_info(listings)").fetchall()]
        assert "application_deadline" in cols
        assert "listing_status" in cols

        version = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        assert version == SCHEMA_VERSION
        conn.close()
