"""Tests for web UI routes and JSON API endpoints."""

from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from jobhaul.db.queries import save_analysis, upsert_listing
from jobhaul.db.schema import SCHEMA_SQL
from jobhaul.models import AnalysisResult, RawListing


@pytest.fixture
def db(tmp_path):
    db_path = str(tmp_path / "test.db")
    # Allow cross-thread usage for TestClient (runs async in separate thread)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def client(db):
    from jobhaul.web.app import app

    with patch("jobhaul.web.app._get_db", return_value=db):
        yield TestClient(app)


@pytest.fixture
def seeded_db(db):
    """Insert sample data into the database."""
    listing_id = upsert_listing(
        db,
        RawListing(
            title="Python Developer",
            company="Acme Corp",
            location="Stockholm",
            description="Build Python apps",
            url="https://example.com/1",
            published_at="2024-01-01",
            is_remote=True,
            employment_type="Full-time",
            source="platsbanken",
            external_id="ext-1",
            source_url="https://example.com/1",
        ),
    )
    save_analysis(
        db,
        AnalysisResult(
            listing_id=listing_id,
            match_score=85,
            match_reasons=["Strong Python skills", "Location match"],
            missing_skills=["Docker"],
            strengths=["Python expertise"],
            concerns=["Junior level"],
            summary="Good fit overall",
            application_notes="Apply now",
            profile_hash="abc123",
        ),
    )

    # Add a second listing without analysis
    upsert_listing(
        db,
        RawListing(
            title="JS Developer",
            company="Beta Inc",
            location="Remote",
            description="Build JS apps",
            is_remote=True,
            source="remoteok",
            external_id="ext-2",
        ),
    )
    return db


# --- HTML Route Tests ---


class TestDashboard:
    def test_dashboard_empty(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Dashboard" in resp.text

    def test_dashboard_with_data(self, client, seeded_db):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Python Developer" in resp.text
        assert "85/100" in resp.text


class TestListingsPage:
    def test_listings_empty(self, client):
        resp = client.get("/listings")
        assert resp.status_code == 200
        assert "No listings match" in resp.text

    def test_listings_with_data(self, client, seeded_db):
        resp = client.get("/listings")
        assert resp.status_code == 200
        assert "Python Developer" in resp.text
        assert "JS Developer" in resp.text

    def test_listings_filter_source(self, client, seeded_db):
        resp = client.get("/listings?source=platsbanken")
        assert resp.status_code == 200
        assert "Python Developer" in resp.text

    def test_listings_filter_remote(self, client, seeded_db):
        resp = client.get("/listings?remote_only=true")
        assert resp.status_code == 200

    def test_listings_sort_score(self, client, seeded_db):
        resp = client.get("/listings?sort=score")
        assert resp.status_code == 200

    def test_listings_pagination(self, client, seeded_db):
        resp = client.get("/listings?page=1")
        assert resp.status_code == 200


class TestListingDetail:
    def test_detail_exists(self, client, seeded_db):
        resp = client.get("/listings/1")
        assert resp.status_code == 200
        assert "Python Developer" in resp.text
        assert "Acme Corp" in resp.text
        assert "Strong Python skills" in resp.text  # analysis list items
        assert "85/100" in resp.text

    def test_detail_not_found(self, client):
        resp = client.get("/listings/999")
        assert resp.status_code == 404

    def test_detail_no_analysis(self, client, seeded_db):
        resp = client.get("/listings/2")
        assert resp.status_code == 200
        assert "JS Developer" in resp.text
        assert "No analysis yet" in resp.text


class TestScanPage:
    def test_scan_form(self, client):
        resp = client.get("/scan")
        assert resp.status_code == 200
        assert "Scan" in resp.text


# --- JSON API Tests ---


class TestAPIListings:
    def test_api_listings_empty(self, client):
        resp = client.get("/api/listings")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_api_listings_with_data(self, client, seeded_db):
        resp = client.get("/api/listings")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2

    def test_api_listings_filter_source(self, client, seeded_db):
        resp = client.get("/api/listings?source=platsbanken")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["title"] == "Python Developer"

    def test_api_listings_filter_remote(self, client, seeded_db):
        resp = client.get("/api/listings?remote_only=true")
        assert resp.status_code == 200
        data = resp.json()
        assert all(item["is_remote"] for item in data)

    def test_api_listings_with_limit(self, client, seeded_db):
        resp = client.get("/api/listings?limit=1")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1

    def test_api_listings_includes_analysis(self, client, seeded_db):
        resp = client.get("/api/listings")
        data = resp.json()
        analyzed = [d for d in data if d["analysis"] is not None]
        assert len(analyzed) == 1
        assert analyzed[0]["analysis"]["match_score"] == 85
        assert analyzed[0]["analysis"]["match_reasons"] == ["Strong Python skills", "Location match"]


class TestAPIListingDetail:
    def test_api_detail_exists(self, client, seeded_db):
        resp = client.get("/api/listings/1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["title"] == "Python Developer"
        assert data["analysis"]["match_score"] == 85

    def test_api_detail_not_found(self, client):
        resp = client.get("/api/listings/999")
        assert resp.status_code == 404


class TestAPIStats:
    def test_api_stats_empty(self, client):
        resp = client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_listings"] == 0

    def test_api_stats_with_data(self, client, seeded_db):
        resp = client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_listings"] == 2
        assert data["total_analyses"] == 1
        assert data["avg_score"] == 85.0


# --- _parse_optional_int Tests ---


class TestParseOptionalInt:
    def test_none_returns_none(self):
        from jobhaul.web.app import _parse_optional_int
        assert _parse_optional_int(None) is None

    def test_empty_string_returns_none(self):
        from jobhaul.web.app import _parse_optional_int
        assert _parse_optional_int("") is None

    def test_whitespace_returns_none(self):
        from jobhaul.web.app import _parse_optional_int
        assert _parse_optional_int("   ") is None

    def test_valid_int(self):
        from jobhaul.web.app import _parse_optional_int
        assert _parse_optional_int("42") == 42

    def test_negative_int(self):
        from jobhaul.web.app import _parse_optional_int
        assert _parse_optional_int("-5") == -5

    def test_zero(self):
        from jobhaul.web.app import _parse_optional_int
        assert _parse_optional_int("0") == 0

    def test_float_truncation(self):
        from jobhaul.web.app import _parse_optional_int
        assert _parse_optional_int("3.7") == 3

    def test_float_truncation_negative(self):
        from jobhaul.web.app import _parse_optional_int
        assert _parse_optional_int("-2.9") == -2

    def test_whitespace_padded(self):
        from jobhaul.web.app import _parse_optional_int
        assert _parse_optional_int("  10  ") == 10

    def test_non_numeric_returns_none(self):
        from jobhaul.web.app import _parse_optional_int
        assert _parse_optional_int("abc") is None

    def test_very_large_number(self):
        from jobhaul.web.app import _parse_optional_int
        assert _parse_optional_int("999999999") == 999999999

    def test_mixed_text_returns_none(self):
        from jobhaul.web.app import _parse_optional_int
        assert _parse_optional_int("12abc") is None


# --- Filter Edge Cases (HTTP) ---


class TestFilterEdgeCasesHTTP:
    def test_empty_min_score(self, client, seeded_db):
        resp = client.get("/listings?min_score=")
        assert resp.status_code == 200

    def test_empty_days(self, client, seeded_db):
        resp = client.get("/listings?days=")
        assert resp.status_code == 200

    def test_empty_all_filters(self, client, seeded_db):
        resp = client.get("/listings?min_score=&days=&source=&sort=date")
        assert resp.status_code == 200

    def test_non_numeric_min_score(self, client, seeded_db):
        resp = client.get("/listings?min_score=abc")
        assert resp.status_code == 200

    def test_non_numeric_days(self, client, seeded_db):
        resp = client.get("/listings?days=xyz")
        assert resp.status_code == 200

    def test_negative_min_score(self, client, seeded_db):
        resp = client.get("/listings?min_score=-1")
        assert resp.status_code == 200

    def test_float_min_score(self, client, seeded_db):
        resp = client.get("/listings?min_score=3.5")
        assert resp.status_code == 200

    def test_float_days(self, client, seeded_db):
        resp = client.get("/listings?days=7.5")
        assert resp.status_code == 200

    def test_very_large_days(self, client, seeded_db):
        resp = client.get("/listings?days=99999")
        assert resp.status_code == 200

    def test_sort_score_with_empty_filters(self, client, seeded_db):
        resp = client.get("/listings?sort=score&min_score=&days=")
        assert resp.status_code == 200

    def test_api_limit_zero(self, client, seeded_db):
        resp = client.get("/api/listings?limit=0")
        assert resp.status_code == 200
        data = resp.json()
        # limit=0 is falsy, so no limit applied
        assert len(data) == 2

    def test_api_limit_float(self, client, seeded_db):
        resp = client.get("/api/listings?limit=1.9")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1


# --- Dashboard Analysis Counts ---


class TestDashboardAnalysisCounts:
    def test_dashboard_zero_analyses(self, client, db):
        """Dashboard with no analyses shows no top matches."""
        resp = client.get("/")
        assert resp.status_code == 200
        # No "85/100" or similar score text expected
        assert "/100" not in resp.text

    def test_dashboard_one_analysis(self, client, seeded_db):
        """Dashboard with one analysis shows it in top matches."""
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Python Developer" in resp.text
        assert "85/100" in resp.text

    def test_dashboard_more_than_ten(self, client, db):
        """Dashboard shows at most 10 top matches."""
        # Insert 15 listings with analyses
        for i in range(15):
            lid = upsert_listing(
                db,
                RawListing(
                    title=f"Dev {i}",
                    company=f"Co {i}",
                    description="Coding",
                    source="platsbanken",
                    external_id=f"ext-top-{i}",
                ),
            )
            save_analysis(
                db,
                AnalysisResult(
                    listing_id=lid,
                    match_score=90 - i,
                    summary=f"Match {i}",
                    profile_hash="abc123",
                ),
            )

        resp = client.get("/")
        assert resp.status_code == 200
        # The dashboard query uses limit=10, so at most 10 entries
        text = resp.text
        score_count = sum(1 for s in range(76, 91) if f"{s}/100" in text)
        assert score_count <= 10


class TestAnalysisErrorBadges:
    """Test warning/error badges for failed analyses (Issue #17)."""

    def test_listings_page_shows_warning_for_failed(self, client, db):
        """Listings page shows warning badge for analysis with error."""
        listing_id = upsert_listing(
            db,
            RawListing(
                title="Failed Analysis Job",
                company="ErrorCo",
                description="Test",
                source="platsbanken",
                external_id="ext-fail-1",
            ),
        )
        save_analysis(
            db,
            AnalysisResult(
                listing_id=listing_id,
                match_score=0,
                profile_hash="abc123",
                analysis_error="LLM timeout after 90s",
            ),
        )
        resp = client.get("/listings")
        assert resp.status_code == 200
        # Should contain warning emoji (&#9888; = ⚠)
        assert "&#9888;" in resp.text or "\u26a0" in resp.text

    def test_listings_page_shows_error_for_permanently_failed(self, client, db):
        """Listings page shows error badge for fail_count >= 5."""
        listing_id = upsert_listing(
            db,
            RawListing(
                title="Perm Failed Job",
                company="FailCo",
                description="Test",
                source="platsbanken",
                external_id="ext-permfail-1",
            ),
        )
        # Simulate 5 failures
        for i in range(5):
            save_analysis(
                db,
                AnalysisResult(
                    listing_id=listing_id,
                    match_score=0,
                    profile_hash="abc123",
                    analysis_error=f"timeout {i+1}",
                ),
            )
        resp = client.get("/listings")
        assert resp.status_code == 200
        # Should contain cross mark emoji (&#10060; = ❌)
        assert "&#10060;" in resp.text

    def test_detail_shows_error_message(self, client, db):
        """Detail page shows the actual error message."""
        listing_id = upsert_listing(
            db,
            RawListing(
                title="Error Detail Job",
                company="ErrCo",
                description="Test",
                source="platsbanken",
                external_id="ext-errdetail-1",
            ),
        )
        save_analysis(
            db,
            AnalysisResult(
                listing_id=listing_id,
                match_score=0,
                profile_hash="abc123",
                analysis_error="Connection refused to API",
            ),
        )
        resp = client.get(f"/listings/{listing_id}")
        assert resp.status_code == 200
        assert "Connection refused to API" in resp.text
        assert "Analysis Error" in resp.text

    def test_detail_shows_fail_count(self, client, db):
        """Detail page shows fail count when > 0."""
        listing_id = upsert_listing(
            db,
            RawListing(
                title="Fail Count Job",
                company="CountCo",
                description="Test",
                source="platsbanken",
                external_id="ext-fc-1",
            ),
        )
        # 3 failures
        for i in range(3):
            save_analysis(
                db,
                AnalysisResult(
                    listing_id=listing_id,
                    match_score=0,
                    profile_hash="abc123",
                    analysis_error=f"timeout {i+1}",
                ),
            )
        resp = client.get(f"/listings/{listing_id}")
        assert resp.status_code == 200
        assert "Fail Count" in resp.text
        assert "3/5" in resp.text

    def test_dashboard_shows_warning_badge(self, client, db):
        """Dashboard shows warning badge for failed analysis in top matches."""
        listing_id = upsert_listing(
            db,
            RawListing(
                title="Dashboard Warn Job",
                company="WarnCo",
                description="Test",
                source="platsbanken",
                external_id="ext-dashwarn-1",
            ),
        )
        save_analysis(
            db,
            AnalysisResult(
                listing_id=listing_id,
                match_score=1,
                profile_hash="abc123",
                analysis_error="timeout",
            ),
        )
        resp = client.get("/")
        assert resp.status_code == 200
        # Warning badge should show for analysis with error
        assert "&#9888;" in resp.text or "\u26a0" in resp.text


# --- Deadline / Expired status tests (Issue #14) ---


class TestDeadlineDisplay:
    def test_listings_page_shows_deadline(self, client, db):
        """Listings page shows application_deadline when set."""
        upsert_listing(
            db,
            RawListing(
                title="Deadline Job",
                company="DeadlineCo",
                description="Test",
                source="platsbanken",
                external_id="ext-deadline-1",
                application_deadline="2025-06-15",
            ),
        )
        resp = client.get("/listings?include_expired=true")
        assert resp.status_code == 200
        assert "2025-06-15" in resp.text

    def test_detail_shows_deadline(self, client, db):
        """Detail page shows application_deadline."""
        listing_id = upsert_listing(
            db,
            RawListing(
                title="Deadline Detail Job",
                company="DeadlineCo",
                description="Test",
                source="platsbanken",
                external_id="ext-deadline-2",
                application_deadline="2025-12-31",
            ),
        )
        resp = client.get(f"/listings/{listing_id}")
        assert resp.status_code == 200
        assert "2025-12-31" in resp.text
        assert "Application Deadline" in resp.text

    def test_expired_filter_excludes_by_default(self, client, db):
        """Default filter should only show active listings."""
        from jobhaul.db.queries import mark_likely_expired

        id1 = upsert_listing(
            db,
            RawListing(
                title="Active Job",
                company="ActiveCo",
                description="Active",
                source="platsbanken",
                external_id="ext-active-1",
            ),
        )
        id2 = upsert_listing(
            db,
            RawListing(
                title="Expired Job",
                company="ExpiredCo",
                description="Expired",
                source="platsbanken",
                external_id="ext-expired-1",
            ),
        )
        mark_likely_expired(db, id2)

        resp = client.get("/listings")
        assert resp.status_code == 200
        assert "Active Job" in resp.text
        assert "Expired Job" not in resp.text

    def test_expired_filter_includes_when_checked(self, client, db):
        """Include expired checkbox shows expired listings."""
        from jobhaul.db.queries import mark_likely_expired

        id1 = upsert_listing(
            db,
            RawListing(
                title="Active Job 2",
                company="ActiveCo",
                description="Active",
                source="platsbanken",
                external_id="ext-active-2",
            ),
        )
        id2 = upsert_listing(
            db,
            RawListing(
                title="Expired Job 2",
                company="ExpiredCo",
                description="Expired",
                source="platsbanken",
                external_id="ext-expired-2",
            ),
        )
        mark_likely_expired(db, id2)

        resp = client.get("/listings?include_expired=true")
        assert resp.status_code == 200
        assert "Expired Job 2" in resp.text

    def test_detail_shows_status_badge_expired(self, client, db):
        """Detail page shows status badge for expired listings."""
        from jobhaul.db.queries import mark_likely_expired

        listing_id = upsert_listing(
            db,
            RawListing(
                title="Status Badge Job",
                company="BadgeCo",
                description="Test",
                source="platsbanken",
                external_id="ext-badge-1",
            ),
        )
        mark_likely_expired(db, listing_id)

        resp = client.get(f"/listings/{listing_id}")
        assert resp.status_code == 200
        # Should show clock emoji for likely_expired (&#128336; = 🕐)
        assert "Likely Expired" in resp.text
