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
