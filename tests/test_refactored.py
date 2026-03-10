"""Tests for refactored code: get_db, ensure_collectors_registered, service layer."""

from __future__ import annotations

import sqlite3
from unittest.mock import AsyncMock, patch

import pytest

from jobhaul.db.schema import SCHEMA_SQL
from jobhaul.models import CollectorResult, Profile, RawListing


# --- get_db() context manager ---


class TestGetDbContextManager:
    """Test the shared get_db() context manager."""

    def test_yields_connection(self, tmp_path):
        from jobhaul.db import get_db

        db_path = tmp_path / "test.db"
        with get_db(db_path) as conn:
            assert isinstance(conn, sqlite3.Connection)
            # Should be usable
            conn.execute("SELECT 1")

    def test_closes_connection_on_exit(self, tmp_path):
        from jobhaul.db import get_db

        db_path = tmp_path / "test.db"
        with get_db(db_path) as conn:
            pass
        # Connection should be closed — executing should fail
        with pytest.raises(Exception):
            conn.execute("SELECT 1")

    def test_closes_connection_on_exception(self, tmp_path):
        from jobhaul.db import get_db

        db_path = tmp_path / "test.db"
        try:
            with get_db(db_path) as conn:
                raise ValueError("test error")
        except ValueError:
            pass
        with pytest.raises(Exception):
            conn.execute("SELECT 1")

    def test_initializes_schema(self, tmp_path):
        from jobhaul.db import get_db

        db_path = tmp_path / "test.db"
        with get_db(db_path) as conn:
            # Should have the listings table
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            table_names = {r[0] for r in tables}
            assert "listings" in table_names
            assert "listing_sources" in table_names
            assert "analyses" in table_names


# --- ensure_collectors_registered() ---


class TestEnsureCollectorsRegistered:
    """Test centralized collector registration."""

    def test_registers_core_collectors(self):
        from jobhaul.collectors import ensure_collectors_registered
        from jobhaul.collectors.registry import _registry

        ensure_collectors_registered()

        # Core collectors should always be present
        assert "platsbanken" in _registry
        assert "jooble" in _registry
        assert "remoteok" in _registry

    def test_idempotent(self):
        from jobhaul.collectors import ensure_collectors_registered
        from jobhaul.collectors.registry import _registry

        ensure_collectors_registered()
        count_before = len(_registry)
        ensure_collectors_registered()
        assert len(_registry) == count_before

    def test_optional_collectors_dont_fail(self):
        """Calling ensure_collectors_registered() should not raise even if optional collectors are missing."""
        from jobhaul.collectors import ensure_collectors_registered

        # Should not raise
        ensure_collectors_registered()


# --- service.collect_listings() ---


@pytest.fixture
def service_db(tmp_path):
    db_path = str(tmp_path / "test_service.db")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def minimal_profile():
    return Profile(
        name="test",
        roles=["Developer"],
        search_terms=["python"],
        skills=["Python"],
        location="Stockholm",
    )


class TestServiceCollectListings:
    """Test service.collect_listings() basic flow."""

    @pytest.mark.asyncio
    async def test_collects_from_mock_collector(self, service_db, minimal_profile):
        from jobhaul.service import collect_listings

        raw = RawListing(
            title="Test Job",
            company="TestCo",
            description="A test job",
            source="platsbanken",
            external_id="test-1",
        )
        mock_result = CollectorResult(source="platsbanken", listings=[raw])

        mock_collector = AsyncMock()
        mock_collector.name = "platsbanken"
        mock_collector.collect.return_value = mock_result

        with patch("jobhaul.collectors.ensure_collectors_registered"), \
             patch("jobhaul.collectors.registry.get_all_collectors", return_value=[mock_collector]):
            total, skipped, ids = await collect_listings(minimal_profile, service_db)

        assert total == 1
        assert skipped == 0
        assert "test-1" in ids

        # Verify it was actually inserted
        row = service_db.execute("SELECT * FROM listings WHERE id = 1").fetchone()
        assert row["title"] == "Test Job"

    @pytest.mark.asyncio
    async def test_collects_from_specific_source(self, service_db, minimal_profile):
        from jobhaul.service import collect_listings

        raw = RawListing(
            title="Specific Job",
            company="SpecCo",
            description="Specific",
            source="jooble",
            external_id="spec-1",
        )
        mock_result = CollectorResult(source="jooble", listings=[raw])

        mock_collector = AsyncMock()
        mock_collector.name = "jooble"
        mock_collector.collect.return_value = mock_result

        with patch("jobhaul.collectors.ensure_collectors_registered"), \
             patch("jobhaul.collectors.registry.get_collector", return_value=mock_collector):
            total, skipped, ids = await collect_listings(
                minimal_profile, service_db, source="jooble"
            )

        assert total == 1
        assert "spec-1" in ids

    @pytest.mark.asyncio
    async def test_empty_collection(self, service_db, minimal_profile):
        from jobhaul.service import collect_listings

        mock_result = CollectorResult(source="platsbanken", listings=[])

        mock_collector = AsyncMock()
        mock_collector.name = "platsbanken"
        mock_collector.collect.return_value = mock_result

        with patch("jobhaul.collectors.ensure_collectors_registered"), \
             patch("jobhaul.collectors.registry.get_all_collectors", return_value=[mock_collector]):
            total, skipped, ids = await collect_listings(minimal_profile, service_db)

        assert total == 0
        assert skipped == 0
        assert len(ids) == 0
