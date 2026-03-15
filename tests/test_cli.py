"""Tests for CLI commands."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager, contextmanager
from unittest.mock import AsyncMock, patch

import pytest
from typer.testing import CliRunner

from jobhaul.cli import app
from jobhaul.models import AnalysisResult, Profile, RawListing

runner = CliRunner()


@pytest.fixture
def mock_db(tmp_path):
    """Provide a temp DB and patch get_db and async_get_db context managers."""
    from jobhaul.db.schema import init_db

    db_path = str(tmp_path / "test.db")
    conn = init_db(db_path)

    @contextmanager
    def fake_get_db(db_path=None):
        yield conn

    @asynccontextmanager
    async def fake_async_get_db(db_path=None):
        yield conn

    with patch("jobhaul.cli.get_db", fake_get_db), \
         patch("jobhaul.cli.async_get_db", fake_async_get_db):
        yield conn

    conn.close()


@pytest.fixture
def mock_profile():
    """Mock load_profile to return a test profile."""
    profile = Profile(
        name="Test",
        roles=["developer"],
        search_terms=["python"],
        skills=["Python"],
        location="Stockholm",
    )
    with patch("jobhaul.cli.load_profile", return_value=profile):
        yield profile


@pytest.fixture
def profile_file(tmp_path):
    import yaml

    data = {
        "name": "Test",
        "roles": ["developer"],
        "search_terms": ["python"],
        "skills": ["Python"],
        "location": "Stockholm",
        "sources": {
            "platsbanken": {"enabled": True, "region": "abc"},
            "remoteok": {"enabled": False},
            "jooble": {"enabled": False},
        },
        "llm": {"adapter": "claude-cli", "model": "claude-sonnet-4-20250514"},
    }
    path = tmp_path / "profile.yaml"
    path.write_text(yaml.dump(data))
    return path


class TestListCommand:
    def test_list_empty(self, mock_db, mock_profile):
        result = runner.invoke(app, ["list"])
        assert result.exit_code == 0
        assert "No listings found" in result.output

    def test_list_with_data(self, mock_db, mock_profile):
        from jobhaul.db.queries import upsert_listing

        upsert_listing(
            mock_db,
            RawListing(
                title="Python Dev",
                company="Acme",
                source="platsbanken",
                external_id="1",
            ),
        )
        result = runner.invoke(app, ["list"])
        assert result.exit_code == 0
        assert "Python Dev" in result.output


class TestShowCommand:
    def test_show_not_found(self, mock_db, mock_profile):
        result = runner.invoke(app, ["show", "999"])
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_show_existing(self, mock_db, mock_profile):
        from jobhaul.db.queries import upsert_listing

        listing_id = upsert_listing(
            mock_db,
            RawListing(
                title="Python Dev",
                company="Acme",
                location="Stockholm",
                description="Build stuff",
                source="platsbanken",
                external_id="1",
            ),
        )
        result = runner.invoke(app, ["show", str(listing_id)])
        assert result.exit_code == 0
        assert "Python Dev" in result.output
        assert "Acme" in result.output


class TestStatsCommand:
    def test_stats_empty(self, mock_db):
        result = runner.invoke(app, ["stats"])
        assert result.exit_code == 0
        assert "Total unique listings: 0" in result.output

    def test_stats_with_data(self, mock_db):
        from jobhaul.db.queries import save_analysis, upsert_listing

        listing_id = upsert_listing(
            mock_db,
            RawListing(
                title="Dev", company="Co", source="platsbanken", external_id="1"
            ),
        )
        save_analysis(
            mock_db,
            AnalysisResult(listing_id=listing_id, match_score=75, profile_hash="h"),
        )
        result = runner.invoke(app, ["stats"])
        assert result.exit_code == 0
        assert "Total unique listings: 1" in result.output


class TestConfigCommands:
    def test_config_show(self, profile_file):
        with patch("jobhaul.cli.load_profile") as mock_load:
            mock_load.return_value = Profile(name="Test", skills=["Python"])
            result = runner.invoke(app, ["config", "show"])
            assert result.exit_code == 0
            assert "name: Test" in result.output

    def test_config_init_already_exists(self, tmp_path):
        existing = tmp_path / "profile.yaml"
        existing.write_text("existing")
        with patch("jobhaul.cli.init_profile", side_effect=FileExistsError("exists")):
            result = runner.invoke(app, ["config", "init"])
            assert result.exit_code == 1


class TestListCommandWithFlags:
    def test_list_shows_flags_column(self, mock_db):
        from jobhaul.db.queries import upsert_listing
        from jobhaul.models import Flags

        upsert_listing(
            mock_db,
            RawListing(
                title="AI Developer",
                company="Startup Inc",
                description="Build AI products",
                source="platsbanken",
                external_id="1",
            ),
        )

        profile = Profile(
            name="Test",
            flags=Flags(boost=["AI", "startup"]),
        )
        with patch("jobhaul.cli.load_profile", return_value=profile):
            result = runner.invoke(app, ["list"])
            assert result.exit_code == 0
            assert "AI" in result.output
            assert "Flags" in result.output  # Column header exists


class TestListCommandSortByScore:
    def test_top_sorts_by_score(self, mock_db, mock_profile):
        from jobhaul.db.queries import save_analysis, upsert_listing

        id1 = upsert_listing(
            mock_db,
            RawListing(title="Low Score", company="Co", source="p", external_id="1"),
        )
        id2 = upsert_listing(
            mock_db,
            RawListing(title="High Score", company="Co", source="p", external_id="2"),
        )
        save_analysis(mock_db, AnalysisResult(listing_id=id1, match_score=30, profile_hash="h"))
        save_analysis(mock_db, AnalysisResult(listing_id=id2, match_score=90, profile_hash="h"))

        result = runner.invoke(app, ["list", "--top", "2"])
        assert result.exit_code == 0
        # High Score should appear before Low Score
        output = result.output
        high_pos = output.find("High Score")
        low_pos = output.find("Low Score")
        assert high_pos < low_pos


class TestShowCommandWithFlags:
    def test_show_displays_boost_flags(self, mock_db):
        from jobhaul.db.queries import upsert_listing
        from jobhaul.models import Flags

        listing_id = upsert_listing(
            mock_db,
            RawListing(
                title="AI Engineer",
                company="TechCo",
                description="Work on AI",
                source="platsbanken",
                external_id="1",
            ),
        )
        profile = Profile(name="Test", flags=Flags(boost=["AI"]))
        with patch("jobhaul.cli.load_profile", return_value=profile):
            result = runner.invoke(app, ["show", str(listing_id)])
            assert result.exit_code == 0
            assert "AI" in result.output


class TestScanCommand:
    def test_scan_skip_analysis(self, mock_db, profile_file):
        with patch("jobhaul.cli.load_profile") as mock_load:
            from jobhaul.models import SourceConfig

            mock_load.return_value = Profile(
                name="Test",
                search_terms=["python"],
                sources={"platsbanken": SourceConfig(enabled=False)},
            )

            # Import collectors to register them
            import jobhaul.collectors.jooble  # noqa: F401
            import jobhaul.collectors.platsbanken  # noqa: F401
            import jobhaul.collectors.remoteok  # noqa: F401

            result = runner.invoke(app, ["scan", "--skip-analysis"])
            assert result.exit_code == 0
            assert "Total:" in result.output


class TestScanRetryFailed:
    """Test --retry-failed flag (Issue #17)."""

    def test_retry_failed_no_failures(self, mock_db, mock_profile):
        """--retry-failed with no failed analyses prints message."""
        result = runner.invoke(app, ["scan", "--retry-failed"])
        assert result.exit_code == 0
        assert "No failed analyses to retry" in result.output

    def test_retry_failed_queues_failed(self, mock_db, mock_profile):
        from unittest.mock import AsyncMock, patch as mock_patch

        from jobhaul.db.queries import save_analysis, upsert_listing

        # Insert a listing with a failed analysis
        listing_id = upsert_listing(
            mock_db,
            RawListing(
                title="Failed Job",
                company="Co",
                source="platsbanken",
                external_id="fail-1",
            ),
        )
        save_analysis(
            mock_db,
            AnalysisResult(
                listing_id=listing_id,
                match_score=0,
                profile_hash="test-hash",
                analysis_error="LLM timeout",
            ),
        )

        # Mock the adapter and analyze_listing to return success
        mock_result = AnalysisResult(
            listing_id=listing_id,
            match_score=75,
            profile_hash="test-hash",
        )

        with mock_patch("jobhaul.analysis.claude_cli.ClaudeCliAdapter"),              mock_patch("jobhaul.analysis.matcher.compute_profile_hash", return_value="test-hash"),              mock_patch("jobhaul.analysis.matcher.analyze_listing", new_callable=AsyncMock, return_value=mock_result):
            result = runner.invoke(app, ["scan", "--retry-failed"])

        assert result.exit_code == 0
        assert "Retrying 1 failed" in result.output
