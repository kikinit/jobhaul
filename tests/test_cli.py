"""Tests for CLI commands."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
from typer.testing import CliRunner

from jobhaul.cli import app
from jobhaul.models import AnalysisResult, RawListing

runner = CliRunner()


@pytest.fixture
def mock_db(tmp_path):
    """Provide a temp DB and patch ensure_data_dir + init_db."""
    from jobhaul.db.schema import init_db

    db_path = str(tmp_path / "test.db")
    conn = init_db(db_path)

    with patch("jobhaul.cli._get_db", return_value=conn):
        yield conn

    conn.close()


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
    def test_list_empty(self, mock_db):
        result = runner.invoke(app, ["list"])
        assert result.exit_code == 0
        assert "No listings found" in result.output

    def test_list_with_data(self, mock_db):
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
    def test_show_not_found(self, mock_db):
        result = runner.invoke(app, ["show", "999"])
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_show_existing(self, mock_db):
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
            from jobhaul.models import Profile

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


class TestScanCommand:
    def test_scan_skip_analysis(self, mock_db, profile_file):
        with patch("jobhaul.cli.load_profile") as mock_load:
            from jobhaul.models import Profile, SourceConfig

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
