"""Tests for profile configuration loading."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from jobhaul.config import load_profile
from jobhaul.models import Profile


@pytest.fixture
def valid_profile_data():
    return {
        "name": "Test User",
        "roles": ["developer"],
        "search_terms": ["python"],
        "skills": ["Python", "JavaScript"],
        "location": "Stockholm",
        "remote_only": False,
        "employment": ["full-time"],
        "seniority": "junior",
        "education": "CS Degree",
        "languages": [{"language": "English", "level": "fluent"}],
        "summary": "A test user",
        "exclusions": ["gambling"],
        "sources": {
            "platsbanken": {"enabled": True, "region": "abc"},
            "remoteok": {"enabled": True},
        },
        "llm": {"adapter": "claude-cli", "model": "claude-sonnet-4-20250514"},
    }


def test_load_valid_profile(tmp_path, valid_profile_data):
    profile_path = tmp_path / "profile.yaml"
    profile_path.write_text(yaml.dump(valid_profile_data))

    profile = load_profile(profile_path)
    assert profile.name == "Test User"
    assert "developer" in profile.roles
    assert profile.skills == ["Python", "JavaScript"]
    assert profile.location == "Stockholm"
    assert profile.sources["platsbanken"].enabled is True


def test_load_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError, match="Profile not found"):
        load_profile(tmp_path / "nonexistent.yaml")


def test_load_invalid_yaml(tmp_path):
    profile_path = tmp_path / "profile.yaml"
    profile_path.write_text("not a mapping")

    with pytest.raises(ValueError, match="expected a YAML mapping"):
        load_profile(profile_path)


def test_load_invalid_schema(tmp_path):
    profile_path = tmp_path / "profile.yaml"
    # name is required by the model
    profile_path.write_text(yaml.dump({"roles": 123}))

    with pytest.raises(ValueError, match="Invalid profile"):
        load_profile(profile_path)


def test_load_minimal_profile(tmp_path):
    profile_path = tmp_path / "profile.yaml"
    profile_path.write_text(yaml.dump({"name": "Minimal"}))

    profile = load_profile(profile_path)
    assert profile.name == "Minimal"
    assert profile.roles == []
    assert profile.skills == []


def test_profile_defaults():
    profile = Profile(name="Test")
    assert profile.remote_only is False
    assert profile.will_relocate is False
    assert profile.llm.adapter == "claude-cli"
    assert profile.sources == {}


def test_example_profile_loads():
    example = Path(__file__).parent.parent / "config" / "profile.example.yaml"
    if example.exists():
        profile = load_profile(example)
        assert profile.name == "Mattias"
        assert len(profile.search_terms) > 0
