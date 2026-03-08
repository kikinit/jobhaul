"""All Pydantic models."""

from __future__ import annotations

from pydantic import BaseModel, Field


class LanguageEntry(BaseModel):
    language: str
    level: str


class SourceConfig(BaseModel):
    enabled: bool = False
    api_key: str = ""
    region: str = ""


class LLMConfig(BaseModel):
    adapter: str = "claude-cli"
    model: str = "claude-sonnet-4-20250514"


class Profile(BaseModel):
    name: str
    roles: list[str] = Field(default_factory=list)
    search_terms: list[str] = Field(default_factory=list)
    skills: list[str] = Field(default_factory=list)
    learning: list[str] = Field(default_factory=list)
    location: str = ""
    remote_only: bool = False
    will_relocate: bool = False
    employment: list[str] = Field(default_factory=list)
    seniority: str = ""
    education: str = ""
    languages: list[LanguageEntry] = Field(default_factory=list)
    summary: str = ""
    exclusions: list[str] = Field(default_factory=list)
    sources: dict[str, SourceConfig] = Field(default_factory=dict)
    llm: LLMConfig = Field(default_factory=LLMConfig)


class JobListing(BaseModel):
    id: int = 0
    title: str
    company: str | None = None
    location: str | None = None
    description: str | None = None
    url: str | None = None
    published_at: str | None = None
    is_remote: bool = False
    employment_type: str | None = None
    sources: list[str] = Field(default_factory=list)
    created_at: str = ""


class RawListing(BaseModel):
    """A listing as returned by a collector, before DB insertion."""

    title: str
    company: str | None = None
    location: str | None = None
    description: str | None = None
    url: str | None = None
    published_at: str | None = None
    is_remote: bool = False
    employment_type: str | None = None
    source: str = ""
    external_id: str = ""
    source_url: str | None = None


class AnalysisResult(BaseModel):
    listing_id: int
    match_score: int = 0
    match_reasons: str | None = None
    missing_skills: str | None = None
    strengths: str | None = None
    concerns: str | None = None
    summary: str | None = None
    application_notes: str | None = None
    profile_hash: str = ""
    analyzed_at: str = ""


class CollectorResult(BaseModel):
    source: str
    listings: list[RawListing] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
