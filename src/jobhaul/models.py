"""Pydantic data models used throughout jobhaul.

This module defines every data structure passed between collectors, the database
layer, the analysis pipeline, and the CLI/web interfaces.  All models inherit
from ``pydantic.BaseModel`` so they support validation and serialization out of
the box.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class LanguageEntry(BaseModel):
    """A single language proficiency entry in a user profile.

    Used inside ``Profile.languages`` to record which languages the user
    speaks and at what level (e.g. "native", "fluent", "basic").
    """

    language: str
    level: str


class SourceConfig(BaseModel):
    """Configuration for a single job-board source (e.g. LinkedIn, Indeed).

    Each source can be independently enabled or disabled and may require
    its own API key, region setting, or Apify token.
    """

    enabled: bool = False
    api_key: str = ""
    region: str = ""
    apify_token: str = ""


class LLMConfig(BaseModel):
    """LLM adapter and model selection for the analysis pipeline.

    Controls which AI backend is used to score and summarize job listings.
    """

    adapter: str = "claude-cli"
    model: str = "claude-sonnet-4-20250514"


class Flags(BaseModel):
    """Keyword flag rules applied to listings during collection and display.

    * **boost** -- terms that highlight a listing as especially interesting.
    * **warn** -- terms that flag potential concerns (e.g. unwanted technologies).
    * **exclude** -- terms that cause a listing to be dropped entirely.
    """

    boost: list[str] = Field(default_factory=list)
    warn: list[str] = Field(default_factory=list)
    exclude: list[str] = Field(default_factory=list)


class ScrapingConfig(BaseModel):
    """Rate-limiting and proxy settings for web scraping collectors.

    The random delay between ``delay_min`` and ``delay_max`` is applied
    between HTTP requests to avoid overloading job-board servers.
    """

    delay_min: float = 3.0
    delay_max: float = 7.0
    max_requests_per_run: int = 50
    proxy: str | None = None


class AnalysisConfig(BaseModel):
    """Tunables for the LLM analysis pipeline.

    ``pre_screen_threshold`` is the minimum keyword-overlap score a listing
    must reach before it is sent to the LLM for full analysis.
    """

    pre_screen_threshold: float = 0.15


class Profile(BaseModel):
    """The user's job-search profile loaded from ``profile.yaml``.

    This is the central configuration object that drives every part of
    jobhaul: which roles to search for, what skills the user has, which
    sources to query, and how listings should be flagged or analyzed.
    """

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
    prompt_context: str | None = None
    exclusions: list[str] = Field(default_factory=list)
    flags: Flags = Field(default_factory=Flags)
    sources: dict[str, SourceConfig] = Field(default_factory=dict)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    scraping: ScrapingConfig = Field(default_factory=ScrapingConfig)
    analysis: AnalysisConfig = Field(default_factory=AnalysisConfig)

    def get_effective_flags(self) -> Flags:
        """Return the effective flag rules, with legacy migration.

        Older profiles stored warning keywords under ``Profile.exclusions``
        instead of ``Profile.flags.warn``.  This method transparently promotes
        those legacy exclusions into ``flags.warn`` so that the rest of the
        codebase only needs to work with a single ``Flags`` object.

        Returns:
            A ``Flags`` instance with ``warn`` populated from ``exclusions``
            when the profile has not yet been migrated to the new format.
        """
        if self.exclusions and not self.flags.warn:
            return Flags(
                boost=self.flags.boost,
                warn=self.exclusions,
                exclude=self.flags.exclude,
            )
        return self.flags


class JobListing(BaseModel):
    """A fully-persisted job listing as stored in the database.

    This is the canonical listing representation used by the analysis
    pipeline, the web UI, and the CLI display commands.  Contrast with
    ``RawListing``, which represents a listing *before* it is saved.
    """

    id: int = 0
    title: str
    company: str | None = None
    location: str | None = None
    description: str | None = None
    url: str | None = None
    published_at: str | None = None
    is_remote: bool = False
    employment_type: str | None = None
    seniority_level: str | None = None
    salary: str | None = None
    sources: list[str] = Field(default_factory=list)
    created_at: str = ""
    application_deadline: str | None = None
    listing_status: str = "active"


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
    seniority_level: str | None = None
    salary: str | None = None
    source: str = ""
    external_id: str = ""
    source_url: str | None = None
    application_deadline: str | None = None
    listing_status: str = "active"


class AnalysisResult(BaseModel):
    """The output of LLM analysis for a single job listing.

    Stores the match score, reasons, strengths, concerns, and any error
    information.  ``fail_count`` tracks how many times analysis has been
    attempted and failed so that retries can be capped.
    """

    listing_id: int
    match_score: int = 0
    match_reasons: list[str] = Field(default_factory=list)
    missing_skills: list[str] = Field(default_factory=list)
    strengths: list[str] = Field(default_factory=list)
    concerns: list[str] = Field(default_factory=list)
    summary: str | None = None
    application_notes: str | None = None
    analysis_error: str | None = None
    fail_count: int = 0
    profile_hash: str = ""
    analyzed_at: str = ""


class Stats(BaseModel):
    """Summary statistics about the database."""

    total_listings: int
    total_source_entries: int
    dedup_savings: int
    total_analyses: int
    avg_score: float
    source_counts: dict[str, int]


class CollectorResult(BaseModel):
    """The return value of a single collector run.

    Groups the raw listings gathered from one source together with any
    error messages that occurred during collection.
    """

    source: str
    listings: list[RawListing] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
