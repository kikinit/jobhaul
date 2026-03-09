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


class Flags(BaseModel):
    boost: list[str] = Field(default_factory=list)
    warn: list[str] = Field(default_factory=list)
    exclude: list[str] = Field(default_factory=list)


class ScrapingConfig(BaseModel):
    delay_min: float = 3.0
    delay_max: float = 7.0
    max_requests_per_run: int = 50
    proxy: str | None = None


class AnalysisConfig(BaseModel):
    pre_screen_threshold: float = 0.15


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
    flags: Flags = Field(default_factory=Flags)
    sources: dict[str, SourceConfig] = Field(default_factory=dict)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    scraping: ScrapingConfig = Field(default_factory=ScrapingConfig)
    analysis: AnalysisConfig = Field(default_factory=AnalysisConfig)

    def get_effective_flags(self) -> Flags:
        """Get flags, treating legacy exclusions as flags.warn if flags.warn is empty."""
        if self.exclusions and not self.flags.warn:
            return Flags(
                boost=self.flags.boost,
                warn=self.exclusions,
                exclude=self.flags.exclude,
            )
        return self.flags


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


class CollectorResult(BaseModel):
    source: str
    listings: list[RawListing] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
