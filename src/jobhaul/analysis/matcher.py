"""Orchestration: listing + profile -> LLM -> result."""

from __future__ import annotations

import hashlib
import json

from jobhaul.analysis.adapter import LLMAdapter
from jobhaul.log import get_logger
from jobhaul.models import AnalysisResult, JobListing, Profile

logger = get_logger(__name__)


def compute_profile_hash(profile: Profile) -> str:
    """Hash the profile to detect when re-analysis is needed."""
    data = profile.model_dump_json(exclude={"sources", "llm"})
    return hashlib.sha256(data.encode()).hexdigest()[:16]


def build_prompt(listing: JobListing, profile: Profile) -> str:
    """Build the LLM analysis prompt."""
    return f"""Analyze this job listing against the candidate profile and return a JSON object.

## Job Listing
- Title: {listing.title}
- Company: {listing.company or 'Unknown'}
- Location: {listing.location or 'Unknown'}
- Remote: {'Yes' if listing.is_remote else 'No'}
- Employment type: {listing.employment_type or 'Unknown'}

Description:
{(listing.description or '')[:3000]}

## Candidate Profile
- Name: {profile.name}
- Roles sought: {', '.join(profile.roles)}
- Skills: {', '.join(profile.skills)}
- Currently learning: {', '.join(profile.learning)}
- Location: {profile.location}
- Remote only: {profile.remote_only}
- Will relocate: {profile.will_relocate}
- Employment preferences: {', '.join(profile.employment)}
- Seniority: {profile.seniority}
- Education: {profile.education}
- Languages: {', '.join(f'{l.language} ({l.level})' for l in profile.languages)}
- Summary: {profile.summary}
- Exclusions (industries to avoid): {', '.join(profile.exclusions)}

## Instructions
Return ONLY a JSON object with these fields:
- match_score: integer 0-100 (how well the candidate fits this role)
- match_reasons: string (why the candidate is a good fit)
- missing_skills: string (skills the job wants that the candidate lacks)
- strengths: string (candidate's strongest selling points for this role)
- concerns: string (potential issues or mismatches)
- summary: string (1-2 sentence recommendation)
- application_notes: string (tips for applying if the candidate should apply)

Return ONLY valid JSON, no markdown formatting or extra text."""


def parse_llm_response(response: str) -> dict:
    """Parse LLM JSON response, handling common formatting issues."""
    text = response.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]  # Remove opening fence
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)

    return json.loads(text)


async def analyze_listing(
    listing: JobListing, profile: Profile, adapter: LLMAdapter
) -> AnalysisResult:
    """Analyze a single listing against the profile using the LLM adapter."""
    profile_hash = compute_profile_hash(profile)
    prompt = build_prompt(listing, profile)

    logger.info("Analyzing listing %d: %s", listing.id, listing.title)
    raw_response = await adapter.analyze(prompt)

    try:
        data = parse_llm_response(raw_response)
    except json.JSONDecodeError as e:
        logger.warning("Failed to parse LLM response for listing %d: %s", listing.id, e)
        data = {
            "match_score": 0,
            "summary": f"Analysis failed: could not parse LLM response ({e})",
        }

    return AnalysisResult(
        listing_id=listing.id,
        match_score=max(0, min(100, int(data.get("match_score", 0)))),
        match_reasons=data.get("match_reasons"),
        missing_skills=data.get("missing_skills"),
        strengths=data.get("strengths"),
        concerns=data.get("concerns"),
        summary=data.get("summary"),
        application_notes=data.get("application_notes"),
        profile_hash=profile_hash,
    )
