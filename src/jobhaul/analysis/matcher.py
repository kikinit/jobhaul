"""Orchestration: listing + profile -> LLM -> result."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re

from jobhaul.analysis.adapter import LLMAdapter
from jobhaul.analysis.claude_cli import LLMTimeoutError
from jobhaul.log import get_logger
from jobhaul.models import AnalysisResult, JobListing, Profile

logger = get_logger(__name__)

RETRY_PROMPT = (
    "Your previous response was not valid JSON. "
    "Please respond with ONLY a JSON object, nothing else."
)

MAX_RETRIES = 2
BACKOFF_DELAYS = [5, 15]


def compute_profile_hash(profile: Profile) -> str:
    """Hash the profile to detect when re-analysis is needed."""
    data = profile.model_dump_json(exclude={"sources", "llm", "scraping", "analysis"})
    return hashlib.sha256(data.encode()).hexdigest()[:16]


def build_prompt(listing: JobListing, profile: Profile) -> str:
    """Build the LLM analysis prompt."""
    return f"""Analyze this job listing against the candidate profile. Return a JSON object.

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
- Seniority: {profile.seniority}
- Roles sought: {', '.join(profile.roles)}
- Skills: {', '.join(profile.skills)}
- Currently learning: {', '.join(profile.learning)}
- Location: {profile.location}
- Remote only: {profile.remote_only}
- Will relocate: {profile.will_relocate}
- Employment preferences: {', '.join(profile.employment)}
- Education: {profile.education}
- Languages: {', '.join(f'{l.language} ({l.level})' for l in profile.languages)}
- Summary: {profile.summary}

## CRITICAL: Skill depth context
ALL candidate skills are at FOUNDATIONAL/ACADEMIC level from university coursework and side projects. The candidate has LESS THAN 1 YEAR of professional experience. Do NOT equate "knows TypeScript" with "5 years production TypeScript". Treat all listed skills as beginner-to-intermediate.

## CRITICAL: Scoring rules (follow strictly)

### Dealbreakers — these CAP the score:
1. **Senior/Lead/Staff/Principal roles → MAX SCORE 10.** The candidate is junior and will not become senior in the next few years. This is effectively an exclusion regardless of tech match.
2. **Roles requiring 3+ years experience → MAX SCORE 25.** The candidate has <1 year.
3. **Roles requiring 5+ years experience → MAX SCORE 10.**
4. **Primary tech the candidate does not know** (e.g., the role is primarily PHP, Go, C#, .NET, Swift, Kotlin/Android, Embedded C, SAP, Salesforce, Dynamics 365) **→ MAX SCORE 20.** Secondary/nice-to-have tech is less important — tech can be learned.

### Boosters:
- **Explicitly junior/graduate/entry-level roles → BOOST +15-25 points.** This is the candidate's target segment.
- **Tech stack closely matches candidate's skills → positive signal** (but remember: foundational level).

### Neutral factors (do NOT penalize):
- Employment type (consulting, staffing, contract, permanent) — all are acceptable.
- Industry warnings — do not reduce score for defense/gambling/staffing. These are shown as labels only.

### Scoring guide:
- 80-100: Strong match — junior/graduate role, good tech overlap, realistic to get hired
- 60-79: Good match — entry-level friendly or role where junior is acceptable
- 40-59: Stretch but possible — mid-level expected, or partial tech mismatch
- 20-39: Unlikely — significant experience gap or wrong primary tech
- 5-15: Senior/Lead role or completely wrong domain
- 0-4: Entirely irrelevant

## Output format
Return ONLY a JSON object with these fields:
- match_score: integer 0-100 (realistic chance of getting this job)
- match_reasons: list of strings (why the candidate fits)
- missing_skills: list of strings (skills the job wants that the candidate lacks)
- strengths: list of strings (candidate's selling points for this role)
- concerns: list of strings (potential issues or mismatches)
- summary: string (1-2 sentence recommendation)
- application_notes: string (tips for applying, or empty if not worth applying)

Return ONLY valid JSON, no markdown formatting or extra text."""


def _to_list(value: str | list | None) -> list[str]:
    """Coerce a value from the LLM to a list of strings."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [value] if value else []


def parse_llm_response(response: str) -> dict:
    """Parse LLM JSON response, handling common formatting issues."""
    if not response or not response.strip():
        raise json.JSONDecodeError("Empty response", response or "", 0)

    text = response.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]  # Remove opening fence
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)

    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try to extract JSON from mixed text by finding { ... } boundaries
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    # Nothing worked
    raise json.JSONDecodeError(
        "No valid JSON found in response", text[:500], 0
    )


def pre_screen(listing: JobListing, profile: Profile) -> float:
    """Simple keyword pre-screen: count profile terms that appear in the listing.

    Returns a ratio from 0.0 (no matches) to 1.0 (all terms match).
    """
    terms = set()
    for term in profile.skills + profile.roles + profile.search_terms:
        terms.add(term.lower())

    if not terms:
        return 1.0  # No terms to match against — pass everything through

    text = " ".join(
        filter(None, [listing.title, listing.description])
    ).lower()

    matched = sum(1 for term in terms if term in text)
    return matched / len(terms)


async def analyze_listing(
    listing: JobListing, profile: Profile, adapter: LLMAdapter
) -> AnalysisResult:
    """Analyze a single listing against the profile using the LLM adapter.

    Retries up to MAX_RETRIES times on timeout or transient errors with
    exponential backoff. On total failure returns a zero-score result with
    analysis_error set so the scan can continue.
    """
    profile_hash = compute_profile_hash(profile)
    prompt = build_prompt(listing, profile)

    logger.info("Analyzing listing %d: %s", listing.id, listing.title)

    last_error: Exception | None = None
    for attempt in range(1 + MAX_RETRIES):
        try:
            raw_response = await adapter.analyze(prompt)
            break
        except (LLMTimeoutError, RuntimeError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                delay = BACKOFF_DELAYS[attempt]
                logger.warning(
                    "LLM call failed for listing %d (attempt %d/%d): %s — retrying in %ds",
                    listing.id, attempt + 1, 1 + MAX_RETRIES, e, delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    "LLM call failed for listing %d after %d attempts: %s",
                    listing.id, 1 + MAX_RETRIES, e,
                )
                return AnalysisResult(
                    listing_id=listing.id,
                    match_score=0,
                    summary=f"Analysis failed: {e}",
                    analysis_error=str(e),
                    profile_hash=profile_hash,
                )

    try:
        data = parse_llm_response(raw_response)
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning(
            "Failed to parse LLM response for listing %d: %s (response: %.500s)",
            listing.id, e, raw_response,
        )
        # Retry once with a clearer prompt
        logger.info("Retrying analysis for listing %d with explicit JSON request", listing.id)
        retry_prompt = prompt + "\n\n" + RETRY_PROMPT
        try:
            raw_response = await adapter.analyze(retry_prompt)
            data = parse_llm_response(raw_response)
        except (json.JSONDecodeError, ValueError, Exception) as retry_e:
            logger.warning(
                "Retry also failed for listing %d: %s", listing.id, retry_e
            )
            data = {
                "match_score": 0,
                "summary": f"Analysis failed: could not parse LLM response ({e})",
            }

    return AnalysisResult(
        listing_id=listing.id,
        match_score=max(0, min(100, int(data.get("match_score", 0)))),
        match_reasons=_to_list(data.get("match_reasons")),
        missing_skills=_to_list(data.get("missing_skills")),
        strengths=_to_list(data.get("strengths")),
        concerns=_to_list(data.get("concerns")),
        summary=data.get("summary"),
        application_notes=data.get("application_notes"),
        profile_hash=profile_hash,
    )
