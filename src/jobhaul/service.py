"""High-level async workflows for collecting and analyzing job listings.

This service layer sits between the user-facing interfaces (CLI and web)
and the lower-level collectors, database, and analysis modules.  It
orchestrates multi-source collection, flag-based filtering, pre-screening,
and LLM analysis in a single place so that callers do not need to
coordinate those steps themselves.
"""

from __future__ import annotations

import sqlite3

from jobhaul.log import get_logger
from jobhaul.models import Flags, Profile

logger = get_logger(__name__)


async def collect_listings(
    profile: Profile,
    conn: sqlite3.Connection,
    source: str | None = None,
    flags: Flags | None = None,
) -> tuple[int, int, set[str]]:
    """Run collection from all (or one) source(s), applying exclusion filters.

    Iterates over the enabled collectors, gathers raw listings, checks
    each one against the exclude flag rules, and upserts surviving
    listings into the database.

    Args:
        profile: The user's job-search profile (determines search terms
            and source credentials).
        conn: An open SQLite connection for persisting listings.
        source: If given, only this single source is queried.  When
            ``None`` every registered collector is used.
        flags: Optional flag rules.  Listings matching an exclude term
            are silently dropped.

    Returns:
        A three-element tuple of ``(total_collected, skipped_excluded,
        collected_external_ids)`` where *collected_external_ids* is the
        set of external IDs that were successfully stored.
    """
    from jobhaul.collectors import ensure_collectors_registered
    from jobhaul.collectors.registry import get_all_collectors, get_collector
    from jobhaul.db.queries import upsert_listing
    from jobhaul.flagging import flag_listing
    from jobhaul.models import JobListing

    ensure_collectors_registered()

    if source:
        collectors = [get_collector(source)]
    else:
        collectors = get_all_collectors()

    collected_external_ids: set[str] = set()
    total_collected = 0
    skipped_excluded = 0
    collection_results = []

    for collector in collectors:
        result = await collector.collect(profile)
        for raw in result.listings:
            if flags:
                temp_listing = JobListing(
                    title=raw.title,
                    company=raw.company,
                    description=raw.description,
                )
                flag_result = flag_listing(temp_listing, flags)
                if flag_result["excluded"]:
                    logger.info("Excluded listing: %s (matched exclude filter)", raw.title)
                    skipped_excluded += 1
                    continue
            upsert_listing(conn, raw)
            collected_external_ids.add(raw.external_id)
        total_collected += len(result.listings) - skipped_excluded
        collection_results.append({
            "source": collector.name,
            "count": len(result.listings),
            "errors": result.errors,
        })

    return total_collected, skipped_excluded, collected_external_ids


async def run_analysis(
    profile: Profile,
    conn: sqlite3.Connection,
    adapter=None,
    limit: int | None = None,
    flags: Flags | None = None,
) -> list[dict]:
    """Analyze unanalyzed listings with pre-screening and flag filtering.

    For each listing that has not yet been analyzed (under the current
    profile hash), this function first applies exclude-flag filtering,
    then runs a cheap keyword pre-screen, and finally sends qualifying
    listings to the LLM adapter for full analysis.

    Args:
        profile: The user's profile, used to build the analysis prompt
            and compute the profile hash.
        conn: An open SQLite connection for reading listings and
            saving analysis results.
        adapter: An LLM adapter instance.  When ``None`` a default
            ``ClaudeCliAdapter`` is created from the profile's LLM
            config.
        limit: Maximum number of listings to analyze in this run.
            ``None`` means no limit.
        flags: Optional flag rules.  Listings matching an exclude term
            are skipped.

    Returns:
        A list of dicts, one per processed listing.  Each dict contains
        ``"listing"`` and ``"result"`` keys (the latter is ``None`` on
        error, with an ``"error"`` key added instead).
    """
    from jobhaul.analysis.claude_cli import ClaudeCliAdapter
    from jobhaul.analysis.matcher import analyze_listing, compute_profile_hash, pre_screen
    from jobhaul.db.queries import get_unanalyzed_listings, save_analysis
    from jobhaul.flagging import flag_listing

    profile_hash = compute_profile_hash(profile)
    if adapter is None:
        adapter = ClaudeCliAdapter(model=profile.llm.model)

    unanalyzed = get_unanalyzed_listings(conn, profile_hash, limit=limit)
    if not unanalyzed:
        return []

    threshold = profile.analysis.pre_screen_threshold
    results = []

    for listing in unanalyzed:
        if flags:
            flag_result = flag_listing(listing, flags)
            if flag_result["excluded"]:
                continue

        score = pre_screen(listing, profile)
        if score < threshold:
            continue

        try:
            result = await analyze_listing(listing, profile, adapter)
            save_analysis(conn, result)
            results.append({
                "listing": listing,
                "result": result,
                "flags": flag_listing(listing, flags) if flags else None,
            })
        except Exception as e:
            logger.warning("Error analyzing %s: %s", listing.title, e)
            results.append({
                "listing": listing,
                "result": None,
                "error": str(e),
            })

    return results


async def retry_failed_analyses(
    profile: Profile,
    conn: sqlite3.Connection,
    adapter=None,
    limit: int | None = None,
) -> list[dict]:
    """Retry analysis for listings that previously failed.

    Fetches listings whose last analysis attempt resulted in an error
    and re-runs the full LLM analysis for each.  This is useful for
    recovering from transient LLM timeouts or rate-limit errors.

    Args:
        profile: The user's profile (used for prompt construction and
            profile hash).
        conn: An open SQLite connection.
        adapter: An LLM adapter instance.  Defaults to a new
            ``ClaudeCliAdapter`` when ``None``.
        limit: Maximum number of failed listings to retry.
            ``None`` means no limit.

    Returns:
        A list of dicts, one per retried listing.  Each dict contains
        ``"listing"`` and ``"result"`` keys (``"result"`` is ``None``
        on error, with an ``"error"`` key added).
    """
    from jobhaul.analysis.claude_cli import ClaudeCliAdapter
    from jobhaul.analysis.matcher import analyze_listing, compute_profile_hash
    from jobhaul.db.queries import get_failed_listings, save_analysis

    profile_hash = compute_profile_hash(profile)
    if adapter is None:
        adapter = ClaudeCliAdapter(model=profile.llm.model)

    failed = get_failed_listings(conn, profile_hash, limit=limit)
    results = []

    for listing in failed:
        try:
            result = await analyze_listing(listing, profile, adapter)
            save_analysis(conn, result)
            results.append({"listing": listing, "result": result})
        except Exception as e:
            results.append({"listing": listing, "result": None, "error": str(e)})

    return results
