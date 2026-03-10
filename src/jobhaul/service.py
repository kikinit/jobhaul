"""Core collection and analysis workflows shared by CLI and web."""

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

    Returns (total_collected, skipped_excluded, collected_external_ids).
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

    Returns list of analysis result summaries.
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
    """Retry listings with analysis errors. Returns summary list."""
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
