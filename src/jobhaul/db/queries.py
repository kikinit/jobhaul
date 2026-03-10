"""Data-access layer for jobhaul's SQLite database.

Every public function in this module accepts an open ``sqlite3.Connection``
as its first argument and performs one logical database operation (query,
insert, update, or upsert).  Higher-level code should never construct SQL
directly; instead, call the helpers defined here.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3

from jobhaul.constants import MAX_ANALYSIS_FAIL_RETRIES
from jobhaul.log import get_logger
from jobhaul.models import AnalysisResult, JobListing, RawListing, Stats

logger = get_logger(__name__)


def _normalize_for_dedup(text: str | None) -> str:
    """Normalize text for dedup: strip all whitespace types, collapse spaces, lowercase."""
    if not text:
        return ""
    # Replace all whitespace types (including \xa0 non-breaking space, tabs, newlines) with space
    result = re.sub(r"[\s\u00a0\u2000-\u200b\u2028\u2029\u202f\u205f\u3000\ufeff]+", " ", text)
    # Strip leading/trailing, lowercase
    return result.strip().lower()


def _compute_dedup_key(title: str, company: str | None) -> str:
    """Compute a SHA-256 dedup key from normalized title and company."""
    t = _normalize_for_dedup(title)
    c = _normalize_for_dedup(company)
    return hashlib.sha256(f"{t}||{c}".encode()).hexdigest()


def find_duplicate(conn: sqlite3.Connection, title: str, company: str | None) -> int | None:
    """Find an existing listing whose title and company match after normalisation.

    Looks up the SHA-256 dedup key first (fast path).  If no match is found,
    falls back to a full-table scan of rows that predate the dedup-key
    migration and have no key yet.

    Args:
        conn: Open database connection.
        title: Job listing title to search for.
        company: Company name (may be *None*).

    Returns:
        The ``id`` of the matching listing, or *None* if no duplicate exists.
    """
    key = _compute_dedup_key(title, company)
    row = conn.execute("SELECT id FROM listings WHERE dedup_key = ?", (key,)).fetchone()
    if row:
        return row["id"]

    # Fallback for rows without dedup_key (pre-migration)
    norm_title = _normalize_for_dedup(title)
    norm_company = _normalize_for_dedup(company)
    rows = conn.execute(
        "SELECT id, title, company FROM listings WHERE dedup_key IS NULL"
    ).fetchall()
    for r in rows:
        if (_normalize_for_dedup(r["title"]) == norm_title
                and _normalize_for_dedup(r["company"]) == norm_company):
            return r["id"]
    return None


def merge_existing_duplicates(conn: sqlite3.Connection) -> int:
    """Scan every listing for duplicates and merge them into the oldest record.

    Groups all listings by their normalised title and company.  When a group
    contains more than one row, the oldest listing (lowest ``id``) is kept
    and every newer duplicate's sources and analyses are re-pointed to the
    keeper before the duplicate row is deleted.

    Args:
        conn: Open database connection.

    Returns:
        The number of individual duplicate rows that were merged (removed).
    """
    rows = conn.execute("SELECT id, title, company FROM listings ORDER BY id").fetchall()

    # Group by normalized title+company
    groups: dict[tuple[str, str], list[int]] = {}
    for row in rows:
        key = (_normalize_for_dedup(row["title"]), _normalize_for_dedup(row["company"]))
        groups.setdefault(key, []).append(row["id"])

    merge_count = 0
    for key, ids in groups.items():
        if len(ids) <= 1:
            continue

        # Keep the oldest (lowest id), merge the rest into it
        keep_id = ids[0]
        for dup_id in ids[1:]:
            # Move listing_sources to the kept listing
            conn.execute(
                """UPDATE OR IGNORE listing_sources SET listing_id = ? WHERE listing_id = ?""",
                (keep_id, dup_id),
            )
            # Delete any source entries that couldn't be moved (conflicts)
            conn.execute("DELETE FROM listing_sources WHERE listing_id = ?", (dup_id,))
            # Move analyses to the kept listing
            conn.execute(
                """UPDATE OR IGNORE analyses SET listing_id = ? WHERE listing_id = ?""",
                (keep_id, dup_id),
            )
            conn.execute("DELETE FROM analyses WHERE listing_id = ?", (dup_id,))
            # Delete the duplicate listing
            conn.execute("DELETE FROM listings WHERE id = ?", (dup_id,))
            merge_count += 1
            logger.info("Merged duplicate listing %d into %d", dup_id, keep_id)

    if merge_count:
        conn.commit()
    return merge_count


def upsert_listing(conn: sqlite3.Connection, raw: RawListing) -> int:
    """Insert a new listing or update an existing duplicate, returning the listing ID.

    If a listing with the same normalised title and company already exists,
    only mutable fields (like ``application_deadline``) are updated on the
    existing row.  Otherwise a brand-new row is inserted.  In both cases a
    corresponding ``listing_sources`` row is added so that the originating
    job board is tracked.

    Args:
        conn: Open database connection.
        raw: The scraped listing data to persist.

    Returns:
        The integer ``id`` of the listing row (existing or newly created).
    """
    existing_id = find_duplicate(conn, raw.title, raw.company)

    if existing_id:
        listing_id = existing_id
        # Update deadline and status if provided on an existing listing
        if raw.application_deadline:
            conn.execute(
                "UPDATE listings SET application_deadline = ? WHERE id = ?",
                (raw.application_deadline, listing_id),
            )
    else:
        dedup_key = _compute_dedup_key(raw.title, raw.company)
        cursor = conn.execute(
            """INSERT INTO listings (title, company, location, description, url,
               published_at, is_remote, employment_type, seniority_level, salary,
               application_deadline, listing_status, dedup_key)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                raw.title,
                raw.company,
                raw.location,
                raw.description,
                raw.url,
                raw.published_at,
                int(raw.is_remote),
                raw.employment_type,
                raw.seniority_level,
                raw.salary,
                raw.application_deadline,
                raw.listing_status,
                dedup_key,
            ),
        )
        listing_id = cursor.lastrowid

    # Add source entry (ignore if already exists for this source+external_id)
    conn.execute(
        """INSERT OR IGNORE INTO listing_sources (listing_id, source, external_id, source_url)
           VALUES (?, ?, ?, ?)""",
        (listing_id, raw.source, raw.external_id, raw.source_url),
    )
    conn.commit()
    return listing_id



def _row_to_listing(row: sqlite3.Row, sources: list[str]) -> JobListing:
    """Convert a DB row + sources list into a JobListing model.

    All columns (seniority_level, salary, application_deadline, listing_status)
    are guaranteed to exist after schema migrations up to v8.
    """
    return JobListing(
        id=row["id"],
        title=row["title"],
        company=row["company"],
        location=row["location"],
        description=row["description"],
        url=row["url"],
        published_at=row["published_at"],
        is_remote=bool(row["is_remote"]),
        employment_type=row["employment_type"],
        seniority_level=row["seniority_level"],
        salary=row["salary"],
        sources=sources,
        created_at=row["created_at"],
        application_deadline=row["application_deadline"],
        listing_status=row["listing_status"] or "active",
    )


def get_listing(conn: sqlite3.Connection, listing_id: int) -> JobListing | None:
    """Fetch a single listing by its primary key, including its source list.

    Args:
        conn: Open database connection.
        listing_id: The ``id`` column value of the listing to retrieve.

    Returns:
        A ``JobListing`` object, or *None* if no listing with that ID exists.
    """
    row = conn.execute("SELECT * FROM listings WHERE id = ?", (listing_id,)).fetchone()
    if not row:
        return None

    sources = [
        r["source"]
        for r in conn.execute(
            "SELECT source FROM listing_sources WHERE listing_id = ?", (listing_id,)
        ).fetchall()
    ]

    return _row_to_listing(row, sources)


def list_listings(
    conn: sqlite3.Connection,
    *,
    days: int = 7,
    source: str | None = None,
    min_score: int | None = None,
    limit: int | None = None,
    sort_by_score: bool = False,
    include_expired: bool = False,
) -> list[JobListing]:
    """Return a filtered list of job listings, newest first by default.

    Supports filtering by recency, source board, minimum analysis score,
    and expiration status.  Results can optionally be sorted by match score
    (descending) instead of creation date.

    Args:
        conn: Open database connection.
        days: Only include listings created within this many days.
        source: If given, only include listings from this job board.
        min_score: If given, only include listings whose analysis score is
            at least this value.
        limit: Maximum number of listings to return.
        sort_by_score: When *True*, sort by match score (highest first)
            instead of creation date.
        include_expired: When *True*, include listings marked as expired.

    Returns:
        A list of ``JobListing`` objects matching the filters.
    """
    query = """
        SELECT l.*, GROUP_CONCAT(ls.source) AS sources_csv
        FROM listings l
        LEFT JOIN listing_sources ls ON l.id = ls.listing_id
        LEFT JOIN analyses a ON l.id = a.listing_id
        WHERE l.created_at >= datetime('now', ?)
    """
    params: list = [f"-{days} days"]

    if not include_expired:
        query += " AND COALESCE(l.listing_status, 'active') = 'active'"

    if source:
        query += " AND ls.source = ?"
        params.append(source)

    if min_score is not None:
        query += " AND a.match_score >= ?"
        params.append(min_score)

    query += " GROUP BY l.id"

    if sort_by_score:
        # Sort by score descending; listings without analysis go to the bottom
        query += " ORDER BY COALESCE(a.match_score, -1) DESC, l.created_at DESC"
    else:
        query += " ORDER BY l.created_at DESC"

    if limit:
        query += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()
    listings = []
    for row in rows:
        sources_csv = row["sources_csv"] or ""
        sources = list(dict.fromkeys(sources_csv.split(","))) if sources_csv else []
        listings.append(_row_to_listing(row, sources))
    return listings


def list_listings_with_analysis(
    conn: sqlite3.Connection,
    *,
    days: int = 7,
    source: str | None = None,
    min_score: int | None = None,
    limit: int | None = None,
    sort_by_score: bool = False,
    include_expired: bool = False,
) -> list[tuple[JobListing, AnalysisResult | None]]:
    """Return listings together with their analysis results in one query.

    Works exactly like ``list_listings`` but also LEFT JOINs the analyses
    table so that each listing is paired with its ``AnalysisResult`` (or
    *None* if not yet analysed).  This avoids the N+1 query problem when
    rendering dashboards or reports.

    Args:
        conn: Open database connection.
        days: Only include listings created within this many days.
        source: If given, only include listings from this job board.
        min_score: If given, only include listings whose analysis score is
            at least this value.
        limit: Maximum number of listings to return.
        sort_by_score: When *True*, sort by match score (highest first)
            instead of creation date.
        include_expired: When *True*, include listings marked as expired.

    Returns:
        A list of ``(JobListing, AnalysisResult | None)`` tuples.
    """
    query = """
        SELECT l.*,
               GROUP_CONCAT(ls.source) AS sources_csv,
               a.match_score AS a_match_score,
               a.match_reasons AS a_match_reasons,
               a.missing_skills AS a_missing_skills,
               a.strengths AS a_strengths,
               a.concerns AS a_concerns,
               a.summary AS a_summary,
               a.application_notes AS a_application_notes,
               a.analysis_error AS a_analysis_error,
               a.fail_count AS a_fail_count,
               a.profile_hash AS a_profile_hash,
               a.analyzed_at AS a_analyzed_at
        FROM listings l
        LEFT JOIN listing_sources ls ON l.id = ls.listing_id
        LEFT JOIN analyses a ON l.id = a.listing_id
        WHERE l.created_at >= datetime('now', ?)
    """
    params: list = [f"-{days} days"]

    if not include_expired:
        query += " AND COALESCE(l.listing_status, 'active') = 'active'"

    if source:
        query += " AND ls.source = ?"
        params.append(source)

    if min_score is not None:
        query += " AND a.match_score >= ?"
        params.append(min_score)

    query += " GROUP BY l.id"

    if sort_by_score:
        query += " ORDER BY COALESCE(a.match_score, -1) DESC, l.created_at DESC"
    else:
        query += " ORDER BY l.created_at DESC"

    if limit:
        query += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()
    results = []
    for row in rows:
        sources_csv = row["sources_csv"] or ""
        sources = list(dict.fromkeys(sources_csv.split(","))) if sources_csv else []
        listing = _row_to_listing(row, sources)

        analysis = None
        if row["a_match_score"] is not None:
            analysis = AnalysisResult(
                listing_id=row["id"],
                match_score=row["a_match_score"],
                match_reasons=_deserialize_list(row["a_match_reasons"]),
                missing_skills=_deserialize_list(row["a_missing_skills"]),
                strengths=_deserialize_list(row["a_strengths"]),
                concerns=_deserialize_list(row["a_concerns"]),
                summary=row["a_summary"],
                application_notes=row["a_application_notes"],
                analysis_error=row["a_analysis_error"],
                fail_count=row["a_fail_count"],
                profile_hash=row["a_profile_hash"],
                analyzed_at=row["a_analyzed_at"],
            )

        results.append((listing, analysis))
    return results


def mark_likely_expired(conn: sqlite3.Connection, listing_id: int) -> None:
    """Set a listing's status to ``'likely_expired'``.

    Use this when a listing was not found during a recent scan and its
    application deadline has passed, but removal has not been confirmed
    by visiting the original URL.

    Args:
        conn: Open database connection.
        listing_id: Primary key of the listing to update.
    """
    conn.execute(
        "UPDATE listings SET listing_status = 'likely_expired' WHERE id = ?",
        (listing_id,),
    )
    conn.commit()


def mark_confirmed_expired(conn: sqlite3.Connection, listing_id: int) -> None:
    """Set a listing's status to ``'confirmed_expired'``.

    Use this when the listing's removal has been verified (e.g. the
    original URL returns a 404 or the job board explicitly marks it
    as closed).

    Args:
        conn: Open database connection.
        listing_id: Primary key of the listing to update.
    """
    conn.execute(
        "UPDATE listings SET listing_status = 'confirmed_expired' WHERE id = ?",
        (listing_id,),
    )
    conn.commit()


def check_and_mark_expired(
    conn: sqlite3.Connection, collected_external_ids: set[str],
) -> int:
    """Mark active listings as likely expired if their deadline has passed.

    Looks at every active listing that has an ``application_deadline`` in
    the past.  If none of that listing's external IDs appeared in the
    current scan's *collected_external_ids*, the listing is marked as
    ``'likely_expired'``.

    Args:
        conn: Open database connection.
        collected_external_ids: The set of external IDs that were seen
            during the most recent collection run.

    Returns:
        The number of listings that were marked as likely expired.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = conn.execute(
        """SELECT l.id, l.application_deadline
           FROM listings l
           WHERE l.listing_status = 'active'
             AND l.application_deadline IS NOT NULL
             AND l.application_deadline < ?""",
        (now,),
    ).fetchall()

    marked = 0
    for row in rows:
        source_ids = conn.execute(
            "SELECT external_id FROM listing_sources WHERE listing_id = ?",
            (row["id"],),
        ).fetchall()
        seen = any(r["external_id"] in collected_external_ids for r in source_ids)
        if not seen:
            mark_likely_expired(conn, row["id"])
            marked += 1
    return marked


def _serialize_list(value: list[str]) -> str | None:
    """Serialize a list to JSON string for DB storage."""
    if not value:
        return None
    return json.dumps(value)


def _deserialize_list(value: str | None) -> list[str]:
    """Deserialize a JSON string from DB back to a list."""
    if not value:
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
        return [str(parsed)]
    except (json.JSONDecodeError, TypeError):
        return [value] if value else []


def save_analysis(conn: sqlite3.Connection, result: AnalysisResult) -> int:
    """Persist an analysis result, inserting or updating as needed.

    Uses an ``INSERT ... ON CONFLICT DO UPDATE`` (upsert) keyed on the
    combination of ``listing_id`` and ``profile_hash``.  If the result
    contains an ``analysis_error``, the ``fail_count`` is incremented from
    the previously stored value so that permanently failing listings can
    be skipped after exceeding the retry limit.

    Args:
        conn: Open database connection.
        result: The ``AnalysisResult`` to save.

    Returns:
        The ``rowid`` of the inserted or updated analyses row.
    """
    # On failure, increment fail_count from existing row
    fail_count = result.fail_count
    if result.analysis_error:
        existing = conn.execute(
            "SELECT fail_count FROM analyses WHERE listing_id = ? AND profile_hash = ?",
            (result.listing_id, result.profile_hash),
        ).fetchone()
        if existing:
            fail_count = existing["fail_count"] + 1
        else:
            fail_count = 1

    cursor = conn.execute(
        """INSERT INTO analyses
           (listing_id, match_score, match_reasons, missing_skills, strengths,
            concerns, summary, application_notes, analysis_error, fail_count, profile_hash)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(listing_id, profile_hash) DO UPDATE SET
               match_score=excluded.match_score,
               match_reasons=excluded.match_reasons,
               missing_skills=excluded.missing_skills,
               strengths=excluded.strengths,
               concerns=excluded.concerns,
               summary=excluded.summary,
               application_notes=excluded.application_notes,
               analysis_error=excluded.analysis_error,
               fail_count=excluded.fail_count,
               analyzed_at=datetime('now')""",
        (
            result.listing_id,
            result.match_score,
            _serialize_list(result.match_reasons),
            _serialize_list(result.missing_skills),
            _serialize_list(result.strengths),
            _serialize_list(result.concerns),
            result.summary,
            result.application_notes,
            result.analysis_error,
            fail_count,
            result.profile_hash,
        ),
    )
    conn.commit()
    return cursor.lastrowid


def get_analysis(conn: sqlite3.Connection, listing_id: int) -> AnalysisResult | None:
    """Fetch the most recent analysis for a given listing.

    Args:
        conn: Open database connection.
        listing_id: Primary key of the listing whose analysis is requested.

    Returns:
        An ``AnalysisResult`` object, or *None* if the listing has not been
        analysed yet.
    """
    row = conn.execute(
        "SELECT * FROM analyses WHERE listing_id = ? ORDER BY analyzed_at DESC LIMIT 1",
        (listing_id,),
    ).fetchone()
    if not row:
        return None
    return AnalysisResult(
        listing_id=row["listing_id"],
        match_score=row["match_score"],
        match_reasons=_deserialize_list(row["match_reasons"]),
        missing_skills=_deserialize_list(row["missing_skills"]),
        strengths=_deserialize_list(row["strengths"]),
        concerns=_deserialize_list(row["concerns"]),
        summary=row["summary"],
        application_notes=row["application_notes"],
        analysis_error=row["analysis_error"],
        fail_count=row["fail_count"],
        profile_hash=row["profile_hash"],
        analyzed_at=row["analyzed_at"],
    )


def get_unanalyzed_listings(
    conn: sqlite3.Connection, profile_hash: str, limit: int | None = None
) -> list[JobListing]:
    """Return listings that still need analysis for a given profile.

    A listing is considered "unanalysed" if it either has no analysis row
    for *profile_hash* at all, or its existing analysis row has
    ``analysis_error`` set (meaning a previous attempt failed).  Listings
    whose ``fail_count`` has reached ``MAX_ANALYSIS_FAIL_RETRIES`` are
    treated as permanently failed and excluded.

    Args:
        conn: Open database connection.
        profile_hash: Hash identifying the user profile to analyse against.
        limit: Maximum number of listings to return.  *None* means no limit.

    Returns:
        A list of ``JobListing`` objects ordered by creation date, newest
        first.
    """
    query = """
        SELECT l.* FROM listings l
        WHERE NOT EXISTS (
            SELECT 1 FROM analyses a
            WHERE a.listing_id = l.id AND a.profile_hash = ?
              AND a.analysis_error IS NULL
        )
        AND NOT EXISTS (
            SELECT 1 FROM analyses a
            WHERE a.listing_id = l.id AND a.profile_hash = ?
              AND a.fail_count >= ?
        )
        ORDER BY l.created_at DESC
    """
    params: list = [profile_hash, profile_hash, MAX_ANALYSIS_FAIL_RETRIES]
    if limit:
        query += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()
    listings = []
    for row in rows:
        sources = [
            r["source"]
            for r in conn.execute(
                "SELECT source FROM listing_sources WHERE listing_id = ?", (row["id"],)
            ).fetchall()
        ]
        listings.append(_row_to_listing(row, sources))
    return listings


def get_failed_listings(
    conn: sqlite3.Connection, profile_hash: str, limit: int | None = None
) -> list[JobListing]:
    """Return listings whose most recent analysis ended with an error.

    Intended for the ``--retry-failed`` CLI flag.  Only includes listings
    whose ``fail_count`` is still below ``MAX_ANALYSIS_FAIL_RETRIES`` so
    that permanently broken analyses are not retried forever.

    Args:
        conn: Open database connection.
        profile_hash: Hash identifying the user profile to filter by.
        limit: Maximum number of listings to return.  *None* means no limit.

    Returns:
        A list of ``JobListing`` objects ordered by creation date, newest
        first.
    """
    query = """
        SELECT l.* FROM listings l
        WHERE EXISTS (
            SELECT 1 FROM analyses a
            WHERE a.listing_id = l.id AND a.profile_hash = ?
              AND a.analysis_error IS NOT NULL
              AND a.fail_count < ?
        )
        ORDER BY l.created_at DESC
    """
    params: list = [profile_hash, MAX_ANALYSIS_FAIL_RETRIES]
    if limit:
        query += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()
    listings = []
    for row in rows:
        sources = [
            r["source"]
            for r in conn.execute(
                "SELECT source FROM listing_sources WHERE listing_id = ?", (row["id"],)
            ).fetchall()
        ]
        listings.append(_row_to_listing(row, sources))
    return listings


def get_stats(conn: sqlite3.Connection) -> Stats:
    """Compute high-level statistics about all data in the database.

    Counts listings, source entries, analyses, and calculates the average
    match score and per-source breakdown.  The ``dedup_savings`` field
    shows how many duplicate source entries were collapsed into shared
    listing rows.

    Args:
        conn: Open database connection.

    Returns:
        A ``Stats`` object with totals, averages, and per-source counts.
    """
    total_listings = conn.execute("SELECT COUNT(*) as c FROM listings").fetchone()["c"]
    total_sources = conn.execute("SELECT COUNT(*) as c FROM listing_sources").fetchone()["c"]
    total_analyses = conn.execute("SELECT COUNT(*) as c FROM analyses").fetchone()["c"]
    avg_score = conn.execute("SELECT AVG(match_score) as a FROM analyses").fetchone()["a"]

    source_counts = {
        r["source"]: r["c"]
        for r in conn.execute(
            "SELECT source, COUNT(*) as c FROM listing_sources GROUP BY source"
        ).fetchall()
    }

    dedup_savings = total_sources - total_listings

    return Stats(
        total_listings=total_listings,
        total_source_entries=total_sources,
        dedup_savings=dedup_savings,
        total_analyses=total_analyses,
        avg_score=round(avg_score, 1) if avg_score else 0,
        source_counts=source_counts,
    )
