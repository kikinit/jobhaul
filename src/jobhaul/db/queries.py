"""All DB operations."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3

from jobhaul.log import get_logger
from jobhaul.models import AnalysisResult, JobListing, RawListing

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
    """Find an existing listing with the same normalized title+company."""
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
    """Scan for existing duplicates and merge them. Returns number of merges performed."""
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
    """Insert or deduplicate a listing, returning the listing ID."""
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


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Check if a column exists in a table."""
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    return column in cols


def _row_to_listing(row: sqlite3.Row, sources: list[str]) -> JobListing:
    """Convert a DB row + sources list into a JobListing model."""
    keys = row.keys()
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
        seniority_level=row["seniority_level"] if "seniority_level" in keys else None,
        salary=row["salary"] if "salary" in keys else None,
        sources=sources,
        created_at=row["created_at"],
        application_deadline=row["application_deadline"] if "application_deadline" in keys else None,
        listing_status=row["listing_status"] if "listing_status" in keys else "active",
    )


def get_listing(conn: sqlite3.Connection, listing_id: int) -> JobListing | None:
    """Get a single listing by ID with its sources."""
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
    """List listings with optional filters."""
    has_status = _has_column(conn, "listings", "listing_status")

    query = """
        SELECT l.*, GROUP_CONCAT(ls.source) AS sources_csv
        FROM listings l
        LEFT JOIN listing_sources ls ON l.id = ls.listing_id
        LEFT JOIN analyses a ON l.id = a.listing_id
        WHERE l.created_at >= datetime('now', ?)
    """
    params: list = [f"-{days} days"]

    if has_status and not include_expired:
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
    """List listings with their analysis in a single query (no N+1)."""
    has_status = _has_column(conn, "listings", "listing_status")

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

    if has_status and not include_expired:
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
    """Mark a listing as likely expired."""
    conn.execute(
        "UPDATE listings SET listing_status = 'likely_expired' WHERE id = ?",
        (listing_id,),
    )
    conn.commit()


def mark_confirmed_expired(conn: sqlite3.Connection, listing_id: int) -> None:
    """Mark a listing as confirmed expired."""
    conn.execute(
        "UPDATE listings SET listing_status = 'confirmed_expired' WHERE id = ?",
        (listing_id,),
    )
    conn.commit()


def check_and_mark_expired(
    conn: sqlite3.Connection, collected_external_ids: set[str],
) -> int:
    """Mark active listings with past deadline not seen in current scan as likely expired.

    Returns the number of listings marked.
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
    """Save an analysis result (upsert on listing_id+profile_hash)."""
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
    """Get the most recent analysis for a listing."""
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
    """Get listings that haven't been successfully analyzed with this profile hash.

    Also re-queues listings whose latest analysis has analysis_error set,
    unless fail_count >= 5 (permanently failed).
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
              AND a.fail_count >= 5
        )
        ORDER BY l.created_at DESC
    """
    params: list = [profile_hash, profile_hash]
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
    """Get listings with analysis_error set (for --retry-failed)."""
    query = """
        SELECT l.* FROM listings l
        WHERE EXISTS (
            SELECT 1 FROM analyses a
            WHERE a.listing_id = l.id AND a.profile_hash = ?
              AND a.analysis_error IS NOT NULL
              AND a.fail_count < 5
        )
        ORDER BY l.created_at DESC
    """
    params: list = [profile_hash]
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


def get_stats(conn: sqlite3.Connection) -> dict:
    """Get summary statistics."""
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

    return {
        "total_listings": total_listings,
        "total_source_entries": total_sources,
        "dedup_savings": dedup_savings,
        "total_analyses": total_analyses,
        "avg_score": round(avg_score, 1) if avg_score else 0,
        "source_counts": source_counts,
    }
