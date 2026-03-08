"""All DB operations."""

from __future__ import annotations

import json
import sqlite3

from jobhaul.models import AnalysisResult, JobListing, RawListing


def _normalize(text: str | None) -> str:
    """Lowercase and strip whitespace for dedup matching."""
    if not text:
        return ""
    return text.strip().lower()


def find_duplicate(conn: sqlite3.Connection, title: str, company: str | None) -> int | None:
    """Find an existing listing with the same normalized title+company."""
    row = conn.execute(
        "SELECT id FROM listings WHERE LOWER(TRIM(title)) = ? AND LOWER(TRIM(COALESCE(company, ''))) = ?",
        (_normalize(title), _normalize(company)),
    ).fetchone()
    return row["id"] if row else None


def upsert_listing(conn: sqlite3.Connection, raw: RawListing) -> int:
    """Insert or deduplicate a listing, returning the listing ID."""
    existing_id = find_duplicate(conn, raw.title, raw.company)

    if existing_id:
        listing_id = existing_id
    else:
        cursor = conn.execute(
            """INSERT INTO listings (title, company, location, description, url,
               published_at, is_remote, employment_type)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                raw.title,
                raw.company,
                raw.location,
                raw.description,
                raw.url,
                raw.published_at,
                int(raw.is_remote),
                raw.employment_type,
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
        sources=sources,
        created_at=row["created_at"],
    )


def list_listings(
    conn: sqlite3.Connection,
    *,
    days: int = 7,
    source: str | None = None,
    min_score: int | None = None,
    limit: int | None = None,
) -> list[JobListing]:
    """List listings with optional filters."""
    query = """
        SELECT DISTINCT l.* FROM listings l
        LEFT JOIN listing_sources ls ON l.id = ls.listing_id
        LEFT JOIN analyses a ON l.id = a.listing_id
        WHERE l.created_at >= datetime('now', ?)
    """
    params: list = [f"-{days} days"]

    if source:
        query += " AND ls.source = ?"
        params.append(source)

    if min_score is not None:
        query += " AND a.match_score >= ?"
        params.append(min_score)

    query += " ORDER BY l.created_at DESC"

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
        listings.append(
            JobListing(
                id=row["id"],
                title=row["title"],
                company=row["company"],
                location=row["location"],
                description=row["description"],
                url=row["url"],
                published_at=row["published_at"],
                is_remote=bool(row["is_remote"]),
                employment_type=row["employment_type"],
                sources=sources,
                created_at=row["created_at"],
            )
        )
    return listings


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
    cursor = conn.execute(
        """INSERT INTO analyses
           (listing_id, match_score, match_reasons, missing_skills, strengths,
            concerns, summary, application_notes, profile_hash)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(listing_id, profile_hash) DO UPDATE SET
               match_score=excluded.match_score,
               match_reasons=excluded.match_reasons,
               missing_skills=excluded.missing_skills,
               strengths=excluded.strengths,
               concerns=excluded.concerns,
               summary=excluded.summary,
               application_notes=excluded.application_notes,
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
        profile_hash=row["profile_hash"],
        analyzed_at=row["analyzed_at"],
    )


def get_unanalyzed_listings(
    conn: sqlite3.Connection, profile_hash: str, limit: int | None = None
) -> list[JobListing]:
    """Get listings that haven't been analyzed with this profile hash."""
    query = """
        SELECT l.* FROM listings l
        WHERE NOT EXISTS (
            SELECT 1 FROM analyses a
            WHERE a.listing_id = l.id AND a.profile_hash = ?
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
        listings.append(
            JobListing(
                id=row["id"],
                title=row["title"],
                company=row["company"],
                location=row["location"],
                description=row["description"],
                url=row["url"],
                published_at=row["published_at"],
                is_remote=bool(row["is_remote"]),
                employment_type=row["employment_type"],
                sources=sources,
                created_at=row["created_at"],
            )
        )
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
