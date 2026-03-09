"""DB init and migrations."""

from __future__ import annotations

import sqlite3

from jobhaul.log import get_logger

logger = get_logger(__name__)

SCHEMA_VERSION = 3

MIGRATIONS = {
    2: "ALTER TABLE analyses ADD COLUMN analysis_error TEXT",
    3: "ALTER TABLE analyses ADD COLUMN fail_count INTEGER NOT NULL DEFAULT 0",
}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    company TEXT,
    location TEXT,
    description TEXT,
    url TEXT,
    published_at TEXT,
    is_remote INTEGER DEFAULT 0,
    employment_type TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS listing_sources (
    listing_id INTEGER NOT NULL REFERENCES listings(id),
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    source_url TEXT,
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source, external_id)
);

CREATE TABLE IF NOT EXISTS analyses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id INTEGER NOT NULL REFERENCES listings(id),
    match_score INTEGER NOT NULL,
    match_reasons TEXT,
    missing_skills TEXT,
    strengths TEXT,
    concerns TEXT,
    summary TEXT,
    application_notes TEXT,
    analysis_error TEXT,
    fail_count INTEGER NOT NULL DEFAULT 0,
    profile_hash TEXT NOT NULL,
    analyzed_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(listing_id, profile_hash)
);

CREATE INDEX IF NOT EXISTS idx_listings_title_company
    ON listings(title, company);

CREATE INDEX IF NOT EXISTS idx_listing_sources_listing_id
    ON listing_sources(listing_id);

CREATE INDEX IF NOT EXISTS idx_analyses_listing_id
    ON analyses(listing_id);

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TEXT DEFAULT (datetime('now'))
);
"""


def run_migrations(conn: sqlite3.Connection) -> None:
    """Apply pending schema migrations."""
    current = conn.execute(
        "SELECT COALESCE(MAX(version), 1) FROM schema_version"
    ).fetchone()[0]
    for version in sorted(MIGRATIONS):
        if version > current:
            conn.execute(MIGRATIONS[version])
            conn.execute(
                "INSERT INTO schema_version (version) VALUES (?)", (version,)
            )
            conn.commit()
            logger.info("Applied DB migration to version %d", version)


def init_db(db_path: str) -> sqlite3.Connection:
    """Initialize the database and return a connection."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    conn.commit()

    # Stamp current version on a brand-new database so migrations are skipped
    row_count = conn.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0]
    if row_count == 0:
        conn.execute(
            "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
        )
        conn.commit()

    run_migrations(conn)

    # Merge any existing duplicates that slipped through with old normalization
    from jobhaul.db.queries import merge_existing_duplicates

    merged = merge_existing_duplicates(conn)
    if merged:
        logger.info("Merged %d existing duplicate(s)", merged)

    logger.info("Database initialized at %s", db_path)
    return conn
