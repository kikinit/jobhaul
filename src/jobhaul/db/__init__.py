"""Database package with shared connection management."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path


@contextmanager
def get_db(db_path: str | Path | None = None):
    """Context manager that yields an initialized DB connection and closes it on exit."""
    from jobhaul.config import ensure_data_dir
    from jobhaul.db.schema import init_db

    path = str(db_path or ensure_data_dir() / "jobhaul.db")
    conn = init_db(path)
    try:
        yield conn
    finally:
        conn.close()
