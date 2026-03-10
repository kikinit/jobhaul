"""Database package for jobhaul with shared connection management.

Provides ``get_db``, a context manager that opens an SQLite connection,
initialises the schema (creating tables and running migrations if needed),
and guarantees the connection is closed when the caller is done.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path


@contextmanager
def get_db(db_path: str | Path | None = None):
    """Open an initialised SQLite database connection and close it automatically.

    This is the main entry point for obtaining a database connection anywhere
    in the application.  It creates the file (and all tables) if it does not
    exist yet, runs any pending schema migrations, and closes the connection
    when the ``with`` block exits.

    Args:
        db_path: Path to the SQLite database file.  When *None* (the
            default), the file ``jobhaul.db`` inside the user data directory
            is used.

    Yields:
        sqlite3.Connection: A ready-to-use database connection with WAL mode
            and foreign keys enabled.
    """
    from jobhaul.config import ensure_data_dir
    from jobhaul.db.schema import init_db

    path = str(db_path or ensure_data_dir() / "jobhaul.db")
    conn = init_db(path)
    try:
        yield conn
    finally:
        conn.close()
