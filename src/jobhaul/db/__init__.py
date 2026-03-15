"""Database package for jobhaul with shared connection management.

Provides ``get_db``, a synchronous context manager, and ``async_get_db``,
an asynchronous context manager that runs DB initialisation in a thread
executor so it does not block the asyncio event loop.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path


@contextmanager
def get_db(db_path: str | Path | None = None):
    """Open an initialised SQLite database connection and close it automatically.

    This is the main entry point for obtaining a database connection in
    synchronous code.  It creates the file (and all tables) if it does not
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


@asynccontextmanager
async def async_get_db(db_path: str | Path | None = None):
    """Async version of ``get_db`` that avoids blocking the event loop.

    Runs ``init_db`` inside a thread executor so that synchronous SQLite I/O
    does not stall the asyncio event loop.  Use this in ``async def`` code
    paths (e.g. the ``scan`` and ``analyze`` CLI commands).

    Args:
        db_path: Path to the SQLite database file.  When *None* the default
            location is used.

    Yields:
        sqlite3.Connection: A ready-to-use database connection.
    """
    from jobhaul.config import ensure_data_dir
    from jobhaul.db.schema import init_db

    path = str(db_path or ensure_data_dir() / "jobhaul.db")
    loop = asyncio.get_event_loop()
    conn = await loop.run_in_executor(None, init_db, path)
    try:
        yield conn
    finally:
        conn.close()
