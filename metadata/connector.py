"""DuckDB connection management for metadata extraction."""

from __future__ import annotations

from pathlib import Path
from typing import Generator

import duckdb


class DuckDBConnector:
    """Context manager that opens and closes a DuckDB connection.

    Usage::

        with DuckDBConnector(":memory:") as conn:
            results = conn.execute("SELECT 1").fetchall()

    Parameters
    ----------
    database:
        File path to a DuckDB database file, or ``":memory:"`` for an
        in-process ephemeral database.  Defaults to ``":memory:"``.
    read_only:
        Open the database in read-only mode.  Recommended for production
        connections to shared warehouse files.
    """

    def __init__(
        self,
        database: str | Path = ":memory:",
        read_only: bool = False,
    ) -> None:
        self.database = str(database)
        self.read_only = read_only
        self._conn: duckdb.DuckDBPyConnection | None = None

    # ------------------------------------------------------------------
    # Context manager protocol
    # ------------------------------------------------------------------

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        self._conn = duckdb.connect(self.database, read_only=self.read_only)
        return self._conn

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def get_connection(
    database: str | Path = ":memory:",
    read_only: bool = False,
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Generator that yields a DuckDB connection and ensures it is closed.

    Intended for use with FastAPI ``Depends`` or similar dependency injection
    patterns::

        def my_endpoint(conn=Depends(get_connection)):
            ...

    Parameters
    ----------
    database:
        File path to a DuckDB database file, or ``":memory:"``.
    read_only:
        Open the database in read-only mode.
    """
    with DuckDBConnector(database, read_only=read_only) as conn:
        yield conn
