"""SchemaReader abstraction for data warehouse metadata access.

Defines a structural protocol so that ``application.mart_service`` can
read table metadata without depending directly on DuckDB.  Any object that
implements ``read_tables() -> list[SourceTable]`` satisfies the protocol,
making it straightforward to add Snowflake, BigQuery, or in-memory readers
without changing the service layer.

``DuckDBSchemaReader`` is the first concrete implementation and wraps the
existing ``DuckDBConnector`` + ``read_tables`` function pair.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from metadata.connector import DuckDBConnector
from metadata.schema import SourceTable
from metadata.schema_reader import read_tables as _read_tables_from_conn


@runtime_checkable
class SchemaReader(Protocol):
    """Structural protocol for reading source table metadata from a data warehouse.

    Any class that provides a ``read_tables`` method returning a list of
    ``SourceTable`` objects satisfies this protocol — no inheritance required.
    """

    def read_tables(self) -> list[SourceTable]:
        """Return metadata for all tables visible to this reader."""
        ...


class DuckDBSchemaReader:
    """Reads source table metadata from a DuckDB database file.

    Implements the ``SchemaReader`` protocol by wrapping the existing
    ``DuckDBConnector`` context manager and ``read_tables`` function.

    Parameters
    ----------
    database_path:
        Absolute or relative path to the DuckDB ``.db`` file, or
        ``":memory:"`` for an in-memory database.
    """

    def __init__(self, database_path: str) -> None:
        self._database_path = database_path

    def read_tables(self) -> list[SourceTable]:
        """Open the DuckDB database and return metadata for all tables."""
        with DuckDBConnector(self._database_path, read_only=True) as conn:
            return _read_tables_from_conn(conn)
