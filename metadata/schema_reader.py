"""Reads table and column metadata from a DuckDB database.

All functions accept an open ``duckdb.DuckDBPyConnection`` and return
Pydantic models defined in ``metadata.schema``.  No LLM calls are made here;
this module is pure SQL against DuckDB's information_schema.
"""

from __future__ import annotations

import duckdb

from metadata.schema import SourceColumn, SourceTable

# Maximum number of distinct sample values collected per column.
_MAX_SAMPLE_VALUES = 5


def read_tables(
    conn: duckdb.DuckDBPyConnection,
    schema: str = "main",
    include_row_counts: bool = True,
    include_sample_values: bool = False,
) -> list[SourceTable]:
    """Return metadata for every base table in *schema*.

    Parameters
    ----------
    conn:
        An open DuckDB connection.
    schema:
        The DuckDB schema (namespace) to inspect.  Defaults to ``"main"``.
    include_row_counts:
        When ``True``, execute ``COUNT(*)`` for each table.  Adds one query
        per table; set to ``False`` for faster catalogue-only reads.
    include_sample_values:
        When ``True``, collect up to five distinct sample values per column.
        Adds one query per column; disabled by default for performance.

    Returns
    -------
    list[SourceTable]
        One entry per base table found in *schema*, sorted by table name.
    """
    table_rows = conn.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        [schema],
    ).fetchall()

    return [
        read_table(
            conn,
            table_name=row[0],
            schema=schema,
            include_row_count=include_row_counts,
            include_sample_values=include_sample_values,
        )
        for row in table_rows
    ]


def read_table(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    schema: str = "main",
    include_row_count: bool = True,
    include_sample_values: bool = False,
) -> SourceTable:
    """Return metadata for a single table.

    Parameters
    ----------
    conn:
        An open DuckDB connection.
    table_name:
        Unquoted table name as it appears in ``information_schema``.
    schema:
        The DuckDB schema that contains *table_name*.
    include_row_count:
        When ``True``, execute ``COUNT(*)`` to populate ``SourceTable.row_count``.
    include_sample_values:
        When ``True``, populate ``SourceColumn.sample_values`` with up to five
        distinct non-null values for each column.

    Returns
    -------
    SourceTable
        Fully populated metadata model for the requested table.

    Raises
    ------
    ValueError
        If *table_name* does not exist in *schema*.
    """
    primary_keys = _get_primary_keys(conn, table_name, schema)
    column_rows = conn.execute(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name   = ?
        ORDER BY ordinal_position
        """,
        [schema, table_name],
    ).fetchall()

    if not column_rows:
        raise ValueError(
            f"Table '{schema}.{table_name}' not found or has no columns."
        )

    columns: list[SourceColumn] = []
    for col_name, data_type, is_nullable_str in column_rows:
        sample_values: list[str] = []
        if include_sample_values:
            sample_values = _get_sample_values(conn, schema, table_name, col_name)

        columns.append(
            SourceColumn(
                name=col_name,
                data_type=data_type,
                is_nullable=(is_nullable_str.upper() == "YES"),
                is_primary_key=(col_name in primary_keys),
                sample_values=sample_values,
            )
        )

    row_count: int | None = None
    if include_row_count:
        row_count = _get_row_count(conn, schema, table_name)

    return SourceTable(
        name=table_name,
        schema_name=schema,
        columns=columns,
        row_count=row_count,
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _get_primary_keys(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    schema: str,
) -> set[str]:
    """Return the set of column names that form the primary key of *table_name*.

    Uses DuckDB's ``duckdb_constraints`` system table which exposes
    constraint metadata including ``UNIQUE`` and ``PRIMARY KEY`` entries.
    Falls back to an empty set if the table has no explicit primary key.
    """
    rows = conn.execute(
        """
        SELECT UNNEST(constraint_column_names) AS col
        FROM duckdb_constraints()
        WHERE schema_name     = ?
          AND table_name      = ?
          AND constraint_type = 'PRIMARY KEY'
        """,
        [schema, table_name],
    ).fetchall()
    return {row[0] for row in rows}


def _get_row_count(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    table_name: str,
) -> int:
    """Execute ``COUNT(*)`` and return the result as an integer."""
    result = conn.execute(
        f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
    ).fetchone()
    return int(result[0]) if result else 0


def _get_sample_values(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    table_name: str,
    column_name: str,
) -> list[str]:
    """Return up to *_MAX_SAMPLE_VALUES* distinct non-null values as strings."""
    rows = conn.execute(
        f"""
        SELECT DISTINCT "{column_name}"
        FROM "{schema}"."{table_name}"
        WHERE "{column_name}" IS NOT NULL
        LIMIT {_MAX_SAMPLE_VALUES}
        """
    ).fetchall()
    return [str(row[0]) for row in rows]
