"""Unit tests for metadata.schema_reader using an in-memory DuckDB database."""

import pytest
import duckdb

from metadata.schema_reader import read_table, read_tables
from metadata.schema import SourceTable


@pytest.fixture()
def conn():
    """Provide a fresh in-memory DuckDB connection with sample tables."""
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE orders (
            order_id   INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_date  DATE,
            total_amount DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO orders VALUES
            (1, 101, '2024-01-01', 150.0),
            (2, 102, '2024-01-02', 200.5),
            (3, 101, '2024-01-03', 75.0)
        """
    )
    connection.execute(
        """
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name        VARCHAR NOT NULL,
            region      VARCHAR
        )
        """
    )
    connection.execute(
        """
        INSERT INTO customers VALUES
            (101, 'Alice', 'North'),
            (102, 'Bob',   'South')
        """
    )
    yield connection
    connection.close()


class TestReadTable:
    def test_returns_source_table_model(self, conn):
        result = read_table(conn, "orders")
        assert isinstance(result, SourceTable)
        assert result.name == "orders"
        assert result.schema_name == "main"

    def test_column_names_and_types(self, conn):
        result = read_table(conn, "orders")
        col_map = {c.name: c for c in result.columns}
        assert "order_id" in col_map
        assert "total_amount" in col_map
        assert col_map["order_id"].data_type.upper() == "INTEGER"

    def test_primary_key_detected(self, conn):
        result = read_table(conn, "orders")
        col_map = {c.name: c for c in result.columns}
        assert col_map["order_id"].is_primary_key is True
        assert col_map["customer_id"].is_primary_key is False

    def test_nullable_flag(self, conn):
        result = read_table(conn, "orders")
        col_map = {c.name: c for c in result.columns}
        # order_date has no NOT NULL constraint → nullable
        assert col_map["order_date"].is_nullable is True
        # customer_id declared NOT NULL
        assert col_map["customer_id"].is_nullable is False

    def test_row_count(self, conn):
        result = read_table(conn, "orders", include_row_count=True)
        assert result.row_count == 3

    def test_row_count_skipped_when_disabled(self, conn):
        result = read_table(conn, "orders", include_row_count=False)
        assert result.row_count is None

    def test_sample_values_collected(self, conn):
        result = read_table(conn, "customers", include_sample_values=True)
        region_col = next(c for c in result.columns if c.name == "region")
        assert set(region_col.sample_values) == {"North", "South"}

    def test_sample_values_empty_when_disabled(self, conn):
        result = read_table(conn, "customers", include_sample_values=False)
        for col in result.columns:
            assert col.sample_values == []

    def test_raises_for_missing_table(self, conn):
        with pytest.raises(ValueError, match="not found"):
            read_table(conn, "nonexistent_table")


class TestReadTables:
    def test_returns_all_tables(self, conn):
        tables = read_tables(conn)
        names = {t.name for t in tables}
        assert names == {"orders", "customers"}

    def test_sorted_by_name(self, conn):
        tables = read_tables(conn)
        assert [t.name for t in tables] == sorted(t.name for t in tables)

    def test_empty_schema_returns_empty_list(self, conn):
        conn.execute("CREATE SCHEMA empty_ns")
        tables = read_tables(conn, schema="empty_ns")
        assert tables == []
