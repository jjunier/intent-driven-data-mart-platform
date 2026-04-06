"""Integration tests for DuckDBSchemaReader.read_tables().

No mocks — all code runs against a real DuckDB database file (tmp_path fixture).
Verifies that the reader correctly discovers tables and maps column metadata.
"""
from __future__ import annotations

import pytest

from metadata.reader import DuckDBSchemaReader
from metadata.schema import SourceTable


@pytest.mark.integration
class TestDuckDBSchemaReaderIntegration:
    def test_read_tables_returns_list(self, duckdb_path):
        tables = DuckDBSchemaReader(duckdb_path).read_tables()
        assert isinstance(tables, list)

    def test_orders_table_discovered(self, duckdb_path):
        tables = DuckDBSchemaReader(duckdb_path).read_tables()
        assert any(t.name == "orders" for t in tables)

    def test_all_entries_are_source_table_instances(self, duckdb_path):
        tables = DuckDBSchemaReader(duckdb_path).read_tables()
        assert all(isinstance(t, SourceTable) for t in tables)

    def test_orders_has_expected_columns(self, duckdb_path):
        tables = DuckDBSchemaReader(duckdb_path).read_tables()
        orders = next(t for t in tables if t.name == "orders")
        col_names = {c.name for c in orders.columns}
        assert {"order_id", "customer_id", "amount"}.issubset(col_names)

    def test_column_data_types_are_non_empty(self, duckdb_path):
        tables = DuckDBSchemaReader(duckdb_path).read_tables()
        orders = next(t for t in tables if t.name == "orders")
        assert all(c.data_type for c in orders.columns)
