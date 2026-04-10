"""Unit tests for metadata.bigquery_reader.

All tests use mock/stub clients — no GCP credentials, network access, or
real BigQuery queries are executed.  The test suite validates:

- ``BigQuerySchemaReader`` satisfies the ``SchemaReader`` protocol.
- ``read_tables()`` issues the expected SQL against INFORMATION_SCHEMA.
- Column metadata (name, data_type, is_nullable, is_primary_key) is mapped
  correctly from query results.
- ``row_count`` is populated when available and falls back to ``None`` on
  failure, without aborting the full call.
- A clear ``ImportError`` is raised when google-cloud-bigquery is not
  installed and no client is injected.
- Constructor options (``include_row_counts=False``) are respected.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from metadata.bigquery_reader import BigQueryConnectionConfig, BigQuerySchemaReader
from metadata.reader import SchemaReader
from metadata.schema import SourceTable


# ---------------------------------------------------------------------------
# Shared helpers / fixtures
# ---------------------------------------------------------------------------


def _make_config(
    project_id: str = "my-project",
    dataset_id: str = "my_dataset",
) -> BigQueryConnectionConfig:
    return BigQueryConnectionConfig(project_id=project_id, dataset_id=dataset_id)


def _make_client(
    table_names: list[str],
    columns_by_table: dict[str, list[tuple[str, str, str]]],
    row_counts: dict[str, int] | None = None,
    row_count_raises: bool = False,
) -> MagicMock:
    """Build a mock BigQuery client whose ``query().result()`` returns canned data.

    Parameters
    ----------
    table_names:
        Ordered list of table names returned by the TABLES query.
    columns_by_table:
        Mapping of table_name → list of (column_name, data_type, is_nullable)
        tuples returned by the COLUMNS query for that table.
    row_counts:
        Mapping of table_id → row_count returned by ``__TABLES__``.
        If ``None``, a default empty mapping is used.
    row_count_raises:
        If ``True``, the ``__TABLES__`` query raises an exception.
    """
    client = MagicMock()

    def _query_side_effect(sql: str) -> MagicMock:
        result_mock = MagicMock()

        if "INFORMATION_SCHEMA.TABLES" in sql:
            rows = [SimpleNamespace(table_name=n) for n in table_names]
            result_mock.result.return_value = rows

        elif "__TABLES__" in sql:
            if row_count_raises:
                result_mock.result.side_effect = Exception("BQ metadata error")
            else:
                rc = row_counts or {}
                rows = [
                    SimpleNamespace(table_id=tid, row_count=cnt)
                    for tid, cnt in rc.items()
                ]
                result_mock.result.return_value = rows

        elif "INFORMATION_SCHEMA.COLUMNS" in sql:
            # Determine which table this COLUMNS query targets
            matched_table = next(
                (t for t in columns_by_table if f"'{t}'" in sql), None
            )
            col_rows = columns_by_table.get(matched_table, [])
            rows = [
                SimpleNamespace(
                    column_name=col,
                    data_type=dtype,
                    is_nullable=nullable,
                )
                for col, dtype, nullable in col_rows
            ]
            result_mock.result.return_value = rows

        else:
            result_mock.result.return_value = []

        return result_mock

    client.query.side_effect = _query_side_effect
    return client


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestSchemaReaderProtocol:
    def test_bigquery_reader_satisfies_schema_reader_protocol(self):
        reader = BigQuerySchemaReader(config=_make_config(), client=MagicMock())
        assert isinstance(reader, SchemaReader)

    def test_bigquery_reader_without_client_still_satisfies_protocol(self):
        # Protocol check is structural — no live client needed.
        reader = BigQuerySchemaReader(config=_make_config())
        assert isinstance(reader, SchemaReader)


# ---------------------------------------------------------------------------
# read_tables — basic happy path
# ---------------------------------------------------------------------------


class TestReadTablesHappyPath:
    @pytest.fixture()
    def client(self) -> MagicMock:
        return _make_client(
            table_names=["customers", "orders"],
            columns_by_table={
                "customers": [
                    ("customer_id", "INT64", "NO"),
                    ("name", "STRING", "YES"),
                ],
                "orders": [
                    ("order_id", "INT64", "NO"),
                    ("total_amount", "FLOAT64", "YES"),
                ],
            },
            row_counts={"customers": 500, "orders": 1200},
        )

    def test_returns_one_source_table_per_table_name(self, client):
        reader = BigQuerySchemaReader(config=_make_config(), client=client)
        result = reader.read_tables()
        assert len(result) == 2
        assert [t.name for t in result] == ["customers", "orders"]

    def test_schema_name_is_dataset_id(self, client):
        reader = BigQuerySchemaReader(config=_make_config(dataset_id="sales"), client=client)
        result = reader.read_tables()
        for table in result:
            assert table.schema_name == "sales"

    def test_columns_mapped_correctly(self, client):
        reader = BigQuerySchemaReader(config=_make_config(), client=client)
        result = reader.read_tables()

        customers = next(t for t in result if t.name == "customers")
        assert len(customers.columns) == 2

        cid = customers.columns[0]
        assert cid.name == "customer_id"
        assert cid.data_type == "INT64"
        assert cid.is_nullable is False

        name_col = customers.columns[1]
        assert name_col.name == "name"
        assert name_col.data_type == "STRING"
        assert name_col.is_nullable is True

    def test_is_primary_key_always_false(self, client):
        reader = BigQuerySchemaReader(config=_make_config(), client=client)
        result = reader.read_tables()
        for table in result:
            for col in table.columns:
                assert col.is_primary_key is False

    def test_sample_values_always_empty(self, client):
        reader = BigQuerySchemaReader(config=_make_config(), client=client)
        result = reader.read_tables()
        for table in result:
            for col in table.columns:
                assert col.sample_values == []

    def test_row_counts_populated(self, client):
        reader = BigQuerySchemaReader(config=_make_config(), client=client)
        result = reader.read_tables()

        customers = next(t for t in result if t.name == "customers")
        orders = next(t for t in result if t.name == "orders")
        assert customers.row_count == 500
        assert orders.row_count == 1200

    def test_returns_list_of_source_table_instances(self, client):
        reader = BigQuerySchemaReader(config=_make_config(), client=client)
        result = reader.read_tables()
        assert all(isinstance(t, SourceTable) for t in result)


# ---------------------------------------------------------------------------
# read_tables — empty dataset
# ---------------------------------------------------------------------------


class TestReadTablesEmptyDataset:
    def test_returns_empty_list_when_no_tables(self):
        client = _make_client(table_names=[], columns_by_table={})
        reader = BigQuerySchemaReader(config=_make_config(), client=client)
        result = reader.read_tables()
        assert result == []


# ---------------------------------------------------------------------------
# row_count fallback behaviour
# ---------------------------------------------------------------------------


class TestRowCountFallback:
    def test_row_count_is_none_when_tables_metadata_raises(self):
        client = _make_client(
            table_names=["orders"],
            columns_by_table={"orders": [("order_id", "INT64", "NO")]},
            row_count_raises=True,
        )
        reader = BigQuerySchemaReader(config=_make_config(), client=client)
        result = reader.read_tables()
        assert result[0].row_count is None

    def test_row_count_is_none_when_table_not_in_metadata(self):
        client = _make_client(
            table_names=["orders"],
            columns_by_table={"orders": [("order_id", "INT64", "NO")]},
            row_counts={},  # no entry for "orders"
        )
        reader = BigQuerySchemaReader(config=_make_config(), client=client)
        result = reader.read_tables()
        assert result[0].row_count is None

    def test_read_tables_does_not_abort_on_row_count_failure(self):
        """A __TABLES__ error must not prevent columns from being returned."""
        client = _make_client(
            table_names=["orders"],
            columns_by_table={"orders": [("order_id", "INT64", "NO")]},
            row_count_raises=True,
        )
        reader = BigQuerySchemaReader(config=_make_config(), client=client)
        result = reader.read_tables()
        assert len(result) == 1
        assert len(result[0].columns) == 1


# ---------------------------------------------------------------------------
# include_row_counts=False
# ---------------------------------------------------------------------------


class TestIncludeRowCountsFalse:
    def test_no_tables_query_issued_when_row_counts_disabled(self):
        client = _make_client(
            table_names=["orders"],
            columns_by_table={"orders": [("order_id", "INT64", "NO")]},
        )
        reader = BigQuerySchemaReader(
            config=_make_config(),
            client=client,
            include_row_counts=False,
        )
        reader.read_tables()

        # Verify that __TABLES__ was never queried
        for mock_call in client.query.call_args_list:
            sql_arg = mock_call.args[0] if mock_call.args else ""
            assert "__TABLES__" not in sql_arg

    def test_row_count_is_none_when_disabled(self):
        client = _make_client(
            table_names=["orders"],
            columns_by_table={"orders": [("order_id", "INT64", "NO")]},
            row_counts={"orders": 999},
        )
        reader = BigQuerySchemaReader(
            config=_make_config(),
            client=client,
            include_row_counts=False,
        )
        result = reader.read_tables()
        assert result[0].row_count is None


# ---------------------------------------------------------------------------
# SQL structure — verify project/dataset interpolated correctly
# ---------------------------------------------------------------------------


class TestSqlInterpolation:
    def test_tables_query_uses_correct_project_and_dataset(self):
        client = _make_client(table_names=[], columns_by_table={})
        reader = BigQuerySchemaReader(
            config=BigQueryConnectionConfig(project_id="proj-x", dataset_id="ds_y"),
            client=client,
            include_row_counts=False,
        )
        reader.read_tables()

        tables_call_sql = client.query.call_args_list[0].args[0]
        assert "proj-x.ds_y.INFORMATION_SCHEMA.TABLES" in tables_call_sql

    def test_columns_query_targets_correct_table(self):
        client = _make_client(
            table_names=["events"],
            columns_by_table={"events": [("event_id", "STRING", "NO")]},
        )
        reader = BigQuerySchemaReader(
            config=BigQueryConnectionConfig(project_id="proj-x", dataset_id="ds_y"),
            client=client,
            include_row_counts=False,
        )
        reader.read_tables()

        # Find the COLUMNS query call
        columns_call = next(
            c for c in client.query.call_args_list
            if "INFORMATION_SCHEMA.COLUMNS" in c.args[0]
        )
        sql = columns_call.args[0]
        assert "proj-x.ds_y.INFORMATION_SCHEMA.COLUMNS" in sql
        assert "'events'" in sql


# ---------------------------------------------------------------------------
# ImportError when google-cloud-bigquery not installed
# ---------------------------------------------------------------------------


class TestImportErrorHandling:
    def test_import_error_raised_with_clear_message_when_bigquery_missing(self):
        """Simulate google-cloud-bigquery not being installed."""
        reader = BigQuerySchemaReader(config=_make_config())  # no client injected

        with patch.dict(sys.modules, {"google.cloud.bigquery": None, "google.cloud": None}):
            with pytest.raises(ImportError, match="google-cloud-bigquery"):
                reader.read_tables()

    def test_injected_client_bypasses_import_check(self):
        """Injected mock client must work even if the real package is absent."""
        client = _make_client(table_names=[], columns_by_table={})
        reader = BigQuerySchemaReader(config=_make_config(), client=client)

        # Should not raise even with the real package potentially missing
        result = reader.read_tables()
        assert result == []


# ---------------------------------------------------------------------------
# BigQueryConnectionConfig
# ---------------------------------------------------------------------------


class TestBigQueryConnectionConfig:
    def test_project_and_dataset_stored(self):
        cfg = BigQueryConnectionConfig(project_id="p", dataset_id="d")
        assert cfg.project_id == "p"
        assert cfg.dataset_id == "d"

    def test_credentials_default_none(self):
        cfg = BigQueryConnectionConfig(project_id="p", dataset_id="d")
        assert cfg.credentials is None

    def test_credentials_not_included_in_repr(self):
        cfg = BigQueryConnectionConfig(
            project_id="p", dataset_id="d", credentials="secret"
        )
        assert "secret" not in repr(cfg)
