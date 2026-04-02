"""Unit tests for metadata.reader — SchemaReader protocol and DuckDBSchemaReader."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from metadata.reader import DuckDBSchemaReader, SchemaReader
from metadata.schema import SourceColumn, SourceTable


# ---------------------------------------------------------------------------
# SchemaReader protocol
# ---------------------------------------------------------------------------


class TestSchemaReaderProtocol:
    def test_duck_db_reader_satisfies_protocol(self):
        reader = DuckDBSchemaReader(":memory:")
        assert isinstance(reader, SchemaReader)

    def test_any_object_with_read_tables_satisfies_protocol(self):
        class InMemoryReader:
            def read_tables(self) -> list[SourceTable]:
                return []

        assert isinstance(InMemoryReader(), SchemaReader)

    def test_object_without_read_tables_does_not_satisfy_protocol(self):
        class NoReader:
            pass

        assert not isinstance(NoReader(), SchemaReader)


# ---------------------------------------------------------------------------
# DuckDBSchemaReader
# ---------------------------------------------------------------------------


class TestDuckDBSchemaReader:
    @pytest.fixture()
    def sample_tables(self) -> list[SourceTable]:
        return [
            SourceTable(
                name="orders",
                schema_name="main",
                columns=[
                    SourceColumn(name="order_id", data_type="INTEGER", is_primary_key=True, is_nullable=False),
                    SourceColumn(name="total_amount", data_type="DOUBLE"),
                ],
            )
        ]

    def test_read_tables_returns_source_tables(self, sample_tables):
        reader = DuckDBSchemaReader("/data/warehouse.db")

        mock_conn = MagicMock()
        mock_connector = MagicMock()
        mock_connector.__enter__ = MagicMock(return_value=mock_conn)
        mock_connector.__exit__ = MagicMock(return_value=False)

        with (
            patch("metadata.reader.DuckDBConnector", return_value=mock_connector),
            patch("metadata.reader._read_tables_from_conn", return_value=sample_tables),
        ):
            result = reader.read_tables()

        assert result == sample_tables

    def test_connector_opened_with_stored_path_and_read_only(self, sample_tables):
        reader = DuckDBSchemaReader("/data/warehouse.db")

        mock_conn = MagicMock()
        mock_connector = MagicMock()
        mock_connector.__enter__ = MagicMock(return_value=mock_conn)
        mock_connector.__exit__ = MagicMock(return_value=False)

        with (
            patch("metadata.reader.DuckDBConnector", return_value=mock_connector) as mock_cls,
            patch("metadata.reader._read_tables_from_conn", return_value=sample_tables),
        ):
            reader.read_tables()

        mock_cls.assert_called_once_with("/data/warehouse.db", read_only=True)

    def test_read_tables_called_with_connection(self, sample_tables):
        reader = DuckDBSchemaReader("/data/warehouse.db")

        mock_conn = MagicMock()
        mock_connector = MagicMock()
        mock_connector.__enter__ = MagicMock(return_value=mock_conn)
        mock_connector.__exit__ = MagicMock(return_value=False)

        with (
            patch("metadata.reader.DuckDBConnector", return_value=mock_connector),
            patch("metadata.reader._read_tables_from_conn", return_value=sample_tables) as mock_fn,
        ):
            reader.read_tables()

        mock_fn.assert_called_once_with(mock_conn)
