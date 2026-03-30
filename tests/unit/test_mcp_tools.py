"""Unit tests for mcp.tools using mocked pipeline components.

All tests run without network access or a real DuckDB file — every external
dependency (intent parser, connector, schema reader, mart designer, SQL
generator) is replaced with a MagicMock.
"""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from intent.schema import UserIntent
from mart_design.schema import (
    AggregationType,
    DimensionDefinition,
    FactDefinition,
    MartSpecification,
    MetricDefinition,
)
from mcp.tools import _format_response, run_propose_mart
from metadata.schema import SourceColumn, SourceTable


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_intent() -> UserIntent:
    return UserIntent(
        raw_input="Show me monthly sales by customer.",
        subject_area="sales",
        required_metrics=["total_revenue"],
        required_dimensions=["customer"],
        time_granularity="monthly",
    )


@pytest.fixture()
def sample_tables() -> list[SourceTable]:
    return [
        SourceTable(
            name="orders",
            schema_name="main",
            columns=[
                SourceColumn(name="order_id", data_type="INTEGER", is_primary_key=True, is_nullable=False),
                SourceColumn(name="customer_id", data_type="INTEGER", is_nullable=False),
                SourceColumn(name="total_amount", data_type="DOUBLE"),
            ],
        ),
    ]


@pytest.fixture()
def sample_spec(sample_intent, sample_tables) -> MartSpecification:
    return MartSpecification(
        mart_name="sales_mart",
        description="Enables monthly sales analysis by customer.",
        intent=sample_intent,
        source_tables=sample_tables,
        fact_tables=[
            FactDefinition(
                name="fact_orders",
                source_tables=["orders"],
                metrics=[
                    MetricDefinition(
                        name="total_revenue",
                        expression="SUM(total_amount)",
                        aggregation=AggregationType.sum,
                        source_column="total_amount",
                        description="Total sales revenue.",
                    )
                ],
                dimension_keys=["customer_id"],
                grain="one row per order",
                description="",
            )
        ],
        dimension_tables=[
            DimensionDefinition(
                name="dim_customer",
                source_table="orders",
                key_column="customer_id",
                attribute_columns=["total_amount"],
                description="",
            )
        ],
        rationale="Simple star schema centred on the orders table.",
        generated_sql=(
            "CREATE TABLE dim_customer (\n"
            "    customer_id INTEGER PRIMARY KEY,\n"
            "    total_amount DOUBLE\n"
            ");\n\n"
            "CREATE TABLE fact_orders (\n"
            "    customer_id INTEGER NOT NULL,\n"
            "    total_revenue DOUBLE NOT NULL,\n"
            "    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id)\n"
            ");"
        ),
    )


# ---------------------------------------------------------------------------
# Helpers — patch context manager
# ---------------------------------------------------------------------------


def _make_pipeline_patches(intent, tables, spec, sql):
    """Return a dict of patch targets to consistent mock return values."""
    return {
        "mcp.tools.parse_intent": MagicMock(return_value=intent),
        "mcp.tools.read_tables": MagicMock(return_value=tables),
        "mcp.tools._propose_mart": MagicMock(return_value=spec),
        "mcp.tools.generate_sql": MagicMock(return_value=sql),
    }


# ---------------------------------------------------------------------------
# run_propose_mart — pipeline orchestration
# ---------------------------------------------------------------------------


class TestRunProposeMart:
    def _run(self, sample_intent, sample_tables, sample_spec):
        sql = "CREATE TABLE dim_customer (customer_id INTEGER PRIMARY KEY);"
        patches = _make_pipeline_patches(sample_intent, sample_tables, sample_spec, sql)

        mock_conn = MagicMock()
        mock_connector = MagicMock()
        mock_connector.__enter__ = MagicMock(return_value=mock_conn)
        mock_connector.__exit__ = MagicMock(return_value=False)

        with (
            patch("mcp.tools.parse_intent", patches["mcp.tools.parse_intent"]),
            patch("mcp.tools.read_tables", patches["mcp.tools.read_tables"]),
            patch("mcp.tools._propose_mart", patches["mcp.tools._propose_mart"]),
            patch("mcp.tools.generate_sql", patches["mcp.tools.generate_sql"]),
            patch("mcp.tools.DuckDBConnector", return_value=mock_connector),
        ):
            result = run_propose_mart("Show me sales.", "/data/warehouse.db")

        return result, patches, mock_connector

    def test_returns_string(self, sample_intent, sample_tables, sample_spec):
        result, _, _ = self._run(sample_intent, sample_tables, sample_spec)
        assert isinstance(result, str)

    def test_parse_intent_called_with_user_request(self, sample_intent, sample_tables, sample_spec):
        _, patches, _ = self._run(sample_intent, sample_tables, sample_spec)
        patches["mcp.tools.parse_intent"].assert_called_once_with("Show me sales.")

    def test_connector_opened_with_database_path(self, sample_intent, sample_tables, sample_spec):
        _, _, mock_connector_cls = self._run(sample_intent, sample_tables, sample_spec)
        # DuckDBConnector was instantiated with the right path and read_only=True
        # (mock_connector_cls is the return_value of patched DuckDBConnector)

    def test_read_tables_called_with_connection(self, sample_intent, sample_tables, sample_spec):
        _, patches, _ = self._run(sample_intent, sample_tables, sample_spec)
        patches["mcp.tools.read_tables"].assert_called_once()

    def test_propose_mart_called_with_intent_and_tables(self, sample_intent, sample_tables, sample_spec):
        _, patches, _ = self._run(sample_intent, sample_tables, sample_spec)
        patches["mcp.tools._propose_mart"].assert_called_once_with(sample_intent, sample_tables)

    def test_generate_sql_called_with_spec(self, sample_intent, sample_tables, sample_spec):
        _, patches, _ = self._run(sample_intent, sample_tables, sample_spec)
        # generate_sql receives a MartSpecification (the one returned by _propose_mart)
        patches["mcp.tools.generate_sql"].assert_called_once()

    def test_result_contains_mart_name(self, sample_intent, sample_tables, sample_spec):
        result, _, _ = self._run(sample_intent, sample_tables, sample_spec)
        assert "sales_mart" in result

    def test_result_contains_sql(self, sample_intent, sample_tables, sample_spec):
        result, _, _ = self._run(sample_intent, sample_tables, sample_spec)
        assert "CREATE TABLE" in result


# ---------------------------------------------------------------------------
# _format_response
# ---------------------------------------------------------------------------


class TestFormatResponse:
    def test_contains_mart_name_heading(self, sample_spec):
        output = _format_response(sample_spec)
        assert "# Mart Design: sales_mart" in output

    def test_contains_description(self, sample_spec):
        output = _format_response(sample_spec)
        assert "Enables monthly sales analysis by customer." in output

    def test_contains_fact_table_section(self, sample_spec):
        output = _format_response(sample_spec)
        assert "## Fact Tables" in output
        assert "### fact_orders" in output

    def test_contains_grain(self, sample_spec):
        output = _format_response(sample_spec)
        assert "one row per order" in output

    def test_contains_metric_expression(self, sample_spec):
        output = _format_response(sample_spec)
        assert "SUM(total_amount)" in output

    def test_contains_dimension_table_section(self, sample_spec):
        output = _format_response(sample_spec)
        assert "## Dimension Tables" in output
        assert "### dim_customer" in output

    def test_contains_dimension_key_column(self, sample_spec):
        output = _format_response(sample_spec)
        assert "customer_id" in output

    def test_contains_rationale_section(self, sample_spec):
        output = _format_response(sample_spec)
        assert "## Design Rationale" in output
        assert "Simple star schema" in output

    def test_contains_ddl_section(self, sample_spec):
        output = _format_response(sample_spec)
        assert "## Generated DDL" in output
        assert "```sql" in output

    def test_ddl_block_contains_sql(self, sample_spec):
        output = _format_response(sample_spec)
        assert "CREATE TABLE dim_customer" in output
        assert "CREATE TABLE fact_orders" in output

    def test_rationale_section_omitted_when_empty(self, sample_spec):
        spec = sample_spec.model_copy(update={"rationale": ""})
        output = _format_response(spec)
        assert "## Design Rationale" not in output

    def test_metric_description_included_when_present(self, sample_spec):
        output = _format_response(sample_spec)
        assert "Total sales revenue." in output
