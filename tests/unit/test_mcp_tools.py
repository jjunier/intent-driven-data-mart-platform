"""Unit tests for mcp.tools.

run_propose_mart is tested by mocking the application service layer as a
single unit — internal pipeline steps are covered in test_mart_service.py.
_format_response is tested directly against a fixture MartSpecification.
"""

from __future__ import annotations

from unittest.mock import patch

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
# run_propose_mart — delegates to application service, formats as Markdown
# ---------------------------------------------------------------------------


class TestRunProposeMart:
    def test_returns_string(self, sample_spec):
        with patch("mcp.tools.propose_mart_from_request", return_value=sample_spec):
            result = run_propose_mart("Show me sales.", "/data/warehouse.db")
        assert isinstance(result, str)

    def test_service_called_with_user_request_and_path(self, sample_spec):
        with patch("mcp.tools.propose_mart_from_request", return_value=sample_spec) as mock_svc:
            run_propose_mart("Show me sales.", "/data/warehouse.db")
        mock_svc.assert_called_once_with("Show me sales.", "/data/warehouse.db")

    def test_result_contains_mart_name(self, sample_spec):
        with patch("mcp.tools.propose_mart_from_request", return_value=sample_spec):
            result = run_propose_mart("Show me sales.", "/data/warehouse.db")
        assert "sales_mart" in result

    def test_result_contains_sql(self, sample_spec):
        with patch("mcp.tools.propose_mart_from_request", return_value=sample_spec):
            result = run_propose_mart("Show me sales.", "/data/warehouse.db")
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
