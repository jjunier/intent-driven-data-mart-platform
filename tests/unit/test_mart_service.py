"""Unit tests for application.mart_service.

All external dependencies (LLM clients, schema reader, validators) are mocked
so that tests run without network access or a real database file.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from application.mart_service import propose_mart_from_request
from intent.schema import UserIntent
from mart_design.schema import (
    AggregationType,
    DimensionDefinition,
    FactDefinition,
    MartSpecification,
    MetricDefinition,
)
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
                SourceColumn(
                    name="order_id",
                    data_type="INTEGER",
                    is_primary_key=True,
                    is_nullable=False,
                ),
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
        generated_sql="",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EXPECTED_DDL = "CREATE TABLE dim_customer (customer_id INTEGER PRIMARY KEY);"


def _make_schema_reader_mock(tables: list[SourceTable]) -> MagicMock:
    """Return a mock SchemaReader whose read_tables() returns *tables*."""
    reader = MagicMock()
    reader.read_tables.return_value = tables
    return reader


def _run_service(sample_intent, sample_tables, sample_spec):
    """Execute propose_mart_from_request with all dependencies mocked."""
    mock_reader = _make_schema_reader_mock(sample_tables)

    with (
        patch(
            "application.mart_service.parse_intent",
            return_value=sample_intent,
        ) as mock_parse,
        patch("application.mart_service.validate_intent") as mock_validate,
        patch("application.mart_service.validate_mart_spec") as mock_validate_spec,
        patch(
            "application.mart_service.propose_mart",
            return_value=sample_spec,
        ) as mock_propose,
        patch(
            "application.mart_service.generate_sql",
            return_value=_EXPECTED_DDL,
        ) as mock_gen_sql,
    ):
        result = propose_mart_from_request(
            "Show me monthly sales by customer.", mock_reader
        )

    return (
        result,
        mock_parse,
        mock_validate,
        mock_validate_spec,
        mock_reader,
        mock_propose,
        mock_gen_sql,
    )


# ---------------------------------------------------------------------------
# propose_mart_from_request
# ---------------------------------------------------------------------------


class TestProposeMartFromRequest:
    def test_returns_mart_specification(self, sample_intent, sample_tables, sample_spec):
        result, *_ = _run_service(sample_intent, sample_tables, sample_spec)
        assert isinstance(result, MartSpecification)

    def test_generated_sql_attached_to_spec(self, sample_intent, sample_tables, sample_spec):
        result, *_ = _run_service(sample_intent, sample_tables, sample_spec)
        assert result.generated_sql == _EXPECTED_DDL

    def test_parse_intent_called_with_user_request(self, sample_intent, sample_tables, sample_spec):
        _, mock_parse, *_ = _run_service(sample_intent, sample_tables, sample_spec)
        mock_parse.assert_called_once_with("Show me monthly sales by customer.", client=None)

    def test_validate_intent_called_with_parsed_intent(
        self, sample_intent, sample_tables, sample_spec
    ):
        _, _, mock_validate, *_ = _run_service(sample_intent, sample_tables, sample_spec)
        mock_validate.assert_called_once_with(sample_intent)

    def test_validate_mart_spec_called_with_proposed_spec(
        self, sample_intent, sample_tables, sample_spec
    ):
        _, _, _, mock_validate_spec, *_ = _run_service(sample_intent, sample_tables, sample_spec)
        mock_validate_spec.assert_called_once()

    def test_schema_reader_read_tables_called(self, sample_intent, sample_tables, sample_spec):
        _, _, _, _, mock_reader, *_ = _run_service(sample_intent, sample_tables, sample_spec)
        mock_reader.read_tables.assert_called_once()

    def test_propose_mart_called_with_intent_and_tables(
        self, sample_intent, sample_tables, sample_spec
    ):
        _, _, _, _, _, mock_propose, _ = _run_service(sample_intent, sample_tables, sample_spec)
        mock_propose.assert_called_once_with(sample_intent, sample_tables, client=None)

    def test_generate_sql_called_with_spec(self, sample_intent, sample_tables, sample_spec):
        _, _, _, _, _, mock_propose, mock_gen_sql = _run_service(
            sample_intent, sample_tables, sample_spec
        )
        called_spec = mock_gen_sql.call_args[0][0]
        assert called_spec is mock_propose.return_value

    def test_mart_name_preserved(self, sample_intent, sample_tables, sample_spec):
        result, *_ = _run_service(sample_intent, sample_tables, sample_spec)
        assert result.mart_name == "sales_mart"

    def test_intent_preserved_in_result(self, sample_intent, sample_tables, sample_spec):
        result, *_ = _run_service(sample_intent, sample_tables, sample_spec)
        assert result.intent == sample_intent

    def test_source_tables_preserved_in_result(self, sample_intent, sample_tables, sample_spec):
        result, *_ = _run_service(sample_intent, sample_tables, sample_spec)
        assert result.source_tables == sample_tables
