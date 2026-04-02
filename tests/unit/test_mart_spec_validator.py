"""Unit tests for mart_design.validator."""

from __future__ import annotations

import pytest

from intent.schema import UserIntent
from mart_design.schema import (
    AggregationType,
    DimensionDefinition,
    FactDefinition,
    MartSpecification,
    MetricDefinition,
)
from mart_design.validator import MartSpecValidationError, validate_mart_spec
from metadata.schema import SourceColumn, SourceTable


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def source_tables() -> list[SourceTable]:
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
        SourceTable(
            name="customers",
            schema_name="main",
            columns=[
                SourceColumn(name="customer_id", data_type="INTEGER", is_primary_key=True, is_nullable=False),
                SourceColumn(name="name", data_type="VARCHAR", is_nullable=False),
                SourceColumn(name="region", data_type="VARCHAR"),
            ],
        ),
    ]


@pytest.fixture()
def sample_intent() -> UserIntent:
    return UserIntent(
        raw_input="Show me monthly sales by customer.",
        subject_area="sales",
        required_metrics=["total_revenue"],
        required_dimensions=["customer"],
        time_granularity="monthly",
    )


def _make_spec(
    source_tables,
    sample_intent,
    *,
    dim_source_table: str = "customers",
    dim_key_column: str = "customer_id",
    dim_attribute_columns: list[str] | None = None,
    metric_source_column: str = "total_amount",
    fact_source_tables: list[str] | None = None,
) -> MartSpecification:
    if dim_attribute_columns is None:
        dim_attribute_columns = ["name", "region"]
    if fact_source_tables is None:
        fact_source_tables = ["orders"]

    return MartSpecification(
        mart_name="sales_mart",
        description="Test mart.",
        intent=sample_intent,
        source_tables=source_tables,
        fact_tables=[
            FactDefinition(
                name="fact_orders",
                source_tables=fact_source_tables,
                metrics=[
                    MetricDefinition(
                        name="total_revenue",
                        expression="SUM(total_amount)",
                        aggregation=AggregationType.sum,
                        source_column=metric_source_column,
                    )
                ],
                dimension_keys=["customer_id"],
                grain="one row per order",
            )
        ],
        dimension_tables=[
            DimensionDefinition(
                name="dim_customer",
                source_table=dim_source_table,
                key_column=dim_key_column,
                attribute_columns=dim_attribute_columns,
            )
        ],
    )


# ---------------------------------------------------------------------------
# Valid spec — should not raise
# ---------------------------------------------------------------------------


class TestValidateMartSpecAcceptsValid:
    def test_fully_valid_spec_passes(self, source_tables, sample_intent):
        spec = _make_spec(source_tables, sample_intent)
        validate_mart_spec(spec)  # must not raise

    def test_empty_attribute_columns_passes(self, source_tables, sample_intent):
        spec = _make_spec(source_tables, sample_intent, dim_attribute_columns=[])
        validate_mart_spec(spec)


# ---------------------------------------------------------------------------
# Dimension validation
# ---------------------------------------------------------------------------


class TestValidateMartSpecDimensions:
    def test_unknown_source_table_raises(self, source_tables, sample_intent):
        spec = _make_spec(source_tables, sample_intent, dim_source_table="missing_table")
        with pytest.raises(MartSpecValidationError, match="missing_table"):
            validate_mart_spec(spec)

    def test_unknown_key_column_raises(self, source_tables, sample_intent):
        spec = _make_spec(source_tables, sample_intent, dim_key_column="nonexistent_id")
        with pytest.raises(MartSpecValidationError, match="nonexistent_id"):
            validate_mart_spec(spec)

    def test_unknown_attribute_column_raises(self, source_tables, sample_intent):
        spec = _make_spec(
            source_tables, sample_intent, dim_attribute_columns=["name", "ghost_column"]
        )
        with pytest.raises(MartSpecValidationError, match="ghost_column"):
            validate_mart_spec(spec)

    def test_error_is_value_error_subclass(self, source_tables, sample_intent):
        spec = _make_spec(source_tables, sample_intent, dim_key_column="bad_col")
        with pytest.raises(ValueError):
            validate_mart_spec(spec)

    def test_error_message_names_dimension(self, source_tables, sample_intent):
        spec = _make_spec(source_tables, sample_intent, dim_key_column="bad_col")
        with pytest.raises(MartSpecValidationError, match="dim_customer"):
            validate_mart_spec(spec)


# ---------------------------------------------------------------------------
# Fact metric validation
# ---------------------------------------------------------------------------


class TestValidateMartSpecFactMetrics:
    def test_unknown_metric_source_column_raises(self, source_tables, sample_intent):
        spec = _make_spec(source_tables, sample_intent, metric_source_column="ghost_amount")
        with pytest.raises(MartSpecValidationError, match="ghost_amount"):
            validate_mart_spec(spec)

    def test_error_message_names_fact_and_metric(self, source_tables, sample_intent):
        spec = _make_spec(source_tables, sample_intent, metric_source_column="ghost_amount")
        with pytest.raises(MartSpecValidationError, match="fact_orders"):
            validate_mart_spec(spec)

    def test_column_from_any_fact_source_table_is_accepted(self, source_tables, sample_intent):
        # total_amount is in orders; customer_id is in both orders and customers.
        # Using customer_id as metric source column should pass because orders has it.
        spec = _make_spec(
            source_tables,
            sample_intent,
            metric_source_column="customer_id",
            fact_source_tables=["orders"],
        )
        validate_mart_spec(spec)
