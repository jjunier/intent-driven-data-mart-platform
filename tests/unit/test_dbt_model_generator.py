"""Unit tests for dbt_codegen.model_generator.

All tests are fully deterministic — no mocking required since
model_generator contains no LLM or I/O calls.
"""

from __future__ import annotations

import pytest

from dbt_codegen.model_generator import (
    _indent_columns,
    generate_all_dimension_models,
    generate_all_fact_models,
    generate_dimension_model,
    generate_fact_model,
)
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
def dim_customer() -> DimensionDefinition:
    return DimensionDefinition(
        name="dim_customer",
        source_table="customers",
        key_column="customer_id",
        attribute_columns=["customer_name", "region"],
        description="Customer dimension",
    )


@pytest.fixture()
def dim_product() -> DimensionDefinition:
    return DimensionDefinition(
        name="dim_product",
        source_table="products",
        key_column="product_id",
        attribute_columns=["product_name"],
        description="Product dimension",
    )


@pytest.fixture()
def fact_orders() -> FactDefinition:
    return FactDefinition(
        name="fact_orders",
        source_tables=["orders"],
        metrics=[
            MetricDefinition(
                name="total_revenue",
                expression="SUM(amount)",
                aggregation=AggregationType.sum,
                source_column="amount",
                description="Total sales revenue",
            ),
            MetricDefinition(
                name="order_count",
                expression="COUNT(order_id)",
                aggregation=AggregationType.count,
                source_column="order_id",
                description="",
            ),
        ],
        dimension_keys=["customer_id"],
        grain="one row per order per customer",
        description="Orders fact table",
    )


@pytest.fixture()
def spec(dim_customer, fact_orders) -> MartSpecification:
    intent = UserIntent(
        raw_input="Show me sales by customer.",
        subject_area="sales",
        required_metrics=["total_revenue", "order_count"],
        required_dimensions=["customer"],
    )
    return MartSpecification(
        mart_name="sales_mart",
        description="Sales analysis mart",
        intent=intent,
        source_tables=[
            SourceTable(
                name="orders",
                schema_name="main",
                columns=[
                    SourceColumn(name="order_id", data_type="INTEGER", is_primary_key=True),
                    SourceColumn(name="customer_id", data_type="INTEGER"),
                    SourceColumn(name="amount", data_type="DOUBLE"),
                ],
            ),
            SourceTable(
                name="customers",
                schema_name="main",
                columns=[
                    SourceColumn(name="customer_id", data_type="INTEGER", is_primary_key=True),
                    SourceColumn(name="customer_name", data_type="VARCHAR"),
                    SourceColumn(name="region", data_type="VARCHAR"),
                ],
            ),
        ],
        fact_tables=[fact_orders],
        dimension_tables=[dim_customer],
    )


# ---------------------------------------------------------------------------
# _indent_columns
# ---------------------------------------------------------------------------


class TestIndentColumns:
    def test_single_column_no_comma(self):
        result = _indent_columns(["customer_id"])
        assert result == "        customer_id"

    def test_multiple_columns_last_has_no_comma(self):
        result = _indent_columns(["a", "b", "c"])
        lines = result.split("\n")
        assert lines[-1].strip() == "c"
        assert not lines[-1].endswith(",")

    def test_intermediate_columns_have_comma(self):
        result = _indent_columns(["a", "b", "c"])
        lines = result.split("\n")
        assert lines[0].endswith(",")
        assert lines[1].endswith(",")

    def test_custom_indent(self):
        result = _indent_columns(["col"], indent=4)
        assert result.startswith("    col")


# ---------------------------------------------------------------------------
# generate_dimension_model
# ---------------------------------------------------------------------------


class TestGenerateDimensionModel:
    def test_contains_source_cte(self, dim_customer):
        sql = generate_dimension_model(dim_customer)
        assert "with source as" in sql

    def test_contains_renamed_cte(self, dim_customer):
        sql = generate_dimension_model(dim_customer)
        assert "renamed as" in sql

    def test_uses_source_macro_with_raw_schema(self, dim_customer):
        sql = generate_dimension_model(dim_customer)
        assert "{{ source('raw', 'customers') }}" in sql

    def test_key_column_appears_first_in_select(self, dim_customer):
        sql = generate_dimension_model(dim_customer)
        renamed_block = sql.split("renamed as")[1]
        key_pos = renamed_block.find("customer_id")
        attr_pos = renamed_block.find("customer_name")
        assert key_pos < attr_pos

    def test_all_attribute_columns_present(self, dim_customer):
        sql = generate_dimension_model(dim_customer)
        assert "customer_name" in sql
        assert "region" in sql

    def test_ends_with_select_from_renamed(self, dim_customer):
        sql = generate_dimension_model(dim_customer)
        assert "select * from renamed" in sql

    def test_no_group_by(self, dim_customer):
        sql = generate_dimension_model(dim_customer)
        assert "group by" not in sql.lower()


# ---------------------------------------------------------------------------
# generate_fact_model
# ---------------------------------------------------------------------------


class TestGenerateFactModel:
    def test_contains_source_cte(self, fact_orders):
        sql = generate_fact_model(fact_orders)
        assert "with source as" in sql

    def test_contains_aggregated_cte(self, fact_orders):
        sql = generate_fact_model(fact_orders)
        assert "aggregated as" in sql

    def test_uses_first_source_table(self, fact_orders):
        sql = generate_fact_model(fact_orders)
        assert "{{ source('raw', 'orders') }}" in sql

    def test_metric_expressions_present(self, fact_orders):
        sql = generate_fact_model(fact_orders)
        assert "SUM(amount)" in sql
        assert "COUNT(order_id)" in sql

    def test_metric_aliases_present(self, fact_orders):
        sql = generate_fact_model(fact_orders)
        assert "total_revenue" in sql
        assert "order_count" in sql

    def test_dimension_keys_in_select(self, fact_orders):
        sql = generate_fact_model(fact_orders)
        assert "customer_id" in sql

    def test_group_by_contains_dimension_keys(self, fact_orders):
        sql = generate_fact_model(fact_orders)
        assert "group by" in sql.lower()
        group_by_block = sql.lower().split("group by")[1]
        assert "customer_id" in group_by_block

    def test_ends_with_select_from_aggregated(self, fact_orders):
        sql = generate_fact_model(fact_orders)
        assert "select * from aggregated" in sql

    def test_single_source_table_used(self):
        """MVP: only source_tables[0] is referenced even when multiple are listed."""
        fact = FactDefinition(
            name="fact_multi",
            source_tables=["primary_table", "secondary_table"],
            metrics=[
                MetricDefinition(
                    name="cnt",
                    expression="COUNT(*)",
                    aggregation=AggregationType.count,
                    source_column="id",
                )
            ],
            dimension_keys=["dim_id"],
            grain="one row per event",
        )
        sql = generate_fact_model(fact)
        assert "primary_table" in sql
        assert "secondary_table" not in sql


# ---------------------------------------------------------------------------
# generate_all_dimension_models / generate_all_fact_models
# ---------------------------------------------------------------------------


class TestGenerateAllModels:
    def test_dimension_model_keys_are_filenames(self, spec):
        models = generate_all_dimension_models(spec)
        assert "dim_customer.sql" in models

    def test_fact_model_keys_are_filenames(self, spec):
        models = generate_all_fact_models(spec)
        assert "fact_orders.sql" in models

    def test_multiple_dimensions_produce_multiple_files(self, dim_customer, dim_product, fact_orders):
        intent = UserIntent(
            raw_input="x",
            subject_area="sales",
            required_metrics=["total_revenue"],
            required_dimensions=["customer", "product"],
        )
        spec = MartSpecification(
            mart_name="m",
            description="d",
            intent=intent,
            source_tables=[],
            fact_tables=[fact_orders],
            dimension_tables=[dim_customer, dim_product],
        )
        models = generate_all_dimension_models(spec)
        assert "dim_customer.sql" in models
        assert "dim_product.sql" in models
        assert len(models) == 2

    def test_dimension_model_content_is_non_empty(self, spec):
        models = generate_all_dimension_models(spec)
        assert models["dim_customer.sql"].strip() != ""

    def test_fact_model_content_is_non_empty(self, spec):
        models = generate_all_fact_models(spec)
        assert models["fact_orders.sql"].strip() != ""
