"""Unit tests for dbt_codegen.schema_yaml_generator.

All tests are fully deterministic — no mocking required.
YAML structural validity is verified by parsing the output with
``yaml.safe_load`` before asserting on individual entries.
"""

from __future__ import annotations

import yaml
import pytest

from dbt_codegen.schema_yaml_generator import generate_schema_yml, _quote
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
        grain="one row per order",
        description="Orders fact table",
    )


@pytest.fixture()
def spec(dim_customer, fact_orders) -> MartSpecification:
    intent = UserIntent(
        raw_input="Show me sales by customer.",
        subject_area="sales",
        required_metrics=["total_revenue"],
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
                columns=[SourceColumn(name="order_id", data_type="INTEGER")],
            )
        ],
        fact_tables=[fact_orders],
        dimension_tables=[dim_customer],
    )


# ---------------------------------------------------------------------------
# _quote helper
# ---------------------------------------------------------------------------


class TestQuote:
    def test_wraps_in_double_quotes(self):
        assert _quote("hello") == '"hello"'

    def test_empty_string_renders_as_empty_quotes(self):
        assert _quote("") == '""'

    def test_escapes_existing_double_quotes(self):
        result = _quote('say "hi"')
        assert '\\"' in result


# ---------------------------------------------------------------------------
# YAML structural validity
# ---------------------------------------------------------------------------


class TestSchemaYmlStructure:
    def test_output_is_parseable_yaml(self, spec):
        yml = generate_schema_yml(spec)
        parsed = yaml.safe_load(yml)
        assert parsed is not None

    def test_version_is_2(self, spec):
        parsed = yaml.safe_load(generate_schema_yml(spec))
        assert parsed["version"] == 2

    def test_models_key_is_a_list(self, spec):
        parsed = yaml.safe_load(generate_schema_yml(spec))
        assert isinstance(parsed["models"], list)

    def test_model_count_equals_dim_plus_fact(self, spec):
        parsed = yaml.safe_load(generate_schema_yml(spec))
        expected = len(spec.dimension_tables) + len(spec.fact_tables)
        assert len(parsed["models"]) == expected


# ---------------------------------------------------------------------------
# Dimension model entries
# ---------------------------------------------------------------------------


class TestDimensionModelEntry:
    def _get_dim_model(self, spec, name="dim_customer") -> dict:
        parsed = yaml.safe_load(generate_schema_yml(spec))
        return next(m for m in parsed["models"] if m["name"] == name)

    def test_dimension_model_name_present(self, spec):
        model = self._get_dim_model(spec)
        assert model["name"] == "dim_customer"

    def test_materialized_is_table(self, spec):
        model = self._get_dim_model(spec)
        assert model["config"]["materialized"] == "table"

    def test_key_column_has_not_null_and_unique(self, spec):
        model = self._get_dim_model(spec)
        col = next(c for c in model["columns"] if c["name"] == "customer_id")
        assert "not_null" in col["tests"]
        assert "unique" in col["tests"]

    def test_attribute_columns_present_without_tests(self, spec):
        model = self._get_dim_model(spec)
        col_names = [c["name"] for c in model["columns"]]
        assert "customer_name" in col_names
        assert "region" in col_names
        # Attribute columns have no tests in MVP
        for col in model["columns"]:
            if col["name"] in ("customer_name", "region"):
                assert col.get("tests") is None

    def test_description_field_present(self, spec):
        model = self._get_dim_model(spec)
        assert "description" in model


# ---------------------------------------------------------------------------
# Fact model entries
# ---------------------------------------------------------------------------


class TestFactModelEntry:
    def _get_fact_model(self, spec, name="fact_orders") -> dict:
        parsed = yaml.safe_load(generate_schema_yml(spec))
        return next(m for m in parsed["models"] if m["name"] == name)

    def test_fact_model_name_present(self, spec):
        model = self._get_fact_model(spec)
        assert model["name"] == "fact_orders"

    def test_materialized_is_table(self, spec):
        model = self._get_fact_model(spec)
        assert model["config"]["materialized"] == "table"

    def test_dimension_key_has_not_null(self, spec):
        model = self._get_fact_model(spec)
        col = next(c for c in model["columns"] if c["name"] == "customer_id")
        assert "not_null" in col["tests"]
        assert "unique" not in col["tests"]

    def test_metric_columns_have_not_null(self, spec):
        model = self._get_fact_model(spec)
        for metric_name in ("total_revenue", "order_count"):
            col = next(c for c in model["columns"] if c["name"] == metric_name)
            assert "not_null" in col["tests"]

    def test_metric_description_included(self, spec):
        model = self._get_fact_model(spec)
        col = next(c for c in model["columns"] if c["name"] == "total_revenue")
        assert col["description"] == "Total sales revenue"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestSchemaYmlEdgeCases:
    def test_empty_description_does_not_break_yaml(self):
        dim = DimensionDefinition(
            name="dim_empty",
            source_table="t",
            key_column="id",
            attribute_columns=[],
            description="",
        )
        intent = UserIntent(
            raw_input="x",
            subject_area="s",
            required_metrics=["m"],
            required_dimensions=["d"],
        )
        spec = MartSpecification(
            mart_name="m",
            description="",
            intent=intent,
            source_tables=[],
            fact_tables=[],
            dimension_tables=[dim],
        )
        yml = generate_schema_yml(spec)
        parsed = yaml.safe_load(yml)
        assert parsed is not None

    def test_multiple_dimensions_all_appear(self, dim_customer, fact_orders):
        dim2 = DimensionDefinition(
            name="dim_product",
            source_table="products",
            key_column="product_id",
            attribute_columns=["product_name"],
        )
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
            dimension_tables=[dim_customer, dim2],
        )
        parsed = yaml.safe_load(generate_schema_yml(spec))
        names = [m["name"] for m in parsed["models"]]
        assert "dim_customer" in names
        assert "dim_product" in names
