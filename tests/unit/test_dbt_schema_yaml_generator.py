"""Unit tests for dbt_codegen.schema_yaml_generator.

All tests are fully deterministic — no mocking required.
YAML structural validity is verified by parsing the output with
``yaml.safe_load`` before asserting on individual entries.
"""

from __future__ import annotations

import yaml
import pytest

from dbt_codegen.schema_yaml_generator import (
    generate_schema_yml,
    _quote,
    _should_add_accepted_values,
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


# ---------------------------------------------------------------------------
# relationships test
# ---------------------------------------------------------------------------


class TestRelationshipsTest:
    def _get_fact_col(self, spec, col_name: str) -> dict:
        parsed = yaml.safe_load(generate_schema_yml(spec))
        fact_model = next(m for m in parsed["models"] if m["name"] == "fact_orders")
        return next(c for c in fact_model["columns"] if c["name"] == col_name)

    def test_dimension_key_has_relationships_test(self, spec):
        col = self._get_fact_col(spec, "customer_id")
        test_names = [
            t if isinstance(t, str) else list(t.keys())[0]
            for t in col["tests"]
        ]
        assert "relationships" in test_names

    def test_relationships_to_points_to_dim_model(self, spec):
        col = self._get_fact_col(spec, "customer_id")
        rel_test = next(
            t for t in col["tests"]
            if isinstance(t, dict) and "relationships" in t
        )
        assert "dim_customer" in rel_test["relationships"]["to"]

    def test_relationships_field_matches_dim_key_column(self, spec):
        col = self._get_fact_col(spec, "customer_id")
        rel_test = next(
            t for t in col["tests"]
            if isinstance(t, dict) and "relationships" in t
        )
        assert rel_test["relationships"]["field"] == "customer_id"

    def test_not_null_still_present_alongside_relationships(self, spec):
        col = self._get_fact_col(spec, "customer_id")
        assert "not_null" in col["tests"]

    def test_no_relationships_when_dim_key_not_matched(self):
        """dimension_key with no corresponding DimensionDefinition → no relationships test."""
        intent = UserIntent(
            raw_input="x",
            subject_area="sales",
            required_metrics=["cnt"],
            required_dimensions=["unknown"],
        )
        fact = FactDefinition(
            name="fact_orders",
            source_tables=["orders"],
            metrics=[
                MetricDefinition(
                    name="cnt",
                    expression="COUNT(*)",
                    aggregation=AggregationType.count,
                    source_column="id",
                )
            ],
            dimension_keys=["orphan_key"],  # no matching dim
            grain="one row per event",
        )
        spec = MartSpecification(
            mart_name="m",
            description="",
            intent=intent,
            source_tables=[],
            fact_tables=[fact],
            dimension_tables=[],  # empty — no dim to match
        )
        parsed = yaml.safe_load(generate_schema_yml(spec))
        fact_model = next(m for m in parsed["models"] if m["name"] == "fact_orders")
        col = next(c for c in fact_model["columns"] if c["name"] == "orphan_key")
        test_names = [
            t if isinstance(t, str) else list(t.keys())[0]
            for t in col["tests"]
        ]
        assert "relationships" not in test_names
        assert "not_null" in test_names

    def test_output_with_relationships_is_parseable_yaml(self, spec):
        yml = generate_schema_yml(spec)
        parsed = yaml.safe_load(yml)
        assert parsed is not None


# ---------------------------------------------------------------------------
# _should_add_accepted_values
# ---------------------------------------------------------------------------


class TestShouldAddAcceptedValues:
    def _make_col(self, data_type: str, sample_values: list[str]) -> SourceColumn:
        return SourceColumn(
            name="status",
            data_type=data_type,
            sample_values=sample_values,
        )

    def test_returns_true_for_varchar_with_few_samples(self):
        col = self._make_col("VARCHAR", ["active", "inactive"])
        assert _should_add_accepted_values(col) is True

    def test_returns_true_for_text_type(self):
        col = self._make_col("TEXT", ["a", "b", "c"])
        assert _should_add_accepted_values(col) is True

    def test_returns_false_for_empty_sample_values(self):
        col = self._make_col("VARCHAR", [])
        assert _should_add_accepted_values(col) is False

    def test_returns_false_when_more_than_ten_samples(self):
        col = self._make_col("VARCHAR", [str(i) for i in range(11)])
        assert _should_add_accepted_values(col) is False

    def test_returns_true_for_exactly_ten_samples(self):
        col = self._make_col("VARCHAR", [str(i) for i in range(10)])
        assert _should_add_accepted_values(col) is True

    def test_returns_false_for_integer_type(self):
        col = self._make_col("INTEGER", ["1", "2"])
        assert _should_add_accepted_values(col) is False

    def test_returns_false_for_double_type(self):
        col = self._make_col("DOUBLE", ["1.0", "2.0"])
        assert _should_add_accepted_values(col) is False

    def test_returns_false_for_bigint_type(self):
        col = self._make_col("BIGINT", ["100"])
        assert _should_add_accepted_values(col) is False

    def test_case_insensitive_data_type_match(self):
        col = self._make_col("varchar", ["a", "b"])
        assert _should_add_accepted_values(col) is True


# ---------------------------------------------------------------------------
# accepted_values test in schema.yml output
# ---------------------------------------------------------------------------


class TestAcceptedValuesInSchemaYml:
    def _make_spec_with_sample_values(
        self, data_type: str, sample_values: list[str]
    ) -> MartSpecification:
        intent = UserIntent(
            raw_input="x",
            subject_area="sales",
            required_metrics=["total_revenue"],
            required_dimensions=["customer"],
        )
        return MartSpecification(
            mart_name="m",
            description="",
            intent=intent,
            source_tables=[
                SourceTable(
                    name="customers",
                    schema_name="main",
                    columns=[
                        SourceColumn(
                            name="customer_id",
                            data_type="INTEGER",
                            is_primary_key=True,
                        ),
                        SourceColumn(
                            name="status",
                            data_type=data_type,
                            sample_values=sample_values,
                        ),
                    ],
                )
            ],
            fact_tables=[],
            dimension_tables=[
                DimensionDefinition(
                    name="dim_customer",
                    source_table="customers",
                    key_column="customer_id",
                    attribute_columns=["status"],
                )
            ],
        )

    def _get_status_col(self, spec: MartSpecification) -> dict:
        parsed = yaml.safe_load(generate_schema_yml(spec))
        dim_model = next(m for m in parsed["models"] if m["name"] == "dim_customer")
        return next(c for c in dim_model["columns"] if c["name"] == "status")

    def test_accepted_values_added_when_conditions_met(self):
        spec = self._make_spec_with_sample_values("VARCHAR", ["active", "inactive"])
        col = self._get_status_col(spec)
        assert col.get("tests") is not None
        test_names = [
            t if isinstance(t, str) else list(t.keys())[0]
            for t in col["tests"]
        ]
        assert "accepted_values" in test_names

    def test_accepted_values_contains_sample_values(self):
        spec = self._make_spec_with_sample_values("VARCHAR", ["active", "inactive"])
        col = self._get_status_col(spec)
        av_test = next(
            t for t in col["tests"]
            if isinstance(t, dict) and "accepted_values" in t
        )
        assert set(av_test["accepted_values"]["values"]) == {"active", "inactive"}

    def test_no_accepted_values_when_sample_values_empty(self):
        spec = self._make_spec_with_sample_values("VARCHAR", [])
        col = self._get_status_col(spec)
        assert col.get("tests") is None

    def test_no_accepted_values_for_integer_column(self):
        spec = self._make_spec_with_sample_values("INTEGER", ["1", "2"])
        col = self._get_status_col(spec)
        assert col.get("tests") is None

    def test_no_accepted_values_when_too_many_samples(self):
        spec = self._make_spec_with_sample_values(
            "VARCHAR", [str(i) for i in range(11)]
        )
        col = self._get_status_col(spec)
        assert col.get("tests") is None

    def test_accepted_values_output_is_parseable_yaml(self):
        spec = self._make_spec_with_sample_values("VARCHAR", ["a", "b", "c"])
        yml = generate_schema_yml(spec)
        parsed = yaml.safe_load(yml)
        assert parsed is not None

    def test_key_column_never_gets_accepted_values(self):
        """PK columns must not have accepted_values even if source has sample_values."""
        intent = UserIntent(
            raw_input="x",
            subject_area="sales",
            required_metrics=["m"],
            required_dimensions=["d"],
        )
        spec = MartSpecification(
            mart_name="m",
            description="",
            intent=intent,
            source_tables=[
                SourceTable(
                    name="customers",
                    schema_name="main",
                    columns=[
                        SourceColumn(
                            name="customer_id",
                            data_type="VARCHAR",
                            is_primary_key=True,
                            sample_values=["c1", "c2", "c3"],
                        ),
                    ],
                )
            ],
            fact_tables=[],
            dimension_tables=[
                DimensionDefinition(
                    name="dim_customer",
                    source_table="customers",
                    key_column="customer_id",
                    attribute_columns=[],
                )
            ],
        )
        parsed = yaml.safe_load(generate_schema_yml(spec))
        dim_model = next(m for m in parsed["models"] if m["name"] == "dim_customer")
        pk_col = next(c for c in dim_model["columns"] if c["name"] == "customer_id")
        test_names = [
            t if isinstance(t, str) else list(t.keys())[0]
            for t in pk_col["tests"]
        ]
        assert "accepted_values" not in test_names
