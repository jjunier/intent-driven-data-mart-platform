"""Unit tests for dbt_codegen.sources_yaml_generator.

All tests are fully deterministic — no mocking required.
YAML structural validity is verified by parsing the output with
``yaml.safe_load`` before asserting on individual entries.

Consistency check: source table names in sources.yml must match
the table names referenced by {{ source('raw', ...) }} in model SQL.
"""

from __future__ import annotations

import yaml
import pytest

from dbt_codegen._constants import RAW_SCHEMA
from dbt_codegen.model_generator import generate_fact_model, generate_dimension_model
from dbt_codegen.sources_yaml_generator import generate_sources_yml, _quote
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
def source_orders() -> SourceTable:
    return SourceTable(
        name="orders",
        schema_name="main",
        columns=[SourceColumn(name="order_id", data_type="INTEGER")],
        description="Raw orders table",
    )


@pytest.fixture()
def source_customers() -> SourceTable:
    return SourceTable(
        name="customers",
        schema_name="main",
        columns=[SourceColumn(name="customer_id", data_type="INTEGER")],
        description="",
    )


@pytest.fixture()
def spec(source_orders, source_customers) -> MartSpecification:
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
        source_tables=[source_orders, source_customers],
        fact_tables=[
            FactDefinition(
                name="fact_orders",
                source_tables=["orders"],
                metrics=[
                    MetricDefinition(
                        name="total_revenue",
                        expression="SUM(amount)",
                        aggregation=AggregationType.sum,
                        source_column="amount",
                    )
                ],
                dimension_keys=["customer_id"],
                grain="one row per order",
            )
        ],
        dimension_tables=[
            DimensionDefinition(
                name="dim_customer",
                source_table="customers",
                key_column="customer_id",
                attribute_columns=["customer_name"],
            )
        ],
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


class TestSourcesYmlStructure:
    def test_output_is_parseable_yaml(self, spec):
        yml = generate_sources_yml(spec)
        parsed = yaml.safe_load(yml)
        assert parsed is not None

    def test_version_is_2(self, spec):
        parsed = yaml.safe_load(generate_sources_yml(spec))
        assert parsed["version"] == 2

    def test_sources_key_is_a_list(self, spec):
        parsed = yaml.safe_load(generate_sources_yml(spec))
        assert isinstance(parsed["sources"], list)

    def test_exactly_one_source_group(self, spec):
        parsed = yaml.safe_load(generate_sources_yml(spec))
        assert len(parsed["sources"]) == 1

    def test_source_group_name_matches_raw_schema_constant(self, spec):
        parsed = yaml.safe_load(generate_sources_yml(spec))
        assert parsed["sources"][0]["name"] == RAW_SCHEMA

    def test_tables_key_is_a_list(self, spec):
        parsed = yaml.safe_load(generate_sources_yml(spec))
        assert isinstance(parsed["sources"][0]["tables"], list)


# ---------------------------------------------------------------------------
# Table entries
# ---------------------------------------------------------------------------


class TestTableEntries:
    def test_all_source_tables_appear(self, spec):
        parsed = yaml.safe_load(generate_sources_yml(spec))
        table_names = [t["name"] for t in parsed["sources"][0]["tables"]]
        assert "orders" in table_names
        assert "customers" in table_names

    def test_table_count_matches_source_tables(self, spec):
        parsed = yaml.safe_load(generate_sources_yml(spec))
        assert len(parsed["sources"][0]["tables"]) == len(spec.source_tables)

    def test_table_names_match_source_table_names(self, spec):
        parsed = yaml.safe_load(generate_sources_yml(spec))
        expected = {t.name for t in spec.source_tables}
        actual = {t["name"] for t in parsed["sources"][0]["tables"]}
        assert actual == expected

    def test_description_included_for_non_empty(self, spec):
        parsed = yaml.safe_load(generate_sources_yml(spec))
        orders_entry = next(
            t for t in parsed["sources"][0]["tables"] if t["name"] == "orders"
        )
        assert orders_entry["description"] == "Raw orders table"

    def test_empty_description_included_as_empty_string(self, spec):
        parsed = yaml.safe_load(generate_sources_yml(spec))
        customers_entry = next(
            t for t in parsed["sources"][0]["tables"] if t["name"] == "customers"
        )
        assert customers_entry["description"] == ""

    def test_empty_source_tables_produces_empty_tables_list(self):
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
            dimension_tables=[],
        )
        parsed = yaml.safe_load(generate_sources_yml(spec))
        # tables key may be None (empty YAML list) or empty list
        tables = parsed["sources"][0].get("tables") or []
        assert tables == []


# ---------------------------------------------------------------------------
# Consistency with model_generator source() references
# ---------------------------------------------------------------------------


class TestSourceReferenceConsistency:
    """Verify that table names in sources.yml match {{ source('raw', ...) }}
    references produced by model_generator for the same spec."""

    def test_fact_source_table_declared_in_sources_yml(self, spec):
        """source_tables[0] used by fact model must appear in sources.yml."""
        parsed = yaml.safe_load(generate_sources_yml(spec))
        declared_names = {t["name"] for t in parsed["sources"][0]["tables"]}
        fact = spec.fact_tables[0]
        assert fact.source_tables[0] in declared_names

    def test_dimension_source_table_declared_in_sources_yml(self, spec):
        """source_table used by dimension model must appear in sources.yml."""
        parsed = yaml.safe_load(generate_sources_yml(spec))
        declared_names = {t["name"] for t in parsed["sources"][0]["tables"]}
        dim = spec.dimension_tables[0]
        assert dim.source_table in declared_names

    def test_source_group_name_matches_model_generator_constant(self, spec):
        """The source group name in sources.yml must equal RAW_SCHEMA used
        by model_generator when building {{ source(...) }} references."""
        fact_sql = generate_fact_model(spec.fact_tables[0])
        dim_sql = generate_dimension_model(spec.dimension_tables[0])
        parsed = yaml.safe_load(generate_sources_yml(spec))
        source_name = parsed["sources"][0]["name"]
        # Both model SQL strings must reference the same source name
        assert f"source('{source_name}'," in fact_sql
        assert f"source('{source_name}'," in dim_sql
