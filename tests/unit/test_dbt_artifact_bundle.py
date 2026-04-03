"""Unit tests for dbt_codegen.schema (DbtArtifactBundle) and
the generate_dbt_artifacts service function.

Tests verify that the bundle correctly aggregates outputs from
model_generator and schema_yaml_generator, and that the service
function wires them together without touching propose_mart_from_request.
"""

from __future__ import annotations

import pytest

from application.mart_service import generate_dbt_artifacts
from dbt_codegen.schema import DbtArtifactBundle
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
def spec() -> MartSpecification:
    intent = UserIntent(
        raw_input="Show me revenue by customer.",
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
                ],
            ),
        ],
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
        generated_sql="CREATE TABLE dim_customer (...);",
    )


# ---------------------------------------------------------------------------
# DbtArtifactBundle dataclass
# ---------------------------------------------------------------------------


class TestDbtArtifactBundle:
    def test_default_instance_is_empty(self):
        bundle = DbtArtifactBundle()
        assert bundle.fact_models == {}
        assert bundle.dimension_models == {}
        assert bundle.schema_yml == ""
        assert bundle.sources_yml == ""

    def test_all_files_includes_fact_under_facts_path(self):
        bundle = DbtArtifactBundle(fact_models={"fact_orders.sql": "select 1"})
        files = bundle.all_files()
        assert "models/marts/facts/fact_orders.sql" in files

    def test_all_files_includes_dimension_under_dimensions_path(self):
        bundle = DbtArtifactBundle(dimension_models={"dim_customer.sql": "select 1"})
        files = bundle.all_files()
        assert "models/marts/dimensions/dim_customer.sql" in files

    def test_all_files_includes_schema_yml(self):
        bundle = DbtArtifactBundle(schema_yml="version: 2\n")
        files = bundle.all_files()
        assert "models/marts/schema.yml" in files

    def test_all_files_omits_schema_yml_when_empty(self):
        bundle = DbtArtifactBundle(schema_yml="")
        files = bundle.all_files()
        assert "models/marts/schema.yml" not in files

    def test_all_files_content_matches_input(self):
        bundle = DbtArtifactBundle(fact_models={"fact_orders.sql": "select * from x"})
        files = bundle.all_files()
        assert files["models/marts/facts/fact_orders.sql"] == "select * from x"

    def test_all_files_includes_sources_yml_at_models_root(self):
        bundle = DbtArtifactBundle(sources_yml="version: 2\n")
        files = bundle.all_files()
        assert "models/sources.yml" in files

    def test_all_files_omits_sources_yml_when_empty(self):
        bundle = DbtArtifactBundle(sources_yml="")
        files = bundle.all_files()
        assert "models/sources.yml" not in files

    def test_sources_yml_path_is_outside_marts_directory(self):
        """sources.yml must be at models/ root, not inside models/marts/."""
        bundle = DbtArtifactBundle(sources_yml="version: 2\n")
        files = bundle.all_files()
        assert "models/sources.yml" in files
        assert "models/marts/sources.yml" not in files


# ---------------------------------------------------------------------------
# generate_dbt_artifacts service function
# ---------------------------------------------------------------------------


class TestGenerateDbtArtifacts:
    def test_returns_dbt_artifact_bundle(self, spec):
        bundle = generate_dbt_artifacts(spec)
        assert isinstance(bundle, DbtArtifactBundle)

    def test_fact_models_key_present(self, spec):
        bundle = generate_dbt_artifacts(spec)
        assert "fact_orders.sql" in bundle.fact_models

    def test_dimension_models_key_present(self, spec):
        bundle = generate_dbt_artifacts(spec)
        assert "dim_customer.sql" in bundle.dimension_models

    def test_schema_yml_is_non_empty(self, spec):
        bundle = generate_dbt_artifacts(spec)
        assert bundle.schema_yml.strip() != ""

    def test_sources_yml_is_non_empty(self, spec):
        bundle = generate_dbt_artifacts(spec)
        assert bundle.sources_yml.strip() != ""

    def test_generated_sql_on_spec_is_unchanged(self, spec):
        """generate_dbt_artifacts must not mutate the input spec."""
        original_sql = spec.generated_sql
        generate_dbt_artifacts(spec)
        assert spec.generated_sql == original_sql

    def test_all_files_returns_expected_paths(self, spec):
        bundle = generate_dbt_artifacts(spec)
        files = bundle.all_files()
        assert "models/sources.yml" in files
        assert "models/marts/facts/fact_orders.sql" in files
        assert "models/marts/dimensions/dim_customer.sql" in files
        assert "models/marts/schema.yml" in files

    def test_fact_sql_references_source_table(self, spec):
        bundle = generate_dbt_artifacts(spec)
        assert "orders" in bundle.fact_models["fact_orders.sql"]

    def test_dimension_sql_references_source_table(self, spec):
        bundle = generate_dbt_artifacts(spec)
        assert "customers" in bundle.dimension_models["dim_customer.sql"]
