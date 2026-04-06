"""Integration tests for application.mart_service.generate_dbt_artifacts.

Builds a MartSpecification from a real DuckDB schema (no LLM) and runs the
full artifact generation pipeline.  Validates file paths, content structure,
and YAML well-formedness.
"""
from __future__ import annotations

import yaml
import pytest

from application.mart_service import generate_dbt_artifacts
from dbt_codegen.schema import DbtArtifactBundle
from metadata.reader import DuckDBSchemaReader
from tests.integration.conftest import build_spec_from_tables


@pytest.fixture()
def artifact_bundle(duckdb_path, fixture_intent):
    """DbtArtifactBundle generated from a real DuckDB schema — no LLM."""
    source_tables = DuckDBSchemaReader(duckdb_path).read_tables()
    spec = build_spec_from_tables(fixture_intent, source_tables)
    return generate_dbt_artifacts(spec)


@pytest.mark.integration
class TestDbtArtifactIntegration:
    def test_returns_dbt_artifact_bundle(self, artifact_bundle):
        assert isinstance(artifact_bundle, DbtArtifactBundle)

    def test_all_files_returns_non_empty_dict(self, artifact_bundle):
        files = artifact_bundle.all_files()
        assert isinstance(files, dict) and len(files) > 0

    def test_fact_model_path_present(self, artifact_bundle):
        assert "models/marts/facts/fact_orders.sql" in artifact_bundle.all_files()

    def test_dimension_model_path_present(self, artifact_bundle):
        assert "models/marts/dimensions/dim_customer.sql" in artifact_bundle.all_files()

    def test_schema_yml_path_present(self, artifact_bundle):
        assert "models/marts/schema.yml" in artifact_bundle.all_files()

    def test_sources_yml_path_present(self, artifact_bundle):
        assert "models/sources.yml" in artifact_bundle.all_files()

    def test_schema_yml_is_valid_yaml(self, artifact_bundle):
        content = artifact_bundle.all_files()["models/marts/schema.yml"]
        parsed = yaml.safe_load(content)
        assert parsed is not None

    def test_sources_yml_is_valid_yaml(self, artifact_bundle):
        content = artifact_bundle.all_files()["models/sources.yml"]
        parsed = yaml.safe_load(content)
        assert parsed is not None

    def test_fact_model_sql_is_non_empty(self, artifact_bundle):
        content = artifact_bundle.all_files()["models/marts/facts/fact_orders.sql"]
        assert content.strip()

    def test_dimension_model_references_source_macro(self, artifact_bundle):
        content = artifact_bundle.all_files()["models/marts/dimensions/dim_customer.sql"]
        assert "source(" in content
