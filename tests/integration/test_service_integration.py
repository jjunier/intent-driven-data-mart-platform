"""Integration tests for application.mart_service.propose_mart_from_request.

What runs real:
    DuckDBSchemaReader.read_tables(), validate_intent(), validate_mart_spec(),
    generate_sql(), generate_dbt_artifacts()

What is mocked:
    parse_intent()  -> fixture_intent (avoids LLM network call)
    propose_mart()  -> build_spec_from_tables side_effect (avoids LLM network call;
                       receives actual source_tables so validate_mart_spec passes)
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from application.mart_service import generate_dbt_artifacts, propose_mart_from_request
from dbt_codegen.schema import DbtArtifactBundle
from mart_design.schema import MartSpecification
from metadata.reader import DuckDBSchemaReader
from tests.integration.conftest import build_spec_from_tables

_PATCH_PARSE = "application.mart_service.parse_intent"
_PATCH_PROPOSE = "application.mart_service.propose_mart"


@pytest.mark.integration
class TestProposeMartServiceIntegration:
    def test_returns_mart_specification(self, duckdb_path, fixture_intent):
        reader = DuckDBSchemaReader(duckdb_path)
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            result = propose_mart_from_request("sales by customer", reader)
        assert isinstance(result, MartSpecification)

    def test_generated_sql_is_non_empty(self, duckdb_path, fixture_intent):
        reader = DuckDBSchemaReader(duckdb_path)
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            result = propose_mart_from_request("sales by customer", reader)
        assert result.generated_sql != ""

    def test_generated_sql_contains_create_table(self, duckdb_path, fixture_intent):
        reader = DuckDBSchemaReader(duckdb_path)
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            result = propose_mart_from_request("sales by customer", reader)
        assert "CREATE TABLE" in result.generated_sql

    def test_source_tables_read_from_real_db(self, duckdb_path, fixture_intent):
        reader = DuckDBSchemaReader(duckdb_path)
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            result = propose_mart_from_request("sales by customer", reader)
        assert any(t.name == "orders" for t in result.source_tables)

    def test_mart_name_preserved(self, duckdb_path, fixture_intent):
        reader = DuckDBSchemaReader(duckdb_path)
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            result = propose_mart_from_request("sales by customer", reader)
        assert result.mart_name == "sales_mart"

    def test_fact_tables_present(self, duckdb_path, fixture_intent):
        reader = DuckDBSchemaReader(duckdb_path)
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            result = propose_mart_from_request("sales by customer", reader)
        assert len(result.fact_tables) >= 1

    def test_dimension_tables_present(self, duckdb_path, fixture_intent):
        reader = DuckDBSchemaReader(duckdb_path)
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            result = propose_mart_from_request("sales by customer", reader)
        assert len(result.dimension_tables) >= 1

    def test_spec_feeds_into_generate_dbt_artifacts(self, duckdb_path, fixture_intent):
        """Verify the service output flows into dbt artifact generation without error."""
        reader = DuckDBSchemaReader(duckdb_path)
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            spec = propose_mart_from_request("sales by customer", reader)
        bundle = generate_dbt_artifacts(spec)
        assert isinstance(bundle, DbtArtifactBundle)
        assert bundle.all_files()
