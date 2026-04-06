"""Integration tests for the REST API layer.

Full path under test: HTTP request -> FastAPI routing -> DuckDBSchemaReader
(real DuckDB) -> service pipeline (validators, generators) -> HTTP response.

What runs real:
    FastAPI routing, DTO conversion, DuckDBSchemaReader, validate_intent,
    validate_mart_spec, generate_sql, dbt artifact generators

What is mocked:
    parse_intent()  -> fixture_intent
    propose_mart()  -> build_spec_from_tables side_effect (uses actual source_tables)
"""
from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from tests.integration.conftest import build_spec_from_tables

_PATCH_PARSE = "application.mart_service.parse_intent"
_PATCH_PROPOSE = "application.mart_service.propose_mart"

_URL_MARTS = "/api/v1/marts"
_URL_DBT = "/api/v1/marts/dbt-artifacts"


@pytest.fixture()
def api_client() -> TestClient:
    return TestClient(app)


@pytest.mark.integration
class TestMartProposalApiIntegration:
    def test_returns_200(self, api_client, duckdb_path, fixture_intent):
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            response = api_client.post(
                _URL_MARTS,
                json={"user_request": "sales by customer", "database_path": duckdb_path},
            )
        assert response.status_code == 200

    def test_response_mart_name(self, api_client, duckdb_path, fixture_intent):
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            response = api_client.post(
                _URL_MARTS,
                json={"user_request": "sales by customer", "database_path": duckdb_path},
            )
        assert response.json()["mart_name"] == "sales_mart"

    def test_response_contains_generated_sql(self, api_client, duckdb_path, fixture_intent):
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            response = api_client.post(
                _URL_MARTS,
                json={"user_request": "sales by customer", "database_path": duckdb_path},
            )
        assert response.json()["generated_sql"] != ""

    def test_response_fact_tables_non_empty(self, api_client, duckdb_path, fixture_intent):
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            response = api_client.post(
                _URL_MARTS,
                json={"user_request": "sales by customer", "database_path": duckdb_path},
            )
        assert len(response.json()["fact_tables"]) >= 1

    def test_response_dimension_tables_non_empty(self, api_client, duckdb_path, fixture_intent):
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            response = api_client.post(
                _URL_MARTS,
                json={"user_request": "sales by customer", "database_path": duckdb_path},
            )
        assert len(response.json()["dimension_tables"]) >= 1

    def test_internal_fields_not_exposed(self, api_client, duckdb_path, fixture_intent):
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            response = api_client.post(
                _URL_MARTS,
                json={"user_request": "sales by customer", "database_path": duckdb_path},
            )
        body = response.json()
        assert "intent" not in body
        assert "source_tables" not in body


@pytest.mark.integration
class TestMartWithDbtApiIntegration:
    def test_returns_200(self, api_client, duckdb_path, fixture_intent):
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            response = api_client.post(
                _URL_DBT,
                json={"user_request": "sales by customer", "database_path": duckdb_path},
            )
        assert response.status_code == 200

    def test_response_contains_mart_field(self, api_client, duckdb_path, fixture_intent):
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            response = api_client.post(
                _URL_DBT,
                json={"user_request": "sales by customer", "database_path": duckdb_path},
            )
        assert "mart" in response.json()
        assert response.json()["mart"]["mart_name"] == "sales_mart"

    def test_response_contains_dbt_artifacts_field(self, api_client, duckdb_path, fixture_intent):
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            response = api_client.post(
                _URL_DBT,
                json={"user_request": "sales by customer", "database_path": duckdb_path},
            )
        assert "dbt_artifacts" in response.json()
        assert "files" in response.json()["dbt_artifacts"]

    def test_dbt_files_contain_expected_paths(self, api_client, duckdb_path, fixture_intent):
        with patch(_PATCH_PARSE, return_value=fixture_intent), \
             patch(_PATCH_PROPOSE, side_effect=build_spec_from_tables):
            response = api_client.post(
                _URL_DBT,
                json={"user_request": "sales by customer", "database_path": duckdb_path},
            )
        files = response.json()["dbt_artifacts"]["files"]
        assert "models/marts/facts/fact_orders.sql" in files
        assert "models/marts/dimensions/dim_customer.sql" in files
        assert "models/marts/schema.yml" in files
        assert "models/sources.yml" in files
