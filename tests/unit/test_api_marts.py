"""Unit tests for app.routers.marts (REST API layer).

All service functions and infrastructure dependencies are mocked so that
tests run without LLM calls, network access, or a real database file.

Patching targets:
- ``app.routers.marts.propose_mart_from_request``
- ``app.routers.marts.generate_dbt_artifacts``
- ``app.routers.marts.DuckDBSchemaReader``
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers.marts import (
    DbtArtifactResponse,
    DimensionTableResponse,
    FactTableResponse,
    MartProposalResponse,
    MartWithDbtResponse,
    MetricResponse,
    _to_dbt_response,
    _to_mart_response,
)
from dbt_codegen.schema import DbtArtifactBundle
from intent.schema import UserIntent
from intent.validator import IntentValidationError
from mart_design.schema import (
    AggregationType,
    DimensionDefinition,
    FactDefinition,
    MartSpecification,
    MetricDefinition,
)
from mart_design.validator import MartSpecValidationError
from metadata.schema import SourceColumn, SourceTable


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_spec() -> MartSpecification:
    intent = UserIntent(
        raw_input="Show me monthly sales by customer.",
        subject_area="sales",
        required_metrics=["total_revenue"],
        required_dimensions=["customer"],
        time_granularity="monthly",
    )
    return MartSpecification(
        mart_name="sales_mart",
        description="Sales analysis by customer.",
        rationale="Kimball star schema for sales reporting.",
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
            )
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
                        description="Total sales revenue",
                    )
                ],
                dimension_keys=["customer_id"],
                grain="one row per order",
                description="Orders fact table",
            )
        ],
        dimension_tables=[
            DimensionDefinition(
                name="dim_customer",
                source_table="orders",
                key_column="customer_id",
                attribute_columns=[],
                description="Customer dimension",
            )
        ],
        generated_sql="CREATE TABLE dim_customer (...);",
    )


@pytest.fixture()
def sample_bundle() -> DbtArtifactBundle:
    return DbtArtifactBundle(
        fact_models={"fact_orders.sql": "select 1"},
        dimension_models={"dim_customer.sql": "select 2"},
        schema_yml="version: 2\n",
        sources_yml="version: 2\n",
    )


@pytest.fixture()
def client() -> TestClient:
    return TestClient(app)


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------


class TestHealthEndpoint:
    def test_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200

    def test_returns_status_ok(self, client):
        response = client.get("/health")
        assert response.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# POST /api/v1/marts
# ---------------------------------------------------------------------------


class TestMartProposalEndpoint:
    _URL = "/api/v1/marts"
    _PATCH_SERVICE = "app.routers.marts.propose_mart_from_request"
    _PATCH_READER = "app.routers.marts.DuckDBSchemaReader"

    def test_returns_200_on_success(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_READER):
            response = client.post(self._URL, json={
                "user_request": "sales by customer",
                "database_path": ":memory:",
            })
        assert response.status_code == 200

    def test_response_contains_mart_name(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_READER):
            response = client.post(self._URL, json={
                "user_request": "sales by customer",
                "database_path": ":memory:",
            })
        assert response.json()["mart_name"] == "sales_mart"

    def test_response_contains_generated_sql(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_READER):
            response = client.post(self._URL, json={
                "user_request": "sales by customer",
                "database_path": ":memory:",
            })
        assert "generated_sql" in response.json()
        assert response.json()["generated_sql"] != ""

    def test_response_contains_fact_tables(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_READER):
            response = client.post(self._URL, json={
                "user_request": "sales by customer",
                "database_path": ":memory:",
            })
        fact_tables = response.json()["fact_tables"]
        assert len(fact_tables) == 1
        assert fact_tables[0]["name"] == "fact_orders"

    def test_response_contains_dimension_tables(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_READER):
            response = client.post(self._URL, json={
                "user_request": "sales by customer",
                "database_path": ":memory:",
            })
        dim_tables = response.json()["dimension_tables"]
        assert len(dim_tables) == 1
        assert dim_tables[0]["name"] == "dim_customer"

    def test_missing_user_request_returns_422(self, client):
        response = client.post(self._URL, json={"database_path": ":memory:"})
        assert response.status_code == 422

    def test_missing_database_path_returns_422(self, client):
        response = client.post(self._URL, json={"user_request": "sales"})
        assert response.status_code == 422

    def test_intent_validation_error_returns_400(self, client):
        with patch(self._PATCH_SERVICE, side_effect=IntentValidationError("bad intent")), \
             patch(self._PATCH_READER):
            response = client.post(self._URL, json={
                "user_request": "???",
                "database_path": ":memory:",
            })
        assert response.status_code == 400
        assert "bad intent" in response.json()["detail"]

    def test_mart_spec_validation_error_returns_400(self, client):
        with patch(self._PATCH_SERVICE, side_effect=MartSpecValidationError("bad spec")), \
             patch(self._PATCH_READER):
            response = client.post(self._URL, json={
                "user_request": "sales",
                "database_path": ":memory:",
            })
        assert response.status_code == 400

    def test_unexpected_error_returns_500(self, client):
        with patch(self._PATCH_SERVICE, side_effect=RuntimeError("boom")), \
             patch(self._PATCH_READER):
            response = client.post(self._URL, json={
                "user_request": "sales",
                "database_path": ":memory:",
            })
        assert response.status_code == 500

    def test_service_called_with_correct_user_request(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec) as mock_svc, \
             patch(self._PATCH_READER):
            client.post(self._URL, json={
                "user_request": "monthly sales",
                "database_path": ":memory:",
            })
        call_args = mock_svc.call_args
        assert call_args[0][0] == "monthly sales"

    def test_schema_reader_created_with_database_path(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_READER) as mock_reader:
            client.post(self._URL, json={
                "user_request": "sales",
                "database_path": "/tmp/test.duckdb",
            })
        mock_reader.assert_called_once_with("/tmp/test.duckdb")

    def test_internal_fields_not_exposed_in_response(self, client, sample_spec):
        """intent and source_tables must not appear in the API response."""
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_READER):
            response = client.post(self._URL, json={
                "user_request": "sales",
                "database_path": ":memory:",
            })
        body = response.json()
        assert "intent" not in body
        assert "source_tables" not in body


# ---------------------------------------------------------------------------
# POST /api/v1/marts/dbt-artifacts
# ---------------------------------------------------------------------------


class TestMartWithDbtEndpoint:
    _URL = "/api/v1/marts/dbt-artifacts"
    _PATCH_SERVICE = "app.routers.marts.propose_mart_from_request"
    _PATCH_DBT = "app.routers.marts.generate_dbt_artifacts"
    _PATCH_READER = "app.routers.marts.DuckDBSchemaReader"

    def test_returns_200_on_success(self, client, sample_spec, sample_bundle):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_DBT, return_value=sample_bundle), \
             patch(self._PATCH_READER):
            response = client.post(self._URL, json={
                "user_request": "sales by customer",
                "database_path": ":memory:",
            })
        assert response.status_code == 200

    def test_response_contains_mart_field(self, client, sample_spec, sample_bundle):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_DBT, return_value=sample_bundle), \
             patch(self._PATCH_READER):
            response = client.post(self._URL, json={
                "user_request": "sales by customer",
                "database_path": ":memory:",
            })
        assert "mart" in response.json()
        assert response.json()["mart"]["mart_name"] == "sales_mart"

    def test_response_contains_dbt_artifacts_field(self, client, sample_spec, sample_bundle):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_DBT, return_value=sample_bundle), \
             patch(self._PATCH_READER):
            response = client.post(self._URL, json={
                "user_request": "sales by customer",
                "database_path": ":memory:",
            })
        assert "dbt_artifacts" in response.json()
        assert "files" in response.json()["dbt_artifacts"]

    def test_dbt_files_contain_expected_paths(self, client, sample_spec, sample_bundle):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_DBT, return_value=sample_bundle), \
             patch(self._PATCH_READER):
            response = client.post(self._URL, json={
                "user_request": "sales by customer",
                "database_path": ":memory:",
            })
        files = response.json()["dbt_artifacts"]["files"]
        assert "models/marts/facts/fact_orders.sql" in files
        assert "models/marts/dimensions/dim_customer.sql" in files

    def test_intent_validation_error_returns_400(self, client):
        with patch(self._PATCH_SERVICE, side_effect=IntentValidationError("bad intent")), \
             patch(self._PATCH_READER):
            response = client.post(self._URL, json={
                "user_request": "???",
                "database_path": ":memory:",
            })
        assert response.status_code == 400

    def test_missing_fields_returns_422(self, client):
        response = client.post(self._URL, json={})
        assert response.status_code == 422

    def test_dbt_service_called_with_spec(self, client, sample_spec, sample_bundle):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_DBT, return_value=sample_bundle) as mock_dbt, \
             patch(self._PATCH_READER):
            client.post(self._URL, json={
                "user_request": "sales",
                "database_path": ":memory:",
            })
        mock_dbt.assert_called_once_with(sample_spec)


# ---------------------------------------------------------------------------
# DTO conversion helpers
# ---------------------------------------------------------------------------


class TestToMartResponse:
    def test_returns_mart_proposal_response(self, sample_spec):
        result = _to_mart_response(sample_spec)
        assert isinstance(result, MartProposalResponse)

    def test_mart_name_mapped(self, sample_spec):
        assert _to_mart_response(sample_spec).mart_name == "sales_mart"

    def test_description_mapped(self, sample_spec):
        assert _to_mart_response(sample_spec).description == "Sales analysis by customer."

    def test_rationale_mapped(self, sample_spec):
        result = _to_mart_response(sample_spec)
        assert result.rationale == "Kimball star schema for sales reporting."

    def test_fact_tables_count(self, sample_spec):
        assert len(_to_mart_response(sample_spec).fact_tables) == 1

    def test_fact_table_grain_mapped(self, sample_spec):
        fact = _to_mart_response(sample_spec).fact_tables[0]
        assert fact.grain == "one row per order"

    def test_metric_expression_mapped(self, sample_spec):
        metric = _to_mart_response(sample_spec).fact_tables[0].metrics[0]
        assert metric.expression == "SUM(amount)"

    def test_dimension_key_column_mapped(self, sample_spec):
        dim = _to_mart_response(sample_spec).dimension_tables[0]
        assert dim.key_column == "customer_id"

    def test_generated_sql_mapped(self, sample_spec):
        assert _to_mart_response(sample_spec).generated_sql != ""

    def test_intent_not_in_response(self, sample_spec):
        result = _to_mart_response(sample_spec)
        assert not hasattr(result, "intent")

    def test_source_tables_not_in_response(self, sample_spec):
        result = _to_mart_response(sample_spec)
        assert not hasattr(result, "source_tables")


class TestToDbtResponse:
    def test_returns_dbt_artifact_response(self, sample_bundle):
        result = _to_dbt_response(sample_bundle)
        assert isinstance(result, DbtArtifactResponse)

    def test_files_dict_populated(self, sample_bundle):
        result = _to_dbt_response(sample_bundle)
        assert isinstance(result.files, dict)
        assert len(result.files) > 0

    def test_fact_model_path_present(self, sample_bundle):
        result = _to_dbt_response(sample_bundle)
        assert "models/marts/facts/fact_orders.sql" in result.files

    def test_dimension_model_path_present(self, sample_bundle):
        result = _to_dbt_response(sample_bundle)
        assert "models/marts/dimensions/dim_customer.sql" in result.files

    def test_schema_yml_path_present(self, sample_bundle):
        result = _to_dbt_response(sample_bundle)
        assert "models/marts/schema.yml" in result.files

    def test_sources_yml_path_present(self, sample_bundle):
        result = _to_dbt_response(sample_bundle)
        assert "models/sources.yml" in result.files
