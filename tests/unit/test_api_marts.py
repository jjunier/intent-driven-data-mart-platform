"""Unit tests for app.routers.marts (REST API layer).

All service functions and infrastructure dependencies are mocked so that
tests run without LLM calls, network access, or a real database file.

Request body format (Stage 11+):
    {
        "user_request": "...",
        "reader_config": {
            "reader_type": "duckdb",
            "database_path": "..."
        }
    }

Patching targets:
- ``app.routers.marts.propose_mart_from_request``
- ``app.routers.marts.generate_dbt_artifacts``
- ``app.routers.marts._build_reader``
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers.marts import (
    BigQueryReaderConfig,
    DbtArtifactResponse,
    DimensionTableResponse,
    DuckDBReaderConfig,
    FactTableResponse,
    MartProposalResponse,
    MartWithDbtResponse,
    MetricResponse,
    _build_reader,
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
from metadata.bigquery_reader import BigQuerySchemaReader
from metadata.reader import DuckDBSchemaReader, SchemaReader
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
    # raise_server_exceptions=False: allows the global Exception catch-all handler
    # (registered in app.main via add_exception_handler) to return a 500 JSON
    # response instead of re-raising through TestClient's transport layer.
    # Domain-specific handlers (400, 422, 503) are unaffected by this flag.
    return TestClient(app, raise_server_exceptions=False)


# Convenience request body helpers
def _duckdb_body(user_request: str = "sales by customer", path: str = ":memory:") -> dict:
    return {
        "user_request": user_request,
        "reader_config": {"reader_type": "duckdb", "database_path": path},
    }


def _bigquery_body(
    user_request: str = "sales by customer",
    project_id: str = "my-project",
    dataset_id: str = "my_dataset",
) -> dict:
    return {
        "user_request": user_request,
        "reader_config": {
            "reader_type": "bigquery",
            "project_id": project_id,
            "dataset_id": dataset_id,
        },
    }


# Convenience request body helpers
def _duckdb_body(user_request: str = "sales by customer", path: str = ":memory:") -> dict:
    return {
        "user_request": user_request,
        "reader_config": {"reader_type": "duckdb", "database_path": path},
    }


def _bigquery_body(
    user_request: str = "sales by customer",
    project_id: str = "my-project",
    dataset_id: str = "my_dataset",
) -> dict:
    return {
        "user_request": user_request,
        "reader_config": {
            "reader_type": "bigquery",
            "project_id": project_id,
            "dataset_id": dataset_id,
        },
    }


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
# _build_reader factory
# ---------------------------------------------------------------------------


class TestBuildReader:
    def test_duckdb_config_returns_duckdb_schema_reader(self):
        config = DuckDBReaderConfig(database_path=":memory:")
        reader = _build_reader(config)
        assert isinstance(reader, DuckDBSchemaReader)
        assert isinstance(reader, SchemaReader)

    def test_duckdb_reader_receives_database_path(self):
        config = DuckDBReaderConfig(database_path="/data/wh.db")
        with patch("app.routers.marts.DuckDBSchemaReader") as mock_cls:
            _build_reader(config)
        mock_cls.assert_called_once_with("/data/wh.db")

    def test_bigquery_config_returns_bigquery_schema_reader(self):
        config = BigQueryReaderConfig(project_id="p", dataset_id="d")
        reader = _build_reader(config)
        assert isinstance(reader, BigQuerySchemaReader)
        assert isinstance(reader, SchemaReader)

    def test_bigquery_reader_receives_project_and_dataset(self):
        config = BigQueryReaderConfig(project_id="proj-x", dataset_id="ds-y")
        reader = _build_reader(config)
        # Verify config was propagated (access private attr via the reader)
        assert reader._config.project_id == "proj-x"
        assert reader._config.dataset_id == "ds-y"

    def test_bigquery_reader_has_no_injected_client(self):
        """BigQuerySchemaReader must be created without a live client."""
        config = BigQueryReaderConfig(project_id="p", dataset_id="d")
        reader = _build_reader(config)
        assert reader._client is None

    def test_unsupported_reader_type_raises_value_error(self):
        """Defensive branch — Pydantic normally blocks unknown reader_type values."""
        fake_config = MagicMock()
        fake_config.reader_type = "snowflake"
        # Bypass isinstance checks by making both isinstance calls return False
        with patch("app.routers.marts.isinstance", side_effect=[False, False]):
            with pytest.raises(ValueError, match="Unsupported reader_type"):
                _build_reader(fake_config)


# ---------------------------------------------------------------------------
# POST /api/v1/marts — DuckDB reader
# ---------------------------------------------------------------------------


class TestMartProposalEndpointDuckDB:
    _URL = "/api/v1/marts"
    _PATCH_SERVICE = "app.routers.marts.propose_mart_from_request"
    _PATCH_BUILD = "app.routers.marts._build_reader"

    def test_returns_200_on_success(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_duckdb_body())
        assert response.status_code == 200

    def test_response_contains_mart_name(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_duckdb_body())
        assert response.json()["mart_name"] == "sales_mart"

    def test_response_contains_generated_sql(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_duckdb_body())
        assert response.json()["generated_sql"] != ""

    def test_response_contains_fact_tables(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_duckdb_body())
        fact_tables = response.json()["fact_tables"]
        assert len(fact_tables) == 1
        assert fact_tables[0]["name"] == "fact_orders"

    def test_response_contains_dimension_tables(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_duckdb_body())
        dim_tables = response.json()["dimension_tables"]
        assert len(dim_tables) == 1
        assert dim_tables[0]["name"] == "dim_customer"

    def test_build_reader_called_with_duckdb_config(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_BUILD, return_value=MagicMock()) as mock_build:
            client.post(self._URL, json=_duckdb_body(path="/tmp/test.db"))
        config = mock_build.call_args[0][0]
        assert isinstance(config, DuckDBReaderConfig)
        assert config.database_path == "/tmp/test.db"

    def test_service_called_with_correct_user_request(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec) as mock_svc, \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            client.post(self._URL, json=_duckdb_body(user_request="monthly sales"))
        assert mock_svc.call_args[0][0] == "monthly sales"

    def test_internal_fields_not_exposed_in_response(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_duckdb_body())
        body = response.json()
        assert "intent" not in body
        assert "source_tables" not in body

    def test_intent_validation_error_returns_400(self, client):
        with patch(self._PATCH_SERVICE, side_effect=IntentValidationError("bad intent")), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_duckdb_body(user_request="???"))
        assert response.status_code == 400
        assert "bad intent" in response.json()["detail"]

    def test_mart_spec_validation_error_returns_400(self, client):
        with patch(self._PATCH_SERVICE, side_effect=MartSpecValidationError("bad spec")), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_duckdb_body())
        assert response.status_code == 400

    def test_unexpected_error_returns_500(self, client):
        with patch(self._PATCH_SERVICE, side_effect=RuntimeError("boom")), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_duckdb_body())
        assert response.status_code == 500


# ---------------------------------------------------------------------------
# POST /api/v1/marts — BigQuery reader
# ---------------------------------------------------------------------------


class TestMartProposalEndpointBigQuery:
    _URL = "/api/v1/marts"
    _PATCH_SERVICE = "app.routers.marts.propose_mart_from_request"
    _PATCH_BUILD = "app.routers.marts._build_reader"

    def test_returns_200_with_bigquery_reader_type(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_bigquery_body())
        assert response.status_code == 200

    def test_build_reader_called_with_bigquery_config(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_BUILD, return_value=MagicMock()) as mock_build:
            client.post(self._URL, json=_bigquery_body(project_id="proj-x", dataset_id="ds-y"))
        config = mock_build.call_args[0][0]
        assert isinstance(config, BigQueryReaderConfig)
        assert config.project_id == "proj-x"
        assert config.dataset_id == "ds-y"

    def test_bigquery_response_contains_mart_name(self, client, sample_spec):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_bigquery_body())
        assert response.json()["mart_name"] == "sales_mart"

    def test_bigquery_request_missing_project_id_returns_422(self, client):
        body = {
            "user_request": "sales",
            "reader_config": {"reader_type": "bigquery", "dataset_id": "ds"},
        }
        response = client.post(self._URL, json=body)
        assert response.status_code == 422

    def test_bigquery_request_missing_dataset_id_returns_422(self, client):
        body = {
            "user_request": "sales",
            "reader_config": {"reader_type": "bigquery", "project_id": "proj"},
        }
        response = client.post(self._URL, json=body)
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# POST /api/v1/marts — input validation
# ---------------------------------------------------------------------------


class TestMartProposalInputValidation:
    _URL = "/api/v1/marts"

    def test_missing_user_request_returns_422(self, client):
        body = {"reader_config": {"reader_type": "duckdb", "database_path": ":memory:"}}
        response = client.post(self._URL, json=body)
        assert response.status_code == 422

    def test_missing_reader_config_returns_422(self, client):
        response = client.post(self._URL, json={"user_request": "sales"})
        assert response.status_code == 422

    def test_invalid_reader_type_returns_422(self, client):
        body = {
            "user_request": "sales",
            "reader_config": {"reader_type": "snowflake"},
        }
        response = client.post(self._URL, json=body)
        assert response.status_code == 422

    def test_duckdb_missing_database_path_returns_422(self, client):
        body = {
            "user_request": "sales",
            "reader_config": {"reader_type": "duckdb"},
        }
        response = client.post(self._URL, json=body)
        assert response.status_code == 422

    def test_legacy_top_level_database_path_returns_422(self, client):
        """Old format (database_path at top level) is no longer accepted."""
        response = client.post(self._URL, json={
            "user_request": "sales",
            "database_path": ":memory:",
        })
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# POST /api/v1/marts/dbt-artifacts
# ---------------------------------------------------------------------------


class TestMartWithDbtEndpoint:
    _URL = "/api/v1/marts/dbt-artifacts"
    _PATCH_SERVICE = "app.routers.marts.propose_mart_from_request"
    _PATCH_DBT = "app.routers.marts.generate_dbt_artifacts"
    _PATCH_BUILD = "app.routers.marts._build_reader"

    def test_returns_200_on_success(self, client, sample_spec, sample_bundle):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_DBT, return_value=sample_bundle), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_duckdb_body())
        assert response.status_code == 200

    def test_response_contains_mart_field(self, client, sample_spec, sample_bundle):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_DBT, return_value=sample_bundle), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_duckdb_body())
        assert "mart" in response.json()
        assert response.json()["mart"]["mart_name"] == "sales_mart"

    def test_response_contains_dbt_artifacts_field(self, client, sample_spec, sample_bundle):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_DBT, return_value=sample_bundle), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_duckdb_body())
        assert "dbt_artifacts" in response.json()
        assert "files" in response.json()["dbt_artifacts"]

    def test_dbt_files_contain_expected_paths(self, client, sample_spec, sample_bundle):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_DBT, return_value=sample_bundle), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_duckdb_body())
        files = response.json()["dbt_artifacts"]["files"]
        assert "models/marts/facts/fact_orders.sql" in files
        assert "models/marts/dimensions/dim_customer.sql" in files

    def test_intent_validation_error_returns_400(self, client):
        with patch(self._PATCH_SERVICE, side_effect=IntentValidationError("bad intent")), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_duckdb_body(user_request="???"))
        assert response.status_code == 400

    def test_missing_fields_returns_422(self, client):
        response = client.post(self._URL, json={})
        assert response.status_code == 422

    def test_dbt_service_called_with_spec(self, client, sample_spec, sample_bundle):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_DBT, return_value=sample_bundle) as mock_dbt, \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            client.post(self._URL, json=_duckdb_body())
        mock_dbt.assert_called_once_with(sample_spec)

    def test_bigquery_reader_type_returns_200(self, client, sample_spec, sample_bundle):
        with patch(self._PATCH_SERVICE, return_value=sample_spec), \
             patch(self._PATCH_DBT, return_value=sample_bundle), \
             patch(self._PATCH_BUILD, return_value=MagicMock()):
            response = client.post(self._URL, json=_bigquery_body())
        assert response.status_code == 200


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
