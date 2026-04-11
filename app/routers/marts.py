"""REST API endpoints for mart design and dbt artifact generation.

This module is a thin controller layer.  All business logic lives in
``application.mart_service``.  Responsibilities here are:

- Declare request / response DTOs (Pydantic models).
- Build the appropriate ``SchemaReader`` from the incoming ``reader_config``
  via the private ``_build_reader`` factory.
- Delegate to service functions.
- Map domain exceptions to HTTP error responses.
- Convert domain models to response DTOs.

Reader selection
----------------
Callers choose a data warehouse reader by setting ``reader_config.reader_type``
in the request body.  Currently supported values:

``"duckdb"``
    Reads a local DuckDB file.  Requires ``database_path``.

``"bigquery"``
    Reads a Google BigQuery dataset.  Requires ``project_id`` and
    ``dataset_id``.  No credentials field is exposed in this version;
    Application Default Credentials (ADC) are used by the underlying
    ``BigQuerySchemaReader`` when the reader is first invoked.
"""

from __future__ import annotations

from typing import Annotated, Literal, Union

from fastapi import APIRouter
from pydantic import BaseModel, Field

from application.mart_service import generate_dbt_artifacts, propose_mart_from_request
from dbt_codegen.schema import DbtArtifactBundle
from mart_design.schema import MartSpecification
from metadata.reader import DuckDBSchemaReader, SchemaReader

router = APIRouter(prefix="/api/v1", tags=["marts"])


# ---------------------------------------------------------------------------
# Reader configuration DTOs
# ---------------------------------------------------------------------------


class DuckDBReaderConfig(BaseModel):
    """Connection settings for a local DuckDB database.

    Attributes
    ----------
    reader_type:
        Discriminator field — must be ``"duckdb"``.
    database_path:
        Absolute or relative path to the DuckDB ``.db`` file, or
        ``":memory:"`` for an ephemeral in-memory database.
    """

    reader_type: Literal["duckdb"] = "duckdb"
    database_path: str = Field(
        ...,
        examples=[":memory:", "/data/warehouse.db"],
        description="Path to the DuckDB database file, or ':memory:' for in-memory.",
    )


class BigQueryReaderConfig(BaseModel):
    """Connection settings for a Google BigQuery dataset.

    No credentials field is included in this version; the underlying
    ``BigQuerySchemaReader`` uses Application Default Credentials (ADC)
    when invoked.  Credentials management and secret automation are
    deferred to a later stage.

    Attributes
    ----------
    reader_type:
        Discriminator field — must be ``"bigquery"``.
    project_id:
        GCP project that owns the dataset.
    dataset_id:
        BigQuery dataset to inspect.
    """

    reader_type: Literal["bigquery"] = "bigquery"
    project_id: str = Field(
        ...,
        examples=["my-gcp-project"],
        description="GCP project ID that owns the BigQuery dataset.",
    )
    dataset_id: str = Field(
        ...,
        examples=["my_dataset"],
        description="BigQuery dataset to read source table metadata from.",
    )


# Discriminated union — Pydantic uses reader_type to select the right model.
ReaderConfig = Annotated[
    Union[DuckDBReaderConfig, BigQueryReaderConfig],
    Field(discriminator="reader_type"),
]


# ---------------------------------------------------------------------------
# Request DTO
# ---------------------------------------------------------------------------


class MartProposalRequest(BaseModel):
    """Request body for mart proposal endpoints.

    Attributes
    ----------
    user_request:
        Free-form natural language description of the desired data mart.
    reader_config:
        Data warehouse connection settings.  Set ``reader_type`` to select
        the appropriate reader (``"duckdb"`` or ``"bigquery"``).
    """

    user_request: str = Field(
        ...,
        examples=["Show me monthly sales broken down by customer region"],
        description="Natural language description of the desired data mart.",
    )
    reader_config: ReaderConfig = Field(
        ...,
        description="Data warehouse connection settings including reader_type.",
    )


# ---------------------------------------------------------------------------
# Response DTOs
# ---------------------------------------------------------------------------


class MetricResponse(BaseModel):
    name: str
    expression: str
    description: str


class FactTableResponse(BaseModel):
    name: str
    grain: str
    metrics: list[MetricResponse]
    dimension_keys: list[str]


class DimensionTableResponse(BaseModel):
    name: str
    key_column: str
    attribute_columns: list[str]


class MartProposalResponse(BaseModel):
    """Response body for ``POST /api/v1/marts``.

    Contains the mart design proposal without raw internal fields
    (``intent``, ``source_tables``) that are not relevant to API consumers.
    """

    mart_name: str
    description: str
    rationale: str
    fact_tables: list[FactTableResponse]
    dimension_tables: list[DimensionTableResponse]
    generated_sql: str


class DbtArtifactResponse(BaseModel):
    """dbt project files as a flat path-to-content mapping.

    Keys are relative paths (e.g. ``models/marts/facts/fact_orders.sql``).
    Values are the full file contents.
    """

    files: dict[str, str]


class MartWithDbtResponse(BaseModel):
    """Response body for ``POST /api/v1/marts/dbt-artifacts``."""

    mart: MartProposalResponse
    dbt_artifacts: DbtArtifactResponse


# ---------------------------------------------------------------------------
# Reader factory
# ---------------------------------------------------------------------------


def _build_reader(config: ReaderConfig) -> SchemaReader:
    """Return the appropriate ``SchemaReader`` for the given *config*.

    DuckDB
        Returns a ``DuckDBSchemaReader`` immediately; no I/O occurs here.
    BigQuery
        Imports ``BigQuerySchemaReader`` lazily so that the module stays
        importable when ``google-cloud-bigquery`` is not installed.
        Returns a ``BigQuerySchemaReader`` configured from *config*; the
        actual BigQuery client is created (and GCP is contacted) only when
        ``read_tables()`` is called on the returned reader.

    Raises
    ------
    ValueError
        If *config* carries an unrecognised ``reader_type``.  In practice
        Pydantic's discriminated union prevents this at request-parse time,
        so this branch is purely defensive.
    """
    if isinstance(config, DuckDBReaderConfig):
        return DuckDBSchemaReader(config.database_path)

    if isinstance(config, BigQueryReaderConfig):
        from metadata.bigquery_reader import (  # noqa: PLC0415
            BigQueryConnectionConfig,
            BigQuerySchemaReader,
        )
        bq_config = BigQueryConnectionConfig(
            project_id=config.project_id,
            dataset_id=config.dataset_id,
        )
        return BigQuerySchemaReader(bq_config)

    raise ValueError(f"Unsupported reader_type: {config.reader_type!r}")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/marts",
    response_model=MartProposalResponse,
    summary="Propose a data mart from a natural language request",
    response_description="Mart design proposal with DDL",
)
def propose_mart_endpoint(request: MartProposalRequest) -> MartProposalResponse:
    """Run the full mart design pipeline and return a structured proposal.

    The pipeline:
    1. Parses the natural language request into a structured intent (LLM).
    2. Reads the source schema from the configured data warehouse.
    3. Proposes a Kimball star-schema mart design (LLM).
    4. Generates ``CREATE TABLE`` DDL.

    Domain exceptions propagate to the global exception handlers registered
    in ``app.main``:

    - ``IntentValidationError`` / ``MartSpecValidationError`` → 400
    - ``ImportError`` (optional dependency missing) → 503
    - All other exceptions → 500
    """
    schema_reader = _build_reader(request.reader_config)
    spec = propose_mart_from_request(request.user_request, schema_reader)
    return _to_mart_response(spec)


@router.post(
    "/marts/dbt-artifacts",
    response_model=MartWithDbtResponse,
    summary="Propose a data mart and generate dbt project files",
    response_description="Mart design proposal with dbt model SQL and YAML",
)
def propose_mart_with_dbt_endpoint(request: MartProposalRequest) -> MartWithDbtResponse:
    """Run the full mart design pipeline and additionally generate dbt artifacts.

    Returns the same mart proposal as ``POST /api/v1/marts`` plus a
    ``dbt_artifacts`` object containing ready-to-use dbt project files
    (model SQL, ``schema.yml``, ``sources.yml``).

    Domain exceptions propagate to the global exception handlers registered
    in ``app.main``:

    - ``IntentValidationError`` / ``MartSpecValidationError`` → 400
    - ``ImportError`` (optional dependency missing) → 503
    - All other exceptions → 500
    """
    schema_reader = _build_reader(request.reader_config)
    spec = propose_mart_from_request(request.user_request, schema_reader)
    bundle = generate_dbt_artifacts(spec)
    return MartWithDbtResponse(
        mart=_to_mart_response(spec),
        dbt_artifacts=_to_dbt_response(bundle),
    )


# ---------------------------------------------------------------------------
# DTO conversion helpers
# ---------------------------------------------------------------------------


def _to_mart_response(spec: MartSpecification) -> MartProposalResponse:
    """Convert a ``MartSpecification`` domain model to the API response DTO."""
    return MartProposalResponse(
        mart_name=spec.mart_name,
        description=spec.description,
        rationale=spec.rationale,
        fact_tables=[
            FactTableResponse(
                name=fact.name,
                grain=fact.grain,
                metrics=[
                    MetricResponse(
                        name=m.name,
                        expression=m.expression,
                        description=m.description,
                    )
                    for m in fact.metrics
                ],
                dimension_keys=fact.dimension_keys,
            )
            for fact in spec.fact_tables
        ],
        dimension_tables=[
            DimensionTableResponse(
                name=dim.name,
                key_column=dim.key_column,
                attribute_columns=dim.attribute_columns,
            )
            for dim in spec.dimension_tables
        ],
        generated_sql=spec.generated_sql,
    )


def _to_dbt_response(bundle: DbtArtifactBundle) -> DbtArtifactResponse:
    """Convert a ``DbtArtifactBundle`` to the API response DTO."""
    return DbtArtifactResponse(files=bundle.all_files())
