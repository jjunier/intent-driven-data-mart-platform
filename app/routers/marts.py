"""REST API endpoints for mart design and dbt artifact generation.

This module is a thin controller layer.  All business logic lives in
``application.mart_service``.  Responsibilities here are:

- Declare request / response DTOs (Pydantic models).
- Create infrastructure dependencies (``DuckDBSchemaReader``).
- Delegate to service functions.
- Map domain exceptions to HTTP error responses.
- Convert domain models to response DTOs.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from application.mart_service import generate_dbt_artifacts, propose_mart_from_request
from dbt_codegen.schema import DbtArtifactBundle
from intent.validator import IntentValidationError
from mart_design.schema import MartSpecification
from mart_design.validator import MartSpecValidationError
from metadata.reader import DuckDBSchemaReader

router = APIRouter(prefix="/api/v1", tags=["marts"])


# ---------------------------------------------------------------------------
# Request DTO
# ---------------------------------------------------------------------------


class MartProposalRequest(BaseModel):
    """Request body for mart proposal endpoints.

    Attributes
    ----------
    user_request:
        Free-form natural language description of the desired data mart.
    database_path:
        File path to the DuckDB database to use as the data warehouse source.
        Use ``:memory:`` for an in-memory database.
    """

    user_request: str
    database_path: str


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
    2. Reads the source schema from the given DuckDB database.
    3. Proposes a Kimball star-schema mart design (LLM).
    4. Generates ``CREATE TABLE`` DDL.

    Raises ``400`` for invalid intent or unresolvable column references.
    Raises ``500`` for unexpected errors.
    """
    schema_reader = DuckDBSchemaReader(request.database_path)
    try:
        spec = propose_mart_from_request(request.user_request, schema_reader)
    except (IntentValidationError, MartSpecValidationError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
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

    Raises ``400`` for invalid intent or unresolvable column references.
    Raises ``500`` for unexpected errors.
    """
    schema_reader = DuckDBSchemaReader(request.database_path)
    try:
        spec = propose_mart_from_request(request.user_request, schema_reader)
        bundle = generate_dbt_artifacts(spec)
    except (IntentValidationError, MartSpecValidationError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
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
