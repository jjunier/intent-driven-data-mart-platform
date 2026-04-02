"""Application service for data mart design.

This module is the single entry point for the mart proposal pipeline.
It is deliberately free of any delivery-mechanism imports (MCP, FastAPI,
CLI) so that every caller can format the returned ``MartSpecification``
however it likes.
"""

from __future__ import annotations

import anthropic

from intent.parser import parse_intent
from intent.validator import validate_intent
from mart_design.designer import propose_mart
from mart_design.schema import MartSpecification
from mart_design.sql_generator import generate_sql
from mart_design.validator import validate_mart_spec
from metadata.reader import SchemaReader


def propose_mart_from_request(
    user_request: str,
    schema_reader: SchemaReader,
    client: anthropic.Anthropic | None = None,
) -> MartSpecification:
    """Run the full mart design pipeline and return a typed specification.

    Steps
    -----
    1. Parse ``user_request`` into a structured ``UserIntent`` via the LLM.
    2. Validate the intent structurally (metrics, dimensions, granularity).
    3. Read source table metadata via ``schema_reader``.
    4. Propose a ``MartSpecification`` (fact + dimension tables) via the LLM.
    5. Validate that all column references exist in the source tables.
    6. Generate ``CREATE TABLE`` DDL and attach it to the specification.

    Parameters
    ----------
    user_request:
        Free-form natural language description of what the user wants to
        analyse.
    schema_reader:
        Any object implementing ``SchemaReader.read_tables()``; typically a
        ``DuckDBSchemaReader`` but can be any compatible reader (Snowflake,
        BigQuery, in-memory stub for tests, etc.).
    client:
        An ``anthropic.Anthropic`` instance shared across LLM calls in this
        pipeline run.  When ``None`` (default) each LLM function creates its
        own client from ``settings.anthropic_api_key``.  Pass an explicit
        client to enable injection in tests or to reuse a configured instance.

    Returns
    -------
    MartSpecification
        A fully populated mart specification including ``generated_sql``.
        Callers are responsible for formatting the result (Markdown, JSON,
        plain text, etc.).
    """
    intent = parse_intent(user_request, client=client)
    validate_intent(intent)

    source_tables = schema_reader.read_tables()

    spec = propose_mart(intent, source_tables, client=client)
    validate_mart_spec(spec)
    sql = generate_sql(spec)
    return spec.model_copy(update={"generated_sql": sql})
