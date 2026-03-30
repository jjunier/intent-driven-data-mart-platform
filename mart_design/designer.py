"""Proposes a data mart design from a UserIntent and source table metadata.

Uses Claude API ``tool_use`` to guarantee a structured ``MartSpecification``
response.  The model is forced to call the ``propose_mart`` tool, whose nested
schema mirrors ``FactDefinition`` and ``DimensionDefinition``.  The caller-
supplied ``intent`` and ``source_tables`` are injected locally so they are
never re-serialised through the tool schema.
"""

from __future__ import annotations

import json

import anthropic

from app.config import settings
from intent.schema import UserIntent
from mart_design.schema import MartSpecification
from metadata.schema import SourceTable

_MODEL = "claude-opus-4-6"

# ---------------------------------------------------------------------------
# Tool definition
# ---------------------------------------------------------------------------

_PROPOSE_MART_TOOL: anthropic.types.ToolParam = {
    "name": "propose_mart",
    "description": (
        "Propose a complete Kimball-style data mart design grounded in the "
        "provided user intent and source table schemas.  Every column reference "
        "must exist in the supplied source tables."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "mart_name": {
                "type": "string",
                "description": "Short snake_case name for the mart (e.g. 'sales_mart').",
            },
            "description": {
                "type": "string",
                "description": "One-sentence description of what the mart enables.",
            },
            "fact_tables": {
                "type": "array",
                "description": "One or more fact table definitions.",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Fact table name (e.g. 'fact_orders').",
                        },
                        "source_tables": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Source table names used to build this fact table.",
                        },
                        "metrics": {
                            "type": "array",
                            "description": "Measurable metrics stored in this fact table.",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {
                                        "type": "string",
                                        "description": "Metric name (e.g. 'total_revenue').",
                                    },
                                    "expression": {
                                        "type": "string",
                                        "description": "SQL expression (e.g. 'SUM(total_amount)').",
                                    },
                                    "aggregation": {
                                        "type": "string",
                                        "enum": [
                                            "sum",
                                            "count",
                                            "count_distinct",
                                            "avg",
                                            "min",
                                            "max",
                                        ],
                                        "description": "Aggregation function applied to the metric.",
                                    },
                                    "source_column": {
                                        "type": "string",
                                        "description": "Source column the metric is derived from.",
                                    },
                                    "description": {
                                        "type": "string",
                                        "description": "Optional metric description.",
                                    },
                                },
                                "required": [
                                    "name",
                                    "expression",
                                    "aggregation",
                                    "source_column",
                                ],
                            },
                        },
                        "dimension_keys": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "FK column names in the fact table referencing dimensions.",
                        },
                        "grain": {
                            "type": "string",
                            "description": (
                                "Row-level grain (e.g. 'one row per order per day')."
                            ),
                        },
                        "description": {
                            "type": "string",
                            "description": "Optional fact table description.",
                        },
                    },
                    "required": [
                        "name",
                        "source_tables",
                        "metrics",
                        "dimension_keys",
                        "grain",
                    ],
                },
            },
            "dimension_tables": {
                "type": "array",
                "description": "One or more dimension table definitions.",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Dimension table name (e.g. 'dim_customer').",
                        },
                        "source_table": {
                            "type": "string",
                            "description": "Source table this dimension is built from.",
                        },
                        "key_column": {
                            "type": "string",
                            "description": "Primary key column of the dimension.",
                        },
                        "attribute_columns": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Non-key descriptive columns to include.",
                        },
                        "description": {
                            "type": "string",
                            "description": "Optional dimension description.",
                        },
                    },
                    "required": [
                        "name",
                        "source_table",
                        "key_column",
                        "attribute_columns",
                    ],
                },
            },
            "rationale": {
                "type": "string",
                "description": "Design decisions and trade-offs worth noting.",
            },
        },
        "required": ["mart_name", "description", "fact_tables", "dimension_tables"],
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def propose_mart(
    intent: UserIntent,
    source_tables: list[SourceTable],
) -> MartSpecification:
    """Propose a data mart design grounded in *intent* and *source_tables*.

    Calls the Claude API and forces the ``propose_mart`` tool so that the
    response is always a well-typed nested object.

    Parameters
    ----------
    intent:
        Structured user intent produced by ``intent.parser.parse_intent``.
    source_tables:
        Table and column metadata read from the data warehouse.

    Returns
    -------
    MartSpecification
        Complete mart design proposal ready for SQL generation.

    Raises
    ------
    ValueError
        If the model returns no ``tool_use`` block.
    """
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    user_message = _build_user_message(intent, source_tables)

    response = client.messages.create(
        model=_MODEL,
        max_tokens=4096,
        tools=[_PROPOSE_MART_TOOL],
        tool_choice={"type": "tool", "name": "propose_mart"},
        messages=[{"role": "user", "content": user_message}],
    )

    tool_inputs = _extract_tool_inputs(response)
    return MartSpecification(
        intent=intent,
        source_tables=source_tables,
        **tool_inputs,
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _build_user_message(intent: UserIntent, source_tables: list[SourceTable]) -> str:
    """Compose the user message containing intent and schema context."""
    schema_lines: list[str] = []
    for table in source_tables:
        col_parts = []
        for col in table.columns:
            flags = []
            if col.is_primary_key:
                flags.append("PK")
            if not col.is_nullable:
                flags.append("NOT NULL")
            suffix = f" ({', '.join(flags)})" if flags else ""
            col_parts.append(f"  {col.name} {col.data_type}{suffix}")
        schema_lines.append(f"Table: {table.name}")
        schema_lines.extend(col_parts)

    schema_text = "\n".join(schema_lines)
    intent_text = json.dumps(intent.model_dump(), indent=2)

    return (
        "Propose a data mart design for the following analysis intent.\n\n"
        f"## User Intent\n```json\n{intent_text}\n```\n\n"
        f"## Available Source Tables\n```\n{schema_text}\n```\n\n"
        "Design a mart that satisfies the intent using only the columns listed above."
    )


def _extract_tool_inputs(response: anthropic.types.Message) -> dict:
    """Return the input dict from the first ``tool_use`` block in *response*.

    Raises
    ------
    ValueError
        If no ``tool_use`` content block is present.
    """
    for block in response.content:
        if block.type == "tool_use":
            return block.input  # type: ignore[return-value]
    raise ValueError(
        "Claude did not return a tool_use block. "
        f"Stop reason: {response.stop_reason!r}. "
        f"Content: {response.content!r}"
    )
