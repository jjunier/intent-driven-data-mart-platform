"""Generates dbt schema.yml from a MartSpecification.

This module is a pure transformation — no LLM calls, no I/O.
The output is a valid YAML string that can be written directly to
``models/marts/schema.yml`` in a dbt project.

Column-level test assignment rules (MVP):
- Dimension ``key_column``      → ``not_null`` + ``unique``
- Fact ``dimension_keys``       → ``not_null``
- Fact metric columns           → ``not_null``
- Dimension ``attribute_columns`` → no tests (follow-up scope)
"""

from __future__ import annotations

from mart_design.schema import (
    DimensionDefinition,
    FactDefinition,
    MartSpecification,
    MetricDefinition,
)

# Materialisation is always ``table`` in the MVP.
_MATERIALIZATION = "table"

# YAML indentation width (spaces).
_INDENT = "  "


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_schema_yml(spec: MartSpecification) -> str:
    """Return the full contents of a dbt ``schema.yml`` for *spec*.

    Parameters
    ----------
    spec:
        A fully validated mart specification.

    Returns
    -------
    str
        A valid YAML string beginning with ``version: 2``.
    """
    lines: list[str] = ["version: 2", "", "models:"]

    for dim in spec.dimension_tables:
        lines.extend(_dimension_model_entry(dim))

    for fact in spec.fact_tables:
        lines.extend(_fact_model_entry(fact))

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Per-model entry builders
# ---------------------------------------------------------------------------


def _dimension_model_entry(dim: DimensionDefinition) -> list[str]:
    """Return YAML lines for a single dimension model entry."""
    lines: list[str] = [
        f"{_INDENT}- name: {dim.name}",
        f"{_INDENT}  description: {_quote(dim.description)}",
        f"{_INDENT}  config:",
        f"{_INDENT}    materialized: {_MATERIALIZATION}",
        f"{_INDENT}  columns:",
    ]

    # Primary key column — not_null + unique
    lines.extend(_column_entry(dim.key_column, "", tests=["not_null", "unique"]))

    # Attribute columns — no tests in MVP
    for attr in dim.attribute_columns:
        lines.extend(_column_entry(attr, ""))

    return lines


def _fact_model_entry(fact: FactDefinition) -> list[str]:
    """Return YAML lines for a single fact model entry."""
    lines: list[str] = [
        f"{_INDENT}- name: {fact.name}",
        f"{_INDENT}  description: {_quote(fact.description)}",
        f"{_INDENT}  config:",
        f"{_INDENT}    materialized: {_MATERIALIZATION}",
        f"{_INDENT}  columns:",
    ]

    # Dimension key columns — not_null
    for key in fact.dimension_keys:
        lines.extend(_column_entry(key, "", tests=["not_null"]))

    # Metric columns — not_null
    for metric in fact.metrics:
        lines.extend(_column_entry(metric.name, metric.description, tests=["not_null"]))

    return lines


# ---------------------------------------------------------------------------
# Shared column entry builder
# ---------------------------------------------------------------------------


def _column_entry(
    name: str,
    description: str,
    tests: list[str] | None = None,
) -> list[str]:
    """Return YAML lines for a single column entry under ``columns:``."""
    col_indent = _INDENT * 2  # 4 spaces under models list
    lines = [
        f"{col_indent}  - name: {name}",
        f"{col_indent}    description: {_quote(description)}",
    ]
    if tests:
        lines.append(f"{col_indent}    tests:")
        for test in tests:
            lines.append(f"{col_indent}      - {test}")
    return lines


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _quote(value: str) -> str:
    """Wrap *value* in double quotes for safe YAML string output.

    Empty strings are rendered as ``""`` rather than a bare empty value,
    which some YAML parsers treat differently.
    """
    escaped = value.replace('"', '\\"')
    return f'"{escaped}"'
