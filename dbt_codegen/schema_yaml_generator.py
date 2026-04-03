"""Generates dbt schema.yml from a MartSpecification.

This module is a pure transformation — no LLM calls, no I/O.
The output is a valid YAML string that can be written directly to
``models/marts/schema.yml`` in a dbt project.

Column-level test assignment rules:
- Dimension ``key_column``        → ``not_null`` + ``unique``
- Dimension ``attribute_columns`` → ``accepted_values`` (conditional, see below)
- Fact ``dimension_keys``         → ``not_null`` + ``relationships`` (when a matching
                                     dimension table is found in the spec)
- Fact metric columns             → ``not_null``

``accepted_values`` is generated only when ALL of the following hold:
1. ``SourceColumn.sample_values`` is non-empty.
2. ``len(sample_values) <= 10``  (larger sets are not useful as allowed-value lists).
3. The column's ``data_type`` contains a string-like keyword
   (VARCHAR, TEXT, CHAR, or STRING).

``relationships`` is generated when ``dimension_key`` can be matched to a
``DimensionDefinition.key_column`` in ``spec.dimension_tables``.
"""

from __future__ import annotations

from typing import Union

from mart_design.schema import (
    DimensionDefinition,
    FactDefinition,
    MartSpecification,
)
from metadata.schema import SourceColumn, SourceTable

# A dbt test entry is either a plain name (str) or a parameterised test (dict).
DbtTestEntry = Union[str, dict]

# Materialisation is always ``table`` in the MVP.
_MATERIALIZATION = "table"

# YAML indentation width (spaces).
_INDENT = "  "

# String-like data type keywords that qualify for accepted_values.
_STRING_TYPE_KEYWORDS = ("VARCHAR", "TEXT", "CHAR", "STRING")

# Maximum number of sample values that may be used for accepted_values.
_MAX_ACCEPTED_VALUES = 10


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
    dim_key_lookup = _build_dim_key_lookup(spec.dimension_tables)
    column_lookup = _build_column_lookup(spec.source_tables)

    lines: list[str] = ["version: 2", "", "models:"]

    for dim in spec.dimension_tables:
        lines.extend(_dimension_model_entry(dim, column_lookup))

    for fact in spec.fact_tables:
        lines.extend(_fact_model_entry(fact, dim_key_lookup))

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Per-model entry builders
# ---------------------------------------------------------------------------


def _dimension_model_entry(
    dim: DimensionDefinition,
    column_lookup: dict[str, dict[str, SourceColumn]],
) -> list[str]:
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

    # Attribute columns — accepted_values when evidence is sufficient
    src_cols = column_lookup.get(dim.source_table, {})
    for attr in dim.attribute_columns:
        src_col = src_cols.get(attr)
        tests: list[DbtTestEntry] | None = None
        if src_col and _should_add_accepted_values(src_col):
            tests = [_accepted_values_test(src_col.sample_values)]
        lines.extend(_column_entry(attr, "", tests=tests))

    return lines


def _fact_model_entry(
    fact: FactDefinition,
    dim_key_lookup: dict[str, DimensionDefinition],
) -> list[str]:
    """Return YAML lines for a single fact model entry."""
    lines: list[str] = [
        f"{_INDENT}- name: {fact.name}",
        f"{_INDENT}  description: {_quote(fact.description)}",
        f"{_INDENT}  config:",
        f"{_INDENT}    materialized: {_MATERIALIZATION}",
        f"{_INDENT}  columns:",
    ]

    # Dimension key columns — not_null + relationships (when dim found)
    for key in fact.dimension_keys:
        tests: list[DbtTestEntry] = ["not_null"]
        dim = dim_key_lookup.get(key)
        if dim:
            tests.append(_relationships_test(dim))
        lines.extend(_column_entry(key, "", tests=tests))

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
    tests: list[DbtTestEntry] | None = None,
) -> list[str]:
    """Return YAML lines for a single column entry under ``columns:``.

    Supports both simple string tests (``"not_null"``) and parameterised
    dict tests (``{"relationships": {...}}``).
    """
    col_indent = _INDENT * 2  # 4 spaces under the models list
    lines = [
        f"{col_indent}  - name: {name}",
        f"{col_indent}    description: {_quote(description)}",
    ]
    if tests:
        lines.append(f"{col_indent}    tests:")
        for test in tests:
            if isinstance(test, str):
                lines.append(f"{col_indent}      - {test}")
            else:
                lines.extend(_render_parameterised_test(test, col_indent))
    return lines


# ---------------------------------------------------------------------------
# Parameterised test renderers
# ---------------------------------------------------------------------------


def _relationships_test(dim: DimensionDefinition) -> dict:
    """Return the parameterised relationships test dict for a dimension key."""
    return {
        "relationships": {
            "to": f"ref('{dim.name}')",
            "field": dim.key_column,
        }
    }


def _accepted_values_test(sample_values: list[str]) -> dict:
    """Return the parameterised accepted_values test dict."""
    return {"accepted_values": {"values": list(sample_values)}}


def _render_parameterised_test(test: dict, col_indent: str) -> list[str]:
    """Render a single parameterised test dict into YAML lines.

    Handles two known shapes:
    - ``{"relationships": {"to": ..., "field": ...}}``
    - ``{"accepted_values": {"values": [...]}}``

    Any unknown top-level key is rendered with its sub-keys as scalars;
    list values are rendered as YAML sequence items.
    """
    lines: list[str] = []
    test_indent = f"{col_indent}      "  # aligns under "tests:"
    param_indent = f"{col_indent}          "  # aligns under test name

    for test_name, params in test.items():
        lines.append(f"{test_indent}- {test_name}:")
        for param_key, param_value in params.items():
            if isinstance(param_value, list):
                lines.append(f"{param_indent}{param_key}:")
                for item in param_value:
                    lines.append(f"{param_indent}  - {_quote(item)}")
            else:
                lines.append(f"{param_indent}{param_key}: {_quote(str(param_value))}")

    return lines


# ---------------------------------------------------------------------------
# Condition helpers
# ---------------------------------------------------------------------------


def _should_add_accepted_values(col: SourceColumn) -> bool:
    """Return True when a column qualifies for an accepted_values test.

    Conditions (all must hold):
    1. ``sample_values`` is non-empty.
    2. ``len(sample_values) <= _MAX_ACCEPTED_VALUES``.
    3. ``data_type`` contains a string-like keyword.
    """
    if not col.sample_values:
        return False
    if len(col.sample_values) > _MAX_ACCEPTED_VALUES:
        return False
    return any(
        keyword in col.data_type.upper()
        for keyword in _STRING_TYPE_KEYWORDS
    )


# ---------------------------------------------------------------------------
# Lookup builders
# ---------------------------------------------------------------------------


def _build_dim_key_lookup(
    dimension_tables: list[DimensionDefinition],
) -> dict[str, DimensionDefinition]:
    """Return ``{key_column: DimensionDefinition}`` for fast lookup."""
    return {dim.key_column: dim for dim in dimension_tables}


def _build_column_lookup(
    source_tables: list[SourceTable],
) -> dict[str, dict[str, SourceColumn]]:
    """Return ``{table_name: {column_name: SourceColumn}}`` for fast lookup."""
    return {
        table.name: {col.name: col for col in table.columns}
        for table in source_tables
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _quote(value: str) -> str:
    """Wrap *value* in double quotes for safe YAML string output."""
    escaped = value.replace('"', '\\"')
    return f'"{escaped}"'
