"""Generates dbt model SQL files from a MartSpecification.

This module is a pure transformation — no LLM calls, no I/O.
Each public function accepts domain objects and returns a SQL string
suitable for writing directly into a dbt ``models/`` directory.

MVP constraints (by design):
- ``raw_schema`` is always ``"raw"``.
- Fact tables are assumed to have a single source table (``source_tables[0]``).
- All models are materialised as ``table`` (incremental is a follow-up).
- ``source()`` is used for every source reference; ``ref()`` is not generated
  in this MVP because mart models are not chained.
"""

from __future__ import annotations

from dbt_codegen._constants import RAW_SCHEMA
from mart_design.schema import DimensionDefinition, FactDefinition, MartSpecification


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_dimension_model(dim: DimensionDefinition) -> str:
    """Return dbt model SQL for a single dimension table.

    The generated SQL follows the standard dbt staging pattern:
    ``source`` CTE → ``renamed`` CTE → ``select * from renamed``.

    Parameters
    ----------
    dim:
        A dimension definition from the mart specification.

    Returns
    -------
    str
        Complete dbt model SQL file content.
    """
    source_ref = f"{{{{ source('{RAW_SCHEMA}', '{dim.source_table}') }}}}"

    all_columns = [dim.key_column] + list(dim.attribute_columns)
    col_lines = _indent_columns(all_columns)

    return (
        f"with source as (\n\n"
        f"    select * from {source_ref}\n\n"
        f"),\n\n"
        f"renamed as (\n\n"
        f"    select\n"
        f"{col_lines}\n"
        f"    from source\n\n"
        f")\n\n"
        f"select * from renamed\n"
    )


def generate_fact_model(fact: FactDefinition) -> str:
    """Return dbt model SQL for a single fact table.

    The generated SQL selects from the first source table only (MVP: single
    source), applies metric expressions via a ``GROUP BY`` on dimension keys.

    Parameters
    ----------
    fact:
        A fact definition from the mart specification.

    Returns
    -------
    str
        Complete dbt model SQL file content.
    """
    primary_source = fact.source_tables[0]
    source_ref = f"{{{{ source('{RAW_SCHEMA}', '{primary_source}') }}}}"

    # Dimension key columns (GROUP BY targets)
    dim_key_lines = _indent_columns(fact.dimension_keys)

    # Metric columns: use the LLM-generated expression with an alias
    metric_lines = "\n".join(
        f"        {metric.expression} as {metric.name},"
        for metric in fact.metrics
    )
    # Remove trailing comma from last metric line
    metric_lines = metric_lines.rstrip(",")

    # GROUP BY list (positional or by name — use column names for clarity)
    group_by_cols = ",\n        ".join(fact.dimension_keys)

    # Build SELECT list: dim keys first, then metrics
    select_dim_lines = "\n".join(
        f"        {key}," for key in fact.dimension_keys
    )

    return (
        f"with source as (\n\n"
        f"    select * from {source_ref}\n\n"
        f"),\n\n"
        f"aggregated as (\n\n"
        f"    select\n"
        f"{select_dim_lines}\n"
        f"        {_join_metric_select(fact)}\n"
        f"    from source\n"
        f"    group by\n"
        f"        {group_by_cols}\n\n"
        f")\n\n"
        f"select * from aggregated\n"
    )


def generate_all_dimension_models(spec: MartSpecification) -> dict[str, str]:
    """Return ``{filename: sql}`` for every dimension table in *spec*.

    Parameters
    ----------
    spec:
        A fully validated mart specification.

    Returns
    -------
    dict[str, str]
        Keys are filenames (e.g. ``"dim_customer.sql"``); values are SQL content.
    """
    return {
        f"{dim.name}.sql": generate_dimension_model(dim)
        for dim in spec.dimension_tables
    }


def generate_all_fact_models(spec: MartSpecification) -> dict[str, str]:
    """Return ``{filename: sql}`` for every fact table in *spec*.

    Parameters
    ----------
    spec:
        A fully validated mart specification.

    Returns
    -------
    dict[str, str]
        Keys are filenames (e.g. ``"fact_orders.sql"``); values are SQL content.
    """
    return {
        f"{fact.name}.sql": generate_fact_model(fact)
        for fact in spec.fact_tables
    }


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _indent_columns(column_names: list[str], indent: int = 8) -> str:
    """Return column names as indented, comma-separated lines.

    All columns except the last are followed by a comma.
    """
    prefix = " " * indent
    lines = []
    for i, name in enumerate(column_names):
        comma = "," if i < len(column_names) - 1 else ""
        lines.append(f"{prefix}{name}{comma}")
    return "\n".join(lines)


def _join_metric_select(fact: FactDefinition) -> str:
    """Return metric SELECT lines as a single string for inline embedding."""
    parts = []
    for i, metric in enumerate(fact.metrics):
        comma = "," if i < len(fact.metrics) - 1 else ""
        parts.append(f"{metric.expression} as {metric.name}{comma}")
    return "\n        ".join(parts)
