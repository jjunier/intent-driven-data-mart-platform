"""Generates CREATE TABLE DDL from a MartSpecification.

This module is a pure transformation — no LLM calls are made.
Column data types are resolved from ``SourceColumn`` metadata where
available; aggregation-based defaults are applied for metric columns.

Dimension tables are always emitted before fact tables so the DDL can be
executed top-to-bottom without forward-reference errors.
"""

from __future__ import annotations

from mart_design.schema import (
    AggregationType,
    DimensionDefinition,
    FactDefinition,
    MartSpecification,
    MetricDefinition,
)
from metadata.schema import SourceColumn, SourceTable

# ---------------------------------------------------------------------------
# Type inference
# ---------------------------------------------------------------------------

# Default SQL data type per aggregation function.
# min / max are handled separately — they inherit the source column type.
_AGGREGATION_TYPE: dict[AggregationType, str] = {
    AggregationType.sum: "DOUBLE",
    AggregationType.count: "BIGINT",
    AggregationType.count_distinct: "BIGINT",
    AggregationType.avg: "DOUBLE",
    AggregationType.min: "DOUBLE",
    AggregationType.max: "DOUBLE",
}

# Fallback when a column cannot be resolved from source metadata.
_DEFAULT_TYPE = "VARCHAR"
_DEFAULT_KEY_TYPE = "BIGINT"
_DEFAULT_METRIC_TYPE = "DOUBLE"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_sql(spec: MartSpecification) -> str:
    """Generate ``CREATE TABLE`` DDL for every table in *spec*.

    Dimension tables are emitted first so that foreign key references in fact
    tables resolve correctly when the statements are executed in order.

    Parameters
    ----------
    spec:
        A complete mart design proposal produced by ``mart_design.designer``.

    Returns
    -------
    str
        One ``CREATE TABLE`` statement per mart table, each separated by a
        blank line.  The string ends without a trailing newline.
    """
    column_lookup = _build_column_lookup(spec.source_tables)
    statements: list[str] = []

    for dim in spec.dimension_tables:
        statements.append(_generate_dimension_ddl(dim, column_lookup))

    for fact in spec.fact_tables:
        statements.append(
            _generate_fact_ddl(fact, spec.dimension_tables, column_lookup)
        )

    return "\n\n".join(statements)


# ---------------------------------------------------------------------------
# DDL generators
# ---------------------------------------------------------------------------


def _generate_dimension_ddl(
    dim: DimensionDefinition,
    column_lookup: dict[str, dict[str, SourceColumn]],
) -> str:
    """Return a ``CREATE TABLE`` statement for a dimension table."""
    source_cols = column_lookup.get(dim.source_table, {})
    col_lines: list[str] = []

    # Primary key column
    key_col = source_cols.get(dim.key_column)
    key_type = key_col.data_type if key_col else _DEFAULT_KEY_TYPE
    col_lines.append(f"    {dim.key_column} {key_type} PRIMARY KEY")

    # Attribute columns
    for attr_name in dim.attribute_columns:
        attr_col = source_cols.get(attr_name)
        attr_type = attr_col.data_type if attr_col else _DEFAULT_TYPE
        not_null = " NOT NULL" if (attr_col and not attr_col.is_nullable) else ""
        col_lines.append(f"    {attr_name} {attr_type}{not_null}")

    body = ",\n".join(col_lines)
    return f"CREATE TABLE {dim.name} (\n{body}\n);"


def _generate_fact_ddl(
    fact: FactDefinition,
    dimension_tables: list[DimensionDefinition],
    column_lookup: dict[str, dict[str, SourceColumn]],
) -> str:
    """Return a ``CREATE TABLE`` statement for a fact table."""
    dim_key_types = _build_dim_key_types(dimension_tables, column_lookup)
    col_lines: list[str] = []

    # Dimension FK columns
    for dim_key in fact.dimension_keys:
        dim_type = dim_key_types.get(dim_key, _DEFAULT_KEY_TYPE)
        col_lines.append(f"    {dim_key} {dim_type} NOT NULL")

    # Metric columns
    for metric in fact.metrics:
        metric_type = _infer_metric_type(metric, column_lookup, fact.source_tables)
        col_lines.append(f"    {metric.name} {metric_type} NOT NULL")

    # Foreign key constraints (only for dimension keys present in this fact)
    fk_lines: list[str] = []
    for dim in dimension_tables:
        if dim.key_column in fact.dimension_keys:
            fk_lines.append(
                f"    FOREIGN KEY ({dim.key_column})"
                f" REFERENCES {dim.name}({dim.key_column})"
            )

    body = ",\n".join(col_lines + fk_lines)
    return f"CREATE TABLE {fact.name} (\n{body}\n);"


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _build_column_lookup(
    source_tables: list[SourceTable],
) -> dict[str, dict[str, SourceColumn]]:
    """Return ``{table_name: {column_name: SourceColumn}}`` for fast lookup."""
    return {
        table.name: {col.name: col for col in table.columns}
        for table in source_tables
    }


def _build_dim_key_types(
    dimension_tables: list[DimensionDefinition],
    column_lookup: dict[str, dict[str, SourceColumn]],
) -> dict[str, str]:
    """Return ``{key_column_name: sql_type}`` for every dimension key."""
    result: dict[str, str] = {}
    for dim in dimension_tables:
        source_cols = column_lookup.get(dim.source_table, {})
        key_col = source_cols.get(dim.key_column)
        result[dim.key_column] = key_col.data_type if key_col else _DEFAULT_KEY_TYPE
    return result


def _infer_metric_type(
    metric: MetricDefinition,
    column_lookup: dict[str, dict[str, SourceColumn]],
    source_table_names: list[str],
) -> str:
    """Infer the SQL data type for a metric column.

    ``min`` and ``max`` preserve the source column type when it can be
    resolved.  All other aggregations use ``_AGGREGATION_TYPE``.
    """
    if metric.aggregation in (AggregationType.min, AggregationType.max):
        for table_name in source_table_names:
            col = column_lookup.get(table_name, {}).get(metric.source_column)
            if col:
                return col.data_type
    return _AGGREGATION_TYPE.get(metric.aggregation, _DEFAULT_METRIC_TYPE)
