"""Validates a LLM-generated MartSpecification against its source table metadata.

The LLM may reference column names that do not exist in the source tables.
This validator catches those mismatches before SQL generation, so errors are
surfaced with clear messages rather than producing invalid DDL silently.

Checks performed
----------------
- Every ``DimensionDefinition.key_column`` must exist in the columns of the
  dimension's declared ``source_table``.
- Every ``DimensionDefinition.attribute_column`` must exist in the columns of
  the dimension's declared ``source_table``.
- Every ``MetricDefinition.source_column`` must exist in at least one of the
  fact table's declared ``source_tables``.
"""

from __future__ import annotations

from mart_design.schema import MartSpecification


class MartSpecValidationError(ValueError):
    """Raised when a ``MartSpecification`` references columns absent from source tables."""


def validate_mart_spec(spec: MartSpecification) -> None:
    """Raise ``MartSpecValidationError`` if *spec* references non-existent columns.

    Parameters
    ----------
    spec:
        The ``MartSpecification`` returned by the mart designer.

    Raises
    ------
    MartSpecValidationError
        If any column reference in the specification cannot be resolved to a
        column in ``spec.source_tables``.
    """
    column_index = _build_column_index(spec)
    _validate_dimensions(spec, column_index)
    _validate_fact_metrics(spec, column_index)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _build_column_index(spec: MartSpecification) -> dict[str, set[str]]:
    """Return a mapping of table_name -> set of column names from source tables."""
    return {
        table.name: {col.name for col in table.columns}
        for table in spec.source_tables
    }


def _validate_dimensions(
    spec: MartSpecification,
    column_index: dict[str, set[str]],
) -> None:
    for dim in spec.dimension_tables:
        table_name = dim.source_table
        known = column_index.get(table_name)

        if known is None:
            raise MartSpecValidationError(
                f"Dimension '{dim.name}' references source table '{table_name}' "
                f"which is not present in source_tables. "
                f"Available tables: {sorted(column_index)}."
            )

        if dim.key_column not in known:
            raise MartSpecValidationError(
                f"Dimension '{dim.name}' key_column '{dim.key_column}' "
                f"does not exist in source table '{table_name}'. "
                f"Available columns: {sorted(known)}."
            )

        for attr in dim.attribute_columns:
            if attr not in known:
                raise MartSpecValidationError(
                    f"Dimension '{dim.name}' attribute_column '{attr}' "
                    f"does not exist in source table '{table_name}'. "
                    f"Available columns: {sorted(known)}."
                )


def _validate_fact_metrics(
    spec: MartSpecification,
    column_index: dict[str, set[str]],
) -> None:
    for fact in spec.fact_tables:
        # Collect all columns reachable from this fact's source tables.
        reachable: set[str] = set()
        for table_name in fact.source_tables:
            reachable.update(column_index.get(table_name, set()))

        for metric in fact.metrics:
            if metric.source_column not in reachable:
                raise MartSpecValidationError(
                    f"Fact '{fact.name}' metric '{metric.name}' references "
                    f"source_column '{metric.source_column}' which does not exist "
                    f"in any of the fact's source tables {fact.source_tables}. "
                    f"Reachable columns: {sorted(reachable)}."
                )
