"""Generates dbt sources.yml from a MartSpecification.

This module is a pure transformation — no LLM calls, no I/O.
The output is a valid YAML string that can be written directly to
``models/sources.yml`` in a dbt project.

The source group name is taken from ``_constants.RAW_SCHEMA`` so that
this file and ``model_generator`` always refer to the same source name,
keeping ``{{ source('raw', ...) }}`` macro references consistent with
the declarations here.
"""

from __future__ import annotations

from dbt_codegen._constants import RAW_SCHEMA
from mart_design.schema import MartSpecification

# YAML indentation width (spaces).
_INDENT = "  "


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_sources_yml(spec: MartSpecification) -> str:
    """Return the full contents of a dbt ``sources.yml`` for *spec*.

    One source group (``raw``) is declared, containing every table listed
    in ``spec.source_tables``.  This matches the ``{{ source('raw', ...) }}``
    references emitted by ``model_generator``.

    Parameters
    ----------
    spec:
        A fully validated mart specification.

    Returns
    -------
    str
        A valid YAML string beginning with ``version: 2``.
    """
    lines: list[str] = [
        "version: 2",
        "",
        "sources:",
        f"{_INDENT}- name: {RAW_SCHEMA}",
        f"{_INDENT}  tables:",
    ]

    for table in spec.source_tables:
        lines.extend(_table_entry(table.name, table.description))

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _table_entry(name: str, description: str) -> list[str]:
    """Return YAML lines for a single table entry under ``tables:``."""
    table_indent = _INDENT * 2  # 4 spaces under the source list
    return [
        f"{table_indent}  - name: {name}",
        f"{table_indent}    description: {_quote(description)}",
    ]


def _quote(value: str) -> str:
    """Wrap *value* in double quotes for safe YAML string output."""
    escaped = value.replace('"', '\\"')
    return f'"{escaped}"'
