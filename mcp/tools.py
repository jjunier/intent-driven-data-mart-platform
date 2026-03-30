"""Pure pipeline handler functions exposed as MCP tools.

This module contains no MCP framework imports so that every function can be
unit-tested without a running MCP server.  ``mcp/server.py`` imports and
wraps these functions with the FastMCP decorator.
"""

from __future__ import annotations

from mart_design.designer import propose_mart as _propose_mart
from mart_design.schema import MartSpecification
from mart_design.sql_generator import generate_sql
from metadata.connector import DuckDBConnector
from metadata.schema_reader import read_tables
from intent.parser import parse_intent


# ---------------------------------------------------------------------------
# Public pipeline handler
# ---------------------------------------------------------------------------


def run_propose_mart(user_request: str, database_path: str) -> str:
    """Run the full data mart design pipeline and return a formatted report.

    Steps
    -----
    1. Parse ``user_request`` into a structured ``UserIntent`` via the LLM.
    2. Open the DuckDB database at ``database_path`` and read table metadata.
    3. Propose a ``MartSpecification`` (fact + dimension tables) via the LLM.
    4. Generate ``CREATE TABLE`` DDL from the specification.
    5. Return a Markdown-formatted report containing the design and DDL.

    Parameters
    ----------
    user_request:
        Free-form natural language description of what the user wants to analyse.
    database_path:
        Absolute or relative path to the DuckDB database file.
        Pass ``":memory:"`` only for testing purposes.

    Returns
    -------
    str
        A Markdown report with the mart name, fact/dimension table summaries,
        rationale, and ready-to-run ``CREATE TABLE`` SQL.
    """
    intent = parse_intent(user_request)

    with DuckDBConnector(database_path, read_only=True) as conn:
        source_tables = read_tables(conn)

    spec = _propose_mart(intent, source_tables)
    sql = generate_sql(spec)
    spec = spec.model_copy(update={"generated_sql": sql})

    return _format_response(spec)


# ---------------------------------------------------------------------------
# Response formatter
# ---------------------------------------------------------------------------


def _format_response(spec: MartSpecification) -> str:
    """Render a ``MartSpecification`` as a Markdown report.

    Parameters
    ----------
    spec:
        A fully populated mart specification; ``spec.generated_sql`` must
        already contain the DDL string produced by ``generate_sql()``.

    Returns
    -------
    str
        Human-readable Markdown intended as the MCP tool response.
    """
    lines: list[str] = []

    lines.append(f"# Mart Design: {spec.mart_name}")
    lines.append("")
    lines.append(spec.description)
    lines.append("")

    # ── Fact tables ──────────────────────────────────────────────────────
    lines.append("## Fact Tables")
    for fact in spec.fact_tables:
        lines.append("")
        lines.append(f"### {fact.name}")
        lines.append(f"- **Grain:** {fact.grain}")
        lines.append(f"- **Source tables:** {', '.join(fact.source_tables)}")
        if fact.description:
            lines.append(f"- **Description:** {fact.description}")
        lines.append("- **Metrics:**")
        for metric in fact.metrics:
            desc = f" — {metric.description}" if metric.description else ""
            lines.append(f"  - `{metric.name}`: `{metric.expression}`{desc}")
        lines.append(f"- **Dimension keys:** {', '.join(fact.dimension_keys)}")

    lines.append("")

    # ── Dimension tables ──────────────────────────────────────────────────
    lines.append("## Dimension Tables")
    for dim in spec.dimension_tables:
        lines.append("")
        lines.append(f"### {dim.name}")
        lines.append(f"- **Source table:** {dim.source_table}")
        lines.append(f"- **Key column:** {dim.key_column}")
        lines.append(f"- **Attributes:** {', '.join(dim.attribute_columns)}")
        if dim.description:
            lines.append(f"- **Description:** {dim.description}")

    lines.append("")

    # ── Rationale ────────────────────────────────────────────────────────
    if spec.rationale:
        lines.append("## Design Rationale")
        lines.append("")
        lines.append(spec.rationale)
        lines.append("")

    # ── Generated DDL ────────────────────────────────────────────────────
    lines.append("## Generated DDL")
    lines.append("")
    lines.append("```sql")
    lines.append(spec.generated_sql)
    lines.append("```")

    return "\n".join(lines)
