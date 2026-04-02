"""MCP tool handlers — thin wrappers over the application service layer.

This module contains no pipeline orchestration logic.  All business logic
lives in ``application.mart_service``.  Functions here are responsible only
for invoking the service and formatting the result for MCP consumers.

Keeping this module free of MCP framework imports allows every function to
be unit-tested without a running MCP server.  ``mcp/server.py`` imports and
wraps these functions with the FastMCP decorator.
"""

from __future__ import annotations

from application.mart_service import propose_mart_from_request
from mart_design.schema import MartSpecification


# ---------------------------------------------------------------------------
# Public pipeline handler
# ---------------------------------------------------------------------------


def run_propose_mart(user_request: str, database_path: str) -> str:
    """Run the data mart design pipeline and return a formatted Markdown report.

    Delegates all orchestration to ``application.mart_service`` and formats
    the resulting ``MartSpecification`` as Markdown for MCP consumers.

    Parameters
    ----------
    user_request:
        Free-form natural language description of what the user wants to
        analyse.
    database_path:
        Absolute or relative path to the DuckDB database file.

    Returns
    -------
    str
        A Markdown report with the mart name, fact/dimension table summaries,
        rationale, and ready-to-run ``CREATE TABLE`` SQL.
    """
    spec = propose_mart_from_request(user_request, database_path)
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
