"""FastMCP server — exposes the data mart design pipeline as MCP tools.

Run directly for local testing::

    python -m mcp.server

Or register in ``claude_desktop_config.json``::

    {
      "mcpServers": {
        "data-mart": {
          "command": "python",
          "args": ["-m", "mcp.server"],
          "env": {
            "ANTHROPIC_API_KEY": "<your-key>",
            "PYTHONPATH": "<repo-root>"
          }
        }
      }
    }
"""

from mcp.server.fastmcp import FastMCP

from mcp.tools import run_propose_mart

mcp = FastMCP(
    "intent-driven-data-mart-platform",
    instructions=(
        "Use the propose_mart tool to design a data mart from a natural language "
        "request and a DuckDB database file. The tool reads the live schema, "
        "proposes fact and dimension tables, and returns ready-to-run CREATE TABLE SQL."
    ),
)


@mcp.tool()
def propose_mart(user_request: str, database_path: str) -> str:
    """Propose a complete data mart design from a natural language request.

    Reads the schema from the DuckDB database at ``database_path``,
    interprets ``user_request`` to extract analysis intent, proposes a
    Kimball-style star schema, and returns a Markdown report with the
    full mart design and ready-to-run ``CREATE TABLE`` SQL.

    Args:
        user_request: Natural language description of what you want to analyse
            (e.g. "Show me monthly revenue by product category and region").
        database_path: Absolute path to the DuckDB ``.db`` file that contains
            the source tables.

    Returns:
        Markdown report containing the mart name, fact and dimension table
        summaries, design rationale, and ``CREATE TABLE`` DDL.
    """
    return run_propose_mart(user_request, database_path)


if __name__ == "__main__":
    mcp.run()
