# Architecture

## Data Flow

```
User Input
    │
    ▼
intent/parser.py           ← LLM extracts structured Intent from natural language
    │
    ▼
metadata/schema_reader.py  ← Reads table/column info from the data warehouse
    │
    ▼
mart_design/designer.py    ← LLM proposes MartDesign (fact + dimension tables)
    │
    ├──► mart_design/sql_generator.py  ← Generates DDL/SQL from the design
    │
    └──► mcp/tools.py                  ← Exposes results as MCP tools to Claude
```

## Key Models

| Model | Location | Description |
|-------|----------|-------------|
| `Intent` | `intent/schema.py` | Parsed user intent |
| `MartDesign` | `mart_design/schema.py` | Proposed mart structure |
| `TableDefinition` | `mart_design/schema.py` | Single table in the mart |

## MCP Integration

The `mcp/server.py` exposes the following tools to Claude:
- `parse_intent` — converts user text to a structured Intent
- `get_schema` — retrieves DW schema for a given dataset
- `propose_mart` — runs the full design pipeline and returns a MartDesign
- `generate_sql` — turns a MartDesign into executable DDL
