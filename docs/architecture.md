# Architecture

## Data Flow

```
User Input (text)
      │
      ▼
intent/parser.py           ← LLM: extract structured Intent
      │
      ▼
metadata/schema_reader.py  ← Read DuckDB table/column metadata
      │
      ▼
mart_design/designer.py    ← LLM: propose MartDesign (fact + dimensions)
      │
      ├──► mart_design/sql_generator.py  ← Generate DDL
      │
      └──► mcp/tools.py                  ← Expose as MCP tool
```

## Key Models

| Model | Location | Description |
|-------|----------|-------------|
| `UserIntent` | `intent/schema.py` | Parsed user intent |
| `SourceTable` | `metadata/schema.py` | Source DW table metadata |
| `SourceColumn` | `metadata/schema.py` | Source DW column metadata |
| `MartSpecification` | `mart_design/schema.py` | Complete mart design proposal |
| `FactDefinition` | `mart_design/schema.py` | Fact table definition |
| `DimensionDefinition` | `mart_design/schema.py` | Dimension table definition |

## MCP Integration

The `mcp/server.py` exposes the following tools to Claude:
- `parse_intent` — converts user text to a structured UserIntent
- `get_schema` — retrieves DW schema for a given dataset
- `propose_mart` — runs the full design pipeline and returns a MartSpecification
- `generate_sql` — turns a MartSpecification into executable DDL
