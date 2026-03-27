# Intent-Driven Data Mart Platform

> Describe what you want to analyze. Get a data mart — designed and ready to deploy.

---

## Overview

Intent-Driven Data Mart Platform is an AI-powered tool that bridges the gap between business intent and data engineering. Instead of manually designing star schemas and writing DDL, users describe what they want to analyze in plain language. The platform reads the existing data warehouse schema, interprets the intent, and proposes a fully structured data mart with ready-to-run SQL.

---

## Problem

Designing a data mart is repetitive yet error-prone work. A data engineer must:
1. Understand what the business wants to measure
2. Survey dozens of source tables in the warehouse
3. Manually design fact and dimension tables
4. Write DDL and transformation logic

This process is slow, requires deep domain knowledge, and produces inconsistent results across teams.

---

## Goal

Automate the data mart design process by combining:
- **User intent** expressed in natural language
- **Source schema** read directly from the data warehouse
- **LLM reasoning** to propose a coherent, deployable mart design

---

## Core Features

| Feature | Description |
|---------|-------------|
| Intent parsing | Converts free-form user text into a structured intent model |
| Schema introspection | Reads table and column metadata from the connected data warehouse |
| Mart proposal | Proposes fact and dimension tables grounded in the actual schema |
| SQL generation | Outputs `CREATE TABLE` DDL ready for review and deployment |
| MCP integration | Exposes the full pipeline as callable tools for Claude |

---

## MVP Scope

The first MVP focuses on a single end-to-end flow:

**Input:** Natural language description of the desired mart + DuckDB schema
**Output:** Fact/dimension table design + `CREATE TABLE` SQL printed to console

**Included in MVP**
- Intent → structured `Intent` model (via LLM)
- DuckDB schema reader
- `Intent` + schema → `MartDesign` proposal (via LLM)
- DDL generation from `MartDesign`
- One MCP tool: `propose_mart`, callable from Claude Desktop

**Explicitly excluded from MVP**
- Web UI, REST API
- BigQuery / Snowflake / Redshift connectors
- Multi-candidate mart comparison
- Automatic mart deployment
- Data lineage tracking

---

## Architecture

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

---

## Tech Stack

| Layer | Choice | Reason |
|-------|--------|--------|
| Language | Python 3.11+ | Ecosystem fit for data + AI |
| LLM | Claude (Anthropic API) | Structured output, tool use |
| Data warehouse (MVP) | DuckDB | Zero-infra, easy local testing |
| MCP server | `mcp` Python SDK | Native Claude Desktop integration |
| Data validation | Pydantic v2 | Strict schema for Intent and MartDesign |
| Package management | `pyproject.toml` | Modern Python standard |

---

## Roadmap

**v0.2**
- BigQuery and Snowflake connectors
- Multi-turn conversational refinement of the mart design

**v0.3**
- Multiple mart proposals with trade-off comparison
- REST API (`FastAPI`) for external integrations

**v1.0**
- Web UI for non-technical users
- Automatic mart deployment with approval workflow
- Data lineage tracking
