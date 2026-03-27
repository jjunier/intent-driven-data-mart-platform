"""Pydantic models representing a proposed data mart design."""

from enum import Enum

from pydantic import BaseModel

from intent.schema import UserIntent
from metadata.schema import SourceTable


class AggregationType(str, Enum):
    sum = "sum"
    count = "count"
    count_distinct = "count_distinct"
    avg = "avg"
    min = "min"
    max = "max"


class MetricDefinition(BaseModel):
    """A single measurable metric in the fact table."""

    name: str
    expression: str
    aggregation: AggregationType
    source_column: str
    description: str = ""


class DimensionDefinition(BaseModel):
    """A single dimension table in the mart."""

    name: str
    source_table: str
    key_column: str
    attribute_columns: list[str]
    description: str = ""


class FactDefinition(BaseModel):
    """A single fact table in the mart."""

    name: str
    source_tables: list[str]
    metrics: list[MetricDefinition]
    dimension_keys: list[str]
    grain: str
    description: str = ""


class MartSpecification(BaseModel):
    """Complete data mart design proposal.

    The final output of the design pipeline. Contains all information
    needed for design review, SQL generation, and MCP responses.
    """

    mart_name: str
    description: str
    intent: UserIntent
    source_tables: list[SourceTable]
    fact_tables: list[FactDefinition]
    dimension_tables: list[DimensionDefinition]
    rationale: str = ""
    generated_sql: str = ""
