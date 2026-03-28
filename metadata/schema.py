"""Pydantic models representing data warehouse source metadata."""

from pydantic import BaseModel


class SourceColumn(BaseModel):
    """Metadata for a single column in a source table."""

    name: str
    data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False
    sample_values: list[str] = []
    description: str = ""


class SourceTable(BaseModel):
    """Metadata for a single source table in the data warehouse.

    Produced by the schema reader. Passed to the LLM as context
    for mart design decisions.
    """

    name: str
    schema_name: str = "main"
    columns: list[SourceColumn]
    row_count: int | None = None
    description: str = ""
