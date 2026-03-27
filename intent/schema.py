"""Pydantic model representing a structured user intent."""

from pydantic import BaseModel


class UserIntent(BaseModel):
    """Structured representation of what the user wants to analyze.

    Produced by the intent parser from raw natural language input.
    All downstream design decisions are grounded in this model.
    """

    raw_input: str
    subject_area: str
    required_metrics: list[str]
    required_dimensions: list[str]
    filters: dict[str, str] = {}
    time_granularity: str = "daily"
    notes: str = ""
