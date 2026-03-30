"""Parses raw user input into a structured UserIntent using the Claude API.

Uses ``tool_use`` to guarantee structured output — the model is forced to call
the ``extract_intent`` tool, whose parameters map 1-to-1 to ``UserIntent``
fields.  No JSON parsing is required; Anthropic's SDK returns the tool inputs
as a plain ``dict``.
"""

from __future__ import annotations

import anthropic

from app.config import settings
from intent.schema import UserIntent

_MODEL = "claude-opus-4-6"

# ---------------------------------------------------------------------------
# Tool definition
# ---------------------------------------------------------------------------

_EXTRACT_INTENT_TOOL: anthropic.types.ToolParam = {
    "name": "extract_intent",
    "description": (
        "Extract structured data mart intent from a natural language request. "
        "Call this tool with all fields you can determine from the request. "
        "Use sensible defaults for optional fields that are not mentioned."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "subject_area": {
                "type": "string",
                "description": (
                    "Main business domain of the mart "
                    "(e.g. 'sales', 'inventory', 'hr', 'marketing')."
                ),
            },
            "required_metrics": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Quantitative measures to compute "
                    "(e.g. ['total_revenue', 'order_count', 'avg_basket_size'])."
                ),
            },
            "required_dimensions": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Grouping / slicing attributes "
                    "(e.g. ['customer', 'region', 'product_category', 'date'])."
                ),
            },
            "filters": {
                "type": "object",
                "additionalProperties": {"type": "string"},
                "description": (
                    "Fixed filter conditions as key-value pairs "
                    "(e.g. {'channel': 'online', 'year_from': '2023'})."
                ),
            },
            "time_granularity": {
                "type": "string",
                "enum": ["daily", "weekly", "monthly", "quarterly", "yearly"],
                "description": "Desired time aggregation granularity.",
            },
            "notes": {
                "type": "string",
                "description": (
                    "Any additional constraints or clarifications that do not "
                    "fit the other fields."
                ),
            },
        },
        "required": ["subject_area", "required_metrics", "required_dimensions"],
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_intent(raw_input: str) -> UserIntent:
    """Parse a natural language data mart request into a structured ``UserIntent``.

    Calls the Claude API and forces the ``extract_intent`` tool so that the
    response is always a well-typed object — no fragile JSON parsing.

    Parameters
    ----------
    raw_input:
        Free-form text describing what the user wants to analyse.

    Returns
    -------
    UserIntent
        Structured representation grounded in the user's original text.

    Raises
    ------
    ValueError
        If the model returns no ``tool_use`` block (should not happen with
        ``tool_choice`` set to force the tool).
    """
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    response = client.messages.create(
        model=_MODEL,
        max_tokens=1024,
        tools=[_EXTRACT_INTENT_TOOL],
        tool_choice={"type": "tool", "name": "extract_intent"},
        messages=[
            {
                "role": "user",
                "content": (
                    "Extract the structured data mart intent from the following "
                    "request.\n\n"
                    f"Request: {raw_input}"
                ),
            }
        ],
    )

    tool_inputs = _extract_tool_inputs(response)
    return UserIntent(raw_input=raw_input, **tool_inputs)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _extract_tool_inputs(response: anthropic.types.Message) -> dict:
    """Return the input dict from the first ``tool_use`` block in *response*.

    Parameters
    ----------
    response:
        A completed ``anthropic.types.Message`` object.

    Raises
    ------
    ValueError
        If no ``tool_use`` content block is present.
    """
    for block in response.content:
        if block.type == "tool_use":
            return block.input  # type: ignore[return-value]
    raise ValueError(
        "Claude did not return a tool_use block. "
        f"Stop reason: {response.stop_reason!r}. "
        f"Content: {response.content!r}"
    )
