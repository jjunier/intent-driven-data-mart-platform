"""Validates a parsed UserIntent before it enters the mart design pipeline.

Validation is intentionally lightweight: it catches obviously incomplete
intents that the LLM may return when the user's request is too vague.
It does *not* validate against source schema — that happens later in the
MartSpecification validator (Stage 4).
"""

from __future__ import annotations

from intent.schema import UserIntent

_VALID_GRANULARITIES = frozenset({"daily", "weekly", "monthly", "quarterly", "yearly"})


class IntentValidationError(ValueError):
    """Raised when a ``UserIntent`` fails structural validation."""


def validate_intent(intent: UserIntent) -> None:
    """Raise ``IntentValidationError`` if *intent* is structurally invalid.

    Checks performed
    ----------------
    - ``required_metrics`` must contain at least one entry.
    - ``required_dimensions`` must contain at least one entry.
    - ``time_granularity`` must be one of the known values.

    Parameters
    ----------
    intent:
        The ``UserIntent`` produced by the intent parser.

    Raises
    ------
    IntentValidationError
        If any check fails.  The message names the failing field and
        describes the constraint so callers can surface it to the user.
    """
    if not intent.required_metrics:
        raise IntentValidationError(
            "required_metrics must contain at least one metric. "
            "The intent parser returned an empty list — the user request may "
            "be too vague to extract measurable quantities."
        )

    if not intent.required_dimensions:
        raise IntentValidationError(
            "required_dimensions must contain at least one dimension. "
            "The intent parser returned an empty list — the user request may "
            "be too vague to identify grouping attributes."
        )

    if intent.time_granularity not in _VALID_GRANULARITIES:
        raise IntentValidationError(
            f"time_granularity {intent.time_granularity!r} is not recognised. "
            f"Valid values: {sorted(_VALID_GRANULARITIES)}."
        )
