"""Unit tests for intent.validator."""

from __future__ import annotations

import pytest

from intent.schema import UserIntent
from intent.validator import IntentValidationError, validate_intent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_intent(**overrides) -> UserIntent:
    """Return a valid UserIntent, overriding selected fields."""
    defaults = {
        "raw_input": "Show me monthly revenue by region.",
        "subject_area": "sales",
        "required_metrics": ["total_revenue"],
        "required_dimensions": ["region"],
        "time_granularity": "monthly",
    }
    defaults.update(overrides)
    return UserIntent(**defaults)


# ---------------------------------------------------------------------------
# Valid intent — should not raise
# ---------------------------------------------------------------------------


class TestValidateIntentAcceptsValid:
    def test_fully_populated_intent_passes(self):
        validate_intent(_make_intent())  # must not raise

    def test_all_valid_granularities_pass(self):
        for granularity in ("daily", "weekly", "monthly", "quarterly", "yearly"):
            validate_intent(_make_intent(time_granularity=granularity))

    def test_multiple_metrics_and_dimensions_pass(self):
        intent = _make_intent(
            required_metrics=["revenue", "order_count"],
            required_dimensions=["region", "product", "customer"],
        )
        validate_intent(intent)  # must not raise


# ---------------------------------------------------------------------------
# required_metrics
# ---------------------------------------------------------------------------


class TestValidateIntentRequiredMetrics:
    def test_empty_metrics_raises(self):
        intent = _make_intent(required_metrics=[])
        with pytest.raises(IntentValidationError, match="required_metrics"):
            validate_intent(intent)

    def test_error_is_value_error_subclass(self):
        intent = _make_intent(required_metrics=[])
        with pytest.raises(ValueError):
            validate_intent(intent)


# ---------------------------------------------------------------------------
# required_dimensions
# ---------------------------------------------------------------------------


class TestValidateIntentRequiredDimensions:
    def test_empty_dimensions_raises(self):
        intent = _make_intent(required_dimensions=[])
        with pytest.raises(IntentValidationError, match="required_dimensions"):
            validate_intent(intent)


# ---------------------------------------------------------------------------
# time_granularity
# ---------------------------------------------------------------------------


class TestValidateIntentTimeGranularity:
    def test_unknown_granularity_raises(self):
        intent = _make_intent(time_granularity="hourly")
        with pytest.raises(IntentValidationError, match="time_granularity"):
            validate_intent(intent)

    def test_error_message_includes_bad_value(self):
        intent = _make_intent(time_granularity="annually")
        with pytest.raises(IntentValidationError, match="annually"):
            validate_intent(intent)
