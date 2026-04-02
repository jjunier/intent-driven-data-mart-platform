"""Unit tests for intent.parser using a mocked Anthropic client.

All tests run without network access — the ``anthropic.Anthropic`` client is
replaced with a ``unittest.mock.MagicMock`` that returns pre-built response
objects.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from intent.parser import _extract_tool_inputs, parse_intent
from intent.schema import UserIntent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tool_use_block(inputs: dict) -> SimpleNamespace:
    """Build a fake tool_use content block."""
    return SimpleNamespace(type="tool_use", input=inputs)


def _make_text_block(text: str = "ok") -> SimpleNamespace:
    """Build a fake text content block."""
    return SimpleNamespace(type="text", text=text)


def _make_response(*blocks, stop_reason: str = "tool_use") -> SimpleNamespace:
    """Build a fake anthropic.types.Message-like object."""
    return SimpleNamespace(content=list(blocks), stop_reason=stop_reason)


# ---------------------------------------------------------------------------
# _extract_tool_inputs
# ---------------------------------------------------------------------------


class TestExtractToolInputs:
    def test_returns_inputs_from_tool_use_block(self):
        expected = {"subject_area": "sales", "required_metrics": ["revenue"]}
        response = _make_response(_make_tool_use_block(expected))
        assert _extract_tool_inputs(response) == expected

    def test_skips_text_blocks_before_tool_use(self):
        inputs = {"subject_area": "hr", "required_metrics": ["headcount"]}
        response = _make_response(_make_text_block(), _make_tool_use_block(inputs))
        assert _extract_tool_inputs(response) == inputs

    def test_raises_when_no_tool_use_block(self):
        response = _make_response(_make_text_block("some text"), stop_reason="end_turn")
        with pytest.raises(ValueError, match="tool_use"):
            _extract_tool_inputs(response)


# ---------------------------------------------------------------------------
# parse_intent
# ---------------------------------------------------------------------------


_FULL_INPUTS = {
    "subject_area": "sales",
    "required_metrics": ["total_revenue", "order_count"],
    "required_dimensions": ["product_category", "region"],
    "filters": {"channel": "online"},
    "time_granularity": "monthly",
    "notes": "Online channel only.",
}

_MINIMAL_INPUTS = {
    "subject_area": "inventory",
    "required_metrics": ["stock_level"],
    "required_dimensions": ["warehouse"],
}


def _make_mock_client(tool_inputs: dict) -> MagicMock:
    """Return a mock ``anthropic.Anthropic`` client whose ``messages.create``
    returns a fake response containing *tool_inputs* in a tool_use block."""
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _make_response(
        _make_tool_use_block(tool_inputs)
    )
    return mock_client


class TestParseIntent:
    def test_returns_user_intent_instance(self):
        result = parse_intent(
            "Analyze monthly online sales by category and region.",
            client=_make_mock_client(_FULL_INPUTS),
        )
        assert isinstance(result, UserIntent)

    def test_raw_input_preserved(self):
        raw = "Show me inventory stock by warehouse."
        result = parse_intent(raw, client=_make_mock_client(_MINIMAL_INPUTS))
        assert result.raw_input == raw

    def test_full_fields_mapped_correctly(self):
        result = parse_intent("some request", client=_make_mock_client(_FULL_INPUTS))
        assert result.subject_area == "sales"
        assert result.required_metrics == ["total_revenue", "order_count"]
        assert result.required_dimensions == ["product_category", "region"]
        assert result.filters == {"channel": "online"}
        assert result.time_granularity == "monthly"
        assert result.notes == "Online channel only."

    def test_optional_fields_use_defaults_when_absent(self):
        result = parse_intent("some request", client=_make_mock_client(_MINIMAL_INPUTS))
        assert result.filters == {}
        assert result.time_granularity == "daily"
        assert result.notes == ""

    def test_api_called_once(self):
        mock_client = _make_mock_client(_FULL_INPUTS)
        parse_intent("any input", client=mock_client)
        mock_client.messages.create.assert_called_once()

    def test_tool_choice_forces_extract_intent(self):
        mock_client = _make_mock_client(_FULL_INPUTS)
        parse_intent("any input", client=mock_client)
        call_kwargs = mock_client.messages.create.call_args.kwargs
        assert call_kwargs["tool_choice"] == {"type": "tool", "name": "extract_intent"}
