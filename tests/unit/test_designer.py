"""Unit tests for mart_design.designer using a mocked Anthropic client.

All tests run without network access — the ``anthropic.Anthropic`` client is
replaced with a ``unittest.mock.MagicMock`` that returns pre-built response
objects matching the ``propose_mart`` tool schema.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from intent.schema import UserIntent
from mart_design.designer import _build_user_message, _extract_tool_inputs, propose_mart
from mart_design.schema import MartSpecification
from metadata.schema import SourceColumn, SourceTable


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_intent() -> UserIntent:
    return UserIntent(
        raw_input="Show me monthly sales revenue and order count by customer and product.",
        subject_area="sales",
        required_metrics=["total_revenue", "order_count"],
        required_dimensions=["customer", "product"],
        time_granularity="monthly",
    )


@pytest.fixture()
def sample_tables() -> list[SourceTable]:
    return [
        SourceTable(
            name="orders",
            schema_name="main",
            columns=[
                SourceColumn(name="order_id", data_type="INTEGER", is_primary_key=True, is_nullable=False),
                SourceColumn(name="customer_id", data_type="INTEGER", is_nullable=False),
                SourceColumn(name="product_id", data_type="INTEGER", is_nullable=False),
                SourceColumn(name="order_date", data_type="DATE"),
                SourceColumn(name="total_amount", data_type="DOUBLE"),
            ],
            row_count=1000,
        ),
        SourceTable(
            name="customers",
            schema_name="main",
            columns=[
                SourceColumn(name="customer_id", data_type="INTEGER", is_primary_key=True, is_nullable=False),
                SourceColumn(name="name", data_type="VARCHAR", is_nullable=False),
                SourceColumn(name="region", data_type="VARCHAR"),
            ],
        ),
    ]


@pytest.fixture()
def sample_tool_inputs() -> dict:
    return {
        "mart_name": "sales_mart",
        "description": "Enables monthly sales analysis by customer and product.",
        "fact_tables": [
            {
                "name": "fact_orders",
                "source_tables": ["orders"],
                "metrics": [
                    {
                        "name": "total_revenue",
                        "expression": "SUM(total_amount)",
                        "aggregation": "sum",
                        "source_column": "total_amount",
                        "description": "Total sales revenue.",
                    },
                    {
                        "name": "order_count",
                        "expression": "COUNT(order_id)",
                        "aggregation": "count",
                        "source_column": "order_id",
                        "description": "",
                    },
                ],
                "dimension_keys": ["customer_id", "product_id"],
                "grain": "one row per order",
                "description": "",
            }
        ],
        "dimension_tables": [
            {
                "name": "dim_customer",
                "source_table": "customers",
                "key_column": "customer_id",
                "attribute_columns": ["name", "region"],
                "description": "",
            }
        ],
        "rationale": "Simple star schema centred on the orders table.",
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tool_use_block(inputs: dict) -> SimpleNamespace:
    return SimpleNamespace(type="tool_use", input=inputs)


def _make_text_block(text: str = "ok") -> SimpleNamespace:
    return SimpleNamespace(type="text", text=text)


def _make_response(*blocks, stop_reason: str = "tool_use") -> SimpleNamespace:
    return SimpleNamespace(content=list(blocks), stop_reason=stop_reason)


def _make_mock_client(tool_inputs: dict) -> MagicMock:
    """Return a mock ``anthropic.Anthropic`` client whose ``messages.create``
    returns a fake response containing *tool_inputs* in a tool_use block."""
    mock_client = MagicMock()
    mock_client.messages.create.return_value = _make_response(
        _make_tool_use_block(tool_inputs)
    )
    return mock_client


# ---------------------------------------------------------------------------
# _extract_tool_inputs
# ---------------------------------------------------------------------------


class TestExtractToolInputs:
    def test_returns_inputs_from_tool_use_block(self):
        inputs = {"mart_name": "test_mart"}
        response = _make_response(_make_tool_use_block(inputs))
        assert _extract_tool_inputs(response) == inputs

    def test_skips_text_blocks_before_tool_use(self):
        inputs = {"mart_name": "test_mart"}
        response = _make_response(_make_text_block(), _make_tool_use_block(inputs))
        assert _extract_tool_inputs(response) == inputs

    def test_raises_when_no_tool_use_block(self):
        response = _make_response(_make_text_block("some text"), stop_reason="end_turn")
        with pytest.raises(ValueError, match="tool_use"):
            _extract_tool_inputs(response)


# ---------------------------------------------------------------------------
# _build_user_message
# ---------------------------------------------------------------------------


class TestBuildUserMessage:
    def test_contains_intent_json(self, sample_intent, sample_tables):
        message = _build_user_message(sample_intent, sample_tables)
        assert "sales" in message
        assert "total_revenue" in message

    def test_contains_table_names(self, sample_intent, sample_tables):
        message = _build_user_message(sample_intent, sample_tables)
        assert "orders" in message
        assert "customers" in message

    def test_contains_column_names(self, sample_intent, sample_tables):
        message = _build_user_message(sample_intent, sample_tables)
        assert "total_amount" in message
        assert "customer_id" in message

    def test_marks_primary_keys(self, sample_intent, sample_tables):
        message = _build_user_message(sample_intent, sample_tables)
        assert "PK" in message

    def test_valid_json_intent_block(self, sample_intent, sample_tables):
        message = _build_user_message(sample_intent, sample_tables)
        # Extract the JSON block between the first ```json and ```
        start = message.index("```json\n") + len("```json\n")
        end = message.index("\n```", start)
        parsed = json.loads(message[start:end])
        assert parsed["subject_area"] == "sales"


# ---------------------------------------------------------------------------
# propose_mart
# ---------------------------------------------------------------------------


class TestProposeMart:
    def test_returns_mart_specification(self, sample_intent, sample_tables, sample_tool_inputs):
        result = propose_mart(sample_intent, sample_tables, client=_make_mock_client(sample_tool_inputs))
        assert isinstance(result, MartSpecification)

    def test_intent_is_preserved(self, sample_intent, sample_tables, sample_tool_inputs):
        result = propose_mart(sample_intent, sample_tables, client=_make_mock_client(sample_tool_inputs))
        assert result.intent == sample_intent

    def test_source_tables_are_preserved(self, sample_intent, sample_tables, sample_tool_inputs):
        result = propose_mart(sample_intent, sample_tables, client=_make_mock_client(sample_tool_inputs))
        assert result.source_tables == sample_tables

    def test_mart_name_mapped(self, sample_intent, sample_tables, sample_tool_inputs):
        result = propose_mart(sample_intent, sample_tables, client=_make_mock_client(sample_tool_inputs))
        assert result.mart_name == "sales_mart"

    def test_fact_table_populated(self, sample_intent, sample_tables, sample_tool_inputs):
        result = propose_mart(sample_intent, sample_tables, client=_make_mock_client(sample_tool_inputs))
        assert len(result.fact_tables) == 1
        fact = result.fact_tables[0]
        assert fact.name == "fact_orders"
        assert len(fact.metrics) == 2

    def test_dimension_table_populated(self, sample_intent, sample_tables, sample_tool_inputs):
        result = propose_mart(sample_intent, sample_tables, client=_make_mock_client(sample_tool_inputs))
        assert len(result.dimension_tables) == 1
        dim = result.dimension_tables[0]
        assert dim.name == "dim_customer"
        assert dim.key_column == "customer_id"

    def test_metric_aggregation_enum(self, sample_intent, sample_tables, sample_tool_inputs):
        result = propose_mart(sample_intent, sample_tables, client=_make_mock_client(sample_tool_inputs))
        from mart_design.schema import AggregationType
        revenue_metric = result.fact_tables[0].metrics[0]
        assert revenue_metric.aggregation == AggregationType.sum

    def test_generated_sql_is_empty_by_default(self, sample_intent, sample_tables, sample_tool_inputs):
        result = propose_mart(sample_intent, sample_tables, client=_make_mock_client(sample_tool_inputs))
        assert result.generated_sql == ""

    def test_api_called_once(self, sample_intent, sample_tables, sample_tool_inputs):
        mock_client = _make_mock_client(sample_tool_inputs)
        propose_mart(sample_intent, sample_tables, client=mock_client)
        mock_client.messages.create.assert_called_once()

    def test_tool_choice_forces_propose_mart(self, sample_intent, sample_tables, sample_tool_inputs):
        mock_client = _make_mock_client(sample_tool_inputs)
        propose_mart(sample_intent, sample_tables, client=mock_client)
        call_kwargs = mock_client.messages.create.call_args.kwargs
        assert call_kwargs["tool_choice"] == {"type": "tool", "name": "propose_mart"}
