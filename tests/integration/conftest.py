"""Shared fixtures and helpers for integration tests.

Infrastructure used in integration tests:
- Real DuckDB database file (via pytest tmp_path)
- Real validators, generators, FastAPI routing, DTO conversion
- LLM calls (parse_intent, propose_mart) are mocked in each test

``build_spec_from_tables`` is a plain function (not a fixture) so it can be
passed directly as ``side_effect`` to the ``propose_mart`` mock.  It builds a
``MartSpecification`` whose column references are consistent with the fixture
orders table, ensuring ``validate_mart_spec`` always passes.
"""
from __future__ import annotations

import duckdb
import pytest

from intent.schema import UserIntent
from mart_design.schema import (
    AggregationType,
    DimensionDefinition,
    FactDefinition,
    MartSpecification,
    MetricDefinition,
)
from metadata.schema import SourceTable


@pytest.fixture()
def duckdb_path(tmp_path) -> str:
    """Create a temporary DuckDB .db file with a single orders table.

    The table schema is:
        orders(order_id INTEGER PK, customer_id INTEGER NOT NULL, amount DOUBLE)

    Returns the absolute file path so it can be passed directly to
    ``DuckDBSchemaReader(database_path)``.
    """
    db_path = str(tmp_path / "test.db")
    conn = duckdb.connect(db_path)
    conn.execute(
        """
        CREATE TABLE orders (
            order_id    INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            amount      DOUBLE
        )
        """
    )
    conn.execute("INSERT INTO orders VALUES (1, 101, 500.0), (2, 102, 750.0)")
    conn.close()
    return db_path


@pytest.fixture()
def fixture_intent() -> UserIntent:
    """A valid UserIntent for use as the mock return value of parse_intent."""
    return UserIntent(
        raw_input="Show me monthly sales by customer.",
        subject_area="sales",
        required_metrics=["total_revenue"],
        required_dimensions=["customer"],
        time_granularity="monthly",
    )


def build_spec_from_tables(
    intent: UserIntent,
    source_tables: list[SourceTable],
    client=None,
) -> MartSpecification:
    """Build a MartSpecification that is guaranteed to pass validate_mart_spec.

    Column references (customer_id, amount) exactly match the fixture orders
    table.  The ``client`` parameter is accepted but ignored so this function
    can be used as ``side_effect`` for the ``propose_mart`` mock, which receives
    ``(intent, source_tables, client=client)`` from the service layer.
    """
    return MartSpecification(
        mart_name="sales_mart",
        description="Monthly sales analysis by customer.",
        rationale="Kimball star schema built on the orders table.",
        intent=intent,
        source_tables=source_tables,
        fact_tables=[
            FactDefinition(
                name="fact_orders",
                source_tables=["orders"],
                metrics=[
                    MetricDefinition(
                        name="total_revenue",
                        expression="SUM(amount)",
                        aggregation=AggregationType.sum,
                        source_column="amount",
                        description="Total sales revenue.",
                    )
                ],
                dimension_keys=["customer_id"],
                grain="one row per order",
                description="Orders fact table.",
            )
        ],
        dimension_tables=[
            DimensionDefinition(
                name="dim_customer",
                source_table="orders",
                key_column="customer_id",
                attribute_columns=[],
                description="Customer dimension.",
            )
        ],
    )
