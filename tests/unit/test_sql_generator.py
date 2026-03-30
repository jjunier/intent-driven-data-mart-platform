"""Unit tests for mart_design.sql_generator.

All tests are fully deterministic — no mocking required since
sql_generator contains no LLM or I/O calls.
"""

from __future__ import annotations

import pytest

from intent.schema import UserIntent
from mart_design.schema import (
    AggregationType,
    DimensionDefinition,
    FactDefinition,
    MartSpecification,
    MetricDefinition,
)
from mart_design.sql_generator import (
    _build_column_lookup,
    _build_dim_key_types,
    _infer_metric_type,
    generate_sql,
)
from metadata.schema import SourceColumn, SourceTable


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def source_tables() -> list[SourceTable]:
    return [
        SourceTable(
            name="orders",
            schema_name="main",
            columns=[
                SourceColumn(name="order_id", data_type="INTEGER", is_primary_key=True, is_nullable=False),
                SourceColumn(name="customer_id", data_type="INTEGER", is_nullable=False),
                SourceColumn(name="order_date", data_type="DATE"),
                SourceColumn(name="total_amount", data_type="DOUBLE"),
            ],
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
def dim_customer() -> DimensionDefinition:
    return DimensionDefinition(
        name="dim_customer",
        source_table="customers",
        key_column="customer_id",
        attribute_columns=["name", "region"],
    )


@pytest.fixture()
def fact_orders(dim_customer) -> FactDefinition:
    return FactDefinition(
        name="fact_orders",
        source_tables=["orders"],
        metrics=[
            MetricDefinition(
                name="total_revenue",
                expression="SUM(total_amount)",
                aggregation=AggregationType.sum,
                source_column="total_amount",
            ),
            MetricDefinition(
                name="order_count",
                expression="COUNT(order_id)",
                aggregation=AggregationType.count,
                source_column="order_id",
            ),
        ],
        dimension_keys=["customer_id"],
        grain="one row per order",
    )


@pytest.fixture()
def spec(dim_customer, fact_orders, source_tables) -> MartSpecification:
    intent = UserIntent(
        raw_input="Show me sales by customer.",
        subject_area="sales",
        required_metrics=["total_revenue", "order_count"],
        required_dimensions=["customer"],
    )
    return MartSpecification(
        mart_name="sales_mart",
        description="Sales analysis by customer.",
        intent=intent,
        source_tables=source_tables,
        fact_tables=[fact_orders],
        dimension_tables=[dim_customer],
    )


# ---------------------------------------------------------------------------
# _build_column_lookup
# ---------------------------------------------------------------------------


class TestBuildColumnLookup:
    def test_keys_are_table_names(self, source_tables):
        lookup = _build_column_lookup(source_tables)
        assert set(lookup.keys()) == {"orders", "customers"}

    def test_nested_keys_are_column_names(self, source_tables):
        lookup = _build_column_lookup(source_tables)
        assert "customer_id" in lookup["orders"]
        assert "name" in lookup["customers"]

    def test_values_are_source_column_objects(self, source_tables):
        lookup = _build_column_lookup(source_tables)
        col = lookup["orders"]["total_amount"]
        assert isinstance(col, SourceColumn)
        assert col.data_type == "DOUBLE"


# ---------------------------------------------------------------------------
# _build_dim_key_types
# ---------------------------------------------------------------------------


class TestBuildDimKeyTypes:
    def test_resolves_type_from_source(self, dim_customer, source_tables):
        lookup = _build_column_lookup(source_tables)
        types = _build_dim_key_types([dim_customer], lookup)
        assert types["customer_id"] == "INTEGER"

    def test_falls_back_to_bigint_when_source_missing(self):
        dim = DimensionDefinition(
            name="dim_unknown",
            source_table="nonexistent",
            key_column="id",
            attribute_columns=[],
        )
        types = _build_dim_key_types([dim], {})
        assert types["id"] == "BIGINT"


# ---------------------------------------------------------------------------
# _infer_metric_type
# ---------------------------------------------------------------------------


class TestInferMetricType:
    def _make_metric(self, aggregation: AggregationType, source_column: str = "amount"):
        return MetricDefinition(
            name="m",
            expression="expr",
            aggregation=aggregation,
            source_column=source_column,
        )

    def test_sum_returns_double(self, source_tables):
        lookup = _build_column_lookup(source_tables)
        m = self._make_metric(AggregationType.sum, "total_amount")
        assert _infer_metric_type(m, lookup, ["orders"]) == "DOUBLE"

    def test_count_returns_bigint(self, source_tables):
        lookup = _build_column_lookup(source_tables)
        m = self._make_metric(AggregationType.count, "order_id")
        assert _infer_metric_type(m, lookup, ["orders"]) == "BIGINT"

    def test_count_distinct_returns_bigint(self, source_tables):
        lookup = _build_column_lookup(source_tables)
        m = self._make_metric(AggregationType.count_distinct, "customer_id")
        assert _infer_metric_type(m, lookup, ["orders"]) == "BIGINT"

    def test_avg_returns_double(self, source_tables):
        lookup = _build_column_lookup(source_tables)
        m = self._make_metric(AggregationType.avg, "total_amount")
        assert _infer_metric_type(m, lookup, ["orders"]) == "DOUBLE"

    def test_min_uses_source_column_type(self, source_tables):
        lookup = _build_column_lookup(source_tables)
        m = self._make_metric(AggregationType.min, "order_date")
        assert _infer_metric_type(m, lookup, ["orders"]) == "DATE"

    def test_max_uses_source_column_type(self, source_tables):
        lookup = _build_column_lookup(source_tables)
        m = self._make_metric(AggregationType.max, "total_amount")
        assert _infer_metric_type(m, lookup, ["orders"]) == "DOUBLE"

    def test_min_falls_back_when_column_not_found(self):
        m = self._make_metric(AggregationType.min, "missing_col")
        assert _infer_metric_type(m, {}, ["orders"]) == "DOUBLE"


# ---------------------------------------------------------------------------
# generate_sql — structure
# ---------------------------------------------------------------------------


class TestGenerateSqlStructure:
    def test_returns_non_empty_string(self, spec):
        assert generate_sql(spec) != ""

    def test_contains_dimension_table_name(self, spec):
        sql = generate_sql(spec)
        assert "dim_customer" in sql

    def test_contains_fact_table_name(self, spec):
        sql = generate_sql(spec)
        assert "fact_orders" in sql

    def test_dimension_table_appears_before_fact_table(self, spec):
        sql = generate_sql(spec)
        assert sql.index("dim_customer") < sql.index("fact_orders")

    def test_tables_separated_by_blank_line(self, spec):
        sql = generate_sql(spec)
        assert "\n\n" in sql


# ---------------------------------------------------------------------------
# generate_sql — dimension DDL
# ---------------------------------------------------------------------------


class TestGenerateSqlDimension:
    def test_primary_key_on_key_column(self, spec):
        sql = generate_sql(spec)
        assert "customer_id INTEGER PRIMARY KEY" in sql

    def test_attribute_column_type_resolved(self, spec):
        sql = generate_sql(spec)
        assert "name VARCHAR" in sql

    def test_not_null_on_non_nullable_attribute(self, spec):
        sql = generate_sql(spec)
        assert "name VARCHAR NOT NULL" in sql

    def test_nullable_attribute_has_no_not_null(self, spec):
        sql = generate_sql(spec)
        # region is nullable — should not have NOT NULL
        assert "region VARCHAR NOT NULL" not in sql
        assert "region VARCHAR" in sql

    def test_dimension_ddl_starts_with_create_table(self, spec):
        sql = generate_sql(spec)
        assert "CREATE TABLE dim_customer" in sql


# ---------------------------------------------------------------------------
# generate_sql — fact DDL
# ---------------------------------------------------------------------------


class TestGenerateSqlFact:
    def test_dimension_key_column_present(self, spec):
        sql = generate_sql(spec)
        assert "customer_id INTEGER NOT NULL" in sql

    def test_sum_metric_is_double(self, spec):
        sql = generate_sql(spec)
        assert "total_revenue DOUBLE NOT NULL" in sql

    def test_count_metric_is_bigint(self, spec):
        sql = generate_sql(spec)
        assert "order_count BIGINT NOT NULL" in sql

    def test_foreign_key_constraint_present(self, spec):
        sql = generate_sql(spec)
        assert "FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id)" in sql

    def test_fact_ddl_starts_with_create_table(self, spec):
        sql = generate_sql(spec)
        assert "CREATE TABLE fact_orders" in sql


# ---------------------------------------------------------------------------
# generate_sql — multiple dimensions
# ---------------------------------------------------------------------------


class TestGenerateSqlMultipleDimensions:
    def test_all_dimension_tables_present(self, source_tables, fact_orders):
        dim_product = DimensionDefinition(
            name="dim_product",
            source_table="orders",
            key_column="order_id",
            attribute_columns=["order_date"],
        )
        intent = UserIntent(
            raw_input="x",
            subject_area="sales",
            required_metrics=["total_revenue"],
            required_dimensions=["customer", "product"],
        )
        spec = MartSpecification(
            mart_name="sales_mart",
            description="desc",
            intent=intent,
            source_tables=source_tables,
            fact_tables=[fact_orders],
            dimension_tables=[
                DimensionDefinition(
                    name="dim_customer",
                    source_table="customers",
                    key_column="customer_id",
                    attribute_columns=["name"],
                ),
                dim_product,
            ],
        )
        sql = generate_sql(spec)
        assert "dim_customer" in sql
        assert "dim_product" in sql

    def test_fact_references_appear_for_matching_keys_only(self, source_tables):
        """FK constraint is only added for dim keys that appear in dimension_keys."""
        dim_a = DimensionDefinition(
            name="dim_customer",
            source_table="customers",
            key_column="customer_id",
            attribute_columns=["name"],
        )
        dim_b = DimensionDefinition(
            name="dim_other",
            source_table="customers",
            key_column="name",
            attribute_columns=[],
        )
        fact = FactDefinition(
            name="fact_orders",
            source_tables=["orders"],
            metrics=[
                MetricDefinition(
                    name="order_count",
                    expression="COUNT(*)",
                    aggregation=AggregationType.count,
                    source_column="order_id",
                )
            ],
            dimension_keys=["customer_id"],  # only customer_id, not name
            grain="one row per order",
        )
        intent = UserIntent(
            raw_input="x",
            subject_area="sales",
            required_metrics=["order_count"],
            required_dimensions=["customer"],
        )
        spec = MartSpecification(
            mart_name="m",
            description="d",
            intent=intent,
            source_tables=source_tables,
            fact_tables=[fact],
            dimension_tables=[dim_a, dim_b],
        )
        sql = generate_sql(spec)
        assert "REFERENCES dim_customer" in sql
        assert "REFERENCES dim_other" not in sql
