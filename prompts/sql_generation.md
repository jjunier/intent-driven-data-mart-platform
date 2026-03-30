# SQL Generation — DDL Conventions

## Purpose

Documents the DDL conventions applied by `mart_design/sql_generator.py`
when converting a `MartSpecification` into `CREATE TABLE` statements.
No LLM is involved — this is a deterministic transformation.

## Output order

Dimension tables are always emitted **before** fact tables.  This allows the
full DDL block to be executed top-to-bottom without forward-reference errors.

## Dimension table DDL

```sql
CREATE TABLE {dim.name} (
    {key_column}       {source_type}  PRIMARY KEY,
    {attribute_col_1}  {source_type}  [NOT NULL],
    ...
);
```

- `key_column` type is taken directly from `SourceColumn.data_type`; falls back to `BIGINT`.
- `attribute_columns` types are taken from `SourceColumn.data_type`; fall back to `VARCHAR`.
- `NOT NULL` is added when `SourceColumn.is_nullable == False`.

## Fact table DDL

```sql
CREATE TABLE {fact.name} (
    {dim_key_1}   {source_type}  NOT NULL,
    ...
    {metric_1}    {inferred_type} NOT NULL,
    ...
    FOREIGN KEY ({dim_key_1}) REFERENCES {dim_name}({dim_key_1}),
    ...
);
```

- Dimension FK columns are listed first, then metric columns, then FK constraints.
- All dimension keys and metric columns are `NOT NULL`.

## Metric type inference

| Aggregation | SQL type |
|-------------|----------|
| `sum` | `DOUBLE` |
| `count` | `BIGINT` |
| `count_distinct` | `BIGINT` |
| `avg` | `DOUBLE` |
| `min` | source column type (falls back to `DOUBLE`) |
| `max` | source column type (falls back to `DOUBLE`) |

## Example output

Given a `MartSpecification` with one dimension and one fact table:

```sql
CREATE TABLE dim_customer (
    customer_id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    region VARCHAR
);

CREATE TABLE fact_orders (
    customer_id INTEGER NOT NULL,
    total_revenue DOUBLE NOT NULL,
    order_count BIGINT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id)
);
```

## What is NOT generated

- Surrogate key columns (`id BIGINT GENERATED ALWAYS AS IDENTITY`) — out of MVP scope.
- `INSERT INTO` / `CREATE VIEW` transformation logic — handled separately.
- Index definitions — left to the deployment layer.
