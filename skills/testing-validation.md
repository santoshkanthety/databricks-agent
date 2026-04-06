---
name: databricks-testing-validation
description: Data testing patterns for Databricks - DLT expectations, Delta constraints, Great Expectations, dbt tests, Spark unit tests, reconciliation, UAT
triggers:
  - testing
  - unit test
  - data quality
  - validate
  - assertion
  - UAT
  - reconciliation
  - Great Expectations
  - dbt test
  - DLT expectations
  - Delta constraints
  - pytest
  - data validation
  - quality checks
  - row count
  - null check
---

# Testing & Validation in Databricks

## Four Testing Levels

| Level | What | When |
|-------|------|------|
| 1. Source data tests | Profile raw data quality | Before Bronze write |
| 2. Pipeline tests | Reconcile row counts, sums | After each layer write |
| 3. Semantic model tests | Query accuracy, metric assertions | After Gold write |
| 4. UAT / Report tests | Business stakeholder sign-off | Pre go-live |

## Level 1: DLT Expectations (Inline Quality Gates)

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(name="silver_orders")
@dlt.expect_or_drop("non_null_order_id",   "order_id IS NOT NULL")       # drop invalid rows
@dlt.expect_or_drop("non_null_customer",    "customer_id IS NOT NULL")
@dlt.expect_or_drop("positive_revenue",    "revenue >= 0")
@dlt.expect_or_fail("no_future_dates",     "order_date <= current_date()")  # fail entire batch
@dlt.expect("valid_status",                                                   # warn only
    "order_status IN ('pending','confirmed','shipped','delivered','cancelled')")
def silver_orders():
    return dlt.read_stream("bronze_orders").withColumn(...)
```

**Expectation modes:**
- `@dlt.expect` — log warning, keep row
- `@dlt.expect_or_drop` — drop invalid rows, continue pipeline
- `@dlt.expect_or_fail` — halt entire pipeline batch on violation

## Level 1: Delta Constraints (Table-Level)

```sql
-- Add CHECK constraints to Delta tables
ALTER TABLE catalog.silver.orders
  ADD CONSTRAINT positive_revenue CHECK (revenue >= 0);

ALTER TABLE catalog.silver.orders
  ADD CONSTRAINT valid_status CHECK (order_status IN ('pending','confirmed','shipped','delivered','cancelled'));

-- List constraints
SHOW TBLPROPERTIES catalog.silver.orders;

-- Remove a constraint
ALTER TABLE catalog.silver.orders DROP CONSTRAINT positive_revenue;
```

## Level 1: Spark DataFrame Assertions

```python
from pyspark.sql import functions as F

def assert_no_nulls(df, columns: list[str], table: str = ""):
    for col in columns:
        count = df.filter(F.col(col).isNull()).count()
        assert count == 0, f"[{table}] Column '{col}' has {count} null values"

def assert_unique(df, columns: list[str], table: str = ""):
    total    = df.count()
    distinct = df.select(columns).distinct().count()
    assert total == distinct, f"[{table}] {total - distinct} duplicate rows on {columns}"

def assert_row_count_between(df, min_rows: int, max_rows: int, table: str = ""):
    count = df.count()
    assert min_rows <= count <= max_rows, \
        f"[{table}] Row count {count} not in [{min_rows}, {max_rows}]"

def assert_values_in(df, col: str, allowed: list, table: str = ""):
    invalid = df.filter(~F.col(col).isin(allowed)).count()
    assert invalid == 0, f"[{table}] Column '{col}' has {invalid} values outside {allowed}"

# Usage
df_silver = spark.table("catalog.silver.orders")
assert_no_nulls(df_silver, ["order_id", "customer_id", "revenue"], "silver.orders")
assert_unique(df_silver, ["order_id"], "silver.orders")
assert_values_in(df_silver, "order_status", ["pending","confirmed","shipped","delivered","cancelled"])
```

## Level 2: Pipeline Reconciliation Tests

```python
def reconcile(source_table: str, target_table: str, 
              key_col: str, measure_col: str, tolerance_pct: float = 0.001):
    """Validate source-to-target row count and sum reconciliation."""
    src = spark.sql(f"SELECT COUNT(*) AS cnt, SUM({measure_col}) AS total FROM {source_table}")
    tgt = spark.sql(f"SELECT COUNT(*) AS cnt, SUM({measure_col}) AS total FROM {target_table}")
    
    src_row = src.collect()[0]
    tgt_row = tgt.collect()[0]
    
    row_diff = abs(src_row["cnt"] - tgt_row["cnt"])
    sum_diff = abs((src_row["total"] or 0) - (tgt_row["total"] or 0))
    sum_tolerance = (src_row["total"] or 0) * tolerance_pct
    
    assert row_diff == 0, \
        f"Row count mismatch: {source_table}={src_row['cnt']}, {target_table}={tgt_row['cnt']}"
    assert sum_diff <= sum_tolerance, \
        f"Sum mismatch on {measure_col}: {source_table}={src_row['total']:.2f}, " \
        f"{target_table}={tgt_row['total']:.2f} (diff={sum_diff:.2f})"
    
    print(f"✓ Reconciled {source_table} → {target_table}: {tgt_row['cnt']:,} rows, ${tgt_row['total']:,.2f}")

# Usage
reconcile("catalog.bronze.orders", "catalog.silver.orders", "order_id", "revenue")
reconcile("catalog.silver.orders", "catalog.gold.fct_orders", "order_id", "gross_revenue")
```

## Level 3: Semantic Model / Metric Tests

```python
def assert_metric(query: str, expected: float, tolerance_pct: float = 0.01, label: str = ""):
    """Assert that a metric query returns an expected value (within tolerance)."""
    actual = spark.sql(query).collect()[0][0]
    diff_pct = abs((actual - expected) / expected) if expected != 0 else abs(actual)
    assert diff_pct <= tolerance_pct, \
        f"[{label}] Expected {expected:.4f}, got {actual:.4f} (diff={diff_pct:.2%})"
    print(f"✓ {label}: {actual:.4f}")

# Run against known-good benchmark values
assert_metric(
    "SELECT SUM(gross_revenue) FROM catalog.gold.fct_orders WHERE date_sk = 20250101",
    expected=1_234_567.89,
    label="2025-01-01 total revenue"
)

# Test YTD metric
assert_metric(
    "SELECT SUM(gross_revenue) FROM catalog.gold.fct_orders WHERE year = 2024",
    expected=14_893_012.00,
    tolerance_pct=0.005,
    label="FY2024 total revenue"
)
```

## Level 3: dbt Tests (Semantic Layer)

```yaml
# models/silver/schema.yml
models:
  - name: silver_orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
      - name: order_status
        tests:
          - not_null
          - accepted_values:
              values: ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']
      - name: revenue
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0

  - name: gold_daily_revenue
    tests:
      - dbt_utils.equal_rowcount:
          compare_model: ref('silver_orders_agg')  # reconcile aggregation
```

```bash
# Run dbt tests
dbt test --select silver_orders gold_daily_revenue

# Run with coverage report
dbt test --store-failures
```

## Level 4: UAT Checklist

**Data Accuracy:**
- [ ] Row counts match source system (within agreed tolerance)
- [ ] Sum of key measures matches source within 0.1%
- [ ] Date ranges are complete (no gaps in daily data)
- [ ] No unexpected nulls in critical columns

**Business Logic:**
- [ ] KPIs match manually calculated values from finance/ops
- [ ] YoY / MoM comparisons are correct
- [ ] Filters (date, region, product) work correctly
- [ ] All metrics exclude cancelled/test orders as expected

**Performance:**
- [ ] Dashboard loads in < 5 seconds for default filter
- [ ] Heavy filters (YTD, global) load in < 15 seconds
- [ ] Pipeline completes within SLA window

**Security:**
- [ ] User A can only see their region's data (row filter test)
- [ ] PII columns are masked for non-privileged users
- [ ] Export disabled for restricted datasets

## Running Tests via CLI

```bash
# Run data quality checks on a table
databricks-agent sql validate --table catalog.silver.orders \
  --checks not-null:order_id,customer_id --checks unique:order_id

# Run reconciliation between layers
databricks-agent sql reconcile --source catalog.bronze.orders \
  --target catalog.silver.orders --measure revenue

# Run full test suite
databricks-agent sql test --config tests/quality.yaml
```

## Automated Quality Dashboard

```sql
-- Query DLT event log for expectation pass/fail rates
SELECT
  expectation_name,
  SUM(passed_records)  AS passed,
  SUM(failed_records)  AS failed,
  SUM(passed_records) / (SUM(passed_records) + SUM(failed_records)) AS pass_rate
FROM catalog.ops.dlt_event_log
WHERE pipeline_name = 'orders-pipeline'
  AND event_date >= DATEADD(DAY, -7, CURRENT_DATE())
GROUP BY 1
ORDER BY pass_rate ASC
```

## CLI Reference

```bash
databricks-agent sql validate --table catalog.silver.orders --checks not-null:order_id,unique:order_id
databricks-agent sql reconcile --source bronze.orders --target silver.orders --measure revenue
databricks-agent sql test --config tests/quality.yaml
databricks-agent pipelines events --name orders-pipeline --filter expectations  # DLT quality events
```
