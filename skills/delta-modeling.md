---
name: databricks-delta-modeling
description: Delta table design, star schema on Delta Lake, fact/dimension tables, SCD strategies, surrogate keys, additivity rules, and anti-patterns
triggers:
  - star schema
  - dimensional modeling
  - fact table
  - dimension
  - SCD
  - slowly changing dimension
  - surrogate key
  - grain
  - additivity
  - snowflake schema
  - data vault
  - delta table design
  - schema design
  - model
  - normalize
  - denormalize
---

# Delta Table Modeling

## Design Process: Business Question First

1. **What business process?** → Orders, Events, Payments, Sessions
2. **What is the grain?** → One row per order / per event / per session per day
3. **What dimensions?** → Customer, Product, Date, Region, Channel
4. **What facts/measures?** → Revenue, Quantity, Duration, Count

## Star Schema vs Snowflake Schema

| Pattern | When to use |
|---------|------------|
| **Star Schema** | OLAP/dashboards, most Databricks analytical workloads |
| **Snowflake Schema** | Large dimension tables with high cardinality hierarchies |
| **Data Vault** | Highly regulated, full audit trail, many source systems |

**Default to Star Schema** for Databricks analytics workloads. Delta Lake's performance characteristics favor denormalized reads.

## Fact Table Design

```sql
-- Transactional Fact Table (one row per business event)
CREATE TABLE catalog.gold.fct_orders (
  -- Surrogate key (for joins with dimension tables)
  order_sk          BIGINT GENERATED ALWAYS AS IDENTITY,
  
  -- Foreign keys to dimensions (surrogate keys)
  customer_sk       BIGINT     NOT NULL,  -- FK to dim_customer
  product_sk        BIGINT     NOT NULL,  -- FK to dim_product
  date_sk           INT        NOT NULL,  -- FK to dim_date (YYYYMMDD integer)
  channel_sk        BIGINT     NOT NULL,  -- FK to dim_channel
  
  -- Natural/business key (for debugging and lineage)
  order_id          STRING     NOT NULL,
  
  -- Facts / measures
  quantity          INT        NOT NULL,
  unit_price        DECIMAL(10,2),
  gross_revenue     DECIMAL(12,2),        -- fully additive
  discount_amount   DECIMAL(10,2),        -- fully additive
  net_revenue       DECIMAL(12,2),        -- fully additive
  
  -- Degenerate dimensions (on the fact, no separate dim table)
  order_status      STRING,
  payment_method    STRING,
  
  -- Metadata
  _source_system    STRING,
  _ingested_at      TIMESTAMP,
  _updated_at       TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
CLUSTER BY (date_sk, customer_sk)  -- cluster on most common filter columns
COMMENT 'Grain: one row per order line item. Owner: data-engineering. SLA: hourly.'
TBLPROPERTIES ('endorsement' = 'certified', 'domain' = 'commerce');
```

## Additivity Rules

| Measure | Additive? | Notes |
|---------|-----------|-------|
| `gross_revenue` | Fully additive | Sum across all dims |
| `order_count` | Fully additive | Count of orders |
| `unit_price` | Non-additive | Average, not sum |
| `inventory_balance` | Semi-additive | Sum by region, average by date |
| `customer_count` | Non-additive (distinct) | Use `COUNT DISTINCT` |
| `margin_pct` | Non-additive | Compute from revenue/cost, not avg |

```sql
-- Correct: compute derived metrics from additive components
SELECT
  date_sk,
  SUM(gross_revenue)                              AS total_gross_revenue,
  SUM(discount_amount)                            AS total_discounts,
  SUM(gross_revenue) - SUM(discount_amount)       AS total_net_revenue,
  SUM(gross_revenue) / SUM(cost)                  AS margin_pct,   -- NOT AVG(margin_pct)
  COUNT(DISTINCT customer_sk)                     AS unique_customers
FROM catalog.gold.fct_orders
GROUP BY date_sk
```

## Dimension Table Design

```sql
-- SCD Type 2 Customer Dimension (full history tracking)
CREATE TABLE catalog.gold.dim_customer (
  customer_sk       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  
  -- Natural key (business identifier)
  customer_id       STRING  NOT NULL,
  
  -- Tracked attributes (create new row when changed)
  customer_name     STRING,
  email             STRING,
  segment           STRING,  -- tracked: changes create new version
  region            STRING,  -- tracked
  
  -- Non-tracked attributes (overwrite in place)
  phone             STRING,
  
  -- SCD Type 2 metadata
  effective_from    DATE    NOT NULL,
  effective_to      DATE,              -- NULL = current record
  is_current        BOOLEAN NOT NULL DEFAULT true,
  _source_system    STRING,
  _updated_at       TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
CLUSTER BY (customer_id)
COMMENT 'SCD Type 2 customer dimension. Full history of segment and region changes.';
```

## SCD Strategies

### SCD Type 1 — Overwrite (no history)

```python
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "catalog.gold.dim_product")
(target.alias("target")
    .merge(source_df.alias("source"), "target.product_id = source.product_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

### SCD Type 2 — Full History

```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Identify changed records
target_df = spark.table("catalog.gold.dim_customer").filter("is_current = true")
source_df = spark.table("catalog.silver.customers")

# Records that have changed tracked columns
changed = (source_df.alias("src")
    .join(target_df.alias("tgt"), "customer_id")
    .filter(
        (F.col("src.segment") != F.col("tgt.segment")) |
        (F.col("src.region") != F.col("tgt.region"))
    )
    .select("src.*")
)

# Step 1: Expire old records
target = DeltaTable.forName(spark, "catalog.gold.dim_customer")
(target.alias("target")
    .merge(changed.alias("source"), "target.customer_id = source.customer_id AND target.is_current = true")
    .whenMatchedUpdate(set={
        "effective_to": F.current_date(),
        "is_current": F.lit(False)
    })
    .execute()
)

# Step 2: Insert new versions
new_versions = changed.withColumn("effective_from", F.current_date()).withColumn("effective_to", F.lit(None)).withColumn("is_current", F.lit(True))
new_versions.write.format("delta").mode("append").saveAsTable("catalog.gold.dim_customer")
```

### SCD Type 6 — Hybrid (current + history)

Keep both `current_*` columns (latest value, always overwritten) and versioned rows:

```sql
ALTER TABLE catalog.gold.dim_customer ADD COLUMNS (
  current_segment STRING,  -- always reflects latest value
  current_region  STRING   -- always reflects latest value
);
```

## Date Dimension

Always use an integer surrogate key `YYYYMMDD` for date dimensions — enables fast integer joins:

```python
from pyspark.sql import functions as F

date_dim = (spark.sql("""
  SELECT SEQUENCE(DATE('2020-01-01'), DATE('2030-12-31'), INTERVAL 1 DAY) AS dates
""").select(F.explode("dates").alias("full_date"))
    .withColumn("date_sk",       F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year",          F.year("full_date"))
    .withColumn("quarter",       F.quarter("full_date"))
    .withColumn("month",         F.month("full_date"))
    .withColumn("month_name",    F.date_format("full_date", "MMMM"))
    .withColumn("week_of_year",  F.weekofyear("full_date"))
    .withColumn("day_of_week",   F.dayofweek("full_date"))
    .withColumn("day_name",      F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend",    F.dayofweek("full_date").isin([1, 7]))
    .withColumn("fiscal_year",   F.when(F.month("full_date") >= 4, F.year("full_date")).otherwise(F.year("full_date") - 1))
)

date_dim.write.format("delta").mode("overwrite").saveAsTable("catalog.gold.dim_date")
```

```bash
# CLI: generate date dimension
databricks-agent sql generate-date-dim --start 2020-01-01 --end 2030-12-31 \
  --target catalog.gold.dim_date --fiscal-year-start April
```

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Many-to-many fact-dimension joins | Incorrect aggregation (fan-out) | Bridge table or flatten at ingest |
| Calculated columns in fact tables | Stale on source change | Compute at query time |
| String keys on facts | Slow joins | Surrogate integer or hash keys |
| No grain documented | Wrong aggregations by consumers | Comment on table: "Grain: one row per X" |
| `SELECT *` joins to wide dims | Reads unnecessary columns | Select only needed dim columns |
| SCD Type 2 without clustering | Slow current-record lookups | Cluster by `customer_id` + filter `is_current` |

## CLI Reference

```bash
databricks-agent sql generate-date-dim --start 2020-01-01 --end 2030-12-31 --target catalog.gold.dim_date
databricks-agent sql validate-schema --table catalog.gold.fct_orders   # Schema quality check
databricks-agent catalog describe --table catalog.gold.fct_orders       # Full metadata
```
