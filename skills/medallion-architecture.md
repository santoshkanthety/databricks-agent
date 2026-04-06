---
name: databricks-medallion-architecture
description: Bronze/Silver/Gold medallion architecture on Databricks Delta Lake - layer design, naming conventions, Delta optimizations, V-Order, Liquid Clustering
triggers:
  - medallion
  - bronze
  - silver
  - gold
  - lakehouse
  - delta lake
  - layer design
  - data architecture
  - delta table
  - liquid clustering
  - v-order
  - z-order
  - optimize
  - vacuum
  - time travel
---

# Medallion Architecture on Databricks Delta Lake

## Layer Principles

| Layer | Purpose | Rule |
|-------|---------|------|
| **Bronze** | Raw ingest — preserve replay | No transformations. Append-only. Never filter. |
| **Silver** | Cleaned, conformed, validated | Quality gates applied. No business logic. |
| **Gold** | Business-ready, aggregated | Aggregations, metrics, star schemas. Read-optimized. |

```
/catalog
├── bronze/     ← raw data, source-aligned tables
├── silver/     ← clean, conformed, deduplicated
├── gold/       ← business aggregations, star schemas, ML features
└── ops/        ← pipeline logs, audit tables, control tables
```

## Bronze Layer Rules

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="bronze_orders",
    comment="Raw orders from landing zone. Append-only. No transformations applied.",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_orders():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", "/mnt/checkpoints/orders_schema")
            .load("/mnt/landing/orders/")
            # ONLY metadata columns added — raw data untouched
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_batch_id", F.lit(spark.conf.get("spark.databricks.job.runId", "manual")))
    )
```

**Bronze anti-patterns:**
- Filtering rows — breaks replay
- Parsing/transforming data — do this in Silver
- Deduplication — preserve raw history
- Schema enforcement — use `schemaEvolutionMode=addNewColumns`

## Silver Layer: Quality Gates

```python
@dlt.table(name="silver_orders")
@dlt.expect_or_drop("non_null_order_id",  "order_id IS NOT NULL")
@dlt.expect_or_drop("non_null_customer",   "customer_id IS NOT NULL")
@dlt.expect_or_drop("positive_revenue",   "revenue >= 0")
@dlt.expect("valid_status", "order_status IN ('pending','confirmed','shipped','delivered','cancelled')")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
            .withColumn("order_date",  F.to_date("order_date_raw", "yyyy-MM-dd"))
            .withColumn("revenue",     F.col("revenue_raw").cast("double"))
            .withColumn("customer_id", F.trim(F.lower("customer_id_raw")))
            .withColumn("_processed_at", F.current_timestamp())
            .drop("order_date_raw", "revenue_raw", "customer_id_raw")  # remove raw cols
    )
```

## Gold Layer: Business Ready

```python
@dlt.table(
    name="gold_revenue_by_region",
    comment="Daily revenue by region. Owner: analytics@company.com. Grain: region + date.",
    table_properties={"endorsement": "certified"}
)
def gold_revenue_by_region():
    return (
        dlt.read("silver_orders")
            .filter(F.col("order_status") != "cancelled")
            .groupBy("order_date", "region")
            .agg(
                F.sum("revenue").alias("gross_revenue"),
                F.count("order_id").alias("order_count"),
                F.countDistinct("customer_id").alias("unique_customers")
            )
    )
```

## Delta Table Optimizations

### OPTIMIZE + Z-ORDER (file compaction + co-location)

```sql
-- Compact small files + co-locate data for common filter columns
OPTIMIZE catalog.silver.orders ZORDER BY (customer_id, order_date);

-- Run after large appends or weekly on active tables
```

```bash
databricks-agent catalog optimize --table catalog.silver.orders --zorder customer_id,order_date
```

### Liquid Clustering (replaces Z-ORDER for new tables)

```sql
-- Enable on table creation (Databricks Runtime 13.3+)
CREATE TABLE catalog.silver.orders
CLUSTER BY (customer_id, order_date)
AS SELECT * FROM bronze_orders;

-- Trigger clustering run
OPTIMIZE catalog.silver.orders;
```

Liquid Clustering advantages over Z-ORDER:
- Incremental — only clusters new/modified files
- Supports up to 4 cluster columns
- No need to re-cluster entire table when columns change

### V-ORDER (write-time optimization for read speed)

```sql
-- V-Order is enabled by default in Databricks Runtime 14.0+
-- Force-enable for older runtimes
ALTER TABLE catalog.gold.revenue_summary SET TBLPROPERTIES ('delta.parquet.vorder.enabled' = 'true');
```

### Auto-Optimize (write-time compaction)

```sql
-- Enable on existing tables
ALTER TABLE catalog.bronze.raw_events
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

## VACUUM (Remove Old Files)

```sql
-- Default retention: 7 days. Do NOT reduce below 7 days if using time travel.
VACUUM catalog.silver.orders RETAIN 168 HOURS;  -- 7 days

-- Check what would be deleted first
VACUUM catalog.silver.orders RETAIN 168 HOURS DRY RUN;
```

```bash
databricks-agent catalog vacuum --table catalog.silver.orders --hours 168
```

## Time Travel

```sql
-- Query as of a timestamp
SELECT * FROM catalog.silver.orders TIMESTAMP AS OF '2025-01-01 00:00:00';

-- Query by version number
SELECT * FROM catalog.silver.orders VERSION AS OF 42;

-- Restore to a previous state
RESTORE TABLE catalog.silver.orders TO TIMESTAMP AS OF '2025-01-01 00:00:00';
RESTORE TABLE catalog.silver.orders TO VERSION AS OF 42;

-- View history
DESCRIBE HISTORY catalog.silver.orders;
```

## Naming Conventions

```
catalog:  <env>_<org>            # prod_acme, dev_acme
schema:   bronze | silver | gold | ops
table:    <layer>_<entity>       # bronze_orders, silver_customers, gold_daily_revenue
view:     v_<metric_name>        # v_monthly_revenue
```

## Folder Structure for DLT Pipelines

```
/pipelines
├── 01_bronze/
│   ├── orders.py
│   └── customers.py
├── 02_silver/
│   ├── orders.py
│   └── customers.py
├── 03_gold/
│   ├── revenue.py
│   └── customers_360.py
└── utils/
    ├── quality_rules.py
    └── schema_registry.py
```

## CLI Reference

```bash
databricks-agent catalog optimize --table catalog.silver.orders --zorder col1,col2
databricks-agent catalog vacuum --table catalog.silver.orders --hours 168
databricks-agent catalog history --table catalog.silver.orders --limit 20
databricks-agent catalog restore --table catalog.silver.orders --version 42
```
