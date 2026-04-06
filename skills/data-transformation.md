---
name: databricks-data-transformation
description: PySpark and Spark SQL transformation patterns - union, append, type casting, surrogate keys, schema drift, Delta merge
triggers:
  - union
  - append
  - stack tables
  - convert data types
  - surrogate key
  - schema alignment
  - type casting
  - data transformation
  - merge
  - upsert
  - deduplication
  - schema drift
  - normalize
  - flatten
---

# Data Transformation Patterns in Databricks

## Unioning / Appending Data Across Sources

Always align schemas before union. Mismatched columns cause silent nulls.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, TimestampType

# Schema-safe union: align all columns from both DataFrames
def union_safe(df1, df2):
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)
    all_cols = sorted(cols1 | cols2)
    
    for c in all_cols:
        if c not in cols1:
            df1 = df1.withColumn(c, F.lit(None).cast(df2.schema[c].dataType))
        if c not in cols2:
            df2 = df2.withColumn(c, F.lit(None).cast(df1.schema[c].dataType))
    
    return df1.select(all_cols).union(df2.select(all_cols))

# Union multiple sources
from functools import reduce
dfs = [spark.table(f"bronze.orders_{region}") for region in ["us", "eu", "apac"]]
combined = reduce(union_safe, dfs)
```

SQL equivalent:

```sql
-- Union with source tracking
SELECT *, 'us' AS source_region FROM bronze.orders_us
UNION ALL
SELECT *, 'eu' AS source_region FROM bronze.orders_eu
UNION ALL
SELECT *, 'apac' AS source_region FROM bronze.orders_apac
```

```bash
# CLI: union multiple tables
databricks-agent sql union --tables bronze.orders_us,bronze.orders_eu,bronze.orders_apac --output silver.orders_combined
```

## Type Conversion Patterns

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Safe type casting with null handling
df = (spark.table("bronze.raw_events")
    .withColumn("event_ts", F.to_timestamp("event_date_str", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("revenue",  F.when(F.col("revenue_str").rlike(r"^\d+\.?\d*$"), 
                                   F.col("revenue_str").cast(DoubleType()))
                              .otherwise(F.lit(None)))
    .withColumn("is_active", F.col("status_flag").cast(BooleanType()))
    .withColumn("user_id",   F.col("user_id_str").cast(LongType()))
)

# Standardize string columns
df = (df
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("country_code", F.upper(F.trim(F.col("country_code"))))
    .withColumn("phone", F.regexp_replace("phone", r"[^\d+]", ""))
)
```

```bash
# CLI: profile column types and flag mismatches
databricks-agent sql profile --table bronze.raw_events --check-types
```

## Surrogate Key Generation

Use SHA2 hashing for deterministic, reproducible surrogate keys:

```python
from pyspark.sql import functions as F

# Hash-based surrogate key (deterministic across runs)
df = df.withColumn(
    "customer_sk",
    F.sha2(F.concat_ws("|", F.col("source_system"), F.col("customer_id")), 256)
)

# Auto-increment surrogate key (use only when order is deterministic)
from pyspark.sql.window import Window
df = df.withColumn(
    "order_sk",
    F.row_number().over(Window.orderBy("order_id"))
)

# UUID surrogate key for new records
df = df.withColumn("record_id", F.expr("uuid()"))
```

## Delta Merge (Upsert)

The canonical pattern for SCD Type 1 and idempotent loads:

```python
from delta.tables import DeltaTable

def merge_into_delta(source_df, target_table: str, merge_keys: list[str]):
    """Generic upsert into Delta table."""
    if not DeltaTable.isDeltaTable(spark, target_table.replace(".", "/")):
        source_df.write.format("delta").saveAsTable(target_table)
        return
    
    target = DeltaTable.forName(spark, target_table)
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
    
    (target.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# Usage
merge_into_delta(
    source_df=silver_customers,
    target_table="gold.dim_customer",
    merge_keys=["customer_id"]
)
```

```sql
-- SQL MERGE syntax
MERGE INTO gold.dim_customer AS target
USING silver.customers AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

```bash
# CLI: run merge operation
databricks-agent sql merge --source silver.customers --target gold.dim_customer --keys customer_id
```

## Deduplication

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Keep latest record per natural key
w = Window.partitionBy("customer_id").orderBy(F.desc("updated_at"))
deduped = (df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# Drop exact duplicates
deduped = df.dropDuplicates(["customer_id", "email"])
```

## Schema Drift Handling

```python
# Auto Loader with schema evolution (recommended)
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/orders_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # or "rescue"
    .load("/mnt/landing/orders/")
)

# For batch: merge schema on write
df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("bronze.orders")
```

## Flatten Nested / Struct Columns

```python
# Explode arrays
from pyspark.sql import functions as F

df_flat = df.select(
    "order_id",
    "customer_id", 
    F.explode("line_items").alias("item")
).select(
    "order_id",
    "customer_id",
    "item.product_id",
    "item.quantity",
    "item.unit_price"
)

# Flatten struct columns
def flatten_struct(df):
    flat_cols = []
    for field in df.schema.fields:
        if hasattr(field.dataType, "fields"):  # StructType
            for nested in field.dataType.fields:
                flat_cols.append(F.col(f"{field.name}.{nested.name}").alias(f"{field.name}_{nested.name}"))
        else:
            flat_cols.append(F.col(field.name))
    return df.select(flat_cols)
```

## Data Quality Checks Before Writing

```python
def assert_no_nulls(df, columns: list[str]):
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        assert null_count == 0, f"Column '{col}' has {null_count} null values"

def assert_unique(df, columns: list[str]):
    total = df.count()
    distinct = df.select(columns).distinct().count()
    assert total == distinct, f"Duplicate keys found: {total - distinct} duplicates on {columns}"

# Apply before writing to Silver
assert_no_nulls(df, ["customer_id", "order_id", "event_ts"])
assert_unique(df, ["order_id"])
```

## CLI Reference

```bash
databricks-agent sql union --tables t1,t2,t3 --output target_table    # Union tables
databricks-agent sql merge --source src --target tgt --keys col1,col2  # Upsert
databricks-agent sql profile --table bronze.events                      # Type profiling
databricks-agent sql dedupe --table bronze.events --keys customer_id   # Deduplication
```
