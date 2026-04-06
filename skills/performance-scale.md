---
name: databricks-performance-scale
description: Databricks cluster optimization, Delta query tuning, caching, partitioning, autoscaling, photon, and capacity planning
triggers:
  - slow query
  - performance
  - optimization
  - cluster sizing
  - autoscaling
  - photon
  - caching
  - partitioning
  - skew
  - shuffle
  - spill
  - query tuning
  - capacity planning
  - cost optimization
  - warehouse sizing
  - adaptive query execution
---

# Performance & Scale in Databricks

## Cluster Type Selection

| Use Case | Cluster Type |
|----------|-------------|
| Interactive notebooks, ad-hoc SQL | SQL Warehouse (serverless preferred) |
| Batch ETL, scheduled jobs | Job Compute (single-node or autoscaling) |
| Streaming pipelines | Job Compute (fixed workers for predictability) |
| ML training (large models) | GPU cluster with spot instances |
| DLT pipelines | Enhanced Autoscaling (managed by DLT) |

```bash
# CLI: list and manage clusters
databricks-agent clusters list
databricks-agent clusters start --name my-cluster
databricks-agent clusters resize --name my-cluster --workers 8
```

## Cluster Sizing Rules

```
Starting point:
  - Small/medium data (<100 GB): Standard_DS3_v2 (14 GB RAM) × 2-4 workers
  - Large ETL (100 GB – 1 TB): Standard_DS4_v2 (28 GB RAM) × 4-8 workers
  - Very large (>1 TB, wide joins): Standard_DS5_v2 (56 GB RAM) × 8-16 workers

Rule of thumb: RAM per executor = 2–4× size of your largest shuffle partition
Target shuffle partition size: 128 MB – 256 MB
```

Enable **Photon** for SQL and Delta workloads — up to 12× speedup on aggregations, joins, and OPTIMIZE operations:

```bash
databricks-agent clusters create --name fast-etl --photon --node Standard_DS4_v2 --workers 4
```

## Autoscaling Configuration

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import AutoScale

w = WorkspaceClient()
w.clusters.create(
    cluster_name="autoscale-etl",
    spark_version="15.4.x-scala2.12",
    node_type_id="Standard_DS4_v2",
    autoscale=AutoScale(min_workers=2, max_workers=10),
    enable_elastic_disk=True,
    spark_conf={
        "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    }
)
```

## Adaptive Query Execution (AQE)

AQE is enabled by default in DBR 9.1+. It automatically:
- Coalesces small shuffle partitions
- Converts sort-merge joins to broadcast joins when one side is small
- Handles data skew by splitting skewed partitions

```python
# Force AQE settings
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

## Diagnosing Performance Issues

```python
# 1. Check query plan
df.explain(mode="formatted")

# 2. Profile with Spark UI — look for:
#    - Stages with high shuffle read/write
#    - Tasks with very different durations (skew)
#    - Spill to disk (executor memory pressure)

# 3. Check partition count and size
print(f"Partitions: {df.rdd.getNumPartitions()}")

# Ideal: 128-256 MB per partition
# Too few partitions: underutilized workers
# Too many partitions: scheduling overhead

# 4. Repartition if needed
df = df.repartition(200, "customer_id")  # co-locate by key
df = df.coalesce(20)                     # reduce (no shuffle)
```

```bash
# CLI: analyze a recent query
databricks-agent sql analyze --query-id abc123 --warehouse my-warehouse
databricks-agent sql slow-queries --warehouse my-warehouse --threshold-ms 5000
```

## Shuffle Tuning

```python
# Set shuffle partitions based on data size
# Rule: data_size_bytes / 128MB = target partition count
spark.conf.set("spark.sql.shuffle.partitions", "200")  # default; adjust per job

# For small data (< 10 GB), reduce to avoid overhead
spark.conf.set("spark.sql.shuffle.partitions", "20")

# For very large data (> 1 TB), increase
spark.conf.set("spark.sql.shuffle.partitions", "2000")
```

## Broadcast Joins (Eliminate Shuffles)

```python
from pyspark.sql import functions as F

# Force broadcast of small dimension table (<= 10 MB)
dim_product = spark.table("gold.dim_product")  # small table
fact_orders = spark.table("silver.orders")      # large table

result = fact_orders.join(F.broadcast(dim_product), "product_id")

# Or set auto-broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
```

## Caching

```python
# Cache when a DataFrame is used 3+ times
df_silver = spark.table("silver.orders").filter("order_date >= '2024-01-01'")
df_silver.cache()
df_silver.count()  # materialize cache

# Use DISK_AND_MEMORY for large DataFrames
from pyspark import StorageLevel
df_silver.persist(StorageLevel.DISK_AND_MEMORY)

# Always unpersist when done
df_silver.unpersist()
```

```sql
-- Cache a table in Spark cache (lost on cluster restart)
CACHE TABLE silver.orders;
UNCACHE TABLE silver.orders;

-- Delta table caching (persistent, survives restarts — Databricks only)
ALTER TABLE catalog.silver.orders SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
```

## Delta Optimization for Query Performance

```sql
-- OPTIMIZE + ZORDER before heavy reads
OPTIMIZE catalog.silver.orders ZORDER BY (customer_id, order_date);

-- For new tables, use Liquid Clustering instead
CREATE TABLE catalog.silver.orders CLUSTER BY (customer_id, order_date) ...;

-- Check file sizes after OPTIMIZE
DESCRIBE DETAIL catalog.silver.orders;
-- Look at: numFiles, sizeInBytes, minFileSize, maxFileSize
```

## SQL Warehouse Sizing

| Warehouse Size | vCPUs | Use Case |
|---|---|---|
| 2X-Small | 8 | Dashboards, light queries |
| Small | 16 | Standard analytics, <1 GB queries |
| Medium | 32 | Heavy aggregations, 1–10 GB queries |
| Large | 64 | Complex joins, 10–100 GB queries |
| X-Large | 128 | Very large fact tables, concurrent users |

```bash
# CLI: resize warehouse
databricks-agent warehouses resize --name my-warehouse --size Large

# Check warehouse query history and P95 latency
databricks-agent warehouses stats --name my-warehouse
```

## Capacity Planning Checklist

- [ ] Data volume per table (GB/TB) and growth rate
- [ ] Peak concurrent users on SQL Warehouse
- [ ] Job SLA requirements (how long can batch take?)
- [ ] Shuffle-heavy operations (joins on large fact tables)
- [ ] Streaming latency requirements
- [ ] Cost budget (spot vs on-demand ratio)

```bash
databricks-agent clusters cost --period 7d --group-by job     # Cost by job
databricks-agent clusters cost --period 30d --group-by cluster # Cost by cluster
databricks-agent warehouses cost --period 30d                   # Warehouse cost
```

## CLI Reference

```bash
databricks-agent clusters list                                    # List clusters
databricks-agent clusters create --name my-cluster --photon      # Create cluster
databricks-agent clusters start --name my-cluster                # Start cluster
databricks-agent clusters resize --name my-cluster --workers 8  # Resize
databricks-agent clusters cost --period 7d                       # Cost report
databricks-agent warehouses list                                  # List warehouses
databricks-agent warehouses resize --name wh --size Large        # Resize warehouse
databricks-agent sql slow-queries --warehouse wh --threshold 5000
```
