---
name: databricks-dlt-pipelines
description: Delta Live Tables (DLT) pipeline patterns, ingestion strategies, CDC, Auto Loader, incremental loads, watermark, and Databricks Workflows
triggers:
  - ETL
  - ELT
  - DLT
  - delta live tables
  - pipeline
  - ingestion
  - incremental load
  - CDC
  - change data capture
  - watermark
  - Auto Loader
  - streaming
  - workflow
  - job
  - trigger
  - continuous
  - scheduled
  - bronze silver gold
---

# DLT Pipelines & Databricks Workflows

## Ingestion Strategy Decision Tree

```
Source data pattern?
├─ Full snapshot each run         → Full load + OVERWRITE
├─ Append-only (logs, events)     → Auto Loader streaming / batch append
├─ Updates via timestamp column   → Watermark incremental load
├─ Deletes + updates (CDC)        → APPLY CHANGES INTO (DLT)
└─ Near-real-time (<5 min SLA)    → Structured Streaming / DLT continuous
```

## Delta Live Tables (DLT) — Bronze to Gold

```python
import dlt
from pyspark.sql import functions as F

# BRONZE: raw ingest from Auto Loader
@dlt.table(
    name="bronze_orders",
    comment="Raw orders from landing zone. No transformations.",
    table_properties={"pipelines.autoOptimize.managed": "true"}
)
def bronze_orders():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", "/mnt/checkpoints/orders_schema")
            .load("/mnt/landing/orders/")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
    )

# SILVER: cleaned with quality constraints
@dlt.table(name="silver_orders", comment="Validated and cleaned orders.")
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_revenue", "revenue >= 0")
@dlt.expect("valid_customer", "customer_id IS NOT NULL")  # warn, don't drop
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
            .withColumn("order_date", F.to_date("order_date_str", "yyyy-MM-dd"))
            .withColumn("revenue", F.col("revenue_str").cast("double"))
            .withColumn("_processed_at", F.current_timestamp())
            .drop("order_date_str", "revenue_str")
    )

# GOLD: business aggregation
@dlt.table(name="gold_daily_revenue", comment="Daily revenue by region. Refreshed hourly.")
def gold_daily_revenue():
    return (
        dlt.read("silver_orders")
            .groupBy("order_date", "region")
            .agg(
                F.sum("revenue").alias("total_revenue"),
                F.count("order_id").alias("order_count"),
                F.countDistinct("customer_id").alias("unique_customers")
            )
    )
```

```bash
# CLI: deploy and run a DLT pipeline
databricks-agent pipelines create --name orders-pipeline --notebook /pipelines/orders.py
databricks-agent pipelines start --name orders-pipeline
databricks-agent pipelines status --name orders-pipeline
```

## CDC with APPLY CHANGES INTO

```python
import dlt

# Source: CDC stream from Kafka / Kinesis / Auto Loader
@dlt.table(name="cdc_raw_customers")
def cdc_raw_customers():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("/mnt/cdc/customers/")

# Target: DLT manages the SCD Type 1 merge automatically
dlt.create_streaming_table("silver_customers")

dlt.apply_changes(
    target="silver_customers",
    source="cdc_raw_customers",
    keys=["customer_id"],
    sequence_by="cdc_timestamp",              # ordering column
    apply_as_deletes=F.expr("op = 'DELETE'"), # CDC delete flag
    except_column_list=["op", "cdc_timestamp"] # exclude CDC metadata cols
)
```

## Watermark Incremental Load (Non-DLT)

```python
from datetime import datetime

def get_last_watermark(target_table: str) -> str:
    try:
        return spark.sql(f"SELECT MAX(updated_at) FROM {target_table}").collect()[0][0]
    except:
        return "1970-01-01"

def incremental_load(source_table: str, target_table: str, ts_col: str = "updated_at"):
    watermark = get_last_watermark(target_table)
    
    new_data = spark.sql(f"""
        SELECT * FROM {source_table}
        WHERE {ts_col} > '{watermark}'
    """)
    
    if new_data.isEmpty():
        print("No new data.")
        return
    
    new_data.write.format("delta").mode("append").saveAsTable(target_table)
    print(f"Loaded {new_data.count()} new rows since {watermark}")
```

```bash
# CLI: run incremental load
databricks-agent jobs run --name incremental-load --param watermark=2025-01-01
```

## Databricks Workflows (Job Orchestration)

```python
# Define a multi-task job via SDK
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

job = w.jobs.create(
    name="daily-etl-pipeline",
    schedule=jobs.CronSchedule(
        quartz_cron_expression="0 0 6 * * ?",  # 6 AM UTC daily
        timezone_id="UTC"
    ),
    tasks=[
        jobs.Task(
            task_key="ingest_bronze",
            notebook_task=jobs.NotebookTask(notebook_path="/pipelines/01_ingest"),
            new_cluster=jobs.ClusterSpec(
                spark_version="15.4.x-scala2.12",
                node_type_id="Standard_DS3_v2",
                num_workers=2
            )
        ),
        jobs.Task(
            task_key="transform_silver",
            depends_on=[jobs.TaskDependency(task_key="ingest_bronze")],
            notebook_task=jobs.NotebookTask(notebook_path="/pipelines/02_transform"),
            job_cluster_key="ingest_bronze"  # reuse cluster
        ),
        jobs.Task(
            task_key="build_gold",
            depends_on=[jobs.TaskDependency(task_key="transform_silver")],
            notebook_task=jobs.NotebookTask(notebook_path="/pipelines/03_gold"),
            job_cluster_key="ingest_bronze"
        )
    ]
)
print(f"Created job: {job.job_id}")
```

```bash
# CLI: manage jobs
databricks-agent jobs list                              # List all jobs
databricks-agent jobs run --name daily-etl-pipeline    # Trigger a run
databricks-agent jobs status --run-id 12345            # Check run status
databricks-agent jobs cancel --run-id 12345            # Cancel a run
databricks-agent jobs history --name daily-etl-pipeline --limit 10
```

## Idempotent Pipeline Patterns

Every pipeline must be safe to re-run:

```python
# Pattern 1: OVERWRITE for full loads
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.summary")

# Pattern 2: MERGE for upserts (already idempotent)
# Pattern 3: Structured Streaming with checkpoints (exactly-once)
(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/silver_orders")
    .outputMode("append")
    .toTable("silver.orders")
)

# Pattern 4: Partition overwrite for date-partitioned tables
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.format("delta").mode("overwrite").partitionBy("order_date").saveAsTable("silver.orders")
```

## Control Table Pattern

```python
# Store pipeline state in Delta table
def update_pipeline_state(pipeline_name: str, status: str, rows_loaded: int):
    spark.sql(f"""
        INSERT INTO catalog.ops.pipeline_log
        VALUES ('{pipeline_name}', '{status}', {rows_loaded}, current_timestamp())
    """)

def get_pipeline_last_run(pipeline_name: str):
    return spark.sql(f"""
        SELECT MAX(run_ts) FROM catalog.ops.pipeline_log
        WHERE pipeline_name = '{pipeline_name}' AND status = 'success'
    """).collect()[0][0]
```

## Anti-Patterns to Avoid

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Transforming in Bronze | Breaks replay | Bronze = raw only |
| Hardcoded credentials | Security risk | Use Databricks secrets |
| Full refresh daily | Costly + slow | Watermark or CDC |
| `collect()` in loops | Driver OOM | Use `agg()` or joins |
| Nested `withColumn()` for 30+ cols | Plan explosion | Use `select()` instead |
| No checkpoints in streaming | Reprocesses all data | Always set checkpoint |

## CLI Reference

```bash
databricks-agent pipelines list                          # List DLT pipelines
databricks-agent pipelines create --name p1 --notebook /path/to/nb
databricks-agent pipelines start --name p1 --full-refresh
databricks-agent pipelines status --name p1
databricks-agent pipelines events --name p1 --limit 20
databricks-agent jobs list                               # List Workflow jobs
databricks-agent jobs run --name my-job
databricks-agent jobs status --run-id 123
databricks-agent jobs history --name my-job
```
