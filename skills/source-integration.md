---
name: databricks-source-integration
description: Data source integration patterns - Auto Loader, JDBC/ODBC, cloud storage, Kafka, REST APIs, CSV files, and the databricks-agent UI for pipeline configuration
triggers:
  - data ingestion
  - connections
  - pipeline setup
  - sources
  - JDBC
  - ODBC
  - databases
  - APIs
  - REST API
  - Kafka
  - Auto Loader
  - cloud storage
  - S3
  - ADLS
  - GCS
  - CSV
  - JSON
  - Parquet
  - streaming
  - batch ingest
---

# Source Integration in Databricks

## Configuration UI

Use the `databricks-agent ui` command for a visual, code-free pipeline designer:

```bash
databricks-agent ui
# Opens at http://localhost:8000
# Configure sources, destinations, and schedules visually
```

## Auto Loader (Cloud Storage — Recommended for Files)

Auto Loader efficiently ingests files from cloud storage (S3, ADLS Gen2, GCS) using directory listing or file notifications. It's the preferred Bronze ingestion pattern.

```python
# JSON files
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/orders_schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # handle new fields
    .load("abfss://landing@storageaccount.dfs.core.windows.net/orders/")
)

# Parquet files
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/events_schema")
    .load("s3://my-bucket/events/")
)

# CSV files with schema enforcement
from pyspark.sql.types import *
schema = StructType([
    StructField("order_id",   StringType(),    False),
    StructField("customer_id", StringType(),   False),
    StructField("revenue",    DoubleType(),    True),
    StructField("order_date", DateType(),      True),
])

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(schema)
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/csv_schema")
    .load("abfss://landing@storage.dfs.core.windows.net/csv-orders/")
)

# Write stream to Bronze Delta table
(df.withColumn("_ingested_at", F.current_timestamp())
   .withColumn("_source_file", F.input_file_name())
   .writeStream
   .format("delta")
   .option("checkpointLocation", "/mnt/checkpoints/bronze_orders")
   .outputMode("append")
   .toTable("catalog.bronze.orders")
)
```

```bash
# CLI: set up Auto Loader pipeline
databricks-agent pipelines create --name orders-autoloader \
  --source s3://my-bucket/orders/ --format json \
  --target catalog.bronze.orders --checkpoint /mnt/checkpoints/orders
```

## JDBC Sources (Databases)

```python
# PostgreSQL
df = (spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://hostname:5432/dbname")
    .option("dbtable", "public.orders")
    .option("user", dbutils.secrets.get("my-scope", "pg-user"))
    .option("password", dbutils.secrets.get("my-scope", "pg-password"))
    .option("driver", "org.postgresql.Driver")
    .option("numPartitions", "8")
    .option("partitionColumn", "order_id")
    .option("lowerBound", "1")
    .option("upperBound", "10000000")
    .load()
)

# SQL Server
df = (spark.read
    .format("jdbc")
    .option("url", "jdbc:sqlserver://hostname:1433;databaseName=mydb")
    .option("dbtable", "dbo.orders")
    .option("user", dbutils.secrets.get("my-scope", "sql-user"))
    .option("password", dbutils.secrets.get("my-scope", "sql-password"))
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)

# Snowflake
df = (spark.read
    .format("snowflake")
    .options(**{
        "sfURL": "account.snowflakecomputing.com",
        "sfUser": dbutils.secrets.get("snow-scope", "user"),
        .option("sfPassword", dbutils.secrets.get("snow-scope", "password"))
        "sfDatabase": "MY_DB",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "COMPUTE_WH",
        "dbtable": "ORDERS"
    })
    .load()
)
```

```bash
# CLI: configure database source
databricks-agent connect --source postgres \
  --host hostname --port 5432 --database dbname \
  --secret-scope my-scope --secret-user pg-user --secret-password pg-password
```

## REST API Ingestion

```python
import httpx
from pyspark.sql import SparkSession

def ingest_rest_api(base_url: str, token: str, endpoint: str, params: dict = None):
    """Paginated REST API ingestion."""
    records = []
    page = 1
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    while True:
        response = httpx.get(
            f"{base_url}/{endpoint}",
            headers=headers,
            params={**(params or {}), "page": page, "per_page": 100},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        if not data.get("results"):
            break
        
        records.extend(data["results"])
        
        if not data.get("next"):
            break
        page += 1
    
    return spark.createDataFrame(records)

# Usage
api_token = dbutils.secrets.get("api-scope", "my-api-token")
df = ingest_rest_api(
    base_url="https://api.myservice.com/v1",
    token=api_token,
    endpoint="orders",
    params={"status": "completed", "updated_after": "2025-01-01"}
)
df.write.format("delta").mode("append").saveAsTable("catalog.bronze.api_orders")
```

## Kafka / Confluent Streaming

```python
# Read from Kafka topic
df_kafka = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", dbutils.secrets.get("kafka-scope", "brokers"))
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", 
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username='{dbutils.secrets.get('kafka-scope', 'api-key')}' "
            f"password='{dbutils.secrets.get('kafka-scope', 'api-secret')}';")
    .option("subscribe", "orders-topic")
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON payload
from pyspark.sql.types import *
order_schema = StructType([
    StructField("order_id",   StringType()),
    StructField("customer_id", StringType()),
    StructField("revenue",    DoubleType()),
    StructField("event_time", TimestampType()),
])

df_orders = (df_kafka
    .select(F.from_json(F.col("value").cast("string"), order_schema).alias("data"))
    .select("data.*")
    .withColumn("_kafka_offset", F.col("offset"))
    .withColumn("_kafka_partition", F.col("partition"))
    .withColumn("_ingested_at", F.current_timestamp())
)
```

## Delta Sharing (Cross-Organization)

```python
# Read a Delta Share (from another organization or cloud)
df = (spark.read
    .format("deltaSharing")
    .option("responseFormat", "delta")
    .load("https://sharing.databricks.com/delta-sharing/your-profile#share.schema.table")
)
```

## Validation After Ingest

```python
def validate_ingest(df, table_name: str, expected_min_rows: int = 1):
    row_count = df.count()
    assert row_count >= expected_min_rows, \
        f"{table_name}: Expected >= {expected_min_rows} rows, got {row_count}"
    
    null_counts = df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c) 
        for c in df.columns
    ]).collect()[0].asDict()
    
    high_null_cols = {col: cnt for col, cnt in null_counts.items() if cnt / row_count > 0.5}
    if high_null_cols:
        print(f"WARNING: High null rate in columns: {high_null_cols}")
    
    return row_count
```

## CLI Reference

```bash
databricks-agent ui                                              # Launch config UI
databricks-agent pipelines create --source s3://... --format json --target bronze.orders
databricks-agent connect --source postgres --host h --port 5432 # Configure DB source
databricks-agent sql query --sql "SELECT COUNT(*) FROM bronze.orders"  # Verify ingest
```
