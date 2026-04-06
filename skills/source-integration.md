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
  - PostgreSQL
  - postgres
  - JDBC
  - RDS
  - Aurora
  - Azure Database for PostgreSQL
  - Cloud SQL
  - logical replication
  - Debezium
  - pg_cdc
  - read replica
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

## PostgreSQL

PostgreSQL is one of the most common OLTP sources fed into Databricks. Use JDBC for batch/incremental loads and logical replication (via Debezium or native CDC) for real-time pipelines.

### Secrets Setup (Always First)

```bash
# Store credentials in Databricks Secrets — never hardcode
databricks secrets create-scope --scope postgres-prod
databricks secrets put-secret --scope postgres-prod --key host
databricks secrets put-secret --scope postgres-prod --key port
databricks secrets put-secret --scope postgres-prod --key database
databricks secrets put-secret --scope postgres-prod --key user
databricks secrets put-secret --scope postgres-prod --key password
```

### Full Load (Partitioned Parallel Read)

Parallelise reads using a numeric or date partition column. Always point at a **read replica** for large extracts to avoid impacting production.

```python
pg_host = dbutils.secrets.get("postgres-prod", "host")
pg_db   = dbutils.secrets.get("postgres-prod", "database")
pg_user = dbutils.secrets.get("postgres-prod", "user")
pg_pass = dbutils.secrets.get("postgres-prod", "password")

jdbc_url = f"jdbc:postgresql://{pg_host}:5432/{pg_db}"

# Partitioned read — Spark spawns one task per partition
df = (spark.read
    .format("jdbc")
    .option("url",             jdbc_url)
    .option("dbtable",         "public.orders")
    .option("user",            pg_user)
    .option("password",        pg_pass)
    .option("driver",          "org.postgresql.Driver")
    # Parallelism — tune numPartitions to worker count
    .option("numPartitions",   "8")
    .option("partitionColumn", "order_id")          # must be numeric
    .option("lowerBound",      "1")
    .option("upperBound",      "100000000")
    # Performance
    .option("fetchsize",       "10000")             # rows per JDBC fetch
    .option("pushDownPredicate", "true")            # push WHERE to Postgres
    .load()
)

df.write.format("delta").mode("overwrite").saveAsTable("catalog.bronze.pg_orders")
```

### Incremental Watermark Load

```python
from pyspark.sql import functions as F

def pg_incremental_load(table: str, watermark_col: str = "updated_at",
                        target: str = None, scope: str = "postgres-prod"):
    """Load only rows newer than the last watermark."""
    pg_host = dbutils.secrets.get(scope, "host")
    pg_db   = dbutils.secrets.get(scope, "database")
    pg_user = dbutils.secrets.get(scope, "user")
    pg_pass = dbutils.secrets.get(scope, "password")
    jdbc_url = f"jdbc:postgresql://{pg_host}:5432/{pg_db}"

    target = target or f"catalog.bronze.pg_{table.replace('.', '_')}"

    # Get last watermark from Delta table
    try:
        last_wm = spark.sql(f"SELECT MAX({watermark_col}) FROM {target}").collect()[0][0]
        last_wm = last_wm or "1970-01-01 00:00:00"
    except Exception:
        last_wm = "1970-01-01 00:00:00"

    # Push the WHERE predicate down to PostgreSQL
    query = f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_wm}') AS incremental"

    df = (spark.read
        .format("jdbc")
        .option("url",      jdbc_url)
        .option("dbtable",  query)
        .option("user",     pg_user)
        .option("password", pg_pass)
        .option("driver",   "org.postgresql.Driver")
        .option("fetchsize", "5000")
        .load()
        .withColumn("_ingested_at", F.current_timestamp())
    )

    if df.isEmpty():
        print(f"No new rows in {table} since {last_wm}")
        return

    df.write.format("delta").mode("append").saveAsTable(target)
    print(f"Loaded {df.count():,} rows from {table} (watermark: {last_wm})")

# Usage
pg_incremental_load("public.orders",    watermark_col="updated_at")
pg_incremental_load("public.customers", watermark_col="modified_at")
```

### SSL / TLS Connection (RDS, Azure Database for PostgreSQL)

```python
# For managed PostgreSQL (AWS RDS, Azure Database, Cloud SQL) — SSL required
jdbc_url_ssl = (
    f"jdbc:postgresql://{pg_host}:5432/{pg_db}"
    "?sslmode=require"
    "&sslrootcert=/dbfs/mnt/certs/rds-ca-bundle.pem"   # upload cert to DBFS first
)

df = (spark.read
    .format("jdbc")
    .option("url",      jdbc_url_ssl)
    .option("dbtable",  "public.orders")
    .option("user",     pg_user)
    .option("password", pg_pass)
    .option("driver",   "org.postgresql.Driver")
    .load()
)
```

```bash
# Upload SSL certificate to DBFS
dbutils.fs.cp("file:/local/path/rds-ca-bundle.pem", "dbfs:/mnt/certs/rds-ca-bundle.pem")
```

### CDC via Debezium + Kafka

For sub-minute latency, use PostgreSQL logical replication (requires `wal_level = logical` on the server):

```python
# PostgreSQL → Debezium → Kafka → Databricks Structured Streaming
df_cdc = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", dbutils.secrets.get("kafka-scope", "brokers"))
    .option("subscribe", "postgres.public.orders")          # Debezium topic naming
    .option("startingOffsets", "latest")
    .load()
)

# Parse Debezium envelope (before/after/op)
from pyspark.sql.types import *
from pyspark.sql import functions as F

debezium_schema = StructType([
    StructField("before", StringType()),
    StructField("after",  StringType()),
    StructField("op",     StringType()),   # c=create, u=update, d=delete, r=read
    StructField("ts_ms",  LongType()),
])

df_parsed = (df_cdc
    .select(F.from_json(F.col("value").cast("string"), debezium_schema).alias("d"))
    .select(
        F.from_json("d.after",  order_schema).alias("after"),
        F.from_json("d.before", order_schema).alias("before"),
        F.col("d.op").alias("cdc_op"),
        F.col("d.ts_ms").alias("cdc_ts_ms"),
    )
)

# Apply changes to Delta table using DLT APPLY CHANGES
import dlt

dlt.create_streaming_table("silver_orders_from_postgres")
dlt.apply_changes(
    target="silver_orders_from_postgres",
    source="bronze_pg_orders_cdc",
    keys=["order_id"],
    sequence_by="cdc_ts_ms",
    apply_as_deletes=F.expr("cdc_op = 'd'"),
    except_column_list=["cdc_op", "cdc_ts_ms"]
)
```

### Schema Introspection

```python
def list_pg_tables(schema: str = "public", scope: str = "postgres-prod") -> list[dict]:
    """List all tables and row counts in a PostgreSQL schema."""
    pg_host = dbutils.secrets.get(scope, "host")
    pg_db   = dbutils.secrets.get(scope, "database")
    jdbc_url = f"jdbc:postgresql://{pg_host}:5432/{pg_db}"

    query = f"""(
        SELECT
            table_name,
            pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS size,
            (SELECT reltuples::BIGINT FROM pg_class WHERE relname = table_name) AS row_estimate
        FROM information_schema.tables
        WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'
        ORDER BY pg_total_relation_size(quote_ident(table_name)) DESC
    ) AS schema_info"""

    return (spark.read.format("jdbc")
        .option("url",      jdbc_url)
        .option("dbtable",  query)
        .option("user",     dbutils.secrets.get(scope, "user"))
        .option("password", dbutils.secrets.get(scope, "password"))
        .option("driver",   "org.postgresql.Driver")
        .load()
        .collect()
    )
```

```bash
# CLI: configure and test PostgreSQL connection
databricks-agent connect --source postgres \
  --host db.example.com --port 5432 --database prod_db \
  --secret-scope postgres-prod

# List tables in a schema
databricks-agent catalog list-source --source postgres --schema public

# Run full load
databricks-agent sql ingest --source postgres --table public.orders \
  --target catalog.bronze.pg_orders --mode overwrite

# Run incremental load
databricks-agent sql ingest --source postgres --table public.orders \
  --target catalog.bronze.pg_orders --mode incremental --watermark updated_at

# Profile table before ingestion
databricks-agent sql profile --source postgres --table public.orders
```

## Other JDBC Sources

```python
# SQL Server
df = (spark.read.format("jdbc")
    .option("url",      "jdbc:sqlserver://hostname:1433;databaseName=mydb")
    .option("dbtable",  "dbo.orders")
    .option("user",     dbutils.secrets.get("sql-scope", "user"))
    .option("password", dbutils.secrets.get("sql-scope", "password"))
    .option("driver",   "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)

# Snowflake
df = (spark.read.format("snowflake")
    .options(**{
        "sfURL":       "account.snowflakecomputing.com",
        "sfUser":      dbutils.secrets.get("snow-scope", "user"),
        "sfPassword":  dbutils.secrets.get("snow-scope", "password"),
        "sfDatabase":  "MY_DB",
        "sfSchema":    "PUBLIC",
        "sfWarehouse": "COMPUTE_WH",
        "dbtable":     "ORDERS",
    })
    .load()
)
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
