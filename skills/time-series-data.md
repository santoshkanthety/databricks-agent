---
name: databricks-time-series-data
description: Time series patterns in Databricks - gap detection, date spines, binning, LOCF, streaming windows, IoT and sensor data patterns
triggers:
  - time series
  - gaps
  - sparse data
  - binning
  - time buckets
  - IoT
  - sensor data
  - tick data
  - date spine
  - gap filling
  - LOCF
  - last observation
  - normalize time
  - streaming window
  - tumbling window
  - sliding window
  - session window
---

# Time Series Data in Databricks

## Gap Detection in Sparse Time Series

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Detect gaps in a daily time series
w = Window.partitionBy("device_id").orderBy("event_date")

df_gaps = (spark.table("silver.sensor_readings")
    .withColumn("prev_date", F.lag("event_date", 1).over(w))
    .withColumn("days_gap",  F.datediff("event_date", F.col("prev_date")))
    .filter(F.col("days_gap") > 1)
    .select("device_id", "prev_date", "event_date", "days_gap")
)

df_gaps.show()
```

```sql
-- SQL: find gaps in hourly IoT data
WITH hourly AS (
  SELECT device_id, DATE_TRUNC('hour', event_ts) AS hour
  FROM silver.sensor_readings
  GROUP BY 1, 2
),
with_lag AS (
  SELECT *,
    LAG(hour) OVER (PARTITION BY device_id ORDER BY hour) AS prev_hour
  FROM hourly
)
SELECT device_id, prev_hour, hour,
  (UNIX_TIMESTAMP(hour) - UNIX_TIMESTAMP(prev_hour)) / 3600 AS gap_hours
FROM with_lag
WHERE (UNIX_TIMESTAMP(hour) - UNIX_TIMESTAMP(prev_hour)) / 3600 > 1
ORDER BY gap_hours DESC
```

```bash
databricks-agent sql gap-detect --table silver.sensor_readings \
  --ts-col event_ts --partition-col device_id --interval 1h
```

## Date Spine (Fill Gaps)

A date spine ensures every time bucket has a row, even with no data:

```python
from pyspark.sql import functions as F

def generate_date_spine(start: str, end: str, grain: str = "day") -> "DataFrame":
    """Generate a complete date spine from start to end."""
    if grain == "day":
        return spark.sql(f"""
            SELECT explode(sequence(DATE('{start}'), DATE('{end}'), INTERVAL 1 DAY)) AS date
        """)
    elif grain == "hour":
        return spark.sql(f"""
            SELECT explode(sequence(
                TIMESTAMP('{start}'), TIMESTAMP('{end}'), INTERVAL 1 HOUR
            )) AS ts
        """)
    elif grain == "month":
        return spark.sql(f"""
            SELECT explode(sequence(DATE('{start}'), DATE('{end}'), INTERVAL 1 MONTH)) AS month
        """)

# Build spine × device cross-join, then left-join actuals
spine = generate_date_spine("2025-01-01", "2025-12-31", "day")
devices = spark.table("silver.sensor_readings").select("device_id").distinct()
full_grid = spine.crossJoin(devices)

# Left join actuals onto full grid
df_filled = (full_grid
    .join(
        spark.table("silver.sensor_readings").select("device_id", "event_date", "reading"),
        on=["device_id", "date"],
        how="left"
    )
    .withColumn("reading_filled", F.coalesce("reading", F.lit(0)))
)
```

## LOCF — Last Observation Carried Forward

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Window: all preceding rows, ordered by timestamp
w_locf = (Window.partitionBy("device_id")
          .orderBy("ts")
          .rowsBetween(Window.unboundedPreceding, Window.currentRow))

df_locf = (df_filled
    .withColumn("reading_locf",
        F.last(F.col("reading"), ignorenulls=True).over(w_locf)
    )
)
```

```sql
-- SQL LOCF equivalent
SELECT
  device_id,
  ts,
  reading,
  LAST_VALUE(reading IGNORE NULLS) OVER (
    PARTITION BY device_id
    ORDER BY ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS reading_locf
FROM sensor_spine
```

## Timestamp Binning (Fixed-Interval Buckets)

```python
from pyspark.sql import functions as F

# Bin to 5-minute intervals
df = df.withColumn(
    "ts_5min",
    F.from_unixtime(
        (F.unix_timestamp("event_ts") / 300).cast("long") * 300
    ).cast("timestamp")
)

# Bin to hourly
df = df.withColumn("ts_hour", F.date_trunc("hour", "event_ts"))

# Bin to daily
df = df.withColumn("ts_day", F.date_trunc("day", "event_ts"))

# Aggregate by bin
df_hourly = (df.groupBy("device_id", "ts_hour")
    .agg(
        F.avg("temperature").alias("avg_temp"),
        F.max("temperature").alias("max_temp"),
        F.min("temperature").alias("min_temp"),
        F.count("*").alias("reading_count")
    )
)
```

```bash
databricks-agent sql bin --table silver.sensor_readings --ts-col event_ts --interval 5m \
  --agg avg:temperature,max:temperature --output silver.sensor_hourly
```

## Z-Score Normalization

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Z-score: (value - mean) / stddev, per device
w_device = Window.partitionBy("device_id")

df_norm = (df
    .withColumn("mean_temp", F.avg("temperature").over(w_device))
    .withColumn("stddev_temp", F.stddev("temperature").over(w_device))
    .withColumn("temp_zscore",
        (F.col("temperature") - F.col("mean_temp")) / F.col("stddev_temp")
    )
    .withColumn("is_anomaly", F.abs("temp_zscore") > 3)  # flag outliers
)

# Min-Max normalization (0 to 1)
df_norm = (df
    .withColumn("min_temp", F.min("temperature").over(w_device))
    .withColumn("max_temp", F.max("temperature").over(w_device))
    .withColumn("temp_normalized",
        (F.col("temperature") - F.col("min_temp")) /
        (F.col("max_temp") - F.col("min_temp"))
    )
)
```

## Structured Streaming Windows

```python
from pyspark.sql import functions as F

# Tumbling window (non-overlapping)
df_tumbling = (df_stream
    .withWatermark("event_ts", "10 minutes")  # late data tolerance
    .groupBy(
        F.window("event_ts", "1 hour"),        # 1-hour tumbling window
        "device_id"
    )
    .agg(
        F.avg("temperature").alias("avg_temp"),
        F.count("*").alias("reading_count")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "device_id", "avg_temp", "reading_count"
    )
)

# Sliding window (overlapping — e.g., 1-hour window every 15 minutes)
df_sliding = (df_stream
    .withWatermark("event_ts", "10 minutes")
    .groupBy(
        F.window("event_ts", "1 hour", "15 minutes"),  # (size, slide)
        "device_id"
    )
    .agg(F.avg("temperature").alias("avg_temp"))
)

# Session window (gap-based — groups events until silence gap)
df_session = (df_stream
    .withWatermark("event_ts", "30 minutes")
    .groupBy(
        F.session_window("event_ts", "30 minutes"),  # new session after 30-min gap
        "user_id"
    )
    .agg(F.count("*").alias("events_in_session"))
)
```

## IoT / Sensor Data Patterns

```python
# Multi-sensor pivot: one row per timestamp, one column per sensor
df_pivoted = (df.groupBy("device_id", "ts_5min")
    .pivot("sensor_type", ["temperature", "humidity", "pressure"])
    .agg(F.avg("reading"))
)

# Detect anomalies with rolling statistics
w_rolling = (Window.partitionBy("device_id")
             .orderBy("ts")
             .rowsBetween(-23, 0))  # 24-hour rolling window

df_anomaly = (df
    .withColumn("rolling_mean", F.avg("temperature").over(w_rolling))
    .withColumn("rolling_std",  F.stddev("temperature").over(w_rolling))
    .withColumn("z_score", (F.col("temperature") - F.col("rolling_mean")) / F.col("rolling_std"))
    .withColumn("is_anomaly", F.abs("z_score") > 3)
)

# Alert on anomalies
anomalies = df_anomaly.filter("is_anomaly = true")
if anomalies.count() > 0:
    print(f"⚠️ {anomalies.count()} anomalies detected")
    anomalies.write.format("delta").mode("append").saveAsTable("ops.sensor_anomalies")
```

## CLI Reference

```bash
databricks-agent sql gap-detect --table silver.readings --ts-col ts --partition device_id --interval 1h
databricks-agent sql bin --table silver.readings --ts-col ts --interval 5m --agg avg:temp
databricks-agent sql fill-gaps --table silver.readings --ts-col ts --method locf
```
