---
name: databricks-spark-sql-mastery
description: Spark SQL and DataFrame API mastery - window functions, aggregations, CTEs, query optimization, and advanced analytical patterns
triggers:
  - spark sql
  - dataframe api
  - window function
  - aggregation
  - GROUP BY
  - CTE
  - subquery
  - analytical query
  - OVER PARTITION
  - rank
  - lead lag
  - running total
  - pivot
  - unpivot
  - query optimization
  - explain plan
---

# Spark SQL & DataFrame API Mastery

## Filter vs Row Context (Spark Equivalent of DAX Context)

In Spark, every transformation is stateless — the "context" is always explicit in your query. Use Window specs to define partitioning context for analytical calculations.

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Window spec = Spark's "context transition"
w_customer = Window.partitionBy("customer_id").orderBy("order_date")
w_all      = Window.partitionBy()  # global window

df = df.withColumn("cumulative_revenue", F.sum("revenue").over(w_customer))
df = df.withColumn("pct_of_total",       F.col("revenue") / F.sum("revenue").over(w_all))
```

## CALCULATE Equivalent: Conditional Aggregations

```python
# Equivalent of CALCULATE(SUM(revenue), region = "US")
df.groupBy("period").agg(
    F.sum(F.when(F.col("region") == "US", F.col("revenue")).otherwise(0)).alias("us_revenue"),
    F.sum(F.when(F.col("region") == "EU", F.col("revenue")).otherwise(0)).alias("eu_revenue"),
    F.countDistinct(F.when(F.col("is_new_customer"), F.col("customer_id"))).alias("new_customers"),
)
```

```sql
-- SQL equivalent
SELECT
  period,
  SUM(CASE WHEN region = 'US' THEN revenue ELSE 0 END) AS us_revenue,
  SUM(CASE WHEN region = 'EU' THEN revenue ELSE 0 END) AS eu_revenue,
  COUNT(DISTINCT CASE WHEN is_new_customer THEN customer_id END) AS new_customers
FROM orders
GROUP BY period
```

## Time Intelligence Patterns

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Year-to-Date (YTD)
w_ytd = (Window.partitionBy(F.year("order_date"))
               .orderBy("order_date")
               .rowsBetween(Window.unboundedPreceding, Window.currentRow))
df = df.withColumn("revenue_ytd", F.sum("revenue").over(w_ytd))

# Month-to-Date (MTD)
w_mtd = (Window.partitionBy(F.year("order_date"), F.month("order_date"))
               .orderBy("order_date")
               .rowsBetween(Window.unboundedPreceding, Window.currentRow))
df = df.withColumn("revenue_mtd", F.sum("revenue").over(w_mtd))

# Year-over-Year (YoY) — join to prior year
df_prior = df.withColumn("order_date", F.date_add("order_date", 365)).withColumnRenamed("revenue", "revenue_prior_year")
df_yoy = df.join(df_prior, ["order_date", "customer_id"], "left")
df_yoy = df_yoy.withColumn("yoy_growth", (F.col("revenue") - F.col("revenue_prior_year")) / F.col("revenue_prior_year"))
```

```sql
-- SQL: rolling 30-day revenue
SELECT
  order_date,
  SUM(revenue) OVER (
    ORDER BY order_date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS rolling_30d_revenue
FROM daily_revenue
ORDER BY order_date
```

## Running Totals & Rankings

```python
# Running total
w = Window.partitionBy("region").orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df = df.withColumn("running_total", F.sum("revenue").over(w))

# Rank with ties (dense_rank vs rank vs row_number)
w_rank = Window.partitionBy("category").orderBy(F.desc("revenue"))
df = (df
    .withColumn("rank",       F.rank().over(w_rank))         # gaps after ties
    .withColumn("dense_rank", F.dense_rank().over(w_rank))   # no gaps
    .withColumn("row_num",    F.row_number().over(w_rank))   # unique, no ties
)

# Top-N per group
top3_per_category = df.filter(F.col("dense_rank") <= 3)
```

## Pareto Analysis (80/20)

```python
w_all = Window.orderBy(F.desc("revenue"))
w_running = Window.orderBy(F.desc("revenue")).rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_pareto = (df.groupBy("product_id").agg(F.sum("revenue").alias("revenue"))
    .withColumn("pct_of_total", F.col("revenue") / F.sum("revenue").over(Window.partitionBy()))
    .withColumn("cumulative_pct", F.sum("pct_of_total").over(w_running))
    .withColumn("in_top_80pct", F.col("cumulative_pct") <= 0.80)
)
```

## LAG / LEAD (Period-over-Period)

```python
w = Window.partitionBy("customer_id").orderBy("order_date")

df = (df
    .withColumn("prev_order_date",    F.lag("order_date", 1).over(w))
    .withColumn("next_order_date",    F.lead("order_date", 1).over(w))
    .withColumn("days_since_last",    F.datediff("order_date", F.col("prev_order_date")))
    .withColumn("revenue_mom_change", F.col("revenue") - F.lag("revenue", 1).over(w))
)
```

## Dynamic Segmentation

```python
# RFM Segmentation
from pyspark.sql import functions as F

df_rfm = df.groupBy("customer_id").agg(
    F.datediff(F.current_date(), F.max("order_date")).alias("recency"),
    F.count("order_id").alias("frequency"),
    F.sum("revenue").alias("monetary")
)

# Bucket into quartiles
for metric in ["recency", "frequency", "monetary"]:
    df_rfm = df_rfm.withColumn(f"{metric}_score",
        F.ntile(4).over(Window.orderBy(metric if metric == "recency" else F.desc(metric)))
    )

df_rfm = df_rfm.withColumn("rfm_score",
    (F.col("recency_score") + F.col("frequency_score") + F.col("monetary_score")) / 3
)
```

## CTEs and Subqueries

```sql
-- Readable multi-step transforms via CTEs
WITH base AS (
  SELECT customer_id, order_date, revenue
  FROM silver.orders
  WHERE order_date >= '2024-01-01'
),
monthly_agg AS (
  SELECT
    customer_id,
    DATE_TRUNC('month', order_date) AS month,
    SUM(revenue) AS monthly_revenue
  FROM base
  GROUP BY 1, 2
),
ranked AS (
  SELECT *,
    RANK() OVER (PARTITION BY month ORDER BY monthly_revenue DESC) AS rank
  FROM monthly_agg
)
SELECT * FROM ranked WHERE rank <= 10
```

## PIVOT / UNPIVOT

```python
# Pivot: rows → columns
df_pivot = (df.groupBy("region")
    .pivot("product_category", ["Electronics", "Apparel", "Home"])
    .agg(F.sum("revenue"))
)

# Unpivot: columns → rows (stack)
from pyspark.sql.functions import expr
df_unpivot = df_pivot.select(
    "region",
    expr("stack(3, 'Electronics', Electronics, 'Apparel', Apparel, 'Home', Home) AS (category, revenue)")
)
```

## Query Performance Rules

1. **Predicate pushdown first** — filter early, before joins
2. **Avoid UDFs** on large tables — use native Spark functions instead
3. **Cache sparingly** — only for DataFrames used 3+ times in the same notebook
4. **Use `EXPLAIN FORMATTED`** to understand query plans before running on large data
5. **Avoid `count()` in loops** — collect counts in a single pass with `agg()`
6. **Prefer `select()` over `withColumn()`** for many columns — avoids plan explosion

```python
# Check execution plan
df.explain(mode="formatted")  # "simple", "extended", "codegen", "cost", "formatted"

# Cache when reused
df_silver.cache()
df_silver.count()  # trigger materialization

# Release when done
df_silver.unpersist()
```

## CLI Reference

```bash
databricks-agent sql query --sql "SELECT * FROM catalog.schema.table LIMIT 100"  # Run query
databricks-agent sql query --file my_query.sql --warehouse my-warehouse           # Run from file
databricks-agent sql explain --sql "SELECT ..." --warehouse my-warehouse          # EXPLAIN plan
databricks-agent sql history --warehouse my-warehouse --limit 50                  # Recent queries
```
