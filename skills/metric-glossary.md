---
name: databricks-metric-glossary
description: Metric definitions, dbt metrics, Unity Catalog documentation, Databricks AI/BI Semantic Layer, business glossary, and metric dependency tracking
triggers:
  - metric documentation
  - measure documentation
  - business glossary
  - dbt metrics
  - semantic layer
  - metric definition
  - KPI definition
  - dependencies
  - lineage
  - data dictionary
  - metric catalog
  - undocumented tables
  - metric audit
---

# Metric Glossary & Semantic Layer in Databricks

## Three Documentation Layers

Every metric needs three levels of documentation:

1. **Technical** — SQL definition, source tables, grain
2. **Business** — plain-language definition, owner, SLA
3. **Governance** — lineage, quality SLA, certified status

## Documenting Metrics in Unity Catalog

```sql
-- Step 1: Create a metrics view with full documentation
CREATE OR REPLACE VIEW catalog.gold.v_monthly_revenue
COMMENT 'Monthly revenue by region. Grain: one row per region per calendar month. 
Owner: analytics@company.com. SLA: updated by 6 AM UTC on 1st of month. 
Source: silver.orders. Do not use for P&L — use gold.v_net_revenue which excludes refunds.'
AS
SELECT
  DATE_TRUNC('month', order_date)  AS month,
  region,
  SUM(revenue)                     AS gross_revenue,
  COUNT(DISTINCT order_id)         AS order_count,
  COUNT(DISTINCT customer_id)      AS unique_customers
FROM catalog.silver.orders
WHERE order_status != 'cancelled'
GROUP BY 1, 2;

-- Step 2: Tag the view
ALTER VIEW catalog.gold.v_monthly_revenue
SET TAGS ('domain' = 'revenue', 'endorsement' = 'certified', 'team' = 'analytics');
```

## dbt Metrics (Semantic Layer)

Define metrics in dbt's semantic layer so any BI tool can query them consistently:

```yaml
# models/metrics/revenue_metrics.yml
semantic_models:
  - name: orders
    description: "Core orders fact table. Grain: one row per order."
    model: ref('silver_orders')
    entities:
      - name: order
        type: primary
        expr: order_id
      - name: customer
        type: foreign
        expr: customer_id
    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
      - name: region
        type: categorical
    measures:
      - name: gross_revenue
        agg: sum
        expr: revenue
      - name: order_count
        agg: count_distinct
        expr: order_id

metrics:
  - name: monthly_revenue
    label: "Monthly Gross Revenue"
    description: "Total revenue from completed orders, grouped by month. Excludes refunds."
    type: simple
    type_params:
      measure: gross_revenue
    filter: "{{ Dimension('order__order_status') }} != 'cancelled'"

  - name: revenue_growth_mom
    label: "Revenue Month-over-Month Growth %"
    description: "Percentage change in gross revenue vs prior month."
    type: ratio
    type_params:
      numerator:
        name: monthly_revenue
      denominator:
        name: monthly_revenue
        offset_window: 1 month
```

```bash
# CLI: list and document metrics
databricks-agent catalog metrics list --catalog my_catalog
databricks-agent catalog metrics describe --name monthly_revenue
```

## Batch Documentation Generation

```bash
# Generate a full data dictionary as Markdown
databricks-agent catalog document --schema catalog.gold --output docs/gold_dictionary.md

# Generate HTML glossary
databricks-agent catalog document --schema catalog.gold --format html --output docs/

# Export to CSV for stakeholder review
databricks-agent catalog document --schema catalog.gold --format csv --output metrics_audit.csv
```

```python
# Programmatic: generate documentation for all tables
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

tables = w.tables.list(catalog_name="my_catalog", schema_name="gold")
for table in tables:
    print(f"Table: {table.full_name}")
    print(f"  Comment: {table.comment or '⚠️ UNDOCUMENTED'}")
    print(f"  Owner: {table.owner}")
    print(f"  Created: {table.created_at}")
```

## Dependency Analysis

```bash
# Find all metrics that depend on a source table
databricks-agent catalog dependencies --table catalog.silver.orders

# Show full dependency graph
databricks-agent catalog lineage --table catalog.gold.v_monthly_revenue --upstream --depth 3
```

```python
# Show column-level dependencies
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

col_lineage = w.lineage_tracking.column_lineage(
    table_name="catalog.gold.v_monthly_revenue",
    column_name="gross_revenue"
)
for upstream in col_lineage.upstream_cols:
    print(f"  ← {upstream.table_name}.{upstream.name}")
```

## Quality Audit Checklist

```bash
# Find undocumented tables (no comment)
databricks-agent catalog audit --schema catalog.gold --check undocumented

# Find tables with no owner set
databricks-agent catalog audit --schema catalog.gold --check no-owner

# Find PII columns without masking policies
databricks-agent catalog audit --check pii-unmasked --catalog my_catalog

# Full governance report
databricks-agent catalog audit --catalog my_catalog --output governance_report.md
```

```python
# Audit: list all tables missing documentation
import pandas as pd
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
tables = list(w.tables.list(catalog_name="my_catalog", schema_name="gold"))

audit_rows = []
for t in tables:
    audit_rows.append({
        "table": t.full_name,
        "has_comment": bool(t.comment),
        "has_owner": bool(t.owner),
        "tags": t.properties or {},
        "endorsed": t.properties.get("endorsement", "none") if t.properties else "none"
    })

df_audit = pd.DataFrame(audit_rows)
print(df_audit[~df_audit["has_comment"]])  # Undocumented tables
```

## Standard Metric Template

Every metric definition must include:

```markdown
## [Metric Name]
**Label**: Human-readable name shown in dashboards
**Owner**: team@company.com
**SLA**: Updated by [time] on [schedule]
**Grain**: One row per [dimension(s)]
**Definition**: Plain-language description — what is included, what is excluded
**SQL Definition**:
```sql
SELECT ... FROM ... WHERE ...
```
**Source Tables**: `catalog.schema.table1`, `catalog.schema.table2`
**Endorsed**: Certified / Promoted / None
**Tags**: domain=[X], pii=[true/false]
**Known Limitations**: Any caveats, edge cases, or gotchas
```

## CLI Reference

```bash
databricks-agent catalog metrics list --catalog my_catalog         # List all metrics
databricks-agent catalog metrics describe --name monthly_revenue   # Describe a metric
databricks-agent catalog document --schema catalog.gold            # Generate docs
databricks-agent catalog audit --schema catalog.gold               # Governance audit
databricks-agent catalog dependencies --table catalog.silver.orders # Dependency map
```
