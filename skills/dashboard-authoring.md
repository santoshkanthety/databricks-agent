---
name: databricks-dashboard-authoring
description: Databricks AI/BI Lakeview dashboards, DBSQL visualizations, design principles, interactivity, layout, and accessibility
triggers:
  - dashboard
  - visualization
  - chart
  - report
  - lakeview
  - DBSQL
  - sql dashboard
  - filters
  - parameters
  - drilldown
  - KPI widget
  - counter
  - heatmap
  - AI/BI
  - design
  - layout
---

# Dashboard Authoring in Databricks

## Dashboard Types

| Type | Use Case | Access |
|------|---------|--------|
| **Lakeview (AI/BI Dashboards)** | Modern, pixel-perfect, AI-assisted | Workspace → Dashboards |
| **Legacy DBSQL Dashboards** | Simple, widget-based | Workspace → SQL → Dashboards |
| **Databricks Apps** | Full web apps with custom code | Apps section |

**Recommendation**: Use **Lakeview Dashboards** for all new work. Legacy DBSQL dashboards are being deprecated.

## Lakeview Dashboard Design Principles

### Layout by Audience

**Executive (C-suite) Dashboard:**
- 3–5 KPI counters at the top (revenue, growth, churn)
- 1–2 trend lines (time series)
- No more than 2 filters
- Mobile-friendly layout
- No raw data tables

**Analyst / Operational Dashboard:**
- Rich filters and parameters (date range, region, product)
- Drilldown capability
- Data tables with sorting/export
- 6–10 widgets acceptable
- Clear data freshness indicator

**F-pattern layout (eyes scan left-to-right, top-to-bottom):**
```
[ KPI 1 ] [ KPI 2 ] [ KPI 3 ]
[       Revenue Trend (wide)  ]
[ Top Products ] [ Region Map ]
[ Detail Table               ]
```

## Writing Dashboard Queries

```sql
-- Parameterized query for Lakeview (use {{ parameter_name }} syntax)
SELECT
  DATE_TRUNC('{{ time_grain }}', order_date)  AS period,
  region,
  SUM(revenue)                                AS total_revenue,
  COUNT(DISTINCT customer_id)                 AS unique_customers
FROM catalog.gold.orders
WHERE
  order_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
  AND (region = '{{ region }}' OR '{{ region }}' = 'All')
GROUP BY 1, 2
ORDER BY 1, 2
```

```bash
# CLI: list dashboards in workspace
databricks-agent dashboards list

# Open a dashboard for editing
databricks-agent dashboards open --name "Revenue Overview"

# Export dashboard definition
databricks-agent dashboards export --name "Revenue Overview" --output dashboard.json
```

## KPI Counter Widget

Best for single-number metrics at the top of any dashboard:

```sql
-- Query for a KPI counter
SELECT
  SUM(revenue)                                              AS current_revenue,
  SUM(revenue) - LAG(SUM(revenue)) OVER (ORDER BY month)   AS revenue_delta,
  (SUM(revenue) - LAG(SUM(revenue)) OVER (ORDER BY month))
    / LAG(SUM(revenue)) OVER (ORDER BY month)              AS pct_change
FROM catalog.gold.monthly_revenue
WHERE month = DATE_TRUNC('month', CURRENT_DATE())
```

Widget config:
- **Value field**: `current_revenue`
- **Comparison field**: `pct_change`
- **Format**: `$#,##0.00` for currency, `0.0%` for percentage
- **Positive = Good**: green for revenue, red for churn

## Time Series Chart

```sql
SELECT
  order_date                    AS date,
  SUM(revenue)                  AS revenue,
  AVG(SUM(revenue)) OVER (
    ORDER BY order_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  )                             AS revenue_7d_avg
FROM catalog.gold.daily_revenue
WHERE order_date >= DATEADD(DAY, -90, CURRENT_DATE())
GROUP BY 1
ORDER BY 1
```

Chart config:
- X-axis: `date` (time series)
- Y-axis 1: `revenue` (bar)
- Y-axis 2: `revenue_7d_avg` (line, secondary axis)
- Legend: visible
- Annotations: mark goal/target line

## Heatmap (Day × Hour Activity)

```sql
SELECT
  DAYOFWEEK(event_ts)  AS day_of_week,
  HOUR(event_ts)       AS hour_of_day,
  COUNT(*)             AS event_count
FROM catalog.silver.user_events
WHERE event_date >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1, 2
```

## Drilldown / Linked Dashboards

In Lakeview, link widgets to other dashboards via parameters:
1. Set a parameter on the target dashboard (e.g., `customer_id`)
2. In source dashboard → widget → **Link action** → select target dashboard + pass `customer_id` as parameter

## Filters and Parameters

```sql
-- Date range parameter
-- Parameter name: date_range, Type: date range
WHERE order_date BETWEEN {{ date_range.start }} AND {{ date_range.end }}

-- Multi-select dropdown
-- Parameter name: regions, Type: multi-select
WHERE region IN ({{ regions }})

-- Text search
-- Parameter name: search_term
WHERE LOWER(customer_name) LIKE LOWER('%{{ search_term }}%')
```

## Data Freshness Indicator

Always show consumers when data was last updated:

```sql
SELECT
  MAX(_processed_at)          AS last_updated,
  DATEDIFF(MINUTE, MAX(_processed_at), CURRENT_TIMESTAMP()) AS minutes_ago
FROM catalog.silver.orders
```

Add as a text widget: `Last updated: {{ last_updated }} ({{ minutes_ago }}m ago)`

## Accessibility & Quality Standards

- [ ] All charts have descriptive titles (not just metric names)
- [ ] Color palette is colorblind-safe (avoid red/green only)
- [ ] KPI counters show units (%, $, users — not bare numbers)
- [ ] Tooltips enabled on all charts
- [ ] Date format is unambiguous (2025-01-15, not 01/15/25)
- [ ] No more than 7 colors in a single chart (cognitive load)
- [ ] Null handling visible (show "No data" not empty)
- [ ] Filter defaults are meaningful (not empty/"Select all" causing full-table scan)

## Sharing & Permissions

```bash
# Share a dashboard
databricks-agent dashboards share --name "Revenue Overview" --group analytics-team --permission CAN_VIEW

# Publish for embed
databricks-agent dashboards publish --name "Revenue Overview"
```

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import PermissionLevel

w = WorkspaceClient()
dashboard = w.dashboards.get_by_name("Revenue Overview")
w.dashboards.set_permissions(
    dashboard_id=dashboard.id,
    access_control_list=[{"group_name": "analysts", "permission_level": PermissionLevel.CAN_VIEW}]
)
```

## CLI Reference

```bash
databricks-agent dashboards list                          # List all dashboards
databricks-agent dashboards open --name "My Dashboard"   # Open in browser
databricks-agent dashboards export --name "My Dashboard" # Export to JSON
databricks-agent dashboards share --name "My Dashboard" --group team --permission CAN_VIEW
databricks-agent warehouses list                          # List SQL warehouses for dashboards
```
