---
name: databricks-data-catalog-lineage
description: Unity Catalog lineage, data governance, column-level lineage, tagging, endorsement, and impact analysis in Databricks
triggers:
  - data catalog
  - lineage
  - data governance
  - unity catalog
  - metadata
  - endorsement
  - sensitive data
  - column lineage
  - data discovery
  - impact analysis
  - data dictionary
  - table tags
  - purview
---

# Data Catalog & Lineage in Databricks

## Unity Catalog as the Governance Layer

Databricks Unity Catalog is your single governance layer for all data assets. Every table, view, function, volume, and model lives in a three-level namespace: `catalog.schema.table`.

## Scanning & Discovering Assets

```python
# List all tables in a schema
spark.sql("SHOW TABLES IN catalog_name.schema_name").show()

# Search across Unity Catalog with INFORMATION_SCHEMA
spark.sql("""
  SELECT table_catalog, table_schema, table_name, table_type, created_by, last_altered
  FROM catalog_name.information_schema.tables
  WHERE table_schema = 'bronze'
  ORDER BY last_altered DESC
""").show()

# Get column metadata
spark.sql("""
  SELECT table_name, column_name, data_type, is_nullable, comment
  FROM catalog_name.information_schema.columns
  WHERE table_schema = 'silver'
""").show()
```

## Column-Level Lineage

Unity Catalog automatically captures column-level lineage for SQL and DataFrame operations. Access it via the Databricks UI or API:

```bash
# CLI: view table lineage
databricks-agent catalog lineage --table catalog.schema.table_name

# API: fetch lineage graph
databricks-agent catalog lineage --table catalog.schema.table_name --format json
```

To expose lineage programmatically:

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Get upstream lineage
lineage = w.lineage_tracking.table_lineage(table_name="catalog.schema.my_table")
for upstream in lineage.upstreams:
    print(f"Upstream: {upstream.table_info.name}")
```

## Tagging Assets

Tags are key-value pairs on catalogs, schemas, tables, and columns.

```sql
-- Tag a table
ALTER TABLE catalog.schema.customers SET TAGS ('domain' = 'crm', 'pii' = 'true', 'tier' = 'gold');

-- Tag a column
ALTER TABLE catalog.schema.customers ALTER COLUMN email SET TAGS ('pii' = 'true', 'classification' = 'confidential');

-- Tag a schema
ALTER SCHEMA catalog.schema SET TAGS ('team' = 'data-engineering', 'env' = 'production');

-- List tags
SHOW TAGS ON TABLE catalog.schema.customers;
```

```bash
# CLI: apply tags in bulk
databricks-agent catalog tag --table catalog.schema.customers --tags domain=crm,pii=true
```

## Table & Column Comments (Business Glossary)

```sql
-- Document a table
COMMENT ON TABLE catalog.schema.orders IS 'One row per customer order. Grain: order_id. Owner: data-engineering@company.com. SLA: refreshed hourly.';

-- Document columns
ALTER TABLE catalog.schema.orders ALTER COLUMN order_status COMMENT 'Current order state. Values: pending, confirmed, shipped, delivered, cancelled. Updated on every state transition.';

ALTER TABLE catalog.schema.orders ALTER COLUMN order_total COMMENT 'Total invoice amount in USD including tax. Excludes refunds. Use net_revenue for P&L reporting.';
```

```bash
# Generate a full data dictionary for a schema
databricks-agent catalog document --schema catalog.schema --output data_dictionary.md
```

## Endorsement Workflow (Data Products)

Databricks supports two endorsement levels in Unity Catalog UI, and you can encode this as tags:

```sql
-- Mark a table as promoted (validated by team)
ALTER TABLE catalog.gold.revenue_summary SET TAGS ('endorsement' = 'promoted', 'certified_by' = 'analytics-team', 'certified_date' = '2025-01-15');

-- Mark as certified (org-wide trusted asset)
ALTER TABLE catalog.gold.revenue_summary SET TAGS ('endorsement' = 'certified', 'sla' = '99.9', 'support_contact' = 'data-platform@company.com');
```

## Impact Analysis

Before modifying upstream tables, check downstream dependencies:

```bash
# Find all tables that read from a source table
databricks-agent catalog impact --table catalog.bronze.raw_orders

# Find all jobs/notebooks that reference a table
databricks-agent catalog impact --table catalog.bronze.raw_orders --include-jobs
```

```python
# Programmatic impact analysis via SDK
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

lineage = w.lineage_tracking.table_lineage(table_name="catalog.bronze.raw_orders")
print("Downstream tables:")
for ds in lineage.downstreams:
    print(f"  {ds.table_info.name}")
```

## Governance Maturity Model

| Level | Capability |
|-------|-----------|
| 1 – Aware | Tables registered in Unity Catalog, basic ACLs |
| 2 – Defined | Column comments, table owners, schema tags |
| 3 – Managed | Full lineage captured, PII columns tagged, row filters active |
| 4 – Optimized | Certified data products, SLA-tracked, automated quality gates |

## Sensitive Data Patterns

```sql
-- Dynamic data masking for PII columns (Unity Catalog)
CREATE OR REPLACE FUNCTION catalog.security.mask_email(email STRING)
RETURN CASE WHEN is_member('data-engineers') THEN email
            ELSE CONCAT(LEFT(email, 2), '***@***.com') END;

ALTER TABLE catalog.silver.customers ALTER COLUMN email
  SET MASK catalog.security.mask_email;
```

```bash
# Audit: find all unmasked PII columns
databricks-agent catalog audit --tag pii=true --check-masks
```

## CLI Reference

```bash
databricks-agent catalog list --schema catalog.schema          # List all tables
databricks-agent catalog describe --table catalog.schema.name  # Full table metadata
databricks-agent catalog lineage --table catalog.schema.name   # Show lineage graph
databricks-agent catalog tag --table catalog.schema.name --tags key=value
databricks-agent catalog document --schema catalog.schema      # Generate data dictionary
databricks-agent catalog impact --table catalog.schema.name    # Downstream impact analysis
databricks-agent catalog audit --schema catalog.schema         # Governance audit
```
