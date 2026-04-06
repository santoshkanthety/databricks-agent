---
name: databricks-security-governance
description: Unity Catalog row filters, column masks, table ACLs, data masking, service principals, workspace access, PII governance, and audit logging
triggers:
  - row filter
  - column mask
  - data masking
  - RLS
  - row-level security
  - column-level security
  - access control
  - permissions
  - ACL
  - service principal
  - PII
  - sensitive data
  - audit log
  - governance
  - Unity Catalog security
  - GRANT
  - REVOKE
---

# Security & Governance in Databricks

## Unity Catalog Security Model

```
Account Admin
  └── Metastore Admin
        └── Catalog Owner / Catalog Admin
              └── Schema Owner / Schema Admin
                    └── Table Owner + GRANT permissions
```

## Table-Level ACLs

```sql
-- Grant read access to a group
GRANT SELECT ON TABLE catalog.gold.revenue_summary TO `analytics-team`;

-- Grant full access to data engineers
GRANT ALL PRIVILEGES ON SCHEMA catalog.silver TO `data-engineering`;

-- Grant access to a service principal (for jobs/automation)
GRANT SELECT ON TABLE catalog.gold.revenue_summary TO `service-principal-id`;

-- Revoke access
REVOKE SELECT ON TABLE catalog.gold.revenue_summary FROM `analytics-team`;

-- Inspect current permissions
SHOW GRANTS ON TABLE catalog.gold.revenue_summary;
SHOW GRANTS TO `analytics-team`;
```

```bash
# CLI: manage permissions
databricks-agent catalog grant --table catalog.gold.revenue_summary --group analytics-team --privilege SELECT
databricks-agent catalog revoke --table catalog.gold.revenue_summary --group analytics-team --privilege SELECT
databricks-agent catalog show-grants --table catalog.gold.revenue_summary
```

## Row Filters (Unity Catalog — DBR 12.2+)

Row filters restrict which rows a user or group can see. Applied at query time, transparently.

```sql
-- Create a row filter function
CREATE OR REPLACE FUNCTION catalog.security.orders_by_region(region_col STRING)
RETURNS BOOLEAN
RETURN
  is_account_admin()                       -- admins see all
  OR is_member('global-analytics')         -- global team sees all
  OR region_col = current_user_region();   -- others see only their region

-- Custom helper: get user's region from a mapping table
CREATE OR REPLACE FUNCTION catalog.security.current_user_region()
RETURNS STRING
RETURN (
  SELECT region FROM catalog.security.user_region_map
  WHERE user_email = current_user()
  LIMIT 1
);

-- Apply the row filter to a table
ALTER TABLE catalog.silver.orders
  SET ROW FILTER catalog.security.orders_by_region ON (region);

-- Remove row filter
ALTER TABLE catalog.silver.orders DROP ROW FILTER;

-- Test: what does the filter return for current user?
SELECT * FROM catalog.silver.orders LIMIT 10;  -- should only show your region
```

```bash
# CLI: apply row filter
databricks-agent catalog row-filter --table catalog.silver.orders \
  --function catalog.security.orders_by_region --columns region
```

## Column Masks (Dynamic Data Masking)

Column masks hide or transform sensitive column values based on the user's identity or group membership.

```sql
-- Mask: show full email only to data engineers, otherwise show masked version
CREATE OR REPLACE FUNCTION catalog.security.mask_email(email STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_member('data-engineering') THEN email
    ELSE CONCAT(LEFT(email, 2), '***@***.com')
  END;

-- Mask: credit card — show only last 4 digits
CREATE OR REPLACE FUNCTION catalog.security.mask_credit_card(card STRING)
RETURNS STRING
RETURN CONCAT('****-****-****-', RIGHT(card, 4));

-- Mask: null out salary for non-HR members
CREATE OR REPLACE FUNCTION catalog.security.mask_salary(salary DOUBLE)
RETURNS DOUBLE
RETURN IF(is_member('hr-team'), salary, NULL);

-- Apply masks to table columns
ALTER TABLE catalog.silver.customers
  ALTER COLUMN email   SET MASK catalog.security.mask_email;
ALTER TABLE catalog.silver.customers
  ALTER COLUMN card_number SET MASK catalog.security.mask_credit_card;
ALTER TABLE catalog.silver.employees
  ALTER COLUMN salary SET MASK catalog.security.mask_salary;

-- Verify: check column metadata
DESCRIBE EXTENDED catalog.silver.customers email;
```

## PII Classification Workflow

```sql
-- Step 1: Tag all PII columns
ALTER TABLE catalog.silver.customers
  ALTER COLUMN email          SET TAGS ('pii' = 'true', 'pii_type' = 'email');
ALTER TABLE catalog.silver.customers
  ALTER COLUMN phone          SET TAGS ('pii' = 'true', 'pii_type' = 'phone');
ALTER TABLE catalog.silver.customers
  ALTER COLUMN date_of_birth  SET TAGS ('pii' = 'true', 'pii_type' = 'dob', 'classification' = 'sensitive');

-- Step 2: Apply masks to all tagged columns (query to find unmasked PII)
SELECT table_name, column_name, tag_value
FROM catalog.information_schema.column_tags
WHERE tag_name = 'pii' AND tag_value = 'true';
```

```bash
# Audit: find PII columns without masks applied
databricks-agent catalog audit --check pii-unmasked --catalog my_catalog
```

## Service Principals (Automation Auth)

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import ServicePrincipal

w = WorkspaceClient()

# Create a service principal
sp = w.service_principals.create(display_name="etl-pipeline-sp")
print(f"Service Principal ID: {sp.id}")

# Grant the SP access to a catalog
w.grants.update(
    full_name="my_catalog",
    securables_type="catalog",
    changes=[{"principal": str(sp.id), "add": ["USE_CATALOG", "USE_SCHEMA", "SELECT"]}]
)
```

```bash
# CLI: manage service principals
databricks-agent catalog service-principals list
databricks-agent catalog grant --service-principal etl-pipeline-sp \
  --catalog my_catalog --privilege USE_CATALOG,USE_SCHEMA,SELECT
```

## Workspace Access Control

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import Group

w = WorkspaceClient()

# Create groups
analysts = w.groups.create(display_name="analysts")
engineers = w.groups.create(display_name="data-engineering")

# Add users to groups
w.groups.patch(group_id=analysts.id, operations=[{
    "op": "add",
    "path": "members",
    "value": [{"value": "user-id-here"}]
}])
```

## Audit Logging

All Unity Catalog access is logged to the system tables (enabled by default on DBR 12.0+):

```sql
-- Query audit log for table access events
SELECT
  event_time,
  user_identity.email AS user,
  request_params.table_full_name AS table_accessed,
  action_name
FROM system.access.audit
WHERE action_name IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
  AND event_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
ORDER BY event_time DESC
LIMIT 100;

-- Find who accessed PII tables
SELECT user_identity.email, COUNT(*) AS access_count
FROM system.access.audit
WHERE request_params.table_full_name LIKE '%customers%'
  AND event_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC;

-- Failed access attempts (403s)
SELECT event_time, user_identity.email, request_params, status_code
FROM system.access.audit
WHERE status_code = 403
  AND event_time >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
ORDER BY event_time DESC;
```

```bash
# CLI: audit report
databricks-agent catalog audit --schema catalog.silver --report security --days 30
```

## Security Checklist

- [ ] All production tables have explicit GRANT statements (no `ALL PRIVILEGES` for end users)
- [ ] PII columns tagged (`pii=true`) and masks applied
- [ ] Row filters applied for region/tenant isolation
- [ ] Service principals used for jobs (no personal PAT tokens in job configs)
- [ ] Audit log queries set up in a dashboard
- [ ] Unity Catalog system tables enabled
- [ ] No credentials stored in notebooks (use `dbutils.secrets.get()`)

## Secrets Management

```python
# Never hardcode credentials — use Databricks Secrets
host     = dbutils.secrets.get(scope="my-scope", key="jdbc-host")
password = dbutils.secrets.get(scope="my-scope", key="jdbc-password")

# Create secrets via CLI
# databricks secrets create-scope --scope my-scope
# databricks secrets put-secret --scope my-scope --key jdbc-host
```

## CLI Reference

```bash
databricks-agent catalog grant --table t --group g --privilege SELECT
databricks-agent catalog revoke --table t --group g --privilege SELECT
databricks-agent catalog show-grants --table catalog.silver.orders
databricks-agent catalog row-filter --table t --function fn --columns col
databricks-agent catalog audit --schema catalog.silver --check pii-unmasked
databricks-agent catalog service-principals list
```
