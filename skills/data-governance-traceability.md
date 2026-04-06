---
name: databricks-data-governance-traceability
description: End-to-end data governance traceability in Databricks - regulatory compliance (GDPR, CCPA, HIPAA), data lineage chains, retention policies, classification, consent tracking, and audit-ready evidence
triggers:
  - governance traceability
  - data lineage chain
  - GDPR
  - CCPA
  - HIPAA
  - data retention
  - right to erasure
  - data subject request
  - DSR
  - consent tracking
  - regulatory compliance
  - data classification
  - impact assessment
  - DPIA
  - breach detection
  - data residency
  - sovereignty
  - cross-border transfer
  - traceability report
  - evidence package
---

# Data Governance & Traceability in Databricks

## The Traceability Stack

Full traceability means you can answer: *"For any piece of data — where did it come from, where did it go, who touched it, and can I prove it?"*

```
Source System
  └─ Bronze (raw ingest — Unity Catalog lineage captured)
       └─ Silver (transform — column-level lineage tracked)
            └─ Gold (aggregate — metrics traced to source columns)
                 └─ Dashboard / ML Model — consumer recorded
```

Every hop is captured by Unity Catalog's lineage engine automatically for SQL and DataFrame operations.

## Data Classification Framework

```python
# Classification taxonomy — apply as Unity Catalog tags
CLASSIFICATION_LEVELS = {
    "public":        {"description": "No restrictions",           "retention_years": 3},
    "internal":      {"description": "Internal use only",         "retention_years": 5},
    "confidential":  {"description": "Business sensitive",        "retention_years": 7},
    "restricted":    {"description": "PII / regulated data",      "retention_years": 10},
    "critical":      {"description": "Financial / health records", "retention_years": 10},
}

# Apply classification to a table
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

w.tables.update(
    full_name="catalog.silver.customers",
    comment="Customer PII. Classification: restricted. Contains GDPR-regulated personal data.",
)

# Tag with classification metadata
spark.sql("""
  ALTER TABLE catalog.silver.customers SET TAGS (
    'data_classification' = 'restricted',
    'contains_pii'        = 'true',
    'gdpr_applicable'     = 'true',
    'data_owner'          = 'privacy@company.com',
    'retention_years'     = '7',
    'review_date'         = '2026-01-01'
  )
""")
```

## GDPR Right-to-Erasure (Right to be Forgotten)

```python
from delta.tables import DeltaTable
from databricks.sdk import WorkspaceClient

def erase_subject_data(subject_id: str, catalog: str = "prod") -> dict:
    """
    Execute GDPR erasure request across all tables containing a subject's data.
    Returns evidence package for compliance audit.
    """
    w = WorkspaceClient()
    evidence = {"subject_id": subject_id, "request_ts": str(spark.sql("SELECT current_timestamp()").collect()[0][0]), "tables_affected": []}

    # Tables containing this subject's data (pre-mapped in governance registry)
    subject_tables = spark.sql(f"""
        SELECT full_table_name, subject_key_column
        FROM {catalog}.ops.pii_table_registry
        WHERE subject_type = 'customer'
    """).collect()

    for row in subject_tables:
        table_name = row["full_table_name"]
        key_col    = row["subject_key_column"]

        # Count records before deletion
        count_before = spark.sql(f"SELECT COUNT(*) FROM {table_name} WHERE {key_col} = '{subject_id}'").collect()[0][0]

        if count_before > 0:
            # Delete from Delta table
            dt = DeltaTable.forName(spark, table_name)
            dt.delete(f"{key_col} = '{subject_id}'")

            evidence["tables_affected"].append({
                "table": table_name,
                "records_deleted": count_before,
                "deleted_at": str(spark.sql("SELECT current_timestamp()").collect()[0][0])
            })
            print(f"  ✓ Deleted {count_before} records from {table_name}")

    # Log erasure event to immutable audit table
    spark.createDataFrame([evidence]).write.format("delta").mode("append").saveAsTable(f"{catalog}.ops.erasure_audit_log")

    return evidence

# Usage
evidence = erase_subject_data(subject_id="CUST-98765", catalog="prod")
print(f"Erasure complete. {len(evidence['tables_affected'])} tables affected.")
```

```bash
# CLI: erasure request
databricks-agent catalog erasure --subject-id CUST-98765 --catalog prod --evidence-output erasure_evidence.json
```

## Data Subject Access Request (DSAR / SAR)

```python
def generate_dsar_report(subject_id: str, catalog: str = "prod") -> str:
    """Compile all data held about a subject for a Subject Access Request."""
    report_parts = []

    # Query all tables where this subject appears
    subject_tables = spark.sql(f"""
        SELECT full_table_name, subject_key_column
        FROM {catalog}.ops.pii_table_registry
        WHERE subject_type = 'customer'
    """).collect()

    for row in subject_tables:
        df = spark.sql(f"""
            SELECT * FROM {row['full_table_name']}
            WHERE {row['subject_key_column']} = '{subject_id}'
        """)
        if df.count() > 0:
            report_parts.append({
                "table": row["full_table_name"],
                "records": df.toPandas().to_dict(orient="records")
            })

    import json
    report = json.dumps({"subject_id": subject_id, "data": report_parts}, indent=2, default=str)

    # Save report to secure location
    output_path = f"abfss://compliance@storage.dfs.core.windows.net/dsar/{subject_id}.json"
    dbutils.fs.put(output_path, report, overwrite=True)
    return output_path
```

## Data Lineage Chain Report

```python
def build_lineage_chain(table_name: str, depth: int = 5) -> dict:
    """Recursively build the full upstream lineage chain for a table."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    chain = {"table": table_name, "upstreams": []}

    if depth == 0:
        return chain

    lineage = w.lineage_tracking.table_lineage(table_name=table_name)
    for upstream in (lineage.upstreams or []):
        if upstream.table_info:
            upstream_chain = build_lineage_chain(upstream.table_info.name, depth - 1)
            chain["upstreams"].append(upstream_chain)

    return chain

# Export lineage report for auditors
import json
chain = build_lineage_chain("catalog.gold.revenue_summary")
print(json.dumps(chain, indent=2))
```

```bash
# CLI: generate full lineage report
databricks-agent catalog lineage catalog.gold.revenue_summary --depth 5 --format json --output lineage_report.json
```

## Data Retention Policy Enforcement

```python
from datetime import date, timedelta

def apply_retention_policy(table_name: str, retention_years: int, date_col: str = "_ingested_at"):
    """Delete records older than the retention period."""
    cutoff_date = date.today() - timedelta(days=retention_years * 365)

    # Count records to be deleted
    count = spark.sql(f"""
        SELECT COUNT(*) FROM {table_name}
        WHERE CAST({date_col} AS DATE) < '{cutoff_date}'
    """).collect()[0][0]

    if count == 0:
        print(f"No records to purge from {table_name} (cutoff: {cutoff_date})")
        return

    # Delete expired records
    from delta.tables import DeltaTable
    dt = DeltaTable.forName(spark, table_name)
    dt.delete(f"CAST({date_col} AS DATE) < '{cutoff_date}'")

    # Log to retention audit
    spark.sql(f"""
        INSERT INTO catalog.ops.retention_audit_log VALUES
        ('{table_name}', '{cutoff_date}', {count}, current_timestamp(), current_user())
    """)

    print(f"✓ Purged {count} records from {table_name} older than {cutoff_date}")

# Run retention for each classified table
retention_schedule = {
    "catalog.bronze.raw_events":    3,
    "catalog.silver.transactions":  7,
    "catalog.silver.customers":     7,
}
for table, years in retention_schedule.items():
    apply_retention_policy(table, years)
```

## Cross-Border Data Transfer Controls

```sql
-- Tag tables with data residency requirements
ALTER TABLE catalog.silver.eu_customers SET TAGS (
  'data_residency'      = 'EU',
  'gdpr_region'         = 'EEA',
  'transfer_restricted' = 'true',
  'sccs_required'       = 'true'
);

-- View all cross-border restricted tables
SELECT full_name, tag_name, tag_value
FROM catalog.information_schema.table_tags
WHERE tag_name = 'transfer_restricted' AND tag_value = 'true';
```

## Consent Tracking

```python
# Record consent events in an immutable Delta table
def record_consent(subject_id: str, consent_type: str, granted: bool, channel: str):
    consent_record = spark.createDataFrame([{
        "subject_id":    subject_id,
        "consent_type":  consent_type,  # "marketing", "analytics", "profiling"
        "granted":       granted,
        "channel":       channel,       # "web", "mobile", "email"
        "recorded_at":   str(spark.sql("SELECT current_timestamp()").collect()[0][0]),
        "recorded_by":   "consent-service-v2",
    }])
    consent_record.write.format("delta").mode("append").saveAsTable("catalog.compliance.consent_log")

# Query current consent status
def get_subject_consent(subject_id: str) -> dict:
    return spark.sql(f"""
        WITH latest AS (
            SELECT consent_type, granted,
                   ROW_NUMBER() OVER (PARTITION BY consent_type ORDER BY recorded_at DESC) AS rn
            FROM catalog.compliance.consent_log
            WHERE subject_id = '{subject_id}'
        )
        SELECT consent_type, granted FROM latest WHERE rn = 1
    """).toPandas().set_index("consent_type")["granted"].to_dict()
```

## Audit-Ready Evidence Package

```bash
# Generate compliance evidence package (for SOC2, GDPR, HIPAA audits)
databricks-agent catalog evidence-package \
  --catalog prod \
  --regulation GDPR \
  --period 2025-01-01:2025-12-31 \
  --output compliance_evidence_2025.zip
```

```python
# Automated compliance report from system tables
spark.sql("""
  SELECT
    u.user_identity.email                                  AS accessor,
    u.request_params.table_full_name                       AS table_accessed,
    COUNT(*)                                               AS access_count,
    MIN(u.event_time)                                      AS first_access,
    MAX(u.event_time)                                      AS last_access
  FROM system.access.audit u
  JOIN catalog.information_schema.table_tags t
    ON u.request_params.table_full_name = CONCAT(t.catalog_name, '.', t.schema_name, '.', t.table_name)
  WHERE t.tag_name = 'gdpr_applicable' AND t.tag_value = 'true'
    AND u.event_time >= DATEADD(DAY, -90, CURRENT_TIMESTAMP())
  GROUP BY 1, 2
  ORDER BY access_count DESC
""").show()
```

## Governance Traceability Checklist

- [ ] All PII tables tagged (`gdpr_applicable=true`, `data_classification=restricted`)
- [ ] Column-level lineage captured for all Gold/reporting tables
- [ ] Erasure procedure tested and evidenced
- [ ] DSAR report generation tested end-to-end
- [ ] Retention policies deployed and scheduled
- [ ] Consent log is append-only (no updates/deletes allowed)
- [ ] Data residency tags applied to all EU/regulated data
- [ ] Audit log queries running in compliance dashboard

## CLI Reference

```bash
databricks-agent catalog lineage tbl --depth 5 --format json     # Full lineage chain
databricks-agent catalog audit --catalog prod --check gdpr        # GDPR compliance check
databricks-agent catalog erasure --subject-id X --catalog prod    # GDPR erasure
databricks-agent catalog retention --catalog prod --dry-run       # Preview retention purge
```
