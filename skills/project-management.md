---
name: databricks-project-management
description: Databricks project delivery lifecycle - discovery, foundation, build, UAT, go-live, data product mindset, agile sprints, RAID logs
triggers:
  - project plan
  - sprint
  - milestone
  - scope
  - backlog
  - data product
  - agile
  - go-live
  - RAID log
  - delivery
  - timeline
  - stakeholder
  - requirements
  - UAT
  - hypercare
---

# Databricks Project Delivery & Management

## 5-Phase Delivery Structure

### Phase 1 — Discovery (Week 1–2)
- Stakeholder interviews: who consumes the data, what decisions do they make?
- Source system inventory: databases, APIs, files, streams
- Data quality assessment: profiling key source tables
- Define the data product(s): owner, consumers, SLA, business value
- Risk register (RAID log) initialized

**Deliverables**: Source inventory, data quality report, project charter, architecture diagram

### Phase 2 — Foundation (Week 2–4)
- Unity Catalog setup: catalogs, schemas, permissions
- Environment scaffold: dev / staging / prod workspaces
- CI/CD pipeline: GitHub Actions or Azure DevOps → Databricks
- Monitoring baseline: job alerts, data quality dashboards
- Bronze ingestion for all sources

**Deliverables**: Workspace setup, Bronze layer running, monitoring alerts

### Phase 3 — Core Delivery (Week 4–8)
- Silver transformations + quality gates
- Gold aggregations + metric definitions
- Dashboard / BI layer connected
- Unit tests + integration tests
- Performance baseline established

**Deliverables**: Silver + Gold tables certified, dashboards in UAT

### Phase 4 — Hardening & UAT (Week 8–10)
- UAT with business stakeholders (data accuracy sign-off)
- Performance testing (volume, concurrency)
- Security review (RLS, column masking, PII audit)
- SLA testing (end-to-end pipeline latency)
- Runbook authored

**Deliverables**: UAT sign-off, security approval, runbook

### Phase 5 — Go-Live & Hypercare (Week 10–12)
- Production cutover (Blue-Green or parallel run)
- Monitoring dashboards handed to ops team
- Hypercare support (2–4 weeks on-call)
- Post-delivery review (actuals vs estimates, lessons learned)

**Deliverables**: Production release, hypercare SLA, retrospective

## Data Product Mindset

Every Databricks deliverable should be treated as a data product:

| Attribute | Definition |
|-----------|-----------|
| **Owner** | One person accountable for quality and SLA |
| **Consumers** | Named teams/people who depend on this data |
| **SLA** | Freshness SLA (e.g., "updated by 7 AM UTC") |
| **Quality** | Defined quality thresholds (completeness, uniqueness) |
| **Discoverability** | Documented in Unity Catalog with tags and comments |
| **Feedback loop** | Slack channel or JIRA project for consumer issues |

## Agile Sprint Template (2-week sprints)

```
Epic: Silver Layer - Orders
├── Story: Ingest raw orders via Auto Loader (3 pts)
│   ├── Task: Create Bronze table schema (1 pt)
│   ├── Task: Configure Auto Loader checkpoint (1 pt)
│   └── Task: Write Bronze ingestion notebook (1 pt)
├── Story: Transform orders to Silver (5 pts)
│   ├── Task: Define DLT expectations (1 pt)
│   ├── Task: Implement transformations (2 pts)
│   ├── Task: Write unit tests (1 pt)
│   └── Task: Performance test on prod volume (1 pt)
└── Story: Build daily revenue Gold table (3 pts)
    ├── Task: Define metric SQL (1 pt)
    ├── Task: Add Unity Catalog documentation (1 pt)
    └── Task: Connect to SQL Dashboard (1 pt)
```

## RAID Log Template

| ID | Type | Description | Owner | Status | Mitigation |
|----|------|-------------|-------|--------|------------|
| R-01 | Risk | Source API rate-limited to 100 req/min | Dev Team | Open | Implement backoff + caching |
| A-01 | Assumption | Source data is append-only | Data Owner | Confirmed | — |
| I-01 | Issue | Missing historical data before 2022 | PM | Open | Backfill from archive |
| D-01 | Dependency | Unity Catalog admin access required | Infra Team | In Progress | Admin creating catalog |

## Go-Live Readiness Checklist

- [ ] All Bronze/Silver/Gold tables certified in Unity Catalog
- [ ] End-to-end pipeline tested at production data volume
- [ ] Job failure alerts configured (email/Slack)
- [ ] Data quality alerts configured (thresholds defined)
- [ ] RLS and column masking verified by security team
- [ ] Runbook reviewed and approved
- [ ] Consumer training completed
- [ ] Rollback plan documented (RESTORE TABLE or repoint to old source)
- [ ] SLA monitoring dashboard live
- [ ] On-call rotation assigned for hypercare

## Stakeholder Communication Templates

**Weekly Status Update:**
```
[Project Name] - Week [N] Status
Status: 🟢 On Track / 🟡 At Risk / 🔴 Blocked

This week:
- Completed: [list]
- In progress: [list]
- Blockers: [list]

Next week:
- Planned: [list]

Key metrics:
- Tables delivered: X/Y
- Tests passing: X/Y
- Pipeline SLA: Met/Missed (avg latency: Xm)
```

## Post-Delivery Metrics

Track these for 4 weeks post go-live:

- **Pipeline reliability**: % of scheduled runs that succeeded
- **SLA adherence**: % of runs that met freshness SLA
- **Data quality**: % of records passing all quality checks
- **Consumer adoption**: Active users of dashboards / tables
- **Support tickets**: Issues raised by consumers

## CLI Reference

```bash
databricks-agent doctor                          # Environment health check
databricks-agent jobs list                       # List all jobs
databricks-agent pipelines list                  # List DLT pipelines
databricks-agent catalog audit --catalog prod    # Governance readiness
```
