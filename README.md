# databricks-agent

> Give Claude Code enterprise-grade Databricks superpowers — community-driven, AI-powered analytics engineering automation.

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white&labelColor=0a0a0f)](https://python.org)
[![Databricks](https://img.shields.io/badge/Databricks-SDK-FF3621?style=for-the-badge&logo=databricks&logoColor=white&labelColor=0a0a0f)](https://github.com/databricks/databricks-sdk-py)
[![License](https://img.shields.io/badge/License-MIT-00f0ff?style=for-the-badge&labelColor=0a0a0f)](LICENSE)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Santosh%20Kanthety-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white&labelColor=0a0a0f)](https://www.linkedin.com/in/santoshkanthety/)

</div>

---

## What is this?

**databricks-agent** is an AI-native CLI + Claude Code skills layer that turns Claude into a Databricks expert. Once installed, Claude understands your Databricks workspace, Delta Lake patterns, Unity Catalog governance, DLT pipelines, and analytics engineering workflows natively — no copy-pasting documentation, no context switching.

Inspired by [powerbi-agent](https://github.com/santoshkanthety/powerbi-agent), built for the Databricks lakehouse ecosystem.

---

## Prerequisites

Before installing, ensure the following are in place:

| Requirement | Version | Notes |
|---|---|---|
| **Python** | 3.10 – 3.14 | `python --version` |
| **pip** | Latest | `pip install --upgrade pip` |
| **Claude Code** | Latest | [Install guide](https://claude.ai/code) — required for skills |
| **Databricks Workspace** | Any cloud | Azure / AWS / GCP — Unity Catalog recommended |
| **Databricks CLI or SDK** | Latest | Installed automatically via `pip install databricks-agent` |
| **Authentication** | PAT or OAuth | Personal Access Token **or** OAuth M2M (service principal) |
| **SQL Warehouse** | Active | Required for `databricks-agent sql query` commands |
| **Unity Catalog** | Enabled | Required for `catalog` commands — metastore must be attached |
| **Delta Live Tables** | Optional | Required for `pipelines` commands |
| **MLflow** | Optional | `pip install "databricks-agent[ml]"` — for ML workflow skills |

### Getting a Personal Access Token (PAT)

1. Open your Databricks workspace
2. Click your username (top-right) → **Settings** → **Developer**
3. Click **Access Tokens** → **Generate new token**
4. Set a description and expiry, then copy the token
5. Run `databricks-agent connect setup` and paste it when prompted

### Supported Clouds

| Cloud | Host format |
|---|---|
| **Azure** | `https://adb-<id>.<region>.azuredatabricks.net` |
| **AWS** | `https://<id>.cloud.databricks.com` |
| **GCP** | `https://<id>.<region>.gcp.databricks.com` |

---

## Quickstart

```bash
# Install
pip install "databricks-agent[all]"

# Connect to your workspace
databricks-agent connect setup

# Install Claude Code skills
databricks-agent skills install

# Verify everything works
databricks-agent doctor
```

Then open Claude Code and start asking:

> *"List all tables in my gold schema and find any undocumented ones"*
> *"Run an incremental load from bronze.orders to silver.orders"*
> *"Check the status of my nightly ETL job"*
> *"Explain the lineage of catalog.gold.revenue_summary"*

---

## Features

### CLI Command Suite

| Command Group | Capabilities |
|---|---|
| `databricks-agent connect` | Workspace connection, profile management |
| `databricks-agent sql` | Execute queries, list warehouses |
| `databricks-agent jobs` | List, run, cancel, and monitor jobs |
| `databricks-agent clusters` | List, start, and inspect clusters |
| `databricks-agent catalog` | Unity Catalog: tables, lineage, grants, audits |
| `databricks-agent pipelines` | DLT pipeline management |
| `databricks-agent skills` | Install / manage Claude Code skills |
| `databricks-agent doctor` | Environment diagnostics |
| `databricks-agent ui` | Launch web configuration UI |

### 15 Claude Code Skills

Once installed via `databricks-agent skills install`, Claude activates these skills automatically when it detects Databricks topics:

| Skill | Covers |
|---|---|
| `databricks-connect` | Connection setup, auth, troubleshooting |
| `data-catalog-lineage` | Unity Catalog governance, lineage, tagging |
| `data-transformation` | PySpark transforms, merge, dedup, schema drift |
| `spark-sql-mastery` | Window functions, aggregations, CTEs, explain plans |
| `dlt-pipelines` | Delta Live Tables, Auto Loader, CDC, Workflows |
| `metric-glossary` | dbt metrics, semantic layer, documentation |
| `medallion-architecture` | Bronze/Silver/Gold patterns, Delta optimizations |
| `performance-scale` | Cluster tuning, AQE, partitioning, Photon |
| `project-management` | Delivery lifecycle, sprints, RAID logs, go-live |
| `dashboard-authoring` | Lakeview dashboards, DBSQL, design principles |
| `security-governance` | Row filters, column masks, grants, audit logs |
| `source-integration` | Auto Loader, JDBC, Kafka, REST APIs |
| `delta-modeling` | Star schema on Delta, SCD strategies, fact/dim design |
| `testing-validation` | DLT expectations, reconciliation, dbt tests |
| `time-series-data` | Gap detection, date spines, LOCF, streaming windows |
| `data-governance-traceability` | GDPR/CCPA, right-to-erasure, lineage chains, retention, consent |
| `cyber-security` | Network isolation, secrets hygiene, threat detection, zero-trust |

---

## Architecture

```
databricks-agent/
├── skills/                     ← 15 Claude Code knowledge files (.md)
│   ├── databricks-connect.md
│   ├── data-catalog-lineage.md
│   ├── medallion-architecture.md
│   └── ...
├── src/databricks_agent/
│   ├── cli.py                  ← Click CLI (main entry point)
│   ├── connect.py              ← Workspace authentication
│   ├── sql.py                  ← SQL Warehouse query execution
│   ├── jobs.py                 ← Jobs / Workflows management
│   ├── clusters.py             ← Cluster management
│   ├── catalog.py              ← Unity Catalog operations
│   ├── pipelines.py            ← DLT pipeline management
│   ├── doctor.py               ← Environment diagnostics
│   ├── skills/installer.py     ← Skill installation manager
│   └── web/app.py              ← FastAPI config UI (optional)
└── tests/
    └── test_cli.py
```

**Three layers:**

1. **Skills Layer** — Markdown files teaching Claude Databricks domain expertise. Activated contextually by keyword triggers. No code execution — pure knowledge.
2. **CLI Layer** — Click commands that interface with Databricks REST APIs via `databricks-sdk`. All actions the skills recommend can be executed directly.
3. **Web UI Layer** (optional) — FastAPI app for visual pipeline configuration without writing YAML.

---

## Installation

```bash
# Core CLI only
pip install databricks-agent

# With web UI
pip install "databricks-agent[ui]"

# With MLflow/ML support
pip install "databricks-agent[ml]"

# Everything
pip install "databricks-agent[all]"
```

### Authentication

databricks-agent uses the [Databricks SDK](https://github.com/databricks/databricks-sdk-py) which supports:
- **Personal Access Token (PAT)** — `databricks-agent connect setup`
- **~/.databrickscfg profiles** — `databricks-agent connect list`
- **Environment variables** — `DATABRICKS_HOST` + `DATABRICKS_TOKEN`
- **OAuth M2M** — `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET`
- **Azure managed identity**, Entra ID, and more (via SDK auto-discovery)

---

## Examples

```bash
# Run a SQL query
databricks-agent sql query --sql "SELECT * FROM catalog.gold.revenue LIMIT 10" --warehouse my-wh

# Check job run status
databricks-agent jobs run --name nightly-etl
databricks-agent jobs status --run-id 12345

# Unity Catalog audit
databricks-agent catalog audit my_catalog --schema gold

# View table lineage
databricks-agent catalog lineage catalog.gold.revenue_summary

# Start a DLT pipeline
databricks-agent pipelines start --name orders-pipeline

# List cluster state
databricks-agent clusters list
```

---

## Contributing

PRs welcome! See [CONTRIBUTING.md](CONTRIBUTING.md).

Focus areas:
- Additional skills (ML/MLflow, streaming patterns, cost optimization)
- More CLI commands (workspace files, secrets, volumes)
- Web UI improvements
- Integration tests

---

## License

MIT — see [LICENSE](LICENSE).

---

*Built by [Santosh Kanthety](https://github.com/santoshkanthety) · [LinkedIn](https://www.linkedin.com/in/santoshkanthety/) · 20+ years of Technology & Data transformation delivery and strategy · Inspired by [powerbi-agent](https://github.com/santoshkanthety/powerbi-agent)*
