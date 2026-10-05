---
name: databricks-fabric-apps
description: Expose Databricks and Unity Catalog data to a Microsoft Fabric App built on the Rayfin SDK — mirrored Azure Databricks catalogs, OneLake shortcuts, and the lakehouse connector chain that gets Delta tables in front of a Rayfin app, plus when Databricks Apps or a Lakeview dashboard is the better answer. Use when the user mentions: Fabric App on Databricks data, Rayfin, rayfin CLI, mirrored Unity Catalog, mirrored Azure Databricks catalog, OneLake shortcut to Delta, Fabric shortcut to Databricks, surface Databricks data in a Fabric app, Databricks Apps, custom app over Unity Catalog, cross-platform app Databricks Fabric.
license: MIT
---

# Databricks data in a Fabric App (Rayfin)

Fabric Apps (preview) is a managed backend-as-a-service in Microsoft Fabric: a TypeScript
decorator data model becomes a SQL database, an Entra-authenticated GraphQL API, storage and
static hosting, deployed with `npx rayfin up`. It can read Fabric data items through typed
connectors — and that is the hinge, because **there is no Databricks connector.** The supported
connector types are Fabric items: lakehouse, warehouse, SQL database in Fabric, and semantic
model.

So the honest answer to "can a Fabric App read my Unity Catalog tables" is: yes, through a
chain, and the chain is the design decision. State it before anyone starts building.

This skill is the Databricks half of a doctrine pair; `powerbi-fabric-apps` in
[powerbi-agent](https://github.com/santoshkanthety/powerbi-agent) owns the Rayfin SDK, CLI and
security surface in full. Read that for anything Rayfin-specific.

## First: is a Fabric App even the right host?

| Situation | Build |
|---|---|
| Users live in Databricks; data stays in Unity Catalog | **Databricks Apps** — native, UC-governed, no data movement |
| A chart or table over UC data, no bespoke interaction | **Lakeview dashboard** — `databricks-dashboard-authoring` |
| Users live in Fabric; the app sits beside Power BI reports and Fabric items | **Fabric App** — this skill |
| Data already lands in OneLake, or is mirrored there for Power BI anyway | **Fabric App** — the chain costs nothing extra |
| Organisation is consolidating the serving layer on Fabric | **Fabric App** |

Choosing a Fabric App for data that lives only in Databricks means creating a path for that
data into Fabric. That is a governance decision with an owner, not an implementation detail.
If the only reason is "the team knows React", Databricks Apps also hosts a React frontend.

## The chain

```
Unity Catalog (Delta, Databricks)
   │
   ├── (A) Mirrored Azure Databricks catalog in Fabric      ← preferred
   │        read-only, metadata-synced, no copy
   │
   └── (B) OneLake shortcut to the Delta location (ADLS/S3)
            per-table or per-schema, no copy
   │
   ▼
Fabric lakehouse  (tables appear in the SQL analytics endpoint)
   │
   ▼
rayfin connector add --type <lakehouse connector type>
   │
   ▼
Fabric App — typed, delegated reads from the app's data layer
```

### (A) Mirrored Azure Databricks catalog — the default

Fabric can mirror an Azure Databricks Unity Catalog catalog as a Fabric item. Metadata syncs;
the data is read in place from the underlying storage. Nothing is copied, and Unity Catalog
remains the source of truth for the schema.

What to verify before relying on it:

- **It is read-only in Fabric.** An app that needs to write belongs in its own Fabric SQL
  database (which Rayfin provisions anyway) or back in Databricks. Do not plan writes through
  the mirror.
- **Sync is metadata, not instantaneous data.** A table created in UC appears after a sync;
  a row written to an existing table is visible on read. Know which latency the app's users
  will notice.
- **Permissions do not carry across.** Unity Catalog grants do not become Fabric item
  permissions. Whoever can read the mirrored catalog in Fabric can read it regardless of their
  UC grants — so re-establish access control on the Fabric side, deliberately, and say so to
  whoever owns the data. This is the single most important sentence in this skill.
- It is an Azure Databricks feature. For Databricks on AWS or GCP, use (B).

### (B) OneLake shortcut

A shortcut points a lakehouse table or folder at the Delta data in ADLS Gen2 or S3. Also no
copy, also read-only, and it works regardless of cloud. It is more granular — per table or
schema — and correspondingly more to maintain: a new table in UC does not appear until someone
creates a shortcut for it.

Prefer (A) when it is available and you want the whole catalog; prefer (B) for a handful of
curated gold tables, which is the more common real requirement anyway.

### What not to do

**Do not build a pipeline that copies gold tables from Databricks into a Fabric lakehouse just
so an app can read them**, unless someone has explicitly accepted owning a second copy. A copy
means two refresh schedules, two sets of grants, two answers to the same question, and a
reconciliation job nobody budgeted for. Say this plainly when it comes up; the shortcut or the
mirror exists precisely to avoid it.

## Wiring the connector

Once the tables are visible in a Fabric lakehouse, the app side is ordinary Rayfin work:

```bash
npx rayfin connector types -v                     # what the installed CLI supports
npx rayfin connector search --workspace-id <ws> --type <lakehouse-type> --json
npx rayfin connector add --type <lakehouse-type> \
    --workspace-id <ws> --item-id <lakehouse-item-id> --name goldLake
npx rayfin connector inspect --name goldLake --entity <table> --rows 10
npx rayfin connector invoke goldLake <operation> --input '{...}'
```

Confirm the exact connector type name and operation set from `rayfin connector types -v` — do
not take it from this file. The Rayfin surface is version-locked per project, and the
authoritative source is `.agents/skills/rayfin/SKILL.md` inside the project plus the
`rayfin docs` CLI. **Never write Rayfin API code from memory.**

Query discipline on the app side mirrors everything in `databricks-performance-scale`:

- Bound every result set. An unbounded read of a gold fact table through an app endpoint is a
  capacity incident on the Fabric side and a storage-read bill on the Databricks side.
- Project only the columns the UI renders.
- Push aggregation into the query; do not aggregate in the browser.
- Cache per user interaction, not per component render.
- Serve pre-aggregated gold tables, not silver. If the app needs a shape that does not exist,
  add it to the gold layer in Databricks where it is tested and governed —
  `databricks-medallion-architecture`.

## Governance, written down

An app spanning both platforms crosses a governance boundary, so record the answers rather than
discovering them later:

| Question | Where the answer must come from |
|---|---|
| Who may read this data in Fabric? | explicit Fabric item permissions — UC grants do not transfer |
| Who may use the app? | Fabric item **Run and interact**; workspace roles do not supersede it |
| Where is the lineage recorded? | both sides: UC lineage stops at the mirror; `databricks-data-catalog-lineage` |
| Does the app persist any of this data? | Rayfin's SQL database is a real second copy — `databricks-data-governance-traceability` |
| What happens on a UC schema change? | mirror syncs metadata; the app's typed schema does not. Treat an entity change as a code change |
| Is any of it regulated or subject to erasure? | if yes, a copy in the app's SQL database is now in scope. Resolve before building |

The deployed app authenticates with Fabric SSO only — no other provider is available after
deployment — and row-level rules in a Rayfin app come from its `@role` decorators, not from
Unity Catalog row filters. **UC row filters and column masks do not follow the data through a
mirror or a shortcut.** If a table's security depends on them, it must not reach an app this
way; serve a pre-filtered gold table instead.

## Operating it

- The Databricks side costs storage reads and whatever compute refreshes the gold layer; the
  Fabric side draws CUs for the app's services and every connector query. Both bills are real
  and they are billed to different owners — name both.
- A failure in the app can originate in four places: the app, the connector, the lakehouse
  mirror/shortcut, or the UC table. Diagnose in that order, from the app's error category
  outward, rather than guessing.
- Pin and record versions: the Rayfin CLI, the connector package, and the Databricks CLI used
  to build the gold layer.

## Related skills

| Need | Skill |
|---|---|
| Rayfin SDK, CLI, templates, security in full | `powerbi-fabric-apps`, `powerbi-fabric-app-templates` (powerbi-agent) |
| Fabric capacity cost of the app | `powerbi-fabric-capacity` (powerbi-agent) |
| Gold layer the app should read | `databricks-medallion-architecture`, `databricks-delta-modeling` |
| Grants, masks, row filters and what survives the chain | `databricks-security-governance` |
| Lineage across the boundary | `databricks-data-catalog-lineage`, `databricks-data-governance-traceability` |
| Native Databricks alternative | `databricks-dashboard-authoring` |
| Query cost and shaping | `databricks-performance-scale`, `databricks-spark-sql-mastery` |
| Deploying the gold layer that feeds it | `databricks-asset-bundles` |
