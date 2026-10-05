---
name: databricks-asset-bundles
description: Deploy Databricks jobs, DLT pipelines, notebooks, dashboards, models and apps as code with Databricks Asset Bundles — databricks.yml structure, targets and modes, variables and complex overrides, validate/deploy/run/summary/destroy, and the CI patterns that make dev→staging→prod reproducible instead of hand-clicked. Use when the user mentions: asset bundle, DAB, databricks.yml, bundle validate, bundle deploy, bundle run, bundle destroy, bundle summary, bundle target, deployment mode development production, Databricks CI/CD, deploy a job as code, promote to production, dbx replacement.
license: MIT
---

# Databricks Asset Bundles

A bundle is a directory with a `databricks.yml` that declares resources — jobs, pipelines,
notebooks, dashboards, models, apps — plus one target per environment. `databricks bundle
deploy` makes the workspace match the file. That is the whole idea, and it is the difference
between a platform you can rebuild and one that exists only in whatever someone clicked.

DABs supersede `dbx`. If a repo still uses `dbx`, migrating it is usually the highest-value
change available, because everything else — review, rollback, environment parity, CI — depends
on the deployment being declarative.

Requires the unified `databricks` CLI — see `databricks-cli` for install and auth.

## Structure

```
my-project/
├── databricks.yml              # bundle name, includes, targets, variables
├── resources/
│   ├── ingest_job.yml          # one file per resource; keep them small
│   └── silver_pipeline.yml
├── src/
│   ├── notebooks/
│   └── jobs/                   # plain Python — testable without a workspace
└── tests/
```

```yaml
bundle:
  name: sales-platform

include:
  - resources/*.yml

variables:
  catalog:
    description: Unity Catalog catalog for this environment
  warehouse_id:
    description: SQL warehouse backing the dashboards

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-xxx.azuredatabricks.net
    variables:
      catalog: dev_sales

  prod:
    mode: production
    workspace:
      host: https://adb-yyy.azuredatabricks.net
      root_path: /Shared/.bundle/sales-platform
    run_as:
      service_principal_name: sp-sales-platform
    variables:
      catalog: prod_sales
    permissions:
      - level: CAN_VIEW
        group_name: analytics-readers
```

### `mode` is the setting people miss

| `mode: development` | `mode: production` |
|---|---|
| Resources prefixed `[dev <user>]` and deployed under the user's own path | Deployed at the declared `root_path`, no prefix |
| Schedules and triggers **paused** | Schedules active |
| Concurrent runs allowed | Normal job semantics |
| Marked as dev in the UI | Validated more strictly — e.g. `run_as` and permissions must be explicit |

Two consequences worth stating up front:

- **A dev deploy will not run on schedule.** A pipeline that "deployed fine but never ran" is
  almost always a development-mode target.
- **Two people deploying the same dev bundle do not collide**, because each gets their own
  prefix and path. That is a feature; do not work around it by hardcoding paths.

## Variables and overrides

```yaml
# resources/ingest_job.yml
resources:
  jobs:
    ingest:
      name: ingest-${bundle.target}
      tasks:
        - task_key: land_bronze
          notebook_task:
            notebook_path: ../src/notebooks/land_bronze.py
            base_parameters:
              catalog: ${var.catalog}
          job_cluster_key: main
      job_clusters:
        - job_cluster_key: main
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 2
```

- `${var.x}`, `${bundle.target}`, `${workspace.current_user.userName}` and resource references
  interpolate; prefer them over duplicating a value across targets.
- Override per target by repeating only the keys that change under that target's `resources:`.
  Maps merge; **lists replace**. A target that redefines `tasks:` replaces the whole list — the
  most common surprise in a bundle diff.
- Pass a variable at the command line with `--var="catalog=scratch"` for a one-off, never for
  anything a pipeline repeats.
- **No secrets in `databricks.yml`.** Reference a secret scope, or supply the value from the CI
  secret store as an environment variable.

## The deploy loop

```bash
databricks bundle validate -t dev            # 1. schema + interpolation; always first
databricks bundle deploy   -t dev            # 2.
databricks bundle summary  -t dev            # 3. what exists now, with URLs
databricks bundle run ingest -t dev          # 4. run one resource and stream its output
```

- **`validate` before every deploy.** It catches the unresolved variable and the malformed
  resource without touching the workspace.
- **`summary` after every deploy.** It is the only cheap way to confirm what the deploy actually
  produced, and it gives the run URLs.
- `bundle run <key> -t <target>` is how you test a job without hunting for it in the UI.
- `bundle deploy --force-lock` steals the deployment lock. Only use it when you know the holding
  process is dead; stealing a live lock corrupts deployment state.

### `destroy` is destructive

```bash
databricks bundle destroy -t dev
```

It deletes the deployed resources and the bundle's workspace files. **Never run it against a
production target without an explicit confirmation naming the target and listing what
`summary` says will go.** It does not touch Unity Catalog data, but it does remove the jobs and
pipelines that maintain it.

## Promotion

The whole point is that `dev`, `staging` and `prod` differ only in the target block:

1. Change the resource definition once.
2. `validate` and `deploy` to `dev`; run it.
3. Merge to the branch that CI deploys to `staging`; run the same resource there.
4. Promote the **same commit** to `prod`. Never edit `databricks.yml` between staging and prod —
   a difference that appears at that point has had no testing at all.

In CI:

```bash
export DATABRICKS_HOST=...            # OAuth M2M service principal, from the secret store
export DATABRICKS_CLIENT_ID=...
export DATABRICKS_CLIENT_SECRET=...

databricks bundle validate -t prod
databricks bundle deploy   -t prod
databricks bundle summary  -t prod --output json > deploy-summary.json
```

- Pin the CLI version in the runner. A CLI upgrade is a deployment behaviour change.
- Use a service principal with `run_as` set explicitly in the production target, so a job does
  not silently run as whoever deployed last.
- Keep `deploy-summary.json` as a build artifact; it is your record of what shipped.
- Fail the pipeline on a non-zero exit code. Do not parse stdout for the word "error".

## Resource types worth knowing

| Resource | Notes |
|---|---|
| `jobs` | tasks, dependencies, job clusters, schedules, notifications — `databricks-dlt-pipelines` for task design |
| `pipelines` | DLT; `development: true` in dev targets so updates are cheap |
| `dashboards` | Lakeview dashboards as files, with `warehouse_id` per target — `databricks-dashboard-authoring` |
| `models` / `experiments` | MLflow registration |
| `apps` | Databricks Apps, with their own source path and config |
| `schemas` / `volumes` | Unity Catalog objects the bundle owns. Be deliberate: bundling a schema means `destroy` can delete it |
| `clusters` | prefer job clusters over all-purpose clusters for scheduled work; cheaper and isolated |

## Failure modes

| Symptom | Cause |
|---|---|
| Deployed but never runs on schedule | `mode: development` pauses schedules |
| "cannot resolve variable" on validate | variable declared but given no value in that target |
| A target's task list lost tasks | list override replaces, it does not merge |
| Deploy succeeds, job runs as the wrong identity | no explicit `run_as`; it defaults to the deployer |
| Deploy blocked on a lock | a previous deploy died — confirm it is dead before `--force-lock` |
| Resource vanished after a rename | the bundle key is the identity; renaming a key destroys and recreates |
| Works locally, fails in CI | path assumptions, or a CLI version difference between the two |

That last row deserves its own habit: **renaming a resource key is a delete plus a create.**
If the old resource holds state — a streaming checkpoint, a DLT pipeline's history — say so
before renaming.

## Related skills

| Need | Skill |
|---|---|
| CLI install, auth, profiles, JSON contract | `databricks-cli` |
| What goes inside a job or pipeline | `databricks-dlt-pipelines`, `databricks-data-transformation` |
| Catalog and grant design the bundle deploys into | `databricks-security-governance`, `databricks-data-catalog-lineage` |
| Dashboards as bundle resources | `databricks-dashboard-authoring` |
| Cluster sizing and cost of what you declared | `databricks-performance-scale` |
| Tests to run before promotion | `databricks-testing-validation` |
| Release process around the promotion path | `databricks-project-management` |
