---
name: databricks-cli
description: Drive Databricks from the terminal with the unified databricks CLI — OAuth and profile authentication, Unity Catalog browsing, workspace file sync, compute, jobs, pipelines, SQL warehouses, apps, and the --output json contract that makes every command scriptable. Use when the user mentions: databricks CLI, databricks auth login, .databrickscfg, databricks profile, DATABRICKS_HOST, databricks configure, databricks catalogs list, databricks jobs run-now, databricks sync, databricks workspace export, databricks fs, OAuth U2M, service principal M2M, headless Databricks, script Databricks.
license: MIT
---

# The `databricks` CLI

The unified `databricks` CLI (v0.2xx, Go, one self-contained binary) is the execution layer
under most Databricks automation: it fronts the whole REST API surface, carries Databricks
Asset Bundles, and emits JSON for every command. Where `databricks-agent` gives Claude
intent-level commands, this is what the platform actually speaks — and what you fall back to
for anything the agent does not wrap.

**Do not confuse it with the legacy Python CLI** (`pip install databricks-cli`, the
`dbfs`/`databricks workspace` tool from before v0.100). The legacy CLI is deprecated, its
command surface differs, and mixing the two inside one script is a reliable source of
"command not found" on another machine.

```bash
databricks --version        # expect 0.2xx+; record this in any pipeline log
```

## Install

```bash
brew tap databricks/tap && brew install databricks      # macOS
winget install Databricks.DatabricksCLI                 # Windows
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh   # Linux
```

In CI, pin the version rather than installing `latest`, so a CLI release cannot change a
pipeline's behaviour on an unrelated day.

## Authentication — get this right once

Credentials live in `~/.databrickscfg` as named profiles, or in environment variables. The
resolution order is: command flags, then environment variables, then the profile, then the
bundle target's host.

### Interactive (a person at a keyboard) — OAuth U2M

```bash
databricks auth login --host https://<workspace>.cloud.databricks.com --profile dev
databricks auth profiles                 # list profiles and whether each is valid
databricks auth token --profile dev      # inspect the current token (do not log the output)
databricks current-user me --profile dev # the one-command "am I actually signed in" check
```

OAuth tokens refresh; a personal access token does not and will expire mid-pipeline.

### Automation — OAuth M2M with a service principal

```bash
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_CLIENT_ID=<service-principal-application-id>
export DATABRICKS_CLIENT_SECRET=<oauth-secret>     # from the secret store, never a file in the repo
```

Rules, and they are not optional:

- **Never put a token or secret on the command line.** Process arguments are readable by other
  users and land in shell history and CI logs. Use environment variables sourced from a secret
  store, or a profile.
- **Never commit `~/.databrickscfg`,** and never echo `databricks auth token` output.
- **Prefer OAuth over a PAT** for anything that runs more than once.
- **One profile per environment** (`dev`, `staging`, `prod`), and pass `--profile` explicitly in
  every scripted command. Relying on `DEFAULT` is how a dev script deploys to production.

## The JSON contract

```bash
databricks catalogs list --output json | jq -r '.[].name'
databricks jobs list --output json | jq '.[] | {job_id, name: .settings.name}'
```

- **Always `--output json` in scripts.** The human table format is not a stable interface.
- **Check the exit code.** Non-zero means failure; do not infer success from output shape or
  from the absence of the word "error".
- **Paginate.** List commands page; `jobs list` and `tables list` in particular will quietly
  return a first page on a large workspace. Use the CLI's paging flags, or follow
  `next_page_token`, rather than assuming one call saw everything.
- Many commands accept `--json @file.json` for a full request body — the right move when a
  payload outgrows flags, and it keeps the payload reviewable in git.

## Command surface by job

### Unity Catalog

```bash
databricks catalogs list
databricks schemas list <catalog>
databricks tables list <catalog> <schema>
databricks tables get <catalog>.<schema>.<table>          # full column + property detail
databricks grants get table <catalog>.<schema>.<table>
databricks grants update table <catalog>.<schema>.<table> --json @grant.json
databricks volumes list <catalog> <schema>
```

`tables get` is the one to reach for before writing any query: it gives real column names,
types, comments and table properties, which beats guessing a schema and getting a silent
wrong answer. Governance reasoning lives in `databricks-security-governance` and
`databricks-data-catalog-lineage`.

### Workspace files and sync

```bash
databricks workspace list /Users/<me>
databricks workspace export-dir /Repos/team/project ./local --overwrite
databricks workspace import-dir ./local /Repos/team/project --overwrite
databricks sync ./src /Workspace/Users/<me>/src --watch      # continuous local → workspace
```

`databricks sync --watch` is the inner-loop tool: edit locally in your editor, run in the
workspace. For anything that ships, use a bundle (`databricks-asset-bundles`) rather than
syncing into a user folder.

### Compute

```bash
databricks clusters list --output json
databricks clusters get <cluster-id>
databricks clusters start <cluster-id>
databricks clusters events <cluster-id>      # why it died, and when it autoscaled
databricks instance-pools list
```

`clusters events` is the first stop for a job that failed for no apparent reason — spot
reclamation and OOM show up here and nowhere else.

### Jobs and pipelines

```bash
databricks jobs list --output json
databricks jobs get <job-id>
databricks jobs run-now <job-id> --json '{"notebook_params":{"env":"dev"}}'
databricks jobs list-runs --job-id <job-id> --limit 10
databricks jobs get-run <run-id> --output json
databricks jobs get-run-output <run-id>
databricks jobs cancel-run <run-id>

databricks pipelines list-pipelines
databricks pipelines get <pipeline-id>
databricks pipelines start-update <pipeline-id> [--full-refresh]
databricks pipelines list-pipeline-events <pipeline-id>
```

**`--full-refresh` on a pipeline update truncates and rebuilds the target tables.** Name the
pipeline and the tables and get an explicit yes before running it. It is not a retry button,
and on a streaming pipeline it also discards checkpoint state.

For a run that must be waited on, poll `get-run` with a bounded backoff and a hard deadline,
and treat `INTERNAL_ERROR`, `TIMEDOUT`, `CANCELED` and `FAILED` as distinct — reporting
"failed" for a cancellation sends someone debugging the wrong thing.

### SQL

```bash
databricks warehouses list --output json
databricks warehouses get <warehouse-id>
databricks warehouses start <warehouse-id>
```

Run queries through `dba sql query` (this package) or the SQL execution API. A cold warehouse
costs the first query 30+ seconds; start it before a batch rather than letting the first user
pay for it.

### Apps and bundles

```bash
databricks apps list
databricks apps get <app-name>
databricks apps logs <app-name>

databricks bundle validate | deploy | run | summary | destroy      # databricks-asset-bundles
```

### Secrets

```bash
databricks secrets list-scopes
databricks secrets list-secrets <scope>
databricks secrets put-secret <scope> <key>          # reads the value from stdin
```

Reference secrets from code as `dbutils.secrets.get(...)`. Never `get-secret` to inspect a
value and never pass a value as a flag — the CLI reads from stdin precisely so the value stays
off the command line.

## Guardrails

- **`--help` before first use of a command in a session.** The CLI ships new commands
  frequently, and flags have moved between releases.
- **Confirm before anything destructive**, naming the object: `clusters delete`,
  `jobs delete`, `pipelines delete`, `bundle destroy`, `schemas delete --force`,
  `tables delete`, `--full-refresh`, any `grants update` that removes a privilege. Deletes in
  Unity Catalog are not undoable by you.
- **Never widen a grant to clear a permission error.** Report the missing privilege and let a
  data owner decide.
- **Say which profile and workspace you are acting against** in the first command of a
  session. The commonest serious mistake with this CLI is correct commands against the wrong
  environment.
- Read-only inspection needs no confirmation. Lists, gets and events are free to run.

## Pairs with the Databricks pane

Kurt Buhler's `databricks-cli` plugin for Claude Code ships a `/databricks-pane` sidebar that
follows this CLI — workspace, Unity Catalog, compute, jobs, pipelines, apps and dashboards as
a live tree that highlights what Claude reads or changes
([power-bi-agentic-development](https://github.com/data-goblin/power-bi-agentic-development),
GPL-3.0, installed separately). It needs the same CLI and profiles described here, so getting
authentication right once serves both.

## Related skills

| Need | Skill |
|---|---|
| Deploying jobs, pipelines and apps as code | `databricks-asset-bundles` |
| Connection setup through this package | `databricks-connect` |
| Grants, lineage, governance reasoning | `databricks-security-governance`, `databricks-data-catalog-lineage` |
| Pipeline design rather than invocation | `databricks-dlt-pipelines` |
| Query authoring | `databricks-spark-sql-mastery` |
| Cost and sizing of the compute you started | `databricks-performance-scale` |
