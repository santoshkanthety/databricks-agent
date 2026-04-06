"""
databricks-agent CLI — AI-powered Databricks analytics engineering automation.
"""

from __future__ import annotations

import json
import sys
from typing import Optional

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

console = Console()

BANNER = r"""
  ____        _        _          _      _
 |  _ \  __ _| |_ __ _| |__  _ __(_) ___| | _____
 | | | |/ _` | __/ _` | '_ \| '__| |/ __| |/ / __|
 | |_| | (_| | || (_| | |_) | |  | | (__|   <\__ \
 |____/ \__,_|\__\__,_|_.__/|_|  |_|\___|_|\_\___/

       A g e n t     b y   S a n t o s h  K a n t h e t y
"""


def _print_banner():
    console.print(Panel(Text(BANNER, style="bold cyan"), border_style="cyan"))


@click.group()
@click.version_option(package_name="databricks-agent")
def main():
    """databricks-agent: AI-powered Databricks analytics engineering for Claude Code."""


# ─────────────────────────────── CONNECT ──────────────────────────────────────

@main.group()
def connect():
    """Manage workspace connections and authentication."""


@connect.command("setup")
@click.option("--host", prompt="Workspace URL (e.g. https://adb-XXXX.Y.azuredatabricks.net)", help="Workspace URL")
@click.option("--token", prompt="Personal Access Token", hide_input=True, help="Databricks PAT")
@click.option("--warehouse", default="", help="Default SQL Warehouse name or ID")
@click.option("--catalog", default="", help="Default catalog")
def connect_setup(host: str, token: str, warehouse: str, catalog: str):
    """Connect to a Databricks workspace."""
    from databricks_agent.connect import save_config, test_connection

    config = {"host": host.rstrip("/"), "token": token}
    if warehouse:
        config["default_warehouse"] = warehouse
    if catalog:
        config["default_catalog"] = catalog

    save_config(config)
    console.print("[cyan]Testing connection...[/cyan]")
    ok, msg = test_connection()
    if ok:
        console.print(f"[green]✓ {msg}[/green]")
    else:
        console.print(f"[red]✗ {msg}[/red]")
        sys.exit(1)


@connect.command("list")
def connect_list():
    """List profiles from ~/.databrickscfg."""
    from databricks_agent.connect import list_profiles

    profiles = list_profiles()
    if not profiles:
        console.print("[yellow]No profiles found in ~/.databrickscfg[/yellow]")
        return

    table = Table(title="Databricks Profiles", header_style="bold cyan")
    table.add_column("Profile")
    table.add_column("Host")
    table.add_column("Auth")

    for p in profiles:
        auth = "PAT" if p.get("token") else ("OAuth" if p.get("client_id") else "Unknown")
        table.add_row(p.get("profile", "—"), p.get("host", "—"), auth)

    console.print(table)


@connect.command("test")
@click.option("--profile", default=None, help="Profile name from ~/.databrickscfg")
def connect_test(profile: Optional[str]):
    """Test the current workspace connection."""
    from databricks_agent.connect import test_connection, get_workspace_client

    if profile:
        from databricks_agent.connect import get_workspace_client as gwc
        try:
            w = gwc(profile=profile)
            me = w.current_user.me()
            console.print(f"[green]✓ Connected as {me.user_name} to {w.config.host} (profile: {profile})[/green]")
        except Exception as e:
            console.print(f"[red]✗ {e}[/red]")
    else:
        ok, msg = test_connection()
        if ok:
            console.print(f"[green]✓ {msg}[/green]")
        else:
            console.print(f"[red]✗ {msg}[/red]")


# ─────────────────────────────── SQL ──────────────────────────────────────────

@main.group()
def sql():
    """Execute SQL queries and manage SQL Warehouses."""


@sql.command("query")
@click.option("--sql", "query_str", default=None, help="SQL statement to execute")
@click.option("--file", "query_file", default=None, type=click.Path(exists=True), help="SQL file to execute")
@click.option("--warehouse", default=None, help="Warehouse name or ID (uses default if not set)")
@click.option("--catalog", default=None, help="Default catalog for this query")
@click.option("--schema", default=None, help="Default schema for this query")
@click.option("--output", type=click.Choice(["table", "json", "csv"]), default="table", help="Output format")
@click.option("--limit", default=100, help="Max rows to display")
def sql_query(query_str, query_file, warehouse, catalog, schema, output, limit):
    """Execute a SQL query on a SQL Warehouse."""
    from databricks_agent.connect import get_config
    from databricks_agent.sql import run_query, get_warehouse_id

    if query_file:
        with open(query_file) as f:
            query_str = f.read()
    if not query_str:
        console.print("[red]Provide --sql or --file[/red]")
        sys.exit(1)

    config = get_config()
    wh_name = warehouse or config.get("default_warehouse")
    if not wh_name:
        console.print("[red]No warehouse specified. Run: databricks-agent connect setup[/red]")
        sys.exit(1)

    try:
        wh_id = get_warehouse_id(wh_name)
        rows = run_query(query_str, wh_id, catalog=catalog, schema=schema)
    except Exception as e:
        console.print(f"[red]Query failed: {e}[/red]")
        sys.exit(1)

    rows = rows[:limit]

    if output == "json":
        click.echo(json.dumps(rows, indent=2, default=str))
    elif output == "csv":
        if rows:
            click.echo(",".join(rows[0].keys()))
            for row in rows:
                click.echo(",".join(str(v) for v in row.values()))
    else:
        if not rows:
            console.print("[dim]No results.[/dim]")
            return
        table = Table(header_style="bold cyan", show_lines=False)
        for col in rows[0].keys():
            table.add_column(col)
        for row in rows:
            table.add_row(*[str(v) if v is not None else "[dim]null[/dim]" for v in row.values()])
        console.print(table)
        console.print(f"[dim]{len(rows)} row(s)[/dim]")


@sql.command("warehouses")
def sql_warehouses():
    """List all SQL Warehouses."""
    from databricks_agent.sql import list_warehouses

    warehouses = list_warehouses()
    table = Table(title="SQL Warehouses", header_style="bold cyan")
    table.add_column("ID")
    table.add_column("Name")
    table.add_column("Size")
    table.add_column("State")
    table.add_column("Type")

    for wh in warehouses:
        state_style = "green" if wh["state"] == "RUNNING" else "yellow"
        table.add_row(
            wh["id"], wh["name"], wh["size"] or "—",
            f"[{state_style}]{wh['state']}[/{state_style}]",
            wh["type"]
        )
    console.print(table)


# ─────────────────────────────── JOBS ─────────────────────────────────────────

@main.group()
def jobs():
    """Manage Databricks Jobs and Workflows."""


@jobs.command("list")
@click.option("--filter", "name_filter", default=None, help="Filter by job name")
def jobs_list(name_filter):
    """List all jobs in the workspace."""
    from databricks_agent.jobs import list_jobs

    job_list = list_jobs(name_filter=name_filter)
    table = Table(title="Databricks Jobs", header_style="bold cyan")
    table.add_column("Job ID", style="dim")
    table.add_column("Name")
    table.add_column("Creator")

    for j in job_list:
        table.add_row(str(j["job_id"]), j["name"], j["creator"] or "—")
    console.print(table)
    console.print(f"[dim]{len(job_list)} job(s)[/dim]")


@jobs.command("run")
@click.option("--name", default=None, help="Job name")
@click.option("--job-id", type=int, default=None, help="Job ID")
@click.option("--param", multiple=True, help="Notebook params as key=value (repeatable)")
def jobs_run(name, job_id, param):
    """Trigger a job run."""
    from databricks_agent.jobs import run_job

    params = dict(p.split("=", 1) for p in param) if param else {}
    run_id = run_job(job_id=job_id, job_name=name, notebook_params=params)
    console.print(f"[green]✓ Job triggered. Run ID: {run_id}[/green]")
    console.print(f"[dim]Check status: databricks-agent jobs status --run-id {run_id}[/dim]")


@jobs.command("status")
@click.option("--run-id", type=int, required=True, help="Run ID")
def jobs_status(run_id):
    """Check the status of a job run."""
    from databricks_agent.jobs import get_run_status

    status = get_run_status(run_id=run_id)
    state_style = "green" if status["result"] == "SUCCESS" else ("red" if status["result"] == "FAILED" else "yellow")
    console.print(f"Run [bold]{run_id}[/bold]: [{state_style}]{status['state']}[/{state_style}] / {status['result']}")
    if status["message"]:
        console.print(f"[dim]{status['message']}[/dim]")
    if status["url"]:
        console.print(f"[link={status['url']}]{status['url']}[/link]")


@jobs.command("cancel")
@click.option("--run-id", type=int, required=True, help="Run ID to cancel")
def jobs_cancel(run_id):
    """Cancel a running job."""
    from databricks_agent.jobs import cancel_run

    cancel_run(run_id=run_id)
    console.print(f"[yellow]Run {run_id} cancelled.[/yellow]")


@jobs.command("history")
@click.option("--name", default=None, help="Job name")
@click.option("--job-id", type=int, default=None, help="Job ID")
@click.option("--limit", default=10, help="Number of runs to show")
def jobs_history(name, job_id, limit):
    """Show recent run history for a job."""
    from databricks_agent.jobs import get_run_history

    runs = get_run_history(job_id=job_id, job_name=name, limit=limit)
    table = Table(title=f"Job History (last {limit})", header_style="bold cyan")
    table.add_column("Run ID")
    table.add_column("State")
    table.add_column("Result")
    table.add_column("Start Time")

    for r in runs:
        result_style = "green" if r["result"] == "SUCCESS" else ("red" if r["result"] == "FAILED" else "dim")
        table.add_row(
            str(r["run_id"]),
            r["state"],
            f"[{result_style}]{r['result']}[/{result_style}]",
            str(r["start_time"])
        )
    console.print(table)


# ─────────────────────────────── CLUSTERS ─────────────────────────────────────

@main.group()
def clusters():
    """Manage Databricks all-purpose and job clusters."""


@clusters.command("list")
def clusters_list():
    """List all clusters in the workspace."""
    from databricks_agent.clusters import list_clusters

    cluster_list = list_clusters()
    table = Table(title="Databricks Clusters", header_style="bold cyan")
    table.add_column("Cluster ID", style="dim")
    table.add_column("Name")
    table.add_column("State")
    table.add_column("Workers")
    table.add_column("Node Type")
    table.add_column("Runtime")

    for c in cluster_list:
        state_style = "green" if c["state"] == "RUNNING" else ("dim" if c["state"] == "TERMINATED" else "yellow")
        workers = str(c["num_workers"]) if c["num_workers"] is not None else "—"
        table.add_row(
            c["cluster_id"], c["name"],
            f"[{state_style}]{c['state']}[/{state_style}]",
            workers, c["worker_node"] or "—", c["spark_version"] or "—"
        )
    console.print(table)


@clusters.command("start")
@click.option("--name", default=None, help="Cluster name")
@click.option("--cluster-id", default=None, help="Cluster ID")
def clusters_start(name, cluster_id):
    """Start a cluster."""
    from databricks_agent.clusters import start_cluster

    start_cluster(cluster_id=cluster_id, cluster_name=name)
    console.print(f"[green]✓ Cluster start initiated.[/green]")


@clusters.command("info")
@click.option("--name", default=None, help="Cluster name")
@click.option("--cluster-id", default=None, help="Cluster ID")
def clusters_info(name, cluster_id):
    """Show detailed cluster information."""
    from databricks_agent.clusters import get_cluster_info

    info = get_cluster_info(cluster_id=cluster_id, cluster_name=name)
    for k, v in info.items():
        if v is not None:
            console.print(f"  [bold]{k}:[/bold] {v}")


# ─────────────────────────────── CATALOG ──────────────────────────────────────

@main.group()
def catalog():
    """Unity Catalog operations — tables, lineage, grants, tags, audits."""


@catalog.command("list")
@click.argument("catalog_name")
@click.option("--schema", "schema_name", default=None, help="Schema name (lists tables if provided)")
def catalog_list(catalog_name, schema_name):
    """List schemas in a catalog, or tables in a schema."""
    if schema_name:
        from databricks_agent.catalog import list_tables
        tables = list_tables(catalog_name, schema_name)
        table = Table(title=f"Tables in {catalog_name}.{schema_name}", header_style="bold cyan")
        table.add_column("Name")
        table.add_column("Type")
        table.add_column("Format")
        table.add_column("Owner")
        table.add_column("Comment")

        for t in tables:
            comment_style = "dim" if t["comment"].startswith("⚠️") else ""
            table.add_row(t["name"], t["table_type"], t["data_source_format"],
                          t["owner"] or "—", f"[{comment_style}]{t['comment']}[/{comment_style}]")
        console.print(table)
    else:
        from databricks_agent.catalog import list_schemas
        schemas = list_schemas(catalog_name)
        table = Table(title=f"Schemas in {catalog_name}", header_style="bold cyan")
        table.add_column("Schema")
        table.add_column("Owner")
        table.add_column("Comment")
        for s in schemas:
            table.add_row(s["name"], s["owner"] or "—", s["comment"] or "—")
        console.print(table)


@catalog.command("describe")
@click.argument("table_full_name")
def catalog_describe(table_full_name):
    """Show full metadata for a table (catalog.schema.table)."""
    from databricks_agent.catalog import describe_table

    info = describe_table(table_full_name)
    console.print(f"[bold cyan]{info['full_name']}[/bold cyan]")
    console.print(f"  Type:    {info['table_type']} / {info['format']}")
    console.print(f"  Owner:   {info['owner'] or '—'}")
    console.print(f"  Comment: {info['comment']}")
    console.print(f"  Tags:    {info['tags'] or '—'}")
    console.print()

    if info["columns"]:
        col_table = Table(title="Columns", header_style="bold")
        col_table.add_column("Column")
        col_table.add_column("Type")
        col_table.add_column("Nullable")
        col_table.add_column("Comment")
        col_table.add_column("Tags")

        for col in info["columns"]:
            comment_style = "dim" if not col["comment"] else ""
            col_table.add_row(
                col["name"], col["type"],
                "Y" if col["nullable"] else "N",
                f"[{comment_style}]{col['comment'] or '—'}[/{comment_style}]",
                str(col["tags"]) if col["tags"] else "—"
            )
        console.print(col_table)


@catalog.command("lineage")
@click.argument("table_full_name")
def catalog_lineage(table_full_name):
    """Show upstream and downstream lineage for a table."""
    from databricks_agent.catalog import get_table_lineage

    lineage = get_table_lineage(table_full_name)
    console.print(f"[bold]Lineage for:[/bold] {table_full_name}")
    console.print()
    console.print(f"[bold cyan]Upstreams ({len(lineage['upstreams'])}):[/bold cyan]")
    for u in lineage["upstreams"]:
        console.print(f"  ← {u}")
    console.print()
    console.print(f"[bold cyan]Downstreams ({len(lineage['downstreams'])}):[/bold cyan]")
    for d in lineage["downstreams"]:
        console.print(f"  → {d}")


@catalog.command("show-grants")
@click.argument("full_name")
@click.option("--type", "securable_type", default="TABLE", help="Securable type (TABLE, SCHEMA, CATALOG)")
def catalog_show_grants(full_name, securable_type):
    """Show permissions on a securable."""
    from databricks_agent.catalog import show_grants

    grants = show_grants(full_name, securable_type)
    table = Table(title=f"Grants on {full_name}", header_style="bold cyan")
    table.add_column("Principal")
    table.add_column("Privileges")

    for g in grants:
        table.add_row(g["principal"], ", ".join(g["privileges"]))
    console.print(table)


@catalog.command("grant")
@click.argument("full_name")
@click.option("--principal", required=True, help="User, group, or service principal")
@click.option("--privilege", required=True, help="Comma-separated privileges (SELECT, MODIFY, etc.)")
@click.option("--type", "securable_type", default="TABLE")
def catalog_grant(full_name, principal, privilege, securable_type):
    """Grant privilege on a securable."""
    from databricks_agent.catalog import grant_permission

    privs = [p.strip() for p in privilege.split(",")]
    grant_permission(full_name, securable_type, principal, privs)
    console.print(f"[green]✓ Granted {privilege} on {full_name} to {principal}[/green]")


@catalog.command("audit")
@click.argument("catalog_name")
@click.option("--schema", "schema_name", default=None, help="Limit to specific schema")
def catalog_audit(catalog_name, schema_name):
    """Find tables missing documentation, owner, or tags."""
    from databricks_agent.catalog import run_governance_audit

    issues = run_governance_audit(catalog_name, schema_name)
    if not issues:
        console.print("[green]✓ No governance issues found.[/green]")
        return

    table = Table(title=f"Governance Issues in {catalog_name}", header_style="bold red")
    table.add_column("Table")
    table.add_column("Issues")

    for i in issues:
        table.add_row(i["table"], ", ".join(i["issues"]))
    console.print(table)
    console.print(f"[yellow]{len(issues)} table(s) with governance issues[/yellow]")


# ─────────────────────────────── PIPELINES ────────────────────────────────────

@main.group()
def pipelines():
    """Manage Delta Live Tables (DLT) pipelines."""


@pipelines.command("list")
def pipelines_list():
    """List all DLT pipelines."""
    from databricks_agent.pipelines import list_pipelines

    pipeline_list = list_pipelines()
    table = Table(title="Delta Live Tables Pipelines", header_style="bold cyan")
    table.add_column("Pipeline ID", style="dim")
    table.add_column("Name")
    table.add_column("State")
    table.add_column("Target Schema")
    table.add_column("Creator")

    for p in pipeline_list:
        state_style = "green" if p["state"] == "RUNNING" else ("dim" if p["state"] == "IDLE" else "yellow")
        table.add_row(
            p["pipeline_id"], p["name"],
            f"[{state_style}]{p['state']}[/{state_style}]",
            p["target"] or "—", p["creator"] or "—"
        )
    console.print(table)


@pipelines.command("status")
@click.option("--name", default=None, help="Pipeline name")
@click.option("--pipeline-id", default=None, help="Pipeline ID")
def pipelines_status(name, pipeline_id):
    """Get current pipeline status."""
    from databricks_agent.pipelines import get_pipeline_status

    status = get_pipeline_status(pipeline_id=pipeline_id, name=name)
    state_style = "green" if status["state"] == "RUNNING" else "yellow"
    console.print(f"[bold]{status['name']}[/bold] [{state_style}]{status['state']}[/{state_style}]")
    if status["cause"]:
        console.print(f"  Cause: {status['cause']}")


@pipelines.command("start")
@click.option("--name", default=None, help="Pipeline name")
@click.option("--pipeline-id", default=None, help="Pipeline ID")
@click.option("--full-refresh", is_flag=True, default=False, help="Full refresh (reprocess all data)")
def pipelines_start(name, pipeline_id, full_refresh):
    """Start a DLT pipeline update."""
    from databricks_agent.pipelines import start_pipeline

    start_pipeline(pipeline_id=pipeline_id, name=name, full_refresh=full_refresh)
    mode = "full refresh" if full_refresh else "incremental"
    console.print(f"[green]✓ Pipeline started ({mode}).[/green]")


@pipelines.command("events")
@click.option("--name", default=None, help="Pipeline name")
@click.option("--pipeline-id", default=None, help="Pipeline ID")
@click.option("--limit", default=20, help="Number of events to show")
def pipelines_events(name, pipeline_id, limit):
    """Show recent pipeline events."""
    from databricks_agent.pipelines import get_pipeline_events

    events = get_pipeline_events(pipeline_id=pipeline_id, name=name, limit=limit)
    table = Table(title="Pipeline Events", header_style="bold cyan")
    table.add_column("Timestamp")
    table.add_column("Level")
    table.add_column("Type")
    table.add_column("Message")

    for e in events:
        level_style = "red" if e["level"] == "ERROR" else ("yellow" if e["level"] == "WARN" else "dim")
        table.add_row(
            str(e["timestamp"]),
            f"[{level_style}]{e['level']}[/{level_style}]",
            e["event_type"] or "—",
            (e["message"] or "")[:100]
        )
    console.print(table)


# ─────────────────────────────── SKILLS ───────────────────────────────────────

@main.group()
def skills():
    """Install and manage Claude Code skills for Databricks."""


@skills.command("install")
@click.option("--force", is_flag=True, default=False, help="Overwrite existing skill files")
def skills_install(force):
    """Install databricks-agent skills into ~/.claude/skills/."""
    from databricks_agent.skills.installer import install_skills

    console.print("[bold cyan]Installing Databricks skills for Claude Code...[/bold cyan]")
    count = install_skills(force=force)
    console.print(f"[green]✓ {count} skill(s) installed.[/green]")
    console.print("[dim]Skills will activate automatically when Claude Code detects Databricks topics.[/dim]")


@skills.command("uninstall")
def skills_uninstall():
    """Remove databricks-agent skills from ~/.claude/skills/."""
    from databricks_agent.skills.installer import uninstall_skills

    removed = uninstall_skills()
    console.print(f"[yellow]{removed} skill(s) removed.[/yellow]")


@skills.command("list")
def skills_list():
    """List skill installation status."""
    from databricks_agent.skills.installer import list_skills

    list_skills()


# ─────────────────────────────── DOCTOR ───────────────────────────────────────

@main.command()
def doctor():
    """Run environment diagnostics."""
    _print_banner()
    from databricks_agent.doctor import run_doctor

    ok = run_doctor()
    if ok:
        console.print("\n[green]✓ All required checks passed.[/green]")
    else:
        console.print("\n[red]✗ Some checks failed. See above for fixes.[/red]")
        sys.exit(1)


# ─────────────────────────────── UI ───────────────────────────────────────────

@main.command()
@click.option("--port", default=8000, help="Port for the web UI")
@click.option("--host", "bind_host", default="127.0.0.1", help="Bind host")
def ui(port, bind_host):
    """Launch the web configuration UI."""
    try:
        import uvicorn
        from databricks_agent.web.app import app as fastapi_app
    except ImportError:
        console.print("[red]Web UI requires the 'ui' extra. Run: pip install databricks-agent[ui][/red]")
        sys.exit(1)

    console.print(f"[cyan]Launching databricks-agent UI at http://{bind_host}:{port}[/cyan]")
    uvicorn.run(fastapi_app, host=bind_host, port=port)


if __name__ == "__main__":
    main()
