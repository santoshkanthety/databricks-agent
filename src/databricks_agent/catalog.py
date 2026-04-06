"""Unity Catalog operations - tables, schemas, lineage, grants, tags."""

from __future__ import annotations

from typing import Optional


def list_catalogs() -> list[dict]:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    return [
        {"name": c.name, "owner": c.owner, "comment": c.comment}
        for c in w.catalogs.list()
    ]


def list_schemas(catalog_name: str) -> list[dict]:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    return [
        {"name": s.name, "catalog": s.catalog_name, "owner": s.owner, "comment": s.comment}
        for s in w.schemas.list(catalog_name=catalog_name)
    ]


def list_tables(catalog_name: str, schema_name: str) -> list[dict]:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    return [
        {
            "name": t.name,
            "full_name": t.full_name,
            "table_type": t.table_type.value if t.table_type else "UNKNOWN",
            "data_source_format": t.data_source_format.value if t.data_source_format else "—",
            "owner": t.owner,
            "comment": t.comment or "⚠️ UNDOCUMENTED",
            "created_at": t.created_at,
            "updated_at": t.updated_at,
        }
        for t in w.tables.list(catalog_name=catalog_name, schema_name=schema_name)
    ]


def describe_table(full_name: str) -> dict:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    catalog_name, schema_name, table_name = full_name.split(".")
    t = w.tables.get(full_name=full_name)

    columns = []
    if t.columns:
        for col in t.columns:
            columns.append({
                "name": col.name,
                "type": col.type_text,
                "nullable": col.nullable,
                "comment": col.comment or "",
                "tags": col.tags or {},
            })

    return {
        "full_name": t.full_name,
        "table_type": t.table_type.value if t.table_type else "UNKNOWN",
        "format": t.data_source_format.value if t.data_source_format else "—",
        "owner": t.owner,
        "comment": t.comment or "⚠️ UNDOCUMENTED",
        "storage_location": t.storage_location,
        "columns": columns,
        "properties": t.properties,
        "tags": t.tags or {},
        "created_at": t.created_at,
        "updated_at": t.updated_at,
    }


def grant_permission(
    full_name: str,
    securable_type: str,
    principal: str,
    privileges: list[str],
) -> None:
    from databricks_agent.connect import get_workspace_client
    from databricks.sdk.service.catalog import PermissionsChange, Privilege, SecurableType

    w = get_workspace_client()
    w.grants.update(
        full_name=full_name,
        securable_type=SecurableType(securable_type.upper()),
        changes=[
            PermissionsChange(
                principal=principal,
                add=[Privilege(p.upper()) for p in privileges],
            )
        ],
    )


def show_grants(full_name: str, securable_type: str = "TABLE") -> list[dict]:
    from databricks_agent.connect import get_workspace_client
    from databricks.sdk.service.catalog import SecurableType

    w = get_workspace_client()
    perms = w.grants.get(
        full_name=full_name,
        securable_type=SecurableType(securable_type.upper()),
    )
    if not perms.privilege_assignments:
        return []
    return [
        {"principal": pa.principal, "privileges": [p.value for p in (pa.privileges or [])]}
        for pa in perms.privilege_assignments
    ]


def get_table_lineage(full_name: str) -> dict:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    lineage = w.lineage_tracking.table_lineage(table_name=full_name)

    upstreams = [u.table_info.name for u in (lineage.upstreams or []) if u.table_info]
    downstreams = [d.table_name for d in (lineage.downstreams or [])]

    return {
        "table": full_name,
        "upstreams": upstreams,
        "downstreams": downstreams,
    }


def run_governance_audit(catalog_name: str, schema_name: Optional[str] = None) -> list[dict]:
    """Return tables missing documentation, owner, or tags."""
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    issues = []

    schemas = (
        [w.schemas.get(full_name=f"{catalog_name}.{schema_name}")]
        if schema_name
        else list(w.schemas.list(catalog_name=catalog_name))
    )

    for schema in schemas:
        for table in w.tables.list(catalog_name=catalog_name, schema_name=schema.name):
            row = {"table": table.full_name, "issues": []}
            if not table.comment:
                row["issues"].append("missing comment")
            if not table.owner:
                row["issues"].append("no owner")
            if not table.tags:
                row["issues"].append("no tags")
            if row["issues"]:
                issues.append(row)

    return issues
