"""SQL Warehouse query execution."""

from __future__ import annotations

import time
from typing import Optional

from databricks.sdk.service.sql import StatementState


def run_query(
    sql: str,
    warehouse_id: str,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    timeout_seconds: int = 120,
) -> list[dict]:
    """Execute SQL on a SQL Warehouse and return rows as list of dicts."""
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    response = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        wait_timeout=f"{timeout_seconds}s",
    )

    # Poll until done
    start = time.time()
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.time() - start > timeout_seconds:
            w.statement_execution.cancel_execution(response.statement_id)
            raise TimeoutError(f"Query timed out after {timeout_seconds}s")
        time.sleep(1)
        response = w.statement_execution.get_statement(response.statement_id)

    if response.status.state == StatementState.FAILED:
        raise RuntimeError(f"Query failed: {response.status.error.message}")

    if not response.result or not response.result.data_array:
        return []

    cols = [c.name for c in response.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in response.result.data_array]


def get_warehouse_id(name_or_id: str) -> str:
    """Resolve a warehouse by name or ID."""
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    warehouses = list(w.warehouses.list())

    # Try exact ID match
    for wh in warehouses:
        if wh.id == name_or_id:
            return wh.id

    # Try name match (case-insensitive)
    for wh in warehouses:
        if wh.name and wh.name.lower() == name_or_id.lower():
            return wh.id

    raise ValueError(f"No warehouse found with name or ID: '{name_or_id}'")


def list_warehouses() -> list[dict]:
    """List all SQL warehouses in the workspace."""
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    return [
        {
            "id": wh.id,
            "name": wh.name,
            "size": wh.cluster_size,
            "state": wh.state.value if wh.state else "UNKNOWN",
            "type": wh.warehouse_type.value if wh.warehouse_type else "CLASSIC",
        }
        for wh in w.warehouses.list()
    ]
