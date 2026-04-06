"""
Unit tests for sql.py — SQL Warehouse execution.

NOTE: sql.py uses lazy local imports, so we patch at databricks_agent.connect level.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from databricks_agent.sql import get_warehouse_id, list_warehouses, run_query

_PATCH_CLIENT = "databricks_agent.connect.get_workspace_client"


# ── get_warehouse_id ───────────────────────────────────────────────────────────

def test_get_warehouse_id_by_exact_id():
    wh = MagicMock()
    wh.id = "wh-001"
    wh.name = "Prod Warehouse"

    w = MagicMock()
    w.warehouses.list.return_value = [wh]

    with patch(_PATCH_CLIENT, return_value=w):
        result = get_warehouse_id("wh-001")

    assert result == "wh-001"


def test_get_warehouse_id_by_name_case_insensitive():
    wh = MagicMock()
    wh.id = "wh-002"
    wh.name = "Prod Warehouse"

    w = MagicMock()
    w.warehouses.list.return_value = [wh]

    with patch(_PATCH_CLIENT, return_value=w):
        result = get_warehouse_id("prod warehouse")

    assert result == "wh-002"


def test_get_warehouse_id_not_found():
    wh = MagicMock()
    wh.id = "wh-999"
    wh.name = "Other"

    w = MagicMock()
    w.warehouses.list.return_value = [wh]

    with patch(_PATCH_CLIENT, return_value=w):
        with pytest.raises(ValueError, match="No warehouse found"):
            get_warehouse_id("nonexistent")


def test_get_warehouse_id_prefers_id_over_name():
    """If a warehouse ID happens to match another warehouse's name, ID wins."""
    wh_a = MagicMock()
    wh_a.id = "wh-001"
    wh_a.name = "wh-002"  # name is the ID of another warehouse

    wh_b = MagicMock()
    wh_b.id = "wh-002"
    wh_b.name = "Staging"

    w = MagicMock()
    w.warehouses.list.return_value = [wh_a, wh_b]

    with patch(_PATCH_CLIENT, return_value=w):
        result = get_warehouse_id("wh-002")

    assert result == "wh-002"  # exact ID match for wh_b wins


# ── run_query ──────────────────────────────────────────────────────────────────

def test_run_query_returns_rows_as_dicts():
    from databricks.sdk.service.sql import StatementState

    col1 = MagicMock()
    col1.name = "order_id"
    col2 = MagicMock()
    col2.name = "revenue"

    resp = MagicMock()
    resp.status.state = StatementState.SUCCEEDED
    resp.statement_id = "stmt-001"
    resp.result.data_array = [["1001", "999.99"], ["1002", "1234.56"]]
    resp.manifest.schema.columns = [col1, col2]

    w = MagicMock()
    w.statement_execution.execute_statement.return_value = resp

    with patch(_PATCH_CLIENT, return_value=w):
        rows = run_query("SELECT order_id, revenue FROM gold.orders LIMIT 2", "wh-001")

    assert len(rows) == 2
    assert rows[0] == {"order_id": "1001", "revenue": "999.99"}
    assert rows[1] == {"order_id": "1002", "revenue": "1234.56"}


def test_run_query_empty_result():
    from databricks.sdk.service.sql import StatementState

    resp = MagicMock()
    resp.status.state = StatementState.SUCCEEDED
    resp.result.data_array = None

    w = MagicMock()
    w.statement_execution.execute_statement.return_value = resp

    with patch(_PATCH_CLIENT, return_value=w):
        rows = run_query("SELECT * FROM empty_table", "wh-001")

    assert rows == []


def test_run_query_raises_on_failure():
    from databricks.sdk.service.sql import StatementState

    resp = MagicMock()
    resp.status.state = StatementState.FAILED
    resp.status.error.message = "Table not found: gold.missing_table"

    w = MagicMock()
    w.statement_execution.execute_statement.return_value = resp

    with patch(_PATCH_CLIENT, return_value=w):
        with pytest.raises(RuntimeError, match="Table not found"):
            run_query("SELECT * FROM gold.missing_table", "wh-001")


def test_run_query_timeout_cancels():
    """
    BUG FIX: start time is now set BEFORE execute_statement, so the total
    timeout is correctly bounded to timeout_seconds (not 2×).
    """
    from databricks.sdk.service.sql import StatementState

    resp_pending = MagicMock()
    resp_pending.status.state = StatementState.PENDING
    resp_pending.statement_id = "stmt-timeout"

    w = MagicMock()
    w.statement_execution.execute_statement.return_value = resp_pending
    w.statement_execution.get_statement.return_value = resp_pending

    with patch(_PATCH_CLIENT, return_value=w):
        with patch("databricks_agent.sql.time") as mock_time:
            # start=0, sdk call takes nothing, first poll check sees time=200 > 120
            mock_time.time.side_effect = [0, 0, 200]
            mock_time.sleep = MagicMock()
            with pytest.raises(TimeoutError, match="timed out"):
                run_query("SELECT SLEEP(300)", "wh-001", timeout_seconds=120)

    w.statement_execution.cancel_execution.assert_called_once_with("stmt-timeout")


def test_run_query_with_catalog_and_schema():
    """catalog and schema parameters are passed to execute_statement."""
    from databricks.sdk.service.sql import StatementState

    resp = MagicMock()
    resp.status.state = StatementState.SUCCEEDED
    resp.result.data_array = None

    w = MagicMock()
    w.statement_execution.execute_statement.return_value = resp

    with patch(_PATCH_CLIENT, return_value=w):
        run_query("SELECT 1", "wh-001", catalog="my_catalog", schema="gold")

    call_kwargs = w.statement_execution.execute_statement.call_args.kwargs
    assert call_kwargs["catalog"] == "my_catalog"
    assert call_kwargs["schema"] == "gold"


# ── list_warehouses ────────────────────────────────────────────────────────────

def test_list_warehouses_formats_state():
    wh = MagicMock()
    wh.id = "wh-123"
    wh.name = "Starter"
    wh.cluster_size = "X-Small"
    wh.state.value = "RUNNING"
    wh.warehouse_type.value = "PRO"

    w = MagicMock()
    w.warehouses.list.return_value = [wh]

    with patch(_PATCH_CLIENT, return_value=w):
        result = list_warehouses()

    assert result[0]["state"] == "RUNNING"
    assert result[0]["type"] == "PRO"
    assert result[0]["size"] == "X-Small"


def test_list_warehouses_handles_none_state():
    wh = MagicMock()
    wh.id = "wh-456"
    wh.name = "Stopped"
    wh.cluster_size = "Small"
    wh.state = None
    wh.warehouse_type = None

    w = MagicMock()
    w.warehouses.list.return_value = [wh]

    with patch(_PATCH_CLIENT, return_value=w):
        result = list_warehouses()

    assert result[0]["state"] == "UNKNOWN"
    assert result[0]["type"] == "CLASSIC"


def test_list_warehouses_multiple():
    def _wh(i, state="RUNNING"):
        wh = MagicMock()
        wh.id = f"wh-{i}"
        wh.name = f"Warehouse {i}"
        wh.cluster_size = "Medium"
        wh.state.value = state
        wh.warehouse_type.value = "PRO"
        return wh

    w = MagicMock()
    w.warehouses.list.return_value = [_wh(1), _wh(2, "STOPPED"), _wh(3)]

    with patch(_PATCH_CLIENT, return_value=w):
        result = list_warehouses()

    assert len(result) == 3
    assert result[1]["state"] == "STOPPED"
