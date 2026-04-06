"""
Unit tests for catalog.py — Unity Catalog operations.

All Databricks SDK calls are mocked so tests run without a live workspace.

NOTE: catalog.py uses lazy local imports inside each function
      (from databricks_agent.connect import get_workspace_client),
      so we patch at the connect module level.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from databricks_agent.catalog import (
    describe_table,
    get_table_lineage,
    grant_permission,
    list_schemas,
    list_tables,
    run_governance_audit,
    show_grants,
)

_PATCH_CLIENT = "databricks_agent.connect.get_workspace_client"


# ── helpers ────────────────────────────────────────────────────────────────────

def _mock_table(
    name="orders",
    full_name="catalog.gold.orders",
    table_type="EXTERNAL",
    fmt="DELTA",
    owner="data_team",
    comment="Order fact table",
    tags=None,
    columns=None,
):
    t = MagicMock()
    t.name = name
    t.full_name = full_name
    t.table_type.value = table_type
    t.data_source_format.value = fmt
    t.owner = owner
    t.comment = comment
    t.tags = tags if tags is not None else {}
    t.created_at = 1700000000000
    t.updated_at = 1700000001000
    t.columns = columns or []
    t.storage_location = f"abfss://gold@storage.dfs.core.windows.net/{name}"
    t.properties = {}
    return t


# ── list_schemas ───────────────────────────────────────────────────────────────

def test_list_schemas_returns_dicts():
    w = MagicMock()
    schema = MagicMock()
    schema.name = "gold"
    schema.catalog_name = "my_catalog"
    schema.owner = "data_team"
    schema.comment = "Gold layer"
    w.schemas.list.return_value = [schema]

    with patch(_PATCH_CLIENT, return_value=w):
        result = list_schemas("my_catalog")

    assert len(result) == 1
    assert result[0]["name"] == "gold"
    assert result[0]["owner"] == "data_team"


def test_list_schemas_empty():
    w = MagicMock()
    w.schemas.list.return_value = []

    with patch(_PATCH_CLIENT, return_value=w):
        result = list_schemas("empty_catalog")

    assert result == []


# ── list_tables ────────────────────────────────────────────────────────────────

def test_list_tables_marks_undocumented():
    t = MagicMock()
    t.name = "mystery"
    t.full_name = "cat.gold.mystery"
    t.table_type.value = "EXTERNAL"
    t.data_source_format.value = "DELTA"
    t.owner = "someone"
    t.comment = None
    t.tags = {}
    t.created_at = 0
    t.updated_at = 0

    w = MagicMock()
    w.tables.list.return_value = [t]

    with patch(_PATCH_CLIENT, return_value=w):
        result = list_tables("cat", "gold")

    assert result[0]["comment"] == "⚠️ UNDOCUMENTED"


def test_list_tables_with_comment():
    t = _mock_table()
    w = MagicMock()
    w.tables.list.return_value = [t]

    with patch(_PATCH_CLIENT, return_value=w):
        result = list_tables("catalog", "gold")

    assert result[0]["comment"] == "Order fact table"
    assert result[0]["owner"] == "data_team"


def test_list_tables_multiple():
    tables = [_mock_table(name=f"t{i}", full_name=f"cat.gold.t{i}") for i in range(5)]
    w = MagicMock()
    w.tables.list.return_value = tables

    with patch(_PATCH_CLIENT, return_value=w):
        result = list_tables("cat", "gold")

    assert len(result) == 5


# ── describe_table ─────────────────────────────────────────────────────────────

def test_describe_table_no_dead_unpack():
    """
    BUG FIX: describe_table previously did:
        catalog_name, schema_name, table_name = full_name.split(".")
    These variables were never used. Fix removes the unpack entirely.
    Verify that the API is called with full_name directly.
    """
    col = MagicMock()
    col.name = "order_id"
    col.type_text = "BIGINT"
    col.nullable = False
    col.comment = "Primary key"
    col.tags = {}

    t = _mock_table(columns=[col])
    w = MagicMock()
    w.tables.get.return_value = t

    with patch(_PATCH_CLIENT, return_value=w):
        result = describe_table("catalog.gold.orders")

    assert result["full_name"] == "catalog.gold.orders"
    assert len(result["columns"]) == 1
    assert result["columns"][0]["name"] == "order_id"
    w.tables.get.assert_called_once_with(full_name="catalog.gold.orders")


def test_describe_table_empty_columns():
    t = _mock_table(columns=[])
    w = MagicMock()
    w.tables.get.return_value = t

    with patch(_PATCH_CLIENT, return_value=w):
        result = describe_table("catalog.silver.customers")

    assert result["columns"] == []


def test_describe_table_undocumented_fallback():
    t = _mock_table(comment=None)
    t.comment = None
    w = MagicMock()
    w.tables.get.return_value = t

    with patch(_PATCH_CLIENT, return_value=w):
        result = describe_table("catalog.bronze.raw")

    assert result["comment"] == "⚠️ UNDOCUMENTED"


# ── get_table_lineage ──────────────────────────────────────────────────────────

def test_get_table_lineage_upstreams_and_downstreams():
    """
    BUG FIX: downstreams previously used d.table_name (wrong field),
    now uses d.table_info.name consistent with upstreams.
    """
    upstream = MagicMock()
    upstream.table_info.name = "catalog.bronze.raw_orders"

    downstream = MagicMock()
    downstream.table_info.name = "catalog.gold.revenue_summary"

    lineage = MagicMock()
    lineage.upstreams = [upstream]
    lineage.downstreams = [downstream]

    w = MagicMock()
    w.lineage_tracking.table_lineage.return_value = lineage

    with patch(_PATCH_CLIENT, return_value=w):
        result = get_table_lineage("catalog.silver.orders")

    assert result["upstreams"] == ["catalog.bronze.raw_orders"]
    assert result["downstreams"] == ["catalog.gold.revenue_summary"]


def test_get_table_lineage_empty():
    lineage = MagicMock()
    lineage.upstreams = []
    lineage.downstreams = []

    w = MagicMock()
    w.lineage_tracking.table_lineage.return_value = lineage

    with patch(_PATCH_CLIENT, return_value=w):
        result = get_table_lineage("catalog.gold.revenue")

    assert result["upstreams"] == []
    assert result["downstreams"] == []


def test_get_table_lineage_skips_null_table_info():
    """table_info can be None for external nodes — should be filtered out."""
    upstream_no_info = MagicMock()
    upstream_no_info.table_info = None

    upstream_with_info = MagicMock()
    upstream_with_info.table_info.name = "catalog.bronze.events"

    downstream_no_info = MagicMock()
    downstream_no_info.table_info = None

    lineage = MagicMock()
    lineage.upstreams = [upstream_no_info, upstream_with_info]
    lineage.downstreams = [downstream_no_info]

    w = MagicMock()
    w.lineage_tracking.table_lineage.return_value = lineage

    with patch(_PATCH_CLIENT, return_value=w):
        result = get_table_lineage("catalog.silver.events")

    assert result["upstreams"] == ["catalog.bronze.events"]
    assert result["downstreams"] == []


def test_get_table_lineage_multi_hop():
    """Medallion 3-hop lineage: bronze → silver → gold, both sides populated."""
    up1 = MagicMock()
    up1.table_info.name = "cat.bronze.raw"
    up2 = MagicMock()
    up2.table_info.name = "cat.bronze.raw_customers"

    down1 = MagicMock()
    down1.table_info.name = "cat.gold.revenue"

    lineage = MagicMock()
    lineage.upstreams = [up1, up2]
    lineage.downstreams = [down1]

    w = MagicMock()
    w.lineage_tracking.table_lineage.return_value = lineage

    with patch(_PATCH_CLIENT, return_value=w):
        result = get_table_lineage("cat.silver.orders")

    assert len(result["upstreams"]) == 2
    assert len(result["downstreams"]) == 1


# ── run_governance_audit ───────────────────────────────────────────────────────

def test_governance_audit_catches_all_issues():
    table_bad = MagicMock()
    table_bad.full_name = "catalog.gold.mystery_table"
    table_bad.comment = None
    table_bad.owner = None
    table_bad.tags = {}

    schema = MagicMock()
    schema.name = "gold"

    w = MagicMock()
    w.schemas.list.return_value = [schema]
    w.tables.list.return_value = [table_bad]

    with patch(_PATCH_CLIENT, return_value=w):
        issues = run_governance_audit("catalog")

    assert len(issues) == 1
    assert "missing comment" in issues[0]["issues"]
    assert "no owner" in issues[0]["issues"]
    assert "no tags" in issues[0]["issues"]


def test_governance_audit_clean_table_no_issues():
    table_good = MagicMock()
    table_good.full_name = "catalog.gold.orders"
    table_good.comment = "Order fact table"
    table_good.owner = "data_team"
    table_good.tags = {"domain": "commerce"}

    schema = MagicMock()
    schema.name = "gold"

    w = MagicMock()
    w.schemas.list.return_value = [schema]
    w.tables.list.return_value = [table_good]

    with patch(_PATCH_CLIENT, return_value=w):
        issues = run_governance_audit("catalog")

    assert issues == []


def test_governance_audit_schema_filter():
    """When schema_name is provided, only that schema is checked."""
    schema = MagicMock()
    schema.name = "gold"

    w = MagicMock()
    w.schemas.get.return_value = schema
    w.tables.list.return_value = []

    with patch(_PATCH_CLIENT, return_value=w):
        issues = run_governance_audit("catalog", schema_name="gold")

    w.schemas.get.assert_called_once_with(full_name="catalog.gold")
    assert issues == []


# ── show_grants ────────────────────────────────────────────────────────────────

def test_show_grants_returns_principal_privilege_pairs():
    priv1 = MagicMock()
    priv1.value = "SELECT"
    priv2 = MagicMock()
    priv2.value = "MODIFY"

    pa = MagicMock()
    pa.principal = "analytics_team"
    pa.privileges = [priv1, priv2]

    perms = MagicMock()
    perms.privilege_assignments = [pa]

    w = MagicMock()
    w.grants.get.return_value = perms

    # SecurableType is locally imported inside show_grants — patch at source
    with patch(_PATCH_CLIENT, return_value=w):
        with patch("databricks.sdk.service.catalog.SecurableType", MagicMock()):
            result = show_grants("catalog.gold.orders", "TABLE")

    assert result[0]["principal"] == "analytics_team"
    assert "SELECT" in result[0]["privileges"]
    assert "MODIFY" in result[0]["privileges"]


def test_show_grants_empty():
    perms = MagicMock()
    perms.privilege_assignments = None

    w = MagicMock()
    w.grants.get.return_value = perms

    with patch(_PATCH_CLIENT, return_value=w):
        with patch("databricks.sdk.service.catalog.SecurableType", MagicMock()):
            result = show_grants("catalog.gold.orders", "TABLE")

    assert result == []
