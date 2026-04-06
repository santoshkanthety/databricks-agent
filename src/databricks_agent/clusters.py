"""Databricks cluster management."""

from __future__ import annotations

from typing import Optional


def list_clusters() -> list[dict]:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    return [
        {
            "cluster_id": c.cluster_id,
            "name": c.cluster_name,
            "state": c.state.value if c.state else "UNKNOWN",
            "driver_node": c.driver_node_type_id,
            "worker_node": c.node_type_id,
            "num_workers": c.num_workers,
            "spark_version": c.spark_version,
            "creator": c.creator_user_name,
        }
        for c in w.clusters.list()
    ]


def start_cluster(cluster_id: Optional[str] = None, cluster_name: Optional[str] = None) -> None:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    if cluster_name and not cluster_id:
        cluster_id = _resolve_cluster_id(w, cluster_name)
    w.clusters.start(cluster_id=cluster_id)


def terminate_cluster(cluster_id: Optional[str] = None, cluster_name: Optional[str] = None) -> None:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    if cluster_name and not cluster_id:
        cluster_id = _resolve_cluster_id(w, cluster_name)
    w.clusters.delete(cluster_id=cluster_id)


def get_cluster_info(cluster_id: Optional[str] = None, cluster_name: Optional[str] = None) -> dict:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    if cluster_name and not cluster_id:
        cluster_id = _resolve_cluster_id(w, cluster_name)
    c = w.clusters.get(cluster_id=cluster_id)
    return {
        "cluster_id": c.cluster_id,
        "name": c.cluster_name,
        "state": c.state.value if c.state else "UNKNOWN",
        "driver_node": c.driver_node_type_id,
        "worker_node": c.node_type_id,
        "num_workers": c.num_workers,
        "autoscale_min": c.autoscale.min_workers if c.autoscale else None,
        "autoscale_max": c.autoscale.max_workers if c.autoscale else None,
        "spark_version": c.spark_version,
        "spark_conf": c.spark_conf,
        "creator": c.creator_user_name,
    }


def _resolve_cluster_id(w, name: str) -> str:
    for c in w.clusters.list():
        if c.cluster_name and c.cluster_name.lower() == name.lower():
            return c.cluster_id
    raise ValueError(f"No cluster found with name: '{name}'")
