"""Delta Live Tables (DLT) pipeline management."""

from __future__ import annotations

from typing import Optional


def list_pipelines() -> list[dict]:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    return [
        {
            "pipeline_id": p.pipeline_id,
            "name": p.name,
            "state": p.state.value if p.state else "UNKNOWN",
            "creator": p.creator_user_name,
            "target": p.target,
        }
        for p in w.pipelines.list_pipelines()
    ]


def get_pipeline_status(pipeline_id: Optional[str] = None, name: Optional[str] = None) -> dict:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    if name and not pipeline_id:
        pipeline_id = _resolve_pipeline_id(w, name)

    p = w.pipelines.get(pipeline_id=pipeline_id)
    return {
        "pipeline_id": p.pipeline_id,
        "name": p.name,
        "state": p.state.value if p.state else "UNKNOWN",
        "cause": p.cause,
        "cluster_id": p.cluster_id,
        "creator": p.creator_user_name,
        "last_modified": p.last_modified,
    }


def start_pipeline(pipeline_id: Optional[str] = None, name: Optional[str] = None,
                   full_refresh: bool = False) -> None:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    if name and not pipeline_id:
        pipeline_id = _resolve_pipeline_id(w, name)

    w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=full_refresh)


def stop_pipeline(pipeline_id: Optional[str] = None, name: Optional[str] = None) -> None:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    if name and not pipeline_id:
        pipeline_id = _resolve_pipeline_id(w, name)

    w.pipelines.stop(pipeline_id=pipeline_id)


def get_pipeline_events(pipeline_id: Optional[str] = None, name: Optional[str] = None,
                        limit: int = 20) -> list[dict]:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    if name and not pipeline_id:
        pipeline_id = _resolve_pipeline_id(w, name)

    events = []
    for event in w.pipelines.list_pipeline_events(pipeline_id=pipeline_id):
        events.append({
            "timestamp": event.timestamp,
            "level": event.level.value if event.level else "INFO",
            "event_type": event.event_type,
            "message": event.message,
            "maturity_level": event.maturity_level.value if event.maturity_level else "—",
        })
        if len(events) >= limit:
            break
    return events


def _resolve_pipeline_id(w, name: str) -> str:
    for p in w.pipelines.list_pipelines():
        if p.name and p.name.lower() == name.lower():
            return p.pipeline_id
    raise ValueError(f"No pipeline found with name: '{name}'")
