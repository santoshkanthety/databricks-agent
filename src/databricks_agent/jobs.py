"""Databricks Jobs / Workflows management."""

from __future__ import annotations

from typing import Optional


def list_jobs(name_filter: Optional[str] = None) -> list[dict]:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    jobs = []
    for job in w.jobs.list(name=name_filter):
        jobs.append({
            "job_id": job.job_id,
            "name": job.settings.name if job.settings else "—",
            "creator": job.creator_user_name,
            "created_time": job.created_time,
        })
    return jobs


def run_job(job_id: Optional[int] = None, job_name: Optional[str] = None,
            notebook_params: Optional[dict] = None) -> int:
    """Trigger a job run by ID or name. Returns run_id."""
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()

    if job_name and not job_id:
        for job in w.jobs.list(name=job_name):
            job_id = job.job_id
            break
        if not job_id:
            raise ValueError(f"No job found with name: '{job_name}'")

    run = w.jobs.run_now(job_id=job_id, notebook_params=notebook_params or {})
    return run.run_id


def get_run_status(run_id: int) -> dict:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    run = w.jobs.get_run(run_id=run_id)
    state = run.state
    return {
        "run_id": run_id,
        "job_id": run.job_id,
        "state": state.life_cycle_state.value if state else "UNKNOWN",
        "result": state.result_state.value if state and state.result_state else "—",
        "message": state.state_message if state else "",
        "start_time": run.start_time,
        "end_time": run.end_time,
        "url": run.run_page_url,
    }


def cancel_run(run_id: int) -> None:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()
    w.jobs.cancel_run(run_id=run_id)


def get_run_history(job_id: Optional[int] = None, job_name: Optional[str] = None,
                    limit: int = 10) -> list[dict]:
    from databricks_agent.connect import get_workspace_client

    w = get_workspace_client()

    if job_name and not job_id:
        for job in w.jobs.list(name=job_name):
            job_id = job.job_id
            break

    runs = []
    for run in w.jobs.list_runs(job_id=job_id, limit=limit):
        state = run.state
        runs.append({
            "run_id": run.run_id,
            "state": state.life_cycle_state.value if state else "UNKNOWN",
            "result": state.result_state.value if state and state.result_state else "—",
            "start_time": run.start_time,
            "end_time": run.end_time,
        })
    return runs
