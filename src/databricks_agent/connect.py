"""Databricks workspace connection management."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

CONFIG_DIR = Path.home() / ".databricks-agent"
CONFIG_FILE = CONFIG_DIR / "config.json"
DATABRICKS_CFG = Path.home() / ".databrickscfg"


def get_config() -> dict:
    """Load saved connection config."""
    if CONFIG_FILE.exists():
        return json.loads(CONFIG_FILE.read_text())
    return {}


def save_config(config: dict) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(json.dumps(config, indent=2))


def get_workspace_client(profile: Optional[str] = None):
    """Return an authenticated WorkspaceClient using saved config or env vars."""
    from databricks.sdk import WorkspaceClient

    config = get_config()
    host = config.get("host") or os.environ.get("DATABRICKS_HOST")
    token = config.get("token") or os.environ.get("DATABRICKS_TOKEN")

    if profile:
        return WorkspaceClient(profile=profile)
    if host and token:
        return WorkspaceClient(host=host, token=token)
    # Fall back to SDK auto-discovery (.databrickscfg, env vars, OAuth)
    return WorkspaceClient()


def list_profiles() -> list[dict]:
    """List profiles from ~/.databrickscfg."""
    profiles = []
    if not DATABRICKS_CFG.exists():
        return profiles

    current: dict = {}
    for line in DATABRICKS_CFG.read_text().splitlines():
        line = line.strip()
        if line.startswith("[") and line.endswith("]"):
            if current:
                profiles.append(current)
            current = {"profile": line[1:-1]}
        elif "=" in line:
            key, _, value = line.partition("=")
            current[key.strip()] = value.strip()
    if current:
        profiles.append(current)
    return profiles


def test_connection() -> tuple[bool, str]:
    """Verify workspace connectivity. Returns (success, message)."""
    try:
        w = get_workspace_client()
        me = w.current_user.me()
        return True, f"Connected as {me.user_name} to {w.config.host}"
    except Exception as e:
        return False, str(e)
