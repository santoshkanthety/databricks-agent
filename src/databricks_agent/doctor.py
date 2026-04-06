"""Environment diagnostics for databricks-agent."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

from rich.console import Console
from rich.table import Table

console = Console()

_checks: list[tuple[str, callable]] = []


def check(name: str):
    """Decorator to register a diagnostic check."""
    def decorator(fn):
        _checks.append((name, fn))
        return fn
    return decorator


@check("Python version")
def _check_python():
    v = sys.version_info
    ok = v >= (3, 10)
    return ok, f"Python {v.major}.{v.minor}.{v.micro}", "Requires Python >= 3.10"


@check("databricks-sdk installed")
def _check_sdk():
    try:
        import databricks.sdk  # noqa: F401
        import importlib.metadata
        version = importlib.metadata.version("databricks-sdk")
        return True, f"databricks-sdk {version}", ""
    except ImportError:
        return False, "Not installed", "Run: pip install databricks-sdk"


@check("Workspace connection")
def _check_connection():
    try:
        from databricks_agent.connect import test_connection
        ok, msg = test_connection()
        return ok, msg, "Run: databricks-agent connect"
    except Exception as e:
        return False, str(e), "Run: databricks-agent connect"


@check("Default warehouse configured")
def _check_warehouse():
    from databricks_agent.connect import get_config
    config = get_config()
    if config.get("default_warehouse"):
        return True, config["default_warehouse"], ""
    return False, "Not set", "Run: databricks-agent connect (select a warehouse)"


@check("Default catalog configured")
def _check_catalog():
    from databricks_agent.connect import get_config
    config = get_config()
    if config.get("default_catalog"):
        return True, config["default_catalog"], ""
    return False, "Not set (optional)", "Run: databricks-agent connect"


@check("Skills installed")
def _check_skills():
    skills_dir = Path.home() / ".claude" / "skills"
    if not skills_dir.exists():
        return False, "~/.claude/skills/ not found", "Run: databricks-agent skills install"
    installed = list(skills_dir.glob("databricks-*.md"))
    count = len(installed)
    if count == 0:
        return False, "No databricks skills installed", "Run: databricks-agent skills install"
    return True, f"{count} skills installed in ~/.claude/skills/", ""


@check("FastAPI (optional, for UI)")
def _check_fastapi():
    try:
        import fastapi  # noqa: F401
        return True, f"fastapi {importlib.metadata.version('fastapi')}", ""
    except Exception:
        return False, "Not installed (optional)", "Run: pip install databricks-agent[ui]"


@check("MLflow (optional, for ML workflows)")
def _check_mlflow():
    try:
        import mlflow  # noqa: F401
        return True, f"mlflow {mlflow.__version__}", ""
    except Exception:
        return False, "Not installed (optional)", "Run: pip install databricks-agent[ml]"


def run_doctor() -> bool:
    """Run all diagnostics. Returns True if all required checks pass."""
    table = Table(title="databricks-agent Environment Check", show_header=True, header_style="bold cyan")
    table.add_column("Check", style="bold")
    table.add_column("Status", width=6)
    table.add_column("Details")
    table.add_column("Fix")

    all_ok = True
    for name, fn in _checks:
        try:
            ok, detail, fix = fn()
        except Exception as e:
            ok, detail, fix = False, "Error", str(e)
        status = "[green]✓[/green]" if ok else "[red]✗[/red]"
        if not ok and "optional" not in detail.lower():
            all_ok = False
        table.add_row(name, status, detail, fix)

    console.print(table)
    return all_ok
