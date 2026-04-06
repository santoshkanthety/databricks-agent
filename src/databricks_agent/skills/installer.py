"""Install/uninstall databricks-agent Claude Code skills."""

from __future__ import annotations

import shutil
from pathlib import Path

from rich.console import Console
from rich.table import Table

console = Console()

SKILLS_SOURCE_DIR = Path(__file__).parent.parent.parent.parent / "skills"
CLAUDE_SKILLS_DIR = Path.home() / ".claude" / "skills"

SKILL_FILES = [
    "databricks-connect.md",
    "data-catalog-lineage.md",
    "data-transformation.md",
    "spark-sql-mastery.md",
    "dlt-pipelines.md",
    "metric-glossary.md",
    "medallion-architecture.md",
    "performance-scale.md",
    "project-management.md",
    "dashboard-authoring.md",
    "security-governance.md",
    "source-integration.md",
    "delta-modeling.md",
    "testing-validation.md",
    "time-series-data.md",
    "data-governance-traceability.md",
    "cyber-security.md",
]


def install_skills(force: bool = False) -> int:
    """Copy skill files to ~/.claude/skills/. Returns count of installed files."""
    CLAUDE_SKILLS_DIR.mkdir(parents=True, exist_ok=True)
    installed = 0

    for skill_file in SKILL_FILES:
        src = SKILLS_SOURCE_DIR / skill_file
        dst = CLAUDE_SKILLS_DIR / skill_file

        if not src.exists():
            console.print(f"  [yellow]⚠[/yellow] Source not found: {src}")
            continue

        if dst.exists() and not force:
            console.print(f"  [dim]→ Skipped (exists): {skill_file}[/dim]")
            continue

        shutil.copy2(src, dst)
        console.print(f"  [green]✓[/green] Installed: {skill_file}")
        installed += 1

    return installed


def uninstall_skills() -> int:
    """Remove databricks skill files from ~/.claude/skills/. Returns count removed."""
    removed = 0
    for skill_file in SKILL_FILES:
        dst = CLAUDE_SKILLS_DIR / skill_file
        if dst.exists():
            dst.unlink()
            console.print(f"  [red]✗[/red] Removed: {skill_file}")
            removed += 1
    return removed


def list_skills() -> None:
    """Display skill installation status table."""
    table = Table(title="Databricks Agent Skills", show_header=True, header_style="bold cyan")
    table.add_column("Skill File", style="bold")
    table.add_column("Status", width=10)
    table.add_column("Location")

    for skill_file in SKILL_FILES:
        dst = CLAUDE_SKILLS_DIR / skill_file
        installed = dst.exists()
        status = "[green]Installed[/green]" if installed else "[dim]Not installed[/dim]"
        location = str(dst) if installed else "—"
        table.add_row(skill_file, status, location)

    console.print(table)
