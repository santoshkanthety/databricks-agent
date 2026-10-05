"""Install/uninstall databricks-agent Claude Code skills.

Skills ship in the canonical Agent Skills layout — one directory per skill with
``SKILL.md`` as the entrypoint — and are installed to
``~/.claude/skills/<skill-name>/SKILL.md``, which is the only layout Claude Code
discovers. Skill metadata (name, description) is read from each ``SKILL.md``
frontmatter, so the files themselves are the single source of truth.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

from rich.console import Console
from rich.table import Table

console = Console()

CLAUDE_SKILLS_DIR = Path.home() / ".claude" / "skills"

# Skills source resolution — works for both `pip install` and `git clone` setups:
#   Installed (pip):  skills live at databricks_agent/skills/data/  (force-include)
#   Development:      skills live at project_root/skills/
_PKG_DATA_DIR = Path(__file__).parent / "data"
_REPO_SKILLS_DIR = Path(__file__).parent.parent.parent.parent / "skills"
SKILLS_SOURCE_DIR = _PKG_DATA_DIR if _PKG_DATA_DIR.is_dir() else _REPO_SKILLS_DIR

SKILL_NAMES = [
    "databricks-connect",
    "databricks-cli",
    "databricks-asset-bundles",
    "databricks-fabric-apps",
    "databricks-data-catalog-lineage",
    "databricks-data-transformation",
    "databricks-spark-sql-mastery",
    "databricks-dlt-pipelines",
    "databricks-metric-glossary",
    "databricks-medallion-architecture",
    "databricks-performance-scale",
    "databricks-project-management",
    "databricks-dashboard-authoring",
    "databricks-security-governance",
    "databricks-source-integration",
    "databricks-delta-modeling",
    "databricks-testing-validation",
    "databricks-time-series-data",
    "databricks-data-governance-traceability",
    "databricks-cyber-security",
]

# Back-compat alias — this list was named SKILL_FILES and held "<name>.md" strings
# before the canonical directory layout landed.
SKILL_FILES = [f"{name}.md" for name in SKILL_NAMES]

# Legacy flat-file names installed by databricks-agent <= 0.1. Claude Code never
# discovered these (it requires <skill-name>/SKILL.md), so they are dead files in
# ~/.claude/skills/. Several also collided with powerbi-agent's identically named
# files, so whichever pack installed last silently won.
_LEGACY_FLAT_NAMES = [
    "databricks-connect",
    "data-catalog-lineage",
    "data-transformation",
    "spark-sql-mastery",
    "dlt-pipelines",
    "metric-glossary",
    "medallion-architecture",
    "performance-scale",
    "project-management",
    "dashboard-authoring",
    "security-governance",
    "source-integration",
    "delta-modeling",
    "testing-validation",
    "time-series-data",
    "data-governance-traceability",
    "cyber-security",
]


def read_skill_description(skill_md: Path) -> str:
    """Read the ``description`` field from a SKILL.md frontmatter block."""
    try:
        text = skill_md.read_text(encoding="utf-8")
    except OSError:
        return ""
    if not text.startswith("---"):
        return ""
    end = text.find("\n---", 3)
    if end == -1:
        return ""
    m = re.search(r"^description:\s*(.+)$", text[3:end], re.M)
    return m.group(1).strip() if m else ""


def _clean_legacy_flat_files() -> int:
    """Remove pre-0.2 flat ``<name>.md`` skill files that Claude Code never loaded."""
    removed = 0
    for legacy in _LEGACY_FLAT_NAMES:
        stale = CLAUDE_SKILLS_DIR / f"{legacy}.md"
        if stale.is_file():
            stale.unlink()
            removed += 1
    return removed


def install_skills(force: bool = False) -> int:
    """Copy skill directories to ~/.claude/skills/. Returns count of installed skills."""
    CLAUDE_SKILLS_DIR.mkdir(parents=True, exist_ok=True)
    installed = 0

    for skill_name in SKILL_NAMES:
        src = SKILLS_SOURCE_DIR / skill_name
        dst = CLAUDE_SKILLS_DIR / skill_name

        if not (src / "SKILL.md").exists():
            console.print(f"  [yellow]⚠[/yellow] Source not found: {src}")
            continue

        if dst.exists() and not force:
            console.print(f"  [dim]→ Skipped (exists): {skill_name}[/dim]")
            continue

        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)
        console.print(f"  [green]✓[/green] Installed: {skill_name}")
        installed += 1

    legacy = _clean_legacy_flat_files()
    if legacy:
        console.print(f"  [dim]Cleaned up {legacy} legacy flat skill file(s) from a previous version[/dim]")

    return installed


def uninstall_skills() -> int:
    """Remove databricks skills from ~/.claude/skills/. Returns count removed."""
    removed = 0
    for skill_name in SKILL_NAMES:
        dst = CLAUDE_SKILLS_DIR / skill_name
        if dst.is_dir():
            shutil.rmtree(dst)
            console.print(f"  [red]✗[/red] Removed: {skill_name}")
            removed += 1

    removed += _clean_legacy_flat_files()
    return removed


def list_skills() -> None:
    """Display skill installation status table."""
    table = Table(title="Databricks Agent Skills", show_header=True, header_style="bold cyan")
    table.add_column("Skill", style="bold")
    table.add_column("Status", width=10)
    table.add_column("Description")

    for skill_name in SKILL_NAMES:
        dst = CLAUDE_SKILLS_DIR / skill_name
        installed = (dst / "SKILL.md").exists()
        status = "[green]Installed[/green]" if installed else "[dim]Not installed[/dim]"
        desc = read_skill_description(SKILLS_SOURCE_DIR / skill_name / "SKILL.md")
        # The description doubles as trigger text, so trim it for the table.
        table.add_row(skill_name, status, desc.split(". Use when", 1)[0])

    console.print(table)
