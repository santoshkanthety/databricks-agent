"""
Tests for databricks-agent skills installer.

Validates skills follow the shared schema (see SKILL_SCHEMA.md) and install into
the canonical ~/.claude/skills/<skill-name>/SKILL.md layout that Claude Code
actually discovers.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

from databricks_agent.skills.installer import (
    SKILL_NAMES,
    SKILLS_SOURCE_DIR,
    install_skills,
    list_skills,
    read_skill_description,
    uninstall_skills,
)

REPO_ROOT = Path(__file__).parent.parent

sys.path.insert(0, str(REPO_ROOT / "scripts"))
from validate_skills import validate  # noqa: E402


def test_all_skills_exist():
    missing = [n for n in SKILL_NAMES if not (SKILLS_SOURCE_DIR / n / "SKILL.md").exists()]
    assert missing == [], "Missing skills:\n" + "\n".join(missing)


def test_skill_names_match_disk():
    on_disk = {p.name for p in SKILLS_SOURCE_DIR.iterdir() if (p / "SKILL.md").exists()}
    assert set(SKILL_NAMES) == on_disk, (
        f"Only in SKILL_NAMES: {sorted(set(SKILL_NAMES) - on_disk)}. "
        f"Only on disk: {sorted(on_disk - set(SKILL_NAMES))}."
    )


def test_skills_pass_shared_schema_validator():
    """The same validator runs in powerbi-agent — the two packs share one schema."""
    errors = validate(SKILLS_SOURCE_DIR)
    assert errors == [], "Schema violations:\n" + "\n".join(errors)


def test_every_skill_is_platform_prefixed():
    """Prefixes prevent collisions with powerbi-agent's identically named skills."""
    unprefixed = [n for n in SKILL_NAMES if not n.startswith("databricks-")]
    assert unprefixed == [], f"Skills missing the databricks- prefix: {unprefixed}"


def test_every_skill_has_trigger_text_in_description():
    """Claude routes on description, so trigger phrases must be folded into it."""
    thin = []
    for name in SKILL_NAMES:
        desc = read_skill_description(SKILLS_SOURCE_DIR / name / "SKILL.md")
        if len(desc) < 40:
            thin.append(f"{name}: {desc!r}")
    assert thin == [], "Descriptions too thin to route on:\n" + "\n".join(thin)


def test_no_flat_skill_files_remain():
    stray = sorted(p.name for p in SKILLS_SOURCE_DIR.glob("*.md"))
    assert stray == [], f"Flat skill files must be converted to <name>/SKILL.md: {stray}"


def test_shares_no_directory_name_with_powerbi_agent():
    """
    Ten concerns exist in both packs. Prefixing is what stops one pack from
    overwriting the other in ~/.claude/skills/.
    """
    shared_concerns = [
        "medallion-architecture", "data-catalog-lineage",
        "data-governance-traceability", "data-transformation",
        "source-integration", "performance-scale", "testing-validation",
        "time-series-data", "cyber-security", "project-management",
    ]
    for concern in shared_concerns:
        assert concern not in SKILL_NAMES, (
            f"{concern!r} is unprefixed and would collide with powerbi-agent"
        )
        assert f"databricks-{concern}" in SKILL_NAMES


def test_install_creates_canonical_layout():
    with tempfile.TemporaryDirectory() as tmp_dir:
        target = Path(tmp_dir) / "skills"

        with patch("databricks_agent.skills.installer.CLAUDE_SKILLS_DIR", target):
            count = install_skills(force=False)

        installed = [d for d in target.iterdir() if (d / "SKILL.md").exists()]
        assert count == len(installed) == len(SKILL_NAMES)
        assert list(target.glob("*.md")) == [], "installer must not write flat .md files"


def test_install_skips_existing_without_force():
    with tempfile.TemporaryDirectory() as tmp_dir:
        target = Path(tmp_dir) / "skills"
        target.mkdir()

        with patch("databricks_agent.skills.installer.CLAUDE_SKILLS_DIR", target):
            install_skills(force=False)
            assert install_skills(force=False) == 0


def test_install_overwrites_with_force():
    with tempfile.TemporaryDirectory() as tmp_dir:
        target = Path(tmp_dir) / "skills"
        target.mkdir()

        with patch("databricks_agent.skills.installer.CLAUDE_SKILLS_DIR", target):
            install_skills(force=False)
            assert install_skills(force=True) == len(SKILL_NAMES)


def test_install_cleans_up_legacy_flat_files():
    """Upgrading from <=0.1 must remove the dead flat files it left behind."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        target = Path(tmp_dir) / "skills"
        target.mkdir()
        (target / "medallion-architecture.md").write_text("legacy", encoding="utf-8")
        (target / "spark-sql-mastery.md").write_text("legacy", encoding="utf-8")
        (target / "unrelated-other-tool.md").write_text("keep me", encoding="utf-8")

        with patch("databricks_agent.skills.installer.CLAUDE_SKILLS_DIR", target):
            install_skills(force=True)

        assert not (target / "medallion-architecture.md").exists()
        assert not (target / "spark-sql-mastery.md").exists()
        assert (target / "unrelated-other-tool.md").exists(), "must not touch other tools' files"


def test_uninstall_removes_all():
    with tempfile.TemporaryDirectory() as tmp_dir:
        target = Path(tmp_dir) / "skills"
        target.mkdir()

        with patch("databricks_agent.skills.installer.CLAUDE_SKILLS_DIR", target):
            install_skills(force=True)
            before = len([d for d in target.iterdir() if d.is_dir()])
            removed = uninstall_skills()

        assert removed == before
        assert before == len(SKILL_NAMES)


def test_list_skills_runs():
    with tempfile.TemporaryDirectory() as tmp_dir:
        with patch("databricks_agent.skills.installer.CLAUDE_SKILLS_DIR", Path(tmp_dir)):
            list_skills()


def test_skill_count():
    assert len(SKILL_NAMES) == 20, f"Expected 20 skills, got {len(SKILL_NAMES)}"
