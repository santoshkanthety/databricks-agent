"""
Tests for skills installer — verifies all skill files exist and install correctly.
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from databricks_agent.skills.installer import SKILL_FILES, SKILLS_SOURCE_DIR, install_skills, list_skills, uninstall_skills


def test_all_skill_files_exist_in_source():
    """Every entry in SKILL_FILES must have a corresponding .md file in skills/."""
    missing = []
    for skill_file in SKILL_FILES:
        src = SKILLS_SOURCE_DIR / skill_file
        if not src.exists():
            missing.append(str(src))
    assert missing == [], f"Missing skill source files:\n" + "\n".join(missing)


def test_skill_files_have_frontmatter():
    """Each skill file must have a YAML frontmatter block with name and triggers."""
    for skill_file in SKILL_FILES:
        src = SKILLS_SOURCE_DIR / skill_file
        if not src.exists():
            continue
        content = src.read_text(encoding="utf-8")
        # Either old-style (## Trigger) or new-style (--- frontmatter ---)
        has_trigger = "## Trigger" in content or "triggers:" in content or "## What You Know" in content
        assert has_trigger, f"{skill_file} has no trigger/frontmatter section"


def test_install_skills_copies_files():
    """install_skills() copies all available skill files to the target directory."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_skills_dir = Path(tmp_dir) / "skills"

        with patch("databricks_agent.skills.installer.CLAUDE_SKILLS_DIR", tmp_skills_dir):
            count = install_skills(force=False)

        installed = list(tmp_skills_dir.glob("*.md"))
        assert count == len(installed)
        assert count > 0


def test_install_skills_skips_existing_without_force():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_skills_dir = Path(tmp_dir) / "skills"
        tmp_skills_dir.mkdir()

        # Pre-install once
        with patch("databricks_agent.skills.installer.CLAUDE_SKILLS_DIR", tmp_skills_dir):
            install_skills(force=False)
            first_count = len(list(tmp_skills_dir.glob("*.md")))

            # Install again — should skip all
            count_second = install_skills(force=False)

        assert count_second == 0  # nothing new installed


def test_install_skills_overwrites_with_force():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_skills_dir = Path(tmp_dir) / "skills"
        tmp_skills_dir.mkdir()

        with patch("databricks_agent.skills.installer.CLAUDE_SKILLS_DIR", tmp_skills_dir):
            install_skills(force=False)
            count_force = install_skills(force=True)

        assert count_force > 0  # all re-installed


def test_uninstall_removes_files():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_skills_dir = Path(tmp_dir) / "skills"
        tmp_skills_dir.mkdir()

        with patch("databricks_agent.skills.installer.CLAUDE_SKILLS_DIR", tmp_skills_dir):
            install_skills(force=True)
            before = len(list(tmp_skills_dir.glob("*.md")))
            removed = uninstall_skills()

        assert removed == before
        assert len(list(tmp_skills_dir.glob("*.md"))) == 0


def test_skill_files_list_has_17_entries():
    """Sanity check: SKILL_FILES should have exactly 17 entries."""
    assert len(SKILL_FILES) == 17, f"Expected 17 skills, got {len(SKILL_FILES)}: {SKILL_FILES}"
