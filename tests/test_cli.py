"""Tests for databricks-agent CLI."""

from __future__ import annotations

from click.testing import CliRunner
from databricks_agent.cli import main


def test_main_help():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "databricks-agent" in result.output


def test_connect_help():
    runner = CliRunner()
    result = runner.invoke(main, ["connect", "--help"])
    assert result.exit_code == 0


def test_sql_help():
    runner = CliRunner()
    result = runner.invoke(main, ["sql", "--help"])
    assert result.exit_code == 0


def test_jobs_help():
    runner = CliRunner()
    result = runner.invoke(main, ["jobs", "--help"])
    assert result.exit_code == 0


def test_clusters_help():
    runner = CliRunner()
    result = runner.invoke(main, ["clusters", "--help"])
    assert result.exit_code == 0


def test_catalog_help():
    runner = CliRunner()
    result = runner.invoke(main, ["catalog", "--help"])
    assert result.exit_code == 0


def test_pipelines_help():
    runner = CliRunner()
    result = runner.invoke(main, ["pipelines", "--help"])
    assert result.exit_code == 0


def test_skills_help():
    runner = CliRunner()
    result = runner.invoke(main, ["skills", "--help"])
    assert result.exit_code == 0
