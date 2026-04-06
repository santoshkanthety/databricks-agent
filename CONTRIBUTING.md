# Contributing to databricks-agent

Thank you for your interest in contributing!

## Development Setup

```bash
git clone https://github.com/santoshkanthety/databricks-agent
cd databricks-agent
pip install -e ".[all]"
```

## Running Tests

```bash
pytest
```

## Adding a New Skill

1. Create `skills/your-skill-name.md` with the frontmatter format:
   ```markdown
   ---
   name: databricks-your-skill
   description: One-line description
   triggers:
     - keyword1
     - keyword2
   ---
   # Content...
   ```
2. Add the filename to `SKILL_FILES` in `src/databricks_agent/skills/installer.py`
3. Submit a PR

## Adding a New CLI Command

1. Add a new command to the relevant group in `src/databricks_agent/cli.py`
2. Add the underlying logic to the appropriate module (e.g., `sql.py`, `jobs.py`)
3. Add a help test in `tests/test_cli.py`

## Code Style

```bash
ruff check src/
ruff format src/
```

## Reporting Issues

Please use [GitHub Issues](https://github.com/santoshkanthety/databricks-agent/issues).
