# Attributions & Credits

**databricks-agent** is an original work by **Santosh Kanthety**, MIT-licensed.

It is the Databricks adapter over the same delivery doctrine as
[powerbi-agent](https://github.com/santoshkanthety/powerbi-agent). Both repos share one
[skill schema](SKILL_SCHEMA.md) and one validator (`scripts/validate_skills.py`, byte-identical
in both) — see that file for the contract.

---

## Inspirations

### power-bi-agentic-development — Kurt Buhler (data-goblin)
- Repository: https://github.com/data-goblin/power-bi-agentic-development
- License: **GPL-3.0**
- Established the pattern of Claude Code skill files as domain knowledge modules for agentic
  data work, and in its v26.40 line shipped a `databricks-cli` plugin with a `/databricks-pane`
  sidebar that follows the `databricks` CLI, alongside a `fabric-data-app` plugin following the
  Rayfin CLI.
- **No code, skill file content, or documentation text was copied or derived from that
  project.** The `databricks-cli`, `databricks-asset-bundles` and `databricks-fabric-apps`
  skills added in v0.2 are original works, written from the public Databricks CLI help output,
  Databricks and Microsoft Learn documentation, and independent delivery experience.
  Capability overlap in a shared problem domain is not derivation — but the credit for pointing
  at these areas first is real, and is recorded here deliberately.
- The `databricks-cli` skill *references* that plugin as a companion for users who want the
  pane, because both depend on the same CLI and profile setup. A reference is not a derivation,
  and the plugin is installed separately under its own GPL-3.0 terms.
- If you fork this project and wish to incorporate any content from
  `power-bi-agentic-development`, you must comply with its GPL-3.0 terms.

### Rayfin / Fabric Apps — Microsoft
- Repositories: https://github.com/microsoft/rayfin · https://github.com/microsoft/awesome-rayfin
- License: **MIT** (packages published on npm as `@microsoft/rayfin-*`)
- The `databricks-fabric-apps` skill describes how Unity Catalog data reaches a Fabric App; it
  defers every Rayfin SDK and CLI specific to `powerbi-fabric-apps` in `powerbi-agent` and,
  above that, to the **version-locked in-project skill** Rayfin installs at
  `.agents/skills/rayfin/SKILL.md`. Nothing from the Rayfin SDK is reimplemented or vendored
  here, and the skill explicitly instructs against writing Rayfin APIs from memory — Rayfin is
  in preview and its surface moves.
- Written from Microsoft Learn's public documentation:
  [Fabric Apps overview](https://learn.microsoft.com/fabric/apps/overview),
  [Rayfin CLI reference](https://learn.microsoft.com/fabric/apps/cli-reference).

### pbi-cli — Mina Saad
- Repository: https://github.com/MinaSaad1/pbi-cli
- License: MIT
- No direct bearing on this repo; credited in `powerbi-agent`'s
  [ATTRIBUTIONS.md](https://github.com/santoshkanthety/powerbi-agent/blob/main/ATTRIBUTIONS.md),
  where its contributions are actually used.

---

## Key Dependencies

| Package | License | Purpose |
|---|---|---|
| [databricks-sdk](https://github.com/databricks/databricks-sdk-py) | Apache-2.0 | Workspace, Unity Catalog, jobs, SQL APIs |
| [Click](https://github.com/pallets/click) | BSD-3-Clause | CLI framework |
| [Rich](https://github.com/Textualize/rich) | MIT | Terminal output formatting |

---

## Databricks & Microsoft Technologies

This tool automates and extends:
- **Databricks Lakehouse Platform** — https://databricks.com
- **Unity Catalog** — governance, lineage, grants, row filters and column masks
- **Delta Lake / Delta Live Tables** — https://delta.io
- **Databricks Asset Bundles** — declarative deployment of jobs, pipelines, dashboards and apps
- **Databricks CLI** (unified, v0.2xx) — https://docs.databricks.com/dev-tools/cli/
- **Apache Spark** — https://spark.apache.org
- **Microsoft Fabric / OneLake / Fabric Apps** — for the cross-platform serving path

All product names are trademarks of their respective owners. This project is not affiliated
with, endorsed by, or sponsored by Databricks, Microsoft, or the Apache Software Foundation.

---

## Community

Built with gratitude for the Databricks and wider data community — everyone who published a
notebook, a blog post, a half-finished repo, or a 2am forum answer that someone else could
learn from.
