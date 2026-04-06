---
name: databricks-connect
description: Quick start guide for connecting databricks-agent to a Databricks workspace using PAT tokens or OAuth
triggers:
  - connect
  - databricks connect
  - connection issues
  - authentication
  - workspace
  - token
  - oauth
  - profile
  - databricks-agent connect
  - how to connect
  - no workspace
---

# Connecting to Databricks

## Quick Start

```bash
# Step 1: Install databricks-agent
pip install "databricks-agent[all]"

# Step 2: Connect to your workspace
databricks-agent connect

# Step 3: Verify connection
databricks-agent doctor

# Step 4: Install Claude Code skills
databricks-agent skills install
```

## Connection Methods

### Method 1: Interactive Setup (Recommended)

```bash
databricks-agent connect
# Prompts for:
#   Workspace URL: https://adb-1234567890.azuredatabricks.net
#   Authentication: [1] PAT token  [2] OAuth (browser)
#   Default warehouse (optional)
#   Default catalog (optional)
```

### Method 2: Environment Variables

```bash
export DATABRICKS_HOST="https://adb-1234567890.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi1234567890abcdef"

# Or for OAuth M2M
export DATABRICKS_CLIENT_ID="your-service-principal-client-id"
export DATABRICKS_CLIENT_SECRET="your-service-principal-secret"
```

### Method 3: Databricks Config File (~/.databrickscfg)

```ini
[DEFAULT]
host  = https://adb-1234567890.azuredatabricks.net
token = dapi1234567890abcdef

[dev]
host  = https://adb-0987654321.azuredatabricks.net
token = dapi0987654321abcdef
```

```bash
# Use a specific profile
databricks-agent connect --profile dev

# List configured profiles
databricks-agent connect --list
```

## Switching Workspaces

```bash
# Connect to a different workspace
databricks-agent connect --host https://adb-9876543210.azuredatabricks.net

# Switch active profile
databricks-agent connect --profile prod
```

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `401 Unauthorized` | Invalid or expired token | Regenerate PAT in workspace Settings → Developer |
| `403 Forbidden` | Missing permissions | Ask admin for `CAN USE` on warehouse or cluster |
| `Connection timed out` | Firewall / VPN | Ensure VPN is connected, check network ACLs |
| `Workspace not found` | Wrong host URL | Verify URL format: `https://adb-XXXXX.Y.azuredatabricks.net` |
| `No warehouses found` | No SQL Warehouse created | Create one in workspace or request from admin |
| `databricks SDK not found` | Missing dependency | Run `pip install databricks-sdk` |

```bash
# Full environment diagnostics
databricks-agent doctor
```

## Installation Options

```bash
# Core only (CLI, no web UI)
pip install databricks-agent

# With web configuration UI
pip install "databricks-agent[ui]"

# With ML/MLflow support
pip install "databricks-agent[ml]"

# Everything
pip install "databricks-agent[all]"
```

## Generating a PAT Token

1. Open your Databricks workspace
2. Click your username (top-right) → **Settings**
3. Click **Developer** → **Access Tokens** → **Generate new token**
4. Set a description and expiry
5. Copy the token immediately (shown once)
6. Run `databricks-agent connect` and paste the token
