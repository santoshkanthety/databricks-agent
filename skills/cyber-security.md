---
name: databricks-cyber-security
description: Databricks cybersecurity posture - network isolation, secrets hygiene, threat detection, zero-trust architecture, credential scanning, SOC2/HIPAA controls, and incident response
triggers:
  - cyber security
  - cybersecurity
  - network isolation
  - private endpoint
  - secrets
  - credential leak
  - threat detection
  - zero trust
  - SOC2
  - HIPAA
  - PCI-DSS
  - vulnerability
  - attack surface
  - hardening
  - incident response
  - access review
  - privilege escalation
  - lateral movement
  - exfiltration
  - IP allowlist
  - VNet injection
  - firewall
---

# Cybersecurity in Databricks

## Attack Surface Overview

Databricks attack surface has four main areas to harden:

| Area | Primary Risk | Control |
|------|-------------|---------|
| **Network** | Unauthorised workspace access | Private endpoints, VNet injection, IP allowlist |
| **Identity** | Credential theft, over-provisioned access | OAuth, MFA, least privilege, PAT rotation |
| **Data** | Exfiltration, unauthorised reads | Unity Catalog ACLs, column masks, row filters |
| **Compute** | Code injection, cryptomining, data staging | Init script controls, no-internet clusters |

## Network Hardening

### Private Endpoints (Azure)

```bash
# All traffic stays on private network — no public internet exposure
# Enable via Databricks workspace settings → Networking → Private endpoints

# Verify: public access should be disabled
databricks-agent clusters info --name prod-cluster
# Look for: "enableNoPublicIp": true
```

```python
# Create workspace with no public IP (via Terraform / ARM — do this at provisioning time)
# For existing workspaces: disable public access in Azure Portal
# → Databricks workspace → Networking → Deny public network access: Enabled
```

### IP Access Lists

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.settings import CreateIpAccessList, ListType

w = WorkspaceClient()

# Allowlist: only permitted IP ranges can access the workspace
w.ip_access_lists.create(
    label="corporate-vpn",
    list_type=ListType.ALLOW,
    ip_addresses=[
        "203.0.113.0/24",    # Corporate office
        "198.51.100.10/32",  # VPN gateway
    ]
)

# Blocklist: explicitly deny known bad ranges
w.ip_access_lists.create(
    label="blocked-ranges",
    list_type=ListType.BLOCK,
    ip_addresses=["192.0.2.0/24"]
)
```

```bash
databricks-agent clusters list  # Check active clusters for public IP exposure
```

## Secrets Management — Zero Credential Leakage

**Rule: No credentials in notebooks, job configs, or Git commits. Ever.**

```python
# WRONG — never do this
password = "MySecretP@ssw0rd"
conn = psycopg2.connect(host="db.example.com", password=password)

# CORRECT — always use Databricks Secrets
host     = dbutils.secrets.get(scope="prod-db", key="host")
password = dbutils.secrets.get(scope="prod-db", key="password")
conn_str = f"jdbc:postgresql://{host}/mydb"
```

```bash
# Create a secret scope (Databricks-backed)
databricks secrets create-scope --scope prod-db

# Store secrets (CLI or API)
databricks secrets put-secret --scope prod-db --key host
databricks secrets put-secret --scope prod-db --key password

# List scopes (does NOT reveal secret values)
databricks secrets list-scopes

# ACL: restrict scope to a specific group
databricks secrets put-acl --scope prod-db --principal data-engineering --permission READ
```

### Credential Scanning in Notebooks

```python
import re

CREDENTIAL_PATTERNS = [
    (r"password\s*=\s*['\"][^'\"]{6,}['\"]",  "hardcoded password"),
    (r"token\s*=\s*['\"]dapi[a-f0-9]{32}['\"]", "Databricks PAT in code"),
    (r"(?:aws_secret|aws_access_key)\s*=\s*['\"][A-Za-z0-9/+=]{20,}['\"]", "AWS credential"),
    (r"-----BEGIN (?:RSA|EC|OPENSSH) PRIVATE KEY-----", "private key"),
    (r"[a-zA-Z0-9._%+-]+:[^@\s]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", "credential in connection string"),
]

def scan_notebook_for_credentials(notebook_content: str) -> list[dict]:
    """Scan notebook content for potential credential leaks."""
    findings = []
    for pattern, finding_type in CREDENTIAL_PATTERNS:
        matches = re.finditer(pattern, notebook_content, re.IGNORECASE)
        for match in matches:
            findings.append({
                "type": finding_type,
                "match_preview": match.group()[:30] + "...",
                "position": match.start()
            })
    return findings

# Scan all notebooks in a workspace path
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
for obj in w.workspace.list("/Repos"):
    if obj.object_type.value == "NOTEBOOK":
        export = w.workspace.export(obj.path)
        import base64
        content = base64.b64decode(export.content).decode("utf-8")
        findings = scan_notebook_for_credentials(content)
        if findings:
            print(f"⚠️  {obj.path}: {len(findings)} potential credential(s) found")
            for f in findings:
                print(f"   [{f['type']}] {f['match_preview']}")
```

```bash
# CLI: scan for credential leaks
databricks-agent catalog audit --catalog prod --check credential-leak
```

## Threat Detection via Audit Logs

```sql
-- Detect unusual access patterns in Unity Catalog audit logs
-- 1. Off-hours access to sensitive tables
SELECT
  user_identity.email                     AS user,
  request_params.table_full_name          AS table_name,
  event_time,
  HOUR(event_time)                        AS hour_of_day
FROM system.access.audit
WHERE action_name = 'SELECT'
  AND HOUR(event_time) NOT BETWEEN 7 AND 20       -- outside business hours
  AND request_params.table_full_name LIKE '%customers%'
  AND event_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
ORDER BY event_time DESC;

-- 2. Bulk data export (high row count reads)
SELECT user_identity.email, request_params.table_full_name, response.result.rows_returned, event_time
FROM system.access.audit
WHERE response.result.rows_returned > 100000  -- large result sets
  AND event_time >= DATEADD(DAY, -1, CURRENT_TIMESTAMP())
ORDER BY response.result.rows_returned DESC;

-- 3. Failed access attempts (potential probing)
SELECT user_identity.email, request_params.table_full_name, COUNT(*) AS failures
FROM system.access.audit
WHERE status_code = 403
  AND event_time >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
GROUP BY 1, 2
HAVING COUNT(*) >= 5
ORDER BY failures DESC;

-- 4. New service principals or users accessing production
SELECT DISTINCT user_identity.email, MIN(event_time) AS first_seen
FROM system.access.audit
WHERE event_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
  AND request_params.catalog_name = 'prod'
GROUP BY 1
HAVING MIN(event_time) >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())  -- first appeared in last 7 days
ORDER BY first_seen DESC;
```

## Least-Privilege Access Review

```python
from databricks.sdk import WorkspaceClient

def review_overprivileged_access(catalog: str) -> list[dict]:
    """Find principals with more access than needed."""
    w = WorkspaceClient()
    issues = []

    # Check for ALL PRIVILEGES grants (overly broad)
    from databricks.sdk.service.catalog import SecurableType
    perms = w.grants.get(full_name=catalog, securable_type=SecurableType.CATALOG)

    for assignment in (perms.privilege_assignments or []):
        privs = [p.value for p in (assignment.privileges or [])]
        if "ALL_PRIVILEGES" in privs or len(privs) > 4:
            issues.append({
                "principal": assignment.principal,
                "privileges": privs,
                "risk": "Overprivileged — review and restrict to minimum required"
            })

    return issues
```

```bash
# Access review report
databricks-agent catalog show-grants prod --type CATALOG
databricks-agent catalog audit prod --check overprivileged
```

## Cluster Security Hardening

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSpec

w = WorkspaceClient()

# Secure cluster configuration
secure_cluster = {
    "cluster_name": "secure-prod-cluster",
    "spark_version": "15.4.x-scala2.12",
    "node_type_id": "Standard_DS4_v2",
    "num_workers": 4,

    # No public IP — traffic stays on VNet
    "enable_elastic_disk": True,

    # Restrict outbound internet (no exfiltration path)
    "spark_conf": {
        "spark.databricks.pyspark.enableProcessIsolation": "true",
    },

    # Disable direct filesystem access for users
    "data_security_mode": "USER_ISOLATION",  # Unity Catalog mode

    # Log all cluster events
    "cluster_log_conf": {
        "dbfs": {"destination": "dbfs:/cluster-logs"}
    }
}
```

## PAT Token Hygiene

```python
from databricks.sdk import WorkspaceClient

def audit_pat_tokens() -> list[dict]:
    """List all PAT tokens and flag expired or long-lived ones."""
    from datetime import datetime

    w = WorkspaceClient()
    token_list = w.token_management.list()
    issues = []

    for token in token_list:
        days_old = (datetime.now().timestamp() * 1000 - (token.creation_time or 0)) / (1000 * 86400)
        expiry_days = ((token.expiry_time or float("inf")) - datetime.now().timestamp() * 1000) / (1000 * 86400)

        if expiry_days == float("inf"):
            issues.append({"token": token.token_id, "owner": token.created_by_id, "issue": "No expiry set (non-expiring token)"})
        elif days_old > 90:
            issues.append({"token": token.token_id, "owner": token.created_by_id, "issue": f"Token is {days_old:.0f} days old — should rotate"})

    return issues
```

```bash
databricks-agent catalog audit prod --check pat-tokens  # Flag long-lived or non-expiring tokens
```

## Incident Response Playbook

### Suspected Data Exfiltration
```sql
-- Step 1: Identify what was exported and by whom
SELECT user_identity.email, request_params.table_full_name,
       response.result.rows_returned, event_time
FROM system.access.audit
WHERE user_identity.email = 'suspect@company.com'
  AND response.result.rows_returned > 1000
ORDER BY event_time DESC;

-- Step 2: Check if new external shares were created
SELECT * FROM system.access.audit
WHERE action_name IN ('createShare', 'updateShare', 'addShareRecipient')
  AND event_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP());
```

```bash
# Step 3: Revoke all access immediately
databricks-agent catalog revoke prod --principal suspect@company.com --privilege ALL

# Step 4: Disable user account (via workspace admin or Identity Provider)
# Step 5: Preserve audit logs by exporting to secure storage
```

## Compliance Controls Reference

| Standard | Key Controls | Databricks Implementation |
|---|---|---|
| **SOC 2** | Access control, audit logs, encryption | Unity Catalog ACLs, system.access.audit, Delta encryption |
| **HIPAA** | PHI access control, audit trails, BAA | Column masks on PHI, audit log retention, BAA with Databricks |
| **GDPR** | Right to erasure, data minimisation, consent | Erasure procedures, retention policies, consent log |
| **PCI-DSS** | Cardholder data isolation, access review | Column masks on card data, quarterly access review |

## CLI Reference

```bash
databricks-agent catalog audit prod --check credential-leak   # Scan for credential exposure
databricks-agent catalog audit prod --check overprivileged    # Overly broad permissions
databricks-agent catalog audit prod --check pat-tokens        # Token hygiene
databricks-agent catalog show-grants prod --type CATALOG      # Full access review
```
