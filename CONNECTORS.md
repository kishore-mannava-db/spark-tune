# Connectors

This plugin uses the **Databricks REST API** directly via `curl` and does not require an MCP server.

## Authentication

All API calls authenticate using a Databricks CLI profile. The skill reads credentials from `~/.databrickscfg` via the `--profile` flag (defaults to `DEFAULT`).

```
[DEFAULT]
host  = https://<workspace>.cloud.databricks.com
token = dapi...
```

## Required Permissions

The personal access token (PAT) must have these workspace permissions:

| Permission | Why |
|------------|-----|
| **Can View** | Read job definitions, run metadata, and cluster info |
| **Can Attach To** | Access cluster log delivery paths |
| **Can Manage** | Reset job configurations and trigger runs (redeploy step only) |

## API Endpoints Used

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `GET` | `/api/2.1/jobs/runs/list` | List recent runs for a job ID |
| `GET` | `/api/2.1/jobs/runs/get` | Get run details (cluster ID, state, duration) |
| `GET` | `/api/2.0/clusters/get` | Get cluster config (instance type, Spark conf) |
| `GET` | `/api/2.0/fs/files{path}` | Download driver stdout, stderr, and event logs |
| `GET` | `/api/2.0/fs/directories{path}` | List event log subdirectories |
| `POST` | `/api/2.1/jobs/reset` | Update job definition with tuned Spark config |
| `POST` | `/api/2.1/jobs/run-now` | Trigger a new run after applying tuning changes |

## No MCP Server Required

This plugin calls Databricks APIs directly using `curl` with bearer-token auth. No `~~databricks` connector placeholder or `.mcp.json` configuration is needed.
