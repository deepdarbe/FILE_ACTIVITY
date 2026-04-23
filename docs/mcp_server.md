# FILE_ACTIVITY — MCP Server

`file-activity-mcp` is a [Model Context Protocol](https://modelcontextprotocol.io)
server that exposes the FILE_ACTIVITY dashboard to MCP-aware agents
(Claude Code, Claude Desktop, etc.). It is a **thin wrapper** over the
existing FastAPI REST API — every tool call is one HTTP request to a
running dashboard. No SQLite or business logic lives here.

> Issue tracker: [#65](https://github.com/deepdarbe/FILE_ACTIVITY/issues/65)

## Why thin?

The dashboard already enforces validation, audit logging and (where
configured) authorisation. Re-implementing any of that in the MCP layer
would be a regression — instead the MCP server speaks HTTP to the same
endpoints the web UI and PowerShell module use.

## Install

```powershell
# In the repo (or after `pip install file-activity[mcp]`):
pip install -r requirements.txt -r requirements-mcp.txt
```

`requirements-mcp.txt` adds two packages and is intentionally separate
from `requirements.txt` so dashboard / scanner deployments stay slim.

## Configure

The server takes its base URL from (in priority order):

1. The `FILEACTIVITY_BASE_URL` environment variable.
2. `dashboard.host` / `dashboard.port` in `config.yaml`
   (or whatever `FILEACTIVITY_CONFIG` points at).
3. `http://127.0.0.1:8085` as the hard-coded fallback.

| Env var                       | Default                  | Purpose                                 |
|-------------------------------|--------------------------|-----------------------------------------|
| `FILEACTIVITY_BASE_URL`       | from `config.yaml`       | Dashboard root URL.                     |
| `FILEACTIVITY_API_KEY`        | *(unset)*                | Bearer token sent on every request.     |
| `FILEACTIVITY_TIMEOUT`        | `30`                     | Per-request timeout, seconds.           |
| `FILEACTIVITY_MCP_TRANSPORT`  | `stdio`                  | `stdio` or `sse`.                       |
| `FILEACTIVITY_MCP_HOST`       | `127.0.0.1`              | Bind host when transport = `sse`.       |
| `FILEACTIVITY_MCP_PORT`       | `8765`                   | Bind port when transport = `sse`.       |
| `FILEACTIVITY_MCP_LOG_LEVEL`  | `INFO`                   | Python logging level.                   |

## Register with Claude Code

```bash
claude mcp add file-activity -- python -m src.mcp_server
```

Or, after `pip install -e .`, via the installed launcher:

```bash
claude mcp add file-activity -- file-activity-mcp
```

For Claude Desktop, add the equivalent block to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "file-activity": {
      "command": "python",
      "args": ["-m", "src.mcp_server"],
      "env": {
        "FILEACTIVITY_BASE_URL": "http://files01.lan:8085",
        "FILEACTIVITY_API_KEY": "..."
      }
    }
  }
}
```

## Tools

15 tools, action-prefixed per the issue. Every tool that mutates state
requires `confirm: true` in its arguments — call it once without
`confirm` to get a `"would call X with Y"` preview, then re-invoke.

| Tool                     | Endpoint                                        | Write |
|--------------------------|-------------------------------------------------|-------|
| `scan_list_sources`      | `GET /api/sources`                              |       |
| `scan_run`               | `POST /api/scan/{id}`                           | yes   |
| `scan_status`            | `GET /api/scan/progress/{id}`                   |       |
| `report_summary`         | `GET /api/overview/{id}`                        |       |
| `report_duplicates`      | `GET /api/reports/duplicates/{id}`              |       |
| `report_orphan_sids`     | `GET /api/security/orphan-sids/{id}`            |       |
| `pii_list_findings`      | `GET /api/compliance/pii/findings`              |       |
| `pii_subject_export`     | `GET /api/compliance/pii/subject`               |       |
| `archive_dry_run`        | `POST /api/archive/dry-run`                     |       |
| `archive_run`            | `POST /api/archive/run`                         | yes   |
| `hold_list_active`       | `GET /api/compliance/legal-holds/active`        |       |
| `hold_add`               | `POST /api/compliance/legal-holds`              | yes   |
| `hold_release`           | `POST /api/compliance/legal-holds/{id}/release` | yes   |
| `audit_query`            | `GET /api/audit/events`                         |       |
| `audit_verify_chain`     | `GET /api/audit/verify`                         |       |

## Security notes

- **Default = local only.** With no `FILEACTIVITY_API_KEY`, the server
  trusts the loopback dashboard. That's fine for laptop/dev use, where
  Claude Code, the dashboard and the MCP server all run as the same
  user on the same host.
- **Production = bearer token.** When the dashboard is reachable beyond
  loopback (reverse proxy, remote agent, etc.), set `FILEACTIVITY_API_KEY`
  to a token your gateway validates. The MCP server forwards it on every
  request as `Authorization: Bearer <token>`.
- **Audit chain is append-only.** Every write goes through the same
  REST endpoints the dashboard uses, so the tamper-evident audit log
  (issue #38) records the operation just as it would for a clicked
  button. The MCP server itself writes nothing to the database.
- **Confirm-gate for destructive ops.** `scan_run`, `archive_run`,
  `hold_add` and `hold_release` all require `confirm=true`. Without it
  the server returns a structured preview — the LLM sees what it
  *would* do without anything actually happening.
- **Stdio is safe by default.** The default transport is stdio, which
  means the MCP server only exists for the lifetime of the parent
  process (Claude Code) and never opens a network port of its own.

## Tests

```bash
pytest tests/test_mcp_tools.py        # schema + confirm-gate units
pytest tests/test_mcp_integration.py  # MCP -> live FastAPI on a random port
pytest tests/test_mcp_eval.py         # 15 realistic admin questions
```
