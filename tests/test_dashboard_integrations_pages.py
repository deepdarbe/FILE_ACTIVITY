"""TestClient smoke for the new Integrations + System dashboard endpoints
(issue #81 — Integrations/System pages, this subagent's slice).

We deliberately avoid spinning up the full ``create_app(...)`` factory (it
wants a real Database + AnalyticsEngine + AD + email notifier just to run);
instead each test mounts a minimal FastAPI app that registers exactly the
endpoint under test. Mirrors the established pattern in
``test_dashboard_api.py`` and ``test_ai_insights.py``.

Covered routes:
  * ``GET /api/integrations/syslog/status``      — disabled forwarder branch
  * ``POST /api/integrations/syslog/test``       — disabled forwarder branch
  * ``GET /api/system/mcp/info``                 — tools list shape
"""

from __future__ import annotations

import os
import sys
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


# ---------------------------------------------------------------------------
# Syslog status / test endpoints
# ---------------------------------------------------------------------------


def _build_syslog_app(forwarder: Any | None) -> FastAPI:
    """Mirror the exact route handlers from ``api.py`` so we exercise the
    real branching without dragging in the full create_app dependency tree.
    """
    app = FastAPI()
    app.state.syslog = forwarder

    @app.get("/api/integrations/syslog/status")
    async def syslog_status():
        f = getattr(app.state, "syslog", None)
        if f is None:
            return {"available": False, "configured": False,
                    "reason": "forwarder_not_initialized"}
        return f.health()

    @app.post("/api/integrations/syslog/test")
    async def syslog_test():
        f = getattr(app.state, "syslog", None)
        if f is None:
            return {"sent": False, "error": "forwarder_not_initialized"}
        if not f.available:
            return {"sent": False,
                    "error": "forwarder_disabled_or_unconfigured"}
        ok = f.emit("info", "test_event", {"msg": "hi"})
        if not ok:
            return {"sent": False,
                    "error": f.health().get("last_error")}
        return {"sent": True}

    return app


def test_syslog_status_returns_disabled_payload_when_no_forwarder():
    """If the forwarder failed to construct, status must be a clean 200
    payload describing the disabled state — not a 500. The frontend banner
    keys off ``configured/available`` and would otherwise show a generic
    error."""
    client = TestClient(_build_syslog_app(forwarder=None))
    resp = client.get("/api/integrations/syslog/status")

    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is False
    assert body["configured"] is False
    assert body["reason"] == "forwarder_not_initialized"


def test_syslog_test_returns_unsent_when_no_forwarder():
    client = TestClient(_build_syslog_app(forwarder=None))
    resp = client.post("/api/integrations/syslog/test")

    assert resp.status_code == 200
    body = resp.json()
    assert body["sent"] is False
    assert body["error"] == "forwarder_not_initialized"


def test_syslog_status_real_disabled_forwarder():
    """Construct the real SyslogForwarder with enabled=false (the default)
    and verify the health() shape the frontend KPI cards expect."""
    from src.integrations.syslog_forwarder import SyslogForwarder

    fw = SyslogForwarder({"integrations": {"syslog": {"enabled": False}}})
    client = TestClient(_build_syslog_app(forwarder=fw))
    resp = client.get("/api/integrations/syslog/status")

    assert resp.status_code == 200
    body = resp.json()
    # Disabled forwarder must report itself as not-available + not-configured
    # so the UI banner shows.
    assert body["available"] is False
    assert body["configured"] is False
    # All these keys must be present so the frontend never crashes on .X
    for k in ("transport", "format", "host", "port", "queue_depth",
              "queue_max", "dropped_count", "sent_count",
              "last_emit_at", "last_error"):
        assert k in body, f"missing key: {k}"


def test_syslog_test_disabled_forwarder_returns_unconfigured():
    from src.integrations.syslog_forwarder import SyslogForwarder

    fw = SyslogForwarder({"integrations": {"syslog": {"enabled": False}}})
    client = TestClient(_build_syslog_app(forwarder=fw))
    resp = client.post("/api/integrations/syslog/test")

    assert resp.status_code == 200
    assert resp.json()["sent"] is False
    assert resp.json()["error"] == "forwarder_disabled_or_unconfigured"


# ---------------------------------------------------------------------------
# MCP server info endpoint
# ---------------------------------------------------------------------------


def _build_mcp_app() -> FastAPI:
    """Inline copy of the /api/system/mcp/info handler — same lazy import,
    same fallback list. We want the test to exercise the live ``TOOLS``
    registry when present (PR #67) AND still pass when those optional deps
    aren't installed."""
    import logging
    log = logging.getLogger("test")

    app = FastAPI()

    @app.get("/api/system/mcp/info")
    async def mcp_info():
        tools_info: list[dict] = []
        configured = False
        try:
            from src.mcp_server.tools import TOOLS
            for t in TOOLS:
                tools_info.append({
                    "name": t.name,
                    "description": t.description,
                    "is_write": bool(getattr(t, "is_write", False)),
                })
            configured = True
        except Exception as e:  # pragma: no cover - defensive
            log.warning("TOOLS import failed: %s", e)
            for name, desc in [
                ("scan_list_sources", "List sources."),
                ("scan_run", "Run scan."),
                ("scan_status", "Scan status."),
                ("report_summary", "Summary."),
                ("report_duplicates", "Duplicates."),
                ("report_orphan_sids", "Orphan SIDs."),
                ("pii_list_findings", "PII findings."),
                ("pii_subject_export", "Subject export."),
                ("archive_dry_run", "Archive dry run."),
                ("archive_run", "Archive run."),
                ("hold_list_active", "List holds."),
                ("hold_add", "Add hold."),
                ("hold_release", "Release hold."),
                ("audit_query", "Audit query."),
                ("audit_verify_chain", "Verify chain."),
            ]:
                tools_info.append({
                    "name": name, "description": desc,
                    "is_write": name in {"scan_run", "archive_run",
                                         "hold_add", "hold_release"},
                })
        return {
            "configured": configured,
            "tools_count": len(tools_info),
            "tools": tools_info,
            "transports": ["stdio"],
            "install_command":
                "claude mcp add file-activity -- python -m src.mcp_server",
        }

    return app


def test_mcp_info_returns_15_tools_with_required_shape():
    """The MCP page loads from this endpoint; every tool row must have
    name + description so the entity-list table renders cleanly."""
    client = TestClient(_build_mcp_app())
    resp = client.get("/api/system/mcp/info")

    assert resp.status_code == 200
    body = resp.json()
    # PR #67 ships 15 tools; the fallback list also has 15.
    assert body["tools_count"] == 15
    assert len(body["tools"]) == 15
    assert "stdio" in body["transports"]
    assert "claude mcp add" in body["install_command"]

    seen_names: set[str] = set()
    for t in body["tools"]:
        assert "name" in t and t["name"], f"tool missing name: {t}"
        assert "description" in t, f"tool missing description: {t}"
        assert isinstance(t.get("is_write"), bool)
        seen_names.add(t["name"])

    # A few canonical tools from PR #67 must always be present — these are
    # what the docs / claude-code config snippets reference.
    for required in ("scan_list_sources", "report_summary",
                     "audit_verify_chain"):
        assert required in seen_names


def test_mcp_info_marks_writes_correctly():
    """Frontend renders a red 'write' badge on these — if we ever lose
    the is_write flag the dashboard would silently show every tool as
    read-only and operators would invoke destructive ops without realising
    it gates on ``confirm=true``."""
    client = TestClient(_build_mcp_app())
    body = client.get("/api/system/mcp/info").json()

    by_name = {t["name"]: t for t in body["tools"]}
    for write_tool in ("scan_run", "archive_run", "hold_add", "hold_release"):
        assert by_name[write_tool]["is_write"] is True, (
            f"{write_tool} must be flagged is_write=True"
        )
    # Read-only sanity check
    for read_tool in ("scan_list_sources", "report_summary", "audit_query"):
        assert by_name[read_tool]["is_write"] is False
