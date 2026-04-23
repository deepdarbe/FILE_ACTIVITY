"""Unit tests for the file-activity MCP tool layer (issue #65).

Covers schema validation and the confirm-gate. The HTTP layer is
exercised against a mock httpx transport so these tests don't need a
running dashboard.
"""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from src.mcp_server.config import Settings
from src.mcp_server.tools import (
    TOOL_INDEX,
    TOOLS,
    AuditQueryInput,
    HoldAddInput,
    ScanRunInput,
    dispatch,
)


# ---------------------------------------------------------------------------
# Fixtures: a mock httpx.AsyncClient that records the calls it receives so
# we can assert the right URL/body was sent for each tool.
# ---------------------------------------------------------------------------


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[httpx.Request] = []
        self.next_response: httpx.Response = httpx.Response(200, json={"ok": True})

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        return self.next_response


def _make_client(recorder: _Recorder) -> httpx.AsyncClient:
    transport = httpx.MockTransport(recorder.handler)
    return httpx.AsyncClient(transport=transport)


def _settings() -> Settings:
    return Settings(base_url="http://test:8085", api_key=None, timeout=5.0)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Registry & schema sanity
# ---------------------------------------------------------------------------


def test_registry_has_required_tools():
    """Issue #65 mandates >=12 tools across the listed action prefixes."""
    names = {t.name for t in TOOLS}
    required = {
        "scan_list_sources", "scan_run", "scan_status",
        "report_summary", "report_duplicates", "report_orphan_sids",
        "pii_list_findings", "pii_subject_export",
        "archive_dry_run", "archive_run",
        "hold_list_active", "hold_add", "hold_release",
        "audit_query", "audit_verify_chain",
    }
    missing = required - names
    assert not missing, f"Missing required tools: {missing}"
    assert len(TOOLS) >= 12


def test_every_tool_has_a_schema():
    for tool in TOOLS:
        schema = tool.json_schema()
        assert schema["type"] == "object"
        assert "properties" in schema or schema.get("type") == "object"


def test_write_tools_carry_confirm_field():
    write_tools = [t for t in TOOLS if t.is_write]
    assert write_tools, "expected at least one write-flagged tool"
    for tool in write_tools:
        schema = tool.json_schema()
        props = schema.get("properties", {})
        assert "confirm" in props, f"{tool.name} missing confirm field"


def test_schema_rejects_invalid_args():
    with pytest.raises(Exception):
        ScanRunInput.model_validate({"source_id": -1})
    with pytest.raises(Exception):
        HoldAddInput.model_validate({"pattern": "", "reason": "x"})
    with pytest.raises(Exception):
        AuditQueryInput.model_validate({"since_days": 0})


# ---------------------------------------------------------------------------
# Confirm-gate: writes without confirm=True must NOT call the API
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tool_name,args", [
    ("scan_run", {"source_id": 1}),
    ("archive_run", {"source_id": 1, "days": 365}),
    ("hold_add", {"pattern": "/x/*", "reason": "case 42"}),
    ("hold_release", {"hold_id": 7}),
])
def test_write_without_confirm_returns_preview(tool_name, args):
    rec = _Recorder()
    client = _make_client(rec)
    result = _run(dispatch(tool_name, args, client, _settings()))
    assert isinstance(result, dict) and result.get("preview") is True
    assert result["tool"] == tool_name
    assert "confirm=true" in result["message"]
    assert rec.calls == [], "write tool must not hit the API without confirm"


def test_write_with_confirm_invokes_api():
    rec = _Recorder()
    rec.next_response = httpx.Response(200, json={"status": "started"})
    client = _make_client(rec)
    result = _run(dispatch("scan_run", {"source_id": 5, "confirm": True},
                           client, _settings()))
    assert result == {"status": "started"}
    assert len(rec.calls) == 1
    assert rec.calls[0].method == "POST"
    assert rec.calls[0].url.path == "/api/scan/5"


# ---------------------------------------------------------------------------
# Read tools: dispatch goes straight through and we get the right URL.
# ---------------------------------------------------------------------------


def test_scan_list_sources_hits_sources_endpoint():
    rec = _Recorder()
    rec.next_response = httpx.Response(200, json=[{"id": 1, "name": "demo"}])
    client = _make_client(rec)
    result = _run(dispatch("scan_list_sources", {}, client, _settings()))
    assert result == [{"id": 1, "name": "demo"}]
    assert rec.calls[0].url.path == "/api/sources"


def test_report_duplicates_passes_query_params():
    rec = _Recorder()
    rec.next_response = httpx.Response(200, json={"groups": []})
    client = _make_client(rec)
    _run(dispatch(
        "report_duplicates",
        {"source_id": 3, "min_size": 1024, "page_size": 25},
        client, _settings(),
    ))
    req = rec.calls[0]
    assert req.url.path == "/api/reports/duplicates/3"
    qs = dict(req.url.params)
    assert qs["min_size"] == "1024"
    assert qs["page_size"] == "25"


def test_pii_list_findings_filters_pattern():
    rec = _Recorder()
    rec.next_response = httpx.Response(200, json={"findings": []})
    client = _make_client(rec)
    _run(dispatch(
        "pii_list_findings",
        {"pattern": "email", "limit": 10},
        client, _settings(),
    ))
    req = rec.calls[0]
    assert req.url.path == "/api/compliance/pii/findings"
    qs = dict(req.url.params)
    assert qs["pattern"] == "email"
    assert qs["page_size"] == "10"


def test_audit_query_translates_actor_to_username():
    rec = _Recorder()
    rec.next_response = httpx.Response(
        200, json={"events": [{"id": 1}, {"id": 2}, {"id": 3}], "total": 3}
    )
    client = _make_client(rec)
    result = _run(dispatch(
        "audit_query",
        {"actor": "alice", "action": "archive", "since_days": 7, "limit": 2},
        client, _settings(),
    ))
    req = rec.calls[0]
    assert req.url.path == "/api/audit/events"
    qs = dict(req.url.params)
    assert qs["username"] == "alice"
    assert qs["event_type"] == "archive"
    assert qs["days"] == "7"
    # limit is applied client-side after the page is fetched.
    assert len(result["events"]) == 2


def test_audit_query_handles_list_response():
    """The /api/audit/events endpoint may return a bare list."""
    rec = _Recorder()
    rec.next_response = httpx.Response(200, json=[{"id": 1}])
    client = _make_client(rec)
    result = _run(dispatch("audit_query", {"since_days": 1},
                           client, _settings()))
    assert result == {"events": [{"id": 1}], "total": 1, "page": 1}


def test_unknown_tool_raises_keyerror():
    rec = _Recorder()
    client = _make_client(rec)
    with pytest.raises(KeyError):
        _run(dispatch("nope", {}, client, _settings()))


def test_invalid_args_surfaces_as_valueerror():
    rec = _Recorder()
    client = _make_client(rec)
    with pytest.raises(ValueError):
        _run(dispatch("scan_status", {"source_id": "not-a-number"},
                      client, _settings()))


def test_http_error_propagates_with_body():
    rec = _Recorder()
    rec.next_response = httpx.Response(404, json={"detail": "no such source"})
    client = _make_client(rec)
    with pytest.raises(RuntimeError) as exc:
        _run(dispatch("scan_status", {"source_id": 999},
                      client, _settings()))
    assert "404" in str(exc.value)
    assert "no such source" in str(exc.value)


def test_bearer_token_added_when_configured():
    rec = _Recorder()
    client = _make_client(rec)
    settings = Settings(base_url="http://test:8085", api_key="s3cret", timeout=5.0)
    _run(dispatch("scan_list_sources", {}, client, settings))
    auth = rec.calls[0].headers.get("authorization")
    assert auth == "Bearer s3cret"


# ---------------------------------------------------------------------------
# Schema export — JSON-Schema dicts must round-trip through json.dumps so
# the MCP layer can ship them on the wire without any custom encoder.
# ---------------------------------------------------------------------------


def test_all_schemas_are_json_serialisable():
    for tool in TOOLS:
        # Will raise if anything in the schema is a non-JSON type
        # (datetime, set, ...). Catch it now, not at first list_tools.
        json.dumps(tool.json_schema())


def test_index_matches_tuple():
    assert set(TOOL_INDEX.keys()) == {t.name for t in TOOLS}
