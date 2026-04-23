"""End-to-end test: MCP tools driving a real (small) FastAPI app on a
random port over the loopback interface.

This is the integration counterpart to test_mcp_tools.py — instead of
mocking httpx we boot uvicorn in a daemon thread serving a stand-in
FastAPI app that mirrors only the routes our tools call. That's enough
to prove URL/body/header wiring works end-to-end without dragging in
SQLite, DuckDB, the scanner, etc.
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time
from typing import Any, Optional

import httpx
import pytest

fastapi = pytest.importorskip("fastapi")
uvicorn = pytest.importorskip("uvicorn")
from fastapi import FastAPI  # noqa: E402

from src.mcp_server.config import Settings  # noqa: E402
from src.mcp_server.tools import dispatch  # noqa: E402


# ---------------------------------------------------------------------------
# Test fixture: a tiny FastAPI app implementing exactly the endpoints
# touched by our 15 tools. We record every incoming request so each test
# can assert what the MCP layer actually sent over HTTP.
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def live_dashboard():
    """Boot a stand-in FastAPI app on a random port for the whole module."""
    app = FastAPI()
    received: list[dict[str, Any]] = []

    def record(path: str, request, body: Optional[dict] = None) -> None:
        received.append({
            "path": path,
            "query": dict(request.query_params),
            "body": body,
            "auth": request.headers.get("authorization"),
        })

    @app.get("/api/sources")
    async def sources(request: fastapi.Request):
        record("/api/sources", request)
        return [
            {"id": 1, "name": "demo", "unc_path": "\\\\server\\share",
             "archive_dest": "D:\\Archive\\demo"},
        ]

    @app.post("/api/scan/{sid}")
    async def scan(sid: int, request: fastapi.Request):
        record(f"/api/scan/{sid}", request)
        return {"status": "started", "message": f"scan {sid} kicked off"}

    @app.get("/api/scan/progress/{sid}")
    async def scan_progress(sid: int, request: fastapi.Request):
        record(f"/api/scan/progress/{sid}", request)
        return {"status": "running", "file_count": 1234, "total_size": 9999, "finished": False}

    @app.get("/api/overview/{sid}")
    async def overview(sid: int, request: fastapi.Request):
        record(f"/api/overview/{sid}", request)
        return {
            "scan_id": 42, "has_data": True,
            "total_files": 1234, "total_size": 5_000_000,
            "total_size_formatted": "5 MB",
        }

    @app.get("/api/reports/duplicates/{sid}")
    async def duplicates(sid: int, request: fastapi.Request):
        record(f"/api/reports/duplicates/{sid}", request)
        return {"groups": [], "total_waste_size": 0}

    @app.get("/api/security/orphan-sids/{sid}")
    async def orphan_sids(sid: int, request: fastapi.Request):
        record(f"/api/security/orphan-sids/{sid}", request)
        return {"source_id": sid, "orphan_sids": []}

    @app.get("/api/compliance/pii/findings")
    async def pii_findings(request: fastapi.Request):
        record("/api/compliance/pii/findings", request)
        return {"total": 0, "page": 1, "page_size": 50, "findings": []}

    @app.get("/api/compliance/pii/subject")
    async def pii_subject(request: fastapi.Request):
        record("/api/compliance/pii/subject", request)
        return {"term": request.query_params.get("term"), "matches": 0, "files": []}

    @app.post("/api/archive/dry-run")
    async def archive_dry_run(body: dict, request: fastapi.Request):
        record("/api/archive/dry-run", request, body)
        return {"file_count": 12, "total_size": 1_000_000, "sample": []}

    @app.post("/api/archive/run")
    async def archive_run(body: dict, request: fastapi.Request):
        record("/api/archive/run", request, body)
        return {"archived": 12, "operation_id": 99}

    @app.get("/api/compliance/legal-holds/active")
    async def holds_active(request: fastapi.Request):
        record("/api/compliance/legal-holds/active", request)
        return {"holds": [{"id": 1, "pattern": "/x/*", "reason": "case 1"}]}

    @app.post("/api/compliance/legal-holds")
    async def hold_add(body: dict, request: fastapi.Request):
        record("/api/compliance/legal-holds", request, body)
        return {"id": 5, "ok": True}

    @app.post("/api/compliance/legal-holds/{hid}/release")
    async def hold_release(hid: int, body: dict, request: fastapi.Request):
        record(f"/api/compliance/legal-holds/{hid}/release", request, body)
        return {"ok": True}

    @app.get("/api/audit/events")
    async def audit_events(request: fastapi.Request):
        record("/api/audit/events", request)
        return {
            "events": [
                {"id": 1, "event_type": "archive", "username": "alice"},
                {"id": 2, "event_type": "archive", "username": "alice"},
            ],
            "total": 2,
            "page": 1,
        }

    @app.get("/api/audit/verify")
    async def audit_verify(request: fastapi.Request):
        record("/api/audit/verify", request)
        return {"verified": True, "total": 100, "broken_at": None, "broken_reason": None}

    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port,
                            log_level="warning", lifespan="off")
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait for the server to accept connections (≤ 3 s).
    deadline = time.time() + 3.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                break
        except OSError:
            time.sleep(0.05)
    else:
        pytest.fail("uvicorn test server never came up")

    yield {"port": port, "received": received}

    server.should_exit = True
    thread.join(timeout=3)


def _settings(port: int, api_key: str | None = None) -> Settings:
    return Settings(base_url=f"http://127.0.0.1:{port}", api_key=api_key, timeout=5.0)


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Real end-to-end calls
# ---------------------------------------------------------------------------


def test_scan_list_sources_end_to_end(live_dashboard):
    settings = _settings(live_dashboard["port"])

    async def _go():
        async with httpx.AsyncClient() as client:
            return await dispatch("scan_list_sources", {}, client, settings)

    result = _run(_go())
    assert isinstance(result, list) and result[0]["id"] == 1
    paths = [c["path"] for c in live_dashboard["received"]]
    assert "/api/sources" in paths


def test_scan_status_passes_path_param(live_dashboard):
    settings = _settings(live_dashboard["port"])

    async def _go():
        async with httpx.AsyncClient() as client:
            return await dispatch("scan_status", {"source_id": 7},
                                  client, settings)

    result = _run(_go())
    assert result["status"] == "running"
    assert any(c["path"] == "/api/scan/progress/7" for c in live_dashboard["received"])


def test_archive_run_confirm_gate_end_to_end(live_dashboard):
    settings = _settings(live_dashboard["port"])

    async def _preview():
        async with httpx.AsyncClient() as client:
            return await dispatch(
                "archive_run",
                {"source_id": 1, "days": 365},
                client, settings,
            )

    out = _run(_preview())
    assert out.get("preview") is True
    # The dashboard must NOT see this call.
    archive_calls = [c for c in live_dashboard["received"]
                     if c["path"] == "/api/archive/run"]
    assert archive_calls == []

    async def _execute():
        async with httpx.AsyncClient() as client:
            return await dispatch(
                "archive_run",
                {"source_id": 1, "days": 365, "confirm": True},
                client, settings,
            )

    result = _run(_execute())
    assert result["archived"] == 12
    archive_calls = [c for c in live_dashboard["received"]
                     if c["path"] == "/api/archive/run"]
    assert len(archive_calls) == 1
    assert archive_calls[0]["body"] == {"source_id": 1, "days": 365}


def test_audit_query_pipeline(live_dashboard):
    settings = _settings(live_dashboard["port"])

    async def _go():
        async with httpx.AsyncClient() as client:
            return await dispatch(
                "audit_query",
                {"actor": "alice", "action": "archive",
                 "since_days": 14, "limit": 1},
                client, settings,
            )

    result = _run(_go())
    # Server returned 2 events, our limit clipped to 1.
    assert len(result["events"]) == 1
    call = next(c for c in live_dashboard["received"]
                if c["path"] == "/api/audit/events")
    assert call["query"]["username"] == "alice"
    assert call["query"]["event_type"] == "archive"
    assert call["query"]["days"] == "14"


def test_bearer_token_reaches_dashboard(live_dashboard):
    settings = _settings(live_dashboard["port"], api_key="end2end-token")

    async def _go():
        async with httpx.AsyncClient() as client:
            return await dispatch("hold_list_active", {}, client, settings)

    _run(_go())
    call = next(c for c in live_dashboard["received"]
                if c["path"] == "/api/compliance/legal-holds/active"
                and c["auth"] == "Bearer end2end-token")
    assert call is not None
