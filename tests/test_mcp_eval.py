"""MCP eval suite — realistic admin questions mapped to expected tool
calls and argument subsets.

This follows the mcp-builder methodology referenced in issue #65: each
case asserts that *given a question*, the right tool is selected with
the right arguments. We deliberately do NOT spin up an LLM; instead we
build the server in-process, drive ``call_tool`` directly with the
arguments an LLM would produce, and assert the dispatch + URL pipeline
behaves as required.

The suite is skipped if the ``mcp`` SDK is not installed, mirroring the
optional-dependency posture in setup.py.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Optional

import httpx
import pytest

mcp = pytest.importorskip("mcp")  # noqa: F841 — skip module if SDK missing

from src.mcp_server.config import Settings  # noqa: E402
from src.mcp_server.server import build_server  # noqa: E402
from src.mcp_server.tools import TOOL_INDEX, dispatch  # noqa: E402


# ---------------------------------------------------------------------------
# Eval cases — what an admin would type into Claude Code, the tool we
# expect Claude to pick, and the argument keys/values we expect to see.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EvalCase:
    question: str
    expected_tool: str
    expected_args_subset: dict[str, Any]
    # Args the LLM would actually pass — used by the dispatch path so we
    # can verify the underlying URL ends up correct. None means "use
    # expected_args_subset as-is".
    args: Optional[dict[str, Any]] = None


CASES: tuple[EvalCase, ...] = (
    EvalCase(
        question="Which file shares are configured?",
        expected_tool="scan_list_sources",
        expected_args_subset={},
    ),
    EvalCase(
        question="Start a fresh scan of source 3.",
        expected_tool="scan_run",
        expected_args_subset={"source_id": 3, "confirm": True},
    ),
    EvalCase(
        question="What's the latest scan status for source 1?",
        expected_tool="scan_status",
        expected_args_subset={"source_id": 1},
    ),
    EvalCase(
        question="Give me the KPI overview for source 2.",
        expected_tool="report_summary",
        expected_args_subset={"source_id": 2},
    ),
    EvalCase(
        question="Show duplicate-file groups bigger than 1 GB on source 1.",
        expected_tool="report_duplicates",
        expected_args_subset={"source_id": 1, "min_size": 1_073_741_824},
    ),
    EvalCase(
        question="List orphan owner SIDs from the latest scan of source 4.",
        expected_tool="report_orphan_sids",
        expected_args_subset={"source_id": 4},
    ),
    EvalCase(
        question="Show all PII findings of type email, top 25.",
        expected_tool="pii_list_findings",
        expected_args_subset={"pattern": "email", "limit": 25},
    ),
    EvalCase(
        question="Run a GDPR Article 17 export for the subject 'jane@x.com'.",
        expected_tool="pii_subject_export",
        expected_args_subset={"term": "jane@x.com"},
    ),
    EvalCase(
        question="Preview what would be archived from source 1 if we used 365 days.",
        expected_tool="archive_dry_run",
        expected_args_subset={"source_id": 1, "days": 365},
    ),
    EvalCase(
        question="Actually archive source 1 with the stale-files policy.",
        expected_tool="archive_run",
        expected_args_subset={"source_id": 1, "policy_name": "stale-files",
                              "confirm": True},
    ),
    EvalCase(
        question="What legal holds are currently active?",
        expected_tool="hold_list_active",
        expected_args_subset={},
    ),
    EvalCase(
        question="Place a legal hold on /Projects/Acme/* for case 2024-17.",
        expected_tool="hold_add",
        expected_args_subset={"pattern": "/Projects/Acme/*",
                              "reason": "case 2024-17", "confirm": True},
        args={"pattern": "/Projects/Acme/*", "reason": "case 2024-17",
              "case_ref": "2024-17", "confirm": True},
    ),
    EvalCase(
        question="Release legal hold #5.",
        expected_tool="hold_release",
        expected_args_subset={"hold_id": 5, "confirm": True},
    ),
    EvalCase(
        question="Show audit events from user 'alice' in the last 7 days.",
        expected_tool="audit_query",
        expected_args_subset={"actor": "alice", "since_days": 7},
    ),
    EvalCase(
        question="Verify the audit hash chain.",
        expected_tool="audit_verify_chain",
        expected_args_subset={},
    ),
)


# ---------------------------------------------------------------------------
# Static checks: every case targets a real tool, args validate, write
# tools have confirm=True so the eval doesn't accidentally assert a
# preview response.
# ---------------------------------------------------------------------------


def test_eval_suite_has_at_least_ten_cases():
    assert len(CASES) >= 10


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.expected_tool)
def test_case_targets_known_tool_with_valid_args(case: EvalCase):
    tool = TOOL_INDEX.get(case.expected_tool)
    assert tool is not None, f"unknown tool: {case.expected_tool}"

    args = case.args or case.expected_args_subset
    # Pydantic raises on invalid args — that's the assertion.
    validated = tool.input_model.model_validate(args)
    dumped = validated.model_dump()
    for key, value in case.expected_args_subset.items():
        assert key in dumped, f"{case.expected_tool}: missing key {key}"
        assert dumped[key] == value, (
            f"{case.expected_tool}: expected {key}={value!r}, got {dumped[key]!r}"
        )


# ---------------------------------------------------------------------------
# Live drive: run every case through dispatch() against a mock httpx
# transport. Confirms the tool actually translates question-shaped args
# into the right HTTP call.
# ---------------------------------------------------------------------------


class _Echo:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        # Default to a payload shape every tool can swallow.
        return httpx.Response(200, json={"ok": True, "events": [], "groups": [],
                                         "findings": [], "files": [],
                                         "holds": [], "verified": True,
                                         "total": 0, "page": 1, "page_size": 50})


def _settings() -> Settings:
    return Settings(base_url="http://eval:8085", api_key=None, timeout=5.0)


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.expected_tool)
def test_dispatch_executes_each_eval_case(case: EvalCase):
    echo = _Echo()
    transport = httpx.MockTransport(echo.handler)

    async def _go():
        async with httpx.AsyncClient(transport=transport) as client:
            args = case.args or case.expected_args_subset
            return await dispatch(case.expected_tool, dict(args),
                                  client, _settings())

    loop = asyncio.new_event_loop()
    try:
        result = loop.run_until_complete(_go())
    finally:
        loop.close()

    tool = TOOL_INDEX[case.expected_tool]
    if tool.is_write:
        # confirm=True is set in the case — we expect a real call, not a preview.
        assert not (isinstance(result, dict) and result.get("preview")), (
            f"write tool {case.expected_tool} returned preview despite confirm=true"
        )
        assert echo.requests, f"{case.expected_tool} made no HTTP call"
    else:
        # Read-only tool — must always hit the API.
        assert echo.requests, f"{case.expected_tool} made no HTTP call"


# ---------------------------------------------------------------------------
# MCP-protocol level smoke test: build the real server, list tools, call
# one of them through the actual server.call_tool dispatch path. Doesn't
# require a stdio loop — we drive the registered handlers directly.
# ---------------------------------------------------------------------------


def test_build_server_registers_all_tools():
    server = build_server(settings=_settings())

    # The low-level Server stores its handlers in `request_handlers`
    # keyed by request type. We simply check the listing works by
    # invoking the registered list_tools handler directly.
    from mcp import types

    handler = server.request_handlers[types.ListToolsRequest]
    loop = asyncio.new_event_loop()
    try:
        result = loop.run_until_complete(
            handler(types.ListToolsRequest(method="tools/list"))
        )
    finally:
        loop.close()

    # ServerResult wrapping ListToolsResult
    inner = result.root if hasattr(result, "root") else result
    names = {t.name for t in inner.tools}
    assert {c.expected_tool for c in CASES} <= names, (
        f"server is missing tools required by eval suite: "
        f"{ {c.expected_tool for c in CASES} - names }"
    )
    # Every advertised schema must round-trip JSON.
    for tool in inner.tools:
        json.dumps(tool.inputSchema)
