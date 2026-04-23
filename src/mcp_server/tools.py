"""Tool implementations for the file-activity MCP server.

Each tool is a thin wrapper over a single FastAPI endpoint. The pattern is
deliberately uniform so it can be audited at a glance:

    1. ``ToolDef`` declares the tool name, description, Pydantic input
       schema and whether it is a *write* (mutates state on the server).
    2. ``run`` deserialises the raw arguments dict into the Pydantic model,
       enforces the confirm-gate for writes, and dispatches to ``_handler``.
    3. ``_handler`` does the HTTP call via the shared ``httpx.AsyncClient``
       and returns a JSON-serialisable dict.

We never import anything from ``src.storage`` or ``src.archiver``; the
contract this server exposes is exactly what the REST API exposes (the
issue is explicit about this — see project context).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

import httpx
from pydantic import BaseModel, Field, ValidationError

from .config import Settings

# ---------------------------------------------------------------------------
# Pydantic input schemas
# ---------------------------------------------------------------------------


class _ConfirmModel(BaseModel):
    """Marker base for write tools — they all carry ``confirm: bool``.

    Mirrors PowerShell ``-Confirm`` / ``-WhatIf``. Default is False so a
    naive caller gets a dry-run-style "would call X" response instead of
    accidentally triggering an archive or releasing a legal hold.
    """

    confirm: bool = Field(
        default=False,
        description="Must be true to actually execute the write. False returns a preview.",
    )


class ScanListSourcesInput(BaseModel):
    pass


class ScanRunInput(_ConfirmModel):
    source_id: int = Field(..., ge=1, description="Numeric source id (see scan_list_sources).")


class ScanStatusInput(BaseModel):
    source_id: int = Field(..., ge=1)


class ReportSummaryInput(BaseModel):
    source_id: int = Field(..., ge=1)


class ReportDuplicatesInput(BaseModel):
    source_id: int = Field(..., ge=1)
    min_size: int = Field(default=0, ge=0, description="Minimum file size in bytes.")
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=500)


class ReportOrphanSidsInput(BaseModel):
    source_id: int = Field(..., ge=1)
    max_unique_sids: Optional[int] = Field(default=None, ge=1, le=100000)


class PiiListFindingsInput(BaseModel):
    pattern: Optional[str] = Field(default=None, description="Filter by pattern_name (e.g. 'email').")
    limit: int = Field(default=50, ge=1, le=1000, description="Page size; mapped to API page_size.")


class PiiSubjectExportInput(BaseModel):
    term: str = Field(..., min_length=1, description="Subject term (name, email, etc).")


class ArchiveDryRunInput(BaseModel):
    source_id: int = Field(..., ge=1)
    days: Optional[int] = Field(default=None, ge=1, description="Override age threshold (days).")


class ArchiveRunInput(_ConfirmModel):
    source_id: int = Field(..., ge=1)
    days: Optional[int] = Field(default=None, ge=1)
    policy_name: Optional[str] = Field(default=None, description="Use a stored policy by name.")


class HoldListActiveInput(BaseModel):
    pass


class HoldAddInput(_ConfirmModel):
    pattern: str = Field(..., min_length=1, description="Glob/path pattern to freeze.")
    reason: str = Field(..., min_length=1)
    case_ref: Optional[str] = None
    created_by: str = Field(default="mcp")


class HoldReleaseInput(_ConfirmModel):
    hold_id: int = Field(..., ge=1)
    released_by: str = Field(default="mcp")


class AuditQueryInput(BaseModel):
    actor: Optional[str] = Field(default=None, description="Username filter.")
    action: Optional[str] = Field(default=None, description="event_type filter.")
    source_id: Optional[int] = Field(default=None, ge=1)
    since_days: int = Field(default=30, ge=1, le=365)
    limit: int = Field(default=100, ge=1, le=10000)


class AuditVerifyChainInput(BaseModel):
    since_seq: int = Field(default=1, ge=1)
    end_seq: Optional[int] = Field(default=None, ge=1)


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------


# Handler signature: (client, settings, validated_input_model) -> dict
Handler = Callable[[httpx.AsyncClient, Settings, BaseModel], Awaitable[Any]]


@dataclass(frozen=True)
class ToolDef:
    name: str
    description: str
    input_model: type[BaseModel]
    handler: Handler
    is_write: bool = False

    def json_schema(self) -> dict[str, Any]:
        # Pydantic v2: model_json_schema() returns a JSON-Schema-shaped dict
        # which is exactly what the MCP `tools/list` response wants.
        schema = self.input_model.model_json_schema()
        # MCP requires top-level "type": "object" (Pydantic supplies it).
        schema.setdefault("type", "object")
        return schema


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


async def _get(client: httpx.AsyncClient, settings: Settings, path: str,
               params: Optional[dict[str, Any]] = None) -> Any:
    url = f"{settings.base_url}{path}"
    resp = await client.get(url, params=params, headers=settings.headers(),
                            timeout=settings.timeout)
    return _unwrap(resp)


async def _post(client: httpx.AsyncClient, settings: Settings, path: str,
                json_body: Optional[dict[str, Any]] = None,
                params: Optional[dict[str, Any]] = None) -> Any:
    url = f"{settings.base_url}{path}"
    resp = await client.post(url, json=json_body, params=params,
                             headers=settings.headers(),
                             timeout=settings.timeout)
    return _unwrap(resp)


def _unwrap(resp: httpx.Response) -> Any:
    """Return parsed JSON, or raise a tidy error for non-2xx."""
    if resp.status_code >= 400:
        # Surface the API error message verbatim — FastAPI HTTPException
        # bodies are usually {"detail": "..."} which is more useful than
        # a generic 4xx string.
        try:
            body = resp.json()
        except Exception:
            body = {"raw": resp.text}
        raise RuntimeError(
            f"Dashboard returned HTTP {resp.status_code}: {json.dumps(body)[:500]}"
        )
    if not resp.content:
        return {}
    try:
        return resp.json()
    except Exception:
        return {"raw": resp.text}


def _confirm_preview(tool_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Standard 'would call X' response when ``confirm=False`` on a write."""
    redacted = {k: v for k, v in payload.items() if k != "confirm"}
    return {
        "preview": True,
        "tool": tool_name,
        "message": (
            f"Dry-run: would call {tool_name} with {json.dumps(redacted, sort_keys=True)}. "
            f"Re-invoke with confirm=true to execute."
        ),
        "args": redacted,
    }


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


async def _h_scan_list_sources(client, settings, _inp):  # type: ignore[no-untyped-def]
    return await _get(client, settings, "/api/sources")


async def _h_scan_run(client, settings, inp: ScanRunInput):
    return await _post(client, settings, f"/api/scan/{inp.source_id}")


async def _h_scan_status(client, settings, inp: ScanStatusInput):
    return await _get(client, settings, f"/api/scan/progress/{inp.source_id}")


async def _h_report_summary(client, settings, inp: ReportSummaryInput):
    return await _get(client, settings, f"/api/overview/{inp.source_id}")


async def _h_report_duplicates(client, settings, inp: ReportDuplicatesInput):
    return await _get(
        client, settings,
        f"/api/reports/duplicates/{inp.source_id}",
        params={"page": inp.page, "page_size": inp.page_size, "min_size": inp.min_size},
    )


async def _h_report_orphan_sids(client, settings, inp: ReportOrphanSidsInput):
    params: dict[str, Any] = {}
    if inp.max_unique_sids is not None:
        params["max_unique_sids"] = inp.max_unique_sids
    return await _get(client, settings,
                      f"/api/security/orphan-sids/{inp.source_id}",
                      params=params or None)


async def _h_pii_list_findings(client, settings, inp: PiiListFindingsInput):
    params: dict[str, Any] = {"page": 1, "page_size": inp.limit}
    if inp.pattern:
        params["pattern"] = inp.pattern
    return await _get(client, settings, "/api/compliance/pii/findings", params=params)


async def _h_pii_subject_export(client, settings, inp: PiiSubjectExportInput):
    return await _get(client, settings, "/api/compliance/pii/subject",
                      params={"term": inp.term, "format": "json"})


async def _h_archive_dry_run(client, settings, inp: ArchiveDryRunInput):
    body: dict[str, Any] = {"source_id": inp.source_id}
    if inp.days is not None:
        body["days"] = inp.days
    return await _post(client, settings, "/api/archive/dry-run", json_body=body)


async def _h_archive_run(client, settings, inp: ArchiveRunInput):
    body: dict[str, Any] = {"source_id": inp.source_id}
    if inp.days is not None:
        body["days"] = inp.days
    if inp.policy_name:
        body["policy_name"] = inp.policy_name
    return await _post(client, settings, "/api/archive/run", json_body=body)


async def _h_hold_list_active(client, settings, _inp):  # type: ignore[no-untyped-def]
    return await _get(client, settings, "/api/compliance/legal-holds/active")


async def _h_hold_add(client, settings, inp: HoldAddInput):
    body = {
        "pattern": inp.pattern,
        "reason": inp.reason,
        "case_ref": inp.case_ref,
        "created_by": inp.created_by,
    }
    return await _post(client, settings, "/api/compliance/legal-holds", json_body=body)


async def _h_hold_release(client, settings, inp: HoldReleaseInput):
    body = {"released_by": inp.released_by}
    return await _post(
        client, settings,
        f"/api/compliance/legal-holds/{inp.hold_id}/release",
        json_body=body,
    )


async def _h_audit_query(client, settings, inp: AuditQueryInput):
    # The dashboard endpoint paginates by page (page_size baked in). We
    # surface ``limit`` in our schema for a cleaner LLM contract; map by
    # asking for one big page (page=1).
    params: dict[str, Any] = {"days": inp.since_days, "page": 1}
    if inp.actor:
        params["username"] = inp.actor
    if inp.action:
        params["event_type"] = inp.action
    if inp.source_id is not None:
        params["source_id"] = inp.source_id
    result = await _get(client, settings, "/api/audit/events", params=params)
    # Normalise: always return a paged-style dict so the caller can rely
    # on ``events`` being a list (the dashboard returns either a list
    # directly or a dict — the PowerShell module makes the same fix-up).
    if isinstance(result, list):
        events = result
        result = {"events": events, "total": len(events), "page": 1}
    if isinstance(result, dict) and "events" in result and inp.limit:
        result["events"] = result["events"][: inp.limit]
    return result


async def _h_audit_verify_chain(client, settings, inp: AuditVerifyChainInput):
    params: dict[str, Any] = {"since_seq": inp.since_seq}
    if inp.end_seq is not None:
        params["end_seq"] = inp.end_seq
    return await _get(client, settings, "/api/audit/verify", params=params)


# ---------------------------------------------------------------------------
# Registry — single source of truth
# ---------------------------------------------------------------------------


TOOLS: tuple[ToolDef, ...] = (
    ToolDef(
        name="scan_list_sources",
        description="List configured file-share sources (id, name, UNC path, archive dest).",
        input_model=ScanListSourcesInput,
        handler=_h_scan_list_sources,
    ),
    ToolDef(
        name="scan_run",
        description=(
            "Start a background scan of the given source. Write op — pass "
            "confirm=true to execute. Returns {status, message}."
        ),
        input_model=ScanRunInput,
        handler=_h_scan_run,
        is_write=True,
    ),
    ToolDef(
        name="scan_status",
        description="Return live scan progress for a source (status, file_count, total_size).",
        input_model=ScanStatusInput,
        handler=_h_scan_status,
    ),
    ToolDef(
        name="report_summary",
        description="Latest-scan KPI summary: totals, stale/large/duplicate sizes, top extensions/owners.",
        input_model=ReportSummaryInput,
        handler=_h_report_summary,
    ),
    ToolDef(
        name="report_duplicates",
        description="Paged list of duplicate-content groups with wasted bytes per group.",
        input_model=ReportDuplicatesInput,
        handler=_h_report_duplicates,
    ),
    ToolDef(
        name="report_orphan_sids",
        description="Owner SIDs that no longer resolve in AD for the latest scan.",
        input_model=ReportOrphanSidsInput,
        handler=_h_report_orphan_sids,
    ),
    ToolDef(
        name="pii_list_findings",
        description="Browse persisted PII findings (redacted snippets). Optional pattern filter.",
        input_model=PiiListFindingsInput,
        handler=_h_pii_list_findings,
    ),
    ToolDef(
        name="pii_subject_export",
        description="GDPR Article 17/30 subject export — every file mentioning the term.",
        input_model=PiiSubjectExportInput,
        handler=_h_pii_subject_export,
    ),
    ToolDef(
        name="archive_dry_run",
        description="Preview archive candidates for a source (file count, total size, sample). Read-only.",
        input_model=ArchiveDryRunInput,
        handler=_h_archive_dry_run,
    ),
    ToolDef(
        name="archive_run",
        description=(
            "Execute the archive workflow (copy-verify-delete) for a source. "
            "Write op — pass confirm=true. Either days or policy_name is required."
        ),
        input_model=ArchiveRunInput,
        handler=_h_archive_run,
        is_write=True,
    ),
    ToolDef(
        name="hold_list_active",
        description="List active legal holds (pattern, reason, case_ref, created_at).",
        input_model=HoldListActiveInput,
        handler=_h_hold_list_active,
    ),
    ToolDef(
        name="hold_add",
        description=(
            "Create a new legal hold that freezes paths matching pattern. "
            "Write op — pass confirm=true."
        ),
        input_model=HoldAddInput,
        handler=_h_hold_add,
        is_write=True,
    ),
    ToolDef(
        name="hold_release",
        description=(
            "Release an active legal hold by id. Write op — pass confirm=true. "
            "Holds are never deleted, only released; the audit chain records both."
        ),
        input_model=HoldReleaseInput,
        handler=_h_hold_release,
        is_write=True,
    ),
    ToolDef(
        name="audit_query",
        description=(
            "Query the tamper-evident audit log. Filters: actor (username), "
            "action (event_type), source_id, since_days (default 30)."
        ),
        input_model=AuditQueryInput,
        handler=_h_audit_query,
    ),
    ToolDef(
        name="audit_verify_chain",
        description=(
            "Verify the SHA-256 hash chain over audit_log_chain. Returns "
            "{verified, total, broken_at, broken_reason}."
        ),
        input_model=AuditVerifyChainInput,
        handler=_h_audit_verify_chain,
    ),
)


TOOL_INDEX: dict[str, ToolDef] = {t.name: t for t in TOOLS}


# ---------------------------------------------------------------------------
# Dispatch entry point used by both server.py and the eval suite
# ---------------------------------------------------------------------------


async def dispatch(
    name: str,
    raw_args: Optional[dict[str, Any]],
    client: httpx.AsyncClient,
    settings: Settings,
) -> Any:
    """Validate args, enforce confirm-gate, run the handler.

    Raised exceptions are propagated; the server layer turns them into MCP
    error responses. Tests can call this directly without a stdio loop.
    """
    tool = TOOL_INDEX.get(name)
    if tool is None:
        raise KeyError(f"Unknown tool: {name}")

    raw_args = raw_args or {}
    try:
        validated = tool.input_model.model_validate(raw_args)
    except ValidationError as e:
        # Re-raise as ValueError so MCP layer can map to a tool-side error
        # rather than a server crash.
        raise ValueError(f"Invalid arguments for {name}: {e}") from e

    if tool.is_write:
        confirm = bool(getattr(validated, "confirm", False))
        if not confirm:
            return _confirm_preview(name, validated.model_dump())

    return await tool.handler(client, settings, validated)


__all__ = [
    "TOOLS",
    "TOOL_INDEX",
    "ToolDef",
    "dispatch",
    # Schemas (re-exported for tests / external consumers)
    "ScanListSourcesInput",
    "ScanRunInput",
    "ScanStatusInput",
    "ReportSummaryInput",
    "ReportDuplicatesInput",
    "ReportOrphanSidsInput",
    "PiiListFindingsInput",
    "PiiSubjectExportInput",
    "ArchiveDryRunInput",
    "ArchiveRunInput",
    "HoldListActiveInput",
    "HoldAddInput",
    "HoldReleaseInput",
    "AuditQueryInput",
    "AuditVerifyChainInput",
]
