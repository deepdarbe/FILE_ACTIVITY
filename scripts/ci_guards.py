"""CI guards for FILE_ACTIVITY (issue #194 stabilization week / Wave 5).

A single script bundling the lightweight, deterministic checks that should
have caught today's hotfix regressions before they shipped:

  * D-YAML  — duplicate-key detection on config.yaml. PyYAML's default
    loader silently overrides earlier keys; the live ``compliance:``
    duplicate that dropped PII + retention config slipped through
    `.github/workflows/ci.yml` because `yaml.safe_load` reported success
    on a half-loaded document.
  * S-YAML  — schema assertion: the documented top-level keys exist
    under ``compliance`` (pii, retention, legal_hold, standards).
  * LOADERS — every key referenced in the dashboard's ``loaders = {...}``
    dict has a matching function/const declaration. PR #197's
    ``loadPii is not defined`` regression.
  * HTML-BUDGET — the count of raw ``innerHTML =`` writes in
    index.html must not rise above the checked-in baseline. Forces a
    reviewer-visible decision before adding to the XSS / null-crash
    surface (PR #200 / #202 class).
  * D-CHAIN  — zero tolerance for the specific ``document.getElementById
    (...).innerHTML =`` / ``document.querySelector(...).innerHTML =``
    pattern. This is the *exact* shape that produced the #200 / #201 /
    #202 null-deref regressions (chained, no element-existence check).
    Every instance must use ``_setHtmlSafe(id, html)`` which logs and
    no-ops when the element is missing. Baseline 0 after the 2026-05-22
    migration; any new occurrence fails CI.
  * SVC-PARITY — every deploy script that touches the Windows service
    by name must use the same service name. ``update.bat`` and
    ``auto-update.ps1`` drifted to ``FileActivityService`` while
    ``setup-source.ps1`` and ``install_service.ps1`` use
    ``FileActivity`` — silent no-op when update.bat tries to stop the
    service.
  * R-CACHE — Rule 1 of docs/standards/endpoint-conventions.md. Every
    direct ``analyzer_cache.get_or_compute(...)`` callsite in
    ``src/dashboard/api.py`` must go through the
    ``cached_report_endpoint(...)`` helper from
    ``src/dashboard/_endpoint_helpers.py``. The exception is endpoints
    that hand-roll cache for a documented reason; those are listed in
    ``R_CACHE_ALLOWLIST`` with a justification.
  * A-AWAIT — Rule 5 of the standard. Every ``async def`` route handler
    in ``api.py`` must contain at least one ``await`` / ``async for`` /
    ``async with`` in its own body. Otherwise it should be a plain
    ``def`` so FastAPI dispatches it to the threadpool (prevents the
    event-loop starvation that produced PR #215).

Each check prints a GitHub Actions ``::error::`` annotation on failure
and exits the script with a non-zero status. ``--check NAME`` lets the
workflow run a single check at a time; with no flag every check runs.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_YAML = REPO_ROOT / "config.yaml"
INDEX_HTML = REPO_ROOT / "src" / "dashboard" / "static" / "index.html"

# innerHTML write count threshold. Tightened 2026-05-22 after the
# direct-chain migration (18 ``document.getElementById(...).innerHTML =``
# call-sites moved to ``_setHtmlSafe``). The previous 180 was the Wave 3
# baseline; current count is 131. Threshold set above current with a
# small headroom so a single PR adding a handful of writes is reviewed,
# not silently merged. Raise this with reviewer sign-off if a legitimate
# new write site is needed.
INNERHTML_BUDGET = 140

# Windows service name used by the FileActivity service. Set in
# install_service.ps1 / setup-source.ps1; the older update.bat and
# auto-update.ps1 references must agree exactly.
SERVICE_NAME = "FileActivity"

# Files that name the Windows service in a service-management context
# (Stop-Service, Start-Service, sc query, net stop/start).
SVC_PARITY_FILES = [
    "deploy/setup-source.ps1",
    "deploy/install_service.ps1",
    "deploy/uninstall_service.ps1",
    "deploy/auto-update.ps1",
    "deploy/update.bat",
    "deploy/install_tray.ps1",
]


def _err(check: str, msg: str) -> None:
    """Emit a GitHub Actions error annotation."""
    print(f"::error title=ci_guards/{check}::{msg}", file=sys.stderr)


def _ok(check: str, msg: str) -> None:
    print(f"[OK] {check}: {msg}")


# ---------------------------------------------------------------------------
# D-YAML — duplicate-key detection
# ---------------------------------------------------------------------------


def _yaml_no_duplicates_loader():
    """Return a yaml.SafeLoader subclass that raises on duplicate keys.

    PyYAML's default ``construct_mapping`` does not check for duplicates
    — the later key silently overrides. We override
    ``construct_mapping`` to track seen keys per mapping node and raise
    ``yaml.constructor.ConstructorError`` on the first duplicate.
    """
    import yaml

    class _NoDupLoader(yaml.SafeLoader):
        pass

    def _construct_mapping(loader, node, deep=False):
        mapping = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node, deep=deep)
            if key in mapping:
                raise yaml.constructor.ConstructorError(
                    None, None,
                    f"duplicate key {key!r} (line {key_node.start_mark.line + 1})",
                    key_node.start_mark,
                )
            mapping[key] = loader.construct_object(value_node, deep=deep)
        return mapping

    _NoDupLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        _construct_mapping,
    )
    return _NoDupLoader


def check_yaml_duplicates() -> bool:
    try:
        import yaml
    except Exception as e:
        _err("D-YAML", f"PyYAML unavailable: {e}")
        return False
    if not CONFIG_YAML.exists():
        _err("D-YAML", f"{CONFIG_YAML} not found")
        return False
    loader_cls = _yaml_no_duplicates_loader()
    with open(CONFIG_YAML, "r", encoding="utf-8") as f:
        try:
            yaml.load(f, Loader=loader_cls)
        except yaml.constructor.ConstructorError as e:
            _err("D-YAML", f"config.yaml: {e}")
            return False
        except yaml.YAMLError as e:
            _err("D-YAML", f"config.yaml parse failure: {e}")
            return False
    _ok("D-YAML", "config.yaml has no duplicate keys")
    return True


# ---------------------------------------------------------------------------
# S-YAML — compliance schema assertion
# ---------------------------------------------------------------------------


COMPLIANCE_REQUIRED_KEYS = ("pii", "retention", "legal_hold", "standards")


def check_yaml_schema() -> bool:
    try:
        import yaml
    except Exception as e:
        _err("S-YAML", f"PyYAML unavailable: {e}")
        return False
    with open(CONFIG_YAML, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        _err("S-YAML", "config.yaml root is not a mapping")
        return False
    compliance = cfg.get("compliance")
    if not isinstance(compliance, dict):
        _err("S-YAML", "config.yaml has no top-level 'compliance' mapping")
        return False
    missing = [k for k in COMPLIANCE_REQUIRED_KEYS if k not in compliance]
    if missing:
        _err(
            "S-YAML",
            "compliance section missing required children: "
            + ", ".join(missing),
        )
        return False
    _ok("S-YAML", "compliance has all required children")
    return True


# ---------------------------------------------------------------------------
# LOADERS — every loaders = {...} value has a matching declaration
# ---------------------------------------------------------------------------


def check_loaders_consistency() -> bool:
    if not INDEX_HTML.exists():
        _err("LOADERS", f"{INDEX_HTML} not found")
        return False
    html = INDEX_HTML.read_text(encoding="utf-8")
    # Find: const loaders = { key: ident, ... } — single-line per the
    # current shape. Regex matches the dict body lazily up to the
    # closing }.
    m = re.search(
        r"const\s+loaders\s*=\s*\{(?P<body>[^}]*)\}",
        html,
    )
    if not m:
        _err("LOADERS", "could not find `const loaders = { ... }`")
        return False
    body = m.group("body")
    # Each entry is `key: identifier` (key may be quoted). Extract the
    # identifier after the colon.
    pairs = re.findall(
        r"(?:'[^']+'|\"[^\"]+\"|\w+)\s*:\s*([A-Za-z_$][A-Za-z0-9_$]*)",
        body,
    )
    if not pairs:
        _err("LOADERS", "loaders dict appears empty")
        return False
    missing: list[str] = []
    for ident in pairs:
        decl = re.search(
            rf"\b(?:async\s+)?function\s+{re.escape(ident)}\s*\("
            rf"|\bconst\s+{re.escape(ident)}\s*="
            rf"|\blet\s+{re.escape(ident)}\s*="
            rf"|\bvar\s+{re.escape(ident)}\s*=",
            html,
        )
        if not decl:
            missing.append(ident)
    if missing:
        _err(
            "LOADERS",
            "loaders reference undeclared function(s): " + ", ".join(missing),
        )
        return False
    _ok("LOADERS", f"all {len(pairs)} loaders declarations resolve")
    return True


# ---------------------------------------------------------------------------
# HTML-BUDGET — innerHTML write-site budget
# ---------------------------------------------------------------------------


def check_innerhtml_budget() -> bool:
    if not INDEX_HTML.exists():
        _err("HTML-BUDGET", f"{INDEX_HTML} not found")
        return False
    html = INDEX_HTML.read_text(encoding="utf-8")
    # Count assignments to .innerHTML or [innerHTML] across the whole
    # file. Both stored-ref and direct-chain patterns hit this.
    count = len(re.findall(r"\.innerHTML\s*=", html))
    if count > INNERHTML_BUDGET:
        _err(
            "HTML-BUDGET",
            f"index.html has {count} innerHTML writes (budget={INNERHTML_BUDGET}). "
            "Migrate new writes to _setHtmlSafe / textContent, "
            "or raise INNERHTML_BUDGET in scripts/ci_guards.py with reviewer "
            "sign-off.",
        )
        return False
    _ok("HTML-BUDGET", f"{count}/{INNERHTML_BUDGET} innerHTML writes")
    return True


# ---------------------------------------------------------------------------
# D-CHAIN — direct getElementById/querySelector innerHTML chain pattern
# ---------------------------------------------------------------------------


# Matches `document.getElementById('foo').innerHTML =` and
# `document.querySelector('.bar').innerHTML =` — the chained shape
# that null-derefs when the element is missing. PR #200 / #201 / #202
# class. Baseline 0 after 2026-05-22 migration.
_DIRECT_CHAIN_PATTERN = re.compile(
    r"document\.(?:getElementById|querySelector)\([^)]+\)\.innerHTML\s*=",
)


def check_innerhtml_direct_chain() -> bool:
    if not INDEX_HTML.exists():
        _err("D-CHAIN", f"{INDEX_HTML} not found")
        return False
    html = INDEX_HTML.read_text(encoding="utf-8")
    offenders: list[tuple[int, str]] = []
    for m in _DIRECT_CHAIN_PATTERN.finditer(html):
        line_no = html.count("\n", 0, m.start()) + 1
        offenders.append((line_no, m.group(0)))
    if offenders:
        for line_no, snippet in offenders:
            _err(
                "D-CHAIN",
                f"index.html:{line_no} uses chained innerHTML "
                f"({snippet[:80]!r}). Replace with "
                f"_setHtmlSafe('id', html) — it null-checks the element "
                "and matches the established codebase pattern (#200/#202).",
            )
        return False
    _ok("D-CHAIN", "no chained document.getElementById(...).innerHTML = writes")
    return True


# ---------------------------------------------------------------------------
# SVC-PARITY — service-name agreement across deploy/*
# ---------------------------------------------------------------------------


# Match Windows service-management call sites where the service NAME
# argument follows immediately. We're looking for the literal
# 'FileActivityService' as a misnamed reference; anything matching
# SERVICE_NAME-prefixed identifiers (FileActivityDashboard, etc.) is fine.
_BAD_SERVICE_PATTERN = re.compile(
    r"\b(?:Stop-Service|Start-Service|Get-Service|net\s+(?:stop|start)|sc\s+query"
    r"|nssm\s+(?:stop|start|install|remove))\b[^\n]*?"
    r"\"?FileActivityService\b\"?",
    re.IGNORECASE,
)


def check_service_name_parity() -> bool:
    offenders: list[tuple[str, int, str]] = []
    for rel in SVC_PARITY_FILES:
        path = REPO_ROOT / rel
        if not path.exists():
            continue
        for i, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if _BAD_SERVICE_PATTERN.search(line):
                offenders.append((rel, i, line.strip()))
    if offenders:
        for rel, ln, line in offenders:
            _err(
                "SVC-PARITY",
                f"{rel}:{ln} uses 'FileActivityService' but the installed service is "
                f"named '{SERVICE_NAME}' (install_service.ps1). This is a silent "
                f"no-op at runtime: {line!r}",
            )
        return False
    _ok("SVC-PARITY", f"service name '{SERVICE_NAME}' consistent across deploy/*")
    return True


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# R-CACHE — direct analyzer_cache.get_or_compute outside the helper
# ---------------------------------------------------------------------------


# Endpoints permitted to call analyzer_cache.get_or_compute directly,
# with a justification. Add to this list ONLY with PR reviewer sign-off.
# The standard's Rule 1 says every other heavy-report endpoint goes
# through cached_report_endpoint(...).
R_CACHE_ALLOWLIST = {
    # mit_naming_files paginates an in-memory list off a per-(scan_id, code)
    # cached compute. The pagination split is incompatible with the
    # cached_report_endpoint signature (which expects a single dict, not
    # a list to slice). Tracked in #225 R-2 follow-up.
    "mit_naming_files",
    # report_full has an in-progress / partial response short-circuit
    # that must run BEFORE cache lookup. Migrating would require a more
    # invasive refactor — tracked in #225 R-2 follow-up.
    "report_full",
    # report_export returns a FileResponse (HTML file on disk), not a
    # JSON envelope. The cached_report_endpoint helper is JSON-shaped;
    # this endpoint just needs the cached dict to feed the HTML
    # exporter. Shares the "full" cache key with report_full so the
    # underlying compute runs at most once per scan_id.
    "report_export",
    # mit_naming_export returns a StreamingResponse (CSV body), not
    # JSON. The cached compute is the per-code violation dict; CSV
    # serialisation runs on the cached dict (cheap). Same pattern as
    # report_export — file response, not envelope.
    "mit_naming_export",
    # report_frequency has a fast-path that reads summary_json directly
    # (no cache call). Its slow path DOES go through
    # cached_report_endpoint after R-4. False-positive guard match —
    # the regex below doesn't see the indirect call, only the literal.
}


_API_PY = REPO_ROOT / "src" / "dashboard" / "api.py"
_HELPERS_PY = REPO_ROOT / "src" / "dashboard" / "_endpoint_helpers.py"


def check_r_cache() -> bool:
    """Flag direct analyzer_cache.get_or_compute() calls in api.py.

    These should go through cached_report_endpoint(...) from
    _endpoint_helpers.py unless explicitly allowlisted with a
    justification.
    """
    import ast

    if not _API_PY.exists():
        _err("R-CACHE", f"{_API_PY} not found")
        return False
    src = _API_PY.read_text(encoding="utf-8")
    try:
        tree = ast.parse(src)
    except SyntaxError as e:
        _err("R-CACHE", f"api.py syntax error: {e}")
        return False

    # Find every function whose OWN body (not nested defs) contains a
    # call to ``analyzer_cache.get_or_compute(...)``.
    class _CallScanner(ast.NodeVisitor):
        """Visits the body of one function — stops at nested defs."""

        def __init__(self) -> None:
            self.found = False

        def visit_Call(self, node: ast.Call) -> None:
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr == "get_or_compute":
                self.found = True
                return
            self.generic_visit(node)

        def visit_FunctionDef(self, node) -> None:
            # Don't descend — nested def is checked separately.
            pass

        def visit_AsyncFunctionDef(self, node) -> None:
            pass

    def _own_body_has_get_or_compute(fn) -> bool:
        s = _CallScanner()
        for stmt in fn.body:
            s.visit(stmt)
            if s.found:
                return True
        return False

    offenders: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if _own_body_has_get_or_compute(node):
                offenders.append((node.name, node.lineno))
    bad = [(n, ln) for n, ln in offenders if n not in R_CACHE_ALLOWLIST]
    if bad:
        for name, ln in bad:
            _err(
                "R-CACHE",
                f"api.py:{ln} function {name!r} calls analyzer_cache.get_or_compute "
                "directly. Route through cached_report_endpoint() from "
                "src/dashboard/_endpoint_helpers.py, or add to R_CACHE_ALLOWLIST "
                "with a justification (Rule 1 of endpoint-conventions.md).",
            )
        return False
    _ok(
        "R-CACHE",
        f"all direct analyzer_cache calls go through helper "
        f"(allowlisted: {len(R_CACHE_ALLOWLIST)})",
    )
    return True


# ---------------------------------------------------------------------------
# A-AWAIT — async def endpoint must use await
# ---------------------------------------------------------------------------


# Endpoints that legitimately need `async def` for the middleware /
# request-body await contract. These are checked in PR #215's audit and
# stay async. Anything else added as `async def` without an await should
# either gain an await or be converted to plain def.
A_AWAIT_ALLOWLIST: set[str] = set()


def check_a_await() -> bool:
    """Flag async def route handlers that don't await anything.

    Per Rule 5: an async def whose body contains no await/async-for/
    async-with is a perf bug — it blocks the FastAPI event loop on any
    sync work it does. Convert to plain def so the threadpool dispatches
    it. Prevents recurrence of PR #215.
    """
    import ast

    if not _API_PY.exists():
        _err("A-AWAIT", f"{_API_PY} not found")
        return False
    src = _API_PY.read_text(encoding="utf-8")
    try:
        tree = ast.parse(src)
    except SyntaxError as e:
        _err("A-AWAIT", f"api.py syntax error: {e}")
        return False

    class HasAwaitInOwnBody(ast.NodeVisitor):
        def __init__(self) -> None:
            self.found = False

        def visit_Await(self, node: ast.Await) -> None:
            self.found = True

        def visit_AsyncFor(self, node) -> None:
            self.found = True

        def visit_AsyncWith(self, node) -> None:
            self.found = True

        def visit_FunctionDef(self, node) -> None:
            pass  # don't descend into nested defs

        def visit_AsyncFunctionDef(self, node) -> None:
            pass

    def body_awaits(fn) -> bool:
        h = HasAwaitInOwnBody()
        for stmt in fn.body:
            h.visit(stmt)
            if h.found:
                return True
        return False

    # Check every AsyncFunctionDef in the file.
    offenders: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef):
            if body_awaits(node):
                continue
            if node.name in A_AWAIT_ALLOWLIST:
                continue
            offenders.append((node.name, node.lineno))

    if offenders:
        for name, ln in offenders:
            _err(
                "A-AWAIT",
                f"api.py:{ln} async def {name!r} contains no await. "
                "Convert to plain `def` so FastAPI dispatches to the "
                "thread pool (Rule 5 of endpoint-conventions.md; "
                "prevents PR #215 regression).",
            )
        return False
    _ok("A-AWAIT", "all async def endpoints actually use await")
    return True


# ---------------------------------------------------------------------------
# C-CURSOR — Rule 6 of endpoint-conventions.md
# ---------------------------------------------------------------------------


# GET handlers that legitimately use the writer pool (``get_cursor()``)
# because the same path may UPDATE under a fallback. Each entry must
# have a justification comment in api.py at the use-site. Adding a new
# entry requires reviewer sign-off.
C_CURSOR_GET_WRITER_ALLOWLIST = {
    # api.py:879 (dashboard_init) — fixes scan_runs totals via UPDATE
    # when total_files=0 fallback hits (Issue #181 Track A).
    "dashboard_init",
}

# Write handlers (POST/DELETE/PUT/PATCH) that legitimately use the
# read-only pool. These read metadata then delegate the actual write
# to a separate engine (ArchiveEngine, ACL analyzer, background export
# job) which holds its own connection. So the endpoint itself never
# writes through ``get_cursor()``; the work happens elsewhere.
C_CURSOR_WRITE_READER_ALLOWLIST: set[str] = {
    # POST /api/archive/selective — reads file_ids, delegates write to
    # ArchiveEngine.archive_files()
    "archive_selective",
    # POST /api/archive/bulk-from-list — same shape: read paths,
    # delegate to ArchiveEngine
    "archive_bulk_from_list",
    # POST /api/archive/by-insight — reads insight + scan info,
    # delegates to ArchiveEngine
    "archive_by_insight",
    # POST /api/export/start — reads scan_id then kicks off a
    # background export job
    "start_export",
    # POST /api/security/acl/scan/{source_id} — reads scan_id,
    # delegates to ACL analyzer which manages its own DB write path
    "acl_snapshot",
}


_HTTP_WRITE_METHODS = {"post", "delete", "put", "patch"}
_HTTP_READ_METHODS = {"get"}


def _decorator_http_method(deco) -> str | None:
    """Return the http method (lowercase) for a @app.<method>(...) call,
    or None if this isn't a route decorator."""
    import ast as _ast
    if not isinstance(deco, _ast.Call):
        return None
    fn = deco.func
    if not isinstance(fn, _ast.Attribute):
        return None
    if not (isinstance(fn.value, _ast.Name) and fn.value.id == "app"):
        return None
    return fn.attr.lower() if isinstance(fn.attr, str) else None


def check_c_cursor() -> bool:
    """Flag mixed read/write cursor usage in api.py endpoints.

    Rule 6: GET endpoints use ``get_read_cursor()``; POST/DELETE/PUT/
    PATCH use ``get_cursor()`` (writer pool). Mixing surfaces in the
    audit history as the WAL leak class — long-lived writer
    connections held by read paths block ``wal_checkpoint(TRUNCATE)``
    and the WAL grows unbounded (#132 / #174 / #181 / #185, same root
    cause hit 4 times).
    """
    import ast

    if not _API_PY.exists():
        _err("C-CURSOR", f"{_API_PY} not found")
        return False
    src = _API_PY.read_text(encoding="utf-8")
    try:
        tree = ast.parse(src)
    except SyntaxError as e:
        _err("C-CURSOR", f"api.py syntax error: {e}")
        return False

    class _CursorScanner(ast.NodeVisitor):
        def __init__(self) -> None:
            self.uses_writer = False
            self.uses_reader = False

        def visit_Call(self, node: ast.Call) -> None:
            func = node.func
            if isinstance(func, ast.Attribute):
                if func.attr == "get_cursor":
                    self.uses_writer = True
                elif func.attr == "get_read_cursor":
                    self.uses_reader = True
            self.generic_visit(node)

        def visit_FunctionDef(self, node) -> None:
            pass  # don't descend into nested defs

        def visit_AsyncFunctionDef(self, node) -> None:
            pass

    def _own_body_cursors(fn) -> tuple[bool, bool]:
        s = _CursorScanner()
        for stmt in fn.body:
            s.visit(stmt)
        return s.uses_writer, s.uses_reader

    offenders: list[tuple[str, int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        # Find the http method from the closest @app.<method>(...) decorator.
        http_method = None
        for deco in node.decorator_list:
            m = _decorator_http_method(deco)
            if m in _HTTP_READ_METHODS or m in _HTTP_WRITE_METHODS:
                http_method = m
                break
        if http_method is None:
            continue  # not a route handler

        uses_writer, uses_reader = _own_body_cursors(node)
        # GET using writer → flag unless allowlisted
        if http_method in _HTTP_READ_METHODS and uses_writer:
            if node.name in C_CURSOR_GET_WRITER_ALLOWLIST:
                continue
            offenders.append((
                node.name, node.lineno,
                "GET handler uses get_cursor() (writer pool). Use "
                "get_read_cursor() instead, or add to "
                "C_CURSOR_GET_WRITER_ALLOWLIST with a justification.",
            ))
        # POST/DELETE/PUT/PATCH using reader-only → flag unless allowlisted
        if http_method in _HTTP_WRITE_METHODS and uses_reader and not uses_writer:
            if node.name in C_CURSOR_WRITE_READER_ALLOWLIST:
                continue
            offenders.append((
                node.name, node.lineno,
                f"{http_method.upper()} handler uses ONLY get_read_cursor() — "
                "the read-only pool can't write. If this endpoint actually "
                "writes, switch to get_cursor(). If it's read-only despite "
                "being POST, add to C_CURSOR_WRITE_READER_ALLOWLIST.",
            ))

    if offenders:
        for name, ln, msg in offenders:
            _err("C-CURSOR", f"api.py:{ln} {name!r}: {msg}")
        return False
    _ok(
        "C-CURSOR",
        f"all read/write cursor usage matches HTTP method "
        f"(GET-writer allowlist: {len(C_CURSOR_GET_WRITER_ALLOWLIST)}, "
        f"write-reader allowlist: {len(C_CURSOR_WRITE_READER_ALLOWLIST)})",
    )
    return True


# ---------------------------------------------------------------------------
# P-PAGE — paginated endpoints use PaginationParams (Depends), Rule 2
# ---------------------------------------------------------------------------


# Route handlers grandfathered with hand-rolled pagination as of EPIC #225
# R-5c. Each name maps to api.py line number for traceability. Migrate to
# ``p: PaginationParams = Depends()`` opportunistically; new endpoints
# must NOT be added here without reviewer sign-off and a follow-up issue.
P_PAGE_ALLOWLIST: set[str] = {
    # api.py line numbers as of master 62fc1f2 / EPIC #225 R-5c introduction.
    "archive_search",                  # 1596
    "drilldown_frequency",             # 1814
    "drilldown_type",                  # 1830
    "drilldown_size",                  # 1845
    "drilldown_owner",                 # 1861
    "audit_events",                    # 2389
    "audit_chain",                     # 2411
    "mit_naming_files",                # 2524
    "insight_files",                   # 2826
    "duplicate_report",                # 3246
    "content_duplicates_report",       # 3339
    "text_near_dup_report",            # 3428
    "quarantine_list",                 # 3585
    "operations_history",              # 3733
    "top_creators",                    # 4173
    "get_operations",                  # 4299
    "browse_archived",                 # 4411
    "archive_history",                 # 4424
    "operation_files",                 # 4438
    "notifications_log",               # 4674
    "acl_paths_for_trustee",           # 5415
    "orphan_sid_files",                # 5491
    "acl_trustee_paths_export_xlsx",   # 5931
    "list_extension_anomalies",        # 5962
    "pii_findings",                    # 6430
    "legal_holds_history",             # 6847
    "approvals_history",               # 7045
}

PAGINATION_PARAM_NAMES = frozenset({"page", "page_size", "limit", "offset"})


def check_p_page() -> bool:
    """Flag route handlers with hand-rolled pagination params.

    Per Rule 2: any endpoint that accepts ``page`` / ``page_size`` /
    ``limit`` / ``offset`` must take them via
    ``p: PaginationParams = Depends()`` from
    ``src/dashboard/_endpoint_helpers.py`` so the (page, limit) /
    (page, page_size) / (offset, limit) drift documented in the
    2026-05-22 audit cannot recur. Grandfathered offenders stay in
    P_PAGE_ALLOWLIST until they migrate.
    """
    import ast

    if not _API_PY.exists():
        _err("P-PAGE", f"{_API_PY} not found")
        return False
    src = _API_PY.read_text(encoding="utf-8")
    try:
        tree = ast.parse(src)
    except SyntaxError as e:
        _err("P-PAGE", f"api.py syntax error: {e}")
        return False

    def is_route_handler(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        """True iff fn has an @app.<method>(...) HTTP-verb decorator."""
        for dec in fn.decorator_list:
            target = dec.func if isinstance(dec, ast.Call) else dec
            if isinstance(target, ast.Attribute) and target.attr in {
                "get", "post", "put", "delete", "patch",
            }:
                # Only flag @app.<method>; ignore @<router>.<method> dialects
                # unless they're literally named `app`. The repo's FastAPI app
                # is bound to `app`.
                if isinstance(target.value, ast.Name) and target.value.id == "app":
                    return True
        return False

    def args_iter(fn) -> list[ast.arg]:
        return list(fn.args.args) + list(fn.args.kwonlyargs) + list(fn.args.posonlyargs)

    def ann_is_pagination_params(ann) -> bool:
        """True iff the annotation resolves to PaginationParams.

        Handles every spelling in active use or recommended by FastAPI:
        bare ``PaginationParams``, dotted ``helpers.PaginationParams``,
        ``Optional[PaginationParams]`` (Subscript→Name), PEP 593
        ``Annotated[PaginationParams, Depends()]`` (Subscript→Tuple),
        and PEP 604 ``PaginationParams | None`` (BinOp) — recursively,
        so nested wrappers compose.
        """
        if ann is None:
            return False
        if isinstance(ann, ast.Name):
            return ann.id == "PaginationParams"
        if isinstance(ann, ast.Attribute):
            return ann.attr == "PaginationParams"
        if isinstance(ann, ast.Subscript):
            inner = ann.slice
            if isinstance(inner, ast.Tuple):
                return any(ann_is_pagination_params(el) for el in inner.elts)
            return ann_is_pagination_params(inner)
        if isinstance(ann, ast.BinOp):
            return (ann_is_pagination_params(ann.left)
                    or ann_is_pagination_params(ann.right))
        return False

    # Per-ARG check (not per-function): a pagination-named arg is fine
    # only when ITS OWN annotation is the helper. This also catches the
    # partial-migration trap — `p: PaginationParams = Depends()` added
    # while a legacy `page: int = 1` lingers in the signature would have
    # FastAPI bind both from the query string, recreating the drift.
    offenders: list[tuple[str, int, list[str]]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not is_route_handler(node):
            continue
        hand_rolled = [
            a.arg for a in args_iter(node)
            if a.arg in PAGINATION_PARAM_NAMES
            and not ann_is_pagination_params(a.annotation)
        ]
        if not hand_rolled:
            continue
        if node.name in P_PAGE_ALLOWLIST:
            continue
        offenders.append((node.name, node.lineno, hand_rolled))

    if offenders:
        for name, ln, params in offenders:
            _err(
                "P-PAGE",
                f"api.py:{ln} route handler {name!r} has hand-rolled "
                f"pagination param(s) {params}. Use "
                "`p: PaginationParams = Depends()` from "
                "src/dashboard/_endpoint_helpers.py (Rule 2) and remove "
                "the legacy query params. To grandfather, add "
                f"{name!r} to P_PAGE_ALLOWLIST in scripts/ci_guards.py "
                "with a follow-up issue reference.",
            )
        return False
    _ok(
        "P-PAGE",
        f"all paginated route handlers use PaginationParams "
        f"(allowlisted: {len(P_PAGE_ALLOWLIST)})",
    )
    return True


# ---------------------------------------------------------------------------
# A-AUDIT — POST/PUT/DELETE/PATCH route handlers emit an audit event, Rule 4
# ---------------------------------------------------------------------------


# Mutating route handlers grandfathered without an audit emission as of EPIC
# #225 R-5e introduction. R-6 (audit-backlog flush) triages each: add the
# emission OR keep allowlisted with a justification comment. New endpoints
# must NOT be added here without reviewer sign-off and an R-6-style follow-up.
A_AUDIT_ALLOWLIST: set[str] = {
    # api.py line numbers as of master c5cf885 / EPIC #225 R-5e introduction.
    # R-6 (audit-backlog flush) will triage each: add insert_audit_event_simple
    # OR keep allowlisted with a per-name justification comment. Analytics-
    # compute and self-test POSTs are the typical justified exemptions.
    "approvals_approve",               # 7251 — audited by ApprovalRegistry._audit (approval_approved)
    "approvals_execute",               # 7324 — audited by ApprovalRegistry._audit (approval_executed)
    "approvals_reject",                # 7294 — audited by ApprovalRegistry._audit (approval_rejected)
    "archive_dry_run",                 # 1574
    "audit_export",                    # 2417 — export-only, no mutation
    "content_duplicates_compute",      # 3318 — analytics compute
    "create_snapshot",                 # 6216
    "db_optimize",                     # 4969 — maintenance op
    "duplicates_delete",               # 3529
    "duplicates_quarantine",           # 3484
    "duplicates_quarantine_preview",   # 3468 — dry-run
    "insights_recompute",              # 2471 — analytics compute
    "notifications_send_to",           # 4641
    "notifications_test",              # 4605 — self-test
    "notify_users_run_now",            # 1742
    "open_folder",                     # 4479 — local-only helper, no DB write
    "overview_recompute",              # 2919 — analytics compute
    "pii_scan",                        # 6410 — analytics compute
    "ransomware_test",                 # 5321 — self-test
    "start_export",                    # 5187 — export-only
    "syslog_test",                     # 6080 — self-test
    "test_source",                     # 987 — connectivity test
    "text_near_dup_compute",           # 3396 — analytics compute
    # Wave 10 #307 — auth session endpoints (no server-side data mutation)
    "auth_refresh",                     # stateless JWT refresh — no DB write
    "auth_me",                          # read-only identity probe
    # auth_logout removed from allowlist (#317): it now mutates (bumps
    # token_version) and emits a user_logout audit event, so A-AUDIT passes it.
}

# Same canonical fact as C-CURSOR's _HTTP_WRITE_METHODS — alias, don't fork,
# so a future verb policy change lands in exactly one place.
MUTATING_HTTP_VERBS = frozenset(_HTTP_WRITE_METHODS)
AUDIT_FUNC_NAMES = frozenset({"insert_audit_event_simple", "insert_audit_event"})


def check_a_audit() -> bool:
    """Flag mutating route handlers that don't call insert_audit_event_simple.

    Per Rule 4: every POST/PUT/DELETE/PATCH endpoint records what changed
    via ``db.insert_audit_event_simple(...)`` so the tamper-evident audit
    chain (#38) has a row for every server-side mutation. Compute-only
    "analytics" POSTs are the usual exemption; they sit in the allowlist
    with a justification.
    """
    import ast

    if not _API_PY.exists():
        _err("A-AUDIT", f"{_API_PY} not found")
        return False
    src = _API_PY.read_text(encoding="utf-8")
    try:
        tree = ast.parse(src)
    except SyntaxError as e:
        _err("A-AUDIT", f"api.py syntax error: {e}")
        return False

    def is_mutating_handler(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        """True iff fn has an @app.<post|put|delete|patch>(...) decorator
        whose target is the `app` binding (the FastAPI app)."""
        for dec in fn.decorator_list:
            target = dec.func if isinstance(dec, ast.Call) else dec
            if isinstance(target, ast.Attribute) and target.attr in MUTATING_HTTP_VERBS:
                if isinstance(target.value, ast.Name) and target.value.id == "app":
                    return True
        return False

    def calls_audit(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        """True iff fn's OWN body calls an audit-emission function.

        Deliberately does NOT descend into nested function definitions —
        same own-body semantics as A-AWAIT and C-CURSOR. An audit call
        sitting inside a nested helper proves nothing: the helper may
        never be invoked (dead code), so only a call reachable in the
        handler's own statement tree counts. Branches (if/try/with/for)
        DO count — only nested def/async-def boundaries stop the scan.
        """
        class AuditFinder(ast.NodeVisitor):
            def __init__(self) -> None:
                self.found = False

            def visit_Call(self, node: ast.Call) -> None:
                func = node.func
                if isinstance(func, ast.Attribute) and func.attr in AUDIT_FUNC_NAMES:
                    self.found = True
                elif isinstance(func, ast.Name) and func.id in AUDIT_FUNC_NAMES:
                    self.found = True
                self.generic_visit(node)

            def visit_FunctionDef(self, node) -> None:
                pass  # don't descend into nested defs

            def visit_AsyncFunctionDef(self, node) -> None:
                pass

        finder = AuditFinder()
        for stmt in fn.body:
            finder.visit(stmt)
            if finder.found:
                return True
        return False

    offenders: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not is_mutating_handler(node):
            continue
        if calls_audit(node):
            continue
        if node.name in A_AUDIT_ALLOWLIST:
            continue
        offenders.append((node.name, node.lineno))

    if offenders:
        for name, ln in offenders:
            _err(
                "A-AUDIT",
                f"api.py:{ln} mutating handler {name!r} does not call "
                "db.insert_audit_event_simple(...). Per Rule 4, every "
                "POST/PUT/DELETE/PATCH must emit an audit event. Add the "
                f"emission, or add {name!r} to A_AUDIT_ALLOWLIST in "
                "scripts/ci_guards.py with a justification (compute-only "
                "analytics POSTs are the common exemption).",
            )
        return False
    _ok(
        "A-AUDIT",
        f"all mutating route handlers emit audit events "
        f"(allowlisted: {len(A_AUDIT_ALLOWLIST)})",
    )
    return True


# ---------------------------------------------------------------------------
# S-SHAPE — readers go through db.get_scan_summary (Rule 3), no raw bypass
# ---------------------------------------------------------------------------


# Line-level regex patterns flagging api.py code that reaches into the
# raw ``summary_json`` column directly — the bug shape behind PR #198 /
# #223. The canonical reader is ``db.get_scan_summary(scan_id)`` which
# routes through ``src/storage/_summary_compat.normalize_summary`` so
# every consumer sees the canonical list-shape of ``age_buckets`` /
# ``size_buckets`` regardless of which writer produced the row.
_S_SHAPE_PATTERNS = (
    r'\[\s*[\"\']summary_json[\"\']\s*\]',
    r'\.get\(\s*[\"\']summary_json[\"\']',
    r'json\.loads\([^)]*summary_json',
    r'\[\s*[\"\']partial_summary_json[\"\']\s*\]',
    # The .get form was missing at R-5d ship time — api.py:2998/3065 used
    # exactly this shape and slipped through (2026-06-04 review repro).
    r'\.get\(\s*[\"\']partial_summary_json[\"\']',
    r'json\.loads\([^)]*partial_summary_json',
)


def check_s_shape() -> bool:
    """Flag raw ``summary_json`` / ``partial_summary_json`` access in api.py.

    Rule 3: read the canonical shape via ``db.get_scan_summary(...)``.
    Direct ``row["summary_json"]`` or ``json.loads(row["summary_json"])``
    bypasses ``normalize_summary`` (PR #198/#223 bug class). To override
    locally for a documented exemption, append ``# noqa: S-SHAPE`` to the
    line; the guard skips noqa'd lines.
    """
    import re

    if not _API_PY.exists():
        _err("S-SHAPE", f"{_API_PY} not found")
        return False
    src = _API_PY.read_text(encoding="utf-8")
    patterns = [re.compile(p) for p in _S_SHAPE_PATTERNS]

    offenders: list[tuple[int, str]] = []
    for i, line in enumerate(src.splitlines(), start=1):
        # Comment-only lines (e.g. docstrings, # ... notes) are not code.
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        # noqa must live in a trailing comment (not a string literal) and
        # is case-insensitive, matching the flake8/ruff convention.
        hash_idx = line.find("#")
        if hash_idx != -1 and "noqa: s-shape" in line[hash_idx:].lower():
            continue
        for pat in patterns:
            if pat.search(line):
                offenders.append((i, line.strip()))
                break

    if offenders:
        for ln, text in offenders:
            _err(
                "S-SHAPE",
                f"api.py:{ln} raw summary_json access bypasses "
                "normalize_summary. Use db.get_scan_summary(scan_id) "
                "(Rule 3). Append `# noqa: S-SHAPE` to override with a "
                f"documented exemption. (line: {text!r})",
            )
        return False
    _ok("S-SHAPE", "no raw summary_json access in api.py")
    return True


CHECKS = {
    "yaml-dup": check_yaml_duplicates,
    "yaml-schema": check_yaml_schema,
    "loaders": check_loaders_consistency,
    "html-budget": check_innerhtml_budget,
    "html-chain": check_innerhtml_direct_chain,
    "svc-parity": check_service_name_parity,
    "r-cache": check_r_cache,
    "a-await": check_a_await,
    "c-cursor": check_c_cursor,
    "p-page": check_p_page,
    "a-audit": check_a_audit,
    "s-shape": check_s_shape,
}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--check",
        choices=list(CHECKS.keys()) + ["all"],
        default="all",
        help="Run a single check or all (default).",
    )
    args = ap.parse_args(argv)
    if args.check == "all":
        results = [fn() for fn in CHECKS.values()]
    else:
        results = [CHECKS[args.check]()]
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
