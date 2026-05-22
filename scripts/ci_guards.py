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
