"""Audit L-1: assert the f-string SQL/DuckDB call *sites* flagged as
"config-derived only" are not reachable from any FastAPI request
handler.

The audit lists these sites:

* ``src/storage/backup_manager.py:~270`` — ``VACUUM INTO`` literal
* ``src/storage/analytics.py:67-68 81-82`` — DuckDB ``SET`` + ``ATTACH``
* ``src/playground/data_access.py:119-120 130-131`` — DuckDB ``SET`` + ``ATTACH``

The test below is intentionally a static, grep-shaped reachability
proxy — full call-graph analysis would over-fit, while this catches
the practical regression (a new request handler that wires user input
into one of these modules' f-string code paths).

We only care about *module-level* imports: a request-handler module
that imports a protected module at the top level means the protected
module is loaded for free on every request. Function-local imports
(the dashboard's pattern, ``from src.storage.backup_manager import
BackupManager`` inside ``_get_backup_manager``) are deliberately
exempt — they are intentional, lazy, and inspected here separately
via the ``CODEQL-SAFE`` marker on each f-string call site.

When this test breaks, *don't* just allow-list the new caller — read
the audit (``docs/architecture/security-audit-2026-04-28.md``, L-1)
first. If user-controlled data really has reached one of these
sites, the f-string must be rewritten to use parameter binding.
"""

from __future__ import annotations

import ast
import os
from pathlib import Path
from typing import Set

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"

# Files (module dotted paths) that hold the audited f-string SQL sites.
# ``src.storage.analytics`` was removed in #194 D2 (DuckDB engine
# eliminated; aggregates moved to plain SQLite).
PROTECTED_MODULES = {
    "src.storage.backup_manager",
    "src.playground.data_access",
}


# Markers we accept as "this module hosts request handlers".
REQUEST_HANDLER_MARKERS = (
    "@app.get",
    "@app.post",
    "@app.put",
    "@app.delete",
    "@app.patch",
    "@router.get",
    "@router.post",
    "@router.put",
    "@router.delete",
    "@router.patch",
)


def _module_name(py_path: Path) -> str:
    rel = py_path.relative_to(REPO_ROOT).with_suffix("")
    return ".".join(rel.parts)


def _is_request_handler_module(py_path: Path) -> bool:
    try:
        text = py_path.read_text(encoding="utf-8")
    except OSError:
        return False
    return any(marker in text for marker in REQUEST_HANDLER_MARKERS)


def _module_level_imports(py_path: Path) -> Set[str]:
    """Return dotted module names imported at *module level* only.

    Walks the AST and looks at top-level Import / ImportFrom nodes,
    skipping anything nested inside a function or class definition.
    """
    try:
        text = py_path.read_text(encoding="utf-8")
    except OSError:
        return set()
    try:
        tree = ast.parse(text, filename=str(py_path))
    except SyntaxError:
        return set()
    out: Set[str] = set()
    for node in tree.body:  # only direct module-level statements
        if isinstance(node, ast.Import):
            for alias in node.names:
                out.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                out.add(node.module)
    return out


def _all_python_files() -> list[Path]:
    return [p for p in SRC.rglob("*.py") if "__pycache__" not in p.parts]


def _module_to_path(module: str) -> Path:
    rel = module.replace(".", os.sep) + ".py"
    return REPO_ROOT / rel


def _import_closure_module_level(start_modules: Set[str]) -> Set[str]:
    """BFS through *module-level* imports, restricted to ``src.*``."""
    seen: Set[str] = set()
    stack = list(start_modules)
    while stack:
        mod = stack.pop()
        if mod in seen:
            continue
        seen.add(mod)
        path = _module_to_path(mod)
        if not path.exists():
            continue
        for imp in _module_level_imports(path):
            if imp.startswith("src.") and imp not in seen:
                stack.append(imp)
    return seen


def test_protected_modules_not_reachable_from_request_handlers():
    """No request-handler module under ``src/`` may import (at module
    level, transitively through other module-level imports) any of the
    modules with config-only f-string SQL.

    Function-local imports (the dashboard's pattern) are deliberately
    NOT counted — the comment ``CODEQL-SAFE: value is config-derived``
    on each call site documents the manual review.
    """
    request_handler_files = [
        p for p in _all_python_files() if _is_request_handler_module(p)
    ]
    assert request_handler_files, (
        "test scaffolding broken: expected at least one request-handler "
        "module under src/"
    )

    violations: list[tuple[str, str]] = []
    for handler_path in request_handler_files:
        handler_mod = _module_name(handler_path)
        closure = _import_closure_module_level({handler_mod})
        bad = closure & PROTECTED_MODULES
        for hit in bad:
            violations.append((handler_mod, hit))

    assert not violations, (
        "audit L-1 regression: request-handler modules now import "
        "config-only-SQL modules at module level:\n  "
        + "\n  ".join(f"{h} -> {p}" for h, p in violations)
        + "\nLazy imports (inside a function body) are fine; review "
        "docs/architecture/security-audit-2026-04-28.md before "
        "allow-listing a top-level edge."
    )


def test_protected_files_carry_codeql_safe_marker():
    """Belt-and-braces: every protected module must contain the
    ``CODEQL-SAFE`` marker comment that documents *why* the f-string
    is acceptable. If someone deletes the comment we want to know."""
    expected_marker = "CODEQL-SAFE: value is config-derived"
    for mod in PROTECTED_MODULES:
        path = _module_to_path(mod)
        text = path.read_text(encoding="utf-8")
        assert expected_marker in text, (
            f"audit L-1: {mod} is missing the CODEQL-SAFE marker "
            "that documents the justification for its f-string SQL"
        )
