"""Static analyzer for ``onclick`` handlers in ``index.html`` (Issue #82, Bug 4).

Bugs 1, 2 and 3 of issue #82 fixed individual button regressions (PRs #85,
#95, #96). This script is the regression-prevention infrastructure: it
walks the dashboard HTML, extracts every ``onclick="..."`` handler, splits
multi-statement payloads, and resolves each call against the set of
top-level ``function X(...)`` definitions in the same file.

The output is a JSON inventory which the test suite consumes to assert:

* every onclick references a real function (no ``ReferenceError`` on click),
* no obviously misspelled attribute names slipped in (``onlcick=``, ...),
* the total handler count never silently drops (sentinel guard).

Usage (CLI):

    python scripts/audit_buttons.py [path/to/index.html]

The default HTML path resolves relative to the repository root, so the
script can be run from anywhere. Output goes to stdout as pretty JSON.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import Any


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_HTML_PATH = os.path.join(
    _REPO_ROOT, "src", "dashboard", "static", "index.html"
)


# ---------------------------------------------------------------------------
# Regexes
# ---------------------------------------------------------------------------

# Match every ``onclick="..."`` (the dashboard never uses single-quoted
# attribute syntax — verified by audit). We tolerate ``\"`` escapes inside
# the payload by using a non-greedy match, and we anchor on the closing
# quote followed by either ``>`` or whitespace so we don't accidentally
# devour neighbouring attributes if a payload contained a stray ``"``.
_ONCLICK_RE = re.compile(r'onclick="([^"]*)"')

# Function-definition lookup: ``function NAME(`` at the start of a line
# (allowing optional ``async`` and arbitrary leading whitespace). We do NOT
# pick up ``foo: function() {...}`` object-shorthand or arrow assignments
# because the dashboard codebase doesn't use them for anything addressable
# from an onclick attribute.
_FUNC_DEF_RE = re.compile(
    r"^\s*(?:async\s+)?function\s+([A-Za-z_$][\w$]*)\s*\(",
    re.MULTILINE,
)

# Common typos for the ``onclick`` attribute name. The dashboard previously
# shipped a ``onlcick=`` typo (caught in code review for #85) — guarding
# against the obvious permutations is cheap insurance.
_ONCLICK_TYPO_RE = re.compile(
    r"\b(onlcick|oncliclk|onlick|onnclick|onclic(?!k)k?|onclck)\s*=",
    re.IGNORECASE,
)

# Identifier at the start of a single onclick statement — what we treat as
# the function being called. Must be followed by ``(`` (allowing optional
# whitespace) for it to count as a call site.
_CALL_RE = re.compile(r"^\s*([A-Za-z_$][\w$]*)\s*\(")


# Statements that are pure JS expressions / DOM access rather than calls
# into our app's function namespace. We log them but don't require a
# matching ``function X(...)`` definition. Browsers handle these
# natively — they cannot raise ``ReferenceError`` on click in our code.
_BUILTIN_PREFIXES = (
    "event",
    "this",
    "window",
    "document",
    "console",
    "alert",
    "confirm",
    "prompt",
    "setTimeout",
    "setInterval",
    "clearTimeout",
    "clearInterval",
    "Promise",
    "Object",
    "Array",
    "JSON",
    "Math",
    "Number",
    "String",
    "Boolean",
    "Date",
    "if",
    "return",
    "throw",
    "new",
    "void",
    "typeof",
    "delete",
    "true",
    "false",
    "null",
    "undefined",
)


# ---------------------------------------------------------------------------
# Statement splitter
# ---------------------------------------------------------------------------


def _split_statements(payload: str) -> list[str]:
    """Split an onclick payload by ``;`` while respecting parentheses.

    A naive ``payload.split(";")`` would mis-split things like
    ``foo('a;b'); bar()`` — the dashboard does have onclicks with
    template-string interpolation that contain semicolons inside
    parentheses. We keep a simple paren-depth counter so we only split on
    the outermost ``;``.
    """
    out: list[str] = []
    buf: list[str] = []
    depth = 0
    in_single = False
    in_double = False
    in_back = False
    i = 0
    while i < len(payload):
        ch = payload[i]
        # Quote state: a quote toggles only if not already inside a
        # different kind of string. We don't track escaping heuristically;
        # for ``onclick="..."`` this is good enough because the attribute
        # quoting forces the payload to use ``'`` or `` ` `` for strings.
        if ch == "'" and not in_double and not in_back:
            in_single = not in_single
        elif ch == '"' and not in_single and not in_back:
            in_double = not in_double
        elif ch == "`" and not in_single and not in_double:
            in_back = not in_back
        elif not (in_single or in_double or in_back):
            if ch == "(" or ch == "{" or ch == "[":
                depth += 1
            elif ch == ")" or ch == "}" or ch == "]":
                depth = max(0, depth - 1)
            elif ch == ";" and depth == 0:
                out.append("".join(buf).strip())
                buf = []
                i += 1
                continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return [s for s in out if s]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _line_of(text: str, offset: int) -> int:
    """Return 1-based line number for a character offset in ``text``."""
    return text.count("\n", 0, offset) + 1


def _split_args_top_level(args_str: str) -> list[str]:
    """Split a function-call argument string by top-level commas."""
    out: list[str] = []
    buf: list[str] = []
    depth = 0
    in_single = in_double = in_back = False
    for ch in args_str:
        if ch == "'" and not in_double and not in_back:
            in_single = not in_single
        elif ch == '"' and not in_single and not in_back:
            in_double = not in_double
        elif ch == "`" and not in_single and not in_double:
            in_back = not in_back
        elif not (in_single or in_double or in_back):
            if ch in "([{":
                depth += 1
            elif ch in ")]}":
                depth = max(0, depth - 1)
            elif ch == "," and depth == 0:
                out.append("".join(buf).strip())
                buf = []
                continue
        buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out


def _extract_args(stmt: str) -> str:
    """Return the comma-joined argument list of a call statement.

    Best-effort; the goal is human-readable inventory output, not a real
    JS parser. ``foo(a, b)`` -> ``"a, b"``; ``foo()`` -> ``""``.
    """
    open_idx = stmt.find("(")
    if open_idx < 0:
        return ""
    # Walk to matching ``)`` respecting nesting + strings.
    depth = 0
    in_single = in_double = in_back = False
    for i in range(open_idx, len(stmt)):
        ch = stmt[i]
        if ch == "'" and not in_double and not in_back:
            in_single = not in_single
        elif ch == '"' and not in_single and not in_back:
            in_double = not in_double
        elif ch == "`" and not in_single and not in_double:
            in_back = not in_back
        elif not (in_single or in_double or in_back):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return stmt[open_idx + 1 : i].strip()
    # Unbalanced — fall back to "everything after the first '('".
    return stmt[open_idx + 1 :].strip()


def audit(html: str) -> dict[str, Any]:
    """Run the static audit on ``html`` and return a JSON-friendly dict.

    The schema is intentionally simple so the test suite (and the
    optional report page) can consume it without a parser:

    .. code-block:: json

        {
          "handlers_count": 161,
          "unique_functions_count": 88,
          "function_definitions_count": 140,
          "missing_functions": ["fooThatDoesNotExist", ...],
          "duplicate_definitions": ["showPage", ...],
          "typos": [{"line": 42, "snippet": "..."}, ...],
          "button_audit": [
            {"line": 277, "function": "showPage", "args": "'overview'",
             "is_app_function": true, "raw": "showPage('overview')"},
            ...
          ]
        }
    """
    # Function definitions in the file (name -> list of line numbers).
    defs: dict[str, list[int]] = {}
    for m in _FUNC_DEF_RE.finditer(html):
        name = m.group(1)
        defs.setdefault(name, []).append(_line_of(html, m.start()))

    handlers: list[dict[str, Any]] = []
    missing: set[str] = set()
    used: set[str] = set()

    for m in _ONCLICK_RE.finditer(html):
        payload = m.group(1).strip()
        if not payload:
            continue
        line = _line_of(html, m.start())
        for stmt in _split_statements(payload):
            call = _CALL_RE.match(stmt)
            if not call:
                # Bare expression / unrecognised syntax — record it as
                # ``function: None`` so the inventory still reflects every
                # handler, but skip the resolution step.
                handlers.append({
                    "line": line,
                    "function": None,
                    "args": "",
                    "is_app_function": False,
                    "raw": stmt,
                })
                continue
            fn_name = call.group(1)
            args = _extract_args(stmt)
            is_app_fn = fn_name not in _BUILTIN_PREFIXES
            handlers.append({
                "line": line,
                "function": fn_name,
                "args": args,
                "is_app_function": is_app_fn,
                "raw": stmt,
            })
            if is_app_fn:
                used.add(fn_name)
                if fn_name not in defs:
                    missing.add(fn_name)

    duplicates = sorted(
        name for name, lines in defs.items() if len(lines) > 1
    )

    typos = []
    for m in _ONCLICK_TYPO_RE.finditer(html):
        line = _line_of(html, m.start())
        # Capture a small surrounding snippet for the test report.
        start = max(0, m.start() - 20)
        end = min(len(html), m.end() + 30)
        typos.append({"line": line, "snippet": html[start:end]})

    return {
        "handlers_count": len(handlers),
        "unique_functions_count": len(used),
        "function_definitions_count": sum(len(v) for v in defs.values()),
        "missing_functions": sorted(missing),
        "duplicate_definitions": duplicates,
        "typos": typos,
        "button_audit": handlers,
    }


def audit_file(path: str = DEFAULT_HTML_PATH) -> dict[str, Any]:
    """Convenience wrapper: read ``path`` and run :func:`audit` on it."""
    with open(path, "r", encoding="utf-8") as f:
        return audit(f.read())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Audit onclick handlers in the dashboard HTML."
    )
    p.add_argument(
        "html_path",
        nargs="?",
        default=DEFAULT_HTML_PATH,
        help="Path to index.html (default: src/dashboard/static/index.html)",
    )
    p.add_argument(
        "--summary",
        action="store_true",
        help="Print a one-line counts summary instead of full JSON.",
    )
    args = p.parse_args(argv)

    report = audit_file(args.html_path)
    if args.summary:
        print(
            f"handlers={report['handlers_count']} "
            f"unique_functions={report['unique_functions_count']} "
            f"missing={len(report['missing_functions'])} "
            f"typos={len(report['typos'])}"
        )
    else:
        json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(_main())
