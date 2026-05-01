"""Run ``node --check`` on every inline ``<script>`` block in
``src/dashboard/static/index.html`` (or any HTML file passed in).

Why this exists
---------------

Issue #194 D3 of the stabilisation audit
(``docs/architecture/audit-2026-04-28.md``) calls out that the
dashboard ships **370 KB of inline JS** with no parse gate. PR #193
shipped a JS parse error that broke every sidebar menu in production
because nothing in CI inspected the script body. CLAUDE.md's
stabilisation-week rules now say:

    `node --check` is mandatory for any `index.html` edit
    (PR #193 regression was a JS parse error — would have been
    caught in 1 second).

This script automates that gate: it extracts each inline ``<script>``
block (skipping ``<script src=...>``) and runs ``node --check`` on
each independently, so a syntax error in block N still surfaces even
if block 1 looked fine. Returns non-zero on any failure with a
file-relative line/column hint.

Local::

    python scripts/check_inline_js.py
    python scripts/check_inline_js.py path/to/other.html

CI: invoked from ``.github/workflows/ci.yml`` ``syntax`` job.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_HTML = REPO_ROOT / "src" / "dashboard" / "static" / "index.html"


# ``<script ...>BODY</script>`` where the opening tag does NOT carry a
# ``src=`` attribute. Multi-line, non-greedy.
_INLINE_SCRIPT = re.compile(
    r"<script(?![^>]*\bsrc\s*=)[^>]*>(.*?)</script>",
    re.DOTALL | re.IGNORECASE,
)


def _line_of_offset(text: str, offset: int) -> int:
    """1-indexed line number in ``text`` for byte ``offset``."""
    return text.count("\n", 0, offset) + 1


def _check_one(body: str, label: str) -> tuple[bool, str]:
    """Pipe ``body`` to ``node --check``. Returns (ok, stderr_text)."""
    proc = subprocess.run(
        ["node", "--check", "-"],
        input=body,
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0:
        return True, ""
    err = (proc.stderr or proc.stdout or "<no diagnostic>").strip()
    return False, f"[{label}] node --check failed:\n{err}"


def check_html(html_path: Path) -> int:
    if shutil.which("node") is None:
        print(
            "node not found on PATH — install Node.js or add the runner step",
            file=sys.stderr,
        )
        return 2

    if not html_path.exists():
        print(f"file not found: {html_path}", file=sys.stderr)
        return 2

    text = html_path.read_text(encoding="utf-8")
    failures: list[str] = []
    block_count = 0
    for match in _INLINE_SCRIPT.finditer(text):
        block_count += 1
        body = match.group(1)
        body_start = match.start(1)
        line_no = _line_of_offset(text, body_start)
        label = f"{html_path.name}:line ~{line_no} (block #{block_count})"
        ok, err = _check_one(body, label)
        if not ok:
            failures.append(err)

    rel = html_path.relative_to(REPO_ROOT) if html_path.is_relative_to(REPO_ROOT) else html_path
    if failures:
        for f in failures:
            print(f, file=sys.stderr)
        print(
            f"\n{rel}: {len(failures)} of {block_count} inline script "
            f"block(s) failed parse",
            file=sys.stderr,
        )
        return 1

    print(f"{rel}: {block_count} inline script block(s) parsed clean")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "html", nargs="?", type=Path, default=DEFAULT_HTML,
        help=f"HTML file to scan (default: {DEFAULT_HTML.relative_to(REPO_ROOT)})",
    )
    args = parser.parse_args(argv)
    return check_html(args.html)


if __name__ == "__main__":
    raise SystemExit(main())
