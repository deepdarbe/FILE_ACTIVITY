"""H-3 (issue #158): CI grep guard.

The private ``_insert_audit_event_unchained`` /
``_insert_audit_event_simple_unchained`` variants bypass the
tamper-evident hash chain (issue #38). They MUST only be referenced from
within ``src/storage/database.py`` itself; any other call-site silently
re-opens the audit-bypass hole the wrapper closed.

This test fails the build if a new code path imports/references the
private names anywhere outside ``database.py`` (and the test files that
intentionally exercise them).
"""

from __future__ import annotations

from pathlib import Path


_FORBIDDEN_NEEDLES = (
    "_insert_audit_event_unchained",
    "_insert_audit_event_simple_unchained",
)


def test_no_external_callsites_to_unchained_audit():
    """No code outside database.py may reference the private unchained
    audit-event APIs. Any reference re-introduces H-3 (audit bypass)."""
    repo_root = Path(__file__).resolve().parent.parent
    offenders: list[str] = []

    for py in repo_root.rglob("*.py"):
        # Skip hidden dirs (e.g. .venv, .git worktrees), the database
        # module itself (which legitimately defines the private names),
        # and test files (which may exercise the boundary).
        rel = py.relative_to(repo_root)
        if any(part.startswith(".") for part in rel.parts):
            continue
        if "database.py" in py.name:
            continue
        if py.name.startswith("test_"):
            continue

        try:
            text = py.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue

        for needle in _FORBIDDEN_NEEDLES:
            if needle in text:
                offenders.append(f"{rel}: {needle}")

    assert not offenders, (
        "Bypass of audit chain detected — these files reference the "
        "private unchained audit-event APIs. Use the public "
        "``insert_audit_event`` / ``insert_audit_event_simple`` instead "
        "(they auto-route to the chained variant when enabled):\n  "
        + "\n  ".join(offenders)
    )
