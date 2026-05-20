"""Config flag-rot migrator (issue #194 stabilization week / Wave 8 / D7).

setup-source.ps1 preserves `config\\config.yaml` verbatim across
updates so operator customisations survive. The unintended side
effect: any new safe-default we ship (e.g. ``parquet_staging.enabled:
false`` from #174) never reaches the customer's machine because
their config still has the old ``true``.

This script reads ``scripts/config_migrations.yaml`` for the list of
rules and applies them to a target config file. A rule fires only
when the customer's CURRENT value matches the documented
``old_default`` — that is the "this looks like the previous shipped
default, not an operator customisation" signal. Any other value
(including an explicit operator choice) is left alone.

The text-level edit is intentional: PyYAML's round-trip would strip
comments, and the customer's config has long inline rationale (e.g.
the parquet_staging block has 13 lines of explanation). We track
indent context to identify the right ``leaf: value`` line and rewrite
only that line. Backup-then-write is mandatory; the original lands
next to the file with a timestamp suffix.

Usage:
    python scripts/migrate_config.py CONFIG_PATH [--dry-run]

Exit codes:
    0 — no migrations needed, or all migrations applied successfully
    1 — at least one rule failed (parse error, ambiguity, etc.); file
        unchanged
"""

from __future__ import annotations

import argparse
import logging
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import yaml

logger = logging.getLogger("file_activity.migrate_config")


REPO_ROOT = Path(__file__).resolve().parent.parent
RULES_FILE = REPO_ROOT / "scripts" / "config_migrations.yaml"


# ---------------------------------------------------------------------------
# Rule loading
# ---------------------------------------------------------------------------


def load_rules(path: Path = RULES_FILE) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    rules = doc.get("migrations") or []
    if not isinstance(rules, list):
        raise ValueError(f"{path}: 'migrations' must be a list")
    out: list[dict] = []
    for i, r in enumerate(rules):
        if not isinstance(r, dict):
            raise ValueError(f"{path}: migration #{i} is not a mapping")
        for k in ("path", "old_default", "new_default"):
            if k not in r:
                raise ValueError(
                    f"{path}: migration #{i} missing required key {k!r}"
                )
        out.append(r)
    return out


# ---------------------------------------------------------------------------
# Customer config inspection
# ---------------------------------------------------------------------------


def _navigate(doc: Any, path: str) -> tuple[bool, Any]:
    """Return (found, value) for a dotted path inside a parsed mapping."""
    cur = doc
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return False, None
        cur = cur[part]
    return True, cur


# ---------------------------------------------------------------------------
# Text-level edit (preserves comments / formatting)
# ---------------------------------------------------------------------------


def _indent_of(line: str) -> int:
    """Number of leading-space chars; tabs not supported by spec."""
    return len(line) - len(line.lstrip(" "))


def _iter_leaf_assignments(
    lines: list[str],
) -> Iterator[tuple[int, str, list[str]]]:
    """Yield ``(line_index, leaf_key, dotted_path_parts)`` for every line
    that looks like a ``key: value`` assignment.

    Tracks an indent stack so nested keys resolve to their full path.
    Comments and blank lines are skipped. Sequence members (``- ...``)
    are ignored — we never migrate inside a list. List headers (``key:``
    with no value) push the key onto the path for the children.
    """
    # Stack of (indent, key) pairs; deepest entry is the current parent.
    stack: list[tuple[int, str]] = []
    leaf_pattern = re.compile(
        r"^(?P<indent> *)(?P<key>[A-Za-z_][\w\-]*)\s*:\s*(?P<rest>.*)$"
    )
    for idx, raw in enumerate(lines):
        line = raw.rstrip("\n")
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        # We don't traverse into list items
        if stripped.startswith("- "):
            continue
        m = leaf_pattern.match(line)
        if not m:
            continue
        indent = len(m.group("indent"))
        key = m.group("key")
        rest = m.group("rest")
        # Pop deeper-or-equal-indent entries off the stack
        while stack and stack[-1][0] >= indent:
            stack.pop()
        # Strip trailing comment to detect "container" vs "leaf"
        value_part = rest.split("#", 1)[0].strip()
        path_parts = [k for _, k in stack] + [key]
        if value_part == "":
            # Container — push onto stack for children, do not emit
            stack.append((indent, key))
            continue
        # Leaf assignment — emit
        yield idx, key, path_parts


def _format_scalar(value: Any) -> str:
    """Render a Python value back to YAML scalar text the way PyYAML
    would emit it on a single line. Conservative: only handles the
    types we accept in migration rules (bool, int, str, None)."""
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    # String — quote if it contains characters YAML would re-interpret
    s = str(value)
    if re.search(r"[\s:#'\"\[\]\{\},&*!|>%@`]", s) or s in {
        "true", "false", "null", "yes", "no", "on", "off"
    } or s == "":
        return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return s


def _replace_value_on_line(line: str, new_value: Any) -> str:
    """Rewrite the value portion of a ``  key: <value>`` line while
    preserving the indent, the key, the colon, the trailing comment if
    any, and the line ending. The pattern is anchored so it never
    matches sequence items or comments.

    Quote style is preserved for string values: if the operator wrote
    ``host: "0.0.0.0"`` (double-quoted), the replacement is
    ``host: "127.0.0.1"`` — not the bare YAML form. This keeps the
    visual change minimal so the operator scanning the file after a
    migration sees only the value flip, not a style rewrite.
    """
    m = re.match(
        r"^(?P<prefix> *[A-Za-z_][\w\-]*\s*:\s*)"
        r"(?P<value>(?:'[^']*'|\"[^\"]*\"|[^#\r\n]*?))"
        r"(?P<trail>\s*(?:#.*)?(?:\r?\n)?)$",
        line,
    )
    if not m:
        raise ValueError(f"unparseable assignment line: {line!r}")
    old_value = m["value"].strip()
    new_scalar = _format_scalar(new_value)
    # If the operator's old value used double or single quotes AND the
    # new value is a string that doesn't strictly require them, mirror
    # the quote style. We don't downgrade a value that needs quotes.
    if isinstance(new_value, str):
        if old_value.startswith('"') and old_value.endswith('"'):
            new_scalar = '"' + new_value.replace("\\", "\\\\").replace('"', '\\"') + '"'
        elif old_value.startswith("'") and old_value.endswith("'"):
            new_scalar = "'" + new_value.replace("'", "''") + "'"
    return f"{m['prefix']}{new_scalar}{m['trail']}"


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


class MigrationResult:
    def __init__(self, path: str, action: str, detail: str = ""):
        self.path = path
        self.action = action  # "applied" | "skipped" | "missing" | "error"
        self.detail = detail

    def __repr__(self) -> str:
        return f"<MigrationResult {self.path} {self.action} {self.detail!r}>"


def migrate(
    config_path: Path,
    rules: list[dict],
    dry_run: bool = False,
) -> list[MigrationResult]:
    """Apply migration rules to ``config_path``. Returns a per-rule
    result list. The file is modified atomically (write to a temp,
    rename over the original) only when at least one rule fires AND
    re-parsing the resulting text yields the expected values.
    """
    results: list[MigrationResult] = []

    if not config_path.exists():
        for r in rules:
            results.append(
                MigrationResult(r["path"], "error", "config file not found")
            )
        return results

    text = config_path.read_text(encoding="utf-8")
    try:
        doc = yaml.safe_load(text) or {}
    except yaml.YAMLError as e:
        for r in rules:
            results.append(
                MigrationResult(r["path"], "error", f"parse failure: {e}")
            )
        return results

    # Decide per-rule whether to apply.
    to_apply: list[tuple[dict, int]] = []  # (rule, line_index)
    lines = text.splitlines(keepends=True)

    # Build an index of leaf assignments keyed by dotted path.
    leaf_lookup: dict[str, list[int]] = {}
    for line_idx, _key, path_parts in _iter_leaf_assignments(lines):
        leaf_lookup.setdefault(".".join(path_parts), []).append(line_idx)

    for rule in rules:
        path = rule["path"]
        found, current = _navigate(doc, path)
        if not found:
            results.append(MigrationResult(path, "missing", "key not present"))
            continue
        if current != rule["old_default"]:
            results.append(MigrationResult(
                path, "skipped",
                f"current={current!r} != old_default={rule['old_default']!r}",
            ))
            continue
        line_idxs = leaf_lookup.get(path, [])
        if len(line_idxs) != 1:
            results.append(MigrationResult(
                path, "error",
                f"found {len(line_idxs)} candidate lines (need exactly 1)",
            ))
            continue
        to_apply.append((rule, line_idxs[0]))
        results.append(MigrationResult(
            path, "applied",
            f"{current!r} -> {rule['new_default']!r}",
        ))

    if not to_apply or dry_run:
        return results

    # Apply edits.
    new_lines = list(lines)
    for rule, line_idx in to_apply:
        try:
            new_lines[line_idx] = _replace_value_on_line(
                new_lines[line_idx], rule["new_default"],
            )
        except Exception as e:
            for r_existing in results:
                if r_existing.path == rule["path"]:
                    r_existing.action = "error"
                    r_existing.detail = f"line rewrite failed: {e}"
            return results

    new_text = "".join(new_lines)

    # Verify the result still parses AND the new values are present.
    try:
        new_doc = yaml.safe_load(new_text) or {}
    except yaml.YAMLError as e:
        for r in results:
            if r.action == "applied":
                r.action = "error"
                r.detail = f"post-edit parse failed: {e}"
        return results
    for rule, _line_idx in to_apply:
        _, new_val = _navigate(new_doc, rule["path"])
        if new_val != rule["new_default"]:
            for r_existing in results:
                if r_existing.path == rule["path"]:
                    r_existing.action = "error"
                    r_existing.detail = (
                        f"post-edit value {new_val!r} != expected "
                        f"{rule['new_default']!r}"
                    )
            return results

    # Backup + atomic write.
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = config_path.with_suffix(config_path.suffix + f".bak-{ts}")
    shutil.copy2(config_path, backup)
    tmp = config_path.with_suffix(config_path.suffix + ".tmp")
    tmp.write_text(new_text, encoding="utf-8")
    tmp.replace(config_path)
    logger.info("migrated %s (backup at %s)", config_path, backup)

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "config",
        help="Path to the customer's config.yaml",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change, don't write.",
    )
    ap.add_argument(
        "--rules",
        default=str(RULES_FILE),
        help=f"Path to migration rules file (default: {RULES_FILE}).",
    )
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )

    rules = load_rules(Path(args.rules))
    results = migrate(Path(args.config), rules, dry_run=args.dry_run)

    applied = [r for r in results if r.action == "applied"]
    errors = [r for r in results if r.action == "error"]

    for r in results:
        prefix = "::error::" if r.action == "error" else ""
        logger.info("%s%-9s %s — %s", prefix, r.action, r.path, r.detail)

    if errors:
        logger.error("config migration FAILED — %d rule(s) errored", len(errors))
        return 1
    if applied:
        verb = "WOULD APPLY" if args.dry_run else "applied"
        logger.info("config migration: %s %d rule(s)", verb, len(applied))
    else:
        logger.info("config migration: no changes needed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
