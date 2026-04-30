"""Config flag-rot migrator — debt item D7 (audit-2026-04-28.md).

Why this exists
---------------

``deploy/setup-source.ps1`` correctly preserves ``config\\config.yaml``
across updates so operator customisations survive. The flip side is
that any *safe-default change* we ship in the source tree's
``config.yaml`` never reaches the customer's machine, because their
file is left alone. Specifically: when the parquet-staging path was
flipped from ``enabled: true`` to ``enabled: false`` (#174 / #185 /
#186 — the WAL-leak fix), the customer's preserved config kept the
old, broken value, and the WAL leak persisted across our fix.

What this does
--------------

A small, hand-curated list of *safety-flag bumps* lives below in
``MIGRATIONS``. Each entry says: "before vN we shipped key=A; from vN
we ship key=B; if the customer's value is still A, that means they
never overrode it — bump them to B." Values that diverge from both
defaults (intentional customisation) are left alone. The migrator
backs the original up to ``config.yaml.bak-<UTC ts>`` before any
write, so a wrong call is one ``cp`` away from being undone.

Comment preservation
--------------------

Standard PyYAML round-trips do not preserve comments. Rather than
add ``ruamel.yaml`` as a dependency during stabilisation week, this
module finds each target key with PyYAML's ``compose()`` Mark API
(line/column info), then rewrites that one line in place. Comments
on the line and on every line we don't touch are left intact.

CLI
---

::

    python -m src.utils.config_migrator --config /path/to/config.yaml
    python -m src.utils.config_migrator --config ... --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

import yaml
from yaml.nodes import MappingNode, ScalarNode

logger = logging.getLogger("file_activity.utils.config_migrator")


@dataclass(frozen=True)
class FlagBump:
    """A single declared default change.

    ``yaml_path`` is the dotted-tuple key path (e.g.
    ``("scanner", "parquet_staging", "enabled")``).
    ``previous_default`` and ``new_default`` are the *Python* values
    we expect to see after PyYAML decodes the file (``True`` / ``False``
    for booleans, ``str`` for strings, etc.). The migrator only fires
    when the customer's value equals ``previous_default``.
    """

    yaml_path: tuple[str, ...]
    previous_default: Any
    new_default: Any
    reason: str
    since: str  # ISO date the change shipped, for audit trail


# Canonical safety-flag bumps. Add new entries here when shipping a
# default change that meaningfully fixes prod-blocker behaviour and you
# want existing installations to inherit it on next ``update.cmd``.
#
# Keep this list short. Each entry is a small contract with the
# customer ("we will silently re-flip your setting if you never
# touched it"); abuse costs trust.
MIGRATIONS: list[FlagBump] = [
    FlagBump(
        yaml_path=("scanner", "parquet_staging", "enabled"),
        previous_default=True,
        new_default=False,
        reason=(
            "WAL leak / mid-scan abort — the DuckDB ATTACH(READ_WRITE) "
            "ingest path deadlocked with the live dashboard reader on "
            "100k+-row scans. Fixed by leaving the executemany path on "
            "by default (#174, #185, #186)."
        ),
        since="2026-04-26",
    ),
]


# ---------------------------------------------------------------------------
# YAML node walking — find the ScalarNode that backs a given key path so
# we can read its source line number.
# ---------------------------------------------------------------------------


def _find_scalar_at_path(
    root, path: Sequence[str]
) -> Optional[ScalarNode]:
    """Walk a YAML Node tree following ``path`` and return the leaf
    ScalarNode, or ``None`` if any segment is missing / non-scalar."""
    cur = root
    for key in path:
        if not isinstance(cur, MappingNode):
            return None
        next_node = None
        for key_node, value_node in cur.value:
            if isinstance(key_node, ScalarNode) and key_node.value == key:
                next_node = value_node
                break
        if next_node is None:
            return None
        cur = next_node
    return cur if isinstance(cur, ScalarNode) else None


# ---------------------------------------------------------------------------
# Line-level rewrite — replace exactly the value on a specific line,
# leave the leading key, the colon, and any trailing inline comment alone.
# ---------------------------------------------------------------------------


def _yaml_repr(value: Any) -> str:
    """Render a Python scalar back to its YAML literal form for the
    small set of types the migrator handles."""
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value is None:
        return "null"
    if isinstance(value, str):
        # Quote only if the original looks like it needs to be quoted —
        # safety flags so far are unquoted bools, so this branch is
        # mostly defensive.
        return f'"{value}"' if any(c in value for c in " :#") else value
    return str(value)


def _rewrite_value_on_line(line: str, old_lit: str, new_lit: str
                            ) -> Optional[str]:
    """Replace ``<key>: <old_lit>`` with ``<key>: <new_lit>`` on a
    single source line, preserving the leading key+colon+spaces and
    the trailing whitespace + inline comment. Returns ``None`` if the
    expected literal isn't present.

    Anchored by the ``: `` separator so a key whose name happens to
    contain ``true`` (e.g. ``trueish:``) does not collide.
    """
    sep = ": "
    sep_idx = line.find(sep)
    if sep_idx < 0:
        return None
    head = line[: sep_idx + len(sep)]
    tail = line[sep_idx + len(sep):]

    # Tail = "<old_lit>[<spaces><# comment>]<eol>". Parse off the
    # value, keep whatever's after.
    if not tail.startswith(old_lit):
        return None
    rest = tail[len(old_lit):]
    # If the literal is followed by anything other than whitespace,
    # end-of-line, or a comment, refuse — we don't understand this
    # line.
    if rest and not (rest[0] in (" ", "\t", "#", "\n", "\r")):
        return None
    return f"{head}{new_lit}{rest}"


# ---------------------------------------------------------------------------
# Public entrypoint.
# ---------------------------------------------------------------------------


def _nested_get(data: dict, path: Sequence[str]) -> Any:
    cur: Any = data
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur


def migrate_config(
    config_path: Path, *, dry_run: bool = False,
) -> list[FlagBump]:
    """Apply registered safety-flag migrations to ``config_path``.

    The original file is backed up to ``<config>.bak-<UTC ts>`` before
    any write. Returns the list of bumps that were (or would be, with
    ``dry_run=True``) applied.

    Raises ``FileNotFoundError`` if ``config_path`` doesn't exist.
    """
    original = config_path.read_text(encoding="utf-8")
    data = yaml.safe_load(original) or {}
    root_node = yaml.compose(original)
    lines = original.splitlines(keepends=True)

    applied: list[FlagBump] = []
    changed = False

    for bump in MIGRATIONS:
        current = _nested_get(data, bump.yaml_path)
        if current != bump.previous_default:
            continue

        scalar = _find_scalar_at_path(root_node, bump.yaml_path)
        if scalar is None:
            logger.warning(
                "config_migrator: %s present in parsed YAML but not in "
                "node tree; skipping",
                ".".join(bump.yaml_path),
            )
            continue

        line_no = scalar.start_mark.line  # 0-indexed
        if line_no >= len(lines):
            logger.warning(
                "config_migrator: %s line %d out of range",
                ".".join(bump.yaml_path), line_no,
            )
            continue
        old_lit = _yaml_repr(bump.previous_default)
        new_lit = _yaml_repr(bump.new_default)
        rewritten = _rewrite_value_on_line(
            lines[line_no], old_lit, new_lit,
        )
        if rewritten is None:
            logger.warning(
                "config_migrator: could not rewrite %s; expected '%s' "
                "on line %d, got %r",
                ".".join(bump.yaml_path), old_lit, line_no + 1,
                lines[line_no].rstrip("\n"),
            )
            continue

        lines[line_no] = rewritten
        applied.append(bump)
        changed = True
        logger.info(
            "config_migrator: bumped %s: %s -> %s (%s; since %s)",
            ".".join(bump.yaml_path), old_lit, new_lit,
            bump.reason, bump.since,
        )

    if not applied or dry_run or not changed:
        return applied

    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
    backup_path = config_path.with_name(
        config_path.name + f".bak-{ts}"
    )
    backup_path.write_text(original, encoding="utf-8")
    config_path.write_text("".join(lines), encoding="utf-8")
    logger.info(
        "config_migrator: %d bump(s) applied; backup at %s",
        len(applied), backup_path,
    )
    return applied


# ---------------------------------------------------------------------------
# CLI — invoked from setup-source.ps1 / update.cmd.
# ---------------------------------------------------------------------------


def _format_bump(b: FlagBump) -> str:
    return (
        f"  - {'.'.join(b.yaml_path)}: "
        f"{_yaml_repr(b.previous_default)} -> {_yaml_repr(b.new_default)}\n"
        f"    reason: {b.reason}\n"
        f"    since:  {b.since}"
    )


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--config", type=Path, required=True,
        help="Path to customer config.yaml",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would change without rewriting the file",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress 'config is up to date' on no-op runs",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if not args.config.exists():
        print(f"config not found: {args.config}", file=sys.stderr)
        return 2

    applied = migrate_config(args.config, dry_run=args.dry_run)
    if applied:
        verb = "Would apply" if args.dry_run else "Applied"
        print(f"{verb} {len(applied)} migration(s):")
        for b in applied:
            print(_format_bump(b))
    elif not args.quiet:
        print("config is up to date — no migrations applied")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
