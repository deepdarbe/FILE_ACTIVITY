"""Tests for the config flag-rot migrator (debt D7).

The migrator is intentionally narrow: it only applies a hand-curated
list of safety-flag bumps and only fires when the customer's value
matches the previous default. Tests pin that behaviour, plus the
"don't touch user customisations" promise that makes the feature
worth shipping at all.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.utils import config_migrator as cm  # noqa: E402


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(body, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# A patched migration list the tests use as their canonical fixture.
# Lets us exercise the migrator against well-known shapes without being
# at the mercy of changes in the live ``MIGRATIONS`` list.
# ---------------------------------------------------------------------------

_FIXTURE_BUMPS = [
    cm.FlagBump(
        yaml_path=("scanner", "parquet_staging", "enabled"),
        previous_default=True,
        new_default=False,
        reason="WAL leak (#185)",
        since="2026-04-26",
    ),
]


@pytest.fixture
def patched_migrations():
    with patch.object(cm, "MIGRATIONS", _FIXTURE_BUMPS):
        yield _FIXTURE_BUMPS


# ---------------------------------------------------------------------------
# Happy path.
# ---------------------------------------------------------------------------


_CONFIG_OLD_DEFAULT = """\
scanner:
  parquet_staging:
    enabled: true                # legacy WAL-leak path
    flush_rows: 50000
"""


def test_bumps_value_when_customer_matches_previous_default(
    tmp_path, patched_migrations,
):
    cfg = _write(tmp_path, _CONFIG_OLD_DEFAULT)

    applied = cm.migrate_config(cfg)

    assert len(applied) == 1
    assert applied[0].yaml_path == ("scanner", "parquet_staging", "enabled")
    data = yaml.safe_load(cfg.read_text())
    assert data["scanner"]["parquet_staging"]["enabled"] is False


def test_preserves_inline_comment_and_unrelated_lines(
    tmp_path, patched_migrations,
):
    cfg = _write(tmp_path, _CONFIG_OLD_DEFAULT)
    cm.migrate_config(cfg)

    text = cfg.read_text()
    # Inline comment survives.
    assert "# legacy WAL-leak path" in text
    # Untouched siblings survive byte-for-byte.
    assert "flush_rows: 50000" in text


def test_creates_backup_with_timestamp(tmp_path, patched_migrations):
    cfg = _write(tmp_path, _CONFIG_OLD_DEFAULT)
    cm.migrate_config(cfg)

    backups = sorted(p.name for p in tmp_path.iterdir()
                     if p.name.startswith("config.yaml.bak-"))
    assert len(backups) == 1
    backup_text = (tmp_path / backups[0]).read_text()
    assert backup_text == _CONFIG_OLD_DEFAULT


# ---------------------------------------------------------------------------
# No-op cases: customer config already on the new default, or the
# customer set a divergent value (intentional customisation we must
# never overwrite).
# ---------------------------------------------------------------------------


def test_skips_when_customer_already_on_new_default(
    tmp_path, patched_migrations,
):
    body = _CONFIG_OLD_DEFAULT.replace("enabled: true", "enabled: false")
    cfg = _write(tmp_path, body)

    applied = cm.migrate_config(cfg)

    assert applied == []
    # No backup created.
    assert not list(tmp_path.glob("config.yaml.bak-*"))
    # File untouched.
    assert cfg.read_text() == body


def test_skips_when_customer_diverged_to_unrelated_value(
    tmp_path, patched_migrations,
):
    """A non-bool value somewhere in the path should never trigger a
    bump — the migrator only fires on an exact match against
    ``previous_default``."""
    body = """\
scanner:
  parquet_staging:
    enabled: "maybe"
"""
    cfg = _write(tmp_path, body)

    applied = cm.migrate_config(cfg)

    assert applied == []
    assert cfg.read_text() == body


def test_skips_when_path_missing_entirely(tmp_path, patched_migrations):
    body = """\
scanner:
  max_workers: 4
"""
    cfg = _write(tmp_path, body)

    applied = cm.migrate_config(cfg)

    assert applied == []
    assert cfg.read_text() == body


def test_skips_when_parent_missing(tmp_path, patched_migrations):
    body = "general:\n  language: tr\n"
    cfg = _write(tmp_path, body)

    applied = cm.migrate_config(cfg)

    assert applied == []
    assert cfg.read_text() == body


# ---------------------------------------------------------------------------
# Idempotence + dry-run.
# ---------------------------------------------------------------------------


def test_second_run_is_a_noop(tmp_path, patched_migrations):
    cfg = _write(tmp_path, _CONFIG_OLD_DEFAULT)
    first = cm.migrate_config(cfg)
    assert len(first) == 1

    second = cm.migrate_config(cfg)
    assert second == []

    backups = list(tmp_path.glob("config.yaml.bak-*"))
    # First run made one backup; second run made none.
    assert len(backups) == 1


def test_dry_run_does_not_write_or_backup(
    tmp_path, patched_migrations,
):
    cfg = _write(tmp_path, _CONFIG_OLD_DEFAULT)

    applied = cm.migrate_config(cfg, dry_run=True)

    assert len(applied) == 1
    assert cfg.read_text() == _CONFIG_OLD_DEFAULT
    assert not list(tmp_path.glob("config.yaml.bak-*"))


# ---------------------------------------------------------------------------
# Defensive paths — the migrator must refuse to rewrite anything it
# doesn't understand rather than silently mangle.
# ---------------------------------------------------------------------------


def test_unexpected_literal_on_line_skipped(
    tmp_path, monkeypatch, caplog,
):
    """If two safety flags share a key name and the wrong line literal
    sits at the resolved location, the migrator must skip rather than
    rewrite the wrong value."""
    # Hand-crafted bump whose previous_default deliberately doesn't
    # match the literal on disk.
    bumps = [
        cm.FlagBump(
            yaml_path=("scanner", "parquet_staging", "enabled"),
            previous_default=True,
            new_default=False,
            reason="test",
            since="t",
        ),
    ]
    body = """\
scanner:
  parquet_staging:
    enabled: false           # already on new default
"""
    cfg = _write(tmp_path, body)
    monkeypatch.setattr(cm, "MIGRATIONS", bumps)

    applied = cm.migrate_config(cfg)
    assert applied == []
    # File untouched.
    assert cfg.read_text() == body


def test_real_config_yaml_round_trips_cleanly(tmp_path):
    """The shipped ``config.yaml`` already has every safety flag on the
    new default. Running the migrator against a copy of the real file
    must return an empty change set (sanity guard against future
    drift between MIGRATIONS and the shipped config)."""
    real = (REPO_ROOT / "config.yaml").read_text()
    cfg = _write(tmp_path, real)

    applied = cm.migrate_config(cfg)

    assert applied == [], (
        "shipped config.yaml is not on its own latest defaults — "
        "either MIGRATIONS lists a flag whose shipped default still "
        "matches previous_default, or the shipped default regressed."
    )
    # File untouched.
    assert cfg.read_text() == real


# ---------------------------------------------------------------------------
# CLI smoke.
# ---------------------------------------------------------------------------


def test_cli_dry_run_reports_changes(
    tmp_path, capsys, patched_migrations,
):
    cfg = _write(tmp_path, _CONFIG_OLD_DEFAULT)
    rc = cm._main(["--config", str(cfg), "--dry-run"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Would apply 1 migration" in out
    assert "scanner.parquet_staging.enabled: true -> false" in out
    # File untouched.
    assert cfg.read_text() == _CONFIG_OLD_DEFAULT


def test_cli_missing_config_returns_2(tmp_path, capsys):
    rc = cm._main(["--config", str(tmp_path / "nope.yaml")])
    assert rc == 2


def test_cli_quiet_on_noop(tmp_path, capsys, patched_migrations):
    body = _CONFIG_OLD_DEFAULT.replace("enabled: true", "enabled: false")
    cfg = _write(tmp_path, body)
    rc = cm._main(["--config", str(cfg), "--quiet"])
    assert rc == 0
    assert capsys.readouterr().out == ""
