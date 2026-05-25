"""Tests for scripts/migrate_config.py (issue #194 Wave 8 / D7).

Covers the customer-facing safety guarantees:
  * old_default → new_default flip happens
  * operator customisations (any value != old_default) preserved
  * comments + formatting around the migrated line preserved
  * backup written before modification
  * post-edit YAML must parse and contain the expected new value
  * missing keys and absent files don't crash
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

# Import the migrator script as a module (not a package).
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "migrate_config.py"
sys.path.insert(0, str(SCRIPT.parent))
import migrate_config  # noqa: E402


@pytest.fixture
def rules_flip_parquet():
    return [
        {
            "path": "scanner.parquet_staging.enabled",
            "old_default": True,
            "new_default": False,
            "reason": "test",
            "pr": "#174",
        }
    ]


@pytest.fixture
def rules_flip_host():
    return [
        {
            "path": "dashboard.host",
            "old_default": "0.0.0.0",
            "new_default": "127.0.0.1",
            "reason": "test",
            "pr": "#158",
        }
    ]


def _write(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Happy path — value matches old_default → flip applied
# ---------------------------------------------------------------------------


def test_flip_applied_when_value_matches_old_default(tmp_path, rules_flip_parquet):
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  parquet_staging:
    # operator's comment about parquet
    enabled: true
    flush_rows: 50000
""")
    results = migrate_config.migrate(cfg, rules_flip_parquet)
    assert [r.action for r in results] == ["applied"]
    new = cfg.read_text(encoding="utf-8")
    assert "enabled: false" in new
    # Comment preserved
    assert "operator's comment about parquet" in new
    # flush_rows untouched
    assert "flush_rows: 50000" in new
    # Backup created next to the file
    backups = list(tmp_path.glob("config.yaml.bak-*"))
    assert len(backups) == 1
    assert "enabled: true" in backups[0].read_text(encoding="utf-8")


def test_string_value_flip(tmp_path, rules_flip_host):
    cfg = _write(tmp_path / "config.yaml", """\
dashboard:
  host: "0.0.0.0"
  port: 8085
""")
    results = migrate_config.migrate(cfg, rules_flip_host)
    assert results[0].action == "applied"
    new = cfg.read_text(encoding="utf-8")
    parsed = yaml.safe_load(new)
    assert parsed["dashboard"]["host"] == "127.0.0.1"
    assert parsed["dashboard"]["port"] == 8085
    # Quote style preserved: operator wrote "0.0.0.0", we write "127.0.0.1".
    assert 'host: "127.0.0.1"' in new


def test_quote_style_preserved_single_quotes(tmp_path, rules_flip_host):
    cfg = _write(tmp_path / "config.yaml", """\
dashboard:
  host: '0.0.0.0'
""")
    results = migrate_config.migrate(cfg, rules_flip_host)
    assert results[0].action == "applied"
    new = cfg.read_text(encoding="utf-8")
    assert "host: '127.0.0.1'" in new


def test_quote_style_preserved_bare(tmp_path, rules_flip_host):
    cfg = _write(tmp_path / "config.yaml", """\
dashboard:
  host: 0.0.0.0
""")
    results = migrate_config.migrate(cfg, rules_flip_host)
    assert results[0].action == "applied"
    new = cfg.read_text(encoding="utf-8")
    assert "host: 127.0.0.1" in new
    assert 'host: "127.0.0.1"' not in new


# ---------------------------------------------------------------------------
# Operator customisation — value differs from old_default → DO NOT TOUCH
# ---------------------------------------------------------------------------


def test_operator_value_preserved(tmp_path, rules_flip_parquet):
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  parquet_staging:
    enabled: false
""")
    results = migrate_config.migrate(cfg, rules_flip_parquet)
    assert results[0].action == "skipped"
    # File unchanged
    assert cfg.read_text(encoding="utf-8") == """\
scanner:
  parquet_staging:
    enabled: false
"""
    # No backup written when nothing applied
    assert not list(tmp_path.glob("config.yaml.bak-*"))


def test_operator_value_arbitrary_preserved(tmp_path, rules_flip_host):
    """Customer with LAN bind (operator set 0.0.0.0 deliberately AFTER
    we shipped 127.0.0.1 default) — value happens to equal old_default
    again. This is the migration-rule's blind spot, accepted: we flip
    it back. Cover with a different non-default value to confirm
    skipping logic."""
    cfg = _write(tmp_path / "config.yaml", """\
dashboard:
  host: "192.168.1.10"
""")
    results = migrate_config.migrate(cfg, rules_flip_host)
    assert results[0].action == "skipped"
    assert cfg.read_text(encoding="utf-8").count("192.168.1.10") == 1


# ---------------------------------------------------------------------------
# Comment + formatting preservation
# ---------------------------------------------------------------------------


def test_inline_comment_preserved(tmp_path, rules_flip_parquet):
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  parquet_staging:
    enabled: true     # PR #174 — was unsafe pre-2026-03
    flush_rows: 50000
""")
    results = migrate_config.migrate(cfg, rules_flip_parquet)
    assert results[0].action == "applied"
    new = cfg.read_text(encoding="utf-8")
    assert "# PR #174 — was unsafe pre-2026-03" in new
    assert "enabled: false" in new


def test_long_comment_block_preserved(tmp_path, rules_flip_parquet):
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  parquet_staging:
    # Tarama sirasinda satirlari Parquet dosyasina biriktirir, sonra DuckDB
    # uzerinden tek INSERT ile SQLite'a ingest eder. 100k+ satirli scan'lerde
    # row-by-row INSERT'e gore 10-50x hizli; pyarrow yoksa otomatik olarak
    # klasik bulk_insert_scanned_files yoluna dusulur.
    #
    # Issue #174 — varsayilan KAPALI. Manuel acmak icin: enabled: true.
    enabled: true
    flush_rows: 50000
""")
    results = migrate_config.migrate(cfg, rules_flip_parquet)
    assert results[0].action == "applied"
    new = cfg.read_text(encoding="utf-8")
    # Every original comment line is still in the file
    assert "Tarama sirasinda satirlari Parquet dosyasina biriktirir" in new
    assert "Issue #174 — varsayilan KAPALI" in new
    assert "row-by-row INSERT'e gore 10-50x hizli" in new


# ---------------------------------------------------------------------------
# Safety — dry-run, missing keys, broken YAML
# ---------------------------------------------------------------------------


def test_dry_run_does_not_modify(tmp_path, rules_flip_parquet):
    original = """\
scanner:
  parquet_staging:
    enabled: true
"""
    cfg = _write(tmp_path / "config.yaml", original)
    results = migrate_config.migrate(cfg, rules_flip_parquet, dry_run=True)
    assert results[0].action == "applied"  # would-apply
    assert cfg.read_text(encoding="utf-8") == original
    assert not list(tmp_path.glob("config.yaml.bak-*"))


def test_missing_key_does_not_crash(tmp_path, rules_flip_parquet):
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  # parquet_staging block absent entirely
  enrich_sizes: true
""")
    results = migrate_config.migrate(cfg, rules_flip_parquet)
    assert results[0].action == "missing"
    # File unchanged: the key isn't present as a real assignment
    parsed = yaml.safe_load(cfg.read_text(encoding="utf-8"))
    assert "parquet_staging" not in parsed["scanner"]
    assert not list(tmp_path.glob("config.yaml.bak-*"))


def test_missing_file_records_error(tmp_path, rules_flip_parquet):
    cfg = tmp_path / "nonexistent.yaml"
    results = migrate_config.migrate(cfg, rules_flip_parquet)
    assert results[0].action == "error"


def test_broken_yaml_is_not_modified(tmp_path, rules_flip_parquet):
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  parquet_staging:
    enabled: true
this is not: valid: yaml: here
""")
    results = migrate_config.migrate(cfg, rules_flip_parquet)
    assert results[0].action == "error"
    # Original preserved
    assert "this is not: valid: yaml: here" in cfg.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Multi-rule end-to-end
# ---------------------------------------------------------------------------


def test_multiple_rules_applied_in_one_pass(tmp_path):
    rules = [
        {
            "path": "scanner.parquet_staging.enabled",
            "old_default": True,
            "new_default": False,
            "reason": "x",
            "pr": "#174",
        },
        {
            "path": "dashboard.host",
            "old_default": "0.0.0.0",
            "new_default": "127.0.0.1",
            "reason": "y",
            "pr": "#158",
        },
        {
            "path": "backup.corruption_check_mode",
            "old_default": "quick",
            "new_default": "skip",
            "reason": "z",
            "pr": "#131",
        },
    ]
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  parquet_staging:
    enabled: true
dashboard:
  host: "0.0.0.0"
  port: 8085
backup:
  corruption_check_mode: "quick"
  enabled: true
""")
    results = migrate_config.migrate(cfg, rules)
    actions = sorted(r.action for r in results)
    assert actions == ["applied", "applied", "applied"]
    parsed = yaml.safe_load(cfg.read_text(encoding="utf-8"))
    assert parsed["scanner"]["parquet_staging"]["enabled"] is False
    assert parsed["dashboard"]["host"] == "127.0.0.1"
    assert parsed["dashboard"]["port"] == 8085  # untouched
    assert parsed["backup"]["corruption_check_mode"] == "skip"
    assert parsed["backup"]["enabled"] is True  # untouched


def test_partial_rule_set_only_flips_matching(tmp_path):
    rules = [
        {
            "path": "scanner.parquet_staging.enabled",
            "old_default": True,
            "new_default": False,
            "reason": "x",
            "pr": "#174",
        },
        {
            "path": "watcher.backend",
            "old_default": "polling",
            "new_default": "watchdog",
            "reason": "y",
            "pr": "#14",
        },
    ]
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  parquet_staging:
    enabled: true
watcher:
  backend: "watchdog"
""")
    results = migrate_config.migrate(cfg, rules)
    by_path = {r.path: r for r in results}
    assert by_path["scanner.parquet_staging.enabled"].action == "applied"
    assert by_path["watcher.backend"].action == "skipped"
    parsed = yaml.safe_load(cfg.read_text(encoding="utf-8"))
    assert parsed["scanner"]["parquet_staging"]["enabled"] is False
    assert parsed["watcher"]["backend"] == "watchdog"


# ---------------------------------------------------------------------------
# Shipped rules file loads cleanly
# ---------------------------------------------------------------------------


def test_shipped_rules_file_parses():
    rules = migrate_config.load_rules()
    assert len(rules) >= 1, "no migration rules shipped"
    for r in rules:
        assert "path" in r
        assert "old_default" in r
        assert "new_default" in r
        assert "." in r["path"], f"rule path must be dotted: {r['path']!r}"


def test_orphan_sid_rule_flips_preserved_false(tmp_path):
    """2026-05-24 operator rule: a preserved config with the orphaned-SID
    report disabled is flipped on (domain-joined fleets), siblings intact."""
    cfg = _write(tmp_path / "config.yaml", """\
security:
  orphan_sid:
    enabled: false
    cache_ttl_minutes: 1440
""")
    rules = migrate_config.load_rules()
    results = migrate_config.migrate(cfg, rules)
    by_path = {r.path: r for r in results}
    assert by_path["security.orphan_sid.enabled"].action == "applied"
    parsed = yaml.safe_load(cfg.read_text(encoding="utf-8"))
    assert parsed["security"]["orphan_sid"]["enabled"] is True
    assert parsed["security"]["orphan_sid"]["cache_ttl_minutes"] == 1440


# ---------------------------------------------------------------------------
# set_if_missing — insert an absent key (the #8/#9 + #1 durable fix)
# ---------------------------------------------------------------------------


@pytest.fixture
def rules_parquet_set_if_missing():
    return [
        {
            "path": "scanner.parquet_staging.enabled",
            "old_default": True,
            "new_default": False,
            "set_if_missing": True,
            "reason": "test",
            "pr": "#174",
        }
    ]


def test_set_if_missing_inserts_leaf_under_existing_parent(
    tmp_path, rules_parquet_set_if_missing
):
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  parquet_staging:
    flush_rows: 50000
""")
    results = migrate_config.migrate(cfg, rules_parquet_set_if_missing)
    assert [r.action for r in results] == ["inserted"]
    parsed = yaml.safe_load(cfg.read_text(encoding="utf-8"))
    assert parsed["scanner"]["parquet_staging"]["enabled"] is False
    assert parsed["scanner"]["parquet_staging"]["flush_rows"] == 50000
    assert len(list(tmp_path.glob("config.yaml.bak-*"))) == 1


def test_set_if_missing_inserts_intermediate_container(
    tmp_path, rules_parquet_set_if_missing
):
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  enrich_sizes: true
  size_enrich_workers: 8
""")
    results = migrate_config.migrate(cfg, rules_parquet_set_if_missing)
    assert [r.action for r in results] == ["inserted"]
    parsed = yaml.safe_load(cfg.read_text(encoding="utf-8"))
    assert parsed["scanner"]["parquet_staging"]["enabled"] is False
    # Existing siblings untouched.
    assert parsed["scanner"]["enrich_sizes"] is True
    assert parsed["scanner"]["size_enrich_workers"] == 8


def test_set_if_missing_inserts_top_level_when_root_absent(
    tmp_path, rules_parquet_set_if_missing
):
    cfg = _write(tmp_path / "config.yaml", """\
dashboard:
  port: 8085
""")
    results = migrate_config.migrate(cfg, rules_parquet_set_if_missing)
    assert [r.action for r in results] == ["inserted"]
    parsed = yaml.safe_load(cfg.read_text(encoding="utf-8"))
    assert parsed["scanner"]["parquet_staging"]["enabled"] is False
    assert parsed["dashboard"]["port"] == 8085


def test_set_if_missing_preserves_comments(tmp_path, rules_parquet_set_if_missing):
    cfg = _write(tmp_path / "config.yaml", """\
# top of file
scanner:
  # a comment about scanner
  enrich_sizes: true   # inline note
""")
    results = migrate_config.migrate(cfg, rules_parquet_set_if_missing)
    assert results[0].action == "inserted"
    new = cfg.read_text(encoding="utf-8")
    assert "# top of file" in new
    assert "# a comment about scanner" in new
    assert "# inline note" in new


def test_set_if_missing_present_value_still_flips(
    tmp_path, rules_parquet_set_if_missing
):
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  parquet_staging:
    enabled: true
""")
    results = migrate_config.migrate(cfg, rules_parquet_set_if_missing)
    assert results[0].action == "applied"
    parsed = yaml.safe_load(cfg.read_text(encoding="utf-8"))
    assert parsed["scanner"]["parquet_staging"]["enabled"] is False


def test_set_if_missing_present_at_new_default_is_skipped(
    tmp_path, rules_parquet_set_if_missing
):
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  parquet_staging:
    enabled: false
""")
    results = migrate_config.migrate(cfg, rules_parquet_set_if_missing)
    assert results[0].action == "skipped"
    assert not list(tmp_path.glob("config.yaml.bak-*"))


def test_set_if_missing_scalar_ancestor_is_error(
    tmp_path, rules_parquet_set_if_missing
):
    # Non-canonical: parquet_staging written as a scalar, not a mapping.
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  parquet_staging: true
""")
    results = migrate_config.migrate(cfg, rules_parquet_set_if_missing)
    assert results[0].action == "error"
    # File left untouched.
    assert "parquet_staging: true" in cfg.read_text(encoding="utf-8")
    assert not list(tmp_path.glob("config.yaml.bak-*"))


def test_set_if_missing_dry_run_does_not_write(
    tmp_path, rules_parquet_set_if_missing
):
    original = """\
scanner:
  enrich_sizes: true
"""
    cfg = _write(tmp_path / "config.yaml", original)
    results = migrate_config.migrate(cfg, rules_parquet_set_if_missing, dry_run=True)
    assert results[0].action == "inserted"  # would-insert
    assert cfg.read_text(encoding="utf-8") == original
    assert not list(tmp_path.glob("config.yaml.bak-*"))


def test_set_if_missing_is_idempotent(tmp_path, rules_parquet_set_if_missing):
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  enrich_sizes: true
""")
    first = migrate_config.migrate(cfg, rules_parquet_set_if_missing)
    assert first[0].action == "inserted"
    # Re-run: key now present at new_default -> skipped, no further write.
    second = migrate_config.migrate(cfg, rules_parquet_set_if_missing)
    assert second[0].action == "skipped"
    parsed = yaml.safe_load(cfg.read_text(encoding="utf-8"))
    assert parsed["scanner"]["parquet_staging"]["enabled"] is False


# ---------------------------------------------------------------------------
# Shipped set_if_missing rules — the real #8/#9 + #1 customer scenario
# ---------------------------------------------------------------------------


def test_shipped_parquet_rule_inserts_when_absent(tmp_path):
    """A preserved config that never carried scanner.parquet_staging must
    get enabled:false inserted (the #8/#9 lock-source auto-disable)."""
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  enrich_sizes: true
  size_enrich_workers: 8
""")
    rules = migrate_config.load_rules()
    results = migrate_config.migrate(cfg, rules)
    by_path = {r.path: r for r in results}
    assert by_path["scanner.parquet_staging.enabled"].action == "inserted"
    parsed = yaml.safe_load(cfg.read_text(encoding="utf-8"))
    assert parsed["scanner"]["parquet_staging"]["enabled"] is False


def test_shipped_read_owner_rule_inserts_true_when_absent(tmp_path):
    """A preserved config missing scanner.read_owner must get true inserted
    so a rescan actually collects owners (issue #1)."""
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  enrich_sizes: true
""")
    rules = migrate_config.load_rules()
    results = migrate_config.migrate(cfg, rules)
    by_path = {r.path: r for r in results}
    assert by_path["scanner.read_owner"].action == "inserted"
    parsed = yaml.safe_load(cfg.read_text(encoding="utf-8"))
    assert parsed["scanner"]["read_owner"] is True


def test_shipped_read_owner_explicit_false_is_preserved(tmp_path):
    """An operator who explicitly disabled read_owner keeps it off — the
    read_owner rule is insert-only, never a flip."""
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  read_owner: false
""")
    rules = migrate_config.load_rules()
    results = migrate_config.migrate(cfg, rules)
    by_path = {r.path: r for r in results}
    assert by_path["scanner.read_owner"].action == "skipped"
    parsed = yaml.safe_load(cfg.read_text(encoding="utf-8"))
    assert parsed["scanner"]["read_owner"] is False


def test_shipped_rules_both_insert_under_existing_scanner(tmp_path):
    """The customer's real case: scanner exists but lacks BOTH keys. Both
    inserts must land under the same scanner block (no duplicate scanner)."""
    cfg = _write(tmp_path / "config.yaml", """\
scanner:
  enrich_sizes: true
dashboard:
  port: 8085
""")
    rules = migrate_config.load_rules()
    results = migrate_config.migrate(cfg, rules)
    by_path = {r.path: r for r in results}
    assert by_path["scanner.parquet_staging.enabled"].action == "inserted"
    assert by_path["scanner.read_owner"].action == "inserted"
    text = cfg.read_text(encoding="utf-8")
    assert text.count("scanner:") == 1, "must not create a duplicate scanner block"
    parsed = yaml.safe_load(text)
    assert parsed["scanner"]["parquet_staging"]["enabled"] is False
    assert parsed["scanner"]["read_owner"] is True
    assert parsed["scanner"]["enrich_sizes"] is True
    assert parsed["dashboard"]["port"] == 8085


def test_shipped_rules_applied_against_shipped_config_is_noop(tmp_path):
    """The shipped config.yaml is the post-migration state by definition.
    Running the migrator against a copy of the shipped config must
    produce ZERO 'applied' results."""
    import shutil as _shutil
    shipped = Path(__file__).resolve().parent.parent / "config.yaml"
    cfg = tmp_path / "config.yaml"
    _shutil.copy(shipped, cfg)
    rules = migrate_config.load_rules()
    results = migrate_config.migrate(cfg, rules)
    changed = [r for r in results if r.action in ("applied", "inserted")]
    assert changed == [], (
        f"shipped config.yaml triggers migrations against shipped rules: {changed}"
    )
