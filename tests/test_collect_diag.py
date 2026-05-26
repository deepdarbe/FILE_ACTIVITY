"""Tests for scripts/collect_diag.py — the diagnostics bundle collector.

Covers: snapshot structure, secret redaction, owner-resolution ratio against
a real SQLite Database, key-flag surfacing, log tail, markdown render, the
in-memory zip, and graceful behaviour when the DB file is missing (the tool
must still produce a bundle when the DB is the thing that is broken).
"""

import io
import os
import sys
import zipfile

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import scripts.collect_diag as cd  # noqa: E402
from src.storage.database import Database  # noqa: E402


def _seed_db(tmp_path, owners):
    """Build a Database with one source/scan and ``owners`` rows.

    ``owners`` is a list of owner values (None / '' = unresolved).
    Returns (db, db_path).
    """
    db_path = tmp_path / "diag.db"
    db = Database({"path": str(db_path)})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources (name, unc_path) VALUES ('ortak', '/share')")
        source_id = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs (source_id, status, total_files) "
            "VALUES (?, 'completed', ?)",
            (source_id, len(owners)),
        )
        scan_id = cur.lastrowid
        rows = [
            (source_id, scan_id, f"/share/f{i}.txt", f"f{i}.txt", f"f{i}.txt",
             "txt", 100, owner)
            for i, owner in enumerate(owners)
        ]
        cur.executemany(
            "INSERT INTO scanned_files (source_id, scan_id, file_path, "
            "relative_path, file_name, extension, file_size, owner) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    return db, db_path


def _config(tmp_path, db_path, log_path):
    return {
        "general": {"log_file": str(log_path)},
        "database": {"path": str(db_path)},
        "scanner": {"read_owner": True, "parquet_staging": {"enabled": False}},
        "audit": {"chain_enabled": False},
        "mail": {"smtp_password": "hunter2", "api_token": "secret-abc", "host": "mail.local"},
    }


# ---------------------------------------------------------------------------
# redaction
# ---------------------------------------------------------------------------
def test_redaction_masks_secret_like_keys():
    src = {
        "smtp_password": "x", "api_token": "y", "client_secret": "z",
        "host": "mail.local", "port": 25,
        "nested": {"bind_password": "p", "name": "svc"},
    }
    out = cd._redact_config(src)
    assert out["smtp_password"] == cd._REDACTED
    assert out["api_token"] == cd._REDACTED
    assert out["client_secret"] == cd._REDACTED
    assert out["nested"]["bind_password"] == cd._REDACTED
    # non-secret values survive
    assert out["host"] == "mail.local"
    assert out["port"] == 25
    assert out["nested"]["name"] == "svc"


# ---------------------------------------------------------------------------
# collect() structure + owner resolution
# ---------------------------------------------------------------------------
def test_collect_structure_and_flags(tmp_path):
    db, db_path = _seed_db(tmp_path, ["CORP\\alice", "bob", None, ""])
    log_path = tmp_path / "app.log"
    log_path.write_text("line1\nline2\nERROR boom\n", encoding="utf-8")
    config = _config(tmp_path, db_path, log_path)

    diag = cd.collect(db, config, log_lines=50, redact=True)

    assert set(diag) >= {"meta", "environment", "key_flags", "config", "log", "database"}
    assert diag["meta"]["redacted"] is True
    # key flags surfaced verbatim from config
    assert diag["key_flags"]["scanner.read_owner"] is True
    assert diag["key_flags"]["scanner.parquet_staging.enabled"] is False
    # secrets masked inside the embedded config
    assert diag["config"]["mail"]["smtp_password"] == cd._REDACTED
    assert diag["config"]["mail"]["host"] == "mail.local"
    db.close()


def test_owner_resolution_ratio(tmp_path):
    db, db_path = _seed_db(tmp_path, ["CORP\\alice", "bob", None, "", None])
    log_path = tmp_path / "app.log"
    log_path.write_text("x\n", encoding="utf-8")
    diag = cd.collect(db, _config(tmp_path, db_path, log_path), redact=True)

    sources = diag["database"]["sources"]
    assert len(sources) == 1
    owner = sources[0]["owner_resolution"]
    assert owner["rows"] == 5
    assert owner["unresolved"] == 3       # None, '', None
    assert owner["resolved"] == 2         # alice, bob
    assert owner["unresolved_pct"] == 60.0
    # redacted: no sample owner names leaked
    assert "top_owners" not in owner
    db.close()


def test_owner_samples_only_when_not_redacted(tmp_path):
    db, db_path = _seed_db(tmp_path, ["alice", "alice", "bob", None])
    log_path = tmp_path / "app.log"
    log_path.write_text("x\n", encoding="utf-8")
    diag = cd.collect(db, _config(tmp_path, db_path, log_path), redact=False)

    owner = diag["database"]["sources"][0]["owner_resolution"]
    assert "top_owners" in owner
    names = {row["owner"] for row in owner["top_owners"]}
    assert names == {"alice", "bob"}
    # unc_path is surfaced only without redaction
    assert diag["database"]["sources"][0].get("unc_path") == "/share"
    db.close()


# ---------------------------------------------------------------------------
# log tail
# ---------------------------------------------------------------------------
def test_tail_returns_last_n_lines(tmp_path):
    log_path = tmp_path / "big.log"
    log_path.write_text("\n".join(f"line{i}" for i in range(1000)), encoding="utf-8")
    text, meta = cd._tail(str(log_path), 10)
    lines = text.splitlines()
    assert lines[-1] == "line999"
    assert len(lines) == 10
    assert meta["exists"] is True


def test_tail_missing_file():
    text, meta = cd._tail("/no/such/file.log", 10)
    assert text == ""
    assert meta["exists"] is False


# ---------------------------------------------------------------------------
# render + bundle
# ---------------------------------------------------------------------------
def test_render_markdown_has_key_sections(tmp_path):
    db, db_path = _seed_db(tmp_path, ["alice", None])
    log_path = tmp_path / "app.log"
    log_path.write_text("hello\n", encoding="utf-8")
    diag = cd.collect(db, _config(tmp_path, db_path, log_path))
    md = cd.render_markdown(diag)
    assert "Domain-joined" in md
    assert "Owner resolution" in md
    assert "Key config flags" in md
    db.close()


def test_build_bundle_bytes_is_valid_zip(tmp_path):
    db, db_path = _seed_db(tmp_path, ["alice"])
    log_path = tmp_path / "app.log"
    log_path.write_text("hello\nworld\n", encoding="utf-8")
    diag = cd.collect(db, _config(tmp_path, db_path, log_path))
    md = cd.render_markdown(diag)
    payload = cd.build_bundle_bytes(diag, md)

    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        names = set(zf.namelist())
        assert {"report.md", "diag.json", "log_tail.txt"} <= names
        assert b"Domain-joined" in zf.read("report.md")
    db.close()


# ---------------------------------------------------------------------------
# robustness: the DB is the thing that's broken
# ---------------------------------------------------------------------------
def test_collect_survives_missing_db(tmp_path):
    log_path = tmp_path / "app.log"
    log_path.write_text("only the log survived\n", encoding="utf-8")
    config = _config(tmp_path, tmp_path / "does-not-exist.db", log_path)
    db = Database({"path": str(tmp_path / "does-not-exist.db")})

    diag = cd.collect(db, config)  # must not raise

    assert diag["database"]["errors"]            # recorded, not crashed
    assert diag["log"]["tail"].startswith("only the log survived")
    assert diag["environment"]["hostname"]


def test_upload_requires_repo_and_token():
    with pytest.raises(ValueError):
        cd.upload_to_github("body", "", "")


def test_resolve_github_from_telemetry(monkeypatch):
    monkeypatch.setenv("FILEACTIVITY_TELEMETRY_TOKEN", "tok-123")
    config = {
        "telemetry": {
            "github": {"repo": "o/r", "token_env": "FILEACTIVITY_TELEMETRY_TOKEN",
                       "label": "diag"},
            "privacy": {"redact_paths": True},
        }
    }
    repo, token, label, scrub = cd.resolve_github(config)
    assert repo == "o/r"
    assert token == "tok-123"
    assert label == "diag"
    assert scrub is True
    # explicit args win over config/env
    repo2, token2, _, _ = cd.resolve_github(config, "x/y", "argtok")
    assert (repo2, token2) == ("x/y", "argtok")


def test_scrub_paths_masks_unc_and_home():
    text = r"open \\fileserver\Finans\rapor.xlsx and C:\Users\ahmet\Desktop"
    out = cd._scrub_paths(text)
    assert "fileserver" not in out
    assert "Finans" not in out
    assert "ahmet" not in out
    assert "<redacted>" in out
