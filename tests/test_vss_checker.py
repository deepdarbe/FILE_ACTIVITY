"""Tests for #340 Faz 4 — VssChecker (VSS recoverability probe).

Pure logic only: parse_json (the PowerShell JSON shapes) and find_recoverable
with an injected shadow list + a monkeypatched os.path.exists. The actual
WMI/PowerShell subprocess is Windows-admin-only and validated on-box, not here.
"""

from __future__ import annotations

import importlib.util
import os

import pytest

from src.scanner.vss_checker import VssChecker

HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None
requires_fastapi = pytest.mark.skipif(
    not HAS_FASTAPI, reason="fastapi not installed in this environment")

_DEV = r"\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy7"


def test_parse_json_array():
    import json
    txt = json.dumps([{"device": _DEV, "drive": "E:",
                       "created": "2026-07-17 03:00:00"}])
    out = VssChecker.parse_json(txt)
    assert len(out) == 1 and out[0]["drive"] == "E:"


def test_parse_json_single_object_and_junk():
    # ConvertTo-Json emits a bare object (not a 1-element array) for one shadow.
    assert len(VssChecker.parse_json('{"device":"X","drive":"E:"}')) == 1
    assert VssChecker.parse_json("") == []
    assert VssChecker.parse_json(None) == []
    assert VssChecker.parse_json("not json") == []


def test_find_recoverable_hit(monkeypatch):
    shadows = [{"device": _DEV, "drive": "E:",
                "created": "2026-07-17 03:00:00"}]
    hit = _DEV + r"\ortak\a.xlsx"
    monkeypatch.setattr(os.path, "exists", lambda p: p == hit)
    r = VssChecker().find_recoverable(r"E:\ortak\a.xlsx", shadows=shadows)
    assert r["recoverable"] is True
    assert r["shadow_path"] == hit
    assert r["shadow_created"] == "2026-07-17 03:00:00"


def test_find_recoverable_miss(monkeypatch):
    # A shadow exists on E: but the file is not inside it → definitively False.
    shadows = [{"device": _DEV, "drive": "E:"}]
    monkeypatch.setattr(os.path, "exists", lambda p: False)
    r = VssChecker().find_recoverable(r"E:\ortak\gone.xlsx", shadows=shadows)
    assert r["recoverable"] is False


def test_no_shadow_on_volume_is_unknown():
    # Shadows exist, but none for E: → unknown (not a false "not recoverable").
    shadows = [{"device": _DEV, "drive": "C:"}]
    r = VssChecker().find_recoverable(r"E:\ortak\a.xlsx", shadows=shadows)
    assert r["recoverable"] is None


def test_unc_and_bad_paths_are_unknown():
    vc = VssChecker()
    assert vc.find_recoverable(r"\\fs\share\x", shadows=[])["recoverable"] is None
    assert vc.find_recoverable("", shadows=[])["recoverable"] is None
    assert vc.find_recoverable("E:", shadows=[])["recoverable"] is None       # no rel
    assert vc.find_recoverable("E:\\", shadows=[])["recoverable"] is None     # rel empty


def test_drive_match_is_case_insensitive(monkeypatch):
    shadows = [{"device": _DEV, "drive": "e:"}]
    monkeypatch.setattr(os.path, "exists", lambda p: True)
    r = VssChecker().find_recoverable(r"E:\x.txt", shadows=shadows)
    assert r["recoverable"] is True


def test_traversal_is_refused(monkeypatch):
    # Even with a matching shadow and exists()->True, a '..' segment must abort
    # the lookup (defence in depth against path traversal / py/path-injection).
    shadows = [{"device": _DEV, "drive": "E:"}]
    monkeypatch.setattr(os.path, "exists", lambda p: True)
    r = VssChecker().find_recoverable(r"E:\..\..\Windows\win.ini", shadows=shadows)
    assert r["recoverable"] is None


@requires_fastapi
def test_endpoint_resolves_uid_from_db(tmp_path):
    from fastapi.testclient import TestClient

    from src.dashboard.api import create_app
    from src.storage.database import Database

    db = Database({"path": str(tmp_path / "vss.db"),
                   "retention": {"auto_cleanup_on_startup": False}})
    db.connect()
    with db.get_cursor() as cur:
        cur.execute("INSERT INTO sources(name, unc_path) VALUES('e', 'x')")
        cur.execute(
            "INSERT INTO file_audit_events"
            "(source_id, event_time, event_type, username, file_path, file_name) "
            "VALUES(1, datetime('now'), 'delete', 'alice', ?, ?)",
            (r"E:\ortak\a.xlsx", "a.xlsx"))
    client = TestClient(create_app(db, {"dashboard": {"auth": {"enabled": False}}}))

    # Valid uid resolves the DB path; on Linux (no VSS) recoverable is None but
    # the endpoint shape + uid->path resolution are exercised.
    r = client.get("/api/forensic/recoverable?uid=fae:1")
    assert r.status_code == 200
    assert set(r.json()) == {"recoverable", "shadow_path", "shadow_created"}
    # Bad / unknown uid → unknown, never a 500.
    assert client.get("/api/forensic/recoverable?uid=garbage").json()["recoverable"] is None
    assert client.get("/api/forensic/recoverable?uid=fae:9999").json()["recoverable"] is None
    assert client.get("/api/forensic/recoverable?uid=ual:abc").json()["recoverable"] is None
    db.close()
