"""Smoke tests for issue #81 — Security dashboard pages.

Three pages live under the "Guvenlik" sidebar group:

* Orphan SIDs        (XLSX export added)
* Ransomware Alerts  (XLSX export + bulk acknowledge added)
* ACL Analyzer       (XLSX export for sprawl + per-trustee view added)

These tests boot the real ``create_app(...)`` factory against a tmp
SQLite, seed only the rows each endpoint needs, and exercise the new
endpoints through an in-process FastAPI ``TestClient``. We deliberately
keep the seeded data tiny — the goal is shape + content-type + non-zero
sheet, not full coverage of the underlying analyzers (those have their
own dedicated suites: ``test_orphan_sid.py``, ``test_ransomware_detector.py``,
``test_acl_analyzer.py``).
"""

from __future__ import annotations

import io
import os
import sys

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.dashboard.api import create_app  # noqa: E402
from src.storage.database import Database  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class _StubADLookup:
    """ADLookup-compatible stub. Anything in ``orphans`` is reported as not
    found — used by OrphanSidAnalyzer to flag a SID as orphaned. Everything
    else resolves cleanly.
    """

    def __init__(self, orphans=()):
        self.orphans = set(orphans)

    def lookup(self, name, force_refresh=False):  # noqa: D401 - stub
        if name in self.orphans:
            return {"username": name, "email": None, "display_name": None,
                    "found": False, "source": "live"}
        return {"username": name, "email": f"{name}@x", "display_name": name,
                "found": True, "source": "live"}


class _StubEmailNotifier:
    """EmailNotifier replacement — never sends, never fails."""

    def __init__(self):
        self.available = False

    def send(self, *a, **kw):  # noqa: D401 - stub
        return False



_BASE_CONFIG = {
    # Issue #158 C-1: dashboard auth defaults ON. TestClient's
    # ``client.host`` is the literal "testclient" which isn't on the
    # localhost bypass list, so every endpoint would 401. Disable auth
    # here so the integration tests below drive endpoints without
    # juggling Bearer tokens — auth itself is covered by
    # ``tests/test_dashboard_auth.py``.
    "dashboard": {"auth": {"enabled": False}},
    "security": {
        "ransomware": {
            "enabled": True,
            "rename_velocity_threshold": 50,
            "rename_velocity_window": 60,
            "deletion_velocity_threshold": 100,
            "deletion_velocity_window": 60,
            "risky_new_extensions": ["encrypted"],
            "canary_file_names": ["_AAAA_canary_DO_NOT_DELETE.txt"],
            "auto_kill_session": False,
            "notification_email": "",
        },
        "orphan_sid": {
            "enabled": True,
            "cache_ttl_minutes": 1440,
            "max_unique_sids": 1000,
            "require_dual_approval_for_reassign": False,
        },
    },
    "analytics": {},
}


@pytest.fixture
def client(tmp_path):
    """Boot a real ``create_app`` against a tmp SQLite + stubbed deps.

    Returns ``(client, db, source_id, scan_id)`` so each test can seed
    additional rows as needed.
    """
    db_path = tmp_path / "sec.db"
    db = Database({"path": str(db_path)})
    db.connect()

    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (name, unc_path) VALUES ('s1', '/share')"
        )
        source_id = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs (source_id, status) VALUES (?, 'completed')",
            (source_id,),
        )
        scan_id = cur.lastrowid
        # Two owners — alice will be flagged orphan via the stub AD lookup.
        rows = []
        for i in range(3):
            rows.append((source_id, scan_id, f"/share/alice/f{i}.txt",
                         f"alice/f{i}.txt", f"f{i}.txt", "txt", 100,
                         "DOMAIN\\alice"))
        for i in range(2):
            rows.append((source_id, scan_id, f"/share/bob/f{i}.txt",
                         f"bob/f{i}.txt", f"f{i}.txt", "txt", 200,
                         "DOMAIN\\bob"))
        cur.executemany(
            """INSERT INTO scanned_files
               (source_id, scan_id, file_path, relative_path, file_name,
                extension, file_size, owner)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )

    app = create_app(
        db,
        _BASE_CONFIG,
        ad_lookup=_StubADLookup(orphans=["DOMAIN\\alice"]),
        email_notifier=_StubEmailNotifier(),
    )
    return TestClient(app), db, source_id, scan_id


# ---------------------------------------------------------------------------
# /api/security/feature-flags
# ---------------------------------------------------------------------------


def test_feature_flags_endpoint(client):
    c, _db, _src, _scan = client
    r = c.get("/api/security/feature-flags")
    assert r.status_code == 200
    body = r.json()
    assert body["ransomware"]["enabled"] is True
    assert body["orphan_sid"]["enabled"] is True
    assert "acl" in body


# ---------------------------------------------------------------------------
# Orphan SIDs
# ---------------------------------------------------------------------------


_XLSX_MIME = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)


def _assert_xlsx(resp):
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith(_XLSX_MIME)
    body = resp.content
    # XLSX is a ZIP archive — magic bytes 'PK\x03\x04'.
    assert body[:2] == b"PK", "response is not a ZIP/XLSX payload"
    assert len(body) > 200, "XLSX should not be near-empty"


def test_orphan_sids_report(client):
    c, _db, source_id, _scan = client
    r = c.get(f"/api/security/orphan-sids/{source_id}")
    assert r.status_code == 200
    body = r.json()
    assert body["source_id"] == source_id
    sids = {row["sid"] for row in body["orphan_sids"]}
    assert "DOMAIN\\alice" in sids
    assert "DOMAIN\\bob" not in sids  # bob resolves


def test_orphan_sids_xlsx_export(client):
    c, _db, source_id, _scan = client
    r = c.get(f"/api/security/orphan-sids/{source_id}/export.xlsx")
    _assert_xlsx(r)
    # Check filename hint on Content-Disposition
    cd = r.headers.get("content-disposition", "")
    assert f"orphan_sids_source{source_id}" in cd


# ---------------------------------------------------------------------------
# Ransomware
# ---------------------------------------------------------------------------


def _seed_ransomware_alert(db, *, rule="rename_velocity", severity="critical",
                            ack=False):
    with db.get_cursor() as cur:
        cur.execute(
            """INSERT INTO ransomware_alerts
               (rule_name, severity, source_id, username, file_count,
                sample_paths, acknowledged_at, acknowledged_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                rule, severity, 1, "attacker", 5,
                '["/share/x.encrypted"]',
                ("2024-01-01 00:00:00" if ack else None),
                ("admin" if ack else None),
            ),
        )
        return cur.lastrowid


def test_ransomware_alerts_list(client):
    c, db, _src, _scan = client
    aid = _seed_ransomware_alert(db)
    r = c.get("/api/security/ransomware/alerts?since_minutes=1440")
    assert r.status_code == 200
    body = r.json()
    ids = [a["id"] for a in body]
    assert aid in ids


def test_ransomware_alerts_xlsx_export(client):
    c, db, _src, _scan = client
    _seed_ransomware_alert(db)
    r = c.get("/api/security/ransomware/alerts/export.xlsx?since_minutes=1440")
    _assert_xlsx(r)


def test_ransomware_acknowledge_all(client):
    c, db, _src, _scan = client
    a1 = _seed_ransomware_alert(db, rule="rename_velocity")
    a2 = _seed_ransomware_alert(db, rule="risky_extension")
    # One pre-acknowledged should be skipped.
    a3 = _seed_ransomware_alert(db, rule="mass_deletion", ack=True)

    r = c.post("/api/security/ransomware/alerts/acknowledge-all"
               "?by_user=ops&since_minutes=1440")
    assert r.status_code == 200
    body = r.json()
    assert body["acknowledged"] is True
    # a1 + a2 are touched; a3 already had acknowledged_at set.
    assert body["rows_updated"] >= 2

    with db.get_cursor() as cur:
        cur.execute(
            "SELECT id, acknowledged_by FROM ransomware_alerts "
            "WHERE id IN (?, ?, ?) ORDER BY id",
            (a1, a2, a3),
        )
        rows = {row["id"]: row["acknowledged_by"] for row in cur.fetchall()}
    assert rows[a1] == "ops"
    assert rows[a2] == "ops"
    # a3 untouched (was already acknowledged before the bulk call).
    assert rows[a3] == "admin"


# ---------------------------------------------------------------------------
# ACL Analyzer
# ---------------------------------------------------------------------------


def _seed_acl_snapshot(db, *, scan_id, trustee_sid="S-1-5-21-T",
                       trustee_name="trusty", file_path="/share/file.txt",
                       mask=0x001F01FF, ace_type="ALLOW"):
    with db.get_cursor() as cur:
        cur.execute(
            """INSERT INTO file_acl_snapshots
               (scan_id, file_path, trustee_sid, trustee_name,
                permissions_mask, permission_name, is_inherited, ace_type)
               VALUES (?,?,?,?,?,?,?,?)""",
            (scan_id, file_path, trustee_sid, trustee_name,
             mask, "FullControl", 0, ace_type),
        )


def test_acl_sprawl_xlsx_export(client):
    c, db, _src, scan_id = client
    # Seed three snapshots for the same trustee so detect_sprawl returns it.
    for i in range(3):
        _seed_acl_snapshot(db, scan_id=scan_id,
                            file_path=f"/share/wide_{i}.txt")
    r = c.get("/api/security/acl/sprawl/export.xlsx?severity_threshold=1")
    _assert_xlsx(r)


def test_acl_trustee_paths_xlsx_export(client):
    c, db, _src, scan_id = client
    _seed_acl_snapshot(db, scan_id=scan_id, trustee_sid="S-1-5-21-X",
                       file_path="/share/x.txt")
    r = c.get("/api/security/acl/trustee/S-1-5-21-X/paths/export.xlsx?limit=10")
    _assert_xlsx(r)


def test_acl_sprawl_json(client):
    c, db, _src, scan_id = client
    _seed_acl_snapshot(db, scan_id=scan_id, trustee_sid="S-1-5-21-Y",
                       file_path="/share/y.txt")
    r = c.get("/api/security/acl/sprawl?severity_threshold=1")
    assert r.status_code == 200
    body = r.json()
    assert "trustees" in body
