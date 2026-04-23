"""Tests for issue #37: ransomware detector.

Covers all four detection rules + DB persistence sanity. Uses an isolated
SQLite file under tmp_path so the suite is hermetic and parallel-safe.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.security.ransomware_detector import RansomwareDetector  # noqa: E402
from src.storage.database import Database  # noqa: E402


@pytest.fixture
def detector(tmp_path):
    db_path = tmp_path / "ransom.db"
    db = Database({"path": str(db_path)})
    db.connect()
    cfg = {
        "security": {
            "ransomware": {
                "enabled": True,
                "rename_velocity_threshold": 50,
                "rename_velocity_window": 60,
                "deletion_velocity_threshold": 100,
                "deletion_velocity_window": 60,
                # Trim defaults a touch so the test exercises an explicit list.
                "risky_new_extensions": ["encrypted", "locked", "wcry"],
                "canary_file_names": ["_AAAA_canary_DO_NOT_DELETE.txt"],
                "auto_kill_session": False,
                "notification_email": "",
            }
        }
    }
    return RansomwareDetector(db, cfg), db


def _fire_renames(detector, n, *, user="alice", source=1, base_ts=None):
    """Inject n rename events spaced 100ms apart inside the velocity window."""
    base_ts = base_ts or datetime.now()
    last = None
    for i in range(n):
        ts = base_ts + timedelta(milliseconds=i * 100)
        last = detector.consume_event({
            "timestamp": ts,
            "source_id": source,
            "username": user,
            "file_path": f"/share/data/file_{i}.txt.bak",
            "old_path": f"/share/data/file_{i}.txt",
            "event_type": "rename",
        }) or last
    return last


def test_rename_velocity_fires_above_threshold(detector):
    det, db = detector
    # 50 events should NOT fire (threshold is exclusive: > N).
    alert = _fire_renames(det, 50)
    assert alert is None

    # The 51st event puts us above the threshold.
    alert = _fire_renames(det, 1, base_ts=datetime.now())
    assert alert is not None
    assert alert["rule_name"] == "rename_velocity"
    assert alert["severity"] == "critical"
    assert alert["file_count"] >= 51
    assert alert["username"] == "alice"
    assert alert["sample_paths"], "should carry sample paths"
    assert len(alert["sample_paths"]) <= 20


def test_risky_extension_immediate_alert(detector):
    det, db = detector
    alert = det.consume_event({
        "source_id": 7,
        "username": "bob",
        "file_path": "/share/finance/budget.xlsx.encrypted",
        "event_type": "modify",
    })
    assert alert is not None
    assert alert["rule_name"] == "risky_extension"
    assert alert["severity"] == "critical"
    assert alert["details"]["extension"] == "encrypted"
    assert alert["sample_paths"] == ["/share/finance/budget.xlsx.encrypted"]


def test_canary_access_fires_immediately(detector):
    det, db = detector
    alert = det.consume_event({
        "source_id": 3,
        "username": "carol",
        "file_path": "/share/root/_AAAA_canary_DO_NOT_DELETE.txt",
        "event_type": "access",
    })
    assert alert is not None
    assert alert["rule_name"] == "canary_access"
    assert alert["severity"] == "critical"
    assert alert["file_count"] == 1
    assert "canary" in alert["details"]["message"].lower()


def test_mass_deletion_fires_above_threshold(detector):
    det, db = detector
    base_ts = datetime.now()
    last = None
    # 100 deletes -> no alert; 101st triggers.
    for i in range(101):
        ts = base_ts + timedelta(milliseconds=i * 50)
        last = det.consume_event({
            "timestamp": ts,
            "source_id": 2,
            "username": "dave",
            "file_path": f"/share/x/del_{i}.bin",
            "event_type": "delete",
        }) or last
    assert last is not None
    assert last["rule_name"] == "mass_deletion"
    assert last["severity"] == "critical"
    assert last["file_count"] >= 101


def test_db_persistence_and_get_active_alerts(detector):
    det, db = detector
    # Two distinct rules fired by two distinct users so cooldown does not
    # suppress the second insert.
    a1 = det.consume_event({
        "source_id": 1,
        "username": "u1",
        "file_path": "/share/x/f.encrypted",
        "event_type": "modify",
    })
    a2 = det.consume_event({
        "source_id": 2,
        "username": "u2",
        "file_path": "/share/y/_AAAA_canary_DO_NOT_DELETE.txt",
        "event_type": "access",
    })
    assert a1 and a2

    rows = det.get_active_alerts(since_minutes=60)
    assert len(rows) >= 2

    # Sample paths should round-trip through JSON.
    for r in rows:
        assert r["severity"] == "critical"
        assert r["rule_name"] in {"risky_extension", "canary_access"}
        assert isinstance(r["sample_paths"], list)
        assert r["sample_paths"]

    # Direct DB peek: confirm the row layout.
    with db.get_cursor() as cur:
        cur.execute("SELECT * FROM ransomware_alerts ORDER BY id ASC")
        raw = cur.fetchall()
    assert len(raw) >= 2
    first = raw[0]
    assert first["severity"] in {"critical", "warning", "info"}
    assert first["rule_name"]
    sp = json.loads(first["sample_paths"])
    assert isinstance(sp, list) and sp


def test_disabled_detector_is_noop(tmp_path):
    db = Database({"path": str(tmp_path / "off.db")})
    db.connect()
    det = RansomwareDetector(db, {"security": {"ransomware": {"enabled": False}}})
    out = det.consume_event({
        "source_id": 1, "username": "x",
        "file_path": "/share/y.encrypted", "event_type": "modify",
    })
    assert out is None


def test_dedupe_suppresses_repeat_alerts(detector):
    det, db = detector
    a1 = det.consume_event({
        "source_id": 1, "username": "alice",
        "file_path": "/share/x/a.encrypted", "event_type": "modify",
    })
    a2 = det.consume_event({
        "source_id": 1, "username": "alice",
        "file_path": "/share/x/b.encrypted", "event_type": "modify",
    })
    assert a1 is not None
    assert a2 is None  # cooldown swallowed it


def test_deploy_canaries_writes_files(detector, tmp_path):
    det, _db = detector
    share = tmp_path / "share"
    share.mkdir()
    placed = det.deploy_canaries(source_id=1, share_root=str(share))
    assert placed >= 1
    contents = sorted(p.name for p in share.iterdir())
    # At least the configured canary should be present.
    assert "_AAAA_canary_DO_NOT_DELETE.txt" in contents

    # Re-running is idempotent: count stays the same, no exception.
    placed2 = det.deploy_canaries(source_id=1, share_root=str(share))
    assert placed2 == placed


def test_smb_kill_is_safe_on_linux():
    """Sanity: smb_session module imports and returns the windows_only sentinel
    on non-Windows platforms — without ever invoking a subprocess."""
    from src.security.smb_session import kill_user_session
    out = kill_user_session("DOMAIN\\alice", dry_run=True)
    # On the CI box (Linux) we expect the windows_only branch.
    if out.get("error") == "windows_only":
        assert out["killed"] == 0
    else:
        # If somebody runs the suite on Windows, the result must still carry
        # the documented keys.
        assert "killed_session_ids" in out
        assert "dry_run" in out
