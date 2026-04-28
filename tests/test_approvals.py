"""Tests for issue #112: two-person approval framework (Phase 1).

Coverage:
  * is_required short-circuits when approvals.enabled=false.
  * is_required gates only ops in require_for.
  * request creates a pending row + audit event.
  * approve refuses self-approval (B == A).
  * approve succeeds for a different user.
  * execute only after approve (refuses pending/rejected).
  * reject blocks execute.
  * expire_stale flips old pending rows to 'expired'.
  * Snapshot restore endpoint routes through approval when enabled.
  * Snapshot restore endpoint executes immediately when disabled
    (default — backwards compat).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.database import Database  # noqa: E402
from src.security.approvals import (  # noqa: E402
    ApprovalRegistry,
    SelfApprovalError,
    InvalidStateError,
    ApprovalNotFound,
)


# ── Fixtures ───────────────────────────────────────────────


def _make_db(tmp_path) -> Database:
    db = Database({"path": str(tmp_path / "approvals.db")})
    db.connect()
    return db


def _make_registry(tmp_path, **overrides) -> tuple[Database, ApprovalRegistry]:
    db = _make_db(tmp_path)
    cfg = {
        "approvals": {
            "enabled": True,
            "require_for": ["snapshot_restore"],
            "expiry_hours": 24,
            # Issue #158 H-2: enabled=true + identity_source=
            # 'client_supplied' is now refused at construction. Tests
            # that don't care about identity routing use 'windows'
            # which falls back to env vars on Linux CI runners.
            "identity_source": "windows",
        }
    }
    cfg["approvals"].update(overrides)
    return db, ApprovalRegistry(db, cfg)


# ── is_required ────────────────────────────────────────────


def test_approval_disabled_returns_false_for_is_required(tmp_path):
    db = _make_db(tmp_path)
    reg = ApprovalRegistry(db, {"approvals": {"enabled": False,
                                              "require_for": ["snapshot_restore"]}})
    assert reg.is_required("snapshot_restore") is False
    assert reg.is_required("anything_else") is False


def test_is_required_only_for_listed_ops(tmp_path):
    db, reg = _make_registry(tmp_path)
    assert reg.is_required("snapshot_restore") is True
    assert reg.is_required("archive_bulk") is False


# ── request ────────────────────────────────────────────────


def test_request_creates_pending_row(tmp_path):
    db, reg = _make_registry(tmp_path)
    req = reg.request("snapshot_restore", {"snapshot_id": "snap-001"}, "alice")
    assert req.id > 0
    assert req.status == "pending"
    assert req.operation_type == "snapshot_restore"
    assert req.requested_by == "alice"
    assert req.payload == {"snapshot_id": "snap-001"}
    assert req.expires_at  # populated

    with db.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM pending_approvals")
        assert cur.fetchone()["c"] == 1
        cur.execute(
            "SELECT COUNT(*) AS c FROM file_audit_events "
            "WHERE event_type='approval_requested'"
        )
        assert cur.fetchone()["c"] == 1


# ── approve ────────────────────────────────────────────────


def test_approve_refuses_self_approval(tmp_path):
    db, reg = _make_registry(tmp_path)
    req = reg.request("snapshot_restore", {"snapshot_id": "s"}, "alice")
    with pytest.raises(SelfApprovalError):
        reg.approve(req.id, "alice")
    # Row must still be pending after refusal.
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT status FROM pending_approvals WHERE id=?", (req.id,)
        )
        assert cur.fetchone()["status"] == "pending"


def test_approve_succeeds_for_different_user(tmp_path):
    db, reg = _make_registry(tmp_path)
    req = reg.request("snapshot_restore", {"snapshot_id": "s"}, "alice")
    out = reg.approve(req.id, "bob")
    assert out.status == "approved"
    assert out.approved_by == "bob"
    assert out.approved_at
    # Audit event recorded.
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM file_audit_events "
            "WHERE event_type='approval_approved'"
        )
        assert cur.fetchone()["c"] == 1


def test_approve_unknown_id_raises(tmp_path):
    db, reg = _make_registry(tmp_path)
    with pytest.raises(ApprovalNotFound):
        reg.approve(9999, "bob")


# ── execute ────────────────────────────────────────────────


def test_execute_only_after_approve(tmp_path):
    db, reg = _make_registry(tmp_path)
    req = reg.request("snapshot_restore", {"snapshot_id": "s1"}, "alice")
    # Execute before approve refuses.
    with pytest.raises(InvalidStateError):
        reg.execute(req.id, lambda payload: {"ran": True})
    # Approve, then execute succeeds.
    reg.approve(req.id, "bob")
    captured = {}

    def _executor(payload):
        captured.update(payload)
        return {"ok": True, "snapshot_id": payload["snapshot_id"]}

    result = reg.execute(req.id, _executor)
    assert result == {"ok": True, "snapshot_id": "s1"}
    assert captured == {"snapshot_id": "s1"}
    # Re-execute on already-executed row refuses.
    with pytest.raises(InvalidStateError):
        reg.execute(req.id, _executor)


def test_reject_blocks_execute(tmp_path):
    db, reg = _make_registry(tmp_path)
    req = reg.request("snapshot_restore", {"snapshot_id": "s"}, "alice")
    out = reg.reject(req.id, "bob", "looks wrong")
    assert out.status == "rejected"
    assert out.rejected_by == "bob"
    assert out.rejection_reason == "looks wrong"
    with pytest.raises(InvalidStateError):
        reg.execute(req.id, lambda p: {"ok": True})


# ── expire_stale ───────────────────────────────────────────


def test_expire_stale_marks_old_pending_as_expired(tmp_path):
    db, reg = _make_registry(tmp_path)
    req = reg.request("snapshot_restore", {"snapshot_id": "s"}, "alice")
    # Manually backdate expires_at into the past.
    past = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    with db.get_cursor() as cur:
        cur.execute(
            "UPDATE pending_approvals SET expires_at=? WHERE id=?",
            (past, req.id),
        )
    # Plus one fresh row that should NOT expire.
    fresh = reg.request("snapshot_restore", {"snapshot_id": "s2"}, "alice")
    n = reg.expire_stale()
    assert n == 1
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT status FROM pending_approvals WHERE id=?", (req.id,)
        )
        assert cur.fetchone()["status"] == "expired"
        cur.execute(
            "SELECT status FROM pending_approvals WHERE id=?", (fresh.id,)
        )
        assert cur.fetchone()["status"] == "pending"


# ── Endpoint integration ───────────────────────────────────


def _make_app(tmp_path, *, enabled=False, require_for=None):
    """Build a real FastAPI app pointed at a tmp DB so we can hit the
    snapshot-restore endpoint with TestClient. Stubs the BackupManager
    so we don't need a real snapshot file."""
    import importlib

    db = _make_db(tmp_path)
    # Issue #158 H-2: enabled=true + identity_source='client_supplied'
    # is now refused at boot. Tests that exercise the approval-routed
    # path must use a safe identity_source; keeping this branch in the
    # fixture (instead of in every caller) avoids cascading edits.
    if enabled:
        identity_source = "header"
    else:
        # When approvals are disabled the registry doesn't enforce the
        # safety check, so we can keep the legacy "client_supplied"
        # value to mirror the legacy default config and prove the
        # endpoint still falls through to immediate execution.
        identity_source = "client_supplied"
    cfg = {
        "database": {"path": db.db_path},
        "backup": {"enabled": False},  # we stub the manager below
        # Issue #158 C-1: dashboard auth defaults ON. Disable it for
        # the integration tests since we want to drive the snapshot
        # endpoints directly with TestClient (no Bearer header).
        "dashboard": {"auth": {"enabled": False}},
        "approvals": {
            "enabled": enabled,
            "require_for": list(require_for or []),
            "expiry_hours": 24,
            "identity_source": identity_source,
            "identity_header": "X-Forwarded-User",
        },
        "analytics": {"enabled": False},
        "active_directory": {"enabled": False},
        "smtp": {"enabled": False},
        "audit": {"chain_enabled": False},
    }

    api = importlib.import_module("src.dashboard.api")
    app = api.create_app(db, cfg)

    # Replace the backup manager with a stub that captures restore() calls.
    class _StubManager:
        enabled = True
        backup_dir = "/tmp"
        keep_last_n = 1
        keep_weekly = 1
        restored_ids = []

        def list_snapshots(self):
            return []

        def restore(self, snapshot_id):
            self.restored_ids.append(snapshot_id)

    stub = _StubManager()
    app.state.backup_manager = stub
    return app, stub


def test_snapshot_restore_executes_immediately_when_disabled(tmp_path):
    """Default config (approvals.enabled=false) must preserve legacy
    behaviour: the endpoint runs the restore right away."""
    from fastapi.testclient import TestClient

    app, stub = _make_app(tmp_path, enabled=False)
    client = TestClient(app, raise_server_exceptions=False)
    r = client.post(
        "/api/system/backups/restore/snap-xyz",
        # Audit M-3: safety_token now required alongside confirm.
        json={"confirm": True, "safety_token": "RESTORE"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body == {"ok": True, "restored": "snap-xyz"}
    assert stub.restored_ids == ["snap-xyz"]


def test_snapshot_restore_routes_through_approval_when_enabled(tmp_path):
    """When approvals.enabled=true and snapshot_restore is required,
    the endpoint must queue a pending row and NOT call restore()."""
    from fastapi.testclient import TestClient

    app, stub = _make_app(
        tmp_path, enabled=True, require_for=["snapshot_restore"]
    )
    client = TestClient(app, raise_server_exceptions=False)

    # Step 1: alice requests the restore — gets a pending id back.
    # Issue #158 H-2: identity_source is now 'header' (the safe value
    # for tests that flip enabled=true), so we send the requester's
    # name via X-Forwarded-User instead of body['username'].
    r = client.post(
        "/api/system/backups/restore/snap-abc",
        # Audit M-3: safety_token now required alongside confirm.
        # H-2 (#159): identity_source 'header', so requester via X-Forwarded-User.
        json={"confirm": True, "safety_token": "RESTORE"},
        headers={"X-Forwarded-User": "alice"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "pending"
    assert body["requested_by"] == "alice"
    assert "pending_approval_id" in body
    pid = body["pending_approval_id"]
    # Restore must NOT have run yet.
    assert stub.restored_ids == []

    # Step 2: alice tries to self-approve — refused.
    r2 = client.post(
        f"/api/approvals/{pid}/approve",
        json={"approved_by": "alice"},
    )
    assert r2.status_code == 403, r2.text

    # Step 3: bob approves — succeeds.
    r3 = client.post(
        f"/api/approvals/{pid}/approve",
        json={"approved_by": "bob"},
    )
    assert r3.status_code == 200, r3.text
    assert r3.json()["approval"]["status"] == "approved"

    # Step 4: execute runs the restore.
    r4 = client.post(f"/api/approvals/{pid}/execute", json={})
    assert r4.status_code == 200, r4.text
    out = r4.json()
    assert out["ok"] is True
    assert out["result"]["restored"] == "snap-abc"
    assert stub.restored_ids == ["snap-abc"]

    # History reflects the executed row.
    r5 = client.get("/api/approvals/history?limit=10")
    assert r5.status_code == 200
    rows = r5.json()["rows"]
    assert any(row["id"] == pid and row["status"] == "executed" for row in rows)
