"""Tests for issue #158 H-2: refuse the unsafe approvals combo.

ApprovalRegistry must refuse construction when
``approvals.enabled=true`` AND ``identity_source='client_supplied'``.
That combo lets any caller claim the requester's username on the
second leg of the two-person rule, which defeats the gate entirely.

Coverage:
  * enabled=true + client_supplied -> RuntimeError at construction.
  * enabled=true + windows -> ok.
  * enabled=true + header -> ok.
  * enabled=false + client_supplied -> ok (no enforcement).
  * enabled=false + (any other identity_source) -> ok.
"""

from __future__ import annotations

import os
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.security.approvals import ApprovalRegistry  # noqa: E402
from src.storage.database import Database  # noqa: E402


def _db(tmp_path) -> Database:
    db = Database({"path": str(tmp_path / "approvals.db")})
    db.connect()
    return db


# ---------------------------------------------------------------------------
# Refusal path: the structurally-unsafe combo.
# ---------------------------------------------------------------------------


def test_enabled_with_client_supplied_raises(tmp_path):
    db = _db(tmp_path)
    cfg = {"approvals": {
        "enabled": True,
        "identity_source": "client_supplied",
        "require_for": ["snapshot_restore"],
    }}
    with pytest.raises(RuntimeError) as exc:
        ApprovalRegistry(db, cfg)
    msg = str(exc.value)
    assert "client_supplied" in msg
    assert "self-approval" in msg.lower() or "bypass" in msg.lower()


def test_enabled_with_default_identity_source_raises(tmp_path):
    """When ``identity_source`` is omitted entirely, identity.py defaults
    to ``client_supplied`` — same unsafe combo, must also be refused."""
    db = _db(tmp_path)
    cfg = {"approvals": {
        "enabled": True,
        "require_for": ["snapshot_restore"],
        # identity_source intentionally absent
    }}
    with pytest.raises(RuntimeError):
        ApprovalRegistry(db, cfg)


# ---------------------------------------------------------------------------
# Allowed combinations.
# ---------------------------------------------------------------------------


def test_enabled_with_windows_ok(tmp_path):
    db = _db(tmp_path)
    cfg = {"approvals": {
        "enabled": True,
        "identity_source": "windows",
        "require_for": ["snapshot_restore"],
    }}
    reg = ApprovalRegistry(db, cfg)
    assert reg.enabled is True
    assert reg.is_required("snapshot_restore") is True


def test_enabled_with_header_ok(tmp_path):
    db = _db(tmp_path)
    cfg = {"approvals": {
        "enabled": True,
        "identity_source": "header",
        "identity_header": "X-Forwarded-User",
        "require_for": ["snapshot_restore"],
    }}
    reg = ApprovalRegistry(db, cfg)
    assert reg.enabled is True


def test_disabled_with_client_supplied_ok(tmp_path):
    """When the framework is disabled we don't enforce the safety check
    — every existing endpoint runs straight through anyway, so the
    config is benign even if it would be unsafe to flip enabled=true."""
    db = _db(tmp_path)
    cfg = {"approvals": {
        "enabled": False,
        "identity_source": "client_supplied",
    }}
    reg = ApprovalRegistry(db, cfg)
    assert reg.enabled is False


def test_disabled_with_default_identity_source_ok(tmp_path):
    db = _db(tmp_path)
    cfg = {"approvals": {"enabled": False}}
    reg = ApprovalRegistry(db, cfg)
    assert reg.enabled is False


def test_disabled_with_windows_ok(tmp_path):
    db = _db(tmp_path)
    cfg = {"approvals": {
        "enabled": False,
        "identity_source": "windows",
    }}
    reg = ApprovalRegistry(db, cfg)
    assert reg.enabled is False


# ---------------------------------------------------------------------------
# Case-insensitivity / whitespace tolerance: configs in YAML often have
# stray casing. We normalise via strip().lower() so 'CLIENT_SUPPLIED'
# and ' client_supplied ' must also be refused.
# ---------------------------------------------------------------------------


def test_enabled_with_uppercase_client_supplied_raises(tmp_path):
    db = _db(tmp_path)
    cfg = {"approvals": {
        "enabled": True,
        "identity_source": "CLIENT_SUPPLIED",
    }}
    with pytest.raises(RuntimeError):
        ApprovalRegistry(db, cfg)


def test_enabled_with_padded_client_supplied_raises(tmp_path):
    db = _db(tmp_path)
    cfg = {"approvals": {
        "enabled": True,
        "identity_source": "  client_supplied  ",
    }}
    with pytest.raises(RuntimeError):
        ApprovalRegistry(db, cfg)
