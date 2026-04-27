"""Tests for ``src.storage.operations_tracker.OperationsRegistry`` (#125)."""

from __future__ import annotations

import time

import pytest

from src.storage.operations_tracker import (
    OperationStatus,
    OperationsRegistry,
)


def test_start_returns_op_id():
    reg = OperationsRegistry()
    op_id = reg.start("scan", "Tarama: \\\\fs01\\dept",
                      metadata={"source_id": 1})
    assert isinstance(op_id, str) and op_id
    active = reg.list_active()
    assert len(active) == 1
    assert active[0].op_id == op_id
    assert active[0].type == "scan"
    assert active[0].label == "Tarama: \\\\fs01\\dept"
    assert active[0].metadata == {"source_id": 1}
    # started_at must be a recent unix ts
    assert abs(active[0].started_at - time.time()) < 5


def test_finish_removes_from_active():
    reg = OperationsRegistry()
    a = reg.start("scan", "A")
    b = reg.start("analysis", "B")
    assert len(reg.list_active()) == 2
    reg.finish(a)
    remaining = reg.list_active()
    assert len(remaining) == 1
    assert remaining[0].op_id == b
    # Idempotent: finishing the same id twice is safe.
    reg.finish(a)
    # Unknown op_id is silent.
    reg.finish("does-not-exist")
    assert len(reg.list_active()) == 1


def test_progress_updates_pct_and_eta():
    reg = OperationsRegistry()
    op = reg.start("scan", "Tarama")
    reg.progress(op, pct=42, eta_seconds=120)
    [snap] = reg.list_active()
    assert snap.progress_pct == 42
    assert snap.eta_seconds == 120
    # New label flows through.
    reg.progress(op, label="Tarama (yeniden)")
    [snap2] = reg.list_active()
    assert snap2.label == "Tarama (yeniden)"
    # Out-of-range pct clamps to [0,100].
    reg.progress(op, pct=999)
    [snap3] = reg.list_active()
    assert snap3.progress_pct == 100
    reg.progress(op, pct=-50)
    [snap4] = reg.list_active()
    assert snap4.progress_pct == 0
    # Unknown id is a silent no-op (no raise).
    reg.progress("nope", pct=10)


def test_list_active_returns_only_running():
    reg = OperationsRegistry()
    assert reg.list_active() == []
    a = reg.start("scan", "A")
    time.sleep(0.01)
    b = reg.start("snapshot", "B")
    active = reg.list_active()
    # Sorted oldest-first
    assert [op.op_id for op in active] == [a, b]
    reg.finish(a)
    active2 = reg.list_active()
    assert [op.op_id for op in active2] == [b]
    reg.finish(b)
    assert reg.list_active() == []


def test_to_public_dict_is_json_safe():
    reg = OperationsRegistry()
    op = reg.start("snapshot", "DB anlik goruntu",
                   metadata={"reason": "manual"})
    [status] = reg.list_active()
    d = status.to_public_dict()
    assert d["type"] == "snapshot"
    assert d["label"] == "DB anlik goruntu"
    assert d["metadata"] == {"reason": "manual"}
    assert "started_at" in d
    assert d["op_id"] == op


def test_long_label_truncated():
    reg = OperationsRegistry()
    label = "x" * 1000
    reg.start("scan", label)
    [snap] = reg.list_active()
    assert len(snap.label) <= 200


def test_dataclass_default_metadata_isolated():
    """Defaulting metadata via field(default_factory=dict) means two
    OperationStatus objects must NOT share the same dict instance."""
    a = OperationStatus(type="scan", label="A", started_at=time.time())
    b = OperationStatus(type="scan", label="B", started_at=time.time())
    a.metadata["k"] = 1
    assert "k" not in b.metadata
