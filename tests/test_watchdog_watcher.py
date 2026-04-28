"""Tests for the event-driven WatchdogWatcher (issue #14).

Covers:
    * availability probe (Linux + watchdog installed)
    * lazy import (mocked ImportError)
    * file create / modify / delete callbacks
    * UNC path fallback selection
    * stop() idempotency
    * isolation between two watchers on different paths
    * burst coalescing via the debounce window
    * JSON serialisation of events
    * WatcherFactory backend selection (config / missing dep / default)
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from unittest import mock

import pytest

from src.scanner import watchdog_watcher as ww
from src.scanner.watchdog_watcher import (
    WatchdogWatcher,
    WatcherFactory,
    _is_unc_path,
)


# ─── Helpers ──────────────────────────────────────────────────────────


class _CallbackRecorder:
    def __init__(self) -> None:
        self.events: list[dict] = []
        self.cond = threading.Condition()

    def __call__(self, evt: dict) -> None:
        with self.cond:
            self.events.append(evt)
            self.cond.notify_all()

    def wait_for(self, n: int, timeout: float = 5.0) -> bool:
        end = time.monotonic() + timeout
        with self.cond:
            while len(self.events) < n:
                remaining = end - time.monotonic()
                if remaining <= 0:
                    return False
                self.cond.wait(timeout=remaining)
        return True


def _make_watcher(tmp_path, callback=None, debounce_ms: int = 50,
                  source_id: int = 1) -> WatchdogWatcher:
    return WatchdogWatcher(
        db=None,
        source_id=source_id,
        path=str(tmp_path),
        callback=callback,
        debounce_ms=debounce_ms,
    )


# ─── 1. Availability probe ────────────────────────────────────────────


@pytest.mark.skipif(not sys.platform.startswith("linux"),
                    reason="availability test pinned to Linux host")
def test_available_true_on_linux_with_watchdog(tmp_path):
    assert WatchdogWatcher.available(str(tmp_path)) is True


# ─── 2. Lazy import ───────────────────────────────────────────────────


def test_lazy_import_failure_marks_unavailable(monkeypatch):
    def _boom():
        raise ImportError("no watchdog here")
    monkeypatch.setattr(ww, "_import_watchdog", _boom)
    assert WatchdogWatcher.available("/tmp") is False


# ─── 3. Create / 4. Modify / 5. Delete ────────────────────────────────


def test_create_event_fires_callback(tmp_path):
    rec = _CallbackRecorder()
    w = _make_watcher(tmp_path, callback=rec, debounce_ms=50, source_id=10)
    w.start()
    try:
        target = tmp_path / "hello.txt"
        target.write_text("hi")
        assert rec.wait_for(1, timeout=5.0), "create event never fired"
        types = {e["event_type"] for e in rec.events}
        assert "create" in types or "modify" in types
        assert any(e["file_name"] == "hello.txt" for e in rec.events)
    finally:
        w.stop()


def test_modify_event_fires_callback(tmp_path):
    target = tmp_path / "x.txt"
    target.write_text("seed")
    rec = _CallbackRecorder()
    w = _make_watcher(tmp_path, callback=rec, debounce_ms=50, source_id=11)
    w.start()
    try:
        # Sleep briefly so the create-burst debounces past us, then mutate.
        time.sleep(0.2)
        rec.events.clear()
        target.write_text("updated content")
        assert rec.wait_for(1, timeout=5.0), "modify event never fired"
        assert any(e["file_name"] == "x.txt" for e in rec.events)
    finally:
        w.stop()


def test_delete_event_fires_callback(tmp_path):
    target = tmp_path / "rm.txt"
    target.write_text("bye")
    rec = _CallbackRecorder()
    w = _make_watcher(tmp_path, callback=rec, debounce_ms=50, source_id=12)
    w.start()
    try:
        time.sleep(0.2)
        rec.events.clear()
        target.unlink()
        assert rec.wait_for(1, timeout=5.0), "delete event never fired"
        assert any(e["event_type"] == "delete" for e in rec.events)
    finally:
        w.stop()


# ─── 6. UNC path → polling fallback ───────────────────────────────────


def test_unc_path_detected():
    assert _is_unc_path(r"\\server\share") is True
    assert _is_unc_path("//server/share") is True
    assert _is_unc_path("/tmp/local") is False


def test_unc_path_makes_watcher_unavailable():
    assert WatchdogWatcher.available(r"\\server\share") is False


def test_factory_picks_polling_for_unc_path():
    from src.scanner.file_watcher import FileWatcher
    cfg = {"watcher": {"backend": "watchdog", "poll_interval_seconds": 30}}
    src = mock.Mock(id=99, unc_path=r"\\fileserver\share")
    w = WatcherFactory.create(cfg, src, callback=None)
    assert isinstance(w, FileWatcher)


# ─── 7. Stop is idempotent ────────────────────────────────────────────


def test_stop_is_idempotent(tmp_path):
    w = _make_watcher(tmp_path, source_id=20)
    # stop() before start() must not raise
    w.stop()
    w.start()
    assert w.is_running() is True
    w.stop()
    assert w.is_running() is False
    # Second stop() must also not raise
    w.stop()
    assert w.is_running() is False


# ─── 8. Two watchers do not interfere ─────────────────────────────────


def test_multiple_watchers_isolated(tmp_path):
    a_dir = tmp_path / "a"
    b_dir = tmp_path / "b"
    a_dir.mkdir()
    b_dir.mkdir()
    rec_a = _CallbackRecorder()
    rec_b = _CallbackRecorder()
    w_a = WatchdogWatcher(source_id=101, path=str(a_dir),
                           callback=rec_a, debounce_ms=50)
    w_b = WatchdogWatcher(source_id=102, path=str(b_dir),
                           callback=rec_b, debounce_ms=50)
    w_a.start()
    w_b.start()
    try:
        (a_dir / "only_a.txt").write_text("a")
        assert rec_a.wait_for(1, timeout=5.0)
        # b should NOT have observed events for files in a
        assert all("only_a" not in e.get("file_name", "") for e in rec_b.events)
        # Now poke b
        rec_a.events.clear()
        rec_b.events.clear()
        (b_dir / "only_b.txt").write_text("b")
        assert rec_b.wait_for(1, timeout=5.0)
        assert all("only_b" not in e.get("file_name", "") for e in rec_a.events)
    finally:
        w_a.stop()
        w_b.stop()


# ─── 9. Burst coalescing ──────────────────────────────────────────────


def test_burst_coalesces_to_fewer_callbacks(tmp_path):
    rec = _CallbackRecorder()
    w = _make_watcher(tmp_path, callback=rec, debounce_ms=400, source_id=30)
    w.start()
    try:
        target = tmp_path / "burst.txt"
        target.write_text("0")
        # 100 rapid writes — well under one debounce window apiece.
        for i in range(100):
            target.write_text(str(i))
        # wait long enough for the debounce timer to flush
        time.sleep(1.0)
        per_path = [e for e in rec.events if e["file_name"] == "burst.txt"]
        # 100 raw FS events must collapse to far fewer dispatches
        assert len(per_path) < 50, f"no coalescing: {len(per_path)} events"
        assert len(per_path) >= 1
    finally:
        w.stop()


# ─── 10. JSON serialisation ───────────────────────────────────────────


def test_event_json_serialises():
    evt = {
        "source_id": 1,
        "event_type": "create",
        "file_path": "/tmp/x",
        "file_name": "x",
        "size": 0,
        "time": "2026-04-28 12:00:00",
        "backend": "watchdog",
    }
    blob = WatchdogWatcher.serialise_event(evt)
    parsed = json.loads(blob)
    assert parsed["event_type"] == "create"
    assert parsed["backend"] == "watchdog"


def test_event_json_handles_datetime():
    from datetime import datetime
    evt = {"event_type": "modify", "ts": datetime(2026, 4, 28, 12, 0, 0)}
    blob = WatchdogWatcher.serialise_event(evt)
    parsed = json.loads(blob)
    assert parsed["ts"].startswith("2026-04-28")


# ─── 11. Factory selection: explicit polling override ────────────────


def test_factory_returns_polling_when_backend_polling(tmp_path):
    from src.scanner.file_watcher import FileWatcher
    cfg = {"watcher": {"backend": "polling", "poll_interval_seconds": 30}}
    src = mock.Mock(id=1, unc_path=str(tmp_path))
    w = WatcherFactory.create(cfg, src, callback=None)
    assert isinstance(w, FileWatcher)


# ─── 12. Factory selection: missing watchdog ─────────────────────────


def test_factory_returns_polling_when_watchdog_missing(monkeypatch, tmp_path):
    from src.scanner.file_watcher import FileWatcher

    def _boom():
        raise ImportError("simulated missing watchdog")
    monkeypatch.setattr(ww, "_import_watchdog", _boom)
    cfg = {"watcher": {"backend": "watchdog", "poll_interval_seconds": 30}}
    src = mock.Mock(id=1, unc_path=str(tmp_path))
    w = WatcherFactory.create(cfg, src, callback=None)
    assert isinstance(w, FileWatcher)


# ─── 13. Factory selection: default → watchdog ───────────────────────


def test_factory_returns_watchdog_by_default(tmp_path):
    cfg = {"watcher": {"poll_interval_seconds": 30}}  # no explicit backend
    src = mock.Mock(id=1, unc_path=str(tmp_path))
    w = WatcherFactory.create(cfg, src, callback=None)
    assert isinstance(w, WatchdogWatcher)


# ─── 14. get_status registry merge ────────────────────────────────────


def test_get_status_includes_running_watcher(tmp_path):
    w = _make_watcher(tmp_path, source_id=777)
    w.start()
    try:
        status = WatcherFactory.get_status(777)
        assert status.get("backend") == "watchdog"
        assert status.get("running") is True
    finally:
        w.stop()
