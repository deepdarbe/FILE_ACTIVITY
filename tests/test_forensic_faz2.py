"""Tests for #340 Faz 2 — deletion-forensics correlation.

Three parts:
  A. 4656↔4660 handle correlation → "what was deleted" (full path).
  B. 4624 logon → client_ip/workstation second pass → "from where".
  C. USN parent-FRN → full path (FrnResolver + raw-FRN threading).

Discipline (matches the rest of the suite):
  * No pywin32 / fastapi imported. The Event Log collect() loop is exercised
    by injecting a FAKE ``win32evtlog`` / ``win32security`` into sys.modules;
    parsers/helpers take plain lists; FrnResolver imports cleanly on POSIX.
  * Platform-agnostic: os.path.basename does NOT split backslashes on POSIX,
    so file_name is asserted against os.path.basename(path), never a literal.
  * main.py is never imported (it swaps sys.stdout at import time).
"""

from __future__ import annotations

import os
import struct
import sys
import types
from datetime import datetime

import pytest

from src.storage.database import Database
from src.user_activity.event_collector import EventCollector, _DROPPED
from src.scanner.backends._ntfs_records import parse_usn_records
from src.scanner.backends.frn_resolver import FrnResolver


# ─── Fixtures / fakes ─────────────────────────────────────────────────────


@pytest.fixture
def db(tmp_path):
    d = Database({
        "path": str(tmp_path / "f2.db"),
        "retention": {"auto_cleanup_on_startup": False},
    })
    d.connect()
    yield d
    d.close()


class _FakeEvent:
    """Minimal stand-in for a pywin32 EventLogRecord."""

    def __init__(self, event_id, strings, when=None):
        self.EventID = event_id
        self.StringInserts = list(strings)
        self.TimeGenerated = when or datetime.now()


class _FakeEvtLog(types.ModuleType):
    """Fake win32evtlog exposing exactly what collect() touches."""

    EVENTLOG_BACKWARDS_READ = 0x0008
    EVENTLOG_SEQUENTIAL_READ = 0x0001

    def __init__(self, batches):
        super().__init__("win32evtlog")
        self._batches = iter(list(batches))

    def OpenEventLog(self, server, log_type):
        return "HANDLE"

    def ReadEventLog(self, handle, flags, offset):
        try:
            return next(self._batches)
        except StopIteration:
            return []

    def CloseEventLog(self, handle):
        return None


def _install_fake_win32(monkeypatch, batches):
    monkeypatch.setitem(sys.modules, "win32evtlog", _FakeEvtLog(batches))
    monkeypatch.setitem(sys.modules, "win32security",
                        types.ModuleType("win32security"))


def _rows(db):
    with db.get_read_cursor() as cur:
        cur.execute(
            "SELECT username, file_path, file_name, extension, access_type, "
            "client_ip, event_id FROM user_access_logs ORDER BY id")
        return [dict(r) for r in cur.fetchall()]


# Realistic StringInserts arrays (indices per event_collector constants).
def _s4660(user="mehmet", domain="ITWISE", logon="0x3e7",
           handle="0x2b4", process="0x1a8"):
    return ["S-1-5-21-x", user, domain, logon, "Security",
            handle, process, "C:\\Windows\\explorer.exe"]


def _s4656(user="mehmet", domain="ITWISE", logon="0x3e7",
           obj=r"E:\ortak\rapor.xlsx", handle="0x2b4",
           mask="0x10000", process="0x1a8"):
    return ["S-1-5-21-x", user, domain, logon, "Security", "File",
            obj, handle, mask, process, "explorer.exe"]


def _s4624(logon="0x3e7", wks="HR-PC", ip="10.0.0.5"):
    s = ["-"] * 19
    s[7] = logon
    s[11] = wks
    s[18] = ip
    return s


# ─── Part A: 4656↔4660 correlation ────────────────────────────────────────


def test_parse_4660_carries_correlation_keys():
    c = EventCollector(None, {})
    rec = c._parse_4660(_FakeEvent(4660, _s4660()), _s4660())
    assert rec is not None
    assert rec["handle_id"] == "0x2b4"
    assert rec["process_id"] == "0x1a8"
    assert rec["logon_id"] == "0x3e7"
    # Placeholder path until a 4656 supplies the real one.
    assert rec["file_path"] == "[HandleId:0x2b4]"
    assert rec["access_type"] == "delete"


def test_correlate_4656_patches_pending_4660():
    c = EventCollector(None, {})
    pending = {("0x2b4", "0x1a8"): c._parse_4660(
        _FakeEvent(4660, _s4660()), _s4660())}
    out = c._correlate_4656(_s4656(), pending)
    assert out is not None and out is not _DROPPED
    path = r"E:\ortak\rapor.xlsx"
    assert out["file_path"] == path
    assert out["file_name"] == os.path.basename(path)
    assert pending == {}  # consumed


def test_correlate_4656_reapplies_directory_filter():
    """Amendment 10: the [HandleId:x] placeholder passed the path filter but a
    real directory path (trailing backslash) must be dropped."""
    c = EventCollector(None, {})
    pending = {("0x2b4", "0x1a8"): c._parse_4660(
        _FakeEvent(4660, _s4660()), _s4660())}
    out = c._correlate_4656(_s4656(obj="E:\\ortak\\"), pending)
    assert out is _DROPPED
    assert pending == {}


def test_correlate_4656_reapplies_excluded_extension():
    c = EventCollector(None, {})
    pending = {("0x2b4", "0x1a8"): c._parse_4660(
        _FakeEvent(4660, _s4660()), _s4660())}
    out = c._correlate_4656(_s4656(obj=r"E:\ortak\build.tmp"), pending)
    assert out is _DROPPED


def test_correlate_4656_no_pending_returns_none():
    c = EventCollector(None, {})
    assert c._correlate_4656(_s4656(), {}) is None


def test_collect_correlates_delete_full_path(monkeypatch, db):
    """Backwards read: 4660 (delete) seen BEFORE its 4656 (path). The single
    resulting row carries the real path + IP; the 4656 is consumed (not a
    second row)."""
    batch = [
        _FakeEvent(4660, _s4660()),
        _FakeEvent(4656, _s4656()),
        _FakeEvent(4624, _s4624()),
    ]
    _install_fake_win32(monkeypatch, [batch])
    c = EventCollector(db, {})
    res = c.collect(hours=24)
    assert res["collected"] == 1
    rows = _rows(db)
    assert len(rows) == 1
    r = rows[0]
    path = r"E:\ortak\rapor.xlsx"
    assert r["file_path"] == path
    assert r["file_name"] == os.path.basename(path)
    assert r["access_type"] == "delete"
    assert r["event_id"] == 4660
    assert r["client_ip"] == "10.0.0.5 (HR-PC)"


def test_collect_unresolved_4660_flushed_with_placeholder(monkeypatch, db):
    """A 4660 with no matching 4656 must NOT be lost — it is flushed at run end
    with the placeholder path (only the path is unknown, never the row)."""
    batch = [
        _FakeEvent(4660, _s4660()),
        _FakeEvent(4624, _s4624()),
    ]
    _install_fake_win32(monkeypatch, [batch])
    c = EventCollector(db, {})
    res = c.collect(hours=24)
    assert res["collected"] == 1
    rows = _rows(db)
    assert len(rows) == 1
    assert rows[0]["file_path"] == "[HandleId:0x2b4]"
    # IP still attaches via the logon second pass.
    assert rows[0]["client_ip"] == "10.0.0.5 (HR-PC)"


def test_collect_4656_without_4660_inserts_normally(monkeypatch, db):
    """A 4656 that matches no pending 4660 keeps Faz 1 behavior: its own row."""
    batch = [_FakeEvent(4656, _s4656())]
    _install_fake_win32(monkeypatch, [batch])
    c = EventCollector(db, {})
    res = c.collect(hours=24)
    assert res["collected"] == 1
    rows = _rows(db)
    assert len(rows) == 1
    assert rows[0]["event_id"] == 4656
    assert rows[0]["access_type"] == "delete"


# ─── Part B: 4624 logon → IP/workstation ──────────────────────────────────


def test_parse_4624_logon_defensive_length():
    c = EventCollector(None, {})
    assert c._parse_4624_logon(["a", "b"]) is None            # too short
    lid, ip, wks = c._parse_4624_logon(_s4624())
    assert (lid, ip, wks) == ("0x3e7", "10.0.0.5", "HR-PC")


def test_apply_logon_ips_fills_and_formats():
    c = EventCollector(None, {})
    logon_map = {"0x3e7": ("10.0.0.5", "HR-PC")}
    rows = [
        {"logon_id": "0x3e7", "client_ip": None},   # → filled + wks
        {"logon_id": "0x3e7", "client_ip": "1.2.3.4"},  # already set, untouched
        {"logon_id": "0xUNKNOWN", "client_ip": None},   # no logon match
        {"client_ip": None},                             # no logon_id
    ]
    c._apply_logon_ips(rows, logon_map)
    assert rows[0]["client_ip"] == "10.0.0.5 (HR-PC)"
    assert rows[1]["client_ip"] == "1.2.3.4"
    assert rows[2]["client_ip"] is None
    assert rows[3]["client_ip"] is None


def test_apply_logon_ips_drops_loopback():
    c = EventCollector(None, {})
    for junk in ("-", "::1", "127.0.0.1"):
        rows = [{"logon_id": "L", "client_ip": None}]
        c._apply_logon_ips(rows, {"L": (junk, "-")})
        assert rows[0]["client_ip"] is None


def test_apply_logon_ips_ip_only_when_no_workstation():
    c = EventCollector(None, {})
    rows = [{"logon_id": "L", "client_ip": None}]
    c._apply_logon_ips(rows, {"L": ("10.0.0.9", "-")})
    assert rows[0]["client_ip"] == "10.0.0.9"


# ─── Part C: USN raw FRN + FrnResolver ────────────────────────────────────


def _usn_record(frn, parent_frn, name, attributes=0x20):
    name_bytes = name.encode("utf-16-le")
    name_off = 60
    rec_len = (name_off + len(name_bytes) + 7) & ~7
    buf = bytearray(rec_len)
    struct.pack_into("<I", buf, 0, rec_len)
    struct.pack_into("<H", buf, 4, 2)      # MajorVersion
    struct.pack_into("<Q", buf, 8, frn)
    struct.pack_into("<Q", buf, 16, parent_frn)
    struct.pack_into("<I", buf, 52, attributes)
    struct.pack_into("<H", buf, 56, len(name_bytes))
    struct.pack_into("<H", buf, 58, name_off)
    buf[name_off:name_off + len(name_bytes)] = name_bytes
    return bytes(buf)


def test_parse_usn_exposes_raw_and_masked_frn():
    """The masked frn/parent_frn feed the parent-chain dict keys; the raw
    values keep the NTFS sequence number that OpenFileById needs (#340 Faz 2)."""
    parent_full = (7 << 48) | 5      # sequence 7 in the high 16 bits
    frn_full = (3 << 48) | 100
    buf = b"\x00" * 8 + _usn_record(frn_full, parent_full, "a.txt")
    parsed = list(parse_usn_records(buf, offset=8))
    assert len(parsed) == 1
    p = parsed[0]
    assert p["frn"] == 100 and p["parent_frn"] == 5          # masked
    assert p["frn_raw"] == frn_full                           # raw preserved
    assert p["parent_frn_raw"] == parent_full


def test_frn_resolver_handle_ok():
    assert FrnResolver._handle_ok(1234) is True
    for bad in (None, 0, -1, 0xFFFFFFFFFFFFFFFF):
        assert FrnResolver._handle_ok(bad) is False


def test_frn_resolver_final_path_strips_prefixes():
    import ctypes

    class _FakeK32:
        def __init__(self, value):
            self.value = value

        def GetFinalPathNameByHandleW(self, handle, buf, buflen, flags):
            if len(self.value) >= buflen:
                return len(self.value) + 5
            buf.value = self.value
            return len(self.value)

    # \\?\ extended-length prefix stripped.
    assert FrnResolver._final_path(_FakeK32(r"\\?\E:\ortak"), 1) == r"E:\ortak"
    # \\?\UNC\ → \\ (UNC) prefix rewritten.
    assert FrnResolver._final_path(
        _FakeK32(r"\\?\UNC\srv\share\dir"), 1) == r"\\srv\share\dir"
    # Failure (length 0) → None.
    assert FrnResolver._final_path(_FakeK32(""), 1) is None


def test_frn_resolver_caches_success(monkeypatch):
    r = FrnResolver(cache_size=8)
    calls = []

    def _impl(volume, frn):
        calls.append(frn)
        return "E:\\dir%d" % frn

    monkeypatch.setattr(r, "_resolve_impl", _impl)
    assert r.resolve("\\\\.\\E:", 10) == "E:\\dir10"
    assert r.resolve("\\\\.\\E:", 10) == "E:\\dir10"   # cache hit
    assert calls == [10]                                # underlying call once


def test_frn_resolver_lru_eviction(monkeypatch):
    r = FrnResolver(cache_size=2)
    calls = []

    def _impl(volume, frn):
        calls.append(frn)
        return "p%d" % frn

    monkeypatch.setattr(r, "_resolve_impl", _impl)
    r.resolve("V", 1)
    r.resolve("V", 2)
    r.resolve("V", 3)      # evicts 1 (oldest)
    r.resolve("V", 1)      # miss again → re-invokes
    assert calls == [1, 2, 3, 1]


def test_frn_resolver_disables_after_consecutive_failures(monkeypatch):
    r = FrnResolver()
    monkeypatch.setattr(r, "_resolve_impl", lambda v, f: None)
    for i in range(1, 21):
        assert r.resolve("V", i) is None
    assert r._disabled is True
    # Once disabled it short-circuits without calling the impl.
    monkeypatch.setattr(r, "_resolve_impl",
                        lambda v, f: (_ for _ in ()).throw(AssertionError("called")))
    assert r.resolve("V", 999) is None


def test_frn_resolver_success_resets_failure_streak(monkeypatch):
    r = FrnResolver()
    seq = {"n": 0}

    def _impl(volume, frn):
        seq["n"] += 1
        # fail, fail, succeed, repeated — never 20 in a row.
        return None if seq["n"] % 3 else "ok"

    monkeypatch.setattr(r, "_resolve_impl", _impl)
    for i in range(60):
        r.resolve("V", i)
    assert r._disabled is False


def test_frn_resolver_none_inputs():
    r = FrnResolver()
    assert r.resolve(None, 5) is None
    assert r.resolve("V", None) is None


@pytest.mark.skipif(sys.platform == "win32",
                    reason="on Windows resolve() may hit a real volume handle")
def test_frn_resolver_graceful_on_posix():
    """No win32 available → every resolve returns None and never raises."""
    r = FrnResolver()
    assert r.resolve("\\\\.\\E:", 123456) is None
    r.close()  # idempotent, must not raise
