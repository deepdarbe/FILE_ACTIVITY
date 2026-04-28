"""Issue #175 — tests for the post-walk size + timestamp enrich pass.

Linux-runnable. Exercises :class:`src.scanner.size_enricher.SizeEnricher`
against a real ``Database`` rooted at ``tmp_path``: the stat backend is
the cross-platform fallback the customer hits when the FSCTL backend is
unavailable, and it's also the one we can fully drive on a Linux CI
runner without admin / NTFS volumes.

Coverage:
  * available -> True on Linux (stat path)
  * enrich populates file_size + last_modify_time on a 10-row scan
  * permission-denied stat -> skipped, not crashed
  * missing files (TOCTOU) -> skipped
  * size_enrich_max_mb guard
  * progress callback fires at the documented intervals
  * empty path iterator -> 0, no DB write
  * DB UPDATE retried via bulk_update_file_sizes (lock once, succeed)
  * DB lock raise after retries propagates
  * default config OFF (enrich_sizes=false) -> 0, no calls
  * _run_size_enrich integration with FileScanner orchestrator
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.scanner.size_enricher import SizeEnricher  # noqa: E402
from src.storage.database import Database  # noqa: E402
from src.storage.models import Source  # noqa: E402


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


def _make_db(tmp_path: Path) -> Database:
    db = Database({"path": str(tmp_path / "test.db")})
    db.connect()
    return db


_seed_counter = {"n": 0}


def _seed_scan(db: Database, paths: list[str]) -> tuple[int, int]:
    """Insert a fresh source + scan_run + N path-only rows, return
    (source_id, scan_id). Each call uses a fresh ``test_src_<N>`` name
    so back-to-back invocations don't collide on the UNIQUE index."""
    _seed_counter["n"] += 1
    name = f"test_src_{_seed_counter['n']}"
    src = Source(name=name, unc_path=f"/tmp/{name}", enabled=True)
    source_id = db.add_source(src)
    scan_id = db.create_scan_run(source_id)
    rows = []
    for p in paths:
        rows.append({
            "source_id": source_id,
            "scan_id": scan_id,
            "file_path": p,
            "relative_path": os.path.basename(p),
            "file_name": os.path.basename(p),
            "extension": (os.path.splitext(p)[1] or "").lstrip(".") or None,
            "file_size": 0,                   # MFT signature
            "creation_time": None,
            "last_access_time": None,
            "last_modify_time": None,
            "owner": None,
            "attributes": 0,
        })
    db.bulk_insert_scanned_files(rows)
    return source_id, scan_id


# ──────────────────────────────────────────────────────────────────────
# 1. available + happy path
# ──────────────────────────────────────────────────────────────────────


def test_available_true_on_linux(tmp_path):
    db = _make_db(tmp_path)
    try:
        e = SizeEnricher({"scanner": {}}, db)
        assert e.available is True
    finally:
        db.close()


def test_enrich_populates_size_and_mtime(tmp_path):
    """10-row synthetic scan, real tmp_path files; after enrich every
    row has file_size > 0 and last_modify_time set."""
    db = _make_db(tmp_path)
    try:
        # Create real files of varying sizes 1..10 bytes.
        files: list[str] = []
        for i in range(10):
            p = tmp_path / f"file_{i:02d}.bin"
            p.write_bytes(b"x" * (i + 1))
            files.append(str(p))

        source_id, scan_id = _seed_scan(db, files)

        e = SizeEnricher({"scanner": {"size_enrich_workers": 2}}, db)
        n = e.enrich(scan_id, source_id, iter(files))
        assert n == 10

        # Verify the rows really got updated.
        with db.get_cursor() as cur:
            cur.execute(
                "SELECT file_path, file_size, last_modify_time "
                "FROM scanned_files WHERE scan_id=? ORDER BY file_path",
                (scan_id,),
            )
            rows = cur.fetchall()
        assert len(rows) == 10
        for i, row in enumerate(rows):
            assert row["file_size"] == i + 1, row
            assert row["last_modify_time"], row  # populated, ISO string
    finally:
        db.close()


# ──────────────────────────────────────────────────────────────────────
# 2. error tolerance
# ──────────────────────────────────────────────────────────────────────


def test_permission_denied_paths_skipped(tmp_path):
    """A path that can't be stat'd (we mock os.stat to raise) is
    counted as skipped and does not crash the pass."""
    db = _make_db(tmp_path)
    try:
        good = tmp_path / "good.bin"
        good.write_bytes(b"hello")
        bad = tmp_path / "bad.bin"
        bad.write_bytes(b"world")

        source_id, scan_id = _seed_scan(db, [str(good), str(bad)])

        real_stat = os.stat

        def _flaky_stat(path, **kw):
            if path == str(bad):
                raise PermissionError("denied")
            return real_stat(path, **kw)

        e = SizeEnricher({"scanner": {"size_enrich_workers": 1}}, db)
        with patch("src.scanner.size_enricher.os.stat", side_effect=_flaky_stat):
            n = e.enrich(scan_id, source_id, iter([str(good), str(bad)]))
        assert n == 1                # only `good` enriched
        assert e.last_skipped == 1   # bad counted as skipped
    finally:
        db.close()


def test_missing_files_skipped(tmp_path):
    """File enumerated by the MFT walker but deleted before the enrich
    pass — TOCTOU race. Should skip, not crash."""
    db = _make_db(tmp_path)
    try:
        existing = tmp_path / "exists.bin"
        existing.write_bytes(b"xxx")
        gone = tmp_path / "gone.bin"
        # never write `gone` — its os.stat raises FileNotFoundError

        source_id, scan_id = _seed_scan(db, [str(existing), str(gone)])

        e = SizeEnricher({"scanner": {}}, db)
        n = e.enrich(scan_id, source_id, iter([str(existing), str(gone)]))
        assert n == 1
        assert e.last_skipped == 1
    finally:
        db.close()


def test_size_enrich_max_mb_guard(tmp_path):
    """When max_mb is set, files larger than the cap are skipped."""
    db = _make_db(tmp_path)
    try:
        small = tmp_path / "small.bin"
        small.write_bytes(b"x" * 100)
        big = tmp_path / "big.bin"
        big.write_bytes(b"x" * (3 * 1_048_576))   # 3 MB

        source_id, scan_id = _seed_scan(db, [str(small), str(big)])

        e = SizeEnricher(
            {"scanner": {"size_enrich_max_mb": 1}}, db
        )
        n = e.enrich(scan_id, source_id, iter([str(small), str(big)]))
        assert n == 1            # only small enriched
        assert e.last_skipped == 1
    finally:
        db.close()


# ──────────────────────────────────────────────────────────────────────
# 3. progress callback
# ──────────────────────────────────────────────────────────────────────


def test_progress_callback_fires(tmp_path, monkeypatch):
    """Callback receives stage='size_enrich' and a non-decreasing
    processed counter. We lower PROGRESS_EVERY so a small test set still
    triggers an emit."""
    db = _make_db(tmp_path)
    try:
        files = []
        for i in range(15):
            p = tmp_path / f"f_{i}.bin"
            p.write_bytes(b"y")
            files.append(str(p))
        source_id, scan_id = _seed_scan(db, files)

        # Lower the threshold so we see at least one mid-pass emit.
        monkeypatch.setattr(
            "src.scanner.size_enricher.PROGRESS_EVERY", 5
        )

        calls: list[dict] = []

        def cb(**kw):
            calls.append(kw)

        e = SizeEnricher({"scanner": {"size_enrich_workers": 1}}, db)
        n = e.enrich(scan_id, source_id, iter(files), progress_cb=cb)
        assert n == 15
        # At least one mid-pass emit + the final flush.
        assert len(calls) >= 2
        for c in calls:
            assert c.get("stage") == "size_enrich"
            assert isinstance(c.get("processed"), int)
        # Counter is non-decreasing.
        seen = [c["processed"] for c in calls]
        assert seen == sorted(seen)
        assert seen[-1] == 15
    finally:
        db.close()


# ──────────────────────────────────────────────────────────────────────
# 4. empty input
# ──────────────────────────────────────────────────────────────────────


def test_empty_path_iterator_no_db_write(tmp_path):
    """Empty input → 0 rows enriched, bulk_update_file_sizes never called."""
    db = _make_db(tmp_path)
    try:
        source_id = db.add_source(
            Source(name="empty_src", unc_path="/empty", enabled=True)
        )
        scan_id = db.create_scan_run(source_id)

        with patch.object(db, "bulk_update_file_sizes") as bu:
            e = SizeEnricher({"scanner": {}}, db)
            n = e.enrich(scan_id, source_id, iter([]))
        assert n == 0
        bu.assert_not_called()
    finally:
        db.close()


# ──────────────────────────────────────────────────────────────────────
# 5. DB lock retry
# ──────────────────────────────────────────────────────────────────────


class _ConnWrapper:
    """Sqlite Connection facade that delegates everything but lets us
    inject side effects on executemany. Plain ``patch.object`` doesn't
    work here because sqlite3.Connection.executemany is a read-only
    C-level slot."""

    def __init__(self, real_conn, em_side_effect):
        self._real = real_conn
        self._em_side_effect = em_side_effect

    def executemany(self, sql, params):
        return self._em_side_effect(sql, params)

    # Pass-through for the contextmanager + commit/rollback used in
    # ``Database.get_conn``. ``Database.bulk_update_file_sizes`` only
    # touches executemany on the conn — no other methods are needed.
    def commit(self):
        return self._real.commit()

    def rollback(self):
        return self._real.rollback()

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_bulk_update_retries_on_lock_then_succeeds(tmp_path, monkeypatch):
    """First two executemany calls raise 'database is locked', third
    succeeds → enrich returns the success rowcount and does not raise."""
    db = _make_db(tmp_path)
    try:
        import time as _t
        sleep_calls = []
        monkeypatch.setattr(_t, "sleep", lambda s: sleep_calls.append(s))

        files: list[str] = []
        for i in range(3):
            p = tmp_path / f"r_{i}.bin"
            p.write_bytes(b"z")
            files.append(str(p))
        source_id, scan_id = _seed_scan(db, files)

        real_conn = db._get_conn()
        attempts = {"n": 0}

        def flaky_em(sql, params):
            attempts["n"] += 1
            if attempts["n"] <= 2:
                raise sqlite3.OperationalError("database is locked")
            return real_conn.executemany(sql, params)

        wrapped = _ConnWrapper(real_conn, flaky_em)
        with patch.object(db, "_get_conn", return_value=wrapped):
            e = SizeEnricher({"scanner": {"size_enrich_workers": 1}}, db)
            n = e.enrich(scan_id, source_id, iter(files))
        assert n == 3
        assert attempts["n"] == 3
        # Two backoff sleeps fired (1s + 2s).
        assert sleep_calls == [1, 2]
    finally:
        db.close()


def test_bulk_update_propagates_after_5_retries(tmp_path, monkeypatch):
    """If every retry fails the OperationalError surfaces."""
    db = _make_db(tmp_path)
    try:
        import time as _t
        monkeypatch.setattr(_t, "sleep", lambda s: None)

        p = tmp_path / "p.bin"
        p.write_bytes(b"z")
        source_id, scan_id = _seed_scan(db, [str(p)])

        real_conn = db._get_conn()

        def always_locked(sql, params):
            raise sqlite3.OperationalError("database is locked")

        wrapped = _ConnWrapper(real_conn, always_locked)
        with patch.object(db, "_get_conn", return_value=wrapped):
            e = SizeEnricher({"scanner": {"size_enrich_workers": 1}}, db)
            with pytest.raises(sqlite3.OperationalError):
                e.enrich(scan_id, source_id, iter([str(p)]))
    finally:
        db.close()


# ──────────────────────────────────────────────────────────────────────
# 6. orchestrator integration
# ──────────────────────────────────────────────────────────────────────


def test_run_size_enrich_disabled_returns_zero(tmp_path):
    """enrich_sizes=false → silent skip, no SizeEnricher constructed."""
    from src.scanner.file_scanner import FileScanner

    db = _make_db(tmp_path)
    try:
        scanner = FileScanner(db, {"scanner": {"enrich_sizes": False}})
        with patch("src.scanner.size_enricher.SizeEnricher") as Mock:
            res = scanner._run_size_enrich(scan_id=99)
        assert res["enriched"] == 0
        assert res.get("skipped_disabled") is True
        Mock.assert_not_called()
    finally:
        db.close()


def test_run_size_enrich_default_on(tmp_path):
    """Default config (no enrich_sizes key) treats it as ON, drives the
    enricher over rows with file_size=0."""
    from src.scanner.file_scanner import FileScanner

    db = _make_db(tmp_path)
    try:
        # Two real files.
        f1 = tmp_path / "x.bin"
        f1.write_bytes(b"hello")
        f2 = tmp_path / "y.bin"
        f2.write_bytes(b"world!!")
        _src, scan_id = _seed_scan(db, [str(f1), str(f2)])

        scanner = FileScanner(db, {"scanner": {}})
        # _current_source_id is normally set by scan_source(); for
        # _run_size_enrich isolation we set it manually.
        scanner._current_source_id = 1
        result = scanner._run_size_enrich(scan_id=scan_id)
        assert result["enriched"] == 2
        # And the rows now have non-zero sizes.
        with db.get_cursor() as cur:
            cur.execute(
                "SELECT file_size FROM scanned_files WHERE scan_id=?",
                (scan_id,),
            )
            sizes = sorted(r["file_size"] for r in cur.fetchall())
        assert sizes == [5, 7]
    finally:
        db.close()


def test_chunk_boundary_writes(tmp_path, monkeypatch):
    """Lower CHUNK_SIZE so we exercise multiple writes in one call;
    confirm the result rowcount equals the input size."""
    db = _make_db(tmp_path)
    try:
        monkeypatch.setattr("src.scanner.size_enricher.CHUNK_SIZE", 3)

        files: list[str] = []
        for i in range(7):
            p = tmp_path / f"c_{i}.bin"
            p.write_bytes(b"a" * (i + 1))
            files.append(str(p))
        source_id, scan_id = _seed_scan(db, files)

        write_count = {"n": 0}
        real_bu = db.bulk_update_file_sizes

        def counting_bu(rows):
            write_count["n"] += 1
            return real_bu(rows)

        with patch.object(db, "bulk_update_file_sizes", side_effect=counting_bu):
            e = SizeEnricher({"scanner": {"size_enrich_workers": 1}}, db)
            n = e.enrich(scan_id, source_id, iter(files))
        assert n == 7
        # 7 rows / chunk size 3 = 2 full + 1 trailing flush => 3 writes.
        assert write_count["n"] == 3
    finally:
        db.close()


def test_bulk_update_file_sizes_empty_returns_zero(tmp_path):
    """Database.bulk_update_file_sizes with [] -> 0, no SQL executed."""
    db = _make_db(tmp_path)
    try:
        n = db.bulk_update_file_sizes([])
        assert n == 0
    finally:
        db.close()


def test_bulk_update_only_target_scan(tmp_path):
    """An UPDATE for scan_id=A must not touch identical paths under
    scan_id=B (composite key correctness)."""
    db = _make_db(tmp_path)
    try:
        f = tmp_path / "shared.bin"
        f.write_bytes(b"hello")
        _src_a, scan_a = _seed_scan(db, [str(f)])
        _src_b, scan_b = _seed_scan(db, [str(f)])

        # Enrich only scan_a.
        e = SizeEnricher({"scanner": {}}, db)
        n = e.enrich(scan_a, _src_a, iter([str(f)]))
        assert n == 1

        with db.get_cursor() as cur:
            cur.execute(
                "SELECT scan_id, file_size FROM scanned_files "
                "WHERE file_path=? ORDER BY scan_id",
                (str(f),),
            )
            rows = cur.fetchall()
        assert rows[0]["scan_id"] == scan_a
        assert rows[0]["file_size"] == 5
        assert rows[1]["scan_id"] == scan_b
        assert rows[1]["file_size"] == 0   # untouched
    finally:
        db.close()
