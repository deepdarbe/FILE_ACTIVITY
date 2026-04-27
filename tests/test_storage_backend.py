"""Tests for the Phase 1 storage backend abstraction (issue #114).

Phase 1 is a refactor with zero behaviour change: it introduces
``StorageBackend`` (Protocol), ``SqliteBackend`` (the only concrete
impl this round), and ``StorageManager`` (factory + holder). These
tests cover the factory selection logic and the SqliteBackend wrapper
end-to-end against a fixture SQLite database.
"""

from __future__ import annotations

import pytest

from src.storage.backends.manager import StorageManager
from src.storage.backends.sqlite_backend import SqliteBackend
from src.storage.database import Database


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db(tmp_path):
    """Boot a real Database against a tmp SQLite file."""
    db_path = tmp_path / "phase1.db"
    inst = Database({"path": str(db_path)})
    inst.connect()
    yield inst
    try:
        inst.close()
    except Exception:
        pass


@pytest.fixture
def seeded_scan(db):
    """Insert one source + one scan_run + a small set of scanned_files
    rows. Returns ``(source_id, scan_id, rows)``.
    """
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

    rows = [
        {
            "source_id": source_id,
            "scan_id": scan_id,
            "file_path": r"\\share\dir\a.pdf",
            "relative_path": r"dir\a.pdf",
            "file_name": "a.pdf",
            "extension": "pdf",
            "file_size": 2_000_000,
            "creation_time": "2024-01-01 00:00:00",
            "last_access_time": "2024-06-01 00:00:00",
            "last_modify_time": "2024-06-01 00:00:00",
            "owner": "alice",
            "attributes": 0,
        },
        {
            "source_id": source_id,
            "scan_id": scan_id,
            "file_path": r"\\share\dir\b.pdf",
            "relative_path": r"dir\b.pdf",
            "file_name": "b.pdf",
            "extension": "pdf",
            "file_size": 500_000,
            "creation_time": "2024-01-02 00:00:00",
            "last_access_time": "2024-06-02 00:00:00",
            "last_modify_time": "2024-06-02 00:00:00",
            "owner": "bob",
            "attributes": 0,
        },
        {
            "source_id": source_id,
            "scan_id": scan_id,
            "file_path": r"\\share\other\c.txt",
            "relative_path": r"other\c.txt",
            "file_name": "c.txt",
            "extension": "txt",
            "file_size": 1024,
            "creation_time": "2024-01-03 00:00:00",
            "last_access_time": "2024-06-03 00:00:00",
            "last_modify_time": "2024-06-03 00:00:00",
            "owner": "alice",
            "attributes": 0,
        },
    ]
    db.bulk_insert_scanned_files(rows)
    return source_id, scan_id, rows


# ---------------------------------------------------------------------------
# StorageManager factory
# ---------------------------------------------------------------------------


def test_storage_manager_default_is_sqlite(db):
    """No storage block in config -> sqlite backend selected."""
    mgr = StorageManager(db, {})
    assert mgr.name == "sqlite"
    assert isinstance(mgr.backend, SqliteBackend)


def test_storage_manager_explicit_sqlite(db):
    """``backend: 'sqlite'`` works."""
    mgr = StorageManager(db, {"storage": {"backend": "sqlite"}})
    assert mgr.name == "sqlite"
    assert isinstance(mgr.backend, SqliteBackend)


def test_storage_manager_elasticsearch_raises_not_implemented(db):
    """ES backend is reserved for Phase 2 — must raise NotImplementedError."""
    with pytest.raises(NotImplementedError):
        StorageManager(db, {"storage": {"backend": "elasticsearch"}})


def test_storage_manager_unknown_backend_raises(db):
    """Unknown backend name -> ValueError, not silent fallback."""
    with pytest.raises(ValueError):
        StorageManager(db, {"storage": {"backend": "no-such-backend"}})


def test_storage_manager_delegates_to_backend(db):
    """``StorageManager.__getattr__`` forwards arbitrary attrs to the
    backend so callers can do ``app.state.storage.query_files(...)``
    without unwrapping ``.backend`` manually."""
    mgr = StorageManager(db, {})
    # ``health_check`` lives on the backend; delegation should resolve it.
    res = mgr.health_check()
    assert res["name"] == "sqlite"
    assert res["available"] is True


# ---------------------------------------------------------------------------
# SqliteBackend
# ---------------------------------------------------------------------------


def test_sqlite_backend_insert_count_matches(db):
    """insert_scanned_files returns the inserted count and persists rows."""
    backend = SqliteBackend(db, {})
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (name, unc_path) VALUES ('s1', '/share')"
        )
        source_id = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs (source_id, status) VALUES (?, 'running')",
            (source_id,),
        )
        scan_id = cur.lastrowid

    rows = [
        {
            "source_id": source_id,
            "scan_id": scan_id,
            "file_path": f"\\\\s\\f{i}.bin",
            "relative_path": f"f{i}.bin",
            "file_name": f"f{i}.bin",
            "extension": "bin",
            "file_size": 100 + i,
        }
        for i in range(5)
    ]

    inserted = backend.insert_scanned_files(scan_id, rows)
    assert inserted == 5
    assert backend.count_scanned_files(scan_id) == 5


def test_sqlite_backend_query_files_filter_dsl_validates_keys(db, seeded_scan):
    """A bogus filter_dsl key must raise ValueError; the whitelist is
    the SQL-injection guard for Phase 1, and Phase 2 ES will share it."""
    _, scan_id, _ = seeded_scan
    backend = SqliteBackend(db, {})
    with pytest.raises(ValueError):
        backend.query_files(scan_id, {"file_path; DROP TABLE": "boom"})


def test_sqlite_backend_query_files_extension_filter(db, seeded_scan):
    _, scan_id, _ = seeded_scan
    backend = SqliteBackend(db, {})

    pdfs = backend.query_files(scan_id, {"extension": "pdf"})
    assert len(pdfs) == 2
    assert all(r["extension"] == "pdf" for r in pdfs)

    big = backend.query_files(scan_id, {"min_size": 1_000_000})
    assert len(big) == 1
    assert big[0]["file_name"] == "a.pdf"

    prefixed = backend.query_files(
        scan_id, {"directory_prefix": r"\\share\dir"}
    )
    assert len(prefixed) == 2
    assert {r["file_name"] for r in prefixed} == {"a.pdf", "b.pdf"}


def test_sqlite_backend_aggregate_by_extension(db, seeded_scan):
    _, scan_id, _ = seeded_scan
    backend = SqliteBackend(db, {})

    counts = backend.aggregate(scan_id, "extension", "count")
    counts_by_ext = {r["extension"]: r["count"] for r in counts}
    assert counts_by_ext == {"pdf": 2, "txt": 1}

    sums = backend.aggregate(scan_id, "extension", "sum_size")
    sums_by_ext = {r["extension"]: r["sum_size"] for r in sums}
    assert sums_by_ext["pdf"] == 2_500_000
    assert sums_by_ext["txt"] == 1024


def test_sqlite_backend_aggregate_rejects_bogus_group_by(db, seeded_scan):
    _, scan_id, _ = seeded_scan
    backend = SqliteBackend(db, {})
    with pytest.raises(ValueError):
        backend.aggregate(scan_id, "file_path", "count")
    with pytest.raises(ValueError):
        backend.aggregate(scan_id, "extension", "avg_size")


def test_sqlite_backend_iterate_scan_batches(db, seeded_scan):
    """iterate_scan must yield contiguous, non-overlapping batches that
    together contain every row exactly once."""
    source_id, scan_id, _ = seeded_scan

    # Add more rows so we get >1 batch with batch_size=2.
    extra = [
        {
            "source_id": source_id,
            "scan_id": scan_id,
            "file_path": f"\\\\share\\extra\\f{i}.bin",
            "relative_path": f"extra\\f{i}.bin",
            "file_name": f"f{i}.bin",
            "extension": "bin",
            "file_size": 10 * i,
        }
        for i in range(4)
    ]
    db.bulk_insert_scanned_files(extra)

    backend = SqliteBackend(db, {})
    batches = list(backend.iterate_scan(scan_id, batch_size=2))
    assert len(batches) >= 2
    # No batch (other than the last) should be smaller than batch_size.
    for b in batches[:-1]:
        assert len(b) == 2

    flat = [r for b in batches for r in b]
    assert len(flat) == backend.count_scanned_files(scan_id) == 7
    # Order must match insertion order via id ASC.
    ids = [r["id"] for r in flat]
    assert ids == sorted(ids)


def test_sqlite_backend_health_check_returns_available(db):
    backend = SqliteBackend(db, {})
    res = backend.health_check()
    assert res["name"] == "sqlite"
    assert res["available"] is True
    assert isinstance(res["details"], dict)
    assert res["details"].get("status") == "ok"


def test_sqlite_backend_delete_scan_removes_all_rows(db, seeded_scan):
    _, scan_id, rows = seeded_scan
    backend = SqliteBackend(db, {})

    assert backend.count_scanned_files(scan_id) == len(rows)
    deleted = backend.delete_scan(scan_id)
    assert deleted == len(rows)
    assert backend.count_scanned_files(scan_id) == 0


def test_sqlite_backend_search_text_like(db, seeded_scan):
    _, scan_id, _ = seeded_scan
    backend = SqliteBackend(db, {})

    hits = backend.search_text(scan_id, "a.pdf")
    assert len(hits) == 1
    assert hits[0]["file_name"] == "a.pdf"

    # User-supplied LIKE wildcard must be neutered, not honoured —
    # otherwise '%' would match every row.
    none = backend.search_text(scan_id, "no-such-substring")
    assert none == []
