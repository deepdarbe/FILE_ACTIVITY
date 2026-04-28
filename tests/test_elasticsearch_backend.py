"""Integration tests for :class:`ElasticsearchBackend` (issue #114, Phase 2).

Skips cleanly when:
  - the ``elasticsearch`` client is not installed, or
  - ``testcontainers[elasticsearch]`` is not installed, or
  - Docker is not reachable on the host.

This is by design: the Phase 2 PR ships the backend as an optional
component. CI hosts without Docker (or without the optional deps)
should still get a green pytest run for the rest of the suite.
"""

from __future__ import annotations

import os
import time

import pytest

# ---- Optional-dep gating ---------------------------------------------------
elasticsearch = pytest.importorskip(
    "elasticsearch", reason="elasticsearch client not installed"
)

try:
    from testcontainers.elasticsearch import ElasticSearchContainer  # type: ignore
except Exception:  # pragma: no cover - import guard
    ElasticSearchContainer = None  # type: ignore[assignment]


from src.storage.backends.elasticsearch_backend import ElasticsearchBackend
from src.storage.database import Database


# ---- Fixtures --------------------------------------------------------------


@pytest.fixture(scope="module")
def es_container():
    """Spin up a single-node ES container for the module.

    Skips the entire module if testcontainers / Docker is unavailable.
    """
    if ElasticSearchContainer is None:
        pytest.skip("Docker / testcontainers unavailable")
    try:
        # ES 8.x; ``ElasticSearchContainer`` defaults to a recent tag
        # but pin if the user supplied one via env.
        image = os.environ.get(
            "FILEACTIVITY_TEST_ES_IMAGE",
            "docker.elastic.co/elasticsearch/elasticsearch:8.11.0",
        )
        container = ElasticSearchContainer(image)
        # Single-node, no security so the test client doesn't need
        # certs / api_key. Production config in operator-runbook.md.
        container.with_env("xpack.security.enabled", "false")
        container.start()
    except Exception as e:  # pragma: no cover - host-dependent
        pytest.skip(f"Docker / testcontainers unavailable: {e!r}")
    try:
        yield container
    finally:
        try:
            container.stop()
        except Exception:
            pass


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "phase2.db"
    inst = Database({"path": str(db_path)})
    inst.connect()
    yield inst
    try:
        inst.close()
    except Exception:
        pass


@pytest.fixture
def seeded(db, es_container):
    """Insert one source + one scan_run, build the backend, and
    return ``(backend, source_id, scan_id)``."""
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

    cfg = {
        "storage": {
            "backend": "elasticsearch",
            "elasticsearch": {
                "hosts": [es_container.get_url()],
                "verify_certs": False,
                "request_timeout": 30,
            },
        }
    }
    backend = ElasticsearchBackend(db, cfg)
    return backend, source_id, scan_id


def _refresh(backend: ElasticsearchBackend, scan_id: int, source_id: int) -> None:
    """Tests ONLY: force refresh so subsequent reads see writes.
    Production code does NOT do this; we keep the production paths
    refresh-free per the operator runbook."""
    index = f"scanned_files-{source_id}-{scan_id}"
    try:
        backend.client.indices.refresh(index=index)
    except Exception:
        pass


# ---- Tests -----------------------------------------------------------------


def test_insert_and_count(seeded):
    backend, source_id, scan_id = seeded
    rows = [
        {
            "source_id": source_id,
            "file_path": r"\\share\dir\a.pdf",
            "file_name": "a.pdf",
            "extension": "pdf",
            "file_size": 2_000_000,
            "owner": "alice",
            "last_modify_time": "2024-06-01T00:00:00",
        },
        {
            "source_id": source_id,
            "file_path": r"\\share\dir\b.pdf",
            "file_name": "b.pdf",
            "extension": "pdf",
            "file_size": 500_000,
            "owner": "bob",
            "last_modify_time": "2024-06-02T00:00:00",
        },
        {
            "source_id": source_id,
            "file_path": r"\\share\other\c.txt",
            "file_name": "c.txt",
            "extension": "txt",
            "file_size": 1024,
            "owner": "alice",
            "last_modify_time": "2024-06-03T00:00:00",
        },
    ]
    inserted = backend.insert_scanned_files(scan_id, rows)
    assert inserted == 3
    _refresh(backend, scan_id, source_id)
    assert backend.count_scanned_files(scan_id) == 3


def test_query_files_extension_filter(seeded):
    backend, source_id, scan_id = seeded
    rows = [
        {
            "source_id": source_id,
            "file_path": r"\\share\dir\a.pdf",
            "file_name": "a.pdf",
            "extension": "pdf",
            "file_size": 2_000_000,
            "owner": "alice",
        },
        {
            "source_id": source_id,
            "file_path": r"\\share\dir\b.pdf",
            "file_name": "b.pdf",
            "extension": "pdf",
            "file_size": 500_000,
            "owner": "bob",
        },
        {
            "source_id": source_id,
            "file_path": r"\\share\other\c.txt",
            "file_name": "c.txt",
            "extension": "txt",
            "file_size": 1024,
            "owner": "alice",
        },
    ]
    backend.insert_scanned_files(scan_id, rows)
    _refresh(backend, scan_id, source_id)

    pdfs = backend.query_files(scan_id, {"extension": "pdf"})
    assert len(pdfs) == 2
    assert all(r["extension"] == "pdf" for r in pdfs)

    # Whitelist enforcement matches SqliteBackend.
    with pytest.raises(ValueError):
        backend.query_files(scan_id, {"file_path; DROP": "x"})


def test_aggregate_group_by_extension(seeded):
    backend, source_id, scan_id = seeded
    rows = [
        {
            "source_id": source_id,
            "file_path": r"\\share\dir\a.pdf",
            "file_name": "a.pdf",
            "extension": "pdf",
            "file_size": 2_000_000,
            "owner": "alice",
        },
        {
            "source_id": source_id,
            "file_path": r"\\share\dir\b.pdf",
            "file_name": "b.pdf",
            "extension": "pdf",
            "file_size": 500_000,
            "owner": "bob",
        },
        {
            "source_id": source_id,
            "file_path": r"\\share\other\c.txt",
            "file_name": "c.txt",
            "extension": "txt",
            "file_size": 1024,
            "owner": "alice",
        },
    ]
    backend.insert_scanned_files(scan_id, rows)
    _refresh(backend, scan_id, source_id)

    counts = backend.aggregate(scan_id, "extension", "count")
    counts_by_ext = {r["extension"]: r["count"] for r in counts}
    assert counts_by_ext == {"pdf": 2, "txt": 1}

    sums = backend.aggregate(scan_id, "extension", "sum_size")
    sums_by_ext = {r["extension"]: r["sum_size"] for r in sums}
    assert sums_by_ext["pdf"] == 2_500_000
    assert sums_by_ext["txt"] == 1024

    with pytest.raises(ValueError):
        backend.aggregate(scan_id, "file_path", "count")
    with pytest.raises(ValueError):
        backend.aggregate(scan_id, "extension", "avg_size")


def test_search_text(seeded):
    backend, source_id, scan_id = seeded
    rows = [
        {
            "source_id": source_id,
            "file_path": r"\\share\dir\quarterly_report.pdf",
            "file_name": "quarterly_report.pdf",
            "extension": "pdf",
            "file_size": 1024,
        },
        {
            "source_id": source_id,
            "file_path": r"\\share\other\notes.txt",
            "file_name": "notes.txt",
            "extension": "txt",
            "file_size": 1024,
        },
    ]
    backend.insert_scanned_files(scan_id, rows)
    _refresh(backend, scan_id, source_id)

    hits = backend.search_text(scan_id, "quarterly")
    paths = [h["file_path"] for h in hits]
    assert any("quarterly_report" in p for p in paths)


def test_iterate_scan(seeded):
    backend, source_id, scan_id = seeded
    rows = [
        {
            "source_id": source_id,
            "file_path": f"\\\\share\\bulk\\f{i}.bin",
            "file_name": f"f{i}.bin",
            "extension": "bin",
            "file_size": 100 + i,
        }
        for i in range(5)
    ]
    backend.insert_scanned_files(scan_id, rows)
    _refresh(backend, scan_id, source_id)

    batches = list(backend.iterate_scan(scan_id, batch_size=2))
    flat = [r for b in batches for r in b]
    assert len(flat) == 5


def test_delete_scan(seeded):
    backend, source_id, scan_id = seeded
    rows = [
        {
            "source_id": source_id,
            "file_path": r"\\share\d\a.pdf",
            "file_name": "a.pdf",
            "extension": "pdf",
            "file_size": 1,
        },
    ]
    backend.insert_scanned_files(scan_id, rows)
    _refresh(backend, scan_id, source_id)
    assert backend.count_scanned_files(scan_id) == 1
    backend.delete_scan(scan_id)
    _refresh(backend, scan_id, source_id)
    assert backend.count_scanned_files(scan_id) == 0


def test_health_check(seeded):
    backend, _, _ = seeded
    res = backend.health_check()
    assert res["name"] == "elasticsearch"
    assert isinstance(res["details"], dict)
    # Container is up — ping should succeed.
    assert res["available"] is True


def test_health_check_never_raises(db):
    """Pointed at a dead host, health_check still returns a dict."""
    cfg = {
        "storage": {
            "backend": "elasticsearch",
            "elasticsearch": {
                "hosts": ["http://127.0.0.1:1"],  # closed port
                "verify_certs": False,
                "request_timeout": 1,
            },
        }
    }
    backend = ElasticsearchBackend(db, cfg)
    res = backend.health_check()
    assert res["name"] == "elasticsearch"
    assert res["available"] is False
    assert "details" in res
