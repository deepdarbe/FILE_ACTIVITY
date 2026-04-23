"""Tests for issue #34: compute_scan_summary v2 aggregates.

Fixture: 30 files across age/size/extension/owner dimensions.
Covers all 7 new keys + existing keys backward-compat.
"""
from __future__ import annotations

import os
import sys
import tempfile
from datetime import datetime, timedelta

import pytest

# Ensure repo root on sys.path so "import src.*" works when running
# pytest directly from a worktree.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.database import Database  # noqa: E402


# Config'daki size_buckets ile ayni esikler — default_config fallback
SIZE_BUCKETS = {
    "tiny": 102400,          # < 100 KB
    "small": 1048576,        # < 1 MB
    "medium": 104857600,     # < 100 MB
    "large": 1073741824,     # < 1 GB
}


def _ts(days_ago: int) -> str:
    return (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")


@pytest.fixture
def db_with_scan(tmp_path):
    """30 dosya iceren bir scan hazirla."""
    db_path = tmp_path / "test.db"
    config = {
        "path": str(db_path),
        # size_buckets config'i pass ederek test izolasyonu saglaniyor
        "analysis": {"size_buckets": SIZE_BUCKETS},
    }
    db = Database(config)
    db.connect()

    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources(name, unc_path, archive_dest) VALUES(?, ?, ?)",
            ("test_src", "/tmp/src", "/tmp/arch"),
        )
        source_id = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs(source_id, status) VALUES(?, 'completed')",
            (source_id,),
        )
        scan_id = cur.lastrowid

        # 30 dosya: 5 yas kovasina * ~6 dosya, farkli boyut/uzanti/owner dagilimi
        files = []
        # Yas kovalarina dagilim: (age_days, count)
        age_distribution = [
            (10, 6),    # 0-30
            (60, 6),    # 31-90
            (150, 6),   # 91-180
            (300, 6),   # 181-365
            (500, 6),   # 366+
        ]
        idx = 0
        extensions_cycle = ["exe", "txt", "pdf", "jpg", "bat", "doc"]
        # Boyut cycle'i: tiny, small, medium, large, huge dagilimi
        sizes_cycle = [
            50_000,            # tiny
            500_000,           # small
            50_000_000,        # medium
            500_000_000,       # large
            2_000_000_000,     # huge
            10_000,            # tiny
        ]
        # owner cycle — 3 normal, 1 NULL, 1 empty per 6
        owners_cycle = ["alice", "bob", "carol", None, "", "alice"]

        for age_days, count in age_distribution:
            access_ts = _ts(age_days)
            modify_ts = _ts(age_days + 1)
            for i in range(count):
                ext = extensions_cycle[idx % len(extensions_cycle)]
                size = sizes_cycle[idx % len(sizes_cycle)]
                owner = owners_cycle[idx % len(owners_cycle)]
                fname = f"f{idx}.{ext}"
                files.append((
                    source_id,
                    scan_id,
                    f"/tmp/src/{fname}",
                    fname,
                    fname,
                    ext,
                    size,
                    access_ts,
                    modify_ts,
                    owner,
                ))
                idx += 1

        # En buyuk dosyayi tekillestir — top_large_files[0] > [1] garantili
        mega = list(files[0])
        mega[6] = 9_000_000_000  # file_size
        mega[2] = "/tmp/src/MEGA.bin"
        mega[3] = "MEGA.bin"
        mega[4] = "MEGA.bin"
        mega[5] = "bin"
        files[0] = tuple(mega)

        cur.executemany(
            "INSERT INTO scanned_files("
            "source_id, scan_id, file_path, relative_path, file_name, "
            "extension, file_size, last_access_time, last_modify_time, owner"
            ") VALUES(?,?,?,?,?,?,?,?,?,?)",
            files,
        )

    yield db, scan_id, files

    db.close()


def test_summary_v2_has_version_and_all_new_keys(db_with_scan):
    db, scan_id, _files = db_with_scan
    s = db.compute_scan_summary(scan_id)

    assert s["summary_json_version"] == 2

    new_keys = {
        "age_buckets",
        "size_buckets",
        "extension_size_breakdown",
        "top_risky_files",
        "top_large_files",
        "orphan_owner_count",
        "summary_json_version",
    }
    missing = new_keys - set(s.keys())
    assert not missing, f"Missing keys: {missing}"


def test_age_buckets_structure(db_with_scan):
    db, scan_id, _files = db_with_scan
    s = db.compute_scan_summary(scan_id)

    age = s["age_buckets"]
    assert isinstance(age, list)
    assert len(age) == 5

    labels = [b["label"] for b in age]
    assert labels == ["0-30", "31-90", "91-180", "181-365", "366+"]

    # Her kova 6 dosya icermeli (fixture dagilimi)
    for b in age:
        assert b["file_count"] == 6, f"Bucket {b['label']}: {b['file_count']}"
        assert "days_min" in b and "days_max" in b
        assert "total_size" in b


def test_size_buckets_matches_config(db_with_scan):
    db, scan_id, _files = db_with_scan
    s = db.compute_scan_summary(scan_id)

    sb = s["size_buckets"]
    assert isinstance(sb, list)
    # 4 config entry + 1 "huge" = 5
    assert len(sb) == len(SIZE_BUCKETS) + 1

    labels = [b["label"] for b in sb]
    # Config siralamasi + huge
    assert "huge" in labels
    for key in SIZE_BUCKETS:
        assert key in labels

    # Toplam dosya sayisi 30 olmali
    total = sum(b["file_count"] for b in sb)
    assert total == 30


def test_top_large_files_sorted_desc(db_with_scan):
    db, scan_id, _files = db_with_scan
    s = db.compute_scan_summary(scan_id)

    tl = s["top_large_files"]
    assert len(tl) > 1
    assert tl[0]["file_size"] > tl[1]["file_size"]
    # Tum alanlar dolu
    for f in tl[:3]:
        assert "file_path" in f
        assert "relative_path" in f
        assert "file_size" in f
        assert "owner" in f
        assert "last_access_time" in f
        assert "extension" in f


def test_top_risky_files_only_risky_extensions(db_with_scan):
    db, scan_id, _files = db_with_scan
    s = db.compute_scan_summary(scan_id)

    risky_set = {"exe", "bat", "ps1", "vbs", "cmd", "com", "scr", "msi", "js", "wsf"}
    for f in s["top_risky_files"]:
        assert f["extension"] in risky_set
    # Sira: boyuta gore azalan
    sizes = [f["file_size"] for f in s["top_risky_files"]]
    assert sizes == sorted(sizes, reverse=True)


def test_orphan_owner_count_matches_manual(db_with_scan):
    db, scan_id, files = db_with_scan
    s = db.compute_scan_summary(scan_id)

    # Manuel sayim: owner None veya ''
    manual = sum(1 for row in files if row[9] is None or row[9] == "")
    assert s["orphan_owner_count"] == manual


def test_extension_size_breakdown_ordered_by_size(db_with_scan):
    db, scan_id, _files = db_with_scan
    s = db.compute_scan_summary(scan_id)

    esb = s["extension_size_breakdown"]
    assert isinstance(esb, list)
    # Azalan sirada
    sizes = [e["size"] for e in esb]
    assert sizes == sorted(sizes, reverse=True)
    # En fazla 20
    assert len(esb) <= 20


def test_existing_keys_still_present(db_with_scan):
    db, scan_id, _files = db_with_scan
    s = db.compute_scan_summary(scan_id)

    existing = {
        "total_files", "total_size", "owner_count",
        "stale_count", "stale_size",
        "risky_count",
        "large_count", "large_size",
        "duplicate_groups", "duplicate_waste_size", "duplicate_files",
        "top_extensions", "top_owners",
    }
    missing = existing - set(s.keys())
    assert not missing, f"v1 keys lost: {missing}"
    assert s["total_files"] == 30


def test_backfill_reruns_v1_summaries(db_with_scan):
    """summary_json_version < 2 olan scan'ler yeniden hesaplanmali."""
    import json as _json

    db, scan_id, _files = db_with_scan

    # Kasten v1 (versionsuz) bir summary yerlestir
    with db.get_cursor() as cur:
        cur.execute(
            "UPDATE scan_runs SET summary_json=? WHERE id=?",
            (_json.dumps({"total_files": 0}), scan_id),
        )

    n = db.backfill_missing_summaries()
    assert n == 1

    s = db.get_scan_summary(scan_id)
    assert s is not None
    assert s["summary_json_version"] == 2
    assert s["total_files"] == 30
