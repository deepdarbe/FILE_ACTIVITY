"""Tests for ContentDuplicateEngine (issue #35).

Fixture layout:
    - dup_a.bin, dup_b.bin: ayni icerik (true duplicate)
    - false_dup.bin: ayni boyut + ayni ad tabanli ama farkli icerik
      (name-only dedup'in yakalardi, content dedup yakalamamali)
    - unique_1/2/3.bin: tamamen benzersiz dosyalar

`compute` sonrasi:
    - tam olarak 1 true duplicate grup
    - grupta 2 member
    - `bytes_hashed` < tum dosyalarin toplam boyutu
      (prefix-only eleme, full-hash gerektirmeyen dosyalar tasarruf saglar)
    - `get_report` 1. sayfada grubu dondurur
"""

import os
import sys
import pytest

# Repo kokunu sys.path'e ekle (tests klasoru pytest tarafindan kesfedildiginde)
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.storage.database import Database  # noqa: E402
from src.analyzer.content_duplicates import ContentDuplicateEngine  # noqa: E402


# --- Fixture helpers ---------------------------------------------------

# Boyutlar prefix_bytes = 4096'nin ustune cikacak sekilde secildi
# ki prefix tier'in ilk 4 KB'dan sonra karar verebilmesini test edelim.
FILE_SIZE = 8192  # 8 KB


def _write(path: str, data: bytes, size: int) -> None:
    """Fixture dosyasi uret: `data` ile baslayip toplam `size` bayta pad."""
    assert len(data) <= size
    with open(path, "wb") as f:
        f.write(data)
        remaining = size - len(data)
        if remaining > 0:
            f.write(b"\x00" * remaining)


@pytest.fixture
def fixture_files(tmp_path):
    """6 dosyali fixture olustur. Returns list[(path, size)]."""
    root = tmp_path / "scan_root"
    root.mkdir()

    files = []

    # True duplicate cifti — ayni icerik, ayni boyut
    dup_payload = b"DUPLICATE-CONTENT-" + b"A" * 200
    dup_a = root / "dup_a.bin"
    dup_b = root / "sub" / "dup_b.bin"
    dup_b.parent.mkdir()
    _write(str(dup_a), dup_payload, FILE_SIZE)
    _write(str(dup_b), dup_payload, FILE_SIZE)
    files.append((str(dup_a), FILE_SIZE))
    files.append((str(dup_b), FILE_SIZE))

    # False duplicate: ayni boyut, farkli icerik. Prefix hash'i de farkli
    # olacak sekilde ilk byte'i degistiriyoruz ki prefix tier elesin.
    false_dup = root / "false_dup.bin"
    _write(str(false_dup), b"NOT-SAME-CONTENT-" + b"B" * 200, FILE_SIZE)
    files.append((str(false_dup), FILE_SIZE))

    # Uc benzersiz dosya — farkli boyutlarda, size tier'da hic eslesmez.
    unique_1 = root / "unique_1.bin"
    _write(str(unique_1), b"U1", FILE_SIZE + 100)
    files.append((str(unique_1), FILE_SIZE + 100))

    unique_2 = root / "unique_2.bin"
    _write(str(unique_2), b"U2", FILE_SIZE + 200)
    files.append((str(unique_2), FILE_SIZE + 200))

    unique_3 = root / "unique_3.bin"
    _write(str(unique_3), b"U3", FILE_SIZE + 300)
    files.append((str(unique_3), FILE_SIZE + 300))

    return files


@pytest.fixture
def db_and_scan(tmp_path, fixture_files):
    """Bos bir DB olustur, source + scan_run + scanned_files doldur."""
    db_path = tmp_path / "test.db"
    db = Database({"path": str(db_path), "retention": {"auto_cleanup_on_startup": False}})
    db.connect()

    # Source olustur (raw SQL, Source modelini import etmeye gerek yok)
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (name, unc_path, archive_dest, enabled) VALUES (?, ?, ?, 1)",
            ("test-src", str(tmp_path / "scan_root"), ""),
        )
        source_id = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs (source_id, status, total_files) VALUES (?, 'completed', ?)",
            (source_id, len(fixture_files)),
        )
        scan_id = cur.lastrowid

    # scanned_files doldur
    scanned = []
    for path, size in fixture_files:
        scanned.append({
            "source_id": source_id,
            "scan_id": scan_id,
            "file_path": path,
            "relative_path": os.path.basename(path),
            "file_name": os.path.basename(path),
            "extension": "bin",
            "file_size": size,
        })
    db.bulk_insert_scanned_files(scanned)

    yield db, source_id, scan_id, fixture_files

    db.close()


# --- Tests -------------------------------------------------------------


def test_compute_finds_one_true_group(db_and_scan):
    db, source_id, scan_id, files = db_and_scan
    config = {
        "content_duplicates": {
            "enabled": True,
            # Fixture dosyalari kucuk oldugu icin min_bytes'i dusuruyoruz.
            "min_bytes": 1024,
            "workers": 1,  # Test deterministic olsun
            "prefix_bytes": 4096,
        }
    }
    engine = ContentDuplicateEngine(db, config)

    stats = engine.compute(scan_id, min_bytes=1024)

    # Size tier: 3 dosya ayni 8192 boyutunda (dup_a, dup_b, false_dup)
    # -> 1 size grup.
    assert stats["total_size_groups"] == 1, stats
    # True grup: tam olarak 1 (dup_a + dup_b)
    assert stats["true_groups"] == 1, stats

    # Prefix tier yalnizca dup_a/dup_b'yi ayni bucket'a koyar; false_dup
    # farkli prefix'e dustugu icin singleton olup prefix_collisions'e girmez.
    assert stats["prefix_collisions"] == 1, stats

    # Full-hash yapilan dosya sayisi: sadece prefix bucket'inda kalan iki dosya.
    assert stats["files_hashed_fully"] == 2, stats

    # bytes_hashed < toplam dosya boyutu toplami (prefix tier tasarruf sagladi)
    total_size = sum(sz for _, sz in files)
    assert stats["bytes_hashed"] < total_size, (
        f"bytes_hashed={stats['bytes_hashed']} total_size={total_size} — "
        "prefix short-circuit tasarruf saglayamadi"
    )

    assert stats["duration_seconds"] >= 0


def test_get_report_returns_group(db_and_scan):
    db, source_id, scan_id, _files = db_and_scan
    config = {"content_duplicates": {"enabled": True, "min_bytes": 1024, "workers": 1}}
    engine = ContentDuplicateEngine(db, config)

    engine.compute(scan_id, min_bytes=1024)
    report = engine.get_report(scan_id, page=1, page_size=50)

    assert report["scan_id"] == scan_id
    assert report["total_groups"] == 1
    assert report["page"] == 1
    assert len(report["groups"]) == 1

    group = report["groups"][0]
    assert group["file_count"] == 2
    assert group["file_size"] == FILE_SIZE
    # waste_size = (count - 1) * file_size
    assert group["waste_size"] == FILE_SIZE
    assert len(group["files"]) == 2
    # content_hash mevcut ve hex string
    assert isinstance(group["content_hash"], str)
    assert len(group["content_hash"]) == 64  # SHA-256 hex


def test_compute_is_idempotent(db_and_scan):
    """Ayni scan_id icin compute tekrar cagrildiginda tablolar duplicate
    satir birakmaz (UNIQUE/DELETE kombinasyonu caliziyor)."""
    db, source_id, scan_id, _ = db_and_scan
    config = {"content_duplicates": {"enabled": True, "min_bytes": 1024, "workers": 1}}
    engine = ContentDuplicateEngine(db, config)

    engine.compute(scan_id, min_bytes=1024)
    engine.compute(scan_id, min_bytes=1024)

    report = engine.get_report(scan_id)
    assert report["total_groups"] == 1
    assert len(report["groups"][0]["files"]) == 2
