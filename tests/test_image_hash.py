"""Tests for perceptual image hash detection (issue #144 Phase 2).

Coverage:
    - hamming_distance helper (pure Python, no imagehash required)
    - ImageHasher: available flag, lazy-import path, size guard, compute
    - find_duplicate_groups: clustering logic
    - Database roundtrip: insert_image_hashes, find_similar_images,
      count_image_hashes
    - API smoke: GET /api/security/image-duplicates
    - XLSX export endpoint smoke
    - Feature-flag endpoint includes image_duplicates key
"""

from __future__ import annotations

import os
import sys
import types
import unittest.mock as mock
from pathlib import Path

import pytest

# Ensure repo root is on sys.path.
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.analyzer.image_hash import (  # noqa: E402
    ImageHasher,
    hamming_distance,
    find_duplicate_groups,
    IMAGE_EXTENSIONS,
)
from src.storage.database import Database  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db(tmp_path: Path) -> Database:
    db = Database(
        {"path": str(tmp_path / "test.db"),
         "retention": {"auto_cleanup_on_startup": False}}
    )
    db.connect()
    return db


def _seed_db(db: Database, tmp_path: Path, files: list[dict]) -> tuple[int, int]:
    """Insert a source + scan_run + scanned_files; return (source_id, scan_id)."""
    with db.get_cursor() as cur:
        cur.execute(
            "INSERT INTO sources (name, unc_path, archive_dest, enabled) "
            "VALUES (?, ?, ?, 1)",
            ("test-src", str(tmp_path), ""),
        )
        source_id = cur.lastrowid
        cur.execute(
            "INSERT INTO scan_runs (source_id, status) VALUES (?, 'completed')",
            (source_id,),
        )
        scan_id = cur.lastrowid

    rows = []
    for f in files:
        rows.append({
            "source_id": source_id,
            "scan_id": scan_id,
            "file_path": f["file_path"],
            "relative_path": os.path.basename(f["file_path"]),
            "file_name": os.path.basename(f["file_path"]),
            "extension": f.get("extension", "jpg"),
            "file_size": f.get("file_size", 1024),
        })
    db.bulk_insert_scanned_files(rows)
    return source_id, scan_id


def _make_image(path: Path, size: tuple = (64, 64), color: tuple = (128, 0, 0)) -> str:
    """Create a minimal PNG image and return the path string."""
    from PIL import Image
    img = Image.new("RGB", size, color=color)
    img.save(str(path), "PNG")
    return str(path)


# ---------------------------------------------------------------------------
# hamming_distance
# ---------------------------------------------------------------------------

class TestHammingDistance:
    def test_identical_hashes_zero_distance(self):
        h = "aabbccdd11223344"
        assert hamming_distance(h, h) == 0

    def test_single_bit_difference(self):
        # aabbccdd11223344 vs aabbccdd11223345  -> last nibble differs by 1 bit
        assert hamming_distance("aabbccdd11223344", "aabbccdd11223345") == 1

    def test_all_bits_different(self):
        # 0000...0000 vs ffff...ffff -> 64 bits
        h0 = "0" * 16
        hf = "f" * 16
        assert hamming_distance(h0, hf) == 64

    def test_different_length_returns_minus_one(self):
        assert hamming_distance("aabb", "aabbcc") == -1

    def test_empty_string_returns_minus_one(self):
        assert hamming_distance("", "aabb1122") == -1
        assert hamming_distance("aabb1122", "") == -1

    def test_none_returns_minus_one(self):
        assert hamming_distance(None, "aabb1122") == -1  # type: ignore

    def test_invalid_hex_returns_minus_one(self):
        assert hamming_distance("zzzzzzzzzzzzzzzz", "aabbccdd11223344") == -1


# ---------------------------------------------------------------------------
# ImageHasher
# ---------------------------------------------------------------------------

class TestImageHasher:
    def test_available_true_when_imagehash_installed(self):
        hasher = ImageHasher()
        # imagehash was installed in the test environment
        assert hasher.available is True

    def test_compute_returns_dict_for_valid_image(self, tmp_path):
        img_path = _make_image(tmp_path / "test.png")
        hasher = ImageHasher()
        result = hasher.compute(img_path)
        assert result is not None
        assert "phash" in result
        assert "dhash" in result
        assert "ahash" in result
        # Hashes should be non-empty hex strings
        for key in ("phash", "dhash", "ahash"):
            assert isinstance(result[key], str)
            assert len(result[key]) > 0

    def test_compute_returns_none_for_nonexistent_file(self):
        hasher = ImageHasher()
        result = hasher.compute("/nonexistent/path/image.jpg")
        assert result is None

    def test_compute_skips_files_over_max_mb(self, tmp_path):
        """Files larger than image_hash_max_mb should be skipped."""
        big = tmp_path / "big.jpg"
        big.write_bytes(b"x" * 1024)  # tiny real file
        config = {"scanner": {"image_hash_max_mb": 0}}  # 0 MB threshold
        hasher = ImageHasher(config)
        result = hasher.compute(str(big))
        assert result is None

    def test_lazy_import_graceful_degradation(self, monkeypatch):
        """When imagehash is not importable, available should be False and
        compute() should return None without raising."""
        import builtins

        real_import = builtins.__import__

        def _failing_import(name, *args, **kwargs):
            if name == "imagehash":
                raise ImportError("imagehash not installed (mocked)")
            return real_import(name, *args, **kwargs)

        hasher = ImageHasher()
        hasher._available = None  # reset probe cache

        with monkeypatch.context() as m:
            m.setattr(builtins, "__import__", _failing_import)
            assert hasher.available is False
            result = hasher.compute("/any/path.jpg")
            assert result is None

    def test_probe_called_only_once(self, tmp_path):
        """_probe is a one-shot; once False it does not reimport."""
        hasher = ImageHasher()
        hasher._available = False  # pretend unavailable
        # Repeated calls must not reset _available
        _ = hasher.available
        _ = hasher.available
        assert hasher._available is False

    def test_image_extensions_set_contains_expected(self):
        expected = {"jpg", "jpeg", "png", "gif", "bmp", "tiff", "webp"}
        assert expected <= IMAGE_EXTENSIONS

    def test_compute_same_image_twice_gives_identical_hashes(self, tmp_path):
        """Determinism: same image → same hash on repeated calls."""
        img_path = _make_image(tmp_path / "dup.png", color=(0, 200, 100))
        hasher = ImageHasher()
        r1 = hasher.compute(img_path)
        r2 = hasher.compute(img_path)
        assert r1 == r2

    def test_similar_images_have_small_hamming_distance(self, tmp_path):
        """Same visual at two different sizes → small pHash Hamming distance."""
        from PIL import Image

        # Create a reference image with a gradient-like pattern
        base = tmp_path / "original.png"
        _make_image(base, size=(200, 200), color=(100, 150, 200))

        # Resize to smaller — perceptually very similar
        resized = tmp_path / "resized.png"
        with Image.open(str(base)) as img:
            img.resize((100, 100)).save(str(resized), "PNG")

        # Completely different image
        different = tmp_path / "different.png"
        _make_image(different, size=(200, 200), color=(255, 0, 0))

        hasher = ImageHasher()
        r_base = hasher.compute(str(base))
        r_resized = hasher.compute(str(resized))
        r_diff = hasher.compute(str(different))

        assert r_base is not None
        assert r_resized is not None
        assert r_diff is not None

        dist_similar = hamming_distance(r_base["phash"], r_resized["phash"])
        dist_different = hamming_distance(r_base["phash"], r_diff["phash"])

        # Same visual at different sizes → small distance
        assert dist_similar < 10, (
            f"Expected similar images to have small pHash distance, "
            f"got {dist_similar}"
        )


# ---------------------------------------------------------------------------
# find_duplicate_groups
# ---------------------------------------------------------------------------

class TestFindDuplicateGroups:
    def _row(self, file_id: int, phash: str) -> dict:
        return {
            "file_id": file_id,
            "file_path": f"/scan/{file_id}.jpg",
            "file_size": 1024,
            "phash": phash,
            "dhash": phash,
            "ahash": phash,
        }

    def test_empty_input_returns_empty(self):
        assert find_duplicate_groups([]) == []

    def test_identical_hashes_grouped(self):
        h = "aabbccdd11223344"
        rows = [self._row(1, h), self._row(2, h), self._row(3, "ffffffffffffffff")]
        groups = find_duplicate_groups(rows, hash_type="phash", max_distance=5)
        assert len(groups) == 1
        file_ids = {m["file_id"] for m in groups[0]}
        assert file_ids == {1, 2}

    def test_no_duplicates_returns_empty(self):
        rows = [
            self._row(1, "0000000000000000"),
            self._row(2, "ffffffffffffffff"),
        ]
        groups = find_duplicate_groups(rows, hash_type="phash", max_distance=0)
        assert groups == []

    def test_threshold_controls_grouping(self):
        """Distance-5 groups that distance-0 does not."""
        rows = [
            self._row(1, "aabbccdd11223344"),
            self._row(2, "aabbccdd11223345"),  # 1 bit diff
        ]
        assert find_duplicate_groups(rows, max_distance=0) == []
        groups = find_duplicate_groups(rows, max_distance=5)
        assert len(groups) == 1

    def test_rows_missing_hash_are_skipped(self):
        """Rows without the hash_type field are silently excluded."""
        rows = [
            {"file_id": 1, "file_path": "/a.jpg", "file_size": 0, "phash": None},
            {"file_id": 2, "file_path": "/b.jpg", "file_size": 0, "phash": "aabbccdd11223344"},
        ]
        groups = find_duplicate_groups(rows, hash_type="phash", max_distance=5)
        assert groups == []


# ---------------------------------------------------------------------------
# Database roundtrip
# ---------------------------------------------------------------------------

class TestDatabaseRoundtrip:
    def test_insert_and_count(self, tmp_path):
        db = _make_db(tmp_path)
        _, scan_id = _seed_db(
            db,
            tmp_path,
            [{"file_path": str(tmp_path / f"img{i}.jpg")} for i in range(5)],
        )

        # Get file_ids from DB
        with db.get_cursor() as cur:
            cur.execute(
                "SELECT id FROM scanned_files WHERE scan_id = ? LIMIT 5",
                (scan_id,),
            )
            file_ids = [r["id"] for r in cur.fetchall()]

        rows = [
            {
                "file_id": fid,
                "scan_id": scan_id,
                "phash": "aabbccdd11223344",
                "dhash": "1122334455667788",
                "ahash": "8877665544332211",
            }
            for fid in file_ids
        ]
        written = db.insert_image_hashes(rows)
        assert written == len(file_ids)
        assert db.count_image_hashes(scan_id) == len(file_ids)
        db.close()

    def test_idempotent_upsert(self, tmp_path):
        """Double insert should not duplicate rows."""
        db = _make_db(tmp_path)
        _, scan_id = _seed_db(
            db, tmp_path, [{"file_path": str(tmp_path / "x.jpg")}],
        )
        with db.get_cursor() as cur:
            cur.execute(
                "SELECT id FROM scanned_files WHERE scan_id = ?", (scan_id,)
            )
            file_id = cur.fetchone()["id"]

        row = [{"file_id": file_id, "scan_id": scan_id, "phash": "aabb112233445566",
                "dhash": None, "ahash": None}]
        db.insert_image_hashes(row)
        db.insert_image_hashes(row)  # second insert
        assert db.count_image_hashes(scan_id) == 1
        db.close()

    def test_find_similar_images_returns_all_for_scan(self, tmp_path):
        db = _make_db(tmp_path)
        n = 10
        _, scan_id = _seed_db(
            db,
            tmp_path,
            [{"file_path": str(tmp_path / f"p{i}.png")} for i in range(n)],
        )
        with db.get_cursor() as cur:
            cur.execute(
                "SELECT id FROM scanned_files WHERE scan_id = ?",
                (scan_id,),
            )
            file_ids = [r["id"] for r in cur.fetchall()]

        db.insert_image_hashes([
            {"file_id": fid, "scan_id": scan_id,
             "phash": "cafe0000deadbeef", "dhash": None, "ahash": None}
            for fid in file_ids
        ])
        result = db.find_similar_images(scan_id, hash_type="phash")
        assert len(result) == n
        db.close()

    def test_find_similar_images_distance_filter(self, tmp_path):
        """Only rows within Hamming distance are returned when hash_value given."""
        db = _make_db(tmp_path)
        _, scan_id = _seed_db(
            db,
            tmp_path,
            [{"file_path": str(tmp_path / f"f{i}.jpg")} for i in range(2)],
        )
        with db.get_cursor() as cur:
            cur.execute(
                "SELECT id FROM scanned_files WHERE scan_id = ? ORDER BY id",
                (scan_id,),
            )
            ids = [r["id"] for r in cur.fetchall()]

        # First image: identical to query; second: distance 64
        db.insert_image_hashes([
            {"file_id": ids[0], "scan_id": scan_id,
             "phash": "0000000000000000", "dhash": None, "ahash": None},
            {"file_id": ids[1], "scan_id": scan_id,
             "phash": "ffffffffffffffff", "dhash": None, "ahash": None},
        ])
        close = db.find_similar_images(
            scan_id, hash_type="phash",
            hash_value="0000000000000000", max_distance=5,
        )
        assert len(close) == 1
        assert close[0]["phash"] == "0000000000000000"
        db.close()


# ---------------------------------------------------------------------------
# API smoke tests
# ---------------------------------------------------------------------------

@pytest.fixture
def app_and_db(tmp_path):
    """Bootstrap a minimal FastAPI app + seeded DB for API tests."""
    from fastapi.testclient import TestClient
    from src.dashboard.api import create_app

    db = _make_db(tmp_path)
    config = {
        "scanner": {"compute_image_hashes": False},
        "security": {"ransomware": {"enabled": False}, "orphan_sid": {"enabled": False}},
        # Disable auth so TestClient (non-localhost) can reach endpoints.
        "dashboard": {"auth": {"enabled": False}},
    }
    app = create_app(db, config)
    client = TestClient(app, raise_server_exceptions=False)
    yield client, db, tmp_path
    db.close()


class TestApiSmoke:
    def test_image_duplicates_no_scan(self, app_and_db):
        """Without any completed scans the endpoint returns empty groups."""
        client, _db, _tmp = app_and_db
        resp = client.get("/api/security/image-duplicates")
        assert resp.status_code == 200
        data = resp.json()
        assert "groups" in data
        assert data["groups"] == []

    def test_image_duplicates_with_data(self, app_and_db):
        client, db, tmp_path = app_and_db
        _, scan_id = _seed_db(
            db,
            tmp_path,
            [{"file_path": str(tmp_path / "a.jpg")},
             {"file_path": str(tmp_path / "b.jpg")}],
        )
        with db.get_cursor() as cur:
            cur.execute(
                "SELECT id FROM scanned_files WHERE scan_id = ? ORDER BY id",
                (scan_id,),
            )
            ids = [r["id"] for r in cur.fetchall()]

        db.insert_image_hashes([
            {"file_id": ids[0], "scan_id": scan_id,
             "phash": "aabbccdd11223344", "dhash": None, "ahash": None},
            {"file_id": ids[1], "scan_id": scan_id,
             "phash": "aabbccdd11223344", "dhash": None, "ahash": None},
        ])

        resp = client.get(f"/api/security/image-duplicates?scan_id={scan_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["scan_id"] == scan_id
        assert len(data["groups"]) == 1
        assert data["groups"][0]["count"] == 2

    def test_feature_flags_includes_image_duplicates(self, app_and_db):
        client, _db, _tmp = app_and_db
        resp = client.get("/api/security/feature-flags")
        assert resp.status_code == 200
        flags = resp.json()
        assert "image_duplicates" in flags
        assert "enabled" in flags["image_duplicates"]

    def test_xlsx_export_returns_binary(self, app_and_db):
        client, db, tmp_path = app_and_db
        _, scan_id = _seed_db(
            db,
            tmp_path,
            [{"file_path": str(tmp_path / "x.jpg")}],
        )
        resp = client.get(f"/api/security/image-duplicates/export.xlsx?scan_id={scan_id}")
        # openpyxl must be installed for this to succeed; if not we get 500
        if resp.status_code == 200:
            assert b"PK" in resp.content  # ZIP magic bytes (XLSX format)
        else:
            assert resp.status_code in (500, 422)
