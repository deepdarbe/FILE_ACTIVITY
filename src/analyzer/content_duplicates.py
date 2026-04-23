"""Tiered content-hash duplicate detection pipeline (issue #35).

Algoritma (fclones/jdupes'dan esinlenildi):

1. SIZE grouping: ayni boyutta >=2 dosyayi bul. SQL tarafinda yapilir;
   icerik okunmaz. Disk I/O = 0.
2. PREFIX hash (ilk 4 KB): her aday dosyanin ilk ~4 KB'si okunur,
   SHA-256 hesaplanir. (size, prefix_hash) ile grupla; tekiller duser.
   Cok buyuk ama farkli baslayan dosyalari tam hashlemeden eler.
3. FULL hash: kalan (prefix-collision) gruplar icin tam SHA-256
   (1 MB chunk'lar). (size, full_hash) ile grupla; tekiller duser.
   Geriye kalan = gercek icerik kopyalari.

Sonuclar `duplicate_hash_groups` + `duplicate_hash_members` tablolarina
yazilir; endpoint tekrar hesaplamak yerine buradan okur.

Dikkat:
- Worker havuzu: `concurrent.futures.ProcessPoolExecutor` (CPU-bound).
- `hashlib.sha256`, 1 MB chunk. mmap kullanmayiz — Windows'ta buyuk
  dosyalarda sorun cikariyor.
- Per-dosya hatalar (PermissionError/OSError) graceful handle edilir:
  dosyayi atla, debug logla, gruba devam et.
"""

import hashlib
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Optional

logger = logging.getLogger("file_activity.analyzer.content_duplicates")

# Tam hash icin chunk boyutu. mmap kullanmiyoruz (Windows + buyuk dosya
# = EACCES/ShareViolation). Senkron read(1 MB) her durumda guvenli.
_FULL_HASH_CHUNK = 1 << 20  # 1 MB


def _hash_prefix(file_path: str, prefix_bytes: int) -> Optional[tuple]:
    """Bir dosyanin ilk `prefix_bytes` baytini SHA-256'la.

    Returns:
        (file_path, hex_digest, bytes_read) veya hata varsa None.
    """
    try:
        h = hashlib.sha256()
        bytes_read = 0
        with open(file_path, "rb") as f:
            data = f.read(prefix_bytes)
            bytes_read = len(data)
            h.update(data)
        return (file_path, h.hexdigest(), bytes_read)
    except (PermissionError, OSError) as e:
        logger.debug("prefix hash atlandi %s: %s", file_path, e)
        return None


def _hash_full(file_path: str) -> Optional[tuple]:
    """Bir dosyanin tamamini SHA-256'la (1 MB chunk)."""
    try:
        h = hashlib.sha256()
        bytes_read = 0
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(_FULL_HASH_CHUNK)
                if not chunk:
                    break
                h.update(chunk)
                bytes_read += len(chunk)
        return (file_path, h.hexdigest(), bytes_read)
    except (PermissionError, OSError) as e:
        logger.debug("full hash atlandi %s: %s", file_path, e)
        return None


class ContentDuplicateEngine:
    """Tiered content-hash duplicate detector.

    Kullanim:
        engine = ContentDuplicateEngine(db, config)
        stats = engine.compute(scan_id, min_bytes=1_048_576)
        page = engine.get_report(scan_id, page=1, page_size=50)
    """

    def __init__(self, db, config: dict):
        self.db = db
        cd_cfg = (config or {}).get("content_duplicates", {}) or {}
        self.enabled = bool(cd_cfg.get("enabled", True))
        self.default_min_bytes = int(cd_cfg.get("min_bytes", 1_048_576))
        self.workers = max(1, int(cd_cfg.get("workers", 4)))
        self.prefix_bytes = max(1, int(cd_cfg.get("prefix_bytes", 4096)))

    # ---- Public API --------------------------------------------------

    def compute(
        self,
        scan_id: int,
        *,
        min_bytes: Optional[int] = None,
        max_groups: Optional[int] = None,
    ) -> dict:
        """Bir scan icin uc kademeli hash pipeline'i calistir ve sonuclari
        kalici olarak yaz.

        Returns:
            {
              "total_size_groups": <size ile eslesen grup sayisi>,
              "prefix_collisions": <prefix hash'i ayni olan grup sayisi>,
              "true_groups": <gercek icerik kopya grubu sayisi>,
              "files_hashed_fully": <full hash yapilan dosya sayisi>,
              "bytes_hashed": <toplam okunan bayt>,
              "duration_seconds": <suresi>,
            }
        """
        started = time.monotonic()
        if min_bytes is None:
            min_bytes = self.default_min_bytes
        bytes_hashed = 0

        # --- Tier 1: SIZE grouping --------------------------------------
        size_groups = self._fetch_size_groups(scan_id, min_bytes)
        total_size_groups = len(size_groups)
        logger.info(
            "content-duplicates scan=%s tier1 size: %d grup (min_bytes=%d)",
            scan_id, total_size_groups, min_bytes,
        )
        if not size_groups:
            self._persist([], scan_id)
            duration = time.monotonic() - started
            return {
                "total_size_groups": 0,
                "prefix_collisions": 0,
                "true_groups": 0,
                "files_hashed_fully": 0,
                "bytes_hashed": 0,
                "duration_seconds": round(duration, 3),
            }

        if max_groups is not None:
            # Debug/test amacli grup sayisini sinirla
            size_groups = size_groups[:max_groups]

        # --- Tier 2: PREFIX hash ----------------------------------------
        # (size, prefix_hash) -> [file_paths]
        prefix_candidates: list[tuple[int, str]] = []
        for sz, paths in size_groups:
            for p in paths:
                prefix_candidates.append((sz, p))

        prefix_hashes = self._run_hash_pool(
            [p for _, p in prefix_candidates],
            hash_fn=_hash_prefix,
            hash_fn_arg=self.prefix_bytes,
            label="prefix",
        )
        # Topla
        prefix_buckets: dict[tuple[int, str], list[str]] = {}
        file_to_size = {p: sz for sz, p in prefix_candidates}
        for path, digest, nread in prefix_hashes:
            bytes_hashed += nread
            key = (file_to_size[path], digest)
            prefix_buckets.setdefault(key, []).append(path)
        # Singleton'lari dus: prefix'i tekilse iki tarafta ayni boyutta
        # farkli dosya olmasi demek, full hashe gerek yok.
        prefix_collision_groups = [
            (sz, paths) for (sz, _h), paths in prefix_buckets.items() if len(paths) >= 2
        ]
        prefix_collisions = len(prefix_collision_groups)
        logger.info(
            "content-duplicates scan=%s tier2 prefix: %d kolizyon grubu",
            scan_id, prefix_collisions,
        )

        # --- Tier 3: FULL hash ------------------------------------------
        full_candidates = []
        for sz, paths in prefix_collision_groups:
            for p in paths:
                full_candidates.append((sz, p))
        files_hashed_fully = len(full_candidates)

        full_hashes = self._run_hash_pool(
            [p for _, p in full_candidates],
            hash_fn=_hash_full,
            hash_fn_arg=None,
            label="full",
        )
        # Grupla
        full_buckets: dict[tuple[int, str], list[str]] = {}
        file_to_size_full = {p: sz for sz, p in full_candidates}
        for path, digest, nread in full_hashes:
            bytes_hashed += nread
            key = (file_to_size_full[path], digest)
            full_buckets.setdefault(key, []).append(path)

        # True duplicate gruplari (>=2 member)
        true_groups_data = []
        for (sz, digest), paths in full_buckets.items():
            if len(paths) >= 2:
                true_groups_data.append((sz, digest, paths))
        true_groups = len(true_groups_data)
        logger.info(
            "content-duplicates scan=%s tier3 full: %d gercek grup, %d dosya hashlendi, %.1f MB okundu",
            scan_id, true_groups, files_hashed_fully, bytes_hashed / 1048576,
        )

        # --- Persist ----------------------------------------------------
        self._persist(true_groups_data, scan_id)

        duration = time.monotonic() - started
        return {
            "total_size_groups": total_size_groups,
            "prefix_collisions": prefix_collisions,
            "true_groups": true_groups,
            "files_hashed_fully": files_hashed_fully,
            "bytes_hashed": bytes_hashed,
            "duration_seconds": round(duration, 3),
        }

    def get_report(
        self,
        scan_id: int,
        page: int = 1,
        page_size: int = 50,
    ) -> dict:
        """Cached sonuclari `duplicate_hash_groups` + members'tan oku."""
        page = max(1, int(page))
        page_size = max(1, min(1000, int(page_size)))
        offset = (page - 1) * page_size

        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt, "
                "COALESCE(SUM(waste_size), 0) AS total_waste, "
                "COALESCE(SUM(file_count), 0) AS total_files "
                "FROM duplicate_hash_groups WHERE scan_id = ?",
                (scan_id,),
            )
            summary = cur.fetchone() or {"cnt": 0, "total_waste": 0, "total_files": 0}
            total_groups = summary["cnt"]

            cur.execute(
                "SELECT id, content_hash, file_size, file_count, waste_size, computed_at "
                "FROM duplicate_hash_groups "
                "WHERE scan_id = ? "
                "ORDER BY waste_size DESC, id ASC "
                "LIMIT ? OFFSET ?",
                (scan_id, page_size, offset),
            )
            group_rows = cur.fetchall()

            groups = []
            for g in group_rows:
                cur.execute(
                    "SELECT file_path, file_id FROM duplicate_hash_members "
                    "WHERE group_id = ? ORDER BY file_path ASC",
                    (g["id"],),
                )
                members = [dict(r) for r in cur.fetchall()]
                groups.append({
                    "id": g["id"],
                    "content_hash": g["content_hash"],
                    "file_size": g["file_size"],
                    "file_count": g["file_count"],
                    "waste_size": g["waste_size"],
                    "computed_at": g["computed_at"],
                    "files": members,
                })

        total_pages = max(1, -(-total_groups // page_size))
        return {
            "scan_id": scan_id,
            "total_groups": total_groups,
            "total_waste_size": summary["total_waste"],
            "total_files": summary["total_files"],
            "groups": groups,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }

    # ---- Internals ---------------------------------------------------

    def _fetch_size_groups(self, scan_id: int, min_bytes: int) -> list[tuple[int, list[str]]]:
        """SQL: ayni boyutta >=2 dosya, >= min_bytes."""
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT file_size, GROUP_CONCAT(file_path, ?) AS paths, COUNT(*) AS cnt "
                "FROM scanned_files WHERE scan_id = ? AND file_size >= ? "
                "GROUP BY file_size HAVING COUNT(*) >= 2",
                ("\x1f", scan_id, min_bytes),
            )
            rows = cur.fetchall()
        out: list[tuple[int, list[str]]] = []
        for r in rows:
            paths_raw = r["paths"] or ""
            paths = [p for p in paths_raw.split("\x1f") if p]
            if len(paths) >= 2:
                out.append((r["file_size"], paths))
        return out

    def _run_hash_pool(self, paths: list[str], hash_fn, hash_fn_arg, label: str) -> list[tuple]:
        """Path listesini ProcessPool uzerinde hashle, sonuclari topla.

        Progress logu: her ~%10'da bir. Pattern `task_scheduler._run_notify_users`'dan.
        """
        total = len(paths)
        if total == 0:
            return []
        results: list[tuple] = []
        progress_step = max(10, total // 10)

        # Tek dosyada kullanicinin overhead'i ProcessPool'dan daha yuksek
        # olabilir; kucuk set'lerde sekillendirmeden seri calistir.
        if total < 4 or self.workers == 1:
            for idx, p in enumerate(paths, start=1):
                r = hash_fn(p, hash_fn_arg) if hash_fn_arg is not None else hash_fn(p)
                if r is not None:
                    results.append(r)
                if idx % progress_step == 0 or idx == total:
                    logger.info(
                        "content-duplicates %s ilerleme: %d/%d (serial)",
                        label, idx, total,
                    )
            return results

        with ProcessPoolExecutor(max_workers=self.workers) as pool:
            if hash_fn_arg is not None:
                futures = {pool.submit(hash_fn, p, hash_fn_arg): p for p in paths}
            else:
                futures = {pool.submit(hash_fn, p): p for p in paths}

            for idx, fut in enumerate(as_completed(futures), start=1):
                try:
                    r = fut.result()
                except Exception as e:
                    # Worker process hatasi — dosyayi atla, gruba devam
                    logger.debug("%s hash worker hatasi %s: %s", label, futures[fut], e)
                    r = None
                if r is not None:
                    results.append(r)
                if idx % progress_step == 0 or idx == total:
                    logger.info(
                        "content-duplicates %s ilerleme: %d/%d",
                        label, idx, total,
                    )
        return results

    def _persist(self, true_groups_data: list[tuple[int, str, list[str]]], scan_id: int) -> None:
        """true duplicate gruplarini tablolarala idempotent olarak yaz.

        Once ayni scan_id icin eski satirlari sil (tam yeniden hesaplama
        senaryosu), sonra yeni gruplari ve dosya uyeliklerini ekle.
        """
        with self.db.get_cursor() as cur:
            # ON DELETE CASCADE var ama indeks guvenligi icin members'i da
            # elle silelim (FK bazi SQLite yapilandirmalarinda gevsek).
            cur.execute(
                "DELETE FROM duplicate_hash_members WHERE group_id IN ("
                "SELECT id FROM duplicate_hash_groups WHERE scan_id = ?)",
                (scan_id,),
            )
            cur.execute(
                "DELETE FROM duplicate_hash_groups WHERE scan_id = ?",
                (scan_id,),
            )

            # file_path -> file_id lookup (tek sorguyla, buyuk scan'lerde
            # N+1 sorgusundan kacinmak icin). IN() parametre limiti 999
            # oldugu icin buyuk kumeler icin yine batch'leriz.
            all_paths: list[str] = []
            for _sz, _digest, paths in true_groups_data:
                all_paths.extend(paths)
            path_to_id: dict[str, int] = {}
            BATCH = 500
            for i in range(0, len(all_paths), BATCH):
                batch = all_paths[i : i + BATCH]
                if not batch:
                    continue
                placeholders = ",".join("?" * len(batch))
                cur.execute(
                    f"SELECT id, file_path FROM scanned_files "
                    f"WHERE scan_id = ? AND file_path IN ({placeholders})",
                    (scan_id, *batch),
                )
                for row in cur.fetchall():
                    path_to_id[row["file_path"]] = row["id"]

            for sz, digest, paths in true_groups_data:
                count = len(paths)
                waste = sz * (count - 1)
                cur.execute(
                    "INSERT INTO duplicate_hash_groups "
                    "(scan_id, content_hash, file_size, file_count, waste_size) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (scan_id, digest, sz, count, waste),
                )
                group_id = cur.lastrowid
                cur.executemany(
                    "INSERT INTO duplicate_hash_members (group_id, file_path, file_id) "
                    "VALUES (?, ?, ?)",
                    [(group_id, p, path_to_id.get(p)) for p in paths],
                )
