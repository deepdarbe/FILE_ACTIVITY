"""MinHash + LSH near-duplicate detection for text/document files (roadmap
ADOPT: near-dup). Complements the exact SHA-256 content-dup pipeline
(``content_duplicates.py``) by catching files that are *similar* but not
byte-identical — e.g. two revisions of a document, a copy with minor edits.

Uses the optional ``datasketch`` package (PyPI, MIT). When it is not
installed the engine degrades gracefully: ``available`` is False,
``compute()`` returns a zeroed summary, and a single WARNING is logged —
exactly the ``image_hash.py`` pattern.

Pipeline (mirrors ``ContentDuplicateEngine``):

1. Candidate fetch (SQL, no I/O): text-like extensions, ``min_bytes`` ≤
   size ≤ ``max_bytes``, capped at ``max_files``.
2. Signature: read up to ``max_bytes`` of text, build word k-shingles,
   fold into a ``MinHash(num_perm)``.
3. Candidate pairs: ``MinHashLSH(threshold)`` returns near-neighbours in
   ~O(n); each pair's Jaccard is verified before it counts.
4. Cluster (Union-Find) → groups of ≥2; persist to
   ``text_near_dup_groups`` / ``text_near_dup_members``.

Cost note: reading content is expensive, so this is **opt-in**
(``text_near_duplicates.enabled: false`` by default) and triggered
on-demand via the dashboard, never automatically per scan. Plain-text
only for now — office/pdf extraction is out of scope (documented).
"""

from __future__ import annotations

import logging
import time
from typing import Optional

logger = logging.getLogger("file_activity.analyzer.text_minhash")

# Text-like extensions eligible for shingling. Binary/office formats need
# an extractor (out of scope) so they are excluded — including them would
# shingle compressed bytes and produce garbage similarity.
TEXT_EXTENSIONS = frozenset({
    "txt", "md", "markdown", "csv", "tsv", "log", "json", "xml", "yaml",
    "yml", "html", "htm", "rtf", "tex", "ini", "cfg", "conf", "sql", "py",
    "js", "ts", "java", "c", "cpp", "h", "cs", "go", "rb", "php", "sh",
})

_DEFAULT_MIN_BYTES = 1024            # skip tiny files (no meaningful shingles)
_DEFAULT_MAX_BYTES = 10 * 1_048_576  # read at most 10 MB of content
_DEFAULT_NUM_PERM = 128
_DEFAULT_THRESHOLD = 0.8             # Jaccard similarity for "near-duplicate"
_DEFAULT_SHINGLE = 5                 # word k-shingle size
_DEFAULT_MAX_FILES = 50_000          # bound the cost of a single run


def _read_text(file_path: str, max_bytes: int) -> Optional[str]:
    """Read up to ``max_bytes`` of a file and decode as UTF-8 (lenient).

    Returns the text, or None on any OS error. Decoding never raises —
    undecodable bytes are replaced, which is fine for shingling.
    """
    try:
        with open(file_path, "rb") as handle:
            raw = handle.read(max_bytes)
    except (OSError, ValueError) as exc:
        logger.debug("text near-dup: read skipped %s: %s", file_path, exc)
        return None
    return raw.decode("utf-8", errors="replace")


def _word_shingles(text: str, k: int) -> set:
    """Return the set of word k-shingles for ``text``.

    A shingle is ``k`` consecutive whitespace-split tokens joined by a
    space. Texts shorter than ``k`` tokens yield a single shingle (the
    whole text) so tiny-but-valid files still get a signature.
    """
    tokens = text.split()
    if not tokens:
        return set()
    if len(tokens) < k:
        return {" ".join(tokens)}
    return {" ".join(tokens[i:i + k]) for i in range(len(tokens) - k + 1)}


class TextNearDuplicateEngine:
    """MinHash+LSH near-duplicate detector for text files.

    Usage::

        engine = TextNearDuplicateEngine(db, config)
        if engine.available:
            stats = engine.compute(scan_id)
            page = engine.get_report(scan_id, page=1, page_size=50)
    """

    def __init__(self, db, config: dict):
        self.db = db
        cfg = (config or {}).get("text_near_duplicates", {}) or {}
        self.enabled = bool(cfg.get("enabled", False))
        self.min_bytes = int(cfg.get("min_bytes", _DEFAULT_MIN_BYTES))
        self.max_bytes = int(cfg.get("max_bytes", _DEFAULT_MAX_BYTES))
        self.num_perm = max(16, int(cfg.get("num_perm", _DEFAULT_NUM_PERM)))
        self.threshold = float(cfg.get("threshold", _DEFAULT_THRESHOLD))
        self.shingle_size = max(1, int(cfg.get("shingle_size", _DEFAULT_SHINGLE)))
        self.default_max_files = int(cfg.get("max_files", _DEFAULT_MAX_FILES))
        exts = cfg.get("extensions")
        self.extensions = (
            frozenset(str(e).lower().lstrip(".") for e in exts)
            if exts else TEXT_EXTENSIONS
        )
        self._available: Optional[bool] = None

    # ---- availability -------------------------------------------------
    @property
    def available(self) -> bool:
        if self._available is None:
            self._probe()
        return bool(self._available)

    def _probe(self) -> None:
        try:
            from datasketch import MinHash, MinHashLSH  # noqa: F401
            self._available = True
        except ImportError as exc:
            self._available = False
            logger.warning(
                "datasketch yuklu degil — metin near-dup devre disi. "
                "Yuklemek icin: pip install datasketch. Hata: %s", exc,
            )

    # ---- compute ------------------------------------------------------
    def compute(self, scan_id: int, *, threshold: Optional[float] = None,
                max_files: Optional[int] = None) -> dict:
        """Run the MinHash+LSH pipeline for one scan and persist groups.

        Returns a summary dict (always — zeroed when unavailable/disabled).
        """
        started = time.monotonic()
        summary = {
            "files_considered": 0,
            "files_hashed": 0,
            "groups": 0,
            "duplicate_files": 0,
            "waste_size": 0,
            "duration_seconds": 0.0,
            "available": self.available,
        }
        if not self.available:
            return summary

        thr = float(threshold) if threshold is not None else self.threshold
        cap = int(max_files) if max_files is not None else self.default_max_files

        from datasketch import MinHash, MinHashLSH

        candidates = self._fetch_candidates(scan_id, cap)
        summary["files_considered"] = len(candidates)
        if len(candidates) < 2:
            self._persist([], scan_id)
            summary["duration_seconds"] = round(time.monotonic() - started, 3)
            return summary

        # --- signatures ---
        minhashes: list = []
        kept: list[dict] = []  # parallel to minhashes
        progress_step = max(50, len(candidates) // 10)
        for idx, row in enumerate(candidates, start=1):
            text = _read_text(row["file_path"], self.max_bytes)
            if text is None:
                continue
            shingles = _word_shingles(text, self.shingle_size)
            if not shingles:
                continue
            m = MinHash(num_perm=self.num_perm)
            for sh in shingles:
                m.update(sh.encode("utf-8"))
            minhashes.append(m)
            kept.append(row)
            if idx % progress_step == 0 or idx == len(candidates):
                logger.info("text near-dup scan=%s signatures %d/%d",
                            scan_id, idx, len(candidates))
        summary["files_hashed"] = len(kept)
        if len(kept) < 2:
            self._persist([], scan_id)
            summary["duration_seconds"] = round(time.monotonic() - started, 3)
            return summary

        # --- LSH candidate pairs + verified union ---
        lsh = MinHashLSH(threshold=thr, num_perm=self.num_perm)
        for i, m in enumerate(minhashes):
            lsh.insert(str(i), m)

        parent = list(range(len(kept)))

        def _find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def _union(a: int, b: int) -> None:
            ra, rb = _find(a), _find(b)
            if ra != rb:
                parent[rb] = ra

        pair_sims: dict[tuple[int, int], float] = {}
        for i, m in enumerate(minhashes):
            for cand in lsh.query(m):
                j = int(cand)
                if j <= i:
                    continue
                sim = m.jaccard(minhashes[j])
                if sim >= thr:
                    _union(i, j)
                    pair_sims[(i, j)] = sim

        # --- collect groups ---
        buckets: dict[int, list[int]] = {}
        for i in range(len(kept)):
            buckets.setdefault(_find(i), []).append(i)

        groups_data = []
        for members in buckets.values():
            if len(members) < 2:
                continue
            rows = [kept[i] for i in members]
            sizes = [int(r["file_size"] or 0) for r in rows]
            total_size = sum(sizes)
            waste = total_size - max(sizes) if sizes else 0
            sims = [pair_sims[p] for p in pair_sims
                    if p[0] in members and p[1] in members]
            avg_sim = round(sum(sims) / len(sims), 4) if sims else None
            groups_data.append({
                "rows": rows,
                "file_count": len(rows),
                "total_size": total_size,
                "waste_size": waste,
                "avg_similarity": avg_sim,
            })

        self._persist(groups_data, scan_id)

        summary["groups"] = len(groups_data)
        summary["duplicate_files"] = sum(g["file_count"] for g in groups_data)
        summary["waste_size"] = sum(g["waste_size"] for g in groups_data)
        summary["duration_seconds"] = round(time.monotonic() - started, 3)
        logger.info(
            "text near-dup scan=%s done: %d grup, %d dosya, %.1f MB israf, %.1fs",
            scan_id, summary["groups"], summary["duplicate_files"],
            summary["waste_size"] / 1_048_576, summary["duration_seconds"],
        )
        return summary

    # ---- report -------------------------------------------------------
    def get_report(self, scan_id: int, page: int = 1,
                   page_size: int = 50) -> dict:
        """Read cached near-dup groups (read-only pool, Rule 6)."""
        page = max(1, int(page))
        page_size = max(1, min(1000, int(page_size)))
        offset = (page - 1) * page_size

        with self.db.get_read_cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt, COALESCE(SUM(waste_size),0) AS waste, "
                "COALESCE(SUM(file_count),0) AS files "
                "FROM text_near_dup_groups WHERE scan_id = ?",
                (scan_id,),
            )
            s = cur.fetchone() or {"cnt": 0, "waste": 0, "files": 0}
            total_groups = s["cnt"]

            cur.execute(
                "SELECT id, file_count, total_size, waste_size, avg_similarity, "
                "computed_at FROM text_near_dup_groups WHERE scan_id = ? "
                "ORDER BY waste_size DESC, id ASC LIMIT ? OFFSET ?",
                (scan_id, page_size, offset),
            )
            group_rows = cur.fetchall()

            groups = []
            for g in group_rows:
                cur.execute(
                    "SELECT file_path, file_id, file_size FROM "
                    "text_near_dup_members WHERE group_id = ? "
                    "ORDER BY file_size DESC, file_path ASC",
                    (g["id"],),
                )
                groups.append({
                    "id": g["id"],
                    "file_count": g["file_count"],
                    "total_size": g["total_size"],
                    "waste_size": g["waste_size"],
                    "avg_similarity": g["avg_similarity"],
                    "computed_at": g["computed_at"],
                    "files": [dict(r) for r in cur.fetchall()],
                })

        total_pages = max(1, -(-total_groups // page_size))
        return {
            "scan_id": scan_id,
            "total_groups": total_groups,
            "total_waste_size": s["waste"],
            "total_files": s["files"],
            "groups": groups,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }

    # ---- internals ----------------------------------------------------
    def _fetch_candidates(self, scan_id: int, cap: int) -> list[dict]:
        placeholders = ",".join("?" * len(self.extensions))
        with self.db.get_read_cursor() as cur:
            cur.execute(
                f"SELECT id, file_path, file_size FROM scanned_files "
                f"WHERE scan_id = ? AND file_size BETWEEN ? AND ? "
                f"AND LOWER(extension) IN ({placeholders}) "
                f"ORDER BY file_size DESC LIMIT ?",
                (scan_id, self.min_bytes, self.max_bytes,
                 *sorted(self.extensions), cap),
            )
            return [dict(r) for r in cur.fetchall()]

    def _persist(self, groups_data: list[dict], scan_id: int) -> None:
        """Idempotent write: clear this scan's groups, then insert fresh."""
        with self.db.get_cursor() as cur:
            cur.execute(
                "DELETE FROM text_near_dup_members WHERE group_id IN ("
                "SELECT id FROM text_near_dup_groups WHERE scan_id = ?)",
                (scan_id,),
            )
            cur.execute(
                "DELETE FROM text_near_dup_groups WHERE scan_id = ?",
                (scan_id,),
            )
            for g in groups_data:
                cur.execute(
                    "INSERT INTO text_near_dup_groups "
                    "(scan_id, file_count, total_size, waste_size, avg_similarity) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (scan_id, g["file_count"], g["total_size"],
                     g["waste_size"], g["avg_similarity"]),
                )
                group_id = cur.lastrowid
                cur.executemany(
                    "INSERT INTO text_near_dup_members "
                    "(group_id, file_id, file_path, file_size) VALUES (?, ?, ?, ?)",
                    [(group_id, r["id"], r["file_path"], r["file_size"])
                     for r in g["rows"]],
                )
