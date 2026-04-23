"""GDPR PII detection engine (issue #58).

Walks ``scanned_files`` rows for a given scan, opens each file in text
mode (skipping binary extensions), runs a configurable set of regex
patterns and records per-file hit counts plus a *redacted* sample
snippet to ``pii_findings``. The redaction policy is intentionally
strict: we never persist a raw email address, IBAN or TC kimlik no.,
only a masked form (``j***n@x.com``) so the table itself does not
become a new PII liability.

The two end-user verbs are:

* :meth:`PiiEngine.scan_source` — bulk scan during/after a regular
  filesystem scan; idempotent (already-scanned files are skipped).
* :meth:`PiiEngine.find_for_subject` — Article 17/30 export driver:
  return every file whose ``pii_findings`` rows mention a search term
  (e.g. an email address or a name fragment). The companion
  :meth:`export_subject_csv` writes the same rows out for an auditor.

Defaults match the Turkish operator brief (TR IBAN, TR mobile, 11-digit
TCKN) plus international email + 16-digit credit card. Users may extend
or override via ``config.compliance.pii.patterns``.

The module deliberately uses only ``re`` and stdlib I/O — no new
external dependencies. ``UnicodeDecodeError`` is treated as "not a
text file" and silently skipped.
"""

from __future__ import annotations

import csv
import logging
import os
import re
import time
from typing import Optional

logger = logging.getLogger("file_activity.compliance.pii_engine")


class PiiEngine:
    """Per-file PII detector with redacted persistence."""

    DEFAULT_PATTERNS = {
        "email":       r"[\w\.-]+@[\w\.-]+\.\w+",
        "iban_tr":     r"\bTR\d{2}\s?\d{4}\s?\d{4}\s?\d{4}\s?\d{4}\s?\d{2}\b",
        "phone_tr":    r"\b(?:\+90|0)?\s?5\d{2}\s?\d{3}\s?\d{2}\s?\d{2}\b",
        "tckn":        r"\b\d{11}\b",
        "credit_card": r"\b(?:\d{4}[\s-]?){3}\d{4}\b",
    }

    DEFAULT_TEXT_EXTENSIONS = {
        "txt", "csv", "html", "htm", "md", "log", "json", "xml",
        "yaml", "yml", "ini", "cfg", "conf", "py", "js", "ts",
        "java", "c", "cpp", "h", "go", "rs", "sql", "sh", "ps1",
        "bat", "rtf",
    }

    # Soft cap on the snippet field. The full sample never appears
    # verbatim in the DB — we mask the middle of the match — but we
    # still cap to keep row sizes bounded for log files with very
    # long lines.
    DEFAULT_SNIPPET_MAX_LENGTH = 60

    def __init__(self, db, config: dict):
        self.db = db
        cfg = ((config or {}).get("compliance", {}) or {}).get("pii", {}) or {}
        self.enabled = bool(cfg.get("enabled", False))
        self.max_file_bytes = int(cfg.get("max_file_bytes", 1_048_576))
        self.snippet_max_length = int(
            cfg.get("snippet_max_length", self.DEFAULT_SNIPPET_MAX_LENGTH)
        )

        # Operators may extend / override built-in patterns. Keys
        # collide on name; user-supplied wins.
        merged = dict(self.DEFAULT_PATTERNS)
        user_patterns = cfg.get("patterns") or {}
        if isinstance(user_patterns, dict):
            for name, regex in user_patterns.items():
                if not name or not regex:
                    continue
                merged[str(name)] = str(regex)

        # Pre-compile (case-insensitive). Bad patterns are dropped with
        # a warning rather than aborting the whole engine.
        self.patterns: dict[str, "re.Pattern[str]"] = {}
        for name, regex in merged.items():
            try:
                self.patterns[name] = re.compile(regex, re.IGNORECASE)
            except re.error as e:
                logger.warning("PII pattern %s ignored (bad regex): %s", name, e)

        # Text extensions: user-supplied list wholly replaces defaults
        # if provided (matches the YAML comment "text_extensions"). An
        # empty list means "scan everything except known binaries" — we
        # still hold a denylist of binary extensions below.
        ext_cfg = cfg.get("text_extensions")
        if ext_cfg is None:
            self.text_extensions = set(self.DEFAULT_TEXT_EXTENSIONS)
        else:
            self.text_extensions = {
                str(e).lower().lstrip(".") for e in ext_cfg if str(e).strip()
            }

    # ──────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────

    # Anything obviously binary that we never want to even try to open
    # in text mode. The text-extension allowlist already filters most
    # noise; this is a belt-and-braces guard for cases where the
    # allowlist has been disabled (empty set) by an operator.
    _BINARY_EXTENSIONS = {
        "exe", "dll", "so", "dylib", "bin", "iso", "img",
        "zip", "rar", "7z", "tar", "gz", "bz2", "xz", "lz4",
        "jpg", "jpeg", "png", "gif", "bmp", "tiff", "webp", "ico",
        "mp3", "mp4", "mkv", "mov", "avi", "wav", "flac", "ogg",
        "pdf", "docx", "xlsx", "pptx", "odt",
        "pyc", "pyo", "class", "o", "obj",
        "db", "sqlite", "sqlite3", "mdb",
    }

    @staticmethod
    def _ext_of(path: str) -> str:
        _, dot, ext = os.path.basename(path).rpartition(".")
        return ext.lower() if dot else ""

    def _is_text_extension(self, path: str) -> bool:
        ext = self._ext_of(path)
        if ext in self._BINARY_EXTENSIONS:
            return False
        # If the operator explicitly cleared the allowlist (empty set),
        # treat anything not in the binary denylist as scannable.
        if not self.text_extensions:
            return True
        return ext in self.text_extensions

    @staticmethod
    def _redact(value: str) -> str:
        """Mask the middle characters of a single hit so storing it in
        the DB does not re-leak the underlying PII.

        Examples::

            john@x.com  ->  j***n@x.com
            TR3300061... -> T***2
            12345678901 -> 1***1

        Very short matches (<=2 chars) are masked entirely.
        """
        if not value:
            return ""
        s = str(value)
        n = len(s)
        if n <= 2:
            return "*" * n
        # Preserve the local-part / pre-@ formatting for emails so the
        # snippet remains recognisable to the operator at a glance.
        if "@" in s:
            local, _, domain = s.partition("@")
            if len(local) <= 2:
                masked_local = "*" * len(local)
            else:
                masked_local = f"{local[0]}***{local[-1]}"
            return f"{masked_local}@{domain}"
        return f"{s[0]}***{s[-1]}"

    def _build_snippet(self, hits: list[str]) -> str:
        """Join up to a handful of redacted hits into one snippet
        bounded by ``snippet_max_length``.
        """
        if not hits:
            return ""
        parts: list[str] = []
        for h in hits:
            parts.append(self._redact(h))
            if sum(len(p) + 2 for p in parts) >= self.snippet_max_length:
                break
        joined = ", ".join(parts)
        if len(joined) > self.snippet_max_length:
            joined = joined[: self.snippet_max_length - 1] + "…"
        return joined

    # ──────────────────────────────────────────────
    # Single-file scan
    # ──────────────────────────────────────────────

    def scan_file(self, path: str, max_bytes: int = 1_000_000) -> dict:
        """Scan one file. Return ``{file_path, scanned_bytes, hits}``.

        ``hits`` is ``{pattern_name: [redacted_snippet, ...]}``. Empty
        ``hits`` means either nothing matched or the file was skipped
        (binary extension / unreadable).
        """
        cap = int(max_bytes or self.max_file_bytes)
        result = {"file_path": path, "scanned_bytes": 0, "hits": {}}

        if not self._is_text_extension(path):
            return result

        try:
            with open(path, "rb") as f:
                raw = f.read(cap)
        except (OSError, IOError) as e:
            logger.debug("PII scan_file: cannot open %s: %s", path, e)
            return result

        result["scanned_bytes"] = len(raw)

        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            try:
                text = raw.decode("latin-1")
            except Exception:
                logger.debug("PII scan_file: undecodable %s", path)
                return result

        hits: dict[str, list[str]] = {}
        for name, regex in self.patterns.items():
            matches = regex.findall(text)
            if not matches:
                continue
            # ``findall`` may return tuples for grouped patterns.
            flat: list[str] = []
            for m in matches:
                if isinstance(m, tuple):
                    flat.append("".join(m))
                else:
                    flat.append(m)
            redacted = [self._redact(v) for v in flat[:10]]
            hits[name] = redacted

        result["hits"] = hits
        return result

    # ──────────────────────────────────────────────
    # Bulk scan persisted to pii_findings
    # ──────────────────────────────────────────────

    def scan_source(self, source_id: int, scan_id: Optional[int] = None,
                    max_files: Optional[int] = None,
                    overwrite_existing: bool = False) -> dict:
        """Walk ``scanned_files`` for ``source_id``, scan each via
        :meth:`scan_file`, persist hits to ``pii_findings``.

        Idempotent: files already represented in ``pii_findings`` for
        this ``scan_id`` are skipped unless ``overwrite_existing`` is
        true (in which case the existing rows are deleted first).

        Returns ``{scan_id, scanned, skipped, hits_total,
        elapsed_seconds}``.
        """
        started = time.time()

        with self.db.get_cursor() as cur:
            # Pick the most recent scan if not specified — matches the
            # rest of the codebase's "latest scan" convention.
            if scan_id is None:
                cur.execute(
                    """SELECT id FROM scan_runs WHERE source_id=?
                       ORDER BY CASE WHEN status='completed' THEN 0 ELSE 1 END,
                                started_at DESC LIMIT 1""",
                    (int(source_id),),
                )
                row = cur.fetchone()
                if not row:
                    return {
                        "scan_id": None,
                        "scanned": 0,
                        "skipped": 0,
                        "hits_total": 0,
                        "elapsed_seconds": 0.0,
                    }
                scan_id = int(row["id"])

            limit_sql = ""
            params: list = [int(source_id), int(scan_id)]
            if max_files is not None and int(max_files) > 0:
                limit_sql = " LIMIT ?"
                params.append(int(max_files))

            cur.execute(
                f"SELECT file_path FROM scanned_files "
                f"WHERE source_id=? AND scan_id=? "
                f"ORDER BY id ASC{limit_sql}",
                params,
            )
            file_paths = [r["file_path"] for r in cur.fetchall()]

        scanned = 0
        skipped = 0
        hits_total = 0

        for idx, fp in enumerate(file_paths, 1):
            # Idempotency: skip if we already have findings for this
            # scan_id + file_path. Operator can force a rescan with
            # overwrite_existing=True.
            with self.db.get_cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pii_findings WHERE scan_id=? "
                    "AND file_path=? LIMIT 1",
                    (scan_id, fp),
                )
                already = cur.fetchone() is not None

            if already and not overwrite_existing:
                skipped += 1
                continue
            if already and overwrite_existing:
                with self.db.get_cursor() as cur:
                    cur.execute(
                        "DELETE FROM pii_findings WHERE scan_id=? "
                        "AND file_path=?",
                        (scan_id, fp),
                    )

            try:
                file_result = self.scan_file(fp, max_bytes=self.max_file_bytes)
            except Exception as e:  # pragma: no cover - defensive
                logger.debug("PII scan_file unexpected error %s: %s", fp, e)
                file_result = {"hits": {}}

            scanned += 1
            file_hits = file_result.get("hits", {}) or {}
            if not file_hits:
                continue

            with self.db.get_cursor() as cur:
                for pattern_name, snippets in file_hits.items():
                    hit_count = len(snippets)
                    sample = self._build_snippet(snippets)
                    cur.execute(
                        """INSERT INTO pii_findings
                           (scan_id, file_path, pattern_name,
                            hit_count, sample_snippet)
                           VALUES (?, ?, ?, ?, ?)""",
                        (scan_id, fp, pattern_name, hit_count, sample),
                    )
                    hits_total += hit_count

            if idx % 1000 == 0:
                logger.info(
                    "PII scan_source progress: source=%s scan=%s "
                    "scanned=%d skipped=%d hits=%d",
                    source_id, scan_id, scanned, skipped, hits_total,
                )

        elapsed = time.time() - started
        logger.info(
            "PII scan_source done: source=%s scan=%s scanned=%d "
            "skipped=%d hits=%d elapsed=%.2fs",
            source_id, scan_id, scanned, skipped, hits_total, elapsed,
        )
        return {
            "scan_id": scan_id,
            "scanned": scanned,
            "skipped": skipped,
            "hits_total": hits_total,
            "elapsed_seconds": round(elapsed, 3),
        }

    # ──────────────────────────────────────────────
    # Article 17 / 30 — per-subject export
    # ──────────────────────────────────────────────

    def find_for_subject(self, search_term: str,
                         limit: int = 1000) -> list[dict]:
        """Return every file mentioning ``search_term`` in either a
        ``pii_findings.sample_snippet`` or a ``pii_findings.file_path``.

        Joined back to ``scanned_files`` so the operator gets
        last_modify_time + owner for each row. Result shape::

            [{file_path, match_count, last_modify_time, owner, hits}]

        ``hits`` is a list of ``{pattern_name, hit_count,
        sample_snippet}`` per pattern.
        """
        term = (search_term or "").strip()
        if not term:
            return []
        like = f"%{term}%"
        cap = max(1, min(int(limit or 1000), 100_000))

        with self.db.get_cursor() as cur:
            cur.execute(
                """
                SELECT
                    p.file_path,
                    p.pattern_name,
                    p.hit_count,
                    p.sample_snippet,
                    p.detected_at
                FROM pii_findings p
                WHERE p.sample_snippet LIKE ?
                   OR p.file_path LIKE ?
                ORDER BY p.file_path ASC, p.pattern_name ASC
                LIMIT ?
                """,
                (like, like, cap),
            )
            rows = [dict(r) for r in cur.fetchall()]

        # Aggregate per file_path.
        by_path: dict[str, dict] = {}
        for r in rows:
            fp = r["file_path"]
            entry = by_path.setdefault(fp, {
                "file_path": fp,
                "match_count": 0,
                "last_modify_time": None,
                "owner": None,
                "hits": [],
            })
            entry["match_count"] += int(r["hit_count"] or 0)
            entry["hits"].append({
                "pattern_name": r["pattern_name"],
                "hit_count": int(r["hit_count"] or 0),
                "sample_snippet": r["sample_snippet"],
            })

        if not by_path:
            return []

        # Enrich with last_modify_time + owner from the most recent
        # scanned_files row per path. Done in one IN-clause query.
        paths = list(by_path.keys())
        with self.db.get_cursor() as cur:
            placeholders = ",".join(["?"] * len(paths))
            cur.execute(
                f"""SELECT file_path,
                           MAX(last_modify_time) AS last_modify_time,
                           MAX(owner)            AS owner
                    FROM scanned_files
                    WHERE file_path IN ({placeholders})
                    GROUP BY file_path""",
                paths,
            )
            for r in cur.fetchall():
                rec = by_path.get(r["file_path"])
                if rec is None:
                    continue
                rec["last_modify_time"] = r["last_modify_time"]
                rec["owner"] = r["owner"]

        return sorted(
            by_path.values(),
            key=lambda x: (-x["match_count"], x["file_path"]),
        )

    def export_subject_csv(self, search_term: str, output_path: str) -> int:
        """Write the result of :meth:`find_for_subject` to a CSV.
        Returns the number of file rows written (one row per file).
        """
        rows = self.find_for_subject(search_term, limit=100_000)
        out_dir = os.path.dirname(output_path)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        with open(output_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow([
                "file_path", "match_count", "last_modify_time",
                "owner", "patterns", "sample_snippets",
            ])
            for r in rows:
                patterns = ";".join(h["pattern_name"] for h in r["hits"])
                snippets = ";".join(
                    h.get("sample_snippet") or "" for h in r["hits"]
                )
                writer.writerow([
                    r["file_path"], r["match_count"],
                    r["last_modify_time"] or "",
                    r["owner"] or "",
                    patterns, snippets,
                ])
        return len(rows)
