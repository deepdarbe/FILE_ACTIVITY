"""Benchmark the PII engine — validates the issue #66 5x claim.

Generates a deterministic ~``corpus_size`` MB text corpus in a tmpdir
once per process (random words seeded with ``random.seed(42)``) with
known fixture counts injected at known positions:

* 10,000 emails
* 5,000 TR IBANs
* 2,000 TCKNs (Turkish national-id numbers)

The PII engine is then run end-to-end via ``PiiEngine.scan_source``
against an in-memory SQLite stub that satisfies the engine's
``get_cursor`` contract — schema is tiny, just enough to let the
engine select files for a fake ``source_id`` / ``scan_id`` pair and
write findings back.

We time the run with both backends (stdlib ``re`` + Hyperscan when the
optional ``hyperscan`` package is importable). The reported throughput
is MB/s (corpus bytes scanned divided by wall time) and matches/sec.

Usage::

    python -m tests.bench.bench_pii \
        [--corpus-size 100] [--repeats 5] \
        [--history bench_history.jsonl]

The default 100 MB corpus runs in well under a minute on every backend
on commodity hardware. Idempotent — same MB count gives the same
content, so successive runs produce numbers comparable to the JSONL
history.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import random
import string
import sys
import sqlite3
import tempfile
import time
from pathlib import Path
from typing import Optional

# Add project root to path when invoked directly so ``src.compliance...``
# imports resolve without an editable install.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.compliance._pii_backends import (  # noqa: E402
    hyperscan_available,
    make_backend,
)
from src.compliance.pii_engine import PiiEngine  # noqa: E402
from tests.bench._runner import (  # noqa: E402
    Bench,
    append_history,
    print_table,
)


# ──────────────────────────────────────────────────────────────────────
# Deterministic corpus generator
# ──────────────────────────────────────────────────────────────────────


# Realistic-ish TR IBAN (TR + 2 check digits + 22 digits). Values are
# fake (no real bank prefix) but match the regex shape used by
# ``DEFAULT_PATTERNS["iban_tr"]``. We mint them deterministically below.
def _make_email(rng: random.Random) -> str:
    name = "".join(rng.choices(string.ascii_lowercase, k=rng.randint(5, 10)))
    domain = "".join(rng.choices(string.ascii_lowercase, k=rng.randint(4, 8)))
    return f"{name}@{domain}.com"


def _make_iban(rng: random.Random) -> str:
    body = "".join(rng.choices(string.digits, k=24))
    return f"TR{body}"


def _make_tckn(rng: random.Random) -> str:
    # 11 digits, no leading zero so the \b...\b match latches cleanly.
    first = rng.choice("123456789")
    rest = "".join(rng.choices(string.digits, k=10))
    return first + rest


def _make_word(rng: random.Random) -> str:
    return "".join(rng.choices(string.ascii_lowercase, k=rng.randint(3, 9)))


# Per-corpus fixture counts — kept proportional to size so a 50 MB run
# scales down sensibly while still producing enough matches for the
# matches/sec figure to be meaningful.
def _fixture_counts(corpus_mb: int) -> dict[str, int]:
    scale = max(1.0, corpus_mb / 100.0)
    return {
        "emails": int(10_000 * scale),
        "ibans": int(5_000 * scale),
        "tckns": int(2_000 * scale),
    }


def generate_corpus(target_dir: Path, corpus_mb: int) -> tuple[Path, dict]:
    """Write ``target_dir / corpus.txt`` of approximately ``corpus_mb`` MB.

    Returns the file path and a dict describing the fixture counts and
    actual byte size. Idempotent: if the file already exists at the
    requested size (within 1%), it is reused as-is — successive runs in
    the same tmpdir do not pay the regeneration cost.
    """
    counts = _fixture_counts(corpus_mb)
    corpus_path = target_dir / f"corpus_{corpus_mb}mb.txt"
    target_bytes = corpus_mb * 1024 * 1024

    if corpus_path.exists():
        actual = corpus_path.stat().st_size
        if abs(actual - target_bytes) <= max(1, target_bytes // 100):
            return corpus_path, {
                "size_bytes": actual,
                "fixtures": counts,
                "regenerated": False,
            }

    rng = random.Random(42)

    # Pre-mint fixtures up front so we know the exact strings to inject.
    # Stored as a flat shuffled list of (kind, value).
    fixtures: list[tuple[str, str]] = []
    for _ in range(counts["emails"]):
        fixtures.append(("email", _make_email(rng)))
    for _ in range(counts["ibans"]):
        fixtures.append(("iban", _make_iban(rng)))
    for _ in range(counts["tckns"]):
        fixtures.append(("tckn", _make_tckn(rng)))
    rng.shuffle(fixtures)

    # Estimate how many lines we'll produce so fixtures sprinkle evenly
    # across the whole corpus rather than clumping at the start. Average
    # line is ~11 words * 6 chars + spaces ~= 80 bytes.
    avg_line_bytes = 80
    estimated_lines = max(len(fixtures) + 1, target_bytes // avg_line_bytes)
    inject_every = max(1, estimated_lines // max(1, len(fixtures)))

    # Write line-by-line to avoid a multi-hundred-MB Python string in
    # RAM. Each line is ~10 random words; every ``inject_every`` lines
    # carries one fixture so they're sprinkled evenly through the
    # corpus, matching the realistic pattern of PII appearing inline.
    fix_idx = 0
    line_idx = 0
    written = 0
    with open(corpus_path, "w", encoding="utf-8", newline="\n") as fh:
        while written < target_bytes:
            words = [_make_word(rng) for _ in range(rng.randint(8, 14))]
            if fix_idx < len(fixtures) and (line_idx % inject_every) == 0:
                _, value = fixtures[fix_idx]
                # Splice into the middle of the line so word boundaries
                # are clean on both sides.
                mid = len(words) // 2
                words.insert(mid, value)
                fix_idx += 1

            line = " ".join(words) + "\n"
            fh.write(line)
            written += len(line.encode("utf-8"))
            line_idx += 1

        # Append any remaining unused fixtures so the total count is
        # exact — guards against the case where the actual line average
        # ran shorter than the 80-byte estimate.
        while fix_idx < len(fixtures):
            _, value = fixtures[fix_idx]
            fh.write(value + "\n")
            fix_idx += 1

    actual = corpus_path.stat().st_size
    return corpus_path, {
        "size_bytes": actual,
        "fixtures": counts,
        "regenerated": True,
    }


# ──────────────────────────────────────────────────────────────────────
# Minimal in-memory DB stub satisfying PiiEngine's contract
# ──────────────────────────────────────────────────────────────────────


class _MiniDb:
    """Just enough of the project ``Database`` to drive ``scan_source``.

    The PII engine only touches three tables: ``scan_runs``,
    ``scanned_files`` and ``pii_findings``. We create them in a private
    sqlite ``:memory:`` connection and expose the project's
    ``get_cursor`` context manager. Rows are dict-like (sqlite3.Row) so
    ``row["id"]`` works the same way the real DB returns them.
    """

    def __init__(self) -> None:
        self._conn = sqlite3.connect(":memory:")
        self._conn.row_factory = sqlite3.Row
        cur = self._conn.cursor()
        cur.executescript(
            """
            CREATE TABLE scan_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL,
                started_at TEXT,
                status TEXT
            );
            CREATE TABLE scanned_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL,
                scan_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                last_modify_time TEXT,
                owner TEXT
            );
            CREATE TABLE pii_findings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                pattern_name TEXT NOT NULL,
                hit_count INTEGER NOT NULL,
                sample_snippet TEXT,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        self._conn.commit()

    @contextlib.contextmanager
    def get_cursor(self):
        cur = self._conn.cursor()
        try:
            yield cur
            self._conn.commit()
        finally:
            cur.close()

    def seed(self, source_id: int, file_paths: list[str]) -> int:
        """Insert a fresh scan_run + the given files. Returns scan_id."""
        with self.get_cursor() as cur:
            cur.execute(
                "INSERT INTO scan_runs (source_id, started_at, status) "
                "VALUES (?, ?, ?)",
                (source_id, time.strftime("%Y-%m-%d %H:%M:%S"), "completed"),
            )
            scan_id = int(cur.lastrowid)
            cur.executemany(
                "INSERT INTO scanned_files (source_id, scan_id, file_path) "
                "VALUES (?, ?, ?)",
                [(source_id, scan_id, p) for p in file_paths],
            )
        return scan_id

    def reset_findings(self) -> None:
        with self.get_cursor() as cur:
            cur.execute("DELETE FROM pii_findings")


# ──────────────────────────────────────────────────────────────────────
# Benchmark harness
# ──────────────────────────────────────────────────────────────────────


def _run_backend(
    backend_name: str,
    corpus_path: Path,
    corpus_bytes: int,
) -> dict:
    """Single benchmark iteration: scan ``corpus_path`` end-to-end.

    Builds a fresh ``PiiEngine`` (which constructs the requested backend
    via :func:`make_backend`) and a fresh in-memory DB seeded with one
    row pointing at the corpus file. Returns throughput + match-count
    payload consumed by ``Bench.run``.
    """
    db = _MiniDb()
    scan_id = db.seed(source_id=1, file_paths=[str(corpus_path)])

    config = {
        "compliance": {
            "pii": {
                "enabled": True,
                "engine": backend_name,
                # Generous file cap so the entire corpus is scanned.
                "max_file_bytes": corpus_bytes + 1,
                # Allow .txt — the default allowlist already includes it
                # but be explicit for clarity.
                "text_extensions": ["txt"],
            }
        }
    }
    engine = PiiEngine(db, config)

    t0 = time.perf_counter()
    summary = engine.scan_source(source_id=1, scan_id=scan_id,
                                  overwrite_existing=True)
    elapsed = time.perf_counter() - t0

    mb = corpus_bytes / (1024 * 1024)
    throughput = mb / elapsed if elapsed > 0 else 0.0
    matches = int(summary.get("hits_total", 0))
    matches_per_sec = matches / elapsed if elapsed > 0 else 0.0

    return {
        "throughput_value": throughput,
        "throughput_unit": "MB/s",
        "extra": {
            "backend": engine.engine_name,
            "matches": matches,
            "matches_per_sec": round(matches_per_sec, 1),
            "scanned_bytes": corpus_bytes,
        },
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark PII engine backends (issue #66 / #70)."
    )
    parser.add_argument("--corpus-size", type=int, default=100,
                        help="Corpus size in MB (default: 100)")
    parser.add_argument("--repeats", type=int, default=5,
                        help="Measured repeats per backend (default: 5)")
    parser.add_argument("--warmup", type=int, default=1,
                        help="Unmeasured warmup runs (default: 1)")
    parser.add_argument(
        "--history", default="bench_history.jsonl",
        help="JSONL history file to append to (default: bench_history.jsonl). "
             "Pass '' to disable.",
    )
    parser.add_argument("--tmpdir", default=None,
                        help="Override tmpdir for the corpus (default: system tmp)")
    args = parser.parse_args(argv)

    tmp_root = Path(args.tmpdir) if args.tmpdir else Path(
        tempfile.gettempdir()
    ) / "file_activity_bench_pii"
    tmp_root.mkdir(parents=True, exist_ok=True)

    print(f"[bench_pii] corpus_size={args.corpus_size} MB tmpdir={tmp_root}")
    corpus_path, info = generate_corpus(tmp_root, args.corpus_size)
    print(
        f"[bench_pii] corpus={corpus_path} bytes={info['size_bytes']:,} "
        f"fixtures={info['fixtures']} regenerated={info['regenerated']}"
    )

    bench = Bench()

    # Always run stdlib re — that's the baseline.
    bench.run(
        name="pii_re",
        fn=lambda: _run_backend("re", corpus_path, info["size_bytes"]),
        repeats=args.repeats,
        warmup=args.warmup,
        throughput_unit="MB/s",
    )

    # Hyperscan is optional. Skip cleanly when the package isn't
    # importable so the harness still works on Windows / sandbox hosts.
    if hyperscan_available():
        bench.run(
            name="pii_hyperscan",
            fn=lambda: _run_backend("hyperscan", corpus_path, info["size_bytes"]),
            repeats=args.repeats,
            warmup=args.warmup,
            throughput_unit="MB/s",
        )
    else:
        bench.skip(
            "pii_hyperscan",
            "hyperscan package not importable; run "
            "`pip install -r requirements-accel.txt`",
        )

    # Rendered table for stdout / GH step summary.
    table = print_table(bench.results)

    # 5x speedup callout — directly validates the #66 claim.
    re_res = next((r for r in bench.results if r.name == "pii_re"), None)
    hs_res = next(
        (r for r in bench.results
         if r.name == "pii_hyperscan" and not r.extra.get("skipped")),
        None,
    )
    if re_res and hs_res and hs_res.median_ms > 0:
        speedup = re_res.median_ms / hs_res.median_ms
        verdict = "PASS" if speedup >= 5.0 else "BELOW TARGET"
        print(
            f"[bench_pii] hyperscan/re speedup = {speedup:.2f}x ({verdict} vs 5x claim)"
        )

    # GH Actions step summary — same markdown table, no duplication.
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        try:
            with open(summary_path, "a", encoding="utf-8") as fh:
                fh.write("## PII engine benchmark\n\n")
                fh.write(table + "\n")
        except OSError as exc:  # pragma: no cover - CI-only path
            print(f"[warn] could not write step summary: {exc}", file=sys.stderr)

    if args.history:
        wrote = append_history(bench.results, path=args.history)
        print(f"[bench_pii] appended {wrote} rows to {args.history}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
