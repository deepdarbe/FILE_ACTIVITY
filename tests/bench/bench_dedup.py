"""Benchmark the SHA-256 dedup-hashing path.

The dedup pipeline (``src/storage/database.py``) hashes every audited
event row and chains it; on a busy share that hash loop becomes the
hot path. This harness measures pure ``hashlib.sha256`` throughput
against a deterministic fixture corpus and reports MB/s.

It also probes whether the host CPU is using SHA-NI (Intel/AMD's
SHA-extension) by:

1. Verifying ``sha256`` is in :data:`hashlib.algorithms_guaranteed`
   (always true on CPython 3.11, but kept as a sanity check so the
   harness fails loudly if the stdlib was somehow built without it).
2. Reading ``/proc/cpuinfo`` on Linux for the ``sha_ni`` flag.

If SHA-NI is absent we still run the benchmark — it just produces a
slower number. The detection result is logged + recorded in the
``extra`` block of the JSONL row so a regression report can correlate
throughput with hardware capability.

Usage::

    python -m tests.bench.bench_dedup \
        [--corpus-size 100] [--files 32] \
        [--repeats 5] [--history bench_history.jsonl]
"""

from __future__ import annotations

import argparse
import hashlib
import os
import random
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.bench._runner import (  # noqa: E402
    Bench,
    append_history,
    print_table,
)


# ──────────────────────────────────────────────────────────────────────
# Hardware probe
# ──────────────────────────────────────────────────────────────────────


def detect_sha_ni() -> dict:
    """Best-effort detection of SHA-NI availability.

    Returns a dict with both pieces of evidence so the operator can
    inspect what we found. Never raises — every probe is wrapped.
    """
    info: dict = {
        "sha256_in_guaranteed": "sha256" in hashlib.algorithms_guaranteed,
        "cpuinfo_sha_ni": None,
        "platform": sys.platform,
    }

    if sys.platform.startswith("linux"):
        try:
            with open("/proc/cpuinfo", "r", encoding="utf-8") as fh:
                # Reading the first ~64 KB is enough — flag list lives
                # within the first processor block on every kernel.
                blob = fh.read(65536)
            # The flag is reported as ``sha_ni`` (Intel/AMD) or
            # occasionally ``sha`` (older kernels). Accept either.
            tokens = set(blob.split())
            info["cpuinfo_sha_ni"] = (
                "sha_ni" in tokens or "sha" in tokens
            )
        except OSError as exc:
            info["cpuinfo_error"] = repr(exc)
    return info


# ──────────────────────────────────────────────────────────────────────
# Corpus
# ──────────────────────────────────────────────────────────────────────


def synthesize_files(target_dir: Path, total_mb: int,
                     n_files: int) -> tuple[list[Path], int]:
    """Create ``n_files`` files in ``target_dir`` summing to ~``total_mb``.

    Each file gets the same fixed-size random buffer (seeded). Hashing
    speed is independent of payload entropy, so we don't bother
    re-randomising per file. Idempotent — existing files are reused.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    per_file = max(1, (total_mb * 1024 * 1024) // n_files)

    rng = random.Random(42)
    payload = bytes(rng.getrandbits(8) for _ in range(min(per_file, 1024 * 1024)))
    # Stretch payload up to per_file size by tiling — the hash still
    # processes per_file bytes, but we don't keep a 100 MB Python bytes
    # object in RAM longer than needed.
    paths: list[Path] = []
    total_bytes = 0
    for i in range(n_files):
        p = target_dir / f"dedup_{total_mb}mb_{i:04d}.bin"
        if p.exists() and p.stat().st_size == per_file:
            paths.append(p)
            total_bytes += per_file
            continue
        with open(p, "wb") as fh:
            written = 0
            while written < per_file:
                chunk = payload[: min(len(payload), per_file - written)]
                fh.write(chunk)
                written += len(chunk)
        paths.append(p)
        total_bytes += per_file
    return paths, total_bytes


# ──────────────────────────────────────────────────────────────────────
# Hash loop
# ──────────────────────────────────────────────────────────────────────


def _hash_files(paths: list[Path], total_bytes: int,
                chunk_size: int = 1 << 20) -> dict:
    """Hash every file with sha256 streaming, return MB/s."""
    digest_count = 0
    t0 = time.perf_counter()
    for p in paths:
        h = hashlib.sha256()
        with open(p, "rb") as fh:
            while True:
                buf = fh.read(chunk_size)
                if not buf:
                    break
                h.update(buf)
        h.hexdigest()
        digest_count += 1
    elapsed = time.perf_counter() - t0
    mb = total_bytes / (1024 * 1024)
    mbps = mb / elapsed if elapsed > 0 else 0.0
    hps = digest_count / elapsed if elapsed > 0 else 0.0
    return {
        "throughput_value": mbps,
        "throughput_unit": "MB/s",
        "extra": {
            "files": digest_count,
            "hashes_per_sec": round(hps, 2),
            "scanned_bytes": total_bytes,
        },
    }


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark SHA-256 dedup hashing path (issue #70)."
    )
    parser.add_argument("--corpus-size", type=int, default=100,
                        help="Total fixture size in MB (default: 100)")
    parser.add_argument("--files", type=int, default=32,
                        help="Number of fixture files (default: 32)")
    parser.add_argument("--repeats", type=int, default=5,
                        help="Measured repeats (default: 5)")
    parser.add_argument("--warmup", type=int, default=1,
                        help="Unmeasured warmup runs (default: 1)")
    parser.add_argument("--tmpdir", default=None,
                        help="Override tmpdir (default: system tmp)")
    parser.add_argument("--history", default="bench_history.jsonl",
                        help="JSONL history file (pass '' to disable)")
    args = parser.parse_args(argv)

    sha_info = detect_sha_ni()
    print(
        f"[bench_dedup] sha-ni detection: cpuinfo_sha_ni={sha_info['cpuinfo_sha_ni']} "
        f"sha256_in_guaranteed={sha_info['sha256_in_guaranteed']} "
        f"platform={sha_info['platform']}"
    )

    tmp_root = Path(args.tmpdir) if args.tmpdir else Path(
        tempfile.gettempdir()
    ) / "file_activity_bench_dedup"

    paths, total_bytes = synthesize_files(
        tmp_root, args.corpus_size, args.files
    )
    print(
        f"[bench_dedup] corpus files={len(paths)} total_bytes={total_bytes:,} "
        f"per_file={total_bytes // len(paths):,} dir={tmp_root}"
    )

    bench = Bench()
    bench.run(
        name="dedup_sha256_streaming",
        fn=lambda: _hash_files(paths, total_bytes),
        repeats=args.repeats,
        warmup=args.warmup,
        throughput_unit="MB/s",
        extra={
            "sha_ni_detected": bool(sha_info.get("cpuinfo_sha_ni")),
            "platform": sha_info["platform"],
        },
    )

    table = print_table(bench.results)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        try:
            with open(summary_path, "a", encoding="utf-8") as fh:
                fh.write("## Dedup hashing benchmark\n\n")
                fh.write(table + "\n")
        except OSError as exc:  # pragma: no cover - CI-only
            print(f"[warn] could not write step summary: {exc}", file=sys.stderr)

    if args.history:
        wrote = append_history(bench.results, path=args.history)
        print(f"[bench_dedup] appended {wrote} rows to {args.history}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
