"""Benchmark the scanner backends side-by-side.

Synthesizes a directory tree with ``(N, fanout)`` controls so each
backend walks an identical layout. The tree lives on tmpfs
(``/dev/shm`` on Linux) when available — that removes disk-cache
warm-up effects, which is the dominant noise source on a cold-cache
``/tmp`` run.

Backends that are Windows-only (``win32_find_ex``, ``ntfs_mft``,
``ntfs_usn_tail``) raise ``RuntimeError`` on construction outside of
Windows; they are recorded as ``skipped`` rows in the result table so
their absence is visible rather than silent.

Reports files/sec — the only throughput unit that's apples-to-apples
across the four backends since some don't return file size or
timestamps at all (``ntfs_mft``).

Usage::

    python -m tests.bench.bench_scanner \
        [--files 10000] [--fanout 10] \
        [--repeats 3] [--history bench_history.jsonl]
"""

from __future__ import annotations

import argparse
import os
import shutil
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
# Tree synthesis
# ──────────────────────────────────────────────────────────────────────


def _pick_tmp_root(prefer_shm: bool = True) -> Path:
    """Prefer ``/dev/shm`` (tmpfs) when writable so we measure walk
    speed, not disk seek latency. Falls back to the system tmpdir.
    """
    if prefer_shm:
        shm = Path("/dev/shm")
        if shm.exists() and os.access(shm, os.W_OK):
            return shm / "file_activity_bench_scanner"
    return Path(tempfile.gettempdir()) / "file_activity_bench_scanner"


def synthesize_tree(root: Path, files: int, fanout: int) -> dict:
    """Create ``files`` empty regular files under ``root`` arranged so
    each directory has at most ``fanout`` children.

    The shape is a balanced N-ary tree of depth ``ceil(log_fanout(files))``
    with leaves containing the actual files. Empty files are fine —
    every backend we measure pays the cost of the ``stat()`` call, and
    file-size readout is a few cycles next to the FS round-trip.

    Idempotent: existing trees with the right file count are reused.
    """
    marker = root / f".tree_{files}_{fanout}"
    if marker.exists():
        return {
            "root": str(root),
            "files": files,
            "fanout": fanout,
            "regenerated": False,
        }

    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)

    # Depth needed so that fanout**depth >= files when each leaf dir
    # holds up to ``fanout`` files. We split per level so dir count
    # also scales with fanout (otherwise a single deep-but-narrow dir
    # makes the SMB-parallel backend look like a serial walker).
    fan = max(2, int(fanout))
    depth = 1
    while fan ** depth < files:
        depth += 1
        if depth > 8:  # pragma: no cover - safety net
            break

    created = 0

    def _populate(dir_path: Path, level: int) -> None:
        nonlocal created
        if created >= files:
            return
        if level == depth:
            # Leaf directory — drop up to fanout files here.
            for i in range(fan):
                if created >= files:
                    return
                p = dir_path / f"file_{created:07d}.txt"
                p.touch()
                created += 1
            return
        for i in range(fan):
            if created >= files:
                return
            sub = dir_path / f"d{level}_{i}"
            sub.mkdir(exist_ok=True)
            _populate(sub, level + 1)

    _populate(root, 1)

    marker.touch()
    return {
        "root": str(root),
        "files": created,
        "fanout": fan,
        "depth": depth,
        "regenerated": True,
    }


# ──────────────────────────────────────────────────────────────────────
# Backend invokers
# ──────────────────────────────────────────────────────────────────────


def _measure_walk(walker, root: str, expected: int) -> dict:
    """Drain a backend's ``walk(root)`` iterator and report files/sec."""
    t0 = time.perf_counter()
    count = 0
    for _row in walker.walk(root):
        count += 1
    elapsed = time.perf_counter() - t0
    fps = count / elapsed if elapsed > 0 else 0.0
    return {
        "throughput_value": fps,
        "throughput_unit": "files/s",
        "extra": {
            "files_walked": count,
            "expected_files": expected,
            "elapsed_s": round(elapsed, 4),
        },
    }


def _bench_smb_parallel(bench: Bench, root: str, expected: int,
                        repeats: int, warmup: int, workers: int) -> None:
    try:
        from src.scanner.backends.smb_parallel import SmbParallelBackend
    except Exception as exc:
        bench.skip("scanner_smb_parallel", f"import failed: {exc}")
        return

    backend = SmbParallelBackend({
        "scanner": {
            "smb_workers": workers,
            "skip_hidden": False,  # don't skip our marker file
            "skip_system": False,
            "read_owner": False,
            "exclude_patterns": [],
        }
    })
    bench.run(
        name="scanner_smb_parallel",
        fn=lambda: _measure_walk(backend, root, expected),
        repeats=repeats,
        warmup=warmup,
        throughput_unit="files/s",
        extra={"workers": workers},
    )


def _bench_win32_find_ex(bench: Bench, root: str, expected: int,
                         repeats: int, warmup: int) -> None:
    if sys.platform != "win32":
        bench.skip(
            "scanner_win32_find_ex",
            "Windows-only backend; sys.platform != 'win32'",
        )
        return
    try:
        from src.scanner.backends.win32_find_ex import Win32FindExBackend
    except Exception as exc:
        bench.skip("scanner_win32_find_ex", f"import failed: {exc}")
        return
    try:
        backend = Win32FindExBackend({"scanner": {}})
    except Exception as exc:
        bench.skip("scanner_win32_find_ex", f"init failed: {exc}")
        return
    bench.run(
        name="scanner_win32_find_ex",
        fn=lambda: _measure_walk(backend, root, expected),
        repeats=repeats,
        warmup=warmup,
        throughput_unit="files/s",
    )


def _bench_ntfs_mft(bench: Bench, root: str, expected: int,
                    repeats: int, warmup: int) -> None:
    if sys.platform != "win32":
        bench.skip(
            "scanner_ntfs_mft",
            "Windows + admin + local NTFS volume required",
        )
        return
    try:
        from src.scanner.backends.ntfs_mft import NtfsMftBackend
    except Exception as exc:
        bench.skip("scanner_ntfs_mft", f"import failed: {exc}")
        return
    try:
        backend = NtfsMftBackend({"scanner": {}})
    except Exception as exc:
        bench.skip("scanner_ntfs_mft", f"init failed: {exc}")
        return
    bench.run(
        name="scanner_ntfs_mft",
        fn=lambda: _measure_walk(backend, root, expected),
        repeats=repeats,
        warmup=warmup,
        throughput_unit="files/s",
    )


def _bench_ntfs_usn_tail(bench: Bench, root: str, expected: int,
                         repeats: int, warmup: int) -> None:
    # USN tail is incremental, not an enumeration backend — there's no
    # apples-to-apples ``walk(root)`` comparison to make. Skip it
    # explicitly so the row exists in the table for transparency.
    bench.skip(
        "scanner_ntfs_usn_tail",
        "incremental backend; no enumeration comparison available",
    )


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark scanner backends side-by-side (issue #70)."
    )
    parser.add_argument("--files", type=int, default=10_000,
                        help="Number of files to synthesize (default: 10000)")
    parser.add_argument("--fanout", type=int, default=10,
                        help="Children per directory (default: 10)")
    parser.add_argument("--repeats", type=int, default=3,
                        help="Measured repeats per backend (default: 3)")
    parser.add_argument("--warmup", type=int, default=1,
                        help="Unmeasured warmup runs (default: 1)")
    parser.add_argument("--workers", type=int, default=16,
                        help="Thread pool size for smb_parallel (default: 16)")
    parser.add_argument("--no-shm", action="store_true",
                        help="Skip /dev/shm even if available")
    parser.add_argument("--history", default="bench_history.jsonl",
                        help="JSONL history file (pass '' to disable)")
    args = parser.parse_args(argv)

    tmp_root = _pick_tmp_root(prefer_shm=not args.no_shm)
    print(f"[bench_scanner] tmp_root={tmp_root}")

    info = synthesize_tree(tmp_root, args.files, args.fanout)
    print(
        f"[bench_scanner] tree files={info['files']} fanout={info['fanout']} "
        f"depth={info.get('depth', '?')} regenerated={info['regenerated']}"
    )

    bench = Bench()
    _bench_smb_parallel(bench, str(tmp_root), info["files"],
                        args.repeats, args.warmup, args.workers)
    _bench_win32_find_ex(bench, str(tmp_root), info["files"],
                         args.repeats, args.warmup)
    _bench_ntfs_mft(bench, str(tmp_root), info["files"],
                    args.repeats, args.warmup)
    _bench_ntfs_usn_tail(bench, str(tmp_root), info["files"],
                         args.repeats, args.warmup)

    table = print_table(bench.results)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        try:
            with open(summary_path, "a", encoding="utf-8") as fh:
                fh.write("## Scanner backend benchmark\n\n")
                fh.write(table + "\n")
        except OSError as exc:  # pragma: no cover - CI-only
            print(f"[warn] could not write step summary: {exc}", file=sys.stderr)

    if args.history:
        wrote = append_history(bench.results, path=args.history)
        print(f"[bench_scanner] appended {wrote} rows to {args.history}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
