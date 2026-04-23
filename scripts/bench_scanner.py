"""Micro-benchmark comparing ``os.walk`` against :class:`SmbParallelBackend`.

Usage::

    python scripts/bench_scanner.py --files 100000 --outdir /tmp/bench-tree

If ``--outdir`` already exists the synthetic tree is reused (useful for
repeated runs against the same layout); otherwise a fresh tree is generated.

The script prints the wall-clock time of each walker, the raw file count each
discovered and the speedup ratio (``os.walk`` / parallel). On SMB we'd expect
5-8x; on local NTFS it is usually closer to 1.5-3x depending on cache state.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from pathlib import Path

# Allow running directly from the repo root without installing.
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.scanner.backends.smb_parallel import SmbParallelBackend  # noqa: E402


def _generate_tree(outdir: Path, target_files: int) -> int:
    """Generate a synthetic directory tree with ~``target_files`` tiny files.

    Layout: a 3-level fan-out. At ~100k files we get ~100 top-level dirs each
    containing ~30 subdirs containing ~30 files. This approximates a real
    share: many subtrees worth parallelizing, files near the leaves.
    """
    if outdir.exists():
        existing = sum(1 for _ in outdir.rglob("*") if _.is_file())
        if existing >= target_files * 0.9:
            print(f"Reusing existing tree at {outdir} ({existing} files)")
            return existing
        print(f"Clearing stale tree at {outdir}")
        shutil.rmtree(outdir)

    outdir.mkdir(parents=True, exist_ok=True)

    # Choose a 3-level layout that lands near target_files.
    top = 100
    mid = 30
    per_dir = max(1, target_files // (top * mid))

    created = 0
    t0 = time.time()
    for i in range(top):
        top_dir = outdir / f"dir_{i:03d}"
        top_dir.mkdir(exist_ok=True)
        for j in range(mid):
            mid_dir = top_dir / f"sub_{j:03d}"
            mid_dir.mkdir(exist_ok=True)
            for k in range(per_dir):
                f = mid_dir / f"file_{k:04d}.dat"
                # empty file — we're measuring traversal, not I/O
                f.touch()
                created += 1
                if created >= target_files:
                    break
            if created >= target_files:
                break
        if created >= target_files:
            break

    print(f"Created {created} files in {time.time() - t0:.1f}s at {outdir}")
    return created


def _bench_oswalk(root: str) -> tuple[float, int]:
    """Serial os.walk + per-file stat — the workload FileScanner actually
    performed before this change."""
    count = 0
    t0 = time.time()
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            full = os.path.join(dirpath, name)
            try:
                os.stat(full)
            except OSError:
                continue
            count += 1
    return time.time() - t0, count


def _bench_parallel(root: str, workers: int) -> tuple[float, int]:
    backend = SmbParallelBackend({"scanner": {
        "smb_workers": workers,
        "exclude_patterns": [],
        "skip_hidden": False,
        "skip_system": False,
        "read_owner": False,
    }})
    count = 0
    t0 = time.time()
    for _record in backend.walk(root):
        count += 1
    return time.time() - t0, count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--files", type=int, default=100_000,
                        help="Number of files in the synthetic tree (default 100000)")
    parser.add_argument("--outdir", type=str, required=True,
                        help="Directory to generate the synthetic tree in")
    parser.add_argument("--workers", type=int, default=32,
                        help="Thread pool size for the parallel backend (default 32)")
    args = parser.parse_args()

    outdir = Path(args.outdir).resolve()
    _generate_tree(outdir, args.files)

    print(f"\nBenchmarking walkers on {outdir}\n")

    walk_elapsed, walk_count = _bench_oswalk(str(outdir))
    print(f"os.walk              : {walk_elapsed:7.2f}s  ({walk_count} files, "
          f"{walk_count / max(walk_elapsed, 1e-9):.0f} files/s)")

    par_elapsed, par_count = _bench_parallel(str(outdir), args.workers)
    print(f"SmbParallelBackend   : {par_elapsed:7.2f}s  ({par_count} files, "
          f"{par_count / max(par_elapsed, 1e-9):.0f} files/s, workers={args.workers})")

    if par_elapsed > 0:
        ratio = walk_elapsed / par_elapsed
        print(f"\nSpeedup              : {ratio:.2f}x")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
