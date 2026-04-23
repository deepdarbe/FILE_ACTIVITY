"""Common benchmark harness used by every ``tests/bench/bench_*`` module.

Three responsibilities:

1. :class:`Bench` — a tiny stopwatch-style runner. ``Bench.run(name, fn)``
   executes ``fn`` ``warmup`` times to prime caches/JIT-equivalent
   internals, then ``repeats`` times for measurement, recording wall-clock
   elapsed seconds via ``time.perf_counter``. The reported number is the
   median (robust to GC pauses) plus p95 (so a tail-latency regression is
   visible in the same row). Throughput is supplied by the caller because
   every harness measures a different unit (MB/s for PII, files/s for
   scanner, hashes/s for dedup).

2. :func:`print_table` — emits a Markdown table to stdout. The same table
   is what the optional ``bench.yml`` workflow appends to
   ``$GITHUB_STEP_SUMMARY`` so a manual run gives the operator a result
   they can paste into an issue.

3. :func:`append_history` — one JSON object per benchmark per run,
   appended to ``bench_history.jsonl`` (gitignored). Each line is fully
   self-describing — timestamp + git SHA + result fields — so ``jq`` /
   ``pandas.read_json(lines=True)`` can plot regressions over time
   without joining against any external state.

The harness is deliberately stdlib-only: tests/bench is a package that
must run on a fresh checkout with nothing more than ``pip install -r
requirements.txt`` already present.
"""

from __future__ import annotations

import json
import os
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from typing import Callable, Optional


# ──────────────────────────────────────────────────────────────────────
# Result record
# ──────────────────────────────────────────────────────────────────────


@dataclass
class BenchResult:
    """One row in the benchmark output table.

    ``throughput_value`` + ``throughput_unit`` keep the schema generic so
    the same dataclass can describe MB/s (PII), files/s (scanner) and
    hashes/s or MB/s (dedup) without per-harness columns. ``extra`` is a
    free-form dict for anything backend-specific (e.g. matches/sec for
    PII, fanout for scanner) — it survives the round-trip through
    ``bench_history.jsonl``.
    """

    name: str
    median_ms: float
    p95_ms: float
    throughput_value: float
    throughput_unit: str
    repeats: int = 0
    extra: dict = field(default_factory=dict)

    # ──────────────────────────────────────────────────────────────────
    # Convenience accessors so the README's documented field names
    # ("throughput_mbps_or_kops") are still queryable from JSONL rows.
    # ──────────────────────────────────────────────────────────────────

    @property
    def throughput_mbps_or_kops(self) -> float:
        return self.throughput_value


# ──────────────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────────────


class Bench:
    """Stopwatch-style benchmark runner.

    The runner deliberately does NOT try to be ``timeit``-clever: each
    benchmarked function is expected to do real work (open a 100 MB file,
    walk 10k inodes, hash a few MB). The variance across that scale of
    work is dominated by I/O / GC, not by call-overhead measurement
    error, so a simple median-of-N is the right tool.
    """

    def __init__(self) -> None:
        self.results: list[BenchResult] = []

    def run(
        self,
        name: str,
        fn: Callable[[], Optional[dict]],
        repeats: int = 5,
        warmup: int = 1,
        throughput_value: float = 0.0,
        throughput_unit: str = "ops/s",
        extra: Optional[dict] = None,
    ) -> BenchResult:
        """Time ``fn`` and append the result.

        Parameters
        ----------
        name
            Stable label for this benchmark — used in the markdown table
            and as the ``name`` field of the JSONL history.
        fn
            Zero-arg callable. May return a dict; any keys present in
            the returned dict OVERRIDE the caller-supplied ``extra`` /
            ``throughput_value`` / ``throughput_unit`` (so a benchmark
            can compute its own throughput from per-run measurements).
        repeats
            Measured iterations. Median + p95 are over these only; the
            warmup runs are discarded.
        warmup
            Unmeasured iterations executed first to stabilise caches.
        throughput_value, throughput_unit
            Default throughput reported when ``fn`` doesn't override.
        extra
            Free-form metadata stored on the result.
        """
        extra = dict(extra or {})

        for _ in range(max(0, warmup)):
            try:
                fn()
            except Exception as exc:  # pragma: no cover - defensive
                # Warmup failures are not fatal but are surfaced so the
                # operator notices a misconfigured benchmark.
                print(f"[warn] {name}: warmup raised {exc!r}", file=sys.stderr)

        timings: list[float] = []
        last_payload: Optional[dict] = None
        for _ in range(max(1, repeats)):
            t0 = time.perf_counter()
            payload = fn()
            elapsed = time.perf_counter() - t0
            timings.append(elapsed)
            if isinstance(payload, dict):
                last_payload = payload

        # Median over a small N is well-defined; p95 with N=5 falls out
        # of statistics.quantiles using inclusive method (i.e. it picks
        # the largest sample, which is the right semantic for "tail").
        median_s = statistics.median(timings)
        if len(timings) >= 2:
            p95_s = statistics.quantiles(
                timings, n=20, method="inclusive"
            )[18]
        else:
            p95_s = timings[0]

        # Allow the function to override throughput / extras based on
        # what it actually measured (e.g. matches/sec computed from the
        # number of regex hits at runtime).
        if last_payload:
            throughput_value = float(
                last_payload.get("throughput_value", throughput_value)
            )
            throughput_unit = str(
                last_payload.get("throughput_unit", throughput_unit)
            )
            payload_extra = last_payload.get("extra")
            if isinstance(payload_extra, dict):
                extra.update(payload_extra)

        result = BenchResult(
            name=name,
            median_ms=round(median_s * 1000.0, 3),
            p95_ms=round(p95_s * 1000.0, 3),
            throughput_value=round(throughput_value, 3),
            throughput_unit=throughput_unit,
            repeats=len(timings),
            extra=extra,
        )
        self.results.append(result)
        return result

    def skip(self, name: str, reason: str) -> BenchResult:
        """Record a skipped benchmark — keeps the row visible in the
        output table so missing backends are not silently invisible.
        """
        result = BenchResult(
            name=name,
            median_ms=0.0,
            p95_ms=0.0,
            throughput_value=0.0,
            throughput_unit="skipped",
            repeats=0,
            extra={"skipped": True, "reason": reason},
        )
        self.results.append(result)
        return result


# ──────────────────────────────────────────────────────────────────────
# Output helpers
# ──────────────────────────────────────────────────────────────────────


def print_table(results: list[BenchResult]) -> str:
    """Print a Markdown table summarising ``results`` and return it.

    Returning the string lets the GH Actions wrapper redirect it into
    ``$GITHUB_STEP_SUMMARY`` without re-implementing the formatter.
    """
    header = "| Benchmark | Median (ms) | p95 (ms) | Throughput | Unit | Notes |"
    sep = "|---|---:|---:|---:|---|---|"
    lines = [header, sep]
    for r in results:
        if r.extra.get("skipped"):
            lines.append(
                f"| {r.name} | — | — | — | skipped | {r.extra.get('reason', '')} |"
            )
            continue
        notes_parts: list[str] = []
        for k, v in r.extra.items():
            if k in ("skipped", "reason"):
                continue
            notes_parts.append(f"{k}={v}")
        notes = "; ".join(notes_parts)
        lines.append(
            f"| {r.name} | {r.median_ms:.2f} | {r.p95_ms:.2f} | "
            f"{r.throughput_value:.2f} | {r.throughput_unit} | {notes} |"
        )
    rendered = "\n".join(lines)
    print(rendered)
    return rendered


def _git_sha() -> Optional[str]:
    """Best-effort git SHA lookup. ``None`` outside a git checkout."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            timeout=2.0,
        )
        return out.decode("ascii", errors="replace").strip() or None
    except Exception:
        return None


def append_history(
    results: list[BenchResult],
    path: str = "bench_history.jsonl",
) -> int:
    """Append one JSON object per ``BenchResult`` to ``path``.

    Each line carries the timestamp and git SHA so the file is fully
    self-describing — a regression detector can replay the file with no
    side metadata. Returns the number of rows written.
    """
    if not results:
        return 0

    sha = _git_sha()
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)

    with open(path, "a", encoding="utf-8") as fh:
        for r in results:
            row = {
                "timestamp": ts,
                "git_sha": sha,
                **asdict(r),
            }
            fh.write(json.dumps(row, sort_keys=True) + "\n")
    return len(results)


__all__ = [
    "Bench",
    "BenchResult",
    "print_table",
    "append_history",
]
