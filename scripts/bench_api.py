"""HTTP-level latency benchmark for the dashboard's hot endpoints.

Why this exists
---------------
``scripts/bench_storage.py`` (PR #217) measures SQL-level latency
(SQLite vs DuckDB) but says nothing about the **end-to-end HTTP path** —
FastAPI dispatch, anyio threadpool overhead, JSON serialisation, response
size. The customer's "every page is waiting" symptom (PR #215) and the
"Treemap empty waiting" symptom (PR #227) both lived at this layer, not
the SQL layer.

This script runs against the dashboard running on the customer machine
(or a copy of it) and reports p50/p95/p99 + cache hit ratio per
endpoint, for both **cold** (cache miss) and **warm** (cache hit)
states.

Usage
-----
On the customer machine, with the FileActivity service running::

    python scripts/bench_api.py --base http://localhost:8085 \\
        --source-id 1 --repeats 20

Outputs a table + JSON sidecar ``bench_api_<ts>.json`` for diffing
across runs (before/after an index migration, before/after a code
fix).

Endpoint set (5 critical reads, the ones the dashboard hits on every
load):

  /api/dashboard/init                       - overview KPIs
  /api/reports/frequency/{source_id}        - Erisim Sikligi page
  /api/reports/types/{source_id}            - Dosya Turleri page
  /api/reports/sizes/{source_id}            - Boyut Dagilimi page
  /api/reports/duplicates/{source_id}       - Kopya Dosyalar page

Cold vs warm:
  - Cold: hit each endpoint once *before* it's been called this session.
    The analyzer_cache LRU is empty, the DB-tier cache may or may not
    be hot (depends on prior session). First-call latency dominates.
  - Warm: hit each endpoint --repeats more times. The analyzer_cache
    LRU is now hot; latency is just the response-serialise path.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import urllib.error
import urllib.request
from typing import Iterable

ENDPOINTS = [
    ("dashboard_init", "/api/dashboard/init"),
    ("frequency", "/api/reports/frequency/{source_id}"),
    ("types", "/api/reports/types/{source_id}"),
    ("sizes", "/api/reports/sizes/{source_id}"),
    ("duplicates", "/api/reports/duplicates/{source_id}"),
]


def _http_get(url: str, timeout: float = 120.0) -> tuple[int, float, dict]:
    """Return (status_code, elapsed_seconds, parsed_json_or_empty)."""
    t0 = time.perf_counter()
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            elapsed = time.perf_counter() - t0
            body = resp.read()
            try:
                parsed = json.loads(body.decode("utf-8")) if body else {}
            except Exception:
                parsed = {}
            return resp.status, elapsed, parsed
    except urllib.error.HTTPError as e:
        elapsed = time.perf_counter() - t0
        return e.code, elapsed, {}
    except Exception as e:
        elapsed = time.perf_counter() - t0
        print(f"  HTTP error: {e}", file=sys.stderr)
        return -1, elapsed, {}


def _bench_one(
    name: str, url: str, repeats: int
) -> dict:
    """Hit ``url`` ``repeats`` times. First call is the cold sample;
    the rest are warm. Both are reported separately."""
    samples = []
    status_codes = []
    cache_hits = 0
    cache_misses = 0
    response_sizes: list[int] = []
    for i in range(repeats):
        status, elapsed, body = _http_get(url)
        samples.append(elapsed)
        status_codes.append(status)
        cache = body.get("cache", {}) if isinstance(body, dict) else {}
        if isinstance(cache, dict):
            if cache.get("hit"):
                cache_hits += 1
            else:
                cache_misses += 1
        response_sizes.append(len(json.dumps(body).encode("utf-8")))
    cold = samples[0] if samples else float("nan")
    warm = samples[1:] if len(samples) > 1 else []
    return {
        "name": name,
        "url": url,
        "status_codes": status_codes,
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "response_size_bytes_p50": int(statistics.median(response_sizes)) if response_sizes else 0,
        "cold_seconds": cold,
        "warm_count": len(warm),
        "warm_min": min(warm) if warm else float("nan"),
        "warm_p50": statistics.median(warm) if warm else float("nan"),
        "warm_p95": _percentile(warm, 0.95),
        "warm_p99": _percentile(warm, 0.99),
        "warm_max": max(warm) if warm else float("nan"),
        "samples": samples,
    }


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = (len(s) - 1) * p
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def _fmt_t(seconds: float) -> str:
    if seconds != seconds:  # NaN
        return "      n/a"
    if seconds < 0.001:
        return f"{seconds * 1e6:7.0f} us"
    if seconds < 1.0:
        return f"{seconds * 1000:7.1f} ms"
    return f"{seconds:7.2f} s "


def _print_table(results: list[dict]) -> None:
    print()
    print(f"{'Endpoint':<16} {'cold':>10} {'warm p50':>10} {'warm p95':>10} "
          f"{'warm p99':>10} {'cache':>8} {'bytes':>10}")
    print("-" * 84)
    for r in results:
        cache_total = r["cache_hits"] + r["cache_misses"]
        cache_ratio = (
            f"{r['cache_hits']}/{cache_total}" if cache_total else "  —"
        )
        # Flag bad status codes
        bad_status = [c for c in r["status_codes"] if c < 200 or c >= 300]
        status_marker = f" ⚠ {bad_status[0]}" if bad_status else ""
        print(
            f"{r['name']:<16} "
            f"{_fmt_t(r['cold_seconds']):>10} "
            f"{_fmt_t(r['warm_p50']):>10} "
            f"{_fmt_t(r['warm_p95']):>10} "
            f"{_fmt_t(r['warm_p99']):>10} "
            f"{cache_ratio:>8} "
            f"{r['response_size_bytes_p50']:>9,d}B"
            f"{status_marker}"
        )
    print()
    print("Cold = first call (cache cold). Warm = subsequent calls (cache warm).")
    print("cache column shows analyzer_cache hits / total calls.")


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--base",
        default="http://localhost:8085",
        help="Base URL of the running FileActivity dashboard (default: http://localhost:8085).",
    )
    ap.add_argument(
        "--source-id",
        type=int,
        required=True,
        help="Source ID to bench against. See /api/sources for the list.",
    )
    ap.add_argument(
        "--repeats",
        type=int,
        default=10,
        help="Calls per endpoint. First is cold; rest are warm (default: 10).",
    )
    ap.add_argument(
        "--endpoints",
        nargs="*",
        choices=[name for name, _ in ENDPOINTS] + ["all"],
        default=["all"],
        help="Subset of endpoints to bench (default: all).",
    )
    args = ap.parse_args(list(argv) if argv else None)

    base = args.base.rstrip("/")
    sid = args.source_id

    selected = args.endpoints
    if "all" in selected:
        names = [n for n, _ in ENDPOINTS]
    else:
        names = list(selected)

    print(f"Bench target: {base}")
    print(f"source_id={sid}, repeats={args.repeats}")
    print()

    results = []
    for name, template in ENDPOINTS:
        if name not in names:
            continue
        url = base + template.replace("{source_id}", str(sid))
        print(f"  benching {name:<16} {url}")
        r = _bench_one(name, url, args.repeats)
        results.append(r)

    _print_table(results)

    ts = time.strftime("%Y%m%d-%H%M%S")
    out_path = f"bench_api_{ts}.json"
    with open(out_path, "w") as f:
        json.dump({
            "base": base,
            "source_id": sid,
            "repeats": args.repeats,
            "results": results,
        }, f, indent=2)
    print(f"JSON sidecar: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
