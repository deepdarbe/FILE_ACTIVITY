# Benchmark harness (issue #70)

`tests/bench/` produces deterministic, run-over-run comparable
performance numbers for the parts of FILE_ACTIVITY whose throughput
matters: the PII regex engine, the four scanner backends, and the
SHA-256 dedup-hashing path.

The harness is **not** auto-discovered by `pytest` — the package has
no `test_*` modules, so a normal `pytest` run skips it entirely.
Invoke each benchmark directly via `python -m`.

## Quick start

```bash
# 1. PII engine (validates the issue #66 5x speedup claim)
python -m tests.bench.bench_pii

# 2. Scanner backends (smb_parallel always; Windows backends skipped on Linux)
python -m tests.bench.bench_scanner

# 3. SHA-256 dedup hashing (probes SHA-NI availability)
python -m tests.bench.bench_dedup
```

Every harness writes a Markdown table to stdout and appends one JSON
object per benchmark to `bench_history.jsonl` (gitignored).

## CLI reference

### `bench_pii.py`

```
python -m tests.bench.bench_pii \
    [--corpus-size MB]   # default 100
    [--repeats N]        # default 5
    [--warmup N]         # default 1
    [--history PATH]     # default bench_history.jsonl, '' to disable
    [--tmpdir DIR]       # default <tmp>/file_activity_bench_pii
```

Generates a deterministic corpus (random words seeded with
`random.seed(42)` plus 10k emails / 5k IBANs / 2k TCKNs at known
positions) and times `PiiEngine.scan_source` end-to-end against an
in-memory SQLite stub. Reports MB/s and matches/sec for both `re` and
`hyperscan` backends; if the optional `hyperscan` package isn't
importable the harness records `pii_hyperscan` as a `skipped` row and
continues.

After the table, the harness prints a `speedup = Nx (PASS|BELOW
TARGET)` line directly comparing the two backends against the issue
#66 ≥ 5x claim.

### `bench_scanner.py`

```
python -m tests.bench.bench_scanner \
    [--files N]          # default 10000
    [--fanout N]         # default 10  (children per directory)
    [--repeats N]        # default 3
    [--warmup N]         # default 1
    [--workers N]        # default 16  (smb_parallel thread count)
    [--no-shm]           # skip /dev/shm even if available
    [--history PATH]
```

Synthesises a balanced N-ary directory tree (defaults to ~10k empty
files arranged with fanout 10) on tmpfs (`/dev/shm` when writable on
Linux) and times each available backend's `walk(root)` iteration.

| Backend                 | Linux                        | Windows |
|-------------------------|------------------------------|---------|
| `smb_parallel`          | runs (default 16 workers)    | runs    |
| `win32_find_ex`         | skipped (Windows-only ctypes)| runs    |
| `ntfs_mft`              | skipped (NTFS + admin)       | runs    |
| `ntfs_usn_tail`         | skipped (incremental, not enumeration) | skipped (no enumeration parity) |

Reports files/sec.

### `bench_dedup.py`

```
python -m tests.bench.bench_dedup \
    [--corpus-size MB]   # default 100
    [--files N]          # default 32
    [--repeats N]        # default 5
    [--warmup N]         # default 1
    [--tmpdir DIR]
    [--history PATH]
```

Builds N fixture files of equal size totalling `corpus-size` MB and
streams `hashlib.sha256` over each in 1 MiB chunks. Probes for SHA-NI
by inspecting `/proc/cpuinfo` (Linux) and `hashlib.algorithms_guaranteed`;
the detection result is logged AND stored in the JSONL row's `extra`
field so a regression detector can correlate throughput changes with
hardware capability changes (e.g. moving the CI runner pool).

Hashing always runs — SHA-NI absence is not fatal, just slower.

## JSONL history format

Each line of `bench_history.jsonl` is a single JSON object:

```json
{
  "timestamp": "2026-04-23T18:42:11Z",
  "git_sha": "abcd1234...",
  "name": "pii_hyperscan",
  "median_ms": 412.3,
  "p95_ms": 438.1,
  "throughput_value": 242.6,
  "throughput_unit": "MB/s",
  "repeats": 5,
  "extra": {
    "backend": "hyperscan",
    "matches": 17000,
    "matches_per_sec": 41245.7,
    "scanned_bytes": 104857600
  }
}
```

The schema is intentionally flat-ish: every row carries enough context
to be plotted standalone, no joining against external metadata
required.

## Consuming for regression tracking

```bash
# 1. PII MB/s over time, one line per backend:
jq -r 'select(.name|startswith("pii_")) |
       [.timestamp, .name, .throughput_value] | @tsv' \
   bench_history.jsonl

# 2. Quick "did anything regress >10%" check between latest two runs
#    of pii_hyperscan:
jq -s 'map(select(.name=="pii_hyperscan")) | sort_by(.timestamp) |
       .[-2:] | (.[1].median_ms / .[0].median_ms - 1) * 100' \
   bench_history.jsonl
```

Or in Python:

```python
import pandas as pd
df = pd.read_json("bench_history.jsonl", lines=True)
pii = df[df.name.str.startswith("pii_")]
pii.pivot_table(
    index="timestamp", columns="name", values="throughput_value"
).plot()
```

## Determinism guarantees

* Same hardware + same git SHA → numbers within ~5% noise (variance
  budget for GC pauses + OS scheduler jitter).
* Corpus generators are seeded (`random.seed(42)`); they regenerate
  only when the requested size differs from what's already on disk.
* Tmpfs is preferred over disk to remove cold-cache effects from the
  scanner walk benchmark.

## Adding a new benchmark

1. Add `tests/bench/bench_<thing>.py`.
2. Inside, instantiate `tests.bench._runner.Bench`, call `.run(name,
   fn, repeats=...)` for each variant you want to time.
3. Have `fn` return `{"throughput_value": float, "throughput_unit":
   str, "extra": {...}}` so the runner records it.
4. Call `print_table(bench.results)` and `append_history(bench.results)`
   at the end.
5. Document the harness in this file and (optionally) wire it into
   `.github/workflows/bench.yml`.
