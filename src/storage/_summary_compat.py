"""Shape normaliser for ``scan_runs.summary_json``.

EPIC #225 R-3. Two unrelated writers append to the same JSON blob:

  * ``compute_scan_summary`` (final, post-scan) writes ``age_buckets``
    and ``size_buckets`` as **LIST of dicts** with ``label`` /
    ``file_count`` / ``total_size`` keys.
  * ``partial_summary_v2.PartialSummaryV2Builder`` (live, during scan)
    writes the same keys as **DICT of label → count**.

Same JSON column, two shapes. Consumers — most notably the
``report_frequency`` endpoint (PR #198 / #223) — had to runtime-detect
and convert. This module is the single source of truth for that
conversion. Every reader goes through ``normalize_summary(...)``
before touching the dict.

Canonical shape (what readers see):

  age_buckets  : list[{label, days_min, days_max, file_count, total_size}]
  size_buckets : list[{label, min_bytes, max_bytes, file_count, total_size}]
  top_extensions / top_owners / top_risky_files / top_large_files :
    list of dicts (already canonical in both writers)
  everything else : passes through unchanged.

``normalize_summary`` is **idempotent** — passing an already-canonical
dict returns an equivalent dict. Safe to call multiple times.
"""
from __future__ import annotations

from typing import Any


# Mapping between the partial_summary_v2 dict-shape keys and the
# compute_scan_summary list-shape labels. Different label conventions
# bite us in two places:
#   - age: v2 uses "<30d" / "30-60d" / "60-90d" / "90-180d" /
#     "180-365d" / ">365d"; compute_scan_summary uses
#     "0-30" / "31-90" / "91-180" / "181-365" / "366+".
#   - size: v2 uses "<1MB" / "1-10MB" / "10-100MB" / "100-1GB" /
#     ">1GB"; compute_scan_summary builds labels off config
#     (tiny/small/medium/large/xlarge).

_AGE_V2_TO_LIST = [
    # (v2_key,       label,      days_min, days_max)
    ("<30d",         "0-30",     0,        30),
    ("30-60d",       "31-90",    31,       60),   # v2 "30-60" maps to list "31-90" approx
    ("60-90d",       "31-90",    61,       90),   # both fall in "31-90" list bucket
    ("90-180d",      "91-180",   91,       180),
    ("180-365d",     "181-365",  181,      365),
    (">365d",        "366+",     366,      None),
]

_SIZE_V2_TO_LIST = [
    # (v2_key,       label,    min_bytes,   max_bytes)
    ("<1MB",         "tiny",   0,           1024 * 1024 - 1),
    ("1-10MB",       "small",  1024 * 1024, 10 * 1024 * 1024 - 1),
    ("10-100MB",     "medium", 10 * 1024 * 1024, 100 * 1024 * 1024 - 1),
    ("100-1GB",      "large",  100 * 1024 * 1024, 1024 * 1024 * 1024 - 1),
    (">1GB",         "xlarge", 1024 * 1024 * 1024, None),
]


def _age_buckets_dict_to_list(d: dict) -> list[dict]:
    """Collapse v2 dict-shape age_buckets into canonical list-shape.

    v2 has 6 buckets but list-shape has 5 (30-60 and 60-90 merge into
    31-90). We sum counts when merging — no information loss since
    the list shape is the "official" one going forward.
    """
    out_by_label: dict[str, dict] = {}
    for v2_key, label, dmin, dmax in _AGE_V2_TO_LIST:
        if v2_key not in d:
            continue
        cnt = int(d.get(v2_key, 0) or 0)
        if label in out_by_label:
            out_by_label[label]["file_count"] += cnt
        else:
            out_by_label[label] = {
                "label": label,
                "days_min": dmin,
                "days_max": dmax,
                "file_count": cnt,
                "total_size": 0,  # v2 doesn't track per-bucket size
            }
    # Preserve canonical order
    order = ["0-30", "31-90", "91-180", "181-365", "366+"]
    return [out_by_label[k] for k in order if k in out_by_label]


def _size_buckets_dict_to_list(d: dict) -> list[dict]:
    """Collapse v2 dict-shape size_buckets into canonical list-shape."""
    out = []
    for v2_key, label, mn, mx in _SIZE_V2_TO_LIST:
        if v2_key not in d:
            continue
        out.append({
            "label": label,
            "min_bytes": mn,
            "max_bytes": mx,
            "file_count": int(d.get(v2_key, 0) or 0),
            "total_size": 0,  # v2 doesn't track per-bucket size
        })
    return out


def normalize_summary(raw: Any) -> Any:
    """Return ``raw`` with bucket fields normalised to the canonical
    list-shape. Idempotent and safe on None / non-dict input.
    """
    if not isinstance(raw, dict):
        return raw
    out = dict(raw)

    age = out.get("age_buckets")
    if isinstance(age, dict):
        out["age_buckets"] = _age_buckets_dict_to_list(age)
    # If already a list (compute_scan_summary shape), leave alone.

    size = out.get("size_buckets")
    if isinstance(size, dict):
        out["size_buckets"] = _size_buckets_dict_to_list(size)

    # top_extensions / top_owners / top_risky_files / top_large_files
    # are already list-shape in both writers — no normalisation needed.
    return out
