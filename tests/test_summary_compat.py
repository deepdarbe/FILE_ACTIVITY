"""Unit tests for src/storage/_summary_compat.py.

Pins the bug class that surfaced as PR #198 / #223: two writers of
the same scan_runs.summary_json key (age_buckets, size_buckets) write
two incompatible shapes. The normaliser is the single chokepoint.
"""
from __future__ import annotations

from src.storage._summary_compat import normalize_summary


# ---------------------------------------------------------------------------
# Pass-through cases
# ---------------------------------------------------------------------------


def test_none_passes_through():
    assert normalize_summary(None) is None


def test_non_dict_passes_through():
    assert normalize_summary("string") == "string"
    assert normalize_summary(42) == 42
    assert normalize_summary([1, 2, 3]) == [1, 2, 3]


def test_empty_dict_unchanged():
    assert normalize_summary({}) == {}


def test_scalars_pass_through():
    raw = {"total_files": 100, "total_size": 1024, "summary_json_version": 2}
    assert normalize_summary(raw) == raw


# ---------------------------------------------------------------------------
# age_buckets — dict (v2) → list (canonical)
# ---------------------------------------------------------------------------


def test_age_buckets_v2_dict_converted_to_list():
    raw = {"age_buckets": {
        "<30d": 100, "30-60d": 50, "60-90d": 30,
        "90-180d": 200, "180-365d": 400, ">365d": 1000,
    }}
    result = normalize_summary(raw)
    assert isinstance(result["age_buckets"], list)
    # 5 buckets in the canonical list shape: 0-30, 31-90, 91-180, 181-365, 366+
    labels = [b["label"] for b in result["age_buckets"]]
    assert labels == ["0-30", "31-90", "91-180", "181-365", "366+"]
    # 30-60d (50) + 60-90d (30) merge into 31-90 (80)
    by_label = {b["label"]: b["file_count"] for b in result["age_buckets"]}
    assert by_label["0-30"] == 100
    assert by_label["31-90"] == 80
    assert by_label["91-180"] == 200
    assert by_label["181-365"] == 400
    assert by_label["366+"] == 1000


def test_age_buckets_v2_partial_keys():
    """Only some v2 keys present — output should only contain those labels."""
    raw = {"age_buckets": {">365d": 500}}
    result = normalize_summary(raw)
    assert len(result["age_buckets"]) == 1
    assert result["age_buckets"][0]["label"] == "366+"
    assert result["age_buckets"][0]["file_count"] == 500


def test_age_buckets_already_canonical_unchanged():
    """compute_scan_summary list shape — should pass through unchanged."""
    raw = {"age_buckets": [
        {"label": "0-30", "days_min": 0, "days_max": 30,
         "file_count": 100, "total_size": 1024},
        {"label": "366+", "days_min": 366, "days_max": None,
         "file_count": 1000, "total_size": 99999},
    ]}
    result = normalize_summary(raw)
    assert result["age_buckets"] == raw["age_buckets"]


def test_normalize_summary_is_idempotent():
    """Normalising a normalised dict gives the same result."""
    raw = {"age_buckets": {"<30d": 100, ">365d": 200}}
    once = normalize_summary(raw)
    twice = normalize_summary(once)
    assert once == twice


# ---------------------------------------------------------------------------
# size_buckets — dict (v2) → list (canonical)
# ---------------------------------------------------------------------------


def test_size_buckets_v2_dict_converted_to_list():
    raw = {"size_buckets": {
        "<1MB": 5000, "1-10MB": 1000, "10-100MB": 500,
        "100-1GB": 100, ">1GB": 10,
    }}
    result = normalize_summary(raw)
    assert isinstance(result["size_buckets"], list)
    labels = [b["label"] for b in result["size_buckets"]]
    assert labels == ["tiny", "small", "medium", "large", "xlarge"]
    counts = {b["label"]: b["file_count"] for b in result["size_buckets"]}
    assert counts["tiny"] == 5000
    assert counts["xlarge"] == 10


def test_size_buckets_already_canonical_unchanged():
    raw = {"size_buckets": [
        {"label": "tiny", "min_bytes": 0, "max_bytes": 1048575,
         "file_count": 5000, "total_size": 1024},
    ]}
    result = normalize_summary(raw)
    assert result["size_buckets"] == raw["size_buckets"]


# ---------------------------------------------------------------------------
# Mixed input — some keys v2, some keys canonical
# ---------------------------------------------------------------------------


def test_mixed_shapes_normalised_independently():
    raw = {
        "age_buckets": {"<30d": 1},                 # v2 dict
        "size_buckets": [{"label": "tiny",          # canonical list
                          "min_bytes": 0,
                          "max_bytes": 1048575,
                          "file_count": 5,
                          "total_size": 10}],
        "total_files": 100,
    }
    result = normalize_summary(raw)
    assert isinstance(result["age_buckets"], list)
    assert isinstance(result["size_buckets"], list)
    assert result["total_files"] == 100


def test_original_dict_not_mutated():
    raw = {"age_buckets": {"<30d": 100}}
    raw_before = dict(raw)
    raw_before["age_buckets"] = dict(raw["age_buckets"])
    normalize_summary(raw)
    # The original dict should be unchanged
    assert raw["age_buckets"] == raw_before["age_buckets"]
