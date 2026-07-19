"""Single source of truth for AI-insight archive predicates.

The interactive "Uygula" preview (``archive_by_insight`` in
``src/dashboard/api.py``) and the background bulk-archive worker
(``src/archiver/archive_job_worker.py``) must select *exactly* the same set of
files, or the preview count and what the worker archives would drift. Both call
:func:`insight_where` so the ``WHERE`` fragment lives in one place.

Each fragment is meant to be AND-ed after ``source_id=? AND scan_id=?`` and is
safe to interpolate directly (no user input — the fragments are constants; the
only bound params are the ``extra`` list returned alongside).
"""

from __future__ import annotations

# Insight types the bulk background job can archive. ``duplicates`` is
# intentionally excluded: it is a GROUP BY / keep-newest computation, not a
# single-row predicate, so it cannot drive a keyset (id-cursor) scan and cannot
# be made resumable without first materialising the victim set.
SUPPORTED_BULK_INSIGHTS = frozenset(
    {"stale_1year", "stale_3year", "temp_files", "large_files"}
)


def insight_where(insight_type: str) -> tuple[str, list]:
    """Return ``(where_fragment, extra_params)`` for a bulk-archivable insight.

    The fragment is AND-ed after ``source_id=? AND scan_id=?``. ``extra_params``
    is the list of bound parameters the fragment needs (currently always empty;
    kept for forward-compatibility with parameterised predicates).

    Raises:
        ValueError: for an unknown / non-bulk-archivable insight type.
    """
    if insight_type == "stale_1year":
        return ("julianday('now') - julianday(last_access_time) > 365", [])
    if insight_type == "stale_3year":
        return ("julianday('now') - julianday(last_access_time) > 1095", [])
    if insight_type == "temp_files":
        return (
            "(LOWER(extension) IN ('tmp','temp','bak','old','log','cache') "
            "OR file_name LIKE '~$%' OR file_name LIKE '%.tmp')",
            [],
        )
    if insight_type == "large_files":
        return (
            "file_size > 104857600 "
            "AND julianday('now') - julianday(last_access_time) > 180",
            [],
        )
    raise ValueError(
        f"insight_type not supported for bulk archive: {insight_type!r}"
    )
