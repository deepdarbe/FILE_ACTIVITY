"""Retention what-if preview page (issue #75).

Lets the operator type an fnmatch pattern (e.g. ``*.log`` or
``*/temp/*``) and an age threshold (days), then reports:

* matching file count
* total bytes / GB matched
* top 10 owners by file count

This is **preview only** — nothing is archived, deleted, or
otherwise mutated. The big yellow warning makes that explicit; real
applies happen in dashboard > Compliance > Retention Policies.
"""

from __future__ import annotations

import fnmatch
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _fnmatch_to_sql_like(pattern: str) -> str:
    """Translate an fnmatch glob to a SQL ``LIKE`` pattern.

    Only ``*`` and ``?`` are supported — character classes (``[abc]``)
    are not, because SQLite's LIKE doesn't support them. We escape
    LIKE's own metacharacters (``%``, ``_``) before substituting glob
    metacharacters in.
    """
    if not pattern:
        return "%"
    # Escape SQL LIKE metacharacters. We use backslash as the escape
    # char; the SQL we build below sets ESCAPE '\\'.
    escaped = re.sub(r"([\\%_])", r"\\\1", pattern)
    # fnmatch * -> SQL %, fnmatch ? -> SQL _
    sql_like = escaped.replace("*", "%").replace("?", "_")
    return sql_like


def _render() -> None:
    import pandas as pd
    import streamlit as st

    from src.playground import auth as _auth
    from src.playground import data_access as _da

    _auth.require_auth()
    st.title("Retention What-If — Yas/desen onizleme")

    st.warning(
        "**Bu sadece on izleme. Gercek apply icin dashboard > Compliance > "
        "Retention Policies.** Bu sayfadan hicbir dosya tasinmaz/silinmez."
    )

    col_a, col_b = st.columns(2)
    with col_a:
        pattern = st.text_input(
            "fnmatch deseni",
            value="*.log",
            help="Ornek: `*.log`, `*/temp/*`, `*.bak`. * ve ? destekli.",
        )
    with col_b:
        age_days = st.number_input(
            "Yas esigi (gun, son degisiklikten)",
            min_value=1, max_value=10_000, value=365, step=30,
        )

    cutoff = (datetime.now() - timedelta(days=int(age_days))).strftime("%Y-%m-%d %H:%M:%S")
    sql_like = _fnmatch_to_sql_like(pattern.strip())

    conn = _da.get_sqlite_conn()

    # We match on file_path (full path) so patterns like ``*/temp/*``
    # work. Use ESCAPE so the user can include a literal % or _ via
    # the regex above; SQLite default escape is none.
    summary = conn.execute(
        r"""
        SELECT COUNT(*) AS match_count,
               COALESCE(SUM(file_size), 0) AS total_size
        FROM scanned_files
        WHERE file_path LIKE ? ESCAPE '\'
          AND last_modify_time IS NOT NULL
          AND last_modify_time <= ?
        """,
        (sql_like, cutoff),
    ).fetchone()

    match_count = int(summary[0] or 0)
    total_size = int(summary[1] or 0)

    m1, m2 = st.columns(2)
    m1.metric("Eslesen dosya", f"{match_count:,}")
    m2.metric("Toplam boyut", f"{total_size / (1024 ** 3):.2f} GB")

    if match_count == 0:
        st.info("Eslesme yok. Deseni veya yas esigini gevsetin.")
        # Sanity check: warn if the user typed a pattern that fnmatch
        # itself wouldn't accept. We can't run fnmatch over 2.5M paths
        # cheaply, but we can at least validate the pattern parses.
        try:
            fnmatch.translate(pattern)
        except re.error as e:
            st.error(f"Desen gecersiz: {e}")
        return

    # Top owners — DuckDB if available (faster on the 2.5M-row table).
    owners_sql = r"""
        SELECT COALESCE(owner, 'Bilinmiyor') AS owner,
               COUNT(*) AS file_count,
               SUM(file_size) AS total_size
        FROM scanned_files
        WHERE file_path LIKE ? ESCAPE '\'
          AND last_modify_time IS NOT NULL
          AND last_modify_time <= ?
        GROUP BY owner
        ORDER BY file_count DESC
        LIMIT 10
    """
    duck = _da.get_duckdb_conn()
    if duck is not None:
        rows = duck.execute(
            owners_sql.replace("FROM scanned_files",
                               "FROM sqlite_db.scanned_files"),
            [sql_like, cutoff],
        ).fetchall()
        owners_df = pd.DataFrame(
            rows, columns=["owner", "file_count", "total_size"]
        )
    else:
        owners_df = pd.read_sql_query(
            owners_sql, conn, params=(sql_like, cutoff)
        )

    st.subheader("En kalabalik 10 sahip")
    owners_df["total_size_mb"] = (owners_df["total_size"] / (1024 ** 2)).round(2)
    st.dataframe(
        owners_df[["owner", "file_count", "total_size_mb"]],
        use_container_width=True,
        hide_index=True,
    )

    # Sample 25 matches so the operator can spot-check the pattern hit
    # what they expected without dumping 2 M rows into the browser.
    sample_df = pd.read_sql_query(
        r"""
        SELECT file_path, owner, last_modify_time, file_size
        FROM scanned_files
        WHERE file_path LIKE ? ESCAPE '\'
          AND last_modify_time IS NOT NULL
          AND last_modify_time <= ?
        ORDER BY last_modify_time ASC
        LIMIT 25
        """,
        conn,
        params=(sql_like, cutoff),
    )
    st.subheader("Ornek 25 eslesme")
    st.dataframe(sample_df, use_container_width=True, hide_index=True)


if os.environ.get("STREAMLIT_RUN_PLAYGROUND_TESTS_ONLY") != "1":
    _render()
