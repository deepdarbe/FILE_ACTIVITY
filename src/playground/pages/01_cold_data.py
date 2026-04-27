"""Cold-data exploration page (issue #75).

Lets the operator slide an *age* threshold (days since last access)
and a *size* threshold (minimum bytes), then group the resulting
files by:

* owner
* extension
* top-level directory (first path segment after the source root)

Renders a Plotly bar chart + a sortable Streamlit dataframe + a
download-CSV button.

Performance target: must render under 2 s on a 2.5M-row
``scanned_files`` table — relies on DuckDB's columnar projection. We
prefer DuckDB if it ATTACHed cleanly; fall back to SQLite otherwise.
"""

from __future__ import annotations

import os
import sys
from io import StringIO
from pathlib import Path

# Ensure project root on sys.path even when the page is loaded as a
# top-level module by the Streamlit page router.
_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _render() -> None:
    import pandas as pd
    import plotly.express as px
    import streamlit as st

    from src.playground import auth as _auth
    from src.playground import data_access as _da

    _auth.require_auth()
    st.title("Cold Data — Sogumus dosyalar")
    st.caption(
        "Belirlenen yastan eski ve minimum boyut esiginin uzerindeki "
        "dosyalar. Salt-okunur sorgu — hicbir dosya tasinmaz/silinmez."
    )

    col_a, col_b, col_c = st.columns(3)
    with col_a:
        age_days = st.select_slider(
            "Yas esigi (gun, son erisimden)",
            options=[180, 365, 1095],
            value=365,
        )
    with col_b:
        size_choice = st.select_slider(
            "Minimum dosya boyutu",
            options=[
                ("0 B", 0),
                ("1 MB", 1_048_576),
                ("10 MB", 10_485_760),
                ("100 MB", 104_857_600),
                ("1 GB", 1_073_741_824),
            ],
            value=("1 MB", 1_048_576),
            format_func=lambda x: x[0],
        )
        size_threshold = size_choice[1]
    with col_c:
        group_by = st.radio(
            "Grupla",
            options=["owner", "extension", "top_level_dir"],
            format_func=lambda x: {
                "owner": "Sahip",
                "extension": "Uzanti",
                "top_level_dir": "Üst-seviye dizin",
            }[x],
            horizontal=False,
        )

    # ── Build query ──────────────────────────────────────────
    # ``last_access_time`` is stored as ISO text. Cold = older than now - N days.
    # We compute the cutoff as a string so SQLite/DuckDB string compare works
    # without parsing the full timestamp.
    from datetime import datetime, timedelta
    cutoff = (datetime.now() - timedelta(days=int(age_days))).strftime("%Y-%m-%d %H:%M:%S")

    if group_by == "owner":
        group_expr = "COALESCE(owner, 'Bilinmiyor')"
    elif group_by == "extension":
        group_expr = "COALESCE(LOWER(extension), 'uzantisiz')"
    else:
        # Top-level directory: take the first segment of relative_path.
        # Both backslashes and forward slashes are possible (UNC vs POSIX),
        # so we normalise via REPLACE before SUBSTR/INSTR.
        group_expr = (
            "CASE "
            " WHEN INSTR(REPLACE(relative_path, CHAR(92), '/'), '/') = 0 "
            "  THEN '(kok)' "
            " ELSE SUBSTR(REPLACE(relative_path, CHAR(92), '/'), 1, "
            "             INSTR(REPLACE(relative_path, CHAR(92), '/'), '/') - 1) "
            "END"
        )

    sql = f"""
        SELECT {group_expr} AS bucket,
               COUNT(*) AS file_count,
               SUM(file_size) AS total_size
        FROM scanned_files
        WHERE file_size >= ?
          AND last_access_time IS NOT NULL
          AND last_access_time <= ?
        GROUP BY bucket
        ORDER BY total_size DESC
        LIMIT 50
    """
    params = [int(size_threshold), cutoff]

    # ── Execute on DuckDB if available, else SQLite ──────────
    duck = _da.get_duckdb_conn()
    df: "pd.DataFrame"
    engine_label: str
    try:
        if duck is not None:
            # DuckDB with sqlite_db ATTACHed — same SQL, different prefix.
            # CHAR(92) and INSTR are valid in both engines.
            duck_sql = sql.replace("FROM scanned_files",
                                   "FROM sqlite_db.scanned_files")
            rows = duck.execute(duck_sql, params).fetchall()
            engine_label = "DuckDB (read-only ATTACH)"
        else:
            sqlite_conn = _da.get_sqlite_conn()
            rows = sqlite_conn.execute(sql, params).fetchall()
            engine_label = "SQLite (read-only)"
    except Exception as e:
        st.error(f"Sorgu basarisiz: {e}")
        return

    df = pd.DataFrame(rows, columns=["bucket", "file_count", "total_size"])
    if df.empty:
        st.info(
            f"Bu kriterlerde sogumus dosya yok "
            f"(yas >= {age_days} gun, boyut >= {size_threshold:,} byte)."
        )
        st.caption(f"Motor: {engine_label}")
        return

    df["total_size_mb"] = (df["total_size"] / (1024 * 1024)).round(2)
    df = df.sort_values("total_size", ascending=False)

    st.caption(
        f"{len(df)} grup, toplam {int(df['file_count'].sum()):,} dosya, "
        f"{df['total_size'].sum() / (1024 ** 3):.2f} GB. Motor: {engine_label}"
    )

    fig = px.bar(
        df.head(20),
        x="bucket",
        y="total_size_mb",
        labels={"bucket": "Grup", "total_size_mb": "Toplam boyut (MB)"},
        title=f"En buyuk 20 cold-data grubu — {group_by}",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Detay tablosu")
    st.dataframe(
        df[["bucket", "file_count", "total_size", "total_size_mb"]],
        use_container_width=True,
        hide_index=True,
    )

    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)
    st.download_button(
        "CSV indir",
        data=csv_buf.getvalue(),
        file_name=f"cold_data_{group_by}_{age_days}d.csv",
        mime="text/csv",
    )


# Render only when actually launched by Streamlit. The
# ``STREAMLIT_RUN_PLAYGROUND_TESTS_ONLY`` opt-out lets pytest import
# this module to verify it parses without spinning up a Streamlit run.
if os.environ.get("STREAMLIT_RUN_PLAYGROUND_TESTS_ONLY") != "1":
    _render()
