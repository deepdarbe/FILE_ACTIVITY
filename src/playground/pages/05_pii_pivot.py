"""PII pivot page (issue #75).

Pivots ``pii_findings`` into a scan_id × pattern_name matrix of hit
counts, renders a heatmap, and lets the operator click into a
specific cell to see redacted sample snippets.

Read-only — snippets stored in ``pii_findings`` are already masked
by the PII engine (issue #58), so no raw PII leaves this page.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

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
    st.title("PII Pivot — Tarama × Desen heatmap")
    st.caption(
        "`pii_findings` tablosu uzerinde scan × pattern_name pivot. "
        "Snippet'lar engine tarafindan zaten redacted — ham PII gosterilmez."
    )

    conn = _da.get_sqlite_conn()

    findings_df = pd.read_sql_query(
        """
        SELECT scan_id, pattern_name, hit_count
        FROM pii_findings
        """,
        conn,
    )
    if findings_df.empty:
        st.info(
            "`pii_findings` tablosu bos. Compliance > PII tarama "
            "calistirildiginda bu sayfa dolmaya baslar."
        )
        return

    # Pivot: rows=scan_id, cols=pattern_name, values=sum(hit_count).
    pivot = (
        findings_df.groupby(["scan_id", "pattern_name"])["hit_count"]
        .sum()
        .reset_index()
        .pivot(index="scan_id", columns="pattern_name", values="hit_count")
        .fillna(0)
        .astype(int)
        .sort_index(ascending=False)
    )

    st.subheader("Pivot tablosu")
    st.dataframe(pivot, use_container_width=True)

    # Heatmap. Plotly's imshow handles arbitrary-shape matrices and
    # is colorbar-friendly out of the box.
    if pivot.size:
        fig = px.imshow(
            pivot.values,
            labels=dict(x="Desen", y="Scan ID", color="Hit count"),
            x=list(pivot.columns),
            y=[str(s) for s in pivot.index],
            aspect="auto",
            color_continuous_scale="Reds",
            text_auto=True,
        )
        fig.update_layout(title="PII hit yogunlugu")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Drill-down — redacted ornek snippet'lar")

    col_a, col_b = st.columns(2)
    with col_a:
        scan_choice = st.selectbox(
            "Scan ID", options=list(pivot.index)
        )
    with col_b:
        pattern_choice = st.selectbox(
            "Desen",
            options=[c for c in pivot.columns if pivot.loc[scan_choice, c] > 0]
            or list(pivot.columns),
        )

    drill_df = pd.read_sql_query(
        """
        SELECT file_path, hit_count, sample_snippet, detected_at
        FROM pii_findings
        WHERE scan_id = ? AND pattern_name = ?
        ORDER BY hit_count DESC
        LIMIT 200
        """,
        conn,
        params=(int(scan_choice), str(pattern_choice)),
    )
    if drill_df.empty:
        st.info("Bu hucre icin kayit yok.")
        return

    st.dataframe(
        drill_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "file_path": st.column_config.TextColumn("Dosya yolu"),
            "hit_count": st.column_config.NumberColumn("Hit"),
            "sample_snippet": st.column_config.TextColumn("Redacted snippet"),
            "detected_at": st.column_config.TextColumn("Tespit"),
        },
    )


if os.environ.get("STREAMLIT_RUN_PLAYGROUND_TESTS_ONLY") != "1":
    _render()
