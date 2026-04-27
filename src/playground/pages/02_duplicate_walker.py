"""Duplicate-walker page (issue #75).

Reads ``duplicate_hash_groups`` (content-hash duplicate detection,
issue #35) plus its ``duplicate_hash_members`` rows, joined back to
``scanned_files`` for owner / mtime context.

Workflow:
1. Pick a group from a dropdown sorted descending by ``waste_size``.
2. Group members render in a table sortable by mtime / owner / path.
3. Total wasted space is shown as a metric callout.

All queries are READ-ONLY.
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
    import streamlit as st

    from src.playground import auth as _auth
    from src.playground import data_access as _da

    _auth.require_auth()
    st.title("Duplicate Walker — Icerik kopyalari")
    st.caption(
        "`duplicate_hash_groups` icerik-tabanli kopya gruplari uzerinde "
        "drill-down. Salt-okunur — silme/birlestirme islemleri "
        "dashboard > Reports > Duplicates uzerinden yapilir."
    )

    conn = _da.get_sqlite_conn()

    # Pick most recent scan that actually has duplicate groups recorded.
    scans_df = pd.read_sql_query(
        """
        SELECT scan_id,
               COUNT(*) AS group_count,
               SUM(waste_size) AS total_waste
        FROM duplicate_hash_groups
        GROUP BY scan_id
        ORDER BY scan_id DESC
        """,
        conn,
    )
    if scans_df.empty:
        st.info(
            "`duplicate_hash_groups` tablosu bos. Once dashboard "
            "uzerinden bir 'Content Duplicates' tarama calistirin."
        )
        return

    scan_options = scans_df["scan_id"].tolist()
    scan_id = st.selectbox(
        "Scan",
        scan_options,
        format_func=lambda s: (
            f"scan #{s} — "
            f"{int(scans_df.loc[scans_df.scan_id == s, 'group_count'].iloc[0])} grup, "
            f"{int(scans_df.loc[scans_df.scan_id == s, 'total_waste'].iloc[0]) / (1024**3):.2f} GB israf"
        ),
    )

    # Total waste callout for the chosen scan.
    total_waste = int(
        scans_df.loc[scans_df.scan_id == scan_id, "total_waste"].iloc[0] or 0
    )
    total_groups = int(
        scans_df.loc[scans_df.scan_id == scan_id, "group_count"].iloc[0] or 0
    )
    callout_a, callout_b = st.columns(2)
    callout_a.metric("Kopya grup sayisi", f"{total_groups:,}")
    callout_b.metric("Toplam israf alan", f"{total_waste / (1024 ** 3):.2f} GB")

    # Top groups for the scan, sorted by group size (file_count) DESC,
    # then waste DESC. Limit so the dropdown stays usable.
    groups_df = pd.read_sql_query(
        """
        SELECT id, content_hash, file_size, file_count, waste_size
        FROM duplicate_hash_groups
        WHERE scan_id = ?
        ORDER BY file_count DESC, waste_size DESC
        LIMIT 500
        """,
        conn,
        params=(int(scan_id),),
    )
    if groups_df.empty:
        st.info("Bu scan'de kopya grubu yok.")
        return

    def _label(row: "pd.Series") -> str:
        return (
            f"{row['file_count']} dosya × {row['file_size'] / (1024 ** 2):.1f} MB "
            f"= {row['waste_size'] / (1024 ** 2):.1f} MB israf "
            f"({row['content_hash'][:12]}…)"
        )
    groups_df["label"] = groups_df.apply(_label, axis=1)

    selected = st.selectbox(
        "Kopya grubu",
        options=groups_df["id"].tolist(),
        format_func=lambda gid: groups_df.loc[
            groups_df["id"] == gid, "label"
        ].iloc[0],
    )

    # Member listing — JOIN to scanned_files for owner/mtime; LEFT JOIN
    # because a member row may pre-date the latest scanned_files write.
    members_df = pd.read_sql_query(
        """
        SELECT
            m.file_path,
            sf.owner,
            sf.last_modify_time,
            sf.last_access_time,
            sf.file_size
        FROM duplicate_hash_members m
        LEFT JOIN scanned_files sf ON sf.id = m.file_id
        WHERE m.group_id = ?
        ORDER BY sf.last_modify_time DESC
        """,
        conn,
        params=(int(selected),),
    )

    st.subheader(f"Grup #{selected} — {len(members_df)} uye")
    st.dataframe(
        members_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "file_path": st.column_config.TextColumn("Dosya yolu"),
            "owner": st.column_config.TextColumn("Sahip"),
            "last_modify_time": st.column_config.TextColumn("Son degisiklik"),
            "last_access_time": st.column_config.TextColumn("Son erisim"),
            "file_size": st.column_config.NumberColumn("Boyut (byte)"),
        },
    )


if os.environ.get("STREAMLIT_RUN_PLAYGROUND_TESTS_ONLY") != "1":
    _render()
