"""Audit timeline page (issue #75).

Filters ``file_audit_events`` by:
* actor (free-text match against ``username``)
* event_type (multi-select drawn from distinct DB values)
* date range

Plots a daily count line chart and lists the most recent matching
events. Read-only.
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta
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
    st.title("Audit Timeline — Denetim olaylari")
    st.caption(
        "`file_audit_events` uzerinde aktor, olay tipi ve tarih "
        "filtresi. Salt-okunur — herhangi bir olay degistirilmez."
    )

    conn = _da.get_sqlite_conn()

    # Distinct event types — small lookup, OK to fetch every render.
    event_types_df = pd.read_sql_query(
        "SELECT DISTINCT event_type FROM file_audit_events ORDER BY event_type",
        conn,
    )
    event_types = event_types_df["event_type"].dropna().tolist()

    today = date.today()
    default_from = today - timedelta(days=30)

    col_a, col_b, col_c = st.columns([2, 2, 2])
    with col_a:
        actor = st.text_input("Aktor (username icerir)", value="")
    with col_b:
        chosen_types = st.multiselect(
            "Olay tipi", options=event_types, default=event_types[:5]
        )
    with col_c:
        date_range = st.date_input(
            "Tarih araligi",
            value=(default_from, today),
        )

    # Streamlit returns a tuple if both bounds are filled; a single date
    # if the user picked one. Normalise.
    if isinstance(date_range, tuple):
        date_from, date_to = date_range
    else:
        date_from = date_to = date_range
    if date_from is None:
        date_from = default_from
    if date_to is None:
        date_to = today

    where = ["event_time >= ?", "event_time <= ?"]
    params: list = [
        date_from.strftime("%Y-%m-%d 00:00:00"),
        date_to.strftime("%Y-%m-%d 23:59:59"),
    ]
    if actor.strip():
        where.append("username LIKE ?")
        params.append(f"%{actor.strip()}%")
    if chosen_types:
        placeholders = ",".join("?" * len(chosen_types))
        where.append(f"event_type IN ({placeholders})")
        params.extend(chosen_types)

    where_sql = " AND ".join(where)

    # Daily counts — DuckDB if available (faster on big tables).
    daily_sql = f"""
        SELECT substr(event_time, 1, 10) AS day,
               COUNT(*) AS event_count
        FROM file_audit_events
        WHERE {where_sql}
        GROUP BY day
        ORDER BY day
    """
    duck = _da.get_duckdb_conn()
    if duck is not None:
        rows = duck.execute(
            daily_sql.replace("FROM file_audit_events",
                              "FROM sqlite_db.file_audit_events"),
            params,
        ).fetchall()
        daily_df = pd.DataFrame(rows, columns=["day", "event_count"])
        engine_label = "DuckDB"
    else:
        daily_df = pd.read_sql_query(daily_sql, conn, params=params)
        engine_label = "SQLite"

    if daily_df.empty:
        st.info("Bu kriterlerde audit olayi yok.")
        st.caption(f"Motor: {engine_label}")
        return

    daily_df["day"] = pd.to_datetime(daily_df["day"], errors="coerce")
    daily_df = daily_df.sort_values("day")

    total = int(daily_df["event_count"].sum())
    st.metric("Toplam olay", f"{total:,}")

    fig = px.line(
        daily_df,
        x="day",
        y="event_count",
        markers=True,
        labels={"day": "Tarih", "event_count": "Olay sayisi"},
        title="Gunluk olay sayisi",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Recent events table (last 200 of the same filter).
    recent_sql = f"""
        SELECT event_time, event_type, username, file_path, details
        FROM file_audit_events
        WHERE {where_sql}
        ORDER BY event_time DESC
        LIMIT 200
    """
    recent_df = pd.read_sql_query(recent_sql, conn, params=params)
    st.subheader("Son olaylar")
    st.dataframe(recent_df, use_container_width=True, hide_index=True)
    st.caption(f"Motor: {engine_label}")


if os.environ.get("STREAMLIT_RUN_PLAYGROUND_TESTS_ONLY") != "1":
    _render()
