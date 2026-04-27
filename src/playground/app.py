"""Streamlit playground entry point (issue #75).

Run with:
    streamlit run src/playground/app.py --server.port 8086

This file is the landing page for the playground. The pages under
``src/playground/pages/`` are auto-discovered by Streamlit's native
multi-page layout (any ``NN_*.py`` file is shown as a sidebar item).

The app is READ-ONLY:
* SQLite is opened via ``sqlite3.connect("file:" + path + "?mode=ro",
  uri=True)``.
* DuckDB ATTACHes the SQLite file with ``READ_ONLY``.
* No write paths exist.

The dashboard FastAPI process and the existing REST contract are
untouched — the playground is a separate process you start on demand.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# When invoked as ``streamlit run src/playground/app.py`` Streamlit's
# script runner doesn't add the project root to ``sys.path``. We do it
# explicitly so ``from src.playground...`` works regardless of CWD.
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st  # noqa: E402  (sys.path mutation must precede)

from src.playground import auth as _auth  # noqa: E402
from src.playground import data_access as _da  # noqa: E402


st.set_page_config(
    page_title="FILE_ACTIVITY — Analytics Playground",
    page_icon=":bar_chart:",
    layout="wide",
)


def _sidebar_status() -> None:
    """Render DB + auth status in the sidebar so every page sees it."""
    with st.sidebar:
        st.header("Playground durumu")
        try:
            db_path = _da.get_db_path_str()
        except Exception as e:  # pragma: no cover - defensive
            db_path = f"(çözümlenemedi: {e})"
        st.caption(f"**DB:** `{db_path}`")
        st.caption(
            "**DuckDB:** "
            + ("kurulu" if _da.have_duckdb() else "yok (SQLite fallback)")
        )
        st.caption(
            "**Auth:** "
            + ("açık (bearer token)" if _auth.auth_enabled() else "KAPALI (dev)")
        )
        st.divider()
        st.caption(
            "Bu araç **salt-okunur** — INSERT/UPDATE/DELETE/ALTER yok. "
            "Gerçek operasyonlar için dashboard'u kullanın."
        )


def main() -> None:
    _auth.require_auth()
    _sidebar_status()

    st.title("FILE_ACTIVITY — Analytics Playground")
    st.caption("İç kullanım için ad-hoc keşif arayüzü. Salt-okunur.")

    st.markdown(
        """
Bu Streamlit uygulaması FILE_ACTIVITY veritabanına **salt-okunur**
bağlanır ve yöneticilere hızlı pivot/grafiklerle veri keşfi imkanı
sunar. **Dashboard'un yerine geçmez** — PowerShell modülü ve MCP
sunucusu hala FastAPI REST API'sini kullanır.

### Bu sayfada
- Veri tabanı yolu ve DuckDB durumu kenar çubuğunda.
- Aşağıdaki sayfalar `src/playground/pages/` altında otomatik
  keşfedilir.

### Sayfalar
1. **Cold Data** — yaş/boyut filtreleri, sahip/uzantı/dizin
   bazında bar grafik + CSV indir.
2. **Duplicate Walker** — `duplicate_hash_groups` üzerinde drill-down.
3. **Audit Timeline** — `file_audit_events` günlük dağılım + filtre.
4. **Retention What-If** — fnmatch + yaş eşik önizleme (sadece
   önizleme; gerçek apply dashboard üzerinden).
5. **PII Pivot** — `pii_findings` üzerinde scan × pattern heatmap.
        """
    )

    # Quick health check so a misconfigured DB path surfaces immediately
    # rather than on the first sub-page click.
    try:
        conn = _da.get_sqlite_conn()
        row = conn.execute("SELECT COUNT(*) FROM scanned_files").fetchone()
        st.success(
            f"SQLite hazır — `scanned_files` satır sayısı: **{row[0]:,}**"
        )
    except FileNotFoundError as e:
        st.warning(str(e))
    except Exception as e:
        st.error(f"SQLite bağlantısı kurulamadı: {e}")


# ``streamlit run`` invokes this module top-to-bottom, so we don't gate
# on ``__name__ == "__main__"`` — that would skip rendering. The
# function form keeps the linter happy and lets unit tests import the
# module without rendering anything.
if os.environ.get("STREAMLIT_RUN_PLAYGROUND_TESTS_ONLY") != "1":
    main()
