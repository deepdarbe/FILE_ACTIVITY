"""Streamlit analytics playground for FILE_ACTIVITY (issue #75).

A standalone, READ-ONLY admin exploration UI. Runs as a separate process
from the FastAPI dashboard. Does NOT replace the dashboard — the REST
contract that the PowerShell module + MCP server depend on is untouched.

Optional dependency tree (see ``requirements-playground.txt``):
    - streamlit
    - plotly
    - pandas

Run with:
    streamlit run src/playground/app.py --server.port 8086
"""

from __future__ import annotations
