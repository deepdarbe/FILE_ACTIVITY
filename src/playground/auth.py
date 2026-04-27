"""Bearer-token middleware for the Streamlit playground (issue #75).

Streamlit doesn't expose the underlying Tornado request directly, so
this module implements a small token gate using two channels:

* ``?token=<X>`` query-string parameter (``st.query_params``).
* ``Authorization: Bearer <X>`` header (``st.context.headers`` in
  Streamlit >= 1.37; we degrade gracefully on older builds).

Behaviour:
* If ``FILEACTIVITY_PLAYGROUND_TOKEN`` is unset, the app runs in
  *dev mode* — no auth, but a red banner is shown so operators can
  spot a misconfigured production deployment.
* If set, every page must call :func:`require_auth` before rendering
  any data; mismatched / missing tokens trigger ``st.stop()``.
"""

from __future__ import annotations

import hmac
import os
from typing import Optional


_ENV_VAR = "FILEACTIVITY_PLAYGROUND_TOKEN"


def expected_token() -> str:
    """The token the operator configured, or ``""`` for dev mode."""
    return (os.environ.get(_ENV_VAR) or "").strip()


def auth_enabled() -> bool:
    return bool(expected_token())


def _read_query_token() -> Optional[str]:
    try:
        import streamlit as st
        # st.query_params is a Mapping-like object since Streamlit 1.30
        params = st.query_params
        # Newer API returns str or list[str] depending on duplicates
        val = params.get("token")
        if isinstance(val, list):
            val = val[0] if val else None
        return (val or "").strip() or None
    except Exception:
        return None


def _read_header_token() -> Optional[str]:
    """Try to read ``Authorization: Bearer <X>`` from the request.

    Streamlit >= 1.37 exposes ``st.context.headers``; older builds
    return ``None`` and we silently fall back to query-param-only
    auth (operators who care should pin a recent Streamlit anyway).
    """
    try:
        import streamlit as st
        ctx = getattr(st, "context", None)
        headers = getattr(ctx, "headers", None) if ctx is not None else None
        if not headers:
            return None
        # Mapping/case-insensitive lookup.
        for key in ("Authorization", "authorization"):
            val = headers.get(key)
            if val:
                break
        else:
            return None
        val = val.strip()
        if val.lower().startswith("bearer "):
            return val[7:].strip() or None
        return None
    except Exception:
        return None


def _provided_token() -> Optional[str]:
    return _read_query_token() or _read_header_token()


def require_auth() -> None:
    """Gate the current Streamlit page on the bearer token.

    Call this once at the top of every page (``app.py`` and every
    file in ``src/playground/pages/``). When auth is disabled it is
    a no-op aside from rendering a red banner.
    """
    import streamlit as st

    expected = expected_token()
    if not expected:
        st.error(
            "Auth devre dışı — prod'da set et (FILEACTIVITY_PLAYGROUND_TOKEN). "
            "Bu yapı yalnızca yerel/dev kullanım icindir."
        )
        return

    provided = _provided_token() or ""
    # Constant-time compare prevents timing oracles on the token.
    if not hmac.compare_digest(provided, expected):
        st.error(
            "Yetkisiz erişim. Doğru bearer token gerekli "
            "(`?token=...` veya `Authorization: Bearer ...`)."
        )
        st.stop()
