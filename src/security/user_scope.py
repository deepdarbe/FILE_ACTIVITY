"""Per-user data scoping for role-based access (Wave 10 #308).

Reads jwt_user from request.state (set by JWT middleware in api.py).
Provides SQL fragment + params for WHERE-clause injection into owner-
bearing queries.
"""
from __future__ import annotations
from typing import Any


def get_owner_scope(request: Any) -> tuple[str, list]:
    """Return (sql_fragment, params) for owner filtering.

    Returns:
        ('', [])  — no filter (admin/manager/no-auth)
        ('AND owner LIKE ?', ['%alice%'])  — viewer-scoped
    """
    jwt_user = getattr(getattr(request, 'state', None), 'jwt_user', None)
    if jwt_user is None:
        return ('', [])
    role = jwt_user.get('role', 'viewer')
    if role in ('admin', 'manager'):
        return ('', [])
    username = jwt_user.get('sub', '')
    if not username:
        return ('', [])
    return ('AND owner LIKE ?', [f'%{username}%'])


def scope_is_restricted(request: Any) -> bool:
    """True iff this request sees only its own files."""
    frag, _ = get_owner_scope(request)
    return bool(frag)


def get_scope_username(request: Any) -> str | None:
    """Return the lowercased username a viewer is scoped to, or None.

    Companion to :func:`get_owner_scope` for endpoints that cannot inject a
    SQL ``owner LIKE ?`` fragment — cached full-list reports and aggregate
    reports — and must instead filter their in-memory result set. None means
    the request is unrestricted (admin / manager / no-auth) and should see
    every owner. A non-None value mirrors the ``'%username%'`` LIKE semantics:
    match when ``username in (owner or '').lower()`` (SQLite LIKE is
    case-insensitive for ASCII, so we lowercase both sides).
    """
    frag, params = get_owner_scope(request)
    if not frag or not params:
        return None
    # get_owner_scope builds ['%username%']; strip the LIKE wildcards back off.
    return str(params[0]).strip('%').lower() or None
