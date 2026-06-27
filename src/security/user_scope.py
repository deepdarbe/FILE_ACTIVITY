"""Per-user data scoping for role-based access (Wave 10 #308).

Reads jwt_user from request.state (set by JWT middleware in api.py).
Provides SQL fragment + params for WHERE-clause injection into owner-
bearing queries.
"""
from __future__ import annotations
from typing import Any, Optional


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
