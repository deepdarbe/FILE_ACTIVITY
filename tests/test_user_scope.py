"""Tests for Wave 10 #308: per-user data scope."""
from unittest.mock import MagicMock


def _make_request(role=None, sub=None):
    req = MagicMock()
    if role is None:
        req.state.jwt_user = None
    else:
        # Use 'alice' as default only when sub is not explicitly provided
        actual_sub = sub if sub is not None else 'alice'
        req.state.jwt_user = {'role': role, 'sub': actual_sub}
    return req


class TestGetOwnerScope:
    def test_no_jwt_no_filter(self):
        from src.security.user_scope import get_owner_scope
        frag, params = get_owner_scope(_make_request(role=None))
        assert frag == '' and params == []

    def test_admin_no_filter(self):
        from src.security.user_scope import get_owner_scope
        frag, params = get_owner_scope(_make_request(role='admin', sub='boss'))
        assert frag == '' and params == []

    def test_manager_no_filter(self):
        from src.security.user_scope import get_owner_scope
        frag, params = get_owner_scope(_make_request(role='manager', sub='mgr'))
        assert frag == '' and params == []

    def test_viewer_scoped(self):
        from src.security.user_scope import get_owner_scope
        frag, params = get_owner_scope(_make_request(role='viewer', sub='alice'))
        assert 'LIKE' in frag
        assert params == ['%alice%']

    def test_viewer_empty_sub_no_filter(self):
        from src.security.user_scope import get_owner_scope
        frag, params = get_owner_scope(_make_request(role='viewer', sub=''))
        assert frag == '' and params == []


class TestScopeIsRestricted:
    def test_admin_not_restricted(self):
        from src.security.user_scope import scope_is_restricted
        assert not scope_is_restricted(_make_request(role='admin'))

    def test_viewer_is_restricted(self):
        from src.security.user_scope import scope_is_restricted
        assert scope_is_restricted(_make_request(role='viewer', sub='bob'))

    def test_no_jwt_not_restricted(self):
        from src.security.user_scope import scope_is_restricted
        assert not scope_is_restricted(_make_request(role=None))


class TestGetScopeUsername:
    """The in-memory companion to get_owner_scope, used by cached/aggregate
    report endpoints that can't inject a SQL LIKE fragment (#308 consistency)."""

    def test_admin_none(self):
        from src.security.user_scope import get_scope_username
        assert get_scope_username(_make_request(role='admin', sub='boss')) is None

    def test_manager_none(self):
        from src.security.user_scope import get_scope_username
        assert get_scope_username(_make_request(role='manager', sub='mgr')) is None

    def test_no_jwt_none(self):
        from src.security.user_scope import get_scope_username
        assert get_scope_username(_make_request(role=None)) is None

    def test_viewer_lowercased_username(self):
        from src.security.user_scope import get_scope_username
        assert get_scope_username(_make_request(role='viewer', sub='Alice')) == 'alice'

    def test_viewer_empty_sub_none(self):
        from src.security.user_scope import get_scope_username
        assert get_scope_username(_make_request(role='viewer', sub='')) is None

    def test_username_matches_like_semantics(self):
        """The returned value mirrors '%username%' LIKE: substring, ci."""
        from src.security.user_scope import get_scope_username
        user = get_scope_username(_make_request(role='viewer', sub='alice'))
        # owner 'CORP\\Alice' contains 'alice' case-insensitively → match
        assert user in 'corp\\alice'.lower()
