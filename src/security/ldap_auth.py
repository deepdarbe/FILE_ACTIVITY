"""LDAP credential validation for per-user login (Wave 10, #307).

Authenticates a user by binding to AD with their own credentials (not the
service account). Returns user info on success, None on failure.

Separate from ADLookup which uses service-account bind for info queries.
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def _escape_ldap(value: str) -> str:
    """Escape special characters for LDAP filter injection protection."""
    # RFC 4515 escaping
    escape_map = {
        '\\': r'\5c',
        '*': r'\2a',
        '(': r'\28',
        ')': r'\29',
        '\x00': r'\00',
    }
    for char, escaped in escape_map.items():
        value = value.replace(char, escaped)
    return value


class LDAPAuthenticator:
    """Validates user credentials against Active Directory via ldap3 bind."""

    def __init__(self, config: dict):
        """config: the full app config dict (reads active_directory section)."""
        self._ad_cfg = config.get('active_directory', {})
        self._auth_cfg = config.get('dashboard', {}).get('auth', {})
        self._enabled = self._ad_cfg.get('enabled', False)
        self._admin_groups = self._auth_cfg.get('admin_groups', [])
        self._manager_groups = self._auth_cfg.get('manager_groups', [])

    def _escape_ldap(self, value: str) -> str:
        return _escape_ldap(value)

    def _normalize_username(self, username: str) -> str:
        """Strip DOMAIN\\ prefix and @domain suffix to get sAMAccountName."""
        if '\\' in username:
            username = username.split('\\', 1)[1]
        if '@' in username:
            username = username.split('@', 1)[0]
        return username.strip()

    def authenticate(self, username: str, password: str) -> Optional[dict]:
        """Bind to AD as the user and return user info, or None on failure.

        Returns dict with: username, display_name, email, groups, role
        Returns None if: ldap3 not available, AD disabled, bad credentials, error.
        """
        if not self._enabled:
            logger.debug("LDAP auth disabled (active_directory.enabled=false)")
            return None

        try:
            import ldap3  # noqa: F401
            from ldap3 import Server, Connection, ALL, SUBTREE  # noqa: F401
            from ldap3.core.exceptions import LDAPException
        except ImportError:
            logger.warning("ldap3 not installed — LDAP auth unavailable")
            return None

        username = self._normalize_username(username)
        safe_username = self._escape_ldap(username)

        ad_server = self._ad_cfg.get('server', '')
        bind_dn = self._ad_cfg.get('bind_dn', '')
        bind_password = self._ad_cfg.get('bind_password', '')
        base_dn = self._ad_cfg.get('base_dn', '')
        user_filter_tpl = self._ad_cfg.get('user_filter', '(sAMAccountName={username})')
        email_attr = self._ad_cfg.get('email_attribute', 'mail')
        name_attr = self._ad_cfg.get('name_attribute', 'displayName')
        timeout = self._ad_cfg.get('timeout_seconds', 5)

        try:
            from ldap3 import Server as Srv, Connection as Conn, ALL as LDAP_ALL, SUBTREE as ST
            server = Srv(ad_server, get_info=LDAP_ALL, connect_timeout=timeout)

            # Step 1: service-account bind to find the user's DN
            with Conn(server, user=bind_dn, password=bind_password,
                      auto_bind=True, receive_timeout=timeout) as svc_conn:
                user_filter = user_filter_tpl.replace('{username}', safe_username)
                svc_conn.search(
                    search_base=base_dn,
                    search_filter=user_filter,
                    search_scope=ST,
                    attributes=['distinguishedName', email_attr, name_attr, 'memberOf'],
                )
                if not svc_conn.entries:
                    logger.info("LDAP auth: user not found: %s", username)
                    return None

                entry = svc_conn.entries[0]
                user_dn = str(entry.distinguishedName)
                display_name = str(getattr(entry, name_attr, username) or username)
                email = str(getattr(entry, email_attr, '') or '')
                member_of_raw = entry.memberOf.values if hasattr(entry, 'memberOf') else []

            # Step 2: re-bind as the user to validate credentials
            try:
                with Conn(server, user=user_dn, password=password,
                          auto_bind=True, receive_timeout=timeout):
                    pass  # successful bind = valid credentials
            except LDAPException:
                logger.info("LDAP auth: invalid credentials for %s", username)
                return None

            # Parse group CN names from memberOf DNs
            groups = []
            for dn in member_of_raw:
                dn_str = str(dn)
                # Extract CN from "CN=GroupName,OU=..."
                for part in dn_str.split(','):
                    part = part.strip()
                    if part.upper().startswith('CN='):
                        groups.append(part[3:])
                        break

            return {
                'username': username,
                'display_name': display_name,
                'email': email,
                'groups': groups,
            }

        except Exception as exc:
            logger.error("LDAP auth error for %s: %s", username, exc)
            return None
