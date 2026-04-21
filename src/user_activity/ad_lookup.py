"""Active Directory lookup - kullanici e-posta + display name.

ldap3 ile AD/LDAP'a sorgu atar, kullanici adindan e-posta ve tam adi
cikarir. Sonuclari SQLite'ta TTL bazli cache'ler; AD indisponible iken
ya da sorgu timeout olursa eski cache degerini doner.

Konfigurasyon (config.yaml):
    active_directory:
      enabled: true
      server: "ldap://dc.example.com"   # ldaps:// icin port 636 kullanin
      bind_dn: "CN=svc_fileactivity,CN=Users,DC=example,DC=com"
      bind_password: ""                  # veya AD_BIND_PASSWORD env var
      base_dn: "DC=example,DC=com"
      user_filter: "(sAMAccountName={username})"
      email_attribute: "mail"
      name_attribute: "displayName"
      cache_ttl_minutes: 60
      timeout_seconds: 5

enabled=false ise modul no-op calisir, lookup() None doner.
"""

import logging
import os
import threading
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("file_activity.ad_lookup")

try:
    from ldap3 import Server, Connection, SAFE_SYNC, ALL as LDAP3_ALL  # type: ignore
    _HAVE_LDAP3 = True
except ImportError:
    Server = None
    Connection = None
    _HAVE_LDAP3 = False


class ADLookup:
    """AD/LDAP uzerinden kullanici bilgisi cozumleyici."""

    def __init__(self, db, config: dict):
        self.db = db
        ad_conf = config.get("active_directory", {}) or {}
        self.enabled = bool(ad_conf.get("enabled", False))
        self.server = ad_conf.get("server", "").strip()
        self.bind_dn = ad_conf.get("bind_dn", "").strip()
        # Sifre oncelik: env var > config
        self.bind_password = (
            os.environ.get("AD_BIND_PASSWORD")
            or ad_conf.get("bind_password", "")
        )
        self.base_dn = ad_conf.get("base_dn", "").strip()
        self.user_filter = ad_conf.get("user_filter", "(sAMAccountName={username})")
        self.email_attr = ad_conf.get("email_attribute", "mail")
        self.name_attr = ad_conf.get("name_attribute", "displayName")
        self.cache_ttl = timedelta(minutes=int(ad_conf.get("cache_ttl_minutes", 60)))
        self.timeout = int(ad_conf.get("timeout_seconds", 5))
        self._lock = threading.Lock()
        self._ensure_cache_table()

        self._init_error: Optional[str] = None
        if not _HAVE_LDAP3:
            self._init_error = "ldap3 paketi yuklenmemis"
        elif not self.enabled:
            self._init_error = "config.active_directory.enabled=false"
        elif not self.server or not self.base_dn:
            self._init_error = "server veya base_dn eksik"

        if self._init_error:
            logger.info("ADLookup devre disi: %s", self._init_error)

    @property
    def available(self) -> bool:
        return self._init_error is None

    def _ensure_cache_table(self):
        """SQLite'ta ad_user_cache tablosunu olustur."""
        with self.db.get_cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ad_user_cache (
                    username TEXT PRIMARY KEY,
                    email TEXT,
                    display_name TEXT,
                    found INTEGER NOT NULL DEFAULT 0,
                    looked_up_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

    def _normalize_username(self, username: str) -> str:
        """DOMAIN\\user veya user@domain formatlarindan sadece sam'i cikar."""
        if not username:
            return ""
        if "\\" in username:
            username = username.split("\\", 1)[1]
        if "@" in username:
            username = username.split("@", 1)[0]
        return username.strip().lower()

    def _get_cached(self, username: str) -> Optional[dict]:
        with self.db.get_cursor() as cur:
            cur.execute(
                "SELECT email, display_name, found, looked_up_at "
                "FROM ad_user_cache WHERE username = ?",
                (username,),
            )
            row = cur.fetchone()
        if not row:
            return None
        try:
            ts = datetime.strptime(row["looked_up_at"], "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            ts = None
        return {
            "email": row["email"],
            "display_name": row["display_name"],
            "found": bool(row["found"]),
            "looked_up_at": ts,
            "stale": ts is None or (datetime.now() - ts) > self.cache_ttl,
        }

    def _set_cache(self, username: str, email: Optional[str],
                    display_name: Optional[str], found: bool):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.db.get_cursor() as cur:
            cur.execute(
                """
                INSERT INTO ad_user_cache (username, email, display_name, found, looked_up_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                    email = excluded.email,
                    display_name = excluded.display_name,
                    found = excluded.found,
                    looked_up_at = excluded.looked_up_at
                """,
                (username, email, display_name, 1 if found else 0, now),
            )

    def _query_ad(self, username: str) -> Optional[dict]:
        """AD'ye canli sorgu. Basarisiz olursa None."""
        if not self.available:
            return None
        try:
            server = Server(self.server, get_info=LDAP3_ALL, connect_timeout=self.timeout)
            conn = Connection(
                server,
                user=self.bind_dn,
                password=self.bind_password,
                client_strategy=SAFE_SYNC,
                auto_bind=True,
                receive_timeout=self.timeout,
            )
            search_filter = self.user_filter.format(
                username=self._escape_ldap(username)
            )
            status, result, response, _req = conn.search(
                search_base=self.base_dn,
                search_filter=search_filter,
                attributes=[self.email_attr, self.name_attr],
            )
            try:
                conn.unbind()
            except Exception:
                pass
            if not status or not response:
                return {"email": None, "display_name": None, "found": False}
            entry = response[0].get("attributes") or {}
            email = entry.get(self.email_attr)
            name = entry.get(self.name_attr)
            if isinstance(email, list):
                email = email[0] if email else None
            if isinstance(name, list):
                name = name[0] if name else None
            return {"email": email, "display_name": name, "found": bool(email or name)}
        except Exception as e:
            logger.warning("AD sorgusu basarisiz (%s): %s", username, e)
            return None

    @staticmethod
    def _escape_ldap(value: str) -> str:
        """LDAP filter injection koruma."""
        if not value:
            return ""
        out = []
        for ch in value:
            if ch in ("*", "(", ")", "\\", "\0"):
                out.append(f"\\{ord(ch):02x}")
            else:
                out.append(ch)
        return "".join(out)

    def lookup(self, username: str, force_refresh: bool = False) -> Optional[dict]:
        """Kullanici icin e-posta + display name doner.

        Sonuc: {"username", "email", "display_name", "found", "source"}
        source: "cache" | "live" | "stale-cache" | None (AD devre disi)
        """
        uname = self._normalize_username(username)
        if not uname:
            return None

        with self._lock:
            cached = self._get_cached(uname)

            if cached and not force_refresh and not cached["stale"]:
                return {
                    "username": uname,
                    "email": cached["email"],
                    "display_name": cached["display_name"],
                    "found": cached["found"],
                    "source": "cache",
                }

            if not self.available:
                # AD devre disi; cache varsa onu don, yoksa None
                if cached:
                    return {
                        "username": uname,
                        "email": cached["email"],
                        "display_name": cached["display_name"],
                        "found": cached["found"],
                        "source": "stale-cache",
                    }
                return None

            live = self._query_ad(uname)
            if live is None:
                # AD erisilmez, eski cache varsa onu don
                if cached:
                    return {
                        "username": uname,
                        "email": cached["email"],
                        "display_name": cached["display_name"],
                        "found": cached["found"],
                        "source": "stale-cache",
                    }
                return None

            self._set_cache(uname, live["email"], live["display_name"], live["found"])
            return {
                "username": uname,
                "email": live["email"],
                "display_name": live["display_name"],
                "found": live["found"],
                "source": "live",
            }

    def health(self) -> dict:
        info = {
            "enabled": self.enabled,
            "ldap3_installed": _HAVE_LDAP3,
            "available": self.available,
            "server": self.server or None,
            "base_dn": self.base_dn or None,
        }
        if self._init_error:
            info["init_error"] = self._init_error
        # Canli test: bind dene
        if self.available:
            try:
                server = Server(self.server, connect_timeout=self.timeout)
                conn = Connection(
                    server,
                    user=self.bind_dn,
                    password=self.bind_password,
                    client_strategy=SAFE_SYNC,
                    auto_bind=True,
                    receive_timeout=self.timeout,
                )
                info["bind_ok"] = True
                try:
                    conn.unbind()
                except Exception:
                    pass
            except Exception as e:
                info["bind_ok"] = False
                info["bind_error"] = str(e)
        return info
