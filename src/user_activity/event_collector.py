"""Windows Event Log Kolektörü.

Windows Security Event Log'dan dosya erişim olaylarını toplar.

Desteklenen Event ID'ler:
  - 4663: Dosya sistemi nesnelerine erişim (NTFS Audit)
  - 5145: Ağ paylaşım nesnelerine erişim (SMB Audit)
  - 4656: Dosya handle istendi
  - 4660: Nesne silindi

Gereksinimler:
  - Yönetici yetkisi (Event Log okuma)
  - Audit Policy aktif olmalı:
    auditpol /set /subcategory:"File System" /success:enable
    auditpol /set /subcategory:"File Share" /success:enable
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("file_activity.user_activity.collector")

# Access type mapping
ACCESS_MASK_MAP = {
    0x1: "read",        # ReadData / ListDirectory
    0x2: "write",       # WriteData / AddFile
    0x4: "append",      # AppendData / AddSubdirectory
    0x6: "write",       # WriteData + AppendData
    0x10: "execute",    # Execute
    0x20: "read",       # ReadAttributes
    0x40: "write",      # WriteAttributes
    0x80: "read",       # ReadExtendedAttributes
    0x100: "write",     # WriteExtendedAttributes
    0x10000: "delete",  # DELETE
    0x20000: "read",    # READ_CONTROL
    0x40000: "write",   # WRITE_DAC
    0x80000: "write",   # WRITE_OWNER
}


def _parse_access_mask(mask_hex: str) -> str:
    """Access mask'tan erişim türünü çöz."""
    try:
        mask = int(mask_hex, 16) if isinstance(mask_hex, str) else int(mask_hex)
    except (ValueError, TypeError):
        return "unknown"

    if mask & 0x10000:
        return "delete"
    if mask & (0x2 | 0x4 | 0x40 | 0x100 | 0x40000 | 0x80000):
        return "write"
    if mask & (0x1 | 0x20 | 0x80 | 0x20000):
        return "read"
    return "other"


class EventCollector:
    """Windows Event Log'dan dosya erişim olaylarını toplar."""

    def __init__(self, db, config: dict):
        self.db = db
        self.config = config.get("user_activity", {})
        self.batch_size = self.config.get("batch_size", 500)
        self.event_ids = self.config.get("event_ids", [4663, 5145, 4660, 4656])
        self.exclude_users = set(self.config.get("exclude_users", [
            "SYSTEM", "LOCAL SERVICE", "NETWORK SERVICE",
            "DWM-1", "DWM-2", "UMFD-0", "UMFD-1",
        ]))
        self.exclude_extensions = set(self.config.get("exclude_extensions", [
            "tmp", "log", "etl", "evtx"
        ]))

    def collect(self, source_id: int = None, hours: int = 24,
                server: str = None) -> dict:
        """Event Log'dan olayları topla ve veritabanına yaz.

        Args:
            source_id: Kaynak ID (opsiyonel, filtre için)
            hours: Kaç saatlik veri toplanacak
            server: Uzak sunucu adı (None = lokal)

        Returns:
            {"collected": int, "filtered": int, "errors": int}
        """
        try:
            import win32evtlog
            import win32security
        except ImportError:
            logger.error("pywin32 yüklü değil. pip install pywin32")
            return {"collected": 0, "filtered": 0, "errors": 1,
                    "error": "pywin32 gerekli"}

        server_name = server  # None = localhost
        log_type = "Security"
        collected = 0
        filtered = 0
        errors = 0
        batch = []

        start_time = datetime.now() - timedelta(hours=hours)
        logger.info(f"Event Log toplama: son {hours} saat, sunucu: {server or 'localhost'}")

        try:
            handle = win32evtlog.OpenEventLog(server_name, log_type)
            flags = win32evtlog.EVENTLOG_BACKWARDS_READ | win32evtlog.EVENTLOG_SEQUENTIAL_READ

            while True:
                events = win32evtlog.ReadEventLog(handle, flags, 0)
                if not events:
                    break

                for event in events:
                    # Zaman kontrolü
                    event_time = event.TimeGenerated
                    if event_time < start_time:
                        # Geriye doğru okuduğumuz için burada dur
                        win32evtlog.CloseEventLog(handle)
                        self._flush_batch(batch)
                        return {"collected": collected, "filtered": filtered, "errors": errors}

                    # Event ID filtresi
                    if event.EventID & 0xFFFF not in self.event_ids:
                        continue

                    try:
                        record = self._parse_event(event)
                        if not record:
                            filtered += 1
                            continue

                        if source_id:
                            record["source_id"] = source_id

                        batch.append(record)
                        collected += 1

                        if len(batch) >= self.batch_size:
                            self._flush_batch(batch)
                            batch = []
                            if collected % 5000 == 0:
                                logger.info(f"Toplanan: {collected:,} olay")

                    except Exception as e:
                        errors += 1
                        logger.debug(f"Olay ayrıştırma hatası: {e}")

            win32evtlog.CloseEventLog(handle)

        except Exception as e:
            logger.error(f"Event Log okuma hatası: {e}")
            errors += 1

        # Kalan batch
        self._flush_batch(batch)

        logger.info(f"Event Log toplama tamamlandı: {collected:,} olay, {filtered:,} filtrelendi, {errors} hata")
        return {"collected": collected, "filtered": filtered, "errors": errors}

    def _parse_event(self, event) -> Optional[dict]:
        """Bir Windows Event'i ayrıştır."""
        event_id = event.EventID & 0xFFFF
        strings = event.StringInserts or []

        if event_id == 4663:
            return self._parse_4663(event, strings)
        elif event_id == 5145:
            return self._parse_5145(event, strings)
        elif event_id == 4660:
            return self._parse_4660(event, strings)
        elif event_id == 4656:
            return self._parse_4656(event, strings)
        return None

    def _parse_4663(self, event, strings: list) -> Optional[dict]:
        """Event ID 4663: Dosya sistemi erişim denemesi.

        StringInserts dizisi:
        [0] SubjectUserSid
        [1] SubjectUserName
        [2] SubjectDomainName
        [3] SubjectLogonId
        [4] ObjectServer
        [5] ObjectType
        [6] ObjectName (dosya yolu)
        [7] HandleId
        [8] AccessList / AccessMask
        [9] ProcessId
        [10] ProcessName
        """
        if len(strings) < 9:
            return None

        username = strings[1] if len(strings) > 1 else ""
        domain = strings[2] if len(strings) > 2 else ""
        file_path = strings[6] if len(strings) > 6 else ""
        access_mask = strings[8] if len(strings) > 8 else "0x1"

        return self._build_record(
            username, domain, file_path, access_mask,
            event.TimeGenerated, None, 4663
        )

    def _parse_5145(self, event, strings: list) -> Optional[dict]:
        """Event ID 5145: Ağ paylaşım erişim denetimi.

        StringInserts dizisi:
        [0] SubjectUserSid
        [1] SubjectUserName
        [2] SubjectDomainName
        [3] SubjectLogonId
        [4] ObjectType
        [5] IpAddress
        [6] IpPort
        [7] ShareName
        [8] ShareLocalPath
        [9] RelativeTargetName (dosya adı)
        [10] AccessMask
        [11] AccessList
        """
        if len(strings) < 10:
            return None

        username = strings[1] if len(strings) > 1 else ""
        domain = strings[2] if len(strings) > 2 else ""
        client_ip = strings[5] if len(strings) > 5 else None
        share_name = strings[7] if len(strings) > 7 else ""
        relative_name = strings[9] if len(strings) > 9 else ""
        access_mask = strings[10] if len(strings) > 10 else "0x1"

        file_path = f"{share_name}\\{relative_name}" if relative_name else share_name

        return self._build_record(
            username, domain, file_path, access_mask,
            event.TimeGenerated, client_ip, 5145
        )

    def _parse_4660(self, event, strings: list) -> Optional[dict]:
        """Event ID 4660: Nesne silindi.

        StringInserts dizisi:
        [0] SubjectUserSid
        [1] SubjectUserName
        [2] SubjectDomainName
        [3] SubjectLogonId
        [4] ObjectServer
        [5] HandleId
        [6] ProcessId
        [7] ProcessName
        """
        if len(strings) < 4:
            return None

        username = strings[1] if len(strings) > 1 else ""
        domain = strings[2] if len(strings) > 2 else ""
        # 4660 does not include the file path directly; it references
        # a handle from a prior 4656 event. We record it with limited info.
        file_path = f"[HandleId:{strings[5]}]" if len(strings) > 5 else "[unknown]"

        return self._build_record(
            username, domain, file_path, "0x10000",  # DELETE mask
            event.TimeGenerated, None, 4660
        )

    def _parse_4656(self, event, strings: list) -> Optional[dict]:
        """Event ID 4656: Handle to object requested.

        StringInserts dizisi:
        [0] SubjectUserSid
        [1] SubjectUserName
        [2] SubjectDomainName
        [3] SubjectLogonId
        [4] ObjectServer
        [5] ObjectType
        [6] ObjectName (dosya yolu)
        [7] HandleId
        [8] AccessMask
        [9] ProcessId
        [10] ProcessName
        """
        if len(strings) < 9:
            return None

        username = strings[1] if len(strings) > 1 else ""
        domain = strings[2] if len(strings) > 2 else ""
        file_path = strings[6] if len(strings) > 6 else ""
        access_mask = strings[8] if len(strings) > 8 else "0x1"

        return self._build_record(
            username, domain, file_path, access_mask,
            event.TimeGenerated, None, 4656
        )

    def _build_record(self, username: str, domain: str, file_path: str,
                      access_mask: str, event_time, client_ip: str,
                      event_id: int) -> Optional[dict]:
        """Filtrele ve kayıt oluştur."""
        # Kullanıcı filtresi
        if not username or username.endswith("$"):
            return None  # Makine hesapları
        if username.upper() in self.exclude_users:
            return None

        # Dosya yolu filtresi
        if not file_path or file_path.endswith("\\"):
            return None  # Dizin erişimi

        file_name = os.path.basename(file_path)
        ext = os.path.splitext(file_name)[1].lower().lstrip(".")
        if ext in self.exclude_extensions:
            return None

        access_type = _parse_access_mask(access_mask)

        return {
            "username": username,
            "domain": domain,
            "file_path": file_path,
            "file_name": file_name,
            "extension": ext or None,
            "access_type": access_type,
            "access_time": event_time,
            "client_ip": client_ip,
            "file_size": 0,
            "event_id": event_id,
        }

    def _flush_batch(self, batch: list):
        """Batch'i veritabanına yaz."""
        if batch:
            try:
                self.db.bulk_insert_access_logs(batch)
            except Exception as e:
                logger.error(f"Batch yazma hatası: {e}")

    @staticmethod
    def check_audit_policy() -> dict:
        """Mevcut audit policy durumunu kontrol et."""
        import subprocess
        result = {"file_system": False, "file_share": False}
        try:
            output = subprocess.run(
                ["auditpol", "/get", "/subcategory:File System"],
                capture_output=True, text=True, timeout=10
            ).stdout
            result["file_system"] = "Success" in output

            output = subprocess.run(
                ["auditpol", "/get", "/subcategory:File Share"],
                capture_output=True, text=True, timeout=10
            ).stdout
            result["file_share"] = "Success" in output
        except Exception as e:
            logger.warning(f"Audit policy kontrol hatası: {e}")
        return result

    @staticmethod
    def enable_audit_policy() -> dict:
        """Audit policy'yi etkinleştir (yönetici gerekir)."""
        import subprocess
        results = {}
        for sub in ["File System", "File Share"]:
            try:
                r = subprocess.run(
                    ["auditpol", "/set", f"/subcategory:{sub}", "/success:enable"],
                    capture_output=True, text=True, timeout=10
                )
                results[sub] = r.returncode == 0
            except Exception as e:
                results[sub] = False
                logger.error(f"Audit policy etkinleştirme hatası ({sub}): {e}")
        return results
