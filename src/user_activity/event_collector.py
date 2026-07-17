"""Windows Event Log Kolektörü.

Windows Security Event Log'dan dosya erişim olaylarını toplar.

Desteklenen Event ID'ler:
  - 4663: Dosya sistemi nesnelerine erişim (NTFS Audit)
  - 5145: Ağ paylaşım nesnelerine erişim (SMB Audit)
  - 4656: Dosya handle istendi (gerçek ObjectName burada)
  - 4660: Nesne silindi (yalnız HandleId — 4656 ile korele edilir)
  - 4624: Hesap oturum açtı (LogonId → kaynak IP/workstation eşlemesi)

Gereksinimler:
  - Yönetici yetkisi (Event Log okuma)
  - Audit Policy aktif olmalı:
    auditpol /set /subcategory:"File System" /success:enable
    auditpol /set /subcategory:"File Share" /success:enable
    auditpol /set /subcategory:"Logon" /success:enable   # 4624 için

#340 Faz 2 — korelasyon:
  * 4656↔4660 handle korelasyonu → "ne silindi" (tam yol). Güvenlik günlüğü
    geriye doğru okunur; 4660 (silme) ilgili 4656'dan (yol) ÖNCE görülür,
    bu yüzden 4660 kayıtları geçici bir map'te tutulur ve eşleşen 4656
    gelince gerçek yolla yamanır.
  * 4624 → LogonId bazlı client_ip/workstation ikinci geçişle doldurulur
    ("nereden"). 4624 kayıtları user_access_logs'a YAZILMAZ.
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


# ── StringInserts index positions ────────────────────────────────────────
# Amendment 2: these layouts are UNTESTED against live events and KNOWN TO
# DRIFT across Windows builds and audit configs (real 4663 has been observed
# with AccessList at [8]/AccessMask at [9]; real 4656 can insert a
# TransactionId between HandleId and AccessList). They are named constants so
# a drift is a one-line fix, and every access below is length-guarded. The
# first-run diagnostic in EventCollector.collect() logs one raw StringInserts
# array per event ID so the operator can validate these against live events.

# 4663 — file system access
IDX_4663_USERNAME = 1
IDX_4663_DOMAIN = 2
IDX_4663_LOGON_ID = 3
IDX_4663_OBJECT_NAME = 6
IDX_4663_ACCESS_MASK = 8

# 5145 — network share access (carries IpAddress directly)
IDX_5145_USERNAME = 1
IDX_5145_DOMAIN = 2
IDX_5145_LOGON_ID = 3
IDX_5145_IP_ADDRESS = 5
IDX_5145_SHARE_NAME = 7
IDX_5145_RELATIVE_TARGET = 9
IDX_5145_ACCESS_MASK = 10

# 4660 — object deleted (references a prior 4656 handle; no path of its own)
IDX_4660_USERNAME = 1
IDX_4660_DOMAIN = 2
IDX_4660_LOGON_ID = 3
IDX_4660_HANDLE_ID = 5
IDX_4660_PROCESS_ID = 6

# 4656 — handle to object requested (carries the real ObjectName)
IDX_4656_USERNAME = 1
IDX_4656_DOMAIN = 2
IDX_4656_LOGON_ID = 3
IDX_4656_OBJECT_NAME = 6
IDX_4656_HANDLE_ID = 7
IDX_4656_ACCESS_MASK = 8
IDX_4656_PROCESS_ID = 9

# 4624 — account logon (LogonId → client_ip/workstation map)
IDX_4624_LOGON_ID = 7          # TargetLogonId
IDX_4624_WORKSTATION = 11      # WorkstationName
IDX_4624_IP_ADDRESS = 18       # IpAddress

# Bound the pending-4660 correlation map against HandleId reuse.
_PENDING_4660_CAP = 10000

# Safety cap on the in-run buffer. Rows are held (not flushed) so the 4624
# IP second pass can patch them; this bounds memory so a misconfigured
# read-auditing SACL can't OOM the box. Rows flushed at this cap simply miss
# the IP second pass (floor: client_ip stays as parsed).
_MAX_BUFFER = 200000

# Sentinel: a 4656 resolved a pending 4660 but the real path failed the
# re-applied filters (amendment 10) — the row is dropped, not inserted.
_DROPPED = object()


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
        self.event_ids = self.config.get(
            "event_ids", [4663, 5145, 4660, 4656, 4624])
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
            import win32security  # noqa: F401  (imported for parity / API availability)
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
        # (handle_id, process_id) -> unresolved 4660 record. Held OUT of the
        # batch until a matching 4656 supplies the real path (amendment A).
        pending_4660: dict = {}
        # LogonId -> (ip, workstation). Built from 4624; NEVER inserted.
        logon_map: dict = {}
        # First-run StringInserts diagnostic bookkeeping (amendment 2b).
        seen_event_ids: set = set()

        start_time = datetime.now() - timedelta(hours=hours)
        logger.info(
            f"Event Log toplama: son {hours} saat, sunucu: {server or 'localhost'}")

        def _enqueue(rec: dict) -> None:
            """Append a finished record to the batch and count it. Flushes on
            the safety cap (with the IP second pass applied to that slice)."""
            nonlocal collected
            if source_id:
                rec["source_id"] = source_id
            batch.append(rec)
            collected += 1
            if len(batch) >= _MAX_BUFFER:
                self._apply_logon_ips(batch, logon_map)
                self._flush_batch(batch)
                batch.clear()

        handle = None
        try:
            handle = win32evtlog.OpenEventLog(server_name, log_type)
            flags = win32evtlog.EVENTLOG_BACKWARDS_READ | win32evtlog.EVENTLOG_SEQUENTIAL_READ

            done = False
            while not done:
                events = win32evtlog.ReadEventLog(handle, flags, 0)
                if not events:
                    break

                for event in events:
                    # Zaman kontrolü — geriye doğru okuduğumuz için ilk eski
                    # olayda dur (exit path #1; flush finally'de).
                    if event.TimeGenerated < start_time:
                        done = True
                        break

                    event_id = event.EventID & 0xFFFF
                    if event_id not in self.event_ids:
                        continue

                    strings = event.StringInserts or []
                    self._log_first_seen(event_id, strings, seen_event_ids)

                    try:
                        # 4624 → logon map only (never inserted).
                        if event_id == 4624:
                            parsed = self._parse_4624_logon(strings)
                            if parsed:
                                logon_map[parsed[0]] = (parsed[1], parsed[2])
                            continue

                        # 4660 → hold for correlation; release later.
                        if event_id == 4660:
                            rec = self._parse_4660(event, strings)
                            if rec is None:
                                filtered += 1
                                continue
                            key = (rec.get("handle_id"), rec.get("process_id"))
                            prior = pending_4660.get(key)
                            if prior is not None:
                                # HandleId reuse before the older 4660 resolved
                                # — keep it (placeholder path), never lose a row.
                                _enqueue(prior)
                            pending_4660[key] = rec
                            if len(pending_4660) > _PENDING_4660_CAP:
                                old_key = next(iter(pending_4660))
                                _enqueue(pending_4660.pop(old_key))
                            continue

                        # 4656 → resolve a pending 4660 if HandleId+ProcessId
                        # match; else insert the 4656 itself (Faz 1 behavior).
                        if event_id == 4656:
                            result = self._correlate_4656(strings, pending_4660)
                            if result is _DROPPED:
                                filtered += 1
                                continue
                            if result is not None:
                                _enqueue(result)
                                continue
                            # no pending match → fall through to normal insert

                        rec = self._parse_event(event)
                        if not rec:
                            filtered += 1
                            continue
                        _enqueue(rec)

                    except Exception as e:
                        errors += 1
                        logger.debug(f"Olay ayrıştırma hatası: {e}")

        except Exception as e:  # exit path #3 (exception)
            logger.error(f"Event Log okuma hatası: {e}")
            errors += 1
        finally:
            # Amendment 4: the end-of-run flush runs on ALL THREE exit paths
            # (time-cutoff break, normal loop end, exception). Release every
            # unresolved 4660 with its [HandleId:x] placeholder path — never
            # lose the row, only the (unknown) path.
            for rec in pending_4660.values():
                _enqueue(rec)
            pending_4660.clear()
            # 4624 IP/workstation second pass over the buffered rows, then
            # flush what remains (exit path #2 = normal end also lands here).
            self._apply_logon_ips(batch, logon_map)
            self._flush_batch(batch)
            batch.clear()
            if handle is not None:
                try:
                    win32evtlog.CloseEventLog(handle)
                except Exception:
                    pass

        logger.info(
            f"Event Log toplama tamamlandı: {collected:,} olay, "
            f"{filtered:,} filtrelendi, {errors} hata")
        return {"collected": collected, "filtered": filtered, "errors": errors}

    # ── First-run diagnostic (amendment 2b) ──────────────────────────────

    def _log_first_seen(self, event_id: int, strings: list, seen: set) -> None:
        """The first time each event ID appears in a run, log one raw
        StringInserts array (repr, truncated) so the operator can validate the
        drift-prone index constants against live events."""
        if event_id in seen:
            return
        seen.add(event_id)
        try:
            raw = repr(list(strings))
        except Exception:
            raw = "<repr failed>"
        if len(raw) > 300:
            raw = raw[:300] + "...(truncated)"
        logger.info("İlk %s olayı StringInserts[%d]: %s",
                    event_id, len(strings), raw)

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

        username = strings[IDX_4663_USERNAME] if len(strings) > IDX_4663_USERNAME else ""
        domain = strings[IDX_4663_DOMAIN] if len(strings) > IDX_4663_DOMAIN else ""
        file_path = strings[IDX_4663_OBJECT_NAME] if len(strings) > IDX_4663_OBJECT_NAME else ""
        access_mask = strings[IDX_4663_ACCESS_MASK] if len(strings) > IDX_4663_ACCESS_MASK else "0x1"
        logon_id = strings[IDX_4663_LOGON_ID] if len(strings) > IDX_4663_LOGON_ID else None

        return self._build_record(
            username, domain, file_path, access_mask,
            event.TimeGenerated, None, 4663, logon_id=logon_id
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

        username = strings[IDX_5145_USERNAME] if len(strings) > IDX_5145_USERNAME else ""
        domain = strings[IDX_5145_DOMAIN] if len(strings) > IDX_5145_DOMAIN else ""
        client_ip = strings[IDX_5145_IP_ADDRESS] if len(strings) > IDX_5145_IP_ADDRESS else None
        share_name = strings[IDX_5145_SHARE_NAME] if len(strings) > IDX_5145_SHARE_NAME else ""
        relative_name = strings[IDX_5145_RELATIVE_TARGET] if len(strings) > IDX_5145_RELATIVE_TARGET else ""
        access_mask = strings[IDX_5145_ACCESS_MASK] if len(strings) > IDX_5145_ACCESS_MASK else "0x1"
        logon_id = strings[IDX_5145_LOGON_ID] if len(strings) > IDX_5145_LOGON_ID else None

        file_path = f"{share_name}\\{relative_name}" if relative_name else share_name

        return self._build_record(
            username, domain, file_path, access_mask,
            event.TimeGenerated, client_ip, 5145, logon_id=logon_id
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

        4660 dosya yolunu içermez; önceki bir 4656'nın HandleId'sine referans
        verir. handle_id/process_id/logon_id extra anahtar olarak taşınır
        (bulk_insert bunları .get() ile görmezden gelir) ve collect()
        4656↔4660 korelasyonunda kullanır (amendment A).
        """
        if len(strings) < 4:
            return None

        username = strings[IDX_4660_USERNAME] if len(strings) > IDX_4660_USERNAME else ""
        domain = strings[IDX_4660_DOMAIN] if len(strings) > IDX_4660_DOMAIN else ""
        logon_id = strings[IDX_4660_LOGON_ID] if len(strings) > IDX_4660_LOGON_ID else None
        handle_id = strings[IDX_4660_HANDLE_ID] if len(strings) > IDX_4660_HANDLE_ID else None
        process_id = strings[IDX_4660_PROCESS_ID] if len(strings) > IDX_4660_PROCESS_ID else None

        file_path = f"[HandleId:{handle_id}]" if handle_id is not None else "[unknown]"

        rec = self._build_record(
            username, domain, file_path, "0x10000",  # DELETE mask
            event.TimeGenerated, None, 4660, logon_id=logon_id
        )
        if rec is not None:
            rec["handle_id"] = handle_id
            rec["process_id"] = process_id
        return rec

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

        username = strings[IDX_4656_USERNAME] if len(strings) > IDX_4656_USERNAME else ""
        domain = strings[IDX_4656_DOMAIN] if len(strings) > IDX_4656_DOMAIN else ""
        file_path = strings[IDX_4656_OBJECT_NAME] if len(strings) > IDX_4656_OBJECT_NAME else ""
        access_mask = strings[IDX_4656_ACCESS_MASK] if len(strings) > IDX_4656_ACCESS_MASK else "0x1"
        logon_id = strings[IDX_4656_LOGON_ID] if len(strings) > IDX_4656_LOGON_ID else None

        return self._build_record(
            username, domain, file_path, access_mask,
            event.TimeGenerated, None, 4656, logon_id=logon_id
        )

    # ── Correlation helpers (Faz 2) ──────────────────────────────────────

    def _correlate_4656(self, strings: list, pending_4660: dict):
        """Try to resolve a pending 4660 with this 4656's real ObjectName.

        Returns:
            * the patched 4660 record (dict) — matched, path passed re-filter;
            * ``_DROPPED`` — matched, but the real path failed the re-applied
              path filters (amendment 10) → caller drops it;
            * ``None`` — no pending 4660 matched → caller inserts the 4656
              normally (Faz 1 behavior).

        The (HandleId, ProcessId) key is matched on the raw Event-Log string
        formatting, which is identical across the 4656/4660 pair.
        """
        if len(strings) <= IDX_4656_HANDLE_ID:
            return None
        handle_id = strings[IDX_4656_HANDLE_ID]
        process_id = strings[IDX_4656_PROCESS_ID] if len(strings) > IDX_4656_PROCESS_ID else None
        key = (handle_id, process_id)
        rec = pending_4660.pop(key, None)
        if rec is None:
            return None

        object_name = strings[IDX_4656_OBJECT_NAME] if len(strings) > IDX_4656_OBJECT_NAME else ""
        # Amendment 10: the [HandleId:x] placeholder trivially passed the path
        # filters in _build_record — the REAL path may be a directory or an
        # excluded extension, so re-apply those checks now.
        if not object_name or object_name.endswith("\\"):
            return _DROPPED
        file_name = os.path.basename(object_name)
        ext = os.path.splitext(file_name)[1].lower().lstrip(".")
        if ext in self.exclude_extensions:
            return _DROPPED

        rec["file_path"] = object_name
        rec["file_name"] = file_name
        rec["extension"] = ext or None
        return rec

    def _parse_4624_logon(self, strings: list):
        """Extract (LogonId, IpAddress, WorkstationName) from a 4624 logon.

        Returns None when the mandatory LogonId is absent. Used ONLY to build
        the in-run logon→IP map; 4624 rows are never inserted.
        """
        if len(strings) <= IDX_4624_LOGON_ID:
            return None
        logon_id = strings[IDX_4624_LOGON_ID]
        if not logon_id:
            return None
        ip = strings[IDX_4624_IP_ADDRESS] if len(strings) > IDX_4624_IP_ADDRESS else None
        wks = strings[IDX_4624_WORKSTATION] if len(strings) > IDX_4624_WORKSTATION else None
        return logon_id, ip, wks

    def _apply_logon_ips(self, batch: list, logon_map: dict) -> None:
        """Second pass (amendment B): fill client_ip on buffered rows whose
        logon_id resolves to a 4624 logon. Workstation is appended as
        "ip (WKS)". Rows that already carry a client_ip (e.g. 5145/SMB) or
        lack a logon_id are left untouched. No schema change — client_ip is
        an existing column.
        """
        if not logon_map:
            return
        for rec in batch:
            if rec.get("client_ip"):
                continue
            lid = rec.get("logon_id")
            if not lid:
                continue
            info = logon_map.get(lid)
            if not info:
                continue
            ip = self._clean_ip(info[0])
            if not ip:
                continue
            wks = info[1]
            if wks and wks not in ("-", ""):
                rec["client_ip"] = f"{ip} ({wks})"
            else:
                rec["client_ip"] = ip

    @staticmethod
    def _clean_ip(ip):
        """Normalize an Event-Log IpAddress. Console / loopback logons report
        '-', '::1' or '127.0.0.1' — meaningless as a REMOTE origin, so they
        map to None (leave the row IP-less rather than misleading)."""
        if not ip:
            return None
        ip = ip.strip()
        if ip in ("-", "::1", "127.0.0.1", ""):
            return None
        return ip

    def _build_record(self, username: str, domain: str, file_path: str,
                      access_mask: str, event_time, client_ip: str,
                      event_id: int, logon_id: str = None) -> Optional[dict]:
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

        # #340 Faz 1 — normalize pywintypes TimeGenerated (a datetime
        # subclass) to the schema's TEXT format. Stores LOCAL wall-clock
        # DELIBERATELY: user_access_logs defaults use
        # datetime('now','localtime') and _detect_night_access reads
        # strftime('%H') — storing UTC would skew night detection by the
        # box's offset. (Window filters comparing against UTC
        # datetime('now') may over-include by <= the offset; harmless.)
        if hasattr(event_time, "strftime"):
            event_time = event_time.strftime("%Y-%m-%d %H:%M:%S")

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
            # #340 Faz 2 — transient correlation keys (not columns; the
            # bulk insert reads only the declared keys via .get()).
            "logon_id": logon_id,
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
