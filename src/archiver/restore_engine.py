"""Arşivden geri yükleme motoru."""

import os
import shutil
import logging
from typing import Optional

from src.storage.database import Database
from src.i18n.messages import t

logger = logging.getLogger("file_activity.archiver.restore")


class RestoreEngine:
    """Arşivlenmiş dosyaları orijinal konumuna geri yükler."""

    def __init__(self, db: Database):
        self.db = db

    def restore_by_id(self, archive_id: int, op_id: int = None) -> dict:
        """Arşiv ID'si ile geri yükle."""
        record = self.db.get_archived_file_by_id(archive_id)
        if not record:
            return {"success": False, "error": t("restore_not_found", identifier=f"ID:{archive_id}")}

        if record.get("restored_at"):
            return {"success": False, "error": f"Zaten geri yüklenmiş: ID:{archive_id}"}

        return self._restore(record, op_id=op_id)

    def restore_by_path(self, original_path: str) -> dict:
        """Orijinal yol ile geri yükle."""
        record = self.db.get_archived_file_by_path(original_path)
        if not record:
            return {"success": False, "error": t("restore_not_found", identifier=original_path)}

        return self._restore(record)

    def _restore(self, record: dict, op_id: int = None) -> dict:
        """Geri yükleme işlemini gerçekleştir."""
        archive_path = record["archive_path"]
        original_path = record["original_path"]

        # Arşiv dosyası var mı?
        if not os.path.exists(archive_path):
            return {
                "success": False,
                "error": f"Arşiv dosyası bulunamadı: {archive_path}",
            }

        # Hedef zaten var mı?
        if os.path.exists(original_path):
            return {
                "success": False,
                "error": f"Hedef zaten mevcut: {original_path}",
            }

        try:
            # Orijinal dizin yapısını oluştur
            os.makedirs(os.path.dirname(original_path), exist_ok=True)

            # Kopyala
            shutil.copy2(archive_path, original_path)

            # ACL/yetki geri yukleme (win32security)
            try:
                import win32security
                import ntsecuritycon as con
                sec_info = (
                    win32security.DACL_SECURITY_INFORMATION |
                    win32security.OWNER_SECURITY_INFORMATION |
                    win32security.GROUP_SECURITY_INFORMATION
                )
                sd = win32security.GetFileSecurity(archive_path, sec_info)
                win32security.SetFileSecurity(original_path, sec_info, sd)
            except ImportError:
                pass  # pywin32 not available
            except Exception as e:
                logger.debug("ACL geri yukleme basarisiz %s: %s", original_path, e)

            # Arşiv indeksini güncelle
            self.db.mark_restored(record["id"])

            # Audit event kaydi
            try:
                self.db.insert_audit_event_simple(
                    record.get("source_id"), 'restore', 'system',
                    original_path,
                    f'Restored from {archive_path}',
                    detected_by='restorer'
                )
            except Exception:
                pass

            logger.info(t("restore_completed", path=original_path))

            return {
                "success": True,
                "original_path": original_path,
                "archive_id": record["id"],
            }

        except Exception as e:
            logger.error("Geri yükleme hatası: %s - %s", original_path, e)
            return {"success": False, "error": str(e)}

    def preview_restore(self, archive_ids: list) -> dict:
        """Geri yukleme onizlemesi: olusturulacak dizinler, cakismalar."""
        dirs_to_create = set()
        conflicts = []
        restorable = []
        missing = []
        total_size = 0

        for aid in archive_ids:
            record = self.db.get_archived_file_by_id(aid)
            if not record:
                missing.append(aid)
                continue
            if record.get("restored_at"):
                continue

            original_path = record["original_path"]
            archive_path = record["archive_path"]

            # Arsiv dosyasi var mi?
            if not os.path.exists(archive_path):
                missing.append(aid)
                continue

            # Hedef dizin var mi?
            orig_dir = os.path.dirname(original_path)
            if not os.path.exists(orig_dir):
                dirs_to_create.add(orig_dir)

            # Hedef dosya zaten var mi?
            if os.path.exists(original_path):
                conflicts.append({
                    "id": record["id"],
                    "file_name": record["file_name"],
                    "original_path": original_path,
                    "file_size": record["file_size"]
                })
            else:
                restorable.append({
                    "id": record["id"],
                    "file_name": record["file_name"],
                    "original_path": original_path,
                    "file_size": record["file_size"]
                })
                total_size += record["file_size"]

        return {
            "restorable_count": len(restorable),
            "conflict_count": len(conflicts),
            "missing_count": len(missing),
            "dirs_to_create": sorted(dirs_to_create),
            "dirs_to_create_count": len(dirs_to_create),
            "conflicts": conflicts[:50],
            "restorable": restorable[:50],
            "total_size": total_size
        }

    def bulk_restore(self, archive_ids: list, source_id: int = None) -> dict:
        """Toplu geri yukleme. Dizin yapisi otomatik olusturulur."""
        # Restore operation kaydi olustur
        op_id = None
        if source_id:
            try:
                op_id = self.db.create_archive_operation(
                    'restore', source_id, 'manual', 'bulk_restore'
                )
            except Exception:
                pass

        restored = 0
        failed = 0
        total_size = 0
        errors = []

        for aid in archive_ids:
            result = self.restore_by_id(aid, op_id=op_id)
            if result.get("success"):
                restored += 1
                record = self.db.get_archived_file_by_id(aid)
                if record:
                    total_size += record.get("file_size", 0)
            else:
                failed += 1
                errors.append({"id": aid, "error": result.get("error", "")})

        # Tamamla
        if op_id:
            try:
                status = 'completed' if failed == 0 else ('partial' if restored > 0 else 'failed')
                self.db.complete_archive_operation(op_id, restored, total_size, status)
            except Exception:
                pass

        return {
            "restored": restored,
            "failed": failed,
            "total_size": total_size,
            "errors": errors[:20],
            "operation_id": op_id
        }
