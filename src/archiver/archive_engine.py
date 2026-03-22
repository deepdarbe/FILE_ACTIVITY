"""Arşivleme motoru - Kopyala-Doğrula-Sil güvenlik akışı.

Dosyaları arsiv hedefine taşır, dizin yapısını korur, checksum doğrular.
"""

import os
import hashlib
import shutil
import logging
from typing import Optional

from src.storage.database import Database
from src.i18n.messages import t
from src.utils.size_formatter import format_size

logger = logging.getLogger("file_activity.archiver.engine")


class ArchiveEngine:
    """Güvenli arşivleme motoru."""

    def __init__(self, db: Database, config: dict):
        self.db = db
        self.verify_checksum = config.get("archiving", {}).get("verify_checksum", True)
        self.dry_run = config.get("archiving", {}).get("dry_run", False)
        self.cleanup_empty = config.get("archiving", {}).get("cleanup_empty_dirs", True)

    def archive_files(self, files: list[dict], archive_dest: str,
                       source_unc: str, source_id: int,
                       archived_by: str = "manual",
                       dry_run: bool = None,
                       trigger_type: str = "manual",
                       trigger_detail: str = None) -> dict:
        """Dosya listesini arşivle.

        Args:
            files: Arşivlenecek dosya listesi (scanned_files kayıtları)
            archive_dest: Arşiv hedef dizini
            source_unc: Kaynak UNC kök yolu
            source_id: Kaynak ID
            archived_by: Arşivleme nedeni/politika adı
            dry_run: Override dry_run modu
            trigger_type: Tetikleyen tip (manual, policy, ai_insight, scheduled)
            trigger_detail: Tetikleyen detay (politika adi veya insight turu)

        Returns:
            {"archived": int, "failed": int, "total_size": int, "errors": list}
        """
        is_dry_run = dry_run if dry_run is not None else self.dry_run

        # Arsiv islemi kaydi olustur
        op_id = None
        if not is_dry_run:
            try:
                op_id = self.db.create_archive_operation(
                    'archive', source_id, trigger_type,
                    trigger_detail or archived_by
                )
            except Exception as e:
                logger.warning("Arsiv islem kaydi olusturulamadi: %s", e)

        archived = 0
        failed = 0
        total_size = 0
        errors = []

        for file_info in files:
            try:
                result = self._archive_single_file(
                    file_info, archive_dest, source_unc, source_id,
                    archived_by, is_dry_run, operation_id=op_id
                )
                if result:
                    archived += 1
                    total_size += file_info["file_size"]
                    # Audit event kaydi
                    if not is_dry_run:
                        try:
                            archive_path = os.path.join(archive_dest, file_info.get("relative_path", ""))
                            self.db.insert_audit_event_simple(
                                source_id, 'archive', 'system',
                                file_info["file_path"],
                                f'Archived to {archive_path} | Policy: {archived_by}',
                                detected_by='archiver'
                            )
                        except Exception:
                            pass
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                errors.append({"path": file_info["file_path"], "error": str(e)})
                logger.error("Arşivleme hatası: %s - %s", file_info["file_path"], e)

        # Arsiv islemini tamamla
        if op_id:
            try:
                status = 'completed' if failed == 0 else ('partial' if archived > 0 else 'failed')
                files_summary = [{"path": f["file_path"], "size": f["file_size"]} for f in files[:500]]
                self.db.complete_archive_operation(
                    op_id, archived, total_size, status,
                    error=str(errors[:5]) if errors else None,
                    files_json=files_summary
                )
            except Exception as e:
                logger.warning("Arsiv islem tamamlama kaydi hata: %s", e)

        summary = {
            "archived": archived,
            "failed": failed,
            "total_size": total_size,
            "total_size_formatted": format_size(total_size),
            "dry_run": is_dry_run,
            "errors": errors,
            "operation_id": op_id,
        }

        logger.info(t("archive_completed", count=archived, size=format_size(total_size)))
        return summary

    def _archive_single_file(self, file_info: dict, archive_dest: str,
                              source_unc: str, source_id: int,
                              archived_by: str, is_dry_run: bool,
                              operation_id: int = None) -> bool:
        """Tek dosyayı arşivle. Başarılı ise True döner."""

        src_path = file_info["file_path"]
        rel_path = file_info["relative_path"]
        dst_path = os.path.join(archive_dest, rel_path)

        # Dry-run modu
        if is_dry_run:
            logger.info(t("archive_dry_run", path=rel_path, size=format_size(file_info["file_size"])))
            return True

        logger.info(t("archive_file", path=rel_path))

        # Kaynak dosya var mı kontrol
        if not os.path.exists(src_path):
            logger.warning("Kaynak dosya bulunamadı: %s", src_path)
            return False

        # 1. Hedef dizin oluştur (yapı korunur)
        dst_dir = os.path.dirname(dst_path)
        os.makedirs(dst_dir, exist_ok=True)

        # 2. Kopyala
        shutil.copy2(src_path, dst_path)

        # 2b. ACL/yetki kopyalama (win32security)
        try:
            import win32security
            import ntsecuritycon as con
            sec_info = (
                win32security.DACL_SECURITY_INFORMATION |
                win32security.OWNER_SECURITY_INFORMATION |
                win32security.GROUP_SECURITY_INFORMATION
            )
            sd = win32security.GetFileSecurity(src_path, sec_info)
            win32security.SetFileSecurity(dst_path, sec_info, sd)
        except ImportError:
            pass  # pywin32 not available
        except Exception as e:
            logger.debug("ACL kopyalama basarisiz %s: %s", rel_path, e)

        # 3. Checksum doğrula
        checksum = None
        if self.verify_checksum:
            src_hash = self._sha256(src_path)
            dst_hash = self._sha256(dst_path)
            if src_hash != dst_hash:
                # Doğrulama başarısız - hedefi sil
                os.remove(dst_path)
                logger.error(t("archive_checksum_fail", path=rel_path))
                return False
            checksum = src_hash
            logger.debug(t("archive_checksum_ok", path=rel_path))

        # 4. Arşiv indeksine kaydet
        self.db.insert_archived_file({
            "source_id": source_id,
            "original_path": src_path,
            "relative_path": rel_path,
            "archive_path": dst_path,
            "file_name": file_info["file_name"],
            "extension": file_info.get("extension"),
            "file_size": file_info["file_size"],
            "creation_time": file_info.get("creation_time"),
            "last_access_time": file_info.get("last_access_time"),
            "last_modify_time": file_info.get("last_modify_time"),
            "owner": file_info.get("owner"),
            "archived_by": archived_by,
            "checksum": checksum,
            "operation_id": operation_id,
        })

        # 5. Kaynak dosyayı sil
        os.remove(src_path)

        # 6. Boş dizinleri temizle
        if self.cleanup_empty:
            self._cleanup_empty_parents(os.path.dirname(src_path), source_unc)

        return True

    def _sha256(self, path: str) -> str:
        """SHA-256 checksum hesapla."""
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    def _cleanup_empty_parents(self, dir_path: str, stop_at: str):
        """Boş üst dizinleri temizle, stop_at'a kadar."""
        dir_path = os.path.normpath(dir_path)
        stop_at = os.path.normpath(stop_at)

        while dir_path != stop_at and dir_path.startswith(stop_at):
            try:
                if os.path.isdir(dir_path) and not os.listdir(dir_path):
                    os.rmdir(dir_path)
                    logger.debug("Boş dizin silindi: %s", dir_path)
                else:
                    break
            except OSError:
                break
            dir_path = os.path.dirname(dir_path)
