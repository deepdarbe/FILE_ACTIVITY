"""Zamanlanmış görev çalıştırıcı.

APScheduler ile cron tabanlı görevleri yönetir.
"""

import logging
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger("file_activity.scheduler")


class TaskScheduler:
    """Zamanlanmış görevleri yöneten scheduler."""

    def __init__(self, db, config, ad_lookup=None, email_notifier=None):
        self.db = db
        self.config = config
        self.ad_lookup = ad_lookup
        self.email_notifier = email_notifier
        self.scheduler = BackgroundScheduler(
            job_defaults={"coalesce": True, "max_instances": 1}
        )
        self._jobs = {}

    def start(self):
        """Scheduler'ı başlat ve veritabanındaki görevleri yükle."""
        self._load_tasks()
        # Issue #77: register the daily SQLite backup job from config.
        # This is config-driven (not stored in scheduled_tasks) so it
        # works on fresh installs without a manual setup step.
        self._register_daily_backup_job()
        # Issue #112: hourly expire_stale job for the two-person
        # approval queue. Always-on (cheap no-op when there are no
        # pending rows or approvals.enabled=false).
        self._register_approval_expiry_job()
        self.scheduler.start()
        logger.info("TaskScheduler başlatıldı")

    def stop(self):
        """Scheduler'ı durdur."""
        self.scheduler.shutdown(wait=False)
        logger.info("TaskScheduler durduruldu")

    def _load_tasks(self):
        """Veritabanındaki aktif görevleri yükle."""
        tasks = self.db.get_scheduled_tasks(enabled_only=True)
        for task in tasks:
            self._add_job(task)

    def _add_job(self, task):
        """Tek bir görevi scheduler'a ekle."""
        task_id = task["id"]
        cron_expr = task["cron_expression"]

        try:
            parts = cron_expr.strip().split()
            if len(parts) != 5:
                logger.error(f"Geçersiz cron: {cron_expr} (görev {task_id})")
                return

            trigger = CronTrigger(
                minute=parts[0], hour=parts[1],
                day=parts[2], month=parts[3],
                day_of_week=parts[4]
            )

            job = self.scheduler.add_job(
                self._execute_task,
                trigger=trigger,
                args=[task],
                id=f"task_{task_id}",
                name=f"{task['task_type']}:{task.get('source_name', '?')}",
                replace_existing=True
            )
            self._jobs[task_id] = job
            logger.info(f"Görev eklendi: {task_id} ({cron_expr})")

        except Exception as e:
            logger.error(f"Görev eklenemedi {task_id}: {e}")

    def _execute_task(self, task):
        """Bir görevi çalıştır."""
        task_id = task["id"]
        task_type = task["task_type"]
        source_id = task["source_id"]
        started_at = datetime.now()

        logger.info(f"Görev çalışıyor: {task_id} ({task_type})")

        try:
            if task_type == "scan":
                result = self._run_scan(source_id)
            elif task_type == "archive":
                result = self._run_archive(task)
            elif task_type == "notify_users":
                result = self._run_notify_users(task)
            elif task_type == "audit_export":
                # Issue #38: WORM export of hash-chained audit log.
                result = self._run_audit_export(task)
            elif task_type == "daily_backup":
                # Issue #77: SQLite snapshot + prune.
                result = self._run_daily_backup(task)
            else:
                result = {"status": "error", "message": f"Bilinmeyen görev türü: {task_type}"}

            self.db.update_task_run(
                task_id, started_at, datetime.now(),
                result.get("status", "completed"), result
            )
            logger.info(f"Görev tamamlandı: {task_id} -> {result.get('status')}")

        except Exception as e:
            logger.error(f"Görev hatası {task_id}: {e}")
            self.db.update_task_run(
                task_id, started_at, datetime.now(),
                "error", {"error": str(e)}
            )

    def _run_scan(self, source_id):
        """Tarama görevi çalıştır."""
        from src.scanner.file_scanner import FileScanner

        src = self.db.get_source_by_id(source_id)
        if not src:
            return {"status": "error", "message": "Kaynak bulunamadı"}

        scanner = FileScanner(self.db, self.config)
        return scanner.scan_source(src.id, src.name, src.unc_path)

    def _run_archive(self, task):
        """Arşivleme görevi çalıştır."""
        from src.archiver.archive_policy import ArchivePolicyEngine
        from src.archiver.archive_engine import ArchiveEngine

        source_id = task["source_id"]
        policy_id = task.get("policy_id")

        src = self.db.get_source_by_id(source_id)
        if not src:
            return {"status": "error", "message": "Kaynak bulunamadı"}
        if not src.archive_dest:
            return {"status": "error", "message": "Arşiv hedefi tanımlı değil"}

        scan_id = self.db.get_latest_scan_id(source_id)
        if not scan_id:
            return {"status": "error", "message": "Tarama verisi bulunamadı"}

        policy_engine = ArchivePolicyEngine(self.db)

        if policy_id:
            policy = self.db.get_policy_by_id(policy_id)
            if not policy:
                return {"status": "error", "message": "Politika bulunamadı"}
            files = policy_engine.get_files_by_policy(source_id, scan_id, policy["name"])
        else:
            # Varsayılan: 365 günden eski
            files = policy_engine.get_files_by_days(source_id, scan_id, 365)

        if not files:
            return {"status": "completed", "archived": 0, "message": "Arşivlenecek dosya yok"}

        engine = ArchiveEngine(self.db, self.config)
        return engine.archive_files(files, src.archive_dest, src.unc_path, source_id, f"scheduled:{task['id']}")

    def _run_notify_users(self, task):
        """Kullanici bildirim gorevi — verimlilik raporunu e-posta ile gonder.

        Her dosya sahibi icin:
          1. compute_user_score(db, owner) ile skor hesapla
          2. ad_lookup.lookup(owner) ile e-posta cozumle
          3. email_notifier.send_user_report(...) ile HTML e-posta gonder

        AD/SMTP erisilmezse 'skipped' sayar ama hata atmaz. Sonuc ozeti
        scheduled_tasks run log'una yazilir. Ayrintili gonderim kaydi
        notification_log tablosunda tutulur (EmailNotifier yapar).
        """
        source_id = task["source_id"]

        # Lazy import: scheduler ana import agini yuklemesin
        from src.user_activity.efficiency_score import compute_user_score

        if self.email_notifier is None or not self.email_notifier.available:
            msg = "EmailNotifier mevcut degil veya SMTP devre disi"
            logger.warning("notify_users atlandi (gorev %s): %s", task.get("id"), msg)
            return {"status": "skipped", "message": msg,
                    "sent": 0, "skipped": 0, "failed": 0}

        scan_id = self.db.get_latest_scan_id(source_id, include_running=False)
        if not scan_id:
            return {"status": "error", "message": "Tarama verisi bulunamadi",
                    "sent": 0, "skipped": 0, "failed": 0}

        # Kaynakta sahibi olan tum kullanicilar
        with self.db.get_cursor() as cur:
            cur.execute(
                """
                SELECT owner, COUNT(*) AS file_count
                FROM scanned_files
                WHERE scan_id = ? AND owner IS NOT NULL AND owner != ''
                GROUP BY owner
                ORDER BY file_count DESC
                """,
                (scan_id,),
            )
            owners = [row["owner"] for row in cur.fetchall()]

        if not owners:
            return {"status": "completed", "message": "Bu taramada sahibi olan dosya yok",
                    "sent": 0, "skipped": 0, "failed": 0}

        total = len(owners)
        logger.info("notify_users: %d kullaniciya bildirim gonderilecek", total)

        sent = 0
        skipped = 0
        failed = 0
        skipped_users: list = []
        # Her 25 kullanicida bir ilerleme logu
        progress_step = max(25, total // 10)

        # Persistent SMTP session — her kullanicida yeni TCP baglanti
        # acmamak icin (rate-limited sunucularda onemli). EmailNotifier'in
        # session() API'si yoksa eski fallback'e (her send kendi baglantisi).
        session_cm = None
        if hasattr(self.email_notifier, "session"):
            try:
                session_cm = self.email_notifier.session()
                smtp_session = session_cm.__enter__()
            except Exception as e:
                logger.warning("SMTP oturumu acilamadi, per-send baglanti: %s", e)
                session_cm = None
                smtp_session = None
        else:
            smtp_session = None

        try:
            for idx, owner in enumerate(owners, start=1):
                if idx % progress_step == 0 or idx == total:
                    logger.info(
                        "notify_users ilerleme: %d/%d (sent=%d, skipped=%d, failed=%d)",
                        idx, total, sent, skipped, failed,
                    )
                # 1. Skor hesapla
                try:
                    score = compute_user_score(self.db, owner, scan_id=scan_id)
                except Exception as e:
                    logger.warning("skor hesaplama hatasi %s: %s", owner, e)
                    failed += 1
                    continue

                # 2. AD'den e-posta coz
                email = None
                display_name = None
                if self.ad_lookup is not None:
                    try:
                        info = self.ad_lookup.lookup(owner)
                        if info:
                            email = info.get("email")
                            display_name = info.get("display_name")
                    except Exception as e:
                        logger.debug("AD lookup hatasi %s: %s", owner, e)
                if not email:
                    skipped += 1
                    skipped_users.append({"owner": owner, "reason": "no email"})
                    continue

                # 3. Gonder — smtp_session varsa onu kullan, yoksa per-send
                send_kwargs = {
                    "username": owner,
                    "email": email,
                    "score_result": score,
                    "display_name": display_name,
                }
                if smtp_session is not None:
                    send_kwargs["smtp_session"] = smtp_session
                result = self.email_notifier.send_user_report(**send_kwargs)
                if result.get("ok"):
                    sent += 1
                else:
                    failed += 1
                    logger.warning("notify gonderilemedi %s (%s): %s", owner, email,
                                   result.get("error"))
        finally:
            if session_cm is not None:
                try:
                    session_cm.__exit__(None, None, None)
                except Exception as e:
                    logger.debug("SMTP oturumu kapatma hatasi: %s", e)

        status = "completed" if failed == 0 else ("partial" if sent > 0 else "error")
        return {
            "status": status,
            "scan_id": scan_id,
            "source_id": source_id,
            "candidates": len(owners),
            "sent": sent,
            "skipped": skipped,
            "failed": failed,
            # Ilk 20 atlanani detay icin ekle — log cok sisirilmesin
            "skipped_users_sample": skipped_users[:20],
        }

    def _run_audit_export(self, task):
        """Run a WORM JSONL export of the audit chain since the last export.

        Source-id is ignored — exports are global. Output dir + signing key
        come from ``audit:`` config. Returns the AuditExporter result dict
        with task status injected so the scheduler run-log is meaningful.
        """
        try:
            from src.storage.audit_export import AuditExporter
        except Exception as e:
            return {"status": "error", "message": f"AuditExporter import failed: {e}"}
        try:
            exporter = AuditExporter(self.db, self.config)
            result = exporter.export_since_last()
            result["status"] = "completed"
            return result
        except Exception as e:
            logger.error("audit_export task failed: %s", e)
            return {"status": "error", "message": str(e)}

    def _run_daily_backup(self, task=None):
        """Issue #77: scheduled SQLite snapshot + prune.

        Snapshot failures are caught + reported as ``status=error`` but
        never raised — the scheduler will retry tomorrow.
        """
        try:
            from src.storage.backup_manager import BackupManager
        except Exception as e:
            return {"status": "error", "message": f"backup_manager import failed: {e}"}

        backup_cfg = (self.config or {}).get("backup") or {}
        if not backup_cfg.get("enabled", True):
            return {"status": "skipped", "message": "backup.enabled=false"}

        db_path = (
            (self.config or {}).get("database", {}).get("path")
            or "data/file_activity.db"
        )
        try:
            mgr = BackupManager(db_path, self.config)
            meta = mgr.snapshot(reason="scheduled-daily")
            deleted = 0
            try:
                deleted = mgr.prune()
            except Exception as e:
                # Prune failure shouldn't fail the whole job — the
                # snapshot itself succeeded, which is what matters.
                logger.error("daily_backup prune failed: %s", e, exc_info=True)
            return {
                "status": "completed",
                "snapshot_id": meta.id,
                "size_bytes": meta.size_bytes,
                "sha256": meta.sha256,
                "pruned": deleted,
            }
        except Exception as e:
            logger.error("daily_backup snapshot failed: %s", e, exc_info=True)
            return {"status": "error", "message": str(e)}

    def _register_daily_backup_job(self):
        """Register the config-driven daily backup job (issue #77)."""
        backup_cfg = (self.config or {}).get("backup") or {}
        if not backup_cfg.get("enabled", True):
            logger.info("daily_backup not registered: backup.enabled=false")
            return
        try:
            hour = int(backup_cfg.get("daily_snapshot_hour", 2))
        except (TypeError, ValueError):
            hour = 2
        # Clamp to valid CronTrigger range; default to 2 if nonsense.
        if hour < 0 or hour > 23:
            logger.warning(
                "daily_snapshot_hour=%r out of range — defaulting to 2", hour
            )
            hour = 2
        try:
            trigger = CronTrigger(minute="0", hour=str(hour))
            self.scheduler.add_job(
                self._run_daily_backup,
                trigger=trigger,
                id="daily_backup",
                name="daily_backup:sqlite",
                replace_existing=True,
            )
            logger.info(
                "daily_backup job registered (cron: 0 %d * * *)", hour
            )
        except Exception as e:
            logger.error("Failed to register daily_backup: %s", e)

    def _run_approval_expiry(self):
        """Flip stale pending approvals to ``expired`` (issue #112)."""
        try:
            from src.security.approvals import ApprovalRegistry
        except Exception as e:
            logger.warning("approvals import failed: %s", e)
            return {"status": "error", "message": str(e)}
        try:
            registry = ApprovalRegistry(self.db, self.config)
            n = registry.expire_stale()
            logger.info("expire_stale_approvals: flipped %d row(s)", n)
            return {"status": "ok", "expired": n}
        except Exception as e:
            logger.error("expire_stale_approvals failed: %s", e, exc_info=True)
            return {"status": "error", "message": str(e)}

    def _register_approval_expiry_job(self):
        """Register the hourly approval-expiry job (issue #112)."""
        approvals_cfg = (self.config or {}).get("approvals") or {}
        # Allow operators to override the cron with
        # approvals.expire_check_hour (still hourly, just lets them set
        # which minute each hour the job runs). Default minute=5 to
        # avoid the top-of-hour scheduler-startup rush.
        try:
            minute = int(approvals_cfg.get("expire_check_minute", 5))
        except (TypeError, ValueError):
            minute = 5
        if minute < 0 or minute > 59:
            minute = 5
        try:
            trigger = CronTrigger(minute=str(minute))
            self.scheduler.add_job(
                self._run_approval_expiry,
                trigger=trigger,
                id="expire_stale_approvals",
                name="expire_stale_approvals",
                replace_existing=True,
            )
            logger.info(
                "expire_stale_approvals job registered (cron: %d * * * *)",
                minute,
            )
        except Exception as e:
            logger.error(
                "Failed to register expire_stale_approvals: %s", e
            )

    def reload_tasks(self):
        """Görevleri yeniden yükle."""
        # Mevcut görevleri kaldır
        for job_id in list(self._jobs.keys()):
            try:
                self.scheduler.remove_job(f"task_{job_id}")
            except Exception:
                pass
        self._jobs.clear()
        self._load_tasks()
        logger.info("Görevler yeniden yüklendi")

    def get_next_runs(self):
        """Sonraki çalışma zamanlarını getir."""
        result = []
        for task_id, job in self._jobs.items():
            result.append({
                "task_id": task_id,
                "job_name": job.name,
                "next_run": str(job.next_run_time) if job.next_run_time else None
            })
        return result
