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
        # Issue #110 Phase 2: register the daily quarantine purge job.
        # Hard-deletes quarantine_log rows older than quarantine_days.
        self._register_daily_quarantine_purge_job()
        # Issue #113: daily capacity-check job. No-op when
        # forecast.enabled=false, smtp.enabled=false, or
        # forecast.alarm_email is empty — all three independently disable.
        self._register_capacity_check_job()
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

    def _run_daily_quarantine_purge(self, task=None):
        """Issue #110 Phase 2: hard-delete quarantine_log rows older than
        ``duplicates.quarantine.quarantine_days``.

        Per-file errors never abort the batch — they're surfaced in the
        returned summary so operators can review the next morning.
        Defensive SHA-256 verification happens inside ``purge_one``;
        any mismatch returns ``abort_sha_mismatch`` and the file is
        preserved (forensic) rather than silently deleted.
        """
        try:
            from src.archiver.duplicate_cleaner import DuplicateCleaner
        except Exception as e:
            return {
                "status": "error",
                "message": f"duplicate_cleaner import failed: {e}",
            }

        dup_cfg = ((self.config or {}).get("duplicates") or {}).get(
            "quarantine"
        ) or {}
        if not dup_cfg.get("enabled", True):
            return {"status": "skipped",
                    "message": "duplicates.quarantine.enabled=false"}

        try:
            cleaner = DuplicateCleaner(self.db, self.config)
            results = cleaner.purge_expired(purged_by="scheduler")
            summary = {
                "status": "completed",
                "candidates": len(results),
                "purged": sum(1 for r in results if r.status == "purged"),
                "skipped_missing": sum(
                    1 for r in results if r.status == "skipped_missing"
                ),
                "abort_sha_mismatch": sum(
                    1 for r in results
                    if r.status == "abort_sha_mismatch"
                ),
                "skipped_already_purged": sum(
                    1 for r in results
                    if r.status == "skipped_already_purged"
                ),
                "skipped_restored": sum(
                    1 for r in results if r.status == "skipped_restored"
                ),
                "errors": sum(1 for r in results if r.status == "error"),
            }
            logger.info("daily_quarantine_purge summary: %s", summary)
            return summary
        except Exception as e:
            logger.error("daily_quarantine_purge failed: %s", e,
                         exc_info=True)
            return {"status": "error", "message": str(e)}

    def _register_daily_quarantine_purge_job(self):
        """Register the config-driven daily quarantine purge job (#110)."""
        dup_cfg = ((self.config or {}).get("duplicates") or {}).get(
            "quarantine"
        ) or {}
        if not dup_cfg.get("enabled", True):
            logger.info(
                "daily_quarantine_purge not registered: "
                "duplicates.quarantine.enabled=false"
            )
            return
        try:
            hour = int(dup_cfg.get("purge_hour", 3))
        except (TypeError, ValueError):
            hour = 3
        if hour < 0 or hour > 23:
            logger.warning(
                "purge_hour=%r out of range — defaulting to 3", hour
            )
            hour = 3
        try:
            trigger = CronTrigger(minute="0", hour=str(hour))
            self.scheduler.add_job(
                self._run_daily_quarantine_purge,
                trigger=trigger,
                id="daily_quarantine_purge",
                name="daily_quarantine_purge:duplicates",
                replace_existing=True,
            )
            logger.info(
                "daily_quarantine_purge job registered (cron: 0 %d * * *)",
                hour,
            )
        except Exception as e:
            logger.error(
                "Failed to register daily_quarantine_purge: %s", e
            )

    # ──────────────────────────────────────────────────────────────────
    # Issue #113 — daily capacity-alarm digest
    # ──────────────────────────────────────────────────────────────────

    def _run_capacity_check(self):
        """Daily capacity check (issue #113).

        For every source with a completed scan history, run the linear
        forecast. If ``capacity_alarm_at`` is set AND falls within
        ``forecast.alarm_lead_days`` of today, batch the alert into a
        single digest email to ``forecast.alarm_email``.

        Returns a dict so the run-log is meaningful. Never raises —
        scheduler retries tomorrow.
        """
        forecast_cfg = (self.config or {}).get("forecast") or {}
        if not forecast_cfg.get("enabled", True):
            return {"status": "skipped", "reason": "forecast.enabled=false"}
        alarm_email = (forecast_cfg.get("alarm_email") or "").strip()
        if not alarm_email:
            return {"status": "skipped", "reason": "alarm_email empty"}
        if self.email_notifier is None or not self.email_notifier.available:
            return {"status": "skipped", "reason": "email_notifier unavailable"}

        try:
            lead_days = int(forecast_cfg.get("alarm_lead_days", 30))
        except (TypeError, ValueError):
            lead_days = 30
        try:
            pct = float(forecast_cfg.get("capacity_threshold_pct", 85))
        except (TypeError, ValueError):
            pct = 85.0
        horizon_days = max(lead_days * 2, 90)  # always look further than the lead

        # Disk total (best-effort) for the default threshold.
        disk_total = 0
        try:
            import shutil as _shutil
            import os as _os
            target = _os.path.dirname(_os.path.abspath(self.db.db_path)) or "."
            disk_total = int(_shutil.disk_usage(target).total)
        except Exception:
            disk_total = 0
        threshold = int(disk_total * pct / 100.0) if disk_total > 0 else 0

        try:
            from src.reports.forecast import forecast_growth
        except Exception as e:
            logger.error("daily_capacity_check forecast import failed: %s", e)
            return {"status": "error", "message": str(e)}

        now = datetime.now()
        alarmed: list = []
        with self.db.get_cursor() as cur:
            cur.execute("SELECT id, name FROM sources WHERE enabled = 1")
            sources = [dict(r) for r in cur.fetchall()]
        for src in sources:
            sid = int(src["id"])
            with self.db.get_cursor() as cur:
                cur.execute(
                    """
                    SELECT started_at, total_size, total_files
                    FROM scan_runs
                    WHERE source_id = ?
                      AND status = 'completed'
                      AND total_size IS NOT NULL
                      AND total_size > 0
                    ORDER BY started_at ASC
                    """,
                    (sid,),
                )
                rows = [dict(r) for r in cur.fetchall()]
            if not rows:
                continue
            try:
                result = forecast_growth(
                    rows, horizon_days=horizon_days,
                    capacity_threshold_bytes=threshold or None,
                    source_id=sid,
                )
            except Exception as e:
                logger.warning("forecast failed for source %s: %s", sid, e)
                continue
            if not result.capacity_alarm_at:
                continue
            try:
                alarm_dt = datetime.fromisoformat(result.capacity_alarm_at)
            except Exception:
                continue
            days_to = (alarm_dt - now).days
            if days_to > lead_days:
                continue
            alarmed.append({
                "source_id": sid,
                "source_name": src.get("name") or "?",
                "alarm_at": result.capacity_alarm_at,
                "days_to_alarm": days_to,
                "predicted_bytes": int(result.predicted_bytes),
                "last_bytes": int(result.last_bytes),
                "samples_used": result.samples_used,
            })

        if not alarmed:
            return {"status": "completed", "alarmed": 0,
                    "checked_sources": len(sources)}

        # Build a plain-text digest (HTML is optional; keep it simple).
        lines = [
            f"File Activity capacity forecast — {now.strftime('%Y-%m-%d')}",
            f"Threshold: {pct:.1f}% of disk "
            f"(~{threshold / (1024 ** 3):.1f} GiB)" if threshold else
            "Threshold: per-request",
            "",
            "Sources approaching capacity within "
            f"{lead_days} days:",
            "",
        ]
        for a in alarmed:
            lines.append(
                f"  • [{a['source_id']}] {a['source_name']}: "
                f"alarm {a['alarm_at']} ({a['days_to_alarm']}d) "
                f"— last {a['last_bytes'] / (1024**3):.1f} GiB, "
                f"projected {a['predicted_bytes'] / (1024**3):.1f} GiB"
            )
        body = "\n".join(lines)

        # Use a minimal MIME-text send — EmailNotifier.send_user_report is
        # tied to user-score reports; we just need a plain digest. Reuse
        # its low-level SMTP plumbing.
        try:
            from email.mime.text import MIMEText
            from email.utils import formataddr
            msg = MIMEText(body, "plain", "utf-8")
            subject_prefix = self.email_notifier.subject_prefix or "[File Activity]"
            msg["Subject"] = (
                f"{subject_prefix} Kapasite Uyarisi: "
                f"{len(alarmed)} kaynak"
            )
            msg["From"] = formataddr(
                (self.email_notifier.from_name, self.email_notifier.from_address)
            )
            msg["To"] = alarm_email
            recipients = [alarm_email]
            cc = (self.email_notifier.admin_cc or "").strip()
            if cc and cc not in recipients:
                msg["Cc"] = cc
                recipients.append(cc)
            with self.email_notifier._connect() as smtp:
                smtp.sendmail(
                    self.email_notifier.from_address,
                    recipients,
                    msg.as_string(),
                )
            logger.info(
                "daily_capacity_check digest sent to %s (%d sources)",
                alarm_email, len(alarmed),
            )
            return {
                "status": "completed",
                "alarmed": len(alarmed),
                "checked_sources": len(sources),
                "to": alarm_email,
            }
        except Exception as e:
            logger.error("daily_capacity_check email send failed: %s", e)
            return {"status": "error", "message": str(e),
                    "alarmed": len(alarmed)}

    def _register_capacity_check_job(self):
        """Register the once-a-day capacity-alarm digest job (issue #113)."""
        forecast_cfg = (self.config or {}).get("forecast") or {}
        if not forecast_cfg.get("enabled", True):
            logger.info("daily_capacity_check not registered: forecast.enabled=false")
            return
        if not (forecast_cfg.get("alarm_email") or "").strip():
            logger.info(
                "daily_capacity_check not registered: forecast.alarm_email empty"
            )
            return
        # Run once a day at 03:15 — after the daily backup (02:00) so the
        # snapshot reflects the freshest scans, before business hours.
        try:
            trigger = CronTrigger(minute="15", hour="3")
            self.scheduler.add_job(
                self._run_capacity_check,
                trigger=trigger,
                id="daily_capacity_check",
                name="daily_capacity_check",
                replace_existing=True,
            )
            logger.info("daily_capacity_check job registered (cron: 15 3 * * *)")
        except Exception as e:
            logger.error("Failed to register daily_capacity_check: %s", e)

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
