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

    def __init__(self, db, config):
        self.db = db
        self.config = config
        self.scheduler = BackgroundScheduler(
            job_defaults={"coalesce": True, "max_instances": 1}
        )
        self._jobs = {}

    def start(self):
        """Scheduler'ı başlat ve veritabanındaki görevleri yükle."""
        self._load_tasks()
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
