"""FILE ACTIVITY Windows Service.

Runs as a Windows service to:
1. Auto-start file watchers for all enabled sources
2. Execute scheduled scans and archives
3. Restart after system reboot automatically
"""

import sys
import os
import time
import logging
import threading

# Add parent to path for imports
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

logger = logging.getLogger("file_activity.service")


class FileActivityService:
    """Core service logic (can run standalone or as Windows service)."""

    def __init__(self, config_path="config.yaml"):
        self.config_path = config_path
        self.running = False
        self.watchers = {}
        self.scheduler = None
        self.dashboard_thread = None
        self.db = None

    def start(self):
        from src.utils.config_loader import load_config
        from src.utils.logging_setup import setup_logging
        from src.storage.database import Database
        from src.scanner.file_watcher import FileWatcher
        from src.scheduler.task_scheduler import TaskScheduler

        self.config = load_config(self.config_path)
        setup_logging(self.config)

        self.db = Database(self.config.get("database", {}))
        self.db.connect()

        self.running = True
        logger.info("FILE ACTIVITY Service starting...")

        # Start watchers for all enabled sources
        sources = self.db.get_sources(enabled_only=True)
        watcher_interval = self.config.get("watcher", {}).get("interval", 300)

        for src in sources:
            try:
                w = FileWatcher(self.db, src.id, src.unc_path, watcher_interval)
                w.start()
                self.watchers[src.id] = w
                logger.info("Watcher started: %s (%s)", src.name, src.unc_path)
            except Exception as e:
                logger.error("Watcher failed for %s: %s", src.name, e)

        # Start scheduler
        try:
            self.scheduler = TaskScheduler(self.db, self.config)
            self.scheduler.start()
            logger.info("Task scheduler started")
        except Exception as e:
            logger.error("Scheduler failed: %s", e)

        # Start dashboard in background
        dash_config = self.config.get("dashboard", {})
        if dash_config.get("auto_start", True):
            self.dashboard_thread = threading.Thread(target=self._run_dashboard, daemon=True)
            self.dashboard_thread.start()

        logger.info("FILE ACTIVITY Service started (%d watchers, scheduler active)", len(self.watchers))

    def _run_dashboard(self):
        try:
            import uvicorn
            from src.dashboard.api import create_app
            dash = self.config.get("dashboard", {})
            app = create_app(self.db, self.config)
            uvicorn.run(app, host=dash.get("host", "0.0.0.0"), port=dash.get("port", 8085), log_level="warning")
        except Exception as e:
            logger.error("Dashboard failed: %s", e)

    def stop(self):
        logger.info("FILE ACTIVITY Service stopping...")
        self.running = False
        for sid, w in self.watchers.items():
            w.stop()
        if self.scheduler:
            self.scheduler.stop()
        if self.db:
            self.db.close()
        logger.info("FILE ACTIVITY Service stopped")

    def run_forever(self):
        """Run as standalone (non-service) mode."""
        self.start()
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()


# Windows Service wrapper using pywin32
try:
    import win32serviceutil
    import win32service
    import win32event
    import servicemanager

    class FileActivityWindowsService(win32serviceutil.ServiceFramework):
        _svc_name_ = "FileActivityService"
        _svc_display_name_ = "FILE ACTIVITY - File Share Monitor"
        _svc_description_ = "Windows File Share Analysis, Monitoring and Archiving Service"

        def __init__(self, args):
            win32serviceutil.ServiceFramework.__init__(self, args)
            self.stop_event = win32event.CreateEvent(None, 0, 0, None)
            self.service = FileActivityService()

        def SvcStop(self):
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            self.service.stop()
            win32event.SetEvent(self.stop_event)

        def SvcDoRun(self):
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, '')
            )
            self.service.start()
            win32event.WaitForSingleObject(self.stop_event, win32event.INFINITE)

except ImportError:
    pass  # pywin32 not available, service mode disabled


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ("install", "remove", "start", "stop", "restart"):
        win32serviceutil.HandleCommandLine(FileActivityWindowsService)
    else:
        # Standalone mode
        svc = FileActivityService()
        svc.run_forever()
