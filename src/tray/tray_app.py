"""FILE ACTIVITY system tray app (issue #151).

Minimal pystray + Pillow tray icon that wraps the FileActivity Windows service:

    Right-click menu:
      - Open Dashboard    -> opens http://localhost:8085 in default browser
      - Start Service
      - Stop Service
      - Restart Service
      - Open Logs Folder  -> explorer.exe <InstallDir>\\logs
      - Quit

    Status icon:
      green  = service Running
      yellow = service StartPending / StopPending / Paused
      red    = service Stopped / NotInstalled / unknown

The tray app is optional. pystray + Pillow are not in requirements.txt;
install via requirements-tray.txt. The tray app does NOT itself host the
dashboard — it only controls the FileActivity service installed by
deploy/install_service.ps1.

Run:
    python -m src.tray.tray_app

Service control commands assume the service name "FileActivity" (default
from install_service.ps1). Override with --service-name.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import threading
import time
import webbrowser

logger = logging.getLogger("file_activity.tray")

DEFAULT_SERVICE_NAME = "FileActivity"
DEFAULT_DASHBOARD_URL = "http://localhost:8085"
DEFAULT_INSTALL_DIR = r"C:\FileActivity"
POLL_INTERVAL_SECONDS = 5


def _run_powershell(command: str, timeout: int = 15) -> tuple[int, str, str]:
    """Run a PowerShell one-liner. Returns (exit_code, stdout, stderr).

    We shell out to powershell.exe rather than depend on pywin32 so the tray
    app remains portable to plain Python venvs.
    """
    try:
        proc = subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy", "Bypass",
                "-Command", command,
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return proc.returncode, proc.stdout.strip(), proc.stderr.strip()
    except FileNotFoundError:
        return 127, "", "powershell.exe not found"
    except subprocess.TimeoutExpired:
        return 124, "", f"timeout after {timeout}s"


def get_service_status(service_name: str) -> str:
    """Return one of: Running, Stopped, StartPending, StopPending, Paused,
    NotInstalled, Unknown."""
    rc, out, _err = _run_powershell(
        f"(Get-Service -Name '{service_name}' -ErrorAction SilentlyContinue).Status"
    )
    if rc != 0 or not out:
        return "NotInstalled"
    return out.strip() or "Unknown"


def start_service(service_name: str) -> tuple[bool, str]:
    rc, out, err = _run_powershell(f"Start-Service -Name '{service_name}'")
    return rc == 0, err or out


def stop_service(service_name: str) -> tuple[bool, str]:
    rc, out, err = _run_powershell(f"Stop-Service -Name '{service_name}' -Force")
    return rc == 0, err or out


def restart_service(service_name: str) -> tuple[bool, str]:
    rc, out, err = _run_powershell(f"Restart-Service -Name '{service_name}' -Force")
    return rc == 0, err or out


def _make_icon_image(color: str):
    """Build a 64x64 RGBA PIL image of a filled circle. Local import so the
    module can still be imported in environments without Pillow (status checks
    work without a GUI)."""
    from PIL import Image, ImageDraw  # type: ignore

    img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    fill = {
        "green":  (46, 160, 67, 255),
        "yellow": (210, 153, 34, 255),
        "red":    (218, 54, 51, 255),
    }.get(color, (128, 128, 128, 255))
    draw.ellipse((6, 6, 58, 58), fill=fill, outline=(20, 20, 20, 255), width=2)
    return img


def _color_for_status(status: str) -> str:
    if status == "Running":
        return "green"
    if status in ("StartPending", "StopPending", "Paused", "PausePending", "ContinuePending"):
        return "yellow"
    return "red"


class TrayApp:
    """Wires pystray.Icon to the service control helpers above."""

    def __init__(
        self,
        service_name: str = DEFAULT_SERVICE_NAME,
        dashboard_url: str = DEFAULT_DASHBOARD_URL,
        install_dir: str = DEFAULT_INSTALL_DIR,
    ) -> None:
        self.service_name = service_name
        self.dashboard_url = dashboard_url
        self.install_dir = install_dir
        self._icon = None
        self._stop_event = threading.Event()
        self._last_status = "Unknown"

    # --- menu actions ---
    def _on_open_dashboard(self, _icon=None, _item=None) -> None:
        try:
            webbrowser.open(self.dashboard_url)
        except Exception as e:
            logger.error("Open dashboard failed: %s", e)

    def _on_start(self, _icon=None, _item=None) -> None:
        ok, msg = start_service(self.service_name)
        logger.info("Start-Service %s -> ok=%s msg=%s", self.service_name, ok, msg)
        self._refresh_status(force=True)

    def _on_stop(self, _icon=None, _item=None) -> None:
        ok, msg = stop_service(self.service_name)
        logger.info("Stop-Service %s -> ok=%s msg=%s", self.service_name, ok, msg)
        self._refresh_status(force=True)

    def _on_restart(self, _icon=None, _item=None) -> None:
        ok, msg = restart_service(self.service_name)
        logger.info("Restart-Service %s -> ok=%s msg=%s", self.service_name, ok, msg)
        self._refresh_status(force=True)

    def _on_open_logs(self, _icon=None, _item=None) -> None:
        logs_dir = os.path.join(self.install_dir, "logs")
        try:
            os.startfile(logs_dir)  # type: ignore[attr-defined]  # Windows-only
        except Exception as e:
            logger.error("Open logs folder failed: %s", e)

    def _on_quit(self, icon=None, _item=None) -> None:
        self._stop_event.set()
        if icon is not None:
            icon.stop()
        elif self._icon is not None:
            self._icon.stop()

    # --- status polling ---
    def _refresh_status(self, force: bool = False) -> None:
        status = get_service_status(self.service_name)
        if status == self._last_status and not force:
            return
        self._last_status = status
        if self._icon is None:
            return
        try:
            self._icon.icon = _make_icon_image(_color_for_status(status))
            self._icon.title = f"FILE ACTIVITY ({status})"
        except Exception as e:
            logger.debug("Icon refresh failed: %s", e)

    def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._refresh_status()
            except Exception as e:
                logger.debug("Status poll failed: %s", e)
            self._stop_event.wait(POLL_INTERVAL_SECONDS)

    # --- entry point ---
    def run(self) -> int:
        try:
            import pystray  # type: ignore
        except ImportError:
            sys.stderr.write(
                "pystray not installed. Run: pip install -r requirements-tray.txt\n"
            )
            return 2

        menu = pystray.Menu(
            pystray.MenuItem("Open Dashboard", self._on_open_dashboard, default=True),
            pystray.MenuItem("Start Service", self._on_start),
            pystray.MenuItem("Stop Service", self._on_stop),
            pystray.MenuItem("Restart Service", self._on_restart),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Open Logs Folder", self._on_open_logs),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Quit", self._on_quit),
        )

        initial_status = get_service_status(self.service_name)
        self._last_status = initial_status
        self._icon = pystray.Icon(
            "FileActivity",
            icon=_make_icon_image(_color_for_status(initial_status)),
            title=f"FILE ACTIVITY ({initial_status})",
            menu=menu,
        )

        poller = threading.Thread(target=self._poll_loop, daemon=True)
        poller.start()
        try:
            self._icon.run()
        finally:
            self._stop_event.set()
        return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FILE ACTIVITY system tray (issue #151)")
    parser.add_argument("--service-name", default=DEFAULT_SERVICE_NAME)
    parser.add_argument("--dashboard-url", default=DEFAULT_DASHBOARD_URL)
    parser.add_argument("--install-dir", default=DEFAULT_INSTALL_DIR)
    parser.add_argument("--log-level", default="WARNING")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.WARNING),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    app = TrayApp(
        service_name=args.service_name,
        dashboard_url=args.dashboard_url,
        install_dir=args.install_dir,
    )
    return app.run()


if __name__ == "__main__":
    raise SystemExit(main())
