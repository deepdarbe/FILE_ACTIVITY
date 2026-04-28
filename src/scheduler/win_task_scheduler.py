"""Windows Görev Zamanlayıcı entegrasyonu.

Windows Task Scheduler üzerinden görevleri kaydetmeye ve kaldırmaya yarar.
Bu sayede uygulama kapalıyken bile görevler tetiklenebilir.

Audit L-2 (security-audit-2026-04-28):
``create_windows_task`` was the only schtasks ``/TR`` builder in this
module and had **zero callers** anywhere in ``src/``. It built a command
line by interpolating ``command_args`` directly into the ``/TR`` value,
which is the canonical CodeQL ``py/command-line-injection`` pattern. We
deleted it instead of guarding it — the right substitute when scheduling
is needed is either Windows-side configuration or the in-process APScheduler
path that ``main.py`` already wires up.
"""

from __future__ import annotations

import logging
import subprocess

logger = logging.getLogger("file_activity.scheduler.win")

TASK_PREFIX = "FileActivity_"


def remove_windows_task(task_name: str):
    """Windows Task Scheduler'dan görev kaldır."""
    full_name = f"{TASK_PREFIX}{task_name}"
    try:
        result = subprocess.run(
            ["schtasks", "/Delete", "/TN", full_name, "/F"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            logger.info(f"Windows görevi silindi: {full_name}")
            return True, "Görev silindi"
        else:
            return False, result.stderr.strip()
    except Exception as e:
        return False, str(e)


def list_windows_tasks():
    """FileActivity ile başlayan Windows görevlerini listele."""
    try:
        result = subprocess.run(
            ["schtasks", "/Query", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, timeout=30
        )
        tasks = []
        for line in result.stdout.strip().split("\n"):
            if TASK_PREFIX in line:
                parts = line.strip('"').split('","')
                if len(parts) >= 3:
                    tasks.append({
                        "name": parts[0].replace("\\", ""),
                        "next_run": parts[1],
                        "status": parts[2] if len(parts) > 2 else "Bilinmiyor"
                    })
        return tasks
    except Exception as e:
        logger.error(f"Windows görevleri listelenemedi: {e}")
        return []


