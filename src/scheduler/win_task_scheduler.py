"""Windows Görev Zamanlayıcı entegrasyonu.

Windows Task Scheduler üzerinden görevleri kaydetmeye ve kaldırmaya yarar.
Bu sayede uygulama kapalıyken bile görevler tetiklenebilir.
"""

import logging
import subprocess
import os
import sys

logger = logging.getLogger("file_activity.scheduler.win")

TASK_PREFIX = "FileActivity_"


def create_windows_task(task_name: str, cron_expression: str, command_args: str,
                        python_exe: str = None, script_path: str = None):
    """Windows Task Scheduler'a görev ekle.

    Args:
        task_name: Görev adı (önek otomatik eklenir)
        cron_expression: Cron ifadesi (basit dönüşüm yapılır)
        command_args: main.py'ye geçilecek argümanlar
        python_exe: Python yolu (varsayılan: sys.executable)
        script_path: main.py yolu (varsayılan: otomatik algıla)
    """
    python_exe = python_exe or sys.executable
    script_path = script_path or os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "main.py")

    full_name = f"{TASK_PREFIX}{task_name}"

    # Cron -> schtasks dönüşümü (basitleştirilmiş)
    schedule_args = _cron_to_schtasks(cron_expression)
    if not schedule_args:
        logger.error(f"Cron dönüştürülemedi: {cron_expression}")
        return False, "Cron ifadesi Windows Task Scheduler'a dönüştürülemedi"

    cmd = [
        "schtasks", "/Create",
        "/TN", full_name,
        "/TR", f'"{python_exe}" "{script_path}" {command_args}',
        *schedule_args,
        "/F"  # Mevcut görevi üzerine yaz
    ]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            logger.info(f"Windows görevi oluşturuldu: {full_name}")
            return True, f"Görev oluşturuldu: {full_name}"
        else:
            logger.error(f"schtasks hatası: {result.stderr}")
            return False, f"Hata: {result.stderr.strip()}"
    except subprocess.TimeoutExpired:
        return False, "Zaman aşımı"
    except Exception as e:
        return False, str(e)


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


def _cron_to_schtasks(cron_expression: str):
    """Basit cron -> schtasks dönüşümü.

    Desteklenen desenler:
    - * * * * *     -> /SC MINUTE /MO 1
    - 0 * * * *     -> /SC HOURLY
    - 0 2 * * *     -> /SC DAILY /ST 02:00
    - 0 2 * * 0     -> /SC WEEKLY /D SUN /ST 02:00
    - 0 2 1 * *     -> /SC MONTHLY /D 1 /ST 02:00
    """
    parts = cron_expression.strip().split()
    if len(parts) != 5:
        return None

    minute, hour, dom, month, dow = parts

    day_map = {"0": "SUN", "1": "MON", "2": "TUE", "3": "WED",
               "4": "THU", "5": "FRI", "6": "SAT", "7": "SUN"}

    # Her dakika
    if all(p == "*" for p in parts):
        return ["/SC", "MINUTE", "/MO", "1"]

    # Her saat
    if minute != "*" and hour == "*" and dom == "*" and month == "*" and dow == "*":
        return ["/SC", "HOURLY"]

    time_str = f"{int(hour):02d}:{int(minute):02d}" if hour != "*" and minute != "*" else "00:00"

    # Haftalık
    if dow != "*" and dom == "*":
        days = []
        for d in dow.split(","):
            d = d.strip()
            if d in day_map:
                days.append(day_map[d])
            elif "-" in d:
                start, end = d.split("-")
                for i in range(int(start), int(end) + 1):
                    if str(i) in day_map:
                        days.append(day_map[str(i)])
        if days:
            return ["/SC", "WEEKLY", "/D", ",".join(days), "/ST", time_str]

    # Aylık
    if dom != "*" and month == "*" and dow == "*":
        return ["/SC", "MONTHLY", "/D", dom, "/ST", time_str]

    # Günlük (varsayılan)
    if dom == "*" and month == "*" and dow == "*" and hour != "*":
        return ["/SC", "DAILY", "/ST", time_str]

    return None
