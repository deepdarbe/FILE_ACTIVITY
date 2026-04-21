"""FILE ACTIVITY - Dagitim Paketi Olusturucu.

Tum gerekli dosyalari tek bir ZIP'e paketler.
Her guncelleme sonrasi calistirilir.

Kullanim:
  python pack.py
  python pack.py --output dist/FileActivity_v1.2.zip
"""

import os
import sys
import zipfile
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Paketlenecek dosya ve dizinler
INCLUDE = [
    # Ana dosyalar
    "main.py",
    "dev_server.py",
    "setup.py",
    "config.yaml",
    "requirements.txt",
    "file_activity.spec",
    "build.bat",
    "pack.py",
    "VERSION",

    # Kaynak kod
    "src/",

    # Kurulum & dagitim
    "deploy/",
    "scripts/",

    # Dokumantasyon
    "docs/",
]

# Haric tutulacak desenler
EXCLUDE_PATTERNS = [
    "__pycache__",
    ".pyc",
    ".pyo",
    ".egg-info",
    ".git",
    ".claude",
    "node_modules",
    "dist/",
    "build/",
    "reports/",
    "logs/",
    "data/",
    ".env",
    "*.db",
    "*.log",
]


def should_exclude(path: str) -> bool:
    """Dosyanin haric tutulup tutulmayacagini kontrol et."""
    for pattern in EXCLUDE_PATTERNS:
        if pattern in path:
            return True
    return False


def collect_files() -> list[tuple[str, str]]:
    """Paketlenecek dosyalari topla. [(disk_path, zip_path), ...]"""
    files = []

    for item in INCLUDE:
        full_path = os.path.join(PROJECT_ROOT, item)

        if os.path.isfile(full_path):
            if not should_exclude(item):
                files.append((full_path, item))

        elif os.path.isdir(full_path):
            for root, dirs, filenames in os.walk(full_path):
                # Haric dizinleri atla
                dirs[:] = [d for d in dirs if not should_exclude(d)]

                for fname in filenames:
                    disk_path = os.path.join(root, fname)
                    rel_path = os.path.relpath(disk_path, PROJECT_ROOT)

                    if not should_exclude(rel_path):
                        files.append((disk_path, rel_path))

    return files


def create_zip(output_path: str = None) -> str:
    """ZIP paketi olustur."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if not output_path:
        os.makedirs(os.path.join(PROJECT_ROOT, "dist"), exist_ok=True)
        output_path = os.path.join(PROJECT_ROOT, "dist", f"FileActivity_{timestamp}.zip")

    files = collect_files()

    print(f"\n{'=' * 50}")
    print(f"  FILE ACTIVITY - Paket Olusturucu")
    print(f"{'=' * 50}")
    print(f"  Cikti: {output_path}")
    print(f"  Dosya sayisi: {len(files)}")
    print()

    total_size = 0
    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for disk_path, zip_path in sorted(files, key=lambda x: x[1]):
            # ZIP icinde FileActivity/ prefix ekle
            arcname = f"FileActivity/{zip_path}"
            zf.write(disk_path, arcname)
            fsize = os.path.getsize(disk_path)
            total_size += fsize

            # Kisa gosterim
            ext = os.path.splitext(zip_path)[1]
            icon = {"py": "PY", ".yaml": "YM", ".html": "HT", ".bat": "BT",
                    ".ps1": "PS", ".csv": "CS", ".txt": "TX", ".spec": "SP"
                    }.get(ext, "  ")
            print(f"  [{icon}] {zip_path}")

    zip_size = os.path.getsize(output_path)

    print(f"\n{'-' * 50}")
    print(f"  Toplam:     {len(files)} dosya")
    print(f"  Ham boyut:  {total_size / 1024 / 1024:.1f} MB")
    print(f"  ZIP boyut:  {zip_size / 1024 / 1024:.1f} MB")
    print(f"  Sikistirma: %{(1 - zip_size / total_size) * 100:.0f}")
    print(f"\n  Paket hazir: {output_path}")
    print(f"{'=' * 50}\n")

    return output_path


if __name__ == "__main__":
    output = None
    if "--output" in sys.argv:
        idx = sys.argv.index("--output")
        if idx + 1 < len(sys.argv):
            output = sys.argv[idx + 1]

    create_zip(output)
