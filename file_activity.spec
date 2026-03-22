# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec - FILE ACTIVITY EXE build."""

import os
import sys

block_cipher = None
ROOT = os.path.abspath('.')

# Force src to be found
sys.path.insert(0, ROOT)

# Use PyInstaller's collect helpers
from PyInstaller.utils.hooks import collect_submodules, collect_data_files

# Collect ALL submodules from src package
src_imports = collect_submodules('src')
print(f"[SPEC] Collected {len(src_imports)} src submodules:")
for m in sorted(src_imports):
    print(f"  {m}")

# Data files
data_files = [
    ('config.yaml', '.'),
]

static_dir = os.path.join(ROOT, 'src', 'dashboard', 'static')
if os.path.isdir(static_dir):
    for root, dirs, files in os.walk(static_dir):
        for f in files:
            full = os.path.join(root, f)
            rel_dir = os.path.relpath(root, ROOT)
            data_files.append((full, rel_dir))

i18n_dir = os.path.join(ROOT, 'src', 'i18n')
if os.path.isdir(i18n_dir):
    for root, dirs, files in os.walk(i18n_dir):
        for f in files:
            if f.endswith('.py'):
                continue
            full = os.path.join(root, f)
            rel_dir = os.path.relpath(root, ROOT)
            data_files.append((full, rel_dir))

# CRITICAL: Also add all src .py files as data so they exist at runtime
for root, dirs, files in os.walk(os.path.join(ROOT, 'src')):
    if '__pycache__' in root:
        continue
    for f in files:
        if f.endswith('.py'):
            full = os.path.join(root, f)
            rel_dir = os.path.relpath(root, ROOT)
            data_files.append((full, rel_dir))

print(f"[SPEC] Data files: {len(data_files)}")

a = Analysis(
    ['main.py'],
    pathex=[ROOT],
    binaries=[],
    datas=data_files,
    hiddenimports=src_imports + [
        # SQLite (built-in, but ensure it's bundled)
        'sqlite3',
        # Win32 (pywin32 hidden modules)
        'win32timezone',
        'win32api',
        'win32con',
        'win32file',
        'win32security',
        'win32evtlog',
        'win32net',
        'win32netcon',
        'ntsecuritycon',
        'pywintypes',
        'winreg',
        # Web
        'uvicorn',
        'uvicorn.logging',
        'uvicorn.loops',
        'uvicorn.loops.auto',
        'uvicorn.protocols',
        'uvicorn.protocols.http',
        'uvicorn.protocols.http.auto',
        'uvicorn.protocols.websockets',
        'uvicorn.protocols.websockets.auto',
        'uvicorn.lifespan',
        'uvicorn.lifespan.on',
        'fastapi',
        'fastapi.responses',
        'fastapi.staticfiles',
        'pydantic',
        'starlette',
        'starlette.routing',
        'starlette.staticfiles',
        'starlette.responses',
        'anyio',
        'anyio._backends',
        'anyio._backends._asyncio',
        'click',
        'yaml',
        'apscheduler',
        'apscheduler.schedulers.background',
        'apscheduler.triggers.cron',
        # Reports
        'openpyxl',
        'reportlab',
        'reportlab.lib',
        'reportlab.platypus',
        # Windows Service
        'win32serviceutil',
        'win32service',
        'win32event',
        'servicemanager',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=['runtime_hook.py'],
    excludes=['tkinter', 'matplotlib', 'numpy', 'scipy', 'PIL', 'pytest'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='FileActivity',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='FileActivity',
)
