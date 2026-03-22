"""PyInstaller runtime hook - ensure src package is importable."""
import sys
import os

# When frozen, add the _internal directory to path
if getattr(sys, 'frozen', False):
    base = sys._MEIPASS
    if base not in sys.path:
        sys.path.insert(0, base)
    # Also add the directory containing the exe
    exe_dir = os.path.dirname(sys.executable)
    internal = os.path.join(exe_dir, '_internal')
    if os.path.isdir(internal) and internal not in sys.path:
        sys.path.insert(0, internal)
