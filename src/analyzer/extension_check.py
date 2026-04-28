"""Yanlis-uzanti tespiti (issue #144 Phase 1 — Czkawka pattern).

Customer dedup tools surface "wrong extension" findings (e.g., a file
named ``rapor.pdf`` whose magic bytes actually decode as ``application/zip``).
This is a common ransomware / payload disguise pattern that pure
byte-hash dedup never catches.

The module ships an ``ExtensionChecker`` that:

* Lazy-imports ``python-magic`` (libmagic). When the lib is missing it
  gracefully no-ops with a single WARNING — operators can install it
  via ``pip install -r requirements-accel.txt``.
* Maps the declared extension to the set of MIME types we expect
  (e.g. ``pdf`` -> ``{'application/pdf'}``). Unknown extensions are
  skipped (we cannot say anything useful).
* Categorises mismatches into ``low | medium | high | critical``.
  Executable-as-document (a ``.pdf`` whose magic is ``application/x-dosexec``)
  is the canonical critical pattern.

Phase 2 (perceptual hashes) and Phase 3 (broken file detection) are
deferred — see issue #144 for the deferred work.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Iterable, List, Optional

logger = logging.getLogger("file_activity.analyzer.extension_check")


# ──────────────────────────────────────────────────────────────────────
# Result model
# ──────────────────────────────────────────────────────────────────────


@dataclass
class ExtensionAnomaly:
    """One mismatch row. Mirrors ``extension_anomalies`` columns."""

    file_path: str
    declared_ext: Optional[str]      # 'pdf' (already lowercased, no leading dot)
    detected_mime: Optional[str]     # 'application/zip'
    detected_ext: Optional[str]      # 'zip' (best-guess from MIME)
    severity: str                    # 'low'|'medium'|'high'|'critical'

    def as_row(self) -> tuple:
        return (
            self.file_path,
            self.declared_ext,
            self.detected_mime,
            self.detected_ext,
            self.severity,
        )


# ──────────────────────────────────────────────────────────────────────
# Severity rules
# ──────────────────────────────────────────────────────────────────────


# Top ~30 common types. Each declared extension maps to the set of MIME
# strings we'll accept. Several Office formats are ZIP archives under the
# hood, so we list both the canonical Office MIME and ``application/zip``
# as "expected" to avoid false positives on lenient libmagic builds.
EXPECTED_MIMES: dict[str, set[str]] = {
    # Documents
    "pdf": {"application/pdf"},
    "doc": {"application/msword", "application/x-ole-storage",
            "application/CDFV2"},
    "docx": {
        "application/vnd.openxmlformats-officedocument."
        "wordprocessingml.document",
        "application/zip",
    },
    "xls": {"application/vnd.ms-excel", "application/x-ole-storage",
            "application/CDFV2"},
    "xlsx": {
        "application/vnd.openxmlformats-officedocument."
        "spreadsheetml.sheet",
        "application/zip",
    },
    "ppt": {"application/vnd.ms-powerpoint", "application/x-ole-storage",
            "application/CDFV2"},
    "pptx": {
        "application/vnd.openxmlformats-officedocument."
        "presentationml.presentation",
        "application/zip",
    },
    "rtf": {"application/rtf", "text/rtf"},
    "odt": {"application/vnd.oasis.opendocument.text", "application/zip"},
    "ods": {"application/vnd.oasis.opendocument.spreadsheet",
            "application/zip"},

    # Plain text / markup
    "txt": {"text/plain"},
    "csv": {"text/csv", "text/plain"},
    "log": {"text/plain"},
    "md": {"text/plain", "text/markdown"},
    "json": {"application/json", "text/plain"},
    "xml": {"application/xml", "text/xml", "text/plain"},
    "html": {"text/html"},
    "htm": {"text/html"},
    "yaml": {"text/plain", "application/yaml"},
    "yml": {"text/plain", "application/yaml"},

    # Images
    "jpg": {"image/jpeg"},
    "jpeg": {"image/jpeg"},
    "png": {"image/png"},
    "gif": {"image/gif"},
    "bmp": {"image/bmp", "image/x-ms-bmp"},
    "tif": {"image/tiff"},
    "tiff": {"image/tiff"},
    "webp": {"image/webp"},
    "svg": {"image/svg+xml", "text/xml", "text/plain"},

    # Audio / video
    "mp3": {"audio/mpeg"},
    "wav": {"audio/x-wav", "audio/wav"},
    "mp4": {"video/mp4"},
    "avi": {"video/x-msvideo"},
    "mov": {"video/quicktime"},
    "mkv": {"video/x-matroska"},

    # Archives
    "zip": {"application/zip"},
    "rar": {"application/x-rar", "application/vnd.rar"},
    "7z": {"application/x-7z-compressed"},
    "tar": {"application/x-tar"},
    "gz": {"application/gzip", "application/x-gzip"},

    # Executable / script (declared extension matches binary -> NOT an
    # anomaly. We DO list these because a ``.exe`` whose magic is
    # ``application/pdf`` is a different kind of disguise.)
    "exe": {"application/x-dosexec", "application/x-msdownload",
            "application/vnd.microsoft.portable-executable"},
    "dll": {"application/x-dosexec", "application/x-msdownload",
            "application/vnd.microsoft.portable-executable"},
    "msi": {"application/x-msi", "application/x-ole-storage"},
}

# MIME -> a "best guess" canonical extension shown in the report column.
_MIME_TO_EXT: dict[str, str] = {
    "application/pdf": "pdf",
    "application/zip": "zip",
    "application/x-rar": "rar",
    "application/vnd.rar": "rar",
    "application/x-7z-compressed": "7z",
    "application/x-tar": "tar",
    "application/gzip": "gz",
    "application/x-gzip": "gz",
    "application/x-dosexec": "exe",
    "application/x-msdownload": "exe",
    "application/vnd.microsoft.portable-executable": "exe",
    "application/x-ole-storage": "doc",
    "application/CDFV2": "doc",
    "application/msword": "doc",
    "application/vnd.ms-excel": "xls",
    "application/vnd.ms-powerpoint": "ppt",
    "application/json": "json",
    "application/xml": "xml",
    "application/rtf": "rtf",
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/gif": "gif",
    "image/bmp": "bmp",
    "image/tiff": "tif",
    "image/webp": "webp",
    "image/svg+xml": "svg",
    "audio/mpeg": "mp3",
    "audio/wav": "wav",
    "audio/x-wav": "wav",
    "video/mp4": "mp4",
    "video/x-msvideo": "avi",
    "video/quicktime": "mov",
    "video/x-matroska": "mkv",
    "text/plain": "txt",
    "text/csv": "csv",
    "text/html": "html",
    "text/xml": "xml",
    "text/markdown": "md",
}

# Extensions that are normally executable / scriptable. A *document*
# extension (pdf, docx, jpg, …) whose magic resolves to one of the
# entries in ``_EXECUTABLE_MIMES`` is the canonical "ransomware payload
# disguised as a document" pattern -> CRITICAL severity.
_EXECUTABLE_MIMES: set[str] = {
    "application/x-dosexec",
    "application/x-msdownload",
    "application/vnd.microsoft.portable-executable",
    "application/x-msi",
    "application/x-sharedlib",
    "application/x-mach-binary",
    "application/x-elf",
    "application/x-executable",
}

# Extensions where we're confident the file SHOULD be a benign document
# format. Used together with ``_EXECUTABLE_MIMES`` for the critical rule.
_DOCUMENT_LIKE_EXTS: set[str] = {
    "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "rtf", "odt",
    "ods", "txt", "csv", "log", "md", "json", "xml", "html", "htm",
    "yaml", "yml",
    "jpg", "jpeg", "png", "gif", "bmp", "tif", "tiff", "webp", "svg",
    "mp3", "wav", "mp4", "avi", "mov", "mkv",
    "zip", "rar", "7z", "tar", "gz",
}


# ──────────────────────────────────────────────────────────────────────
# Checker
# ──────────────────────────────────────────────────────────────────────


class ExtensionChecker:
    """Detects extension/MIME mismatches via libmagic.

    Lazy import — if ``python-magic`` (or its ``libmagic`` C dependency)
    is unavailable the checker logs ONE WARNING on first use and then
    returns ``None`` for every probe. This keeps the scanner working on
    minimal Linux/Windows installs where libmagic isn't shipped.
    """

    # Class-level guard so we only emit the import warning once even
    # when the scanner instantiates many checkers.
    _import_warned = False

    def __init__(self):
        self._magic = None
        self._magic_unavailable = False
        self._init_magic()

    # ── lazy import ──────────────────────────────────────────────────

    def _init_magic(self) -> None:
        """Try to construct a ``magic.Magic(mime=True)`` instance.

        Failure modes:
          * ``ImportError``  — python-magic not installed.
          * ``OSError`` / ``magic.MagicException`` — libmagic.so missing.

        On either failure we set ``_magic_unavailable=True`` and never
        retry (avoid log spam).
        """
        try:
            import magic  # type: ignore[import-not-found]
        except ImportError:
            self._magic_unavailable = True
            if not ExtensionChecker._import_warned:
                logger.warning(
                    "python-magic not installed; extension-check disabled. "
                    "Install via 'pip install -r requirements-accel.txt'."
                )
                ExtensionChecker._import_warned = True
            return

        try:
            # ``mime=True`` returns 'application/zip' instead of the
            # human-readable description.
            self._magic = magic.Magic(mime=True)
        except Exception as e:  # libmagic missing or broken
            self._magic_unavailable = True
            if not ExtensionChecker._import_warned:
                logger.warning(
                    "libmagic unavailable (%s); extension-check disabled. "
                    "On Windows install 'python-magic-bin'; on Linux ensure "
                    "the 'libmagic1' system package is present.", e,
                )
                ExtensionChecker._import_warned = True

    # ── public API ───────────────────────────────────────────────────

    @property
    def available(self) -> bool:
        return self._magic is not None and not self._magic_unavailable

    def check_file(self, path: str) -> Optional[ExtensionAnomaly]:
        """Probe one file. Returns the anomaly or None.

        Returns None when:
          * libmagic is unavailable
          * the file has no extension or an unknown one (no ground truth)
          * the detected MIME matches one of the expected MIMEs
          * the file can't be opened (PermissionError / OSError) — we log
            at debug and move on; this isn't an anomaly.
        """
        if not self.available:
            return None

        try:
            file_name = os.path.basename(path)
            ext = os.path.splitext(file_name)[1].lower().lstrip(".")
        except Exception:
            return None

        if not ext:
            return None
        expected = EXPECTED_MIMES.get(ext)
        if expected is None:
            # Unknown extension — we cannot say whether the magic matches.
            return None

        try:
            detected_mime = self._magic.from_file(path)
        except (PermissionError, FileNotFoundError) as e:
            logger.debug("extension-check skip %s: %s", path, e)
            return None
        except Exception as e:  # libmagic occasionally raises on weird files
            logger.debug("extension-check magic.from_file failed %s: %s",
                         path, e)
            return None

        if not detected_mime:
            return None

        # Normalise: libmagic sometimes returns 'application/zip; charset=binary'
        detected_mime = detected_mime.split(";", 1)[0].strip().lower()

        if detected_mime in expected:
            return None  # all good

        severity = self._severity_for(ext, detected_mime)
        detected_ext = _MIME_TO_EXT.get(detected_mime)

        return ExtensionAnomaly(
            file_path=path,
            declared_ext=ext,
            detected_mime=detected_mime,
            detected_ext=detected_ext,
            severity=severity,
        )

    def check_files(self, paths: Iterable[str]) -> List[ExtensionAnomaly]:
        """Convenience wrapper — call ``check_file`` over an iterable.

        Used by the scanner's optional post-walk pass. Returns only the
        anomaly hits (None entries dropped).
        """
        out: List[ExtensionAnomaly] = []
        if not self.available:
            return out
        for p in paths:
            try:
                hit = self.check_file(p)
            except Exception as e:  # never let one bad file break the pass
                logger.debug("extension-check unexpected error %s: %s", p, e)
                continue
            if hit is not None:
                out.append(hit)
        return out

    # ── severity rules ───────────────────────────────────────────────

    @staticmethod
    def _severity_for(declared_ext: str, detected_mime: str) -> str:
        """Map (declared ext, detected MIME) -> severity bucket.

        Critical: a document/image/text extension whose actual content
        is an executable. This is the ransomware-payload pattern the
        customer asked for.

        High: declared as a top-tier business document (pdf/docx/xlsx)
        but actually any *non*-archive container we don't recognise as
        plausibly that format.

        Medium: any other concrete mismatch with a known content type
        (e.g. a ``.png`` whose magic is ``image/jpeg``).

        Low: text-vs-text confusions (``.json`` detected as ``text/plain``
        — usually still readable, just not strictly typed).
        """
        ext = (declared_ext or "").lower()
        mime = (detected_mime or "").lower()

        # Critical: payload disguised as a document/image/text.
        if mime in _EXECUTABLE_MIMES and ext in _DOCUMENT_LIKE_EXTS:
            return "critical"

        # High: top-tier business document with a clearly different format.
        # We exclude the ZIP-like Office case here — that's already in the
        # expected set so this branch only fires when the MIME is something
        # else entirely.
        TOP_DOCS = {"pdf", "docx", "xlsx", "pptx", "doc", "xls", "ppt"}
        if ext in TOP_DOCS:
            # If libmagic reports an archive that ISN'T in the expected
            # set for this ext, treat as high (e.g. a ``.pdf`` that's a
            # ``.rar`` archive — common bundled payload).
            if mime in {"application/x-rar", "application/vnd.rar",
                        "application/x-7z-compressed",
                        "application/x-tar", "application/gzip",
                        "application/x-gzip"}:
                return "high"
            # Generic doc-vs-other-content.
            return "high"

        # Low: very loose text-family confusion (we still want to surface
        # but it isn't actionable security content).
        TEXTY = {"text/plain", "text/csv", "text/html", "text/markdown"}
        if mime in TEXTY and ext in {"json", "xml", "yaml", "yml", "html",
                                       "htm", "md", "csv", "txt", "log"}:
            return "low"

        return "medium"
