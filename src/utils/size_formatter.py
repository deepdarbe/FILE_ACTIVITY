"""Dosya boyutu formatlama yardımcıları."""


def format_size(size_bytes: int) -> str:
    """Byte değerini okunabilir formata çevir."""
    if size_bytes < 0:
        return "0 B"
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(size_bytes) < 1024.0:
            if unit == "B":
                return f"{size_bytes} B"
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} EB"


def parse_size(size_str: str) -> int:
    """Okunabilir boyutu byte'a çevir. Örn: '100MB' -> 104857600"""
    size_str = size_str.strip().upper()
    units = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
    for suffix, multiplier in sorted(units.items(), key=lambda x: -len(x[0])):
        if size_str.endswith(suffix):
            num = float(size_str[:-len(suffix)].strip())
            return int(num * multiplier)
    return int(float(size_str))
