"""Kullanici verimlilik skoru + standart uyumsuzluk raporu.

Bir kullanici icin son taranan dosyalara bakarak 0-100 arasi bir
verimlilik skoru uretir ve iyilestirme icin somut onerileri listeler.

Skor algoritmasi (base 100, her faktor kirmizi puan dusurur):

    Faktor                              Ceza (puan)             Sinir
    ----------------------------------  ----------------------  -----
    Erismedigi eski dosyalar (1+ yil)   -2 her 10 dosya         -30
    Buyuk dosyalar (>100 MB)            -1 her dosya            -15
    MIT adlandirma ihlalleri            -1 her 10 dosya         -20
    Kopya dosyalar (ayni ad+boyut)      -1 her 5 kopya          -15
    Dormant hesap (90+ gun hareketsiz)  -10                     -10
    Dizilimsiz ekstrem dosya buyumesi   -1 her aktif gun        -5
                                                                -----
                                              TOPLAM MAX CEZA:  -95
    (En dusuk skor 5, ciddi uyumsuzlukta bile pozitif kalir.)

Sonuc:
    {
      "username": "jdoe",
      "score": 78,
      "grade": "B",            # A 90+, B 75+, C 60+, D 45+, E <45
      "factors": [
        {"name": "stale_files", "count": 42, "penalty": 8, "max": 30,
         "label": "1+ yildir erisilmeyen dosyalar"},
        ...
      ],
      "non_compliance": {
        "stale_files": 42,
        "oversized_files": 3,
        "naming_violations": 15,
        "duplicate_files": 8,
        "dormant": false,
      },
      "suggestions": [
        "42 adet 1 yildir erismediginiz dosya var - arsivlemeyi dusunun",
        "3 adet 100MB'dan buyuk dosya tespit edildi - sikistirma faydali olabilir",
        ...
      ],
      "scan_id": 12,
      "computed_at": "2026-04-21T08:15:00"
    }
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("file_activity.efficiency_score")

STALE_THRESHOLD_DAYS = 365
OVERSIZED_THRESHOLD_BYTES = 100 * 1024 * 1024  # 100 MB
DORMANT_THRESHOLD_DAYS = 90


def _grade(score: int) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 45:
        return "D"
    return "E"


def _pluralize(count: int, singular: str, plural: str) -> str:
    return f"{count} {singular if count == 1 else plural}"


def compute_user_score(db, username: str,
                        source_id: Optional[int] = None,
                        scan_id: Optional[int] = None) -> dict:
    """Kullanici verimlilik skoru hesapla.

    source_id verilmezse tum kaynaklardaki son tamamlanmis tarama kullanilir.
    scan_id verilirse direk o scan uzerinde calisir (source_id opsiyonel).

    username bulunamazsa veya hic dosya sahibi degilse score=100 ile temiz doner.
    """
    if scan_id is None and source_id is not None:
        scan_id = db.get_latest_scan_id(source_id, include_running=False)
    elif scan_id is None and source_id is None:
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT id FROM scan_runs WHERE status = 'completed'
                ORDER BY started_at DESC LIMIT 1
            """)
            row = cur.fetchone()
            scan_id = row["id"] if row else None

    if scan_id is None:
        return _empty_result(username, reason="Tarama verisi bulunamadi")

    factors: list = []
    total_penalty = 0
    nc = {
        "stale_files": 0,
        "oversized_files": 0,
        "naming_violations": 0,
        "duplicate_files": 0,
        "dormant": False,
    }

    with db.get_cursor() as cur:
        # Toplam kullanici dosyalari (hicbiri yoksa erken don)
        cur.execute("""
            SELECT COUNT(*) AS total, COALESCE(SUM(file_size), 0) AS total_size
            FROM scanned_files WHERE scan_id = ? AND owner = ?
        """, (scan_id, username))
        row = cur.fetchone()
        total_files = row["total"]
        total_size = row["total_size"]

        if total_files == 0:
            return _empty_result(username, scan_id=scan_id,
                                 reason="Bu kullaniciya ait dosya bulunamadi")

        # 1. Stale (1+ yil erisilmeyen) dosyalar
        stale_cutoff = (datetime.now() - timedelta(days=STALE_THRESHOLD_DAYS)).strftime('%Y-%m-%d')
        cur.execute("""
            SELECT COUNT(*) AS cnt FROM scanned_files
            WHERE scan_id = ? AND owner = ?
              AND last_access_time IS NOT NULL
              AND last_access_time <= ?
        """, (scan_id, username, stale_cutoff))
        stale_count = cur.fetchone()["cnt"]
        if stale_count > 0:
            penalty = min(30, (stale_count // 10) * 2)
            nc["stale_files"] = stale_count
            if penalty > 0:
                total_penalty += penalty
                factors.append({
                    "name": "stale_files",
                    "label": "1+ yildir erisilmeyen dosyalar",
                    "count": stale_count,
                    "penalty": penalty,
                    "max": 30,
                })

        # 2. Oversized dosyalar (>100 MB)
        cur.execute("""
            SELECT COUNT(*) AS cnt FROM scanned_files
            WHERE scan_id = ? AND owner = ? AND file_size > ?
        """, (scan_id, username, OVERSIZED_THRESHOLD_BYTES))
        oversized_count = cur.fetchone()["cnt"]
        if oversized_count > 0:
            penalty = min(15, oversized_count)
            nc["oversized_files"] = oversized_count
            if penalty > 0:
                total_penalty += penalty
                factors.append({
                    "name": "oversized_files",
                    "label": "100 MB'dan buyuk dosyalar",
                    "count": oversized_count,
                    "penalty": penalty,
                    "max": 15,
                })

        # 3. Kopya dosyalar (ayni file_name + file_size > 1 kez)
        cur.execute("""
            SELECT COALESCE(SUM(cnt - 1), 0) AS dup_extras FROM (
                SELECT file_name, file_size, COUNT(*) AS cnt
                FROM scanned_files
                WHERE scan_id = ? AND owner = ? AND file_size > 0
                GROUP BY file_name, file_size
                HAVING COUNT(*) > 1
            )
        """, (scan_id, username))
        dup_extras = cur.fetchone()["dup_extras"] or 0
        if dup_extras > 0:
            nc["duplicate_files"] = dup_extras
            penalty = min(15, dup_extras // 5)
            if penalty > 0:
                total_penalty += penalty
                factors.append({
                    "name": "duplicate_files",
                    "label": "Kopya dosyalar (ayni isim+boyut)",
                    "count": dup_extras,
                    "penalty": penalty,
                    "max": 15,
                })

    # 4. MIT naming violations — analyzer'i kullanici dosyalari uzerinde calistir
    try:
        from src.scanner.file_scanner import MITNamingAnalyzer
        analyzer = MITNamingAnalyzer()
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT file_path, file_name FROM scanned_files
                WHERE scan_id = ? AND owner = ?
            """, (scan_id, username))
            for row in cur:
                analyzer.analyze(row["file_path"], row["file_name"])
        report = analyzer.get_report()
        violations = int(report.get("total_issues", 0) or 0)
        if violations > 0:
            nc["naming_violations"] = violations
            penalty = min(20, violations // 10)
            if penalty > 0:
                total_penalty += penalty
                factors.append({
                    "name": "naming_violations",
                    "label": "MIT adlandirma ihlalleri",
                    "count": violations,
                    "penalty": penalty,
                    "max": 20,
                })
    except Exception as e:
        logger.debug("MIT naming analyzer calistirilamadi: %s", e)

    # 5. Dormant account (90+ gun aktivite yok) — user_access_logs'tan
    try:
        with db.get_cursor() as cur:
            cur.execute("""
                SELECT MAX(access_time) AS last_seen FROM user_access_logs
                WHERE username = ?
            """, (username,))
            row = cur.fetchone()
            last_seen = row["last_seen"] if row else None
        if last_seen:
            try:
                last_dt = datetime.strptime(last_seen[:19], "%Y-%m-%d %H:%M:%S")
                days_idle = (datetime.now() - last_dt).days
                if days_idle >= DORMANT_THRESHOLD_DAYS:
                    total_penalty += 10
                    nc["dormant"] = True
                    factors.append({
                        "name": "dormant",
                        "label": f"{days_idle} gundur aktif degil",
                        "count": days_idle,
                        "penalty": 10,
                        "max": 10,
                    })
            except (ValueError, TypeError):
                pass
        # last_seen yoksa event log hic gelmemis; cezalandirmaca (data gap'i)
    except Exception as e:
        logger.debug("Dormant check hata: %s", e)

    score = max(5, 100 - total_penalty)
    suggestions = _build_suggestions(nc, total_files)

    return {
        "username": username,
        "score": score,
        "grade": _grade(score),
        "total_penalty": total_penalty,
        "factors": factors,
        "non_compliance": nc,
        "suggestions": suggestions,
        "total_files": total_files,
        "total_size": total_size,
        "scan_id": scan_id,
        "computed_at": datetime.now().isoformat(),
    }


def _build_suggestions(nc: dict, total_files: int) -> list:
    out = []
    if nc.get("stale_files", 0) > 0:
        out.append(
            f"{nc['stale_files']} adet 1 yildir erismediginiz dosya var — "
            f"arsivleme once buradan baslamali."
        )
    if nc.get("oversized_files", 0) > 0:
        out.append(
            f"{nc['oversized_files']} adet 100 MB'dan buyuk dosya tespit edildi — "
            f"sikistirma veya uzun sureli depolamaya tasima dusunun."
        )
    if nc.get("duplicate_files", 0) > 0:
        out.append(
            f"{nc['duplicate_files']} kopya dosya fark edildi — "
            f"birini tutup digerlerini silmek alan kazandirir."
        )
    if nc.get("naming_violations", 0) > 0:
        out.append(
            f"{nc['naming_violations']} dosya MIT adlandirma standartlarina uymuyor — "
            f"arama/filtrelemede zorluk yasayabilirsiniz."
        )
    if nc.get("dormant"):
        out.append(
            "Son 90 gunden beri sisteme aktif erisim kaydiniz yok — "
            "hesap yoneticinizle konusmanizi oneririz."
        )
    if not out and total_files > 0:
        out.append("Herhangi bir uyumsuzluk tespit edilmedi — iyi gidiyorsunuz.")
    return out


def _empty_result(username: str, scan_id: Optional[int] = None,
                   reason: str = "") -> dict:
    return {
        "username": username,
        "score": 100,
        "grade": "A",
        "total_penalty": 0,
        "factors": [],
        "non_compliance": {
            "stale_files": 0, "oversized_files": 0, "naming_violations": 0,
            "duplicate_files": 0, "dormant": False,
        },
        "suggestions": [reason] if reason else [],
        "total_files": 0,
        "total_size": 0,
        "scan_id": scan_id,
        "computed_at": datetime.now().isoformat(),
    }
