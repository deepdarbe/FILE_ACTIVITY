"""FILE ACTIVITY - SQLite Veritabani Baslangic Scripti.

Kullanim: python scripts/init_db.py [--config config.yaml] [--force]

Bu script:
1. data/ dizinini olusturur
2. SQLite veritabani dosyasini olusturur
3. Tum tablolari olusturur
4. FTS5 arama indeksini olusturur
"""

import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.config_loader import load_config
from src.storage.database import Database


def main():
    parser = argparse.ArgumentParser(description="FILE ACTIVITY - SQLite Veritabani Olustur")
    parser.add_argument("--config", "-c", default="config.yaml", help="Config dosya yolu")
    parser.add_argument("--force", "-f", action="store_true", help="Mevcut DB varsa sil ve yeniden olustur")
    args = parser.parse_args()

    config = load_config(args.config)
    db_conf = config.get("database", {})
    db_path = db_conf.get("path", "data/file_activity.db")
    db_abs = os.path.abspath(db_path)

    print(f"\nFILE ACTIVITY - SQLite Veritabani Kurulumu")
    print(f"{'=' * 45}")
    print(f"  DB Yolu: {db_abs}")

    if args.force and os.path.exists(db_abs):
        os.remove(db_abs)
        print(f"  [!] Mevcut veritabani silindi")

    # Dizin olustur
    os.makedirs(os.path.dirname(db_abs), exist_ok=True)

    # Baglanti kur (tablolari otomatik olusturur)
    db = Database(db_conf)
    db.connect()

    # Tablo sayisi kontrol
    with db.get_cursor() as cur:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [r["name"] for r in cur.fetchall()]

    print(f"\n  Tablolar ({len(tables)}):")
    for t in tables:
        with db.get_cursor() as cur:
            cur.execute(f"SELECT COUNT(*) as cnt FROM [{t}]")
            cnt = cur.fetchone()["cnt"]
        print(f"    - {t} ({cnt:,} kayit)")

    db.close()
    print(f"\n  [OK] Veritabani hazir: {db_abs}\n")


if __name__ == "__main__":
    main()
