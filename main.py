"""FILE ACTIVITY - Windows File Share Analysis and Archiving System."""

import sys
import os
import io
import json
import logging

# Fix encoding for Windows consoles (cp1252/cp850 cannot handle Turkish chars)
if sys.stdout and hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr and hasattr(sys.stderr, 'buffer'):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import click

from src.utils.config_loader import load_config
from src.utils.logging_setup import setup_logging
from src.utils.size_formatter import format_size
from src.i18n.messages import set_language, t
from src.storage.database import Database, DatabaseConnectionError
from src.storage.models import Source, ArchivePolicy, ScheduledTask

logger = logging.getLogger("file_activity")

# Global context
pass_db = click.make_pass_decorator(Database, ensure=True)


class AppContext:
    def __init__(self):
        self.config = None
        self.db = None


pass_ctx = click.make_pass_decorator(AppContext, ensure=True)


@click.group()
@click.option("--config", "-c", default="config.yaml", help="Konfigurasyon dosyasi yolu")
@click.pass_context
def cli(ctx, config):
    """FILE ACTIVITY - Windows Dosya Paylasim Analiz ve Arsivleme Sistemi"""
    app = AppContext()
    app.config = load_config(config)
    setup_logging(app.config)
    set_language(app.config.get("general", {}).get("language", "tr"))

    # Store config path for error messages
    db_conf = app.config.get("database", {})
    db_conf["_config_path"] = config

    app.db = Database(db_conf)
    # Issue #38: opt-in tamper-evident audit chain. Default off → no behaviour change.
    audit_cfg = app.config.get("audit", {}) or {}
    if audit_cfg.get("chain_enabled", False):
        app.db.set_audit_chain_enabled(True)

    # Check which command is being invoked
    invoked = ctx.invoked_subcommand

    # 'check' and 'version' commands don't need DB
    no_db_commands = ["check", "version"]

    if invoked in no_db_commands:
        # Don't connect to DB for these commands
        pass
    else:
        try:
            app.db.connect()
        except DatabaseConnectionError as e:
            click.echo(str(e), err=True)
            ctx.exit(1)
            return

    ctx.ensure_object(AppContext)
    ctx.obj = app

    # Temizlik
    ctx.call_on_close(app.db.close)


# -----------------------------------------------
# CHECK KOMUTU - SQLite baglanti testi
# -----------------------------------------------

@cli.command("check")
@click.pass_context
def check_system(ctx):
    """Sistem ve SQLite baglanti kontrolu."""
    app = ctx.obj
    db_conf = app.config.get("database", {})

    click.echo("")
    click.echo("  FILE ACTIVITY - Sistem Kontrolu")
    click.echo("  " + "=" * 40)
    click.echo("")

    # Python
    import platform
    click.echo(f"  Python:     {platform.python_version()}")
    click.echo(f"  Platform:   {platform.platform()}")
    click.echo("")

    # Config
    import os
    db_path = db_conf.get("path", "data/file_activity.db")
    db_abs = os.path.abspath(db_path)
    db_exists = os.path.exists(db_abs)
    db_size = os.path.getsize(db_abs) if db_exists else 0
    click.echo(f"  DB Engine:  SQLite")
    click.echo(f"  DB Path:    {db_abs}")
    click.echo(f"  DB Exists:  {'Yes' if db_exists else 'No (will be created on first use)'}")
    if db_exists:
        click.echo(f"  DB Size:    {db_size / 1024 / 1024:.1f} MB")
    click.echo("")

    # Connection test
    click.echo("  Baglanti testi...")
    ok, msg = app.db.try_connect()
    if ok:
        click.echo("  [OK] " + msg)
        click.echo("")

        # Table check
        try:
            with app.db.get_cursor() as cur:
                cur.execute("""
                    SELECT name FROM sqlite_master
                    WHERE type='table' ORDER BY name
                """)
                tables = [r["name"] for r in cur.fetchall()]
            click.echo(f"  Tablolar ({len(tables)}):")
            for tbl in tables:
                cur2 = app.db._get_conn().cursor()
                cur2.execute(f"SELECT COUNT(*) as cnt FROM [{tbl}]")
                cnt = cur2.fetchone()["cnt"]
                cur2.close()
                click.echo(f"    - {tbl} ({cnt:,} kayit)")
        except Exception as e:
            click.echo(f"  Tablo kontrolu hatasi: {e}")
    else:
        click.echo("  [HATA] Baglanti basarisiz!")
        click.echo(msg)

    click.echo("")


@cli.command("version")
def version():
    """Versiyon bilgisi."""
    click.echo("FILE ACTIVITY v1.0.0")
    click.echo("Windows File Share Analysis & Archiving System")


# -----------------------------------------------
# SOURCE KOMUTLARI
# -----------------------------------------------

@cli.group()
def source():
    """Dosya paylasim kaynagi yonetimi."""
    pass


@source.command("add")
@click.option("--name", "-n", required=True, help="Kaynak adı")
@click.option("--path", "-p", required=True, help="UNC yolu (\\\\server\\share)")
@click.option("--archive-dest", "-a", default=None, help="Arşiv hedefi")
@click.pass_context
def source_add(ctx, name, path, archive_dest):
    """Yeni kaynak ekle."""
    app = ctx.obj
    s = Source(name=name, unc_path=path, archive_dest=archive_dest)
    try:
        sid = app.db.add_source(s)
        click.echo(t("source_added", name=name) + f" (ID: {sid})")
    except Exception as e:
        click.echo(f"Hata: {e}", err=True)


@source.command("list")
@click.pass_context
def source_list(ctx):
    """Kaynakları listele."""
    app = ctx.obj
    sources = app.db.get_sources()
    if not sources:
        click.echo("Kaynak bulunamadı. 'source add' ile ekleyin.")
        return

    click.echo(f"\n{'ID':<5} {'Ad':<20} {'UNC Yolu':<45} {'Arşiv Hedefi':<30} {'Durum':<8} {'Son Tarama'}")
    click.echo("─" * 140)
    for s in sources:
        status = "Aktif" if s.enabled else "Pasif"
        scanned = str(s.last_scanned_at)[:19] if s.last_scanned_at else "Hiç"
        click.echo(f"{s.id:<5} {s.name:<20} {s.unc_path:<45} {(s.archive_dest or '-'):<30} {status:<8} {scanned}")


@source.command("remove")
@click.option("--name", "-n", required=True, help="Silinecek kaynak adı")
@click.confirmation_option(prompt="Bu kaynağı ve tüm verilerini silmek istediğinize emin misiniz?")
@click.pass_context
def source_remove(ctx, name):
    """Kaynak sil."""
    app = ctx.obj
    if app.db.remove_source(name):
        click.echo(t("source_removed", name=name))
    else:
        click.echo(t("source_not_found", name=name), err=True)


@source.command("test")
@click.option("--name", "-n", required=True, help="Test edilecek kaynak adı")
@click.pass_context
def source_test(ctx, name):
    """Kaynak bağlantı testi."""
    from src.scanner.share_resolver import test_connectivity
    app = ctx.obj
    src = app.db.get_source_by_name(name)
    if not src:
        click.echo(t("source_not_found", name=name), err=True)
        return
    ok, msg = test_connectivity(src.unc_path)
    click.echo(msg)


# ═══════════════════════════════════════════════════
# SCAN KOMUTLARI
# ═══════════════════════════════════════════════════

@cli.command("scan")
@click.option("--source", "-s", "source_name", default=None, help="Kaynak adı")
@click.option("--all", "scan_all", is_flag=True, help="Tüm aktif kaynakları tara")
@click.pass_context
def scan(ctx, source_name, scan_all):
    """Dosya taraması başlat."""
    from src.scanner.file_scanner import FileScanner
    app = ctx.obj
    scanner = FileScanner(app.db, app.config)

    if scan_all:
        sources = app.db.get_sources(enabled_only=True)
    elif source_name:
        src = app.db.get_source_by_name(source_name)
        if not src:
            click.echo(t("source_not_found", name=source_name), err=True)
            return
        sources = [src]
    else:
        click.echo("--source veya --all belirtmelisiniz.", err=True)
        return

    for src in sources:
        click.echo(f"\n{'═' * 60}")
        click.echo(f"Taraniyor: {src.name} ({src.unc_path})")
        click.echo(f"{'═' * 60}")
        result = scanner.scan_source(src.id, src.name, src.unc_path)
        click.echo(f"Durum: {result['status']}")
        click.echo(f"Dosya: {result['total_files']:,} | Boyut: {format_size(result['total_size'])} | Hata: {result['errors']}")


# ═══════════════════════════════════════════════════
# REPORT KOMUTLARI
# ═══════════════════════════════════════════════════

@cli.group()
def report():
    """Raporlama komutları."""
    pass


def _resolve_source(app, source_name):
    """Kaynak adını çöz, yoksa hata ver."""
    src = app.db.get_source_by_name(source_name)
    if not src:
        click.echo(t("source_not_found", name=source_name), err=True)
        return None
    return src


@report.command("status")
@click.option("--source", "-s", "source_name", required=True, help="Kaynak adı")
@click.pass_context
def report_status(ctx, source_name):
    """Mevcut durum raporu."""
    from src.analyzer.report_generator import ReportGenerator
    app = ctx.obj
    src = _resolve_source(app, source_name)
    if not src:
        return

    gen = ReportGenerator(app.db, app.config)
    data = gen.generate_status_report(src.id)

    if "error" in data:
        click.echo(data["error"], err=True)
        return

    click.echo(f"\n╔══ DURUM RAPORU: {data['source']['name']} ══╗")
    click.echo(f"  Yol:           {data['source']['path']}")
    click.echo(f"  Toplam Dosya:  {data['total_files']:,}")
    click.echo(f"  Toplam Boyut:  {data['total_size_formatted']}")
    click.echo(f"  Uzantı Sayısı: {data['type_count']}")
    click.echo(f"  En Eski:       {data['oldest_file']}")
    click.echo(f"  En Yeni:       {data['newest_file']}")
    click.echo(f"  Rapor Zamanı:  {data['generated_at'][:19]}")


@report.command("frequency")
@click.option("--source", "-s", "source_name", required=True, help="Kaynak adı")
@click.option("--days", "-d", multiple=True, type=int, help="Özel gün aralıkları")
@click.pass_context
def report_frequency(ctx, source_name, days):
    """Erişim sıklığı raporu."""
    from src.analyzer.report_generator import ReportGenerator
    app = ctx.obj
    src = _resolve_source(app, source_name)
    if not src:
        return

    gen = ReportGenerator(app.db, app.config)
    custom = list(days) if days else None
    data = gen.generate_frequency_report(src.id, custom)

    if "error" in data:
        click.echo(data["error"], err=True)
        return

    click.echo(f"\n╔══ ERİŞİM SIKLIĞI: {src.name} ══╗\n")
    click.echo(f"  {'Kriter':<30} {'Dosya Sayısı':>15} {'Toplam Boyut':>15}")
    click.echo(f"  {'─' * 62}")
    for b in data["frequency"]:
        click.echo(f"  {b['label']:<30} {b['file_count']:>15,} {b['total_size_formatted']:>15}")


@report.command("types")
@click.option("--source", "-s", "source_name", required=True, help="Kaynak adı")
@click.option("--top", "-t", default=20, help="En üst N uzantı")
@click.pass_context
def report_types(ctx, source_name, top):
    """Dosya türü analiz raporu."""
    from src.analyzer.report_generator import ReportGenerator
    app = ctx.obj
    src = _resolve_source(app, source_name)
    if not src:
        return

    gen = ReportGenerator(app.db, app.config)
    data = gen.generate_type_report(src.id)

    if "error" in data:
        click.echo(data["error"], err=True)
        return

    click.echo(f"\n╔══ DOSYA TÜRÜ ANALİZİ: {src.name} ══╗\n")
    click.echo(f"  {'Uzantı':<12} {'Sayı':>10} {'Toplam':>12} {'Ortalama':>12} {'Min':>10} {'Max':>10}")
    click.echo(f"  {'─' * 70}")
    for t_row in data["types"][:top]:
        click.echo(
            f"  {t_row['extension']:<12} {t_row['file_count']:>10,} "
            f"{t_row['total_size_formatted']:>12} {t_row['avg_size_formatted']:>12} "
            f"{t_row['min_size_formatted']:>10} {t_row['max_size_formatted']:>10}"
        )


@report.command("sizes")
@click.option("--source", "-s", "source_name", required=True, help="Kaynak adı")
@click.pass_context
def report_sizes(ctx, source_name):
    """Boyut dağılımı raporu."""
    from src.analyzer.report_generator import ReportGenerator
    app = ctx.obj
    src = _resolve_source(app, source_name)
    if not src:
        return

    gen = ReportGenerator(app.db, app.config)
    data = gen.generate_size_report(src.id)

    if "error" in data:
        click.echo(data["error"], err=True)
        return

    click.echo(f"\n╔══ BOYUT DAĞILIMI: {src.name} ══╗\n")
    click.echo(f"  {'Kategori':<10} {'Aralık':<20} {'Dosya Sayısı':>15} {'Toplam Boyut':>15}")
    click.echo(f"  {'─' * 65}")
    for s_row in data["sizes"]:
        click.echo(
            f"  {s_row['label']:<10} {s_row['range_formatted']:<20} "
            f"{s_row['file_count']:>15,} {s_row['total_size_formatted']:>15}"
        )


@report.command("full")
@click.option("--source", "-s", "source_name", required=True, help="Kaynak adı")
@click.option("--format", "-f", "fmt", type=click.Choice(["json", "text"]), default="text")
@click.option("--output", "-o", default=None, help="Çıktı dosyası")
@click.pass_context
def report_full(ctx, source_name, fmt, output):
    """Tam birleştirilmiş rapor."""
    from src.analyzer.report_generator import ReportGenerator
    app = ctx.obj
    src = _resolve_source(app, source_name)
    if not src:
        return

    gen = ReportGenerator(app.db, app.config)
    data = gen.generate_full_report(src.id)

    if "error" in data:
        click.echo(data["error"], err=True)
        return

    if fmt == "json":
        content = json.dumps(data, indent=2, default=str, ensure_ascii=False)
    else:
        lines = []
        lines.append(f"═══ TAM RAPOR: {data['source']['name']} ═══")
        lines.append(f"Yol: {data['source']['path']}")
        s = data["summary"]
        lines.append(f"Dosya: {s['total_files']:,} | Boyut: {s['total_size_formatted']} | Uzantı: {s['type_count']}")
        lines.append("")
        lines.append("── Erişim Sıklığı ──")
        for b in data["frequency"]:
            lines.append(f"  {b['label']:<30} {b['file_count']:>10,}  {b['total_size_formatted']:>12}")
        lines.append("")
        lines.append("── Dosya Türleri (İlk 15) ──")
        for t_row in data["types"][:15]:
            lines.append(f"  .{t_row['extension']:<10} {t_row['file_count']:>8,}  {t_row['total_size_formatted']:>12}")
        lines.append("")
        lines.append("── Boyut Dağılımı ──")
        for s_row in data["sizes"]:
            lines.append(f"  {s_row['label']:<10} {s_row['file_count']:>10,}  {s_row['total_size_formatted']:>12}")
        content = "\n".join(lines)

    if output:
        with open(output, "w", encoding="utf-8") as f:
            f.write(content)
        click.echo(f"Rapor kaydedildi: {output}")
    else:
        click.echo(content)


# ═══════════════════════════════════════════════════
# ARCHIVE KOMUTLARI
# ═══════════════════════════════════════════════════

@cli.group()
def archive():
    """Arşivleme komutları."""
    pass


@archive.command("run")
@click.option("--source", "-s", "source_name", required=True, help="Kaynak adı")
@click.option("--policy", "-p", default=None, help="Politika adı")
@click.option("--days", "-d", type=int, default=None, help="Erişim gün eşiği")
@click.confirmation_option(prompt="Arşivleme başlatılacak. Devam etmek istiyor musunuz?")
@click.pass_context
def archive_run(ctx, source_name, policy, days):
    """Arşivleme çalıştır."""
    from src.archiver.archive_policy import ArchivePolicyEngine
    from src.archiver.archive_engine import ArchiveEngine
    app = ctx.obj

    src = _resolve_source(app, source_name)
    if not src:
        return
    if not src.archive_dest:
        click.echo("Bu kaynağın arşiv hedefi tanımlı değil. 'source add --archive-dest' ile ekleyin.", err=True)
        return

    scan_id = app.db.get_latest_scan_id(src.id)
    if not scan_id:
        click.echo(t("no_scan_data"), err=True)
        return

    policy_engine = ArchivePolicyEngine(app.db)

    if policy:
        files = policy_engine.get_files_by_policy(src.id, scan_id, policy)
        archived_by = policy
    elif days:
        files = policy_engine.get_files_by_days(src.id, scan_id, days)
        archived_by = f"manual:{days}days"
    else:
        click.echo("--policy veya --days belirtmelisiniz.", err=True)
        return

    if not files:
        click.echo("Arşivlenecek dosya bulunamadı.")
        return

    click.echo(f"Arşivlenecek: {len(files):,} dosya")

    engine = ArchiveEngine(app.db, app.config)
    result = engine.archive_files(files, src.archive_dest, src.unc_path, src.id, archived_by)

    click.echo(f"\nArşivlenen: {result['archived']:,}")
    click.echo(f"Başarısız:  {result['failed']:,}")
    click.echo(f"Boyut:      {result['total_size_formatted']}")


@archive.command("dry-run")
@click.option("--source", "-s", "source_name", required=True, help="Kaynak adı")
@click.option("--days", "-d", type=int, required=True, help="Erişim gün eşiği")
@click.pass_context
def archive_dry_run(ctx, source_name, days):
    """Arşivleme önizleme (dosya taşımadan)."""
    from src.archiver.archive_policy import ArchivePolicyEngine
    from src.archiver.archive_engine import ArchiveEngine
    app = ctx.obj

    src = _resolve_source(app, source_name)
    if not src:
        return

    scan_id = app.db.get_latest_scan_id(src.id)
    if not scan_id:
        click.echo(t("no_scan_data"), err=True)
        return

    policy_engine = ArchivePolicyEngine(app.db)
    files = policy_engine.get_files_by_days(src.id, scan_id, days)

    if not files:
        click.echo("Arşivlenecek dosya bulunamadı.")
        return

    total_size = sum(f["file_size"] for f in files)
    click.echo(f"\n[KURU ÇALIŞTIRMA] {len(files):,} dosya arşivlenecek ({format_size(total_size)})\n")
    for f in files[:50]:
        click.echo(f"  {f['relative_path']:<60} {format_size(f['file_size']):>10}")
    if len(files) > 50:
        click.echo(f"  ... ve {len(files) - 50:,} dosya daha")


@archive.command("search")
@click.option("--query", "-q", required=True, help="Arama sorgusu")
@click.option("--extension", "-e", default=None, help="Uzantı filtresi")
@click.option("--page", default=1, help="Sayfa numarası")
@click.pass_context
def archive_search(ctx, query, extension, page):
    """Arşiv indeksinde ara."""
    app = ctx.obj
    result = app.db.search_archived_files(query, extension=extension, page=page)

    click.echo(f"\nToplam: {result['total']} sonuç (Sayfa {result['page']})\n")
    if not result["results"]:
        click.echo("Sonuç bulunamadı.")
        return

    click.echo(f"  {'ID':<6} {'Dosya Adı':<30} {'Boyut':>10} {'Arşiv Tarihi':<20} {'Orijinal Yol'}")
    click.echo(f"  {'─' * 100}")
    for r in result["results"]:
        click.echo(
            f"  {r['id']:<6} {r['file_name']:<30} {format_size(r['file_size']):>10} "
            f"{str(r['archived_at'])[:19]:<20} {r['original_path']}"
        )


@archive.command("stats")
@click.pass_context
def archive_stats(ctx):
    """Arşiv istatistikleri."""
    app = ctx.obj
    stats = app.db.get_archive_stats()
    click.echo(f"\n╔══ ARŞİV İSTATİSTİKLERİ ══╗")
    click.echo(f"  Toplam Arşivlenen: {stats['total_archived']:,}")
    click.echo(f"  Şu An Arşivde:    {stats['currently_archived']:,}")
    click.echo(f"  Geri Yüklenen:     {stats['total_restored']:,}")
    click.echo(f"  Arşiv Boyutu:      {format_size(stats['archived_size'])}")
    click.echo(f"  Kaynak Sayısı:     {stats['source_count']}")


# ═══════════════════════════════════════════════════
# RESTORE KOMUTU
# ═══════════════════════════════════════════════════

@cli.command("restore")
@click.option("--id", "archive_id", type=int, default=None, help="Arşiv ID")
@click.option("--path", "original_path", default=None, help="Orijinal dosya yolu")
@click.pass_context
def restore(ctx, archive_id, original_path):
    """Arşivden geri yükle."""
    from src.archiver.restore_engine import RestoreEngine
    app = ctx.obj
    engine = RestoreEngine(app.db)

    if archive_id:
        result = engine.restore_by_id(archive_id)
    elif original_path:
        result = engine.restore_by_path(original_path)
    else:
        click.echo("--id veya --path belirtmelisiniz.", err=True)
        return

    if result["success"]:
        click.echo(f"Geri yüklendi: {result['original_path']}")
    else:
        click.echo(f"Hata: {result['error']}", err=True)


# ═══════════════════════════════════════════════════
# POLICY KOMUTLARI
# ═══════════════════════════════════════════════════

@cli.group()
def policy():
    """Arşiv politikası yönetimi."""
    pass


@policy.command("add")
@click.option("--name", "-n", required=True, help="Politika adı")
@click.option("--source", "-s", "source_name", default=None, help="Kaynak adı (boş = tümü)")
@click.option("--access-days", type=int, default=None, help="Erişim gün eşiği")
@click.option("--modify-days", type=int, default=None, help="Değişiklik gün eşiği")
@click.option("--min-size", type=int, default=None, help="Minimum boyut (bytes)")
@click.option("--max-size", type=int, default=None, help="Maksimum boyut (bytes)")
@click.option("--extensions", default=None, help="Uzantılar (virgülle ayrılmış)")
@click.option("--exclude-ext", default=None, help="Hariç uzantılar (virgülle ayrılmış)")
@click.pass_context
def policy_add(ctx, name, source_name, access_days, modify_days, min_size, max_size, extensions, exclude_ext):
    """Yeni arşiv politikası oluştur."""
    from src.archiver.archive_policy import ArchivePolicyEngine
    app = ctx.obj

    source_id = None
    if source_name:
        src = app.db.get_source_by_name(source_name)
        if not src:
            click.echo(t("source_not_found", name=source_name), err=True)
            return
        source_id = src.id

    ext_list = [e.strip().lower().lstrip(".") for e in extensions.split(",")] if extensions else None
    excl_list = [e.strip().lower().lstrip(".") for e in exclude_ext.split(",")] if exclude_ext else None

    engine = ArchivePolicyEngine(app.db)
    rules_json = engine.create_policy_rules(
        access_days=access_days, modify_days=modify_days,
        min_size=min_size, max_size=max_size,
        extensions=ext_list, exclude_extensions=excl_list
    )

    pol = ArchivePolicy(name=name, source_id=source_id, rules_json=rules_json)
    pid = app.db.add_policy(pol)
    click.echo(t("policy_added", name=name) + f" (ID: {pid})")


@policy.command("list")
@click.pass_context
def policy_list(ctx):
    """Politikaları listele."""
    app = ctx.obj
    policies = app.db.get_policies()
    if not policies:
        click.echo("Politika bulunamadı.")
        return

    click.echo(f"\n{'ID':<5} {'Ad':<20} {'Kaynak':<20} {'Kurallar':<40} {'Durum'}")
    click.echo("─" * 90)
    for p in policies:
        src_name = p.get("source_name") or "Tümü"
        status = "Aktif" if p["enabled"] else "Pasif"
        rules = str(p["rules_json"])[:38]
        click.echo(f"{p['id']:<5} {p['name']:<20} {src_name:<20} {rules:<40} {status}")


@policy.command("remove")
@click.option("--name", "-n", required=True, help="Silinecek politika adı")
@click.pass_context
def policy_remove(ctx, name):
    """Politika sil."""
    app = ctx.obj
    if app.db.remove_policy(name):
        click.echo(t("policy_removed", name=name))
    else:
        click.echo("Politika bulunamadı.", err=True)


# ═══════════════════════════════════════════════════
# SCHEDULE KOMUTLARI
# ═══════════════════════════════════════════════════

@cli.group()
def schedule():
    """Zamanlanmış görev yönetimi."""
    pass


@schedule.command("add")
@click.option("--type", "-t", "task_type", required=True, type=click.Choice(["scan", "archive"]))
@click.option("--source", "-s", "source_name", required=True, help="Kaynak adı")
@click.option("--policy", "-p", "policy_name", default=None, help="Politika adı (archive için)")
@click.option("--cron", "-c", required=True, help="Cron ifadesi (örn: '0 2 * * 0')")
@click.pass_context
def schedule_add(ctx, task_type, source_name, policy_name, cron):
    """Zamanlanmış görev ekle."""
    app = ctx.obj
    src = _resolve_source(app, source_name)
    if not src:
        return

    policy_id = None
    if policy_name:
        pol = app.db.get_policy_by_name(policy_name)
        if not pol:
            click.echo("Politika bulunamadı.", err=True)
            return
        policy_id = pol["id"]

    task = ScheduledTask(
        task_type=task_type, source_id=src.id,
        policy_id=policy_id, cron_expression=cron
    )
    tid = app.db.add_scheduled_task(task)
    click.echo(t("schedule_added", type=task_type, cron=cron) + f" (ID: {tid})")


@schedule.command("list")
@click.pass_context
def schedule_list(ctx):
    """Zamanlanmış görevleri listele."""
    app = ctx.obj
    tasks = app.db.get_scheduled_tasks()
    if not tasks:
        click.echo("Zamanlanmış görev bulunamadı.")
        return

    click.echo(f"\n{'ID':<5} {'Tür':<10} {'Kaynak':<20} {'Politika':<15} {'Cron':<20} {'Durum':<8} {'Son Çalışma'}")
    click.echo("─" * 100)
    for t_row in tasks:
        status = "Aktif" if t_row["enabled"] else "Pasif"
        last = str(t_row["last_run_at"])[:19] if t_row["last_run_at"] else "Hiç"
        pol_name = t_row.get("policy_name") or "-"
        click.echo(
            f"{t_row['id']:<5} {t_row['task_type']:<10} {t_row.get('source_name','?'):<20} "
            f"{pol_name:<15} {t_row['cron_expression']:<20} {status:<8} {last}"
        )


@schedule.command("remove")
@click.option("--id", "task_id", required=True, type=int, help="Görev ID")
@click.pass_context
def schedule_remove(ctx, task_id):
    """Zamanlanmış görev sil."""
    app = ctx.obj
    if app.db.remove_scheduled_task(task_id):
        click.echo(t("schedule_removed", id=task_id))
    else:
        click.echo("Görev bulunamadı.", err=True)


# ═══════════════════════════════════════════════════
# DASHBOARD KOMUTU
# ═══════════════════════════════════════════════════

@cli.command("dashboard")
@click.option("--host", "-h", default=None, help="Host adresi")
@click.option("--port", "-p", type=int, default=None, help="Port numarası")
@click.pass_context
def dashboard(ctx, host, port):
    """Web dashboard başlat."""
    import uvicorn
    from src.dashboard.api import create_app
    app = ctx.obj

    dash_config = app.config.get("dashboard", {})
    host = host or dash_config.get("host", "0.0.0.0")
    port = port or dash_config.get("port", 8085)

    fastapi_app = create_app(app.db, app.config)
    click.echo(t("dashboard_started", host=host, port=port))
    uvicorn.run(fastapi_app, host=host, port=port, log_level="info")


# ═══════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    cli()
