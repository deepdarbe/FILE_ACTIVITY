"""Rapor HTML/JSON dışa aktarma modülü.

Tarama sonrası otomatik rapor üretir ve dosyaya kaydeder.
"""

import os
import json
import logging
from datetime import datetime

from src.utils.size_formatter import format_size

logger = logging.getLogger("file_activity.analyzer.exporter")


class ReportExporter:
    """Rapor verilerini HTML ve JSON formatında dışa aktarır."""

    def __init__(self, config: dict):
        self.config = config
        self.output_dir = config.get("reports", {}).get("output_dir", "reports")

    def export_full_report(self, data: dict, source_name: str) -> dict:
        """Tam raporu hem JSON hem HTML olarak kaydet.

        Returns:
            {"json_path": str, "html_path": str}
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = source_name.replace("\\", "_").replace("/", "_").replace(" ", "_")

        # Dizin oluştur
        os.makedirs(self.output_dir, exist_ok=True)

        base = f"{safe_name}_{timestamp}"
        json_path = os.path.join(self.output_dir, f"{base}.json")
        html_path = os.path.join(self.output_dir, f"{base}.html")

        # JSON
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str, ensure_ascii=False)

        # HTML
        html = self._render_html(data)
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)

        logger.info(f"Rapor kaydedildi: {html_path}")
        return {"json_path": json_path, "html_path": html_path}

    def _render_html(self, data: dict) -> str:
        """Rapor verisinden HTML oluştur."""
        source = data.get("source", {})
        summary = data.get("summary", {})
        frequency = data.get("frequency", [])
        types = data.get("types", [])
        sizes = data.get("sizes", [])
        generated = data.get("generated_at", "")[:19]

        # Frequency bar chart
        freq_max = max((f["file_count"] for f in frequency), default=1)
        freq_bars = ""
        for f in frequency:
            pct = (f["file_count"] / freq_max * 100) if freq_max else 0
            freq_bars += f"""
            <div class="bar-row">
                <div class="bar-label">{f['label']}</div>
                <div class="bar-track"><div class="bar-fill" style="width:{pct:.0f}%"></div></div>
                <div class="bar-value">{f['file_count']:,} dosya ({f['total_size_formatted']})</div>
            </div>"""

        # Types table
        type_rows = ""
        for i, t in enumerate(types[:20]):
            type_rows += f"""
            <tr>
                <td>{i+1}</td>
                <td>.{t['extension']}</td>
                <td class="num">{t['file_count']:,}</td>
                <td class="num">{t['total_size_formatted']}</td>
                <td class="num">{t['avg_size_formatted']}</td>
                <td class="num">{t['min_size_formatted']}</td>
                <td class="num">{t['max_size_formatted']}</td>
            </tr>"""

        # Size chart
        size_max = max((s["file_count"] for s in sizes), default=1)
        colors = ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#ec4899']
        size_bars = ""
        for i, s in enumerate(sizes):
            pct = (s["file_count"] / size_max * 100) if size_max else 0
            color = colors[i % len(colors)]
            size_bars += f"""
            <div class="bar-row">
                <div class="bar-label">{s['label']} ({s['range_formatted']})</div>
                <div class="bar-track"><div class="bar-fill" style="width:{pct:.0f}%;background:{color}"></div></div>
                <div class="bar-value">{s['file_count']:,} dosya ({s['total_size_formatted']})</div>
            </div>"""

        # Arşiv önerisi
        archivable_files = 0
        archivable_size = 0
        for f in frequency:
            if f.get("days", 0) >= 365:
                archivable_files = f["file_count"]
                archivable_size = f.get("total_size", 0)
                break
        archivable_size_fmt = format_size(archivable_size)
        total_files = summary.get("total_files", 0)
        archive_pct = (archivable_files / total_files * 100) if total_files else 0

        return f"""<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FILE ACTIVITY - Tarama Raporu: {source.get('name','')}</title>
<style>
:root {{
    --bg: #0f172a; --bg2: #1e293b; --text: #f1f5f9;
    --muted: #94a3b8; --accent: #3b82f6; --success: #22c55e;
    --warning: #f59e0b; --danger: #ef4444; --border: #334155;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: 'Segoe UI', sans-serif; background: var(--bg); color: var(--text); padding: 32px; }}
.container {{ max-width: 1100px; margin: 0 auto; }}
h1 {{ font-size: 24px; color: var(--accent); margin-bottom: 4px; }}
.subtitle {{ color: var(--muted); font-size: 13px; margin-bottom: 32px; }}
.cards {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 16px; margin-bottom: 32px; }}
.card {{ background: var(--bg2); border: 1px solid var(--border); border-radius: 8px; padding: 20px; }}
.card-label {{ font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; }}
.card-value {{ font-size: 28px; font-weight: 700; margin-top: 6px; }}
.card-sub {{ font-size: 12px; color: var(--muted); margin-top: 4px; }}
.section {{ background: var(--bg2); border: 1px solid var(--border); border-radius: 8px; padding: 24px; margin-bottom: 24px; }}
.section h2 {{ font-size: 16px; font-weight: 600; margin-bottom: 16px; }}
table {{ width: 100%; border-collapse: collapse; }}
th {{ text-align: left; padding: 8px 12px; font-size: 11px; text-transform: uppercase; color: var(--muted); border-bottom: 1px solid var(--border); background: rgba(0,0,0,0.2); }}
td {{ padding: 8px 12px; font-size: 13px; border-bottom: 1px solid rgba(51,65,85,0.5); }}
.num {{ text-align: right; }}
.bar-row {{ display: flex; align-items: center; margin-bottom: 8px; }}
.bar-label {{ width: 200px; font-size: 12px; color: var(--muted); flex-shrink: 0; }}
.bar-track {{ flex: 1; height: 22px; background: var(--bg); border-radius: 4px; overflow: hidden; margin: 0 12px; }}
.bar-fill {{ height: 100%; background: var(--accent); border-radius: 4px; }}
.bar-value {{ width: 180px; font-size: 12px; color: var(--muted); text-align: right; flex-shrink: 0; }}
.recommendation {{ background: rgba(59,130,246,0.1); border: 1px solid var(--accent); border-radius: 8px; padding: 20px; margin-bottom: 24px; }}
.recommendation h3 {{ color: var(--accent); margin-bottom: 8px; font-size: 14px; }}
.recommendation p {{ font-size: 13px; color: var(--text); line-height: 1.6; }}
.stat-highlight {{ color: var(--warning); font-weight: 700; }}
.footer {{ text-align: center; color: var(--muted); font-size: 11px; margin-top: 32px; padding-top: 16px; border-top: 1px solid var(--border); }}
@media print {{
    body {{ background: white; color: #333; padding: 16px; }}
    .card, .section, .recommendation {{ border-color: #ddd; background: #fafafa; }}
    .bar-track {{ background: #eee; }}
    .bar-fill {{ background: #2563eb; }}
    .card-label, .bar-label, .bar-value, th {{ color: #666; }}
}}
</style>
</head>
<body>
<div class="container">

<h1>FILE ACTIVITY - Tarama Raporu</h1>
<div class="subtitle">{source.get('name','')} | {source.get('path','')} | {generated}</div>

<!-- OZET KARTLARI -->
<div class="cards">
    <div class="card">
        <div class="card-label">Toplam Dosya</div>
        <div class="card-value">{summary.get('total_files', 0):,}</div>
    </div>
    <div class="card">
        <div class="card-label">Toplam Boyut</div>
        <div class="card-value">{summary.get('total_size_formatted', '-')}</div>
    </div>
    <div class="card">
        <div class="card-label">Uzanti Sayisi</div>
        <div class="card-value">{summary.get('type_count', 0)}</div>
    </div>
    <div class="card">
        <div class="card-label">En Eski Dosya</div>
        <div class="card-value" style="font-size:16px">{(summary.get('oldest_file') or '-')[:10]}</div>
    </div>
    <div class="card">
        <div class="card-label">En Yeni Dosya</div>
        <div class="card-value" style="font-size:16px">{(summary.get('newest_file') or '-')[:10]}</div>
    </div>
</div>

<!-- ARSIV ONERISI -->
<div class="recommendation">
    <h3>Arsivleme Onerisi</h3>
    <p>
        1 yildan uzun suredir erisilemyen <span class="stat-highlight">{archivable_files:,}</span> dosya
        (<span class="stat-highlight">{archivable_size_fmt}</span>) arsivlenerek
        toplamda <span class="stat-highlight">%{archive_pct:.1f}</span> alan kazanilabilir.
    </p>
</div>

<!-- ERISIM SIKLIGI -->
<div class="section">
    <h2>Erisim Sikligi Dagilimi</h2>
    {freq_bars}
</div>

<!-- DOSYA TURLERI -->
<div class="section">
    <h2>Dosya Turu Analizi (Ilk 20)</h2>
    <table>
        <thead><tr><th>#</th><th>Uzanti</th><th class="num">Sayi</th><th class="num">Toplam</th><th class="num">Ortalama</th><th class="num">Min</th><th class="num">Max</th></tr></thead>
        <tbody>{type_rows}</tbody>
    </table>
</div>

<!-- BOYUT DAGILIMI -->
<div class="section">
    <h2>Boyut Dagilimi</h2>
    {size_bars}
</div>

<div class="footer">
    FILE ACTIVITY v1.0 | Rapor olusturulma: {generated} | Bu rapor otomatik olarak uretilmistir.
</div>

</div>
</body>
</html>"""
