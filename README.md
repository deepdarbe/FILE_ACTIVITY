# FILE ACTIVITY

**Windows File Share Analysis, Archiving & Compliance System**

[🇹🇷 Türkçe](#-türkçe) | [🇬🇧 English](#-english)

---

# 🇬🇧 English

Enterprise file share analysis tool that detects risky/stale/duplicate files, archives them securely, and checks compliance with MIT Libraries naming standards.

## Quick Setup

### Option A — Source install from master (recommended, always latest)

**Requirements:** Python 3.10+ on target (check with `python --version`).

```powershell
# PowerShell (Run as Admin):
powershell -ExecutionPolicy Bypass -Command "irm https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/setup-source.ps1 | iex"
```

Installs to `C:\FileActivity\`, creates a Python venv, installs all
dependencies (including DuckDB analytics), configures firewall, and offers to
start the dashboard. Re-run the same command any time to update — data,
logs, reports, and `config.yaml` are preserved.

To update later: `C:\FileActivity\update.cmd`

### Option B — EXE release install (no Python needed)

Requires a prebuilt EXE released on GitHub Releases.

```powershell
# PowerShell (Run as Admin):
$f="$env:TEMP\fa.ps1"; (New-Object Net.WebClient).DownloadFile("https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/setup.ps1",$f); powershell -ExecutionPolicy Bypass -File $f
```

### Manual install
```powershell
# Source:
git clone https://github.com/deepdarbe/FILE_ACTIVITY.git
cd FILE_ACTIVITY
python -m venv .venv && .venv\Scripts\activate
pip install -r requirements.txt
python main.py dashboard

# Or EXE (from Releases):
C:\FileActivity\bin\FileActivity.exe --config C:\FileActivity\config\config.yaml dashboard
```

Dashboard: **http://localhost:8085**

## Project Structure

```
FILE_ACTIVITY/
├── main.py                          # CLI entry point (Click)
├── config.yaml                      # Main configuration
├── src/
│   ├── scanner/                     # File Scanning Engine
│   │   ├── file_scanner.py          #   Recursive scan + FileNameAnalyzer + MITNamingAnalyzer
│   │   ├── file_watcher.py          #   Real-time change monitoring
│   │   ├── share_resolver.py        #   UNC path resolution
│   │   └── win_attributes.py        #   Windows file attributes (ACL, owner, timestamps)
│   ├── analyzer/                    # Analysis Modules
│   │   ├── frequency_analyzer.py    #   Access frequency analysis (30/60/90/180/365+ days)
│   │   ├── type_analyzer.py         #   File type distribution
│   │   ├── size_analyzer.py         #   Size distribution (tiny → huge)
│   │   ├── ai_insights.py           #   AI recommendations and risk score
│   │   ├── report_generator.py      #   Combined report generator
│   │   └── report_exporter.py       #   HTML/JSON/XLS export
│   ├── archiver/                    # Archive Engine
│   │   ├── archive_engine.py        #   Copy-Verify(SHA256)-Delete workflow
│   │   ├── archive_policy.py        #   Policy-based archiving
│   │   └── restore_engine.py        #   Restore (single/bulk, directory recreation)
│   ├── storage/                     # Data Layer
│   │   ├── database.py              #   SQLite database (WAL, thread-safe, FTS5)
│   │   └── models.py                #   Data models
│   ├── dashboard/                   # Web Interface
│   │   ├── api.py                   #   FastAPI REST endpoints (~30 endpoints)
│   │   └── static/
│   │       └── index.html           #   Single-page dashboard (Chart.js, D3.js)
│   ├── user_activity/               # User Activity Tracking
│   │   ├── event_collector.py       #   Windows Event Log collection (4663, 5145, 4660)
│   │   └── user_analyzer.py         #   User risk scoring, anomaly detection
│   ├── scheduler/                   # Task Scheduler
│   │   ├── task_scheduler.py        #   APScheduler cron-based scheduling
│   │   └── win_task_scheduler.py    #   Windows Task Scheduler integration
│   ├── service/                     # Windows Service
│   │   └── file_activity_service.py #   Run as Windows Service
│   ├── i18n/                        # Internationalization
│   │   └── messages.py              #   Turkish/English messages
│   └── utils/                       # Utilities
│       ├── config_loader.py         #   YAML configuration loader
│       ├── logging_setup.py         #   Logging configuration
│       └── size_formatter.py        #   Size formatting (KB/MB/GB/TB)
├── deploy/                          # Deployment & Updates
│   ├── setup.ps1                    #   One-command setup (downloads EXE from GitHub Releases)
│   ├── auto-update.ps1              #   Auto-update from GitHub
│   ├── deploy.ps1                   #   Remote multi-server deployment (PSRemoting)
│   ├── install.bat                  #   Installation script
│   ├── update.bat                   #   Manual update (preserves data)
│   ├── uninstall.bat                #   Uninstall script
│   └── service_install.bat          #   Windows Service setup (NSSM/TaskScheduler)
├── scripts/                         # Helper Scripts
│   ├── init_db.py                   #   Database initialization
│   └── Configure-FileAudit.ps1     #   Windows file audit configuration
├── build.bat                        # PyInstaller EXE build
├── pack.py                          # ZIP package creator
└── file_activity.spec               # PyInstaller configuration
```

## Dashboard Pages

| Page | Description |
|------|-------------|
| **Overview** | Risk score, KPI cards, file age distribution, growth trend, AI recommendations |
| **Sources** | UNC share source management |
| **Treemap** | File/directory size visualization |
| **Access Frequency** | Files not accessed for 30/60/90/180/365+ days |
| **File Types** | Extension-based distribution |
| **Size Distribution** | Tiny/Small/Medium/Large/Huge categories |
| **User Activity** | Top users, department analysis, time series |
| **Anomalies** | High volume, night access, mass delete, large transfer detection |
| **AI Insights** | Automated recommendations and health score |
| **Archiving** | Archive statistics, search, restore |
| **Archive History** | Detailed log of all archive/restore operations |
| **Duplicate Files** | Duplicate file groups, selective archiving, optional hash verification |
| **Growth Analysis** | Yearly/monthly/daily growth charts, top file creators |
| **Naming Compliance** | MIT Libraries File Naming Scheme compliance analysis |
| **Policies** | Archive policy management |
| **Scheduling** | Automated scan and archive schedules |

## Update

### Automatic update (same install command):
```powershell
$f="$env:TEMP\fa.ps1"; (New-Object Net.WebClient).DownloadFile("https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/setup.ps1",$f); powershell -ExecutionPolicy Bypass -File $f
```

**Preserved during update:** database, config, logs, reports

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/sources` | Source list |
| GET | `/api/risk-score/{id}` | Risk score and KPIs |
| GET | `/api/reports/frequency/{id}` | Access frequency |
| GET | `/api/reports/types/{id}` | File types |
| GET | `/api/reports/sizes/{id}` | Size distribution |
| GET | `/api/reports/duplicates/{id}` | Duplicate file groups |
| GET | `/api/reports/mit-naming/{id}` | MIT naming compliance |
| GET | `/api/growth/{id}` | Growth statistics |
| GET | `/api/reports/top-creators/{id}` | Top file creators |
| GET | `/api/insights/{id}` | AI recommendations |
| GET | `/api/archive/history` | Archive history |
| POST | `/api/archive/by-insight` | Archive by AI recommendation |
| POST | `/api/archive/selective` | Archive selected files |
| POST | `/api/restore/bulk` | Bulk restore |
| GET | `/api/users/overview` | User activity overview |
| GET | `/api/anomalies` | Anomaly alerts |

## Technology Stack

| Layer | Technology |
|-------|------------|
| Backend | Python 3.10+, FastAPI, Click |
| Frontend | Vanilla JS, Chart.js, D3.js |
| Database | SQLite (WAL, FTS5) |
| Windows API | pywin32 (Event Log, ACL, Security) |
| Scheduling | APScheduler |
| Reporting | openpyxl, reportlab |
| Packaging | PyInstaller |

---

# 🇹🇷 Türkçe

Kurumsal dosya paylaşımlarını analiz eden, riskli/eski/kopya dosyaları tespit edip arşivleyen ve MIT Libraries adlandırma standartlarına uyum kontrolü yapan bir Windows yönetim aracı.

## Hızlı Kurulum

### Gereksinimler
- Windows 10/11 veya Windows Server 2016+
- Python veya Git **gerekmez** (standalone EXE)

### Tek Komutla Kurulum

```powershell
# PowerShell (Admin olarak çalıştırın):
$f="$env:TEMP\fa.ps1"; (New-Object Net.WebClient).DownloadFile("https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/setup.ps1",$f); powershell -ExecutionPolicy Bypass -File $f
```

### Manuel Kurulum
```powershell
# Son sürümü indirin:
# https://github.com/deepdarbe/FILE_ACTIVITY/releases/latest
# ZIP'i açın, ardından çalıştırın:
C:\FileActivity\bin\FileActivity.exe --config C:\FileActivity\config\config.yaml dashboard
```

Dashboard: **http://localhost:8085**

## Dashboard Sayfaları

| Sayfa | Açıklama |
|-------|----------|
| **Genel Bakış** | Risk skoru, KPI kartları, dosya yaşı dağılımı, büyüme trendi, AI önerileri |
| **Kaynaklar** | UNC paylaşım kaynakları yönetimi |
| **Treemap Harita** | Dosya/dizin büyüklük haritası |
| **Erişim Sıklığı** | 30/60/90/180/365+ gün erişilmemiş dosyalar |
| **Dosya Türleri** | Uzantı bazlı dağılım |
| **Boyut Dağılımı** | Tiny/Small/Medium/Large/Huge kategorileri |
| **Kullanıcı Aktivite** | Top kullanıcılar, departman analizi, zaman serisi |
| **Anomaliler** | Yüksek hacim, gece erişimi, toplu silme, büyük transfer tespiti |
| **AI Insights** | Otomatik öneriler ve sağlık skoru |
| **Arşivleme** | Arşiv istatistikleri, arama, geri yükleme |
| **Arşiv Geçmişi** | Tüm arşiv/geri yükleme işlemlerinin detaylı kaydı |
| **Kopya Dosyalar** | Duplike dosya grupları, seçici arşivleme, opsiyonel hash doğrulama |
| **Büyüme Analizi** | Yıllık/aylık/günlük büyüme grafikleri, en çok dosya oluşturanlar |
| **Adlandırma Uyumu** | MIT Libraries File Naming Scheme uyum analizi |
| **Politikalar** | Arşiv politika yönetimi |
| **Zamanlama** | Otomatik tarama ve arşivleme zamanları |

## Güncelleme

### Otomatik güncelleme (aynı kurulum komutu):
```powershell
$f="$env:TEMP\fa.ps1"; (New-Object Net.WebClient).DownloadFile("https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/setup.ps1",$f); powershell -ExecutionPolicy Bypass -File $f
```

**Güncelleme sırasında korunan veriler:** veritabanı, config, loglar, raporlar

## CLI Komutları

```bash
# Kaynak yönetimi
FileActivity.exe add-source -n SERVER01 -p "\\server\share" -a "\\archive\dest"
FileActivity.exe remove-source -n SERVER01
FileActivity.exe test-connection -n SERVER01

# Tarama
FileActivity.exe scan -s SERVER01
FileActivity.exe scan --all

# Raporlar
FileActivity.exe report-age -s SERVER01
FileActivity.exe report-types -s SERVER01
FileActivity.exe report-size -s SERVER01
FileActivity.exe report-owners -s SERVER01

# Arşivleme
FileActivity.exe archive -s SERVER01 -p eski-dosyalar
FileActivity.exe restore --archive-id 42

# Politikalar
FileActivity.exe create-policy -n eski-dosyalar --access-days 365
FileActivity.exe list-policies

# Zamanlama
FileActivity.exe schedule-task -t scan -s SERVER01 --cron "0 2 * * *"

# Dashboard
FileActivity.exe --config config.yaml dashboard
```

## API Endpointleri

| Method | Endpoint | Açıklama |
|--------|----------|----------|
| GET | `/api/sources` | Kaynak listesi |
| GET | `/api/risk-score/{id}` | Risk skoru ve KPI'lar |
| GET | `/api/reports/frequency/{id}` | Erişim sıklığı |
| GET | `/api/reports/types/{id}` | Dosya türleri |
| GET | `/api/reports/sizes/{id}` | Boyut dağılımı |
| GET | `/api/reports/duplicates/{id}` | Kopya dosya grupları |
| GET | `/api/reports/mit-naming/{id}` | MIT adlandırma uyumu |
| GET | `/api/growth/{id}` | Büyüme istatistikleri |
| GET | `/api/reports/top-creators/{id}` | En çok dosya oluşturanlar |
| GET | `/api/insights/{id}` | AI önerileri |
| GET | `/api/archive/history` | Arşiv geçmişi |
| POST | `/api/archive/by-insight` | AI önerisiyle arşivleme |
| POST | `/api/archive/selective` | Seçili dosyaları arşivle |
| POST | `/api/restore/bulk` | Toplu geri yükleme |
| GET | `/api/users/overview` | Kullanıcı aktivite özeti |
| GET | `/api/anomalies` | Anomali uyarıları |

## Teknoloji

| Katman | Teknoloji |
|--------|-----------|
| Backend | Python 3.10+, FastAPI, Click |
| Frontend | Vanilla JS, Chart.js, D3.js |
| Veritabanı | SQLite (WAL, FTS5) |
| Windows API | pywin32 (Event Log, ACL, Security) |
| Zamanlama | APScheduler |
| Raporlama | openpyxl, reportlab |
| Paketleme | PyInstaller |

---

## License / Lisans

Private - Internal Use Only
