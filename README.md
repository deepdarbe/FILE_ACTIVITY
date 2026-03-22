# FILE ACTIVITY

**Windows File Share Analysis, Archiving & Compliance System**

Kurumsal dosya paylaşımlarını analiz eden, riskli/eski/kopya dosyaları tespit edip arşivleyen ve MIT Libraries adlandırma standartlarına uyum kontrolü yapan bir Windows yönetim aracı.

---

## Proje Şeması

```
FILE_ACTIVITY/
│
├── main.py                          # CLI giriş noktası (Click)
├── config.yaml                      # Ana konfigürasyon
├── requirements.txt                 # Python bağımlılıkları
├── setup.py                         # Kurulum scripti
├── dev_server.py                    # Mock geliştirme sunucusu
│
├── src/
│   ├── scanner/                     # Dosya Tarama Motoru
│   │   ├── file_scanner.py          #   Recursive tarama + FileNameAnalyzer + MITNamingAnalyzer
│   │   ├── file_watcher.py          #   Gerçek zamanlı değişiklik izleme
│   │   ├── share_resolver.py        #   UNC yol çözümleme
│   │   └── win_attributes.py        #   Windows dosya öznitelikleri (ACL, owner, zaman)
│   │
│   ├── analyzer/                    # Analiz Modülleri
│   │   ├── frequency_analyzer.py    #   Erişim sıklığı analizi (30/60/90/180/365+ gün)
│   │   ├── type_analyzer.py         #   Dosya türü dağılımı
│   │   ├── size_analyzer.py         #   Boyut dağılımı (tiny → huge)
│   │   ├── ai_insights.py           #   AI önerileri ve risk skoru
│   │   ├── report_generator.py      #   Birleşik rapor üretici
│   │   └── report_exporter.py       #   HTML/JSON/XLS export
│   │
│   ├── archiver/                    # Arşivleme Motoru
│   │   ├── archive_engine.py        #   Kopyala-Doğrula(SHA256)-Sil akışı
│   │   ├── archive_policy.py        #   Politika tabanlı arşivleme
│   │   └── restore_engine.py        #   Geri yükleme (tekli/toplu, dizin yeniden oluşturma)
│   │
│   ├── storage/                     # Veri Katmanı
│   │   ├── database.py              #   SQLite veritabanı (WAL, thread-safe, FTS5)
│   │   └── models.py                #   Veri modelleri
│   │
│   ├── dashboard/                   # Web Arayüzü
│   │   ├── api.py                   #   FastAPI REST endpointleri (~30 endpoint)
│   │   └── static/
│   │       └── index.html           #   Tek sayfa dashboard (Chart.js, D3.js)
│   │
│   ├── user_activity/               # Kullanıcı Aktivite Takibi
│   │   ├── event_collector.py       #   Windows Event Log toplama (4663, 5145, 4660)
│   │   └── user_analyzer.py         #   Kullanıcı risk skoru, anomali tespiti
│   │
│   ├── scheduler/                   # Görev Zamanlayıcı
│   │   ├── task_scheduler.py        #   APScheduler ile cron tabanlı zamanlama
│   │   └── win_task_scheduler.py    #   Windows Task Scheduler entegrasyonu
│   │
│   ├── service/                     # Windows Servisi
│   │   └── file_activity_service.py #   Windows Service olarak çalışma
│   │
│   ├── i18n/                        # Çoklu Dil
│   │   └── messages.py              #   Türkçe/İngilizce mesajlar
│   │
│   └── utils/                       # Yardımcılar
│       ├── config_loader.py         #   YAML konfigürasyon yükleyici
│       ├── logging_setup.py         #   Loglama yapılandırması
│       └── size_formatter.py        #   Boyut formatlama (KB/MB/GB/TB)
│
├── deploy/                          # Dağıtım & Güncelleme
│   ├── auto-update.ps1              #   GitHub'dan otomatik güncelleme
│   ├── deploy.ps1                   #   Uzak sunucu toplu dağıtım (PSRemoting)
│   ├── install.bat                  #   Kurulum scripti (EXE veya kaynak kod)
│   ├── update.bat                   #   Manuel güncelleme (veri korunarak)
│   ├── uninstall.bat                #   Kaldırma scripti
│   ├── service_install.bat          #   Windows Servis kurulumu (NSSM/TaskScheduler)
│   └── servers_template.csv         #   Toplu dağıtım sunucu listesi şablonu
│
├── scripts/                         # Yardımcı Scriptler
│   ├── init_db.py                   #   Veritabanı ilk kurulum
│   └── Configure-FileAudit.ps1     #   Windows dosya denetimi yapılandırma
│
├── build.bat                        # PyInstaller ile EXE derleme
├── pack.py                          # ZIP paket oluşturma
├── file_activity.spec               # PyInstaller yapılandırması
└── docs/
    └── project-diagram.html         # Proje diyagramı
```

---

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
| **Büyüme Analizi** | Yıllık/aylık/günlük büyüme grafikleri, top file creators |
| **Adlandırma Uyumu** | MIT Libraries File Naming Scheme uyum analizi |
| **Politikalar** | Arşiv politika yönetimi |
| **Zamanlama** | Otomatik tarama ve arşivleme zamanları |

---

## Hızlı Kurulum

### Gereksinimler
- Windows 10/11 veya Windows Server 2016+
- Python 3.10+
- Git

### Tek Komutla Kurulum

```powershell
# PowerShell (Admin olarak çalıştırın):
irm https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/setup.ps1 | iex
```

### Manuel Kurulum

```powershell
# 1. Repo'yu klonla
git clone https://github.com/deepdarbe/FILE_ACTIVITY.git C:\FileActivity\repo

# 2. Bağımlılıkları kur
pip install -r C:\FileActivity\repo\requirements.txt

# 3. Dizin yapısını oluştur
mkdir C:\FileActivity\config, C:\FileActivity\data, C:\FileActivity\logs, C:\FileActivity\reports

# 4. Config kopyala
copy C:\FileActivity\repo\config.yaml C:\FileActivity\config\

# 5. Dashboard'u başlat
python C:\FileActivity\repo\main.py dashboard --config C:\FileActivity\config\config.yaml
```

Dashboard: **http://localhost:8085**

---

## Otomatik Güncelleme

### Günlük otomatik güncelleme zamanlama:
```powershell
powershell -File C:\FileActivity\repo\deploy\auto-update.ps1 -SetupSchedule
```

### Manuel güncelleme:
```powershell
powershell -File C:\FileActivity\repo\deploy\auto-update.ps1
```

Güncelleme sırasında **korunan** veriler:
- `data\file_activity.db` (veritabanı)
- `config\config.yaml` (ayarlar)
- `logs\` (log dosyaları)
- `reports\` (raporlar)

---

## CLI Komutları

```bash
# Kaynak yönetimi
python main.py add-source -n SERVER01 -p "\\server\share" -a "\\archive\dest"
python main.py remove-source -n SERVER01
python main.py test-connection -n SERVER01

# Tarama
python main.py scan -s SERVER01
python main.py scan --all

# Raporlar
python main.py report-age -s SERVER01
python main.py report-types -s SERVER01
python main.py report-size -s SERVER01
python main.py report-owners -s SERVER01
python main.py export-data -s SERVER01 -f json -o rapor.json

# Arşivleme
python main.py archive -s SERVER01 -p eski-dosyalar
python main.py restore --archive-id 42

# Politikalar
python main.py create-policy -n eski-dosyalar --access-days 365
python main.py list-policies

# Zamanlama
python main.py schedule-task -t scan -s SERVER01 --cron "0 2 * * *"

# Dashboard
python main.py dashboard --port 8085
```

---

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

---

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

## Lisans

Private - Internal Use Only
