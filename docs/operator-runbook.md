# FILE ACTIVITY — Operator Runbook

> Operatör için günlük işletim kılavuzu. Kurulum, güncelleme, sık karşılaşılan
> senaryolar, performans ayarı, yedekleme/kurtarma ve sorun giderme tek bir
> yerde. v1.9.0-rc1 sürümü için yazıldı.

İçindekiler:

- [Kurulum (Install)](#kurulum-install)
- [Service Mode Kurulum (issue #151)](#service-mode-kurulum-issue-151)
- [Güncelleme (Update)](#guncelleme-update)
- [Sık Karşılaşılan Senaryolar](#sik-karsilasilan-senaryolar)
- [Performans Ayarı](#performans-ayari)
- [Yedekleme + Kurtarma](#yedekleme--kurtarma)
- [Sorun Giderme](#sorun-giderme)

---

## Kurulum (Install)

### Önerilen yol — kaynak yükleme (`setup-source.ps1`)

**Gereksinim:** Hedefte Python 3.10+ (yoksa script otomatik 3.11 indirir).
Komut Admin PowerShell ile çalıştırılır.

```powershell
# PowerShell (Run as Admin):
powershell -ExecutionPolicy Bypass -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; irm https://raw.githubusercontent.com/deepdarbe/FILE_ACTIVITY/master/deploy/setup-source.ps1 | iex"
```

Baştaki TLS 1.2 ataması Windows Server 2012/2016 üzerinde zorunludur —
PowerShell 5.1 hâlâ TLS 1.0/1.1 default'ında ve GitHub bunları 2018'den beri
reddediyor. Yeni sistemlerde zararsız, kanonik form olarak kullanın.

Script ne yapar:

- `C:\FileActivity\` altına yükler
- Python 3.10+ yoksa 3.11 indirir/kurar
- `.venv` oluşturur, tüm bağımlılıkları yükler (DuckDB analytics dâhil)
- Windows Firewall kuralı ekler (8085)
- Dashboard'u başlatmayı önerir
- Aynı komutu tekrar koşunca **veri / log / rapor / `config.yaml` korunur**

PowerShell modülü (`powershell\FileActivity\`) otomatik kurulur ve user
PSModulePath'e eklenir. Yeni session'da `Import-Module FileActivity` ile
hazırdır.

Dashboard: **http://localhost:8085**

### Alternatif — tek dosya EXE (Releases)

Python gerekmez. Tag'li her sürüm için
`file-activity-<version>-win64.exe` + `.sha256` yayınlanır.
EXE'yi bir `config.yaml` yanına koy, çift tıkla. EXE statik bir snapshot —
otomatik güncelleme istiyorsan kaynak install'u tercih et.

İlgili: README "Quick Setup" bölümü.

---

## Service Mode Kurulum (issue #151)

Operatör şikâyeti: "projeyi kapatmak açmak command prompt'ta işlem yapmak
zahmetli oluyor". `start_dashboard.cmd` ile cmd penceresi açıp Task
Manager'dan kapatma akışı dağıtık üretimde kabul edilemez. Service mode
bu akışı bitirir.

### Mimari

NSSM (Non-Sucking Service Manager, BSD-like license) python.exe + main.py
`dashboard` subcommand'ını gerçek bir Windows servisi olarak sarar:

- `Start = SERVICE_AUTO_START` — boot'ta otomatik başlar
- `AppExit Default Restart` — crash sonrası 5 sn delay ile yeniden başlar
- `AppStdout / AppStderr` — `logs\service.out` ve `logs\service.err`
  (10 MB'da rotate)

NSSM zip'i `nssm.cc/release/nssm-2.24.zip` adresinden ilk kurulumda
indirilir, `<InstallDir>\bin\nssm.exe` konumuna düşer.

### Kurulum

`setup-source.ps1` artık iki opt-in soru sorar (default H — geriye dönük
uyumluluk için davranış değişmemeli):

```
Hizmet olarak yuklensin mi (Windows Service, otomatik baslatma + crash recovery)?
[E] Evet  [H] Hayir (sadece manuel start_dashboard.cmd ile)
```

E seçilirse `deploy\install_service.ps1` çağrılır. Standalone kurmak için
de manuel:

```powershell
# Yönetici PowerShell:
powershell -ExecutionPolicy Bypass -File C:\FileActivity\deploy\install_service.ps1
```

Tray ikon (opsiyonel — durum göstergesi + sağ tık menü):

```
Sistem tepsisi simgesi yuklensin mi (durum gostergesi + tek tikla yeniden baslat)?
[E] Evet  [H] Hayir
```

E seçilirse `pystray` + `Pillow` venv'e yüklenir, `start_tray.cmd`
oluşturulur, Startup klasörüne kısayol bırakılır (logon'da otomatik).

### Yönetim — üç yol

**1. Services.msc (klasik):** "FILE ACTIVITY - File Share Monitor" satırı.
Sağ tık → Start / Stop / Restart.

**2. PowerShell cmdlet'leri** (`Import-Module FileActivity` sonrası):

```powershell
Start-FileActivityService
Stop-FileActivityService
Restart-FileActivityService
Get-FileActivityServiceStatus    # Status + StartType döner

# Servis kurulu değilse hata fırlatmaz, Status='NotInstalled' döner:
if ((Get-FileActivityServiceStatus).Status -ne 'Running') {
    Start-FileActivityService
}
```

Her cmdlet yönetici hakkı gerektirir; non-admin session'da Türkçe friendly
hata döner.

**3. Tray ikon:** Sağ tık → Open Dashboard / Start / Stop / Restart /
Open Logs Folder / Quit. Status renk kodu:

- yeşil = Running
- sarı  = StartPending / StopPending / Paused
- kırmızı = Stopped / NotInstalled

### Loglar

NSSM stdout/stderr ayrı dosyalarda, 10 MB rotation:

- `C:\FileActivity\logs\service.out` — uvicorn / dashboard normal çıktı
- `C:\FileActivity\logs\service.err` — exception trace, crash detayı

Watchdog/scheduler iç logları yine `config.yaml` `logging.path` altında.

### Geri dönüş (rollback to manual mode)

```powershell
powershell -ExecutionPolicy Bypass -File C:\FileActivity\deploy\uninstall_service.ps1
```

NSSM servisi kaldırır. Tray kısayolunu da silmek için:

```powershell
powershell -ExecutionPolicy Bypass -File C:\FileActivity\deploy\uninstall_service.ps1 -RemoveTrayShortcut
```

`start_dashboard.cmd` her zaman çalışmaya devam eder — debug / standalone
senaryolar için saklı.

### Smoke testi (servis kurulduktan sonra)

```powershell
# 1) Status:
Get-FileActivityServiceStatus
# Beklenen: Status=Running, StartType=Automatic

# 2) HTTP probe:
Invoke-RestMethod http://localhost:8085/api/system/status

# 3) Crash recovery testi (son cümle Run as Admin):
Stop-Process -Name python -Force
Start-Sleep -Seconds 8
Get-FileActivityServiceStatus    # NSSM 5 sn delay + restart sonrası tekrar Running

# 4) Reboot persistence:
Restart-Computer
# Boot sonrası: Get-Service FileActivity -> Running (login bile gerekmez)
```

İlgili: issue #151, `deploy\install_service.ps1`,
`deploy\uninstall_service.ps1`, `src\tray\tray_app.py`.

---

## Dashboard Bearer Token (issue #158)

Customer LAN deployments expose the dashboard to every host on the
subnet. v1.9.0-rc2 ships authentication ON by default + a 127.0.0.1
default bind. The two together close finding **C-1** from
`docs/architecture/security-audit-2026-04-28.md`.

### Konfig hatlari (config.yaml)

```yaml
dashboard:
  host: "127.0.0.1"          # default: loopback only
  port: 8085
  auth:
    enabled: true                                 # default ON
    token_env: "FILEACTIVITY_DASHBOARD_TOKEN"     # NEVER put the token in this file
    allow_unauth_localhost: true                  # 127.0.0.1 / ::1 / localhost bypass
```

### Operasyonel modlar

| Mod | host | auth.enabled | Tarayicidan | Token gerekli mi |
|---|---|---|---|---|
| Tek-host RDP (default) | 127.0.0.1 | true | http://localhost:8085 | Hayir (localhost bypass) |
| LAN expose, secure | 0.0.0.0 | true | http://server:8085 | EVET (Authorization: Bearer ...) |
| LAN expose, unauth | 0.0.0.0 | false | http://server:8085 | REDDEDILIR — `--i-know-what-im-doing` ister |

### Token uretme + export

PowerShell tarafinda hizli bir 32-byte rastgele token:

```powershell
$tok = [System.Convert]::ToBase64String((1..32 | ForEach-Object { Get-Random -Maximum 256 }))
# Servis hesabina kalici export (admin gerekir):
setx /M FILEACTIVITY_DASHBOARD_TOKEN $tok
# Sadece su anki session icin:
$env:FILEACTIVITY_DASHBOARD_TOKEN = $tok
```

NSSM service mode kullaniyorsan: NSSM `AppEnvironmentExtra`
ayarini kullan (`nssm set FileActivity AppEnvironmentExtra
FILEACTIVITY_DASHBOARD_TOKEN=...`) — process'in environment'i
kontrol panelinden setx kadar guvenilir bir kanaldir ve restart
sonrasi otomatik gelir.

### LAN'dan API cagirma

```powershell
$headers = @{ Authorization = "Bearer $env:FILEACTIVITY_DASHBOARD_TOKEN" }
Invoke-RestMethod -Uri "http://server:8085/api/system/health" -Headers $headers

# POST ornegi: dry-run arsiv
Invoke-RestMethod -Uri "http://server:8085/api/archive/run" `
                  -Method Post -Headers $headers `
                  -ContentType "application/json" `
                  -Body (ConvertTo-Json @{ source_id=1; days=365; dry_run=$true })
```

### Token rotate

Token cache process-local. Yeni bir token export et + servisi restart et:

```powershell
setx /M FILEACTIVITY_DASHBOARD_TOKEN <yeni-token>
Restart-Service FileActivity   # NSSM ile kurulduysa
```

### `--i-know-what-im-doing` flag — neden boyle bilerek garip?

`main.py dashboard --bind 0.0.0.0` + `dashboard.auth.enabled: false`
kombinasyonu, tum dashboard endpoint'lerini auth'siz LAN'a aciyor —
dosya tasi, snapshot restore, AD config write, hepsi. v1.9.0-rc2
itibariyle bu kombinasyonu tespit edersek `main.py` baslatmayi
**reddediyor** ve bir CRITICAL log atiyor. Operator bilincli olarak
istediyse `--i-know-what-im-doing` flag'i ile override edebilir.
Flag'i komut hafizasinda tutmak guc: amac zaten budur.

Production'da bu modu kullanma. Reverse-proxy/VPN arkasinda zaten
`auth.enabled=true` + `allow_unauth_localhost=true` (proxy bind'i
loopback'tir) yeterlidir.

### C-2: archiving.dry_run + confirm gate

Ayni PR'de `archiving.dry_run` default `true` oldu. `/api/archive/run`
ve `/api/archive/selective` endpoint'leri artik `dry_run=false` icin
`confirm=true` body alani sart kosuyor. Frontend bunu zaten gonderiyor;
kendi script'lerin varsa ve "tasimadi" diyorsan once dry-run'da
ciktisini incele, sonra `dry_run=false, confirm=true` ile gonder.

```powershell
# Dry-run (tasimaz, sadece raporlar):
Invoke-RestMethod ... -Body (ConvertTo-Json @{
    source_id = 1; days = 365; dry_run = $true
})

# Real run (gercekten tasir):
Invoke-RestMethod ... -Body (ConvertTo-Json @{
    source_id = 1; days = 365; dry_run = $false; confirm = $true
})
```

### H-2: approvals + identity_source guard

`approvals.enabled: true` ile `approvals.identity_source: client_supplied`
yan yana KOYULAMAZ. Bu kombo iki-kisi kuralini etkisiz kiliyor (ikinci
kisi kendi adini istedigi gibi yazabilir). Dashboard bu durumda
ApprovalRegistry'i `None` ile birakir, CRITICAL loglar, ve approval
endpoint'leri 503 doner. Cozum: `identity_source`'u `windows` (Windows
host) veya `header` (auth proxy onunde) olarak ayarla, sonra restart.

---

## Güncelleme (Update)

Kurulum dizininde `update.cmd` yer alır. Çalıştırınca:

1. **Snapshot** — mevcut `data/`, `logs/`, `config.yaml`, ve DB dosyaları
   `update.cmd` snapshot dizinine kopyalanır. Snapshot yolu `InstallDir`
   altındadır (35a9c5f sonrası — daha önce yanlışlıkla `$LOCALAPPDATA`
   kullanılıyordu).
2. **Git fetch + checkout** — master'ın en güncel commit'i çekilir.
3. **`pip install -r requirements.txt`** — yeni bağımlılıklar (varsa)
   eklenir.
4. **VERSION SHA** — Sidebar üst kısmında `1.9.0-rc1` etiketinin yanında
   build commit SHA görünür (88fe9c5 sonrası — version display'ine commit
   SHA eklendi). Operatör hangi build'in koştuğunu kontrol etmek için
   buraya bakar.
5. **Dashboard'u yeniden başlat** — manuel; service ise NSSM/Task
   Scheduler restart edilir.

Beklenenler:

- Veri / config / log korunur
- Snapshot otomatik tutulur — geri dönmek istersen `InstallDir\snapshots\`
  altında günlük damga
- Eğer `requirements.txt` yeni bir paket içeriyorsa pip yükleme süresi
  artar (1-3 dk)

---

## Sık Karşılaşılan Senaryolar

### 1. WAL şişti, dashboard hung — manuel checkpoint + restart

**Belirti:** `data/file_activity.db-wal` 1 GB üstüne çıkmış, dashboard
"Yükleniyor..." durumunda kalıyor. Sidebar üstte WAL warning banner görünür
hale gelir.

**Sebep:** Uzun süreli yazıcı (scan + archive aynı anda), checkpoint
fırsatı bulamamış.

**Çözüm:**

```powershell
# 1) Servis duruyorsa direkt CLI; aksi halde önce dashboard'ı kapat:
Stop-Service FileActivity   # NSSM kurulumu varsa
# veya dashboard process'i öldür: taskkill /F /IM FileActivity.exe

# 2) WAL checkpoint:
sqlite3 C:\FileActivity\data\file_activity.db "PRAGMA wal_checkpoint(TRUNCATE);"

# 3) Servisi başlat:
Start-Service FileActivity
```

Tekrarlanıyorsa: scan zamanlamasıyla archive zamanlamasını üst üste koyma
(Scheduling sayfası).

### 2. Snapshot manuel

**Ne zaman:** Riskli bir işlemden önce (büyük cleanup, retention purge,
compliance modülünde toplu silme). `update.cmd` çalışmadan kendi snapshot'ını
almak istersen.

**Yol 1 — BackupManager (önerilen):**

```bash
curl -X POST http://localhost:8085/api/backup/run
# ya da PowerShell modülünden:
Invoke-RestMethod -Method Post http://localhost:8085/api/backup/run
```

`InstallDir\backups\file_activity_<TIMESTAMP>.db` üretir. Hot backup
(SQLite Online Backup API) — DB kapatmaya gerek yok.

**Yol 2 — File copy (DB kapalıyken):**

```powershell
Stop-Service FileActivity
Copy-Item C:\FileActivity\data\file_activity.db `
          C:\FileActivity\backups\manual-$(Get-Date -Format yyyyMMdd-HHmm).db
Start-Service FileActivity
```

Auto-backup retention default 14 gün; `config.yaml` `backup.retention_days`.

### 3. Tarama durdur (#134)

**Belirti:** Dashboard "Tarama devam ediyor" diyor ama operatör
durdurmak istiyor (yanlış kaynak, prod saatleri, vs.).

**Çözüm:**

```bash
# REST:
curl -X POST http://localhost:8085/api/scan/{source_id}/stop
```

PR #134 sonrası: cancel flag scan worker tarafından her batch sonunda
kontrol edilir (~5-10 sn içinde durur). Önceden silinmiş satırlar geri
yüklenmez — `scan_runs.status='cancelled'` işaretlenir.

İlgili: issue #131 (cancel flag), #134 (3 prod bug fix).

### 4. Eski tarama temizliği (`?keep_last=N`)

**Belirti:** `scan_runs` tablosu 100+ satır, DB şişiyor.

**Çözüm — son N taramayı tut:**

```bash
# keep_last_n_scans alias, query parametresi keep_last destekler:
curl -X POST "http://localhost:8085/api/scans/cleanup?keep_last=5"

# veya 0 — hepsini sil (#133/#134 ile keep_last=0 422 vermez artık):
curl -X POST "http://localhost:8085/api/scans/cleanup?keep_last=0"
```

PR #133 öncesi `keep_last=0` 422 dönerdi (validator min=1). #134 ile
`keep_last=0` legal — "tüm taramalar temizlensin" anlamı.

**Cron önerisi:** Haftalık scheduled task:

```bash
0 3 * * 0  curl -X POST "http://localhost:8085/api/scans/cleanup?keep_last=10"
```

### 5. Quarantine review (#83 + #110)

**Belirti:** Duplicate detection bir sürü dosyayı quarantine'e atmış,
operatör hangi grupların gerçekten silinebileceğine karar verecek.

**Akış:**

1. Dashboard → **Kopya Dosyalar** sayfası
2. Phase 1 (#83): "Quarantine-only delete" — dosya gerçekten silinmez,
   `<archive_root>\.quarantine\` altına taşınır + DB'de `quarantined`
   işaretlenir
3. Phase 2 (#110): listede her quarantine grup için **Restore** veya
   **Hard delete** butonu. Hard delete geri dönüşsüz; restore orijinal
   path'e geri koyar
4. Gain reporter (#83): "Bu grupları silersen X GB kazanılır" özeti
   üst banner'da

**Komut hattı:**

```powershell
Get-FileActivityDuplicates -SourceId 1 |
    Where-Object Quarantined -eq $true |
    Sort-Object WasteSize -Descending |
    Select-Object -First 20
```

### 6. Audit chain doğrulama (`/api/audit/verify`)

**Ne için:** Compliance raporları için audit trail'in bütünlüğü
(satır-satır SHA256 zincir). Tampered kayıt veya silinmiş satır
varsa zincir kırılır.

**Komut:**

```bash
curl http://localhost:8085/api/audit/verify
```

Cevap `{"verified": true, "broken_at_seq": null}` olmalı. PowerShell:

```powershell
$result = Test-FileActivityAuditChain
if (-not $result.Verified) {
    Write-Warning "AUDIT CHAIN BROKEN at seq $($result.BrokenAtSeq)"
}
```

Kırılma tespit edilirse: en son backup'tan restore + breakage zamanı
arasındaki audit eventleri gözden geçir.

---

## Performans Ayarı

### Hyperscan opt-in (Linux)

PII regex tarama default `re` (Python stdlib) kullanır. Linux'ta Hyperscan
aktif edilebilir — **11x hızlanma** (PR #66 benchmark).

```bash
pip install -r requirements-accel.txt   # hyperscan + pyhs
# config.yaml:
# pii:
#   engine: hyperscan
```

**Windows:** Hyperscan binary wheel yok — `re` fallback'inde kalır.
Aynı sonuç, daha yavaş.

İlgili: #64 (engine), #74 (`HS_FLAG_UCP` drop — bazı pattern'lerde sessiz
miss yapıyordu).

### MFT vs SMB scanner

İki backend var:

| Backend | Ne zaman | Performans |
|---|---|---|
| **MFT scanner** (default, lokal NTFS) | Lokal volume taraması | ~1M dosya / 3 dk, peak ~50 MB |
| **SMB scanner** (`os.walk`) | Network share / UNC path | ~2.5M dosya / 45 dk |

MFT yalnızca lokal NTFS volume'larda çalışır (`\\?\C:` gibi). UNC path
otomatik SMB'ye düşer. PR #136 sonrası MFT incremental progress raporu
verir (5'er saniyede phase + count).

**Tavsiye:** Mümkünse MFT (lokal mount); zorda kalırsan SMB.

---

## Yedekleme + Kurtarma

### BackupManager kullanımı

Auto-backup default açık, `config.yaml`:

```yaml
backup:
  enabled: true
  retention_days: 14
  schedule_cron: "0 2 * * *"   # her gece 02:00
  online_backup_pages: 1000    # SQLite Online Backup API page sayısı
```

Manuel:

```bash
curl -X POST http://localhost:8085/api/backup/run
curl http://localhost:8085/api/backup/list
curl -X POST http://localhost:8085/api/backup/restore/{backup_id}
```

### Auto-restore (opt-in, #77 Phase 2)

PR #106 ile geldi. **Default kapalı** — bilinçli aktif et.

```yaml
backup:
  auto_restore_on_corruption: true   # default false
```

Ne yapar: Startup'ta SQLite corruption probe (skip/quick/full) çalışır.
Corruption tespit edilirse en son sağlam backup otomatik restore edilir,
audit log'a kayıt düşer. Kapalıysa sadece error log + dashboard "DB
bozuk" banner.

**Riskler:** Yeni eklenen veri (son backup sonrası) restore'da kaybolur.
Auto-restore sadece "data > zaman" tradeoff'unda kabul edilebilir bir
seçim.

İlgili: #77 Phase 1 (auto-backup), #77 Phase 2 (auto-restore), #119
(corruption probe hotfix).

---

## Sorun Giderme

### Corruption probe modları (skip / quick / full)

`config.yaml`:

```yaml
storage:
  corruption_check:
    mode: skip   # default — 4c38376 sonrası
```

| Mode | Komut | Süre (3.5 GB DB) | Ne zaman |
|---|---|---|---|
| `skip` (default) | yok | 0 sn | Default, prod |
| `quick` | `PRAGMA quick_check` | ~60 sn | Haftalık manuel |
| `full` | `PRAGMA integrity_check` | 5+ dk | Sadece şüpheli durum |

Önceden default `quick` idi — 3.5 GB prod DB'de startup 60 sn'lik delay
yaşatıyordu. PR #119 ile `quick_check` + bounded timeout, 4c38376 ile
default `skip` oldu.

**Manuel full check:**

```bash
sqlite3 C:\FileActivity\data\file_activity.db "PRAGMA integrity_check;"
```

`ok` dönmesi beklenir.

### Read/write contention scan sırasında

**Belirti:** Scan koşarken dashboard donuyor, "database is locked"
hataları log'da.

**Sebep:** Scan worker'lar yazıyor, dashboard read-only sorgu yine de
WAL'a tutunamıyor.

**Çözüm:** PR #134 ile read-only cursor (`PRAGMA query_only=ON`) tüm
read endpoint'lerinde aktif. Yine de 3.5 GB+ DB'lerde scan sırasında
heavy report sayfaları (Treemap, Duplicates) yavaş — alternatif:

- Scan'i gece zamanla
- Ya da scan sırasında sadece Overview / KPI banner'a bak (cached, hızlı)

### "MFT okunuyor (0 kayıt)" üst banner ile uyumsuz

**Belirti:** Sources kartı + DOSYA KPI "0" gösterirken üst ops banner
"123,456 dosya işlendi" diyor.

**Sebep:** Eski versiyonda Sources/KPI sadece `scan_runs.summary_json`
dolduktan sonra güncelleniyordu. Tarama hâlâ koşarken senkronizasyon
kopuk.

**Çözüm:** PR #137/#138 sonrası `live_count` field'ı ile senkronize.
**Sürüm kontrolü:** Sidebar üst sol köşedeki version SHA `a556ac7` veya
sonrası olmalı. Eski build'lerde update et.

### "all zeros during MFT"

**Belirti:** MFT scan koşuyor ama her metric (count, size, owner)
tamamen sıfır görünüyor.

**Sebep:** MFT okuma henüz `scanned_files` insert noktasına gelmedi —
USN journal parse aşamasında. Önceden hiçbir progress reporting yoktu;
operatör "scan donmuş" sanıyordu.

**Çözüm:** PR #136 (issue #135) sonrası phase reporting:

- `phase=mft_read` (USN parse, count artıyor)
- `phase=enriching` (ACL/owner lookup)
- `phase=writing` (DB insert)
- `phase=summary` (final aggregates)

Üst ops banner'da phase görünür. Eğer `phase=mft_read` 5+ dk hareketsizse
gerçek bir hang — log'a bak.

---

## Cross-link

- Roadmap: [`../ROADMAP.md`](../ROADMAP.md)
- Release notes: [`release-notes/v1.9.0-rc1.md`](release-notes/v1.9.0-rc1.md)
- Troubleshooting (decision-tree): [`troubleshooting.md`](troubleshooting.md)
- MCP server: [`mcp_server.md`](mcp_server.md)
- Playground: [`playground.md`](playground.md)
- Üst README: [`../README.md`](../README.md)
