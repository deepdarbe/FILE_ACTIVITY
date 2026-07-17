# Plan — "Dosya Silme Olayları / Forensic" Menüsü (Entegrasyon)

> Kaynak: Burcu Gıda toplu dosya silme adli incelemesinden çıkan öğretiler (2026-06-17/18).
> Amaç: Tek ekranda **kim / ne (tam yol) / ne zaman / nereden (IP-workstation) / kurtarılabilir mi**.
> Bu doküman salt-okunur analizden üretildi; kod henüz yazılmadı.

## Gerçek olay (referans senaryo)
Bir dosya sunucusunda 25 saniyede 87 dosya, tek kullanıcı (domain `Administrator`) tarafından, bir İK
iş istasyonundan **SMB paylaşımı** üzerinden silindi. Adli inceleme şu kaynaklardan yapıldı:
- **NTFS USN Journal** (`fsutil usn readjournal`, reason `0x80000200`=delete|close) → ne + ne zaman (~5 gün).
- **Windows Güvenlik günlüğü** 4656(`%%1537`=DELETE) + 4624(kaynak IP/workstation) → kim/nereden (~16 saat ömür!).
- **VSS gölge kopya** → kurtarma.
- **Synology Drive daemon.log** (source: local/remote) → senkron yönü.
Detay reçete: bridge repo `docs/PLAYBOOK-dosya-silme-forensik.md`.

## Projenin mevcut durumu (FILE_ACTIVITY — %80 hazır)
İki ayrı veri yolu var; biri aktif, biri kapalı:

- **Yol A — USN tail (AKTİF):** `src/scanner/file_watcher.py` → `_on_usn_event` (≈151) → `_record_audit` (≈286)
  → **`file_audit_events`** tablosu. `src/scanner/backends/ntfs_usn_tail.py` (FSCTL USN, FILE_DELETE=0x200).
  UNC'de USN yok → polling fallback. **Eksik:** sadece dosya adı yazıyor, **tam yol yok** (MFT parent lookup yok).
- **Yol B — Event Log (KAPALI / wire-up YOK):** `src/user_activity/event_collector.py` sınıf `EventCollector`
  (≈59), 4663/5145/4660/4656 okur → **`user_access_logs`** tablosu. **main.py / api.py / scheduler'da hiçbir
  yerden çağrılmıyor.** `_parse_access_mask` (≈43) DELETE=0x10000 ayırıyor; 5145'te `client_ip` dolu.
  **Eksik:** `_parse_4660` (≈259) yolu `[HandleId:xxx]` placeholder yazıyor (4656↔4660 korelasyonu yok).

**İlgili tablolar** (`src/storage/database.py`):
- `file_audit_events` (≈770): event_time, event_type('delete'…), username, file_path, file_name, details, detected_by, source_id.
- `user_access_logs` (≈708): username, domain, file_path, access_type, access_time, **client_ip**, event_id.
- `audit_log_chain` (≈797) + `verify_audit_chain()` (≈3060): SHA-256 tamper-evident (delil bütünlüğü; default kapalı `audit.chain_enabled:false`).
- `anomaly_alerts` (≈750): mass_delete vb. `AnomalyDetector._detect_mass_delete` (`user_analyzer.py`≈237) `user_access_logs`'a bakıyor (yani şu an boş).

**API** (`src/dashboard/api.py`, route'lar `create_app()`≈465 içinde): `GET /api/audit/events` (≈2408 → `db.get_audit_events` database.py≈2677), `/api/audit/summary` (≈2415), `/api/audit/verify` (≈2421), `/api/anomalies` (≈2337).
**UI** (`src/dashboard/static/index.html`): sidebar 596–689, `showPage()` ≈2774, "Güvenlik" grubu 641–647. Şablon fonksiyonlar: `loadAnomalies()` ≈4701, `loadUsers()` ≈4374, `loadUserDetail()` ≈4566, `_setHtmlSafe()` ≈2713.

## Yapılacaklar (faz sırası)

### Faz 1 — Event Log yolunu hayata geçir (en yüksek değer)
- `main.py`'ye Click komutu: `collect-events --hours 16` → `EventCollector(db, config).collect()`.
- `config.yaml`'a **`user_activity:`** bölümü ekle (şu an yok): `event_ids`, `exclude_users`, `exclude_extensions`,
  `batch_size`; anomali eşikleri (`user_analyzer.py` ≈189-192): `high_delete_count`, `night_access_threshold`.
- Scheduler'a periyodik tetik (Güvenlik günlüğü ~16s ömürlü → sık topla; geç kalınırsa olay kaçar).

### Faz 2 — "Ne silindi" + "nereden"i tamamla (kritik korelasyon)
- `event_collector.py`: 4656 ObjectName+HandleId'yi geçici map'te tut; aynı LogonId/Handle için 4660 gelince
  gerçek `file_path`'i ata (placeholder yerine). Aynı LogonId'nin 4624'ünden `client_ip`/workstation çek → satıra bağla.
- (Önerilir) `file_watcher` USN tarafında MFT ParentFileReference → tam yol (`ntfs_usn_tail.py` zaten parent ref'i parse ediyor).

### Faz 3 — Birleşik forensic endpoint + sayfa
- DB: `get_file_deletion_events(source_id, username, days, severity, page)` — `file_audit_events WHERE event_type='delete'`
  **UNION** `user_access_logs WHERE access_type='delete'` (iki yol birleşmezse biri ekranda görünmez). Şablon: `get_audit_events` (≈2677).
- Şema: yeni tablo GEREKMEZ. `file_audit_events`'e kaynak IP yok → `details_json`'a koy (şema değişmez) veya idempotent `ALTER TABLE … ADD COLUMN client_ip TEXT`.
- API: `GET /api/forensic/file-deletions?source_id=&username=&days=30&severity=&page=1` (anomaliler yanına, `get_audit_events` deseni). Opsiyonel `/export` (XLS — `report_exporter` mevcut), `POST /api/collect-events`.
- UI: "Güvenlik" grubuna menü `🗑️ Dosya Silme Olayları` → `showPage('forensic-file-deletions')`; sayfa konteyneri Anomaliler (≈919) şablonu; `loadForensicDeletions()` (loadAnomalies şablonu). Sütunlar: Zaman | Kullanıcı | **Tam Yol** | Kaynak IP/Workstation | Logon Tipi | Boyut | Tespit (USN/EventLog) | **Kurtarılabilir (gölge rozeti)** | Detay. Üstte özet kartlar + (ops.) saatlik silme yoğunluğu grafiği. Satır→`loadUserDetail()` drill-down.

### Faz 4 — Kurtarma işareti (VSS)
- `vssadmin list shadows` / verilen shadow kökünde dosya var mı → `details_json.recoverable=true/false` + `shadow_path`. Tam VSS entegrasyonu yerine ilk fazda "gölgede mevcut" rozeti yeterli.

### Faz 5 (opsiyonel)
- Synology Drive `daemon.log` → senkron yönü (source local/remote) ayrı alan.
- `audit.chain_enabled:true` (delil bütünlüğü).

## Riskler / önkoşullar
- **Event Log kapalı** → `user_access_logs` boş; `_detect_mass_delete` ve IP/forensic değeri wire-up'a kadar çalışmaz.
- **"Ne silindi" iki yolda da yarım** (4660 placeholder + USN sadece ad) → korelasyon/MFT lookup şart, yoksa "tam yol" kolonu boş.
- **Kaynak IP yalnız SMB/5145'te** var; yerel/konsol silmede yok.
- **UNC hedefte USN yok** (polling) → o senaryoda Event Log (Yol B) ana kaynak → Faz 1 zorunlu.
- **Retroaktif değil:** toplayıcılar "şu andan itibaren"; geçmiş olay ancak günlük/USN hâlâ tutuyorsa. (Adli incelemede manuel `fsutil`/`wevtutil` yapıldı; proje bunu otomatikleştirir ama geçmiş garanti değil.)
- **Önkoşul script:** `scripts/Configure-FileAudit.ps1` (SACL/auditpol/4GB Security log "overwrite as needed") çalıştırılmalı; admin yetkisi (Event Log + SACL + USN okuma).
- **bridge/VSS/Synology entegrasyonu** projede şu an YOK.

**Özet:** Yeni tablo gerekmez; eksik = (1) EventCollector wire-up, (2) tam yol + IP korelasyonu, (3) iki tabloyu UNION eden tek forensic endpoint/sayfa, (4) VSS rozeti. `get_audit_events`/`loadAnomalies` şablonları doğrudan kopyalanabilir.
