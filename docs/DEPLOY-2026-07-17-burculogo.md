# Deploy Runbook — burculogo (2026-07-17 dalgası)

> Operatör RDP + read-only bridge doğrulaması için. Deploy **operatör tarafından
> kutuda (RDP)** yapılır; `update.cmd` bridge üzerinden güvenilir sürülemez
> (interaktif E/H promptu + snapshot + child-kill). Bridge yalnızca **read-only
> doğrulama** için (HTTP/Get-Service/git gate'e takılır → **pure file read** kullan).

## Mevcut durum (bridge ile doğrulandı, 2026-07-17)

| | Kutu (burculogo) | master |
|---|---|---|
| Sürüm | ~`20775e9` (index.html mtime 2026-07-16 14:38) | `6376b86` |
| İçerir | #341–344 (Faz 1) | + #346/#356/#358/#360 (Faz 2–5), #348/#350/#352/#354, #359 |
| `user_activity.enabled` | **false** (forensic EventLog KAPALI) | — |
| `compliance.pii.enabled` | true | — |
| `Configure-FileAudit.ps1` | **kutuda mevcut** ✅ | — |
| Faz 3 sayfası / vendor d3 | yok / yok | var / var |

## Adım 1 — Kod güncelle (RDP)

```
.\update.cmd
```
- Snapshot promptu: **H** = 2–3 GB'lik yedeği atla (hızlı) · **E**/Enter = yedek al (güvenli varsayılan). (#352)
- Çeker: #348 (erişim sıklığı fix), #350 (stale-tab banner), #352, #354 (self-host grafik), #346/#356/#358/#360 (forensic Faz 2–5), #359 (perf).

## Adım 2 — Bir kez taze yükle (zorunlu, tek sefer)

Tarayıcıda **`http://localhost:8085/?yeni=1`** (veya `Ctrl+Shift+R`). Bu, #350
stale-tab banner'ını devreye alır → bundan sonraki her deploy'da "yeni sürüm"
bandı otomatik çıkar, bu cache ağrısı biter.

## Adım 3 — Forensic'in VERİ göstermesi için (opsiyonel ama gerekli)

Faz 2–5 kod olarak gelir ama "Dosya Silme Olayları" sayfası **EventLog yolu
açık değilse yalnız USN** gösterir. Tam veri için (admin PowerShell):

```powershell
# 1) SACL + auditpol + 4 GB Security log ("overwrite as needed")
.\scripts\Configure-FileAudit.ps1        # kutuda mevcut

# 2) config\config.yaml (KÖK config.yaml DEĞİL — app config\ alt-dizinini okur)
#    user_activity.enabled: true
#    (opsiyonel) audit.chain_enabled: true   # delil zinciri (tamper-evident)
# 3) dashboard'u yeniden başlat (manuel proc'u durdur + başlat)
```

## Doğrulama checklist (bridge read-only / UI)

- [ ] **Grafikler** `/static/vendor/`'dan yükleniyor, CDN yok, console temiz (#354) — internet kesikken bile çalışır
- [ ] Kaynaklar'da **✏️ Arşiv Hedefi** butonu görünür (taze yükleme sonrası) (#342)
- [ ] **Erişim Sıklığı** tamamlanmış tarama için yükleniyor (#348)
- [ ] **Güvenlik ▸ 🗑️ Dosya Silme Olayları** sayfası açılıyor (#356)
- [ ] EventLog açıksa: silme satırları tam yol + kaynak IP ile geliyor; **🛡️ Kurtarılabilir** butonu ✅/❌/⚠️ döndürüyor (#358)
- [ ] **Delil Zinciri** kartı durumu gösteriyor (kapalıysa config gate'i) (#360)
- [ ] `/api/db/stats` ve Kullanıcı Aktivite tarama sırasında kilitlenmiyor (#359 read-pool)
- [ ] Başlangıç saniyeler içinde port'a bind oluyor (#341); retention arka planda

## Bridge read-only doğrulama örnekleri (gate'e takılmaz)

```powershell
# deploy sonrası sürüm teyidi (pure file read):
(Get-Item 'C:\FileActivity\src\dashboard\static\index.html').LastWriteTime
$i = Get-Content 'C:\FileActivity\src\dashboard\static\index.html' -Raw
"Faz3: "   + [bool]($i -match 'fdel-table')
"vendor: " + [bool]($i -match '/static/vendor/d3')
"chain:  " + [bool]($i -match '_loadForensicChain')
```
