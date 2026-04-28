# FILE ACTIVITY — Sorun Giderme (Decision Tree)

> "X oluyor → Y yap" tarzı hızlı arama. Daha derin senaryolar için
> [`operator-runbook.md`](operator-runbook.md).

---

## Dashboard yüklenmiyor / boş ekran

**Adım 1 — Sürüm kontrolü.**

Sidebar üst sol köşedeki version etiketine bak. Beklenen: `1.9.0-rc1` +
commit SHA (örn. `1.9.0-rc1 (a556ac7)`). Eskiyse `update.cmd` çalıştır.

**Adım 2 — Log'a bak.**

```powershell
Get-Content C:\FileActivity\logs\dashboard.log -Tail 50
```

Sık nedenler:

| Log mesajı | Anlam | Çözüm |
|---|---|---|
| `port 8085 already in use` | Eski process hâlâ çalışıyor | `taskkill /F /IM FileActivity.exe`; service ise `Restart-Service FileActivity` |
| `database is locked` | Scan + dashboard çakışma | Scan bitmesini bekle; sürekli olursa runbook §"Read/write contention" |
| `PRAGMA quick_check timeout` | DB corruption probe | Runbook §"Corruption probe modları" — mode'u `skip`'e al |
| `no such table: scan_runs` | Migration eksik / DB silinmiş | Backup'tan restore (`/api/backup/restore/{id}`) |

**Adım 3 — Ağ kontrolü.**

`http://localhost:8085` lokalde açılıyor ama remote açılmıyor → Windows
Firewall kuralı. `setup-source.ps1` rolünü atlamış olabilir:

```powershell
New-NetFirewallRule -DisplayName "FileActivity Dashboard" `
                    -Direction Inbound -LocalPort 8085 `
                    -Protocol TCP -Action Allow
```

---

## Cleanup endpoint 422 dönüyor

**Belirti:**

```
POST /api/scans/cleanup?keep_last=0
→ 422 Unprocessable Entity
```

**Sebep:** Eski validator `keep_last >= 1` zorunlu kılıyordu.

**Çözüm:**

- **Eski build:** En az 1 ile çağır: `?keep_last=1`
- **Yeni build (PR #133/#134 sonrası):** `keep_last=0` legal — "tüm
  taramalar temizlensin" anlamı. Ya da yeni alias kullan:
  `?keep_last_n_scans=0`. Sürüm kontrolü: SHA `1de880c` veya sonrası.

İlgili: #131 (cancel flag), #132 (read-only cursor), #133 (cleanup
keep_last=0), #134 (3 prod bug fix PR).

---

## "Konuma Git" Explorer açmıyor

**Belirti:** Bir dosya/dizin satırında "Konuma Git" → hiçbir şey olmuyor
ya da "Bu işlem desteklenmiyor" hatası.

**Adım 1 — Lokal vs remote kontrolü.**

```powershell
# Dashboard'a aynı host'tan bağlıysan: localhost:8085 → server-side Explorer
# Remote browser'dan bağlıysan: <server>:8085 → server-side Explorer YANLIŞ
```

**Sebep:** PR #85 öncesi dashboard her durumda server'da
`explorer.exe <path>` çalıştırıyordu. Remote bağlandığında server'ın
masaüstünde Explorer açılıyor, sen göremiyorsun (#82 Bug 1).

**Çözüm:**

- **Yeni build (PR #85 sonrası):** Otomatik tespit. Remote ise path
  clipboard'a kopyalanır + toast: "Path kopyalandı, kendi makinende
  Win+R yapıştır". Lokalse Explorer açılır.
- **Eski build:** `update.cmd` çalıştır. Sürüm kontrolü: SHA `5163724`
  veya sonrası.

---

## XLSX export 1M+ satırda crash

**Belirti:** Cleanup raporu / Duplicates listesi büyük; XLSX export
"Excel can't open" diyor ya da export 0-byte dosya üretiyor.

**Sebep:** Excel hard limit: 1,048,576 satır / sheet.

**Çözüm 1 — Multi-sheet (PR #130, #122):**

Yeni build otomatik olarak 1M üzerini birden fazla sheet'e böler
(`Sheet 1` 1-1M, `Sheet 2` 1M-2M, ...). Sürüm kontrolü: SHA `3d5eb0a`
veya sonrası. Eski build'de `update.cmd`.

**Çözüm 2 — CSV fallback:**

XLSX yerine CSV iste:

```
GET /api/reports/duplicates/{id}/export?format=csv
```

CSV'nin satır limiti yok. Excel açarken büyük CSV'yi Power Query
import'la (drag-drop büyük dosyada hang olur).

---

## "MFT okunuyor (0 kayıt)" — banner sayıyı gösteriyor ama KPI 0

**Belirti:** Üst ops banner "MFT phase, 234,567 dosya işlendi" diyor
ama Sources kartı + DOSYA KPI hâlâ "0".

**Adım 1 — Sürüm kontrolü.**

Sidebar version SHA. **Beklenen:** `86fd839` (PR #138) veya sonrası.

| Sürüm | Davranış |
|---|---|
| < `b63cbaa` | MFT phase'inde hiçbir progress yok, "0 kayit" tüm scan boyunca |
| `b63cbaa` (PR #136) | Phase + count banner'da görünür ama Sources/KPI senkronize değil |
| `86fd839` (PR #138) | Sources + DOSYA KPI `live_count` ile banner'a senkron |
| `c483041` (PR #140) | Inline label + bottom status bar da senkron |

**Adım 2 — Eski build'de ne yap?**

`update.cmd`. Operasyon kritikse şimdilik banner'ı referans al; KPI
scan bitince doğru değere oturur.

İlgili: #135 (MFT progress), #137/#138 (Sources/KPI sync), #140
(inline label).

---

## "Tarama bitiyor, banner sıfırlanmıyor"

**Belirti:** Scan tamamlandı, raporlar geldi, ama üst banner hâlâ
"Tarama devam ediyor" diyor.

**Sebep:** Ops tracker (PR #129) state cleanup eksiği. Banner state
ayrı bir endpoint'ten polling yapıyor — bazen final transition kaçıyor.

**Geçici çözüm:** Sayfayı F5 ile yenile.

**Kalıcı çözüm:** SHA `c483041` veya sonrası — live ops count
tutarsızlık fix.

---

## Sidebar mobile'da görünmüyor / sığmıyor

**Belirti:** Tablet/mobile erişimde menü kayıp ya da içerik üstüne
düşüyor.

**Çözüm:** PR #126 (SHA `d6efa0a`) ile responsive sidebar:
- Hamburger menü <768px
- Scrollable, collapse, search, narrow mode

Eski build'de `update.cmd`.

---

## Audit chain "broken at seq N"

**Belirti:**

```
GET /api/audit/verify
→ {"verified": false, "broken_at_seq": 14523}
```

**Sebep:** Bir audit satırı manuel SILINMIŞ ya da modify edilmiş.
SHA256 zinciri kırıldı.

**Çözüm:** Geri dönüş yok — zincir append-only sözleşme. Yapılacaklar:

1. Backup'tan en son sağlam state'i restore et (audit dâhil tüm tabloyu
   geri alır)
2. Compliance ekibine bildir; tampering window'unu (broken_at_seq
   timestamp) raporla
3. Sebep arşivlemesi: kim DB'ye direkt SQL atmış? `audit_events`
   tablosuna manuel `INSERT`/`DELETE` yasak — sadece API üstünden

İlgili: #59 (legal hold), audit chain SHA256 — runbook §"Audit chain".

---

## Daha fazla

Decision tree'de yoksa: [`operator-runbook.md`](operator-runbook.md) — sık
senaryolar bölümü daha geniş.
