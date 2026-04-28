---
category: Q&A
title: "Operator runbook — common scenarios"
---

# Operator runbook — sık senaryolar

Sahada en sık karşılaşılan 4 durumu tek başlık altında topladım. Tam
runbook: [`../operator-runbook.md`](../operator-runbook.md).

## 1. WAL şişti, dashboard hung

**Belirti:** `data/file_activity.db-wal` 1 GB+ , dashboard "Yükleniyor..."
takılı.

**Çözüm:**

```powershell
Stop-Service FileActivity   # ya da: taskkill /F /IM FileActivity.exe
sqlite3 C:\FileActivity\data\file_activity.db "PRAGMA wal_checkpoint(TRUNCATE);"
Start-Service FileActivity
```

Tekrar ediyorsa scan/archive zamanlamasını üst üste koyma.

## 2. Snapshot manuel

Riskli işlemden önce manuel backup:

```bash
curl -X POST http://localhost:8085/api/backup/run
```

`InstallDir\backups\file_activity_<TIMESTAMP>.db` üretir (SQLite Online
Backup API — DB kapatmaya gerek yok).

## 3. Tarama durdur

Yanlış kaynak / prod saatleri / herhangi bir sebep:

```bash
curl -X POST http://localhost:8085/api/scan/{source_id}/stop
```

PR #134 sonrası: cancel flag worker tarafından her batch sonunda
kontrol edilir. ~5-10 sn'de durur.

## 4. Eski tarama temizliği

Son N taramayı tut, gerisini sil:

```bash
curl -X POST "http://localhost:8085/api/scans/cleanup?keep_last=5"
```

`keep_last=0` legal (#133/#134) — "tüm taramalar". Eski build'de 422
döner, `update.cmd` çalıştır.

## Soru / paylaşım

- Hangi senaryoda runbook seni bıraktı? Yorumda yaz, runbook'a
  ekleyelim.
- Sizin shop'ta WAL ne sıklıkta şişiyor — günlük? haftalık?
- Cleanup için `keep_last` kaç tutuyorsunuz?
