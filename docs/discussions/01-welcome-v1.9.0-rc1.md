---
category: Announcements
title: "v1.9.0-rc1 Release Candidate"
---

# v1.9.0-rc1 Release Candidate

Selam topluluk!

`v1.9.0-rc1` etiketini master'a bastık — **47 PR + 4 direkt commit**.
Şu an müşteri prod testindeyiz; geri dönüşler gelirse rc2 ile
toparlayıp final `v1.9.0`'a geçeceğiz.

## Highlights

- **Hyperscan PII** — Linux'ta default `re`'ye göre **11x** hızlanma
  (Windows fallback `re`)
- **Auto-backup** Phase 1 + **opt-in auto-restore** corruption durumunda
- **9 yeni dashboard sayfası** — Security (Orphan SIDs, Ransomware,
  ACL), Compliance (PII findings, Retention, Legal Holds),
  Integrations (Syslog, MCP) + System (Backups)
- **Sidebar responsive** — scroll, search, collapse, narrow + mobile
  hamburger
- **MFT scanner incremental progress** + phase reporting
- **Streamlit analytics playground** (opt-in, read-only, admin-only)
- **Two-person approval framework** Phase 1
- **Chargeback / cost-center raporu** Phase 1
- **Quota / capacity forecast**

Tam liste: [Release notes](../release-notes/v1.9.0-rc1.md).

## Yükseltme

```cmd
C:\FileActivity\update.cmd
```

Snapshot otomatik. Veri / config / log korunur. Sidebar üst sol köşede
`1.9.0-rc1 (<sha>)` görünmeli.

## Sıradaki

- rc2 (varsa) — müşteri test bulguları
- post-rc1 ideas: [Roadmap brainstorm tartışması](03-roadmap-brainstorm.md)

Test eden, hata bulan, "şu da olsa" diyen — buraya yazın.
