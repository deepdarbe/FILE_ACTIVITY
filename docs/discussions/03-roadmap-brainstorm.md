---
category: Ideas
title: "Roadmap brainstorm — sonraki 3 ay"
---

# Roadmap brainstorm — sonraki 3 ay

`v1.9.0-rc1` ile büyük bir Wave kapandı. Şimdi sonraki 3 ay için
kapsam toplama vakti. Aşağıdaki açık issue'ları topluluğa açıyoruz —
"ben ne istiyorum" değil, **"sahada gerçekten lazım mı"** sorusunu
beraber cevaplayalım.

> Roadmap kaynağı: [`../../ROADMAP.md`](../../ROADMAP.md). Burası
> brainstorm — orayı duplicate etmiyoruz, sadece "neye ağırlık
> versek" tartışıyoruz.

## Açık ana parçalar

- **#110** — Duplicates Phase 3 (smart dedupe, hard-link önerisi)
- **#111** — Chargeback Phase 2 (faturalama entegrasyonu, e-mail
  rapor şablonları)
- **#112** — Two-person approval Phase 2 (delegation chains, mobile
  approve)
- **#113** — Capacity forecast Phase 2 (linear → ARIMA / Prophet,
  trigger alerts)
- **#114** — Pluggable storage backend Phase 2 (Postgres? S3? gerçek
  scenario var mı)

## Topluluğa sorular

1. **ElasticSearch backend gerçekten gerekli mi?** Bizdeki SQLite +
   DuckDB stack milyonlarca dosyayı kaldırıyor. ES'i sahada kim
   istiyor, hangi pain point için?
2. **Chargeback için hangi model?** Per-GB? Per-file? Department
   weighted? Sizin kuruluşta nasıl faturalanıyor?
3. **PII dışında ne tarayalım?** Şu anda regex tabanlı PII
   (TC kimlik, IBAN, e-mail, kredi kartı). Eklemek istediğiniz
   pattern? Trade secret detection? Source code leak?
4. **Mobile yeterli mi?** Sidebar PR #126 ile responsive — gerçek
   kullanım mobil tarafta var mı, yoksa daha çok desktop-only mı
   bakıyorsunuz?
5. **Yeni ana feature?** Roadmap'te olmayan ama "şu olsa" dediğiniz?

## Format

Yorumlara **yazılı paragraf** + **upvote** ile katkı yeterli. Hangi
issue'ya kaynak ayırmamız gerektiğini buradan belirleyeceğiz.
