---
category: Show and tell
title: "FILE_ACTIVITY ne için kullanıyorsun?"
---

# FILE_ACTIVITY ne için kullanıyorsun?

Toplulukta kim ne için kullanıyor merak ediyoruz. Geliştirme önceliği
belirlerken çok yardımcı oluyor.

## Şablon (yorumda doldur)

```
- **Hangi share?** (örn. departman dosya server'ı, NAS, AD home dir,
  proje arşivi)
- **File count + size?** (örn. 2.5M dosya / 8 TB)
- **En değerli feature?** (örn. duplicate detection, PII tarama,
  archive policy, chargeback)
- **Eksik bulduğun?** ("Şu özellik olsa hayatım kolaylaşır" listen)
- **Hangi build'desin?** (Sidebar version SHA — `1.9.0-rc1 (xxxxxxx)`)
```

## Bizim referans

- **Share:** karma — bir AD home (~800 user), bir proje paylaşımı,
  bir scan/archive test mount'u
- **File count:** 1M MFT + ~300K SMB
- **En değerli feature:** Duplicate detection (#83 + #110) +
  capacity forecast (#113). 2 hafta'da 600 GB temizlik.
- **Eksik:** Mobile approval flow (Phase 2 — #112). Şu an sadece
  desktop'tan onay verilebiliyor.
- **Build:** `1.9.0-rc1`

Sıra sizde — sahnedeki use case'ler benzer mi, çok farklı mı?
