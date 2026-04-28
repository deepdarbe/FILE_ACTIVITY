---
category: General
title: "Performance benchmarks — paylaş"
---

# Performance benchmarks — paylaş

Sahadaki gerçek rakamlar bize altın değerinde. Roadmap önceliği,
Hyperscan opt-in default'unu açıp açmama, MFT vs SMB seçimi —
hepsinin temeli sizin paylaşacağınız ölçümler.

## Şablon

```
- **Donanım:** CPU / RAM / disk (NVMe? HDD?) / network (1G / 10G)
- **Share boyut:** file count + total size (örn. 2.5M / 8 TB)
- **Backend:** MFT (lokal NTFS) ya da SMB (`os.walk` UNC)
- **Scan süresi:** baştan sona
- **PII engine:** `re` ya da `hyperscan`?
- **Memory peak:** scan sırasında dashboard process en yüksek
- **Build:** sidebar version SHA
- **Notlar:** atlamak istediğin tuhaflık (ilk scan vs incremental,
  cache hit etkisi, vs.)
```

## Bizim referans

- **Donanım:** 8 vCPU / 32 GB RAM / NVMe / 1G NIC (lab)
- **Share boyut:** ~1M dosya MFT (lokal NTFS test mount)
- **Backend:** MFT
- **Scan süresi:** ~3 dk (cold), ~45 sn (incremental)
- **PII engine:** `hyperscan` (Linux container'da test)
- **Memory peak:** ~50 MB scan boyunca (dashboard process)
- **Build:** `1.9.0-rc1`
- **Notlar:** PR #136 sonrası MFT progress phase'leri görünüyor,
  hangi fazda zaman geçiyor net oldu — `phase=enriching` (ACL/owner
  lookup) en pahalısı, toplam süreyi ~%40 buradan harcıyoruz.

## Karşılaştırma noktaları

- **Hyperscan vs `re`:** Linux'ta benchmark harness (#70 / PR #73)
  ile **11x** ölçüldü. Sizin payload'ınızda da aynı oran mı?
- **MFT vs SMB:** Aynı dataset üzerinde 2 backend ile koşan var mı?
- **Scan tekrarları:** Cold vs warm cache farkı?

Yorumda paylaşın — düzenli derleme yapıp roadmap'e referans
ekleyeceğiz.
