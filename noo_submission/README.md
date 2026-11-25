# NOO Submission & Store Validation — Alur Generate Store ID

Dokumen ini menjelaskan alur data NOO dari submit sampai masuk spreadsheet, dan bagaimana Store ID
digenerate oleh dua platform (Streamlit dan Apps Script) tanpa kembar.

## 1. Komponen

| Komponen | Fungsi |
|---|---|
| **Streamlit** (`app_submission.py`, `app_main.py`, `shared.py`) | Submit NOO, Store Validation, Generate ID, Save changes, Download NOO |
| **Supabase** | Penyimpanan utama data NOO (`INPUT_NOO`) dan counter nomor pusat (`store_id_counter` + fungsi `ambil_nomor`) |
| **Spreadsheet NOO** (tab `RAW`) | Salinan data NOO yang sudah di-Save, dipakai tim |
| **Spreadsheet Generator** (`NEW Generator`, `Generator G2G`, `Generator ARCHIVE`) | Tempat user spreadsheet generate ID lewat Apps Script |
| **Apps Script** | Mengisi Store ID otomatis di tab Generator |

## 2. Alur data NOO

```
Submit NOO (Manual / Upload)
   │
   ▼
Supabase INPUT_NOO   (tagging = Need Review, Store ID kosong)
   │              ← TIDAK masuk spreadsheet saat submit
   ▼
Store Validation (Streamlit)
   │  user mengubah Validation, edit kolom, klik Generate ID
   │  (ID hanya ada di layar, belum tersimpan)
   ▼
Save changes
   ├─ simpan ke Supabase (Store ID, status, kolom yang diedit)
   └─ baru kemudian disalin ke spreadsheet NOO tab RAW
        (baris belum ada → ditambah lengkap; sudah ada → di-update)
```

Aturan utamanya: **spreadsheet hanya diisi lewat Save changes**, dan yang masuk hanya data
yang sudah tersimpan di Supabase. Submit saja tidak menulis ke spreadsheet.

### Download NOO
Kolom Store ID di file download diambil dari **Supabase** (yang sudah di-Save), bukan dari layar.
ID yang baru di-generate tapi belum di-Save tampil kosong di file download.

## 3. Alur Generate Store ID

Tujuan: dua platform tidak boleh mengeluarkan ID yang sama untuk toko berbeda.

### Counter pusat (cara utama)
Satu fungsi di Supabase, `ambil_nomor(prefix, jumlah, floor)`, mengambil N nomor berurutan secara
atomik. Streamlit dan Apps Script sama-sama memanggilnya, jadi dua pemanggil bersamaan tidak pernah
mendapat nomor yang sama.

- `prefix` = kode 4 huruf (mis. `IESM`), `jumlah` = banyak baris yang butuh ID.
- `floor` = nomor tertinggi yang sudah diketahui pemanggil dari semua sumber. Counter otomatis naik
  ke atas `floor`, jadi ID lama tidak tertimpa dan counter tidak perlu diisi manual.
- Nomor yang diambil tapi tidak dipakai (mis. Generate ID lalu Refresh) menjadi lompatan.

### Di Streamlit (Generate ID)
1. Baca ulang sumber ID di sheet tanpa cache (tab `DATA GENERATOR`, `id dms`, `NEW Generator` kolom G).
2. Hitung `floor` per prefix = max dari BigQuery, Supabase, sheet, dan ID yang ada di layar.
3. Minta jatah nomor ke counter pusat (1 panggilan per prefix). ID yang sudah ada di basis dilompati.
4. ID tampil di layar dengan status belum tersimpan. Nomor **tidak digeser/dirapatkan lagi**
   (nomor di antaranya bisa sudah diambil Apps Script).

### Di Apps Script
1. Trigger jalan tiap 1 menit dan setiap ada edit di tab Generator.
2. Cari baris yang Store Name-nya terisi tapi belum punya nomor, kelompokkan per kode.
3. Baca sumber ID untuk `floor` **hanya jika ada baris yang butuh nomor**.
4. Minta jatah nomor ke counter pusat (1 panggilan per kode), lalu bagikan berurutan ke baris.
5. Tulis ID, kode, nomor, dan status `OK` sekaligus.

### Saat Save changes
ID yang belum tersimpan dicek lagi ke sheet (tanpa cache) dan Supabase. Jika ternyata sudah dipakai
toko lain, ID dibuat ulang dan toast menampilkan `ID lama → ID baru`.

### Cadangan jika counter pusat tidak terjangkau
- **Streamlit:** kembali ke hitungan lokal (max+1), toast menampilkan "Central ID counter unavailable",
  dan menulis kunci ke tab `LOCK` di spreadsheet NOO (satu baris per sesi, dihapus setelah Save/Refresh,
  basi setelah 10 menit).
- **Apps Script:** memakai hitungan lokal seperti script lama. Script **tidak** membaca tab `LOCK`.
- Pada kondisi cadangan ini risiko ID kembar kembali ada.

## 4. Setup (sekali)

1. **Supabase → SQL Editor:** buat tabel `store_id_counter` dan fungsi `ambil_nomor`
   (SQL diberikan terpisah; fungsi harus di-grant ke role yang dipakai key, mis. `anon`).
2. **Apps Script → Project Settings → Script properties:** isi
   - `SB_URL` = URL project Supabase (tanpa `/` di akhir)
   - `SB_KEY` = key Supabase yang sama dengan yang dipakai Streamlit
   lalu klik **Save script properties**.
3. **Apps Script:** jalankan `assignNumbers` sekali manual (setujui izin `UrlFetchApp`),
   lalu jalankan `installTriggers` sekali.
4. **Spreadsheet NOO:** harus di-share (Editor) ke service account `noo-spreadsheet`
   agar Save changes bisa menulis ke tab `RAW`. Tab `LOCK` dibuat otomatis bila diperlukan.

Urutan: SQL dulu, baru Apps Script. Jika Apps Script dipasang lebih dulu, ia memakai hitungan lokal
sampai SQL dijalankan.

## 5. Konfigurasi penting

| Lokasi | Isi |
|---|---|
| `shared.py` → `PAKAI_COUNTER_PUSAT` | `True` = pakai counter pusat; `False` = hitungan lokal + penomoran ulang lama |
| `shared.py` → `DATA_MERGE_SUMBER` | Daftar tab/kolom sheet yang dibaca sebagai ID yang sudah terpakai |
| `shared.py` → `NOO_SHEET_ID`, `NOO_SHEET_TAB` | Spreadsheet dan tab tujuan salinan NOO (`RAW`) |
| `shared.py` → `NOO_SHEET_KOLOM` | Urutan kolom di tab `RAW` |
| Apps Script → `CFG.TARGETS`, `CFG.SOURCES` | Tab yang diisi otomatis dan tab sumber `floor` |

## 6. Catatan dan batasan

- Tab `NEW Generator` dibaca Streamlit per nomor kolom (kolom G). Pembacaannya belum diuji dengan data
  asli — pastikan nomor setelah Generate ID melewati ID terbaru di tab itu.
- Setelah Generate ID → Need Review → Generate ID lagi, nomor lama tidak dipakai ulang (muncul lompatan).
  Ini disengaja agar tidak bentrok dengan nomor yang diambil Apps Script.
- Key Supabase (`anon`) saat ini tertulis di `shared.py`. Siapa pun yang memegangnya dapat menaikkan
  counter (nomor terbuang, tidak ada data bocor). Disarankan dipindah ke `st.secrets`.
- Fungsi `kirim_noo_ke_sheet` di `shared.py` tidak lagi dipakai (submit tidak menulis ke sheet).
