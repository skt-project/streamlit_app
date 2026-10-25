# BPOM Stock Opname Store

Versi **level store** dari aplikasi BPOM Stock Opname distributor. User memilih **Distributor Company** lalu **Store**
(daftar dari BD), men-download template berisi seluruh SKU + nomor BPOM resmi untuk store itu, mengisi **QTY**, lalu
meng-upload-nya. Hasil disimpan ke **satu** Google Spreadsheet utama (satu worksheet). Tidak ada mode revisi.

**Satu file = satu store.** Distributor dengan banyak store harus download + upload satu kali per store.

## Alur di layar

1. **Pilih Distributor Company**, lalu **Store** (label `Store ID - Store Name · Distributor`; bisa diketik untuk mencari).
   Tidak ada dropdown Distributor Name: company diturunkan dari `reference/Daftar Distributor.xlsx` berdasarkan Distributor Name milik store.
2. **Tanggal Stock Opname + Nama PIC.** *Periode* (`YYYY-MM`) diisi otomatis dari tanggal itu.
3. **Download template** (tombol merah; sheet "Cara Pengisian" memuat store terpilih) dan isi hanya kolom QTY.
   BPOM tambahan: tambah baris baru dengan SKU yang sudah ada.
4. **Upload .xlsx** - diperiksa otomatis; satu baris salah = seluruh upload dibatalkan (tidak ada data masuk).
5. **Submit.**

## Tambah SKU / nomor BPOM langsung

Di Langkah 3 ada form **Tambah SKU / nomor BPOM langsung (opsional)**: pilih SKU (label `SKU - product name`, bisa diketik) dari
BigQuery `skintific-data-warehouse.gt_schema.master_product`, isi nomor BPOM dan QTY, lalu **Tambah ke daftar**. Baris ini digabung
dengan isi file saat validasi dan ikut tersimpan dalam satu `batchUpdate` yang sama (dikosongkan setelah sukses).

- SKU boleh **di luar template** (semua SKU di master_product, SKU tanpa nama produk dilewati). Nama produk dan ISI diambil dari
  katalog template bila SKU-nya ada di sana; bila tidak, dari `product_name` dan `pack_size` master_product.
- Kombinasi SKU + BPOM yang sama dengan baris file atau baris tambahan lain ditolak (seluruh input batal).
- Upload file Excel tetap wajib. Daftar SKU di-cache 1 jam. Bila BigQuery tidak terbaca, hanya fitur ini yang nonaktif (pesan jelas);
  tabel dapat diganti lewat `[app] master_product_table`. Service account butuh akses baca tabel + `bigquery.jobs.create`.

## Aturan validasi

- Header persis sama dengan template (case-sensitive, urutan sama); tidak boleh ada kolom tambahan/hilang. Baris judul di atas header diabaikan.
- Template punya 6 kolom: `Distributor Branch`, `PRODUCT ID`, `PRODUCT NAME`, `ISI (GR / ML)`, `BPOM NUMBER`, `QTY`.
- Kolom A tiap baris harus sama dengan distributor milik store yang dipilih; semua kolom wajib diisi.
- QTY bilangan bulat 0 atau lebih (**tidak boleh negatif**). Template memasang validasi Excel pada kolom QTY.
- PRODUCT ID di file harus SKU di katalog (Sheet1 template resmi). Nomor BPOM di luar katalog = **BPOM tambahan**. SKU di luar template hanya lewat form di atas.
  PRODUCT NAME dan ISI untuk pasangan resmi harus sama dengan katalog.
- Tidak ada kombinasi PRODUCT ID + BPOM NUMBER ganda dalam satu file.
- Kombinasi katalog yang tidak ada di file hanya **peringatan**.
- Setelah file valid, sheet dicek satu kali: bila **Store ID + periode + PRODUCT ID + BPOM NUMBER** sudah ada,
  **seluruh input ditolak** (hubungi BD).
- Semua baris ditulis dalam satu request `spreadsheets.batchUpdate` (atomik); nilai sebagai `stringValue`/`numberValue`.

## Data referensi (dari BD)

| File | Isi |
|---|---|
| `reference/Daftar Distributor.xlsx` | `Distributor Company`, `Distributor Name` |
| `reference/Daftar Store.xlsx` | **Wajib ada sebelum dipakai.** Kolom `Distributor Name`, `Store ID`, `Store Name` (urutan bebas, kolom lain diabaikan) |
| `template/Contoh Excel Response.xlsx` | Template resmi **sekaligus katalog SKU** (`Sheet1`). Sheet kerja BD di file ini tidak ikut ke user |

Daftar store ditolak **seluruhnya** bila: kolom wajib hilang, Store ID kosong/ganda, atau Distributor Name tidak ada di
daftar distributor. Store ID dibaca sebagai teks (nol di depan dipertahankan; `1234.0` dari Excel menjadi `1234`).
Bila `Daftar Store.xlsx` belum ada, aplikasi menampilkan "Daftar store belum tersedia" dan form tidak tampil.

## Skema worksheet (16 kolom)

Distributor Company, Distributor Name, Store ID, Store Name, Periode Stock Opname, Tanggal Stock Opname, Nama PIC,
PRODUCT ID, PRODUCT NAME, ISI (GR / ML), BPOM NUMBER, QTY, created_date, created_by, updated_date, updated_by.

Header dibuat otomatis bila tab kosong (grid dilebarkan bila kurang dari 16 kolom). Header lain yang berbeda **tidak
pernah ditimpa** (error).

## Setup

1. Salin `.streamlit/secrets.toml.example` ke `.streamlit/secrets.toml`; isi `[connections.bigquery]` (service account) dan
   `[app]` dengan `google_sheet_id` (**tepat di bawah header `[app]`**). File ini di-gitignore. Di Streamlit Cloud, isi lewat Settings -> Secrets.
2. Buat spreadsheet **manual** (service account tidak punya kuota Drive; 403 "storage quota") dengan tab `BPOM Stock Opname Store`,
   lalu share sebagai **Editor** ke `client_email`. Sheets API dan Drive API harus aktif.

## Menjalankan

```
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python -m streamlit run app.py
```

Tes: `.\.venv\Scripts\python -m pip install pytest` lalu `.\.venv\Scripts\python -m pytest tests`
(termasuk uji UI end-to-end dengan Google Sheets di-mock; tidak menyentuh spreadsheet asli).

## File

`app.py` (UI) · `validation.py` (validasi form + Excel) · `reference_data.py` (baca file BD) ·
`master_product.py` (daftar SKU dari BigQuery) · `tools/prepare_reference.py` (periksa + pasang data BD) · `template_builder.py` (template per store) · `sheets_store.py` (Google Sheets) · `tests/`

## Persiapan data dari BD (daftar store dan daftar SKU)

Aplikasi tidak berjalan sebelum `reference/Daftar Store.xlsx` ada. Cara memasang file dari BD:

```
.\.venv\Scripts\python tools\prepare_reference.py --stores "C:\...\Daftar Store.xlsx"                       # hanya memeriksa
.\.venv\Scripts\python tools\prepare_reference.py --stores "C:\...\Daftar Store.xlsx" --catalogue "C:\...\SKU.xlsx" --bq --install
```

- Tanpa `--install` tidak ada yang ditulis. Pemasangan hanya terjadi bila SEMUA pemeriksaan lolos; semua masalah dilaporkan
  sekaligus (bukan satu per satu), termasuk saran nama distributor yang mirip bila Distributor Name tidak cocok.
- **Daftar store:** kolom `Distributor Name`, `Store ID`, `Store Name` (urutan bebas, kolom lain diabaikan; baris judul di atas
  header boleh). Hanya sheet pertama dibaca. Dipasang sebagai `reference/Daftar Store.xlsx`.
- **Daftar SKU + BPOM:** dua format diterima: (a) `PRODUCT ID`, `PRODUCT NAME`, `ISI (GR / ML)`, `BPOM NUMBER`, atau
  (b) format BD `SKU`, `BPOM Number` (+ nama/expired, diabaikan); PRODUCT NAME dan ISI dilengkapi dari katalog saat ini lalu
  dari `master_product` (`--bq`, perintah `bq` milik akun Anda, hanya SELECT). Hasilnya menggantikan baris data `Sheet1` pada
  `template/Contoh Excel Response.xlsx` (header, judul, dan sheet lain tidak diubah). Perbedaan terhadap katalog lama
  (+baru / -hilang / nama berubah) ditampilkan sebelum dipasang.
- Setelah dipasang: `python -m pytest tests`, cek lokal, lalu commit dan push.

## Kapasitas dan beban (target: 100 pengguna bersamaan)

Yang dilakukan supaya ringan:

| Titik berat | Penanganan |
|---|---|
| Google Sheets (kuota 60 baca + 60 tulis per menit per service account) | Handle spreadsheet/worksheet/header dibuka **sekali per proses**. Cek duplikat memakai **indeks di memori** (satu `batchGet` 4 kolom, dimuat ulang tiap 2 menit dan diperbarui oleh penulisan sendiri), bukan membaca seluruh sheet tiap submit. Penulisan **digabung (group commit)**: submit yang datang bersamaan masuk dalam satu `batchUpdate`, diberi jarak 1,1 detik; 429 dicoba ulang dengan backoff; kegagalan yang hasilnya tidak pasti memaksa indeks dibaca ulang. Satu lock per (store, periode). |
| CPU/memori per rerun | Daftar store, katalog, dan daftar SKU memakai `cache_resource` (tanpa salinan per rerun); template dibangun sekali per store lalu di-cache; hasil parse file di-memo per isi file; tidak ada rerun saat tombol download diklik. |
| Data ke browser | Company dengan lebih dari 1.500 store memakai kotak cari (maks. 50 hasil), bukan dropdown ribuan opsi. |
| Konfigurasi | `.streamlit/config.toml`: tanpa file watcher, toolbar minimal, tanpa stack trace ke pengguna. |

Hasil uji beban lokal (server Streamlit asli, Google Sheets tiruan dengan kuota 60/60 per menit, 100 browser Chromium sekaligus
menjalani alur lengkap): 88 dari 100 sesi selesai sampai "Berhasil menyimpan" dengan 5.808 baris tepat (88 x 66); 0 error 429;
1 kali baca sheet dan 1 kali buka spreadsheet untuk seluruh sesi; 31 `batchUpdate` untuk 88 submit; memori server puncak sekitar 250 MB;
CPU per eksekusi script median 16 ms. Sisanya (12 sesi) gagal di sisi browser pada rig yang jenuh (100 Chromium dan server di satu laptop,
waktu muat halaman median 42 detik), bukan di server. Lihat laporan untuk batasan uji ini.
