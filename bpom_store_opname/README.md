# BPOM Stock Opname Toko

Pengguna memilih **Region** lalu **Toko** (daftar dari BD; bila toko adalah grup, pilih nama grup), men-download template berisi
seluruh SKU + nomor BPOM resmi untuk toko itu, mengisi **QTY**, lalu meng-upload-nya. Hasil disimpan ke **satu** Google
Spreadsheet utama (satu worksheet). Tidak ada mode revisi. Redaksi di layar mengikuti dokumen "Toko - Redaksi Pengisian BPOM survey".

**Satu file = satu toko (atau satu grup).** Untuk toko/grup lain, pilih lalu download template-nya lagi.

## Alur di layar

1. **Pilih Region**, lalu **Toko** (bisa diketik untuk mencari; grup tampil sebagai satu pilihan `NAMA GRUP · grup (n toko)`).
2. **Tanggal Stock Opname + Nama PIC.** *Periode* (`YYYY-MM`) diisi otomatis dari tanggal itu.
3. **Download template** (tombol merah; sheet "Cara Pengisian" memuat toko terpilih) dan isi hanya kolom QTY.
4. **Upload .xlsx** - diperiksa otomatis; satu baris salah = seluruh upload dibatalkan (tidak ada data masuk).
5. **Submit.**

## Aturan validasi

- Header persis sama dengan template (case-sensitive, urutan sama); tidak boleh ada kolom tambahan/hilang. Baris judul di atas header diabaikan.
- 6 kolom: `Nama Toko (By Group)`, `PRODUCT ID`, `PRODUCT NAME`, `ISI (GR / ML)`, `BPOM NUMBER`, `QTY`.
  (Template resmi lama memakai header `Distributor Branch` di kolom A; template yang di-download memakai `Nama Toko (By Group)`.)
- Kolom A tiap baris harus sama dengan toko yang dipilih; semua kolom wajib diisi.
- QTY bilangan bulat 0 atau lebih (**tidak boleh negatif**; keputusan 2026-10-26). Template memasang validasi Excel pada kolom QTY.
- PRODUCT ID di file harus SKU di katalog (Sheet1 template). Nomor BPOM di luar katalog = **BPOM tambahan**.
  PRODUCT NAME dan ISI untuk pasangan resmi harus sama dengan katalog. SKU di luar template hanya lewat form di bawah.
- Tidak ada kombinasi PRODUCT ID + BPOM NUMBER ganda dalam satu file. Kombinasi katalog yang tidak ada di file hanya **peringatan**.
- Setelah file valid, sheet dicek satu kali: bila **Region + Nama Toko + periode + PRODUCT ID + BPOM NUMBER** sudah ada,
  **seluruh input ditolak** (hubungi BD).
- Semua baris ditulis dalam satu request `spreadsheets.batchUpdate` (atomik); nilai sebagai `stringValue`/`numberValue`.

## Tambah SKU / nomor BPOM langsung

Di Langkah 3 ada form **Tambah SKU / nomor BPOM langsung (opsional)**: pilih SKU (label `SKU - product name`, bisa diketik) dari
`reference/Daftar SKU.xlsx` (diekspor BD dari master product), isi nomor BPOM dan QTY, lalu **Tambah ke daftar**.
Aplikasi **tidak memakai BigQuery**: semua data referensi berupa file, dan service account hanya butuh Google Sheets.
Baris yang ditambahkan digabung
dengan isi file saat validasi dan ikut tersimpan dalam satu `batchUpdate` yang sama (dikosongkan setelah sukses).

- SKU boleh **di luar template**. Nama produk dan ISI diambil dari katalog template bila SKU-nya ada di sana; bila tidak, dari
  `Product Name` dan `Pack Size` dari Daftar SKU.
- Kombinasi SKU + BPOM yang sama dengan baris file atau baris tambahan lain ditolak (seluruh input batal). Upload file Excel tetap wajib.
- Bila `Daftar SKU.xlsx` belum ada atau rusak, hanya fitur ini yang nonaktif (pesan jelas); baris tambahan tetap bisa diketik di Excel.

## Data referensi (dari BD)

| File | Isi |
|---|---|
| `reference/Daftar Store.xlsx` | **Wajib ada sebelum dipakai.** Kolom `Region`, `Nama Group Toko` (opsional), `Nama Toko` (urutan/huruf bebas, kolom lain diabaikan, baris judul di atas header boleh). Hanya sheet pertama dibaca |
| `reference/Daftar SKU.xlsx` | Kolom `SKU`, `Product Name`, `Pack Size` (opsional; urutan/huruf bebas, baris judul boleh). Sumber dropdown form "Tambah SKU langsung". Opsional: tanpa file ini fitur itu nonaktif |
| `template/Contoh Excel Response.xlsx` | Template resmi **sekaligus katalog SKU** (`Sheet1`). Sheet kerja BD di file ini tidak ikut ke pengguna |

Aturan daftar toko: baris dengan `Nama Group Toko` terisi menjadi **satu pilihan atas nama grup** (semua toko dalam grup digabung);
baris tanpa grup menjadi pilihan atas nama toko. Pilihan unik per (Region, nama). Daftar ditolak **seluruhnya** bila: kolom Region/Nama
Toko tidak ada, ada baris tanpa Region atau nama, atau satu nama muncul sebagai grup sekaligus sebagai toko tunggal di region yang sama.
Bila `Daftar Store.xlsx` belum ada, aplikasi menampilkan "Daftar toko belum tersedia" dan form tidak tampil.

### Memasang data dari BD

```
.\.venv\Scripts\python tools\prepare_reference.py --stores "C:\...\Daftar Store.xlsx"                       # hanya memeriksa
.\.venv\Scripts\python tools\prepare_reference.py --stores "C:\...\Daftar Store.xlsx" --skus "C:\...\Daftar SKU.xlsx" --catalogue "C:\...\Katalog.xlsx" --install
```

Tanpa `--install` tidak ada yang ditulis; pemasangan hanya bila SEMUA pemeriksaan lolos, dan semua masalah dilaporkan sekaligus.
Katalog SKU+BPOM (`--catalogue`): format template (`PRODUCT ID`, `PRODUCT NAME`, `ISI (GR / ML)`, `BPOM NUMBER`) atau format BD (`SKU`, `BPOM Number`; nama dan ISI
dilengkapi dari katalog saat ini lalu dari daftar SKU); hasilnya menggantikan baris data `Sheet1` pada template.
Setelah dipasang: `python -m pytest tests`, cek lokal, commit, push.

## Skema worksheet (14 kolom)

Region, Nama Toko (By Group), Periode Stock Opname, Tanggal Stock Opname, Nama PIC, PRODUCT ID, PRODUCT NAME, ISI (GR / ML),
BPOM NUMBER, QTY, created_date, created_by, updated_date, updated_by.

Header dibuat otomatis bila tab kosong (grid dilebarkan bila kurang dari 14 kolom). Header lain yang berbeda **tidak pernah ditimpa** (error);
tab dengan skema lama (16 kolom, versi sebelum 2026-10-26) harus dikosongkan atau diganti tab baru.

## Setup

1. Salin `.streamlit/secrets.toml.example` ke `.streamlit/secrets.toml`; isi `[connections.bigquery]` (hanya nama section untuk service account; tanpa akses BigQuery) dan
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

## Kapasitas dan beban (target: 100 pengguna bersamaan)

| Titik berat | Penanganan |
|---|---|
| Google Sheets (kuota 60 baca + 60 tulis per menit per service account) | Handle spreadsheet/worksheet/header dibuka **sekali per proses**. Cek duplikat memakai **indeks di memori** (satu `batchGet` 5 kolom, dimuat ulang tiap 2 menit dan diperbarui oleh penulisan sendiri), bukan membaca seluruh sheet tiap submit. Penulisan **digabung (group commit)**: submit yang datang bersamaan masuk dalam satu `batchUpdate`, diberi jarak 1,1 detik; 429 dicoba ulang dengan backoff; kegagalan yang hasilnya tidak pasti memaksa indeks dibaca ulang. Satu lock per (region, toko). |
| CPU/memori per rerun | Daftar toko, katalog, dan daftar SKU memakai `cache_resource` (tanpa salinan per rerun); template dibangun sekali per toko lalu di-cache; hasil parse file di-memo per isi file; tidak ada rerun saat tombol download diklik. |
| Data ke browser | Region dengan lebih dari 1.500 pilihan toko memakai kotak cari (maks. 50 hasil), bukan dropdown ribuan opsi. |
| Konfigurasi | `.streamlit/config.toml`: tanpa file watcher, toolbar minimal, tanpa stack trace ke pengguna. |

Uji beban lokal (server Streamlit asli, Google Sheets tiruan dengan kuota 60/60 per menit, 100 browser sekaligus, versi sebelum perubahan
Region -> Toko): 88 dari 100 sesi selesai sampai "Berhasil menyimpan" dengan baris tepat; 0 error 429; 1 kali baca sheet untuk seluruh sesi;
31 `batchUpdate` untuk 88 submit; memori server puncak sekitar 250 MB. Sisanya gagal di sisi browser pada rig yang jenuh.

## File

`app.py` (UI) · `validation.py` (validasi form + Excel) · `reference_data.py` (baca daftar toko + katalog) · `master_product.py` (daftar SKU dari file) ·
`tools/prepare_reference.py` (periksa + pasang data BD) · `template_builder.py` (template per toko) · `sheets_store.py` (Google Sheets) · `tests/`
