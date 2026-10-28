# TIMEPHORIA Smart Coverage Tester Feedback

> **Catatan repo publik:** nilai spesifik environment (ID spreadsheet, project GCP, bucket, service account, daftar kode SKU) sengaja dikosongkan di sini menjadi `YOUR_...`. Isi nilai aslinya hanya di `secrets.toml` lokal atau di secrets Streamlit Cloud. Aplikasi ini berada di folder `timephoria_distributor_stock/` pada repo bersama; jalankan dari dalam folder tersebut.

Aplikasi Streamlit untuk supervisor mencatat **jumlah stok Tester Smart Coverage yang tersisa di gudang distributor** (stock on-hand fisik, pcs). Pengguna utama membuka dari HP. Tab Input tidak memakai login. Tab Admin memakai login dari secrets.

| Komponen | Peran |
|---|---|
| **BigQuery** `YOUR_PROJECT_ID.gt_schema.timephoria_smart_coverage_tester_feedback` | Sumber kebenaran |
| **BigQuery** `YOUR_PROJECT_ID.gt_schema.master_distributor` dan `master_product` | Master distributor dan nama SKU, **hanya dibaca** |
| **Google Sheets** `YOUR_OUTPUT_SHEET_ID` → worksheet `Distributor Stock` | Mirror pelaporan |
| **Google Cloud Storage** `gs://YOUR_BUCKET` prefix `Timephoria Distributor Stock Evidence/` | Foto evidence (JPG ≤ 2 MB) |

Aplikasi ini saudara dari Facerinna Submission Program. Tabel, spreadsheet, dan foto Facerinna tidak dipakai dan tidak diubah.

## Aturan utama

**Form.** Urutannya: **Region** → **Nama Supervisor** → **Distributor** → Tanggal Stock → daftar SKU. Daftar SKU langsung berisi semua kode di secrets `[app].sku_codes` (urutan secrets). Tidak ada tombol tambah atau hapus baris. Nama produk diambil dari `master_product` apa adanya, tanpa akhiran "(Tester)". Kode yang tidak ketemu tetap tampil dengan nama kosong dan peringatan di sidebar.

**Pembatasan Region → Supervisor → Distributor bersifat lunak** (tanpa PIN). Ini memudahkan pengisian, bukan pengamanan akses.

**Tanggal stock ≠ waktu submit.**

| Kolom | Arti | Diisi oleh |
|---|---|---|
| `tanggal_stock` | Tanggal stok yang dicatat (boleh tanggal mundur) | Supervisor |
| `created_at` | Waktu submit pertama untuk key itu; tidak berubah saat update | Otomatis |
| `updated_at` | Waktu submit terakhir | Otomatis |

Waktu yang terlihat user, di sheet, dan di metadata foto memakai **WIB** (`YYYY-MM-DD HH:MM:SS WIB`, UTC+7, tanpa DST). BigQuery menyimpan TIMESTAMP dalam UTC.

**Upsert.** Key = `tanggal_stock + distributor_code + sku_code`. `transaction_id` = UUID5 deterministik dari key itu (namespace di kode, jangan diubah). Submit ulang pada key yang sama menimpa: `created_at` dipertahankan, `updated_at` baru, supervisor / stock / foto terbaru menggantikan. Tidak ada mode revisi terpisah dan tidak ada penghapusan baris lain. SKU yang sama diketik dua kali dalam satu submit: baris terakhir yang dipakai.

**Stock.** Satu angka `stock_qty` per SKU: bilangan bulat ≥ 0 (nol boleh). Desimal, negatif, kosong, huruf, dan angka di atas INT64 ditolak. Pesan error menyebut "Stock".

**Foto.**

| Kondisi | Hasil |
|---|---|
| Total stock seluruh SKU pada submit = 0 | Foto tidak wajib |
| Total stock > 0 dan tidak ada satu pun foto | Submit diblokir |
| Ada SKU dengan stock > 0 tanpa foto sendiri, tetapi minimal satu foto terlampir | Peringatan kuat, submit tetap boleh |

Foto yang objeknya tidak relevan disosialisasikan secara verbal, tidak divalidasi aplikasi. Hanya JPG/JPEG (isi file dicek; PNG yang diberi nama `.jpg` ditolak). Hasil kompresi ≤ 2.000.000 byte yang disimpan. Piksel tidak diputar karena EXIF; tag Orientation dipertahankan; EXIF lain (GPS, perangkat) dibuang.

**Aplikasi tidak pernah menghapus atau menimpa foto.** Setiap upload menjadi objek baru (`if_generation_match=0`). Foto lama dan foto yatim (upload berhasil, BigQuery gagal) tetap di bucket. Penghapusan hanya oleh owner lewat gcloud, atas perintah eksplisit.

Urutan submit: validasi → upload foto → `MERGE` BigQuery → baca balik → mirror Sheets. Validasi gagal tidak menulis apa pun. Mirror gagal setelah BigQuery berhasil adalah peringatan, bukan gagal total.

## Tab Admin

Login dari `[admin]` di secrets. Tanpa bagian itu tab menampilkan info bahwa admin belum aktif. Setelah 5 kali gagal, login terkunci 60 detik per sesi. Tab ini hanya membaca.

- Rentang tanggal stock, default 7 hari terakhir, maksimal 92 hari.
- Satu kunjungan = satu tanggal stock yang punya data untuk distributor itu.
- **Lengkap** = pada minimal satu tanggal dalam rentang, semua SKU terisi, dan bila total stock tanggal itu > 0 ada minimal satu foto. Total 0 tidak membutuhkan foto.
- **Belum lengkap** = ada data tetapi tidak ada tanggal yang lengkap. **Belum submit** = tidak ada data di rentang.
- Perbandingan dengan submit sebelumnya (tanggal terakhir sebelum tanggal yang dipilih, untuk distributor yang sama, boleh di luar rentang). Sebelumnya 0 atau tidak ada ditampilkan sebagai "baru". Penanda bila persentase perubahan melewati ambang (default 50%).
- Galeri foto terbaru per key, 8 foto per halaman, 2 kolom.

## Master data

Query distributor (tanpa input user):

```sql
SELECT TRIM(distributor_code) AS distributor_code, TRIM(distributor) AS distributor,
       TRIM(region) AS region, TRIM(spv_tph) AS spv
FROM `YOUR_PROJECT_ID.gt_schema.master_distributor`
WHERE status = 'Active' AND TRIM(IFNULL(spv_tph, '')) NOT IN ('', '-')
  AND IFNULL(brand, '') LIKE '%TPH%'
```

Nama SKU: `master_product` dengan `WHERE sku IN UNNEST(@codes)`. Daftar kode ada di secrets, bukan di kode.

**Pengecekan 2026-10-28** (tabel master ter-refresh harian, angka bisa berubah):

- Distributor yang masuk adalah baris Active dengan `spv_tph` terisi (bukan `-`) **dan** `brand` memuat `TPH` (`LIKE '%TPH%'`). Pengecekan ulang 2026-10-28 setelah filter ini: 96 distributor, 48 supervisor, 14 region.
- Satu supervisor bisa muncul di lebih dari satu region. Dropdown supervisor disaring menurut region yang dipilih, lalu distributor disaring menurut supervisor itu.
- Region kosong pada baris yang lolos filter dikelompokkan sebagai `(Tanpa Region)`.

Setiap pembacaan yang berhasil disimpan sebagai cadangan (memori + `.cache/master_snapshot.json`, di-gitignore). Bila pembacaan berikutnya gagal, aplikasi memakai data terakhir yang valid dan menampilkan waktu baca serta penyebabnya. Di Streamlit Cloud file snapshot hilang saat app di-reboot; cadangan memori tetap hidup selama prosesnya hidup.

## Setup

1. Aktifkan BigQuery API, Cloud Storage API, dan Google Sheets API.
2. Pakai service account yang sama dengan aplikasi saudara (`YOUR_SERVICE_ACCOUNT_EMAIL`): BigQuery Job User + tulis ke dataset `gt_schema`, Storage Object Creator dan Object Viewer pada bucket evidence (bukan penghapus).
3. Tabel `timephoria_smart_coverage_tester_feedback` dipartisi `tanggal_stock` dan di-cluster `distributor_code, sku_code`. DDL ada di `bigquery_store.create_table_sql`. Semua query memakai parameter; hanya nama tabel dari secrets yang di-interpolasi, setelah lolos validasi karakter.
4. Buat spreadsheet output baru dan share sebagai **Editor** ke service account. Tab `Distributor Stock` dibuat aplikasi (header 15 kolom, freeze 1 baris). Header yang tidak cocok membuat aplikasi menolak menulis.
5. Salin `.streamlit/secrets.toml.example` menjadi `.streamlit/secrets.toml`. Di Cloud, `private_key` tetap satu baris dengan `\n`.

## Menjalankan lokal

```powershell
cd timephoria_distributor_stock
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
streamlit run app.py --server.address localhost --server.port 8501
```

Tes tanpa jaringan (BigQuery, GCS, dan Sheets diganti objek palsu):

```powershell
python -m pytest -q
```

PowerShell 5.1: jangan sambung perintah dengan `&&`.

## Deploy ke Streamlit Community Cloud

1. Commit dan push branch `feat/timephoria-distributor-stock` hanya setelah diff di-pindai (tidak ada key, ID spreadsheet, nama project, bucket, email service account, atau kode SKU nyata). Repo ini publik.
2. share.streamlit.io → Create app dari GitHub: repo `skt-project/streamlit_app`, branch `feat/timephoria-distributor-stock`, main file `timephoria_distributor_stock/app.py`, Python 3.13.
3. Advanced settings → Secrets: tempel isi `secrets.toml` lokal (`private_key` satu baris dengan `\n`).
4. Nama app di Cloud memuat **TIMEPHORIA Smart Coverage Tester Feedback**.
5. Deploy, lalu uji dari HP: submit total 0 tanpa foto (berhasil), submit stock > 0 dengan foto (berhasil), stock > 0 tanpa foto (diblokir), lalu tab Admin (login, rentang tanggal, galeri, pagination, perbandingan).
6. Setelah stabil: PR ke `main`, lalu ubah branch app di Cloud ke `main`.

Foto uji yang terupload **tidak bisa dihapus oleh aplikasi**. Pakai data uji yang tandanya jelas, dan jangan mengandalkan aplikasi untuk membersihkan bucket.

## Struktur

`app.py` (UI), `validation.py`, `service.py`, `bigquery_store.py`, `gcs_evidence.py`, `sheets_output.py`, `master_data.py`, `image_processor.py`, `config.py`, `clients.py`, `timeutil.py`, `completeness.py`, `admin_auth.py`, `admin_view.py`, `mobile.py`, `tests/`.
