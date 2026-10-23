# Store Sales Input

> **Catatan repo publik:** nilai spesifik environment (ID spreadsheet, project GCP, bucket, service account) sengaja dikosongkan di sini menjadi `YOUR_...`. Isi nilai aslinya hanya di `secrets.toml` lokal atau di secrets Streamlit Cloud. Aplikasi ini berada di folder `store_sales_input/` pada repo bersama; jalankan dari dalam folder tersebut (`cd store_sales_input`).

Aplikasi Streamlit untuk supervisor mencatat **penjualan store ke end consumer**.

| Komponen | Peran |
|---|---|
| **BigQuery** `YOUR_PROJECT_ID.gt_schema.store_sales_transaction` | Database utama / *source of truth* transaksi |
| **Google Sheets master** `YOUR_MASTER_SHEET_ID` | Sumber master Store (hanya **Non BA**) dan SKU |
| **Google Sheets output** `YOUR_OUTPUT_SHEET_ID` → worksheet `Store Sales` | *Reporting mirror* |
| **Google Cloud Storage** `gs://YOUR_BUCKET` | Foto evidence (JPG ≤ 2 MB), bucket privat |

Proyek ini terpisah dari aplikasi BPOM Audit Stock.

## Aturan utama

**Form.** Urutannya: **Region** → **Selected Store** (hanya store Non BA di region itu) → Tanggal Penjualan → Nama Supervisor → daftar SKU. Daftar SKU langsung berisi **semua SKU** di master (urut master), jadi supervisor tinggal mengisi Sell Out dan Stock tiap SKU. Baris boleh dihapus (🗑) dan ditambah lagi (➕); tombol "Reset daftar SKU" di sidebar mengembalikan semua SKU dan mengosongkan isian. Tidak ada bulk upload (akan melewati aturan evidence foto).

**Tanggal penjualan ≠ waktu submit.** Dua hal ini dipisah di BigQuery, sheet, dan layar:

| Kolom | Arti | Diisi oleh |
|---|---|---|
| `tanggal_penjualan` | Tanggal terjadinya penjualan (DATE, mis. `2026-10-23`); bisa tanggal mundur | Supervisor |
| `created_at` | Waktu submit **pertama** untuk key itu (mis. `2026-10-23 11:00:00 WIB`); tidak berubah saat update | Otomatis |
| `updated_at` | Waktu submit **terakhir** | Otomatis |

**Upsert.** Key transaksi = `tanggal_penjualan + store_id + sku_code`. `transaction_id` = UUID5 deterministik dari key itu.
Semua submit adalah upsert (tidak ada mode Input/Revisi terpisah):

- key belum ada → `INSERT`; key sudah ada → `UPDATE` (tanpa duplikat, boleh submit berkali-kali);
- `created_at` dipertahankan, `updated_at` selalu diperbarui;
- `supervisor_name`, `sell_out_qty`, `stock_qty`, dan foto terbaru menggantikan yang lama;
- SKU yang **tidak** ada di submit tidak disentuh (tidak ada penghapusan).

**Dua quantity per SKU.** Setiap SKU mencatat **Sell Out** (`sell_out_qty`, penjualan store ke end consumer) dan **Stock** (`stock_qty`, stock SKU di store). Keduanya wajib diisi, bilangan bulat, `>= 0` (nol boleh). Desimal (`1.5`, `3.0`) dan negatif ditolak; error ditampilkan per baris dan menyebut kolom yang salah. Tidak ada retur.

**SKU ganda dalam satu sesi.** Baris terakhir yang dipakai; baris sebelumnya diabaikan (dengan peringatan).

**Evidence foto.**

| Kondisi | Hasil |
|---|---|
| Setiap submit, berapa pun qty-nya (termasuk semua 0) | Minimal **satu** foto dari salah satu SKU, kalau tidak submit **diblokir** |
| SKU dengan Sell Out > 0 tanpa foto | Peringatan kuat, submit tetap boleh bila ada ≥ 1 foto |
| SKU dengan Stock > 0 tetapi Sell Out 0, tanpa foto | Tidak ada peringatan |

Foto per SKU bersifat opsional. Bila objek foto tidak relevan dengan evidence, itu disosialisasikan ke user secara verbal (bukan divalidasi aplikasi).

- Per SKU: `st.camera_input` (kamera HP) dan/atau `st.file_uploader` (galeri). Bila keduanya terisi, foto kamera yang dipakai.
- Hanya **JPG/JPEG**. PNG dan format lain ditolak; isi file diperiksa (PNG yang diberi nama `.jpg` tetap ditolak).
- Foto dikompres otomatis ke JPG **≤ 2.000.000 byte (2 MB)** (kualitas diturunkan, lalu resolusi bila perlu). Yang disimpan adalah hasil kompresi, bukan file asli.
- Orientasi **tidak** diubah: piksel tidak diputar berdasarkan EXIF, dan tag Orientation dipertahankan sehingga tampilan sama dengan aslinya. EXIF lain (GPS, perangkat) dibuang.
- Lokasi objek: `Store Sales Evidence/YYYY-MM/{store_id} - {store_name}/{tanggal}_{store_id}_{sku_code}_{transaction_id}_{waktu upload WIB}.jpg`. Waktu upload ada di nama supaya setiap upload menjadi objek baru.
- **Aplikasi tidak pernah menghapus atau menimpa foto.** Submit ulang dengan foto baru hanya mengarahkan BigQuery/sheet ke foto terbaru; foto lama tetap di bucket sampai ada yang memerintahkan penghapusan. Service account hanya punya role *Storage Object Creator*, jadi ini juga dijaga oleh IAM. Foto yang terupload tetapi transaksinya gagal tersimpan juga dibiarkan (dicatat di log).
- Submit ulang **tanpa** foto baru mempertahankan evidence lama.
- Metadata (Store ID/Name, SKU Code/Name, tanggal, supervisor, waktu upload WIB, transaction_id) disimpan sebagai metadata objek; ID objek, URL, nama, dan waktu upload juga ada di BigQuery dan sheet.

**Waktu.** Semua waktu yang tampil (UI, kolom `*_at` di sheet, metadata foto) memakai **WIB**, mis. `2026-10-23 11:00:00 WIB`. Kolom TIMESTAMP di BigQuery menyimpan instan yang sama (BigQuery menampilkannya dalam UTC; itu sifat tipe TIMESTAMP).

**Konsistensi.** Urutan submit: validasi → upload foto → `MERGE` BigQuery → mirror Sheets.

- Validasi gagal → tidak ada panggilan ke storage/BigQuery/Sheets.
- Upload foto atau BigQuery gagal → tidak ada transaksi tersimpan.
- Sheets mirror gagal setelah BigQuery berhasil → **bukan** gagal total; muncul warning, error dicatat di log, dan mirror ikut dikoreksi saat key yang sama disubmit lagi.

## Tab Admin (kelengkapan & galeri evidence)

Tab kedua di aplikasi, terpisah dari form input (tidak mengubah alur submit). Login memakai `[admin]` di secrets
(`username`, `password`); tanpa bagian itu, tab admin nonaktif dan tidak ada kredensial bawaan. Setelah 5 kali salah,
login terkunci 60 detik (per sesi browser, bukan perlindungan penuh: kekuatan password tetap yang utama).

Isi (hanya membaca; tidak ada tombol hapus/ubah):

- **Filter**: Tanggal Penjualan, Region, Status.
- **Ringkasan** total store, Lengkap, Belum lengkap, Belum submit; tabel **per region** (dengan % lengkap) dan **per store**
  (SKU terisi x/11, jumlah foto, supervisor, waktu submit terakhir WIB); unduh CSV.
- **Status "Lengkap"** = semua SKU di master terisi (Sell Out + Stock) **dan** minimal 1 foto evidence.
  "Belum lengkap" = ada data tapi ada SKU belum terisi dan/atau belum ada foto. "Belum submit" = belum ada data pada tanggal itu.
- **Galeri evidence**: pilih store untuk melihat detail per SKU (terisi/belum) dan fotonya, atau lihat semua foto sesuai filter
  (8 foto per halaman). Yang tampil adalah foto **terbaru** per store + SKU; foto lama tetap ada di bucket.

Foto ditampilkan lewat URL bertanda tangan (hanya-baca, kedaluwarsa 60 menit) yang dibuat service account, jadi user admin
**tidak perlu** akses langsung ke bucket. Service account memakai role *Storage Object Viewer* (hanya baca) di samping
*Object Creator*; keduanya tidak bisa menghapus.

## Tampilan HP

Aplikasi dioptimalkan untuk dibuka dari ponsel: judul dan jarak ringkas, SKU tampil sebagai teks tetap (bukan dropdown),
**Sell Out | Stock berdampingan** tanpa menumpuk, keypad **angka** muncul di kolom jumlah, tombol/input cukup besar untuk jari
(font input 16 px agar iOS tidak men-zoom), metrik admin 2x2, galeri 2 kolom, dan sidebar tertutup secara default.
Kamera HP dipakai lewat `st.camera_input`; galeri HP lewat `st.file_uploader`. Layout sudah diperiksa pada lebar 390 px
(tidak ada geser horizontal).

## Setup

### 1. Google Cloud: API dan service account

1. Aktifkan **BigQuery API**, **Cloud Storage API**, dan **Google Sheets API** di project GCP.
2. Service account yang dipakai: `YOUR_SERVICE_ACCOUNT_EMAIL` (sudah ada). Aplikasi butuh:
   - **BigQuery Job User** di project, dan akses tulis ke dataset `gt_schema` (sudah ada: role WRITER di dataset);
   - **Storage Object Creator** pada bucket evidence (lihat langkah 4).
3. JSON key service account hanya masuk ke secrets. Jangan di-commit.

### 2. BigQuery: tabel

Tabel `YOUR_PROJECT_ID.gt_schema.store_sales_transaction` **sudah dibuat** (2026-10-23) dan disesuaikan untuk dua quantity (`qty` diganti nama menjadi `sell_out_qty`, kolom `stock_qty` ditambahkan di akhir; BigQuery hanya mengizinkan kolom baru berstatus nullable, aplikasi selalu mengisinya). DDL untuk environment lain (ganti `YOUR_PROJECT_ID`); `MERGE` memerlukan billing aktif:

```sql
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.gt_schema.store_sales_transaction` (
  transaction_id STRING NOT NULL,
  tanggal_penjualan DATE NOT NULL,
  store_id STRING NOT NULL,
  store_name STRING,
  region STRING,
  store_group STRING,
  sku_code STRING NOT NULL,
  sku_name STRING,
  sell_out_qty INT64 NOT NULL,
  stock_qty INT64,
  supervisor_name STRING,
  evidence_file_id STRING,
  evidence_file_url STRING,
  evidence_file_name STRING,
  evidence_uploaded_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
)
PARTITION BY tanggal_penjualan
CLUSTER BY store_id, sku_code;
```

Semua query aplikasi (`SELECT`, `MERGE`, pengecekan duplikat) memakai *query parameter*; input user tidak pernah disambung ke SQL. Hanya nama tabel dari secrets yang di-interpolasi, dan karakternya divalidasi. BigQuery tidak punya *unique constraint*, jadi keunikan dijaga oleh `MERGE` pada key di atas; setelah tiap submit aplikasi memeriksa duplikat dan memberi warning bila ada.

### 3. Google Sheets: akses service account

Share ke `client_email` service account:

- spreadsheet **master** `YOUR_MASTER_SHEET_ID` → **Viewer**
- spreadsheet **output** `YOUR_OUTPUT_SHEET_ID` → **Editor**

**Master yang berubah/rusak.** Setiap berhasil dibaca, master disimpan sebagai cadangan (memori + `.cache/master_snapshot.json`, di-gitignore). Bila pembacaan berikutnya gagal (header diubah, sheet rusak, API error), aplikasi **tetap memakai data master terakhir yang valid** dan menampilkan peringatan berisi waktu data itu dibaca serta penyebab error. Bila gagal dan belum pernah ada data valid sama sekali, aplikasi berhenti dengan pesan error. Catatan: di Streamlit Cloud file snapshot hilang saat app di-reboot/redeploy; selama proses hidup, cadangan di memori tetap dipakai.

Master dibaca dari worksheet bernama `store_master_worksheet` / `sku_master_worksheet` di secrets. Bila nama itu tidak ada atau headernya tidak cocok, aplikasi memindai semua worksheet dan memakai yang memuat header exact:

- Store (wajib): `Cust. ID, Store Name, Region, Group, BA/ Non BA`. Kolom lain (mis. anggaran, `Budget Q4`) tidak dipakai dan boleh berubah.
- SKU: `SKU Code, SKU Name, STP` (STP tidak dipakai)

Worksheet `Store Sales` di spreadsheet output dibuat otomatis bila belum ada. Bila sudah ada tetapi header tidak persis sama dengan 17 kolom ini, aplikasi **tidak menulis** dan menampilkan error:
`transaction_id, tanggal_penjualan, store_id, store_name, region, store_group, sku_code, sku_name, sell_out_qty, stock_qty, supervisor_name, evidence_file_id, evidence_file_url, evidence_file_name, evidence_uploaded_at, created_at, updated_at`.

### 4. Cloud Storage: bucket evidence

Bucket `YOUR_BUCKET` **sudah dibuat** (2026-10-23): lokasi `asia-southeast2` (Jakarta), uniform bucket-level access, public access prevention *enforced*, soft delete 7 hari. Service account diberi **Storage Object Creator** (membuat objek) dan **Storage Object Viewer** (membaca, untuk galeri admin); tidak bisa menghapus/menimpa. Untuk membuat ulang di environment lain:

```powershell
gcloud storage buckets create gs://NAMA_BUCKET --project=PROJECT --location=asia-southeast2 --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets add-iam-policy-binding gs://NAMA_BUCKET --member="serviceAccount:EMAIL_SERVICE_ACCOUNT" --role=roles/storage.objectCreator
```

**Melihat foto.** Galeri di tab Admin memakai URL bertanda tangan sehingga tidak butuh akses bucket. Link permanen yang
tersimpan di BigQuery/sheet (`https://storage.cloud.google.com/<bucket>/<objek>`) hanya terbuka bagi akun Google yang punya
*Storage Object Viewer* pada bucket. Beri akses hanya bila ada yang perlu membuka link itu langsung:

```powershell
gcloud storage buckets add-iam-policy-binding gs://YOUR_BUCKET --member="user:EMAIL" --role=roles/storage.objectViewer
```

### 5. Secrets

Salin `.streamlit/secrets.toml.example` menjadi `.streamlit/secrets.toml` dan isi nilainya. Bagian yang dibaca:

- `[gcp_service_account]` atau `[connections.bigquery]`: service account. Bila keduanya tidak ada, dipakai Application Default Credentials (`gcloud auth application-default login`).
- `[app]`: ID sheet, tabel BigQuery, **`evidence_bucket`** (wajib), `evidence_root_folder`, nama worksheet master.
- `[admin]`: `username` dan `password` untuk tab Admin (kosong/tidak ada = tab admin nonaktif).

`.streamlit/secrets.toml` ada di `.gitignore`. Jangan commit credential asli.

## Menjalankan lokal

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
streamlit run app.py
```

Tes (tanpa jaringan, semua layanan Google diganti objek palsu):

```powershell
python -m pytest -q
```

## Deploy ke Streamlit Community Cloud

Langkah lengkap ada di bagian bawah jawaban publish; ringkasnya: push ke GitHub **private** → New app di share.streamlit.io
(`app.py`, Python 3.13) → tempel secrets (`[gcp_service_account]`, `[app]`, `[admin]`) → Deploy → uji submit + tab Admin.
Setiap perubahan secrets perlu *Reboot app*. File `.cache/master_snapshot.json` hilang saat app di-reboot.

## Struktur kode

| File | Isi |
|---|---|
| `app.py` | UI Streamlit |
| `validation.py` | Aturan murni: Non BA, SKU, Sell Out/Stock, duplikat, evidence, key upsert |
| `service.py` | Orkestrasi submit dan penanganan kegagalan |
| `bigquery_store.py` | SELECT / MERGE / cek duplikat (parameterized) |
| `sheets_master.py` | Baca master Store & SKU |
| `sheets_output.py` | Upsert mirror ke worksheet `Store Sales` |
| `gcs_evidence.py` | Upload foto ke GCS + URL baca bertanda tangan (tanpa fungsi hapus) |
| `admin_view.py`, `admin_auth.py`, `completeness.py` | Tab Admin: UI, login, hitung kelengkapan |
| `mobile.py` | CSS/JS untuk tampilan HP |
| `image_processor.py` | Validasi JPEG dan kompresi ≤ 2 MB |
| `master_cache.py` | Cadangan master: data valid terakhir dipakai bila spreadsheet error |
| `timeutil.py` | Konversi dan format WIB |
| `clients.py`, `config.py` | Kredensial dan konfigurasi dari secrets |
| `tests/` | pytest (139 tes) |
