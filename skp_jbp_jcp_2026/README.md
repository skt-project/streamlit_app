# SKP JBP / JCP Periode 2026

Aplikasi Streamlit untuk mengumpulkan dokumen **SKP JBP / JCP 2026** per toko, brand, program, dan kuartal.
Dokumen PDF disimpan di **Google Cloud Storage**, metadata dicatat di sheet **`SKP_UPLOAD_LOG`**, dan admin
mendapat dashboard, compliance, gallery per toko, serta download ZIP.

- Upload: tanpa login. Pilih region → pilih toko (daftar hanya berisi toko di region itu; bisa dipersempit
  dengan Kode **atau** Nama) → pilih brand/program/kuartal → upload PDF (maks 1 MB) → rangkuman → konfirmasi.
  Toko yang region-nya kosong di spreadsheet ada di pilihan "Region belum diisi".
- Admin: login sederhana (username/password dari Secrets).
- Responsif: desktop (lebar maks ±1240 px) dan mobile (satu kolom, tombol besar, tabel bisa digeser).

---

## 1. Struktur project

```
skp_jbp_jcp_2026/          # subfolder di repo streamlit_app
├── app.py                     # entry point: routing, landing, halaman sukses
├── requirements.txt
├── pytest.ini
├── .gitignore
├── .streamlit/
│   ├── config.toml            # tema light, primary hitam, maxUploadSize = 1
│   └── secrets.toml.example   # contoh format Secrets (secrets.toml asli TIDAK di-commit)
├── assets/skintific_logo.png  # wordmark SKINTIFIC di landing page
├── src/
│   ├── config.py              # baca Secrets, parsing service account, mode live/mock
│   ├── sheets.py              # Google Sheets: sheet sumber + SKP_UPLOAD_LOG (upsert)
│   ├── storage.py             # GCS (live) / folder lokal (mock) + ZIP di memori
│   ├── validation.py          # aturan PDF, ukuran, nama file, object path, kemiripan nomor SKP
│   ├── service.py             # alur submit (upload + log) dan agregasi admin
│   ├── runtime.py             # wiring backend + cache Streamlit + pesan error ramah pengguna
│   ├── page_upload.py         # halaman upload + dialog konfirmasi
│   ├── page_admin.py          # halaman admin
│   ├── ui.py                  # CSS dan komponen tampilan
│   └── sample_data.py         # data contoh fiktif untuk mode mock
└── tests/
    ├── test_validation.py     # aturan validasi, pencarian, auto-detect, overwrite metadata
    ├── test_service.py        # alur submit, overwrite file, ZIP, konfigurasi (fake lokal)
    └── test_app_smoke.py      # smoke test Streamlit AppTest (mode mock)
```

---

## 2. Cara kerja data

### Sheet sumber `Template`
Header: `Jenis Program, Brand, Kode Toko/Group, Nama Toko/Group, Region, SKP Q1 2026 … SKP Q4 2026`.
Kolom dicari berdasarkan **nama header** (urutan boleh berubah, kolom tambahan diabaikan).

Pembersihan otomatis saat dibaca:
- Baris tanpa *Kode Toko/Group* (mis. baris formula `"JBP"()`) dilewati; baris duplikat persis dihapus.
- `#N/A`, `#REF!` dsb. dianggap kosong; spasi ganda / NBSP dirapikan.
- Ejaan region disatukan tanpa membedakan huruf besar/kecil (`EAST JAVA` → `East Java`), supaya satu region = satu folder.
- Satu kode bisa punya beberapa baris (group). Nama kanonik = nama yang paling sering muncul untuk kode itu;
  variasi nama lain tetap bisa dicari.

**Auto-detect:** brand dari kolom *Brand*, program dari *Jenis Program*, kuartal dipilih otomatis jika
kolom *SKP Qx 2026* berisi nomor. User tetap boleh menambah pilihan manual.

### Nama file & lokasi di GCS
```
Nama file : SKP - {NAMA TOKO} - {NOMOR SKP}.pdf      ("/" diganti "_", karakter ilegal dihapus)
Object    : 2026 Q1/jbp/{region}/{nama file}
            2026 Q4/jcp/{region}/{nama file}
```
Contoh: `2026 Q3/jbp/East Java/SKP - CONTOH BEAUTY STORE - 9280_FCR_SKP_P1B_GT_X_2026.pdf`

- Region kosong → folder `TANPA REGION`.
- Pilihan manual tanpa nomor SKP → `SKP - {NAMA TOKO} - TANPA-NOMOR_{BRAND}_{PROGRAM}_2026_Qx.pdf`.
- Nama file asli user **tidak** dipakai sebagai nama tujuan. Jika tidak mirip dengan nomor SKP, muncul
  **peringatan saja** (tidak ditolak) dan kolom *Status* di log diberi catatan `(cek nama file)`.

### Upload ulang = menimpa
Kunci satu dokumen: **Kode Toko/Group + Brand + Jenis Program + Kuartal**.
- Object path deterministik → upload ulang menimpa file yang sama di GCS.
- Baris log dengan kunci yang sama di-**update**, tidak ditambah (Status `Replaced`).
- Jika nama tujuan berubah (mis. nomor SKP di spreadsheet dikoreksi), file lama kombinasi itu dihapus
  setelah file baru tersimpan, supaya satu kombinasi tetap satu file.

### Sheet `SKP_UPLOAD_LOG`
Dibuat otomatis **pada submit pertama** (membuka aplikasi tidak menulis apa pun). Kolom:
`Timestamp, PIC Submit, Kode Toko/Group, Nama Toko/Group, Region, Brand, Jenis Program, Kuartal, Nomor SKP,
Nama File, GCS Path, Status, Uploader`.
- `Timestamp` dalam WIB (`YYYY-MM-DD HH:MM:SS`).
- `Status`: `Uploaded` (pertama kali) atau `Replaced` (upload ulang), opsional `(cek nama file)`.
- `Uploader` = nama PIC Submit (upload tanpa login, jadi tidak ada identitas lain).
- Data ditulis dengan mode **RAW**: kode seperti `0012` tetap teks dan isian seperti `=HYPERLINK(...)`
  tidak dijalankan sebagai formula.
- Kolom tambahan yang dibuat admin secara manual di sebelah kanan dipertahankan saat update.

---

## 3. Setup Google Cloud

Service account: `NAMA_SERVICE_ACCOUNT@PROJECT_ID.iam.gserviceaccount.com`

Nama bucket yang disarankan: `nama-bucket-skp` (nama bucket GCS unik secara global; boleh diganti,
asal huruf kecil, angka, dan tanda `-`). Semua perintah di bawah **satu baris** dan bisa langsung dijalankan
di PowerShell maupun Bash.

### a. Cek login gcloud
```powershell
gcloud config list
```
Pastikan `account` adalah akun yang boleh membuat bucket di project, dan `project = PROJECT_ID`.
Jika belum: `gcloud auth login` lalu `gcloud config set project PROJECT_ID`.

### b. Aktifkan API (sekali per project; aman dijalankan ulang)
```powershell
gcloud services enable sheets.googleapis.com storage.googleapis.com --project=PROJECT_ID
```

### c. Buat bucket khusus SKP
Aplikasi **tidak** membuat bucket sendiri. Buat sekali (private, region Jakarta):
```powershell
gcloud storage buckets create gs://nama-bucket-skp --project=PROJECT_ID --location=asia-southeast2 --default-storage-class=STANDARD --uniform-bucket-level-access --public-access-prevention
```

### d. Beri izin service account pada bucket
```powershell
gcloud storage buckets add-iam-policy-binding gs://nama-bucket-skp --member="serviceAccount:NAMA_SERVICE_ACCOUNT@PROJECT_ID.iam.gserviceaccount.com" --role="roles/storage.objectAdmin"
```

### e. Verifikasi
```powershell
gcloud storage buckets describe gs://nama-bucket-skp
gcloud storage buckets get-iam-policy gs://nama-bucket-skp
```
Yang harus terlihat: lokasi `ASIA-SOUTHEAST2`, public access prevention `enforced`, uniform bucket-level
access `true`, dan email service account dengan role `roles/storage.objectAdmin`.

Lewat Console (tanpa CLI): **Cloud Storage → Buckets → Create** → nama bucket → *Region* `asia-southeast2
(Jakarta)` → *Standard* → centang **Enforce public access prevention** → *Access control* **Uniform** →
**Create**. Lalu buka bucket → tab **Permissions** → **Grant access** → isi email service account → role
**Storage Object Admin** → **Save**.

Catatan: soft delete GCS (default 7 hari) tetap aktif, jadi file yang tertimpa atau terhapus masih bisa
dipulihkan selama 7 hari. Bucket **jangan** dibuat publik; admin membuka PDF lewat signed URL dari aplikasi.
`Storage Object Admin` dibutuhkan karena upload ulang **menimpa** object (butuh izin hapus), admin
mengunduh ZIP (baca), dan mengecek isi bucket (list). Role ini hanya berlaku untuk bucket tersebut.
Alternatif minimum yang setara: `roles/storage.objectUser`.

### f. Bagikan spreadsheet ke service account
Buka spreadsheet → **Share** → tambahkan email service account sebagai **Editor**
(perlu Editor karena aplikasi membuat dan menulis sheet `SKP_UPLOAD_LOG`).

---

## 4. Streamlit Secrets

Tempel di **Streamlit Cloud → App → Settings → Secrets**, atau simpan lokal di `.streamlit/secrets.toml`
(sudah di-`.gitignore`; lihat `.streamlit/secrets.toml.example`).

```toml
GCP_PROJECT_ID = "PROJECT_ID"
SOURCE_SPREADSHEET_ID = "ID_SPREADSHEET"
SOURCE_SHEET_NAME = "Template"
LOG_SHEET_NAME = "SKP_UPLOAD_LOG"
GCS_BUCKET = "nama-bucket-skp"
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "GANTI_DENGAN_PASSWORD_KUAT"

GCP_SERVICE_ACCOUNT_JSON = '''
{
  "type": "service_account"
}
'''
```

Catatan:
- Isi `GCP_SERVICE_ACCOUNT_JSON` dengan **seluruh isi file JSON** service account, di antara `'''` dan `'''`.
  Bisa juga memakai tabel TOML `[gcp_service_account]` (format yang sama dengan app Streamlit lain di repo ini,
  jadi bloknya bisa disalin dari Secrets app lain yang memakai service account yang sama). Letakkan tabel
  **di bagian paling bawah**, setelah semua key biasa.
- **Jangan** memasukkan credential asli ke repository. Tidak ada credential di source code; tanpa
  `ADMIN_USERNAME`/`ADMIN_PASSWORD` di Secrets, login admin dinonaktifkan.
- Gunakan password admin yang **kuat**: aplikasi bisa diakses publik dan admin dapat mengunduh semua dokumen.
- Opsional: `APP_MODE = "auto" | "live" | "mock"` (default `auto`), `LOCAL_DATA_DIR` (folder mode mock).

### Mode aplikasi
| Mode | Kapan | Sumber toko | File & log |
|---|---|---|---|
| `live` | `auto` + service account ada | Google Sheets | GCS + `SKP_UPLOAD_LOG` |
| `mock` | `auto` tanpa service account, atau `APP_MODE="mock"` | Google Sheets jika SA ada, selain itu data contoh fiktif | folder `.local_data/` |

Di mode live, jika `GCS_BUCKET` belum diisi, **upload dinonaktifkan** dengan pesan jelas (tidak diam-diam
menyimpan ke disk lokal yang hilang saat Streamlit Cloud restart). Pencarian toko tetap berjalan.
Di mode mock, banner kuning selalu tampil.

---

## 5. Deploy ke Streamlit Community Cloud

1. Kode ada di repo `skt-project/streamlit_app` (publik), subfolder `skp_jbp_jcp_2026/`. Jangan commit `secrets.toml` atau file `.json`.
2. **Create app** → repo `skt-project/streamlit_app`, branch `feat/skp-jbp-jcp-2026`, *Main file path*: `skp_jbp_jcp_2026/app.py`.
3. *Advanced settings* → Python 3.12 → tempel Secrets (bagian 4).
4. Deploy. Buka aplikasi → **Admin → Status sistem → Cek koneksi** untuk memastikan sheet dan bucket terbaca.

---

## 6. Jalankan lokal

```bash
cd skp_jbp_jcp_2026
python -m venv .venv
.venv\Scripts\activate            # Windows   (macOS/Linux: source .venv/bin/activate)
pip install -r requirements.txt
streamlit run app.py
```
Tanpa `secrets.toml` aplikasi otomatis berjalan di **mode mock** dengan data contoh (login admin perlu
`ADMIN_USERNAME`/`ADMIN_PASSWORD` di Secrets atau environment variable).

### Test
```bash
python -m pytest
```
Semua test memakai fake lokal; tidak ada panggilan ke Google.

---

## 7. Batasan & catatan operasional

- **Popup loading**: muncul (1) sekali per sesi saat aplikasi dibuka — sambil menyiapkan koneksi Google dan daftar
  toko, (2) setelah file PDF baru masuk — sambil memeriksa file, (3) setelah *Konfirmasi & Upload* — sambil
  mengupload ke GCS dan menulis spreadsheet. Angka 1→98% adalah animasi cepat di browser (±1 detik); popup
  **berhenti di 98%** sampai proses aslinya selesai, baru tampil 100% lalu tertutup. Proses upload file dari
  perangkat ke server terjadi di browser sebelum app berjalan, jadi progresnya tampil di kotak upload bawaan Streamlit.
- **Ukuran file**: dibatasi 1 MB oleh `config.toml`, oleh widget, dan divalidasi ulang di server
  (cek ekstensi `.pdf` + signature `%PDF-`). File > 1 MB ditolak langsung oleh Streamlit dengan pesan
  bawaan *"File must be 1.0MB or smaller"*; di bawah setiap area upload ada penjelasan dalam Bahasa Indonesia.
- **Kuota Google Sheets**: daftar toko di-cache 5 menit, log 30 detik. Perubahan di sheet sumber terlihat
  setelah ≤ 5 menit, atau segera lewat *Admin → Status sistem → Muat ulang data*.
- **ZIP**: dibuat di memori (`io.BytesIO`), tidak disimpan ke GCS. Maksimal 500 file / 300 MB per ZIP;
  persempit filter jika lebih.
- **Gallery**: link *Buka PDF* adalah signed URL yang berlaku 15 menit (hanya mode live).
- **Upload tanpa login**: siapa pun yang punya URL dapat mengupload. Bagikan URL hanya ke tim terkait.
- **Kombinasi tanpa file** di form tidak diupload (ditandai *dilewati* di rangkuman).

## 8. Troubleshooting

| Pesan | Penyebab | Solusi |
|---|---|---|
| Spreadsheet belum dibagikan ke … sebagai Editor | Sheet belum di-share | Share ke email service account (Editor) |
| Sheet 'Template' tidak ditemukan | Nama tab berubah | Sesuaikan `SOURCE_SHEET_NAME` |
| Bucket GCS '…' tidak ditemukan | Nama bucket salah / belum dibuat | Cek `GCS_BUCKET`, buat bucket (3c) |
| Service account tidak punya izin menulis ke bucket | IAM belum diberikan | Jalankan 3d |
| Header sheet log berubah, kolom hilang | Header `SKP_UPLOAD_LOG` diubah manual | Kembalikan nama kolom sesuai bagian 2 |
| GCP_SERVICE_ACCOUNT_JSON bukan JSON yang valid | Format Secrets salah | Tempel ulang isi JSON di antara `'''` |
| Batas kuota Google Sheets tercapai | Terlalu banyak request | Tunggu 1 menit, muat ulang |
