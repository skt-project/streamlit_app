# BPOM Stock Opname Distributor

Distributor memilih **Distributor Company** (daftar dari BD), men-download template yang berisi seluruh Distributor Name
milik company itu beserta daftar SKU + nomor BPOM resmi, mengisi **QTY**, lalu meng-upload-nya. Hasil disimpan ke **satu** Google Spreadsheet utama
(satu worksheet). Tidak ada mode revisi.

## Alur di layar

1. **Pilih Distributor Company** (dari `reference/Daftar Distributor.xlsx`). Tidak ada dropdown Distributor Name: semua Distributor Name milik company itu otomatis ada di template (satu blok baris per distributor).
2. **Tanggal Stock Opname + Nama PIC.** Kolom *Periode* di sheet diisi otomatis dari bulan-tahun tanggal itu (`YYYY-MM`).
3. **Download template** (tombol merah; sheet kedua "Cara Pengisian" berisi panduan) dan isi hanya kolom QTY. BPOM tambahan: tambah baris baru dengan SKU yang sudah ada dan Distributor Name yang sesuai.
4. **Upload .xlsx** - diperiksa otomatis; satu baris salah = seluruh upload dibatalkan (tidak ada data masuk).
5. **Submit.**

## Aturan validasi

- Header persis sama dengan template (case-sensitive, urutan sama); tidak boleh ada kolom tambahan/hilang.
- Kolom A tiap baris harus salah satu Distributor Name milik company yang dipilih; PRODUCT ID, PRODUCT NAME, BPOM NUMBER, QTY wajib.
- QTY bilangan bulat (boleh negatif/0).
- PRODUCT ID harus SKU dari `reference/Data SKU BPOM.xlsx`. Nomor BPOM di luar daftar diterima sebagai **BPOM tambahan**.
  PRODUCT NAME untuk pasangan SKU+BPOM resmi harus sama dengan daftar.
- Tidak ada kombinasi Distributor Name + PRODUCT ID + BPOM NUMBER ganda dalam satu file.
- Kombinasi resmi (per distributor) yang tidak ada di file hanya diberi **peringatan** (tidak memblokir).
- Setelah file valid, sheet dicek satu kali: bila periode yang sama sudah pernah menerima kombinasi
  Distributor Name + PRODUCT ID + BPOM NUMBER yang sama, **seluruh input ditolak** (tidak ada revisi; hubungi BD).
- Semua baris ditulis dalam satu request `batchUpdate` (atomik).

## Data referensi (dari BD)

| File | Isi |
|---|---|
| `reference/Daftar Distributor.xlsx` | `Distributor Company`, `Distributor Name` - sumber dropdown |
| `reference/Data SKU BPOM.xlsx` | `SKU`, `BPOM Product Name`, `BPOM Number`, `Expired Date` - isi template & validasi |
| `template/Contoh Excel Response.xlsx` | Template resmi (header/struktur dipakai apa adanya) |

Memperbarui daftar: ganti file di `reference/` (header harus sama) lalu push. Baris kosong diabaikan.

## Setup

1. Salin `.streamlit/secrets.toml.example` ke `.streamlit/secrets.toml`; isi `[connections.bigquery]` (service account) dan
   `[app]` dengan `google_sheet_id`. File ini di-gitignore - jangan di-commit. Di Streamlit Cloud, isi lewat Settings -> Secrets.
2. Share spreadsheet sebagai **Editor** ke `client_email` service account. Sheets API dan Drive API harus aktif.
3. Service account tidak punya kuota Drive: spreadsheet harus dibuat manual dan ID-nya diisi di `google_sheet_id`.

Skema worksheet (13 kolom): Distributor Company, Distributor Name, Periode Audit, Tanggal Audit, Nama PIC Distributor,
PRODUCT ID, PRODUCT NAME, BPOM NUMBER, QTY, created_date, created_by, updated_date, updated_by. Header dibuat otomatis
bila tab kosong, dan tidak pernah ditimpa bila sudah ada tetapi berbeda.

## Menjalankan

```
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python -m streamlit run app.py
```

Tes: `.\.venv\Scripts\python -m pip install pytest` lalu `.\.venv\Scripts\python -m pytest tests`

## File

`app.py` (UI) · `validation.py` (validasi form + Excel) · `reference_data.py` (baca file BD) ·
`template_builder.py` (template per distributor) · `sheets_store.py` (Google Sheets) · `tests/`
