# BPOM Stock Opname Distributor

Distributor memilih **Distributor Company** (daftar dari BD), men-download template yang berisi seluruh Distributor Name
milik company itu beserta daftar SKU + nomor BPOM resmi, mengisi **QTY**, lalu meng-upload-nya. Hasil disimpan ke **satu** Google Spreadsheet utama
(satu worksheet). Tidak ada mode revisi.

## Alur di layar

1. **Pilih Distributor Company** (dari `reference/Daftar Distributor.xlsx`). Tidak ada dropdown Distributor Name: semua Distributor Branch milik company itu otomatis ada di template (satu blok baris per distributor).
2. **Tanggal Stock Opname + Nama PIC.** Kolom *Periode* di sheet diisi otomatis dari bulan-tahun tanggal itu (`YYYY-MM`).
3. **Download template** (tombol merah; sheet kedua "Cara Pengisian" berisi panduan) dan isi hanya kolom QTY. BPOM tambahan: tambah baris baru dengan SKU yang sudah ada dan Distributor Name yang sesuai.
4. **Upload .xlsx** - diperiksa otomatis; satu baris salah = seluruh upload dibatalkan (tidak ada data masuk).
5. **Submit.**

## Aturan validasi

- Header persis sama dengan template (case-sensitive, urutan sama); tidak boleh ada kolom tambahan/hilang.
- Template punya 6 kolom: `Distributor Branch`, `PRODUCT ID`, `PRODUCT NAME`, `ISI (GR / ML)`, `BPOM NUMBER`, `QTY`.
- Kolom A tiap baris harus salah satu Distributor Branch milik company yang dipilih; semua kolom wajib diisi.
- QTY bilangan bulat 0 atau lebih (**tidak boleh negatif**). Template juga memasang validasi Excel pada kolom QTY.
- PRODUCT ID harus SKU yang ada di katalog (Sheet1 template resmi). Nomor BPOM di luar katalog diterima sebagai **BPOM tambahan**.
  PRODUCT NAME dan ISI untuk pasangan SKU+BPOM resmi harus sama dengan katalog.
- Tidak ada kombinasi Distributor Branch + PRODUCT ID + BPOM NUMBER ganda dalam satu file.
- Kombinasi resmi (per distributor) yang tidak ada di file hanya diberi **peringatan** (tidak memblokir).
- Setelah file valid, sheet dicek satu kali: bila periode yang sama sudah pernah menerima kombinasi
  Distributor Branch + PRODUCT ID + BPOM NUMBER yang sama, **seluruh input ditolak** (tidak ada revisi; hubungi BD).
- Semua baris ditulis dalam satu request `batchUpdate` (atomik).

## Data referensi (dari BD)

| File | Isi |
|---|---|
| `reference/Daftar Distributor.xlsx` | `Distributor Company`, `Distributor Name` - sumber dropdown |
| `template/Contoh Excel Response.xlsx` | Template resmi **sekaligus katalog SKU**: `Sheet1` (PRODUCT ID, PRODUCT NAME, ISI, BPOM NUMBER) dipakai untuk mengisi template dan validasi. Sheet lain di file ini (daftar distributor, SKU tersembunyi) **tidak** ikut ke distributor |

Memperbarui katalog SKU: ganti `Sheet1` pada file template (header harus sama persis). Memperbarui distributor: ganti file di `reference/`. Lalu push.

## Setup

1. Salin `.streamlit/secrets.toml.example` ke `.streamlit/secrets.toml`; isi `[connections.bigquery]` (service account) dan
   `[app]` dengan `google_sheet_id`. File ini di-gitignore - jangan di-commit. Di Streamlit Cloud, isi lewat Settings -> Secrets.
2. Share spreadsheet sebagai **Editor** ke `client_email` service account. Sheets API dan Drive API harus aktif.
3. Service account tidak punya kuota Drive: spreadsheet harus dibuat manual dan ID-nya diisi di `google_sheet_id`.

Skema worksheet (14 kolom): Distributor Company, Distributor Name, Periode Audit, Tanggal Audit, Nama PIC Distributor,
PRODUCT ID, PRODUCT NAME, BPOM NUMBER, QTY, created_date, created_by, updated_date, updated_by, ISI (GR / ML).
Header dibuat otomatis bila tab kosong. Tab lama 13 kolom otomatis ditambah kolom `ISI (GR / ML)` di ujung (data tidak
disentuh); header lain yang berbeda tidak pernah ditimpa.

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
