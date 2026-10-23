# BPOM Stock Opname Distributor

Distributor meng-upload template Excel (PRODUCT ID + BPOM NUMBER + QTY). Master distributor dibaca dari
BigQuery (`gt_schema.master_distributor`), hasil disimpan ke **satu** Google Spreadsheet utama (satu worksheet).

## Panduan pengguna (urutan di layar)

1. **Pilih distributor** - Distributor Company, lalu Distributor Name (dari master, **hanya yang status Active**, tidak bisa diketik bebas).
2. **Periode stock opname, tanggal stock opname, nama PIC** - periode disimpan sebagai `YYYY-MM` (mis. `2026-01`).
3. **Pilih mode** - *Input* untuk pengiriman pertama; *Revisi* untuk mengoreksi data yang sudah terkirim.
4. **(Revisi) Periksa data lama** - tampil otomatis dari Google Sheets.
5. **Download template & isi** - kolom A (nama distributor) sudah terisi otomatis; Anda hanya mengisi QTY. Jangan ubah header/urutan kolom.
6. **Upload .xlsx** - diperiksa otomatis; satu baris salah = seluruh upload dibatalkan (tidak ada data masuk).
7. **Submit** - klik sekali, tunggu pesan hasil.

Aturan file: header persis sama dengan template (case-sensitive); PRODUCT ID/NAME, BPOM NUMBER, QTY wajib;
QTY bilangan bulat (boleh negatif/0); kolom A harus sama dengan nama distributor yang dipilih; tidak boleh ada
kombinasi PRODUCT ID + BPOM NUMBER ganda dalam satu file.

Perilaku:
- **Input**: setelah file lolos validasi, spreadsheet dicek satu kali. Bila ada baris dengan PRODUCT ID + BPOM NUMBER yang
  sama untuk distributor + periode itu, **seluruh input ditolak** dengan pesan duplikasi dan diarahkan ke mode Revisi.
  Bila tidak ada, semua baris di-append dalam satu request.
- **Revisi**: key = Distributor Name + Periode + PRODUCT ID + BPOM NUMBER. Ada -> update (created_* dipertahankan,
  updated_* diperbarui); belum ada -> insert. Baris lama yang tidak ada di file **tidak dihapus**. Semua perubahan
  dikirim dalam satu request `batchUpdate` sehingga gagal = tidak ada yang berubah.
- Nama distributor = nilai kolom `distributor` di master, apa adanya. Template yang di-download adalah salinan template
  resmi dengan kolom A terisi nama itu (file asli tidak diubah). Perbandingan nama saat upload tidak peduli huruf besar/kecil
  dan spasi di sekitar tanda "-".

## Setup

1. Salin `.streamlit/secrets.toml.example` ke `.streamlit/secrets.toml`, isi `[connections.bigquery]`
   (service account; sama dengan aplikasi lain) dan `[app]`. File ini di-gitignore - jangan di-commit.
2. Di GCP project service account: aktifkan **Google Sheets API** dan **Google Drive API**.
3. Service account butuh akses baca ke `gt_schema.master_distributor` + role BigQuery Job User.
   (Sheets API dan Drive API sudah aktif di project `skintific-data-warehouse`.)
4. Spreadsheet: bila belum ada, dibuat otomatis oleh service account (pemiliknya service account, **tidak terlihat
   oleh manusia**), jadi isi `share_with = ["email@..."]` di `[app]`, atau buat spreadsheet sendiri, share sebagai
   Editor ke `client_email`, lalu isi `google_sheet_id`. Pakai `google_sheet_id` untuk produksi agar tidak
   bergantung pada pencarian berdasarkan judul.

## Menjalankan

```
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python -m streamlit run app.py
```

Pakai virtual environment (`.venv`) agar tidak bentrok dengan paket Python global.

Tes validasi: `.\.venv\Scripts\python -m pytest tests` (pytest: `pip install pytest`)

## File

| File | Isi |
|---|---|
| `app.py` | UI Streamlit, koneksi BigQuery/Sheets, alur submit |
| `validation.py` | Validasi form + parsing/validasi Excel (tanpa dependensi Streamlit) |
| `template_builder.py` | Membuat template dengan kolom A terisi per distributor |
| `sheets_store.py` | Skema 13 kolom, buat spreadsheet/worksheet/header, insert & upsert atomik |
| `template/Contoh Excel Response.xlsx` | Salinan byte-identik template asli |
| `tests/test_validation.py` | Tes aturan validasi |
