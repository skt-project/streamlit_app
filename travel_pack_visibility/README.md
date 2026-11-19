# Upload Visibility Travel Pack (pengganti Google Form)

> **Repo publik:** ID spreadsheet, bucket, dan service account TIDAK ada di repo. Isi hanya di
> `.streamlit/secrets.toml` lokal (di-ignore git) atau di Secrets Streamlit Cloud. Daftar toko dan nama
> ASM dibaca langsung dari spreadsheet saat app berjalan; tidak ada data toko di repo.

App Streamlit untuk **ASM** (dibuka dari HP) mengirim foto display Travel Pack per toko. Isinya sama dengan
Google Form "Form Upload Visibility Travel Pack BCD Q4'26":

| Form | App |
|---|---|
| Nama ASM * (pilihan) | Dropdown, diambil dari kolom ASM di daftar toko |
| KODE ST TOKO * (ketik) | Pilih dari daftar, **hanya toko ASM itu dengan Ach SKU (MOQ) kolom K ≥ 4** |
| NAMA TOKO (ketik) | Otomatis dari daftar toko |
| NAMA SPV (ketik) | Terisi dari daftar toko bila ada, boleh diubah, opsional |
| FOTO DISPLAY TOKO PAKE GPS CAMERA * (1 file, maks 10 MB) | 1 foto JPG/PNG, maks 10 MB, disimpan apa adanya |

## Alur

1. Pilih **Nama ASM**. Link bisa dibagikan per ASM: `https://<app>.streamlit.app/?asm=Nama%20ASM`.
2. Progres "x dari y toko sudah upload" + daftar toko (expander).
3. Pilih **Toko** (ketik kode/nama). Toko yang belum upload tampil di atas, yang sudah upload bertanda ✅.
4. Nama SPV, lalu **foto** (pratinjau + info EXIF bila ada).
5. **Submit** → foto ke GCS, 1 baris baru di tab hasil. Pilihan toko direset untuk toko berikutnya.

Submit ulang untuk toko yang sama diperbolehkan (seperti Google Form): menambah baris + foto baru, data lama
tidak diubah. App menolak foto yang sama persis untuk toko yang sama dalam satu sesi (cegah tap ganda).

## Aturan kelayakan toko

- Kolom **K** (`ach_sku_column`) dibaca menurut **posisi**; toko tampil bila nilainya **≥ 4** (`min_ach_sku`).
- Angka `4`, `4.0`, `4,0` diterima. Kosong, teks, `#N/A`, dan nilai persen dianggap **tidak** memenuhi.
- Daftar dibaca ulang tiap 5 menit (atau tombol "Muat ulang" di sidebar), jadi toko yang baru mencapai 4
  ikut muncul tanpa deploy ulang. Syarat dicek ulang saat Submit.
- Header dicari otomatis di 15 baris pertama. Kolom dikenali dari nama header: ASM (`ASM` / `Nama ASM`),
  kode toko (`Kode ST Toko` / `Kode Toko` / `Cust ID` / ...), nama toko, SPV, Region. Bila nama header
  berbeda, isi `asm_header`, `store_code_header`, dst. di secrets.
- Sidebar "Info data" menampilkan tab, baris header, kolom yang dipakai, header kolom K, dan jumlah toko
  yang memenuhi. Muncul peringatan bila header kolom K bukan "Ach ...".

## Tab hasil (log upload)

Format sama dengan log upload app SKP JBP/JCP: satu tab log, header tebal abu-abu dan dibekukan, lebar
kolom diatur, dan tiap sel header punya catatan (note) penjelasan. Tab `output_worksheet` (default
`TRAVEL_PACK_UPLOAD_LOG`) dibuat otomatis dengan format itu bila belum ada; header ditulis dari
`HEADER_SPEC` di `responses.py`.

`Timestamp` (WIB) · `Nama ASM` · `KODE ST TOKO` · `NAMA TOKO` · `Region` · `NAMA SPV` · `Ach SKU (MOQ)`
(nilai saat submit) · `Nama File` · `GCS Path` (link foto, bisa diklik) · `Status` (`Uploaded` = foto
pertama toko itu, `Upload ulang` = toko sudah pernah upload) · `Waktu Foto (EXIF)` · `Lokasi Foto (EXIF)` ·
`Submission ID`

- Semua nilai ditulis RAW: isian user tidak pernah dijalankan sebagai formula. Sesudahnya sel `GCS Path`
  baris itu diubah menjadi `=HYPERLINK(...)`; teks yang tampil tetap `gs://...`. Bila langkah ini gagal,
  sel tetap berisi teks `gs://...`.
- Tab hasil dibaca sebelum foto dikirim (untuk `Status` dan cek header). Bila sheet tidak bisa dibaca,
  submit berhenti dan foto tidak masuk bucket.
- App hanya menulis A–M. Kolom tambahan tim di kanan M boleh ada. Bila header A–M diubah, app menolak
  menulis (tidak ada data yang tertimpa).
- EXIF hanya dicatat, tidak dipakai menolak foto: banyak HP/browser (terutama iPhone) membuang lokasi EXIF.
  Bukti lokasi utama tetap cap GPS yang tercetak di foto.

## Foto

- Bucket GCS privat: `<evidence_prefix>/<Nama ASM>/<KODE> - <NAMA TOKO> - <YYYYmmdd_HHMMSS>_<id>.jpg`.
- Setiap submit = objek baru (`if_generation_match=0`). App tidak punya fungsi hapus/timpa; service account
  cukup role **Storage Object Creator** pada bucket.
- Link `GCS Path` di sheet (membuka `https://storage.cloud.google.com/...`) hanya bisa dibuka akun Google
  yang punya role **Storage Object Viewer** pada bucket. Berikan ke tim yang mengecek foto.
- Bila foto sudah masuk bucket tetapi baris sheet gagal ditulis, user diminta submit lagi; foto pertama
  dibiarkan di bucket dan dicatat di log.

## Setup

1. Siapkan spreadsheet hasil: spreadsheet kosong (tab dibuat app saat submit pertama), atau upload file
   template xlsx (header dibuat dari `HEADER_SPEC`) ke Google Drive lalu buka sebagai Google Sheets.
   Share spreadsheet daftar toko ke service account sebagai **Viewer**, spreadsheet hasil sebagai **Editor**.
2. Salin `.streamlit/secrets.toml.example` → `.streamlit/secrets.toml` (lokal) atau tempel di Streamlit Cloud.
3. Streamlit Cloud: repo ini, branch fitur, main file `travel_pack_visibility/app.py`.

Lokal:

```bash
cd travel_pack_visibility
pip install -r requirements-dev.txt
streamlit run app.py --server.address localhost
python -m pytest -q
```

Test memakai Google palsu (tanpa jaringan); semua nama/kode toko di test fiktif.

## Tidak ada login

Siapa pun yang punya link bisa submit atas nama ASM mana pun, sama seperti app Facerinna. Google Form
aslinya mencatat akun Google pengirim; app ini tidak. Bila perlu, tambahkan PIN per ASM.
