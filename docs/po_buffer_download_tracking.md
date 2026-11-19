# PO Suggestion Tool — bulk ZIP, nama file, tracking download, dan admin

Berlaku untuk `po_buffer.py` v2.1.0.

## Ringkasan perilaku

| Aksi | Hasil |
|---|---|
| Tombol **Download PDF** pada satu toko | 1 file `<nama_toko>_<YYYYMMDD>.pdf`, counter toko itu +1 (individual) |
| **Generate ZIP** lalu **Unduh ZIP** | 1 ZIP `PO_Bulk_<jumlah_toko>_<YYYYMMDD>.zip` berisi tepat 1 PDF per toko; counter setiap toko di dalamnya +1 (bulk) |
| Hanya membuat PDF / ZIP, atau rerun Streamlit | Tidak mencatat apa pun |

Tanggal selalu `Asia/Jakarta`. Tidak ada lagi PDF gabungan multi-toko.

## Nama file

- `<nama_toko>_<YYYYMMDD>.pdf`, mis. `Store Jakarta Selatan_20261119.pdf`.
- Karakter `\ / : * ? " < > |` dan karakter kontrol diganti `-` (`PT Maju/Jaya` → `PT Maju-Jaya`).
- Nama yang sama (tanpa membedakan huruf besar/kecil, seperti Windows/macOS) → `store_code` ditambahkan
  ke **semua** file yang bentrok: `Toko Sama_ST002_20261119.pdf`. Hasilnya tidak bergantung urutan baris.
- Nama kosong → `toko`. Nama dipotong 100 karakter.
- Nama di tombol individual dan di dalam ZIP dihitung oleh fungsi yang sama, jadi identik.
- Jumlah pada nama ZIP = jumlah PDF yang benar-benar ada di dalamnya.

## Toko tanpa data valid

Setiap toko hasil filter masuk salah satu: **dibuatkan PDF** atau **dilewati dengan alasan**.
Sebelum menekan Generate, app menampilkan "N toko hasil filter → M file PDF akan dibuat dalam 1 file ZIP"
dan tabel toko yang dilewati. Alasan yang mungkin:

- `Kode toko kosong`
- `Tidak ada baris inventory untuk toko ini`
- `Tidak ada SKU dengan saran qty (buffer_plan kosong)`
- `Gagal membuat PDF: <error>` (toko lain tetap masuk ZIP)

Toko yang dilewati tidak dicatat sebagai download.

## Penyimpanan tracking

Tabel BigQuery `skintific-data-warehouse.gt_schema.po_buffer_download_log` — satu baris **per toko per klik**.
DDL ada di [`sql/po_buffer_download_log.sql`](../sql/po_buffer_download_log.sql) (identik dengan
`po_buffer_tracking.create_table_ddl()`; ada test yang menjaganya).

| Kolom | Tipe | Arti |
|---|---|---|
| `event_id` | STRING | id satu klik download; semua toko dalam satu ZIP berbagi id ini |
| `store_code` | STRING | toko yang diunduh |
| `store_name` | STRING | nama toko saat diunduh |
| `region` | STRING | region saat diunduh |
| `distributor` | STRING | distributor saat diunduh |
| `download_type` | STRING | `individual` atau `bulk` |
| `file_name` | STRING | nama PDF toko tsb di download itu |
| `bulk_file_count` | INT64 | jumlah PDF dalam ZIP (NULL untuk individual) |
| `downloaded_at` | TIMESTAMP | waktu klik, UTC (ditampilkan WIB) |

- Counter toko = `COUNT(*)` per `store_code`; individual vs bulk = pemisahan `download_type`.
- "Toko apa saja dalam satu bulk" = baris yang berbagi `event_id`.
- Tabel dibuat otomatis (`CREATE TABLE IF NOT EXISTS`) saat pertama dipakai. Jika service account tidak punya
  `bigquery.tables.create` di `gt_schema`, jalankan file SQL di atas satu kali; app lalu memakai tabel itu.
- Service account app butuh `bigquery.tables.updateData` (INSERT) dan `bigquery.tables.getData` pada tabel tsb.

### Mengapa tidak mungkin terhitung dua kali

1. Pencatatan hanya ada di callback `on_click` tombol download (`record_download`). Streamlit memanggilnya
   tepat sekali per klik, sebelum skrip dirender ulang. Membuat PDF/ZIP dan rerun tidak memanggilnya.
2. `event_id` dibuat saat tombol dirender. Event dengan id yang sudah tercatat di session diabaikan.
3. Di BigQuery, `INSERT ... WHERE NOT EXISTS (event_id, store_code)` membuat pengulangan event yang sama
   (retry, flush fallback) tidak menambah baris.

Dua klik nyata pada tombol yang sama = dua download (disengaja).

### Fallback

Jika BigQuery tidak bisa dipakai (tidak ada izin membuat/menulis tabel, gangguan), event ditulis ke file
JSON-lines di folder temp server (`PO_BUFFER_TRACKING_FALLBACK_PATH` untuk mengubah lokasi) dan dikirim ke
BigQuery otomatis saat BigQuery pulih (idempotent). **Keterbatasan:** di Streamlit Cloud file ini sementara
dan per-container, jadi event yang masih di sana hilang jika app restart sebelum sempat terkirim. Dashboard
menampilkan peringatan selama data dilayani dari fallback. Setelah gagal, BigQuery tidak dicoba lagi selama
5 menit agar tidak menghambat klik. Kegagalan tracking tidak pernah memblokir download.

## Login admin

Halaman **🔐 Admin Tracking** (navigasi di atas halaman) menampilkan form login; isi dashboard tidak dirender
sebelum login berhasil, dan fungsi dashboard menolak render tanpa sesi admin. Status login ada di
`st.session_state` (hilang jika tab/browser di-refresh). Tombol **Logout** muncul di bar navigasi setelah login.
Setelah 5 kali gagal, sesi dikunci 30 detik. Password tidak pernah ditulis ke log.

Kredensial (urutan prioritas per field): `[po_buffer_admin]` di Streamlit secrets → environment variable →
default.

| | secrets `[po_buffer_admin]` | environment |
|---|---|---|
| username | `username` | `PO_BUFFER_ADMIN_USERNAME` |
| password | `password` | `PO_BUFFER_ADMIN_PASSWORD` |
| hash bcrypt (disarankan) | `password_hash` | `PO_BUFFER_ADMIN_PASSWORD_HASH` |

**Default `admin` / `12345678`** dipakai bila tidak ada konfigurasi (sesuai requirement awal). Repository ini
publik, sehingga default itu bisa dibaca siapa pun — dashboard menampilkan peringatan selama default aktif.
Atur secrets sebelum dipakai di produksi. Hash dibuat dengan:

```
python -c "import bcrypt,getpass; print(bcrypt.hashpw(getpass.getpass().encode(), bcrypt.gensalt()).decode())"
```

## Dashboard

Total download (individual / via bulk per toko / aksi bulk / jumlah toko), tabel per toko (nama, store code,
region, distributor, individual, bulk, total, download terakhir WIB), filter tanggal (default semua tanggal),
toko, region, distributor, tombol Refresh, export CSV (ringkasan per toko dan log event; UTF-8 BOM, sel
berawalan `= + - @` dinetralkan agar tidak dieksekusi sebagai formula), detail anggota tiap aksi bulk, dan
log 200 event terbaru. Export CSV tidak dihitung sebagai download PO.

## Pengujian

```
pip install -r requirements.txt -r requirements-dev.txt
pytest tests/test_po_buffer_downloads.py tests/test_po_buffer_tracking.py \
       tests/test_po_buffer_admin.py tests/test_po_buffer_app.py -q
```

`test_po_buffer_app.py` menjalankan `po_buffer.py` sungguhan lewat Streamlit `AppTest` dengan BigQuery palsu
(`tests/po_buffer_fakes.py`) dan mensimulasikan klik tombol download; test itu dilewati jika `streamlit`
tidak dapat diimpor. Tidak ada test yang memakai kredensial atau menulis ke BigQuery.
