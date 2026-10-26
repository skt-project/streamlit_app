# Uji beban (performance test)

Menjalankan N pengguna (browser Chromium sungguhan, layar 375 x 667) ke server Streamlit **asli** (`app.main`), dengan Google Sheets
**tiruan** berkuota asli (60 baca + 60 tulis per menit) dan latensi. Tidak ada data uji yang menyentuh spreadsheet atau BigQuery asli.

Alur tiap pengguna: buka halaman -> Region -> Toko -> tanggal + PIC -> (30%) tambah SKU lewat form -> download template -> isi QTY ->
upload -> rincian final -> centang konfirmasi -> Submit -> "Berhasil menyimpan". Setiap pengguna memilih toko yang berbeda.

## Menjalankan

Prasyarat: Python dengan `playwright` (+ `playwright install chromium`) dan `openpyxl`; server memakai `.venv` proyek.

```
python perf/run_load_test.py --users 100 --ramp 2 --server-cores 2
```

| Opsi | Arti |
|---|---|
| `--users` | jumlah pengguna (default 100) |
| `--ramp` | pengguna baru per detik (default 5; **2 = 100 orang dalam 50 detik**, 5 = lonjakan 20 detik) |
| `--server-cores N` | kunci server ke N inti CPU dan browser ke inti sisanya: meniru wadah cloud kecil dan mencegah browser penguji merebut CPU server |
| `--load-only` | pengguna hanya membuka halaman (mengukur biaya memuat halaman) |
| `--pyspy DETIK` | rekam profil GIL server (`pip install py-spy` di venv); ringkas dengan `python perf/summarize_profile.py <profile.raw>` |
| `--out DIR` | folder keluaran (laporan JSON, log server, tangkapan layar saat gagal) |

Laporan memisahkan **beban server** (CPU, memori, Sheets, integritas data) dari **latensi klien**. Pemeriksaan otomatis: tanpa HTTP 429,
hanya `appendCells` yang menulis, tanpa baris ganda, baris tersimpan == harapan, memori puncak < 700 MB, CPU eksekusi script p95 < 300 ms,
spreadsheet dibuka sekali. Keluar dengan kode 1 bila ada pemeriksaan gagal.

## Hasil (laptop 16 inti logis, server dikunci 2 inti, 100 pengguna, 30% menambah SKU)

| Skenario | Selesai | Perjalanan penuh median / p95 | Muat halaman median | CPU server / pengguna | Submit median |
|---|---|---|---|---|---|
| Sebelum `postScriptGC=false`, 2 pengguna/dtk | 100/100 | 152 / 164 dtk | 45 dtk | 1,7 dtk | 15,6 dtk |
| **Sesudah `postScriptGC=false`, 2 pengguna/dtk** | **100/100** | **63 / 78 dtk** | **14 dtk** | **0,94 dtk** | **5,4 dtk** |
| Sebelum, lonjakan 5 pengguna/dtk (100 dalam 20 dtk) | 100/100 | 232 / 244 dtk | 94 dtk | 2,1 dtk | 15,7 dtk |
| **Sesudah, lonjakan 5 pengguna/dtk (100 dalam 20 dtk)** | **100/100** | **98 / 104 dtk** | **38 dtk** | **1,0 dtk** | **9,6 dtk** |

Hasil sisi server pada uji akhir: 0 HTTP 429; 1 pembacaan sheet dan 1 pembukaan spreadsheet untuk seluruh sesi; 100 submit menjadi
27 `batchUpdate` (group commit); 6.630 baris tersimpan tepat (66 atau 67 per toko), tanpa baris ganda; memori server puncak 235 MB.

### Temuan penting

1. **Streamlit memaksa `gc.collect(2)` setelah setiap eksekusi script** (`runner.postScriptGC`, bawaan aktif). Profil py-spy menunjukkan
   **59% waktu GIL server** habis di sana. Mematikannya (di `.streamlit/config.toml`) menurunkan CPU server per pengguna 44%, durasi
   perjalanan penuh 59%, dan durasi eksekusi script median dari 0,44 ke 0,15 dtk.
2. **Hambatan utama adalah satu inti** (event loop + GIL Python), bukan jumlah inti: server memakai sekitar 0,9 inti rata-rata dari 2 inti.
   Kapasitas membuka halaman baru sekitar 2 sesi/detik dengan waktu muat < 1 dtk (diukur dengan `--load-only`); lonjakan 100 sesi dalam
   20 detik membuat antrean (waktu muat median 21 dtk untuk membuka halaman saja).
3. Pada rig ini CPU laptop penguji ikut jenuh (maks 91-100%), jadi latensi klien di tabel adalah batas atas, bukan angka Streamlit Cloud.

### Keterbatasan

- Google Sheets tiruan: latensi dan kuota mengikuti dokumentasi, tapi koneksi nyata, autentikasi, dan latensi jaringan asli belum teruji.
- Server berjalan di Windows lokal; wadah Streamlit Community Cloud (CPU/memori lebih kecil, proxy sendiri) belum diukur.
- Pengguna tiruan tidak berpikir/mengetik: kedatangan 100 orang yang benar-benar serentak jarang terjadi, jadi gunakan `--ramp` sesuai
  perkiraan kedatangan nyata.
