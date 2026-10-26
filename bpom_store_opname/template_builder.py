"""Membuat template untuk satu toko dari template resmi + katalog SKU/BPOM prinsipal.

File template asli tidak pernah diubah: file dibaca ke memori, baris data diganti sesuai daftar
SKU terkini (untuk satu toko) dan kolom A diisi nama toko (By Group), lalu disimpan sebagai bytes baru. Header, baris judul,
lebar kolom, dan gaya sel tetap seperti template.
"""
from __future__ import annotations

from copy import copy
from io import BytesIO
from typing import Sequence

import openpyxl
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.worksheet.datavalidation import DataValidation

from validation import EXPECTED_HEADERS, HEADER_SEARCH_ROWS, INSTRUCTION_SHEET, Sku, _looks_like_header

# (jenis, teks): "title" | "h" = judul bagian | "p" = paragraf | "b" = butir | "ex" = contoh | "warn" = peringatan
_GUIDE = [
    ("h", "Langkah pengisian"),
    ("b", "1. Buka sheet pertama (data). Template ini khusus untuk satu toko (lihat di atas). Kolom Nama Toko (By Group) sudah terisi; jangan diubah."),
    ("b", "2. Kolom PRODUCT ID, PRODUCT NAME, ISI (GR / ML), dan BPOM NUMBER sudah terisi dari daftar prinsipal; jangan dikosongkan atau diubah."),
    ("b", "3. Isi kolom QTY di setiap baris dengan jumlah stok fisik hasil stock opname untuk SKU + nomor BPOM pada baris tersebut."),
    ("b", "4. Simpan sebagai .xlsx lalu upload di form. File diperiksa otomatis; jika ada error, perbaiki lalu upload ulang."),
    ("h", "Aturan kolom QTY"),
    ("b", "- Wajib diisi di setiap baris. Jika tidak ada stok, isi 0 (jangan dikosongkan)."),
    ("b", "- Harus bilangan bulat 0 atau lebih. Tidak boleh negatif, desimal (mis. 2,5), atau huruf."),
    ("h", "Pencatatan per SKU dan nomor BPOM"),
    ("b", "- SKU yang sama dengan nomor BPOM berbeda harus dicatat di baris terpisah dan tidak boleh digabung."),
    ("b", "- Satu kombinasi PRODUCT ID + BPOM NUMBER hanya boleh muncul satu kali dalam satu file."),
    ("h", "Jika ada nomor BPOM tambahan (tidak ada di template)"),
    ("b", "- Cara termudah: pakai form \"Tambah SKU / nomor BPOM langsung\" di aplikasi (pilih SKU dari master product), tanpa mengetik di Excel."),
    ("b", "- Atau tambahkan baris baru di bagian paling bawah data pada sheet pertama."),
    ("b", "- Kolom Nama Toko (By Group): isi sama persis seperti baris lainnya (salin dari baris di atasnya)."),
    ("b", "- PRODUCT ID: harus SKU yang sudah ada di template. SKU yang tidak ada di template akan ditolak."),
    ("b", "- PRODUCT NAME dan ISI (GR / ML): salin dari baris SKU yang sama di template. BPOM NUMBER (nomor baru) dan QTY: isi lengkap."),
    ("ex", "Contoh baris tambahan:  [Nama Toko (By Group)] | SKINTIFIC-05 | [PRODUCT NAME SKU 05] | [ISI SKU 05] | NA11260100999 | 12"),
    ("h", "Yang tidak boleh dilakukan"),
    ("warn", "- Mengubah header, nama kolom, atau urutan kolom di sheet pertama. Baris judul di atas header boleh dibiarkan."),
    ("warn", "- Menghapus baris produk dari template. Seluruh SKU dan nomor BPOM wajib dilaporkan."),
    ("warn", "- Menambah kolom lain, atau mengisi data di luar enam kolom template."),
    ("h", "Penting"),
    ("b", "- Jika ada satu baris saja yang tidak valid, seluruh upload dibatalkan dan tidak ada data yang tersimpan."),
    ("b", "- Satu file = satu toko. Untuk toko lain, pilih toko tersebut di form lalu download template-nya."),
    ("b", "- Data yang sudah terkirim tidak dapat direvisi dari form. Periksa kembali sebelum Submit; hubungi BD bila ada kesalahan."),
    ("b", "- Sheet ini hanya panduan dan tidak ikut dibaca saat upload."),
]


def _add_guide_sheet(wb: openpyxl.Workbook, region: str, toko: str, n_rows: int) -> None:
    ws = wb.create_sheet(INSTRUCTION_SHEET)
    ws.sheet_view.showGridLines = False
    ws.column_dimensions["A"].width = 125
    lines = [
        ("title", "CARA PENGISIAN - BPOM STOCK OPNAME TOKO"),
        ("p", f"Toko: {toko}   |   Region: {region}   |   {n_rows} baris template (SKU + nomor BPOM)"),
        ("p", "Periode pelaksanaan dan pengisian form: 26 Oktober - 8 November 2026. Batas akhir pengisian: 8 November 2026."),
        ("p", ""),
        *_GUIDE,
    ]
    styles = {
        "title": dict(font=Font(bold=True, size=14, color="FFFFFF"), fill=PatternFill("solid", fgColor="B71C1C")),
        "h": dict(font=Font(bold=True, size=12), fill=PatternFill("solid", fgColor="F2F2F2")),
        "p": dict(font=Font(size=11)),
        "b": dict(font=Font(size=11)),
        "ex": dict(font=Font(size=11, italic=True), fill=PatternFill("solid", fgColor="FFF8E1")),
        "warn": dict(font=Font(size=11, color="B71C1C")),
    }
    for row, (kind, text) in enumerate(lines, start=1):
        cell = ws.cell(row, 1, text)
        cell.alignment = Alignment(wrap_text=True, vertical="top")
        for attr, value in styles[kind].items():
            setattr(cell, attr, value)
        if kind == "h" and row > 1:
            ws.row_dimensions[row].height = 22


def build_template(template_bytes: bytes, region: str, toko: str, skus: Sequence[Sku]) -> bytes:
    """Template untuk SATU toko: seluruh SKU + nomor BPOM, kolom A = nama toko (By Group)."""
    wb = openpyxl.load_workbook(BytesIO(template_bytes))
    ws = wb.worksheets[0]
    for extra in wb.worksheets[1:]:     # sheet kerja BD (daftar distributor, SKU tersembunyi, dsb.) tidak ikut ke pengguna
        wb.remove(extra)
    header_row = next(
        (no for no, cells in enumerate(ws.iter_rows(max_row=HEADER_SEARCH_ROWS, values_only=True), start=1)
         if _looks_like_header(cells)),
        None,
    )
    if header_row is None:
        raise ValueError("Header template tidak ditemukan.")

    ws.cell(header_row, 1).value = EXPECTED_HEADERS[0]   # template lama: 'Distributor Branch' -> 'Nama Toko (By Group)'
    ncols = len(EXPECTED_HEADERS)
    first = header_row + 1
    styles = [copy(ws.cell(first, c)._style) for c in range(1, ncols + 1)]   # gaya baris data pertama
    if ws.max_row >= first:
        ws.delete_rows(first, ws.max_row - first + 1)

    row = first
    for sku in skus:
        values = (toko, sku.product_id, sku.product_name, sku.isi, sku.bpom_number, None)
        for c, value in enumerate(values, start=1):
            cell = ws.cell(row, c, value)
            cell._style = copy(styles[c - 1])
        row += 1
    if row > first:   # Excel menolak QTY negatif/desimal/huruf saat diketik (validasi tetap diulang saat upload)
        qty_col = openpyxl.utils.get_column_letter(len(EXPECTED_HEADERS))
        dv = DataValidation(type="whole", operator="greaterThanOrEqual", formula1="0", allow_blank=True,
                            showErrorMessage=True, errorTitle="QTY tidak valid",
                            error="QTY harus bilangan bulat 0 atau lebih (tidak boleh negatif).",
                            showInputMessage=True, promptTitle="QTY",
                            prompt="Isi bilangan bulat 0 atau lebih. Isi 0 bila tidak ada stok.")
        dv.add(f"{qty_col}{first}:{qty_col}{max(row - 1, first)}")
        ws.add_data_validation(dv)
    _add_guide_sheet(wb, region, toko, row - first)
    wb.active = 0   # tetap membuka sheet data
    out = BytesIO()
    wb.save(out)
    return out.getvalue()
