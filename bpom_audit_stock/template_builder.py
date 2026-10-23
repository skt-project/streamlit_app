"""Membuat template untuk satu distributor dari template resmi + daftar SKU/BPOM prinsipal.

File template asli tidak pernah diubah: file dibaca ke memori, baris data diganti sesuai daftar
SKU terkini dan kolom A diisi nama distributor, lalu disimpan sebagai bytes baru. Header, baris judul,
lebar kolom, dan gaya sel tetap seperti template.
"""
from __future__ import annotations

from copy import copy
from io import BytesIO
from typing import Sequence

import openpyxl
from openpyxl.styles import Alignment, Font, PatternFill

from validation import EXPECTED_HEADERS, HEADER_SEARCH_ROWS, INSTRUCTION_SHEET, Sku, _looks_like_header

# (jenis, teks): "title" | "h" = judul bagian | "p" = paragraf | "b" = butir | "ex" = contoh | "warn" = peringatan
_GUIDE = [
    ("h", "Langkah pengisian"),
    ("b", "1. Buka sheet pertama (data). Kolom Distributor Name (Company - Region) sudah terisi; jangan diubah."),
    ("b", "2. Kolom PRODUCT ID, PRODUCT NAME, dan BPOM NUMBER sudah terisi dari daftar prinsipal; jangan dikosongkan atau diubah."),
    ("b", "3. Isi kolom QTY di setiap baris dengan jumlah stok fisik hasil stock opname untuk SKU + nomor BPOM pada baris tersebut."),
    ("b", "4. Simpan sebagai .xlsx lalu upload di form. File diperiksa otomatis; jika ada error, perbaiki lalu upload ulang."),
    ("h", "Aturan kolom QTY"),
    ("b", "- Wajib diisi di setiap baris. Jika tidak ada stok, isi 0 (jangan dikosongkan)."),
    ("b", "- Harus bilangan bulat. Boleh positif, 0, atau negatif. Tidak boleh desimal (mis. 2,5) atau huruf."),
    ("h", "Pencatatan per SKU dan nomor BPOM"),
    ("b", "- SKU yang sama dengan nomor BPOM berbeda harus dicatat di baris terpisah dan tidak boleh digabung."),
    ("b", "- Satu kombinasi PRODUCT ID + BPOM NUMBER hanya boleh muncul satu kali dalam satu file."),
    ("h", "Jika ada nomor BPOM tambahan (tidak ada di template)"),
    ("b", "- Tambahkan baris baru di bagian paling bawah data pada sheet pertama."),
    ("b", "- Kolom Distributor Name: isi sama persis seperti baris lainnya (salin dari baris di atasnya)."),
    ("b", "- PRODUCT ID: harus SKU yang sudah ada di template. SKU yang tidak ada di template akan ditolak."),
    ("b", "- PRODUCT NAME, BPOM NUMBER (nomor baru), dan QTY: isi lengkap."),
    ("ex", "Contoh baris tambahan:  [nama distributor] | SKINTIFIC-05 | 5X Ceramide Barrier Moisture Gel | NA11260100999 | 12"),
    ("h", "Yang tidak boleh dilakukan"),
    ("warn", "- Mengubah header, nama kolom, atau urutan kolom di sheet pertama. Baris judul di atas header boleh dibiarkan."),
    ("warn", "- Menghapus baris produk dari template. Seluruh SKU dan nomor BPOM wajib dilaporkan."),
    ("warn", "- Menambah kolom lain, atau mengisi data di luar lima kolom template."),
    ("h", "Penting"),
    ("b", "- Jika ada satu baris saja yang tidak valid, seluruh upload dibatalkan dan tidak ada data yang tersimpan."),
    ("b", "- Data yang sudah terkirim tidak dapat direvisi dari form. Periksa kembali sebelum Submit; hubungi BD bila ada kesalahan."),
    ("b", "- Sheet ini hanya panduan dan tidak ikut dibaca saat upload."),
]


def _add_guide_sheet(wb: openpyxl.Workbook, distributor_name: str, n_rows: int) -> None:
    ws = wb.create_sheet(INSTRUCTION_SHEET)
    ws.sheet_view.showGridLines = False
    ws.column_dimensions["A"].width = 125
    lines = [
        ("title", "CARA PENGISIAN - BPOM STOCK OPNAME DISTRIBUTOR"),
        ("p", f"Distributor: {distributor_name}   |   Jumlah baris template: {n_rows} kombinasi SKU + nomor BPOM"),
        ("p", "Periode pelaksanaan dan pengisian form: 23 Oktober - 8 November 2026. Batas akhir pengisian: 8 November 2026."),
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


def build_template(template_bytes: bytes, distributor_name: str, skus: Sequence[Sku]) -> bytes:
    wb = openpyxl.load_workbook(BytesIO(template_bytes))
    ws = wb.worksheets[0]
    header_row = next(
        (no for no, cells in enumerate(ws.iter_rows(max_row=HEADER_SEARCH_ROWS, values_only=True), start=1)
         if _looks_like_header(cells)),
        None,
    )
    if header_row is None:
        raise ValueError("Header template tidak ditemukan.")

    ncols = len(EXPECTED_HEADERS)
    first = header_row + 1
    styles = [copy(ws.cell(first, c)._style) for c in range(1, ncols + 1)]   # gaya baris data pertama
    if ws.max_row >= first:
        ws.delete_rows(first, ws.max_row - first + 1)

    for i, sku in enumerate(skus):
        values = (distributor_name, sku.product_id, sku.product_name, sku.bpom_number, None)
        for c, value in enumerate(values, start=1):
            cell = ws.cell(first + i, c, value)
            cell._style = copy(styles[c - 1])
    _add_guide_sheet(wb, distributor_name, len(skus))
    wb.active = 0   # tetap membuka sheet data
    out = BytesIO()
    wb.save(out)
    return out.getvalue()
