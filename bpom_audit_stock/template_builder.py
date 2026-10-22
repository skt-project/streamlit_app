"""Membuat salinan template yang kolom A-nya sudah terisi nama distributor.

File template asli tidak pernah diubah: file dibaca ke memori, kolom pertama diisi, lalu
disimpan sebagai bytes baru. Header, judul, dan isi kolom lain dibiarkan apa adanya.
"""
from __future__ import annotations

from io import BytesIO

import openpyxl

from validation import EXPECTED_HEADERS, HEADER_SEARCH_ROWS, _is_blank, _looks_like_header


def build_prefilled_template(template_bytes: bytes, distributor_name: str) -> bytes:
    wb = openpyxl.load_workbook(BytesIO(template_bytes))
    ws = wb.worksheets[0]
    header_row = next(
        (no for no, cells in enumerate(ws.iter_rows(max_row=HEADER_SEARCH_ROWS, values_only=True), start=1)
         if _looks_like_header(cells)),
        None,
    )
    if header_row is None:
        raise ValueError("Header template tidak ditemukan.")
    for row in ws.iter_rows(min_row=header_row + 1, max_col=len(EXPECTED_HEADERS)):
        if not _is_blank([c.value for c in row[1:]]):   # hanya baris produk
            row[0].value = distributor_name
    out = BytesIO()
    wb.save(out)
    return out.getvalue()
