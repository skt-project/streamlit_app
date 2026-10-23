"""Data referensi dari BD: daftar distributor dan daftar SKU + nomor BPOM.

Kedua file ada di folder `reference/`. Untuk memperbarui daftar, ganti file-nya (header harus sama).
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import openpyxl

from validation import Sku, normalize_name, row_key

DISTRIBUTOR_HEADERS = ("Distributor Company", "Distributor Name")
SKU_HEADERS = ("SKU", "BPOM Product Name", "BPOM Number", "Expired Date")


class ReferenceDataError(Exception):
    """File referensi rusak / tidak sesuai format."""


def _clean(value) -> str:
    return "" if value is None else str(value).strip()


def _read(path: Path, headers: tuple[str, ...]):
    """Hasilkan (nomor_baris, nilai-nilai) untuk baris yang tidak kosong."""
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    except Exception as exc:
        raise ReferenceDataError(f"File '{path.name}' tidak dapat dibaca: {exc}") from None
    try:
        ws = wb.worksheets[0]
        ws.reset_dimensions()
        rows = ws.iter_rows(values_only=True)
        header = tuple(_clean(c) for c in (next(rows, ()) or ())[: len(headers)])
        if header != headers:
            raise ReferenceDataError(f"Header '{path.name}' harus {list(headers)}, ditemukan {list(header)}.")
        for no, row in enumerate(rows, start=2):
            row = tuple(row) + (None,) * (len(headers) - len(row))
            if all(_clean(c) == "" for c in row[: len(headers)]):
                continue
            yield no, row[: len(headers)]
    finally:
        wb.close()


def load_distributors(path: Path) -> list[tuple[str, str]]:
    """[(Distributor Company, Distributor Name)] sesuai file BD."""
    result, seen = [], set()
    for no, (company, name) in _read(path, DISTRIBUTOR_HEADERS):
        company, name = _clean(company), _clean(name)
        if not company or not name:
            raise ReferenceDataError(f"'{path.name}' baris {no}: company dan nama distributor wajib diisi.")
        norm = normalize_name(name)
        if norm in seen:
            raise ReferenceDataError(f"'{path.name}' baris {no}: nama distributor ganda '{name}'.")
        seen.add(norm)
        result.append((company, name))
    if not result:
        raise ReferenceDataError(f"'{path.name}' tidak berisi distributor.")
    return result


def load_skus(path: Path) -> list[Sku]:
    """Daftar SKU + nomor BPOM resmi dari prinsipal (baris kosong dilewati)."""
    result, seen = [], set()
    for no, (product_id, product_name, bpom, expired) in _read(path, SKU_HEADERS):
        product_id, product_name, bpom = _clean(product_id), _clean(product_name), _clean(bpom)
        if not (product_id and product_name and bpom):
            raise ReferenceDataError(f"'{path.name}' baris {no}: SKU, nama produk, dan nomor BPOM wajib diisi.")
        key = row_key(product_id, bpom)
        if key in seen:
            raise ReferenceDataError(f"'{path.name}' baris {no}: kombinasi {product_id} + {bpom} ganda.")
        seen.add(key)
        if isinstance(expired, datetime):
            expired = expired.date()
        result.append(Sku(product_id, product_name, bpom, expired if isinstance(expired, date) else None))
    if not result:
        raise ReferenceDataError(f"'{path.name}' tidak berisi SKU.")
    return result
