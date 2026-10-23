"""Data referensi dari BD: daftar distributor (`reference/`) dan katalog SKU + nomor BPOM (Sheet1 template resmi)."""
from __future__ import annotations

from pathlib import Path

import openpyxl

from validation import EXPECTED_HEADERS, HEADER_SEARCH_ROWS, Sku, normalize_name, row_key

DISTRIBUTOR_HEADERS = ("Distributor Company", "Distributor Name")


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


def load_catalogue(path: Path) -> list[Sku]:
    """Daftar SKU + nama produk + isi + nomor BPOM resmi, dibaca dari Sheet1 template resmi dari BD.

    Template adalah sumber tunggal: yang di-download distributor dan yang dipakai validasi selalu sama.
    """
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    except Exception as exc:
        raise ReferenceDataError(f"Template '{path.name}' tidak dapat dibaca: {exc}") from None
    try:
        ws = wb.worksheets[0]
        ws.reset_dimensions()
        rows = enumerate(ws.iter_rows(values_only=True), start=1)
        header_no = None
        for no, cells in rows:
            if no > HEADER_SEARCH_ROWS:
                break
            if tuple(_clean(c) for c in cells[: len(EXPECTED_HEADERS)]) == EXPECTED_HEADERS:
                header_no = no
                break
        if header_no is None:
            raise ReferenceDataError(f"Header template '{path.name}' harus {list(EXPECTED_HEADERS)}.")
        result, seen = [], set()
        for no, cells in rows:
            cells = tuple(cells) + (None,) * (len(EXPECTED_HEADERS) - len(cells))
            _, product_id, product_name, isi, bpom, _ = (_clean(c) for c in cells[: len(EXPECTED_HEADERS)])
            if not (product_id or product_name or isi or bpom):
                continue
            if not (product_id and product_name and isi and bpom):
                raise ReferenceDataError(
                    f"'{path.name}' baris {no}: PRODUCT ID, PRODUCT NAME, ISI, dan BPOM NUMBER wajib diisi.")
            key = row_key(product_id, bpom)
            if key in seen:
                raise ReferenceDataError(f"'{path.name}' baris {no}: kombinasi {product_id} + {bpom} ganda.")
            seen.add(key)
            result.append(Sku(product_id, product_name, bpom, isi))
    finally:
        wb.close()
    if not result:
        raise ReferenceDataError(f"'{path.name}' tidak berisi SKU.")
    return result
