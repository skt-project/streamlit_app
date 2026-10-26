"""Daftar SKU untuk form "Tambah SKU / nomor BPOM langsung", dibaca dari `reference/Daftar SKU.xlsx`.

File ini diekspor oleh BD dari master product (kolom SKU, Product Name, Pack Size). Aplikasi TIDAK terhubung ke BigQuery:
seluruh data referensi berupa file, sama seperti daftar toko dan katalog template.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import openpyxl

HEADER_SEARCH_ROWS = 10
SKU_COLS = ("sku", "product id")
NAME_COLS = ("product name", "product_name", "nama produk")
PACK_COLS = ("pack size", "pack_size", "isi (gr / ml)", "isi")


class SkuListMissing(Exception):
    """File daftar SKU belum diletakkan di reference/ (bukan file rusak)."""


class SkuListError(Exception):
    """File daftar SKU rusak / tidak sesuai format."""


@dataclass(frozen=True)
class Product:
    sku: str
    product_name: str
    pack_size: str = ""

    @property
    def label(self) -> str:
        return f"{self.sku} - {self.product_name}"


def _clean(value) -> str:
    return "" if value is None else str(value).strip()


def _key(value) -> str:
    return re.sub(r"\s+", " ", _clean(value)).casefold()


def products_from_rows(rows) -> tuple[list[Product], int]:
    """(produk unik per SKU terurut, jumlah baris yang dilewati karena tanpa SKU/nama produk)."""
    seen, result, skipped = set(), [], 0
    for row in rows:
        sku, name, pack = _clean(row["sku"]), _clean(row["product_name"]), _clean(row["pack_size"])
        if not sku or not name:
            skipped += 1
            continue
        if sku.upper() in seen:
            continue
        seen.add(sku.upper())
        result.append(Product(sku, name, pack))
    return sorted(result, key=lambda p: p.sku.upper()), skipped


def load_products(path: Path) -> tuple[list[Product], int]:
    """Baca daftar SKU. Kolom wajib: SKU (atau PRODUCT ID) dan Product Name; Pack Size/ISI opsional.

    Urutan kolom bebas, huruf besar/kecil bebas, baris judul di atas header (10 baris pertama) boleh, hanya sheet pertama dibaca.
    """
    if not path.is_file():
        raise SkuListMissing(f"Daftar SKU belum tersedia: letakkan file '{path.name}' di folder '{path.parent.name}/' "
                             "dengan kolom SKU, Product Name, Pack Size.")
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    except Exception as exc:
        raise SkuListError(f"File '{path.name}' tidak dapat dibaca: {exc}") from None
    try:
        ws = wb.worksheets[0]
        ws.reset_dimensions()
        rows = ws.iter_rows(values_only=True)
        cols = None
        for no, cells in enumerate(rows, start=1):
            header = [_key(c) for c in (cells or ())]
            find = lambda aliases: next((header.index(a) for a in aliases if a in header), None)  # noqa: E731
            i_sku, i_name, i_pack = find(SKU_COLS), find(NAME_COLS), find(PACK_COLS)
            if i_sku is not None and i_name is not None:
                cols = (i_sku, i_name, i_pack)
                break
            if no >= HEADER_SEARCH_ROWS:
                break
        if cols is None:
            raise SkuListError(f"Header '{path.name}' tidak ditemukan. Perlu kolom SKU dan Product Name (opsional Pack Size).")
        i_sku, i_name, i_pack = cols
        data = []
        for cells in rows:
            cells = tuple(cells)
            get = lambda i: cells[i] if i is not None and i < len(cells) else None  # noqa: E731
            if all(_clean(c) == "" for c in cells):
                continue
            data.append({"sku": get(i_sku), "product_name": get(i_name), "pack_size": get(i_pack)})
    finally:
        wb.close()
    products, skipped = products_from_rows(data)
    if not products:
        raise SkuListError(f"'{path.name}' tidak berisi SKU.")
    return products, skipped
