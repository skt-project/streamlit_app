"""Data referensi dari BD: daftar toko (`reference/Daftar Store.xlsx`) serta katalog SKU + nomor BPOM (Sheet1 template resmi)."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import openpyxl

from validation import (EXPECTED_HEADERS, HEADER_SEARCH_ROWS, LEGACY_FIRST_HEADER, Sku, normalize_name, row_key)

# Kolom daftar toko dari BD. Nama kolom dibandingkan tanpa peduli huruf besar/kecil dan spasi; kolom lain diabaikan.
REGION_COLS = ("region",)
GROUP_COLS = ("nama group toko", "nama grup toko", "group toko", "grup toko", "nama group", "nama grup", "group", "grup")
TOKO_COLS = ("nama toko", "toko", "store name")
STORE_HEADERS_HELP = ("Region", "Nama Group Toko", "Nama Toko")      # untuk pesan error


class ReferenceDataError(Exception):
    """File referensi rusak / tidak sesuai format."""


class StoreListMissing(ReferenceDataError):
    """Daftar toko dari BD belum diletakkan di reference/ (bukan file rusak)."""


def _clean(value) -> str:
    return "" if value is None else str(value).strip()


def _col_key(value) -> str:
    return re.sub(r"\s+", " ", _clean(value)).casefold()


@dataclass(frozen=True)
class Toko:
    """Satu pilihan di dropdown: nama grup bila toko milik grup, selain itu nama toko ("Nama Toko (By Group)")."""
    region: str
    name: str                  # nama grup bila ada, selain itu nama toko; ini yang masuk kolom A template dan ke sheet
    is_group: bool = False
    outlets: int = 1           # jumlah baris toko di daftar BD yang digabung ke pilihan ini
    search_text: str = ""      # huruf kecil, untuk kotak cari (dihitung sekali saat memuat)

    @property
    def label(self) -> str:
        return f"{self.name}  ·  grup ({self.outlets} toko)" if self.is_group and self.outlets > 1 else self.name


def load_stores(path: Path, *, max_listed: int = 8) -> list[Toko]:
    """Daftar toko dari BD. Kolom wajib: Region dan Nama Toko; Nama Group Toko opsional (urutan kolom bebas, kolom lain diabaikan).

    Baris dengan Nama Group Toko terisi menjadi SATU pilihan atas nama grup itu (semua toko dalam grup digabung);
    baris tanpa grup menjadi pilihan atas nama toko. Pilihan unik per (Region, nama). Satu masalah saja menggagalkan seluruh
    pemuatan supaya tidak ada toko yang diam-diam hilang. Baris judul di atas header (10 baris pertama) diperbolehkan.
    """
    if not path.is_file():
        raise StoreListMissing(
            f"Daftar toko belum tersedia: letakkan file '{path.name}' di folder '{path.parent.name}/' "
            f"dengan kolom {list(STORE_HEADERS_HELP)}.")
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    except Exception as exc:
        raise ReferenceDataError(f"File '{path.name}' tidak dapat dibaca: {exc}") from None
    problems: list[str] = []
    merged: dict[tuple[str, str], list] = {}      # (region, nama) -> [Region asli, nama asli, is_group, jumlah baris]
    try:
        ws = wb.worksheets[0]
        ws.reset_dimensions()
        rows = ws.iter_rows(values_only=True)
        header, header_no, cols = [], 0, None
        for no, cells in enumerate(rows, start=1):
            candidate = [_col_key(c) for c in (cells or ())]
            if not header:
                header, header_no = candidate, no
            found = _find_store_columns(candidate)
            if found:
                header, header_no, cols = candidate, no, found
                break
            if no >= HEADER_SEARCH_ROWS:
                break
        if cols is None:
            raise ReferenceDataError(
                f"Header '{path.name}' tidak ditemukan. Perlu kolom Region dan Nama Toko (opsional Nama Group Toko); "
                f"ditemukan {header}.")
        i_region, i_group, i_toko = cols
        for no, row in enumerate(rows, start=header_no + 1):
            row = tuple(row) + (None,) * (len(header) - len(row))
            if all(_clean(c) == "" for c in row):
                continue
            region, toko = _clean(row[i_region]), _clean(row[i_toko])
            group = _clean(row[i_group]) if i_group is not None else ""
            name = group or toko
            if not (region and name):
                problems.append(f"baris {no}: Region dan Nama Toko wajib diisi")
                continue
            key = (normalize_name(region), normalize_name(name))
            entry = merged.setdefault(key, [region, name, bool(group), 0])
            entry[3] += 1
            if bool(group) != entry[2]:
                problems.append(f"baris {no}: '{name}' di region '{region}' muncul sebagai grup dan sebagai toko tunggal")
    finally:
        wb.close()
    if problems:
        shown = "; ".join(problems[:max_listed]) + (
            f"; ... (+{len(problems) - max_listed} masalah lain)" if len(problems) > max_listed else "")
        raise ReferenceDataError(f"'{path.name}' bermasalah: {shown}.")
    if not merged:
        raise ReferenceDataError(f"'{path.name}' tidak berisi toko.")
    return [Toko(region, name, is_group, n, f"{name}".lower()) for region, name, is_group, n in merged.values()]


def _find_store_columns(header: list[str]):
    """(indeks region, indeks grup | None, indeks toko) bila header lengkap, selain itu None."""
    def find(aliases):
        return next((header.index(a) for a in aliases if a in header), None)
    region, toko = find(REGION_COLS), find(TOKO_COLS)
    if region is None or toko is None:
        return None
    return region, find(GROUP_COLS), toko


def _header_matches(cells) -> bool:
    cells = tuple(_clean(c) for c in cells[: len(EXPECTED_HEADERS)])
    return len(cells) == len(EXPECTED_HEADERS) and cells[1:] == EXPECTED_HEADERS[1:] and \
        cells[0] in (EXPECTED_HEADERS[0], LEGACY_FIRST_HEADER)


def load_catalogue(path: Path) -> list[Sku]:
    """Daftar SKU + nama produk + isi + nomor BPOM resmi, dibaca dari Sheet1 template resmi dari BD.

    Template adalah sumber tunggal: yang di-download pengguna dan yang dipakai validasi selalu sama.
    Header kolom A boleh "Nama Toko (By Group)" atau "Distributor Branch" (template lama).
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
            if _header_matches(cells):
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
