"""Data referensi dari BD: daftar distributor dan daftar store (`reference/`) serta katalog SKU + nomor BPOM
(Sheet1 template resmi)."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import openpyxl

from validation import EXPECTED_HEADERS, HEADER_SEARCH_ROWS, Sku, normalize_name, row_key

DISTRIBUTOR_HEADERS = ("Distributor Company", "Distributor Name")
STORE_HEADERS = ("Distributor Name", "Store ID", "Store Name")   # kolom wajib; kolom lain diabaikan


class ReferenceDataError(Exception):
    """File referensi rusak / tidak sesuai format."""


class StoreListMissing(ReferenceDataError):
    """Daftar store dari BD belum diletakkan di reference/ (bukan file rusak)."""


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


@dataclass(frozen=True)
class Store:
    company: str        # diturunkan dari daftar distributor (berdasarkan Distributor Name)
    distributor: str    # nama kanonik dari daftar distributor
    store_id: str       # teks (nol di depan dipertahankan)
    store_name: str
    search_text: str = ""   # huruf kecil, untuk kotak cari (dihitung sekali saat memuat)

    @property
    def label(self) -> str:
        return f"{self.store_id} - {self.store_name}"


def _id_text(value) -> str:
    """Store ID sebagai teks; angka bulat dari Excel (mis. 1234.0) dibersihkan menjadi '1234'."""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return _clean(value)


def load_stores(path: Path, distributors: list[tuple[str, str]], *, max_listed: int = 8) -> list[Store]:
    """Daftar store dari BD. Kolom wajib: Distributor Name, Store ID, Store Name (urutan bebas, kolom lain diabaikan).

    Distributor Name harus ada di daftar distributor (Company diturunkan dari sana); Store ID wajib dan unik.
    Satu masalah saja menggagalkan seluruh pemuatan, supaya tidak ada store yang diam-diam hilang.
    """
    if not path.is_file():
        raise StoreListMissing(
            f"Daftar store belum tersedia: letakkan file '{path.name}' di folder '{path.parent.name}/' "
            f"dengan kolom {list(STORE_HEADERS)}.")
    by_norm = {normalize_name(name): (company, name) for company, name in distributors}
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    except Exception as exc:
        raise ReferenceDataError(f"File '{path.name}' tidak dapat dibaca: {exc}") from None
    try:
        ws = wb.worksheets[0]
        ws.reset_dimensions()
        rows = ws.iter_rows(values_only=True)
        # Header boleh di baris mana pun dalam HEADER_SEARCH_ROWS baris pertama (file BD kadang punya baris judul).
        header, header_no = [], 0
        for no, cells in enumerate(rows, start=1):
            candidate = [_clean(c) for c in (cells or ())]
            if not header:
                header, header_no = candidate, no       # kandidat pertama dipakai untuk pesan error bila tidak ada yang cocok
            if all(h in candidate for h in STORE_HEADERS):
                header, header_no = candidate, no
                break
            if no >= HEADER_SEARCH_ROWS:
                break
        missing = [h for h in STORE_HEADERS if h not in header]
        if missing:
            raise ReferenceDataError(f"Header '{path.name}' kurang kolom {missing}; ditemukan {header}.")
        i_dist, i_id, i_name = (header.index(h) for h in STORE_HEADERS)
        result, seen, unknown, problems = [], {}, {}, []
        for no, row in enumerate(rows, start=header_no + 1):
            row = tuple(row) + (None,) * (len(header) - len(row))
            if all(_clean(c) == "" for c in row):
                continue
            dist, store_id, store_name = _clean(row[i_dist]), _id_text(row[i_id]), _clean(row[i_name])
            if not (dist and store_id and store_name):
                problems.append(f"baris {no}: Distributor Name, Store ID, dan Store Name wajib diisi")
                continue
            if store_id.upper() in seen:
                problems.append(f"baris {no}: Store ID '{store_id}' ganda (juga di baris {seen[store_id.upper()]})")
                continue
            if normalize_name(dist) not in by_norm:
                unknown.setdefault(dist, no)
                continue
            seen[store_id.upper()] = no
            company, canonical = by_norm[normalize_name(dist)]
            result.append(Store(company, canonical, store_id, store_name, f"{store_id} {store_name}".lower()))
    finally:
        wb.close()
    if unknown:
        names = [(n, r) for n, r in unknown.items()]
        sample = ", ".join(f"'{n}' (baris {r})" for n, r in names[:max(5, max_listed)])
        problems.append(f"{len(unknown)} Distributor Name tidak ada di daftar distributor: {sample}")
    if problems:
        shown = "; ".join(problems[:max_listed]) + (
            f"; ... (+{len(problems) - max_listed} masalah lain)" if len(problems) > max_listed else "")
        raise ReferenceDataError(f"'{path.name}' bermasalah: {shown}.")
    if not result:
        raise ReferenceDataError(f"'{path.name}' tidak berisi store.")
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
