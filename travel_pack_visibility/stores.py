"""Baca daftar toko (spreadsheet Travel Pack Distribution) dan terapkan aturan kelayakan.

Aturan: toko tampil untuk ASM-nya bila nilai kolom Ach SKU (MOQ) — kolom K, bisa diubah lewat secrets —
>= 4. Nilai kosong, teks, error formula (#N/A) atau persen dianggap TIDAK memenuhi syarat.

Header dicari di beberapa baris awal (header tidak harus di baris 1). Kolom dikenali dari nama header
(lihat FIELD_ALIASES); kolom Ach SKU dibaca menurut POSISI (huruf kolom) seperti aturan yang diberikan.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from gspread.exceptions import WorksheetNotFound

log = logging.getLogger("travel_pack.stores")

HEADER_SCAN_ROWS = 15

# Urutan alias = prioritas. Dibandingkan setelah dinormalisasi (huruf kecil, spasi dirapikan).
FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "asm": ("nama asm", "asm", "asm name", "area sales manager"),
    "store_code": ("kode st toko", "kode toko", "kode st", "st code", "kode store", "store code", "store id",
                   "cust id", "cust. id", "customer code", "kode customer", "cust code"),
    "store_name": ("nama toko", "store name", "nama store", "customer name", "nama customer", "toko"),
    "spv": ("nama spv", "spv", "spv name", "supervisor", "nama supervisor"),
    "region": ("region", "nama region"),
}
REQUIRED_FIELDS = ("asm", "store_code")


class StoreListError(Exception):
    """Daftar toko tidak dapat dibaca. Pesan aman ditampilkan ke user."""


def norm_header(value: Any) -> str:
    return " ".join(str(value or "").replace(" ", " ").lower().split())


def clean(value: Any) -> str:
    return " ".join(str(value or "").replace(" ", " ").split())


def parse_number(value: Any) -> float | None:
    """'4' / '4.0' / '4,0' / ' 5 ' -> angka. Kosong, teks, #N/A, persen -> None."""
    s = str(value if value is not None else "").replace(" ", "").replace(" ", "").strip()
    if not s or "%" in s:
        return None
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".") if s.rfind(",") > s.rfind(".") else s.replace(",", "")
    elif "," in s:
        tail = s.rpartition(",")[2]
        s = s.replace(",", "") if len(tail) == 3 else s.replace(",", ".")
    try:
        v = float(s)
    except ValueError:
        return None
    return v if math.isfinite(v) else None


def fmt_number(value: float | None) -> str:
    if value is None:
        return "-"
    return str(int(value)) if float(value).is_integer() else f"{value:g}"


@dataclass(frozen=True)
class Store:
    code: str
    name: str
    asm: str
    spv: str
    region: str
    ach: float | None
    sheet_row: int

    @property
    def label(self) -> str:
        return f"{self.code} · {self.name}" if self.name else self.code


@dataclass
class StoreList:
    stores: dict[str, Store]                       # key = kode toko (huruf besar)
    min_ach: float
    worksheet: str = ""
    ach_column: str = "K"
    ach_header: str = ""
    columns: dict[str, str] = field(default_factory=dict)   # field -> header yang dipakai
    diagnostics: list[str] = field(default_factory=list)

    def asm_names(self) -> list[str]:
        seen: dict[str, str] = {}
        for s in self.stores.values():
            if s.asm:
                seen.setdefault(s.asm.casefold(), s.asm)
        return sorted(seen.values(), key=str.casefold)

    def stores_for(self, asm: str) -> list[Store]:
        key = clean(asm).casefold()
        return [s for s in self.stores.values() if s.asm.casefold() == key]

    def is_eligible(self, store: Store) -> bool:
        return store.ach is not None and store.ach >= self.min_ach

    def eligible_for(self, asm: str) -> list[Store]:
        return sorted((s for s in self.stores_for(asm) if self.is_eligible(s)),
                      key=lambda s: (s.name.casefold(), s.code))

    def get(self, code: str) -> Store | None:
        return self.stores.get(clean(code).upper())


def find_header(values: Sequence[Sequence[Any]], overrides: Mapping[str, str] | None = None
                ) -> tuple[int, dict[str, int]] | None:
    """Cari baris header; kembalikan (indeks_baris, {field: indeks_kolom}) atau None."""
    aliases = {f: ((norm_header(overrides[f]),) if overrides and overrides.get(f) else a)
               for f, a in FIELD_ALIASES.items()}
    for r, row in enumerate(values[:HEADER_SCAN_ROWS]):
        cells = [norm_header(c) for c in row]
        pos: dict[str, int] = {}
        for f, names in aliases.items():
            for name in names:
                if name in cells:
                    pos[f] = cells.index(name)
                    break
        if all(f in pos for f in REQUIRED_FIELDS):
            return r, pos
    return None


def _cell(row: Sequence[Any], idx: int | None) -> str:
    if idx is None or idx >= len(row):
        return ""
    return clean(row[idx])


def parse_store_list(values: Sequence[Sequence[Any]], *, ach_index: int, ach_column: str, min_ach: float,
                     overrides: Mapping[str, str] | None = None, worksheet: str = "") -> StoreList:
    found = find_header(values, overrides)
    if found is None:
        raise StoreListError("Header daftar toko tidak ditemukan. Dibutuhkan kolom Nama ASM dan Kode Toko "
                             "(mis. 'ASM' dan 'Kode ST Toko').")
    hdr, pos = found
    header_row = values[hdr]
    ach_header = clean(header_row[ach_index]) if ach_index < len(header_row) else ""
    stores: dict[str, Store] = {}
    dup = blank_asm = not_number = 0
    for offset, row in enumerate(values[hdr + 1:], start=hdr + 2):    # nomor baris di sheet (1-based)
        code = _cell(row, pos["store_code"]).upper()
        if not code:
            continue
        if code in stores:
            dup += 1
            continue
        raw_ach = _cell(row, ach_index)
        ach = parse_number(raw_ach)
        if ach is None and raw_ach:
            not_number += 1
        asm = _cell(row, pos["asm"])
        if not asm:
            blank_asm += 1
        stores[code] = Store(code=code, name=_cell(row, pos.get("store_name")), asm=asm,
                             spv=_cell(row, pos.get("spv")), region=_cell(row, pos.get("region")),
                             ach=ach, sheet_row=offset)
    sl = StoreList(stores=stores, min_ach=min_ach, worksheet=worksheet, ach_column=ach_column,
                   ach_header=ach_header,
                   columns={f: clean(header_row[i]) for f, i in pos.items()})
    eligible = sum(1 for s in stores.values() if sl.is_eligible(s))
    sl.diagnostics.append(f"Tab '{worksheet}', header di baris {hdr + 1}. Kolom dipakai: "
                          + ", ".join(f"{f}='{h}'" for f, h in sl.columns.items()) + ".")
    sl.diagnostics.append(f"Kolom {ach_column} (aturan >= {fmt_number(min_ach)}) berheader '{ach_header or '-'}'.")
    sl.diagnostics.append(f"Toko: {len(stores)}; memenuhi syarat: {eligible}; ASM: {len(sl.asm_names())}.")
    if ach_header and "ach" not in ach_header.lower():
        sl.diagnostics.append(f"PERHATIAN: header kolom {ach_column} bukan 'Ach SKU (MOQ)'. Cek ach_sku_column.")
    if not_number:
        sl.diagnostics.append(f"{not_number} toko punya nilai {ach_column} yang bukan angka (dianggap tidak memenuhi).")
    if blank_asm:
        sl.diagnostics.append(f"{blank_asm} toko tanpa nama ASM (tidak bisa dipilih).")
    if dup:
        sl.diagnostics.append(f"{dup} baris kode toko duplikat diabaikan (baris pertama dipakai).")
    return sl


def load_store_list(gc, sheet_id: str, *, worksheet: str, ach_index: int, ach_column: str, min_ach: float,
                    overrides: Mapping[str, str] | None = None) -> StoreList:
    """`gc` = gspread client. Tab `worksheet` dipakai bila diisi; kalau kosong semua tab dipindai."""
    try:
        sh = gc.open_by_key(sheet_id)
    except Exception as exc:
        raise StoreListError("Spreadsheet daftar toko tidak dapat dibuka. Pastikan sudah di-share ke service "
                             f"account (minimal Viewer). ({exc.__class__.__name__})") from exc
    kwargs = dict(ach_index=ach_index, ach_column=ach_column, min_ach=min_ach, overrides=overrides)
    try:
        if worksheet:
            try:
                ws = sh.worksheet(worksheet)
            except WorksheetNotFound as exc:
                raise StoreListError(f"Tab '{worksheet}' tidak ada di spreadsheet daftar toko.") from exc
            return parse_store_list(ws.get_all_values(), worksheet=ws.title, **kwargs)
        for ws in sh.worksheets():
            values = ws.get_all_values()
            if find_header(values, overrides):
                return parse_store_list(values, worksheet=ws.title, **kwargs)
    except StoreListError:
        raise
    except Exception as exc:
        log.exception("Gagal membaca daftar toko")
        raise StoreListError(f"Gagal membaca daftar toko ({exc.__class__.__name__}).") from exc
    raise StoreListError("Tidak ada tab dengan header Nama ASM + Kode Toko di spreadsheet daftar toko.")
