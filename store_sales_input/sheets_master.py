"""Baca master Store dan SKU dari Google Sheets.

Worksheet dicari lebih dulu lewat nama dari secrets; bila tidak ada / headernya tidak
cocok, semua worksheet dipindai dan dipilih yang memuat header exact yang dibutuhkan.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Sequence

from gspread.exceptions import WorksheetNotFound

from validation import Sku, Store, is_non_ba

log = logging.getLogger("store_sales.master")

# Kolom master Store yang disebut di spesifikasi (dipakai di test/dokumentasi).
STORE_HEADERS = [
    "Store/Group ID", "Cust. ID", "Store Name", "Region", "Cust Category",
    "Group", "JCP Budget (inc VAT)", "BA/ Non BA",
]
# Hanya kolom yang benar-benar dipakai aplikasi yang WAJIB ada. Kolom anggaran berganti nama tiap
# periode (mis. "Budget Q4") dan tidak dipakai, jadi tidak boleh menggagalkan pembacaan master.
STORE_REQUIRED = ["Cust. ID", "Store Name", "Region", "Group", "BA/ Non BA"]
SKU_HEADERS = ["SKU Code", "SKU Name", "STP"]
_HEADER_SCAN_ROWS = 10
# Tim kadang mengganti nama kolom di spreadsheet master (mis. 2026-10: "Cust. ID" -> "Kode Toko",
# "Store Name" -> "Nama Toko"). Nama alternatif dipetakan ke nama yang dipakai kode, jadi dua-duanya diterima.
HEADER_ALIASES = {"Kode Toko": "Cust. ID", "Nama Toko": "Store Name"}


class MasterError(Exception):
    """Master tidak dapat dibaca / tidak ditemukan. Pesan aman ditampilkan ke user."""


@dataclass
class MasterData:
    stores: dict[str, Store]                 # hanya Non BA, key = Cust. ID
    skus: dict[str, Sku]
    diagnostics: list[str] = field(default_factory=list)


def find_table(values: Sequence[Sequence[Any]], required: Sequence[str]) -> tuple[int, dict[str, int]] | None:
    """Cari baris header (di beberapa baris awal) yang memuat semua header `required` (exact, setelah trim).

    Kembalikan (indeks_baris_header, {header: indeks_kolom}) atau None.
    """
    for r, row in enumerate(values[:_HEADER_SCAN_ROWS]):
        pos: dict[str, int] = {}
        for c, cell in enumerate(row):
            text = str(cell).strip()
            text = HEADER_ALIASES.get(text, text)
            if text in required and text not in pos:
                pos[text] = c
        if len(pos) == len(required):
            return r, pos
    return None


def _cell(row: Sequence[Any], idx: int) -> str:
    return str(row[idx]).strip() if idx < len(row) and row[idx] is not None else ""


def parse_stores(values: Sequence[Sequence[Any]]) -> tuple[dict[str, Store], list[str]]:
    """Hanya baris `BA/ Non BA` = Non BA (trim + case-insensitive). Cust. ID dipakai sebagai Store ID."""
    found = find_table(values, STORE_REQUIRED)
    if found is None:
        raise MasterError("Header master Store tidak ditemukan. Dibutuhkan: " + " | ".join(STORE_REQUIRED))
    hdr_row, pos = found
    stores: dict[str, Store] = {}
    notes: list[str] = []
    skipped_ba = dup = 0
    for row in values[hdr_row + 1:]:
        cust_id = _cell(row, pos["Cust. ID"])
        if not cust_id:
            continue
        status = _cell(row, pos["BA/ Non BA"])
        if not is_non_ba(status):
            skipped_ba += 1
            continue
        if cust_id in stores:
            dup += 1
            continue
        stores[cust_id] = Store(
            store_id=cust_id,
            store_name=_cell(row, pos["Store Name"]),
            region=_cell(row, pos["Region"]),
            group=_cell(row, pos["Group"]),
            ba_status=status,
        )
    notes.append(f"Store Non BA: {len(stores)}; dilewati (bukan Non BA): {skipped_ba}.")
    if dup:
        notes.append(f"Cust. ID duplikat di master Non BA: {dup} baris diabaikan (baris pertama dipakai).")
    return stores, notes


def parse_skus(values: Sequence[Sequence[Any]]) -> tuple[dict[str, Sku], list[str]]:
    found = find_table(values, SKU_HEADERS)
    if found is None:
        raise MasterError("Header master SKU tidak ditemukan. Dibutuhkan: " + " | ".join(SKU_HEADERS))
    hdr_row, pos = found
    skus: dict[str, Sku] = {}
    dup = 0
    for row in values[hdr_row + 1:]:
        code = _cell(row, pos["SKU Code"])
        if not code:
            continue
        if code in skus:
            dup += 1
            continue
        skus[code] = Sku(code=code, name=_cell(row, pos["SKU Name"]))   # STP sengaja tidak dipakai
    notes = [f"SKU: {len(skus)}."]
    if dup:
        notes.append(f"SKU Code duplikat di master: {dup} baris diabaikan (baris pertama dipakai).")
    return skus, notes


def _locate(spreadsheet, preferred: str, required: Sequence[str], label: str):
    """Cari worksheet yang memuat header `required`; kembalikan (values, nama_worksheet)."""
    tried: list[str] = []
    try:
        ws = spreadsheet.worksheet(preferred)
        values = ws.get_all_values()
        if find_table(values, required):
            return values, preferred
        tried.append(preferred)
    except WorksheetNotFound:                      # worksheet tidak ada → pindai semua
        pass
    for ws in spreadsheet.worksheets():
        if ws.title in tried or ws.title == preferred:
            continue
        values = ws.get_all_values()
        if find_table(values, required):
            return values, ws.title
    raise MasterError(
        f"Worksheet master {label} tidak ditemukan: tidak ada worksheet dengan header "
        + " | ".join(required) + " (atau padanannya: " + ", ".join(f"{k} = {v}" for k, v in HEADER_ALIASES.items())
        + f") (nama yang dikonfigurasi: '{preferred}')."
    )


def load_master(gc, master_sheet_id: str, store_ws: str, sku_ws: str) -> MasterData:
    """Muat master Store + SKU. `gc` = gspread client."""
    try:
        sh = gc.open_by_key(master_sheet_id)
    except Exception as exc:
        raise MasterError(
            "Spreadsheet master tidak dapat dibuka. Pastikan service account sudah di-share "
            f"sebagai minimal Viewer. ({exc.__class__.__name__})"
        ) from exc
    store_values, store_name = _locate(sh, store_ws, STORE_REQUIRED, "Store")
    sku_values, sku_name = _locate(sh, sku_ws, SKU_HEADERS, "SKU")
    stores, s_notes = parse_stores(store_values)
    skus, k_notes = parse_skus(sku_values)
    diag = [f"Worksheet Store: '{store_name}'", *s_notes, f"Worksheet SKU: '{sku_name}'", *k_notes]
    if not stores:
        raise MasterError("Master Store tidak berisi store Non BA.")
    if not skus:
        raise MasterError("Master SKU kosong.")
    return MasterData(stores=stores, skus=skus, diagnostics=diag)
