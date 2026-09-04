"""In-memory .xlsx builders that reproduce BD Support's real template layouts.

Kept faithful to the originals — including the instruction banner above the
header, the blank row in the SKU template, and the CONTOH example row — because
those are exactly the things a naive parser gets wrong.
"""
from __future__ import annotations

import io
import re

import openpyxl

from noo_sku import config

NOO_BANNER = ("1. Semua kolom WAJIB diisi sesuai ketentuan.\n"
              "2. Dimohon untuk TIDAK MENGUBAH URUTAN TEMPLATE")
NOO_EXAMPLE = ["CONTOH", "TOKO JAYA KOSMETIK", "GT", "PT Anugerah Bangun Abadi",
               "11ABA", "DST123", "DST12300010", "JAKARTA BARAT",
               "RUKO BOULEVARD TAMAN PALEM LESTARI NO 1", "Cosmetic Store"]
SKU_EXAMPLE = ["TYY114002", "TIMEPHORIA NAVI EYESHADOW PALETTE 001 ABYSS",
               "A1/010424142D", "T114002 Navi Eyeshadow 002 Siren"]


def _save(wb) -> io.BytesIO:
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


def noo_workbook(rows, *, headers=None, sheet_name=None) -> io.BytesIO:
    """NOO template: banner on row 1, header row 2, CONTOH row 3, data row 4+."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_name or config.NOO_SHEET_NAME
    ws.append([NOO_BANNER])
    ws.append(list(headers or config.NOO_COLUMNS))
    ws.append(list(NOO_EXAMPLE))
    for row in rows:
        ws.append(list(row))
    return _save(wb)


def sku_workbook(rows, *, headers=None, sheet_name=None) -> io.BytesIO:
    """SKU template: banner row 1, blank row 2, header row 3, CONTOH rows 4-5."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_name or config.SKU_SHEET_NAME
    ws.append(["1. Semua kolom WAJIB diisi sesuai ketentuan."])
    ws.append([])
    ws.append(list(headers or config.SKU_COLUMNS))
    ws.append(["CONTOH"])
    ws.append(list(SKU_EXAMPLE))
    for row in rows:
        ws.append(list(row))
    return _save(wb)


def noo_row(store_id="", name="TOKO SUMBER REJEKI", channel="GT",
            branch="CV CECE", customer_code="11CEC", branch_code="DST082",
            store_code="DST08200011", city="Banggai",
            address="JL. MERDEKA NO 10, BANGGAI", store_type="Cosmetic Store"):
    return [store_id, name, channel, branch, customer_code, branch_code,
            store_code, city, address, store_type]


def sku_row(code="SKINTIFIC-296", name="SKINTIFIC TEST PRODUCT",
            db_code="SKC-296", db_name="S296 SKINTIFIC TEST"):
    """MoM 31-Aug-2026 removed the gramasi/size column from the template."""
    return [code, name, db_code, db_name]


PRODUCTS = {
    "SKINTIFIC-296": {"brand": "SKINTIFIC",
                      "product_name": "SKINTIFIC TEST PRODUCT",
                      "pack_size": "30ml"},
    "TCC102001": {"brand": "TIMEPHORIA", "product_name": "TIMEPHORIA TEST",
                  "pack_size": "8g"},
    "F116": {"brand": "FACERINNA", "product_name": "FACERINNA TEST",
             "pack_size": "50ml"},
    "G2G-74": {"brand": "G2G", "product_name": "GLAD2GLOW TEST",
               "pack_size": "300g"},
}


def _col_index(letters: str) -> int:
    """Inverse of writer._col_letter: 'A' -> 0, 'E' -> 4, 'AA' -> 26."""
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch) - 64)
    return n - 1


_A1_RANGE = re.compile(r"^([A-Z]+)(\d+):([A-Z]+)(\d+)$")


def _parse_a1(a1: str):
    """Parse the fully-bounded 'E2:W20000' form used everywhere in this
    codebase into 0-based (start_col, start_row, end_col, end_row), all
    inclusive. Row 0 is the sheet's own row 1 (the header)."""
    m = _A1_RANGE.match(a1)
    if not m:
        raise ValueError(
            f"FakeSheetsClient only supports fully-bounded A1 ranges like "
            f"'E2:W20000', got {a1!r}")
    start_col, start_row, end_col, end_row = m.groups()
    return (_col_index(start_col), int(start_row) - 1,
           _col_index(end_col), int(end_row) - 1)


class FakeSheetsClient:
    """In-memory pool tab that genuinely respects the A1 range it is given,
    for both reads and writes — not just records the call — so tests can
    verify that a formula-bearing column sitting outside the written range is
    left untouched, and that the row writer.append_rows computes as the
    target is the one that actually gets written.
    """

    def __init__(self, values=None):
        self._values = values or {}
        self.written = []  # (tab, rows, a1) exactly as sent to update_range

    def read_values(self, tab, a1="A1:BZ"):
        data = self._values.get(tab, [])
        start_col, start_row, end_col, end_row = _parse_a1(a1)
        return [row[start_col:end_col + 1]
               for row in data[start_row:end_row + 1]]

    def batch_read(self, ranges):
        return [[] for _ in ranges]

    def update_range(self, tab, a1, rows):
        """Writes to EXACTLY the cells named by `a1` — mirrors values.update,
        which has no table-detection step of its own. The caller (writer.py)
        is the one that decides which row to target."""
        self.written.append((tab, rows, a1))
        start_col, start_row, end_col, _ = _parse_a1(a1)
        data = self._values.setdefault(tab, [[]])
        for offset, new_row in enumerate(rows):
            absolute_row = start_row + offset
            while absolute_row >= len(data):
                data.append([])
            row = data[absolute_row]
            if len(row) < end_col + 1:
                row.extend([""] * (end_col + 1 - len(row)))
            for i, value in enumerate(new_row):
                row[start_col + i] = value
        return {"updatedRows": len(rows)}


# ─── Master-data fixtures for the enrichment layer ───────────────────────────
MASTER_DISTRIBUTOR = {
    "DST082": {
        "distributor_code": "DST082", "distributor": "CV CECE",
        "distributor_company": "CV CECE MANDIRI SEJAHTERA",
        "region": "Northern Sulawesi", "region_g2g": "Sulawesi 2",
        "asm": "Ainur Rochman Fawzi", "aom": "M Nur Alim", "pm": "PM Name",
        "asm_skt": "Ainur Rochman Fawzi", "asm_tph": "ASM TPH",
        "asm_fr": "",                      # FACERINNA often blank in real data
        "spv_skt": "Voldy Kendes", "spv_tph": "SPV TPH", "spv_fr": "",
        "aom_skt": "M Nur Alim", "aom_tph": "AOM TPH", "aom_fr": "",
        "area_coverage": "Banggai", "province": "Sulawesi Tengah",
        "city": "Luwuk", "status": "Active",
    },
}

_BASIS_ROW = {
    "cust_id": "IESL00038", "store_name": "TOKO WILDA",
    "address": "JL MERDEKA NO 10 BANGGAI",
    "city": "Banggai", "province": "Sulawesi Tengah",
    "area_coverage": "BANGGAI", "customer_type": "Cosmetic Store",
    "customer_category": "GT",
    "se_skt": "Mohammad Fikram Dam", "se_tph": "SE TPH", "se_fcr": "SE FCR",
    "spv_skt": "Voldy Kendes", "spv_tph": "SPV TPH",
    "aom_skt": "M Nur Alim", "aom_tph": "AOM TPH",
    "reference_id_skt": "DST08200074", "reference_id_tph": "DST08200074",
}
BASIS_BY_CUST = {"IESL00038": _BASIS_ROW}
BASIS_BY_REF = {
    "skt": {"DST08200074": [_BASIS_ROW],
            # a reference id that resolves to two stores -> must stay ambiguous
            "DST08200099": [_BASIS_ROW, dict(_BASIS_ROW, cust_id="IESL00099")]},
    "tph": {"DST08200074": [_BASIS_ROW]},
    "fcr": {"DST08200074": [_BASIS_ROW]},
}


class FakeParsed:
    """Stands in for parsers.ParsedFile without touching a workbook."""

    def __init__(self, rows, columns, first_row=4):
        self.rows = [dict(zip(columns, r)) for r in rows]
        self.row_numbers = list(range(first_row, first_row + len(rows)))
        self.kind = None
        self.headers = list(columns)
        self.header_row = first_row - 1


DISTRIBUTOR = {"distributor_code": "DST082", "distributor_name": "CV CECE",
               "region": "Northern Sulawesi", "status": "Active",
               "active": True, "branch_code": "CEC"}
