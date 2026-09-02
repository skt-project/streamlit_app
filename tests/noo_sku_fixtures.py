"""In-memory .xlsx builders that reproduce BD Support's real template layouts.

Kept faithful to the originals — including the instruction banner above the
header, the blank row in the SKU template, and the CONTOH example row — because
those are exactly the things a naive parser gets wrong.
"""
from __future__ import annotations

import io

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


class FakeSheetsClient:
    """Records appends instead of performing them."""

    def __init__(self, values=None):
        self._values = values or {}
        self.appended = []

    def read_values(self, tab, a1="A1:BZ"):
        return self._values.get(tab, [])

    def batch_read(self, ranges):
        return [[] for _ in ranges]

    def append_values(self, tab, rows):
        self.appended.append((tab, rows))
        return {"updates": {"updatedRows": len(rows)}}


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
