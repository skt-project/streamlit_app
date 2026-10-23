"""Tes validasi, template, dan penyimpanan: python -m pytest tests"""
import sys
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import reference_data as r  # noqa: E402
import sheets_store as store  # noqa: E402
import validation as v  # noqa: E402
from template_builder import build_template  # noqa: E402

NAME = "CV ANDIRA DAHAYU - BENGKULU"
NAME2 = "CV ANDIRA DAHAYU - CURUP"
TEMPLATE = ROOT / "template" / "Contoh Excel Response.xlsx"
DIST_FILE = ROOT / "reference" / "Daftar Distributor.xlsx"


def make(rows, headers=v.EXPECTED_HEADERS, title=True):
    wb = openpyxl.Workbook()
    ws = wb.active
    if title:
        ws.append([None] * (len(v.EXPECTED_HEADERS) - 1) + ["Filled by Distributor"])
    ws.append(list(headers))
    for row in rows:
        ws.append(row)
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


def good(qty=5, pid="P-1", bpom="NA1", name=NAME, isi="10g"):
    return [name, pid, "Produk", isi, bpom, qty]


def messages(res):
    return " | ".join(i.message for i in res.issues)


def catalogue():
    return r.load_catalogue(TEMPLATE)


# ── parsing & aturan kolom ──────────────────────────────────────────────────

def test_valid_file_with_title_row():
    res = v.parse_excel(make([good(), good(qty=0, bpom="NA2"), good(qty=3, bpom="NA3")]), [NAME])
    assert res.ok and [x.qty for x in res.rows] == [5, 0, 3]
    assert res.rows[0].excel_row == 3 and res.rows[0].isi == "10g"


def test_name_match_ignores_case_and_spacing():
    assert v.parse_excel(make([good(name="cv andira dahayu-bengkulu")]), [NAME]).ok


def test_header_exact_case_sensitive_and_new_name():
    h = list(v.EXPECTED_HEADERS); h[5] = "Qty"
    res = v.parse_excel(make([good()], headers=h), [NAME])
    assert not res.ok and "persis" in messages(res)
    old = ["Distributor Name (Company - Region)", *v.EXPECTED_HEADERS[1:]]      # header lama ditolak
    assert not v.parse_excel(make([good()], headers=old), [NAME]).ok
    assert v.EXPECTED_HEADERS[0] == "Distributor Branch"


def test_extra_missing_and_reordered_columns():
    assert not v.parse_excel(make([good() + ["x"]], headers=[*v.EXPECTED_HEADERS, "EXTRA"]), [NAME]).ok
    assert not v.parse_excel(make([good()[:5]], headers=v.EXPECTED_HEADERS[:5]), [NAME]).ok
    swapped = [v.EXPECTED_HEADERS[1], v.EXPECTED_HEADERS[0], *v.EXPECTED_HEADERS[2:]]
    assert "Urutan" in messages(v.parse_excel(make([good()], headers=swapped), [NAME]))


def test_qty_rules_whole_non_negative():
    for bad in (None, "", 1.5, "abc", "1,5", True, -1, -12.0, "-12", -2**40):
        assert v.parse_qty(bad)[0] is None, bad
    assert "negatif" in v.parse_qty(-3)[1]
    assert v.parse_qty(7.0) == (7, None)
    assert v.parse_qty("12") == (12, None)
    assert v.parse_qty(0) == (0, None)
    assert v.parse_qty("-0") == (0, None)


def test_negative_qty_row_is_rejected_with_row_number():
    res = v.parse_excel(make([good(), good(qty=-3, bpom="NA2")]), [NAME])
    assert not res.ok and [(i.row, i.column) for i in res.issues] == [(4, "QTY")] and "negatif" in messages(res)


def test_all_or_nothing_and_row_numbers():
    res = v.parse_excel(make([good(), good(qty=None, bpom="NA2"), good(bpom="NA3")]), [NAME])
    assert not res.ok and [i.row for i in res.issues] == [4]


def test_duplicate_key_within_file():
    res = v.parse_excel(make([good(), good(pid="p-1", bpom="na1")]), [NAME])
    assert not res.ok and "Duplikat dengan baris 3" in messages(res)


def test_required_text_fields_including_isi():
    res = v.parse_excel(make([["CV LAIN - X", "", "N", "", "B", 1]]), [NAME])
    cols = {i.column for i in res.issues}
    assert {v.EXPECTED_HEADERS[0], "PRODUCT ID", v.COL_ISI} <= cols


def test_blank_rows_skipped_empty_and_non_xlsx_rejected():
    assert v.parse_excel(make([good(), [None] * 6, [None] * 6]), [NAME]).ok
    assert not v.parse_excel(make([]), [NAME]).ok
    assert not v.parse_excel(b"bukan excel", [NAME]).ok


def test_form_validation():
    assert v.validate_form(company="A", tanggal="d", pic="x", has_file=True) == []
    assert len(v.validate_form(company=None, tanggal=None, pic="", has_file=False)) == 4


# ── aturan SKU / BPOM tambahan / multi distributor ──────────────────────────

SKUS = [v.Sku("P-1", "Produk", "NA1", "10g"), v.Sku("P-1", "Produk", "NA2", "10g"), v.Sku("P-2", "Lain", "NB1", "5ml")]


def test_sku_rules_known_additional_and_missing():
    rows = [good(), good(bpom="NA2"), good(bpom="NA-BARU", qty=3)]
    res = v.parse_excel(make(rows), [NAME], SKUS)
    assert res.ok and [x.additional for x in res.rows] == [False, False, True]
    assert [k.product_id for _, k in res.missing] == ["P-2"]


def test_unknown_sku_name_and_isi_mismatch_rejected():
    res = v.parse_excel(make([good(pid="P-9")]), [NAME], SKUS)
    assert not res.ok and any(i.column == "PRODUCT ID" for i in res.issues)
    bad_name = good(); bad_name[2] = "Nama Lain"
    assert any(i.column == "PRODUCT NAME" for i in v.parse_excel(make([bad_name]), [NAME], SKUS).issues)
    res = v.parse_excel(make([good(isi="99ml")]), [NAME], SKUS)
    assert not res.ok and any(i.column == v.COL_ISI for i in res.issues)
    assert v.parse_excel(make([good(isi="10G")]), [NAME], SKUS).ok      # beda huruf besar/kecil tidak masalah


def test_distributor_must_belong_to_company_and_same_sku_allowed_per_distributor():
    res = v.parse_excel(make([good(name="CV LAIN - X")]), [NAME, NAME2])
    assert not res.ok and "bukan distributor milik company" in messages(res)
    assert v.parse_excel(make([good(), good(name=NAME2, qty=2)]), [NAME, NAME2]).ok
    rows = [good(), good(name=NAME2, qty=2), good(name=NAME2.lower(), pid="p-1", bpom="na1", qty=3)]
    assert "Duplikat" in messages(v.parse_excel(make(rows), [NAME, NAME2]))


def test_missing_is_reported_per_distributor():
    res = v.parse_excel(make([good()]), [NAME, NAME2], [v.Sku("P-1", "Produk", "NA1", "10g")])
    assert res.ok and [(n, k.bpom_number) for n, k in res.missing] == [(NAME2, "NA1")]


# ── data referensi & template ───────────────────────────────────────────────

def test_reference_data_loads():
    assert len(r.load_distributors(DIST_FILE)) == 94
    skus = catalogue()
    assert len(skus) == 66 and all(s.isi and s.product_name for s in skus)


def test_template_has_a_block_per_distributor_and_only_guide_extra_sheet():
    skus = catalogue()
    raw = TEMPLATE.read_bytes()
    out = build_template(raw, "CV ANDIRA DAHAYU", [NAME, NAME2], skus)
    assert raw == TEMPLATE.read_bytes()                         # template asli tidak berubah
    wb = openpyxl.load_workbook(BytesIO(out))
    assert wb.sheetnames == ["Sheet1", v.INSTRUCTION_SHEET]      # sheet kerja BD tidak ikut
    assert wb.active.title == "Sheet1"
    ws = wb["Sheet1"]
    assert [c.value for c in ws[2]] == list(v.EXPECTED_HEADERS) and ws["F1"].value == "Filled by Distributor"
    rows = [[c.value for c in row] for row in ws.iter_rows(min_row=3)]
    assert rows == [[n, k.product_id, k.product_name, k.isi, k.bpom_number, None] for n in (NAME, NAME2) for k in skus]
    (dv,) = ws.data_validations.dataValidation                  # QTY: bilangan bulat >= 0 di Excel
    assert (dv.type, dv.operator, dv.formula1) == ("whole", "greaterThanOrEqual", "0")
    assert str(dv.sqref) == f"F3:F{ws.max_row}"
    guide = " ".join(str(c.value) for c in wb[v.INSTRUCTION_SHEET]["A"] if c.value)
    assert NAME in guide and NAME2 in guide and "ISI (GR / ML)" in guide and "BPOM tambahan" in guide


def test_filled_multi_distributor_template_passes_and_ignores_guide_sheet():
    skus = catalogue()
    out = build_template(TEMPLATE.read_bytes(), "CO", [NAME, NAME2], skus)
    wb = openpyxl.load_workbook(BytesIO(out)); ws = wb["Sheet1"]
    for i in range(3, ws.max_row + 1):
        ws.cell(i, 6).value = i
    buf = BytesIO(); wb.save(buf)
    res = v.parse_excel(buf.getvalue(), [NAME, NAME2], skus)
    assert res.ok and len(res.rows) == 2 * len(skus) and not res.missing and res.warnings == []
    assert {x.distributor_name for x in res.rows} == {NAME, NAME2}


# ── Google Sheets (mock) ────────────────────────────────────────────────────

def _ws(values):
    ws = MagicMock(); ws.id = 1; ws.title = "W"
    ws.get_all_values.return_value = values
    return ws


def test_insert_writes_14_columns_with_isi_and_period_from_date():
    sh = MagicMock()
    meta = store.OpnameMeta("C", "2026-10-25", "Budi")
    row = v.ExcelRow(3, NAME, "P-1", "Produk", "10g", "NA1", 5, additional=True)
    summary = store.insert_rows(sh, _ws([store.HEADERS]), meta, [row])
    cells = sh.batch_update.call_args[0][0]["requests"][0]["appendCells"]["rows"][0]["values"]
    vals = [c["userEnteredValue"] for c in cells]
    assert len(vals) == len(store.HEADERS) == 14
    assert vals[2] == {"stringValue": "2026-10"} and vals[3] == {"stringValue": "2026-10-25"}
    assert vals[8] == {"numberValue": 5} and vals[13] == {"stringValue": "10g"}
    assert (summary.inserted, summary.additional, summary.distributors) == (1, 1, 1)


def test_duplicates_block_input_same_period_only():
    existing = [store.HEADERS, ["C", NAME, "2026-10", "d", "p", "P-1", "Produk", "NA1", "3", "c", "c", "c", "c", "10g"]]
    sh = MagicMock()
    row = v.ExcelRow(3, NAME, "p-1", "Produk", "10g", "na1", 5)
    try:
        store.insert_rows(sh, _ws(existing), store.OpnameMeta("C", "2026-10-26", "x"), [row])
        raise AssertionError("harus ditolak")
    except store.DuplicateDataError as exc:
        assert len(exc.conflicts) == 1 and not sh.batch_update.called
    assert store.insert_rows(sh, _ws(existing), store.OpnameMeta("C", "2026-11-02", "x"), [row]).inserted == 1


def test_legacy_13_column_header_is_upgraded_not_overwritten():
    ws = MagicMock(); ws.row_values.return_value = list(store._LEGACY_HEADERS); ws.col_count = 13
    sh = MagicMock(); sh.worksheet.return_value = ws
    store.ensure_worksheet(sh, "W")
    ws.add_cols.assert_called_once_with(1)                      # grid 13 kolom diperlebar dulu
    ws.update_cell.assert_called_once_with(1, 14, "ISI (GR / ML)")
    ws.update.assert_not_called()
    ws2 = MagicMock(); ws2.row_values.return_value = ["header", "lain"]; ws2.title = "W"
    sh.worksheet.return_value = ws2
    try:
        store.ensure_worksheet(sh, "W")
        raise AssertionError("harus ditolak")
    except store.SheetsError:
        pass
