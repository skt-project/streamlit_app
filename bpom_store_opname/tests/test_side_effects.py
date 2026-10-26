"""Kontrak efek samping aplikasi (dikunci oleh tes ini):

1. BigQuery: HANYA satu SELECT berparameter ke master_product (baca saja).
2. Google Sheets/Drive: pada saat submit satu-satunya perubahan di spreadsheet output adalah MENAMBAH baris (appendCells). Selain itu,
   hanya persiapan sekali-jalan di dalam spreadsheet output itu sendiri (membuat tab, melebarkan kolom, menulis header di tab kosong).
3. Tidak ada pembuatan/share/pencarian file lewat Drive, dan scope token hanya Sheets (BigQuery: scope bigquery untuk query).
"""
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock

import gspread
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import app  # noqa: E402
import sheets_store as store  # noqa: E402
import validation as v  # noqa: E402

SOURCES = ("app.py", "sheets_store.py", "master_product.py", "reference_data.py", "validation.py", "template_builder.py")


def src(name):
    return (ROOT / name).read_text(encoding="utf-8")


# ── 1. BigQuery: baca saja ──────────────────────────────────────────────────

def test_bigquery_is_called_in_exactly_one_place_with_a_single_select():
    hits = [(n, m.group(0)) for n in SOURCES for m in re.finditer(r"\bclient\.query\(|\.query_and_wait\(|\bclient\.insert_rows|\.load_table|\.create_table|\.delete_table|\.update_table|\.extract_table|\.copy_table", src(n))]
    assert hits == [("master_product.py", "client.query(")]
    call = src("master_product.py").split("client.query(", 1)[1].split("job_config", 1)[0]
    sql = " ".join(re.findall(r'f?"([^"]*)"', call))
    assert sql.lstrip().upper().startswith("SELECT ") and ";" not in sql
    assert not re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|CREATE|ALTER|TRUNCATE|EXPORT|CALL|GRANT|REVOKE)\b", sql.upper())
    assert "@brands" in sql and "@exclude_status" in sql and "SKINTIFIC" not in sql and "DISCONTINU" not in sql.upper()   # nilai lewat parameter


# ── 2/3. Sheets/Drive: hanya menambah baris ─────────────────────────────────

def test_no_drive_or_file_level_calls_anywhere_and_sheets_scope_only():
    forbidden = r"\.create\(|\.share\(|\.update_title\(|\bgc\.open\(|list_spreadsheet_files|del_spreadsheet|\.delete_worksheet\(|" \
                r"\bws\.append_row|\bws\.insert_row|\bws\.delete_rows|\.batch_clear|\.resize\(|import_csv|\.copy\(\s*(?:\w+,\s*)?title"
    for name in ("app.py", "sheets_store.py", "master_product.py", "reference_data.py"):
        assert not re.search(forbidden, src(name)), name
    assert app.SCOPES == ["https://www.googleapis.com/auth/spreadsheets"]


def test_the_only_sheet_mutations_in_the_code_are_the_expected_ones():
    code = src("sheets_store.py")
    counts = {name: len(re.findall(pattern, code)) for name, pattern in {
        "add_worksheet": r"\.add_worksheet\(", "add_cols": r"\.add_cols\(", "header update": r"\bws\.update\(\[HEADERS\]",
        "freeze": r"\.freeze\(", "batch_update": r"\.sh\.batch_update\("}.items()}
    assert counts == {"add_worksheet": 1, "add_cols": 1, "header update": 1, "freeze": 1, "batch_update": 1}
    for other in ("app.py", "master_product.py", "reference_data.py"):
        assert not re.search(r"\.(batch_update|add_worksheet|add_cols|freeze|update)\(\s*\[", src(other)) and ".batch_update(" not in src(other)


def _direct(client):
    """Nama pemanggilan langsung pada client (tanpa pemanggilan bertingkat pada objek turunannya)."""
    return [c[0] for c in client.mock_calls if "." not in c[0] and "(" not in c[0]]


def _row(i=3):
    return v.ExcelRow(i, "TOKO A", "P-1", "Produk", "10g", "NA1", 5)


def test_submit_on_a_ready_tab_only_appends_rows_and_never_writes_anything_else():
    gc = MagicMock()
    sh = gc.open_by_key.return_value
    ws = sh.worksheet.return_value
    ws.id, ws.title, ws.col_count = 7, "W", 14
    ws.row_values.return_value = list(store.HEADERS)                       # header sudah benar
    ws.batch_get.return_value = [[] for _ in store._KEY_COLUMNS]
    opened = store.open_spreadsheet(gc, "SHEET_ID")
    tab = store.ensure_worksheet(opened, "W")
    opname = store.OpnameStore(opened, tab, pacer=store._Pacer(0))
    opname.insert_rows(store.OpnameMeta("Jawa", "TOKO A", "2026-10-26", "Budi"), [_row(), _row(4)])
    # satu-satunya pemanggilan tulis di spreadsheet: batch_update berisi hanya appendCells
    sh.batch_update.assert_called_once()
    requests = sh.batch_update.call_args[0][0]["requests"]
    assert [list(r) for r in requests] == [["appendCells"]]
    assert requests[0]["appendCells"]["fields"] == "userEnteredValue" and len(requests[0]["appendCells"]["rows"]) == 2
    # tidak ada tulis lain di tab / spreadsheet / client
    for forbidden in ("update", "add_cols", "freeze", "clear", "delete_rows", "append_row", "resize", "update_title"):
        assert not getattr(ws, forbidden).called, forbidden
    assert not sh.add_worksheet.called and not sh.share.called
    assert _direct(gc) == ["open_by_key"]                                  # client hanya membuka lewat ID: tanpa create/open(title)/list


def test_first_use_prepares_the_output_spreadsheet_only_inside_itself():
    gc = MagicMock()
    sh = gc.open_by_key.return_value
    sh.worksheet.side_effect = gspread.WorksheetNotFound("x")
    new_ws = sh.add_worksheet.return_value
    new_ws.row_values.return_value = []
    new_ws.col_count = 10                                                   # tab sempit
    store.ensure_worksheet(store.open_spreadsheet(gc, "SHEET_ID"), "Tab Baru")
    sh.add_worksheet.assert_called_once()
    new_ws.add_cols.assert_called_once_with(len(store.HEADERS) - 10)
    new_ws.update.assert_called_once()
    assert new_ws.update.call_args[0][0] == [store.HEADERS]                # header saja
    new_ws.freeze.assert_called_once_with(rows=1)
    assert _direct(gc) == ["open_by_key"] and not sh.share.called


def test_a_different_header_is_never_overwritten_and_a_missing_sheet_id_never_creates_anything():
    ws = MagicMock(); ws.title = "W"; ws.row_values.return_value = ["bukan", "header", "kami"]
    sh = MagicMock(); sh.worksheet.return_value = ws
    with pytest.raises(store.SheetsError):
        store.ensure_worksheet(sh, "W")
    ws.update.assert_not_called()
    gc = MagicMock()
    for empty in (None, ""):
        with pytest.raises(store.SheetsError, match="google_sheet_id"):
            store.open_spreadsheet(gc, empty)
    assert gc.mock_calls == []                                              # tanpa id: tidak ada panggilan ke Google sama sekali
