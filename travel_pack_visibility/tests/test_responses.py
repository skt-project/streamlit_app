import pytest

from conftest import FakeSpreadsheet, FakeWorksheet
from responses import (HEADER_SPEC, LAST_COL, OUTPUT_HEADERS, ResponseSheet, ResponseSheetError, build_index,
                       first_appended_row, hyperlink_formula, row_values)

REC = {"Timestamp": "2026-11-19 10:00:00", "Nama ASM": "Asep", "KODE ST TOKO": "ST001", "NAMA SPV": "=HACK()"}


def test_headers_follow_upload_log_layout():
    assert OUTPUT_HEADERS == ["Timestamp", "Nama ASM", "KODE ST TOKO", "NAMA TOKO", "Region", "NAMA SPV",
                              "Ach SKU (MOQ)", "Nama File", "GCS Path", "Status", "Waktu Foto (EXIF)",
                              "Lokasi Foto (EXIF)", "Submission ID"]
    assert LAST_COL == "M" and len(OUTPUT_HEADERS) == 13
    assert all(note.strip() and width >= 100 for _, width, note in HEADER_SPEC)


def test_creates_tab_with_header_and_appends_raw():
    sh = FakeSpreadsheet()
    rs = ResponseSheet(lambda: sh, "TRAVEL_PACK_UPLOAD_LOG")
    rs.append(REC)
    ws = sh.sheets["TRAVEL_PACK_UPLOAD_LOG"]
    assert ws.values[0] == OUTPUT_HEADERS
    assert ws.values[1][:3] == ["2026-11-19 10:00:00", "Asep", "ST001"]
    assert ws.values[1][OUTPUT_HEADERS.index("NAMA SPV")] == "=HACK()"   # teks (RAW), bukan formula
    assert ("append", "RAW", "INSERT_ROWS", "A1:M1") in ws.calls


def test_new_tab_gets_grey_bold_frozen_header_with_notes_and_widths():
    sh = FakeSpreadsheet()
    ResponseSheet(sh, "TRAVEL_PACK_UPLOAD_LOG").append(REC)
    ws = sh.sheets["TRAVEL_PACK_UPLOAD_LOG"]
    reqs = sh.batch_requests
    fmt = next(r["repeatCell"] for r in reqs if "repeatCell" in r)
    assert fmt["range"]["sheetId"] == ws.id and fmt["range"]["endRowIndex"] == 1
    assert fmt["cell"]["userEnteredFormat"]["textFormat"]["bold"] is True
    assert fmt["cell"]["userEnteredFormat"]["backgroundColor"]["red"] < 1
    frozen = next(r["updateSheetProperties"] for r in reqs if "updateSheetProperties" in r)
    assert frozen["properties"]["gridProperties"]["frozenRowCount"] == 1
    notes = next(r["updateCells"] for r in reqs if "updateCells" in r)
    assert [c["note"] for c in notes["rows"][0]["values"]] == [n for _, _, n in HEADER_SPEC]
    assert notes["fields"] == "note"
    widths = [r["updateDimensionProperties"]["properties"]["pixelSize"] for r in reqs
              if "updateDimensionProperties" in r]
    assert widths == [w for _, w, _ in HEADER_SPEC]


def test_format_failure_still_writes_and_freezes():
    sh = FakeSpreadsheet()
    sh.fail_format = True
    ResponseSheet(sh, "TRAVEL_PACK_UPLOAD_LOG").append(REC)
    ws = sh.sheets["TRAVEL_PACK_UPLOAD_LOG"]
    assert ws.frozen == 1 and len(ws.values) == 2


def test_existing_header_is_not_reformatted():
    ws = FakeWorksheet("TRAVEL_PACK_UPLOAD_LOG", [OUTPUT_HEADERS])
    sh = FakeSpreadsheet([ws])
    ResponseSheet(sh, "TRAVEL_PACK_UPLOAD_LOG").append(REC)
    assert sh.batch_requests == [] and len(ws.values) == 2


def test_gcs_path_becomes_clickable_link_in_new_row():
    ws = FakeWorksheet("TRAVEL_PACK_UPLOAD_LOG", [OUTPUT_HEADERS, row_values({"KODE ST TOKO": "ST009"})])
    rec = dict(REC, **{"GCS Path": "gs://bkt/Travel Pack/Asep/ST001 - Toko \"A\".jpg"})
    ResponseSheet(FakeSpreadsheet([ws]), "TRAVEL_PACK_UPLOAD_LOG").append(rec)
    col = OUTPUT_HEADERS.index("GCS Path")
    assert ("update", "I3", "USER_ENTERED") in ws.calls
    assert ws.values[2][col] == ('=HYPERLINK("https://storage.cloud.google.com/bkt/Travel%20Pack/Asep/'
                                 'ST001%20-%20Toko%20%22A%22.jpg","gs://bkt/Travel Pack/Asep/ST001 - Toko ""A"".jpg")')
    assert ws.values[1][col] == ""                # baris lain tidak disentuh


def test_link_failure_keeps_plain_gs_path():
    ws = FakeWorksheet("TRAVEL_PACK_UPLOAD_LOG", [OUTPUT_HEADERS])
    ws.fail_link = True
    ResponseSheet(FakeSpreadsheet([ws]), "TRAVEL_PACK_UPLOAD_LOG").append(dict(REC, **{"GCS Path": "gs://b/x.jpg"}))
    assert ws.values[1][OUTPUT_HEADERS.index("GCS Path")] == "gs://b/x.jpg"


@pytest.mark.parametrize("value", ["", "https://x/y", "gs://", "gs://bucket", "gs://bucket/"])
def test_hyperlink_formula_ignores_non_gcs(value):
    assert hyperlink_formula(value) is None


def test_first_appended_row():
    assert first_appended_row({"updates": {"updatedRange": "'Log'!A7:M7"}}) == 7
    assert first_appended_row({}) is None and first_appended_row(None) is None


def test_empty_tab_gets_header():
    ws = FakeWorksheet("TRAVEL_PACK_UPLOAD_LOG", [])
    rs = ResponseSheet(FakeSpreadsheet([ws]), "TRAVEL_PACK_UPLOAD_LOG")
    rs.append(REC)
    assert ws.values[0] == OUTPUT_HEADERS and len(ws.values) == 2


def test_extra_admin_columns_to_the_right_allowed():
    ws = FakeWorksheet("TRAVEL_PACK_UPLOAD_LOG", [OUTPUT_HEADERS + ["Cek Admin"]])
    ResponseSheet(FakeSpreadsheet([ws]), "TRAVEL_PACK_UPLOAD_LOG").append(REC)
    assert len(ws.values) == 2


def test_changed_header_refuses_to_write():
    bad = list(OUTPUT_HEADERS)
    bad[2] = "Kode Toko"
    ws = FakeWorksheet("TRAVEL_PACK_UPLOAD_LOG", [bad])
    with pytest.raises(ResponseSheetError, match="Header"):
        ResponseSheet(FakeSpreadsheet([ws]), "TRAVEL_PACK_UPLOAD_LOG").append(REC)
    assert len(ws.values) == 1


def test_open_failure_is_explained():
    def boom():
        raise PermissionError("no")
    with pytest.raises(ResponseSheetError, match="Editor"):
        ResponseSheet(boom, "X").append(REC)


def test_upload_index_counts_and_latest():
    rows = [OUTPUT_HEADERS,
            row_values({"Timestamp": "2026-11-19 09:00:00", "KODE ST TOKO": "st001"}),
            row_values({"Timestamp": "2026-11-19 11:00:00", "KODE ST TOKO": "ST001"}),
            row_values({"Timestamp": "2026-11-18 08:00:00", "KODE ST TOKO": "ST003"}),
            ["", "", ""]]
    idx = build_index(rows)
    assert idx["ST001"].count == 2 and idx["ST001"].last_at == "2026-11-19 11:00:00"
    assert idx["ST003"].count == 1 and set(idx) == {"ST001", "ST003"}


def test_upload_index_missing_tab_is_empty_and_not_created():
    sh = FakeSpreadsheet()
    assert ResponseSheet(sh, "TRAVEL_PACK_UPLOAD_LOG").upload_index() == {}
    assert sh.sheets == {}
