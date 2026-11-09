"""SheetsLogStore against a fake gspread worksheet (no Google calls): RAW data,
clickable GCS Path, extra admin columns untouched."""
import re

import pytest
from gspread.utils import a1_to_rowcol

from src.sheets import (
    LOG_HEADERS,
    SheetsLogStore,
    app_column_runs,
    first_appended_row,
    hyperlink_formula,
    logs_to_frame,
)
from src.storage import browser_url

LOCATION = "gs://skp-bucket/2026 Q4/jbp/East Java/SKP - TOKO (A) - 9784_SKT.pdf"
URL = "https://storage.cloud.google.com/skp-bucket/2026%20Q4/jbp/East%20Java/SKP%20-%20TOKO%20%28A%29%20-%209784_SKT.pdf"


class FakeWorksheet:
    """Keeps a 2D grid of what the Sheets UI would display."""

    def __init__(self, values, fail_user_entered=False):
        self.values = [list(r) for r in values]
        self.calls = []
        self.formulas = {}
        self.fail_user_entered = fail_user_entered

    def get_all_values(self):
        return [list(r) for r in self.values]

    def _set(self, a1, rows, displayed):
        r0, c0 = a1_to_rowcol(a1.split(":")[0])
        for i, row in enumerate(rows):
            while len(self.values) < r0 + i:
                self.values.append([])
            line = self.values[r0 + i - 1]
            for j, v in enumerate(row):
                while len(line) < c0 + j:
                    line.append("")
                line[c0 + j - 1] = displayed(v)

    def batch_update(self, data, value_input_option=None):
        self.calls.append(("batch_update", value_input_option, data))
        if value_input_option == "USER_ENTERED" and self.fail_user_entered:
            raise RuntimeError("quota")
        for item in data:
            def displayed(v):
                m = re.fullmatch(r'=HYPERLINK\("(.*)","(.*)"\)', v) if value_input_option == "USER_ENTERED" else None
                if m:
                    self.formulas[item["range"]] = v
                    return m.group(2).replace('""', '"')
                return v
            self._set(item["range"], item["values"], displayed)

    def append_rows(self, rows, value_input_option=None, insert_data_option=None, table_range=None):
        self.calls.append(("append_rows", value_input_option, rows))
        start = len([r for r in self.values if any(r)]) + 1
        self._set(f"A{start}", rows, lambda v: v)
        return {"updates": {"updatedRange": f"'SKP_UPLOAD_LOG'!A{start}:M{start + len(rows) - 1}"}}

    def freeze(self, rows=None):
        pass


class FakeSpreadsheet:
    def __init__(self, ws):
        self.ws = ws

    def worksheet(self, title):
        return self.ws


def record(pic="Budi", location=LOCATION, quarter="2026 Q4"):
    return {"Timestamp": "2026-11-09 10:00:00", "PIC Submit": pic, "Kode Toko/Group": "T100",
            "Nama Toko/Group": "TOKO (A)", "Region": "East Java", "Brand": "SKINTIFIC", "Jenis Program": "JBP",
            "Kuartal": quarter, "Nomor SKP": "9784/SKT", "Nama File": "f.pdf", "GCS Path": location, "Uploader": pic}


def test_browser_url_encodes_object_and_ignores_non_gs():
    assert browser_url(LOCATION) == URL
    assert browser_url("local://2026 Q4/jbp/x.pdf") is None
    assert browser_url("2026 Q4/jbp/x.pdf") is None
    assert browser_url("gs://only-bucket") is None


def test_hyperlink_formula_shows_gs_text_and_escapes_quotes():
    assert hyperlink_formula(LOCATION) == f'=HYPERLINK("{URL}","{LOCATION}")'
    assert '""' in hyperlink_formula('gs://b/a"b.pdf')
    assert hyperlink_formula("local://x.pdf") is None


def test_first_upload_appends_raw_then_links_gcs_path():
    ws = FakeWorksheet([LOG_HEADERS])
    SheetsLogStore(FakeSpreadsheet(ws), "SKP_UPLOAD_LOG").upsert([record(pic='=HYPERLINK("evil")')])
    kinds = [(c[0], c[1]) for c in ws.calls]
    assert kinds == [("append_rows", "RAW"), ("batch_update", "USER_ENTERED")]
    link_call = ws.calls[-1][2]
    assert link_call == [{"range": "K2", "values": [[f'=HYPERLINK("{URL}","{LOCATION}")']]}]
    df = logs_to_frame(ws.get_all_values())  # what the app reads back = displayed text
    assert df.iloc[0]["GCS Path"] == LOCATION
    assert df.iloc[0]["PIC Submit"] == '=HYPERLINK("evil")'  # user text stays text (written RAW)


def test_reupload_updates_only_app_columns_and_relinks():
    header = LOG_HEADERS + ["Catatan Admin"]
    old = [record(pic="Lama").get(h, "Uploaded" if h == "Status" else "") for h in LOG_HEADERS] + ["=ADMIN_FORMULA()"]
    ws = FakeWorksheet([header, old])
    SheetsLogStore(FakeSpreadsheet(ws), "SKP_UPLOAD_LOG").upsert([record(pic="Baru")])
    raw = ws.calls[0]
    assert raw[0] == "batch_update" and raw[1] == "RAW"
    assert [d["range"] for d in raw[2]] == ["A2:M2"]  # column N (admin) not written
    assert ws.values[1][13] == "=ADMIN_FORMULA()"
    assert ws.calls[1][1] == "USER_ENTERED" and ws.calls[1][2][0]["range"] == "K2"
    assert ws.values[1][LOG_HEADERS.index("PIC Submit")] == "Baru"


def test_link_failure_keeps_plain_gs_location():
    ws = FakeWorksheet([LOG_HEADERS], fail_user_entered=True)
    plan = SheetsLogStore(FakeSpreadsheet(ws), "SKP_UPLOAD_LOG").upsert([record()])
    assert len(plan.appends) == 1
    assert logs_to_frame(ws.get_all_values()).iloc[0]["GCS Path"] == LOCATION


def test_mock_locations_get_no_formula():
    ws = FakeWorksheet([LOG_HEADERS])
    SheetsLogStore(FakeSpreadsheet(ws), "SKP_UPLOAD_LOG").upsert([record(location="local://2026 Q4/jbp/x.pdf")])
    assert [c[1] for c in ws.calls] == ["RAW"]  # nothing to link


@pytest.mark.parametrize("header,runs", [
    (LOG_HEADERS, [(0, 13)]),
    (LOG_HEADERS + ["Catatan"], [(0, 13)]),
    (LOG_HEADERS[:5] + ["Catatan"] + LOG_HEADERS[5:], [(0, 5), (6, 14)]),
])
def test_app_column_runs(header, runs):
    assert app_column_runs(header) == runs


def test_first_appended_row_parsing():
    assert first_appended_row({"updates": {"updatedRange": "'SKP_UPLOAD_LOG'!A7:M9"}}) == 7
    assert first_appended_row({}) is None
