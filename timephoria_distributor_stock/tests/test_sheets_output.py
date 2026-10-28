from datetime import date, datetime, timezone

import pytest
from gspread.exceptions import WorksheetNotFound

from sheets_output import OUTPUT_HEADERS, MirrorError, SheetsMirror

NOW = datetime(2026, 10, 23, 4, 0, tzinfo=timezone.utc)


class FakeWS:
    def __init__(self, rows):
        self.rows = [list(r) for r in rows]
        self.writes = 0
        self.options = []

    def row_values(self, n):
        return list(self.rows[n - 1]) if len(self.rows) >= n else []

    def get(self, rng):
        return [list(r) for r in self.rows]

    def update(self, range_name, values, value_input_option=None):
        self.writes += 1
        self.options.append(value_input_option)
        self.rows = [list(values[0])]

    def freeze(self, rows=0):
        pass

    def batch_update(self, data, value_input_option=None):
        self.writes += 1
        self.options.append(value_input_option)
        for item in data:
            n = int(item["range"].split(":")[0][1:])
            self.rows[n - 1] = list(item["values"][0])

    def append_rows(self, rows, value_input_option=None, table_range=None):
        self.writes += 1
        self.options.append((value_input_option, table_range))
        self.rows.extend([list(r) for r in rows])


class FakeSheet:
    def __init__(self, ws=None):
        self.ws = ws
        self.created = None

    def worksheet(self, name):
        if self.ws is None:
            raise WorksheetNotFound(name)
        return self.ws

    def add_worksheet(self, title, rows, cols):
        self.created = title
        self.ws = FakeWS([])
        return self.ws


def rec(sku="K1", qty=3, created=NOW):
    return {
        "transaction_id": "tx-" + sku, "tanggal_stock": date(2026, 10, 23),
        "distributor_code": "D1", "distributor_name": "Gudang", "region": "R",
        "supervisor_name": "Ani", "sku_code": sku, "sku_name": "N", "stock_qty": qty,
        "evidence_file_id": None, "evidence_file_url": None, "evidence_file_name": None,
        "evidence_uploaded_at": None, "created_at": created, "updated_at": NOW,
    }


def test_missing_worksheet_created_with_headers_and_raw_option():
    sheet = FakeSheet()
    assert SheetsMirror(sheet).upsert([rec()]) == (1, 0)
    assert sheet.created == "Distributor Stock"
    assert sheet.ws.rows[0] == OUTPUT_HEADERS and len(sheet.ws.rows) == 2
    assert "RAW" in sheet.ws.options


def test_wrong_header_blocks_write():
    ws = FakeWS([["a", "b"]])
    with pytest.raises(MirrorError, match="Header"):
        SheetsMirror(FakeSheet(ws)).upsert([rec()])
    assert ws.writes == 0 and ws.rows == [["a", "b"]]


def test_upsert_updates_inserts_and_collapses_duplicate_keys_in_one_batch():
    ws = FakeWS([OUTPUT_HEADERS])
    mirror = SheetsMirror(FakeSheet(ws))
    assert mirror.upsert([rec("K1", 3), rec("K2", 1)]) == (2, 0)
    assert mirror.upsert([rec("K1", 9), rec("K3", 4)]) == (1, 1)
    assert mirror.upsert([rec("K9", 1), rec("K9", 8)]) == (1, 0)
    sku_idx = OUTPUT_HEADERS.index("sku_code")
    qty_idx = OUTPUT_HEADERS.index("stock_qty")
    body = {row[sku_idx]: row[qty_idx] for row in ws.rows[1:]}
    assert body == {"K1": 9, "K2": 1, "K3": 4, "K9": 8}


def test_timestamps_are_wib_and_factory_is_lazy():
    ws = FakeWS([OUTPUT_HEADERS])
    SheetsMirror(FakeSheet(ws)).upsert([rec()])
    row = dict(zip(OUTPUT_HEADERS, ws.rows[1]))
    assert row["tanggal_stock"] == "2026-10-23"
    assert row["created_at"] == "2026-10-23 11:00:00 WIB"
    assert row["evidence_file_url"] == ""
    calls = []
    mirror = SheetsMirror(lambda: calls.append(1) or FakeSheet(FakeWS([OUTPUT_HEADERS])))
    assert calls == []
    mirror.upsert([rec()])
    assert calls == [1]
