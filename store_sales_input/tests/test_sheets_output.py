from datetime import date, datetime, timezone

import pytest
from gspread.exceptions import WorksheetNotFound

from sheets_output import OUTPUT_HEADERS, MirrorError, SheetsMirror

NOW = datetime(2026, 10, 23, 4, 0, tzinfo=timezone.utc)


class FakeWS:
    def __init__(self, rows):
        self.rows = [list(r) for r in rows]
        self.writes = 0

    def row_values(self, n):
        return list(self.rows[n - 1]) if len(self.rows) >= n else []

    def get(self, rng):
        return [list(r) for r in self.rows]

    def update(self, range_name, values, value_input_option=None):
        self.writes += 1
        self.rows = [list(values[0])]

    def freeze(self, rows=0):
        pass

    def batch_update(self, data, value_input_option=None):
        self.writes += 1
        for d in data:
            n = int(d["range"].split(":")[0][1:])
            self.rows[n - 1] = list(d["values"][0])

    def append_rows(self, rows, value_input_option=None, table_range=None):
        self.writes += 1
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
    return {"transaction_id": "tx-" + sku, "tanggal_penjualan": date(2026, 10, 23), "store_id": "S1",
            "store_name": "Toko", "region": "R", "store_group": "G", "sku_code": sku, "sku_name": "N",
            "sell_out_qty": qty, "stock_qty": qty + 100, "supervisor_name": "Budi", "evidence_file_id": None, "evidence_file_url": None,
            "evidence_file_name": None, "evidence_uploaded_at": None, "created_at": created, "updated_at": NOW}


def test_missing_worksheet_created_with_headers():
    sh = FakeSheet()
    assert SheetsMirror(sh).upsert([rec()]) == (1, 0)
    assert sh.created == "Store Sales"
    assert sh.ws.rows[0] == OUTPUT_HEADERS and len(sh.ws.rows) == 2


def test_wrong_header_blocks_write():
    ws = FakeWS([["a", "b"]])
    with pytest.raises(MirrorError, match="Header"):
        SheetsMirror(FakeSheet(ws)).upsert([rec()])
    assert ws.writes == 0 and ws.rows == [["a", "b"]]


def test_upsert_updates_existing_row_and_inserts_new_without_duplicates():
    ws = FakeWS([OUTPUT_HEADERS])
    m = SheetsMirror(FakeSheet(ws))
    assert m.upsert([rec("K1", 3), rec("K2", 1)]) == (2, 0)
    assert m.upsert([rec("K1", 9), rec("K3", 4)]) == (1, 1)      # K1 diupdate, K3 baru
    assert m.upsert([rec("K1", 9)]) == (0, 1)
    qty_idx, sku_idx = OUTPUT_HEADERS.index("sell_out_qty"), OUTPUT_HEADERS.index("sku_code")
    body = ws.rows[1:]
    assert sorted(r[sku_idx] for r in body) == ["K1", "K2", "K3"]
    assert [r for r in body if r[sku_idx] == "K1"][0][qty_idx] == 9


def test_same_key_twice_in_one_batch_inserted_once():
    ws = FakeWS([OUTPUT_HEADERS])
    assert SheetsMirror(FakeSheet(ws)).upsert([rec("K1"), rec("K1", 5)]) == (1, 0)


def test_value_formatting():
    ws = FakeWS([OUTPUT_HEADERS])
    SheetsMirror(FakeSheet(ws)).upsert([rec()])
    row = dict(zip(OUTPUT_HEADERS, ws.rows[1]))
    assert row["tanggal_penjualan"] == "2026-10-23" and row["created_at"] == "2026-10-23 11:00:00 WIB"
    assert row["evidence_file_url"] == "" and row["sell_out_qty"] == 3 and row["stock_qty"] == 103


def test_lazy_spreadsheet_factory_not_called_until_needed():
    calls = []
    m = SheetsMirror(lambda: calls.append(1) or FakeSheet(FakeWS([OUTPUT_HEADERS])))
    assert calls == []
    m.upsert([rec()])
    assert calls == [1]
