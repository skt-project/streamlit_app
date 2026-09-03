"""Unit tests for noo_sku.sources.SheetsClient's Sheets API call shape.

2026-09-03 production incident: every live upload failed with "Upload gagal
saat menulis ke tracker" because `append_column_span` built its range as
`f"{start_col}1:{end_col}"` (e.g. `E1:W`) — valid-looking, but not actually
valid A1 notation once the start and end name *different* columns. Google's
API only allows one side of a range to drop its row number when both sides
name the SAME column (its own documented example is `Sheet1!A5:A`); an
asymmetric range like `E1:W` is rejected outright.

No test caught this before it shipped because every write-path test in
test_noo_sku_enrichment.py runs against FakeSheetsClient, which never touches
real A1 notation at all. These tests replace only the Google API service
object, so the actual range-string construction in SheetsClient itself is
exercised, not bypassed.
"""
from __future__ import annotations

from noo_sku.sources import SheetsClient


class _FakeAppendCall:
    def __init__(self, recorder, **kwargs):
        self._recorder = recorder
        self._kwargs = kwargs

    def execute(self):
        self._recorder.append(self._kwargs)
        return {"updates": {"updatedRows": len(self._kwargs["body"]["values"])}}


class _FakeValues:
    def __init__(self, recorder):
        self._recorder = recorder

    def append(self, **kwargs):
        return _FakeAppendCall(self._recorder, **kwargs)


class _FakeSpreadsheets:
    def __init__(self, recorder):
        self._recorder = recorder

    def values(self):
        return _FakeValues(self._recorder)


class _FakeService:
    def __init__(self, recorder):
        self._recorder = recorder

    def spreadsheets(self):
        return _FakeSpreadsheets(self._recorder)


def _client_with_fake_service():
    """A real SheetsClient with only the Google API service object swapped
    out, so append_column_span's own range-string construction runs for
    real."""
    client = object.__new__(SheetsClient)
    calls = []
    client._svc = _FakeService(calls)
    client.spreadsheet_id = "SPREADSHEET_ID"
    return client, calls


def test_append_column_span_sends_a_fully_bounded_a1_range():
    client, calls = _client_with_fake_service()
    client.append_column_span("POOL NOO STREAMLIT", "E", "W", [["x"] * 19])
    assert len(calls) == 1
    range_ = calls[0]["range"]
    assert range_ == "'POOL NOO STREAMLIT'!E1:W20000"
    # Both the start and end column each carry their own row number - the
    # exact shape that failed live (start had one, end had none).
    start_part, end_part = range_.split("!", 1)[1].split(":")
    assert any(ch.isdigit() for ch in start_part)
    assert any(ch.isdigit() for ch in end_part)


def test_append_column_span_uses_overwrite_not_insert_rows():
    """OVERWRITE targets the next row blank WITHIN the span - reusing BD
    Support's pre-existing formula row - never a brand new inserted row."""
    client, calls = _client_with_fake_service()
    client.append_column_span("POOL SKU STREAMLIT", "C", "M", [["y"] * 11])
    assert calls[0]["insertDataOption"] == "OVERWRITE"
    assert calls[0]["valueInputOption"] == "RAW"


def test_append_column_span_body_matches_the_rows_given():
    client, calls = _client_with_fake_service()
    rows = [["a", "b"], ["c", "d"]]
    client.append_column_span("POOL SKU STREAMLIT", "C", "D", rows)
    assert calls[0]["body"] == {"values": rows}
