"""Unit tests for noo_sku.sources.SheetsClient's Sheets API call shape.

Two live production incidents in a row (2026-09-03/04) came from the exact
Sheets API request shape, not from anything a FakeSheetsClient-based test
could catch:

1. `append_column_span`'s range was built as f"{start}1:{end}" (e.g. "E1:W")
   - not valid A1 notation once start/end name different columns.
2. Even once the range was valid, `values.append`'s range only narrows table
   *detection* - it does not restrict which columns the appended values land
   in. Once BD Support's header row spans the whole sheet, `append` aligned
   new data to the detected table's own first column (A), not the intended
   span.

Both were invisible to every write-path test in test_noo_sku_enrichment.py,
because those all run against FakeSheetsClient, which never touches a real
Google API call shape. These tests replace only the Google API service
object, so SheetsClient's own method bodies run for real.
"""
from __future__ import annotations

from noo_sku.sources import SheetsClient


class _FakeApiCall:
    def __init__(self, recorder, **kwargs):
        self._recorder = recorder
        self._kwargs = kwargs

    def execute(self):
        self._recorder.append(self._kwargs)
        return {"updatedRows": len(self._kwargs["body"]["values"])}


class _FakeValues:
    def __init__(self, recorder):
        self._recorder = recorder

    def update(self, **kwargs):
        return _FakeApiCall(self._recorder, **kwargs)


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
    out, so update_range's own request construction runs for real."""
    client = object.__new__(SheetsClient)
    calls = []
    client._svc = _FakeService(calls)
    client.spreadsheet_id = "SPREADSHEET_ID"
    return client, calls


def test_update_range_targets_exactly_the_given_a1_range():
    client, calls = _client_with_fake_service()
    client.update_range("POOL NOO STREAMLIT", "E7:W7", [["x"] * 19])
    assert len(calls) == 1
    assert calls[0]["range"] == "'POOL NOO STREAMLIT'!E7:W7"


def test_update_range_uses_update_not_append():
    """update writes to exactly the named cells - no table search, no
    ambiguity about which column the data starting landing in. This is the
    fix for the 2026-09-04 incident where values.append silently aligned new
    data to column A regardless of the range's own start column."""
    client, calls = _client_with_fake_service()
    client.update_range("POOL SKU STREAMLIT", "C5:M5", [["y"] * 11])
    assert "insertDataOption" not in calls[0]
    assert calls[0]["valueInputOption"] == "RAW"


def test_update_range_body_matches_the_rows_given():
    client, calls = _client_with_fake_service()
    rows = [["a", "b"], ["c", "d"]]
    client.update_range("POOL SKU STREAMLIT", "C5:D6", rows)
    assert calls[0]["body"] == {"values": rows}


def test_update_range_does_not_touch_columns_outside_the_range():
    """The range names only the owned span - nothing in the request could
    reference a formula or BD-manual column outside it."""
    client, calls = _client_with_fake_service()
    client.update_range("POOL NOO STREAMLIT", "E7:W7", [["x"] * 19])
    range_ = calls[0]["range"]
    assert "'POOL NOO STREAMLIT'!" in range_
    start_end = range_.split("!", 1)[1]
    assert start_end == "E7:W7"
