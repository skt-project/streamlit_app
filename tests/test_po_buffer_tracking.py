"""Download tracking rules of the PO Suggestion Tool (po_buffer_tracking).

Uses an in-memory fake of BigQuery (tests/po_buffer_fakes.py): no credentials,
no network. The counting rules under test:

* an individual download adds 1 to that one store;
* a bulk download adds 1 to EVERY store inside the ZIP;
* generating a PDF/ZIP never counts - only recording a click does;
* the same click can never be counted twice (rerun / replay).

Run: pytest tests/test_po_buffer_tracking.py -q
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
if str(REPO / "tests") not in sys.path:
    sys.path.insert(0, str(REPO / "tests"))

import po_buffer_tracking as T  # noqa: E402
from po_buffer_fakes import FakeClock, FakeTrackingBigQuery  # noqa: E402


def stores(*codes, region="JAWA", distributor="DIST A"):
    return [
        {"store_code": c, "store_name": f"Toko {c}", "region": region,
         "distributor": distributor, "file_name": f"Toko {c}_20261119.pdf"}
        for c in codes
    ]


@pytest.fixture
def bq():
    return FakeTrackingBigQuery()


@pytest.fixture
def store(bq, tmp_path):
    return T.TrackingStore(lambda: bq, fallback_path=str(tmp_path / "spool.jsonl"))


def per_store(store_):
    return T.aggregate_by_store(store_.fetch().events).set_index("store_code")


# ---------------------------------------------------------------------------
# Counting rules
# ---------------------------------------------------------------------------

def test_individual_download_counts_only_the_selected_store(store):
    T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S1"), "evt-1")
    summary = per_store(store)
    assert list(summary.index) == ["S1"]
    assert summary.loc["S1", "individual_downloads"] == 1
    assert summary.loc["S1", "bulk_downloads"] == 0
    assert summary.loc["S1", "total_downloads"] == 1


def test_bulk_download_of_five_stores_counts_each_store_exactly_once(store):
    codes = [f"S{n}" for n in range(1, 6)]
    T.record_download_event(store, T.DOWNLOAD_BULK, stores(*codes), "evt-bulk")
    summary = per_store(store)
    assert sorted(summary.index) == codes
    assert (summary["bulk_downloads"] == 1).all()
    assert (summary["individual_downloads"] == 0).all()
    assert (summary["total_downloads"] == 1).all()
    assert summary["total_downloads"].sum() == 5


def test_bulk_rows_share_one_event_and_record_which_stores_were_in_it(store, bq):
    T.record_download_event(store, T.DOWNLOAD_BULK, stores("S1", "S2", "S3"), "evt-bulk")
    events = store.fetch().events
    assert set(events["event_id"]) == {"evt-bulk"}
    assert set(events["store_code"]) == {"S1", "S2", "S3"}
    assert (events["bulk_file_count"] == 3).all()
    details = T.bulk_event_details(events)
    assert len(details) == 1 and details.loc[0, "store_count"] == 3
    assert "S1" in details.loc[0, "stores"] and "S3" in details.loc[0, "stores"]


def test_individual_and_bulk_downloads_are_kept_apart_per_store(store):
    T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S1"), "e1")
    T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S1"), "e2")
    T.record_download_event(store, T.DOWNLOAD_BULK, stores("S1", "S2"), "e3")
    summary = per_store(store)
    assert summary.loc["S1", ["individual_downloads", "bulk_downloads", "total_downloads"]].tolist() == [2, 1, 3]
    assert summary.loc["S2", ["individual_downloads", "bulk_downloads", "total_downloads"]].tolist() == [0, 1, 1]
    totals = T.summarize_totals(store.fetch().events)
    assert totals == {"total_downloads": 4, "individual_downloads": 2, "bulk_downloads": 2,
                      "bulk_actions": 1, "stores_downloaded": 2}


def test_two_real_clicks_on_the_same_store_count_twice(store):
    T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S1"), "click-1")
    T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S1"), "click-2")
    assert per_store(store).loc["S1", "total_downloads"] == 2


def test_nothing_is_recorded_until_record_is_called(store, bq):
    """Generating PDFs/ZIPs is separate from tracking: no write without a click."""
    assert store.fetch().events.empty
    assert bq.insert_calls == 0


# ---------------------------------------------------------------------------
# Rerun / replay safety
# ---------------------------------------------------------------------------

def test_a_click_delivered_twice_in_one_session_is_counted_once(store):
    seen = set()
    first = T.record_download_event(store, T.DOWNLOAD_BULK, stores("S1", "S2"), "evt", seen_event_ids=seen)
    again = T.record_download_event(store, T.DOWNLOAD_BULK, stores("S1", "S2"), "evt", seen_event_ids=seen)
    assert first.rows == 2 and not first.duplicate
    assert again.duplicate and again.rows == 0
    assert per_store(store)["total_downloads"].tolist() == [1, 1]


def test_a_replayed_event_from_another_session_is_still_counted_once(store, bq):
    """The database guards across sessions: same (event_id, store_code) is never inserted twice."""
    T.record_download_event(store, T.DOWNLOAD_BULK, stores("S1", "S2"), "evt", seen_event_ids=set())
    T.record_download_event(store, T.DOWNLOAD_BULK, stores("S1", "S2"), "evt", seen_event_ids=set())
    assert bq.insert_calls == 2          # it was sent twice ...
    assert len(bq.rows) == 2             # ... but stored once
    assert per_store(store)["total_downloads"].tolist() == [1, 1]


def test_the_same_store_listed_twice_in_one_bulk_is_counted_once(store):
    T.record_download_event(store, T.DOWNLOAD_BULK, stores("S1", "S1", "S2"), "evt")
    events = store.fetch().events
    assert len(events) == 2 and (events["bulk_file_count"] == 2).all()


def test_the_insert_is_idempotent_by_construction():
    sql = T.insert_events_sql()
    assert "NOT EXISTS" in sql and "event_id" in sql and "store_code" in sql


# ---------------------------------------------------------------------------
# Row building
# ---------------------------------------------------------------------------

def test_rows_carry_store_metadata_type_file_and_utc_time():
    moment = datetime(2026, 11, 19, 3, 15, tzinfo=timezone.utc)
    (row,) = T.build_download_rows(T.DOWNLOAD_INDIVIDUAL, stores("S1"), "evt", moment)
    assert row == {
        "event_id": "evt", "store_code": "S1", "store_name": "Toko S1", "region": "JAWA",
        "distributor": "DIST A", "download_type": "individual",
        "file_name": "Toko S1_20261119.pdf", "bulk_file_count": None, "downloaded_at": moment,
    }


def test_rows_reject_unknown_type_empty_event_and_empty_store_list():
    with pytest.raises(ValueError):
        T.build_download_rows("export", stores("S1"), "evt")
    with pytest.raises(ValueError):
        T.build_download_rows(T.DOWNLOAD_BULK, stores("S1"), "")
    with pytest.raises(ValueError):
        T.build_download_rows(T.DOWNLOAD_BULK, [], "evt")
    with pytest.raises(ValueError):
        T.build_download_rows(T.DOWNLOAD_BULK, [{"store_code": None, "store_name": "x"}], "evt")


def test_downloaded_at_defaults_to_now_in_utc():
    (row,) = T.build_download_rows(T.DOWNLOAD_INDIVIDUAL, stores("S1"), "evt")
    assert row["downloaded_at"].tzinfo is not None
    assert row["downloaded_at"].utcoffset().total_seconds() == 0


# ---------------------------------------------------------------------------
# Persistence: BigQuery table, creation, fallback
# ---------------------------------------------------------------------------

def test_events_survive_a_new_store_instance_because_they_live_in_bigquery(bq, tmp_path):
    first = T.TrackingStore(lambda: bq, fallback_path=str(tmp_path / "a.jsonl"))
    T.record_download_event(first, T.DOWNLOAD_BULK, stores("S1", "S2"), "evt")
    restarted = T.TrackingStore(lambda: bq, fallback_path=str(tmp_path / "b.jsonl"))
    assert len(restarted.fetch().events) == 2


def test_the_table_is_created_once_per_process(store, bq):
    T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S1"), "e1")
    T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S2"), "e2")
    store.fetch()
    assert sum(q.lstrip().upper().startswith("CREATE TABLE") for q in bq.queries) == 1


def test_an_existing_table_is_used_when_the_account_cannot_create_tables(tmp_path):
    bq = FakeTrackingBigQuery(can_create=False, table_exists=True)
    store = T.TrackingStore(lambda: bq, fallback_path=str(tmp_path / "s.jsonl"))
    result = T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S1"), "evt")
    assert result.backend == "bigquery" and result.warning is None
    assert len(bq.rows) == 1


def test_without_table_and_without_create_permission_it_falls_back_to_the_local_file(tmp_path):
    bq = FakeTrackingBigQuery(can_create=False, table_exists=False)
    store = T.TrackingStore(lambda: bq, fallback_path=str(tmp_path / "s.jsonl"))
    result = T.record_download_event(store, T.DOWNLOAD_BULK, stores("S1", "S2"), "evt")
    assert result.backend == "local_file"
    assert "hilang" in result.warning  # the limitation is spelled out
    assert bq.rows == []
    fetched = store.fetch()
    assert fetched.source == "local_file" and "restart" in fetched.warning
    assert sorted(fetched.events["store_code"]) == ["S1", "S2"]


def test_a_bigquery_outage_never_loses_the_event(tmp_path):
    bq = FakeTrackingBigQuery(table_exists=True, fail_inserts=True)
    store = T.TrackingStore(lambda: bq, fallback_path=str(tmp_path / "s.jsonl"))
    result = T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S1"), "evt")
    assert result.backend == "local_file"
    assert per_store(store).loc["S1", "total_downloads"] == 1


def test_fallback_rows_are_pushed_to_bigquery_when_it_recovers_without_duplicates(tmp_path):
    clock = FakeClock()
    bq = FakeTrackingBigQuery(table_exists=True, fail_inserts=True)
    spool = tmp_path / "s.jsonl"
    store = T.TrackingStore(lambda: bq, fallback_path=str(spool), clock=clock)

    T.record_download_event(store, T.DOWNLOAD_BULK, stores("S1", "S2"), "evt-outage")
    assert spool.exists() and bq.rows == []

    bq.fail_inserts = False
    clock.advance(T.BQ_RETRY_AFTER_SECONDS + 1)
    T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S3"), "evt-after")

    assert not spool.exists()                      # spool flushed and removed
    assert len(bq.rows) == 3                       # 2 spooled + 1 new, each once
    events = store.fetch().events
    assert len(events) == 3
    assert T.summarize_totals(events)["total_downloads"] == 3


def test_after_a_failure_bigquery_is_not_hammered_until_the_retry_window_passes(tmp_path):
    clock = FakeClock()
    bq = FakeTrackingBigQuery(table_exists=True, fail_inserts=True)
    store = T.TrackingStore(lambda: bq, fallback_path=str(tmp_path / "s.jsonl"), clock=clock)
    T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S1"), "e1")
    calls = bq.insert_calls
    T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S2"), "e2")
    assert bq.insert_calls == calls                # skipped during the window
    clock.advance(T.BQ_RETRY_AFTER_SECONDS + 1)
    T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S3"), "e3")
    assert bq.insert_calls == calls + 1            # tried again afterwards


def test_a_read_failure_is_reported_instead_of_crashing_the_dashboard(tmp_path):
    bq = FakeTrackingBigQuery(table_exists=True)
    store = T.TrackingStore(lambda: bq, fallback_path=str(tmp_path / "s.jsonl"))
    T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S1"), "evt")
    bq.fail_selects = True
    fetched = store.fetch()
    assert fetched.source == "local_file"
    assert "Gagal membaca" in fetched.warning
    assert fetched.events.empty                       # nothing in the local file, not an exception


def test_no_client_configured_uses_the_local_file_and_says_so(tmp_path):
    store = T.TrackingStore(None, fallback_path=str(tmp_path / "s.jsonl"))
    result = T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S1"), "evt")
    assert result.backend == "local_file"
    fetched = store.fetch()
    assert fetched.source == "local_file" and len(fetched.events) == 1


def test_the_fallback_file_tolerates_a_corrupt_line(tmp_path):
    spool = tmp_path / "s.jsonl"
    store = T.TrackingStore(None, fallback_path=str(spool))
    T.record_download_event(store, T.DOWNLOAD_INDIVIDUAL, stores("S1"), "evt")
    with open(spool, "a", encoding="utf-8") as handle:
        handle.write("{not json\n")
    assert len(store.fetch().events) == 1


def test_default_fallback_path_is_outside_the_repository(monkeypatch):
    monkeypatch.delenv(T.FALLBACK_PATH_ENV, raising=False)
    path = Path(T.default_fallback_path()).resolve()
    assert REPO not in path.parents


def test_ddl_file_matches_the_ddl_the_app_runs():
    on_disk = (REPO / "sql" / "po_buffer_download_log.sql").read_text(encoding="utf-8")
    normalise = lambda text: " ".join(text.replace(";", " ").split())
    assert normalise(T.create_table_ddl()) in normalise(on_disk)


def test_ddl_declares_every_tracked_column():
    ddl = T.create_table_ddl()
    for column in T.TRACKING_COLUMNS:
        assert f"  {column} " in ddl


# ---------------------------------------------------------------------------
# Dashboard analysis
# ---------------------------------------------------------------------------

def sample_events():
    day = lambda d, h=3: datetime(2026, 11, d, h, 0, tzinfo=timezone.utc)
    rows = []
    rows += T.build_download_rows(T.DOWNLOAD_INDIVIDUAL, stores("S1", region="JAWA", distributor="DIST A"), "e1", day(10))
    rows += T.build_download_rows(T.DOWNLOAD_INDIVIDUAL, stores("S2", region="SUMATERA", distributor="DIST B"), "e2", day(12))
    mixed_bulk = stores("S1", "S3") + stores("S2", region="SUMATERA", distributor="DIST B")
    rows += T.build_download_rows(T.DOWNLOAD_BULK, mixed_bulk, "e3", day(15))
    return T._normalise_events(pd.DataFrame(rows))


def test_aggregate_orders_by_total_downloads_and_reports_last_download():
    summary = T.aggregate_by_store(sample_events()).set_index("store_code")
    assert summary.loc["S1", "total_downloads"] == 2 and summary.loc["S3", "total_downloads"] == 1
    assert T.aggregate_by_store(sample_events())["total_downloads"].is_monotonic_decreasing
    assert summary.loc["S1", "last_download_at"].tz.zone == "Asia/Jakarta"
    assert summary.loc["S1", "last_download_at"].day == 15


def test_filter_by_date_is_inclusive_and_uses_jakarta_days():
    events = sample_events()
    got = T.filter_events(events, start_date=date(2026, 11, 12), end_date=date(2026, 11, 14))
    assert set(got["event_id"]) == {"e2"}
    got = T.filter_events(events, start_date=date(2026, 11, 15))
    assert set(got["event_id"]) == {"e3"}
    # 18:00 UTC on the 15th is 01:00 on the 16th in Jakarta
    late = T._normalise_events(pd.DataFrame(T.build_download_rows(
        T.DOWNLOAD_INDIVIDUAL, stores("S9"), "late", datetime(2026, 11, 15, 18, 0, tzinfo=timezone.utc))))
    assert T.filter_events(late, start_date=date(2026, 11, 16)).shape[0] == 1
    assert T.filter_events(late, end_date=date(2026, 11, 15)).shape[0] == 0


def test_filter_by_store_region_and_distributor():
    events = sample_events()
    assert set(T.filter_events(events, store_codes=["S3"])["store_code"]) == {"S3"}
    sumatera = T.filter_events(events, regions=["SUMATERA"])
    assert sorted(sumatera["store_code"]) == ["S2", "S2"] and set(sumatera["event_id"]) == {"e2", "e3"}
    dist_a = T.filter_events(events, distributors=["DIST A"])
    assert sorted(dist_a["store_code"]) == ["S1", "S1", "S3"]
    both = T.filter_events(events, regions=["JAWA"], store_codes=["S1"])
    assert sorted(both["event_id"]) == ["e1", "e3"]
    assert T.filter_events(events, regions=["JAWA"], store_codes=["S2"]).empty


def test_empty_data_gives_empty_but_well_formed_results():
    empty = T._empty_events()
    assert T.summarize_totals(empty)["total_downloads"] == 0
    assert list(T.aggregate_by_store(empty).columns) == T.STORE_SUMMARY_COLUMNS
    assert T.bulk_event_details(empty).empty
    assert T.filter_events(empty, start_date=date(2026, 1, 1)).empty


def test_csv_export_is_excel_friendly_and_neutralises_formulas():
    summary = T.aggregate_by_store(sample_events())
    summary.loc[0, "store_name"] = "=HYPERLINK(\"http://evil\")"
    raw = T.to_csv_bytes(summary)
    assert raw.startswith(b"\xef\xbb\xbf")                      # UTF-8 BOM
    text = raw.decode("utf-8-sig")
    assert "'=HYPERLINK" in text and ",=HYPERLINK" not in text
    assert "2026-11-15 10:00:00" in text                         # Jakarta wall time
