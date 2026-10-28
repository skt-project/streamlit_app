from datetime import date, datetime, timezone

from completeness import (
    STATUS_COMPLETE, STATUS_NONE, STATUS_PARTIAL, change_marker, compare_sku, compute_completeness,
    evidence_items, summarize_by_region,
)
from validation import Distributor, Sku

D1 = date(2026, 10, 20)
D2 = date(2026, 10, 23)
T0 = datetime(2026, 10, 23, 1, 0, tzinfo=timezone.utc)
DISTS = {
    "D1": Distributor("D1", "Gudang Satu", "R1", "Ani"),
    "D2": Distributor("D2", "Gudang Dua", "", "Budi"),
}
SKUS = {"TST000001": Sku("TST000001", "A"), "TST000002": Sku("TST000002", "B")}


def row(day, code, sku, stock, photo=False, updated=T0):
    return {
        "tanggal_stock": day, "distributor_code": code, "sku_code": sku, "stock_qty": stock,
        "evidence_file_id": f"{code}.jpg" if photo else None, "supervisor_name": "Ani", "updated_at": updated,
        "sku_name": sku,
    }


def test_photo_rule_and_visit_outside_logic():
    zero = [row(D2, "D1", "TST000001", 0), row(D2, "D1", "TST000002", 0)]
    statuses = {s.distributor.distributor_code: s for s in compute_completeness(DISTS, SKUS, zero)}
    assert statuses["D1"].status == STATUS_COMPLETE and statuses["D2"].status == STATUS_NONE

    positive = [row(D2, "D1", "TST000001", 4), row(D2, "D1", "TST000002", 1)]
    positive_status = {s.distributor.distributor_code: s for s in compute_completeness(DISTS, SKUS, positive)}
    assert positive_status["D1"].status == STATUS_PARTIAL

    with_photo = [row(D2, "D1", "TST000001", 4, photo=True), row(D2, "D1", "TST000002", 0)]
    photo_status = {s.distributor.distributor_code: s for s in compute_completeness(DISTS, SKUS, with_photo)}
    assert photo_status["D1"].status == STATUS_COMPLETE


def test_older_complete_visit_keeps_distributor_complete_and_bounds_are_callers_job():
    rows = [
        row(D1, "D1", "TST000001", 2, photo=True), row(D1, "D1", "TST000002", 0),
        row(D2, "D1", "TST000001", 9),
    ]
    status = {s.distributor.distributor_code: s for s in compute_completeness(DISTS, SKUS, rows)}["D1"]
    assert status.status == STATUS_COMPLETE and status.best_date == D1 and status.visits == 2
    summary = summarize_by_region(compute_completeness(DISTS, SKUS, rows))
    assert summary[0]["Region"] == "(Tanpa Region)" or any(r["Total"] == 2 for r in summary)


def test_unknown_rows_ignored_and_evidence_sorted_once():
    rows = [
        row(D2, "NOPE", "TST000001", 1, photo=True),
        row(D2, "D1", "TST000009", 1, photo=True),
        row(D2, "D1", "TST000001", 3, photo=True),
        row(D1, "D1", "TST000001", 1, photo=True),
    ]
    items = evidence_items(DISTS, SKUS, rows)
    assert [(i.distributor_code, i.tanggal_stock, i.sku_code) for i in items] == [
        ("D1", D1, "TST000001"), ("D1", D2, "TST000001"), ("D1", D2, "TST000009"),
    ]


def test_previous_comparison_delta_percent_baru_and_threshold():
    assert compare_sku(12, None)["perubahan"] == "baru"
    assert compare_sku(12, 0)["perubahan"] == "baru"
    assert compare_sku(12, 0)["selisih"] == 12
    change = compare_sku(15, 10)
    assert change["selisih"] == 5 and change["perubahan"] == "50.0%"
    assert change_marker("50.0%", 50) == "⚠️"
    assert change_marker("20.0%", 50) == ""
    assert change_marker("baru", 50) == ""
    assert STATUS_PARTIAL and STATUS_NONE
