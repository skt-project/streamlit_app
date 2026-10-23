from datetime import datetime, timezone

from completeness import (
    NO_REGION, STATUS_COMPLETE, STATUS_NONE, STATUS_PARTIAL, compute_completeness, evidence_items,
    summarize_by_region,
)
from validation import Sku, Store

T0 = datetime(2026, 10, 23, 1, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 10, 23, 5, 0, tzinfo=timezone.utc)

STORES = {
    "A": Store("A", "Toko A", "JATENG", "G", "Non BA"),
    "B": Store("B", "Toko B", "JATENG", "G", "Non BA"),
    "C": Store("C", "Toko C", "JABAR", "G", "Non BA"),
    "D": Store("D", "Toko D", "", "G", "Non BA"),
}
SKUS = {"K1": Sku("K1", "Satu"), "K2": Sku("K2", "Dua")}


def row(store, sku, so=1, st=2, file_id=None, at=T0, sup="Budi"):
    return {"store_id": store, "sku_code": sku, "sku_name": "n", "sell_out_qty": so, "stock_qty": st,
            "supervisor_name": sup, "evidence_file_id": file_id, "evidence_file_name": "x.jpg" if file_id else None,
            "updated_at": at}


def by_id(statuses):
    return {s.store.store_id: s for s in statuses}


def test_complete_partial_and_none():
    rows = [
        row("A", "K1", file_id="f1"), row("A", "K2"),        # semua SKU + 1 foto → lengkap
        row("B", "K1", file_id="f2"),                        # SKU K2 belum → belum lengkap
        row("C", "K1"), row("C", "K2"),                      # semua SKU tapi TANPA foto → belum lengkap
    ]
    st = by_id(compute_completeness(STORES, SKUS, rows))
    assert st["A"].status == STATUS_COMPLETE and (st["A"].sku_filled, st["A"].photo_count) == (2, 1)
    assert st["B"].status == STATUS_PARTIAL and st["B"].missing_skus == ("K2",)
    assert st["C"].status == STATUS_PARTIAL and st["C"].photo_count == 0 and st["C"].missing_skus == ()
    assert st["D"].status == STATUS_NONE and st["D"].sku_filled == 0 and st["D"].missing_skus == ("K1", "K2")


def test_zero_quantities_count_as_filled():
    st = by_id(compute_completeness(STORES, SKUS, [row("A", "K1", 0, 0, "f"), row("A", "K2", 0, 0)]))
    assert st["A"].status == STATUS_COMPLETE


def test_null_stock_does_not_count_as_filled():
    st = by_id(compute_completeness(STORES, SKUS, [row("A", "K1", 1, None, "f"), row("A", "K2")]))
    assert st["A"].status == STATUS_PARTIAL and st["A"].missing_skus == ("K1",)


def test_rows_for_unknown_store_or_sku_are_ignored():
    rows = [row("ZZZ", "K1", file_id="f"), row("A", "NOPE", file_id="f")]
    st = by_id(compute_completeness(STORES, SKUS, rows))
    assert all(s.status == STATUS_NONE for s in st.values())


def test_latest_submit_and_supervisor():
    rows = [row("A", "K1", at=T0, sup="Lama"), row("A", "K2", at=T1, sup="Baru")]
    s = by_id(compute_completeness(STORES, SKUS, rows))["A"]
    assert s.last_submit == T1 and s.supervisor == "Baru"


def test_blank_region_grouped_and_sorted():
    statuses = compute_completeness(STORES, SKUS, [])
    assert [s.region for s in statuses] == [NO_REGION, "JABAR", "JATENG", "JATENG"]
    assert by_id(statuses)["D"].region == NO_REGION


def test_region_summary_counts_and_percent():
    rows = [row("A", "K1", file_id="f"), row("A", "K2"), row("B", "K1")]
    summary = {r["Region"]: r for r in summarize_by_region(compute_completeness(STORES, SKUS, rows))}
    jt = summary["JATENG"]
    assert (jt["Total Store"], jt[STATUS_COMPLETE], jt[STATUS_PARTIAL], jt[STATUS_NONE]) == (2, 1, 1, 0)
    assert jt["% Lengkap"] == 50.0
    assert summary["JABAR"][STATUS_NONE] == 1 and summary["JABAR"]["% Lengkap"] == 0.0
    assert summary[NO_REGION]["Total Store"] == 1


def test_evidence_items_only_rows_with_photo_sorted():
    rows = [row("B", "K1", file_id="fb"), row("A", "K2", file_id="fa2"), row("A", "K1", file_id="fa1"),
            row("C", "K1"), row("ZZZ", "K1", file_id="x")]
    items = evidence_items(STORES, SKUS, rows)
    assert [(i.store_id, i.sku_code, i.file_id) for i in items] == [
        ("A", "K1", "fa1"), ("A", "K2", "fa2"), ("B", "K1", "fb")]      # tanpa foto & store asing dibuang
    assert items[0].region == "JATENG" and items[0].sku_name == "Satu"
