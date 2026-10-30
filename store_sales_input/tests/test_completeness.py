from datetime import date, datetime, timezone

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
        row("C", "K1"), row("C", "K2"),                      # semua SKU tanpa foto → tetap lengkap (foto opsional)
    ]
    st = by_id(compute_completeness(STORES, SKUS, rows))
    assert st["A"].status == STATUS_COMPLETE and (st["A"].sku_filled, st["A"].photo_count) == (2, 1)
    assert st["B"].status == STATUS_PARTIAL and st["B"].missing_skus == ("K2",)
    assert st["C"].status == STATUS_COMPLETE and st["C"].photo_count == 0 and st["C"].missing_skus == ()
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


# ── Rentang tanggal: beberapa kunjungan per store ───────────────────────────

D1, D2, D3 = date(2026, 10, 16), date(2026, 10, 19), date(2026, 10, 23)


def vrow(store, sku, d, file_id=None, so=1, st=1, at=T0):
    r = row(store, sku, so, st, file_id, at)
    r["tanggal_penjualan"] = d
    return r


def test_store_is_complete_if_any_visit_in_range_is_complete():
    rows = [
        vrow("A", "K1", D1, "f1"), vrow("A", "K2", D1),          # kunjungan 1: lengkap + foto
        vrow("A", "K1", D3),                                     # kunjungan 2: baru 1 SKU, tanpa foto
    ]
    s = by_id(compute_completeness(STORES, SKUS, rows))["A"]
    assert s.status == STATUS_COMPLETE and s.visits == 2
    assert s.best_date == D1 and s.last_visit == D3                # kunjungan terbaik != terakhir
    assert (s.sku_filled, s.missing_skus) == (2, ())               # statistik diambil dari kunjungan terbaik
    assert s.photo_count == 1


def test_no_complete_visit_is_partial_and_best_visit_has_most_skus():
    rows = [vrow("A", "K1", D1, "f1"),                             # 1 SKU + foto
            vrow("A", "K1", D2), vrow("A", "K2", D2, so=1, st=None)]   # 2 SKU, Stock K2 kosong
    s = by_id(compute_completeness(STORES, SKUS, rows))["A"]
    assert s.status == STATUS_PARTIAL and s.visits == 2
    assert s.best_date == D2 and s.sku_filled == 1 and s.missing_skus == ("K2",)   # seri → kunjungan terbaru
    assert s.photo_count == 1                                      # foto dihitung dari seluruh rentang


def test_two_visits_never_double_count_a_store_in_region_summary():
    rows = [vrow("A", "K1", D1, "f1"), vrow("A", "K2", D1), vrow("A", "K1", D3), vrow("A", "K2", D3, "f2")]
    statuses = compute_completeness(STORES, SKUS, rows)
    assert sum(1 for s in statuses if s.store.store_id == "A") == 1
    jt = {r["Region"]: r for r in summarize_by_region(statuses)}["JATENG"]
    assert (jt["Total Store"], jt[STATUS_COMPLETE]) == (2, 1)


def test_same_date_same_sku_is_not_counted_twice():
    rows = [vrow("A", "K1", D1, "f1"), vrow("A", "K1", D1, "f1"), vrow("A", "K2", D1)]
    s = by_id(compute_completeness(STORES, SKUS, rows))["A"]
    assert s.visits == 1 and s.sku_filled == 2


def test_evidence_items_carry_sales_date_and_sort_by_date_within_store():
    rows = [vrow("A", "K1", D3, "late"), vrow("A", "K1", D1, "early"), vrow("A", "K2", D1, "early2")]
    items = evidence_items(STORES, SKUS, rows)
    assert [(i.sales_date, i.file_id) for i in items] == [(D1, "early"), (D1, "early2"), (D3, "late")]
