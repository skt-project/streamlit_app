from datetime import date

import pytest

from sheets_master import MasterError, SKU_HEADERS, STORE_HEADERS, find_table, parse_skus, parse_stores
from validation import (
    LineInput, is_non_ba, make_transaction_id, parse_qty, upsert_key, validate_submission,
)

D = date(2026, 10, 23)
PHOTO = b"\xff\xd8jpeg"


def run(stores, skus, lines, store_id="ST000001", sales_date=D, sup="Budi"):
    return validate_submission(store_id=store_id, sales_date=sales_date, supervisor_name=sup,
                               lines=lines, stores=stores, skus=skus)


def L(row, sku, so="0", st="0", photo=None, photo_error=None):
    return LineInput(row, sku, so, st, photo, photo_error)


# ── Master Store: hanya Non BA ──────────────────────────────────────────────

def _store_sheet(rows):
    return [STORE_HEADERS, *rows]


def test_only_non_ba_stores_loaded():
    values = _store_sheet([
        ["G1", "C1", "Toko A", "R1", "x", "GT", "1", "Non BA"],
        ["G2", "C2", "Toko B", "R2", "x", "GT", "1", "BA"],
        ["G3", "C3", "Toko C", "R3", "x", "MT", "1", "  non ba "],     # trim + case-insensitive
        ["G4", "C4", "Toko D", "R4", "x", "MT", "1", "NON  BA"],       # spasi ganda
        ["G5", "C5", "Toko E", "R5", "x", "MT", "1", ""],
        ["G6", "", "Tanpa Cust ID", "R6", "x", "MT", "1", "Non BA"],
    ])
    stores, _ = parse_stores(values)
    assert set(stores) == {"C1", "C3", "C4"}
    assert "C2" not in stores
    assert stores["C1"].label == "C1 - Toko A"
    assert (stores["C1"].region, stores["C1"].group, stores["C1"].ba_status) == ("R1", "GT", "Non BA")


def test_is_non_ba_variants():
    assert is_non_ba("Non BA") and is_non_ba(" non ba ") and not is_non_ba("BA") and not is_non_ba(None)


def test_store_header_found_below_title_rows_and_extra_columns():
    values = [["Judul"], ["", "x"], ["extra", *STORE_HEADERS],
              ["z", "G", "C1", "Toko", "R", "x", "GT", "1", "Non BA"]]
    stores, _ = parse_stores(values)
    assert list(stores) == ["C1"]


def test_store_missing_header_raises():
    with pytest.raises(MasterError):
        parse_stores([["Cust ID", "Store Name"], ["a", "b"]])
    assert find_table([["SKU Code", "SKU Name"]], SKU_HEADERS) is None


def test_store_budget_column_may_be_renamed_or_absent():
    """Kolom anggaran tidak dipakai: "Budget Q4" (header sebenarnya di master) tidak boleh menggagalkan."""
    hdr = ["Store/Group ID", "Cust. ID", "Store Name", "Region", "Cust Category", "Group", "Budget Q4",
           "BA/ Non BA", "Tagging Store"]
    stores, _ = parse_stores([[""] * 7 + ["129,604,432"], hdr,
                              ["G", "C1", "Toko A", "R", "GT", "G", "1", "Non BA", "JCP"],
                              ["G", "C2", "Toko B", "R", "GT", "G", "1", "BA", "JCP"]])
    assert list(stores) == ["C1"]


# ── Master SKU ──────────────────────────────────────────────────────────────

def test_sku_master_label_and_stp_unused():
    skus, _ = parse_skus([SKU_HEADERS, ["S1", "Produk 1", "12000"], ["S2", "Produk 2", ""],
                          ["", "x", "1"], ["S1", "dup", "1"]])
    assert list(skus) == ["S1", "S2"]
    assert skus["S1"].label == "S1 - Produk 1" and skus["S1"].name == "Produk 1"
    assert not hasattr(skus["S1"], "stp")


def test_sku_must_exist_in_master(stores, skus):
    r = run(stores, skus, [L(1, "NOPE", "1", "1", PHOTO)])
    assert not r.ok and any("tidak ada di master SKU" in e.message for e in r.errors)


def test_store_must_be_non_ba_master(stores, skus):
    r = run(stores, skus, [L(1, "SKU000001")], store_id="BA-STORE")
    assert not r.ok and any("Store" in e.message for e in r.errors)
    assert run(stores, skus, [L(1, "SKU000001")], store_id=None).ok is False


def test_required_form_fields(stores, skus):
    r = run(stores, skus, [L(1, "SKU000001")], sales_date=None, sup="  ")
    msgs = [e.message for e in r.errors]
    assert any("Tanggal" in m for m in msgs) and any("Supervisor" in m for m in msgs)


# ── Quantity (dipakai untuk Sell Out dan Stock) ─────────────────────────────

@pytest.mark.parametrize("raw,expected", [("0", 0), ("5", 5), (" 12 ", 12), (7, 7), (3.0, 3), ("007", 7)])
def test_qty_valid(raw, expected):
    assert parse_qty(raw) == (expected, None)


@pytest.mark.parametrize("raw", ["-1", -3])
def test_negative_qty_rejected(raw):
    assert parse_qty(raw) == (None, "Qty tidak boleh negatif.")


@pytest.mark.parametrize("raw", ["1.5", "1,5", "3.0", ".5", 2.5, float("nan"), float("inf")])
def test_decimal_qty_rejected(raw):
    q, err = parse_qty(raw)
    assert q is None and "bulat" in err


@pytest.mark.parametrize("raw", ["", "  ", None, "abc", "1e3", "1 000", True, str(2**64)])
def test_other_invalid_qty(raw):
    assert parse_qty(raw)[0] is None


def test_error_message_uses_label():
    assert parse_qty("-1", "Stock") == (None, "Stock tidak boleh negatif.")
    assert parse_qty("", "Sell Out") == (None, "Sell Out wajib diisi.")


def test_sell_out_and_stock_both_validated_per_row(stores, skus):
    r = run(stores, skus, [L(1, "SKU000001", "-2", "5"), L(2, "SKU000002", "1.5", "x"),
                           L(3, "SKU000003", "4", "6", PHOTO)])
    by_row = {}
    for e in r.errors:
        if e.row_no:
            by_row.setdefault(e.row_no, []).append(e.message)
    assert set(by_row) == {1, 2}
    assert by_row[1] == ["Sell Out tidak boleh negatif."]
    assert len(by_row[2]) == 2 and any(m.startswith("Sell Out") for m in by_row[2]) \
        and any(m.startswith("Stock") for m in by_row[2])


def test_stock_is_required_but_zero_is_allowed(stores, skus):
    assert not run(stores, skus, [L(1, "SKU000001", "1", "", PHOTO)]).ok
    r = run(stores, skus, [L(1, "SKU000001", "0", "0", PHOTO)])
    assert r.ok and (r.lines[0].sell_out_qty, r.lines[0].stock_qty) == (0, 0)


def test_totals_are_separate(stores, skus):
    r = run(stores, skus, [L(1, "SKU000001", "2", "10", PHOTO), L(2, "SKU000002", "3", "7")])
    assert (r.total_sell_out, r.total_stock) == (5, 17)


# ── Duplikat SKU: baris terakhir menang ─────────────────────────────────────

def test_duplicate_sku_last_row_wins(stores, skus):
    r = run(stores, skus, [L(1, "SKU000001", "5", "1", PHOTO), L(2, "SKU000002", "1", "1", PHOTO),
                           L(3, "SKU000001", "9", "4")])
    assert r.ok
    by = {ln.sku_code: ln for ln in r.lines}
    assert len(r.lines) == 2 and (by["SKU000001"].sell_out_qty, by["SKU000001"].stock_qty) == (9, 4)
    assert by["SKU000001"].photo is None            # foto baris yang dibuang tidak ikut
    assert any("dimasukkan lebih dari sekali" in w.message for w in r.warnings)


def test_invalid_overridden_duplicate_is_not_an_error(stores, skus):
    r = run(stores, skus, [L(1, "SKU000001", "abc", "abc"), L(2, "SKU000001", "0", "0", PHOTO)])
    assert r.ok and r.lines[0].sell_out_qty == 0


# ── Upsert key ──────────────────────────────────────────────────────────────

def test_upsert_key_and_deterministic_transaction_id():
    assert upsert_key(D, " S1 ", "K1") == ("2026-10-23", "S1", "K1")
    a = make_transaction_id(D, "S1", "K1")
    assert a == make_transaction_id(D, "S1", "K1") == make_transaction_id(D, " S1", "K1 ")
    others = {make_transaction_id(date(2026, 10, 24), "S1", "K1"),
              make_transaction_id(D, "S2", "K1"), make_transaction_id(D, "S1", "K2")}
    assert a not in others and len(others) == 3


# ── Aturan evidence: foto mandatory di setiap submit ────────────────────────

def test_photo_mandatory_even_when_everything_is_zero(stores, skus):
    r = run(stores, skus, [L(1, "SKU000001", "0", "0"), L(2, "SKU000002", "0", "0")])
    assert not r.ok and any("Foto evidence wajib" in e.message for e in r.errors)


def test_photo_mandatory_when_quantities_positive(stores, skus):
    r = run(stores, skus, [L(1, "SKU000001", "3", "5")])
    assert not r.ok and any("Foto evidence wajib" in e.message for e in r.errors)


def test_one_photo_from_any_sku_is_enough(stores, skus):
    r = run(stores, skus, [L(1, "SKU000001", "3", "5"), L(2, "SKU000002", "0", "0", PHOTO)])
    assert r.ok and r.photo_count == 1


def test_warning_only_for_sell_out_positive_without_photo(stores, skus):
    r = run(stores, skus, [L(1, "SKU000001", "3", "5", PHOTO), L(2, "SKU000002", "2", "0"),
                           L(3, "SKU000003", "0", "9")])
    assert r.ok
    warned = [w for w in r.warnings if w.message.startswith("PERINGATAN")]
    assert [w.row_no for w in warned] == [2]          # sell out 0 (walau stock > 0) tidak diperingatkan


def test_qty_error_in_one_row_does_not_claim_photo_missing(stores, skus):
    r = run(stores, skus, [L(1, "SKU000001", "x", "1", PHOTO)])
    assert not r.ok and not any("Foto evidence wajib" in e.message for e in r.errors)


def test_photo_error_blocks_that_row(stores, skus):
    r = run(stores, skus, [L(1, "SKU000001", "3", "1", None, "Format foto tidak didukung")])
    assert not r.ok and any("Foto:" in e.message for e in r.errors)


def test_blank_rows_ignored_and_empty_submission_rejected(stores, skus):
    assert run(stores, skus, [LineInput(1), L(2, "SKU000001", "0", "0", PHOTO)]).ok
    r = run(stores, skus, [LineInput(1)])
    assert not r.ok and any("minimal satu SKU" in e.message for e in r.errors)
    assert not any("Foto evidence wajib" in e.message for e in r.errors)


def test_renamed_store_headers_kode_toko_nama_toko_are_accepted():
    """Master Oktober 2026: 'Cust. ID' -> 'Kode Toko', 'Store Name' -> 'Nama Toko' (data sama)."""
    hdr = ["Store/Group ID", "Kode Toko", "Nama Toko", "Region", "Channel", "Group", "Budget Q4", "BA/ Non BA", "Program"]
    stores, _ = parse_stores([[""] * 7 + ["129,604,432"], hdr,
                              ["G", "C1", "Toko A ", "R", "GT", "G", "1", "Non BA", "JCP"],
                              ["G", "C2", "Toko B", "R", "GT", "G", "1", "BA", "JCP"]])
    assert list(stores) == ["C1"] and stores["C1"].store_name == "Toko A" and stores["C1"].region == "R"
    assert find_table([hdr], ["Cust. ID", "Store Name"]) is not None
