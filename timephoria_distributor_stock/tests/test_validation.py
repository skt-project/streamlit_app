from datetime import date

import pytest

from validation import (
    PHOTO_REQUIRED_MSG, TRANSACTION_NAMESPACE, LineInput, make_transaction_id, parse_qty,
    upsert_key, validate_submission,
)

D = date(2026, 10, 23)
PHOTO = b"\xff\xd8jpeg"


def run(distributors, skus, lines, code="D1", tanggal=D, sup="Ani"):
    return validate_submission(
        distributor_code=code, tanggal_stock=tanggal, supervisor_name=sup,
        lines=lines, distributors=distributors, skus=skus,
    )


def line(row, sku, stock="0", photo=None, photo_error=None):
    return LineInput(row, sku, stock, photo, photo_error)


@pytest.mark.parametrize("raw,expected", [
    ("0", 0), ("5", 5), (" 12 ", 12), (7, 7), (3.0, 3), ("007", 7),
])
def test_stock_valid(raw, expected):
    assert parse_qty(raw, "Stock") == (expected, None)


@pytest.mark.parametrize("raw", [-1, "-1", "1.5", "1,5", "3.0", ".5", 2.5, float("nan"), float("inf"),
                                  "", None, "abc", "1e3", "1 000", True, 2**64])
def test_stock_rejected_and_message_names_stock(raw):
    qty, err = parse_qty(raw, "Stock")
    assert qty is None and err and "Stock" in err


def test_duplicate_sku_last_row_wins_and_ignored_row_is_not_validated(distributors, skus):
    res = run(distributors, skus, [
        line(1, "TST000001", "abc"),
        line(2, "TST000001", "4", PHOTO),
    ])
    assert res.ok
    assert [(ln.row_no, ln.stock_qty) for ln in res.lines] == [(2, 4)]
    assert any("diabaikan" in w.message and w.row_no == 1 for w in res.warnings)
    assert not any("abc" in e.message or "angka" in e.message for e in res.errors)


def test_photo_rules(distributors, skus):
    zero = run(distributors, skus, [line(1, "TST000001", "0"), line(2, "TST000002", "0")])
    assert zero.ok and zero.photo_count == 0

    blocked = run(distributors, skus, [line(1, "TST000001", "2"), line(2, "TST000002", "0")])
    assert not blocked.ok
    assert any(e.message == PHOTO_REQUIRED_MSG for e in blocked.errors)

    allowed = run(distributors, skus, [line(1, "TST000001", "2"), line(2, "TST000002", "3", PHOTO)])
    assert allowed.ok
    warnings = [w for w in allowed.warnings if w.message.startswith("PERINGATAN")]
    assert [w.row_no for w in warnings] == [1]
    assert all("TST000002" not in w.message for w in warnings)


def test_qty_error_does_not_add_misleading_photo_message(distributors, skus):
    res = run(distributors, skus, [line(1, "TST000001", "abc"), line(2, "TST000002", "5")])
    assert not res.ok
    assert not any("Foto evidence wajib" in e.message for e in res.errors)
    assert any("Stock" in e.message and e.row_no == 1 for e in res.errors)


def test_transaction_id_is_deterministic_and_uses_a_new_namespace():
    assert str(TRANSACTION_NAMESPACE) == "e68edc98-2183-45c4-8648-874ffe3bb523"
    assert str(TRANSACTION_NAMESPACE) != "6f1d8c0e-5b0a-5c1e-9d3a-2a7b4e9c1f10"
    assert upsert_key(D, " D1 ", " TST000001 ") == ("2026-10-23", "D1", "TST000001")
    assert make_transaction_id(D, " D1 ", " TST000001 ") == make_transaction_id(D, "D1", "TST000001")
    base = make_transaction_id(D, "D1", "TST000001")
    assert base != make_transaction_id(D, "D2", "TST000001")
    assert base != make_transaction_id(D, "D1", "TST000002")
    assert base != make_transaction_id(date(2026, 10, 24), "D1", "TST000001")


def test_distributor_and_supervisor_required(distributors, skus):
    missing = run(distributors, skus, [line(1, "TST000001", "0")], code="NOPE", sup="  ")
    msgs = [e.message for e in missing.errors]
    assert any("Distributor" in m for m in msgs) and any("Supervisor" in m for m in msgs)


def test_unknown_sku_rejected(distributors, skus):
    res = run(distributors, skus, [line(1, "NOPE", "1", PHOTO)])
    assert not res.ok and any("tidak ada di daftar SKU" in e.message for e in res.errors)
