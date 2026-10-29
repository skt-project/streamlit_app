from opening_stock import lookup_ho_qty, parse_opening_rows, parse_qty_cell, spu_for_code


def test_opening_stock_is_grouped_by_spu_and_last_row_wins():
    rows = [
        {"DB Name": " Gudang  Satu ", "SPU": "GRP1", "SPU NAME": "Grup Pendek", "Ending Stock Oct": "4"},
        {"DB Name": "Gudang Satu", "SPU": "GRP1", "SPU NAME": "Grup Satu", "Ending Stock Oct": 8},
        {"DB Name": "Gudang Satu", "SPU": "GRP10", "SPU NAME": "Grup Sepuluh", "Ending Stock Oct": 3},
        {"DB Name": "Gudang Satu", "SPU": "GRP2", "SPU NAME": "Grup Dua", "Ending Stock Oct": ""},
        {"DB Name": "", "SPU": "GRP1", "SPU NAME": "Grup Satu", "Ending Stock Oct": 9},
    ]
    stocks, spus, names = parse_opening_rows(rows)
    assert spus == ["GRP1", "GRP10", "GRP2"]
    assert names["GRP1"] == "Grup Satu"
    assert spu_for_code("GRP10A01", spus) == "GRP10"
    assert spu_for_code("GRP1A002", spus) == "GRP1"
    assert lookup_ho_qty(stocks, "gudang satu", "GRP1A002", spus) == 8
    assert lookup_ho_qty(stocks, "Gudang Satu", "GRP2001", spus) is None
    assert parse_qty_cell("1,250") == 1250
    assert parse_qty_cell("1.5") is None
