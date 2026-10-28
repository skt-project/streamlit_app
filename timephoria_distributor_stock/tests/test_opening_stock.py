from opening_stock import lookup_ho_qty, parse_opening_rows, parse_qty_cell


def test_opening_stock_last_row_wins_and_matches_names_loosely():
    rows = [
        {"DB Name": " Gudang  Satu ", "SKU List": "K1", "Opening stock Oct": "4"},
        {"DB Name": "Gudang Satu", "SKU List": "K1", "Opening stock Oct": 8},
        {"DB Name": "Gudang Satu", "SKU List": "K2", "Opening stock Oct": ""},
        {"DB Name": "", "SKU List": "K1", "Opening stock Oct": 3},
    ]
    stocks = parse_opening_rows(rows)
    assert lookup_ho_qty(stocks, "gudang satu", "K1") == 8
    assert lookup_ho_qty(stocks, "Gudang Satu", "K2") is None
    assert parse_qty_cell("1,250") == 1250
    assert parse_qty_cell("1.5") is None
