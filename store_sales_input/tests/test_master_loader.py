import pytest
from gspread.exceptions import WorksheetNotFound

from sheets_master import SKU_HEADERS, STORE_HEADERS, MasterError, load_master


class WS:
    def __init__(self, title, values):
        self.title, self._v = title, values

    def get_all_values(self):
        return self._v


class Sheet:
    def __init__(self, sheets):
        self.sheets = sheets

    def worksheet(self, name):
        for w in self.sheets:
            if w.title == name:
                return w
        raise WorksheetNotFound(name)

    def worksheets(self):
        return self.sheets


class GC:
    def __init__(self, sheet):
        self.sheet = sheet

    def open_by_key(self, key):
        return self.sheet


STORES = WS("Store Master", [STORE_HEADERS, ["g", "C1", "Toko A", "R", "x", "GT", "1", "Non BA"],
                              ["g", "C2", "Toko B", "R", "x", "GT", "1", "BA"]])
SKUS = WS("SKU Master", [SKU_HEADERS, ["S1", "Produk", "100"]])


def test_loads_by_configured_names():
    m = load_master(GC(Sheet([STORES, SKUS])), "id", "Store Master", "SKU Master")
    assert list(m.stores) == ["C1"] and list(m.skus) == ["S1"]


def test_detects_worksheets_by_exact_headers_when_names_differ():
    renamed = [WS("Data 1", STORES._v), WS("Data 2", SKUS._v), WS("Lain", [["a"]])]
    m = load_master(GC(Sheet(renamed)), "id", "Store Master", "SKU Master")
    assert list(m.stores) == ["C1"] and list(m.skus) == ["S1"]
    assert any("Data 1" in d for d in m.diagnostics)


def test_missing_sku_table_gives_clear_error():
    with pytest.raises(MasterError, match="SKU"):
        load_master(GC(Sheet([STORES])), "id", "Store Master", "SKU Master")


def test_open_failure_is_explained():
    class Bad:
        def open_by_key(self, key):
            raise PermissionError("403")
    with pytest.raises(MasterError, match="di-share"):
        load_master(Bad(), "id", "a", "b")
