import pytest

from conftest import FakeGC, FakeSpreadsheet, FakeWorksheet
from stores import (StoreListError, find_header, fmt_number, load_store_list, parse_number, parse_store_list)

K = 10   # kolom K


def parse(values, **kw):
    kw.setdefault("ach_index", K)
    kw.setdefault("ach_column", "K")
    kw.setdefault("min_ach", 4)
    return parse_store_list(values, worksheet="Store List", **kw)


@pytest.mark.parametrize("raw,expected", [
    ("4", 4.0), ("4.0", 4.0), ("4,0", 4.0), (" 5 ", 5.0), ("4.00", 4.0), ("3.99", 3.99), ("1,234", 1234.0),
    ("1.234,5", 1234.5), ("1,234.5", 1234.5), (7, 7.0), (" 4", 4.0),
    ("", None), ("#N/A", None), ("80%", None), ("abc", None), (None, None), ("nan", None), ("inf", None),
])
def test_parse_number(raw, expected):
    assert parse_number(raw) == expected


def test_fmt_number():
    assert fmt_number(4.0) == "4" and fmt_number(3.5) == "3.5" and fmt_number(None) == "-"


def test_header_found_below_banner(store_values):
    hdr, pos = find_header(store_values)
    assert hdr == 2
    assert pos == {"asm": 2, "spv": 3, "store_code": 4, "store_name": 5, "region": 1}


def test_eligibility_rule_column_k_ge_4(store_values):
    sl = parse(store_values)
    asep = [s.code for s in sl.eligible_for("Asep Contoh")]
    budi = [s.code for s in sl.eligible_for("Budi Contoh")]
    # ST001 (4), ST003 (5), ST004 ("4,0"; nama ASM beda spasi/huruf) memenuhi; ST002 (3) tidak.
    assert sorted(asep) == ["ST001", "ST003", "ST004"]
    # #N/A, kosong, persen, 3.99 tidak memenuhi; 4.00 memenuhi.
    assert budi == ["ST010"]
    assert sl.eligible_for("Cici Contoh") == []
    assert len(sl.stores_for("Cici Contoh")) == 1


def test_codes_upper_and_duplicates_keep_first(store_values):
    sl = parse(store_values)
    assert sl.get("st003").code == "ST003"
    assert sl.get("ST001").name == "Toko Satu" and sl.get("ST001").ach == 4.0
    assert sl.get("ST001").sheet_row == 4
    assert any("duplikat" in d for d in sl.diagnostics)


def test_asm_names_grouped_case_insensitive(store_values):
    sl = parse(store_values)
    assert sl.asm_names() == ["Asep Contoh", "Budi Contoh", "Cici Contoh"]
    assert sl.get("ST004").asm == "asep contoh"
    assert any("tanpa nama ASM" in d for d in sl.diagnostics)


def test_diagnostics_mention_column_header_and_non_numeric(store_values):
    sl = parse(store_values)
    assert sl.ach_header == "Ach SKU (MOQ)"
    assert any("bukan angka" in d for d in sl.diagnostics)
    assert not any("PERHATIAN" in d for d in sl.diagnostics)


def test_warns_when_configured_column_is_not_ach(store_values):
    sl = parse(store_values, ach_index=9, ach_column="J")
    assert any("PERHATIAN" in d for d in sl.diagnostics)


def test_min_threshold_configurable(store_values):
    sl = parse(store_values, min_ach=5)
    assert [s.code for s in sl.eligible_for("Asep Contoh")] == ["ST003"]


def test_header_override(store_values):
    values = [list(r) for r in store_values]
    values[2][2] = "Area Manager X"
    with pytest.raises(StoreListError):
        parse(values)
    sl = parse(values, overrides={"asm": "area manager x"})
    assert "Asep Contoh" in sl.asm_names()


def test_optional_columns_missing():
    values = [["ASM", "Kode Toko", "", "", "", "", "", "", "", "", "Ach SKU (MOQ)"],
              ["Asep", "A1", "", "", "", "", "", "", "", "", "4"]]
    sl = parse(values)
    s = sl.get("A1")
    assert s.name == "" and s.spv == "" and s.region == "" and s.label == "A1"


def test_short_row_without_column_k_is_not_eligible():
    values = [["ASM", "Kode Toko"], ["Asep", "A1"]]
    sl = parse(values)
    assert sl.eligible_for("Asep") == [] and sl.ach_header == ""


def test_load_scans_tabs_and_uses_first_with_header(store_values):
    sh = FakeSpreadsheet([FakeWorksheet("Summary", [["Total", "123"]]), FakeWorksheet("Data", store_values)])
    sl = load_store_list(FakeGC({"sid": sh}), "sid", worksheet="", ach_index=K, ach_column="K", min_ach=4)
    assert sl.worksheet == "Data" and len(sl.eligible_for("Asep Contoh")) == 3


def test_load_named_tab_missing_or_unreadable(store_values):
    sh = FakeSpreadsheet([FakeWorksheet("Data", store_values)])
    gc = FakeGC({"sid": sh})
    with pytest.raises(StoreListError, match="Tab 'Nope'"):
        load_store_list(gc, "sid", worksheet="Nope", ach_index=K, ach_column="K", min_ach=4)
    with pytest.raises(StoreListError, match="di-share"):
        load_store_list(gc, "other", worksheet="", ach_index=K, ach_column="K", min_ach=4)
    empty = FakeGC({"sid": FakeSpreadsheet([FakeWorksheet("X", [["a"]])])})
    with pytest.raises(StoreListError, match="Tidak ada tab"):
        load_store_list(empty, "sid", worksheet="", ach_index=K, ach_column="K", min_ach=4)
