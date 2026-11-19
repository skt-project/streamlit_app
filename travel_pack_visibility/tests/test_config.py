import pytest

from config import ConfigError, column_index, extract_sheet_id, load_config

BASE = {"store_sheet_id": "https://docs.google.com/spreadsheets/d/abc_DEF-1/edit#gid=0",
        "output_sheet_id": "out123", "evidence_bucket": "my-bucket"}


def test_defaults_and_url_extraction():
    cfg = load_config({"app": dict(BASE)})
    assert cfg.store_sheet_id == "abc_DEF-1" and cfg.output_sheet_id == "out123"
    assert cfg.ach_column == "K" and cfg.ach_index == 10 and cfg.min_ach == 4
    assert cfg.output_worksheet == "TRAVEL_PACK_UPLOAD_LOG" and cfg.header_overrides == {}


def test_overrides_and_custom_values():
    cfg = load_config({"app": {**BASE, "ach_sku_column": "l", "min_ach_sku": "5", "asm_header": "Nama ASM",
                               "store_worksheet": "List"}})
    assert cfg.ach_column == "L" and cfg.min_ach == 5.0 and cfg.store_worksheet == "List"
    assert cfg.header_overrides == {"asm": "Nama ASM"}


@pytest.mark.parametrize("patch,msg", [
    ({"store_sheet_id": ""}, "store_sheet_id"),
    ({"evidence_bucket": "Bad Bucket"}, "bucket"),
    ({"ach_sku_column": "K1"}, "ach_sku_column"),
    ({"min_ach_sku": "empat"}, "min_ach_sku"),
])
def test_invalid(patch, msg):
    with pytest.raises(ConfigError, match=msg):
        load_config({"app": {**BASE, **patch}})


def test_missing_section():
    with pytest.raises(ConfigError):
        load_config({})


def test_helpers():
    assert column_index("A") == 0 and column_index("K") == 10 and column_index("AA") == 26
    assert extract_sheet_id("  plainid ") == "plainid"
