"""Parsing and wrong-template detection against the real template layouts
(2026-09-07 REVISI — Indonesian headers).

Rows come back from `parsers.parse_upload` keyed by the template's own
Indonesian header text; `parsers.translate_to_internal` is what re-keys them
onto the existing internal canonical names validators.py addresses. Tests
that feed parsed rows into a validator therefore call it explicitly, exactly
like noo_sku_mapping.py / scripts/run_noo_sku_uat.py do in production.
"""
from __future__ import annotations

import pytest

from noo_sku import config, parsers
from tests import noo_sku_fixtures as fx


@pytest.mark.sanity
def test_noo_template_parses_and_skips_banner_and_worked_example():
    parsed = parsers.parse_upload(fx.noo_workbook([fx.noo_row(), fx.noo_row()]))
    assert parsed.kind == parsers.UPLOAD_NOO
    assert parsed.header_row == config.NOO_HEADER_ROW
    assert len(parsed.rows) == 2, "banner and worked-example row must not become data"
    assert parsed.row_numbers == [4, 5], "row numbers must match what Excel shows"
    assert parsed.rows[0]["Nama Toko"] == "TOKO SUMBER REJEKI"


@pytest.mark.sanity
def test_sku_template_parses_with_blank_row_and_legacy_example_rows():
    parsed = parsers.parse_upload(fx.sku_workbook([fx.sku_row()]))
    assert parsed.kind == parsers.UPLOAD_SKU
    assert parsed.header_row == config.SKU_HEADER_ROW
    assert len(parsed.rows) == 1
    assert parsed.rows[0]["Kode SKU Prinsipal"] == "SKINTIFIC-296"


@pytest.mark.sanity
def test_sku_sample_values_row_is_not_ingested_as_data():
    """Regression: a legacy-shaped SKU file puts CONTOH alone on one row and
    the sample values on the next. Reading that as data would write BD
    Support's example mapping (TYY114002) into the pool on every upload."""
    parsed = parsers.parse_upload(fx.sku_workbook([fx.sku_row()]))
    codes = [r["Kode SKU Prinsipal"] for r in parsed.rows]
    assert "TYY114002" not in codes
    assert codes == ["SKINTIFIC-296"]


@pytest.mark.sanity
def test_noo_builtin_worked_example_has_no_contoh_marker_but_is_still_dropped():
    """2026-09-07: the revised NOO template's worked example carries no
    literal CONTOH cell any more (that marker lived in the now-removed Store
    ID column) - it must still be recognised and dropped, by content
    (config.NOO_BUILTIN_EXAMPLE), not by a marker that no longer exists."""
    parsed = parsers.parse_upload(fx.noo_workbook([fx.noo_row()]))
    names = [r["Nama Toko"] for r in parsed.rows]
    assert "TOKO JAYA KOSMETIK" not in names


@pytest.mark.sanity
def test_sku_template_uploaded_into_noo_section_is_rejected():
    parsed = parsers.parse_upload(fx.sku_workbook([fx.sku_row()]))
    message = parsers.check_template_kind(parsed, parsers.UPLOAD_NOO)
    assert message and "SKU Mapping" in message


@pytest.mark.sanity
def test_noo_template_uploaded_into_sku_section_is_rejected():
    parsed = parsers.parse_upload(fx.noo_workbook([fx.noo_row()]))
    message = parsers.check_template_kind(parsed, parsers.UPLOAD_SKU)
    assert message and "NOO Mapping" in message


def test_matching_template_produces_no_wrong_template_error():
    parsed = parsers.parse_upload(fx.noo_workbook([fx.noo_row()]))
    assert parsers.check_template_kind(parsed, parsers.UPLOAD_NOO) is None


@pytest.mark.sanity
def test_missing_required_column_is_named():
    headers = list(config.NOO_COLUMNS)
    drop_at = headers.index("Kode Toko Pelanggan")
    headers.remove("Kode Toko Pelanggan")
    row = fx.noo_row()[:drop_at] + fx.noo_row()[drop_at + 1:]
    parsed = parsers.parse_upload(fx.noo_workbook([row], headers=headers))
    assert parsers.missing_columns(parsed, parsers.UPLOAD_NOO) == [
        "Kode Toko Pelanggan"]


def test_column_lookup_finds_every_column_on_a_clean_file():
    parsed = parsers.parse_upload(fx.sku_workbook([fx.sku_row()]))
    assert parsers.missing_columns(parsed, parsers.UPLOAD_SKU) == []
    lookup = parsers.column_lookup(parsed, parsers.UPLOAD_SKU)
    assert lookup["Kode SKU Prinsipal"] == "Kode SKU Prinsipal"


@pytest.mark.sanity
def test_parsed_rows_translate_to_the_internal_names_the_validator_expects():
    """Regression: raw parsed rows are keyed by the Indonesian template
    header, not by the internal canonical names validators.py addresses
    ("Principal Product Code", etc) - translate_to_internal is the one
    required step in between."""
    from noo_sku import validators

    parsed = parsers.parse_upload(fx.sku_workbook([fx.sku_row()]))
    assert set(config.SKU_COLUMNS) <= set(parsed.rows[0])

    translated = parsers.translate_to_internal(parsed, parsers.UPLOAD_SKU)
    assert set(config.SKU_INTERNAL_COLUMNS) <= set(translated.rows[0])
    issues, _ = validators.validate_sku(
        translated.rows, translated.row_numbers, distributor_code="DST082",
        product_lookup=fx.PRODUCTS)
    assert validators.split_severity(issues)[0] == []


@pytest.mark.sanity
def test_noo_parsed_rows_translate_to_the_internal_names_the_validator_expects():
    from noo_sku import validators

    parsed = parsers.parse_upload(fx.noo_workbook([fx.noo_row()]))
    assert set(config.NOO_COLUMNS) <= set(parsed.rows[0])

    translated = parsers.translate_to_internal(parsed, parsers.UPLOAD_NOO)
    assert set(config.NOO_INTERNAL_COLUMNS) <= set(translated.rows[0])
    issues, cleaned = validators.validate_noo(
        translated.rows, translated.row_numbers, distributor_code="DST082",
        distributor_name="CV CECE", expected_suffix="CEC")
    assert validators.split_severity(issues)[0] == []
    assert cleaned[0]["Customer Branch Code"] == "DST082"


def test_empty_file_yields_no_rows():
    parsed = parsers.parse_upload(fx.noo_workbook([]))
    assert parsed.rows == []


def test_unreadable_file_raises_parse_error_not_traceback():
    import io

    with pytest.raises(parsers.ParseError):
        parsers.parse_upload(io.BytesIO(b"this is not a workbook"))


def test_unrecognised_template_raises_parse_error():
    import io
    import openpyxl

    wb = openpyxl.Workbook()
    wb.active.append(["Nama", "Alamat", "Kota"])
    wb.active.append(["A", "B", "C"])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    with pytest.raises(parsers.ParseError):
        parsers.parse_upload(buf)


def test_blank_rows_between_data_are_skipped_but_numbering_stays_true():
    parsed = parsers.parse_upload(
        fx.noo_workbook([fx.noo_row(), [""] * 9, fx.noo_row(name="TOKO DUA")]))
    assert len(parsed.rows) == 2
    assert parsed.row_numbers == [4, 6], "row 5 was blank and must be skipped"
