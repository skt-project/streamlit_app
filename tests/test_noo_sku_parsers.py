"""Parsing and wrong-template detection against the real template layouts."""
from __future__ import annotations

import pytest

from noo_sku import config, parsers
from tests import noo_sku_fixtures as fx


@pytest.mark.sanity
def test_noo_template_parses_and_skips_banner_and_example():
    parsed = parsers.parse_upload(fx.noo_workbook([fx.noo_row(), fx.noo_row()]))
    assert parsed.kind == parsers.UPLOAD_NOO
    assert parsed.header_row == config.NOO_HEADER_ROW
    assert len(parsed.rows) == 2, "banner and CONTOH row must not become data"
    assert parsed.row_numbers == [4, 5], "row numbers must match what Excel shows"
    assert parsed.rows[0]["Store Name"] == "TOKO SUMBER REJEKI"


@pytest.mark.sanity
def test_sku_template_parses_with_blank_row_and_two_example_rows():
    parsed = parsers.parse_upload(fx.sku_workbook([fx.sku_row()]))
    assert parsed.kind == parsers.UPLOAD_SKU
    assert parsed.header_row == config.SKU_HEADER_ROW
    assert len(parsed.rows) == 1
    assert parsed.rows[0]["Principal Product Code"] == "SKINTIFIC-296"


@pytest.mark.sanity
def test_sku_sample_values_row_is_not_ingested_as_data():
    """Regression: the SKU template puts CONTOH alone on one row and the sample
    values on the next. Reading that as data would write BD Support's example
    mapping (TYY114002) into the pool on every upload."""
    parsed = parsers.parse_upload(fx.sku_workbook([fx.sku_row()]))
    codes = [r["Principal Product Code"] for r in parsed.rows]
    assert "TYY114002" not in codes
    assert codes == ["SKINTIFIC-296"]


@pytest.mark.sanity
def test_noo_sample_row_shares_its_row_with_contoh_and_is_dropped():
    parsed = parsers.parse_upload(fx.noo_workbook([fx.noo_row()]))
    names = [r["Store Name"] for r in parsed.rows]
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
    headers.remove("Customer Store Code")
    rows = [fx.noo_row()[:6] + fx.noo_row()[7:]]
    parsed = parsers.parse_upload(fx.noo_workbook(rows, headers=headers))
    assert parsers.missing_columns(parsed, parsers.UPLOAD_NOO) == [
        "Customer Store Code"]


def test_header_matching_tolerates_trailing_space_and_parentheses():
    # The real sheet has 'Customer Code ' with a trailing space, and the SKU
    # headers carry '( Di isi oleh Distributor)' hints.
    parsed = parsers.parse_upload(fx.sku_workbook([fx.sku_row()]))
    assert parsers.missing_columns(parsed, parsers.UPLOAD_SKU) == []
    lookup = parsers.column_lookup(parsed, parsers.UPLOAD_SKU)
    assert lookup["Principal Product Code"] == "Principal Product Code"


@pytest.mark.sanity
def test_parsed_rows_are_keyed_by_canonical_column_names():
    """Regression: the sheet header 'Customer Product Name  (...)' has a double
    space that clean() collapses, so raw header text can never be used as a key.
    Parsed rows must come back keyed exactly as the validators address them."""
    from noo_sku import validators
    from tests import noo_sku_fixtures as fixtures

    parsed = parsers.parse_upload(fx.sku_workbook([fx.sku_row()]))
    assert set(config.SKU_COLUMNS) <= set(parsed.rows[0])
    # And the validator must find every column when fed straight from the parser.
    issues, _ = validators.validate_sku(
        parsed.rows, parsed.row_numbers, distributor_code="DST082",
        product_lookup=fixtures.PRODUCTS)
    assert validators.split_severity(issues)[0] == []


@pytest.mark.sanity
def test_noo_parsed_rows_feed_the_validator_without_key_mismatch():
    from noo_sku import validators

    parsed = parsers.parse_upload(fx.noo_workbook([fx.noo_row()]))
    assert set(config.NOO_COLUMNS) <= set(parsed.rows[0])
    issues, cleaned = validators.validate_noo(
        parsed.rows, parsed.row_numbers, distributor_code="DST082",
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
        fx.noo_workbook([fx.noo_row(), [""] * 10, fx.noo_row(name="TOKO DUA")]))
    assert len(parsed.rows) == 2
    assert parsed.row_numbers == [4, 6], "row 5 was blank and must be skipped"
