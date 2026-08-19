"""Customer Code, validation, duplicate classification and safe-write tests.

No credentials, no network — everything here runs against pure functions.
"""
from __future__ import annotations

import pytest

from noo_sku import config, duplicates, validators, writer
from noo_sku.customer_code import (SOURCE_DIST_DATABASE, SOURCE_PO_HISTORY,
                                   CustomerCodeResolver, brand_for_prefix,
                                   split_customer_code)
from noo_sku.normalize import (clean, format_date_noo, format_date_sku,
                               norm_header, now_business, row_hash)
from tests import noo_sku_fixtures as fx

DIST = "DST082"
NAME = "CV CECE"


def _resolver(**kw):
    kw.setdefault("dist_database", {DIST: "CEC"})
    return CustomerCodeResolver(**kw)


# ─── Customer Code ────────────────────────────────────────────────────────────
@pytest.mark.sanity
@pytest.mark.parametrize("brand,expected", [
    ("SKINTIFIC", "11CEC"), ("TIMEPHORIA", "13CEC"), ("FACERINNA", "1ACEC"),
])
def test_customer_code_is_brand_prefix_plus_suffix(brand, expected):
    assert _resolver().customer_code(DIST, brand) == expected


@pytest.mark.sanity
def test_mom_worked_examples_reproduce():
    # The MoM cites 11CEC (CV CECE) and 1AKAS (Karya Ananda Sukses).
    r = CustomerCodeResolver(dist_database={"DST082": "CEC", "DST164": "KAS"})
    assert r.customer_code("DST082", "SKINTIFIC") == "11CEC"
    assert r.customer_code("DST164", "FACERINNA") == "1AKAS"


def test_out_of_scope_brand_has_no_customer_code():
    assert _resolver().customer_code(DIST, "G2G") is None


@pytest.mark.sanity
def test_dist_database_wins_over_po_history():
    r = CustomerCodeResolver(dist_database={DIST: "CEC"},
                             po_history={DIST: "WRONG"})
    resolution = r.resolve(DIST)
    assert resolution.suffix == "CEC"
    assert resolution.source == SOURCE_DIST_DATABASE
    assert resolution.conflict is True, "a disagreement must stay visible"


def test_po_history_fills_gaps_left_by_the_sheet():
    r = CustomerCodeResolver(dist_database={}, po_history={DIST: "CEC"})
    resolution = r.resolve(DIST)
    assert resolution.suffix == "CEC"
    assert resolution.source == SOURCE_PO_HISTORY


@pytest.mark.sanity
def test_unresolved_distributor_yields_no_code_rather_than_a_guess():
    resolution = CustomerCodeResolver().resolve("DST999")
    assert resolution.resolved is False
    assert resolution.customer_code("SKINTIFIC") is None


def test_override_beats_every_other_source():
    r = CustomerCodeResolver(dist_database={DIST: "CEC"},
                             overrides={DIST: "NEW"})
    assert r.resolve(DIST).suffix == "NEW"


def test_coverage_report_separates_resolved_from_unresolved():
    r = _resolver()
    report = r.coverage([DIST, "DST999"])
    assert report["resolved"] == [DIST]
    assert report["unresolved"] == ["DST999"]


@pytest.mark.parametrize("code,brand", [
    ("11CEC", "SKINTIFIC"), ("13KAS", "TIMEPHORIA"), ("1AMTP", "FACERINNA"),
    ("12UDN", None), ("17KOK", None),
])
def test_brand_lookup_from_customer_code(code, brand):
    assert brand_for_prefix(code) == brand


def test_split_customer_code_rejects_unknown_prefix():
    assert split_customer_code("99XYZ") == (None, None)


# ─── Normalisation ────────────────────────────────────────────────────────────
@pytest.mark.sanity
@pytest.mark.parametrize("a,b", [
    ("TOKO  JAYA", "toko jaya"), (" DST082 ", "dst082"), ("nan", ""),
    ("None", ""), ("123.0", "123"),
])
def test_blank_and_spacing_variants_collapse_identically(a, b):
    assert row_hash([a]) == row_hash([b])


def test_hash_is_order_sensitive():
    assert row_hash(["A", "B"]) != row_hash(["B", "A"])


def test_header_normalisation_strips_parentheses_and_trailing_space():
    assert norm_header("Customer Code ") == "customer code"
    assert norm_header("Customer Product Code ( Di isi oleh Distributor)") == \
        "customer product code"


def test_dates_use_the_format_each_destination_tab_already_uses():
    from datetime import date

    assert format_date_sku(date(2026, 3, 7)) == "3/7/2026"
    assert format_date_noo(date(2026, 3, 7)) == "07-Mar-2026"


@pytest.mark.sanity
def test_business_time_is_jakarta_not_utc():
    assert now_business().utcoffset().total_seconds() == 7 * 3600


# ─── NOO validation ───────────────────────────────────────────────────────────
def _noo(rows, suffix="CEC", cities=None):
    parsed = [dict(zip(config.NOO_COLUMNS, r)) for r in rows]
    numbers = list(range(4, 4 + len(rows)))
    return validators.validate_noo(parsed, numbers, distributor_code=DIST,
                                   distributor_name=NAME,
                                   expected_suffix=suffix,
                                   known_cities=cities)


@pytest.mark.sanity
def test_valid_noo_row_passes():
    issues, _ = _noo([fx.noo_row()])
    assert validators.split_severity(issues)[0] == []


@pytest.mark.sanity
def test_blank_store_id_is_allowed_because_the_column_is_optional():
    issues, _ = _noo([fx.noo_row(store_id="")])
    assert not [i for i in issues if i.column == "Store ID"]


def test_malformed_store_id_is_flagged_but_blank_is_not():
    issues, _ = _noo([fx.noo_row(store_id="IEBB1")])
    assert any(i.column == "Store ID" for i in issues)


def test_missing_customer_store_code_is_an_error_with_a_row_number():
    issues, _ = _noo([fx.noo_row(store_code="")])
    err = [i for i in issues if i.column == "Customer Store Code"]
    assert err and err[0].row == 4 and err[0].severity == validators.ERROR


@pytest.mark.sanity
def test_customer_store_code_must_carry_the_session_distributor_code():
    issues, _ = _noo([fx.noo_row(store_code="00011")])
    assert any("tidak diawali" in i.problem
               for i in issues if i.column == "Customer Store Code")


def test_channel_outside_gt_mti_is_rejected():
    issues, _ = _noo([fx.noo_row(channel="Gt Modern", store_type="")])
    assert any(i.column == "Channel" for i in issues)


@pytest.mark.sanity
def test_customer_code_from_another_distributor_is_rejected():
    issues, _ = _noo([fx.noo_row(customer_code="11ABC")])
    assert any("bukan milik" in i.problem
               for i in issues if i.column == "Customer Code")


def test_store_type_must_be_legal_for_the_chosen_channel():
    issues, _ = _noo([fx.noo_row(channel="MTI", store_type="Cosmetic Store")])
    assert any(i.column == "Store Type" for i in issues)
    issues, _ = _noo([fx.noo_row(channel="MTI", store_type="Minimarket")])
    assert not [i for i in issues if i.column == "Store Type"]


def test_unknown_city_warns_but_does_not_block():
    issues, _ = _noo([fx.noo_row(city="Kota Antah Berantah")],
                     cities={"Banggai"})
    city_issues = [i for i in issues if i.column == "City"]
    assert city_issues and city_issues[0].severity == validators.WARNING


@pytest.mark.sanity
def test_distributor_code_in_file_that_differs_from_login_is_rejected():
    """Brief §4: login DST121 + file DST082 must ERROR, never silently switch."""
    issues, cleaned = _noo([fx.noo_row(branch_code="DST999")])
    mismatch = [i for i in issues if i.column == "Customer Branch Code"]
    assert mismatch and mismatch[0].severity == validators.ERROR
    # Session identity still wins on the cleaned row — never the file's value.
    assert cleaned[0]["Customer Branch Code"] == DIST


@pytest.mark.sanity
def test_matching_distributor_code_in_file_passes():
    issues, _ = _noo([fx.noo_row(branch_code=DIST)])
    assert not [i for i in issues if i.column == "Customer Branch Code"]


def test_branch_name_mismatch_is_a_hard_error_and_never_silently_accepted():
    """Decision B: Branch Name is system-authoritative. A disagreeing value in
    the file is rejected, and the written value still comes from the session."""
    issues, cleaned = _noo([fx.noo_row(branch="PT PENYUSUP")])
    err = [i for i in issues if i.column == "Branch Name"]
    assert err and err[0].severity == validators.ERROR
    assert cleaned[0]["Branch Name"] == NAME


def test_matching_branch_name_passes():
    issues, _ = _noo([fx.noo_row(branch=NAME)])
    assert not [i for i in issues if i.column == "Branch Name"]


def test_unresolved_suffix_produces_a_helpful_hint():
    issues, _ = _noo([fx.noo_row(customer_code="")], suffix=None)
    hint = [i for i in issues if i.column == "Customer Code"][0]
    assert "BD Support" in hint.suggestion


# ─── SKU validation ───────────────────────────────────────────────────────────
def _sku(rows, **kw):
    parsed = [dict(zip(config.SKU_COLUMNS, r)) for r in rows]
    numbers = list(range(6, 6 + len(rows)))
    return validators.validate_sku(parsed, numbers, distributor_code=DIST,
                                   product_lookup=fx.PRODUCTS, **kw)


@pytest.mark.sanity
def test_valid_sku_row_passes_and_carries_its_brand():
    issues, cleaned = _sku([fx.sku_row()])
    assert validators.split_severity(issues)[0] == []
    assert cleaned[0]["_brand"] == "SKINTIFIC"


@pytest.mark.sanity
def test_unknown_principal_sku_is_an_error():
    issues, _ = _sku([fx.sku_row(code="SKINTIFIC-999")])
    assert any("tidak ditemukan" in i.problem for i in issues)


def test_out_of_scope_brand_sku_is_rejected():
    issues, _ = _sku([fx.sku_row(code="G2G-74", name="GLAD2GLOW TEST",
                                 size="300g")])
    assert any("di luar cakupan" in i.problem for i in issues)


def test_missing_db_columns_are_errors():
    issues, _ = _sku([fx.sku_row(db_code="", db_name="")])
    cols = {i.column for i in issues}
    assert {"Customer Product Code", "Customer Product Name"} <= cols


def test_name_and_size_mismatches_warn_by_default_and_can_be_made_strict():
    rows = [fx.sku_row(name="NAMA SALAH", size="99ml")]
    issues, _ = _sku(rows)
    assert all(i.severity == validators.WARNING for i in issues)
    strict, _ = _sku(rows, strict_names=True, strict_size=True)
    assert all(i.severity == validators.ERROR for i in strict)
