"""NOO Detector integration — MoM 2026-08-31 §2-§5, §14, §15 (NOO tests 4-6).

Two layers: the pure scoring module (`noo_sku.noo_detector`), ported from the
standalone "Duplicate Store Checker" app rather than reimplemented; and the
pipeline wiring that writes its verdict into POOL NOO STREAMLIT column E
("NOO/Existing") without letting the admin type it.
"""
from __future__ import annotations

import pytest

from noo_sku import config, enrichment, noo_detector, pipeline
from noo_sku.customer_code import CustomerCodeResolver
from noo_sku.normalize import now_business
from tests import noo_sku_fixtures as fx

DIST = "DST082"

EXISTING_STORE = {
    "cust_id": "IESL00038", "store_name": "TOKO WILDA",
    "address": "JL MERDEKA NO 10 BANGGAI", "city": "Banggai",
    "reference_id_skt": "IESL00038",
}


def _resolver():
    return CustomerCodeResolver(dist_database={DIST: "CEC"})


def _dist_enricher():
    return enrichment.DistributorEnricher(
        master_distributor=fx.MASTER_DISTRIBUTOR,
        dist_database={DIST: fx.DISTRIBUTOR})


def _store_enricher():
    return enrichment.StoreEnricher(fx.BASIS_BY_CUST, fx.BASIS_BY_REF)


def _noo_pipeline(rows, ledger=(frozenset(), frozenset())):
    return pipeline.run_noo(
        fx.FakeParsed(rows, config.NOO_COLUMNS), distributor=fx.DISTRIBUTOR,
        resolver=_resolver(), dist_enricher=_dist_enricher(),
        store_enricher=_store_enricher(), ledger=ledger, when=now_business(),
        allowed_branches={DIST: {"name": "CV CECE"}}, company_name="CV CECE")


# ─── Pure scoring module ───────────────────────────────────────────────────
@pytest.mark.sanity
def test_exact_name_address_city_and_reference_id_scores_above_threshold():
    new_store = {"store_name": "TOKO WILDA", "address": "JL MERDEKA NO 10 BANGGAI",
                "city": "Banggai", "reference_id": "IESL00038"}
    result = noo_detector.check_reference_id(new_store, [EXISTING_STORE])
    assert result.matched is True
    assert result.score >= noo_detector.MATCH_THRESHOLD
    assert result.label == noo_detector.LABEL_REFERENCE_EXISTS


@pytest.mark.sanity
def test_completely_different_store_scores_low_and_reads_as_new():
    new_store = {"store_name": "TOKO SANGAT BERBEDA XYZ",
                "address": "JL LAIN SEKALI NO 99", "city": "Kota Lain"}
    result = noo_detector.check_reference_id(new_store, [EXISTING_STORE])
    assert result.matched is False
    assert result.label == noo_detector.LABEL_REFERENCE_NEW


@pytest.mark.sanity
def test_no_candidates_always_reads_as_new():
    result = noo_detector.check_reference_id(
        {"store_name": "TOKO APAPUN", "address": "JL X", "city": "Y"}, [])
    assert result.matched is False
    assert result.score == 0
    assert result.label == noo_detector.LABEL_REFERENCE_NEW


def test_label_text_is_exact_and_unmodified():
    """MoM §4/§5: 'Do not change capitalization, punctuation, spacing.'"""
    assert noo_detector.LABEL_REFERENCE_EXISTS == "Not NOO -> Reference ID not exist"
    assert noo_detector.LABEL_REFERENCE_NEW == "NOO -> Create ID"


@pytest.mark.sanity
def test_mapping_direction_matches_the_mom_table_exactly():
    """MoM §5 table: EXISTS -> 'Not NOO...'; DOES NOT EXIST -> 'NOO -> Create ID'.
    A reversed mapping is the single easiest mistake to make here."""
    match = noo_detector.check_reference_id(
        {"store_name": "TOKO WILDA", "address": "JL MERDEKA NO 10 BANGGAI",
         "city": "Banggai", "reference_id": "IESL00038"}, [EXISTING_STORE])
    no_match = noo_detector.check_reference_id(
        {"store_name": "TOKO BARU SAMA SEKALI", "address": "JL Z", "city": "W"},
        [EXISTING_STORE])
    assert match.label == "Not NOO -> Reference ID not exist"
    assert no_match.label == "NOO -> Create ID"


def test_threshold_is_unchanged_from_the_source_app():
    assert noo_detector.MATCH_THRESHOLD == 70


def test_max_score_without_a_store_id_is_documented_and_below_threshold():
    """The material consequence documented in the module: without Store ID,
    Name+Address+City alone cannot reach 70, because the source template
    collects neither GPS nor NIK/NPWP for the distance/identity terms."""
    perfect = noo_detector.check_reference_id(
        {"store_name": "TOKO WILDA", "address": "JL MERDEKA NO 10 BANGGAI",
         "city": "Banggai"},  # no reference_id supplied
        [EXISTING_STORE])
    assert perfect.score < noo_detector.MATCH_THRESHOLD
    assert perfect.label == noo_detector.LABEL_REFERENCE_NEW


def test_best_candidate_is_returned_not_just_a_boolean():
    result = noo_detector.check_reference_id(
        {"store_name": "TOKO WILDA", "address": "JL MERDEKA NO 10 BANGGAI",
         "city": "Banggai", "reference_id": "IESL00038"},
        [EXISTING_STORE, {"cust_id": "OTHER", "store_name": "TOKO LAIN",
                          "address": "X", "city": "Y"}])
    assert result.best["cust_id"] == "IESL00038"


# ─── Pipeline wiring — NOO test 4/5/6 ─────────────────────────────────────
@pytest.mark.sanity
def test_noo_test_5_reference_id_exists_writes_the_exact_label():
    """NOO Test 5: Reference ID found -> 'Not NOO -> Reference ID not exist'."""
    result = _noo_pipeline([fx.noo_row(store_id="IESL00038",
                                       store_code="DST08200074",
                                       name="TOKO WILDA",
                                       address="JL MERDEKA NO 10 BANGGAI",
                                       city="Banggai")])
    assert not result.errors
    row = result.pool_rows[0]
    assert row["NOO/Existing"] == "Not NOO -> Reference ID not exist"


@pytest.mark.sanity
def test_noo_test_6_reference_id_missing_writes_the_exact_label():
    """NOO Test 6: Reference ID not found -> 'NOO -> Create ID'."""
    result = _noo_pipeline([fx.noo_row(store_id="", store_code="DST08299999",
                                       name="TOKO BARU YANG BELUM PERNAH ADA",
                                       address="JL BARU NO 1",
                                       city="Kota Baru Sekali")])
    assert not result.errors
    assert result.pool_rows[0]["NOO/Existing"] == "NOO -> Create ID"


@pytest.mark.sanity
def test_column_e_is_never_taken_from_the_uploaded_file():
    """The template has no 'NOO/Existing' column at all -- confirm the value
    written is always the detector's, never something the parser could have
    read from user input."""
    assert "NOO/Existing" not in config.NOO_COLUMNS
    result = _noo_pipeline([fx.noo_row(store_code="DST08299999")])
    assert result.pool_rows[0]["NOO/Existing"] in (
        noo_detector.LABEL_REFERENCE_EXISTS, noo_detector.LABEL_REFERENCE_NEW)


@pytest.mark.sanity
def test_noo_existing_is_excluded_from_the_duplicate_hash():
    """Same reasoning as se_kae/spv/etc: the verdict is derived from master
    state at submission time and must not make an unchanged resubmission
    look different just because master data moved between two uploads."""
    from noo_sku import duplicates

    assert "NOO/Existing" not in duplicates.NOO_CONTENT_COLUMNS


def test_all_stores_unions_by_cust_and_by_ref_without_duplicates():
    """A record only reachable through _by_ref must still appear once."""
    se = enrichment.StoreEnricher(
        {"A": {"cust_id": "A", "store_name": "S1"}},
        {"skt": {"R1": [{"cust_id": "", "reference_id_skt": "R1",
                        "store_name": "S2"}]},
         "tph": {}, "fcr": {}})
    stores = se.all_stores()
    assert len(stores) == 2
    assert {s["store_name"] for s in stores} == {"S1", "S2"}


def test_detector_candidate_pool_comes_from_the_authorised_company_only():
    """Uses the same store_enricher the row's own branch enrichment used --
    scoped to the company, matching the source app's own region-scoping
    intent, never a national search."""
    import inspect

    src = inspect.getsource(pipeline.run_noo)
    assert "store_enricher.all_stores()" in src
