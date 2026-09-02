"""NOO Detector — direct Reference-ID lookup (fixed 2026-09-03).

Replaces the earlier fuzzy-scoring version (Name/Address/City/GPS/NIK/NPWP,
threshold >=70), which the 2026-09-03 MoM identified as producing wrong
results for the common case: the real NOO template collects no GPS/NIK/NPWP,
so a genuinely existing store with a blank Store ID could score as low as 65
-- always under the 70 cutoff -- and misclassify as new. See
`noo_sku/noo_detector.py`'s module docstring for the full history.

The fix: reuse the SAME composite-key lookup (`enrichment.StoreEnricher`,
already verified at 99.9% resolution on real data) that already runs for
SE/SPV/AOM enrichment. No fuzzy matching, no second query, no threshold.
"""
from __future__ import annotations

import pytest

from noo_sku import config, duplicates, enrichment, noo_detector, pipeline
from noo_sku.customer_code import CustomerCodeResolver
from noo_sku.normalize import now_business
from tests import noo_sku_fixtures as fx

DIST = "DST082"


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


# ─── classify() — pure, given an already-resolved StoreEnricher result ───────
@pytest.mark.sanity
def test_matched_store_is_not_noo_and_carries_the_masters_own_store_id():
    result = enrichment.EnrichmentResult(matched=True, ambiguous=False,
                                         resolved_store_id="IESL00038")
    verdict = noo_detector.classify(result)
    assert verdict.matched is True
    assert verdict.label == "Not NOO -> Reference ID not exist"
    assert verdict.store_id == "IESL00038"


@pytest.mark.sanity
def test_unmatched_store_is_noo_with_blank_store_id():
    result = enrichment.EnrichmentResult(matched=False, ambiguous=False,
                                         resolved_store_id="")
    verdict = noo_detector.classify(result)
    assert verdict.matched is False
    assert verdict.label == "NOO -> Create ID"
    assert verdict.store_id == ""


@pytest.mark.sanity
def test_ambiguous_match_is_treated_as_not_found_never_guessed():
    """An ambiguous reference id must never produce a store_id -- picking one
    of several candidates would silently attach the wrong store."""
    result = enrichment.EnrichmentResult(matched=True, ambiguous=True,
                                         resolved_store_id="")
    verdict = noo_detector.classify(result)
    assert verdict.matched is False
    assert verdict.label == "NOO -> Create ID"
    assert verdict.store_id == ""


def test_label_text_is_the_exact_historical_wording():
    """Confirmed against real production data: 2,200 / 1,657 rows respectively
    in SKINTIFIC NEW as of the 2026-08-19 audit. The fix is to the matching
    method, not this text -- do not reword it."""
    assert noo_detector.LABEL_REFERENCE_EXISTS == "Not NOO -> Reference ID not exist"
    assert noo_detector.LABEL_REFERENCE_NEW == "NOO -> Create ID"


def test_mapping_direction_is_not_reversed():
    found = noo_detector.classify(enrichment.EnrichmentResult(
        matched=True, ambiguous=False, resolved_store_id="X"))
    not_found = noo_detector.classify(enrichment.EnrichmentResult(
        matched=False, ambiguous=False))
    assert found.label == "Not NOO -> Reference ID not exist"
    assert not_found.label == "NOO -> Create ID"


def test_classify_performs_no_lookup_of_its_own():
    """It must only read what StoreEnricher already computed -- no new query,
    no scoring, so it cannot reintroduce the threshold problem it fixed."""
    import inspect

    src = inspect.getsource(noo_detector.classify)
    for forbidden in ("fuzz.", "score", "threshold", "rapidfuzz"):
        assert forbidden not in src.lower()


# ─── Pipeline wiring — NOO tests 1 & 2 from the 2026-09-03 MoM ───────────────
@pytest.mark.sanity
def test_noo_test_1_existing_store_is_not_noo_and_auto_populates_store_id():
    """Reference ID resolves via master_store_database_basis (cust_id
    IESL00038) -> Not NOO, and the pool's store_id is auto-filled from the
    MASTER's own value, regardless of what (if anything) was typed."""
    result = _noo_pipeline([fx.noo_row(store_id="IESL00038",
                                       store_code="DST08200074",
                                       name="TOKO APAPUN NAMANYA")])
    assert not result.errors
    row = result.pool_rows[0]
    assert row["NOO/Existing"] == "Not NOO -> Reference ID not exist"
    assert row["store_id"] == "IESL00038"


@pytest.mark.sanity
def test_noo_test_2_new_store_is_noo_with_blank_store_id():
    """Reference ID does not resolve -> NOO, store_id stays blank -- never a
    fake or generated identifier."""
    result = _noo_pipeline([fx.noo_row(store_id="", store_code="DST08299999",
                                       name="TOKO YANG BENAR BENAR BARU")])
    assert not result.errors
    row = result.pool_rows[0]
    assert row["NOO/Existing"] == "NOO -> Create ID"
    assert row["store_id"] == ""


@pytest.mark.sanity
def test_a_typed_store_id_that_does_not_resolve_is_not_trusted_verbatim():
    """A wrong/nonexistent Store ID typed by the admin must NOT be copied into
    the pool -- it is looked up, and since it does not resolve, store_id stays
    blank rather than propagating an unverified value."""
    result = _noo_pipeline([fx.noo_row(store_id="IEXX99999",
                                       store_code="DST08299999",
                                       name="TOKO DENGAN ID SALAH KETIK")])
    assert result.pool_rows[0]["store_id"] == ""
    assert result.pool_rows[0]["NOO/Existing"] == "NOO -> Create ID"


@pytest.mark.sanity
def test_store_id_and_noo_existing_are_never_taken_from_the_uploaded_file():
    """Neither column exists in the upload template at all."""
    assert "store_id" not in config.NOO_COLUMNS
    assert "NOO/Existing" not in config.NOO_COLUMNS
    assert "Store ID (Opsional)" in config.NOO_COLUMNS  # the admin's INPUT field


@pytest.mark.sanity
def test_store_id_and_noo_existing_are_excluded_from_the_duplicate_hash():
    """Both are derived from master state at submission time (same reasoning
    as se_kae/spv/etc): a store landing in the master between two uploads of
    otherwise-identical business data must not read as a spurious CORRECTION."""
    assert "NOO/Existing" not in duplicates.NOO_CONTENT_COLUMNS
    assert "store_id" not in duplicates.NOO_CONTENT_COLUMNS


@pytest.mark.sanity
def test_reference_id_exists_count_reflects_the_pipeline_result():
    result = _noo_pipeline([
        fx.noo_row(store_id="IESL00038", store_code="DST08200074",
                  name="TOKO SATU"),
        fx.noo_row(store_id="", store_code="DST08299998", name="TOKO DUA"),
    ])
    assert result.reference_id_exists_count == 1


def test_ambiguous_reference_id_never_populates_store_id_end_to_end():
    """DST08200099 is fixture-configured to resolve to two basis rows."""
    result = _noo_pipeline([fx.noo_row(store_id="", store_code="DST08200099",
                                       name="TOKO AMBIGU")])
    row = result.pool_rows[0]
    assert row["store_id"] == ""
    assert row["NOO/Existing"] == "NOO -> Create ID"


# ─── Separation of concerns (MoM §17) ────────────────────────────────────────
@pytest.mark.sanity
def test_noo_detection_and_duplicate_detection_stay_independent():
    """A row can be an EXACT_DUPLICATE (already uploaded) while its NOO
    Detector verdict is independently computed from master data -- the two
    checks must not be conflated into one."""
    result = _noo_pipeline([fx.noo_row(store_id="IESL00038",
                                       store_code="DST08200074",
                                       name="TOKO SATU")])
    content = duplicates.noo_content(result.pool_rows[0])
    again = _noo_pipeline(
        [fx.noo_row(store_id="IESL00038", store_code="DST08200074",
                   name="TOKO SATU")],
        ledger=(frozenset(), {content}))
    assert again.classifications[0].bucket == duplicates.EXACT_DUPLICATE
    # The NOO Detector still ran and still produced the correct verdict --
    # duplicate-skipping happens later, in the writer, not by short-circuiting
    # detection.
    assert again.pool_rows[0]["NOO/Existing"] == "Not NOO -> Reference ID not exist"
