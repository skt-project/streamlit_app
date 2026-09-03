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


# ─── Regression: multi-branch store-basis scoping (2026-09-03) ───────────────
# Root cause: `_store_basis()` / `load_store_basis()` were scoped to only the
# LOGGED-IN admin's own branch code, not every branch they are authorised for.
# A multi-branch upload naming a SIBLING branch could therefore never find
# that branch's stores in master_store_database_basis at all -- the detector
# then correctly reported "not found" for data it was never given. Confirmed
# live against the two reported reference ids (DST333KLK8500100,
# DST332DPS3200142, both real rows of PT SINAR MAYURI, branches DST333/DST332)
# before any code changed; the fixtures below reproduce the same shape.
KLUNGKUNG_STORE = {
    "cust_id": "IEBU00061", "store_name": "IRMA TOKO",
    "address": "JL. KLUNGKUNG", "city": "Klungkung",
    "reference_id_skt": "DST333KLK8500100", "reference_id_tph": "DST333KLK8500100",
}
DENPASAR_STORE = {
    "cust_id": "IEBD00039", "store_name": "RAHMA TOKO",
    "address": "JL. BATUKARU, TABANAN", "city": "Tabanan",
    "reference_id_skt": "DST332DPS3200142", "reference_id_tph": "DST332DPS3200142",
}

# Scoped to ONLY the login branch (DST334) -- reproduces the bug: neither
# sibling store is present, exactly as `load_store_basis(..., [login])` used
# to return before the fix.
_LOGIN_ONLY_BY_CUST = {}
_LOGIN_ONLY_BY_REF = {"skt": {}, "tph": {}, "fcr": {}}

# Scoped to the FULL company (DST332/333/334/335) -- what the fixed callers
# now pass through.
_COMPANY_WIDE_BY_CUST = {"IEBU00061": KLUNGKUNG_STORE, "IEBD00039": DENPASAR_STORE}
_COMPANY_WIDE_BY_REF = {
    "skt": {"DST333KLK8500100": [KLUNGKUNG_STORE],
           "DST332DPS3200142": [DENPASAR_STORE]},
    "tph": {"DST333KLK8500100": [KLUNGKUNG_STORE],
           "DST332DPS3200142": [DENPASAR_STORE]},
    "fcr": {},
}


def _sinar_mayuri_pipeline(rows, by_cust, by_ref, login="DST334"):
    allowed = {c: {"name": f"PT SINAR MAYURI - {c}"}
              for c in ("DST332", "DST333", "DST334", "DST335")}
    distributor = {"distributor_code": login,
                  "distributor_name": f"PT SINAR MAYURI - {login}"}
    return pipeline.run_noo(
        fx.FakeParsed(rows, config.NOO_COLUMNS), distributor=distributor,
        resolver=CustomerCodeResolver(dist_database={login: "SMI"}),
        dist_enricher=enrichment.DistributorEnricher(
            master_distributor={}, dist_database={login: distributor}),
        store_enricher=enrichment.StoreEnricher(by_cust, by_ref),
        ledger=(frozenset(), frozenset()), when=now_business(),
        allowed_branches=allowed, company_name="PT SINAR MAYURI")


@pytest.mark.sanity
def test_bug_reproduced_with_login_only_scoped_basis_data():
    """Confirms the FAILURE MODE itself: when the store-basis data is scoped
    to only the login branch (the pre-fix behaviour), a sibling branch's
    genuinely-existing store is misclassified as NOO with no store_id --
    proving the defect was in what data was FETCHED, not in the lookup or
    classification logic, both of which are exercised here unchanged."""
    result = _sinar_mayuri_pipeline(
        [fx.noo_row(branch_code="DST333", customer_code="11SMI",
                   store_code="DST333KLK8500100", name="TOKO KLUNGKUNG",
                   branch="PT SINAR MAYURI - DST333")],
        _LOGIN_ONLY_BY_CUST, _LOGIN_ONLY_BY_REF)
    assert not result.errors
    row = result.pool_rows[0]
    assert row["NOO/Existing"] == "NOO -> Create ID"
    assert row["store_id"] == ""


@pytest.mark.sanity
def test_a_dst333_klungkung_reference_id_resolves_to_not_noo():
    """Test A (user-specified): DST333KLK8500100 -> Not NOO, store_id
    populated with the master's own matched identifier."""
    result = _sinar_mayuri_pipeline(
        [fx.noo_row(branch_code="DST333", customer_code="11SMI",
                   store_code="DST333KLK8500100", name="TOKO KLUNGKUNG",
                   branch="PT SINAR MAYURI - DST333")],
        _COMPANY_WIDE_BY_CUST, _COMPANY_WIDE_BY_REF)
    assert not result.errors
    row = result.pool_rows[0]
    assert row["NOO/Existing"] == "Not NOO -> Reference ID not exist"
    assert row["store_id"] == "IEBU00061"


@pytest.mark.sanity
def test_b_dst332_denpasar_reference_id_resolves_to_not_noo():
    """Test B (user-specified): DST332DPS3200142 -> Not NOO, store_id
    populated with the master's own matched identifier."""
    result = _sinar_mayuri_pipeline(
        [fx.noo_row(branch_code="DST332", customer_code="11SMI",
                   store_code="DST332DPS3200142", name="TOKO DENPASAR",
                   branch="PT SINAR MAYURI - DST332")],
        _COMPANY_WIDE_BY_CUST, _COMPANY_WIDE_BY_REF)
    assert not result.errors
    row = result.pool_rows[0]
    assert row["NOO/Existing"] == "Not NOO -> Reference ID not exist"
    assert row["store_id"] == "IEBD00039"


@pytest.mark.sanity
def test_c_genuine_new_store_still_reads_as_noo_with_blank_store_id():
    """Test C (user-specified): a reference id that genuinely does not exist
    anywhere in the company-wide data must still classify as NOO."""
    result = _sinar_mayuri_pipeline(
        [fx.noo_row(branch_code="DST335", customer_code="11SMI",
                   store_code="DST335NEG9999999", name="TOKO BENAR BARU",
                   branch="PT SINAR MAYURI - DST335")],
        _COMPANY_WIDE_BY_CUST, _COMPANY_WIDE_BY_REF)
    assert not result.errors
    row = result.pool_rows[0]
    assert row["NOO/Existing"] == "NOO -> Create ID"
    assert row["store_id"] == ""


@pytest.mark.sanity
def test_d_whitespace_and_case_variants_still_resolve():
    """Test D (user-specified): the lookup must be tolerant of formatting
    noise around an otherwise-correct reference id."""
    result = _sinar_mayuri_pipeline(
        [fx.noo_row(branch_code=" dst333 ", customer_code="11SMI",
                   store_code="  dst333klk8500100  ", name="TOKO KLUNGKUNG",
                   branch="PT SINAR MAYURI - DST333")],
        _COMPANY_WIDE_BY_CUST, _COMPANY_WIDE_BY_REF)
    assert not result.errors
    row = result.pool_rows[0]
    assert row["NOO/Existing"] == "Not NOO -> Reference ID not exist"
    assert row["store_id"] == "IEBU00061"


@pytest.mark.sanity
def test_both_reported_reference_ids_together_in_one_multi_branch_upload():
    """The exact user-reported scenario: one file, both branches, one admin
    logged in as neither of them."""
    result = _sinar_mayuri_pipeline([
        fx.noo_row(branch_code="DST333", customer_code="11SMI",
                  store_code="DST333KLK8500100", name="TOKO KLUNGKUNG",
                  branch="PT SINAR MAYURI - DST333"),
        fx.noo_row(branch_code="DST332", customer_code="11SMI",
                  store_code="DST332DPS3200142", name="TOKO DENPASAR",
                  branch="PT SINAR MAYURI - DST332"),
    ], _COMPANY_WIDE_BY_CUST, _COMPANY_WIDE_BY_REF)
    assert not result.errors
    by_code = {r["customer_store_code"]: r for r in result.pool_rows}
    assert by_code["DST333KLK8500100"]["NOO/Existing"] == \
        "Not NOO -> Reference ID not exist"
    assert by_code["DST333KLK8500100"]["store_id"] == "IEBU00061"
    assert by_code["DST332DPS3200142"]["NOO/Existing"] == \
        "Not NOO -> Reference ID not exist"
    assert by_code["DST332DPS3200142"]["store_id"] == "IEBD00039"


# ─── Regression: query no longer narrows to a single prefix code ────────────
def test_load_store_basis_query_checks_every_authorised_code_not_just_the_first():
    """The secondary bug in the same function: STARTS_WITH used only
    codes[0] as its prefix, silently narrowing the reference-id fallback to
    one branch. Must now check every authorised code."""
    import inspect

    from noo_sku import sources

    src = inspect.getsource(sources.load_store_basis)
    # The comment explaining the old bug legitimately says "codes[0]"; only
    # the executable line matters here.
    code_lines = [l for l in src.splitlines() if not l.strip().startswith("#")]
    code_only = "\n".join(code_lines)
    assert "codes[0]" not in code_only, "must not single out the first code"
    assert 'ScalarQueryParameter("prefix"' not in code_only
    assert 'ArrayQueryParameter("codes"' in code_only
    assert "UNNEST(@codes)" in code_only


def test_store_basis_caller_is_scoped_to_every_authorised_branch():
    """Source-level guard against the exact regression: the Streamlit caller
    must pass the full authorised set, never a single login code, into the
    store-basis loader."""
    from pathlib import Path

    app = (Path(__file__).resolve().parents[1] / "noo_sku_mapping.py").read_text(
        encoding="utf-8")
    assert "_store_basis(tuple(sorted(allowed)))" in app
    assert "_store_basis(dist_code)" not in app


def test_uat_script_caller_is_also_scoped_to_every_authorised_branch():
    """The standalone UAT/production script must not diverge from the
    Streamlit app's behaviour -- it had the identical single-code bug."""
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "scripts"
          / "run_noo_sku_uat.py").read_text(encoding="utf-8")
    assert "authorized_branches" in src
    assert "allowed_branches=allowed" in src
    assert "load_store_basis, creds, project, [code]" not in src
