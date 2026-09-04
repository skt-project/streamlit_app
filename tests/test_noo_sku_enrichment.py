"""Enrichment, pipeline ordering, duplicate classification and write safety.

Covers the 30 scenarios in the refactor brief §28. Everything runs against pure
functions and fakes — no credentials, no network, no spreadsheet.
"""
from __future__ import annotations

import pytest

from noo_sku import config, duplicates, enrichment, pipeline, validators, writer
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


#: Two branches of one company; DST999 is used as the foreign branch.
ALLOWED_BRANCHES = {"DST082": {"name": "CV CECE"},
                    "DST083": {"name": "CV CECE - CABANG DUA"}}


def _noo_pipeline(rows, ledger=(frozenset(), frozenset()), allowed=None):
    return pipeline.run_noo(
        fx.FakeParsed(rows, config.NOO_COLUMNS), distributor=fx.DISTRIBUTOR,
        resolver=_resolver(), dist_enricher=_dist_enricher(),
        store_enricher=_store_enricher(), ledger=ledger, when=now_business(),
        allowed_branches=allowed or ALLOWED_BRANCHES, company_name="CV CECE")


def _sku_pipeline(rows, ledger=(frozenset(), frozenset())):
    return pipeline.run_sku(
        fx.FakeParsed(rows, config.SKU_COLUMNS, first_row=6),
        distributor=fx.DISTRIBUTOR, resolver=_resolver(),
        dist_enricher=_dist_enricher(),
        product_enricher=enrichment.ProductEnricher(fx.PRODUCTS),
        ledger=ledger, product_lookup=fx.PRODUCTS, when=now_business())


# ─── §28.5–8  Distributor enrichment ──────────────────────────────────────────
@pytest.mark.sanity
def test_distributor_enrichment_comes_from_master_distributor():
    values = _dist_enricher().resolve(DIST, "SKINTIFIC").values
    assert values["branch_name"] == "CV CECE"
    assert values["region"] == "Northern Sulawesi"
    assert values["asm"] == "Ainur Rochman Fawzi"


@pytest.mark.sanity
@pytest.mark.parametrize("brand,expected_asm", [
    ("SKINTIFIC", "Ainur Rochman Fawzi"), ("TIMEPHORIA", "ASM TPH"),
])
def test_asm_is_selected_per_brand(brand, expected_asm):
    assert _dist_enricher().resolve(DIST, brand).values["asm"] == expected_asm


def test_facerinna_falls_back_to_the_brand_neutral_column_when_blank():
    # asm_fr is empty in the fixture, mirroring the real 52% coverage.
    assert _dist_enricher().resolve(DIST, "FACERINNA").values["asm"] == \
        "Ainur Rochman Fawzi"


def test_unknown_distributor_falls_back_to_dist_database_and_notes_it():
    enricher = enrichment.DistributorEnricher(
        master_distributor={}, dist_database={DIST: fx.DISTRIBUTOR})
    result = enricher.resolve(DIST, "SKINTIFIC")
    assert result.values["branch_name"] == "CV CECE"
    assert result.matched is False
    assert any(n.status == enrichment.STATUS_MISSING for n in result.notes)


# ─── §28.7–10  SE and store-level enrichment ──────────────────────────────────
@pytest.mark.sanity
def test_se_comes_from_store_basis_because_master_distributor_has_none():
    values = _store_enricher().resolve(
        store_id="IESL00038", store_code="DST08200074",
        brand="SKINTIFIC").values
    assert values["se_kae"] == "Mohammad Fikram Dam"
    assert values["spv"] == "Voldy Kendes"
    assert values["area"] == "BANGGAI"


@pytest.mark.sanity
def test_store_resolves_by_reference_id_when_store_id_is_blank():
    values = _store_enricher().resolve(
        store_id="", store_code="DST08200074", brand="SKINTIFIC").values
    assert values["se_kae"] == "Mohammad Fikram Dam"


def test_facerinna_se_uses_the_se_fcr_column():
    values = _store_enricher().resolve(
        store_id="IESL00038", store_code="DST08200074",
        brand="FACERINNA").values
    assert values["se_kae"] == "SE FCR"


@pytest.mark.sanity
def test_ambiguous_store_match_is_flagged_and_left_blank():
    """Brief §6: never arbitrarily pick one of several matching master rows."""
    result = _store_enricher().resolve(
        store_id="", store_code="DST08200099", brand="SKINTIFIC")
    assert result.ambiguous is True
    assert result.values["se_kae"] == ""
    assert any(n.status == enrichment.STATUS_AMBIGUOUS for n in result.notes)


@pytest.mark.sanity
def test_brand_new_store_is_not_rejected_and_leaves_enrichment_blank():
    """Decision B2: absence from the master is expected for a genuine NOO."""
    result = _store_enricher().resolve(
        store_id="", store_code="DST08299999", brand="SKINTIFIC")
    assert result.matched is False
    assert result.values["se_kae"] == ""
    note = [n for n in result.notes if n.status == enrichment.STATUS_NEW_STORE]
    assert note and note[0].is_blocking is False


# ─── §28.11–12  Product enrichment ────────────────────────────────────────────
@pytest.mark.sanity
def test_product_name_and_size_come_from_master_product():
    values = enrichment.ProductEnricher(fx.PRODUCTS).resolve(
        "SKINTIFIC-296", fallback_name="SALAH", fallback_size="99ml").values
    assert values["product_name"] == "SKINTIFIC TEST PRODUCT"
    assert values["specification"] == "30ml"
    assert values["brand"] == "SKINTIFIC"


def test_unknown_product_keeps_user_values_and_notes_the_miss():
    result = enrichment.ProductEnricher(fx.PRODUCTS).resolve(
        "NOPE-1", fallback_name="USER NAME", fallback_size="1ml")
    assert result.matched is False
    assert result.values["product_name"] == "USER NAME"
    assert any(n.status == enrichment.STATUS_MISSING for n in result.notes)


# ─── §28.17–19  input_time ────────────────────────────────────────────────────
@pytest.mark.sanity
def test_input_time_is_generated_in_asia_jakarta():
    result = _noo_pipeline([fx.noo_row()])
    assert result.when.utcoffset().total_seconds() == 7 * 3600
    assert result.pool_rows[0]["input_time"]


@pytest.mark.sanity
def test_input_time_is_excluded_from_the_content_hash():
    """Brief §13: a different timestamp must not make a row look new."""
    a = _noo_pipeline([fx.noo_row()]).pool_rows[0]
    b = dict(a, input_time="01-Jan-2030 00:00:00")
    assert duplicates.noo_content(a) == duplicates.noo_content(b)
    assert "input_time" not in duplicates.NOO_CONTENT_COLUMNS
    assert "input_time" not in duplicates.SKU_CONTENT_COLUMNS


def test_volatile_enrichment_is_excluded_so_master_drift_is_not_a_correction():
    a = _noo_pipeline([fx.noo_row()]).pool_rows[0]
    drifted = dict(a, se_kae="Orang Baru", spv="SPV Baru", area="AREA BARU")
    assert duplicates.noo_content(a) == duplicates.noo_content(drifted)


# ─── §28.13–16, 28  Duplicate classification on ENRICHED rows ────────────────
@pytest.mark.sanity
def test_unseen_row_is_new():
    result = _noo_pipeline([fx.noo_row()])
    assert result.classifications[0].bucket == duplicates.NEW


@pytest.mark.sanity
def test_exact_duplicate_is_detected_and_skipped():
    first = _noo_pipeline([fx.noo_row()])
    content = duplicates.noo_content(first.pool_rows[0])
    again = _noo_pipeline([fx.noo_row()], ledger=(frozenset(), {content}))
    assert again.classifications[0].bucket == duplicates.EXACT_DUPLICATE
    assert again.eligible_rows == []


@pytest.mark.sanity
def test_correction_is_inserted_as_a_new_row():
    first = _noo_pipeline([fx.noo_row()])
    identity = duplicates.noo_identity(first.pool_rows[0])
    changed = _noo_pipeline([fx.noo_row(address="ALAMAT BARU 99")],
                            ledger=({identity}, frozenset()))
    assert changed.classifications[0].bucket == duplicates.CORRECTION
    assert len(changed.eligible_rows) == 1


@pytest.mark.sanity
def test_duplicate_within_the_same_file_is_warned_not_inserted_twice():
    result = _noo_pipeline([fx.noo_row(), fx.noo_row(name="EJAAN LAIN")])
    assert result.classifications[1].bucket == duplicates.DUPLICATE_IN_FILE
    assert len(result.eligible_rows) == 1


@pytest.mark.sanity
def test_partial_duplicate_upload_keeps_the_valid_new_rows():
    """Brief §12/§16: 82 NEW + 3 CORRECTION + 15 DUP -> 85 inserted."""
    base = _noo_pipeline([fx.noo_row()])
    dup_content = duplicates.noo_content(base.pool_rows[0])
    corr_identity = duplicates.noo_identity(
        _noo_pipeline([fx.noo_row(store_code="DST08200077")]).pool_rows[0])

    rows = [fx.noo_row()]                                        # duplicate
    rows += [fx.noo_row(store_code="DST08200077", name="BERUBAH")]  # correction
    rows += [fx.noo_row(store_code=f"DST082001{i:02d}") for i in range(3)]  # new

    result = _noo_pipeline(rows, ledger=({corr_identity}, {dup_content}))
    assert result.summary["exact_duplicate"] == 1
    assert result.summary["correction"] == 1
    assert result.summary["new"] == 3
    assert len(result.eligible_rows) == 4
    assert result.decision == "confirm"


def test_a_file_of_only_duplicates_is_rejected():
    base = _noo_pipeline([fx.noo_row()])
    content = duplicates.noo_content(base.pool_rows[0])
    result = _noo_pipeline([fx.noo_row()], ledger=(frozenset(), {content}))
    assert result.decision == "reject"
    assert "sudah pernah diupload" in result.message


def test_another_distributors_history_never_collides():
    ours = _noo_pipeline([fx.noo_row()]).pool_rows[0]
    theirs = duplicates.noo_content(dict(ours, customer_branch_code="DST999"))
    result = _noo_pipeline([fx.noo_row()], ledger=(frozenset(), {theirs}))
    assert result.classifications[0].bucket == duplicates.NEW


# ─── §28.19–22  Ordering, validation and preview ──────────────────────────────
@pytest.mark.sanity
def test_rows_failing_validation_never_reach_enrichment_or_the_pool():
    result = _noo_pipeline([fx.noo_row(), fx.noo_row(store_code="")])
    assert result.has_errors
    assert len(result.pool_rows) == 1, "the invalid row must be dropped"
    assert result.summary["error"] == 1


@pytest.mark.sanity
def test_enrichment_runs_before_duplicate_detection():
    """Identity is computed from enriched values, not raw input."""
    result = _noo_pipeline([fx.noo_row()])
    identity = result.classifications[0].identity
    assert identity.startswith(DIST)
    assert result.pool_rows[0]["customer_branch_code"] == DIST


def test_preview_counts_match_the_classifications():
    result = _noo_pipeline([fx.noo_row(),
                            fx.noo_row(store_code="DST08200078")])
    assert result.summary["total"] == 2
    assert result.summary["new"] == 2
    assert result.summary["insertable"] == len(result.eligible_rows)


# ─── §28.24–27, 29–30  Write target, ordering, safety ─────────────────────────
@pytest.mark.sanity
def test_noo_pool_row_matches_the_live_pool_layout_exactly():
    # store_code matches the basis fixture, so enrichment resolves.
    row = _noo_pipeline([fx.noo_row(store_code="DST08200074")]).pool_rows[0]
    assert list(row) == config.POOL_NOO_HEADERS
    values = writer.to_values([row], config.POOL_NOO_HEADERS)[0]
    assert len(values) == len(config.POOL_NOO_HEADERS) == 41
    named = dict(zip(config.POOL_NOO_HEADERS, values))
    assert named["customer_branch_code"] == DIST
    assert named["branch_name"] == "CV CECE"
    assert named["area"] == "BANGGAI"
    # MoM 31-Aug-2026 §5: BD Support formulates the hierarchy themselves.
    assert named["se_kae"] == ""
    assert named["input_time"]


@pytest.mark.sanity
def test_pool_row_for_an_unmatched_new_store_still_has_the_full_41_columns():
    row = _noo_pipeline([fx.noo_row(store_code="DST08299999")]).pool_rows[0]
    assert list(row) == config.POOL_NOO_HEADERS
    assert row["se_kae"] == "" and row["area"] == ""
    assert row["store_name"] and row["customer_branch_code"] == DIST


@pytest.mark.sanity
def test_sku_pool_row_matches_the_live_15_column_layout_exactly():
    """2026-09-03: BD Support prepended DMS/RSA, growing the pool from 13 to
    15 columns. Both are formula/manual columns Streamlit never writes."""
    row = _sku_pipeline([fx.sku_row()]).pool_rows[0]
    assert list(row) == config.POOL_SKU_HEADERS
    values = writer.to_values([row], config.POOL_SKU_HEADERS)[0]
    assert len(values) == 15
    named = dict(zip(config.POOL_SKU_HEADERS, values))
    assert named["DMS"] == "" and named["RSA"] == ""
    assert named["customer_code"] == "11CEC"
    assert named["customer_branch_code"] == DIST
    assert named["product_name"] == "SKINTIFIC TEST PRODUCT"
    assert named["specification"] == "30ml"


def test_unused_pool_columns_are_left_blank():
    row = _noo_pipeline([fx.noo_row()]).pool_rows[0]
    assert all(row[c] == "" for c in config.POOL_NOO_UNUSED)


@pytest.mark.sanity
def test_store_type_vocabulary_is_preserved_exactly():
    """Decision B3: never translate Regular SPM <-> Regular Supermarket."""
    row = _noo_pipeline([fx.noo_row(channel="MTI",
                                    store_type="Regular SPM")]).pool_rows[0]
    assert row["store_type"] == "Regular SPM"


@pytest.mark.sanity
def test_dry_run_validates_and_checks_layout_but_never_appends():
    settings = config.Settings(mode="dry-run", env={"WRITE_ENABLED": "false"})
    client = fx.FakeSheetsClient(
        {config.TAB_POOL_NOO: [config.POOL_NOO_HEADERS]})
    result = _noo_pipeline([fx.noo_row()])
    write = writer.append_rows(client, config.TAB_POOL_NOO,
                               result.eligible_rows,
                               headers=config.POOL_NOO_HEADERS,
                               settings=settings, upload_id="abcd1234")
    assert write.ok and write.dry_run
    assert client.written == []


@pytest.mark.sanity
def test_write_mode_appends_once_to_the_correct_pool():
    """The write is scoped to the owned span (E:W, 19 columns), never the
    full 41-column header - formula and BD-manual columns are structurally
    absent from the payload, not merely blanked."""
    settings = config.Settings(mode="production", env={"WRITE_ENABLED": "true"})
    client = fx.FakeSheetsClient(
        {config.TAB_POOL_NOO: [config.POOL_NOO_HEADERS]})
    result = _noo_pipeline([fx.noo_row()])
    write = writer.append_rows(client, config.TAB_POOL_NOO,
                               result.eligible_rows,
                               headers=config.POOL_NOO_HEADERS,
                               settings=settings, upload_id="abcd1234")
    assert write.ok and not write.dry_run
    assert len(client.written) == 1
    assert client.written[0][0] == config.TAB_POOL_NOO
    _, _, span_columns = writer.owned_span_for("NOO", config.POOL_NOO_HEADERS)
    assert len(span_columns) == 19
    assert len(client.written[0][1][0]) == len(span_columns)


@pytest.mark.sanity
def test_write_is_refused_when_the_live_header_does_not_match():
    """The pool gained a header once already; drift must stop the write."""
    settings = config.Settings(mode="production", env={"WRITE_ENABLED": "true"})
    drifted = ["asm_name", "SOMETHING_ELSE"] + config.POOL_NOO_HEADERS[2:]
    client = fx.FakeSheetsClient({config.TAB_POOL_NOO: [drifted]})
    result = _noo_pipeline([fx.noo_row()])
    with pytest.raises(writer.LayoutMismatch):
        writer.append_rows(client, config.TAB_POOL_NOO, result.eligible_rows,
                           headers=config.POOL_NOO_HEADERS, settings=settings,
                           upload_id="x")
    assert client.written == []


@pytest.mark.sanity
def test_no_write_targets_any_tracker_sheet():
    settings = config.Settings(mode="production", env={"WRITE_ENABLED": "true"})
    client = fx.FakeSheetsClient(
        {config.TAB_POOL_NOO: [config.POOL_NOO_HEADERS],
         config.TAB_POOL_SKU: [config.POOL_SKU_HEADERS]})
    writer.append_rows(client, config.TAB_POOL_NOO,
                       _noo_pipeline([fx.noo_row()]).eligible_rows,
                       headers=config.POOL_NOO_HEADERS, settings=settings,
                       upload_id="x")
    writer.append_rows(client, config.TAB_POOL_SKU,
                       _sku_pipeline([fx.sku_row()]).eligible_rows,
                       headers=config.POOL_SKU_HEADERS, settings=settings,
                       upload_id="y")
    targets = {tab for tab, _, _ in client.written}
    assert targets == {config.TAB_POOL_NOO, config.TAB_POOL_SKU}
    forbidden = set(config.TAB_NOO_MAIN.values()) | {
        config.TAB_SKU_MAPPING, config.TAB_DIST_DATABASE}
    assert not (targets & forbidden)


def test_write_is_disabled_by_default():
    assert config.Settings(env={}).dry_run is True


def test_nothing_is_written_when_every_row_is_ineligible():
    settings = config.Settings(mode="production", env={"WRITE_ENABLED": "true"})
    client = fx.FakeSheetsClient(
        {config.TAB_POOL_NOO: [config.POOL_NOO_HEADERS]})
    write = writer.append_rows(client, config.TAB_POOL_NOO, [],
                               headers=config.POOL_NOO_HEADERS,
                               settings=settings, upload_id="x")
    assert not write.ok and client.written == []


# ─── Formula-safe, column-scoped write (2026-09-03 fix) ───────────────────────
# POOL NOO/SKU STREAMLIT are structured trackers, not append-only tables: BD
# Support pre-fills every row with live XLOOKUP formulas far ahead of any real
# upload (confirmed to row 900+ of the live NOO pool). These tests build a
# fake pool in exactly that shape - formula cells already populated, the
# Streamlit-owned span still blank - and verify the write reuses those rows
# and never disturbs a formula cell, rather than trusting it by construction.
def _row_dict(headers, row):
    """Same ragged-row handling production code uses: a real Sheets API
    response trims trailing empty cells, so a short row means blank, not
    missing."""
    return {h: (row[i] if i < len(row) else "") for i, h in enumerate(headers)}


def _prefilled_noo_pool(n_rows):
    sheet = [config.POOL_NOO_HEADERS]
    for r in range(2, 2 + n_rows):
        row = ["" for _ in config.POOL_NOO_HEADERS]
        for column in config.POOL_NOO_FORMULA_COLUMNS:
            row[config.POOL_NOO_HEADERS.index(column)] = f"=FORMULA_{column}_{r}"
        sheet.append(row)
    return sheet


def _prefilled_sku_pool(n_rows):
    sheet = [config.POOL_SKU_HEADERS]
    for r in range(2, 2 + n_rows):
        row = ["" for _ in config.POOL_SKU_HEADERS]
        for column in config.POOL_SKU_FORMULA_COLUMNS:
            row[config.POOL_SKU_HEADERS.index(column)] = f"=FORMULA_{column}_{r}"
        sheet.append(row)
    return sheet


@pytest.mark.sanity
def test_owned_span_excludes_every_formula_and_manual_noo_column():
    _, _, span = writer.owned_span_for("NOO", config.POOL_NOO_HEADERS)
    touched = set(span)
    assert not touched & config.POOL_NOO_BD_MANUAL
    assert not touched & config.POOL_NOO_FORMULA_COLUMNS
    assert "store_type" in touched       # real, user-submitted field
    assert "location_rating" in touched  # unused-but-safe, sandwiched inside


@pytest.mark.sanity
def test_owned_span_excludes_every_formula_and_manual_sku_column():
    _, _, span = writer.owned_span_for("SKU", config.POOL_SKU_HEADERS)
    touched = set(span)
    assert not touched & config.POOL_SKU_BD_MANUAL
    assert not touched & config.POOL_SKU_FORMULA_COLUMNS
    assert "specification" in touched
    # Trailing, never-populated columns with nothing owned beyond them:
    # correctly excluded from the write entirely, not merely blanked.
    assert "barcode" not in touched and "description" not in touched


@pytest.mark.sanity
def test_next_target_row_reuses_a_formula_row_whose_owned_span_is_blank():
    """2026-09-04: this used to live only inside FakeSheetsClient's own
    approximation of Sheets' behavior - a total blind spot, since the real
    write path never called anything equivalent. It is now a real function
    writer.append_rows actually calls."""
    existing = [["", "", ""], ["", "", ""], ["", "", ""]]  # 3 blank rows
    assert writer._next_target_row(existing) == 0


def test_next_target_row_skips_past_already_filled_rows():
    existing = [["a", "b", "c"], ["d", "", ""], ["", "", ""]]
    assert writer._next_target_row(existing) == 2


def test_next_target_row_is_zero_when_nothing_exists_yet():
    assert writer._next_target_row([]) == 0


@pytest.mark.sanity
def test_single_submission_fills_the_preexisting_formula_row_not_a_new_one():
    settings = config.Settings(mode="production", env={"WRITE_ENABLED": "true"})
    client = fx.FakeSheetsClient({config.TAB_POOL_NOO: _prefilled_noo_pool(3)})
    before = len(client._values[config.TAB_POOL_NOO])
    result = _noo_pipeline([fx.noo_row(store_code="DST08200074")])
    writer.append_rows(client, config.TAB_POOL_NOO, result.eligible_rows,
                       headers=config.POOL_NOO_HEADERS, settings=settings,
                       upload_id="x")
    sheet = client._values[config.TAB_POOL_NOO]
    assert len(sheet) == before          # reused row 2 - the sheet did not grow
    named = _row_dict(config.POOL_NOO_HEADERS, sheet[1])
    assert named["store_name"] == "TOKO SUMBER REJEKI"
    assert named["store_id"] == "IESL00038"
    # Every formula cell in that same row is exactly what BD Support put
    # there - untouched, not recalculated, not blanked.
    for column in config.POOL_NOO_FORMULA_COLUMNS:
        assert named[column] == f"=FORMULA_{column}_2"


@pytest.mark.sanity
def test_multiple_submissions_preserve_formulas_and_leave_other_rows_untouched():
    """Test case: 5 records in one upload - correct target rows, formulas
    preserved, no shifting/duplicating/corrupting existing rows."""
    settings = config.Settings(mode="production", env={"WRITE_ENABLED": "true"})
    client = fx.FakeSheetsClient({config.TAB_POOL_NOO: _prefilled_noo_pool(5)})
    rows = [fx.noo_row(store_code=f"DST082CS0{i:04d}") for i in range(5)]
    result = _noo_pipeline(rows)
    assert len(result.eligible_rows) == 5
    writer.append_rows(client, config.TAB_POOL_NOO, result.eligible_rows,
                       headers=config.POOL_NOO_HEADERS, settings=settings,
                       upload_id="x")
    sheet = client._values[config.TAB_POOL_NOO]
    assert len(sheet) == 6                # header + the 5 pre-filled rows, no more
    for r in range(2, 7):
        named = _row_dict(config.POOL_NOO_HEADERS, sheet[r - 1])
        assert named["store_name"]        # every owned span got filled
        for column in config.POOL_NOO_FORMULA_COLUMNS:
            assert named[column] == f"=FORMULA_{column}_{r}"


@pytest.mark.sanity
def test_a_second_upload_never_touches_a_row_written_by_an_earlier_one():
    settings = config.Settings(mode="production", env={"WRITE_ENABLED": "true"})
    client = fx.FakeSheetsClient({config.TAB_POOL_NOO: _prefilled_noo_pool(3)})
    first = _noo_pipeline([fx.noo_row(store_code="DST082CS00001",
                                      name="TOKO PERTAMA")])
    writer.append_rows(client, config.TAB_POOL_NOO, first.eligible_rows,
                       headers=config.POOL_NOO_HEADERS, settings=settings,
                       upload_id="x")
    second = _noo_pipeline([fx.noo_row(store_code="DST082CS00002",
                                       name="TOKO KEDUA")])
    writer.append_rows(client, config.TAB_POOL_NOO, second.eligible_rows,
                       headers=config.POOL_NOO_HEADERS, settings=settings,
                       upload_id="y")
    sheet = client._values[config.TAB_POOL_NOO]
    assert len(sheet) == 4                # header + 3 pre-filled rows - no growth
    row2 = _row_dict(config.POOL_NOO_HEADERS, sheet[1])
    row3 = _row_dict(config.POOL_NOO_HEADERS, sheet[2])
    assert row2["store_name"] == "TOKO PERTAMA"
    assert row3["store_name"] == "TOKO KEDUA"
    for column in config.POOL_NOO_FORMULA_COLUMNS:
        assert row2[column] == f"=FORMULA_{column}_2"
        assert row3[column] == f"=FORMULA_{column}_3"


@pytest.mark.sanity
def test_sku_submission_preserves_its_preexisting_formula():
    settings = config.Settings(mode="production", env={"WRITE_ENABLED": "true"})
    client = fx.FakeSheetsClient({config.TAB_POOL_SKU: _prefilled_sku_pool(2)})
    result = _sku_pipeline([fx.sku_row()])
    writer.append_rows(client, config.TAB_POOL_SKU, result.eligible_rows,
                       headers=config.POOL_SKU_HEADERS, settings=settings,
                       upload_id="x")
    sheet = client._values[config.TAB_POOL_SKU]
    assert len(sheet) == 3
    named = _row_dict(config.POOL_SKU_HEADERS, sheet[1])
    assert named["product_name"] == "SKINTIFIC TEST PRODUCT"
    assert named["RSA"] == "=FORMULA_RSA_2"
    assert named["barcode"] == "" and named["description"] == ""


@pytest.mark.sanity
def test_write_past_the_prefilled_rows_still_never_touches_a_formula_column():
    """A row beyond BD Support's pre-filled range starts with no formula at
    all. This app does not copy formulas down itself - that is Sheets'/BD
    Support's mechanism, not this application's - it only guarantees it never
    writes INTO a formula column, pre-filled or not."""
    settings = config.Settings(mode="production", env={"WRITE_ENABLED": "true"})
    client = fx.FakeSheetsClient({config.TAB_POOL_NOO: _prefilled_noo_pool(1)})
    for i in range(3):
        result = _noo_pipeline([fx.noo_row(store_code=f"DST082CS000{i:02d}")])
        writer.append_rows(client, config.TAB_POOL_NOO, result.eligible_rows,
                           headers=config.POOL_NOO_HEADERS, settings=settings,
                           upload_id=str(i))
    sheet = client._values[config.TAB_POOL_NOO]
    assert len(sheet) == 4      # 1 pre-filled row + 2 brand-new rows
    for row in sheet[2:]:
        named = _row_dict(config.POOL_NOO_HEADERS, row)
        for column in config.POOL_NOO_FORMULA_COLUMNS:
            assert named[column] == ""


@pytest.mark.sanity
def test_noo_and_not_noo_classification_survive_the_scoped_write_end_to_end():
    settings = config.Settings(mode="production", env={"WRITE_ENABLED": "true"})
    client = fx.FakeSheetsClient({config.TAB_POOL_NOO: _prefilled_noo_pool(2)})
    result = _noo_pipeline([
        fx.noo_row(store_code="DST08200074", name="TOKO ADA"),   # resolves
        fx.noo_row(store_code="DST08299999", name="TOKO BARU"),  # unresolved
    ])
    writer.append_rows(client, config.TAB_POOL_NOO, result.eligible_rows,
                       headers=config.POOL_NOO_HEADERS, settings=settings,
                       upload_id="x")
    check = writer.verify_written(
        client, config.TAB_POOL_NOO, config.POOL_NOO_HEADERS,
        result.eligible_rows,
        input_time=result.pool_rows[0]["input_time"], distributor_code=DIST)
    assert check["passed"] and check["verified"] == 2
    sheet = client._values[config.TAB_POOL_NOO]
    not_noo = _row_dict(config.POOL_NOO_HEADERS, sheet[1])
    is_noo = _row_dict(config.POOL_NOO_HEADERS, sheet[2])
    assert not_noo["NOO/Existing"] == "Not NOO -> Reference ID not exist"
    assert not_noo["store_id"] == "IESL00038"
    assert is_noo["NOO/Existing"] == "NOO -> Create ID"
    assert is_noo["store_id"] == ""


# ─── Guideline ────────────────────────────────────────────────────────────────
def test_guideline_pdf_renders_per_function():
    from noo_sku import guideline

    for kind in (guideline.UPLOAD_NOO, guideline.UPLOAD_SKU):
        assert guideline.build_pdf(kind)[:4] == b"%PDF"
        assert "Panduan" in guideline.title_for(kind)
        assert "Umum" in guideline.as_markdown(kind)


# ─── Write modes (brief §14/§15) ──────────────────────────────────────────────
@pytest.mark.sanity
def test_default_mode_is_dry_run_and_never_writes():
    st = config.Settings(env={})
    assert st.mode == config.MODE_DRY_RUN and st.dry_run is True


@pytest.mark.sanity
def test_pilot_without_write_enabled_stays_a_dry_run():
    st = config.Settings(mode="pilot", env={})
    assert st.is_pilot and st.dry_run is True


@pytest.mark.sanity
def test_pilot_refuses_more_rows_than_its_ceiling():
    st = config.Settings(mode="pilot", env={"WRITE_ENABLED": "true"},
                         pilot_max_rows=3)
    client = fx.FakeSheetsClient({config.TAB_POOL_NOO: [config.POOL_NOO_HEADERS]})
    rows = _noo_pipeline([fx.noo_row(store_code=f"DST082CS0{i:04d}")
                          for i in range(4)]).eligible_rows
    assert len(rows) == 4
    with pytest.raises(writer.PilotLimitExceeded):
        writer.append_rows(client, config.TAB_POOL_NOO, rows,
                           headers=config.POOL_NOO_HEADERS, settings=st,
                           upload_id="x")
    assert client.written == []


@pytest.mark.sanity
def test_pilot_writes_when_within_its_ceiling():
    st = config.Settings(mode="pilot", env={"WRITE_ENABLED": "true"},
                         pilot_max_rows=3)
    client = fx.FakeSheetsClient({config.TAB_POOL_NOO: [config.POOL_NOO_HEADERS]})
    rows = _noo_pipeline([fx.noo_row(store_code=f"DST082CS0{i:04d}")
                          for i in range(2)]).eligible_rows
    result = writer.append_rows(client, config.TAB_POOL_NOO, rows,
                                headers=config.POOL_NOO_HEADERS, settings=st,
                                upload_id="x")
    assert result.ok and not result.dry_run and len(client.written) == 1


def test_invalid_mode_is_rejected():
    with pytest.raises(ValueError):
        config.Settings(mode="banana")


def test_production_mode_has_no_row_ceiling():
    st = config.Settings(mode="production", env={"WRITE_ENABLED": "true"})
    assert st.max_rows is None


# ─── Mapping-source visibility (brief §7) ─────────────────────────────────────
@pytest.mark.sanity
def test_preview_exposes_mapping_source_and_fallback_per_row():
    result = _noo_pipeline([fx.noo_row(store_code="DST08200074")])
    rows = pipeline.mapping_sources(result)
    assert rows[0]["Mapping Source (Store)"] == enrichment.SOURCE_BASIS
    assert rows[0]["Matched On"] == "reference_id_skt"
    # The store matched, but SE is deliberately not written (MoM §5).
    assert rows[0]["SE"] == ""


@pytest.mark.sanity
def test_facerinna_brand_neutral_fallback_is_visible_not_silent():
    result = _noo_pipeline([fx.noo_row(customer_code="1ACEC",
                                       store_code="DST08200074")])
    rows = pipeline.mapping_sources(result)
    assert rows[0]["Brand"] == "FACERINNA"
    assert rows[0]["Fallback"] == "YA"
    assert result.fallback_count == 1
    assert any(n.status == enrichment.STATUS_FALLBACK
               for n in result.enrichment_notes)


@pytest.mark.sanity
def test_ambiguous_store_is_marked_for_review_in_the_preview():
    result = _noo_pipeline([fx.noo_row(store_code="DST08200099")])
    rows = pipeline.mapping_sources(result)
    assert "AMBIGU" in rows[0]["Fallback"]
    assert result.ambiguous_count == 1


# ─── Post-write verification ──────────────────────────────────────────────────
@pytest.mark.sanity
def test_post_write_verification_confirms_the_appended_rows():
    st = config.Settings(mode="pilot", env={"WRITE_ENABLED": "true"})
    client = fx.FakeSheetsClient({config.TAB_POOL_NOO: [config.POOL_NOO_HEADERS]})
    result = _noo_pipeline([fx.noo_row(store_code="DST082CS00001")])
    writer.append_rows(client, config.TAB_POOL_NOO, result.eligible_rows,
                       headers=config.POOL_NOO_HEADERS, settings=st,
                       upload_id="x")
    # FakeSheetsClient.append_column_span already wrote the row for real -
    # no manual replay needed, unlike the old append_values-based fake, which
    # only recorded the call.
    check = writer.verify_written(
        client, config.TAB_POOL_NOO, config.POOL_NOO_HEADERS,
        result.eligible_rows,
        input_time=result.pool_rows[0]["input_time"], distributor_code=DIST)
    assert check["passed"] and check["verified"] == 1


def test_verification_fails_when_nothing_was_written():
    client = fx.FakeSheetsClient({config.TAB_POOL_NOO: [config.POOL_NOO_HEADERS]})
    result = _noo_pipeline([fx.noo_row()])
    check = writer.verify_written(
        client, config.TAB_POOL_NOO, config.POOL_NOO_HEADERS,
        result.eligible_rows, input_time="01-Jan-2030 00:00:00",
        distributor_code=DIST)
    assert not check["passed"] and check["verified"] == 0


# ─── MoM 31-Aug-2026 §17 — NOO multi-branch ───────────────────────────────────
@pytest.mark.sanity
def test_mom_1_multiple_valid_branches_pass():
    """Test 1: several branches of the same company in one file."""
    result = _noo_pipeline([
        fx.noo_row(branch_code="DST082", store_code="DST082CS00001"),
        fx.noo_row(branch_code="DST083", customer_code="11WRM",
                   store_code="DST083CS00002", branch="CV CECE - CABANG DUA"),
    ])
    assert not result.errors, [i.as_text() for i in result.errors]
    assert len(result.eligible_rows) == 2
    assert {r["customer_branch_code"] for r in result.pool_rows} == {
        "DST082", "DST083"}


@pytest.mark.sanity
def test_mom_2_branch_of_another_company_is_rejected():
    """Test 2: a code outside the authorised company fails validation."""
    result = _noo_pipeline([fx.noo_row(branch_code="DST999",
                                       store_code="DST999CS00001")])
    assert result.has_errors
    bad = [i for i in result.errors if i.column == "Customer Branch Code"]
    assert bad and "DST999" in bad[0].problem
    assert result.eligible_rows == []


@pytest.mark.sanity
def test_mom_3_mixed_valid_and_unauthorised_does_not_silently_accept():
    """Test 3: 2 valid + 1 unauthorised - the bad row never reaches the pool."""
    result = _noo_pipeline([
        fx.noo_row(branch_code="DST082", store_code="DST082CS00001"),
        fx.noo_row(branch_code="DST083", customer_code="11WRM",
                   store_code="DST083CS00002", branch="CV CECE - CABANG DUA"),
        fx.noo_row(branch_code="DST999", store_code="DST999CS00003"),
    ])
    assert result.has_errors
    assert result.summary["error"] == 1
    assert len(result.pool_rows) == 2
    assert "DST999" not in {r["customer_branch_code"] for r in result.pool_rows}


@pytest.mark.sanity
def test_mom_4_pool_hierarchy_fields_are_left_blank():
    """Test 4: asm_kam / spv / se_kae / aom are BD Support's to formulate."""
    result = _noo_pipeline([fx.noo_row(store_code="DST08200074")])
    row = result.pool_rows[0]
    for column in ("asm_kam", "spv", "se_kae", "aom"):
        assert row[column] == "", f"{column} must be blank"
    assert row["branch_name"] and row["region"] and row["asm_name"]


def test_store_code_must_match_the_branch_named_on_the_same_row():
    result = _noo_pipeline([fx.noo_row(branch_code="DST083",
                                       customer_code="11WRM",
                                       store_code="DST082CS00001")])
    assert any("tidak diawali kode cabang" in i.problem for i in result.errors)


# ─── MoM 31-Aug-2026 §17 — SKU ────────────────────────────────────────────────
@pytest.mark.sanity
def test_mom_6_valid_principal_sku_passes():
    result = _sku_pipeline([fx.sku_row()])
    assert not result.errors
    assert len(result.eligible_rows) == 1


@pytest.mark.sanity
def test_mom_7_invalid_principal_sku_is_blocked():
    """Test 7: a non-existent Principal SKU must never reach the tracker."""
    result = _sku_pipeline([fx.sku_row(code="SKU999-TIDAK-ADA")])
    assert result.has_errors
    bad = [i for i in result.errors if i.column == "Principal Product Code"]
    assert bad and "tidak ditemukan" in bad[0].problem
    assert result.eligible_rows == []


@pytest.mark.sanity
def test_mom_8_size_column_is_absent_from_the_template_contract():
    assert "Product Size (ml/g)" not in config.SKU_COLUMNS
    assert len(config.SKU_COLUMNS) == 4


@pytest.mark.sanity
def test_mom_9_customer_name_is_the_company_not_the_branch():
    """Test 9: SKU pool customer_name carries the COMPANY name."""
    result = pipeline.run_sku(
        fx.FakeParsed([fx.sku_row()], config.SKU_COLUMNS, first_row=6),
        distributor=fx.DISTRIBUTOR, resolver=_resolver(),
        dist_enricher=_dist_enricher(),
        product_enricher=enrichment.ProductEnricher(fx.PRODUCTS),
        ledger=(frozenset(), frozenset()), product_lookup=fx.PRODUCTS,
        when=now_business(), company_name="CV CECE MANDIRI SEJAHTERA")
    assert result.pool_rows[0]["customer_name"] == "CV CECE MANDIRI SEJAHTERA"


def test_specification_still_comes_from_master_not_from_the_upload():
    result = _sku_pipeline([fx.sku_row()])
    assert result.pool_rows[0]["specification"] == "30ml"


# ─── MoM 31-Aug-2026 §17 Test 5 / 10 — guideline content ─────────────────────
@pytest.mark.sanity
@pytest.mark.parametrize("kind", ["NOO", "SKU"])
def test_mom_5_and_10_removed_guideline_wording_is_gone(kind):
    from noo_sku import guideline

    text = guideline.as_markdown(kind)
    for banned in ("Diisi otomatis oleh sistem", "Toko benar-benar baru",
                   "data masuk ke pool tracker"):
        assert banned not in text, f"{banned!r} still in the {kind} guideline"


@pytest.mark.sanity
def test_noo_guideline_tells_admins_to_confirm_with_bd_support():
    from noo_sku import guideline

    assert "konfirmasi ke BD Support" in guideline.as_markdown("NOO")


@pytest.mark.sanity
def test_bd_support_processing_columns_are_never_written():
    """Added to the pool 2026-09-01; BD Support fills these, not Streamlit.
    NOO/Existing is the one exception — MoM 31-Aug-2026 §2-§4 makes it the
    integrated NOO Detector's output, tested separately below."""
    row = _noo_pipeline([fx.noo_row(store_code="DST08200074")]).pool_rows[0]
    for column in ("DMS", "BASIS", "RSA Name", "BD Support"):
        assert row[column] == "", f"{column} must stay blank"
    assert row["NOO/Existing"] in (
        "Not NOO -> Reference ID not exist", "NOO -> Create ID")
