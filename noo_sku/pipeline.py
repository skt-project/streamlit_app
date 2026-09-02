"""Upload pipeline orchestration.

Enforces the mandated order in one place, so the UI cannot accidentally reorder
it and so the whole flow is testable without Streamlit:

    raw user input
      -> validation
      -> master-data enrichment
      -> normalisation for comparison
      -> final row / identity resolution
      -> row-level duplicate detection
      -> preview + warnings
      -> (user confirmation happens in the UI)
      -> append eligible enriched rows

Rows failing validation are dropped before enrichment and reported separately;
they never reach the duplicate check and never reach the sheet.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from . import config, duplicates, noo_detector, validators, writer
from .normalize import clean, norm_key, now_business


@dataclass
class PipelineResult:
    kind: str
    errors: list = field(default_factory=list)        # validators.Issue
    warnings: list = field(default_factory=list)      # validators.Issue
    enrichment_notes: list = field(default_factory=list)
    pool_rows: list = field(default_factory=list)     # enriched, keyed by pool column
    row_meta: list = field(default_factory=list)      # mapping source per pool row
    classifications: list = field(default_factory=list)
    eligible_rows: list = field(default_factory=list)
    summary: dict = field(default_factory=dict)
    decision: str = "reject"
    message: str = ""
    upload_id: str = ""
    when: object = None

    @property
    def has_errors(self) -> bool:
        return bool(self.errors)

    @property
    def fallback_count(self) -> int:
        return sum(1 for m in self.row_meta if m.get("used_fallback"))

    @property
    def ambiguous_count(self) -> int:
        return sum(1 for m in self.row_meta if m.get("ambiguous"))

    @property
    def reference_id_exists_count(self) -> int:
        return sum(1 for m in self.row_meta
                   if m.get("noo_existing_label") == noo_detector.LABEL_REFERENCE_EXISTS)


def _rows_with_errors(issues) -> set:
    return {i.row for i in issues if i.severity == validators.ERROR}


def run_noo(parsed, *, distributor, resolver, dist_enricher, store_enricher,
            ledger, known_cities=None, when=None, allowed_branches=None,
            company_name=""):
    """Validate, enrich and classify a NOO upload.

    MoM 31-Aug-2026: the file may carry several branches. Authorisation is
    company-level (``allowed_branches``); each row's own Customer Branch Code
    decides which branch it belongs to, and enrichment follows that branch
    rather than the login.
    """
    when = when or now_business()
    result = PipelineResult(kind="NOO", upload_id=writer.new_upload_id(),
                            when=when)
    dist_code = distributor["distributor_code"]

    # 1. validation ----------------------------------------------------------
    issues, cleaned = validators.validate_noo(
        parsed.rows, parsed.row_numbers,
        distributor_code=dist_code,
        distributor_name=distributor["distributor_name"],
        expected_suffix=resolver.resolve(dist_code).suffix,
        known_cities=known_cities,
        allowed_branches=allowed_branches,
        company_name=company_name,
        suffix_for=lambda code: resolver.resolve(code).suffix)
    result.errors, result.warnings = validators.split_severity(issues)

    bad = _rows_with_errors(result.errors)
    good = [(row, number) for row, number in zip(cleaned, parsed.row_numbers)
            if number not in bad]

    # 2. enrichment ----------------------------------------------------------
    pool_rows, numbers = [], []
    for row, number in good:
        brand = _brand_of_customer_code(row.get("Customer Code"))
        # Enrich from the branch this row names, not from the login.
        row_code = norm_key(row.get("Customer Branch Code")) or dist_code
        dist_result = dist_enricher.resolve(row_code, brand, row=number)
        store_result = store_enricher.resolve(
            store_id=row.get("Store ID (Opsional)", ""),
            store_code=row.get("Customer Store Code", ""),
            brand=brand, row=number)
        result.enrichment_notes.extend(dist_result.notes)
        result.enrichment_notes.extend(store_result.notes)

        # MoM 2026-09-03 fix: the NOO Detector's verdict is a DIRECT lookup,
        # not a fuzzy score. It reuses the exact same composite-key match
        # store_result already performed against master_store_database_basis
        # -- no second lookup, no scoring formula.
        detection = noo_detector.classify(store_result)

        # 3. final row resolution --------------------------------------------
        pool_rows.append(writer.build_noo_row(
            row, distributor_code=row_code, dist_values=dist_result.values,
            store_values=store_result.values, when=when,
            noo_existing_label=detection.label,
            resolved_store_id=detection.store_id))
        numbers.append(number)
        result.row_meta.append({
            "row": number,
            "branch": row_code,
            "distributor_source": dist_result.mapping_source,
            "store_source": store_result.mapping_source,
            "matched_on": store_result.matched_on or "-",
            "store_matched": store_result.matched,
            "ambiguous": store_result.ambiguous,
            "used_fallback": dist_result.used_fallback,
            "noo_existing_label": detection.label,
            "resolved_store_id": detection.store_id,
            "brand": brand,
        })

    result.pool_rows = pool_rows

    # 4. duplicate detection on the ENRICHED rows ----------------------------
    identities, contents = ledger
    result.classifications = duplicates.classify(
        pool_rows, numbers, existing_identities=identities,
        existing_contents=contents, identity_fn=duplicates.noo_identity,
        content_fn=duplicates.noo_content)
    return _finalise(result)


def run_sku(parsed, *, distributor, resolver, dist_enricher, product_enricher,
            ledger, product_lookup, when=None, company_name=""):
    """Validate, enrich and classify a SKU upload."""
    when = when or now_business()
    result = PipelineResult(kind="SKU", upload_id=writer.new_upload_id(),
                            when=when)
    dist_code = distributor["distributor_code"]

    issues, cleaned = validators.validate_sku(
        parsed.rows, parsed.row_numbers, distributor_code=dist_code,
        product_lookup=product_lookup)
    result.errors, result.warnings = validators.split_severity(issues)

    bad = _rows_with_errors(result.errors)
    good = [(row, number) for row, number in zip(cleaned, parsed.row_numbers)
            if number not in bad]

    pool_rows, numbers = [], []
    for row, number in good:
        # No size fallback: the gramasi column was removed from the template on
        # 31-Aug-2026, so `specification` comes from master_product alone.
        product = product_enricher.resolve(
            row.get("Principal Product Code", ""),
            fallback_name=row.get("Principal Product Name", ""), row=number)
        result.enrichment_notes.extend(product.notes)

        brand = product.values.get("brand") or row.get("_brand", "")
        dist_result = dist_enricher.resolve(dist_code, brand, row=number)
        result.enrichment_notes.extend(dist_result.notes)

        customer_code = resolver.customer_code(dist_code, brand) or ""
        pool_rows.append(writer.build_sku_row(
            row, distributor_code=dist_code, customer_code=customer_code,
            dist_values=dist_result.values, product_values=product.values,
            when=when, company_name=company_name))
        numbers.append(number)
        result.row_meta.append({
            "row": number,
            "distributor_source": dist_result.mapping_source,
            "product_source": ("MASTER_PRODUCT" if product.matched
                               else "USER INPUT (tidak ada di master)"),
            "used_fallback": dist_result.used_fallback,
            "brand": brand,
            "customer_code": customer_code,
        })

    result.pool_rows = pool_rows

    identities, contents = ledger
    result.classifications = duplicates.classify(
        pool_rows, numbers, existing_identities=identities,
        existing_contents=contents, identity_fn=duplicates.sku_identity,
        content_fn=duplicates.sku_content)
    return _finalise(result)


def _finalise(result: PipelineResult) -> PipelineResult:
    result.summary = duplicates.summarise(
        result.classifications, error_rows=len(_rows_with_errors(result.errors)))
    result.decision, result.message = duplicates.verdict(result.summary)
    result.eligible_rows = [row for row, c in
                            zip(result.pool_rows, result.classifications)
                            if c.insertable]
    return result


def _brand_of_customer_code(customer_code) -> str:
    """Brand implied by a NOO row's Customer Code prefix."""
    code = norm_key(customer_code)
    for brand, prefix in config.BRAND_PREFIX.items():
        if code.startswith(prefix):
            return brand
    return ""


def duplicate_details(result: PipelineResult) -> list:
    """Rows the user should inspect before confirming."""
    return [{"Baris": c.row_number, "Status": c.bucket, "Keterangan": c.note}
            for c in result.classifications if not c.insertable]


def enrichment_details(result: PipelineResult) -> list:
    return [{"Baris": n.row, "Kolom": n.field, "Status": n.status,
             "Keterangan": n.detail} for n in result.enrichment_notes]


def mapping_sources(result: PipelineResult) -> list:
    """Per-row mapping provenance, for the preview and the UAT report.

    Makes a fallback explicit rather than silent - required for verifying
    thin-coverage brands such as FACERINNA.
    """
    rows = []
    for meta, pool in zip(result.row_meta, result.pool_rows):
        entry = {
            "Baris": meta["row"],
            "Brand": meta.get("brand") or "-",
            "Cabang": pool.get("customer_branch_code", ""),
            "Distributor": pool.get("branch_name") or pool.get("customer_name", ""),
            "ASM": pool.get("asm_name") or pool.get("asm", ""),
            "SPV": pool.get("spv", ""),
            "Mapping Source (Distributor)": meta["distributor_source"],
            "Fallback": "YA" if meta.get("used_fallback") else "-",
        }
        if result.kind == "NOO":
            entry["Mapping Source (Store)"] = meta["store_source"]
            entry["Matched On"] = meta["matched_on"]
            entry["SE"] = pool.get("se_kae", "")
            entry["NOO/Existing"] = meta.get("noo_existing_label", "")
            entry["Store ID (auto)"] = meta.get("resolved_store_id", "")
            if meta.get("ambiguous"):
                entry["Fallback"] = "AMBIGU - PERLU DITINJAU"
        else:
            entry["Mapping Source (Product)"] = meta["product_source"]
            entry["Customer Code"] = meta.get("customer_code", "")
        rows.append(entry)
    return rows
