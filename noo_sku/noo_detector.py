"""NOO / Not-NOO classification for POOL NOO STREAMLIT column E ("NOO/Existing").

FIXED 2026-09-03: the previous version of this module scored each uploaded row
against master_store_database_basis with a weighted fuzzy-match formula ported
from the standalone "Duplicate Store Checker" app (Name/Address/City/GPS/NIK/
NPWP, threshold >=70). That formula assumed fields the real NOO template does
not collect (Region, GPS, NIK, NPWP), so a genuinely existing store with a
blank "Store ID (Opsional)" field could score as low as 65 -- always below the
70 cutoff -- and be wrongly classified as brand-new. That was flagged as a
known limitation, and the correct fix turned out to be simpler than the
formula it replaced: the classification does not need fuzzy matching at all.

CORRECTED LOGIC
----------------
The Reference ID the admin supplies (`Store ID (Opsional)`, and `Customer
Store Code` as the composite fallback) is looked up DIRECTLY against
`master_store_database_basis` -- the exact same composite key
(`enrichment.StoreEnricher`) already used for SE/SPV/AOM enrichment, verified
at 99.9% resolution on real data. No scoring, no threshold:

    Reference ID resolves to a Store ID in master_store_database_basis
        -> "Not NOO -> Reference ID not exist"    (this row already exists)
        -> pool `store_id` is auto-populated from the MASTER's own cust_id

    Reference ID does not resolve (or resolves ambiguously)
        -> "NOO -> Create ID"                      (this is a new outlet)
        -> pool `store_id` stays blank -- never a fake/generated value

WHY THE LABEL TEXT IS UNCHANGED
---------------------------------
The literal strings below are BD Support's own historical wording for this
exact tracker column, confirmed against real production data: 2,200 rows of
"Not NOO -> Reference ID not exist" and 1,657 of "NOO -> Create ID" in
SKINTIFIC NEW as of the 2026-08-19 audit -- the same convention the other two
brand trackers (TIMEPHORIA NEW, FACERINNA NEW) and this pool use. The fix here
is to the MATCHING METHOD, not the label text; there is nowhere else in the
tracker family this text could go.
"""
from __future__ import annotations

from dataclasses import dataclass

#: Exact wording used by BD Support's own tracker for this column. Do not
#: reword -- see module docstring.
LABEL_REFERENCE_EXISTS = "Not NOO -> Reference ID not exist"
LABEL_REFERENCE_NEW = "NOO -> Create ID"


@dataclass
class DetectionResult:
    matched: bool
    label: str
    store_id: str = ""


def classify(store_result) -> DetectionResult:
    """Turn a `StoreEnricher.resolve()` result into a NOO/Not-NOO verdict.

    `store_result` is the SAME `enrichment.EnrichmentResult` the pipeline
    already computed for SE/SPV/AOM enrichment -- one composite-key lookup
    against master_store_database_basis serves both purposes, so this performs
    no additional lookup of its own.

    An ambiguous match (more than one basis row for the same reference id) is
    treated the same as no match: never guess which existing store this is,
    and never write a store_id that might be wrong.
    """
    matched = bool(store_result.matched) and not store_result.ambiguous
    if matched:
        return DetectionResult(matched=True, label=LABEL_REFERENCE_EXISTS,
                               store_id=store_result.resolved_store_id)
    return DetectionResult(matched=False, label=LABEL_REFERENCE_NEW,
                           store_id="")
