"""Master-data enrichment.

Runs after validation and before duplicate detection, so that duplicate keys are
computed on the final enriched record rather than on raw user input.

Three resolvers, each pure — they take already-loaded lookup tables and return a
result plus the notes needed to explain themselves in the preview:

    DistributorEnricher  master_distributor, keyed on the authenticated code
    StoreEnricher        master_store_database_basis, composite key
    ProductEnricher      master_product, keyed on SKU

Approved behaviour (decision B2): enrich-if-available, otherwise blank. A store
absent from the master is the *expected* case for a genuine new outlet and must
never reject the row. Only missing **user input** is an error. An ambiguous
master match is never resolved by picking one arbitrarily — it is left blank and
flagged for review.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from . import config
from .normalize import clean, norm_key

# Per-brand column suffixes. master_store_database_basis spells Facerinna's SE
# column `se_fcr` and has no asm_/spv_/aom_ variant for it at all, so those fall
# through to master_distributor. Getting this table wrong silently writes another
# brand's people into the pool, so it is table-driven and tested per brand.
BRAND_SUFFIX = {
    "SKINTIFIC": {"basis": "skt", "dist": "skt"},
    "TIMEPHORIA": {"basis": "tph", "dist": "tph"},
    "FACERINNA": {"basis": "fcr", "dist": "fr"},
}

# Enrichment that legitimately stays blank for a brand-new store (decision B2).
OPTIONAL_ENRICHMENT = frozenset({
    "se_kae", "spv", "aom", "area", "province", "asm_name", "asm_kam", "asm",
})

STATUS_RESOLVED = "resolved"
STATUS_MISSING = "missing"
STATUS_AMBIGUOUS = "ambiguous"
STATUS_NEW_STORE = "new_store"
STATUS_FALLBACK = "fallback"


@dataclass
class EnrichmentNote:
    """One thing worth telling the user about an enrichment attempt."""

    row: int
    field: str
    status: str
    detail: str

    @property
    def is_blocking(self) -> bool:
        # Nothing here blocks: per B2 unavailable enrichment is informational.
        return False


#: How a value was obtained. Surfaced in the preview so a fallback is never
#: silent - required for UAT verification of thin-coverage brands like FACERINNA.
SOURCE_MASTER_DISTRIBUTOR = "MASTER_DISTRIBUTOR"
SOURCE_BASIS = "MASTER_STORE_BASIS"
SOURCE_DIST_DATABASE_SHEET = "DIST DATABASE"
SOURCE_BRAND_NEUTRAL_FALLBACK = "BRAND-NEUTRAL FALLBACK"
SOURCE_NONE = "NOT AVAILABLE"


@dataclass
class EnrichmentResult:
    values: dict = field(default_factory=dict)
    notes: list = field(default_factory=list)
    matched: bool = False
    ambiguous: bool = False
    sources: dict = field(default_factory=dict)
    matched_on: str = ""
    #: The MASTER's own store identifier (`cust_id`) for the matched record,
    #: never the admin's typed value. Blank unless `matched` and not
    #: `ambiguous` — the NOO Detector auto-populates the pool's `store_id` from
    #: this, and only this.
    resolved_store_id: str = ""

    @property
    def mapping_source(self) -> str:
        """Single label describing how this record was mapped."""
        used = set(self.sources.values()) - {SOURCE_NONE}
        if not used:
            return SOURCE_NONE
        fallbacks = [u for u in used if "FALLBACK" in u]
        return fallbacks[0] if fallbacks else sorted(used)[0]

    @property
    def used_fallback(self) -> bool:
        return any("FALLBACK" in u for u in self.sources.values())


def _pick(record, *names):
    """First non-blank value among the named keys of a mapping."""
    if not record:
        return ""
    for name in names:
        value = clean(record.get(name))
        if value:
            return value
    return ""


class DistributorEnricher:
    """Distributor-level attributes, keyed on the authenticated distributor code.

    `master_distributor` is authoritative. Where a per-brand column is blank —
    common for FACERINNA, where asm_fr is only 52% populated — the brand-neutral
    column is tried, then the DIST DATABASE record the session already carries.
    """

    def __init__(self, master_distributor=None, dist_database=None):
        self._md = {norm_key(k): v for k, v in (master_distributor or {}).items()}
        self._dd = {norm_key(k): v for k, v in (dist_database or {}).items()}

    @staticmethod
    def _brand_then_neutral(md, base, suffix):
        """Per-brand column, else the brand-neutral one, reporting which."""
        if suffix:
            value = _pick(md, base + "_" + suffix)
            if value:
                return value, SOURCE_MASTER_DISTRIBUTOR
        value = _pick(md, base)
        if value:
            return value, (SOURCE_BRAND_NEUTRAL_FALLBACK if suffix
                           else SOURCE_MASTER_DISTRIBUTOR)
        return "", SOURCE_NONE

    def resolve(self, distributor_code, brand, row=0) -> EnrichmentResult:
        code = norm_key(distributor_code)
        md = self._md.get(code)
        dd = self._dd.get(code)
        suffix = BRAND_SUFFIX.get(norm_key(brand), {}).get("dist", "")
        result = EnrichmentResult(matched=md is not None)

        name = _pick(md, "distributor", "distributor_company")
        name_source = SOURCE_MASTER_DISTRIBUTOR
        if not name:
            name = _pick(dd, "distributor_name", "company")
            name_source = SOURCE_DIST_DATABASE_SHEET if name else SOURCE_NONE

        region = _pick(md, "region", "region_g2g")
        region_source = SOURCE_MASTER_DISTRIBUTOR
        if not region:
            region = _pick(dd, "region")
            region_source = SOURCE_DIST_DATABASE_SHEET if region else SOURCE_NONE

        asm, asm_source = self._brand_then_neutral(md, "asm", suffix)
        aom, aom_source = self._brand_then_neutral(md, "aom", suffix)
        spv = _pick(md, "spv_" + suffix) if suffix else ""
        spv_source = SOURCE_MASTER_DISTRIBUTOR if spv else SOURCE_NONE

        result.values = {"branch_name": name, "region": region, "asm": asm,
                         "aom": aom, "spv": spv}
        result.sources = {"branch_name": name_source, "region": region_source,
                          "asm": asm_source, "aom": aom_source,
                          "spv": spv_source}

        if md is None:
            result.notes.append(EnrichmentNote(
                row, "branch_name", STATUS_MISSING,
                f"{code} tidak ada di master_distributor; memakai DIST DATABASE."))
        for label in ("asm", "aom", "spv"):
            src = result.sources.get(label)
            if src == SOURCE_NONE:
                result.notes.append(EnrichmentNote(
                    row, label, STATUS_MISSING,
                    f"{label.upper()} untuk brand {brand} tidak tersedia di master."))
            elif src == SOURCE_BRAND_NEUTRAL_FALLBACK:
                result.notes.append(EnrichmentNote(
                    row, label, STATUS_FALLBACK,
                    f"{label.upper()} khusus brand {brand} kosong di master; "
                    f"memakai kolom {label} umum (BRAND-NEUTRAL FALLBACK)."))
        return result


class StoreEnricher:
    """Store-level attributes from master_store_database_basis.

    Composite key, measured at 99.9% resolution on 3,857 historical rows:

      1. `store_id` -> `cust_id`            — unique, 99.9% on its own
      2. `customer_store_code` -> `reference_id_{brand}` — 98.2%, but 303
         reference ids map to more than one basis row

    A reference id that resolves to several rows is treated as **ambiguous**: no
    record is chosen, the fields stay blank and a note is raised. Picking one
    arbitrarily would silently attach the wrong SE to a store.
    """

    def __init__(self, by_cust_id=None, by_reference=None):
        self._by_cust = {norm_key(k): v for k, v in (by_cust_id or {}).items()}
        # {brand_suffix: {reference_id: [records]}}
        self._by_ref = by_reference or {}

    def _lookup(self, store_id, store_code, suffix):
        """Composite key: cust_id first (unique), then a UNIQUE reference id."""
        cust = self._by_cust.get(norm_key(store_id))
        if cust:
            return cust, False, "cust_id"
        table = self._by_ref.get(suffix) or {}
        hits = table.get(norm_key(store_code)) or []
        if len(hits) == 1:
            return hits[0], False, "reference_id_" + suffix
        if len(hits) > 1:
            return None, True, "reference_id_" + suffix
        return None, False, ""

    def resolve(self, *, store_id, store_code, brand, row=0) -> EnrichmentResult:
        suffix = BRAND_SUFFIX.get(norm_key(brand), {}).get("basis", "")
        record, ambiguous, matched_on = self._lookup(store_id, store_code,
                                                     suffix)
        result = EnrichmentResult(matched=record is not None,
                                  ambiguous=ambiguous, matched_on=matched_on)

        if ambiguous:
            result.notes.append(EnrichmentNote(
                row, "se_kae", STATUS_AMBIGUOUS,
                f"'{store_code}' cocok dengan lebih dari satu data master. "
                "Kolom enrichment dikosongkan untuk diperiksa BD Support."))
            result.values = {k: "" for k in
                             ("se_kae", "spv", "aom", "area", "province")}
            result.sources = {k: SOURCE_NONE for k in result.values}
            return result

        if record is None:
            result.notes.append(EnrichmentNote(
                row, "se_kae", STATUS_NEW_STORE,
                "Toko belum ada di master (wajar untuk NOO baru). "
                "Kolom SE/SPV/AOM/Area/Province dikosongkan."))
            result.values = {k: "" for k in
                             ("se_kae", "spv", "aom", "area", "province")}
            result.sources = {k: SOURCE_NONE for k in result.values}
            return result

        result.resolved_store_id = norm_key(record.get("cust_id"))
        result.values = {
            "se_kae": _pick(record, f"se_{suffix}", "se_fcr"),
            "spv": _pick(record, f"spv_{suffix}"),
            "aom": _pick(record, f"aom_{suffix}"),
            "area": _pick(record, "area_coverage"),
            "province": _pick(record, "province"),
            "city": _pick(record, "city"),
            "store_type": _pick(record, "customer_type"),
        }
        result.sources = {k: (SOURCE_BASIS if v else SOURCE_NONE)
                          for k, v in result.values.items()}
        for label in ("se_kae", "spv", "aom"):
            if not result.values.get(label):
                result.notes.append(EnrichmentNote(
                    row, label, STATUS_MISSING,
                    f"{label.upper()} kosong di master untuk toko ini."))
        return result


class ProductEnricher:
    """Product attributes from master_product, keyed on the principal SKU.

    master_product is authoritative for `product_name` and `specification`. The
    user's own values are kept only when the master has none — the guideline
    tells admins these must match the principal exactly, so the master wins.
    `barcode` is not sourced here: the column is INT64 and predominantly 0.
    """

    def __init__(self, products=None):
        self._products = {norm_key(k): v for k, v in (products or {}).items()}

    def resolve(self, sku, *, fallback_name="", fallback_size="",
                row=0) -> EnrichmentResult:
        record = self._products.get(norm_key(sku))
        result = EnrichmentResult(matched=record is not None)
        if record is None:
            result.values = {"product_name": clean(fallback_name),
                             "specification": clean(fallback_size), "brand": ""}
            result.notes.append(EnrichmentNote(
                row, "product_name", STATUS_MISSING,
                f"'{sku}' tidak ada di master_product."))
            return result

        result.values = {
            "product_name": clean(record.get("product_name")) or clean(fallback_name),
            "specification": clean(record.get("pack_size")) or clean(fallback_size),
            "brand": norm_key(record.get("brand")),
        }
        return result


def summarise_notes(notes) -> dict:
    """Counts per status, for the preview panel."""
    out = {}
    for note in notes:
        out[note.status] = out.get(note.status, 0) + 1
    return out
