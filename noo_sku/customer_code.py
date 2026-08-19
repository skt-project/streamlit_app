"""Customer Code derivation.

    Customer Code = brand_prefix(2) + db_suffix(3-4)

Verified against 5,353 rows of the tracker's SKU MAPPING tab: every populated
Customer Code starts with 11 (SKINTIFIC), 13 (TIMEPHORIA) or 1A (FACERINNA),
and the remainder is the distributor's abbreviation.

The suffix has three possible sources, tried in this order:

  1. DIST DATABASE!"Customer Branch Code" — BD Support's own reference, which
     the MoM names as the source of truth. Covers 97 of 215 active distributors.
  2. SKU MAPPING history — what has actually been written to the tracker.
     Agrees with source 1 on 26 of 27 overlapping distributors.
  3. BigQuery dms.gt_po_tracking_all_mv — widest coverage, derived from PO
     history. Agrees with source 1 on all 63 overlapping distributors.

Union coverage is still only 124 of 215 active distributors. The remaining 91
resolve to nothing, and this module returns an explicit "unresolved" rather
than guessing — writing a wrong Customer Code into an operational tracker is
worse than refusing the upload.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from . import config
from .normalize import norm_key

SOURCE_DIST_DATABASE = "DIST_DATABASE"
SOURCE_SKU_HISTORY = "SKU_MAPPING_HISTORY"
SOURCE_PO_HISTORY = "PO_HISTORY"
SOURCE_OVERRIDE = "OVERRIDE"


@dataclass(frozen=True)
class SuffixResolution:
    """The outcome of resolving one distributor's abbreviation."""

    distributor_code: str
    suffix: str | None
    source: str | None
    candidates: dict = field(default_factory=dict)

    @property
    def resolved(self) -> bool:
        return bool(self.suffix)

    @property
    def conflict(self) -> bool:
        """True when two sources disagree about the suffix."""
        distinct = {v for v in self.candidates.values() if v}
        return len(distinct) > 1

    def customer_code(self, brand: str) -> str | None:
        if not self.resolved:
            return None
        prefix = config.BRAND_PREFIX.get(norm_key(brand))
        return f"{prefix}{self.suffix}" if prefix else None

    def all_customer_codes(self) -> dict:
        if not self.resolved:
            return {}
        return {b: f"{p}{self.suffix}" for b, p in config.BRAND_PREFIX.items()}


class CustomerCodeResolver:
    """Resolves distributor code -> abbreviation -> Customer Code.

    Each mapping argument is ``{distributor_code: suffix}``. Callers build them
    from the sources listed in the module docstring; this class stays pure so it
    can be tested without credentials.
    """

    #: Resolution order. Earlier entries win.
    PRIORITY = (SOURCE_OVERRIDE, SOURCE_DIST_DATABASE, SOURCE_SKU_HISTORY,
                SOURCE_PO_HISTORY)

    def __init__(self, dist_database=None, sku_history=None, po_history=None,
                 overrides=None):
        self._sources = {
            SOURCE_OVERRIDE: self._clean(overrides),
            SOURCE_DIST_DATABASE: self._clean(dist_database),
            SOURCE_SKU_HISTORY: self._clean(sku_history),
            SOURCE_PO_HISTORY: self._clean(po_history),
        }

    @staticmethod
    def _clean(mapping) -> dict:
        if not mapping:
            return {}
        out = {}
        for code, suffix in mapping.items():
            code, suffix = norm_key(code), norm_key(suffix)
            if code and suffix:
                out[code] = suffix
        return out

    def resolve(self, distributor_code: str) -> SuffixResolution:
        code = norm_key(distributor_code)
        candidates = {
            name: table.get(code)
            for name, table in self._sources.items()
            if table.get(code)
        }
        for source in self.PRIORITY:
            if candidates.get(source):
                return SuffixResolution(code, candidates[source], source,
                                        candidates)
        return SuffixResolution(code, None, None, candidates)

    def customer_code(self, distributor_code: str, brand: str) -> str | None:
        return self.resolve(distributor_code).customer_code(brand)

    def coverage(self, distributor_codes) -> dict:
        """Report resolution status across a population, for diagnostics."""
        resolved, unresolved, conflicts = [], [], []
        for code in distributor_codes:
            r = self.resolve(code)
            (resolved if r.resolved else unresolved).append(r.distributor_code)
            if r.conflict:
                conflicts.append(r)
        return {"resolved": resolved, "unresolved": unresolved,
                "conflicts": conflicts}


def brand_for_prefix(customer_code: str) -> str | None:
    """Inverse lookup: '11CEC' -> 'SKINTIFIC'."""
    code = norm_key(customer_code)
    for brand, prefix in config.BRAND_PREFIX.items():
        if code.startswith(prefix):
            return brand
    return None


def split_customer_code(customer_code: str) -> tuple[str | None, str | None]:
    """'11CEC' -> ('11', 'CEC'). Returns (None, None) on an unknown prefix."""
    code = norm_key(customer_code)
    for prefix in config.VALID_PREFIXES:
        if code.startswith(prefix) and len(code) > len(prefix):
            return prefix, code[len(prefix):]
    return None, None
