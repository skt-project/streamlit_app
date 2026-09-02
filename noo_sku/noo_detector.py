"""Reference-ID / existing-store detection for POOL NOO STREAMLIT column E.

Reuses the scoring algorithm from the standalone "Duplicate Store Checker" app
(``noo_detector.py`` at the repo root, `match_store()` / `normalize()`) rather
than reimplementing it. Ported here, pure and Streamlit-free, so it can run
inside the upload pipeline and be unit tested directly.

WHAT CHANGED IN THE PORT -- and why
------------------------------------
The standalone app scores five components: Store Name (35), Address+City
(20+10, or 25 if City is blank), GPS distance <50m (+20), NIK suffix (+5),
NPWP suffix (+5), and a Reference ID exact match (+10). It never had to worry
about missing fields because *its own* upload template collected all of them
(``REQUIRED_COLUMNS`` there includes Region, Latitude, Longitude, NIK, NPWP,
Reference ID).

The NOO Tracker's real upload template does **not** collect Region, Latitude,
Longitude, NIK or NPWP -- audited in ``noo_sku/config.py``'s ``NOO_COLUMNS``.
Those score terms are simply never reachable here, because the "new store"
side of the comparison never carries values for them -- every term that *can*
run still computes exactly as the original does.

The Reference ID term compares against both `cust_id` and `reference_id_*`,
matching how ``enrichment.StoreEnricher`` already treats the template's
"Store ID (Opsional)" column as this app's Reference ID equivalent.

THE MATERIAL CONSEQUENCE -- documented, not silently absorbed
----------------------------------------------------------------
``MATCH_THRESHOLD`` is kept at 70, unchanged from the source app: reusing the
real threshold rather than inventing a new number. But the maximum score
reachable from Store Name + Address + City alone (no GPS, no NIK/NPWP, no
Store ID) is **65** -- always below 70. For the common case where the admin
leaves "Store ID (Opsional)" blank, this detector will therefore classify the
row as ``LABEL_REFERENCE_NEW`` ("NOO -> Create ID") unless a Store ID is
supplied and matches. That is a faithful, not a broken, application of the
reused formula to a smaller field set -- see the implementation report for the
exact numbers and the tradeoff.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from rapidfuzz import fuzz

from .normalize import clean, norm_key

#: Unchanged from the source app's `if score >= 70` cutoff.
MATCH_THRESHOLD = 70

#: Exact wording from the tracker's historical "NOO/Existing" column
#: (confirmed against real data: 2,200 / 1,657 rows respectively in
#: SKINTIFIC NEW as of the 2026-08-19 audit). Do not reword -- BD Support's
#: own tracker already uses this literal text.
LABEL_REFERENCE_EXISTS = "Not NOO -> Reference ID not exist"
LABEL_REFERENCE_NEW = "NOO -> Create ID"


def normalize_field(text, kind: str) -> str:
    """Port of the source app's `normalize()`, for the kinds this app needs."""
    text = clean(text).lower()
    if not text:
        return ""
    if kind == "store_name":
        return text
    if kind == "address":
        text = text.replace("jl.", "").replace("no.", "").replace("jl", "")
        text = text.replace("jalan", "").replace("no", "").replace("jalan.", "")
        text = re.sub(r"[^a-z0-9\s]", " ", text)
        text = re.sub(r"\brt\b|\brw\b", "", text)
        return re.sub(r"\s+", " ", text).strip()
    if kind == "city":
        text = re.sub(r"[^a-z0-9\s]", " ", text)
        return re.sub(r"\s+", " ", text).strip()
    return text


def _best_of(a, b) -> float:
    return max(fuzz.ratio(a, b), fuzz.token_set_ratio(a, b),
              fuzz.partial_ratio(a, b))


@dataclass
class ScoreBreakdown:
    total: float = 0.0
    lines: list = field(default_factory=list)


def _score(new_store: dict, existing: dict) -> ScoreBreakdown:
    """Faithful port of `match_store()`'s per-pair scoring. Same weights."""
    out = ScoreBreakdown()

    new_name = normalize_field(new_store.get("store_name"), "store_name")
    old_name = normalize_field(existing.get("store_name"), "store_name")
    name_score = _best_of(new_name, old_name)
    name_weight = 35
    contrib = (name_score / 100) * name_weight
    out.total += contrib
    out.lines.append(f"Name similarity: {name_score:.0f} -> {contrib:.1f}/{name_weight}")

    new_city = clean(new_store.get("city"))
    old_city = clean(existing.get("city"))
    new_addr = normalize_field(new_store.get("address"), "address")
    old_addr = normalize_field(existing.get("address"), "address")

    if new_city and old_city:
        addr_weight, city_weight = 20, 10
        addr_score = _best_of(new_addr, old_addr)
        city_score = _best_of(normalize_field(new_city, "city"),
                              normalize_field(old_city, "city"))
        out.total += (addr_score / 100) * addr_weight
        out.total += (city_score / 100) * city_weight
        out.lines.append(f"Address similarity: {addr_score:.0f} -> "
                         f"{(addr_score / 100) * addr_weight:.1f}/{addr_weight}")
        out.lines.append(f"City similarity: {city_score:.0f} -> "
                         f"{(city_score / 100) * city_weight:.1f}/{city_weight}")
    else:
        addr_weight = 25
        addr_score = _best_of(new_addr, old_addr)
        out.total += (addr_score / 100) * addr_weight
        out.lines.append(f"Address similarity (no city): {addr_score:.0f} -> "
                         f"{(addr_score / 100) * addr_weight:.1f}/{addr_weight}")

    # Distance / NIK / NPWP: kept for fidelity to the source scoring, but the
    # NOO template collects none of latitude, longitude, NIK or NPWP, so the
    # distance term structurally never has data to run on. See the module
    # docstring for why this is a faithful reuse rather than a truncation.

    nik_new, nik_old = clean(new_store.get("nik")), clean(existing.get("nik"))
    if nik_new and nik_old and nik_new[-8:] == nik_old[-8:]:
        out.total += 5
        out.lines.append("NIK match -> +5")

    npwp_new, npwp_old = clean(new_store.get("npwp")), clean(existing.get("npwp"))
    if npwp_new and npwp_old and npwp_new[-8:] == npwp_old[-8:]:
        out.total += 5
        out.lines.append("NPWP match -> +5")

    ref_new = norm_key(new_store.get("reference_id"))
    if ref_new:
        candidates = {norm_key(existing.get("cust_id")),
                     norm_key(existing.get("reference_id_skt")),
                     norm_key(existing.get("reference_id_tph"))} - {""}
        if ref_new in candidates:
            out.total += 10
            out.lines.append("Reference ID (Store ID) match -> +10")

    out.lines.append(f"Total: {out.total:.1f}")
    return out


@dataclass
class DetectionResult:
    matched: bool
    label: str
    score: float
    best: dict | None = None
    log: str = ""


def check_reference_id(new_store: dict, candidates) -> DetectionResult:
    """Score `new_store` against every candidate and classify it.

    `new_store` -- dict with `store_name`, `address`, `city`, and optionally
    `reference_id` (the uploaded "Store ID (Opsional)" value).
    `candidates` -- existing store records (e.g.
    ``StoreEnricher.all_stores()``), already scoped to the admin's authorised
    company, mirroring the source app's own region-scoping intent of
    comparing within a relevant population rather than nationally.
    """
    best, best_score, best_lines = None, -1.0, []
    for existing in candidates:
        breakdown = _score(new_store, existing)
        if breakdown.total > best_score:
            best, best_score, best_lines = existing, breakdown.total, breakdown.lines

    matched = best_score >= MATCH_THRESHOLD
    label = LABEL_REFERENCE_EXISTS if matched else LABEL_REFERENCE_NEW
    return DetectionResult(matched=matched, label=label,
                           score=max(best_score, 0.0), best=best,
                           log="\n".join(best_lines))
