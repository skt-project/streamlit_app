"""
Canonical distributor identity for the Salesman / PJP flows.

THE RULE: a distributor IS its `distributor_code` (DST351, DST352, ...).
The name is a LABEL, resolved from `gt_schema.master_distributor` by that
code — never the other way round, and never trusted from a denormalized
snapshot.

Why this module exists
----------------------
`gt_master_salesman` (the SE Database) and `gt_master_salesman_pjp` both
store `nama_distributor` / `region` / `asm` as free-text values captured at
insert time. Both tables are append-only, so every spelling the master has
ever carried survives forever as its own row. DST351 accumulated two:

    2026-08-11  SILVIA LAURENS        PT ANUGERAH SUKSES MANDIRI - BANGKA
    2026-09-03  NUR DHIA RAHMADONNA   PT ANUGERAH SUKSES MANDIRI - BANGKA
    2026-09-03  PUTRA PRATAM          PT ANUGRAH  SUKSES MANDIRI - BANGKA

One distributor, one code, two names — so anything that groups by NAME sees
two distributors. Resolving by CODE makes the whole class of bug impossible,
which is why `canonical_name_by_code()` is the primary entry point here and
`normalize_distributor_name()` is only a fallback for the legacy join paths
that have no code to work with.

Deliberately NOT done: fuzzy/similarity auto-merging. "ANUGERAH" vs
"ANUGRAH" differs by one letter, but so do genuinely distinct distributors
(e.g. the BANGKA / BELITUNG pair share every word but the last). Silently
merging on edit distance would be a data-corrupting guess. Near-matches are
REPORTED by find_name_variants() / find_cross_code_collisions() so a human
can fix the master; they are never merged automatically.
"""
from __future__ import annotations

import re

# ─── Normalization ─────────────────────────────────────────────────────────

# Punctuation that carries no identity, only typing noise. Kept to this
# exact class so the BigQuery mirror in salesman_pjp.py's _norm_name_sql()
# can use the identical character class with no quoting gymnastics.
_PUNCT_NOISE = re.compile(r"[.,;:]")
# Any run of whitespace around a hyphen -> exactly " - " (fixes the real
# "PT ANUGRAH SUKSES MANDIRI- BELITUNG" vs "... MANDIRI - BELITUNG" split).
_DASH = re.compile(r"\s*[-–—]\s*")
_WS = re.compile(r"\s+")


def normalize_distributor_name(name) -> str:
    """
    The comparison form of a distributor name: uppercase, punctuation noise
    dropped, whitespace collapsed, hyphen spacing regularised.

        "PT Anugrah Sukses Mandiri- Belitung"  ->  "PT ANUGRAH SUKSES MANDIRI - BELITUNG"
        "  PT ANUGRAH  SUKSES MANDIRI -BANGKA" ->  "PT ANUGRAH SUKSES MANDIRI - BANGKA"

    Lossless with respect to identity: it only removes formatting, never a
    word. "BANGKA" and "BELITUNG" stay distinct, and so do "ANUGERAH" and
    "ANUGRAH" — that pair is resolved by CODE, not by normalizing the
    spelling away. Returns "" for blank/None.

    Mirrors the SQL in salesman_pjp.py's `_DIST_CTE`; keep the two in step.
    """
    if name is None:
        return ""
    text = str(name).strip()
    if not text or text.upper() in {"NAN", "NONE", "<NA>", "NAT"}:
        return ""
    text = _PUNCT_NOISE.sub("", text.upper())
    text = _DASH.sub(" - ", text)
    return _WS.sub(" ", text).strip()


# ─── The same normalization, in BigQuery ──────────────────────────────────

def norm_name_sql(col: str) -> str:
    """
    BigQuery mirror of normalize_distributor_name(), as a SQL expression over
    `col`. Upper/trim -> drop [.,;:] -> hyphen runs to " - " -> collapse
    whitespace, in that order.

    The two implementations must agree exactly; tests/test_distributor_naming.py
    pins the shared cases and a differential check compares them over every
    distributor name in the warehouse. The character classes are deliberately
    kept simple enough to express identically in both languages.
    """
    return (
        "TRIM(REGEXP_REPLACE("
        "REGEXP_REPLACE("
        f"REGEXP_REPLACE(UPPER(TRIM({col})), r'[.,;:]', ''), "
        "r'\\s*[-\u2013\u2014]\\s*', ' - '), "
        "r'\\s+', ' '))"
    )


# The ONE definition of "the canonical distributor record", shared by
# salesman_pjp.py, salesman_pjp_v2/ and the gt_master_salesman_v DDL so the
# three can never drift apart.
#
# Emits one row per distributor_code with:
#   distributor_code       the identity
#   distributor_name       master name, formatting-normalized for display
#   distributor_name_raw   master name exactly as the source sheet has it
#   distributor_name_norm  join key for legacy name-based lookups
#   region_g2g             CURRENT org mapping  <- the September 2026 answer
#
# region_g2g is named in full, NEVER aliased to `region`. master_distributor
# has BOTH a `region` column (the OLD mapping: DST351/352 = Southern Sumatera
# 2) and a `region_g2g` column (the CURRENT one: Southern Sumatera 1). An
# alias that shadows the other column's name is exactly how someone reads
# `d.region` and reasonably believes the stale column is being used.
CANONICAL_DIST_CTE = """
    SELECT
        UPPER(TRIM(distributor_code)) AS distributor_code,
        {norm_display}                AS distributor_name,
        UPPER(TRIM(distributor))      AS distributor_name_raw,
        {norm_key}                    AS distributor_name_norm,
        UPPER(TRIM(region_g2g))       AS region_g2g,
        UPPER(TRIM(asm_g2g))          AS asm_g2g
    FROM `{table}`
    WHERE region_g2g IS NOT NULL AND TRIM(region_g2g) != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY UPPER(TRIM(distributor_code))
        ORDER BY CASE WHEN UPPER(TRIM(status)) = 'ACTIVE' THEN 0 ELSE 1 END
    ) = 1
"""

MASTER_DISTRIBUTOR_TABLE = "skintific-data-warehouse.gt_schema.master_distributor"


def canonical_dist_cte(table: str = MASTER_DISTRIBUTOR_TABLE) -> str:
    """
    The canonical-distributor CTE body, ready to drop into a WITH clause.

    `distributor_name` is the NORMALIZED master name. That is a display
    decision, and a deliberate one: the master itself carries formatting
    noise the source sheet introduced (DST352 is stored
    "PT ANUGRAH SUKSES MANDIRI- BELITUNG", missing the space before the
    hyphen; DST227 is mixed-case). Normalizing here renders the
    business-standard form without editing production master data. Only
    formatting is touched — never a word, so no two distributors can merge.
    `distributor_name_raw` keeps the source value for auditing.
    """
    return CANONICAL_DIST_CTE.format(
        table=table,
        norm_display=norm_name_sql("distributor"),
        norm_key=norm_name_sql("distributor"),
    )


# ─── Code-first resolution (the primary path) ─────────────────────────────

def canonical_name_by_code(code, master_by_code: dict, fallback=None) -> str:
    """
    The one true display name for a distributor code.

    `master_by_code` maps an UPPER/TRIMmed distributor_code to its
    master_distributor name. `fallback` (typically the row's own stored
    name) is used only when the code is absent from the master, so a
    distributor that has not been onboarded yet still renders something
    rather than vanishing.

    This is what makes the ANUGERAH/ANUGRAH duplicate disappear: both rows
    carry code DST351, so both resolve to the master's single name.
    """
    key = str(code or "").strip().upper()
    if key and key in master_by_code:
        return master_by_code[key]
    return "" if fallback is None else str(fallback).strip()


def build_master_by_code(rows) -> dict:
    """
    {distributor_code -> distributor_name} from master_distributor rows
    (an iterable of mappings with 'distributor_code' / 'distributor_name').
    Later rows do not clobber earlier ones, so pass the already-deduplicated
    master (the QUALIFY in the SQL prefers Active).
    """
    out: dict[str, str] = {}
    for r in rows:
        code = str(r.get("distributor_code") or "").strip().upper()
        name = str(r.get("distributor_name") or "").strip()
        if code and name and code not in out:
            out[code] = name
    return out


# ─── Drift detectors (report, never auto-merge) ───────────────────────────

def find_name_variants(rows) -> dict[str, list[str]]:
    """
    Codes that appear under more than one distinct name — the defect reported
    for DST351. Returns {code: [names...]} for offending codes only, names
    sorted for stable output.

    Compares RAW names, not normalized ones. A consumer that groups by name
    sees exactly these strings, so any difference splits the distributor in
    two — including a purely cosmetic one. The live SE Database carried both
    kinds at once:

        DST351  "PT ANUGERAH ..." vs "PT ANUGRAH ..."      spelling
        DST227  "UD Mitra Kencana" vs "UD MITRA KENCANA"   case only

    Normalizing first would have hidden DST227 entirely. Use
    variant_is_formatting_only() to tell the two classes apart once found.
    """
    by_code: dict[str, set[str]] = {}
    for r in rows:
        code = str(r.get("distributor_code") or "").strip().upper()
        raw = str(r.get("distributor_name") or "").strip()
        if not code or not raw or not normalize_distributor_name(raw):
            continue
        by_code.setdefault(code, set()).add(raw)
    return {
        code: sorted(variants)
        for code, variants in by_code.items()
        if len(variants) > 1
    }


def variant_is_formatting_only(names) -> bool:
    """
    True when every name in the group collapses to the same normalized form,
    i.e. the split is case/spacing/punctuation noise (DST227) rather than a
    genuine spelling divergence (DST351). Both still need fixing; only the
    second one needs a human to decide which spelling is right.
    """
    forms = {normalize_distributor_name(n) for n in names}
    forms.discard("")
    return len(forms) <= 1


def find_cross_code_collisions(rows) -> dict[str, list[str]]:
    """
    The mirror defect: one name shared by several codes, which would make
    a name-based join fan out. Returns {normalized_name: [codes...]}.
    """
    by_name: dict[str, set[str]] = {}
    for r in rows:
        code = str(r.get("distributor_code") or "").strip().upper()
        norm = normalize_distributor_name(r.get("distributor_name"))
        if not code or not norm:
            continue
        by_name.setdefault(norm, set()).add(code)
    return {
        name: sorted(codes) for name, codes in by_name.items() if len(codes) > 1
    }
