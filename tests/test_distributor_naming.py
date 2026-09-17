"""
Canonical distributor identity — normalization, code-first resolution and
the drift detectors.

No credentials, no network, no Streamlit — pure functions only.
Fixtures mirror the real rows found in gt_schema.gt_master_salesman.

Run: pytest tests/test_distributor_naming.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from distributor_naming import (  # noqa: E402
    build_master_by_code,
    canonical_name_by_code,
    find_cross_code_collisions,
    find_name_variants,
    normalize_distributor_name,
    variant_is_formatting_only,
)

BANGKA = "PT ANUGRAH SUKSES MANDIRI - BANGKA"
BANGKA_TYPO = "PT ANUGERAH SUKSES MANDIRI - BANGKA"   # the reported duplicate
BELITUNG_MASTER = "PT ANUGRAH SUKSES MANDIRI- BELITUNG"  # master's own spacing
BELITUNG_SPACED = "PT ANUGRAH SUKSES MANDIRI - BELITUNG"

# master_distributor as it stands today.
MASTER = [
    {"distributor_code": "DST351", "distributor_name": BANGKA},
    {"distributor_code": "DST352", "distributor_name": BELITUNG_MASTER},
]


# ─── Normalization ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("PT Anugrah Sukses Mandiri - Bangka", BANGKA),
    ("pt anugrah sukses mandiri - bangka", BANGKA),
    ("  PT ANUGRAH  SUKSES   MANDIRI - BANGKA  ", BANGKA),
    ("PT ANUGRAH SUKSES MANDIRI -BANGKA", BANGKA),
    ("PT ANUGRAH SUKSES MANDIRI- BANGKA", BANGKA),
    ("PT ANUGRAH SUKSES MANDIRI-BANGKA", BANGKA),
    # The DST352 spacing split the master actually carries.
    (BELITUNG_MASTER, BELITUNG_SPACED),
    (BELITUNG_SPACED, BELITUNG_SPACED),
    # En/em dashes are hyphens for our purposes.
    ("PT ANUGRAH SUKSES MANDIRI – BANGKA", BANGKA),
    ("PT ANUGRAH SUKSES MANDIRI — BANGKA", BANGKA),
    ("PT. ANUGRAH SUKSES MANDIRI - BANGKA", BANGKA),
])
def test_normalize_collapses_formatting(raw, expected):
    assert normalize_distributor_name(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "   ", "nan", "NaN", "None", "<NA>"])
def test_normalize_blank_like(raw):
    assert normalize_distributor_name(raw) == ""


def test_normalize_never_merges_different_distributors():
    """The safety property: normalization removes formatting, never a word."""
    assert normalize_distributor_name(BANGKA) != normalize_distributor_name(BELITUNG_MASTER)


def test_normalize_does_not_paper_over_a_real_misspelling():
    """
    ANUGERAH vs ANUGRAH must stay DIFFERENT after normalization. Collapsing
    them would mean guessing that a one-letter difference is always a typo —
    which would also merge genuinely distinct companies. That pair is
    resolved by CODE instead; see test_canonical_name_by_code_*.
    """
    assert normalize_distributor_name(BANGKA) != normalize_distributor_name(BANGKA_TYPO)


def test_normalize_is_idempotent():
    for raw in (BANGKA, BANGKA_TYPO, BELITUNG_MASTER, "PT X-Y", "a  b"):
        once = normalize_distributor_name(raw)
        assert normalize_distributor_name(once) == once


# ─── Code-first resolution ─────────────────────────────────────────────────

def test_build_master_by_code():
    m = build_master_by_code(MASTER)
    assert m == {"DST351": BANGKA, "DST352": BELITUNG_MASTER}


@pytest.mark.parametrize("stored_name", [BANGKA, BANGKA_TYPO, "", None, "GARBAGE"])
def test_canonical_name_by_code_ignores_the_stored_name(stored_name):
    """
    THE FIX, in one assertion: whatever spelling a historical row froze in,
    DST351 resolves to the master's single current name.
    """
    m = build_master_by_code(MASTER)
    assert canonical_name_by_code("DST351", m, fallback=stored_name) == BANGKA


def test_canonical_name_by_code_is_case_and_space_insensitive_on_the_code():
    m = build_master_by_code(MASTER)
    for code in ("DST351", "dst351", "  DST351  ", " dst351"):
        assert canonical_name_by_code(code, m) == BANGKA


def test_canonical_name_by_code_keeps_bangka_and_belitung_distinct():
    m = build_master_by_code(MASTER)
    assert canonical_name_by_code("DST351", m) == BANGKA
    assert canonical_name_by_code("DST352", m) == BELITUNG_MASTER
    assert canonical_name_by_code("DST351", m) != canonical_name_by_code("DST352", m)


def test_canonical_name_falls_back_for_unknown_code():
    """An un-onboarded distributor still renders, rather than vanishing."""
    m = build_master_by_code(MASTER)
    assert canonical_name_by_code("DST999", m, fallback="PT NEW CO") == "PT NEW CO"
    assert canonical_name_by_code("DST999", m) == ""
    assert canonical_name_by_code(None, m, fallback="PT NEW CO") == "PT NEW CO"


# ─── Drift detectors ───────────────────────────────────────────────────────

# The five real gt_master_salesman rows for these two codes.
SE_DATABASE_ROWS = [
    {"distributor_code": "DST351", "distributor_name": BANGKA_TYPO},   # 2026-08-11
    {"distributor_code": "DST351", "distributor_name": BANGKA_TYPO},   # 2026-09-03
    {"distributor_code": "DST351", "distributor_name": BANGKA},        # 2026-09-03
    {"distributor_code": "DST352", "distributor_name": BELITUNG_MASTER},
    {"distributor_code": "DST352", "distributor_name": BELITUNG_MASTER},
]


def test_find_name_variants_catches_the_reported_duplicate():
    variants = find_name_variants(SE_DATABASE_ROWS)
    assert set(variants) == {"DST351"}, "only DST351 is split"
    assert variants["DST351"] == sorted([BANGKA, BANGKA_TYPO])


def test_find_name_variants_catches_case_only_splits():
    """
    DST227 in production differs only by case. An earlier version of this
    detector normalized before comparing and silently missed it — yet a
    consumer grouping by raw name still saw two distributors.
    """
    rows = [
        {"distributor_code": "DST227", "distributor_name": "UD Mitra Kencana - Manado"},
        {"distributor_code": "DST227", "distributor_name": "UD MITRA KENCANA - MANADO"},
    ]
    assert set(find_name_variants(rows)) == {"DST227"}


def test_variant_is_formatting_only_separates_the_two_classes():
    assert variant_is_formatting_only(
        ["UD Mitra Kencana - Manado", "UD MITRA KENCANA - MANADO"]) is True
    assert variant_is_formatting_only([BELITUNG_MASTER, BELITUNG_SPACED]) is True
    # A real spelling divergence needs a human to pick the right one.
    assert variant_is_formatting_only([BANGKA, BANGKA_TYPO]) is False


def test_find_name_variants_clean_data_reports_nothing():
    clean = [
        {"distributor_code": "DST351", "distributor_name": BANGKA},
        {"distributor_code": "DST351", "distributor_name": BANGKA},
        {"distributor_code": "DST352", "distributor_name": BELITUNG_MASTER},
    ]
    assert find_name_variants(clean) == {}


def test_find_name_variants_ignores_blanks():
    rows = [
        {"distributor_code": "DST351", "distributor_name": BANGKA},
        {"distributor_code": "DST351", "distributor_name": None},
        {"distributor_code": "DST351", "distributor_name": ""},
    ]
    assert find_name_variants(rows) == {}


def test_find_cross_code_collisions():
    """One name on two codes — a name-based join would fan out."""
    rows = [
        {"distributor_code": "DST351", "distributor_name": BANGKA},
        {"distributor_code": "DST999", "distributor_name": BANGKA},
    ]
    assert find_cross_code_collisions(rows) == {BANGKA: ["DST351", "DST999"]}


def test_find_cross_code_collisions_matches_across_formatting():
    """The two BELITUNG spacings are the same name for collision purposes."""
    rows = [
        {"distributor_code": "DST352", "distributor_name": BELITUNG_MASTER},
        {"distributor_code": "DST353", "distributor_name": BELITUNG_SPACED},
    ]
    assert find_cross_code_collisions(rows) == {BELITUNG_SPACED: ["DST352", "DST353"]}


def test_bangka_and_belitung_are_not_a_collision():
    assert find_cross_code_collisions(MASTER) == {}


def test_live_master_shape_is_clean():
    """master_distributor itself is the source of truth and must be clean."""
    assert find_name_variants(MASTER) == {}
    assert find_cross_code_collisions(MASTER) == {}
