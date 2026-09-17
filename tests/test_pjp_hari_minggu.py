"""
Salesman PJP (G2G) — Frekuensi (I) / Hari (J) / Minggu (K) / Ket. Minggu (L).

No credentials, no network, no Streamlit — pure functions only.

Run: pytest tests/test_pjp_hari_minggu.py -q
"""
from __future__ import annotations

import pytest

from pjp_hari_minggu import (
    HARI_CANONICAL_ORDER,
    HARI_COMBOS_BY_FREKUENSI,
    KET_OPTIONS_BY_FREKUENSI_MINGGU,
    MINGGU_GANJIL,
    MINGGU_GANJIL_GENAP,
    MINGGU_GENAP,
    MINGGU_OPTIONS_BY_FREKUENSI,
    WEEK_POOL,
    auto_callcycle,
    derive_minggu_from_callcycle,
    expand_hari_callcycle,
    ket_minggu_options,
    migrate_legacy_minggu,
    minggu_options_for_frekuensi,
    to_minggu_pattern,
    normalize_callcycle,
    normalize_hari,
    normalize_minggu,
    parity_label,
    week_combos_for_frekuensi,
)

GANJIL, GENAP, BOTH = MINGGU_GANJIL, MINGGU_GENAP, MINGGU_GANJIL_GENAP


# ─── Column J — Hari ───────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("SENIN", "SENIN"),
    ("SENIN/SELASA", "SENIN/SELASA"),
    ("SENIN,SELASA", "SENIN/SELASA"),
    ("SENIN / SELASA", "SENIN/SELASA"),
    ("senin/selasa", "SENIN/SELASA"),
    ("RABU/SENIN", "SENIN/RABU"),
])
def test_hari_syntax_valid(raw, expected):
    got, err = normalize_hari(raw)
    assert err is None and got == expected


@pytest.mark.parametrize("raw", [
    "SENIN/SENIN", "SENIN/SELASA/SENIN", "MINGGU", "SENIN/MINGGU",
    "MONDAY", "MINGU", "SENINX", "", "   ",
])
def test_hari_syntax_invalid(raw):
    got, err = normalize_hari(raw)
    assert got is None and err is not None


def test_hari_sunday_rejected_with_specific_message():
    _, err = normalize_hari("MINGGU")
    assert "MINGGU" in err


@pytest.mark.parametrize("frekuensi,raw", [
    ("F1", "SENIN"), ("F1", "SABTU"),
    ("F2", "SENIN/SELASA"), ("F2", "SENIN/RABU"),
    # F2 also accepts a SINGLE day: the week pattern (1,3 / 2,4) is what
    # makes it twice-monthly, so one day is a complete assignment.
    ("F2", "SENIN"), ("F2", "SELASA"), ("F2", "JUMAT"), ("F2", "SABTU"),
    ("F4", "SENIN"), ("F4", "SENIN/SELASA/RABU/KAMIS"),
    ("F4+", "SENIN"), ("F4+", "SENIN/SELASA/RABU/KAMIS/JUMAT"),
])
def test_hari_day_count_valid(frekuensi, raw):
    got, err = normalize_hari(raw, frekuensi=frekuensi)
    assert err is None and got == raw


@pytest.mark.parametrize("frekuensi,raw", [
    ("F1", "SENIN/SELASA"),
    ("F2", "SENIN/SELASA/RABU"),
    ("F4", "SENIN/SELASA/RABU/KAMIS/JUMAT"),
    ("F4+", "SENIN/SELASA/RABU/KAMIS/JUMAT/SABTU"),
])
def test_hari_day_count_invalid(frekuensi, raw):
    got, err = normalize_hari(raw, frekuensi=frekuensi)
    assert got is None and err is not None


def test_hari_combo_counts_and_no_sunday():
    assert len(HARI_COMBOS_BY_FREKUENSI["F1"]) == 6
    assert len(HARI_COMBOS_BY_FREKUENSI["F2"]) == 21  # 6 single + 15 pairs
    assert len(HARI_COMBOS_BY_FREKUENSI["F4"]) == 56
    assert len(HARI_COMBOS_BY_FREKUENSI["F4+"]) == 62
    assert "MINGGU" not in HARI_CANONICAL_ORDER
    for combos in HARI_COMBOS_BY_FREKUENSI.values():
        for c in combos:
            assert "MINGGU" not in c.split("/")


# ─── Column K — Minggu (options depend on Frekuensi) ──────────────────────

def test_minggu_options_per_frekuensi():
    assert MINGGU_OPTIONS_BY_FREKUENSI["F1"] == [GANJIL, GENAP]
    # F2 picks TWO weeks out of 1-4. A mixed pair (1,2 / 1,4 / 2,3 / 3,4)
    # is neither purely odd nor purely even, so F2 must also offer
    # "Ganjil + Genap" — without it those four pairs are unreachable.
    assert MINGGU_OPTIONS_BY_FREKUENSI["F2"] == [GANJIL, GENAP, BOTH]
    assert MINGGU_OPTIONS_BY_FREKUENSI["F4"] == [BOTH]
    assert MINGGU_OPTIONS_BY_FREKUENSI["F4+"] == [BOTH]
    assert minggu_options_for_frekuensi("F1") == [GANJIL, GENAP]


@pytest.mark.parametrize("frekuensi,raw,expected", [
    ("F1", "Minggu Ganjil", GANJIL),
    ("F1", "minggu ganjil", GANJIL),
    ("F1", "  Minggu   Genap ", GENAP),
    ("F2", "Minggu Genap", GENAP),
    ("F4", "Minggu Ganjil + Genap", BOTH),
    ("F4", "Minggu Ganjil+Genap", BOTH),
    ("F4+", "minggu ganjil + genap", BOTH),
])
def test_minggu_valid(frekuensi, raw, expected):
    got, err = normalize_minggu(raw, frekuensi)
    assert err is None and got == expected


@pytest.mark.parametrize("frekuensi,raw", [
    ("F1", BOTH),      # F1 picks ONE week: always purely odd or purely even
    ("F4", GANJIL),    # F4 covers the whole cycle
    ("F4", GENAP),
    ("F4+", GANJIL),
    ("F1", "Minggu Ketiga"),
    ("F1", ""),
])
def test_minggu_invalid(frekuensi, raw):
    got, err = normalize_minggu(raw, frekuensi)
    assert got is None and err is not None


# ─── Column L — Ket. Minggu / callcycle ───────────────────────────────────

def test_ket_option_matrix_matches_spec():
    assert KET_OPTIONS_BY_FREKUENSI_MINGGU[("F1", GANJIL)] == ["1", "3"]
    assert KET_OPTIONS_BY_FREKUENSI_MINGGU[("F1", GENAP)] == ["2", "4"]
    assert KET_OPTIONS_BY_FREKUENSI_MINGGU[("F2", GANJIL)] == ["1,3"]
    assert KET_OPTIONS_BY_FREKUENSI_MINGGU[("F2", GENAP)] == ["2,4"]
    assert KET_OPTIONS_BY_FREKUENSI_MINGGU[("F4", BOTH)] == ["1,2,3,4"]
    assert KET_OPTIONS_BY_FREKUENSI_MINGGU[("F4+", BOTH)] == ["1,2,3,4,5"]


def test_ket_options_lookup_helper():
    assert ket_minggu_options("F1", GANJIL) == ["1", "3"]
    assert ket_minggu_options("F1", GENAP) == ["2", "4"]
    assert ket_minggu_options("F4", BOTH) == ["1,2,3,4"]
    assert ket_minggu_options("F1", BOTH) == []   # invalid pair -> no options


# F1 — the user picks one of exactly two weeks.
@pytest.mark.parametrize("minggu,raw,expected", [
    (GANJIL, "1", "1"),
    (GANJIL, "3", "3"),
    (GENAP, "2", "2"),
    (GENAP, "4", "4"),
])
def test_f1_callcycle_valid(minggu, raw, expected):
    got, err = normalize_callcycle(raw, "F1", minggu)
    assert err is None and got == expected


# Spec §7 — these must be impossible/rejected.
@pytest.mark.parametrize("minggu,raw", [
    (GANJIL, "2"), (GANJIL, "4"),
    (GENAP, "1"), (GENAP, "3"),
    (GANJIL, "1,3"), (GENAP, "2,4"),
    (GANJIL, "1,2,3,4"), (GANJIL, "5"), (GENAP, "5"),
])
def test_f1_callcycle_invalid(minggu, raw):
    got, err = normalize_callcycle(raw, "F1", minggu)
    assert got is None and err is not None


# F2 / F4 / F4+ — exactly one automatic value each.
@pytest.mark.parametrize("frekuensi,minggu,expected", [
    ("F2", GANJIL, "1,3"),
    ("F2", GENAP, "2,4"),
    ("F4", BOTH, "1,2,3,4"),
    ("F4+", BOTH, "1,2,3,4,5"),
])
def test_automatic_callcycle(frekuensi, minggu, expected):
    assert auto_callcycle(frekuensi, minggu) == expected
    got, err = normalize_callcycle(expected, frekuensi, minggu)
    assert err is None and got == expected


def test_f1_has_no_automatic_value():
    # F1 is the one case the user must decide — never auto-filled.
    assert auto_callcycle("F1", GANJIL) is None
    assert auto_callcycle("F1", GENAP) is None


@pytest.mark.parametrize("frekuensi,minggu,raw", [
    ("F2", GANJIL, "2,4"), ("F2", GENAP, "1,3"),
    ("F2", GANJIL, "1"), ("F2", GANJIL, "1,2,3"),
    ("F4", BOTH, "1,3"), ("F4", BOTH, "2,4"), ("F4", BOTH, "1,2,3,4,5"),
    ("F4+", BOTH, "1,2,3,4"),
])
def test_automatic_callcycle_rejects_other_values(frekuensi, minggu, raw):
    got, err = normalize_callcycle(raw, frekuensi, minggu)
    assert got is None and err is not None


def test_callcycle_normalizes_spacing_and_order():
    got, err = normalize_callcycle("3, 1", "F2", GANJIL)
    assert err is None and got == "1,3"


@pytest.mark.parametrize("raw,minggu,expected", [
    (3.0, GANJIL, "3"),
    (1.0, GANJIL, "1"),
    (2.0, GENAP, "2"),
    (4.0, GENAP, "4"),
    (3, GANJIL, "3"),
])
def test_callcycle_accepts_excel_numeric_cells(raw, minggu, expected):
    # Regression: a single-value F1 Ket. Minggu is stored by Excel as a
    # NUMBER, so pandas hands it back as the float 3.0. A naive str() gives
    # "3.0" and every valid F1 row would be wrongly rejected on import.
    got, err = normalize_callcycle(raw, "F1", minggu)
    assert err is None and got == expected


def test_callcycle_numeric_still_enforces_parity():
    # The numeric coercion must not weaken the Ganjil/Genap rule.
    got, err = normalize_callcycle(2.0, "F1", GANJIL)
    assert got is None and err is not None


def test_callcycle_rejects_unknown_frekuensi_or_minggu():
    assert normalize_callcycle("1", "F9", GANJIL)[0] is None
    assert normalize_callcycle("1", "F1", "Minggu Keempat")[0] is None


# ─── Reverse derivation (display / partial-workbook back-fill) ────────────

@pytest.mark.parametrize("callcycle,expected", [
    ("1", GANJIL), ("3", GANJIL),
    ("2", GENAP), ("4", GENAP),
    ("1,3", GANJIL), ("2,4", GENAP),
    ("1,2,3,4", BOTH),
    ("1,2,3,4,5", BOTH),   # week 5 must NOT flip this to "Ganjil"
    (None, None), ("", None),
])
def test_derive_minggu_from_callcycle(callcycle, expected):
    assert derive_minggu_from_callcycle(callcycle) == expected


# ─── Legacy migration ──────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("Minggu Ganjil", "1,3"),
    ("Minggu Genap", "2,4"),
    ("Minggu Ganjil + Genap", "1,2,3,4"),
    ("minggu ganjil", "1,3"),
    ("1,3", None),
    ("garbage", None),
])
def test_migrate_legacy_minggu(raw, expected):
    assert migrate_legacy_minggu(raw) == expected


def test_legacy_map_is_not_valid_for_f1():
    # Spec §29: a legacy "Minggu Ganjil" cannot become an F1 callcycle,
    # because F1 needs ONE specific week which the phrase never recorded.
    legacy = migrate_legacy_minggu("Minggu Ganjil")
    got, err = normalize_callcycle(legacy, "F1", GANJIL)
    assert got is None and err is not None


# ─── Integration: end-to-end schedule expansion ───────────────────────────

def test_expand_f1_single_visit():
    hari, _ = normalize_hari("SENIN", frekuensi="F1")
    cc, _ = normalize_callcycle("3", "F1", GANJIL)
    assert expand_hari_callcycle(hari, cc) == [("SENIN", "3")]


def test_expand_f2_two_days_two_weeks():
    hari, _ = normalize_hari("SENIN/SELASA", frekuensi="F2")
    cc = auto_callcycle("F2", GANJIL)
    assert expand_hari_callcycle(hari, cc) == [
        ("SENIN", "1"), ("SENIN", "3"),
        ("SELASA", "1"), ("SELASA", "3"),
    ]


def test_expand_f4_every_week_no_week5():
    hari, _ = normalize_hari("SENIN/SELASA", frekuensi="F4")
    cc = auto_callcycle("F4", BOTH)
    pairs = expand_hari_callcycle(hari, cc)
    assert len(pairs) == 8
    assert all(w != "5" for _, w in pairs)


def test_expand_f4plus_includes_week5():
    hari, _ = normalize_hari("SENIN/SELASA", frekuensi="F4+")
    cc = auto_callcycle("F4+", BOTH)
    pairs = expand_hari_callcycle(hari, cc)
    assert len(pairs) == 10
    assert ("SENIN", "5") in pairs and ("SELASA", "5") in pairs


# ─── Outdated-template detection (column layout changed) ──────────────────
# The template's columns were reordered (Frekuensi K->I, Hari I->J) and the
# old single "Minggu Ganjil/..." column became Minggu (K) + Ket. Minggu (L).
# Import matches on HEADER TEXT, never on position, so an old workbook's
# fields are still read correctly — but it cannot supply Column K. These
# guard the legacy-value recognition that drives the single clear
# "download the new template" message instead of many confusing per-row
# errors. See validate_pjp_df() in salesman_pjp.py.

@pytest.mark.parametrize("legacy_value", [
    "Minggu Ganjil", "Minggu Genap", "Minggu Ganjil + Genap",
])
def test_legacy_ket_minggu_values_are_recognisable(legacy_value):
    assert migrate_legacy_minggu(legacy_value) is not None


@pytest.mark.parametrize("current_value", ["1", "3", "1,3", "2,4", "1,2,3,4", "1,2,3,4,5"])
def test_current_ket_minggu_values_are_not_mistaken_for_legacy(current_value):
    # A current-format workbook must never trip the outdated-template check.
    assert migrate_legacy_minggu(current_value) is None


# ─── F2 single-day support ─────────────────────────────────────────────────
# F2 means two visit OCCURRENCES per month, which the week pattern (1,3 or
# 2,4) already provides. So a single day is a complete F2 assignment:
# SENIN + Minggu Ganjil = Senin in week 1 and Senin in week 3. Two days
# remain valid and behave exactly as before.

@pytest.mark.parametrize("day", ["SENIN", "SELASA", "RABU", "KAMIS", "JUMAT", "SABTU"])
def test_f2_accepts_any_single_day(day):
    got, err = normalize_hari(day, frekuensi="F2")
    assert err is None and got == day


@pytest.mark.parametrize("day", ["SENIN", "SELASA", "JUMAT"])
def test_f2_single_day_appears_in_dropdown(day):
    # Must be selectable in Column J, not merely accepted on import.
    assert day in HARI_COMBOS_BY_FREKUENSI["F2"]


@pytest.mark.parametrize("minggu,expected_cc", [(GANJIL, "1,3"), (GENAP, "2,4")])
def test_f2_single_day_keeps_normal_callcycle(minggu, expected_cc):
    # The callcycle rule is untouched by the day-count change.
    assert auto_callcycle("F2", minggu) == expected_cc
    got, err = normalize_callcycle(expected_cc, "F2", minggu)
    assert err is None and got == expected_cc


def test_f2_single_day_expands_to_exactly_two_visits():
    hari, _ = normalize_hari("SENIN", frekuensi="F2")
    cc = auto_callcycle("F2", GANJIL)
    pairs = expand_hari_callcycle(hari, cc)
    assert pairs == [("SENIN", "1"), ("SENIN", "3")]
    assert len(pairs) == 2  # matches "F2 = 2x per month"


def test_f2_two_day_behaviour_unchanged():
    # Regression guard: the pre-existing two-day form still expands the
    # same way it always did (2 days x 2 weeks = 4 rows).
    hari, _ = normalize_hari("SENIN/SELASA", frekuensi="F2")
    cc = auto_callcycle("F2", GANJIL)
    assert expand_hari_callcycle(hari, cc) == [
        ("SENIN", "1"), ("SENIN", "3"), ("SELASA", "1"), ("SELASA", "3"),
    ]


def test_f2_still_rejects_three_or_more_days():
    assert normalize_hari("SENIN/SELASA/RABU", frekuensi="F2")[0] is None


@pytest.mark.parametrize("frekuensi,raw,ok", [
    ("F1", "SENIN", True), ("F1", "SENIN/SELASA", False),   # F1 untouched
    ("F4", "SENIN", True), ("F4", "SENIN/SELASA/RABU/KAMIS/JUMAT", False),
    ("F4+", "SENIN/SELASA/RABU/KAMIS/JUMAT", True),
])
def test_other_frequencies_unaffected_by_f2_change(frekuensi, raw, ok):
    got, _ = normalize_hari(raw, frekuensi=frekuensi)
    assert (got is not None) is ok

# ─── DB minggu week-pattern mapping ────────────────────────────────────────
# Column K "Minggu Ganjil" must become DB minggu "Ganjil", never NULL and
# never a hard-coded default.

@pytest.mark.parametrize("raw,expected", [
    ("Minggu Ganjil", "Ganjil"),
    ("Minggu Genap", "Genap"),
    ("Minggu Ganjil + Genap", "Ganjil + Genap"),
    ("Ganjil", "Ganjil"),
    ("Genap", "Genap"),
    ("Ganjil + Genap", "Ganjil + Genap"),
    ("ganjil", "Ganjil"),
    ("  Minggu   Genap ", "Genap"),
    ("Minggu Ganjil+Genap", "Ganjil + Genap"),
    ("", None),
    (None, None),
    ("Minggu Ketiga", None),
])
def test_to_minggu_pattern(raw, expected):
    assert to_minggu_pattern(raw) == expected


def test_to_minggu_pattern_never_hardcodes_a_default():
    # Unrecognised / blank must stay empty — not "Ganjil".
    assert to_minggu_pattern("") is None
    assert to_minggu_pattern("Setiap Minggu") is None


# ─── F2 specific week-occurrence combinations ──────────────────────────────
# The corrected business rule: F2 = TWO occurrences of the week within the
# month, chosen freely from weeks 1-4. All six pairs are legal, not just the
# parity-matched 1,3 and 2,4. Column K narrows the set by the pair's parity;
# it does not restrict which pairs exist.

F2_ALL_PAIRS = ["1,2", "1,3", "1,4", "2,3", "2,4", "3,4"]


def test_f2_offers_every_two_week_combination():
    # Derived from the rule (C(4,2)), never hand-listed in the module.
    assert week_combos_for_frekuensi("F2") == F2_ALL_PAIRS


def test_f2_combinations_are_derived_not_hardcoded():
    # Every pair reachable through some Column K label, none missing, none
    # duplicated — i.e. the parity grouping partitions the full rule set.
    reachable = [
        cc
        for m in MINGGU_OPTIONS_BY_FREKUENSI["F2"]
        for cc in KET_OPTIONS_BY_FREKUENSI_MINGGU[("F2", m)]
    ]
    assert sorted(reachable) == F2_ALL_PAIRS
    assert len(reachable) == len(set(reachable))


@pytest.mark.parametrize("weeks,expected_label", [
    ("1,3", GANJIL),   # both odd
    ("2,4", GENAP),    # both even
    ("1,2", BOTH),     # mixed
    ("1,4", BOTH),
    ("2,3", BOTH),
    ("3,4", BOTH),
])
def test_f2_pair_parity_label(weeks, expected_label):
    assert parity_label(weeks) == expected_label
    assert weeks in KET_OPTIONS_BY_FREKUENSI_MINGGU[("F2", expected_label)]


@pytest.mark.parametrize("weeks", F2_ALL_PAIRS)
def test_f2_every_pair_validates_and_is_preserved(weeks):
    """The selected combination survives validation UNCHANGED — the core
    requirement. It is never rewritten to 1,3/2,4 and never blanked."""
    minggu = parity_label(weeks)
    got, err = normalize_callcycle(weeks, "F2", minggu)
    assert err is None
    assert got == weeks


@pytest.mark.parametrize("weeks", F2_ALL_PAIRS)
def test_f2_every_pair_drives_generation(weeks):
    """Spec §8: the chosen weeks must reach the generated schedule, not
    merely be displayed. One day x two weeks = exactly two visits, on the
    weeks the user picked."""
    hari, err = normalize_hari("SENIN", frekuensi="F2")
    assert err is None
    minggu = parity_label(weeks)
    cc, err = normalize_callcycle(weeks, "F2", minggu)
    assert err is None
    pairs = expand_hari_callcycle(hari, cc)
    assert pairs == [("SENIN", w) for w in weeks.split(",")]
    assert len(pairs) == 2


# Spec §9 Tests A-D, end to end at the values named in the request.
@pytest.mark.parametrize("day,weeks,expected", [
    ("Senin", "1,2", [("SENIN", "1"), ("SENIN", "2")]),   # Test A
    ("Senin", "1,3", [("SENIN", "1"), ("SENIN", "3")]),   # Test B
    ("Senin", "2,4", [("SENIN", "2"), ("SENIN", "4")]),   # Test C
    ("Rabu",  "3,4", [("RABU", "3"), ("RABU", "4")]),     # Test D
])
def test_f2_single_day_specific_weeks_end_to_end(day, weeks, expected):
    hari, h_err = normalize_hari(day, frekuensi="F2")
    assert h_err is None
    minggu, m_err = normalize_minggu(parity_label(weeks), "F2")
    assert m_err is None
    cc, c_err = normalize_callcycle(weeks, "F2", minggu)
    assert c_err is None and cc == weeks
    assert expand_hari_callcycle(hari, cc) == expected


@pytest.mark.parametrize("weeks", ["1,3", "2,4"])
def test_f2_parity_matched_pairs_stay_automatic(weeks):
    """Backward compatibility: the two pre-existing F2 configurations are
    still the single (auto-filled) option under their parity label, so the
    66k already-stored F2 rows keep validating and nothing needs a backfill."""
    minggu = parity_label(weeks)
    assert KET_OPTIONS_BY_FREKUENSI_MINGGU[("F2", minggu)] == [weeks]
    assert auto_callcycle("F2", minggu) == weeks


def test_f2_mixed_pairs_are_a_real_choice_never_defaulted():
    """Spec §11: a mixed pair cannot be guessed, so nothing is auto-filled.
    The value must come from the user, or stay absent."""
    assert auto_callcycle("F2", BOTH) is None
    assert len(KET_OPTIONS_BY_FREKUENSI_MINGGU[("F2", BOTH)]) == 4


@pytest.mark.parametrize("weeks,minggu", [
    ("1,2", GANJIL),   # mixed pair filed under a parity label
    ("3,4", GENAP),
    ("1,3", BOTH),     # purely-odd pair filed under "both"
    ("2,4", BOTH),
])
def test_f2_parity_mismatch_rejected(weeks, minggu):
    got, err = normalize_callcycle(weeks, "F2", minggu)
    assert got is None and err is not None


@pytest.mark.parametrize("weeks", ["1", "1,2,3", "1,2,3,4", "1,5", "2,6", "1,1"])
def test_f2_structurally_invalid_week_sets_rejected(weeks):
    """Wrong count, out-of-pool week (5 belongs to F4+ only), or duplicates —
    rejected under every Column K label, without hard-coding a whitelist."""
    for minggu in MINGGU_OPTIONS_BY_FREKUENSI["F2"]:
        got, err = normalize_callcycle(weeks, "F2", minggu)
        assert got is None and err is not None, (weeks, minggu)


def test_f2_wrong_week_count_says_so():
    _, err = normalize_callcycle("1,2,3", "F2", BOTH)
    assert "tepat 2 minggu" in err


def test_f2_week_order_normalized_not_lost():
    got, err = normalize_callcycle("4, 3", "F2", BOTH)
    assert err is None and got == "3,4"


def test_f2_mixed_pair_back_derives_to_its_own_label():
    """Spec §10: a workbook carrying only Ket. Minggu must re-derive Column K
    correctly, or the row fails validation and `minggu` lands NULL."""
    for weeks in F2_ALL_PAIRS:
        assert derive_minggu_from_callcycle(weeks) == parity_label(weeks)


def test_f2_mixed_pair_reaches_db_minggu_pattern():
    assert to_minggu_pattern(derive_minggu_from_callcycle("1,2")) == "Ganjil + Genap"


# ─── Non-F2 frequencies are untouched by the F2 rule change ───────────────

def test_f1_unchanged_by_f2_rule_change():
    assert week_combos_for_frekuensi("F1") == ["1", "2", "3", "4"]
    assert MINGGU_OPTIONS_BY_FREKUENSI["F1"] == [GANJIL, GENAP]
    assert KET_OPTIONS_BY_FREKUENSI_MINGGU[("F1", GANJIL)] == ["1", "3"]
    assert KET_OPTIONS_BY_FREKUENSI_MINGGU[("F1", GENAP)] == ["2", "4"]
    # F1 still cannot be "Ganjil + Genap": one week has one parity.
    assert ("F1", BOTH) not in KET_OPTIONS_BY_FREKUENSI_MINGGU


@pytest.mark.parametrize("freq,expected", [
    ("F4", "1,2,3,4"),
    ("F4+", "1,2,3,4,5"),
])
def test_f4_family_unchanged_by_f2_rule_change(freq, expected):
    assert week_combos_for_frekuensi(freq) == [expected]
    assert MINGGU_OPTIONS_BY_FREKUENSI[freq] == [BOTH]
    assert auto_callcycle(freq, BOTH) == expected
    got, err = normalize_callcycle(expected, freq, BOTH)
    assert err is None and got == expected


def test_week_pool_excludes_week_five():
    """Week 5 is F4+'s calendar overflow, not a selectable occurrence — so
    it can never appear in an F1 or F2 choice."""
    assert WEEK_POOL == ["1", "2", "3", "4"]
    assert all("5" not in c for c in week_combos_for_frekuensi("F2"))


def test_legacy_stored_phrases_still_migrate():
    assert migrate_legacy_minggu("Minggu Ganjil") == "1,3"
    assert migrate_legacy_minggu("Minggu Genap") == "2,4"
    assert migrate_legacy_minggu("Minggu Ganjil + Genap") == "1,2,3,4"
