"""
Import-path tests for the Salesman PJP G2G template:
read_template_sheet() + validate_pjp_df() driven by real in-memory
openpyxl workbooks.

These cover the code that the pure-logic suite (test_pjp_hari_minggu.py)
cannot reach — auto-fill, header aliasing, row numbering, outdated-template
detection and the BigQuery column mapping. A code review found four real
bugs in exactly this untested gap, so each is pinned by a test below.

salesman_pjp.py executes Streamlit UI code at import time and so cannot be
imported directly; the needed definitions are AST-extracted into a clean
namespace, which is the same technique the repo's verification scripts use.

Run: pytest tests/test_pjp_import.py -q
"""
from __future__ import annotations

import ast
import sys
from io import BytesIO
from pathlib import Path

import pandas as pd
import pytest
from openpyxl import Workbook

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import pjp_hari_minggu as M  # noqa: E402

SRC = REPO / "salesman_pjp.py"

_WANTED = {
    "PJP_COLS", "PJP_REQUIRED", "_COMBO_SEP", "_EXCEL_ROW_COL", "_excel_row_of",
    "_show", "_safe_name", "_indirect_clean", "_thin_border", "_fill", "_cf_fill",
    "_header_font", "_note_font", "_req_font", "_center", "_vcenter",
    "build_salesman_lookup", "build_store_lookup", "_is_empty",
    "_get_unique_distributors", "validate_row_completeness", "validate_pjp_df",
    "_extract_combo_key", "read_template_sheet", "normalize_phone_id",
    "_PJP_COL_MAP", "DISTRIBUTOR_PASSWORDS", "_get_password_for_distributor",
}


def _load_module_slice():
    tree = ast.parse(SRC.read_text(encoding="utf-8"), filename=str(SRC))
    picked = []
    for node in tree.body:
        name = None
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            name = node.name
        elif isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
        elif isinstance(node, ast.ImportFrom):
            picked.append(node)
            continue
        if name in _WANTED:
            picked.append(node)
    mod = ast.Module(body=picked, type_ignores=[])
    ast.fix_missing_locations(mod)

    ns = {"pd": pd, "re": __import__("re"), "unicodedata": __import__("unicodedata")}
    for key in dir(M):
        if not key.startswith("__"):
            ns[key] = getattr(M, key)
    ns["DAY_OPTIONS"] = M.HARI_CANONICAL_ORDER
    ns["FREQUENCY_OPTIONS"] = M.FREKUENSI_OPTIONS
    exec(compile(mod, str(SRC), "exec"), ns)
    return ns


SP = _load_module_slice()
COLS = [c for c, _, _ in SP["PJP_COLS"]]

SALESMAN_DF = pd.DataFrame({
    "salesman_id": ["GTIDST171001"], "salesman": ["BUDI"],
    "salesman_label": ["GTIDST171001"], "distributor_code": ["DST171"],
})
STORE_DF = pd.DataFrame({
    "store_code": ["ST00001", "ST00002"], "store_name": ["TOKO A", "TOKO B"],
    "region": ["R", "R"], "asm": ["ASM1", "ASM1"],
    "distributor_name": ["DIST", "DIST"], "distributor_code": ["DST171", "DST171"],
    "store_label": ["ST00001", "ST00002"],
})


def _row(**kw):
    idx = {c: i for i, c in enumerate(COLS)}
    cells = [""] * len(COLS)
    for k, v in kw.items():
        cells[idx[k]] = v
    return cells


def _workbook(data_rows, headers=None):
    headers = headers or COLS
    wb = Workbook()
    ws = wb.active
    ws.title = "PJP Template"
    ws.append([""] * len(headers))
    ws.append([""] * len(headers))
    ws.append(headers)
    for r in data_rows:
        ws.append(r)
    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


def _import(data_rows, headers=None):
    df = SP["read_template_sheet"](
        _workbook(data_rows, headers), "PJP Template", 2, SALESMAN_DF, STORE_DF)
    df = df[df["kode_toko"].notna() & (df["kode_toko"].astype(str).str.strip() != "")]
    return df.reset_index(drop=True)


def _validate(df):
    return SP["validate_pjp_df"](
        df, {"DST171": "DIST"}, store_df=STORE_DF,
        salesman_df=SALESMAN_DF, selected_dist_code="DST171")


BASE = {"Salesman ID": "GTIDST171001", "Kode Toko": "ST00001"}


# ─── Ket. Minggu / Minggu auto-fill ────────────────────────────────────────

@pytest.mark.parametrize("frekuensi,expected_ket", [
    ("F4", "1,2,3,4"),
    ("F4+", "1,2,3,4,5"),
])
def test_f4_family_autofills_both_minggu_and_ket(frekuensi, expected_ket):
    # The template's prompts call K and L automatic for F4/F4+, so leaving
    # both blank must succeed rather than fail as "kolom wajib belum terisi".
    df = _import([_row(**BASE, Frekuensi=frekuensi, Hari="SENIN")])
    assert df[M.MINGGU_COL][0] == M.MINGGU_GANJIL_GENAP
    assert df[M.KET_MINGGU_COL][0] == expected_ket
    errors, _ = _validate(df)
    assert errors == []


@pytest.mark.parametrize("minggu,expected", [
    (M.MINGGU_GANJIL, "1,3"),
    (M.MINGGU_GENAP, "2,4"),
])
def test_f2_autofills_ket_from_minggu(minggu, expected):
    df = _import([_row(**BASE, Frekuensi="F2", Hari="SENIN/SELASA", **{M.MINGGU_COL: minggu})])
    assert df[M.KET_MINGGU_COL][0] == expected
    assert _validate(df)[0] == []


def test_f1_is_never_autofilled():
    # F1 has two candidate weeks; picking one is the user's decision.
    df = _import([_row(**BASE, Frekuensi="F1", Hari="SENIN", **{M.MINGGU_COL: M.MINGGU_GANJIL})])
    assert not str(df[M.KET_MINGGU_COL][0]).strip() or pd.isna(df[M.KET_MINGGU_COL][0])
    assert _validate(df)[0], "blank F1 Ket. Minggu must be reported"


# ─── Row numbering ─────────────────────────────────────────────────────────

def test_error_row_number_survives_blank_rows_and_reset_index():
    # Three blank rows, then a bad F1 row -> Excel row 7, not 4.
    rows = [[""] * len(COLS)] * 3 + [
        _row(**BASE, Frekuensi="F1", Hari="SENIN",
             **{M.MINGGU_COL: M.MINGGU_GANJIL, M.KET_MINGGU_COL: "2"})]
    errors, _ = _validate(_import(rows))
    assert errors, "parity mismatch must be reported"
    assert any("Baris 7" in e for e in errors), errors


# ─── Message hygiene ───────────────────────────────────────────────────────

def test_blank_frekuensi_does_not_leak_nan_into_messages():
    df = _import([_row(**BASE, Hari="SENIN",
                       **{M.MINGGU_COL: M.MINGGU_GANJIL, M.KET_MINGGU_COL: "1"})])
    errors, _ = _validate(df)
    assert errors
    assert not any("'nan'" in e.lower() for e in errors), errors


def test_numeric_ket_minggu_is_shown_as_typed():
    # Excel stores a single digit as a number; the message must say 2, not 2.0.
    df = _import([_row(**BASE, Frekuensi="F1", Hari="SENIN",
                       **{M.MINGGU_COL: M.MINGGU_GANJIL, M.KET_MINGGU_COL: 2})])
    errors, _ = _validate(df)
    assert errors and not any("2.0" in e for e in errors), errors


def test_valid_numeric_ket_minggu_is_accepted():
    df = _import([_row(**BASE, Frekuensi="F1", Hari="SENIN",
                       **{M.MINGGU_COL: M.MINGGU_GANJIL, M.KET_MINGGU_COL: 3})])
    assert df[M.KET_MINGGU_COL][0] == "3"
    assert _validate(df)[0] == []


# ─── Outdated-template detection ───────────────────────────────────────────

_OLD_HEADERS = [
    "ASM", "Region", "Nama Distributor", "Kode Distributor", "Salesman ID",
    "Nama Salesman", "Kode Toko", "Nama Toko", "Hari",
    "Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap", "Frekuensi",
]


def _old_row(hari, week, freq):
    return ["ASM1", "R", "DIST", "DST171", "GTIDST171001", "BUDI",
            "ST00001", "TOKO A", hari, week, freq]


@pytest.mark.parametrize("rows", [
    [_old_row("SENIN", "Minggu Ganjil", "F2")],                       # fully legacy
    [_old_row("SENIN", "Minggu Ganjil", "F2"),
     _old_row("SELASA", "1,3", "F2")],                                 # part-corrected
])
def test_outdated_template_gives_one_clear_message(rows):
    errors, _ = _validate(_import(rows, headers=_OLD_HEADERS))
    assert any("Template lama terdeteksi" in e for e in errors), errors


def test_current_template_never_flagged_as_outdated():
    df = _import([_row(**BASE, Frekuensi="F2", Hari="SENIN/SELASA",
                       **{M.MINGGU_COL: M.MINGGU_GANJIL})])
    errors, _ = _validate(df)
    assert not any("Template lama" in e for e in errors), errors


def test_old_headers_are_read_by_name_not_position():
    # Frekuensi/Hari swapped columns between versions; values must not swap.
    df = _import([_old_row("SENIN", "Minggu Ganjil", "F2")], headers=_OLD_HEADERS)
    assert df["Frekuensi"][0] == "F2"
    assert str(df["Hari"][0]).upper() == "SENIN"


# ─── BigQuery mapping ──────────────────────────────────────────────────────

def _bq_payload(df):
    df = df.copy()
    df["snapshot_month"] = "2026-09"
    col_map = SP["_PJP_COL_MAP"]
    present = {c: col_map[c] for c in col_map if c in df.columns}
    return pd.DataFrame(df[list(present.keys())]).rename(columns=present)


def test_callcycle_and_minggu_are_persisted():
    df = _import([_row(**BASE, Frekuensi="F4", Hari="SENIN")])
    bq = _bq_payload(df)
    bq_cols = set(bq.columns)
    assert "callcycle" in bq_cols
    assert "minggu" in bq_cols
    assert bq["minggu"].iloc[0] == "Ganjil + Genap"
    assert bq["callcycle"].iloc[0] == "1,2,3,4"
    for banned in ("hari_minggu", "nomor_minggu", "ket_minggu", "week_number"):
        assert banned not in bq_cols, banned
    assert SP["_EXCEL_ROW_COL"] not in bq_cols, "internal helper column must not be written"


@pytest.mark.parametrize("hari,minggu,expected_pattern,expected_cc", [
    ("SENIN", M.MINGGU_GANJIL, "Ganjil", "1,3"),
    ("SENIN", M.MINGGU_GENAP, "Genap", "2,4"),
    ("SENIN/SELASA", M.MINGGU_GANJIL, "Ganjil", "1,3"),
])
def test_f2_persists_user_minggu_pattern(hari, minggu, expected_pattern, expected_cc):
    df = _import([_row(**BASE, Frekuensi="F2", Hari=hari, **{M.MINGGU_COL: minggu})])
    errors, _ = _validate(df)
    assert errors == []
    assert df["minggu"][0] == expected_pattern
    bq = _bq_payload(df)
    assert bq["minggu"].iloc[0] == expected_pattern
    assert bq["callcycle"].iloc[0] == expected_cc
    assert bq["hari"].iloc[0] == hari


def test_f2_ganjil_plus_genap_needs_an_explicit_week_pair():
    # Ganjil + Genap IS a valid F2 Column K choice now (it is how the mixed
    # pairs are reached), but unlike Ganjil/Genap it does not determine the
    # weeks — four pairs share that label. So it must NOT be auto-filled,
    # and a blank Ket. Minggu must be reported rather than defaulted.
    df = _import([_row(**BASE, Frekuensi="F2", Hari="SENIN",
                       **{M.MINGGU_COL: M.MINGGU_GANJIL_GENAP})])
    assert df[M.MINGGU_COL][0] == M.MINGGU_GANJIL_GENAP
    ket = df[M.KET_MINGGU_COL][0]
    assert pd.isna(ket) or not str(ket).strip(), "must not invent a week pair"
    errors, _ = _validate(df)
    assert errors, "blank F2 Ket. Minggu must be reported, not defaulted"


# ─── F2 specific week combinations, end to end ────────────────────────────
# Streamlit input -> read_template_sheet -> validate -> DataFrame ->
# BigQuery payload. Spec Tests A-E plus the NULL guard (§10).

F2_PAIRS = ["1,2", "1,3", "1,4", "2,3", "2,4", "3,4"]


@pytest.mark.parametrize("weeks", F2_PAIRS)
def test_f2_every_pair_survives_to_the_bq_payload(weeks):
    minggu = M.parity_label(weeks)
    df = _import([_row(**BASE, Frekuensi="F2", Hari="SENIN",
                       **{M.MINGGU_COL: minggu, M.KET_MINGGU_COL: weeks})])
    errors, _ = _validate(df)
    assert errors == []
    bq = _bq_payload(df)
    # The user's selection, unchanged and non-NULL, at the DB boundary.
    assert bq["callcycle"].iloc[0] == weeks
    assert pd.notna(bq["minggu"].iloc[0]) and bq["minggu"].iloc[0]
    assert bq["minggu"].iloc[0] == M.to_minggu_pattern(minggu)
    assert bq["hari"].iloc[0] == "SENIN"


@pytest.mark.parametrize("day,weeks,expected_days,expected_weeks", [
    ("SENIN", "1,2", ["SENIN"], ["1", "2"]),   # Test A
    ("SENIN", "1,3", ["SENIN"], ["1", "3"]),   # Test B
    ("SENIN", "2,4", ["SENIN"], ["2", "4"]),   # Test C
    ("RABU",  "3,4", ["RABU"],  ["3", "4"]),   # Test D
    ("SELASA", "1,4", ["SELASA"], ["1", "4"]),  # Test E
    ("KAMIS",  "2,3", ["KAMIS"],  ["2", "3"]),  # Test E
])
def test_f2_generation_matches_selected_weeks(day, weeks, expected_days, expected_weeks):
    """Spec §8: the stored value must actually drive the generated schedule,
    not just appear in the table. expand_hari_callcycle() is the reference
    implementation of the hari x callcycle fan-out that sync_sfa_web.py
    performs in SQL (SPLIT + LEFT JOIN UNNEST)."""
    minggu = M.parity_label(weeks)
    df = _import([_row(**BASE, Frekuensi="F2", Hari=day,
                       **{M.MINGGU_COL: minggu, M.KET_MINGGU_COL: weeks})])
    assert _validate(df)[0] == []
    bq = _bq_payload(df)
    pairs = M.expand_hari_callcycle(bq["hari"].iloc[0], bq["callcycle"].iloc[0])
    assert pairs == [(d, w) for d in expected_days for w in expected_weeks]


@pytest.mark.parametrize("weeks", F2_PAIRS)
def test_f2_minggu_back_derived_when_column_k_left_blank(weeks):
    """A workbook that fills only Ket. Minggu (or an older one with no
    Minggu column) must still resolve Column K — otherwise `minggu` reaches
    BigQuery as NULL, which is the exact regression this guards."""
    df = _import([_row(**BASE, Frekuensi="F2", Hari="SENIN",
                       **{M.KET_MINGGU_COL: weeks})])
    assert df[M.MINGGU_COL][0] == M.parity_label(weeks)
    assert _validate(df)[0] == []
    bq = _bq_payload(df)
    assert pd.notna(bq["minggu"].iloc[0]) and bq["minggu"].iloc[0]
    assert bq["callcycle"].iloc[0] == weeks


@pytest.mark.parametrize("weeks,minggu", [
    ("1,2", M.MINGGU_GANJIL),          # mixed pair under a parity label
    ("1,3", M.MINGGU_GANJIL_GENAP),    # purely-odd pair under "both"
])
def test_f2_parity_mismatch_rejected_on_import(weeks, minggu):
    df = _import([_row(**BASE, Frekuensi="F2", Hari="SENIN",
                       **{M.MINGGU_COL: minggu, M.KET_MINGGU_COL: weeks})])
    assert _validate(df)[0], "Ket. Minggu must match its Minggu label"


@pytest.mark.parametrize("weeks", ["1", "1,2,3", "1,5"])
def test_f2_structurally_invalid_weeks_rejected_on_import(weeks):
    df = _import([_row(**BASE, Frekuensi="F2", Hari="SENIN",
                       **{M.MINGGU_COL: M.MINGGU_GANJIL_GENAP, M.KET_MINGGU_COL: weeks})])
    assert _validate(df)[0], weeks


def test_f2_mixed_pair_is_not_rewritten_to_a_parity_pair():
    """Spec §2/§11: the selection is preserved verbatim; it is never
    collapsed to 1,3 / 2,4 and never replaced by a default."""
    df = _import([_row(**BASE, Frekuensi="F2", Hari="RABU",
                       **{M.MINGGU_COL: M.MINGGU_GANJIL_GENAP, M.KET_MINGGU_COL: "1,4"})])
    assert _validate(df)[0] == []
    assert _bq_payload(df)["callcycle"].iloc[0] == "1,4"


def test_legacy_f2_rows_still_import_unchanged():
    """Spec §12/Test H: the two pre-existing F2 configurations (66k stored
    rows) behave exactly as before — auto-filled from Column K alone."""
    for minggu, expected in [(M.MINGGU_GANJIL, "1,3"), (M.MINGGU_GENAP, "2,4")]:
        df = _import([_row(**BASE, Frekuensi="F2", Hari="SENIN/SELASA",
                           **{M.MINGGU_COL: minggu})])
        assert _validate(df)[0] == []
        bq = _bq_payload(df)
        assert bq["callcycle"].iloc[0] == expected
        assert bq["minggu"].iloc[0] == M.to_minggu_pattern(minggu)


def test_f1_persists_user_minggu_pattern():
    df = _import([_row(**BASE, Frekuensi="F1", Hari="SENIN",
                       **{M.MINGGU_COL: M.MINGGU_GANJIL, M.KET_MINGGU_COL: "3"})])
    errors, _ = _validate(df)
    assert errors == []
    bq = _bq_payload(df)
    assert bq["minggu"].iloc[0] == "Ganjil"
    assert bq["callcycle"].iloc[0] == "3"


def test_f4_persists_ganjil_plus_genap():
    df = _import([_row(**BASE, Frekuensi="F4", Hari="SENIN/SELASA")])
    errors, _ = _validate(df)
    assert errors == []
    bq = _bq_payload(df)
    assert bq["minggu"].iloc[0] == "Ganjil + Genap"
