"""
Distributor-code coverage for Salesman PJP.

The dropdown and salesman/store/PJP rows are loaded from BigQuery
(gt_schema.master_distributor and related tables). The only local whitelist
is DISTRIBUTOR_PASSWORDS, which gates login. Import/export/validation treat
any distributor_code the same as long as salesman and store ownership match
the selected code.

Run: pytest tests/test_pjp_distributor_codes.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

from pjp_hari_minggu import KET_MINGGU_COL, MINGGU_COL, MINGGU_GANJIL

TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from test_pjp_import import (  # noqa: E402
    SALESMAN_DF,
    STORE_DF,
    SP,
    _bq_payload,
    _import,
    _row,
    _validate,
    _workbook,
)

NEW_DIST_CODES = ("DST358", "DST360", "DST362")
EXISTING_DIST_CODES = ("DST171", "DST356", "DST363", "DST351", "DST352")


def _fixtures(dist_code: str):
    salesman_id = f"GTI{dist_code}001"
    store_code = f"ST{dist_code[-3:]}"
    salesman_df = pd.DataFrame({
        "salesman_id": [salesman_id],
        "salesman": ["BUDI"],
        "salesman_label": [salesman_id],
        "distributor_code": [dist_code],
    })
    store_df = pd.DataFrame({
        "store_code": [store_code],
        "store_name": ["TOKO A"],
        "region": ["R"],
        "asm": ["ASM1"],
        "distributor_name": ["DIST"],
        "distributor_code": [dist_code],
        "store_label": [store_code],
    })
    return salesman_id, store_code, salesman_df, store_df


def _import_for(dist_code: str, **fields):
    salesman_id, store_code, salesman_df, store_df = _fixtures(dist_code)
    payload = {"Salesman ID": salesman_id, "Kode Toko": store_code, **fields}
    df = SP["read_template_sheet"](
        _workbook([_row(**payload)]), "PJP Template", 2, salesman_df, store_df)
    df = df[df["kode_toko"].notna() & (df["kode_toko"].astype(str).str.strip() != "")]
    return df.reset_index(drop=True), salesman_df, store_df


def _validate_for(df, dist_code, store_df, salesman_df):
    return SP["validate_pjp_df"](
        df, {dist_code: "DIST"},
        store_df=store_df, salesman_df=salesman_df,
        selected_dist_code=dist_code)


# ─── Password whitelist ────────────────────────────────────────────────────

def test_new_distributor_codes_are_in_password_whitelist():
    passwords = SP["DISTRIBUTOR_PASSWORDS"]
    for code in NEW_DIST_CODES:
        assert code in passwords, f"{code} missing from DISTRIBUTOR_PASSWORDS"
        assert isinstance(passwords[code], str) and passwords[code].strip(), (
            f"{code} has an empty password")


def test_existing_distributor_codes_are_unchanged():
    passwords = SP["DISTRIBUTOR_PASSWORDS"]
    for code in EXISTING_DIST_CODES:
        assert code in passwords, f"existing code {code} was removed"
    assert passwords["DST171"] == "5bcd0fc2"
    assert passwords["DST356"] == "1a2b3c4d"
    assert passwords["DST363"] == "2b3c4d5e"


@pytest.mark.parametrize("code", NEW_DIST_CODES)
@pytest.mark.parametrize("variant_fn", [
    lambda c: c,
    lambda c: c.lower(),
    lambda c: f"  {c}  ",
    lambda c: c[:3].lower() + c[3:],
])
def test_password_lookup_accepts_case_and_whitespace(code, variant_fn):
    expected = SP["DISTRIBUTOR_PASSWORDS"][code]
    assert SP["_get_password_for_distributor"](variant_fn(code)) == expected


def test_unknown_distributor_still_has_no_password():
    assert SP["_get_password_for_distributor"]("DST999") is None


# ─── Import / validate / export ────────────────────────────────────────────

@pytest.mark.parametrize("dist_code", NEW_DIST_CODES)
def test_import_validate_export_persists_new_distributor_code(dist_code):
    df, salesman_df, store_df = _import_for(dist_code, Frekuensi="F4", Hari="SENIN")
    errors, _ = _validate_for(df, dist_code, store_df, salesman_df)
    assert errors == [], errors

    bq = _bq_payload(df)
    assert "callcycle" in bq.columns
    assert bq["callcycle"].iloc[0] == "1,2,3,4"
    assert df["salesman_id"].iloc[0] == f"GTI{dist_code}001"


@pytest.mark.parametrize("dist_code,frekuensi,hari,extra", [
    ("DST358", "F1", "SENIN", {MINGGU_COL: MINGGU_GANJIL, KET_MINGGU_COL: "3"}),
    ("DST360", "F2", "SENIN/SELASA", {MINGGU_COL: MINGGU_GANJIL}),
    ("DST362", "F4", "SENIN", {}),
])
def test_f_rules_accept_new_distributor_codes(dist_code, frekuensi, hari, extra):
    df, salesman_df, store_df = _import_for(
        dist_code, Frekuensi=frekuensi, Hari=hari, **extra)
    errors, _ = _validate_for(df, dist_code, store_df, salesman_df)
    assert errors == [], errors


@pytest.mark.parametrize("dist_code", NEW_DIST_CODES)
def test_filter_rejects_salesman_from_another_distributor(dist_code):
    df, salesman_df, store_df = _import_for(dist_code, Frekuensi="F4", Hari="SENIN")
    salesman_df = salesman_df.copy()
    salesman_df.loc[0, "distributor_code"] = "DST171"
    errors, _ = _validate_for(df, dist_code, store_df, salesman_df)
    assert errors, "cross-distributor salesman must be rejected"
    assert any("bukan milik distributor yang dipilih" in e for e in errors)


@pytest.mark.parametrize("dist_code", NEW_DIST_CODES)
def test_search_filter_keeps_matching_distributor_rows(dist_code):
    df, salesman_df, store_df = _import_for(dist_code, Frekuensi="F4", Hari="SENIN")
    combined = pd.concat([salesman_df, SALESMAN_DF], ignore_index=True)
    matched = combined[
        combined["distributor_code"].astype(str).str.strip().str.upper() == dist_code]
    assert list(matched["salesman_id"]) == [f"GTI{dist_code}001"]
    errors, _ = _validate_for(df, dist_code, store_df, combined)
    assert errors == [], errors


def test_existing_dst171_import_still_succeeds():
    df = _import([_row(**{
        "Salesman ID": "GTIDST171001",
        "Kode Toko": "ST00001",
        "Frekuensi": "F4",
        "Hari": "SENIN",
    })])
    errors, _ = _validate(df)
    assert errors == []
    assert STORE_DF["distributor_code"].iloc[0] == "DST171"
    assert SALESMAN_DF["distributor_code"].iloc[0] == "DST171"
    assert "DST171" in SP["DISTRIBUTOR_PASSWORDS"]
