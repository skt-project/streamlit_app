"""
Distributor-code coverage for Salesman PJP.

The dropdown and salesman/store/PJP rows are loaded from BigQuery
(gt_schema.master_distributor and related tables). There is no longer any local
whitelist: login is gated by gt_schema.sfa_pjp_distributor_accounts, and the
rules that read it are covered in test_pjp_auth.py. Import/export/validation
treat any distributor_code the same as long as salesman and store ownership
match the selected code.

Run: pytest tests/test_pjp_distributor_codes.py -q
"""
from __future__ import annotations

import sys
from datetime import date
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


# ─── Account gate (replaces the old password whitelist) ────────────────────
# DISTRIBUTOR_PASSWORDS is gone. These tests pin that it stayed gone, and that
# the codes it used to cover still authenticate through the account table's
# rules instead. The rules themselves live in test_pjp_auth.py.

import pjp_auth as A  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
ALL_DIST_CODES = NEW_DIST_CODES + EXISTING_DIST_CODES


def test_hardcoded_credentials_are_gone_from_the_source():
    """The migrated dict must not linger as a second way in."""
    source = (REPO_ROOT / "salesman_pjp.py").read_text(encoding="utf-8")
    for banned in ("DISTRIBUTOR_PASSWORDS = {",
                   "INPUT_DEADLINE = datetime(",
                   "def _get_password_for_distributor",
                   "def _render_password_gate"):
        assert banned not in source, f"{banned!r} is still an auth path"


def test_source_has_no_plaintext_password_comparison():
    source = (REPO_ROOT / "salesman_pjp.py").read_text(encoding="utf-8")
    assert "entered == expected" not in source


def _account(code, password="rahasia123", deadline=date(2026, 12, 31),
             is_active=True):
    return {"distributor_code": code, "distributor_name": f"DIST {code}",
            "username": code, "password_hash": A.hash_password(password),
            "is_active": is_active, "input_deadline": deadline}


@pytest.mark.parametrize("code", ALL_DIST_CODES)
def test_every_distributor_code_authenticates_through_the_account_table(code):
    account = _account(code)
    today = date(2026, 9, 16)
    assert A.evaluate_distributor_login(account, "rahasia123",
                                        username=code, today=today).ok is True
    assert A.evaluate_distributor_login(account, "salah",
                                        username=code, today=today).ok is False


@pytest.mark.parametrize("code", ALL_DIST_CODES)
@pytest.mark.parametrize("variant_fn", [
    lambda c: c,
    lambda c: c.lower(),
    lambda c: f"  {c}  ",
    lambda c: c[:3].lower() + c[3:],
])
def test_account_lookup_accepts_case_and_whitespace(code, variant_fn):
    """Scope checks must survive the same code spellings the old lookup did."""
    account = _account(code)
    assert A.validate_distributor_access(account, code, variant_fn(code),
                                         date(2026, 9, 16)).ok is True


def test_unknown_distributor_has_no_account_and_cannot_log_in():
    assert A.evaluate_distributor_login(None, "apa pun",
                                        today=date(2026, 9, 16)).ok is False


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
