"""
Temporary DST366 override for the NOO submission app (noo_submission/distributor_overrides.py).

The live sheet row for DST366 has an empty Company Code and a NOO coverage ("Jakarta Timur") that is not in
Prefix id.xlsx, so Generate ID fails with "Prefix not found". The override fills ONLY blanks - the sheet
always wins. These tests pin that contract, and run the real noo_submission/shared.py loaders against a
fixture that mimics the gviz CSV (no network, no real Streamlit, no credentials).

Run: pytest tests/test_noo_distributor_overrides.py -q
"""
from __future__ import annotations

import importlib.util
import io
import re
import sys
import types
from pathlib import Path

import openpyxl
import pandas as pd
import pytest

NOO_DIR = Path(__file__).resolve().parents[1] / "noo_submission"

BOGOR = "PT SURYA DONASIN - BOGOR"
JAKARTA = "PT MENSA BINASUKSES - JAKARTA"

# gviz CSV shape of the real tab: row 1 = labels (blank for G/H), row 2 = repeated header (note the trailing
# space in "Distributor Name "), then data. Columns G/H = Company Code / Distributor Code Fix.
_HEADER = ('"distributor_company_name","Distributor Name","Status","Area / Coverage","Area Coverage for NOO",'
           '"Region","","","","","CUST","NAMAC"\n'
           '"distributor_company_name","Distributor Name ","Status","Area / Coverage","Area Coverage for NOO",'
           '"Region","Company Code","Distributor Code Fix","","","TKAZ","TK. AZ"\n')
_BOGOR_ROW = '"PT SURYA DONASIN","PT SURYA DONASIN - BOGOR","Active","Bogor","Jabodetabek","West Java (Sd)","SDS","DST133","Jabodetabek","","",""\n'
_OLD_MENSA = '"PT MENSA BINASUKSES","PT MENSA BINASUKSES - BANDUNG","Inactive","Bandung","Bandung","West Java (Sd)","MBS","DST320","","","",""\n'
# the row as it is live today: blank Company Code, coverage not in Coverage Mapping
SHEET_AS_LIVE = _HEADER + _BOGOR_ROW + _OLD_MENSA + \
    '"PT MENSA BINASUKSES","PT MENSA BINASUKSES - JAKARTA","Active","Jakarta","Jakarta Timur","West Java (Sd)","","DST366","","","",""\n'
# the same row after someone fixes the sheet with DIFFERENT values than the override
SHEET_FIXED = _HEADER + _BOGOR_ROW + _OLD_MENSA + \
    '"PT MENSA BINASUKSES","PT MENSA BINASUKSES - JAKARTA","Active","Jakarta","Bandung","West Java (Sd)","ZZZ","DST366","","","",""\n'


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, NOO_DIR / filename)
    mod = importlib.util.module_from_spec(spec)
    return spec, mod


@pytest.fixture
def ovr(monkeypatch):
    spec, mod = _load("distributor_overrides", "distributor_overrides.py")
    spec.loader.exec_module(mod)
    monkeypatch.setitem(sys.modules, "distributor_overrides", mod)
    return mod


@pytest.fixture
def shared(monkeypatch, ovr):
    """The real shared.py with a fake `streamlit` (identity cache decorators)."""
    def _cache(*a, **k):
        return a[0] if (a and callable(a[0]) and not k) else (lambda f: f)
    fake_st = types.ModuleType("streamlit")
    fake_st.cache_data = _cache
    fake_st.cache_resource = _cache
    fake_st.warning = lambda *a, **k: None
    fake_st.secrets = {}
    monkeypatch.setitem(sys.modules, "streamlit", fake_st)
    # shared.py builds BigQuery credentials at import (secrets, else a local key file). No credentials in tests.
    from google.oauth2 import service_account
    monkeypatch.setattr(service_account.Credentials, "from_service_account_file",
                        classmethod(lambda cls, *a, **k: object()))
    spec, mod = _load("shared", "shared.py")
    monkeypatch.setitem(sys.modules, "shared", mod)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def use_sheet(monkeypatch):
    """Serve CSV text instead of the Google Sheet. Only read_csv is replaced; the real Prefix id.xlsx is read."""
    def _set(csv_text: str):
        real_read_csv = pd.read_csv
        monkeypatch.setattr(pd, "read_csv", lambda *a, **k: real_read_csv(io.StringIO(csv_text), dtype=str))
    return _set


# ─────────────────────────────── pure helpers ───────────────────────────────

def test_override_values_are_wellformed(ovr):
    codes = {str(r[5]).strip() for r in openpyxl.load_workbook(NOO_DIR / "Prefix id.xlsx").active.iter_rows(min_row=2, values_only=True) if r[5]}
    assert ovr.OVERRIDE_DISTRIBUTOR, "override table must not be empty while this file exists"
    for kode, o in ovr.OVERRIDE_DISTRIBUTOR.items():
        assert re.fullmatch(r"DST\d{3}", kode)
        assert re.fullmatch(r"[A-Z0-9]{2,5}", o["company_code"]), "company code looks like MBS/SDS/UDN"
        assert o["prefix"] in codes, "the override prefix must be a real code from Coverage Mapping / Prefix id.xlsx"
        assert len(o["prefix"]) == 4, "PANJANG_PREFIX is 4"


def test_company_code_fills_blank_only(ovr):
    assert ovr.company_code_efektif("DST366", "") == "MBS"
    assert ovr.company_code_efektif("dst366 ", None) == "MBS"          # key is normalised, None is safe
    assert ovr.company_code_efektif("DST366", "ZZZ") == "ZZZ"          # sheet wins
    assert ovr.company_code_efektif("DST133", "") == ""                # other distributors untouched
    assert ovr.company_code_efektif("", "") == ""


def test_lengkapi_prefix_fills_missing_and_never_overwrites(ovr):
    names = {"DST366": JAKARTA}
    original = {BOGOR: "ICWJ"}
    out = ovr.lengkapi_prefix(original, names)
    assert out[JAKARTA] == "ICWJ" and out[BOGOR] == "ICWJ"
    assert original == {BOGOR: "ICWJ"}, "input dict must not be mutated"
    assert ovr.lengkapi_prefix({JAKARTA: "ICWB"}, names)[JAKARTA] == "ICWB"   # sheet wins
    assert ovr.lengkapi_prefix({}, {}) == {}                                   # distributor not in sheet -> nothing


# ───────────────────── real shared.py loaders against the fixture ─────────────────────

def test_negative_control_without_override_dst366_has_no_prefix(shared, ovr, use_sheet, monkeypatch):
    """Reproduces the live failure, proving the tests below exercise the override."""
    use_sheet(SHEET_AS_LIVE)
    monkeypatch.setattr(ovr, "OVERRIDE_DISTRIBUTOR", {})
    assert JAKARTA not in shared.muat_prefix_per_distributor()
    assert shared.muat_distri_code_map()["DST366"]["company_code"] == ""
    assert shared.muat_distri_customer_code_raw()[JAKARTA] == ""


def test_dst366_gets_prefix_and_company_code(shared, use_sheet):
    use_sheet(SHEET_AS_LIVE)
    assert shared.muat_prefix_per_distributor()[JAKARTA] == "ICWJ"
    assert shared.muat_prefix_per_distributor()[BOGOR] == "ICWJ"              # already resolved by the sheet
    info = shared.muat_distri_code_map()["DST366"]
    assert info == {"distributor": JAKARTA, "company_code": "MBS"}
    assert shared.muat_distri_code_map()["DST133"]["company_code"] == "SDS"   # untouched
    raw = shared.muat_distri_customer_code_raw()[JAKARTA]
    assert raw == "MBS"
    assert shared.bangun_customer_code(raw, "G2G-LIPSTICK") == "12MBS"
    assert shared.bangun_customer_code(raw, "NXP-SERUM") == "17MBS"


def test_sheet_wins_once_it_is_fixed(shared, use_sheet):
    use_sheet(SHEET_FIXED)
    assert shared.muat_prefix_per_distributor()[JAKARTA] == "ICWB"            # Bandung coverage from the sheet
    assert shared.muat_distri_code_map()["DST366"]["company_code"] == "ZZZ"
    assert shared.muat_distri_customer_code_raw()[JAKARTA] == "ZZZ"


def test_override_failure_never_breaks_the_prefix_loader(shared, use_sheet, monkeypatch):
    use_sheet(SHEET_AS_LIVE)
    def boom():
        raise RuntimeError("sheet unreachable")
    monkeypatch.setattr(shared, "muat_distri_code_map", boom)
    out = shared.muat_prefix_per_distributor()                                # must not raise
    assert out[BOGOR] == "ICWJ" and JAKARTA not in out                        # = behaviour before this change
