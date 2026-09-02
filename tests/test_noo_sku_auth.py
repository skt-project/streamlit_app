"""Authentication, password self-service, session handling and data isolation.

Pure functions and fakes — no credentials, no network.
"""
from __future__ import annotations

import pytest

from noo_sku import auth, guideline, validators

DIST_A = {"distributor_code": "DST082", "distributor_name": "CV CECE",
          "active": True, "status": "Active"}
DIST_B = {"distributor_code": "DST111", "distributor_name": "PT CSA",
          "active": True, "status": "Active"}


def _account(code="DST082", password=auth.DEFAULT_PASSWORD, must_change=True):
    return {"distributor_code": code, "distributor_name": "CV CECE",
            "password_hash": auth.hash_password(password),
            "must_change_password": must_change}


# ─── Hashing ──────────────────────────────────────────────────────────────────
@pytest.mark.sanity
def test_password_is_stored_hashed_never_plaintext():
    stored = auth.hash_password(auth.DEFAULT_PASSWORD)
    assert stored != auth.DEFAULT_PASSWORD
    assert stored.startswith("$2b$"), "must be a bcrypt hash"
    assert auth.DEFAULT_PASSWORD not in stored


@pytest.mark.sanity
def test_default_password_verifies():
    assert auth.verify_password(auth.DEFAULT_PASSWORD,
                                auth.hash_password(auth.DEFAULT_PASSWORD))


@pytest.mark.sanity
def test_wrong_password_is_rejected():
    assert not auth.verify_password("salah",
                                    auth.hash_password(auth.DEFAULT_PASSWORD))


def test_same_password_hashes_differently_each_time():
    """Per-account salt: 215 accounts sharing the default must not share a hash."""
    a = auth.hash_password(auth.DEFAULT_PASSWORD)
    b = auth.hash_password(auth.DEFAULT_PASSWORD)
    assert a != b
    assert auth.verify_password(auth.DEFAULT_PASSWORD, a)
    assert auth.verify_password(auth.DEFAULT_PASSWORD, b)


def test_empty_password_cannot_be_hashed():
    with pytest.raises(ValueError):
        auth.hash_password("")


@pytest.mark.parametrize("bad", ["", None, "not-a-hash", "$2b$broken"])
def test_malformed_hash_returns_false_rather_than_raising(bad):
    assert auth.verify_password("anything", bad) is False


def test_verify_rejects_an_empty_candidate():
    assert not auth.verify_password("", auth.hash_password("something"))


# ─── Change password ──────────────────────────────────────────────────────────
@pytest.mark.sanity
def test_valid_change_is_accepted():
    stored = auth.hash_password(auth.DEFAULT_PASSWORD)
    check = auth.validate_new_password(auth.DEFAULT_PASSWORD, "rahasia2026",
                                       "rahasia2026", stored)
    assert check.ok


@pytest.mark.sanity
def test_wrong_current_password_is_rejected():
    stored = auth.hash_password(auth.DEFAULT_PASSWORD)
    check = auth.validate_new_password("salah", "rahasia2026", "rahasia2026",
                                       stored)
    assert not check.ok and "saat ini salah" in check.message


@pytest.mark.sanity
def test_confirmation_mismatch_is_rejected():
    stored = auth.hash_password(auth.DEFAULT_PASSWORD)
    check = auth.validate_new_password(auth.DEFAULT_PASSWORD, "rahasia2026",
                                       "rahasia2027", stored)
    assert not check.ok and "Konfirmasi" in check.message


@pytest.mark.sanity
@pytest.mark.parametrize("new", ["", "   "])
def test_empty_new_password_is_rejected(new):
    stored = auth.hash_password(auth.DEFAULT_PASSWORD)
    check = auth.validate_new_password(auth.DEFAULT_PASSWORD, new, new, stored)
    assert not check.ok


def test_short_password_is_rejected():
    stored = auth.hash_password(auth.DEFAULT_PASSWORD)
    check = auth.validate_new_password(auth.DEFAULT_PASSWORD, "abc", "abc",
                                       stored)
    assert not check.ok and "minimal" in check.message


def test_reusing_the_current_password_is_rejected():
    stored = auth.hash_password("rahasia2026")
    check = auth.validate_new_password("rahasia2026", "rahasia2026",
                                       "rahasia2026", stored)
    assert not check.ok


def test_setting_the_default_as_the_new_password_is_rejected():
    stored = auth.hash_password("rahasia2026")
    check = auth.validate_new_password("rahasia2026", auth.DEFAULT_PASSWORD,
                                       auth.DEFAULT_PASSWORD, stored)
    assert not check.ok and "default" in check.message


@pytest.mark.sanity
def test_old_password_stops_working_after_a_change():
    """Simulates the stored hash being replaced, as set_password does."""
    stored = auth.hash_password(auth.DEFAULT_PASSWORD)
    assert auth.verify_password(auth.DEFAULT_PASSWORD, stored)

    check = auth.validate_new_password(auth.DEFAULT_PASSWORD, "rahasia2026",
                                       "rahasia2026", stored)
    assert check.ok
    stored = auth.hash_password("rahasia2026")

    assert not auth.verify_password(auth.DEFAULT_PASSWORD, stored)
    assert auth.verify_password("rahasia2026", stored)


# ─── Session ──────────────────────────────────────────────────────────────────
@pytest.mark.sanity
def test_unauthenticated_state_is_not_authenticated():
    assert auth.is_authenticated({}) is False
    assert auth.is_authenticated({"auth": True}) is False, "needs a distributor"
    assert auth.session_distributor_code({}) is None


@pytest.mark.sanity
def test_login_establishes_the_session_without_storing_the_hash():
    state = {}
    auth.establish_session(state, DIST_A, _account())
    assert auth.is_authenticated(state)
    assert auth.session_distributor_code(state) == "DST082"
    # establish_session strips credential material even when handed a full row.
    assert "password_hash" not in str(state.get("account"))
    assert "password_hash" not in str(state)


@pytest.mark.sanity
def test_logout_clears_every_auth_key():
    state = {}
    auth.establish_session(state, DIST_A, _account())
    auth.clear_session(state)
    assert auth.is_authenticated(state) is False
    for key in auth.SESSION_KEYS:
        assert key not in state


def test_must_change_flag_is_carried_into_the_session():
    state = {}
    auth.establish_session(state, DIST_A, _account(must_change=True))
    assert state["must_change_password"] is True


# ─── Data isolation ───────────────────────────────────────────────────────────
@pytest.mark.sanity
def test_distributor_can_only_act_on_its_own_code():
    state = {}
    auth.establish_session(state, DIST_A, _account())
    assert auth.assert_owns(state, "DST082") is True
    assert auth.assert_owns(state, "dst082") is True, "case-insensitive"
    assert auth.assert_owns(state, " DST082 ") is True, "whitespace-tolerant"
    assert auth.assert_owns(state, "DST111") is False, "another distributor"


@pytest.mark.sanity
def test_a_tampered_session_code_cannot_reach_another_distributor():
    """Even if session_state is edited, ownership is decided by comparison."""
    state = {}
    auth.establish_session(state, DIST_A, _account())
    assert auth.assert_owns(state, DIST_B["distributor_code"]) is False


@pytest.mark.sanity
def test_ownership_fails_closed_when_not_logged_in():
    assert auth.assert_owns({}, "DST082") is False


@pytest.mark.sanity
def test_every_write_and_query_derives_from_the_session_code():
    """The pipeline and writer take the distributor from the session only."""
    import inspect

    from noo_sku import pipeline, writer

    for src in (inspect.getsource(pipeline.run_noo),
                inspect.getsource(pipeline.run_sku)):
        assert 'distributor["distributor_code"]' in src

    # SKU stays login-bound. NOO takes the branch from the row — but only after
    # validate_noo has confirmed that code sits inside the authorised company,
    # so the file still cannot reach another company's data.
    assert "norm_key(distributor_code)" in inspect.getsource(
        writer.build_sku_row)
    assert "Customer Branch Code" in inspect.getsource(writer.build_noo_row)
    assert "row_code not in allowed" in inspect.getsource(
        validators.validate_noo), "branch must be checked against the company"


@pytest.mark.sanity
def test_ledger_reads_are_scoped_to_the_authorised_codes():
    import inspect

    from noo_sku import sources

    for fn in (sources.load_noo_ledger, sources.load_sku_ledger):
        src = inspect.getsource(fn)
        assert "distributor_code" in src
        assert "_as_code_set(distributor_code)" in src,             "filtered at the data layer, across every authorised branch"


def test_account_lookup_is_parameterised_and_active_only():
    import inspect

    from noo_sku import sources

    src = inspect.getsource(sources.load_account)
    assert "@code" in src, "must be parameterised, not string-formatted"
    assert "is_active = TRUE" in src


# ─── Context-aware guidelines ─────────────────────────────────────────────────
@pytest.mark.sanity
def test_noo_guideline_contains_no_sku_instructions():
    text = guideline.as_markdown(guideline.UPLOAD_NOO)
    assert "Customer Store Code" in text
    assert "Principal Product Code" not in text
    assert "SKU Mapping dipakai" not in text


@pytest.mark.sanity
def test_sku_guideline_contains_no_noo_instructions():
    text = guideline.as_markdown(guideline.UPLOAD_SKU)
    assert "Principal Product Code" in text
    assert "Customer Store Code" not in text
    assert "Store Type" not in text


def test_shared_rules_live_only_in_the_general_section():
    noo = guideline.as_markdown(guideline.UPLOAD_NOO)
    sku = guideline.as_markdown(guideline.UPLOAD_SKU)
    assert "Umum" in noo and "Umum" in sku
    assert len(guideline.GENERAL) == 1, "General stays short and separate"


def test_each_function_gets_its_own_titled_pdf():
    noo, sku = (guideline.build_pdf(guideline.UPLOAD_NOO),
                guideline.build_pdf(guideline.UPLOAD_SKU))
    assert noo[:4] == b"%PDF" and sku[:4] == b"%PDF"
    assert noo != sku
    assert "NOO" in guideline.title_for("NOO")
    assert "SKU" in guideline.title_for("SKU")


# ─── Login surface ────────────────────────────────────────────────────────────
@pytest.mark.sanity
def test_login_does_not_reveal_whether_a_distributor_code_exists():
    from pathlib import Path

    app = (Path(__file__).resolve().parents[1] / "noo_sku_mapping.py").read_text(
        encoding="utf-8")
    assert "Kode distributor atau password salah." in app
    assert "tidak dikenali" not in app.split("def render_login")[1].split(
        "def render_change_password")[0]


@pytest.mark.sanity
def test_app_cannot_be_rendered_without_authentication():
    from pathlib import Path

    app = (Path(__file__).resolve().parents[1] / "noo_sku_mapping.py").read_text(
        encoding="utf-8")
    assert "if auth.is_authenticated(st.session_state):" in app
    assert "auth.clear_session(st.session_state)\n    render_login()" in app


def test_no_default_password_literal_in_the_ui():
    from pathlib import Path

    app = (Path(__file__).resolve().parents[1] / "noo_sku_mapping.py").read_text(
        encoding="utf-8")
    assert auth.DEFAULT_PASSWORD not in app, "no hardcoded credential in the UI"
