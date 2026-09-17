"""Bulk deadline management: planning, safety and verification rules.

Pure logic only - no Streamlit, no BigQuery, no credentials. The account store
is a list of row dicts, exactly the shape `pjp_accounts.load_all_accounts`
returns, so every rule that decides what a bulk run will touch is testable
without a network.

Covers the 14 cases the requirement lists, plus the ones that protect
credentials: only the deadline may move, and a half-applied write must never be
reported as success.

Run: pytest tests/test_pjp_bulk_deadline.py -q
"""
from __future__ import annotations

import ast
import sys
from datetime import date
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import pjp_auth as A  # noqa: E402

TODAY = date(2026, 9, 17)
TARGET = date(2026, 9, 30)
HASH_A = "$2b$12$C6UzMDM.H6dfI/f/IKcEe.7zVfHbbnLKu4dq/VoR/eSHcfPcPLLNC"


def acct(code, deadline=TODAY, is_active=True, username=None, hash_=HASH_A):
    return {"distributor_code": code, "distributor_name": f"DIST {code}",
            "username": username or code, "is_active": is_active,
            "input_deadline": deadline, "password_hash": hash_}


ACCOUNTS = [
    acct("DST171"), acct("DST157"), acct("DST152"),
    acct("DST363", deadline=date(2026, 9, 20)),
    acct("DST999", is_active=False),
]
ALL_ACTIVE = [a["distributor_code"] for a in ACCOUNTS if a["is_active"]]


# --- 2. empty selection rejected ---------------------------------------------

@pytest.mark.parametrize("codes", [None, [], ["", "   "]])
def test_empty_selection_is_rejected(codes):
    plan = A.plan_bulk_deadline(ACCOUNTS, codes, TARGET)
    assert plan.ok is False
    assert "minimal satu" in plan.message


# --- 3. invalid date rejected -------------------------------------------------

@pytest.mark.parametrize("bad", [None, "", "   ", "not-a-date", "30-09-2026"])
def test_missing_or_invalid_date_is_rejected(bad):
    plan = A.plan_bulk_deadline(ACCOUNTS, ALL_ACTIVE, bad)
    assert plan.ok is False
    assert "wajib dipilih" in plan.message


def test_no_silent_default_date():
    """A bulk run must never invent a date the admin did not choose."""
    assert A.plan_bulk_deadline(ACCOUNTS, ALL_ACTIVE, None).deadline is None


# --- 4 & 5. target resolution -------------------------------------------------

def test_all_active_selection_resolves_correctly():
    plan = A.plan_bulk_deadline(ACCOUNTS, ALL_ACTIVE, TARGET)
    assert plan.ok
    assert plan.affected == 4
    assert set(plan.changing) == {"DST171", "DST157", "DST152", "DST363"}


def test_multi_distributor_selection_resolves_correctly():
    plan = A.plan_bulk_deadline(ACCOUNTS, ["DST171", "DST363"], TARGET)
    assert plan.ok
    assert set(plan.changing) == {"DST171", "DST363"}
    assert plan.affected == 2


@pytest.mark.parametrize("spelling", ["dst171", "  DST171  ", "Dst171"])
def test_selection_is_case_and_space_insensitive(spelling):
    plan = A.plan_bulk_deadline(ACCOUNTS, [spelling], TARGET)
    assert plan.ok and plan.changing == ("DST171",)


def test_unknown_code_is_rejected_rather_than_ignored():
    """Silently dropping a code would understate what the admin is about to do."""
    plan = A.plan_bulk_deadline(ACCOUNTS, ["DST171", "DSTZZZ"], TARGET)
    assert plan.ok is False
    assert "DSTZZZ" in plan.message


# --- 6. only selected accounts are updated ------------------------------------

def test_only_selected_accounts_are_in_the_work_order():
    plan = A.plan_bulk_deadline(ACCOUNTS, ["DST171"], TARGET)
    assert plan.changing == ("DST171",)
    for other in ("DST157", "DST152", "DST363", "DST999"):
        assert other not in plan.changing
        assert other not in plan.unchanged


def test_inactive_accounts_are_skipped_not_silently_updated():
    plan = A.plan_bulk_deadline(ACCOUNTS, ALL_ACTIVE + ["DST999"], TARGET)
    assert "DST999" in plan.skipped_inactive
    assert "DST999" not in plan.changing


def test_selection_of_only_inactive_accounts_is_rejected():
    plan = A.plan_bulk_deadline(ACCOUNTS, ["DST999"], TARGET)
    assert plan.ok is False
    assert "nonaktif" in plan.message


# --- 11. no-op handling -------------------------------------------------------

def test_noop_when_every_account_already_has_the_target_date():
    plan = A.plan_bulk_deadline(ACCOUNTS, ["DST171", "DST157"], TODAY)
    assert plan.ok is True
    assert plan.is_noop is True
    assert plan.changing == ()
    assert set(plan.unchanged) == {"DST171", "DST157"}


def test_partial_noop_only_lists_the_accounts_that_change():
    """DST363 is on a different date; the other two are already on target."""
    plan = A.plan_bulk_deadline(ACCOUNTS, ALL_ACTIVE, TODAY)
    assert plan.changing == ("DST363",)
    assert set(plan.unchanged) == {"DST171", "DST157", "DST152"}
    assert plan.is_noop is False


def test_preview_shows_the_current_deadline_distribution():
    plan = A.plan_bulk_deadline(ACCOUNTS, ALL_ACTIVE, TARGET)
    assert dict(plan.current_distribution) == {"2026-09-17": 3, "2026-09-20": 1}


# --- 13. post-write verification ----------------------------------------------

def snap(rows):
    return {r["distributor_code"]: dict(r) for r in rows}


def test_verification_passes_on_a_correct_write():
    before = snap([acct("DST171"), acct("DST157")])
    after = snap([acct("DST171", deadline=TARGET), acct("DST157", deadline=TARGET)])
    ok, problems = A.verify_bulk_result(before, after, TARGET,
                                        ["DST171", "DST157"])
    assert ok is True and problems == []


def test_verification_detects_a_half_applied_write():
    """The case that must never be reported as success."""
    before = snap([acct("DST171"), acct("DST157")])
    after = snap([acct("DST171", deadline=TARGET), acct("DST157")])  # 157 missed
    ok, problems = A.verify_bulk_result(before, after, TARGET,
                                        ["DST171", "DST157"])
    assert ok is False
    assert any("DST157" in p for p in problems)


def test_verification_detects_a_changed_password_hash():
    before = snap([acct("DST171")])
    after = snap([acct("DST171", deadline=TARGET, hash_="$2b$12$" + "x" * 53)])
    ok, problems = A.verify_bulk_result(before, after, TARGET, ["DST171"])
    assert ok is False
    assert any("password_hash" in p for p in problems)


def test_verification_detects_a_changed_is_active():
    before = snap([acct("DST171")])
    after = snap([acct("DST171", deadline=TARGET, is_active=False)])
    ok, problems = A.verify_bulk_result(before, after, TARGET, ["DST171"])
    assert ok is False
    assert any("is_active" in p for p in problems)


def test_verification_detects_a_changed_username():
    before = snap([acct("DST171")])
    after = snap([acct("DST171", deadline=TARGET, username="someoneelse")])
    ok, problems = A.verify_bulk_result(before, after, TARGET, ["DST171"])
    assert ok is False
    assert any("username" in p for p in problems)


def test_verification_detects_collateral_damage_to_an_unselected_account():
    before = snap([acct("DST171"), acct("DST157")])
    after = snap([acct("DST171", deadline=TARGET),
                  acct("DST157", deadline=TARGET)])   # 157 was NOT selected
    ok, problems = A.verify_bulk_result(before, after, TARGET, ["DST171"])
    assert ok is False
    assert any("tidak dipilih" in p for p in problems)


def test_verification_detects_a_changed_row_count():
    before = snap([acct("DST171"), acct("DST157")])
    after = snap([acct("DST171", deadline=TARGET)])
    ok, problems = A.verify_bulk_result(before, after, TARGET, ["DST171"])
    assert ok is False


# --- 1, 7, 8, 9, 12, 14: enforced in the UI/DB layer; pinned by source --------

SRC = (REPO / "salesman_pjp.py").read_text(encoding="utf-8")
ACCOUNTS_SRC = (REPO / "pjp_accounts.py").read_text(encoding="utf-8")


def _func(source, name):
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(source, node)
    raise AssertionError(f"{name} not found")


def _code(source, name):
    """Function source WITHOUT its docstring.

    The docstrings here describe the SQL in prose, so inspecting raw source for
    column names gives false positives.
    """
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            body = node.body
            if (body and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and isinstance(body[0].value.value, str)):
                body = body[1:]
            return "\n".join(ast.get_source_segment(source, n) for n in body)
    raise AssertionError(f"{name} not found")


def test_1_bulk_handlers_require_admin():
    """Admin-only, checked in the handler - not merely by the tab being shown."""
    body = _func(SRC, "_render_bulk_deadline")
    assert body.count("require_admin()") >= 2, (
        "bulk needs require_admin at entry AND again inside the submit branch")
    submit_half = body.split("bulk_execute")[-1]
    assert "require_admin()" in submit_half


def test_7_8_9_bulk_update_touches_only_the_deadline_columns():
    body = _code(ACCOUNTS_SRC, "bulk_set_deadline")
    set_clause = body.split("SET", 1)[1].split("WHERE", 1)[0]
    for forbidden in ("password_hash", "is_active", "username", "created_at",
                      "created_by", "last_login_at", "password_changed_at",
                      "distributor_name"):
        assert forbidden not in set_clause, f"{forbidden} must not be updated"
    for allowed in ("input_deadline", "updated_at", "updated_by"):
        assert allowed in set_clause


def test_bulk_update_uses_parameterised_codes_not_interpolation():
    body = _code(ACCOUNTS_SRC, "bulk_set_deadline")
    assert "IN UNNEST(@codes)" in body
    assert "ArrayQueryParameter" in body
    assert "{codes}" not in body and "+ codes" not in body


def test_12_failure_path_does_not_claim_success():
    body = _func(SRC, "_execute_bulk_deadline")
    fail_branch = body.split("if not ok or affected != len(codes):")[1].split("return")[0]
    assert "st.error" in fail_branch
    assert "st.success" not in fail_branch


def test_13_success_is_only_reported_after_verification():
    body = _func(SRC, "_execute_bulk_deadline")
    assert body.index("verify_bulk_result") < body.index("st.success")


def test_14_caches_are_invalidated_after_a_bulk_write():
    body = _func(SRC, "_execute_bulk_deadline")
    assert "_refresh_accounts()" in body
    refresh = _func(SRC, "_refresh_accounts")
    assert "_load_accounts_for_admin.clear()" in refresh
    assert "_fetch_account_for_gate.clear()" in refresh


def test_audit_is_written_for_changed_accounts_and_marks_the_bulk_action():
    body = _func(SRC, "_execute_bulk_deadline")
    assert "bulk_write_audit" in body
    assert "bulk_action: true" in body
    assert "ACTION_CHANGE_DEADLINE" in body
    # audit is built from `codes`, i.e. plan.changing - no-ops get no row
    assert "for code in codes" in body


def test_individual_edit_account_still_exists():
    """Bulk must not have replaced the per-distributor path."""
    assert "def _render_edit_account" in SRC
    assert "Edit Akun" in SRC
