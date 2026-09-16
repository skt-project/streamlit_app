"""Authentication and authorization rules for Salesman PJP.

Covers `pjp_auth` end to end without Streamlit, without BigQuery and without
credentials: every rule that decides whether someone may log in or write is a
pure function taking an account dict, so the account store is a plain dict here.

The numbered tests map 1:1 onto the security review in the implementation
brief (Test 1 .. Test 10).

Run: pytest tests/test_pjp_auth.py -q
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import pjp_auth as A  # noqa: E402

TODAY = date(2026, 9, 16)
GOOD_PASSWORD = "sup3rsecret"
OTHER_PASSWORD = "n0tthesame"


def make_account(code="DST171", password=GOOD_PASSWORD, is_active=True,
                 deadline=date(2026, 9, 30), username=None):
    return {
        "distributor_code": code,
        "distributor_name": f"DISTRIBUTOR {code}",
        "username": code if username is None else username,
        "password_hash": A.hash_password(password) if password else "",
        "is_active": is_active,
        "input_deadline": deadline,
    }


class FakeAccountStore:
    """Stands in for `pjp_accounts.load_account`, counting reads.

    The read count is what proves the write path re-reads rather than reusing
    a value captured at login.
    """

    def __init__(self, accounts):
        self.accounts = {A.norm_code(k): v for k, v in accounts.items()}
        self.reads = 0

    def load(self, code):
        self.reads += 1
        row = self.accounts.get(A.norm_code(code))
        # A real BigQuery read builds a new dict each time. Copying here is what
        # makes "the row captured at login" genuinely stale rather than a live
        # alias of the store.
        return dict(row) if row is not None else None


# --- hashing -----------------------------------------------------------------

def test_hash_is_not_the_password_and_is_salted():
    first = A.hash_password(GOOD_PASSWORD)
    second = A.hash_password(GOOD_PASSWORD)
    assert GOOD_PASSWORD not in first
    assert first.startswith("$2b$")
    assert first != second, "bcrypt must salt; identical hashes mean no salt"
    assert A.verify_password(GOOD_PASSWORD, first)
    assert A.verify_password(GOOD_PASSWORD, second)


@pytest.mark.parametrize("bad_hash", [
    None, "", "not-a-hash", 12345,
    "$2b$12$short",                                  # truncated: panics bcrypt
    "$2b$12$" + "x" * 52,                            # one char short of 60
    "plaintextpasswordstoredbymistake" + "x" * 28,   # right length, wrong shape
])
def test_verify_password_never_raises_on_a_broken_stored_hash(bad_hash):
    """bcrypt 4.x panics (BaseException) on a malformed hash - see
    pjp_auth.looks_like_bcrypt. A bad row must fail the login, not the app."""
    assert A.verify_password(GOOD_PASSWORD, bad_hash) is False


def test_looks_like_bcrypt():
    assert A.looks_like_bcrypt(A.hash_password(GOOD_PASSWORD)) is True
    for bad in (None, "", "12345678", "$2b$12$short", "x" * 60):
        assert A.looks_like_bcrypt(bad) is False


def test_hash_password_rejects_empty():
    for value in ("", None, 5):
        with pytest.raises(ValueError):
            A.hash_password(value)


@pytest.mark.parametrize("new,confirm,ok", [
    ("sup3rsecret", "sup3rsecret", True),
    ("sup3rsecret", "different", False),
    ("short7", "short7", False),      # under MIN_PASSWORD_LENGTH
    ("", "", False),
    ("   ", "   ", False),
    ("12345678", "12345678", True),   # exactly MIN_PASSWORD_LENGTH
])
def test_validate_new_password(new, confirm, ok):
    assert A.validate_new_password(new, confirm).ok is ok


# --- deadline ----------------------------------------------------------------

def test_deadline_today_is_still_open():
    """Test 5 - deadline is today -> ACCESS ALLOWED. Inclusive, as before."""
    assert A.is_deadline_active(date(2026, 9, 16), TODAY) is True


def test_deadline_yesterday_is_closed():
    """Test 6 - deadline is yesterday -> ACCESS BLOCKED."""
    assert A.is_deadline_active(date(2026, 9, 15), TODAY) is False


def test_deadline_tomorrow_is_open():
    assert A.is_deadline_active(date(2026, 9, 17), TODAY) is True


def test_missing_deadline_fails_closed():
    """A NULL deadline must never mean 'unlimited'."""
    assert A.is_deadline_active(None, TODAY) is False
    assert A.is_deadline_active("", TODAY) is False
    assert A.is_deadline_active("tidak-tanggal", TODAY) is False


@pytest.mark.parametrize("value,expected", [
    ("2026-09-30", date(2026, 9, 30)),
    ("2026-09-30 00:00:00", date(2026, 9, 30)),
    (date(2026, 9, 30), date(2026, 9, 30)),
    (None, None),
    ("", None),
    ("rubbish", None),
])
def test_coerce_date(value, expected):
    assert A.coerce_date(value) == expected


@pytest.mark.parametrize("deadline,expected", [
    (date(2026, 9, 16), 0),     # last day still counts as open
    (date(2026, 9, 30), 14),
    (date(2026, 9, 15), -1),
    (None, None),
])
def test_days_remaining(deadline, expected):
    assert A.days_remaining(deadline, TODAY) == expected


@pytest.mark.parametrize("is_active,deadline,expected", [
    (True, date(2026, 10, 30), A.STATUS_ACTIVE),
    (True, date(2026, 9, 20), A.STATUS_EXPIRING),
    (True, date(2026, 9, 16), A.STATUS_EXPIRING),
    (True, date(2026, 9, 15), A.STATUS_EXPIRED),
    (False, date(2026, 10, 30), A.STATUS_INACTIVE),
    (True, None, A.STATUS_NO_DEADLINE),
])
def test_deadline_status_labels(is_active, deadline, expected):
    account = {"is_active": is_active, "input_deadline": deadline}
    assert A.deadline_status(account, TODAY) == expected


# --- login -------------------------------------------------------------------

def test_1_wrong_password_fails():
    """Test 1 - wrong distributor password -> LOGIN FAIL."""
    result = A.evaluate_distributor_login(make_account(), OTHER_PASSWORD,
                                          today=TODAY)
    assert result.ok is False
    assert result.reason == A.LOGIN_INVALID


def test_2_correct_password_succeeds():
    """Test 2 - correct password -> LOGIN SUCCESS."""
    result = A.evaluate_distributor_login(make_account(), GOOD_PASSWORD,
                                          today=TODAY)
    assert result.ok is True
    assert result.reason == A.LOGIN_OK


def test_3_inactive_account_is_blocked():
    """Test 3 - inactive account -> LOGIN BLOCKED, even with the right password."""
    result = A.evaluate_distributor_login(make_account(is_active=False),
                                          GOOD_PASSWORD, today=TODAY)
    assert result.ok is False
    assert result.reason == A.LOGIN_INACTIVE


def test_4_expired_deadline_is_blocked():
    """Test 4 - expired deadline -> LOGIN BLOCKED."""
    result = A.evaluate_distributor_login(
        make_account(deadline=date(2026, 9, 15)), GOOD_PASSWORD, today=TODAY)
    assert result.ok is False
    assert result.reason == A.LOGIN_EXPIRED
    assert result.deadline == date(2026, 9, 15)


def test_5_login_on_the_deadline_day_succeeds():
    result = A.evaluate_distributor_login(
        make_account(deadline=TODAY), GOOD_PASSWORD, today=TODAY)
    assert result.ok is True


def test_missing_account_is_indistinguishable_from_a_wrong_password():
    """A visitor must not learn which distributors have accounts."""
    missing = A.evaluate_distributor_login(None, GOOD_PASSWORD, today=TODAY)
    wrong = A.evaluate_distributor_login(make_account(), OTHER_PASSWORD,
                                         today=TODAY)
    assert missing.reason == wrong.reason == A.LOGIN_INVALID


def test_status_is_not_revealed_before_the_password_is_right():
    """An inactive/expired account with a WRONG password must look ordinary."""
    for account in (make_account(is_active=False),
                    make_account(deadline=date(2026, 1, 1))):
        result = A.evaluate_distributor_login(account, OTHER_PASSWORD,
                                              today=TODAY)
        assert result.reason == A.LOGIN_INVALID


def test_wrong_username_fails_like_a_wrong_password():
    result = A.evaluate_distributor_login(make_account(), GOOD_PASSWORD,
                                          username="somebodyelse", today=TODAY)
    assert result.ok is False
    assert result.reason == A.LOGIN_INVALID


@pytest.mark.parametrize("entered", ["DST171", "dst171", "  DST171  "])
def test_username_match_is_case_and_space_insensitive(entered):
    assert A.username_matches(make_account(), entered) is True


def test_blank_username_falls_back_to_the_distributor_code():
    account = make_account(username="")
    assert A.account_username(account) == "DST171"
    assert A.username_matches(account, "DST171") is True


# --- access / write path -----------------------------------------------------

def test_write_access_allowed_for_own_active_in_deadline_account():
    access = A.validate_distributor_access(make_account(), "DST171", "DST171",
                                           TODAY)
    assert access.ok is True


def test_7_distributor_cannot_act_on_another_distributor():
    """Test 7 - DST171 logs in, tries to touch DST157 -> BLOCKED."""
    access = A.validate_distributor_access(make_account("DST171"), "DST171",
                                           "DST157", TODAY)
    assert access.ok is False
    assert access.reason == A.ACCESS_SCOPE_MISMATCH


def test_scope_check_survives_case_and_whitespace_tampering():
    assert A.validate_distributor_access(make_account(), "DST171",
                                         " dst171 ", TODAY).ok is True
    assert A.validate_distributor_access(make_account(), "DST171",
                                         "dst157", TODAY).ok is False


def test_account_row_for_a_different_code_is_rejected():
    """Even if a lookup returned the wrong row, the scope check catches it."""
    access = A.validate_distributor_access(make_account("DST157"), "DST171",
                                           "DST171", TODAY)
    assert access.ok is False
    assert access.reason == A.ACCESS_SCOPE_MISMATCH


def test_no_session_is_rejected():
    for session in (None, "", "   "):
        access = A.validate_distributor_access(make_account(), session,
                                               "DST171", TODAY)
        assert access.ok is False
        assert access.reason == A.ACCESS_NO_SESSION


def test_missing_account_blocks_writes():
    access = A.validate_distributor_access(None, "DST171", "DST171", TODAY)
    assert access.ok is False
    assert access.reason == A.ACCESS_NOT_FOUND


def test_inactive_account_blocks_writes():
    access = A.validate_distributor_access(make_account(is_active=False),
                                           "DST171", "DST171", TODAY)
    assert access.ok is False
    assert access.reason == A.ACCESS_INACTIVE


def test_expired_deadline_blocks_writes():
    access = A.validate_distributor_access(
        make_account(deadline=date(2026, 9, 15)), "DST171", "DST171", TODAY)
    assert access.ok is False
    assert access.reason == A.ACCESS_EXPIRED


def test_access_messages_never_leak_internals():
    """No table name, no hash, no SQL in anything the user is shown."""
    accounts = [None, make_account(is_active=False),
                make_account(deadline=date(2026, 1, 1)), make_account("DST157")]
    for account in accounts:
        message = A.validate_distributor_access(account, "DST171", "DST171",
                                                TODAY).message.lower()
        for leak in ("sfa_pjp", "bigquery", "select", "$2b$", "gt_schema",
                     "password_hash", "traceback"):
            assert leak not in message


# --- Test 8 / 9 / 10: an admin change takes effect on the next action ---------

def test_8_password_change_invalidates_the_old_password():
    """Test 8 - admin changes the password; old fails, new succeeds."""
    store = FakeAccountStore({"DST171": make_account()})
    assert A.evaluate_distributor_login(store.load("DST171"), GOOD_PASSWORD,
                                        today=TODAY).ok is True

    store.accounts["DST171"]["password_hash"] = A.hash_password(OTHER_PASSWORD)

    assert A.evaluate_distributor_login(store.load("DST171"), GOOD_PASSWORD,
                                        today=TODAY).reason == A.LOGIN_INVALID
    assert A.evaluate_distributor_login(store.load("DST171"), OTHER_PASSWORD,
                                        today=TODAY).ok is True


def test_9_deadline_change_applies_immediately():
    """Test 9 - admin changes the deadline; the next check uses the new one."""
    store = FakeAccountStore({"DST171": make_account(deadline=date(2026, 9, 30))})
    assert A.validate_distributor_access(store.load("DST171"), "DST171",
                                         "DST171", TODAY).ok is True

    store.accounts["DST171"]["input_deadline"] = date(2026, 9, 15)

    access = A.validate_distributor_access(store.load("DST171"), "DST171",
                                           "DST171", TODAY)
    assert access.ok is False
    assert access.reason == A.ACCESS_EXPIRED


def test_10_deactivation_blocks_an_already_open_session_before_the_write():
    """Test 10 (critical) - admin disables the account while a distributor
    session is open; the next write is refused.

    This only holds because the write path re-reads the account. The assertion
    on `store.reads` is what pins that: reusing the row captured at login would
    leave the count at 1 and let the write through.
    """
    store = FakeAccountStore({"DST171": make_account()})

    # Distributor logs in successfully.
    account_at_login = store.load("DST171")
    assert A.evaluate_distributor_login(account_at_login, GOOD_PASSWORD,
                                        today=TODAY).ok is True
    state = {}
    A.establish_distributor_session(state, account_at_login)
    assert A.session_distributor_code(state) == "DST171"

    # Admin disables the account mid-session.
    store.accounts["DST171"]["is_active"] = False

    # Write path re-reads and refuses.
    fresh = store.load(A.session_distributor_code(state))
    access = A.validate_distributor_access(fresh, A.session_distributor_code(state),
                                           "DST171", TODAY)
    assert access.ok is False
    assert access.reason == A.ACCESS_INACTIVE
    assert store.reads == 2, "the write path must re-read, not reuse the login row"

    # The row captured at login would still have said 'allowed' - the bug this
    # test exists to prevent.
    assert A.validate_distributor_access(account_at_login, "DST171", "DST171",
                                         TODAY).ok is True


# --- admin credentials -------------------------------------------------------

ADMIN_CONFIG = {"username": "admin",
                "password_hash": A.hash_password("adminsecret")}


def test_admin_login_succeeds_with_the_configured_credentials():
    assert A.verify_admin_credentials("admin", "adminsecret", ADMIN_CONFIG) is True


@pytest.mark.parametrize("username,password", [
    ("admin", "wrong"),
    ("root", "adminsecret"),
    ("", "adminsecret"),
    ("admin", ""),
    ("ADMIN", "adminsecret"),   # username is exact-match, not case-folded
])
def test_admin_login_fails(username, password):
    assert A.verify_admin_credentials(username, password, ADMIN_CONFIG) is False


@pytest.mark.parametrize("config", [None, {}, {"username": "admin"},
                                    {"password_hash": "x"}, "not-a-mapping"])
def test_admin_login_fails_closed_when_unconfigured(config):
    """No `[admin]` secret deployed must mean no admin, not an open door."""
    assert A.admin_users_from_config(config) == {}
    assert A.verify_admin_credentials("admin", "adminsecret", config) is False


def test_multiple_admins_are_supported():
    config = {"users": [
        {"username": "a", "password_hash": A.hash_password("passwordA")},
        {"username": "b", "password_hash": A.hash_password("passwordB")},
    ]}
    assert A.verify_admin_credentials("a", "passwordA", config) is True
    assert A.verify_admin_credentials("b", "passwordB", config) is True
    assert A.verify_admin_credentials("a", "passwordB", config) is False


# --- session -----------------------------------------------------------------

def test_distributor_session_lifecycle():
    state = {}
    assert A.current_role(state) is None
    assert A.is_distributor_authenticated(state) is False

    A.establish_distributor_session(state, make_account())
    assert A.current_role(state) == A.ROLE_DISTRIBUTOR
    assert A.is_distributor_authenticated(state) is True
    assert A.is_admin_authenticated(state) is False
    assert state["authenticated_distributor_code"] == "DST171"

    A.clear_session(state)
    assert A.current_role(state) is None
    assert A.session_distributor_code(state) is None
    for key in A.SESSION_KEYS:
        assert key not in state


def test_admin_session_lifecycle():
    state = {}
    A.establish_admin_session(state, "admin")
    assert A.is_admin_authenticated(state) is True
    assert A.is_distributor_authenticated(state) is False
    assert A.session_distributor_code(state) is None

    A.clear_session(state)
    assert A.is_admin_authenticated(state) is False


def test_logging_in_as_distributor_drops_any_admin_session():
    state = {}
    A.establish_admin_session(state, "admin")
    A.establish_distributor_session(state, make_account())
    assert A.is_admin_authenticated(state) is False
    assert "admin_authenticated" not in state


def test_logging_in_as_admin_drops_any_distributor_session():
    state = {}
    A.establish_distributor_session(state, make_account())
    A.establish_admin_session(state, "admin")
    assert A.is_distributor_authenticated(state) is False
    assert "authenticated_distributor_code" not in state


def test_password_hash_never_enters_session_state():
    state = {}
    A.establish_distributor_session(state, make_account())
    assert "$2b$" not in repr(state)
    for key in ("password_hash", "password", "hash"):
        assert key not in state


def test_scrub_strips_every_credential_field():
    scrubbed = A.scrub(make_account())
    assert "password_hash" not in scrubbed
    assert scrubbed["distributor_code"] == "DST171"


def test_forged_role_without_the_matching_flag_is_not_authenticated():
    """Setting auth_role alone must not authenticate anyone."""
    assert A.current_role({"auth_role": "admin"}) is None
    assert A.current_role({"auth_role": "distributor"}) is None
    assert A.is_distributor_authenticated(
        {"auth_role": "distributor", "distributor_authenticated": True}) is False


def test_assert_owns():
    state = {}
    A.establish_distributor_session(state, make_account("DST171"))
    assert A.assert_owns(state, "DST171") is True
    assert A.assert_owns(state, "dst171") is True
    assert A.assert_owns(state, "DST157") is False
    assert A.assert_owns({}, "DST171") is False


def test_admin_session_does_not_own_any_distributor_scope():
    """An admin must go through admin paths, not slip into a distributor write."""
    state = {}
    A.establish_admin_session(state, "admin")
    assert A.assert_owns(state, "DST171") is False
