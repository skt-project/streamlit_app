"""Admin login rules of the PO Suggestion Tool (po_buffer_admin).

The session is a plain dict here; in the app it is ``st.session_state``.

Run: pytest tests/test_po_buffer_admin.py -q
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import bcrypt
import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import po_buffer_admin as A  # noqa: E402

DEFAULTS = A.get_admin_credentials(secrets={}, environ={})


# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------

def test_correct_credentials_log_in_and_mark_the_session():
    session = {}
    result = A.attempt_admin_login(session, "admin", "12345678", DEFAULTS)
    assert result.ok
    assert A.is_admin_authenticated(session)
    assert session[A.SESSION_USER_KEY] == "admin"


@pytest.mark.parametrize(
    "username, password",
    [
        ("admin", "wrong"),
        ("admin", "12345679"),
        ("admin", "1234567"),
        ("admin", "123456789"),
        ("Admin", "12345678"),     # username is case-sensitive
        ("root", "12345678"),
        ("admin", "12345678 "),    # the password is not trimmed
        ("admin", "ADMIN"),
    ],
)
def test_wrong_credentials_are_rejected_with_an_error_message(username, password):
    session = {}
    result = A.attempt_admin_login(session, username, password, DEFAULTS)
    assert not result.ok
    assert result.message == A.MSG_BAD_CREDENTIALS
    assert not A.is_admin_authenticated(session)


@pytest.mark.parametrize("username, password", [("", ""), ("admin", ""), ("", "12345678"), ("  ", "x")])
def test_empty_fields_are_rejected(username, password):
    session = {}
    assert not A.attempt_admin_login(session, username, password, DEFAULTS).ok
    assert not A.is_admin_authenticated(session)


def test_username_surrounding_whitespace_is_ignored():
    assert A.attempt_admin_login({}, "  admin  ", "12345678", DEFAULTS).ok


def test_a_session_that_never_logged_in_is_not_authenticated():
    assert not A.is_admin_authenticated({})
    assert not A.is_admin_authenticated({A.SESSION_AUTH_KEY: "yes"})   # only literal True counts
    assert not A.is_admin_authenticated({A.SESSION_AUTH_KEY: 1})


def test_logout_clears_the_login_state():
    session = {}
    A.attempt_admin_login(session, "admin", "12345678", DEFAULTS)
    A.logout_admin(session)
    assert not A.is_admin_authenticated(session)
    assert A.SESSION_USER_KEY not in session
    A.logout_admin(session)  # logging out twice is harmless


def test_repeated_failures_lock_the_session_for_a_while():
    session = {}
    t = 1000.0
    for _ in range(A.MAX_FAILED_ATTEMPTS):
        last = A.attempt_admin_login(session, "admin", "nope", DEFAULTS, now=t)
    assert "Terlalu banyak" in last.message
    # even the right password is refused while locked
    assert not A.attempt_admin_login(session, "admin", "12345678", DEFAULTS, now=t + 1).ok
    assert A.lockout_remaining(session, now=t + 1) > 0
    # ... and accepted once the lock has expired
    assert A.attempt_admin_login(session, "admin", "12345678", DEFAULTS, now=t + A.LOCKOUT_SECONDS + 1).ok


def test_a_successful_login_resets_the_failure_counter():
    session = {}
    for _ in range(A.MAX_FAILED_ATTEMPTS - 1):
        A.attempt_admin_login(session, "admin", "nope", DEFAULTS, now=1.0)
    assert A.attempt_admin_login(session, "admin", "12345678", DEFAULTS, now=2.0).ok
    assert session[A.SESSION_FAILS_KEY] == 0


# ---------------------------------------------------------------------------
# Credential configuration
# ---------------------------------------------------------------------------

def test_default_credentials_are_admin_and_12345678_and_are_flagged_as_default():
    assert DEFAULTS.username == "admin"
    assert DEFAULTS.is_default
    assert A.verify_admin_credentials("admin", "12345678", DEFAULTS)


def test_secrets_override_the_defaults():
    creds = A.get_admin_credentials(
        secrets={A.SECRETS_SECTION: {"username": "boss", "password": "s3cret-pass"}}, environ={}
    )
    assert not creds.is_default
    assert A.verify_admin_credentials("boss", "s3cret-pass", creds)
    assert not A.verify_admin_credentials("admin", "12345678", creds)


def test_environment_variables_override_the_defaults():
    env = {A.ENV_USERNAME: "ops", A.ENV_PASSWORD: "env-pass-1"}
    creds = A.get_admin_credentials(secrets={}, environ=env)
    assert not creds.is_default
    assert A.verify_admin_credentials("ops", "env-pass-1", creds)


def test_secrets_win_over_environment():
    creds = A.get_admin_credentials(
        secrets={A.SECRETS_SECTION: {"username": "from-secrets", "password": "pw-secrets"}},
        environ={A.ENV_USERNAME: "from-env", A.ENV_PASSWORD: "pw-env"},
    )
    assert creds.username == "from-secrets"
    assert A.verify_admin_credentials("from-secrets", "pw-secrets", creds)


def test_a_bcrypt_hash_can_be_configured_instead_of_a_plaintext_password():
    hashed = bcrypt.hashpw(b"hashed-pw", bcrypt.gensalt(rounds=4)).decode()
    creds = A.get_admin_credentials(
        secrets={A.SECRETS_SECTION: {"username": "admin", "password_hash": hashed}}, environ={}
    )
    assert not creds.is_default and creds.password is None
    assert A.verify_admin_credentials("admin", "hashed-pw", creds)
    assert not A.verify_admin_credentials("admin", "12345678", creds)   # default no longer works
    assert not A.verify_admin_credentials("admin", "wrong", creds)


def test_a_malformed_hash_denies_everyone_instead_of_crashing():
    creds = A.get_admin_credentials(
        secrets={A.SECRETS_SECTION: {"password_hash": "not-a-bcrypt-hash"}}, environ={}
    )
    assert not A.verify_admin_credentials("admin", "anything", creds)
    assert not A.verify_admin_credentials("admin", "12345678", creds)


def test_missing_or_broken_secrets_fall_back_to_the_defaults():
    class Exploding:
        def get(self, key, default=None):
            raise FileNotFoundError("no secrets.toml")

    creds = A.get_admin_credentials(secrets=Exploding(), environ={})
    assert creds.is_default and creds.username == "admin"
    assert A.get_admin_credentials(secrets=None, environ=None).is_default


# ---------------------------------------------------------------------------
# The password never leaks
# ---------------------------------------------------------------------------

def test_credentials_repr_does_not_contain_the_password():
    creds = A.get_admin_credentials(
        secrets={A.SECRETS_SECTION: {"username": "boss", "password": "very-secret-pw"}}, environ={}
    )
    assert "very-secret-pw" not in repr(creds) and "very-secret-pw" not in str(creds)
    assert "12345678" not in repr(DEFAULTS)


def test_passwords_are_never_logged(caplog):
    caplog.set_level(logging.DEBUG)
    session = {}
    A.attempt_admin_login(session, "admin", "wrong-password-xyz", DEFAULTS)
    A.attempt_admin_login(session, "intruder", "12345678", DEFAULTS)
    A.attempt_admin_login(session, "admin", "12345678", DEFAULTS)
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert "wrong-password-xyz" not in logged
    assert "12345678" not in logged
    assert "intruder" not in logged
    assert logged  # something was logged (failures + success), just not secrets
