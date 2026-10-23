import pytest

from admin_auth import LOCK_SECONDS, MAX_ATTEMPTS, credentials_match, lock_remaining, record_failure, reset
from config import load_config


def test_credentials_match_exact_only():
    assert credentials_match("admin", "pw-for-tests-only", "admin", "pw-for-tests-only")
    assert credentials_match(" admin ", "pw-for-tests-only", "admin", "pw-for-tests-only")        # username di-trim
    assert not credentials_match("admin", "pw-for-tests-x", "admin", "pw-for-tests-only")
    assert not credentials_match("Admin", "pw-for-tests-only", "admin", "pw-for-tests-only")      # case-sensitive
    assert not credentials_match("admin", " pw-for-tests-only", "admin", "pw-for-tests-only")     # password tidak di-trim
    assert not credentials_match("", "", "admin", "pw-for-tests-only")


def test_no_default_credentials_when_secrets_missing():
    assert not credentials_match("admin", "admin", "", "")
    assert not credentials_match("", "", "", "")


def test_lock_after_max_failures_and_expiry():
    state = {}
    for i in range(MAX_ATTEMPTS - 1):
        record_failure(state, now=100.0)
        assert lock_remaining(state, 100.0) == 0
    record_failure(state, now=100.0)
    assert lock_remaining(state, 100.0) == LOCK_SECONDS
    assert 0 < lock_remaining(state, 100.0 + LOCK_SECONDS - 1) <= 1
    assert lock_remaining(state, 100.0 + LOCK_SECONDS) == 0
    record_failure(state, now=500.0)                                            # hitungan mulai dari nol lagi
    assert lock_remaining(state, 500.0) == 0


def test_reset_clears_lock():
    state = {}
    for _ in range(MAX_ATTEMPTS):
        record_failure(state, now=1.0)
    reset(state)
    assert lock_remaining(state, 1.0) == 0 and state["failures"] == 0


BASE = {"app": {"master_google_sheet_id": "m", "output_google_sheet_id": "o", "bigquery_project": "p",
                "evidence_bucket": "bucket-x"}}


def test_admin_credentials_come_from_secrets_and_are_not_in_repr():
    cfg = load_config({**BASE, "admin": {"username": " admin ", "password": "pw-for-tests-only"}})
    assert cfg.admin_username == "admin" and cfg.admin_password == "pw-for-tests-only"
    assert "pw-for-tests-only" not in repr(cfg)


def test_admin_disabled_without_secrets_section():
    cfg = load_config(BASE)
    assert cfg.admin_username == "" and cfg.admin_password == ""
