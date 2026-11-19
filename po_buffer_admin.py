"""Admin login for the PO Suggestion Tool's download-tracking dashboard.

Pure logic: no Streamlit import. The login state lives in whatever mapping the
caller passes as ``session_state`` (``st.session_state`` in the app, a plain
dict in tests).

Credentials
-----------
Resolved per field, first match wins:

1. Streamlit secrets, section ``[po_buffer_admin]``: ``username``, ``password``
   and/or ``password_hash`` (a bcrypt hash - preferred, plaintext never stored)
2. environment variables ``PO_BUFFER_ADMIN_USERNAME``, ``PO_BUFFER_ADMIN_PASSWORD``,
   ``PO_BUFFER_ADMIN_PASSWORD_HASH``
3. the built-in default ``admin`` / ``12345678`` required by the brief

The default is only a bootstrap value: this repository is public, so anyone can
read it. ``AdminCredentials.is_default`` lets the UI warn while it is in use.
The password is never logged and never shown in ``repr``.
"""
from __future__ import annotations

import hmac
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Mapping, MutableMapping, Optional

logger = logging.getLogger(__name__)

DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_ADMIN_PASSWORD = "12345678"

SECRETS_SECTION = "po_buffer_admin"
ENV_USERNAME = "PO_BUFFER_ADMIN_USERNAME"
ENV_PASSWORD = "PO_BUFFER_ADMIN_PASSWORD"
ENV_PASSWORD_HASH = "PO_BUFFER_ADMIN_PASSWORD_HASH"

SESSION_AUTH_KEY = "po_admin_authenticated"
SESSION_USER_KEY = "po_admin_username"
SESSION_FAILS_KEY = "po_admin_failed_attempts"
SESSION_LOCK_KEY = "po_admin_locked_until"

MAX_FAILED_ATTEMPTS = 5
LOCKOUT_SECONDS = 30

MSG_BAD_CREDENTIALS = "Username atau password salah."
MSG_EMPTY = "Isi username dan password."


@dataclass(frozen=True)
class AdminCredentials:
    username: str
    password: Optional[str] = field(default=None, repr=False)
    password_hash: Optional[str] = field(default=None, repr=False)
    is_default: bool = False


@dataclass(frozen=True)
class LoginResult:
    ok: bool
    message: str = ""


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    return text if text.strip() else None


def get_admin_credentials(
    secrets: Optional[Mapping[str, Any]] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> AdminCredentials:
    """Resolve the admin credentials (see the module docstring for the order)."""
    section: Mapping[str, Any] = {}
    if secrets is not None:
        try:
            section = secrets.get(SECRETS_SECTION) or {}
        except Exception:  # noqa: BLE001 - no secrets file is a normal local setup
            section = {}
    env = environ or {}

    username = _text(section.get("username")) or _text(env.get(ENV_USERNAME))
    password = _text(section.get("password")) or _text(env.get(ENV_PASSWORD))
    password_hash = _text(section.get("password_hash")) or _text(env.get(ENV_PASSWORD_HASH))

    uses_default_secret = password is None and password_hash is None
    return AdminCredentials(
        username=(username or DEFAULT_ADMIN_USERNAME).strip(),
        password=DEFAULT_ADMIN_PASSWORD if uses_default_secret else password,
        password_hash=password_hash,
        is_default=uses_default_secret,
    )


def _bytes(value: Optional[str]) -> bytes:
    return (value or "").encode("utf-8")


def _check_hash(password: str, password_hash: str) -> bool:
    try:
        import bcrypt

        return bcrypt.checkpw(_bytes(password), _bytes(password_hash))
    except Exception:  # noqa: BLE001 - malformed hash or bcrypt missing => deny
        logger.error("Admin login: password_hash could not be verified; denying.")
        return False


def verify_admin_credentials(
    username: str, password: str, credentials: AdminCredentials
) -> bool:
    """Constant-time check of both fields (a wrong username is not revealed early)."""
    user_ok = hmac.compare_digest(_bytes((username or "").strip()), _bytes(credentials.username))
    if credentials.password_hash:
        pass_ok = _check_hash(password or "", credentials.password_hash)
    else:
        pass_ok = hmac.compare_digest(_bytes(password), _bytes(credentials.password))
    return user_ok and pass_ok


def is_admin_authenticated(session_state: Mapping[str, Any]) -> bool:
    return session_state.get(SESSION_AUTH_KEY) is True


def lockout_remaining(session_state: Mapping[str, Any], now: Optional[float] = None) -> int:
    """Seconds left of the temporary lock after too many failed attempts."""
    until = session_state.get(SESSION_LOCK_KEY) or 0.0
    remaining = until - (time.time() if now is None else now)
    return max(0, int(remaining + 0.999))


def attempt_admin_login(
    session_state: MutableMapping[str, Any],
    username: str,
    password: str,
    credentials: AdminCredentials,
    now: Optional[float] = None,
) -> LoginResult:
    """Try to log in; on success the session is marked authenticated.

    After ``MAX_FAILED_ATTEMPTS`` wrong attempts the session is locked for
    ``LOCKOUT_SECONDS``. This is a speed bump per browser session, not a
    substitute for a strong password.
    """
    current = time.time() if now is None else now
    wait = lockout_remaining(session_state, current)
    if wait:
        return LoginResult(False, f"Terlalu banyak percobaan. Coba lagi dalam {wait} detik.")
    if not (username or "").strip() or not password:
        return LoginResult(False, MSG_EMPTY)

    if verify_admin_credentials(username, password, credentials):
        session_state[SESSION_AUTH_KEY] = True
        session_state[SESSION_USER_KEY] = credentials.username
        session_state[SESSION_FAILS_KEY] = 0
        session_state[SESSION_LOCK_KEY] = 0.0
        logger.info("Admin login succeeded")
        return LoginResult(True)

    fails = int(session_state.get(SESSION_FAILS_KEY) or 0) + 1
    session_state[SESSION_FAILS_KEY] = fails
    logger.warning("Admin login failed (attempt %d)", fails)
    if fails >= MAX_FAILED_ATTEMPTS:
        session_state[SESSION_FAILS_KEY] = 0
        session_state[SESSION_LOCK_KEY] = current + LOCKOUT_SECONDS
        return LoginResult(
            False, f"Terlalu banyak percobaan. Coba lagi dalam {LOCKOUT_SECONDS} detik."
        )
    return LoginResult(False, MSG_BAD_CREDENTIALS)


def logout_admin(session_state: MutableMapping[str, Any]) -> None:
    for key in (SESSION_AUTH_KEY, SESSION_USER_KEY, SESSION_FAILS_KEY, SESSION_LOCK_KEY):
        session_state.pop(key, None)
