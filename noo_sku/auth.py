"""Distributor authentication: hashing, validation, session.

Pure functions only — no Streamlit, no network — so every rule here is unit
tested without credentials. The database side lives in `sources.py`.

Why this app has its own credentials table rather than reusing
`po_portal_distributor_users`: that table stores passwords in **plaintext** and
is read directly by `po_portal_suggestion.py`, `_dev` and `_v2`. Hashing it
would break all three. So NOO/SKU keeps `noo_sku_distributor_user`, keyed on the
same `distributor_code`, and no distributor master data is duplicated —
`DIST DATABASE` remains the authority on name, region and active status.
"""
from __future__ import annotations

from dataclasses import dataclass

import bcrypt

#: Handed out at account creation. Never stored as itself — only its hash.
DEFAULT_PASSWORD = "12345678"

MIN_PASSWORD_LENGTH = 8


def hash_password(password: str) -> str:
    """bcrypt hash, safe to store."""
    if not isinstance(password, str) or not password:
        raise ValueError("Password tidak boleh kosong.")
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode()


def verify_password(password: str, stored_hash: str) -> bool:
    """Constant-time check of a candidate password against a stored hash.

    Returns False rather than raising on a malformed or missing hash, so a bad
    row cannot crash the login screen or leak its shape through an error.
    """
    if not password or not stored_hash:
        return False
    try:
        return bcrypt.checkpw(password.encode("utf-8"),
                              str(stored_hash).encode("utf-8"))
    except (ValueError, TypeError):
        return False


def is_default_password(password: str) -> bool:
    return password == DEFAULT_PASSWORD


@dataclass(frozen=True)
class PasswordCheck:
    ok: bool
    message: str = ""


def validate_new_password(current, new, confirm, stored_hash) -> PasswordCheck:
    """Every rule for a self-service password change, in one place."""
    if not current:
        return PasswordCheck(False, "Password saat ini wajib diisi.")
    if not verify_password(current, stored_hash):
        return PasswordCheck(False, "Password saat ini salah.")
    if not new or not new.strip():
        return PasswordCheck(False, "Password baru tidak boleh kosong.")
    if new != confirm:
        return PasswordCheck(False,
                             "Konfirmasi password tidak sama dengan password baru.")
    if len(new) < MIN_PASSWORD_LENGTH:
        return PasswordCheck(
            False, f"Password baru minimal {MIN_PASSWORD_LENGTH} karakter.")
    if new == current:
        return PasswordCheck(False,
                             "Password baru harus berbeda dari password lama.")
    if is_default_password(new):
        return PasswordCheck(
            False, "Password baru tidak boleh sama dengan password default.")
    return PasswordCheck(True, "Password berhasil diperbarui.")


# ─── Session ──────────────────────────────────────────────────────────────────
SESSION_KEYS = ("auth", "distributor", "account", "must_change_password")


#: Never allowed into session state — Streamlit persists and serialises it.
_SENSITIVE_KEYS = ("password_hash", "password", "hash")


def establish_session(state, distributor, account):
    """Mark the session authenticated.

    The distributor code set here is the only identity the rest of the app
    reads. Credential material is stripped defensively rather than trusting
    every caller to hand over a pre-filtered dict.
    """
    state["auth"] = True
    state["distributor"] = distributor
    state["account"] = {k: v for k, v in dict(account).items()
                        if k not in _SENSITIVE_KEYS}
    state["must_change_password"] = bool(account.get("must_change_password"))


def clear_session(state):
    """Drop every authentication key. Used on logout."""
    for key in SESSION_KEYS:
        state.pop(key, None)


def is_authenticated(state) -> bool:
    return bool(state.get("auth")) and bool(state.get("distributor"))


def session_distributor_code(state):
    """The authoritative distributor code for this session.

    Every query and every written row derives from this, never from user input,
    a URL parameter, or anything in an uploaded file.
    """
    if not is_authenticated(state):
        return None
    return (state.get("distributor") or {}).get("distributor_code")


def assert_owns(state, distributor_code) -> bool:
    """Guard for any data path that names a distributor explicitly.

    Returns True only when the code matches the authenticated session, so a
    tampered widget value or query parameter cannot reach another DB's rows.
    """
    session_code = session_distributor_code(state)
    if not session_code:
        return False
    return str(distributor_code).strip().upper() == str(session_code).strip().upper()
