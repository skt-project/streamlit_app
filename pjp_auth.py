"""Authentication and authorization for the Salesman & PJP Template Manager.

Pure functions only - no Streamlit, no network - so every rule here is unit
tested without credentials or a BigQuery connection. The database side lives
in `pjp_accounts.py`; the UI lives in `salesman_pjp.py`.

Why this app has its own credentials table rather than reusing an existing
one (`noo_sku_distributor_user`, `po_portal_distributor_users`):

* `po_portal_distributor_users` is username-keyed, has no deadline column,
  and is read directly by `po_portal_suggestion.py`, `_dev` and `_v2`.
* `noo_sku_distributor_user` is the closest analogue and this module
  deliberately mirrors its shape, but it carries NOO/SKU branch passwords for
  215 branches and has no `input_deadline`. Pointing PJP at it would both
  invalidate all 128 current PJP logins and push a PJP-only concept (the
  monthly input deadline) into another app's table.

So PJP keeps `sfa_pjp_distributor_accounts`, keyed on the same
`distributor_code`. No distributor master data is duplicated -
`gt_schema.master_distributor` remains the authority on name, region and
active status; this table only holds credentials, the account switch and the
deadline.
"""
from __future__ import annotations

import hmac
from dataclasses import dataclass
from datetime import date, datetime

import bcrypt

MIN_PASSWORD_LENGTH = 8

#: Rendered as "Expiring Soon" in the admin dashboard at or under this many days.
EXPIRY_WARNING_DAYS = 7

ROLE_ADMIN = "admin"
ROLE_DISTRIBUTOR = "distributor"

#: A valid bcrypt hash of a value nobody can supply. Verified against when no
#: account matches, so a missing account costs the same wall-clock time as a
#: wrong password and cannot be distinguished by timing.
_DUMMY_HASH = "$2b$12$C6UzMDM.H6dfI/f/IKcEe.7zVfHbbnLKu4dq/VoR/eSHcfPcPLLNC"


# --- Normalisation -----------------------------------------------------------

def norm_code(value) -> str:
    """Distributor codes are compared upper-cased and trimmed, everywhere."""
    if value is None:
        return ""
    return str(value).strip().upper()


def coerce_date(value):
    """Accept whatever BigQuery, pandas or a date_input hands back.

    Returns a plain ``datetime.date`` or None. Never raises - a malformed
    stored deadline must fail closed (see ``is_deadline_active``) rather than
    crash the login screen.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text or text.lower() in {"nat", "none", "nan", ""}:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text[:len(fmt) + 2].strip(), fmt).date()
        except ValueError:
            continue
    try:  # pandas Timestamp / numpy datetime64 and anything else date-like
        return value.to_pydatetime().date()
    except Exception:
        return None


# --- Password hashing --------------------------------------------------------

def hash_password(password: str) -> str:
    """bcrypt hash, safe to store. Never store the password itself."""
    if not isinstance(password, str) or not password:
        raise ValueError("Password tidak boleh kosong.")
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode()


#: A bcrypt hash is exactly 60 chars: "$2<variant>$<cost>$" + 53 of salt+digest.
_BCRYPT_HASH_LEN = 60


def looks_like_bcrypt(value) -> bool:
    """Cheap shape check, run before handing anything to bcrypt.

    This is not cosmetic. bcrypt 4.x is a Rust extension, and a truncated hash
    makes `checkpw` raise `pyo3_runtime.PanicException`, which inherits from
    BaseException - so `except Exception` does NOT catch it and one corrupted
    row would take the login screen down with a Rust panic traceback. Rejecting
    malformed hashes before the call is what makes `verify_password` total.
    """
    text = str(value or "")
    return (len(text) == _BCRYPT_HASH_LEN
            and text[0] == "$" and text[1] == "2"
            and text[3] == "$" and text[6] == "$")


def verify_password(password, stored_hash) -> bool:
    """Constant-time check of a candidate password against a stored hash.

    Returns False rather than raising on a malformed or missing hash, so a bad
    row cannot crash the login screen or leak its shape through an error.
    """
    if not password or not looks_like_bcrypt(stored_hash):
        return False
    try:
        return bcrypt.checkpw(str(password).encode("utf-8"),
                              str(stored_hash).encode("utf-8"))
    except (ValueError, TypeError):
        return False


@dataclass(frozen=True)
class PasswordCheck:
    ok: bool
    message: str = ""


def validate_new_password(new, confirm) -> PasswordCheck:
    """Every rule for an admin setting or resetting a distributor password."""
    if not new or not str(new).strip():
        return PasswordCheck(False, "Password baru tidak boleh kosong.")
    if new != confirm:
        return PasswordCheck(
            False, "Konfirmasi password tidak sama dengan password baru.")
    if len(new) < MIN_PASSWORD_LENGTH:
        return PasswordCheck(
            False, f"Password minimal {MIN_PASSWORD_LENGTH} karakter.")
    return PasswordCheck(True, "Password valid.")


# --- Deadline ----------------------------------------------------------------

def is_deadline_active(deadline, today=None) -> bool:
    """True while input is still open.

    The deadline is **inclusive**: a deadline of 2026-09-16 still allows input
    all of 2026-09-16 and locks from 2026-09-17. This preserves the behaviour
    of the global ``INPUT_DEADLINE`` gate this replaces.

    A missing or unparseable deadline fails **closed**. Every seeded and
    admin-created account has one, so a NULL here means the row is broken, and
    a broken row must not grant open-ended write access to production PJP data.
    """
    parsed = coerce_date(deadline)
    if parsed is None:
        return False
    return (today or date.today()) <= parsed


def days_remaining(deadline, today=None):
    """Days left including today, or None when no deadline is set.

    Deadline == today returns 0 (still open, last day). Negative once expired.
    """
    parsed = coerce_date(deadline)
    if parsed is None:
        return None
    return (parsed - (today or date.today())).days


STATUS_INACTIVE = "Inactive"
STATUS_EXPIRED = "Expired"
STATUS_EXPIRING = "Expiring Soon"
STATUS_ACTIVE = "Active"
STATUS_NO_DEADLINE = "No Deadline"


def deadline_status(account, today=None) -> str:
    """UI-only label for the admin dashboard. Never used to authorize."""
    if not account:
        return STATUS_INACTIVE
    if not account.get("is_active"):
        return STATUS_INACTIVE
    left = days_remaining(account.get("input_deadline"), today)
    if left is None:
        return STATUS_NO_DEADLINE
    if left < 0:
        return STATUS_EXPIRED
    if left <= EXPIRY_WARNING_DAYS:
        return STATUS_EXPIRING
    return STATUS_ACTIVE


# --- Login evaluation --------------------------------------------------------

LOGIN_OK = "ok"
LOGIN_INVALID = "invalid_credentials"
LOGIN_INACTIVE = "inactive"
LOGIN_EXPIRED = "expired"

#: One message for "no such account" and "wrong password" alike, so the login
#: screen never confirms which distributors have accounts.
MSG_INVALID = "Password salah. Silakan coba lagi."


@dataclass(frozen=True)
class LoginResult:
    ok: bool
    reason: str
    deadline: object = None


def account_username(account) -> str:
    """The username that logs in to this account.

    Accounts are seeded with ``username == distributor_code``. A row whose
    username was blanked falls back to the code, so an account can never become
    unreachable because of an empty field.
    """
    name = str((account or {}).get("username") or "").strip()
    return name or norm_code((account or {}).get("distributor_code"))


def username_matches(account, entered) -> bool:
    """Case- and whitespace-insensitive. The password is the secret, not this."""
    return (str(entered or "").strip().casefold()
            == account_username(account).casefold())


def evaluate_distributor_login(account, password, username=None,
                               today=None) -> LoginResult:
    """Decide a distributor login. `account` is a row dict or None.

    Credentials are checked **first**. Account status and deadline are only
    revealed once the password is right - otherwise the login screen would tell
    an unauthenticated visitor which distributors have accounts, which are
    disabled, and when each one's deadline falls.

    A wrong username and a wrong password are the same outcome, with the same
    message, so neither can be probed independently. `username=None` skips the
    username check (used by callers that identify the account another way).
    """
    if not account:
        verify_password(password or "x", _DUMMY_HASH)  # equalise timing
        return LoginResult(False, LOGIN_INVALID)
    if not verify_password(password, account.get("password_hash")):
        return LoginResult(False, LOGIN_INVALID)
    if username is not None and not username_matches(account, username):
        return LoginResult(False, LOGIN_INVALID)
    if not account.get("is_active"):
        return LoginResult(False, LOGIN_INACTIVE)
    deadline = coerce_date(account.get("input_deadline"))
    if not is_deadline_active(deadline, today):
        return LoginResult(False, LOGIN_EXPIRED, deadline)
    return LoginResult(True, LOGIN_OK, deadline)


# --- Access / write-path authorization ---------------------------------------

ACCESS_OK = "ok"
ACCESS_NO_SESSION = "no_session"
ACCESS_SCOPE_MISMATCH = "scope_mismatch"
ACCESS_NOT_FOUND = "not_found"
ACCESS_INACTIVE = "inactive"
ACCESS_EXPIRED = "expired"

_ACCESS_MESSAGES = {
    ACCESS_NO_SESSION: "Sesi Anda sudah berakhir. Silakan login kembali.",
    ACCESS_SCOPE_MISMATCH: "Unauthorized distributor access.",
    ACCESS_NOT_FOUND: "Akun distributor tidak ditemukan. Hubungi administrator.",
    ACCESS_INACTIVE: "Akun distributor ini saat ini tidak aktif. "
                     "Silakan hubungi administrator.",
    ACCESS_EXPIRED: "Periode input untuk distributor ini sudah ditutup.",
}


@dataclass(frozen=True)
class AccessResult:
    ok: bool
    reason: str
    message: str = ""
    deadline: object = None


def validate_distributor_access(account, session_code, requested_code,
                                today=None) -> AccessResult:
    """The single authorization rule, used by both the page gate and the
    write-path guard.

    `account` must be a **freshly read** row, not a cached or session-held one:
    an admin who disables an account or pulls a deadline in must take effect on
    the very next action of an already-open distributor session.

    Checks, in order: a session exists, the code being acted on is the session's
    own code, the account still exists, it is still active, its deadline has not
    passed.
    """
    session_code = norm_code(session_code)
    requested_code = norm_code(requested_code)
    if not session_code:
        return AccessResult(False, ACCESS_NO_SESSION,
                            _ACCESS_MESSAGES[ACCESS_NO_SESSION])
    if not requested_code or requested_code != session_code:
        return AccessResult(False, ACCESS_SCOPE_MISMATCH,
                            _ACCESS_MESSAGES[ACCESS_SCOPE_MISMATCH])
    if not account:
        return AccessResult(False, ACCESS_NOT_FOUND,
                            _ACCESS_MESSAGES[ACCESS_NOT_FOUND])
    if norm_code(account.get("distributor_code")) != session_code:
        return AccessResult(False, ACCESS_SCOPE_MISMATCH,
                            _ACCESS_MESSAGES[ACCESS_SCOPE_MISMATCH])
    if not account.get("is_active"):
        return AccessResult(False, ACCESS_INACTIVE,
                            _ACCESS_MESSAGES[ACCESS_INACTIVE])
    deadline = coerce_date(account.get("input_deadline"))
    if not is_deadline_active(deadline, today):
        return AccessResult(False, ACCESS_EXPIRED,
                            _ACCESS_MESSAGES[ACCESS_EXPIRED], deadline)
    return AccessResult(True, ACCESS_OK, "", deadline)


# --- Bulk deadline planning ---------------------------------------------------
# Pure: works on a list of account row dicts and answers "what would change?"
# before anything touches BigQuery. The admin sees exactly this, and the write
# path is handed only the codes that actually need updating.

BULK_TARGET_ALL_ACTIVE = "all_active"
BULK_TARGET_SELECTED = "selected"
BULK_TARGET_REGION = "region"


@dataclass(frozen=True)
class BulkPlan:
    """The preview and the work order, computed once and reused for both.

    `ok` means the request is well-formed, not that there is work to do: a valid
    request where every account already holds the target date is ok with an
    empty `changing`, which the UI reports as a no-op rather than writing.
    """
    ok: bool
    message: str = ""
    deadline: object = None
    changing: tuple = ()          # codes whose deadline actually differs
    unchanged: tuple = ()         # codes already on the target date
    current_distribution: tuple = ()   # ((date_str, count), ...) newest first
    skipped_inactive: tuple = ()

    @property
    def is_noop(self) -> bool:
        return self.ok and not self.changing

    @property
    def affected(self) -> int:
        return len(self.changing) + len(self.unchanged)


def plan_bulk_deadline(accounts, target_codes, new_deadline, today=None) -> BulkPlan:
    """Work out exactly which accounts a bulk deadline update would change.

    `accounts` is the full account list (row dicts with distributor_code,
    is_active, input_deadline). `target_codes` is the selected population.

    Inactive accounts are never silently included: a disabled account is
    disabled, and quietly moving its deadline would misrepresent what the admin
    asked for. They are reported in `skipped_inactive` instead.
    """
    parsed = coerce_date(new_deadline)
    if parsed is None:
        return BulkPlan(False, "Tanggal deadline baru wajib dipilih.")

    wanted = {norm_code(c) for c in (target_codes or []) if norm_code(c)}
    if not wanted:
        return BulkPlan(False, "Pilih minimal satu distributor.")

    by_code = {norm_code(a.get("distributor_code")): a for a in (accounts or [])}
    missing = sorted(c for c in wanted if c not in by_code)
    if missing:
        return BulkPlan(
            False,
            f"{len(missing)} kode tidak ditemukan pada tabel akun: "
            f"{', '.join(missing[:5])}{'...' if len(missing) > 5 else ''}")

    changing, unchanged, inactive = [], [], []
    counts = {}
    for code in sorted(wanted):
        row = by_code[code]
        if not row.get("is_active"):
            inactive.append(code)
            continue
        current = coerce_date(row.get("input_deadline"))
        key = current.isoformat() if current else "(kosong)"
        counts[key] = counts.get(key, 0) + 1
        (unchanged if current == parsed else changing).append(code)

    if not changing and not unchanged:
        return BulkPlan(
            False,
            "Semua distributor yang dipilih berstatus nonaktif; tidak ada yang diubah.")

    distribution = tuple(sorted(counts.items(), key=lambda kv: kv[0], reverse=True))
    return BulkPlan(True, "", parsed, tuple(changing), tuple(unchanged),
                    distribution, tuple(inactive))


def verify_bulk_result(before, after, deadline, changed_codes) -> tuple:
    """Post-write check: confirm ONLY the deadline moved, on exactly those rows.

    `before` / `after` are ``{code: row}`` snapshots including password_hash.
    Returns ``(ok, [problems])``. Anything that does not line up is a problem -
    a bulk write that half-applied must never be reported as success.
    """
    parsed = coerce_date(deadline)
    problems = []
    changed = {norm_code(c) for c in changed_codes}

    if set(before) != set(after):
        problems.append("jumlah baris akun berubah setelah update")

    for code, row_after in after.items():
        row_before = before.get(code)
        if row_before is None:
            continue
        if code in changed and coerce_date(row_after.get("input_deadline")) != parsed:
            problems.append(f"{code}: deadline tidak tersimpan")
        if code not in changed and (coerce_date(row_after.get("input_deadline"))
                                    != coerce_date(row_before.get("input_deadline"))):
            problems.append(f"{code}: deadline berubah padahal tidak dipilih")
        for field in ("password_hash", "is_active", "username", "distributor_code"):
            if row_before.get(field) != row_after.get(field):
                problems.append(f"{code}: {field} berubah (seharusnya tidak)")
    return (not problems), problems


# --- Admin credentials -------------------------------------------------------

def admin_users_from_config(admin_config) -> dict:
    """Normalise the `[admin]` secrets block into ``{username: password_hash}``.

    Accepts either a single admin::

        [admin]
        username = "admin"
        password_hash = "$2b$12$..."

    or several::

        [admin]
        users = [{username = "a", password_hash = "..."}, ...]
    """
    if not admin_config or not hasattr(admin_config, "get"):
        return {}
    users = {}
    for entry in (admin_config.get("users") or []):
        try:
            name = str(entry.get("username", "")).strip()
            hashed = str(entry.get("password_hash", "")).strip()
        except AttributeError:
            continue
        if name and hashed:
            users[name] = hashed
    name = str(admin_config.get("username", "")).strip()
    hashed = str(admin_config.get("password_hash", "")).strip()
    if name and hashed:
        users[name] = hashed
    return users


def verify_admin_credentials(username, password, admin_config) -> bool:
    """True only for a configured admin username with a matching bcrypt hash.

    Admin credentials live in deployment secrets, never in source. An unknown
    username still runs one bcrypt verification so it cannot be told apart from
    a known username with the wrong password.
    """
    users = admin_users_from_config(admin_config)
    candidate = str(username or "").strip()
    stored = None
    for name, hashed in users.items():
        if hmac.compare_digest(name, candidate):
            stored = hashed
            break
    if stored is None:
        verify_password(password or "x", _DUMMY_HASH)
        return False
    return verify_password(password, stored)


# --- Session -----------------------------------------------------------------

SESSION_KEYS = (
    "auth_role",
    "admin_authenticated",
    "admin_username",
    "distributor_authenticated",
    "authenticated_distributor_code",
    "authenticated_distributor_name",
)

#: Never allowed into session state - Streamlit persists and serialises it.
_SENSITIVE_KEYS = ("password_hash", "password", "hash")


def scrub(account) -> dict:
    """A row dict with every credential field removed."""
    return {k: v for k, v in dict(account or {}).items()
            if k not in _SENSITIVE_KEYS}


def establish_distributor_session(state, account) -> None:
    """Mark the session as one authenticated distributor.

    The code set here is the only distributor identity the rest of the app
    reads. It is never taken from a widget, a URL parameter or an uploaded file.
    """
    state["auth_role"] = ROLE_DISTRIBUTOR
    state["distributor_authenticated"] = True
    state["authenticated_distributor_code"] = norm_code(
        account.get("distributor_code"))
    state["authenticated_distributor_name"] = str(
        account.get("distributor_name") or "").strip()
    state.pop("admin_authenticated", None)
    state.pop("admin_username", None)


def establish_admin_session(state, username) -> None:
    state["auth_role"] = ROLE_ADMIN
    state["admin_authenticated"] = True
    state["admin_username"] = str(username or "").strip()
    state.pop("distributor_authenticated", None)
    state.pop("authenticated_distributor_code", None)
    state.pop("authenticated_distributor_name", None)


def clear_session(state) -> None:
    """Drop every authentication key. Used on logout."""
    for key in SESSION_KEYS:
        state.pop(key, None)


def current_role(state):
    role = state.get("auth_role")
    if role == ROLE_ADMIN and state.get("admin_authenticated"):
        return ROLE_ADMIN
    if role == ROLE_DISTRIBUTOR and state.get("distributor_authenticated"):
        return ROLE_DISTRIBUTOR
    return None


def is_admin_authenticated(state) -> bool:
    return current_role(state) == ROLE_ADMIN


def is_distributor_authenticated(state) -> bool:
    return (current_role(state) == ROLE_DISTRIBUTOR
            and bool(state.get("authenticated_distributor_code")))


def session_distributor_code(state):
    """The authoritative distributor code for this session.

    Every query and every written row derives from this, never from user input.
    """
    if not is_distributor_authenticated(state):
        return None
    return norm_code(state.get("authenticated_distributor_code"))


def assert_owns(state, distributor_code) -> bool:
    """Guard for any data path that names a distributor explicitly.

    True only when the code matches the authenticated session, so a tampered
    widget value or query parameter cannot reach another distributor's rows.
    """
    session_code = session_distributor_code(state)
    if not session_code:
        return False
    return norm_code(distributor_code) == session_code
