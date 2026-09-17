"""BigQuery access for PJP distributor accounts.

Every rule about *whether* a login or a write is allowed lives in `pjp_auth.py`;
this module only reads and writes rows. Split the same way `noo_sku/auth.py` and
`noo_sku/sources.py` are, so the rules stay unit-testable without credentials.

Two hard conventions here:

* **Nothing is cached.** `load_account()` is a point lookup on a ~130-row table
  and is called on every login attempt and immediately before every PJP write.
  Caching it would let an account that an admin has just disabled, or a deadline
  they have just pulled in, keep working for the length of the TTL. That is
  exactly the hole this project exists to close.
* **Every value is a query parameter.** No distributor code, username, hash or
  date is ever interpolated into SQL text.

Writes use `MERGE` / `UPDATE` DML rather than `load_table_from_dataframe`, so an
account can never end up with two rows for the same `distributor_code`, and so
rows are never parked in the streaming buffer where a follow-up `UPDATE` would
fail.
"""
from __future__ import annotations

from datetime import datetime

from google.cloud import bigquery

ACCOUNT_TABLE = "sfa_pjp_distributor_accounts"
AUDIT_TABLE = "sfa_pjp_distributor_account_audit"
DEFAULT_DATASET = "gt_schema"

# Audit actions.
ACTION_CREATE = "CREATE_ACCOUNT"
ACTION_CHANGE_PASSWORD = "CHANGE_PASSWORD"
ACTION_CHANGE_DEADLINE = "CHANGE_DEADLINE"
ACTION_CHANGE_USERNAME = "CHANGE_USERNAME"
ACTION_ACTIVATE = "ACTIVATE_ACCOUNT"
ACTION_DEACTIVATE = "DEACTIVATE_ACCOUNT"
ACTION_LOGIN = "LOGIN"

_ACCOUNT_FIELDS = (
    "distributor_code", "distributor_name", "username", "is_active",
    "input_deadline", "created_at", "updated_at", "created_by", "updated_by",
    "last_login_at", "password_changed_at",
)


def _norm(value) -> str:
    return "" if value is None else str(value).strip().upper()


def _client(credentials, project) -> bigquery.Client:
    return bigquery.Client(credentials=credentials, project=project)


def account_table(project, dataset=DEFAULT_DATASET) -> str:
    return f"`{project}.{dataset}.{ACCOUNT_TABLE}`"


def audit_table(project, dataset=DEFAULT_DATASET) -> str:
    return f"`{project}.{dataset}.{AUDIT_TABLE}`"


def _run(credentials, project, sql, params):
    job = _client(credentials, project).query(
        sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    return job.result()


def _row_to_account(row) -> dict:
    account = {name: row[name] for name in _ACCOUNT_FIELDS if name in row.keys()}
    account["distributor_code"] = _norm(account.get("distributor_code"))
    account["distributor_name"] = str(account.get("distributor_name") or "").strip()
    account["username"] = str(account.get("username") or "").strip()
    account["is_active"] = bool(account.get("is_active"))
    return account


# --- Reads -------------------------------------------------------------------

def load_account(credentials, project, distributor_code,
                 dataset=DEFAULT_DATASET, with_hash=True):
    """One account row, or None when no row exists for that code.

    Inactive and expired accounts are returned too - deciding what to do with
    them is `pjp_auth.evaluate_distributor_login` /
    `pjp_auth.validate_distributor_access`'s job, not this function's.

    The password hash is returned only when `with_hash` is set, so the admin
    dashboard and any display path cannot accidentally render it. It is never
    logged and never placed in session state.
    """
    code = _norm(distributor_code)
    if not code:
        return None
    hash_col = ", password_hash" if with_hash else ""
    sql = f"""
        SELECT distributor_code, distributor_name, username, is_active,
               input_deadline, created_at, updated_at, created_by, updated_by,
               last_login_at, password_changed_at{hash_col}
        FROM {account_table(project, dataset)}
        WHERE UPPER(TRIM(distributor_code)) = @code
        LIMIT 1
    """
    rows = list(_run(credentials, project, sql,
                     [bigquery.ScalarQueryParameter("code", "STRING", code)]))
    if not rows:
        return None
    account = _row_to_account(rows[0])
    if with_hash:
        account["password_hash"] = str(rows[0]["password_hash"] or "")
    return account


def load_all_accounts(credentials, project, dataset=DEFAULT_DATASET):
    """Every account, newest activity first, as a DataFrame.

    `password_hash` is deliberately absent from the projection so it cannot
    reach the admin table, an export, or a stack trace.
    """
    sql = f"""
        SELECT distributor_code, distributor_name, username, is_active,
               input_deadline, created_at, updated_at, created_by, updated_by,
               last_login_at, password_changed_at
        FROM {account_table(project, dataset)}
        ORDER BY distributor_code
    """
    return _client(credentials, project).query(sql).to_dataframe()


def account_exists(credentials, project, distributor_code,
                   dataset=DEFAULT_DATASET) -> bool:
    sql = f"""
        SELECT 1
        FROM {account_table(project, dataset)}
        WHERE UPPER(TRIM(distributor_code)) = @code
        LIMIT 1
    """
    return bool(list(_run(
        credentials, project, sql,
        [bigquery.ScalarQueryParameter("code", "STRING", _norm(distributor_code))])))


# --- Writes ------------------------------------------------------------------

def upsert_account(credentials, project, distributor_code, distributor_name,
                   username, password_hash, is_active, input_deadline,
                   actor, dataset=DEFAULT_DATASET) -> None:
    """Create the account, or update it in place if the code already exists.

    A MERGE on `distributor_code` rather than an append, so the table can never
    hold two rows for one distributor - which would make "which password is
    live?" ambiguous.

    `password_hash` may be None on an update, meaning "leave the stored password
    alone"; on an insert it is required.
    """
    code = _norm(distributor_code)
    if not code:
        raise ValueError("distributor_code wajib diisi.")
    deadline = input_deadline
    if isinstance(deadline, datetime):
        deadline = deadline.date()
    if password_hash is None and not account_exists(
            credentials, project, code, dataset):
        raise ValueError("Password wajib diisi untuk akun baru.")

    sql = f"""
        MERGE {account_table(project, dataset)} T
        USING (
            SELECT @code AS distributor_code,
                   @name AS distributor_name,
                   @username AS username,
                   @hash AS password_hash,
                   @is_active AS is_active,
                   @deadline AS input_deadline,
                   @actor AS actor
        ) S
        ON UPPER(TRIM(T.distributor_code)) = S.distributor_code
        WHEN MATCHED THEN UPDATE SET
            distributor_name    = S.distributor_name,
            username            = S.username,
            password_hash       = COALESCE(S.password_hash, T.password_hash),
            is_active           = S.is_active,
            input_deadline      = S.input_deadline,
            updated_at          = CURRENT_TIMESTAMP(),
            updated_by          = S.actor,
            password_changed_at = IF(S.password_hash IS NULL,
                                     T.password_changed_at, CURRENT_TIMESTAMP())
        WHEN NOT MATCHED THEN INSERT (
            distributor_code, distributor_name, username, password_hash,
            is_active, input_deadline, created_at, updated_at,
            created_by, updated_by, password_changed_at
        ) VALUES (
            S.distributor_code, S.distributor_name, S.username, S.password_hash,
            S.is_active, S.input_deadline, CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(), S.actor, S.actor, CURRENT_TIMESTAMP()
        )
    """
    _run(credentials, project, sql, [
        bigquery.ScalarQueryParameter("code", "STRING", code),
        bigquery.ScalarQueryParameter("name", "STRING",
                                      str(distributor_name or "").strip()),
        bigquery.ScalarQueryParameter("username", "STRING",
                                      str(username or "").strip()),
        bigquery.ScalarQueryParameter("hash", "STRING", password_hash),
        bigquery.ScalarQueryParameter("is_active", "BOOL", bool(is_active)),
        bigquery.ScalarQueryParameter("deadline", "DATE", deadline),
        bigquery.ScalarQueryParameter("actor", "STRING", str(actor or "").strip()),
    ])


#: Struct shape for the batched MERGE below. Declared once so the parameter
#: type and the SELECT list cannot drift apart.
_ACCOUNT_STRUCT = bigquery.StructQueryParameterType(
    bigquery.ScalarQueryParameterType("STRING", name="distributor_code"),
    bigquery.ScalarQueryParameterType("STRING", name="distributor_name"),
    bigquery.ScalarQueryParameterType("STRING", name="username"),
    bigquery.ScalarQueryParameterType("STRING", name="password_hash"),
    bigquery.ScalarQueryParameterType("BOOL", name="is_active"),
    bigquery.ScalarQueryParameterType("DATE", name="input_deadline"),
)


def load_existing_codes(credentials, project, dataset=DEFAULT_DATASET) -> set:
    """Every distributor_code that already has an account, in one query."""
    sql = f"SELECT UPPER(TRIM(distributor_code)) AS code FROM {account_table(project, dataset)}"
    return {r["code"] for r in _client(credentials, project).query(sql).result()
            if r["code"]}


def bulk_upsert_accounts(credentials, project, rows, actor,
                         dataset=DEFAULT_DATASET) -> int:
    """MERGE many accounts in a single statement.

    Used by the migration, which seeds ~130 accounts at once; doing that as one
    MERGE per account is ~130 round trips for no benefit. Same semantics as
    `upsert_account`, including "password_hash=None means keep what is stored".

    `rows` is an iterable of dicts with the keys named in `_ACCOUNT_STRUCT`.
    """
    payload = []
    for row in rows:
        deadline = row.get("input_deadline")
        if isinstance(deadline, datetime):
            deadline = deadline.date()
        payload.append(bigquery.StructQueryParameter(
            None,
            bigquery.ScalarQueryParameter(
                "distributor_code", "STRING", _norm(row.get("distributor_code"))),
            bigquery.ScalarQueryParameter(
                "distributor_name", "STRING",
                str(row.get("distributor_name") or "").strip()),
            bigquery.ScalarQueryParameter(
                "username", "STRING", str(row.get("username") or "").strip()),
            bigquery.ScalarQueryParameter(
                "password_hash", "STRING", row.get("password_hash")),
            bigquery.ScalarQueryParameter(
                "is_active", "BOOL", bool(row.get("is_active", True))),
            bigquery.ScalarQueryParameter("input_deadline", "DATE", deadline),
        ))
    if not payload:
        return 0

    sql = f"""
        MERGE {account_table(project, dataset)} T
        USING (SELECT * FROM UNNEST(@rows)) S
        ON UPPER(TRIM(T.distributor_code)) = S.distributor_code
        WHEN MATCHED THEN UPDATE SET
            distributor_name    = S.distributor_name,
            username            = S.username,
            password_hash       = COALESCE(S.password_hash, T.password_hash),
            is_active           = S.is_active,
            input_deadline      = S.input_deadline,
            updated_at          = CURRENT_TIMESTAMP(),
            updated_by          = @actor,
            password_changed_at = IF(S.password_hash IS NULL,
                                     T.password_changed_at, CURRENT_TIMESTAMP())
        WHEN NOT MATCHED THEN INSERT (
            distributor_code, distributor_name, username, password_hash,
            is_active, input_deadline, created_at, updated_at,
            created_by, updated_by, password_changed_at
        ) VALUES (
            S.distributor_code, S.distributor_name, S.username, S.password_hash,
            S.is_active, S.input_deadline, CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(), @actor, @actor, CURRENT_TIMESTAMP()
        )
    """
    _run(credentials, project, sql, [
        bigquery.ArrayQueryParameter("rows", _ACCOUNT_STRUCT, payload),
        bigquery.ScalarQueryParameter("actor", "STRING", str(actor or "").strip()),
    ])
    return len(payload)


def bulk_write_audit(credentials, project, entries, performed_by,
                     dataset=DEFAULT_DATASET) -> int:
    """Append many audit rows in one statement. See `write_audit` for the rules."""
    codes = [_norm(e[0]) for e in entries]
    actions = [str(e[1]) for e in entries]
    fields = [str(e[2] or "")[:2000] for e in entries]
    if not codes:
        return 0
    sql = f"""
        INSERT INTO {audit_table(project, dataset)}
            (audit_id, distributor_code, action, changed_fields,
             performed_by, performed_at)
        SELECT GENERATE_UUID(), code, action, fields, @actor, CURRENT_TIMESTAMP()
        FROM UNNEST(@codes) AS code WITH OFFSET i
        JOIN UNNEST(@actions) AS action WITH OFFSET j ON i = j
        JOIN UNNEST(@fields) AS fields WITH OFFSET k ON i = k
    """
    _run(credentials, project, sql, [
        bigquery.ArrayQueryParameter("codes", "STRING", codes),
        bigquery.ArrayQueryParameter("actions", "STRING", actions),
        bigquery.ArrayQueryParameter("fields", "STRING", fields),
        bigquery.ScalarQueryParameter("actor", "STRING",
                                      str(performed_by or "").strip()),
    ])
    return len(codes)


def load_hashes(credentials, project, dataset=DEFAULT_DATASET) -> dict:
    """``{distributor_code: password_hash}`` for every account, in one query.

    Only the migration's verification step uses this - it needs to check all
    128 legacy passwords against what was just written, and doing that one
    point-lookup at a time is 128 round trips. Nothing in the app calls it.
    """
    sql = f"""
        SELECT UPPER(TRIM(distributor_code)) AS code, password_hash
        FROM {account_table(project, dataset)}
    """
    return {r["code"]: str(r["password_hash"] or "")
            for r in _client(credentials, project).query(sql).result()}


def set_password(credentials, project, distributor_code, password_hash, actor,
                 dataset=DEFAULT_DATASET) -> None:
    sql = f"""
        UPDATE {account_table(project, dataset)}
        SET password_hash       = @hash,
            password_changed_at = CURRENT_TIMESTAMP(),
            updated_at          = CURRENT_TIMESTAMP(),
            updated_by          = @actor
        WHERE UPPER(TRIM(distributor_code)) = @code
    """
    _run(credentials, project, sql, [
        bigquery.ScalarQueryParameter("hash", "STRING", password_hash),
        bigquery.ScalarQueryParameter("actor", "STRING", str(actor or "").strip()),
        bigquery.ScalarQueryParameter("code", "STRING", _norm(distributor_code)),
    ])


def set_deadline(credentials, project, distributor_code, input_deadline, actor,
                 dataset=DEFAULT_DATASET) -> None:
    deadline = input_deadline
    if isinstance(deadline, datetime):
        deadline = deadline.date()
    sql = f"""
        UPDATE {account_table(project, dataset)}
        SET input_deadline = @deadline,
            updated_at     = CURRENT_TIMESTAMP(),
            updated_by     = @actor
        WHERE UPPER(TRIM(distributor_code)) = @code
    """
    _run(credentials, project, sql, [
        bigquery.ScalarQueryParameter("deadline", "DATE", deadline),
        bigquery.ScalarQueryParameter("actor", "STRING", str(actor or "").strip()),
        bigquery.ScalarQueryParameter("code", "STRING", _norm(distributor_code)),
    ])


def set_active(credentials, project, distributor_code, is_active, actor,
               dataset=DEFAULT_DATASET) -> None:
    """Enable or disable an account.

    This is the only "delete" the admin UI offers. Rows are never physically
    removed: an account carries the audit trail for every PJP snapshot that
    distributor has uploaded, and dropping it would make those unattributable.
    """
    sql = f"""
        UPDATE {account_table(project, dataset)}
        SET is_active  = @is_active,
            updated_at = CURRENT_TIMESTAMP(),
            updated_by = @actor
        WHERE UPPER(TRIM(distributor_code)) = @code
    """
    _run(credentials, project, sql, [
        bigquery.ScalarQueryParameter("is_active", "BOOL", bool(is_active)),
        bigquery.ScalarQueryParameter("actor", "STRING", str(actor or "").strip()),
        bigquery.ScalarQueryParameter("code", "STRING", _norm(distributor_code)),
    ])


def touch_last_login(credentials, project, distributor_code,
                     dataset=DEFAULT_DATASET) -> None:
    """Best-effort login stamp. Never blocks a successful login."""
    sql = f"""
        UPDATE {account_table(project, dataset)}
        SET last_login_at = CURRENT_TIMESTAMP()
        WHERE UPPER(TRIM(distributor_code)) = @code
    """
    _run(credentials, project, sql,
         [bigquery.ScalarQueryParameter("code", "STRING", _norm(distributor_code))])


# --- Audit -------------------------------------------------------------------

def write_audit(credentials, project, distributor_code, action, changed_fields,
                performed_by, dataset=DEFAULT_DATASET) -> None:
    """Append one audit row.

    `changed_fields` is a short human-readable summary of *what* changed - never
    a password, and never a hash. Password changes are recorded as the fact that
    one happened, nothing more.
    """
    sql = f"""
        INSERT INTO {audit_table(project, dataset)}
            (audit_id, distributor_code, action, changed_fields,
             performed_by, performed_at)
        VALUES
            (GENERATE_UUID(), @code, @action, @fields, @actor, CURRENT_TIMESTAMP())
    """
    _run(credentials, project, sql, [
        bigquery.ScalarQueryParameter("code", "STRING", _norm(distributor_code)),
        bigquery.ScalarQueryParameter("action", "STRING", str(action)),
        bigquery.ScalarQueryParameter("fields", "STRING",
                                      str(changed_fields or "")[:2000]),
        bigquery.ScalarQueryParameter("actor", "STRING",
                                      str(performed_by or "").strip()),
    ])


def load_audit(credentials, project, distributor_code=None, limit=200,
               dataset=DEFAULT_DATASET):
    """Recent audit rows, newest first. Optionally scoped to one distributor."""
    where = ""
    params = [bigquery.ScalarQueryParameter("lim", "INT64", int(limit))]
    if distributor_code:
        where = "WHERE UPPER(TRIM(distributor_code)) = @code"
        params.append(bigquery.ScalarQueryParameter(
            "code", "STRING", _norm(distributor_code)))
    sql = f"""
        SELECT performed_at, distributor_code, action, changed_fields, performed_by
        FROM {audit_table(project, dataset)}
        {where}
        ORDER BY performed_at DESC
        LIMIT @lim
    """
    return _client(credentials, project).query(
        sql, job_config=bigquery.QueryJobConfig(query_parameters=params)
    ).to_dataframe()
