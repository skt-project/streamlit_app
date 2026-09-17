# Distributor Account Management — Salesman & PJP Template Manager

How login, account management and the input deadline work in `salesman_pjp.py`
after the September 2026 migration off hard-coded passwords.

**Who this is for:** the G2G admin who creates distributor accounts and sets
deadlines (sections 3–6), and the engineers who maintain or deploy the app
(everything else).

---

## 1. Architecture

```
Admin (secrets)                    Distributor (BigQuery account row)
      |                                        |
      v                                        v
 Admin Dashboard  ---- writes ---->  gt_schema.sfa_pjp_distributor_accounts
                                              |
                                     reads (never cached on the write path)
                                              v
                                     Distributor login
                                              v
                                 Authenticated distributor identity
                                   (one code, fixed for the session)
                                              v
                                     Existing PJP application
                                              v
                                assert_distributor_write_access()
                                              v
                                       BigQuery write
```

Three modules, split so every authorization rule is testable without a network
or a Streamlit runtime:

| File | Role |
|---|---|
| `pjp_auth.py` | **Pure rules.** Hashing, deadline arithmetic, login decisions, scope checks, session shape. No Streamlit, no BigQuery, no I/O. Fully unit tested. |
| `pjp_accounts.py` | **BigQuery only.** Reads and writes account and audit rows with parameterised queries. Holds no policy. |
| `salesman_pjp.py` | **UI and wiring.** Login page, admin dashboard, page gate, write-path guards. |

This mirrors the existing `noo_sku/auth.py` + `noo_sku/sources.py` split in this
same repo.

### Why a new table

`gt_schema.sfa_pjp_distributor_accounts` is new rather than a reuse of:

* `po_portal_distributor_users` — username-keyed, no deadline column, and read
  directly by `po_portal_suggestion.py`, `_dev` and `_v2`.
* `noo_sku_distributor_user` — the closest analogue, and the shape this table
  copies, but it carries NOO/SKU passwords for 215 branches and has no
  `input_deadline`. Pointing PJP at it would invalidate all 128 PJP logins and
  put a PJP-only concept into another app's table.

No distributor master data is duplicated. `gt_schema.master_distributor` remains
the authority on which distributors exist, their name, region and Active status;
the account table only says *who may log in, until when, and with what password*.

---

## 2. BigQuery tables

DDL: [`sql/sfa_pjp_distributor_accounts.sql`](../sql/sfa_pjp_distributor_accounts.sql).
Both statements are `CREATE TABLE IF NOT EXISTS`; re-running them is a no-op.

```sql
CREATE TABLE IF NOT EXISTS
`skintific-data-warehouse.gt_schema.sfa_pjp_distributor_accounts`
(
  distributor_code    STRING NOT NULL,  -- key, upper-case, one row per distributor
  distributor_name    STRING,
  username            STRING,           -- seeded equal to distributor_code
  password_hash       STRING,           -- bcrypt, never plaintext
  is_active           BOOL,             -- FALSE blocks login and every write
  input_deadline      DATE,             -- inclusive; NULL fails closed
  created_at          TIMESTAMP,
  updated_at          TIMESTAMP,
  created_by          STRING,
  updated_by          STRING,
  last_login_at       TIMESTAMP,
  password_changed_at TIMESTAMP
);
```

```sql
CREATE TABLE IF NOT EXISTS
`skintific-data-warehouse.gt_schema.sfa_pjp_distributor_account_audit`
(
  audit_id         STRING NOT NULL,   -- GENERATE_UUID()
  distributor_code STRING,
  action           STRING,            -- CREATE_ACCOUNT | CHANGE_PASSWORD | ...
  changed_fields   STRING,            -- summary only, never a password or hash
  performed_by     STRING,
  performed_at     TIMESTAMP
)
PARTITION BY DATE(performed_at);
```

Audit actions: `CREATE_ACCOUNT`, `CHANGE_PASSWORD`, `CHANGE_DEADLINE`,
`CHANGE_USERNAME`, `ACTIVATE_ACCOUNT`, `DEACTIVATE_ACCOUNT`, `MIGRATE_ACCOUNT`.

All account writes go through a `MERGE` on `distributor_code`, so the table can
never hold two rows for one distributor.

---

## 3. Admin login

Admin credentials are **never** in Python source and never committed. They come
from deployment secrets:

```toml
[admin]
username = "admin"
password_hash = "$2b$12$..."
```

Several admins:

```toml
[admin]
users = [
  { username = "ops.admin",  password_hash = "$2b$12$..." },
  { username = "data.admin", password_hash = "$2b$12$..." },
]
```

Generate or rotate a hash without ever writing the password down:

```bash
python scripts/set_pjp_admin_password.py --username admin
```

It prompts with `getpass` and prints only the TOML block to paste.

If no `[admin]` block is configured, there are no admin users and every admin
login fails — it fails closed, it does not fall open.

**Log in:** open the app, choose the **🔐 Login Admin** tab, enter the username
and password.

---

## 4. Distributor login

**Landing page → 🏢 Login Distributor tab:**

1. Select your distributor (list comes from `master_distributor`, Active only).
2. Enter username (default: your distributor code) and password.
3. Press **🔓 Masuk**.

Internally, on submit:

```
read account fresh from BigQuery   (never cached)
        ↓
verify bcrypt password             → wrong: "Password salah."
        ↓
verify username                    → wrong: same message, deliberately
        ↓
is_active?                         → no:  "Akun Tidak Aktif"
        ↓
today <= input_deadline?           → no:  "Periode Input Sudah Ditutup"
        ↓
establish session, stamp last_login_at, open the PJP app
```

The password is checked **before** account status is revealed. A visitor who
does not know the password cannot learn which distributors have accounts, which
are disabled, or when anyone's deadline falls — a wrong password, a wrong
username and a non-existent account all give the identical message.

**After login there is no distributor selector.** The sidebar shows the one
authenticated distributor and a **🚪 Logout** button.

---

## 5. Managing accounts (admin)

The dashboard has five tabs.

### 📋 Daftar Akun
Every account with status, deadline, deadline status and days remaining, plus
search and a status filter. Passwords and password hashes are never shown — the
query that builds this list does not even select `password_hash`.

Deadline statuses are display labels computed from today's date, never stored:

| Label | Meaning |
|---|---|
| `Active` | enabled, more than 7 days left |
| `Expiring Soon` | enabled, 0–7 days left |
| `Expired` | enabled, deadline has passed |
| `Inactive` | `is_active = FALSE` |
| `No Deadline` | `input_deadline` is NULL — **login is refused** |

### ➕ Tambah Distributor
Distributor, Username, Password, Confirm Password, Input Deadline, Active.

The distributor dropdown lists only distributors that are Active in
`master_distributor` **and** do not already have an account, which is what
prevents a duplicate account alongside the `MERGE` key.

Minimum password length is 8 characters.

### ✏️ Edit Akun
Change Username, Deadline and Active status. The two password boxes are
optional: **leave them empty to change only the deadline or status** — the
stored password is untouched. Filling them requires New Password + Confirm.

### 🗓️ Setting Deadline (bulk)

Moves the input deadline on many accounts at once. Extending the input period is
a near-daily act; before this it meant editing a global constant and pushing a
commit, and after the account migration it would have meant opening 128 accounts
one at a time.

**Target** - pick one:

| Option | Resolves to |
|---|---|
| Semua Akun Aktif | every account with `is_active = TRUE`, counted live (never hard-coded) |
| Pilih Distributor | multi-select with type-to-search, plus select-all / clear |
| Per Region | `region_g2g` from `master_distributor` - the grouping the app already uses, not a new one |

**Deadline Baru** has **no default**. The admin must pick a date; nothing happens
until they do. An optional Reason is recorded in the audit trail.

**Preview comes before any write** and shows how many accounts are affected, how
many actually change, how many are already on the target date, and the current
deadline distribution. Inactive accounts in the selection are listed as skipped
rather than silently updated.

Executing needs a ticked confirmation naming the count and the date. The write:

1. `pjp_accounts.bulk_set_deadline` - one parameterised `UPDATE ... WHERE
   distributor_code IN UNNEST(@codes)`. One statement, so BigQuery applies all
   of it or none; and the `SET` clause names only `input_deadline`,
   `updated_at`, `updated_by`. `password_hash` is not in it.
2. Re-reads the rows and runs `pjp_auth.verify_bulk_result`, which confirms the
   deadline moved on exactly the selected rows and that `password_hash`,
   `is_active`, `username` and `distributor_code` did not change anywhere.
3. Only then reports success. A mismatch, or a row count other than expected,
   reports failure explicitly rather than a partial success.
4. Writes one `CHANGE_DEADLINE` audit row per **changed** account, tagged
   `bulk_action: true` with the batch size and any reason. Accounts already on
   the target date get no audit row.
5. Clears both account caches, so the next read is the new value.

`require_admin()` is checked on entry **and** again inside the submit branch, so
the operation cannot be driven by anything other than an authenticated admin
session. Individual **Edit Akun** is unchanged and remains the path for
per-distributor exceptions.

### 🔁 Aktif/Nonaktif
Deactivate and reactivate. **Accounts are never physically deleted.** An account
carries the audit trail for every PJP snapshot that distributor uploaded;
deleting it would make those unattributable. Deactivating blocks login and every
PJP write immediately.

### 🧾 Audit
The last 300 account actions. Contains no passwords and no hashes.

---

## 6. Deadlines

Per distributor, not global. The deadline is **inclusive**:

| `input_deadline` | 2026-09-16 | 2026-09-17 |
|---|---|---|
| 2026-09-16 | allowed | blocked |

This is the same rule the old global `INPUT_DEADLINE` gate used (`today >
input_deadline` locks).

A **NULL deadline fails closed** — login and writes are refused. A row without a
deadline is a broken row, and a broken row must not grant open-ended write access
to production PJP data. Every seeded and admin-created account has one.

Changes apply immediately: the login path and the write-path guard both read the
account fresh from BigQuery, and saving in the dashboard clears the 30-second
page-gate cache.

---

## 7. Security model

### Password storage
`bcrypt` (`bcrypt.hashpw` / `bcrypt.checkpw`) with a per-password salt. No
plaintext password is ever written to BigQuery, to a log, to session state or to
an audit row. There is no custom cryptography anywhere.

`verify_password` rejects a malformed hash *before* calling bcrypt. This is not
cosmetic: bcrypt 4.x is a Rust extension and a truncated hash makes `checkpw`
raise `pyo3_runtime.PanicException`, which inherits from `BaseException` — so
`except Exception` does **not** catch it, and one corrupted row would take the
login screen down with a Rust panic traceback.

### Session
One authoritative identity per session:

```python
st.session_state["auth_role"]                        # "admin" | "distributor"
st.session_state["admin_authenticated"]              # admin only
st.session_state["admin_username"]                   # admin only
st.session_state["distributor_authenticated"]        # distributor only
st.session_state["authenticated_distributor_code"]   # the scope
st.session_state["authenticated_distributor_name"]
```

The old `auth_{dist_code}` flags are gone. They allowed several distributors to
be unlocked in one session, which is what made sidebar-switching a scope escape.

A role string alone does not authenticate: `current_role()` requires
`auth_role` **and** the matching boolean, and a distributor session additionally
requires a non-empty code. Logging in as one role clears the other. No password
hash ever enters session state (`pjp_auth.scrub`).

### Distributor scope
The authenticated code is the only scope. A distributor user has no distributor
selector at all, and `validate_distributor_access` independently re-checks that
the code being acted on equals the session code — so a tampered widget value,
query parameter or forged session key cannot reach another distributor's rows.

### Write-path authorization
Every distributor-facing BigQuery write calls
`assert_distributor_write_access(code)` immediately before executing, which
**re-reads the account from BigQuery** and re-checks session, scope, existence,
active status and deadline. Passing the login gate earlier grants nothing.

This matters most on the PJP update, which deletes the current month's snapshot
before inserting the replacement. An admin may disable an account or pull a
deadline in while the user sits on the confirmation screen; the guard is what
stops the DELETE running on a stale authorization.

Guarded write paths in `salesman_pjp.py`:

| Guarded call | Covers |
|---|---|
| PJP update | `delete_pjp_records()` → `push_to_bigquery()` |
| Edit salesman | `update_salesman_record()` + the inline `gt_salesman_mapping` UPDATE |
| Replace salesman | `insert_salesman_record()`, `deactivate_previous_mapping()`, `insert_mapping_record()` |
| Deactivate salesman | `deactivate_salesman_mapping()` |
| Add salesman | `insert_salesman_record()`, `insert_mapping_record()` |

Admin writes are separately guarded by `require_admin()`, checked again inside
each submit handler rather than relying on the tab being visible.

### Caching
| Read | Cached? | Why |
|---|---|---|
| Login / write-path account read | **No** | A cached credential is a credential that keeps working after revocation. |
| Page-gate account read | 30s TTL | Keeps ordinary widget clicks off BigQuery. Worst case: a just-disabled account browses read-only screens for ≤30s. It can never write — the write path is uncached. |
| Admin account list | 15s TTL | Display only; the query does not select `password_hash`. |

Every admin change calls `_refresh_accounts()`, which clears both caches.
Streamlit's `cache_data` is process-wide, so this reaches open distributor
sessions too.

### Error handling
Users see plain Indonesian messages. BigQuery errors, SQL, table names, service
account paths, stack traces and password hashes are never rendered — technical
detail goes to stderr for the deployment logs. A BigQuery outage makes
`fetch_account` return `None`, which the caller treats as *no access*: it fails
closed.

---

## 8. Migration from hard-coded passwords

`salesman_pjp.py` used to hold `DISTRIBUTOR_PASSWORDS` — 128 plaintext
`DSTxxx: password` pairs — and `INPUT_DEADLINE = datetime(2026, 9, 11).date()`.

Migration performed 2026-09-16 with
[`scripts/seed_pjp_distributor_accounts.py`](../scripts/seed_pjp_distributor_accounts.py):

1. Read the dict out of the source by AST (nothing in the module executes).
2. bcrypt-hash each password **in memory** — no plaintext touches disk.
3. `MERGE` 128 rows in one statement: `username = distributor_code`,
   `is_active = TRUE`, `input_deadline = 2026-09-11`.
4. Re-read every hash and verify the original password against it: **128/128
   passed**.
5. Only then were `DISTRIBUTOR_PASSWORDS`, `INPUT_DEADLINE`,
   `_get_password_for_distributor`, `_check_distributor_auth` and
   `_render_password_gate` deleted.

`tests/test_pjp_distributor_codes.py` pins that they stayed deleted, so the dict
cannot come back as a second way in.

### Migration assumptions — read these

* **Every account was seeded with `input_deadline = 2026-09-11`**, the old global
  constant. There was no per-distributor deadline anywhere to recover. Because
  that date is already past, **every distributor is locked out until an admin
  sets a new deadline** — which reproduces the app's behaviour on the day of the
  cutover exactly (it had been locked for everyone since 2026-09-12). Open the
  deadlines from the dashboard.
* Passwords were carried over unchanged, so no distributor needs to be told
  anything.
* Usernames were seeded to the distributor code.
* 5 codes had a password but are not Active in `master_distributor`
  (`DST141`, `DST268`, `DST269`, `DST324`, `DST329`). Accounts were created so
  nothing was silently dropped, but they are not selectable on the login page —
  the same as before the migration.
* The app's distributor list resolves to **123** codes, not the 218 Active rows
  in `master_distributor`. See §13 for the full reconciliation. All 123 have an
  account.

Re-running the script is safe: it updates in place and, by default, leaves the
password of an account that already exists alone, so a password the admin has
since changed is not reverted.

---

## 13. Distributor population — reconciliation

Four different counts get quoted for "how many distributors are there". They are
all correct; they count different things.

| Population | Source | Count | Business rule | Evidence |
|---|---|---:|---|---|
| Active in master | `master_distributor` where `status='Active'` | **218** | every onboarded distributor, PJP or not | live query |
| **PJP-selectable** | the same, **and** `TRIM(region_g2g) != ''` | **123** | what the login dropdown offers | `distributor_naming.py` `CANONICAL_DIST_CTE` line 121 |
| Active but blank `region_g2g` | difference of the two above | **95** | excluded from PJP | 218 − 123 |
| PJP accounts | `sfa_pjp_distributor_accounts` | **128** | 123 selectable + 5 legacy | live query |
| Accounts not selectable | accounts whose code is not in the 123 | **5** | `DST141`, `DST268`, `DST269`, `DST324`, `DST329` — all `status='Inactive'` in master | live query |

### Is the 123-vs-218 rule intentional?

**Yes, and it pre-dates this work.** The rule is
`status = 'Active' AND region_g2g != ''`, and it has been the app's distributor
query since **2026-05-18** (commit `f672cd1`), long before the authentication
migration:

```sql
-- salesman_pjp.py, as of f672cd1 (2026-05-18)
FROM `gt_schema.master_distributor`
WHERE region_g2g != '' AND status = 'Active'
```

Commit `acd59dd` (2026-09-14) moved it into the shared
`distributor_naming.CANONICAL_DIST_CTE` without changing its meaning. The
authentication work did not touch it, and must not: `region_g2g` is the G2G org
mapping, so a distributor with no G2G region has no ASM, no region and nothing
to plan a journey against.

**What this means operationally:** a distributor with a blank `region_g2g` cannot
be given a PJP account, because the admin's "Tambah Distributor" dropdown is
built from the same 123. The fix is to populate `region_g2g` in
`master_distributor` — a master-data action, not an app change. Creating an
account for such a code directly in BigQuery would not help; the login dropdown
would still not list it.

**Open business question (not a code question):** whether any of the 95
blank-`region_g2g` Active distributors are *supposed* to submit PJP. That cannot
be answered from the code — it is a master-data completeness question for the
G2G team. Nothing here assumes an answer either way.

### Deadline clock

The deadline compares `datetime.now().date()` — the **server's local date**. This
is unchanged from the old global `INPUT_DEADLINE` gate, and it is the same clock
`snapshot_month` uses, so the deadline and the month a submission lands in can
never disagree.

Confirm the deployment's timezone before go-live. On Streamlit Community Cloud
the container runs **UTC**, which is WIB−7: a deadline of 2026-09-30 would then
stop accepting input at **07:00 WIB on 2026-10-01**, not at local midnight. That
is a 7-hour grace period, not a lockout, so it is safe either way — but the admin
setting deadlines should know which one applies.

---

## 9. Deployment

**Dependencies:** none added. `bcrypt` was already in `requirements.txt`.

**Secrets:** add the `[admin]` block (section 3) to
`.streamlit/secrets.toml` locally — already gitignored — or to the Streamlit
Cloud app's *Secrets* setting. The existing `[connections.bigquery]` and
`[bigquery]` blocks are unchanged; `[bigquery].dataset` selects the dataset and
defaults to `gt_schema`.

**BigQuery:** apply the DDL once per environment:

```bash
bq query --use_legacy_sql=false --project_id=skintific-data-warehouse \
         < sql/sfa_pjp_distributor_accounts.sql
```

**Service account:** the app's existing service account needs
`bigquery.tables.updateData` on `gt_schema` (it already writes
`gt_master_salesman_pjp`, `gt_master_salesman` and `gt_salesman_mapping`).

**Never commit:** `secrets.toml`, service-account JSON, private keys, admin
credentials.

---

## 9a. Go-live runbook (Streamlit Community Cloud)

This app deploys from GitHub `main` to Streamlit Community Cloud. There is no
`deploy/` folder and no Cloud Run service for it; pushing `main` triggers the
redeploy. That is also why the old deadline was changed by editing a constant
and pushing - a lever this release removes.

**The order matters.** Adding the secret first is harmless to the running app
(the old code ignores an unknown `[admin]` section); merging first is not, since
it would leave an admin dashboard nobody can log into and no other way to reopen
input.

### Step 1 - add the `[admin]` secret (human, ~1 minute)

1. Open <https://share.streamlit.io> and sign in.
2. Find the `salesman_pjp` app -> **⋮** -> **Settings** -> **Secrets**.
3. Append the `[admin]` block from the local `.streamlit/secrets.toml`
   (username + bcrypt `password_hash`; the plaintext is not stored anywhere in
   the repo). Leave `[connections.bigquery]` and `[bigquery]` untouched.
4. **Save**. The app restarts; the old code is unaffected.

### Step 2 - merge and deploy

```bash
git checkout main && git pull
git merge migration/cloud-run       # resolves clean; branch already merged main
git push origin main                # triggers the Streamlit Cloud redeploy
```

### Step 3 - verify the deployment

1. Open the app. The landing page must show two tabs: **🏢 Login Distributor**
   and **🔐 Login Admin**.
2. Admin tab -> log in. Anything other than the dashboard means Step 1 did not
   take: *"Akses admin belum dikonfigurasi pada deployment ini."* means the
   secret is missing or malformed.
3. **📋 Daftar Akun** must list 128 accounts. The `tanggal sistem` line is
   today's date, not a deadline - the deadline is the column in the table.
4. Set the intended `input_deadline` per distributor in **✏️ Edit Akun**.
   Nobody can log in until a deadline is in the future (or today).
5. Distributor tab -> log in as one distributor and confirm the sidebar is
   pinned to that distributor with no selector.

### Rollback

```bash
git revert -m 1 <merge-commit> && git push origin main
```

The account tables are additive and are not touched by a rollback; the legacy
`DISTRIBUTOR_PASSWORDS`/`INPUT_DEADLINE` code returns with it. Rolling back does
**not** delete accounts, so rolling forward again needs no re-seeding.

### Deadline clock on Cloud

Community Cloud containers run **UTC**, and the gate compares
`datetime.now().date()` - so a deadline of `2026-09-16` stops accepting input at
**07:00 WIB on 2026-09-17**, not local midnight. Production `main` used the same
expression, so this release changes nothing; set deadlines knowing the effective
cutoff is 07:00 WIB the following morning.

---

## 10. Testing

```bash
python -m pytest tests/test_pjp_auth.py -q             # 81 auth/authz tests
python -m pytest tests/test_pjp_distributor_codes.py -q
python -m pytest tests/ -q                             # full suite
```

`tests/test_pjp_auth.py` covers the whole rule set against an in-memory account
store — no BigQuery, no Streamlit, no credentials. Tests numbered 1–10 map onto
the security review: wrong password, correct password, inactive account, expired
deadline, deadline today, deadline yesterday, scope escape, password change,
deadline change, and deactivation mid-session blocking a write.

Two tests are worth knowing about:

* `test_10_deactivation_blocks_an_already_open_session_before_the_write` asserts
  the account was read **twice**. Reusing the row captured at login would leave
  the count at 1 and let the write through — that assertion is the whole point.
* `test_status_is_not_revealed_before_the_password_is_right` pins that an
  inactive or expired account with a wrong password looks identical to any other
  wrong password.

---

## 11. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| "Akses admin belum dikonfigurasi pada deployment ini." | No `[admin]` block in secrets | Add it (section 3) and restart the app |
| "Password salah." with a password you believe is right | Wrong password, wrong username, or no account for that distributor — all one message by design | Check the username in **Daftar Akun**; reset the password in **Edit Akun** |
| "Akun Tidak Aktif" | `is_active = FALSE` | Reactivate in **🔁 Aktif/Nonaktif** |
| "Periode Input Sudah Ditutup" | `today > input_deadline` | Move the deadline in **Edit Akun** |
| Every distributor is locked out right after the migration | Expected — seeded deadline is the old `INPUT_DEADLINE` (2026-09-11), already past | Set new deadlines in the dashboard |
| A distributor sees "Distributor Tidak Aktif" | Has an account but is not in the app's distributor list | Check `status = 'Active'` and `region_g2g` in `master_distributor` |
| Admin change not visible in an open distributor session | Page-gate cache, ≤30s | Wait, or have them reload. Writes are never affected. |
| "Gagal memuat data distributor dari Database." | BigQuery unreachable or credentials wrong | Check the deployment logs — details go to stderr, not the screen |

---

## 12. Known limitations

* **No rate limiting or lockout** on repeated failed logins. bcrypt's cost makes
  brute force slow, but nothing counts attempts. Streamlit Community Cloud gives
  no per-IP hook to build this on.
* **No distributor self-service password change.** Only an admin can reset a
  password. `noo_sku_mapping.py` has a self-service flow if this is wanted later.
* **Admin credentials are not in BigQuery**, so adding an admin means a secrets
  change and an app restart — deliberate, to keep an app-managed table from being
  able to mint admins.
* **The PJP update is still not atomic.** The DELETE and the INSERT are separate
  statements; if the insert fails after the delete, the current month is empty
  and the app says so. That pre-existing behaviour was explicitly out of scope
  here, and the guard now at least ensures an unauthorized user never starts it.
* **Account deactivation does not end an open session.** The user's next action
  is refused, but the browser tab stays on whatever it last rendered.

### Follow-up security item (separate from this app)

`verify_password` here guards the hash shape before calling bcrypt, because
bcrypt 4.x raises `pyo3_runtime.PanicException` — a `BaseException` — on a
malformed hash, which `except Exception` does not catch. **The same unguarded
pattern remains in `noo_sku/auth.py` and `po_portal/utils/auth.py`.** Those are
different applications and were deliberately left alone; a corrupted
`password_hash` row in either would crash their login screen. Tracked here as a
follow-up, not a blocker for PJP.
