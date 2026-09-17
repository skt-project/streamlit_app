-- Distributor account configuration for the Salesman & PJP Template Manager
-- (salesman_pjp.py). This table is the authoritative source of distributor
-- credentials, account status and the per-distributor input deadline; it
-- replaces the hard-coded DISTRIBUTOR_PASSWORDS dict and the global
-- INPUT_DEADLINE constant that used to live in the Python source.
--
-- Apply with:
--   bq query --use_legacy_sql=false --project_id=skintific-data-warehouse \
--            < sql/sfa_pjp_distributor_accounts.sql
--
-- Both statements are CREATE TABLE IF NOT EXISTS: re-running them is a no-op
-- and never drops or rewrites existing rows.

CREATE TABLE IF NOT EXISTS
`skintific-data-warehouse.gt_schema.sfa_pjp_distributor_accounts`
(
  -- The account key. Matches gt_schema.master_distributor.distributor_code,
  -- which stays the authority on distributor name, region and Active status;
  -- this table only holds credentials, the switch and the deadline.
  distributor_code    STRING NOT NULL OPTIONS(description="Distributor code, e.g. DST171. Upper-case. One row per distributor."),
  distributor_name    STRING OPTIONS(description="Display label copied from master_distributor at creation time. Informational only."),
  username            STRING OPTIONS(description="Login username. Seeded equal to distributor_code."),
  -- bcrypt. Never plaintext, never a home-grown hash, never rendered in the UI.
  password_hash       STRING OPTIONS(description="bcrypt hash of the distributor password. Never stored or logged in plaintext."),
  is_active           BOOL OPTIONS(description="FALSE disables login and every PJP write. Accounts are disabled, never deleted."),
  -- Inclusive: input is still allowed all of input_deadline and locks the
  -- following day. A NULL here fails closed - login and writes are refused.
  input_deadline      DATE OPTIONS(description="Last day this distributor may submit PJP, inclusive. NULL fails closed."),
  created_at          TIMESTAMP,
  updated_at          TIMESTAMP,
  created_by          STRING,
  updated_by          STRING,
  last_login_at       TIMESTAMP,
  password_changed_at TIMESTAMP
)
OPTIONS(
  description="Distributor accounts for the Salesman & PJP Template Manager. Authoritative source of PJP credentials, account status and per-distributor input deadline. Written by the app's Admin dashboard via MERGE on distributor_code."
);


CREATE TABLE IF NOT EXISTS
`skintific-data-warehouse.gt_schema.sfa_pjp_distributor_account_audit`
(
  audit_id         STRING NOT NULL OPTIONS(description="GENERATE_UUID() at insert."),
  distributor_code STRING OPTIONS(description="Account acted on."),
  action           STRING OPTIONS(description="CREATE_ACCOUNT | CHANGE_PASSWORD | CHANGE_DEADLINE | CHANGE_USERNAME | ACTIVATE_ACCOUNT | DEACTIVATE_ACCOUNT | LOGIN"),
  changed_fields   STRING OPTIONS(description="Human-readable summary of what changed. Never contains a password or a hash."),
  performed_by     STRING OPTIONS(description="Admin username from the authenticated session, or 'migration' for the seeding script."),
  performed_at     TIMESTAMP
)
PARTITION BY DATE(performed_at)
OPTIONS(
  description="Append-only audit trail for sfa_pjp_distributor_accounts. Never contains plaintext passwords or password hashes."
);
