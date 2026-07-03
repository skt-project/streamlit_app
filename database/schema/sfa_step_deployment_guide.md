# sfa_step Deployment Guide

**From zero to a running, scheduled sync.** Read [`sfa_step_architecture.md`](sfa_step_architecture.md) first if you haven't — this guide assumes you already know *why* each table/procedure exists; it only covers *how to actually stand it up*.

Nothing in this schema has been created yet. The investigation and script-writing that produced the other 3 files used a **read-only** service account (`BigQuery Data Viewer` + `BigQuery Job User`) — it cannot create a dataset, table, or run a procedure even by accident. Every step below needs a different, more privileged identity, called out explicitly.

---

## Phase 0 — Pre-flight validation (do this before creating anything)

Two assumptions in the design were flagged as **assumed, not confirmed** — resolve them first, because they change what "successful load" means.

**0.1 — Is `sadata.fact_ba_attendance_t` still being fed?**
```sql
SELECT MAX(date) AS latest_date, COUNT(*) AS total_rows
FROM `skintific-data-warehouse.sadata.fact_ba_attendance_t`;
```
At design time this returned `2025-08-07`. If it's still `2025-08-07` (or close to it) when you run this, the pipeline feeding it has likely paused — talk to the `sadata` app owner before turning on a *daily* schedule for `sp_refresh_fact_visit_sadata`; there's no point running a daily job against a source that hasn't moved in months. If it's advanced close to today, proceed as designed.

**0.2 — Does `kode_toko` (PJP) actually join to `cust_id` (store master)?**
```sql
SELECT
  COUNT(*) AS total_pjp_rows,
  COUNTIF(s.cust_id IS NOT NULL) AS matched_rows,
  ROUND(COUNTIF(s.cust_id IS NOT NULL) / COUNT(*) * 100, 1) AS match_pct
FROM `skintific-data-warehouse.gt_schema.gt_master_salesman_pjp` p
LEFT JOIN `skintific-data-warehouse.gt_schema.master_store_database` s
  ON s.cust_id = p.kode_toko;
```
This was never run during design (only eyeballed for format similarity). If `match_pct` is high (>90%), proceed. If it's low, **stop** — `fact_route_plan_pjp.outlet_sk` will be mostly NULL and `vw_route_compliance` will be unusable until you find the real join key (possibly via `gt_schema.master_store_database`'s other identifier columns, or a separate mapping table not surfaced in this investigation).

Both queries are read-only — run them with the same investigation-grade read-only credentials, no new access needed yet.

---

## Phase 1 — Provision access

The read-only account cannot do anything past this point. Create a **new, separate** service account scoped to least privilege — don't elevate the read-only one (keeps the investigation account's audit trail clean, matches the existing principle this project already follows for the read-only account itself).

```bash
gcloud iam service-accounts create sfa-step-etl \
  --project=skintific-data-warehouse \
  --display-name="sfa_step ETL (BigQuery Data Editor + Job User, sfa_step dataset only)"
```

Grant dataset-level (not project-level) `roles/bigquery.dataEditor` once the dataset exists (chicken-and-egg with Phase 2 — easiest path: grant project-level `roles/bigquery.dataEditor` temporarily for Phase 2/3, then tighten to a dataset-level IAM binding on `sfa_step` afterward and revoke the project-level grant). Also grant `roles/bigquery.jobUser` (project-level, required to run any job).

Source datasets (`gt_schema`, `sadata`, `repsly`) only need **read** access for this service account — it never writes to them. If it doesn't already have `BigQuery Data Viewer` on the project (likely it does, if reusing the pattern from the existing read-only investigation account), grant that too.

Download a key and register it the same way the existing read-only key is documented (see [[access_setup]] for the pattern: key file path + project + role, never the key value, in any memory/doc).

---

## Phase 2 — Create the schema

Run `sfa_step_ddl.sql` **in order, top to bottom** — it's already structured as: schema → shared UDF → tables → views → procedures, and views/procedures reference tables that must exist first.

```bash
bq query --use_legacy_sql=false --project_id=skintific-data-warehouse < sfa_step_ddl.sql
```

If your `bq` CLI version chokes on the multi-statement file (some older versions only run one statement per invocation), split and run via the Python client instead:

```python
from google.cloud import bigquery
client = bigquery.Client.from_service_account_json('/path/to/sfa-step-etl-key.json')
with open('sfa_step_ddl.sql') as f:
    sql = f.read()
job = client.query(sql)  # BigQuery scripting runs the whole file as one multi-statement job
job.result()
```

**Verify:**
```sql
SELECT table_name, table_type FROM `skintific-data-warehouse.sfa_step.INFORMATION_SCHEMA.TABLES`;
SELECT routine_name, routine_type FROM `skintific-data-warehouse.sfa_step.INFORMATION_SCHEMA.ROUTINES`;
```
Expect 7 tables (`dim_outlet`, `dim_salesman`, `fact_route_plan_pjp`, `fact_visit`, `dim_outlet_location`, `fact_management_target`, `fact_spv_target` — the 2 control tables, `sync_watermark`/`sync_log`, are created in Phase 3, not here), 6 views, 8 routines (1 function + 7 procedures: `sp_refresh_dim_outlet`, `sp_refresh_dim_salesman`, `sp_refresh_dim_sadata_entities`, `sp_reload_fact_route_plan_pjp`, `sp_refresh_fact_visit_sadata`, `sp_refresh_dim_outlet_location`, `sp_refresh_fact_management_target`, plus `fn_surrogate_key`).

---

## Phase 3 — Initial full load

Run `sfa_step_sync.sql` **section by section, in file order** — the file is already laid out in dependency order (control tables → GT dims → SADATA/REPSLY dims → facts), and the load order matters: a fact load run before its dimension rows exist silently produces NULL foreign keys rather than erroring.

1. **Control tables + watermark seed** (top of file) — creates `sync_watermark`/`sync_log`, seeds 4 starting watermarks.
2. **§1a–§1b3** — all six dimension loads (GT outlet, GT salesman, SADATA outlet, REPSLY outlet, SADATA salesman, REPSLY salesman). Run all of these before any fact load.
3. **§1c** — `fact_route_plan_pjp`. Immediately after, run the Phase 0.2 validation query again against the *loaded* table (`SELECT COUNTIF(outlet_sk IS NULL), COUNT(*) FROM sfa_step.fact_route_plan_pjp`) to confirm the join rate in practice, not just in theory.
4. **§1d–§1e** — `fact_visit` (both SADATA_BA and REPSLY_HISTORICAL).
5. **§1f** — `fact_management_target`.

```bash
bq query --use_legacy_sql=false --project_id=skintific-data-warehouse < sfa_step_sync.sql
```

(Running the whole file at once is fine — it's already ordered correctly. Section-by-section above is for when something fails partway and you need to know what's safe to re-run: every INSERT here is into an empty table, so re-running a section that already partially succeeded will duplicate rows — `TRUNCATE` the specific table and re-run that section, don't re-run the whole file blindly after a partial failure.)

---

## Phase 4 — Post-load validation

Don't trust row counts alone — check the things this design explicitly flagged as uncertain:

```sql
-- FK resolution rates (the real numbers, not the assumed ones)
SELECT 'fact_route_plan_pjp.outlet_sk' AS check_name, COUNTIF(outlet_sk IS NULL) AS nulls, COUNT(*) AS total
FROM `skintific-data-warehouse.sfa_step.fact_route_plan_pjp`
UNION ALL
SELECT 'fact_route_plan_pjp.salesman_sk', COUNTIF(salesman_sk IS NULL), COUNT(*)
FROM `skintific-data-warehouse.sfa_step.fact_route_plan_pjp`
UNION ALL
SELECT 'fact_visit.outlet_sk (SADATA_BA)', COUNTIF(outlet_sk IS NULL), COUNT(*)
FROM `skintific-data-warehouse.sfa_step.fact_visit` WHERE source_system = 'SADATA_BA'
UNION ALL
SELECT 'fact_visit.salesman_sk (SADATA_BA)', COUNTIF(salesman_sk IS NULL), COUNT(*)
FROM `skintific-data-warehouse.sfa_step.fact_visit` WHERE source_system = 'SADATA_BA';
```
If any of these show a high null rate (>10-15%, judgment call), stop and investigate before building anything on top — a dashboard built on silently-unresolved FKs will quietly undercount.

```sql
-- Comply view sanity check — does it produce sensible numbers for at least one brand/month?
SELECT * FROM `skintific-data-warehouse.sfa_step.vw_target_comply` ORDER BY period_month DESC LIMIT 10;
```
Expect `management_target_total` populated for every row (real GT data) and `spv_target_total` **NULL for every row** at this point — `fact_spv_target` is empty until the STEP app starts writing to it (Phase 7). A NULL `comply_pct` here is correct, not a bug, until then.

```sql
-- dim_outlet_location coverage — what fraction of outlets have a real,
-- field-derived GPS point vs. only the sparse master-data fallback?
SELECT
  COUNT(*) AS total_active_outlets,
  COUNTIF(location_source = 'HANDHELD_CHECKIN_DERIVED') AS from_handheld,
  COUNTIF(location_source = 'MASTER_DATA') AS from_master_data,
  COUNTIF(location_source IS NULL) AS no_location_at_all
FROM `skintific-data-warehouse.sfa_step.vw_outlet_location_best`;
```
Expect `from_handheld` to roughly track how many distinct outlets appear in `fact_visit` (SADATA_BA) — it will *not* cover every outlet, only ones a BA has actually checked into. `no_location_at_all` quantifies the outlets neither source can place; that number is the real remaining gap, not the original 87.7%.

---

## Phase 5 — Orchestrate ongoing sync (Airflow)

This codebase already has a house pattern for exactly this shape of job — see `dags/dag_gt_bq_sp_call.py`: a `BigQueryInsertJobOperator` per stored procedure, `gcp_conn_id='gcp-gateaway'`, sequential `CALL`s. Reuse it rather than inventing a new orchestration style.

**You need a new Airflow connection**, not `gcp-gateaway` as-is — that connection's underlying service account is whatever the existing GT pipelines use, which does **not** have write access to `sfa_step` unless explicitly granted. Either (a) add the `sfa_step` dataset-level Data Editor grant to whatever service account `gcp-gateaway` already points at, or (b) register a new Airflow connection (e.g. `gcp-sfa-step`) pointing at the `sfa-step-etl` service account key from Phase 1. (b) is cleaner — keeps this new pipeline's blast radius separate from every other DAG using `gcp-gateaway`.

```python
# dags/dag_sfa_step_sync.py
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import timedelta, datetime
from pendulum import timezone

ALERT_EMAIL = ['<your-team-email>@skintific.com']  # replace before deploying
jakarta_tz = timezone("Asia/Jakarta")

default_args = {
    'owner': 'STEP',
    'start_date': datetime(2026, 7, 1, tzinfo=jakarta_tz),
    'retries': 5,                              # matches the documented SFA Integration
    'retry_delay': timedelta(minutes=10),      # Monitor retry policy (max 5, exp backoff) —
    'retry_exponential_backoff': True,         # see docs/09-sfa-integration-architecture.md §4
    'email': ALERT_EMAIL,
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    dag_id='sfa_step_daily_sync',
    default_args=default_args,
    description='Daily incremental sync into sfa_step (dims, SADATA visits, management target)',
    schedule_interval='0 3 * * *',   # 03:00 Jakarta — after upstream gt_schema/sadata loads land
    catchup=False,
    tags=['BIGQUERY', 'SFA_STEP', 'STORED PROCEDURE'],
) as dag:

    refresh_dim_outlet = BigQueryInsertJobOperator(
        task_id='refresh_dim_outlet',
        gcp_conn_id='gcp-sfa-step',
        configuration={"query": {"query": "CALL `skintific-data-warehouse.sfa_step.sp_refresh_dim_outlet`();", "useLegacySql": False}},
        location='US',
    )
    refresh_dim_salesman = BigQueryInsertJobOperator(
        task_id='refresh_dim_salesman',
        gcp_conn_id='gcp-sfa-step',
        configuration={"query": {"query": "CALL `skintific-data-warehouse.sfa_step.sp_refresh_dim_salesman`();", "useLegacySql": False}},
        location='US',
    )
    refresh_dim_sadata_entities = BigQueryInsertJobOperator(
        task_id='refresh_dim_sadata_entities',
        gcp_conn_id='gcp-sfa-step',
        configuration={"query": {"query": "CALL `skintific-data-warehouse.sfa_step.sp_refresh_dim_sadata_entities`();", "useLegacySql": False}},
        location='US',
    )
    refresh_fact_visit_sadata = BigQueryInsertJobOperator(
        task_id='refresh_fact_visit_sadata',
        gcp_conn_id='gcp-sfa-step',
        configuration={"query": {"query": "CALL `skintific-data-warehouse.sfa_step.sp_refresh_fact_visit_sadata`();", "useLegacySql": False}},
        location='US',
    )
    refresh_dim_outlet_location = BigQueryInsertJobOperator(
        task_id='refresh_dim_outlet_location',
        gcp_conn_id='gcp-sfa-step',
        configuration={"query": {"query": "CALL `skintific-data-warehouse.sfa_step.sp_refresh_dim_outlet_location`();", "useLegacySql": False}},
        location='US',
    )
    refresh_fact_management_target = BigQueryInsertJobOperator(
        task_id='refresh_fact_management_target',
        gcp_conn_id='gcp-sfa-step',
        configuration={"query": {"query": "CALL `skintific-data-warehouse.sfa_step.sp_refresh_fact_management_target`();", "useLegacySql": False}},
        location='US',
    )

    # dims before facts; sadata dims before sadata fact (FK resolution, see file
    # header note in sfa_step_sync.sql); dim_outlet_location AFTER fact_visit since
    # it's a full re-aggregate of fact_visit's check-in GPS data, not an independent
    # source — running it first would aggregate stale/empty data.
    # management_target has no fact dependency, runs independently.
    [refresh_dim_outlet, refresh_dim_salesman] >> refresh_dim_sadata_entities >> refresh_fact_visit_sadata >> refresh_dim_outlet_location
    refresh_fact_management_target  # independent, no dependency edge needed


with DAG(
    dag_id='sfa_step_weekly_route_plan_reload',
    default_args=default_args,
    description='Weekly full reload of fact_route_plan_pjp (source is itself a batch re-upload, not incremental)',
    schedule_interval='0 4 * * 1',   # Monday 04:00 Jakarta
    catchup=False,
    tags=['BIGQUERY', 'SFA_STEP', 'STORED PROCEDURE'],
) as dag_weekly:

    reload_route_plan = BigQueryInsertJobOperator(
        task_id='reload_fact_route_plan_pjp',
        gcp_conn_id='gcp-sfa-step',
        configuration={"query": {"query": "CALL `skintific-data-warehouse.sfa_step.sp_reload_fact_route_plan_pjp`();", "useLegacySql": False}},
        location='US',
    )
```

Drop this in `dags/`, same as every other DAG in this repo. **`repsly.fact_visits_t` deliberately has no DAG** — it's a one-time historical load already done in Phase 3; scheduling it would just re-run a no-op against a dead source.

---

## Phase 6 — Monitoring

`sync_log` (created in Phase 3) is the audit trail — but nothing writes to it yet except the manual `BEGIN...EXCEPTION` example in `sfa_step_sync.sql` §5. Two options, pick based on how much you want to invest now:

- **Minimal (do this first):** rely on Airflow's own task-failure email (`email_on_failure: True` above) + the `email_on_retry: False` / 5-retry policy. Good enough to know *that* something failed.
- **Fuller (do this once the daily DAG has run clean for a week):** wrap each `CALL` in the same `BEGIN...EXCEPTION WHEN ERROR THEN INSERT INTO sync_log...RAISE` pattern shown in `sfa_step_sync.sql` §5, so you also know *what* failed and *how many rows* were affected without digging through Airflow logs. This is what `sync_log` was designed for — it's just not wired into every procedure yet to keep this first deployment pass smaller.

Either way, check weekly: `SELECT * FROM sfa_step.sync_watermark` — if `last_run_at` for any table is more than ~2 days old, something's stuck even if Airflow didn't alert (e.g. the DAG itself got paused).

---

## Phase 7 — Connect the STEP application

`fact_spv_target` is the one table in this schema the app writes to directly — no sync job touches it. When the STEP backend's target-approval workflow is built:
- INSERT a new row per (salesman, brand, period_month) when an SPV proposes/edits a target, `approval_status = 'draft'`.
- UPDATE `approval_status` through the same chain already designed in `docs/04-database-erd.md` (SPV → Area Manager → Distributor Manager) — `vw_target_comply` only counts rows where `approval_status = 'approved'`, so an unapproved draft correctly doesn't move the Comply% yet.
- Every other table in this schema (`vw_target_comply`, `vw_route_compliance`, `vw_salesman_360_summary`, `vw_outlet_active`, `vw_salesman_active`) is **read-only from the app's perspective** — the app's Dashboard/Target Management/Route Evaluate pages query these views, never the underlying `dim_*`/`fact_*` tables directly, so a future change to the sync logic doesn't require an app-side query change as long as the view's output shape stays stable.

---

## Go-Live Checklist

- [ ] Phase 0.1 and 0.2 validation queries re-run with current data, results reviewed
- [ ] `sfa-step-etl` service account created, scoped to `sfa_step` dataset (not project-wide) once the dataset exists
- [ ] Schema created (Phase 2), `INFORMATION_SCHEMA` verified — 7 tables (control tables land in Phase 3), 6 views, 8 routines
- [ ] Initial full load complete (Phase 3), in the documented order
- [ ] FK-resolution null-rate check (Phase 4) reviewed and acceptable
- [ ] `gcp-sfa-step` Airflow connection registered
- [ ] `dags/dag_sfa_step_sync.py` (daily) and weekly route-plan-reload DAG deployed, both **unpaused** in the Airflow UI (DAGs land paused by default in most setups — confirm)
- [ ] First scheduled run observed end-to-end (not just dry-run) — check `sync_watermark.last_run_at` actually advanced
- [ ] Stakeholders briefed on the two known gaps: `fact_visit.is_effective` is NULL for every row today (no reliable order-linkage source — see architecture doc §6), and GT-channel field salesmen have no real visit-execution data source at all in this slice (only the BA/sadata channel does)

---

## Rollback / Contingency

- **Bad initial load:** every table in this slice can be safely `TRUNCATE`d and reloaded from Phase 3 — nothing here is the source of truth for anything outside `sfa_step` itself (it's all derived from `gt_schema`/`sadata`/`repsly`, which are untouched).
- **Bad incremental sync:** `sync_watermark.last_watermark_value` is the single thing that, if wrong, causes either re-processing (harmless, MERGE is idempotent) or skipped rows (not harmless). If a sync run is suspected of having advanced the watermark incorrectly, manually reset it: `UPDATE sfa_step.sync_watermark SET last_watermark_value = '<earlier date>' WHERE sync_table_name = '<table>'` and re-run that procedure.
- **Need to undo a deployment entirely:** `DROP SCHEMA \`skintific-data-warehouse.sfa_step\` CASCADE` — this only affects what this guide created. It cannot touch `gt_schema`/`sadata`/`repsly`/etc. (the `sfa-step-etl` service account was never granted write access there in Phase 1).

---

## Related Documents

[`sfa_step_ddl.sql`](sfa_step_ddl.sql) · [`sfa_step_sync.sql`](sfa_step_sync.sql) · [`sfa_step_data_dictionary.md`](sfa_step_data_dictionary.md) · [`sfa_step_architecture.md`](sfa_step_architecture.md) · house DAG pattern reference: [`../../../docker-airflow/dags/dag_gt_bq_sp_call.py`](../../../docker-airflow/dags/dag_gt_bq_sp_call.py)
