-- =============================================================================
-- STEP — sfa_step synchronization scripts (BigQuery)
-- =============================================================================
-- Companion to sfa_step_ddl.sql. Read that file's header first — it documents
-- which source tables are real/live vs stale/historical vs structurally absent,
-- and why dim_outlet/dim_salesman are federated rosters rather than a single
-- conformed dimension. This file does not repeat those caveats per-statement;
-- it only repeats the ones that change the SYNC STRATEGY specifically.
--
-- IMPORTANT — load order: dims before facts, GT dims (1a/1b) before SADATA/REPSLY
-- dims (1a2/1a3/1b2/1b3) before either fact_visit source (1d/1e), and
-- dim_outlet_location (1e2/2b2) AFTER fact_visit, not before — it's derived FROM
-- fact_visit's check-in GPS data, so running it first would aggregate nothing.
-- fact_visit's FK lookups join on source_system+source_code, so if the dim rows
-- aren't there yet, the fact rows silently land with a NULL outlet_sk/salesman_sk
-- instead of erroring.
--
-- Per-table sync strategy summary (rationale in sfa_step_architecture.md §4):
--   dim_outlet (GT/SADATA/REPSLY) — incremental MERGE (GT) / full reload (SADATA, REPSLY — see 1a2/1a3), daily/one-time. Source: gt_schema.master_store_database (31,261 rows) + sadata.fact_ba_attendance_t distinct store_id + repsly.dim_clients_t (3,922 rows). Watermark: input_date (GT only).
--   dim_salesman (GT/SADATA/REPSLY) — same pattern as dim_outlet. Source: gt_schema.gt_salesman_mapping (317 rows, + gt_master_salesman name-join enrichment) + sadata.fact_ba_attendance_t distinct employee_nik + repsly.master_representative (6,955 rows). Watermark: updated_at (GT only).
--   dim_outlet_location     — full re-aggregate, daily, right after fact_visit's sync. Source: sfa_step.fact_visit itself (not an external table) — median of check_in_latitude/longitude per outlet. Not incremental: cheap at current volume (one row per outlet, not per visit) and correctness matters more than speed here.
--   fact_route_plan_pjp     — full TRUNCATE+RELOAD, weekly. Source: gt_schema.gt_master_salesman_pjp (27,915 rows). The source itself is a batch re-upload, not row-incrementally maintained — MERGE would just re-match every row every time, so truncate+reload is simpler AND correct here, not a shortcut.
--   fact_visit (SADATA_BA)  — incremental MERGE, daily.   Source: sadata.fact_ba_attendance_t (872,873 rows, 198MB — the actual volume driver in this slice). Watermark: date.
--   fact_visit (REPSLY_HIST)— ONE-TIME full load only.    Source: repsly.fact_visits_t — confirmed stale (max date 2024-01-06). Never re-run incrementally; re-running the full load is harmless (idempotent MERGE) but pointless since the source hasn't changed.
--   fact_management_target  — incremental MERGE, daily.   Source: gt_schema.fact_gt_target_v2_t (44,827 rows). Watermark: calendar_date. Filters customer_id IS NOT NULL (29% of source rows are null/placeholder, confirmed by query).
--   fact_spv_target         — NOT SYNCED. Written directly by the STEP app (INSERT/UPDATE from the app's own approval workflow). Listed here only so its absence from the sync schedule isn't mistaken for an oversight.
-- =============================================================================

-- =============================================================================
-- SYNC CONTROL TABLES
-- =============================================================================
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.sync_watermark` (
  sync_table_name     STRING    NOT NULL OPTIONS(description="e.g. 'dim_outlet', 'fact_visit_sadata'"),
  watermark_column    STRING    NOT NULL OPTIONS(description="Source column used as the incremental cursor, e.g. 'input_date'"),
  last_watermark_value STRING   OPTIONS(description="Stored as STRING so one table can hold DATE- and TIMESTAMP-typed watermarks alike; CAST at read time"),
  last_run_at         TIMESTAMP,
  last_run_status     STRING    OPTIONS(description="'success' | 'failed' | 'partial'"),
  PRIMARY KEY (sync_table_name) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.sync_log` (
  sync_run_id       STRING    NOT NULL,
  sync_table_name   STRING    NOT NULL,
  started_at        TIMESTAMP NOT NULL,
  ended_at          TIMESTAMP,
  status            STRING    OPTIONS(description="'success' | 'failed' | 'partial'"),
  rows_inserted     INT64,
  rows_updated      INT64,
  rows_soft_deleted INT64,
  error_message     STRING,
  PRIMARY KEY (sync_run_id) NOT ENFORCED
)
PARTITION BY DATE(started_at);

-- seed the watermark table once (idempotent — MERGE not INSERT, safe to re-run)
MERGE `skintific-data-warehouse.sfa_step.sync_watermark` T
USING (
  SELECT * FROM UNNEST([
    STRUCT('dim_outlet' AS sync_table_name, 'input_date' AS watermark_column, CAST(DATE '2024-01-01' AS STRING) AS last_watermark_value),
    STRUCT('dim_salesman', 'updated_at', CAST(DATE '2024-01-01' AS STRING)),
    STRUCT('fact_visit_sadata', 'date', CAST(DATE '2024-01-01' AS STRING)),
    STRUCT('fact_management_target', 'calendar_date', CAST(DATE '2024-01-01' AS STRING))
  ])
) S
ON T.sync_table_name = S.sync_table_name
WHEN NOT MATCHED THEN INSERT (sync_table_name, watermark_column, last_watermark_value, last_run_at, last_run_status)
VALUES (S.sync_table_name, S.watermark_column, S.last_watermark_value, NULL, NULL);

-- =============================================================================
-- 1. INITIAL FULL LOAD
-- =============================================================================

-- 1a. dim_outlet — full load (run once; subsequent runs use the incremental MERGE below)
INSERT INTO `skintific-data-warehouse.sfa_step.dim_outlet`
  (outlet_sk, source_system, source_outlet_code, store_name, brand, channel, store_grade,
   customer_category, region, distributor_code, distributor_name, asm_name, spv_name, address,
   latitude, longitude, operational_status, source_updated_at, sfa_step_loaded_at, is_deleted)
SELECT
  `skintific-data-warehouse.sfa_step.fn_surrogate_key`('GT', cust_id), 'GT', cust_id, store_name, brand,
  CASE WHEN ba_non_ba = 'BA' THEN 'BA' ELSE 'GT' END,
  COALESCE(sktf_store_grade_q1_25, g2g_store_grade_q1_25),
  customer_category, region, distributor_code, distributor, asm, spv, address,
  SAFE_CAST(latitude AS FLOAT64), SAFE_CAST(longitude AS FLOAT64), customer_status, input_date,
  CURRENT_TIMESTAMP(), FALSE
FROM `skintific-data-warehouse.gt_schema.master_store_database`
WHERE cust_id IS NOT NULL;

-- 1b. dim_salesman — full load
INSERT INTO `skintific-data-warehouse.sfa_step.dim_salesman`
  (salesman_sk, source_system, source_salesman_code, salesman_name, salesman_type, role_type,
   distributor_code, region, spv_name, asm_name, is_active, source_updated_at, sfa_step_loaded_at, is_deleted)
SELECT
  `skintific-data-warehouse.sfa_step.fn_surrogate_key`('GT_MAPPING', m.salesman_id), 'GT_MAPPING', m.salesman_id,
  m.salesman, m.salesman_type, 'SALESMAN', m.distributor_code, gm.region, gm.nama_spv_internal, gm.asm,
  m.is_active, SAFE_CAST(m.updated_at AS TIMESTAMP), CURRENT_TIMESTAMP(), FALSE
FROM `skintific-data-warehouse.gt_schema.gt_salesman_mapping` m
LEFT JOIN `skintific-data-warehouse.gt_schema.gt_master_salesman` gm ON gm.nama_salesman = m.salesman;

-- 1a2. dim_outlet — SADATA-sourced rows (distinct store_id from fact_ba_attendance_t).
-- Required before 1d (fact_visit SADATA_BA) or every row there gets a NULL outlet_sk.
INSERT INTO `skintific-data-warehouse.sfa_step.dim_outlet`
  (outlet_sk, source_system, source_outlet_code, store_name, channel, region, sfa_step_loaded_at, is_deleted)
SELECT
  `skintific-data-warehouse.sfa_step.fn_surrogate_key`('SADATA', store_id), 'SADATA', store_id,
  ANY_VALUE(store_name), ANY_VALUE(channel_kam), ANY_VALUE(region), CURRENT_TIMESTAMP(), FALSE
FROM `skintific-data-warehouse.sadata.fact_ba_attendance_t`
WHERE store_id IS NOT NULL
GROUP BY store_id;

-- 1a3. dim_outlet — REPSLY-sourced rows (repsly.dim_clients_t — the dimension table,
-- not fact_visits_t, so descriptive attributes come along for free).
-- Required before 1e (fact_visit REPSLY_HISTORICAL).
INSERT INTO `skintific-data-warehouse.sfa_step.dim_outlet`
  (outlet_sk, source_system, source_outlet_code, store_name, region, address, latitude, longitude,
   repsly_client_code, sadata_store_id, skintific_code, sfa_step_loaded_at, is_deleted)
SELECT
  `skintific-data-warehouse.sfa_step.fn_surrogate_key`('REPSLY', code), 'REPSLY', code, store_name, region,
  streetaddress, SAFE_CAST(gpslatitude AS FLOAT64), SAFE_CAST(gpslongitude AS FLOAT64),
  code, NULLIF(store_code_sadata, ''), NULLIF(skintific_code, ''), CURRENT_TIMESTAMP(), FALSE
FROM `skintific-data-warehouse.repsly.dim_clients_t`
WHERE code IS NOT NULL;

-- 1b2. dim_salesman — SADATA-sourced rows (distinct employee_nik from fact_ba_attendance_t).
-- Required before 1d (fact_visit SADATA_BA).
INSERT INTO `skintific-data-warehouse.sfa_step.dim_salesman`
  (salesman_sk, source_system, source_salesman_code, salesman_name, role_type, region, sfa_step_loaded_at, is_deleted)
SELECT
  `skintific-data-warehouse.sfa_step.fn_surrogate_key`('SADATA_EMPLOYEE', employee_nik), 'SADATA_EMPLOYEE', employee_nik,
  ANY_VALUE(employee_name), 'BA', ANY_VALUE(region), CURRENT_TIMESTAMP(), FALSE
FROM `skintific-data-warehouse.sadata.fact_ba_attendance_t`
WHERE employee_nik IS NOT NULL
GROUP BY employee_nik;

-- 1b3. dim_salesman — REPSLY-sourced rows (repsly.master_representative — the dimension
-- table, has active/territories/brand/region, richer than distinct codes from fact_visits_t).
-- Required before 1e (fact_visit REPSLY_HISTORICAL).
INSERT INTO `skintific-data-warehouse.sfa_step.dim_salesman`
  (salesman_sk, source_system, source_salesman_code, salesman_name, role_type, region, is_active, sfa_step_loaded_at, is_deleted)
SELECT
  `skintific-data-warehouse.sfa_step.fn_surrogate_key`('REPSLY_REP', code), 'REPSLY_REP', code, name,
  CASE WHEN UPPER(user_role) LIKE '%BA%' THEN 'BA' ELSE 'SALESMAN' END,
  business_region, active, CURRENT_TIMESTAMP(), FALSE
FROM `skintific-data-warehouse.repsly.master_representative`
WHERE code IS NOT NULL;

-- 1c. fact_route_plan_pjp — full load (this IS the only load pattern for this table; see strategy above)
INSERT INTO `skintific-data-warehouse.sfa_step.fact_route_plan_pjp`
  (route_plan_sk, salesman_sk, outlet_sk, source_salesman_name, source_outlet_code, distributor_code,
   distributor_name, region, asm_name, visit_day_of_week, visit_week_pattern, visit_frequency_code,
   batch_uploaded_at, sfa_step_loaded_at, is_deleted)
SELECT
  `skintific-data-warehouse.sfa_step.fn_surrogate_key`('PJP', CONCAT(p.kode_distributor, '|', p.nama_salesman, '|', p.kode_toko, '|', p.hari, '|', p.minggu)),
  sm.salesman_sk,
  ot.outlet_sk,
  p.nama_salesman, p.kode_toko, p.kode_distributor, p.nama_distributor, p.region, p.asm,
  p.hari, p.minggu, p.frekuensi,
  SAFE_CAST(p.uploaded_at AS TIMESTAMP), CURRENT_TIMESTAMP(), FALSE
FROM `skintific-data-warehouse.gt_schema.gt_master_salesman_pjp` p
LEFT JOIN `skintific-data-warehouse.sfa_step.dim_salesman` sm
  ON sm.source_system = 'GT_MAPPING' AND sm.salesman_name = p.nama_salesman
LEFT JOIN `skintific-data-warehouse.sfa_step.dim_outlet` ot
  ON ot.source_system = 'GT' AND ot.source_outlet_code = p.kode_toko;
-- NOTE: validate the outlet_sk join hit-rate before relying on this in production —
-- kode_toko (PJP) vs cust_id (master_store_database) are visually similar in shape
-- ("ICWJ01164" vs "IWJA00173") but this was NOT confirmed by a full join during
-- this design pass. Run: SELECT COUNTIF(outlet_sk IS NULL), COUNT(*) FROM ... right
-- after this load and investigate if the unresolved rate is high.

-- 1d. fact_visit — SADATA_BA full load (primary, freshest source)
INSERT INTO `skintific-data-warehouse.sfa_step.fact_visit`
  (visit_sk, source_system, source_visit_id, salesman_sk, outlet_sk, visit_date, check_in_at, check_out_at,
   check_in_latitude, check_in_longitude, check_in_distance_m, is_call, is_effective, effective_source,
   source_loaded_at, sfa_step_loaded_at, is_deleted)
SELECT
  `skintific-data-warehouse.sfa_step.fn_surrogate_key`('SADATA_BA', CONCAT(v.employee_nik, '|', v.store_id, '|', CAST(v.date AS STRING))),
  'SADATA_BA', CONCAT(v.employee_nik, '|', v.store_id, '|', CAST(v.date AS STRING)),
  sm.salesman_sk, ot.outlet_sk, v.date,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', v.check_in),
  SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', v.check_out),
  SAFE_CAST(v.check_in_latitude AS FLOAT64), SAFE_CAST(v.check_in_longitude AS FLOAT64),
  SAFE_CAST(v.check_in_distance AS FLOAT64),
  v.check_in IS NOT NULL,
  NULL,    -- is_effective: no reliable order-linkage source for this channel today — see ddl.sql header
  NULL,
  v.time_stamp, CURRENT_TIMESTAMP(), FALSE
FROM `skintific-data-warehouse.sadata.fact_ba_attendance_t` v
LEFT JOIN `skintific-data-warehouse.sfa_step.dim_salesman` sm
  ON sm.source_system = 'SADATA_EMPLOYEE' AND sm.source_salesman_code = v.employee_nik
LEFT JOIN `skintific-data-warehouse.sfa_step.dim_outlet` ot
  ON ot.source_system = 'SADATA' AND ot.source_outlet_code = v.store_id;
-- Depends on 1b2/1a2 above having run first in the same load — run this section
-- in file order, not cherry-picked, or these FKs will resolve to NULL.

-- 1e. fact_visit — REPSLY_HISTORICAL full load (one-time only — see strategy note)
INSERT INTO `skintific-data-warehouse.sfa_step.fact_visit`
  (visit_sk, source_system, source_visit_id, salesman_sk, outlet_sk, visit_date, check_in_at, check_out_at,
   check_in_latitude, check_in_longitude, check_in_distance_m, is_call, is_effective, effective_source,
   source_loaded_at, sfa_step_loaded_at, is_deleted)
SELECT
  `skintific-data-warehouse.sfa_step.fn_surrogate_key`('REPSLY_HISTORICAL', CAST(r.visitid AS STRING)),
  'REPSLY_HISTORICAL', CAST(r.visitid AS STRING),
  sm.salesman_sk, ot.outlet_sk,
  SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(r.date, 1, 10)),
  SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', r.dateandtimestart),
  SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', r.dateandtimeend),
  r.latitudestart, r.longitudestart, NULL,
  r.explicitcheckin,
  NULL, NULL,
  TIMESTAMP_MILLIS(r.timestamp), CURRENT_TIMESTAMP(), FALSE
FROM `skintific-data-warehouse.repsly.fact_visits_t` r
LEFT JOIN `skintific-data-warehouse.sfa_step.dim_salesman` sm
  ON sm.source_system = 'REPSLY_REP' AND sm.source_salesman_code = r.representativecode
LEFT JOIN `skintific-data-warehouse.sfa_step.dim_outlet` ot
  ON ot.source_system = 'REPSLY' AND ot.source_outlet_code = CAST(r.clientcode AS STRING);

-- 1e2. dim_outlet_location — derive each outlet's real GPS location from the
-- check-in data just loaded in 1d/1e above. Must run AFTER both fact_visit loads,
-- not before — it aggregates whatever is currently in fact_visit. Same body as
-- sp_refresh_dim_outlet_location() in ddl.sql; CALL the procedure instead of this
-- inline copy where CALL privilege is available — both must be kept in sync if
-- either changes (see sfa_step_deployment_guide.md §5 for the canonical version).
CALL `skintific-data-warehouse.sfa_step.sp_refresh_dim_outlet_location`();

-- 1f. fact_management_target — full load (unpivot 3 brand columns -> 3 rows)
INSERT INTO `skintific-data-warehouse.sfa_step.fact_management_target`
  (target_sk, outlet_sk, source_customer_id, calendar_date, brand, management_target_amount,
   weekly_visit_target, region, distributor_name, spv_name, asm_name, source_loaded_at, sfa_step_loaded_at, is_deleted)
SELECT
  `skintific-data-warehouse.sfa_step.fn_surrogate_key`('GT_TARGET', CONCAT(customer_id, '|', CAST(calendar_date AS STRING), '|', brand_name)),
  `skintific-data-warehouse.sfa_step.fn_surrogate_key`('GT', customer_id),
  customer_id, calendar_date, brand_name, target_amount, weekly_visit_target, region, distributor, spv_name, asm_name,
  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), FALSE
FROM `skintific-data-warehouse.gt_schema.fact_gt_target_v2_t`,
UNNEST([
  STRUCT('Skintific' AS brand_name, skintific_target AS target_amount),
  STRUCT('Glad2Glow', g2g_target),
  STRUCT('Timephoria', timephoria_target)
])
WHERE customer_id IS NOT NULL AND target_amount IS NOT NULL;

-- =============================================================================
-- 2. INCREMENTAL SYNCHRONIZATION (MERGE / upsert)
-- Each block: read watermark -> MERGE only rows newer than it -> advance watermark.
-- Run daily (dim_outlet, dim_salesman, fact_visit_sadata, fact_management_target).
-- =============================================================================

-- 2a. dim_outlet incremental
-- Wrapped in its own BEGIN...END so this section is independently runnable (e.g.
-- as its own Airflow task/BigQueryInsertJobOperator) — BigQuery scripting requires
-- every DECLARE to sit at the start of its enclosing block, so each numbered
-- section below gets its own block rather than sharing one script-level DECLARE area.
BEGIN
DECLARE wm_outlet DATE;
SET wm_outlet = (
  SELECT SAFE_CAST(last_watermark_value AS DATE)
  FROM `skintific-data-warehouse.sfa_step.sync_watermark`
  WHERE sync_table_name = 'dim_outlet'
);

MERGE `skintific-data-warehouse.sfa_step.dim_outlet` T
USING (
  SELECT
    `skintific-data-warehouse.sfa_step.fn_surrogate_key`('GT', cust_id) AS outlet_sk,
    cust_id, store_name, brand,
    CASE WHEN ba_non_ba = 'BA' THEN 'BA' ELSE 'GT' END AS channel,
    COALESCE(sktf_store_grade_q1_25, g2g_store_grade_q1_25) AS store_grade,
    customer_category, region, distributor_code, distributor, asm, spv, address,
    SAFE_CAST(latitude AS FLOAT64) AS latitude, SAFE_CAST(longitude AS FLOAT64) AS longitude,
    customer_status, input_date
  FROM `skintific-data-warehouse.gt_schema.master_store_database`
  WHERE cust_id IS NOT NULL AND DATE(input_date) > wm_outlet
) S
ON T.outlet_sk = S.outlet_sk
WHEN MATCHED THEN UPDATE SET
  store_name = S.store_name, brand = S.brand, channel = S.channel, store_grade = S.store_grade,
  customer_category = S.customer_category, region = S.region, distributor_code = S.distributor_code,
  distributor_name = S.distributor, asm_name = S.asm, spv_name = S.spv, address = S.address,
  latitude = S.latitude, longitude = S.longitude, operational_status = S.customer_status,
  source_updated_at = S.input_date, sfa_step_loaded_at = CURRENT_TIMESTAMP(), is_deleted = FALSE
WHEN NOT MATCHED THEN INSERT (
  outlet_sk, source_system, source_outlet_code, store_name, brand, channel, store_grade, customer_category,
  region, distributor_code, distributor_name, asm_name, spv_name, address, latitude, longitude,
  operational_status, source_updated_at, sfa_step_loaded_at, is_deleted
) VALUES (
  S.outlet_sk, 'GT', S.cust_id, S.store_name, S.brand, S.channel, S.store_grade, S.customer_category,
  S.region, S.distributor_code, S.distributor, S.asm, S.spv, S.address, S.latitude, S.longitude,
  S.customer_status, S.input_date, CURRENT_TIMESTAMP(), FALSE
);

UPDATE `skintific-data-warehouse.sfa_step.sync_watermark`
SET last_watermark_value = CAST(CURRENT_DATE() AS STRING), last_run_at = CURRENT_TIMESTAMP(), last_run_status = 'success'
WHERE sync_table_name = 'dim_outlet';
END;

-- 2b. fact_visit (SADATA_BA channel) incremental — the only fact in this slice
-- with genuinely new rows arriving on a meaningful cadence (subject to the source
-- pipeline resuming — confirmed stale at 2025-08-07 as of this design; re-check
-- freshness before assuming "daily" is real and not just the intended cadence).
BEGIN
DECLARE wm_visit DATE;
SET wm_visit = (
  SELECT SAFE_CAST(last_watermark_value AS DATE)
  FROM `skintific-data-warehouse.sfa_step.sync_watermark`
  WHERE sync_table_name = 'fact_visit_sadata'
);

MERGE `skintific-data-warehouse.sfa_step.fact_visit` T
USING (
  SELECT
    `skintific-data-warehouse.sfa_step.fn_surrogate_key`('SADATA_BA', CONCAT(v.employee_nik, '|', v.store_id, '|', CAST(v.date AS STRING))) AS visit_sk,
    sm.salesman_sk, ot.outlet_sk, v.date,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', v.check_in) AS check_in_at,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', v.check_out) AS check_out_at,
    SAFE_CAST(v.check_in_latitude AS FLOAT64) AS check_in_latitude,
    SAFE_CAST(v.check_in_longitude AS FLOAT64) AS check_in_longitude,
    SAFE_CAST(v.check_in_distance AS FLOAT64) AS check_in_distance_m,
    v.check_in IS NOT NULL AS is_call,
    v.time_stamp AS source_loaded_at
  FROM `skintific-data-warehouse.sadata.fact_ba_attendance_t` v
  LEFT JOIN `skintific-data-warehouse.sfa_step.dim_salesman` sm
    ON sm.source_system = 'SADATA_EMPLOYEE' AND sm.source_salesman_code = v.employee_nik
  LEFT JOIN `skintific-data-warehouse.sfa_step.dim_outlet` ot
    ON ot.source_system = 'SADATA' AND ot.source_outlet_code = v.store_id
  WHERE v.date > wm_visit
) S
ON T.visit_sk = S.visit_sk
WHEN MATCHED THEN UPDATE SET
  check_in_at = S.check_in_at, check_out_at = S.check_out_at, check_in_latitude = S.check_in_latitude,
  check_in_longitude = S.check_in_longitude, check_in_distance_m = S.check_in_distance_m, is_call = S.is_call,
  source_loaded_at = S.source_loaded_at, sfa_step_loaded_at = CURRENT_TIMESTAMP(), is_deleted = FALSE
WHEN NOT MATCHED THEN INSERT (
  visit_sk, source_system, source_visit_id, salesman_sk, outlet_sk, visit_date, check_in_at, check_out_at,
  check_in_latitude, check_in_longitude, check_in_distance_m, is_call, is_effective, effective_source,
  source_loaded_at, sfa_step_loaded_at, is_deleted
) VALUES (
  S.visit_sk, 'SADATA_BA', S.visit_sk, S.salesman_sk, S.outlet_sk, S.date, S.check_in_at, S.check_out_at,
  S.check_in_latitude, S.check_in_longitude, S.check_in_distance_m, S.is_call, NULL, NULL,
  S.source_loaded_at, CURRENT_TIMESTAMP(), FALSE
);

UPDATE `skintific-data-warehouse.sfa_step.sync_watermark`
SET last_watermark_value = CAST(CURRENT_DATE() AS STRING), last_run_at = CURRENT_TIMESTAMP(), last_run_status = 'success'
WHERE sync_table_name = 'fact_visit_sadata';
END;

-- 2b2. dim_outlet_location — re-run immediately after every 2b, same reason as 1e2
-- above: it's a full re-aggregate of fact_visit, so it must run after fact_visit's
-- incremental MERGE lands new check-ins, not before.
CALL `skintific-data-warehouse.sfa_step.sp_refresh_dim_outlet_location`();

-- 2c. fact_management_target incremental (same shape as the procedure in ddl.sql;
-- shown again here inline for anyone running sync without CALL privilege, e.g. a
-- BigQueryInsertJobOperator that submits raw SQL rather than a stored-proc call)
BEGIN
DECLARE wm_target DATE;
SET wm_target = (
  SELECT SAFE_CAST(last_watermark_value AS DATE)
  FROM `skintific-data-warehouse.sfa_step.sync_watermark`
  WHERE sync_table_name = 'fact_management_target'
);

MERGE `skintific-data-warehouse.sfa_step.fact_management_target` T
USING (
  SELECT
    `skintific-data-warehouse.sfa_step.fn_surrogate_key`('GT_TARGET', CONCAT(customer_id, '|', CAST(calendar_date AS STRING), '|', brand_name)) AS target_sk,
    `skintific-data-warehouse.sfa_step.fn_surrogate_key`('GT', customer_id) AS outlet_sk,
    customer_id, calendar_date, brand_name, target_amount, weekly_visit_target, region, distributor, spv_name, asm_name
  FROM `skintific-data-warehouse.gt_schema.fact_gt_target_v2_t`,
  UNNEST([
    STRUCT('Skintific' AS brand_name, skintific_target AS target_amount),
    STRUCT('Glad2Glow', g2g_target),
    STRUCT('Timephoria', timephoria_target)
  ])
  WHERE customer_id IS NOT NULL AND target_amount IS NOT NULL AND calendar_date > wm_target
) S
ON T.target_sk = S.target_sk
WHEN MATCHED THEN UPDATE SET
  management_target_amount = S.target_amount, weekly_visit_target = S.weekly_visit_target, region = S.region,
  distributor_name = S.distributor, spv_name = S.spv_name, asm_name = S.asm_name,
  sfa_step_loaded_at = CURRENT_TIMESTAMP(), is_deleted = FALSE
WHEN NOT MATCHED THEN INSERT (
  target_sk, outlet_sk, source_customer_id, calendar_date, brand, management_target_amount,
  weekly_visit_target, region, distributor_name, spv_name, asm_name, sfa_step_loaded_at, is_deleted
) VALUES (
  S.target_sk, S.outlet_sk, S.customer_id, S.calendar_date, S.brand_name, S.target_amount,
  S.weekly_visit_target, S.region, S.distributor, S.spv_name, S.asm_name, CURRENT_TIMESTAMP(), FALSE
);

UPDATE `skintific-data-warehouse.sfa_step.sync_watermark`
SET last_watermark_value = CAST(CURRENT_DATE() AS STRING), last_run_at = CURRENT_TIMESTAMP(), last_run_status = 'success'
WHERE sync_table_name = 'fact_management_target';
END;

-- =============================================================================
-- 3. FULL REFRESH (fact_route_plan_pjp) — see strategy note at top of file for
-- why this table uses truncate+reload instead of MERGE.
-- =============================================================================
TRUNCATE TABLE `skintific-data-warehouse.sfa_step.fact_route_plan_pjp`;

INSERT INTO `skintific-data-warehouse.sfa_step.fact_route_plan_pjp`
  (route_plan_sk, salesman_sk, outlet_sk, source_salesman_name, source_outlet_code, distributor_code,
   distributor_name, region, asm_name, visit_day_of_week, visit_week_pattern, visit_frequency_code,
   batch_uploaded_at, sfa_step_loaded_at, is_deleted)
SELECT
  `skintific-data-warehouse.sfa_step.fn_surrogate_key`('PJP', CONCAT(p.kode_distributor, '|', p.nama_salesman, '|', p.kode_toko, '|', p.hari, '|', p.minggu)),
  sm.salesman_sk, ot.outlet_sk, p.nama_salesman, p.kode_toko, p.kode_distributor, p.nama_distributor,
  p.region, p.asm, p.hari, p.minggu, p.frekuensi, SAFE_CAST(p.uploaded_at AS TIMESTAMP), CURRENT_TIMESTAMP(), FALSE
FROM `skintific-data-warehouse.gt_schema.gt_master_salesman_pjp` p
LEFT JOIN `skintific-data-warehouse.sfa_step.dim_salesman` sm
  ON sm.source_system = 'GT_MAPPING' AND sm.salesman_name = p.nama_salesman
LEFT JOIN `skintific-data-warehouse.sfa_step.dim_outlet` ot
  ON ot.source_system = 'GT' AND ot.source_outlet_code = p.kode_toko;

-- =============================================================================
-- 4. DELETE HANDLING
-- =============================================================================

-- 4a. Soft delete (default, recommended) — already embedded in every MERGE above
-- as a separate UPDATE step. Shown standalone here for clarity / re-runnability:
UPDATE `skintific-data-warehouse.sfa_step.dim_outlet` T
SET is_deleted = TRUE, sfa_step_loaded_at = CURRENT_TIMESTAMP()
WHERE T.source_system = 'GT' AND T.is_deleted = FALSE
  AND NOT EXISTS (
    SELECT 1 FROM `skintific-data-warehouse.gt_schema.master_store_database` G WHERE G.cust_id = T.source_outlet_code
  );

-- Why soft delete, not hard delete, for dim_outlet/dim_salesman/fact_management_target:
-- STEP's Comply/Route-Compliance views aggregate historical periods (e.g. "what was
-- last month's Comply%?") — hard-deleting a row that legitimately existed during
-- that period would silently corrupt historical reporting. Soft delete preserves
-- the row for any query with an explicit as-of date filter, while vw_outlet_active /
-- vw_salesman_active hide it from "current roster" consumers.

-- 4b. Hard delete (only for fact_visit, and only past a retention window) — visit-
-- level GPS+photo-adjacent data has its own data-retention/privacy lifecycle
-- distinct from dimensional master data. Run monthly, not as part of daily sync:
DELETE FROM `skintific-data-warehouse.sfa_step.fact_visit`
WHERE is_deleted = TRUE
  AND sfa_step_loaded_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 400 DAY);

-- 4c. Deleted-flag synchronization for fact_visit (SADATA_BA) — a visit row can
-- legitimately disappear from the source (e.g. a data-quality correction at
-- source), unlike dim rows which disappear because an outlet/salesman left the
-- roster. Run after each incremental sync of fact_visit:
UPDATE `skintific-data-warehouse.sfa_step.fact_visit` T
SET is_deleted = TRUE, sfa_step_loaded_at = CURRENT_TIMESTAMP()
WHERE T.source_system = 'SADATA_BA' AND T.is_deleted = FALSE
  AND T.visit_date > (SELECT SAFE_CAST(last_watermark_value AS DATE) - 7
                       FROM `skintific-data-warehouse.sfa_step.sync_watermark`
                       WHERE sync_table_name = 'fact_visit_sadata')
  AND NOT EXISTS (
    SELECT 1 FROM `skintific-data-warehouse.sadata.fact_ba_attendance_t` V
    WHERE CONCAT(V.employee_nik, '|', V.store_id, '|', CAST(V.date AS STRING))
        = CONCAT(
            (SELECT sm.source_salesman_code FROM `skintific-data-warehouse.sfa_step.dim_salesman` sm WHERE sm.salesman_sk = T.salesman_sk),
            '|',
            (SELECT ot.source_outlet_code FROM `skintific-data-warehouse.sfa_step.dim_outlet` ot WHERE ot.outlet_sk = T.outlet_sk),
            '|', CAST(T.visit_date AS STRING)
          )
  );
-- NOTE: this re-derives the natural key from the FK lookups rather than storing the
-- natural key directly on fact_visit, which makes it more expensive than it needs
-- to be. A cheaper version: add a denormalized source_natural_key STRING column to
-- fact_visit at the next schema revision and compare directly. Left as designed
-- here (correct, not yet optimal) rather than changing ddl.sql after the fact.

-- =============================================================================
-- 5. ERROR HANDLING & RETRY
-- BigQuery scripting's BEGIN...EXCEPTION WHEN ERROR THEN pattern handles in-job
-- failures; cross-job retry (re-running a failed daily sync) is an orchestration
-- concern — recommend Airflow with exponential backoff (matching the existing
-- SFA Integration Monitor's own retry queue pattern: auto-retry, max 5 attempts,
-- escalate to Exception Center on exhaustion), not BQ-native retry logic.
-- =============================================================================
BEGIN
  DECLARE run_id STRING DEFAULT GENERATE_UUID();
  DECLARE started TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE row_count INT64;

  INSERT INTO `skintific-data-warehouse.sfa_step.sync_log` (sync_run_id, sync_table_name, started_at, status)
  VALUES (run_id, 'dim_outlet', started, 'running');

  BEGIN
    CALL `skintific-data-warehouse.sfa_step.sp_refresh_dim_outlet`();
    SET row_count = @@row_count;

    UPDATE `skintific-data-warehouse.sfa_step.sync_log`
    SET ended_at = CURRENT_TIMESTAMP(), status = 'success', rows_updated = row_count
    WHERE sync_run_id = run_id;
  EXCEPTION WHEN ERROR THEN
    UPDATE `skintific-data-warehouse.sfa_step.sync_log`
    SET ended_at = CURRENT_TIMESTAMP(), status = 'failed', error_message = @@error.message
    WHERE sync_run_id = run_id;
    -- Re-raise so the calling orchestrator (Airflow) sees the job as failed and
    -- applies its own retry/backoff policy rather than this script silently
    -- swallowing the error.
    RAISE USING MESSAGE = @@error.message;
  END;
END;
