-- =============================================================================
-- STEP — sfa_step schema DDL (BigQuery, project: skintific-data-warehouse)
-- =============================================================================
-- SCOPE (core slice, first pass — see sfa_step_architecture.md "Phase 2" for what's
-- deferred: approvals, notifications, recommendation engine, brand-group RBAC,
-- audit log). This slice covers: Outlet, Salesman, Route Plan, Visit (Call/
-- Effective Call), Management Target, SPV Target, and the Comply/Route-Compliance
-- views STEP's Dashboard/Target Management/Route Evaluate pages need.
--
-- GROUNDING — every design decision below is based on querying the REAL warehouse
-- (read-only service account, skintific-data-warehouse project), not assumed:
--   - No `sfa` dataset exists yet. SFA-Handheldv2's BigQuery code is fully wired
--     (clean schema: users/outlets/schedules/visits/visit_items, already has an
--     `effective_call` field) but has never run with MOCK_MODE=false — zero rows
--     exist. Not used as a source here; see architecture doc for the cutover plan.
--   - gt_schema.gt_salesman_mapping (317 rows) is the only GT salesman table with a
--     real, clean salesman_id. gt_schema.gt_master_salesman (318 rows) is the richer
--     attribute table but keys by NAME only — joins to gt_salesman_mapping by exact
--     name match at 247/317 = 78%. This is a real, confirmed data-quality limit, not
--     a hypothetical one — every view/procedure that does this join documents it.
--   - gt_schema.gt_master_salesman_pjp (27,915 rows, fresh — uploaded_at current as
--     of this design) is the real route-plan (PJP = Permanent Journey Plan) source.
--     It is a RECURRING pattern (day-of-week + odd/even-week + frequency code), not
--     date-exploded — modeled as such, not forced into a fake calendar grain.
--   - repsly.fact_visits_t (11,616 rows) is STALE — max date 2024-01-06, confirmed
--     by direct query. Treated as historical-only, never incrementally synced.
--   - sadata.fact_ba_attendance_t (872,873 rows, 198MB) is the largest and freshest
--     real visit-execution table (through 2025-08-07) — primary source for Call.
--   - Neither source has a reliable order-linked "Effective Call" signal today.
--     is_effective is modeled NULLable with an explicit lineage column rather than
--     fabricated from an unverified join — see fact_visit below.
--   - repsly.dim_clients_t's cross-reference columns (skintific_code, store_code_
--     sadata) are SPARSE (157/3,922 = 4% populated for store_code_sadata) — not a
--     reliable bridge. sadata.store_id and gt_schema.cust_id formats are visibly
--     incompatible by direct sampling (e.g. "12octgen8" vs "IWJA00173") — no naive
--     string-match bridge exists. dim_outlet / dim_salesman are therefore modeled as
--     FEDERATED ROSTERS (one row per source-system natural key), not auto-resolved
--     across systems — a master_entity_id column is reserved, nullable, for a future
--     manual/MDM reconciliation pass. This is a deliberate scope boundary, not an
--     oversight — flagged again in the data dictionary and architecture doc.
--   - gt_schema.fact_gt_target_v2_t (44,827 rows, 2024-01-01..2026-05-01) has the
--     REAL per-brand Management Target (skintific_target/g2g_target/timephoria_
--     target/weekly_visit_target) — 71% of rows have a non-null customer_id (the
--     rest are confirmed-by-query null/placeholder rows); every read filters on
--     customer_id IS NOT NULL.
--
-- "Don't affect existing objects": every statement below targets the NEW sfa_step
-- schema only. Nothing in this file reads-with-intent-to-modify, writes, or grants
-- on any existing dataset/table.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS `skintific-data-warehouse.sfa_step`
OPTIONS (
  location = 'US',
  description = 'STEP (Skintific Territory & Execution Platform) — core slice: outlet/salesman roster, route plan, visit execution, target & comply. Owned by STEP app team.'
);

-- =============================================================================
-- Shared UDF — deterministic surrogate key, reused by every table below so the
-- same (source_system, natural_key) always hashes to the same surrogate key across
-- every load (idempotent MERGE without a separate key-lookup table).
-- =============================================================================
CREATE OR REPLACE FUNCTION `skintific-data-warehouse.sfa_step.fn_surrogate_key`(source_system STRING, natural_key STRING)
RETURNS STRING
AS (
  TO_HEX(SHA256(CONCAT(IFNULL(source_system, ''), '|', IFNULL(natural_key, ''))))
);

-- =============================================================================
-- DIM_OUTLET
-- Purpose: federated store roster. Spine = gt_schema.master_store_database (31,261
-- rows, the richest + largest real store master, has brand/grade/geo/assignment).
-- Repsly- and sadata-sourced stores appear as their OWN rows (source_system tag),
-- not merged into the GT row, because no reliable cross-system key exists today.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.dim_outlet` (
  outlet_sk             STRING      NOT NULL OPTIONS(description="Deterministic surrogate key = fn_surrogate_key(source_system, source_outlet_code)"),
  source_system         STRING      NOT NULL OPTIONS(description="'GT' | 'REPSLY' | 'SADATA' — which source system this row's identity is anchored to"),
  source_outlet_code    STRING      NOT NULL OPTIONS(description="Natural key in the source system: cust_id (GT) / clientcode (REPSLY) / store_id (SADATA)"),
  master_entity_id      STRING               OPTIONS(description="Reserved for future cross-system MDM reconciliation. NULL until that project exists — do not assume populated."),
  store_name            STRING,
  brand                 STRING               OPTIONS(description="Skintific / Glad2Glow / Timephoria / etc — from gt_schema.master_store_database.brand where GT-sourced"),
  channel               STRING               OPTIONS(description="GT (general trade) / MT (modern trade) / BA (beauty advisor / in-store staff)"),
  store_grade           STRING               OPTIONS(description="Maps to STEP's Tier S/A/B/C/D concept — sourced from sktf_store_grade_q1_25 / g2g_store_grade_q1_25"),
  customer_category     STRING,
  region                STRING,
  distributor_code      STRING,
  distributor_name      STRING,
  asm_name              STRING,
  spv_name              STRING,
  address               STRING,
  latitude              FLOAT64,
  longitude             FLOAT64,
  operational_status    STRING,
  repsly_client_code    STRING               OPTIONS(description="Bridge to repsly.dim_clients_t.code where matched via skintific_code — sparse, do not assume populated"),
  sadata_store_id       STRING               OPTIONS(description="Bridge to sadata store_id where matched via repsly.dim_clients_t.store_code_sadata — confirmed only 4% coverage"),
  source_updated_at     TIMESTAMP            OPTIONS(description="Source system's own last-updated timestamp (input_date for GT) — NOT sfa_step_loaded_at"),
  sfa_step_loaded_at    TIMESTAMP   NOT NULL OPTIONS(description="When this row was last written by an sfa_step sync job"),
  is_deleted            BOOL        NOT NULL OPTIONS(description="Soft delete — TRUE when the source row disappeared from the latest extract"),
  PRIMARY KEY (outlet_sk) NOT ENFORCED
)
CLUSTER BY source_system, region, brand
OPTIONS (description = "Federated outlet/store roster — see file header for why this is not a single conformed dimension yet.");

-- =============================================================================
-- DIM_SALESMAN
-- Purpose: federated field-personnel roster (GT salesmen today; structurally ready
-- for BA/MT personnel and, later, SFA-Handheldv2 users once that system goes live).
-- =============================================================================
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.dim_salesman` (
  salesman_sk            STRING      NOT NULL OPTIONS(description="Deterministic surrogate key = fn_surrogate_key(source_system, source_salesman_code)"),
  source_system          STRING      NOT NULL OPTIONS(description="'GT_MAPPING' (gt_schema.gt_salesman_mapping) | 'SADATA_EMPLOYEE' (sadata.fact_ba_attendance_t employee_nik)"),
  source_salesman_code   STRING      NOT NULL OPTIONS(description="Natural key in source: salesman_id (GT_MAPPING) / employee_nik (SADATA_EMPLOYEE)"),
  master_entity_id        STRING              OPTIONS(description="Reserved for future cross-system MDM reconciliation, same caveat as dim_outlet.master_entity_id"),
  salesman_name           STRING,
  salesman_type           STRING               OPTIONS(description="From gt_salesman_mapping.salesman_type"),
  role_type               STRING      NOT NULL OPTIONS(description="'SALESMAN' | 'BA' | 'SPV' — coarse role classification"),
  distributor_code        STRING,
  region                  STRING,
  spv_name                STRING               OPTIONS(description="Enriched from gt_master_salesman by exact name match — confirmed 78% match rate (247/317), 22% will be NULL"),
  asm_name                STRING,
  is_active               BOOL,
  source_updated_at       TIMESTAMP,
  sfa_step_loaded_at      TIMESTAMP   NOT NULL,
  is_deleted              BOOL        NOT NULL,
  PRIMARY KEY (salesman_sk) NOT ENFORCED
)
CLUSTER BY source_system, region, distributor_code
OPTIONS (description = "Federated field-personnel roster — see file header for the GT_MAPPING<->gt_master_salesman name-match caveat.");

-- =============================================================================
-- FACT_ROUTE_PLAN_PJP
-- Purpose: GT's recurring journey-plan pattern (PJP), modeled at its native grain
-- (day-of-week + odd/even-week + frequency code) — NOT exploded into calendar dates,
-- because the source itself isn't date-grained. outlet_sk/salesman_sk resolution via
-- kode_toko<->cust_id and nama_salesman<->salesman is ASSUMED format-compatible by
-- visual sampling (e.g. "ICWJ01164" vs "IWJA00173" — similar shape) but NOT yet
-- confirmed by a full join — validate before first production load (see sync.sql).
-- =============================================================================
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.fact_route_plan_pjp` (
  route_plan_sk         STRING      NOT NULL OPTIONS(description="fn_surrogate_key('PJP', CONCAT(distributor_code,'|',source_salesman_code,'|',source_outlet_code,'|',visit_day_of_week,'|',visit_week_pattern))"),
  salesman_sk           STRING               OPTIONS(description="FK to dim_salesman — NULL if nama_salesman didn't resolve"),
  outlet_sk             STRING               OPTIONS(description="FK to dim_outlet — NULL if kode_toko didn't resolve"),
  source_salesman_name  STRING      NOT NULL OPTIONS(description="Raw nama_salesman, kept even when salesman_sk resolves, for audit/debugging unresolved rows"),
  source_outlet_code    STRING      NOT NULL OPTIONS(description="Raw kode_toko, same rationale"),
  distributor_code      STRING,
  distributor_name      STRING,
  region                STRING,
  asm_name              STRING,
  visit_day_of_week     STRING      OPTIONS(description="Raw Indonesian day name from source (hari) — e.g. 'Senin'"),
  visit_week_pattern    STRING      OPTIONS(description="'Minggu Ganjil' (odd) | 'Minggu Genap' (even) | other raw source value"),
  visit_frequency_code  STRING      OPTIONS(description="Raw frekuensi code, e.g. 'F2'"),
  batch_uploaded_at     TIMESTAMP   NOT NULL OPTIONS(description="Source's own uploaded_at — this table is reloaded in batches, not row-incrementally maintained at the source"),
  sfa_step_loaded_at    TIMESTAMP   NOT NULL,
  is_deleted            BOOL        NOT NULL
)
PARTITION BY DATE(batch_uploaded_at)
CLUSTER BY salesman_sk, outlet_sk
OPTIONS (description = "GT recurring route plan (PJP), native grain — see file header for the unresolved-key caveat.");

-- =============================================================================
-- FACT_VISIT  (Call / Effective Call)
-- Purpose: actual visit execution. is_call = TRUE whenever a check-in is present
-- (the visit genuinely happened — this is what "Call" means in Route Evaluate,
-- confirmed against the source Excel's own structural gap: it cannot tell "not
-- visited" from "visited, no sale" because it only has an order-Rp cell).
-- is_effective is deliberately NULLable: no table found in this investigation
-- reliably links a specific visit to a specific order at store+rep+date grain.
-- Populate it only once a real source exists (sadata fact_ba_attendance_t has no
-- order column; SFA-Handheldv2's visit_items.effective_call is purpose-built for
-- this but has zero rows today) — do not backfill with an invented heuristic.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.fact_visit` (
  visit_sk                STRING      NOT NULL OPTIONS(description="fn_surrogate_key(source_system, natural_visit_id)"),
  source_system           STRING      NOT NULL OPTIONS(description="'SADATA_BA' (live-ish, through 2025-08-07 as of this design) | 'REPSLY_HISTORICAL' (frozen, max date 2024-01-06 — historical backfill only, never incrementally synced again)"),
  source_visit_id         STRING      NOT NULL,
  salesman_sk             STRING               OPTIONS(description="FK to dim_salesman, resolved by employee_nik (SADATA_BA) or representativecode (REPSLY_HISTORICAL)"),
  outlet_sk               STRING               OPTIONS(description="FK to dim_outlet, resolved by store_id (SADATA_BA) or clientcode (REPSLY_HISTORICAL)"),
  visit_date              DATE        NOT NULL,
  check_in_at             TIMESTAMP,
  check_out_at            TIMESTAMP,
  check_in_latitude       FLOAT64,
  check_in_longitude      FLOAT64,
  check_in_distance_m     FLOAT64     OPTIONS(description="Distance in meters between check-in GPS and the outlet's registered location, where the source provides it"),
  is_call                 BOOL        NOT NULL OPTIONS(description="TRUE iff check_in_at IS NOT NULL — the visit actually happened, as opposed to merely planned"),
  is_effective             BOOL                OPTIONS(description="NULL = unknown (no reliable order-linkage source yet). Never defaulted to FALSE — NULL and FALSE mean different things downstream."),
  effective_source        STRING               OPTIONS(description="Lineage tag for is_effective once populated, e.g. 'SFA_HANDHELDV2_VISIT_ITEMS' — NULL today"),
  source_loaded_at        TIMESTAMP,
  sfa_step_loaded_at      TIMESTAMP   NOT NULL,
  is_deleted              BOOL        NOT NULL
)
PARTITION BY visit_date
CLUSTER BY salesman_sk, outlet_sk
OPTIONS (description = "Visit execution (Call/Effective Call) — see file header for the is_effective data-gap.");

-- =============================================================================
-- DIM_OUTLET_LOCATION
-- Purpose: a store's real-world GPS coordinate, derived from where field reps have
-- actually stood when checking in (fact_visit.check_in_latitude/longitude) — NOT
-- geocoded from the address text, and NOT a copy of dim_outlet.latitude/longitude
-- (which is sourced from gt_schema.master_store_database and confirmed only 12.3%
-- populated — 3,855/31,261 rows — by direct query). Deliberately a SEPARATE table
-- from dim_outlet, not a column added to it: dim_outlet's lat/long is "what the
-- master data system claims," this table is "what the field evidence shows" — they
-- can and will disagree for some stores, and collapsing them into one column would
-- destroy that signal. Use vw_outlet_location_best below for a single COALESCEd
-- answer when you just want the best available point regardless of source.
--
-- Median (via APPROX_QUANTILES), not average, of all observed check-ins per
-- outlet — robust to the occasional wildly-off GPS reading (bad fix, spoofed
-- location, rep checked in from the parking lot two blocks away) that a simple
-- AVG would let skew the result.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.dim_outlet_location` (
  outlet_sk             STRING      NOT NULL OPTIONS(description="FK to dim_outlet — one row per outlet that has at least one check-in with GPS in fact_visit"),
  latitude               FLOAT64              OPTIONS(description="Median check_in_latitude across all observed visits for this outlet"),
  longitude              FLOAT64              OPTIONS(description="Median check_in_longitude across all observed visits for this outlet"),
  source                 STRING      NOT NULL OPTIONS(description="'HANDHELD_CHECKIN_DERIVED' — always this value today; a future SFA-Handheldv2-direct source would get its own tag, not silently overwrite this one"),
  observation_count       INT64       NOT NULL OPTIONS(description="How many check-in events contributed — treat a location backed by 1-2 observations as low-confidence"),
  location_stddev_m       FLOAT64              OPTIONS(description="Approximate spread of observed check-in points around the median, in meters (haversine-based) — high spread suggests either a large venue (mall, multi-floor) or a data-quality problem, not a precise point"),
  first_observed_at       DATE,
  last_observed_at        DATE,
  sfa_step_loaded_at      TIMESTAMP   NOT NULL,
  PRIMARY KEY (outlet_sk) NOT ENFORCED
)
CLUSTER BY outlet_sk
OPTIONS (description = "Outlet GPS location derived from real handheld check-in data — see file header for why this is separate from dim_outlet.latitude/longitude.");

-- =============================================================================
-- FACT_MANAGEMENT_TARGET
-- Purpose: REAL top-down brand target, unpivoted from gt_schema.fact_gt_target_v2_t's
-- skintific_target/g2g_target/timephoria_target columns into one row per brand.
-- This replaces what the STEP prototype had to invent as a hardcoded constant
-- (MANAGEMENT_TARGET_BY_BRAND in step.js) with a real, synced source.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.fact_management_target` (
  target_sk              STRING      NOT NULL OPTIONS(description="fn_surrogate_key('GT_TARGET', CONCAT(customer_id,'|',CAST(calendar_date AS STRING),'|',brand))"),
  outlet_sk              STRING               OPTIONS(description="FK to dim_outlet — customer_id and cust_id are the SAME GT identity system, so this join is reliable (unlike the cross-system bridges elsewhere in this file)"),
  source_customer_id     STRING      NOT NULL,
  calendar_date          DATE        NOT NULL,
  brand                  STRING      NOT NULL OPTIONS(description="'Skintific' | 'Glad2Glow' | 'Timephoria' — unpivoted from the source's per-brand columns"),
  management_target_amount FLOAT64,
  weekly_visit_target    INT64,
  region                 STRING,
  distributor_name       STRING,
  spv_name               STRING,
  asm_name               STRING,
  source_loaded_at       TIMESTAMP,
  sfa_step_loaded_at     TIMESTAMP   NOT NULL,
  is_deleted             BOOL        NOT NULL
)
PARTITION BY calendar_date
CLUSTER BY brand, outlet_sk
OPTIONS (description = "Real per-brand Management Target, unpivoted from gt_schema.fact_gt_target_v2_t — filtered for customer_id IS NOT NULL at source (29% of source rows are confirmed null/placeholder).");

-- =============================================================================
-- FACT_SPV_TARGET  (STEP-native — no external source; this is genuinely new data
-- STEP introduces. No existing warehouse table captures "what target did the SPV
-- themselves propose/distribute" — only the top-down Management figure exists.)
-- =============================================================================
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.fact_spv_target` (
  spv_target_id          STRING      NOT NULL OPTIONS(description="GENERATE_UUID() at insert — STEP app writes this directly, not synced from elsewhere"),
  salesman_sk             STRING      NOT NULL OPTIONS(description="FK to dim_salesman — the SPV's own salesman_sk if a working SPV proposes their own number, otherwise the salesman being assigned a distributed share"),
  brand                   STRING      NOT NULL,
  period_month            DATE        NOT NULL OPTIONS(description="First-of-month convention, e.g. 2026-07-01 for July 2026"),
  spv_target_amount       FLOAT64     NOT NULL,
  proposed_by             STRING      NOT NULL OPTIONS(description="STEP user id/email of the SPV who proposed this figure"),
  approval_status         STRING      DEFAULT 'draft' NOT NULL OPTIONS(description="draft | submitted | approved | rejected — mirrors STEP's approval workflow"),
  created_at              TIMESTAMP   DEFAULT CURRENT_TIMESTAMP() NOT NULL,
  updated_at              TIMESTAMP,
  is_deleted              BOOL        DEFAULT FALSE NOT NULL,
  PRIMARY KEY (spv_target_id) NOT ENFORCED
)
PARTITION BY period_month
CLUSTER BY brand, salesman_sk
OPTIONS (description = "SPV-proposed target distribution — STEP-native operational table, written by the app, not synced.");

-- =============================================================================
-- VIEWS
-- =============================================================================

-- Operational view: latest active outlet roster (hides soft-deleted + dedupes to
-- the freshest row per source key — protects every downstream consumer from
-- needing to know about is_deleted/loaded_at bookkeeping).
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_outlet_active` AS
SELECT * EXCEPT(is_deleted)
FROM `skintific-data-warehouse.sfa_step.dim_outlet`
WHERE is_deleted = FALSE;

-- Operational view: same pattern for salesman.
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_salesman_active` AS
SELECT * EXCEPT(is_deleted)
FROM `skintific-data-warehouse.sfa_step.dim_salesman`
WHERE is_deleted = FALSE;

-- Operational view: single best-available location per outlet — prefers the
-- handheld-derived GPS point (real field evidence) over dim_outlet's sparse
-- master-data lat/long (12.3% populated, confirmed by direct query), falling back
-- to master data only when no check-in evidence exists yet for that outlet.
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_outlet_location_best` AS
SELECT
  o.outlet_sk,
  o.store_name,
  COALESCE(l.latitude, o.latitude) AS latitude,
  COALESCE(l.longitude, o.longitude) AS longitude,
  CASE
    WHEN l.latitude IS NOT NULL THEN 'HANDHELD_CHECKIN_DERIVED'
    WHEN o.latitude IS NOT NULL THEN 'MASTER_DATA'
    ELSE NULL
  END AS location_source,
  l.observation_count,
  l.location_stddev_m
FROM `skintific-data-warehouse.sfa_step.vw_outlet_active` o
LEFT JOIN `skintific-data-warehouse.sfa_step.dim_outlet_location` l USING (outlet_sk);

-- Reporting/Dashboard view: Target Management's Comply table —
-- Comply % = SUM(SPV Target) / SUM(Management Target) * 100, per brand+month.
-- Matches the STEP prototype's target-management.html formula exactly.
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_target_comply` AS
WITH mgmt AS (
  SELECT brand, DATE_TRUNC(calendar_date, MONTH) AS period_month, SUM(management_target_amount) AS management_target_total
  FROM `skintific-data-warehouse.sfa_step.fact_management_target`
  WHERE is_deleted = FALSE
  GROUP BY brand, period_month
),
spv AS (
  SELECT brand, period_month, SUM(spv_target_amount) AS spv_target_total
  FROM `skintific-data-warehouse.sfa_step.fact_spv_target`
  WHERE is_deleted = FALSE AND approval_status = 'approved'
  GROUP BY brand, period_month
)
SELECT
  COALESCE(m.brand, s.brand) AS brand,
  COALESCE(m.period_month, s.period_month) AS period_month,
  m.management_target_total,
  s.spv_target_total,
  SAFE_DIVIDE(s.spv_target_total, m.management_target_total) * 100 AS comply_pct,
  CASE
    WHEN m.management_target_total IS NULL OR s.spv_target_total IS NULL THEN 'No Data'
    WHEN ROUND(SAFE_DIVIDE(s.spv_target_total, m.management_target_total) * 100, 1) = 100 THEN 'Comply'
    WHEN SAFE_DIVIDE(s.spv_target_total, m.management_target_total) * 100 < 100 THEN 'Under Comply'
    ELSE 'Over Target'
  END AS comply_status
FROM mgmt m
FULL OUTER JOIN spv s USING (brand, period_month);

-- Reporting/Dashboard view: Route Compliance per salesman per ISO week —
-- planned (distinct outlets in the PJP pattern expected that week, approximated
-- from visit_frequency_code) vs actual Call from fact_visit. Documented as an
-- approximation because fact_route_plan_pjp is a recurring pattern, not a
-- per-week exploded calendar — see file header.
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_route_compliance` AS
WITH planned AS (
  SELECT salesman_sk, COUNT(DISTINCT outlet_sk) AS planned_outlets_per_week
  FROM `skintific-data-warehouse.sfa_step.fact_route_plan_pjp`
  WHERE is_deleted = FALSE AND salesman_sk IS NOT NULL AND outlet_sk IS NOT NULL
  GROUP BY salesman_sk
),
actual AS (
  SELECT
    salesman_sk,
    EXTRACT(ISOYEAR FROM visit_date) AS iso_year,
    EXTRACT(ISOWEEK FROM visit_date) AS iso_week,
    COUNT(DISTINCT outlet_sk) AS visited_outlets,
    COUNTIF(is_call) AS call_count,
    COUNTIF(is_effective) AS effective_call_count,
    COUNT(*) AS total_visit_rows
  FROM `skintific-data-warehouse.sfa_step.fact_visit`
  WHERE is_deleted = FALSE AND salesman_sk IS NOT NULL
  GROUP BY salesman_sk, iso_year, iso_week
)
SELECT
  a.salesman_sk,
  a.iso_year,
  a.iso_week,
  p.planned_outlets_per_week,
  a.visited_outlets,
  a.call_count,
  a.effective_call_count,
  SAFE_DIVIDE(a.visited_outlets, p.planned_outlets_per_week) * 100 AS route_compliance_pct,
  SAFE_DIVIDE(a.effective_call_count, a.call_count) * 100 AS effective_call_rate_pct
FROM actual a
LEFT JOIN planned p USING (salesman_sk);

-- Aggregated Summary / Dashboard view: one row per salesman, latest-known figures
-- across Comply, Route Compliance, and Effective Call Rate — the single query
-- STEP's Dashboard and Salesman 360 hexagon chart would read from.
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_salesman_360_summary` AS
SELECT
  s.salesman_sk,
  s.salesman_name,
  s.region,
  s.distributor_code,
  s.spv_name,
  rc.iso_year,
  rc.iso_week,
  rc.route_compliance_pct,
  rc.effective_call_rate_pct,
  rc.call_count,
  rc.effective_call_count
FROM `skintific-data-warehouse.sfa_step.vw_salesman_active` s
LEFT JOIN `skintific-data-warehouse.sfa_step.vw_route_compliance` rc USING (salesman_sk)
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.salesman_sk ORDER BY rc.iso_year DESC, rc.iso_week DESC) = 1;

-- =============================================================================
-- PROCEDURES — one per table, callable on a schedule (Airflow / BQ scheduled
-- query). Bodies mirror the MERGE statements in sfa_step_sync.sql exactly so the
-- two files never drift — sync.sql also keeps the raw MERGE inline for anyone
-- running it ad hoc without CALL privileges.
-- =============================================================================

CREATE OR REPLACE PROCEDURE `skintific-data-warehouse.sfa_step.sp_refresh_dim_outlet`()
BEGIN
  MERGE `skintific-data-warehouse.sfa_step.dim_outlet` T
  USING (
    SELECT
      `skintific-data-warehouse.sfa_step.fn_surrogate_key`('GT', cust_id) AS outlet_sk,
      'GT' AS source_system,
      cust_id AS source_outlet_code,
      store_name, brand,
      CASE WHEN ba_non_ba = 'BA' THEN 'BA' ELSE 'GT' END AS channel,
      COALESCE(sktf_store_grade_q1_25, g2g_store_grade_q1_25) AS store_grade,
      customer_category, region, distributor_code, distributor AS distributor_name,
      asm, spv AS spv_name, address,
      SAFE_CAST(latitude AS FLOAT64) AS latitude, SAFE_CAST(longitude AS FLOAT64) AS longitude,
      customer_status AS operational_status,
      input_date AS source_updated_at
    FROM `skintific-data-warehouse.gt_schema.master_store_database`
    WHERE cust_id IS NOT NULL
  ) S
  ON T.outlet_sk = S.outlet_sk
  WHEN MATCHED THEN UPDATE SET
    store_name = S.store_name, brand = S.brand, channel = S.channel, store_grade = S.store_grade,
    customer_category = S.customer_category, region = S.region, distributor_code = S.distributor_code,
    distributor_name = S.distributor_name, asm_name = S.asm, spv_name = S.spv_name, address = S.address,
    latitude = S.latitude, longitude = S.longitude, operational_status = S.operational_status,
    source_updated_at = S.source_updated_at, sfa_step_loaded_at = CURRENT_TIMESTAMP(), is_deleted = FALSE
  WHEN NOT MATCHED THEN INSERT (
    outlet_sk, source_system, source_outlet_code, store_name, brand, channel, store_grade,
    customer_category, region, distributor_code, distributor_name, asm_name, spv_name, address,
    latitude, longitude, operational_status, source_updated_at, sfa_step_loaded_at, is_deleted
  ) VALUES (
    S.outlet_sk, S.source_system, S.source_outlet_code, S.store_name, S.brand, S.channel, S.store_grade,
    S.customer_category, S.region, S.distributor_code, S.distributor_name, S.asm, S.spv_name, S.address,
    S.latitude, S.longitude, S.operational_status, S.source_updated_at, CURRENT_TIMESTAMP(), FALSE
  );

  -- soft-delete rows no longer present in the GT source
  UPDATE `skintific-data-warehouse.sfa_step.dim_outlet` T
  SET is_deleted = TRUE, sfa_step_loaded_at = CURRENT_TIMESTAMP()
  WHERE T.source_system = 'GT' AND T.is_deleted = FALSE
    AND NOT EXISTS (
      SELECT 1 FROM `skintific-data-warehouse.gt_schema.master_store_database` G
      WHERE G.cust_id = T.source_outlet_code
    );
END;

CREATE OR REPLACE PROCEDURE `skintific-data-warehouse.sfa_step.sp_refresh_dim_salesman`()
BEGIN
  MERGE `skintific-data-warehouse.sfa_step.dim_salesman` T
  USING (
    SELECT
      `skintific-data-warehouse.sfa_step.fn_surrogate_key`('GT_MAPPING', m.salesman_id) AS salesman_sk,
      'GT_MAPPING' AS source_system,
      m.salesman_id AS source_salesman_code,
      m.salesman AS salesman_name,
      m.salesman_type,
      'SALESMAN' AS role_type,
      m.distributor_code,
      gm.region,
      gm.nama_spv_internal AS spv_name,
      gm.asm AS asm_name,
      m.is_active,
      SAFE_CAST(m.updated_at AS TIMESTAMP) AS source_updated_at
    FROM `skintific-data-warehouse.gt_schema.gt_salesman_mapping` m
    LEFT JOIN `skintific-data-warehouse.gt_schema.gt_master_salesman` gm
      ON gm.nama_salesman = m.salesman  -- confirmed 78% match rate (247/317) — 22% will leave region/spv_name/asm_name NULL
  ) S
  ON T.salesman_sk = S.salesman_sk
  WHEN MATCHED THEN UPDATE SET
    salesman_name = S.salesman_name, salesman_type = S.salesman_type, distributor_code = S.distributor_code,
    region = S.region, spv_name = S.spv_name, asm_name = S.asm_name, is_active = S.is_active,
    source_updated_at = S.source_updated_at, sfa_step_loaded_at = CURRENT_TIMESTAMP(), is_deleted = FALSE
  WHEN NOT MATCHED THEN INSERT (
    salesman_sk, source_system, source_salesman_code, salesman_name, salesman_type, role_type,
    distributor_code, region, spv_name, asm_name, is_active, source_updated_at, sfa_step_loaded_at, is_deleted
  ) VALUES (
    S.salesman_sk, S.source_system, S.source_salesman_code, S.salesman_name, S.salesman_type, S.role_type,
    S.distributor_code, S.region, S.spv_name, S.asm_name, S.is_active, S.source_updated_at, CURRENT_TIMESTAMP(), FALSE
  );

  UPDATE `skintific-data-warehouse.sfa_step.dim_salesman` T
  SET is_deleted = TRUE, sfa_step_loaded_at = CURRENT_TIMESTAMP()
  WHERE T.source_system = 'GT_MAPPING' AND T.is_deleted = FALSE
    AND NOT EXISTS (
      SELECT 1 FROM `skintific-data-warehouse.gt_schema.gt_salesman_mapping` G
      WHERE G.salesman_id = T.source_salesman_code
    );
END;

CREATE OR REPLACE PROCEDURE `skintific-data-warehouse.sfa_step.sp_refresh_fact_management_target`()
BEGIN
  -- Self-contained, like every other sp_refresh_* procedure: look up its own
  -- watermark rather than taking a parameter, so every scheduled CALL in the
  -- Airflow DAG takes zero arguments uniformly (see sfa_step_deployment_guide.md §5).
  DECLARE refresh_since DATE;
  SET refresh_since = (
    SELECT SAFE_CAST(last_watermark_value AS DATE)
    FROM `skintific-data-warehouse.sfa_step.sync_watermark`
    WHERE sync_table_name = 'fact_management_target'
  );

  MERGE `skintific-data-warehouse.sfa_step.fact_management_target` T
  USING (
    SELECT * FROM (
      SELECT
        `skintific-data-warehouse.sfa_step.fn_surrogate_key`('GT_TARGET', CONCAT(customer_id, '|', CAST(calendar_date AS STRING), '|', 'Skintific')) AS target_sk,
        `skintific-data-warehouse.sfa_step.fn_surrogate_key`('GT', customer_id) AS outlet_sk,
        customer_id AS source_customer_id, calendar_date, 'Skintific' AS brand,
        skintific_target AS management_target_amount, weekly_visit_target, region, distributor AS distributor_name,
        spv_name, asm_name
      FROM `skintific-data-warehouse.gt_schema.fact_gt_target_v2_t`
      WHERE customer_id IS NOT NULL AND calendar_date >= refresh_since
      UNION ALL
      SELECT
        `skintific-data-warehouse.sfa_step.fn_surrogate_key`('GT_TARGET', CONCAT(customer_id, '|', CAST(calendar_date AS STRING), '|', 'Glad2Glow')),
        `skintific-data-warehouse.sfa_step.fn_surrogate_key`('GT', customer_id),
        customer_id, calendar_date, 'Glad2Glow',
        g2g_target, weekly_visit_target, region, distributor, spv_name, asm_name
      FROM `skintific-data-warehouse.gt_schema.fact_gt_target_v2_t`
      WHERE customer_id IS NOT NULL AND calendar_date >= refresh_since
      UNION ALL
      SELECT
        `skintific-data-warehouse.sfa_step.fn_surrogate_key`('GT_TARGET', CONCAT(customer_id, '|', CAST(calendar_date AS STRING), '|', 'Timephoria')),
        `skintific-data-warehouse.sfa_step.fn_surrogate_key`('GT', customer_id),
        customer_id, calendar_date, 'Timephoria',
        timephoria_target, weekly_visit_target, region, distributor, spv_name, asm_name
      FROM `skintific-data-warehouse.gt_schema.fact_gt_target_v2_t`
      WHERE customer_id IS NOT NULL AND calendar_date >= refresh_since
    )
    WHERE management_target_amount IS NOT NULL
  ) S
  ON T.target_sk = S.target_sk
  WHEN MATCHED THEN UPDATE SET
    management_target_amount = S.management_target_amount, weekly_visit_target = S.weekly_visit_target,
    region = S.region, distributor_name = S.distributor_name, spv_name = S.spv_name, asm_name = S.asm_name,
    sfa_step_loaded_at = CURRENT_TIMESTAMP(), is_deleted = FALSE
  WHEN NOT MATCHED THEN INSERT (
    target_sk, outlet_sk, source_customer_id, calendar_date, brand, management_target_amount,
    weekly_visit_target, region, distributor_name, spv_name, asm_name, sfa_step_loaded_at, is_deleted
  ) VALUES (
    S.target_sk, S.outlet_sk, S.source_customer_id, S.calendar_date, S.brand, S.management_target_amount,
    S.weekly_visit_target, S.region, S.distributor_name, S.spv_name, S.asm_name, CURRENT_TIMESTAMP(), FALSE
  );

  UPDATE `skintific-data-warehouse.sfa_step.sync_watermark`
  SET last_watermark_value = CAST(CURRENT_DATE() AS STRING), last_run_at = CURRENT_TIMESTAMP(), last_run_status = 'success'
  WHERE sync_table_name = 'fact_management_target';
END;

-- Upserts any new distinct employee_nik/store_id seen in sadata since the last run
-- — must run BEFORE sp_refresh_fact_visit_sadata in the DAG (see deployment guide
-- §5), otherwise a newly-appeared BA or store shows up in fact_visit with a NULL FK
-- exactly like the gap found and fixed in the initial full load (sync.sql 1a2/1b2).
CREATE OR REPLACE PROCEDURE `skintific-data-warehouse.sfa_step.sp_refresh_dim_sadata_entities`()
BEGIN
  MERGE `skintific-data-warehouse.sfa_step.dim_outlet` T
  USING (
    SELECT `skintific-data-warehouse.sfa_step.fn_surrogate_key`('SADATA', store_id) AS outlet_sk,
      store_id, ANY_VALUE(store_name) AS store_name, ANY_VALUE(channel_kam) AS channel, ANY_VALUE(region) AS region
    FROM `skintific-data-warehouse.sadata.fact_ba_attendance_t`
    WHERE store_id IS NOT NULL
    GROUP BY store_id
  ) S
  ON T.outlet_sk = S.outlet_sk
  WHEN NOT MATCHED THEN INSERT (outlet_sk, source_system, source_outlet_code, store_name, channel, region, sfa_step_loaded_at, is_deleted)
  VALUES (S.outlet_sk, 'SADATA', S.store_id, S.store_name, S.channel, S.region, CURRENT_TIMESTAMP(), FALSE);

  MERGE `skintific-data-warehouse.sfa_step.dim_salesman` T
  USING (
    SELECT `skintific-data-warehouse.sfa_step.fn_surrogate_key`('SADATA_EMPLOYEE', employee_nik) AS salesman_sk,
      employee_nik, ANY_VALUE(employee_name) AS salesman_name, ANY_VALUE(region) AS region
    FROM `skintific-data-warehouse.sadata.fact_ba_attendance_t`
    WHERE employee_nik IS NOT NULL
    GROUP BY employee_nik
  ) S
  ON T.salesman_sk = S.salesman_sk
  WHEN NOT MATCHED THEN INSERT (salesman_sk, source_system, source_salesman_code, salesman_name, role_type, region, sfa_step_loaded_at, is_deleted)
  VALUES (S.salesman_sk, 'SADATA_EMPLOYEE', S.employee_nik, S.salesman_name, 'BA', S.region, CURRENT_TIMESTAMP(), FALSE);
END;

CREATE OR REPLACE PROCEDURE `skintific-data-warehouse.sfa_step.sp_refresh_fact_visit_sadata`()
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

  UPDATE `skintific-data-warehouse.sfa_step.fact_visit` T
  SET is_deleted = TRUE, sfa_step_loaded_at = CURRENT_TIMESTAMP()
  WHERE T.source_system = 'SADATA_BA' AND T.is_deleted = FALSE AND T.visit_date > wm_visit - 7
    AND NOT EXISTS (
      SELECT 1 FROM `skintific-data-warehouse.sadata.fact_ba_attendance_t` V
      WHERE V.date = T.visit_date
        AND V.employee_nik = (SELECT sm.source_salesman_code FROM `skintific-data-warehouse.sfa_step.dim_salesman` sm WHERE sm.salesman_sk = T.salesman_sk)
        AND V.store_id = (SELECT ot.source_outlet_code FROM `skintific-data-warehouse.sfa_step.dim_outlet` ot WHERE ot.outlet_sk = T.outlet_sk)
    );

  UPDATE `skintific-data-warehouse.sfa_step.sync_watermark`
  SET last_watermark_value = CAST(CURRENT_DATE() AS STRING), last_run_at = CURRENT_TIMESTAMP(), last_run_status = 'success'
  WHERE sync_table_name = 'fact_visit_sadata';
END;

-- Must run AFTER sp_refresh_fact_visit_sadata in the DAG — it aggregates whatever
-- is currently in fact_visit, so running it first would compute medians against
-- stale check-in data. Full re-aggregate every run (not incremental): cheap at
-- current volume (one row per outlet with at least one visit, not per visit), and
-- correctness here matters more than speed — a partial/incremental update could
-- leave an outlet's median computed from a stale subset of its check-ins.
CREATE OR REPLACE PROCEDURE `skintific-data-warehouse.sfa_step.sp_refresh_dim_outlet_location`()
BEGIN
  MERGE `skintific-data-warehouse.sfa_step.dim_outlet_location` T
  USING (
    WITH points AS (
      SELECT outlet_sk, check_in_latitude AS lat, check_in_longitude AS lng, visit_date
      FROM `skintific-data-warehouse.sfa_step.fact_visit`
      WHERE is_deleted = FALSE AND outlet_sk IS NOT NULL
        AND check_in_latitude IS NOT NULL AND check_in_longitude IS NOT NULL
        AND check_in_latitude BETWEEN -90 AND 90 AND check_in_longitude BETWEEN -180 AND 180
    ),
    medians AS (
      SELECT
        outlet_sk,
        APPROX_QUANTILES(lat, 2)[OFFSET(1)] AS median_lat,
        APPROX_QUANTILES(lng, 2)[OFFSET(1)] AS median_lng,
        COUNT(*) AS observation_count,
        MIN(visit_date) AS first_observed_at,
        MAX(visit_date) AS last_observed_at
      FROM points
      GROUP BY outlet_sk
    ),
    spread AS (
      SELECT
        p.outlet_sk,
        STDDEV(ST_DISTANCE(ST_GEOGPOINT(p.lng, p.lat), ST_GEOGPOINT(m.median_lng, m.median_lat))) AS location_stddev_m
      FROM points p
      JOIN medians m USING (outlet_sk)
      GROUP BY p.outlet_sk
    )
    SELECT m.outlet_sk, m.median_lat, m.median_lng, m.observation_count, m.first_observed_at, m.last_observed_at, s.location_stddev_m
    FROM medians m
    LEFT JOIN spread s USING (outlet_sk)
  ) S
  ON T.outlet_sk = S.outlet_sk
  WHEN MATCHED THEN UPDATE SET
    latitude = S.median_lat, longitude = S.median_lng, observation_count = S.observation_count,
    location_stddev_m = S.location_stddev_m, first_observed_at = S.first_observed_at, last_observed_at = S.last_observed_at,
    sfa_step_loaded_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (
    outlet_sk, latitude, longitude, source, observation_count, location_stddev_m, first_observed_at, last_observed_at, sfa_step_loaded_at
  ) VALUES (
    S.outlet_sk, S.median_lat, S.median_lng, 'HANDHELD_CHECKIN_DERIVED', S.observation_count, S.location_stddev_m, S.first_observed_at, S.last_observed_at, CURRENT_TIMESTAMP()
  );
END;

-- =============================================================================
-- SALESMAN_ID_SEQ
-- Purpose: Race-safe sequence counters for salesman ID generation in the
-- salesman_pjp_v2 Streamlit app. Each (dist_code, stype) pair gets its own
-- monotonically increasing next_seq. A BQ MERGE statement (salesman_crud.py:
-- generate_salesman_id) atomically reads-and-increments next_seq, preventing
-- two concurrent "Add Salesman" submissions from getting the same ID even when
-- BQ's session isolation is weak.
--
-- Why MERGE and not MAX+1: MAX+1 over gt_schema.gt_salesman_mapping is a read
-- followed by an unguarded insert — two sessions reading the same MAX race to
-- the same ID. MERGE on this table is atomic at the row level in BQ.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.salesman_id_seq` (
  dist_code    STRING    NOT NULL OPTIONS(description="Distributor code, e.g. DST171"),
  stype        STRING    NOT NULL OPTIONS(description="Salesman type: GTI | MIX | MTI"),
  next_seq     INT64     NOT NULL OPTIONS(description="Next sequence number to assign. Incremented atomically by MERGE in salesman_crud.py:generate_salesman_id."),
  updated_at   TIMESTAMP NOT NULL OPTIONS(description="Wall-clock timestamp of last MERGE — used for debugging stale-read anomalies"),
  PRIMARY KEY (dist_code, stype) NOT ENFORCED
)
OPTIONS (description = "Monotonic sequence counters for salesman ID generation — one row per (distributor, salesman_type) combination. Never delete rows; reset next_seq manually if re-seeding is needed.");

-- =============================================================================
-- AUDIT_LOG
-- Purpose: Immutable record of every write performed by salesman_pjp_v2 and
-- (future) STEP write paths. Append-only — no UPDATE or DELETE on this table.
-- Partitioned by event_date for cost-controlled reads.
--
-- entity_type: 'SALESMAN' | 'PJP' | 'MAPPING' | 'CONFIG'
-- action:      'INSERT' | 'UPDATE' | 'DEACTIVATE' | 'PJP_UPLOAD' | 'CONFIG_SET'
-- payload_json: JSON string with before/after values — not typed to keep the
--   schema stable as the app evolves (payload structure is documented in
--   salesman_crud.py/_audit and pjp_crud.py/commit_pjp_upload).
-- =============================================================================
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.audit_log` (
  event_id      STRING    NOT NULL OPTIONS(description="UUID generated at write time: str(uuid.uuid4())"),
  event_ts      TIMESTAMP NOT NULL OPTIONS(description="CURRENT_TIMESTAMP() at write time (UTC)"),
  event_date    DATE      NOT NULL OPTIONS(description="DATE(event_ts) — partition key"),
  dist_code     STRING    NOT NULL OPTIONS(description="Distributor code of the acting user's session"),
  session_id    STRING             OPTIONS(description="8-char prefix of uuid4 from st.session_state['_v2_session_id']"),
  entity_type   STRING    NOT NULL OPTIONS(description="'SALESMAN' | 'PJP' | 'MAPPING' | 'CONFIG'"),
  action        STRING    NOT NULL OPTIONS(description="'INSERT' | 'UPDATE' | 'DEACTIVATE' | 'PJP_UPLOAD' | 'CONFIG_SET'"),
  entity_id     STRING             OPTIONS(description="salesman_id for SALESMAN/MAPPING actions; NULL for PJP_UPLOAD (scope captured in payload_json)"),
  payload_json  STRING             OPTIONS(description="JSON with context: {before, after} for UPDATE; {scope, row_count} for PJP_UPLOAD; etc.")
)
PARTITION BY event_date
CLUSTER BY dist_code, entity_type
OPTIONS (description = "Append-only audit trail for all salesman_pjp_v2 write operations. Never UPDATE or DELETE rows here.");

-- =============================================================================
-- APP_CONFIG
-- Purpose: Runtime configuration for the salesman_pjp_v2 app, editable by
-- HO Admin without redeployment. Keys are scoped per distributor (dist_code='*'
-- means global). The Streamlit app reads pjp_input_deadline from this table
-- at session start (with st.secrets as the primary source; this table as a
-- live override that HO Admin can set from the STEP Master Data UI).
--
-- Standard keys:
--   pjp_input_deadline : ISO date string (YYYY-MM-DD) — last day SPV/dist can
--                        submit PJP uploads for this monthly cycle.
--   pjp_period_open    : 'true' | 'false' — quick kill-switch independent of
--                        the deadline date; used to open/close mid-cycle.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.app_config` (
  config_key    STRING    NOT NULL OPTIONS(description="Config key, e.g. 'pjp_input_deadline', 'pjp_period_open'"),
  dist_code     STRING    NOT NULL OPTIONS(description="'*' for global; distributor code for per-dist overrides. Per-dist values take precedence over global."),
  config_value  STRING    NOT NULL OPTIONS(description="String value — cast at read time (e.g. date.fromisoformat(), bool comparison)"),
  updated_at    TIMESTAMP NOT NULL OPTIONS(description="Wall-clock timestamp of last UPDATE"),
  updated_by    STRING             OPTIONS(description="dist_code of the session that made the change — from audit perspective only"),
  description   STRING             OPTIONS(description="Human-readable note on what this key controls"),
  PRIMARY KEY (config_key, dist_code) NOT ENFORCED
)
OPTIONS (description = "Runtime configuration table for salesman_pjp_v2 / STEP write paths. HO Admin can update pjp_input_deadline here monthly without redeploying the app.");

-- Seed global defaults (run once after CREATE TABLE)
-- INSERT INTO `skintific-data-warehouse.sfa_step.app_config`
--   (config_key, dist_code, config_value, updated_at, description)
-- VALUES
--   ('pjp_input_deadline', '*', '2026-07-31', CURRENT_TIMESTAMP(), 'Last day SPV/dist can upload PJP for current cycle — update monthly'),
--   ('pjp_period_open',    '*', 'true',        CURRENT_TIMESTAMP(), 'Kill-switch: set to false to close input regardless of deadline date');

CREATE OR REPLACE PROCEDURE `skintific-data-warehouse.sfa_step.sp_reload_fact_route_plan_pjp`()
BEGIN
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
END;
