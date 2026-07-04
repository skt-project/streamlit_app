-- Migration 004: Fix salesman_sk / outlet_sk types INT64 → STRING
-- Reason: dim_salesman.salesman_sk and dim_outlet.outlet_sk are 32-char hex STRING hashes.
--         Migration 001 incorrectly used INT64. users.salesman_sk was fixed in migration 003.
--
-- Tables affected:
--   sfa_web.spv_target        — salesman_sk INT64 → STRING
--   sfa_web.route_assignment  — salesman_sk INT64 → STRING, outlet_sk INT64 → STRING
--
-- Run in BigQuery console: skintific-data-warehouse, dataset sfa_web.
-- Run each statement separately.

-- ── spv_target ────────────────────────────────────────────────
-- No real data yet (seed data will be re-inserted after this migration)
ALTER TABLE `skintific-data-warehouse.sfa_web.spv_target`
  DROP COLUMN salesman_sk;

ALTER TABLE `skintific-data-warehouse.sfa_web.spv_target`
  ADD COLUMN salesman_sk STRING;

-- ── route_assignment ──────────────────────────────────────────
-- No data yet (routes come from fact_route_plan_pjp, not this table)
ALTER TABLE `skintific-data-warehouse.sfa_web.route_assignment`
  DROP COLUMN salesman_sk;

ALTER TABLE `skintific-data-warehouse.sfa_web.route_assignment`
  ADD COLUMN salesman_sk STRING;

ALTER TABLE `skintific-data-warehouse.sfa_web.route_assignment`
  DROP COLUMN outlet_sk;

ALTER TABLE `skintific-data-warehouse.sfa_web.route_assignment`
  ADD COLUMN outlet_sk STRING;

-- VERIFY: Check all column types are now STRING
SELECT table_name, column_name, data_type
FROM `skintific-data-warehouse.sfa_web`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name IN ('spv_target', 'route_assignment')
  AND column_name IN ('salesman_sk', 'outlet_sk')
ORDER BY table_name, column_name;
-- Expected: all 3 rows show STRING
