-- ============================================================
-- Migration 002 – Approval & Skipped-Store Enhancements
-- Run order: execute each statement individually in BigQuery console
-- or via `bq query --use_legacy_sql=false --project_id=skintific-data-warehouse`
-- ============================================================

-- 1. Add SPV override columns to step_visit_item
--    final_qty  : SPV-adjusted quantity (NULL = use original qty)
--    sku_size   : product size label stored at submit time

ALTER TABLE `skintific-data-warehouse.sfa_web.step_visit_item`
  ADD COLUMN IF NOT EXISTS final_qty   INT64   OPTIONS(description='SPV-adjusted quantity; NULL means use original qty'),
  ADD COLUMN IF NOT EXISTS sku_size    STRING  OPTIONS(description='Product size label, e.g. 20ml');

-- 2. Download audit log — one row per PDF download event
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_web.step_visit_download_log` (
  download_id    STRING    NOT NULL OPTIONS(description='UUID primary key'),
  visit_id       STRING    NOT NULL OPTIONS(description='FK → step_visit.visit_id'),
  downloaded_by  STRING    NOT NULL OPTIONS(description='username of the downloader'),
  user_role      STRING             OPTIONS(description='role at time of download'),
  downloaded_at  TIMESTAMP NOT NULL OPTIONS(description='UTC timestamp of download')
);

-- 3. Skipped-store tracking
--    Populated by SE at end-of-day for stores that were not visited;
--    SPV can return them to the salesman or execute the visit themselves.
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_web.step_skipped_store` (
  skipped_store_id  STRING    NOT NULL OPTIONS(description='UUID PK'),
  salesman_sk       STRING    NOT NULL OPTIONS(description='FK → dim_salesman.salesman_sk'),
  outlet_sk         STRING    NOT NULL OPTIONS(description='FK → dim_outlet.outlet_sk'),
  outlet_name       STRING             OPTIONS(description='Snapshot of store name at skip time'),
  distributor_code  STRING             OPTIONS(description='Snapshot of distributor at skip time'),
  brand_group       STRING             OPTIONS(description='Salesman brand group'),
  week_iso          STRING    NOT NULL OPTIONS(description='ISO week, e.g. 2026-W28'),
  visit_date        DATE      NOT NULL OPTIONS(description='Scheduled visit date that was skipped'),
  skipped_at        TIMESTAMP NOT NULL OPTIONS(description='UTC timestamp when SE marked as skipped'),
  status            STRING    NOT NULL OPTIONS(description='PENDING_SPV | RETURNED_TO_SALESMAN | EXECUTED_BY_SPV | EXPIRED'),
  spv_action_by     STRING             OPTIONS(description='SPV username who took action'),
  spv_action_at     TIMESTAMP          OPTIONS(description='UTC timestamp of SPV action'),
  spv_notes         STRING             OPTIONS(description='Optional SPV notes'),
  executed_visit_id STRING             OPTIONS(description='visit_id when SPV executed the visit'),
  is_deleted        BOOL      NOT NULL DEFAULT FALSE,
  created_at        TIMESTAMP NOT NULL OPTIONS(description='Row insert time')
);

-- 4. Add distributor_admin role support: no schema change needed —
--    the `users` table already has `role` STRING and `distributor_code` STRING.
--    Just insert users with role='distributor_admin'.

-- Verification queries (run after migration):
-- SELECT column_name, data_type FROM `skintific-data-warehouse.sfa_web.INFORMATION_SCHEMA.COLUMNS`
--   WHERE table_name = 'step_visit_item' ORDER BY ordinal_position;
-- SELECT COUNT(*) FROM `skintific-data-warehouse.sfa_web.step_visit_download_log`;
-- SELECT COUNT(*) FROM `skintific-data-warehouse.sfa_web.step_skipped_store`;
