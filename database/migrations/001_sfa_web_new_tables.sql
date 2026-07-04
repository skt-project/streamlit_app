-- ============================================================
-- Migration: New sfa_web tables for STEP web app
-- Project:   skintific-data-warehouse  |  Dataset: sfa_web
--
-- HOW TO RUN:
--   Open BigQuery console → New Query
--   Paste ONE statement at a time, click Run.
--   Statements are separated by blank lines below.
--
-- SAFE: sfa_step is never touched. All writes → sfa_web only.
-- ============================================================


-- ── 1. announcement ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_web.announcement` (
  announcement_id   STRING    NOT NULL,
  type              STRING    NOT NULL,
  title             STRING    NOT NULL,
  body              STRING    NOT NULL,
  audience          STRING    NOT NULL,
  created_by        STRING    NOT NULL,
  created_at        TIMESTAMP NOT NULL,
  is_deleted        BOOL      NOT NULL
);


-- ── 2. approval_request ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_web.approval_request` (
  approval_id     STRING    NOT NULL,
  type            STRING    NOT NULL,
  title           STRING    NOT NULL,
  submitted_by    STRING    NOT NULL,
  submitted_at    TIMESTAMP NOT NULL,
  current_value   STRING,
  proposed_value  STRING    NOT NULL,
  reason          STRING    NOT NULL,
  status          STRING    NOT NULL,
  decided_by      STRING,
  decided_at      TIMESTAMP,
  comments_json   STRING,
  is_deleted      BOOL      NOT NULL
);


-- ── 3. notification ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_web.notification` (
  notification_id   STRING    NOT NULL,
  user_id           STRING    NOT NULL,
  type              STRING    NOT NULL,
  title             STRING    NOT NULL,
  body              STRING    NOT NULL,
  is_read           BOOL      NOT NULL,
  deep_link         STRING,
  created_at        TIMESTAMP NOT NULL,
  is_deleted        BOOL      NOT NULL
);


-- ── 4. spv_target ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_web.spv_target` (
  spv_target_id     STRING    NOT NULL,
  salesman_sk       STRING    NOT NULL,   -- 32-char hex hash FK → dim_salesman
  brand             STRING    NOT NULL,
  period_month      DATE      NOT NULL,
  management_target FLOAT64   NOT NULL,
  spv_target        FLOAT64   NOT NULL,
  approval_status   STRING    NOT NULL,
  created_by        STRING    NOT NULL,
  created_at        TIMESTAMP NOT NULL,
  updated_at        TIMESTAMP,
  updated_by        STRING,
  is_deleted        BOOL      NOT NULL
);


-- ── 5. route_assignment ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_web.route_assignment` (
  assignment_id   STRING    NOT NULL,
  salesman_sk     STRING    NOT NULL,   -- 32-char hex hash FK → dim_salesman
  outlet_sk       STRING    NOT NULL,   -- 32-char hex hash FK → dim_outlet
  day_of_week     STRING    NOT NULL,
  sequence_order  INT64     NOT NULL,
  week_pattern    STRING    NOT NULL,
  assigned_by     STRING    NOT NULL,
  assigned_at     TIMESTAMP NOT NULL,
  is_deleted      BOOL      NOT NULL
);


-- ── 6. Add new columns to existing users table ───────────────
-- Run each ALTER separately (BigQuery only allows one ADD COLUMN per statement)

ALTER TABLE `skintific-data-warehouse.sfa_web.users`
  ADD COLUMN IF NOT EXISTS full_name STRING;

ALTER TABLE `skintific-data-warehouse.sfa_web.users`
  ADD COLUMN IF NOT EXISTS email STRING;

ALTER TABLE `skintific-data-warehouse.sfa_web.users`
  ADD COLUMN IF NOT EXISTS salesman_sk STRING;  -- 32-char hex hash FK → dim_salesman

ALTER TABLE `skintific-data-warehouse.sfa_web.users`
  ADD COLUMN IF NOT EXISTS is_active BOOL;

ALTER TABLE `skintific-data-warehouse.sfa_web.users`
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;


-- ── 7. Backfill existing rows ─────────────────────────────────
UPDATE `skintific-data-warehouse.sfa_web.users`
SET full_name = username,
    is_active = TRUE
WHERE full_name IS NULL;
