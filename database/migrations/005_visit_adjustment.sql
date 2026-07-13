-- Migration 005: Distributor Admin invoice adjustment
-- Adds a manual invoice adjustment (delivery fee, discount, promo, misc) to visits.
-- Positive amount = surcharge added to the invoice; negative = reduction/discount.
--
-- Run ONCE against BigQuery before (or after) deploying the backend build that
-- references these columns. The backend degrades gracefully if they are absent,
-- so deploy order is not strict — but the adjustment feature only persists once
-- this has run.
--
--   bq query --use_legacy_sql=false < 005_visit_adjustment.sql
--
-- Physical table is step_visit (logical alias fact_visit → step_visit).

ALTER TABLE `skintific-data-warehouse.sfa_web.step_visit`
  ADD COLUMN IF NOT EXISTS adjustment_amount FLOAT64,
  ADD COLUMN IF NOT EXISTS adjustment_note   STRING;
