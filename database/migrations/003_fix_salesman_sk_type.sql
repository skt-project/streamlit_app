-- ============================================================
-- Migration 003: Fix sfa_web.users.salesman_sk type INT64 → STRING
-- Run in BigQuery console (one statement at a time)
-- ============================================================

-- STEP 1: Drop the INT64 column (it's NULL for all rows so no data loss)
ALTER TABLE `skintific-data-warehouse.sfa_web.users`
DROP COLUMN salesman_sk;

-- STEP 2: Re-add as STRING
ALTER TABLE `skintific-data-warehouse.sfa_web.users`
ADD COLUMN salesman_sk STRING;

-- STEP 3: Link test_se → ERNA (salesman_sk = 60a2ef484fdf77f0be82bc3a1effd132)
UPDATE `skintific-data-warehouse.sfa_web.users`
SET salesman_sk = '60a2ef484fdf77f0be82bc3a1effd132', updated_at = CURRENT_TIMESTAMP()
WHERE username = 'test_se';

-- STEP 4: Link test_spv → NURLELA (salesman_sk = 59c13ab60bc899c3a690138cabe8b5e6)
UPDATE `skintific-data-warehouse.sfa_web.users`
SET salesman_sk = '59c13ab60bc899c3a690138cabe8b5e6', updated_at = CURRENT_TIMESTAMP()
WHERE username = 'test_spv';

-- STEP 5: Verify
SELECT username, role, salesman_sk,
  CASE WHEN salesman_sk IS NULL THEN '⚠ not linked' ELSE '✓ linked' END AS status
FROM `skintific-data-warehouse.sfa_web.users`
ORDER BY role;
