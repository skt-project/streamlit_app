-- ============================================================
-- Migration 002: Auto-link sfa_web.users → sfa_web.dim_salesman
-- Run in BigQuery console (skintific-data-warehouse project)
-- ============================================================

-- STEP 1: Preview matches before applying (run this first, check the output)
-- Shows which users will be linked to which salesman record
SELECT
  u.user_id,
  u.username,
  u.full_name        AS user_full_name,
  u.role,
  u.salesman_sk      AS current_sk,
  s.salesman_sk      AS proposed_sk,
  s.salesman_name,
  s.brand_group
FROM `skintific-data-warehouse.sfa_web.users` u
JOIN `skintific-data-warehouse.sfa_web.dim_salesman` s
  ON LOWER(TRIM(u.full_name)) = LOWER(TRIM(s.salesman_name))
WHERE s.is_active = TRUE
  AND u.is_active = TRUE
  AND u.role IN ('se', 'spv', 'asm')
ORDER BY u.role, u.username;


-- ============================================================
-- STEP 2: Apply the auto-link (run AFTER verifying step 1)
-- Only updates users that have salesman_sk = NULL
-- ============================================================
UPDATE `skintific-data-warehouse.sfa_web.users` u
SET
  u.salesman_sk  = s.salesman_sk,
  u.updated_at   = CURRENT_TIMESTAMP()
FROM `skintific-data-warehouse.sfa_web.dim_salesman` s
WHERE LOWER(TRIM(u.full_name)) = LOWER(TRIM(s.salesman_name))
  AND s.is_active = TRUE
  AND u.is_active = TRUE
  AND u.role IN ('se', 'spv', 'asm')
  AND u.salesman_sk IS NULL;


-- ============================================================
-- STEP 3: Verify — all SE/SPV users should now have a salesman_sk
-- ============================================================
SELECT
  u.username,
  u.full_name,
  u.role,
  u.salesman_sk,
  s.salesman_name,
  CASE WHEN u.salesman_sk IS NULL THEN '⚠ NOT LINKED' ELSE '✓ linked' END AS link_status
FROM `skintific-data-warehouse.sfa_web.users` u
LEFT JOIN `skintific-data-warehouse.sfa_web.dim_salesman` s
  ON u.salesman_sk = s.salesman_sk
WHERE u.role IN ('se', 'spv', 'asm')
  AND u.is_active = TRUE
ORDER BY link_status DESC, u.role, u.username;


-- ============================================================
-- STEP 4: Manual override for any remaining unlinked users
-- (replace values as needed based on step 3 output)
-- ============================================================
-- UPDATE `skintific-data-warehouse.sfa_web.users`
-- SET salesman_sk = <integer_from_dim_salesman>, updated_at = CURRENT_TIMESTAMP()
-- WHERE username = '<username>';
