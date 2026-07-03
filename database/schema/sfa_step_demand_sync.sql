-- =============================================================================
-- STEP — Demand Monitoring Report synchronization scripts (BigQuery)
-- =============================================================================
-- Companion to sfa_step_demand_report.sql. Reuses the EXISTING sfa_step.
-- sync_watermark / sync_log control tables from the core slice (sfa_step_sync.sql)
-- rather than creating new ones — same rationale as dropping po_sync_log in the
-- earlier (now-reverted) PO module design: don't duplicate generic sync
-- infrastructure that already exists.
--
-- DEDUP STRATEGY (functional spec §3.3 — read that first):
--   - line_no is NOT part of the dedup key (confirmed unreliable: 129 different
--     real products sharing one line_no in a single large purchase_order_id).
--   - update_time/date_and_time ARE excluded from the dedup key (volatile
--     re-sync metadata — confirmed two fully-identical business rows differing
--     only by a 25-second update_time gap).
--   - QUALIFY ROW_NUMBER() OVER (PARTITION BY <business tuple> ORDER BY
--     update_time DESC) = 1 keeps exactly one row per genuine business fact.
--   - document_date is bounded to a sane range (2020-01-01 .. CURRENT_DATE()) —
--     198 rows confirmed with typo'd future dates (e.g. year 2056).
--   - Negative quantity/amount rows are KEPT (netted via SUM), not filtered —
--     they represent real returns/cancellations.
-- =============================================================================

-- =============================================================================
-- Seed watermark rows for this module (idempotent — MERGE, safe to re-run)
-- =============================================================================
MERGE `skintific-data-warehouse.sfa_step.sync_watermark` T
USING (
  SELECT * FROM UNNEST([
    STRUCT('stg_demand_daily' AS sync_table_name, 'document_date' AS watermark_column, CAST(DATE '2025-08-01' AS STRING) AS last_watermark_value)
  ])
) S
ON T.sync_table_name = S.sync_table_name
WHEN NOT MATCHED THEN INSERT (sync_table_name, watermark_column, last_watermark_value, last_run_at, last_run_status)
VALUES (S.sync_table_name, S.watermark_column, S.last_watermark_value, NULL, NULL);

-- =============================================================================
-- 1. FULL REFRESH — stg_demand_daily
-- Run once for the initial load, and periodically (e.g. monthly) as a full
-- reconciliation pass alongside the daily incremental (§2) — late corrections to
-- old documents (a return posted weeks after the original order) won't be
-- caught by a short incremental lookback window alone.
-- =============================================================================
CREATE OR REPLACE PROCEDURE `skintific-data-warehouse.sfa_step.sp_full_refresh_stg_demand_daily`()
BEGIN
  TRUNCATE TABLE `skintific-data-warehouse.sfa_step.stg_demand_daily`;

  INSERT INTO `skintific-data-warehouse.sfa_step.stg_demand_daily`
    (purchase_order_id, product_code, client_code, representative_code, document_date,
     quantity, unit_price, total_amount, transaction_type, region, area, channel,
     brand, category, sfa_step_loaded_at)
  SELECT
    purchase_order_id, product_code, client_code, representative_code, document_date,
    quantity, unit_price, total_amount, transaction_type, region, area, channel,
    brand, category, CURRENT_TIMESTAMP()
  FROM (
    SELECT
      po.purchase_order_id, po.product_code, po.client_code, po.representative_code, po.document_date,
      po.quantity, po.unit_price, po.total_amount, po.transaction_type,
      dc.region, dc.area, dc.channel,
      mp.brand, mp.category,
      ROW_NUMBER() OVER (
        PARTITION BY po.purchase_order_id, po.product_code, po.client_code, po.representative_code,
                     po.document_date, po.quantity, po.unit_price, po.total_amount
        ORDER BY po.update_time DESC
      ) AS rn
    FROM `skintific-data-warehouse.repsly.ind_purchase_orders` po
    LEFT JOIN `skintific-data-warehouse.sfa_step.vw_dim_store_demand` dc ON dc.client_code = po.client_code
    LEFT JOIN `skintific-data-warehouse.sfa_step.vw_dim_product_demand` mp
      ON mp.product_code_bridge = REGEXP_REPLACE(po.product_code, r'^IND-', '')  -- best-effort bridge, see functional spec §3.2 — confirm match rate via the reconciliation query (§5) before trusting brand/category downstream
    WHERE po.document_date BETWEEN '2020-01-01' AND CURRENT_DATE()
  )
  WHERE rn = 1;

  UPDATE `skintific-data-warehouse.sfa_step.sync_watermark`
  SET last_watermark_value = CAST(CURRENT_DATE() AS STRING), last_run_at = CURRENT_TIMESTAMP(), last_run_status = 'success'
  WHERE sync_table_name = 'stg_demand_daily';
END;

-- Full (re)build of the 3 daily fact tables FROM stg_demand_daily — cheap,
-- since stg_demand_daily is already deduplicated and orders of magnitude
-- smaller than raw ind_purchase_orders.
CREATE OR REPLACE PROCEDURE `skintific-data-warehouse.sfa_step.sp_full_refresh_fact_daily`()
BEGIN
  TRUNCATE TABLE `skintific-data-warehouse.sfa_step.fact_daily_store_demand`;
  INSERT INTO `skintific-data-warehouse.sfa_step.fact_daily_store_demand`
    (demand_date, client_code, region, area, channel, demand_quantity, demand_amount,
     sku_count, transaction_count, salesman_count, sfa_step_loaded_at)
  SELECT
    document_date, client_code, ANY_VALUE(region), ANY_VALUE(area), ANY_VALUE(channel),
    SUM(quantity), SUM(total_amount),
    COUNT(DISTINCT product_code), COUNT(DISTINCT purchase_order_id), COUNT(DISTINCT representative_code),
    CURRENT_TIMESTAMP()
  FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`
  WHERE client_code IS NOT NULL
  GROUP BY document_date, client_code;

  TRUNCATE TABLE `skintific-data-warehouse.sfa_step.fact_daily_salesman_demand`;
  INSERT INTO `skintific-data-warehouse.sfa_step.fact_daily_salesman_demand`
    (demand_date, representative_code, region, demand_quantity, demand_amount,
     store_count, sku_count, transaction_count, sfa_step_loaded_at)
  SELECT
    document_date, representative_code, ANY_VALUE(region),
    SUM(quantity), SUM(total_amount),
    COUNT(DISTINCT client_code), COUNT(DISTINCT product_code), COUNT(DISTINCT purchase_order_id),
    CURRENT_TIMESTAMP()
  FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`
  WHERE representative_code IS NOT NULL
  GROUP BY document_date, representative_code;

  TRUNCATE TABLE `skintific-data-warehouse.sfa_step.fact_daily_sku_demand`;
  INSERT INTO `skintific-data-warehouse.sfa_step.fact_daily_sku_demand`
    (demand_date, product_code, brand, category, demand_quantity, demand_amount,
     store_count, salesman_count, transaction_count, sfa_step_loaded_at)
  SELECT
    document_date, product_code, ANY_VALUE(brand), ANY_VALUE(category),
    SUM(quantity), SUM(total_amount),
    COUNT(DISTINCT client_code), COUNT(DISTINCT representative_code), COUNT(DISTINCT purchase_order_id),
    CURRENT_TIMESTAMP()
  FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`
  GROUP BY document_date, product_code;
END;

-- =============================================================================
-- 2. INCREMENTAL REFRESH
-- Reprocesses a trailing LOOKBACK window (not just "since last watermark") to
-- catch late-arriving corrections (a return posted against an order from a few
-- days ago) — a pure "only new rows since X" approach would miss those. 14 days
-- is a starting point, not derived from a confirmed observed correction-lag
-- distribution — tighten or widen once that's actually measured in production.
-- =============================================================================
CREATE OR REPLACE PROCEDURE `skintific-data-warehouse.sfa_step.sp_incremental_refresh_demand`(lookback_days INT64)
BEGIN
  DECLARE refresh_from DATE;
  SET refresh_from = DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY);

  -- Delete-then-reinsert the lookback window in stg_demand_daily (simpler and
  -- safer than a MERGE here, since the dedup logic re-derives the full set of
  -- winning rows for the window every time — a MERGE would need the same
  -- ROW_NUMBER() dedup subquery anyway, with no real upsert benefit at this
  -- table's size).
  DELETE FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`
  WHERE document_date >= refresh_from;

  INSERT INTO `skintific-data-warehouse.sfa_step.stg_demand_daily`
    (purchase_order_id, product_code, client_code, representative_code, document_date,
     quantity, unit_price, total_amount, transaction_type, region, area, channel,
     brand, category, sfa_step_loaded_at)
  SELECT
    purchase_order_id, product_code, client_code, representative_code, document_date,
    quantity, unit_price, total_amount, transaction_type, region, area, channel,
    brand, category, CURRENT_TIMESTAMP()
  FROM (
    SELECT
      po.purchase_order_id, po.product_code, po.client_code, po.representative_code, po.document_date,
      po.quantity, po.unit_price, po.total_amount, po.transaction_type,
      dc.region, dc.area, dc.channel, mp.brand, mp.category,
      ROW_NUMBER() OVER (
        PARTITION BY po.purchase_order_id, po.product_code, po.client_code, po.representative_code,
                     po.document_date, po.quantity, po.unit_price, po.total_amount
        ORDER BY po.update_time DESC
      ) AS rn
    FROM `skintific-data-warehouse.repsly.ind_purchase_orders` po
    LEFT JOIN `skintific-data-warehouse.sfa_step.vw_dim_store_demand` dc ON dc.client_code = po.client_code
    LEFT JOIN `skintific-data-warehouse.sfa_step.vw_dim_product_demand` mp
      ON mp.product_code_bridge = REGEXP_REPLACE(po.product_code, r'^IND-', '')
    WHERE po.document_date >= refresh_from AND po.document_date <= CURRENT_DATE()
  )
  WHERE rn = 1;

  -- Re-derive only the affected date partitions in the 3 fact tables — cheap,
  -- partition-scoped delete+insert rather than a full TRUNCATE+rebuild.
  DELETE FROM `skintific-data-warehouse.sfa_step.fact_daily_store_demand` WHERE demand_date >= refresh_from;
  INSERT INTO `skintific-data-warehouse.sfa_step.fact_daily_store_demand`
    (demand_date, client_code, region, area, channel, demand_quantity, demand_amount,
     sku_count, transaction_count, salesman_count, sfa_step_loaded_at)
  SELECT document_date, client_code, ANY_VALUE(region), ANY_VALUE(area), ANY_VALUE(channel),
    SUM(quantity), SUM(total_amount), COUNT(DISTINCT product_code), COUNT(DISTINCT purchase_order_id),
    COUNT(DISTINCT representative_code), CURRENT_TIMESTAMP()
  FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`
  WHERE document_date >= refresh_from AND client_code IS NOT NULL
  GROUP BY document_date, client_code;

  DELETE FROM `skintific-data-warehouse.sfa_step.fact_daily_salesman_demand` WHERE demand_date >= refresh_from;
  INSERT INTO `skintific-data-warehouse.sfa_step.fact_daily_salesman_demand`
    (demand_date, representative_code, region, demand_quantity, demand_amount,
     store_count, sku_count, transaction_count, sfa_step_loaded_at)
  SELECT document_date, representative_code, ANY_VALUE(region),
    SUM(quantity), SUM(total_amount), COUNT(DISTINCT client_code), COUNT(DISTINCT product_code),
    COUNT(DISTINCT purchase_order_id), CURRENT_TIMESTAMP()
  FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`
  WHERE document_date >= refresh_from AND representative_code IS NOT NULL
  GROUP BY document_date, representative_code;

  DELETE FROM `skintific-data-warehouse.sfa_step.fact_daily_sku_demand` WHERE demand_date >= refresh_from;
  INSERT INTO `skintific-data-warehouse.sfa_step.fact_daily_sku_demand`
    (demand_date, product_code, brand, category, demand_quantity, demand_amount,
     store_count, salesman_count, transaction_count, sfa_step_loaded_at)
  SELECT document_date, product_code, ANY_VALUE(brand), ANY_VALUE(category),
    SUM(quantity), SUM(total_amount), COUNT(DISTINCT client_code), COUNT(DISTINCT representative_code),
    COUNT(DISTINCT purchase_order_id), CURRENT_TIMESTAMP()
  FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`
  WHERE document_date >= refresh_from
  GROUP BY document_date, product_code;

  UPDATE `skintific-data-warehouse.sfa_step.sync_watermark`
  SET last_watermark_value = CAST(CURRENT_DATE() AS STRING), last_run_at = CURRENT_TIMESTAMP(), last_run_status = 'success'
  WHERE sync_table_name = 'stg_demand_daily';

  -- Materialized views (agg_weekly_*/agg_monthly_*) refresh automatically on a
  -- schedule BigQuery manages internally once their base table changes — no
  -- explicit REFRESH MATERIALIZED VIEW call is required, unlike some other
  -- warehouse engines. See architecture doc §3 note on MV refresh behavior.
END;

-- =============================================================================
-- 3. VALIDATION QUERIES — run after every refresh, before trusting the result.
-- =============================================================================

-- 3a. Row count sanity: staging should be dramatically smaller than raw (dedup
-- working), and should not be empty.
-- SELECT
--   (SELECT COUNT(*) FROM `skintific-data-warehouse.repsly.ind_purchase_orders`) AS raw_rows,
--   (SELECT COUNT(*) FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`) AS staged_rows;

-- 3b. No rows outside the sane date bound made it through.
-- SELECT COUNT(*) AS rows_outside_bound
-- FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`
-- WHERE document_date < '2020-01-01' OR document_date > CURRENT_DATE();

-- 3c. FK resolution rate — region/channel (from store dim) and brand/category
-- (from the unconfirmed product bridge) — report, don't silently assume.
-- SELECT
--   COUNTIF(region IS NULL) AS missing_region, COUNTIF(channel IS NULL) AS missing_channel,
--   COUNTIF(brand IS NULL) AS missing_brand_via_bridge,
--   COUNT(*) AS total
-- FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`;

-- 3d. Fact tables sum back to staging totals exactly (no rows lost/double-counted
-- in the GROUP BY rebuild).
-- SELECT
--   (SELECT SUM(quantity) FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`) AS staging_total_qty,
--   (SELECT SUM(demand_quantity) FROM `skintific-data-warehouse.sfa_step.fact_daily_store_demand`) AS store_fact_total_qty;
--   -- store_fact_total_qty will be slightly less than staging_total_qty only if
--   -- some staging rows have a NULL client_code (excluded by the WHERE clause in
--   -- sp_full_refresh_fact_daily) — any OTHER discrepancy is a real bug.

-- =============================================================================
-- 4. RECONCILIATION QUERIES — compare raw source vs staged/fact output,
-- specifically targeting the data-quality findings in functional spec §3.3.
-- =============================================================================

-- 4a. Dedup ratio — how much did deduplication actually remove? A ratio near 1.0
-- (almost nothing removed) on a later run when it was previously ~0.98 would
-- indicate the source's duplicate-generation behavior has changed and the dedup
-- key assumptions need re-checking, not just trusted forever.
-- SELECT
--   raw.row_count AS raw_row_count, staged.row_count AS staged_row_count,
--   ROUND(staged.row_count / raw.row_count, 4) AS staged_to_raw_ratio
-- FROM
--   (SELECT COUNT(*) AS row_count FROM `skintific-data-warehouse.repsly.ind_purchase_orders`
--    WHERE document_date BETWEEN '2020-01-01' AND CURRENT_DATE()) raw,
--   (SELECT COUNT(*) AS row_count FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`) staged;

-- 4b. Amount reconciliation — net total_amount should match between raw
-- (date-bounded, but NOT deduplicated) and staged (deduplicated) within the
-- expected shrinkage from removing genuine re-sync duplicates specifically —
-- a mismatch bigger than that suggests the dedup key is too aggressive (merging
-- genuinely distinct transactions) or too loose (missing real duplicates).
-- SELECT
--   (SELECT SUM(total_amount) FROM `skintific-data-warehouse.repsly.ind_purchase_orders`
--    WHERE document_date BETWEEN '2020-01-01' AND CURRENT_DATE()) AS raw_total_amount,
--   (SELECT SUM(total_amount) FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`) AS staged_total_amount;

-- 4c. product_code -> master_product bridge match rate (functional spec §3.2 —
-- confirmed unconfirmed at design time; this is how it gets reconfirmed every run).
-- SELECT
--   COUNT(*) AS total_staged_rows,
--   COUNTIF(brand IS NOT NULL) AS bridged_rows,
--   ROUND(COUNTIF(brand IS NOT NULL) / COUNT(*), 4) AS bridge_match_rate
-- FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`;

-- 4d. representative_code -> master_representative match rate (confirmed 93.6%
-- against raw data at design time — re-check against staged output).
-- SELECT
--   COUNT(DISTINCT po.representative_code) AS distinct_reps_in_staging,
--   COUNT(DISTINCT mr.code) AS matched_reps
-- FROM `skintific-data-warehouse.sfa_step.stg_demand_daily` po
-- LEFT JOIN `skintific-data-warehouse.repsly.master_representative` mr ON mr.code = po.representative_code;
