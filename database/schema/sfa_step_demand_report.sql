-- =============================================================================
-- STEP — Demand Monitoring Report DDL (BigQuery, schema: sfa_step)
-- =============================================================================
-- Companion to docs/step_demand_report_functional_spec.md and
-- docs/step_demand_report_architecture.md — read those first. This is a
-- reporting-only feature: no transactional tables, no write path from STEP.
-- Nothing here has been created in BigQuery — same read-only investigation
-- account used throughout this project.
--
-- LAYERING (matches the architecture doc's recommended strategy):
--   repsly.ind_purchase_orders (raw, 13.9M rows)
--     -> stg_demand_daily (deduped, filtered, enriched — built by sync script)
--       -> fact_daily_store_demand / fact_daily_salesman_demand / fact_daily_sku_demand
--         -> agg_weekly_* / agg_monthly_* (native BigQuery MATERIALIZED VIEWs —
--            simple GROUP BY only, no window functions, per BQ's MV restrictions)
--         -> vw_rolling_7day_* / vw_rolling_30day_* / vw_*_comparison (regular
--            VIEWs with window functions — LAG()/SUM() OVER — which BigQuery
--            materialized views cannot express; safe to leave as regular views
--            since they read from the already-small daily fact tables, not raw data)
-- =============================================================================

-- =============================================================================
-- STAGING — deduplicated, filtered, enriched daily-grain layer. Physical table,
-- not a view: the dedup logic (functional spec §3.3) is expensive enough
-- (QUALIFY ROW_NUMBER over a wide business-column tuple) that doing it once here
-- and building 3 fact tables from this is cheaper than 3x repeating it.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.stg_demand_daily` (
  purchase_order_id     INT64       NOT NULL,
  product_code          STRING      NOT NULL,
  client_code           STRING,
  representative_code   STRING,
  document_date         DATE        NOT NULL,
  quantity              FLOAT64     OPTIONS(description="Net of returns/cancellations — negative values are real (transaction_type='VPNARSTO' in ~0.5% of rows) and are summed in, not filtered out. See functional spec §3.3."),
  unit_price            FLOAT64,
  total_amount          FLOAT64,
  transaction_type      STRING,
  region                STRING               OPTIONS(description="Parsed from ind_dim_clients.territory / business_region — best-effort, NULL where the store dimension itself has NULL territory (confirmed real for some rows, e.g. test stores)"),
  area                  STRING               OPTIONS(description="Parsed from territory's 3rd segment (province), where present"),
  channel               STRING               OPTIONS(description="= ind_dim_clients.store_category ('GT'/'MT')"),
  brand                 STRING               OPTIONS(description="Best-effort from master_product via product_code bridge — UNCONFIRMED join, see functional spec §3.2. NULL is expected for unmatched SKUs, not an error."),
  category              STRING               OPTIONS(description="Same caveat as brand"),
  sfa_step_loaded_at     TIMESTAMP   NOT NULL,
  PRIMARY KEY (purchase_order_id, product_code, document_date) NOT ENFORCED
)
PARTITION BY document_date
CLUSTER BY region, client_code
OPTIONS (description = "Deduplicated, date-filtered, enriched demand staging — see sfa_step_demand_sync.sql for the dedup logic. Rolling ~400-day retention, not a permanent archive (the fact tables built from this are the durable record).");

-- =============================================================================
-- FACT TABLES (daily grain — explicitly requested, and the right grain for a
-- future forecasting/anomaly-detection job to read from directly per functional
-- spec §5)
-- =============================================================================

CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.fact_daily_store_demand` (
  demand_date           DATE        NOT NULL,
  client_code           STRING      NOT NULL,
  region                STRING,
  area                  STRING,
  channel               STRING,
  demand_quantity       FLOAT64     NOT NULL OPTIONS(description="SUM(quantity), net of returns"),
  demand_amount         FLOAT64     NOT NULL,
  sku_count             INT64       NOT NULL OPTIONS(description="COUNT(DISTINCT product_code) — 'Number of SKUs Requested'"),
  transaction_count     INT64       NOT NULL OPTIONS(description="COUNT(DISTINCT purchase_order_id) — 'Number of Demand Transactions'"),
  salesman_count        INT64       NOT NULL OPTIONS(description="COUNT(DISTINCT representative_code) — usually 1 per store per day, can be >1"),
  sfa_step_loaded_at    TIMESTAMP   NOT NULL
)
PARTITION BY demand_date
CLUSTER BY region, client_code
OPTIONS (description = "Daily store-grain demand fact — the base table for Store Demand Report, Store Detail View, and all weekly/monthly/rolling store aggregates.");

CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.fact_daily_salesman_demand` (
  demand_date           DATE        NOT NULL,
  representative_code   STRING      NOT NULL,
  region                STRING,
  demand_quantity       FLOAT64     NOT NULL,
  demand_amount         FLOAT64     NOT NULL,
  store_count           INT64       NOT NULL OPTIONS(description="COUNT(DISTINCT client_code) — 'Number of Stores with Demand'"),
  sku_count             INT64       NOT NULL,
  transaction_count     INT64       NOT NULL,
  sfa_step_loaded_at    TIMESTAMP   NOT NULL
)
PARTITION BY demand_date
CLUSTER BY region, representative_code
OPTIONS (description = "Daily salesman-grain demand fact.");

CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.fact_daily_sku_demand` (
  demand_date           DATE        NOT NULL,
  product_code          STRING      NOT NULL,
  brand                 STRING,
  category              STRING,
  demand_quantity       FLOAT64     NOT NULL,
  demand_amount         FLOAT64     NOT NULL,
  store_count           INT64       NOT NULL OPTIONS(description="'Number of Stores Requesting the SKU'"),
  salesman_count        INT64       NOT NULL OPTIONS(description="'Number of Salesmen Requesting the SKU'"),
  transaction_count     INT64       NOT NULL,
  sfa_step_loaded_at    TIMESTAMP   NOT NULL
)
PARTITION BY demand_date
CLUSTER BY brand, product_code
OPTIONS (description = "Daily SKU-grain demand fact.");

-- =============================================================================
-- AGGREGATE TABLES — native BigQuery MATERIALIZED VIEWs. Simple GROUP BY only
-- (BQ MVs cannot express window functions) — built directly from the daily
-- fact tables above, auto-refreshed by BigQuery itself (no procedure needed for
-- these specifically; see sync script for the refresh cadence note).
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS `skintific-data-warehouse.sfa_step.agg_weekly_store_demand`
PARTITION BY week_start_date
CLUSTER BY region, client_code
AS
SELECT
  DATE_TRUNC(demand_date, WEEK(MONDAY)) AS week_start_date,
  client_code, region, area, channel,
  SUM(demand_quantity) AS demand_quantity,
  SUM(demand_amount) AS demand_amount,
  SUM(sku_count) AS sku_count_sum,
  SUM(transaction_count) AS transaction_count,
  COUNT(DISTINCT demand_date) AS active_days
FROM `skintific-data-warehouse.sfa_step.fact_daily_store_demand`
GROUP BY week_start_date, client_code, region, area, channel;

CREATE MATERIALIZED VIEW IF NOT EXISTS `skintific-data-warehouse.sfa_step.agg_monthly_store_demand`
PARTITION BY month_start_date
CLUSTER BY region, client_code
AS
SELECT
  DATE_TRUNC(demand_date, MONTH) AS month_start_date,
  client_code, region, area, channel,
  SUM(demand_quantity) AS demand_quantity,
  SUM(demand_amount) AS demand_amount,
  SUM(sku_count) AS sku_count_sum,
  SUM(transaction_count) AS transaction_count,
  COUNT(DISTINCT demand_date) AS active_days
FROM `skintific-data-warehouse.sfa_step.fact_daily_store_demand`
GROUP BY month_start_date, client_code, region, area, channel;

CREATE MATERIALIZED VIEW IF NOT EXISTS `skintific-data-warehouse.sfa_step.agg_weekly_salesman_demand`
PARTITION BY week_start_date
CLUSTER BY region, representative_code
AS
SELECT
  DATE_TRUNC(demand_date, WEEK(MONDAY)) AS week_start_date,
  representative_code, region,
  SUM(demand_quantity) AS demand_quantity,
  SUM(demand_amount) AS demand_amount,
  SUM(store_count) AS store_count_sum,
  SUM(transaction_count) AS transaction_count,
  COUNT(DISTINCT demand_date) AS active_days
FROM `skintific-data-warehouse.sfa_step.fact_daily_salesman_demand`
GROUP BY week_start_date, representative_code, region;

CREATE MATERIALIZED VIEW IF NOT EXISTS `skintific-data-warehouse.sfa_step.agg_monthly_salesman_demand`
PARTITION BY month_start_date
CLUSTER BY region, representative_code
AS
SELECT
  DATE_TRUNC(demand_date, MONTH) AS month_start_date,
  representative_code, region,
  SUM(demand_quantity) AS demand_quantity,
  SUM(demand_amount) AS demand_amount,
  SUM(store_count) AS store_count_sum,
  SUM(transaction_count) AS transaction_count,
  COUNT(DISTINCT demand_date) AS active_days
FROM `skintific-data-warehouse.sfa_step.fact_daily_salesman_demand`
GROUP BY month_start_date, representative_code, region;

CREATE MATERIALIZED VIEW IF NOT EXISTS `skintific-data-warehouse.sfa_step.agg_weekly_sku_demand`
PARTITION BY week_start_date
CLUSTER BY brand, product_code
AS
SELECT
  DATE_TRUNC(demand_date, WEEK(MONDAY)) AS week_start_date,
  product_code, brand, category,
  SUM(demand_quantity) AS demand_quantity,
  SUM(demand_amount) AS demand_amount,
  SUM(store_count) AS store_count_sum,
  SUM(transaction_count) AS transaction_count
FROM `skintific-data-warehouse.sfa_step.fact_daily_sku_demand`
GROUP BY week_start_date, product_code, brand, category;

CREATE MATERIALIZED VIEW IF NOT EXISTS `skintific-data-warehouse.sfa_step.agg_monthly_sku_demand`
PARTITION BY month_start_date
CLUSTER BY brand, product_code
AS
SELECT
  DATE_TRUNC(demand_date, MONTH) AS month_start_date,
  product_code, brand, category,
  SUM(demand_quantity) AS demand_quantity,
  SUM(demand_amount) AS demand_amount,
  SUM(store_count) AS store_count_sum,
  SUM(transaction_count) AS transaction_count
FROM `skintific-data-warehouse.sfa_step.fact_daily_sku_demand`
GROUP BY month_start_date, product_code, brand, category;

-- =============================================================================
-- DIMENSION VIEWS — thin reporting-specific views over existing master data.
-- Deliberately NOT new physical tables, and deliberately NOT merged into the
-- core slice's dim_outlet/dim_salesman — see architecture doc §2 for why.
-- =============================================================================

CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_dim_store_demand` AS
SELECT
  code AS client_code, store_name, business_region AS region,
  SPLIT(territory, '>')[SAFE_OFFSET(2)] AS area,
  store_category AS channel, store_grade, brand,
  COALESCE(skt_tph_kae_spv_name, g2g_fcr_kae_spv_name) AS supervisor_name_on_store
FROM `skintific-data-warehouse.repsly.ind_dim_clients`
WHERE store_name NOT LIKE '%test%' AND store_name NOT LIKE '%TEST%' AND store_name NOT LIKE '%HO Skintific%';

CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_dim_salesman_demand` AS
SELECT code AS representative_code, name AS representative_name, business_region AS region, brand, active
FROM `skintific-data-warehouse.repsly.master_representative`;

CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_dim_product_demand` AS
SELECT sku AS product_code_bridge, product_name, brand, category, pack_size
FROM `skintific-data-warehouse.gt_schema.master_product`;

-- =============================================================================
-- ROLLING METRICS — regular VIEWs (window functions, can't be BQ materialized
-- views). Read from the small daily fact tables, not raw data, so recompute-on-
-- read is cheap.
-- =============================================================================

CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_rolling_7day_store_demand` AS
SELECT
  demand_date, client_code, region,
  SUM(demand_quantity) OVER (PARTITION BY client_code ORDER BY UNIX_DATE(demand_date) RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_quantity,
  AVG(demand_quantity) OVER (PARTITION BY client_code ORDER BY UNIX_DATE(demand_date) RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_avg_quantity,
  COUNT(DISTINCT demand_date) OVER (PARTITION BY client_code ORDER BY UNIX_DATE(demand_date) RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_active_days
FROM `skintific-data-warehouse.sfa_step.fact_daily_store_demand`;

CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_rolling_30day_store_demand` AS
SELECT
  demand_date, client_code, region,
  SUM(demand_quantity) OVER (PARTITION BY client_code ORDER BY UNIX_DATE(demand_date) RANGE BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30day_quantity,
  AVG(demand_quantity) OVER (PARTITION BY client_code ORDER BY UNIX_DATE(demand_date) RANGE BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30day_avg_quantity,
  COUNT(DISTINCT demand_date) OVER (PARTITION BY client_code ORDER BY UNIX_DATE(demand_date) RANGE BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30day_active_days
FROM `skintific-data-warehouse.sfa_step.fact_daily_store_demand`;

CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_rolling_7day_sku_demand` AS
SELECT
  demand_date, product_code, brand,
  SUM(demand_quantity) OVER (PARTITION BY product_code ORDER BY UNIX_DATE(demand_date) RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_quantity,
  COUNT(DISTINCT store_count) OVER (PARTITION BY product_code ORDER BY UNIX_DATE(demand_date) RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_store_count_sum
FROM `skintific-data-warehouse.sfa_step.fact_daily_sku_demand`;

CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_rolling_30day_sku_demand` AS
SELECT
  demand_date, product_code, brand,
  SUM(demand_quantity) OVER (PARTITION BY product_code ORDER BY UNIX_DATE(demand_date) RANGE BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30day_quantity
FROM `skintific-data-warehouse.sfa_step.fact_daily_sku_demand`;

-- =============================================================================
-- COMPARATIVE ANALYTICS (DoD / WoW / MoM) — regular VIEWs, LAG() over the
-- aggregate grain (daily fact / weekly agg / monthly agg respectively), never
-- over raw data.
-- =============================================================================

CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_store_demand_dod` AS
SELECT
  demand_date, client_code, region, demand_quantity,
  LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY demand_date) AS prev_day_quantity,
  demand_quantity - LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY demand_date) AS abs_diff,
  SAFE_DIVIDE(demand_quantity - LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY demand_date),
              LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY demand_date)) * 100 AS pct_diff,
  CASE
    WHEN LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY demand_date) IS NULL THEN 'No Data'
    WHEN demand_quantity > LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY demand_date) THEN 'Increasing'
    WHEN demand_quantity < LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY demand_date) THEN 'Decreasing'
    ELSE 'Stable'
  END AS trend_direction
FROM `skintific-data-warehouse.sfa_step.fact_daily_store_demand`;

CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_store_demand_wow` AS
SELECT
  week_start_date, client_code, region, demand_quantity,
  LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY week_start_date) AS prev_week_quantity,
  demand_quantity - LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY week_start_date) AS abs_diff,
  SAFE_DIVIDE(demand_quantity - LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY week_start_date),
              LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY week_start_date)) * 100 AS pct_diff,
  CASE
    WHEN LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY week_start_date) IS NULL THEN 'No Data'
    WHEN demand_quantity > LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY week_start_date) THEN 'Increasing'
    WHEN demand_quantity < LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY week_start_date) THEN 'Decreasing'
    ELSE 'Stable'
  END AS trend_direction
FROM `skintific-data-warehouse.sfa_step.agg_weekly_store_demand`;

CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_store_demand_mom` AS
SELECT
  month_start_date, client_code, region, demand_quantity,
  LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY month_start_date) AS prev_month_quantity,
  demand_quantity - LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY month_start_date) AS abs_diff,
  SAFE_DIVIDE(demand_quantity - LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY month_start_date),
              LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY month_start_date)) * 100 AS pct_diff,
  CASE
    WHEN LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY month_start_date) IS NULL THEN 'No Data'
    WHEN demand_quantity > LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY month_start_date) THEN 'Increasing'
    WHEN demand_quantity < LAG(demand_quantity) OVER (PARTITION BY client_code ORDER BY month_start_date) THEN 'Decreasing'
    ELSE 'Stable'
  END AS trend_direction
FROM `skintific-data-warehouse.sfa_step.agg_monthly_store_demand`;

-- =============================================================================
-- DASHBOARD / KPI VIEWS
-- =============================================================================

-- Demand Overview Dashboard — Total Demand Quantity, Demanding Stores, Demanded
-- SKUs, Salesmen with Demand, for a date param the API layer applies as a filter.
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_demand_overview` AS
SELECT
  demand_date, region,
  SUM(demand_quantity) AS total_demand_quantity,
  COUNT(DISTINCT client_code) AS total_demanding_stores,
  SUM(transaction_count) AS total_demand_transactions
FROM `skintific-data-warehouse.sfa_step.fact_daily_store_demand`
GROUP BY demand_date, region;

-- KPI cards: Demand vs Yesterday/Last Week/Last Month %, in one row per date.
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_kpi_demand` AS
WITH daily AS (
  SELECT demand_date, SUM(demand_quantity) AS demand_quantity
  FROM `skintific-data-warehouse.sfa_step.fact_daily_store_demand`
  GROUP BY demand_date
)
SELECT
  demand_date, demand_quantity,
  LAG(demand_quantity, 1) OVER (ORDER BY demand_date) AS yesterday_quantity,
  SAFE_DIVIDE(demand_quantity - LAG(demand_quantity, 1) OVER (ORDER BY demand_date), LAG(demand_quantity, 1) OVER (ORDER BY demand_date)) * 100 AS pct_vs_yesterday,
  LAG(demand_quantity, 7) OVER (ORDER BY demand_date) AS last_week_same_day_quantity,
  SAFE_DIVIDE(demand_quantity - LAG(demand_quantity, 7) OVER (ORDER BY demand_date), LAG(demand_quantity, 7) OVER (ORDER BY demand_date)) * 100 AS pct_vs_last_week,
  LAG(demand_quantity, 30) OVER (ORDER BY demand_date) AS last_month_same_day_quantity,
  SAFE_DIVIDE(demand_quantity - LAG(demand_quantity, 30) OVER (ORDER BY demand_date), LAG(demand_quantity, 30) OVER (ORDER BY demand_date)) * 100 AS pct_vs_last_month
FROM daily;

-- Store/SKU/Salesman "Active Today / Weekly Avg / Monthly Avg" KPI cards.
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_kpi_active_entities` AS
SELECT
  demand_date,
  COUNT(DISTINCT client_code) AS active_stores,
  (SELECT COUNT(DISTINCT product_code) FROM `skintific-data-warehouse.sfa_step.fact_daily_sku_demand` k WHERE k.demand_date = s.demand_date) AS active_skus,
  (SELECT COUNT(DISTINCT representative_code) FROM `skintific-data-warehouse.sfa_step.fact_daily_salesman_demand` m WHERE m.demand_date = s.demand_date) AS active_salesmen
FROM `skintific-data-warehouse.sfa_step.fact_daily_store_demand` s
GROUP BY demand_date;

-- =============================================================================
-- REPORT VIEWS (List + Detail)
-- =============================================================================

-- Store Demand Report list — per the brief's exact field list. distributor is a
-- typed NULL, not a fake COUNT(0) — it's genuinely unavailable on the source
-- dimension (functional spec §3.2), and a NULL communicates "unknown" honestly
-- where a 0 would falsely imply "confirmed zero distributors".
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_store_demand_report` AS
SELECT
  d.client_code, dim.store_name, dim.region,
  CAST(NULL AS STRING) AS distributor,
  SUM(d.sku_count) AS skus_requested_sum,
  SUM(d.demand_quantity) AS total_demand_quantity,
  MAX(d.demand_date) AS last_demand_date
FROM `skintific-data-warehouse.sfa_step.fact_daily_store_demand` d
LEFT JOIN `skintific-data-warehouse.sfa_step.vw_dim_store_demand` dim ON dim.client_code = d.client_code
GROUP BY d.client_code, dim.store_name, dim.region;

-- Salesman Demand Report list. stores_with_demand is computed for real from the
-- staging grain (not the pre-aggregated daily fact, which would double-count a
-- store across multiple days if naively summed) via a per-representative
-- subquery — same pattern as vw_salesman_detail_summary below.
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_salesman_demand_report` AS
WITH store_counts AS (
  SELECT representative_code, COUNT(DISTINCT client_code) AS distinct_store_count
  FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`
  GROUP BY representative_code
)
SELECT
  m.representative_code, dim.representative_name, m.region,
  stores.distinct_store_count AS stores_with_demand,
  SUM(m.sku_count) AS skus_requested_sum,
  SUM(m.demand_quantity) AS total_demand_quantity,
  MAX(m.demand_date) AS last_demand_date
FROM `skintific-data-warehouse.sfa_step.fact_daily_salesman_demand` m
LEFT JOIN `skintific-data-warehouse.sfa_step.vw_dim_salesman_demand` dim ON dim.representative_code = m.representative_code
LEFT JOIN store_counts stores ON stores.representative_code = m.representative_code
GROUP BY m.representative_code, dim.representative_name, m.region, stores.distinct_store_count;

-- Store Detail View — demand history + top SKUs for one store (API supplies client_code).
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_store_detail_demand_history` AS
SELECT demand_date, client_code, region, demand_quantity, demand_amount, sku_count, transaction_count
FROM `skintific-data-warehouse.sfa_step.fact_daily_store_demand`;

-- Salesman Detail View — distinct store count computed from staging (day-level
-- distinct stores per salesman, correctly avoiding double-count across days that
-- the pre-aggregated fact table's per-day store_count would introduce if summed).
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_salesman_detail_summary` AS
SELECT
  representative_code,
  COUNT(DISTINCT client_code) AS total_managed_stores,
  SUM(quantity) AS total_demand_quantity,
  COUNT(DISTINCT document_date) AS active_days
FROM `skintific-data-warehouse.sfa_step.stg_demand_daily`
GROUP BY representative_code;

-- =============================================================================
-- HEATMAP — demand intensity by date x region/salesman/store, for the brief's
-- requested heatmap visualization (rendering is an app-layer concern; this view
-- supplies the (date, dimension, intensity) tuples directly).
-- =============================================================================
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_demand_heatmap_region` AS
SELECT demand_date, region, SUM(demand_quantity) AS demand_quantity,
  CASE WHEN SUM(demand_quantity) = 0 OR SUM(demand_quantity) IS NULL THEN 'No Demand'
       WHEN SUM(demand_quantity) >= 100000 THEN 'High'
       WHEN SUM(demand_quantity) >= 20000 THEN 'Medium'
       ELSE 'Low' END AS intensity_band
FROM `skintific-data-warehouse.sfa_step.fact_daily_store_demand`
GROUP BY demand_date, region;
