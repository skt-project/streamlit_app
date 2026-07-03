-- =============================================================================
-- STEP — Store Opportunity module DDL (BigQuery, schema: sfa_step)
-- =============================================================================
-- Extension of sfa_step_demand_report.sql — that file built daily demand at
-- (store) and (sku) grain separately. This file adds the COMBINED (store × sku)
-- daily grain needed for Store Detail → SKU Drill-Down → Opportunity scoring,
-- plus the proposal tables for the Offer Proposal workflow.
--
-- Reuse strategy (per the brief's "reuse existing analytical datasets"):
--   - stg_demand_daily     → REUSED (source for both fact_daily_store_demand
--                              and the new fact_daily_store_sku_demand)
--   - fact_daily_store_demand, agg_weekly/monthly_store_demand → REUSED for
--     Store Dashboard (Page 1) and Store Performance List (Page 2)
--   - vw_dim_store_demand, vw_dim_salesman_demand → REUSED for enrichment
--   - vw_store_demand_wow/mom, vw_rolling_30day_store_demand → REUSED for
--     Store Detail trend charts (Page 3)
--   - Everything new below is additive, not a replacement.
-- =============================================================================

-- =============================================================================
-- FACT_DAILY_STORE_SKU_DEMAND
-- The "missing grain" in the existing schema: store × sku × day — needed for
-- the SKU Drill-Down (Page 4) and Opportunity Recommendation (Page 5).
-- Existing fact_daily_store_demand rolls up to store×day; existing
-- fact_daily_sku_demand rolls up to sku×day; neither has both simultaneously.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.fact_daily_store_sku_demand` (
  demand_date           DATE        NOT NULL,
  client_code           STRING      NOT NULL,
  product_code          STRING      NOT NULL,
  region                STRING,
  channel               STRING,
  brand                 STRING,
  category              STRING,
  demand_quantity       FLOAT64     NOT NULL OPTIONS(description="Net of returns — same netting logic as all other demand tables in this schema"),
  demand_amount         FLOAT64     NOT NULL,
  demand_frequency_days INT64       NOT NULL OPTIONS(description="COUNT(DISTINCT document_date) for this (store, sku) pair up to demand_date — a running/daily snapshot rather than a cumulative total; used to distinguish a one-off demand from a recurring pattern"),
  opportunity_score     FLOAT64              OPTIONS(description="0–100, distributor-relative min-max normalization of quantity/frequency/growth — see functional spec §5. Recomputed on every ETL run, not incrementally."),
  trend_direction       STRING               OPTIONS(description="Increasing | Stable | Declining — based on the most recent MoM pct_diff; Stable when abs(pct_diff) < 5%"),
  last_demand_date      DATE,
  sfa_step_loaded_at    TIMESTAMP   NOT NULL
)
PARTITION BY demand_date
CLUSTER BY client_code, product_code
OPTIONS (description = "Store × SKU daily demand grain — the base table for Pages 4 and 5 of the Store Opportunity module.");

-- =============================================================================
-- AGGREGATE TABLES — weekly and monthly rollups at store × sku grain.
-- Native BigQuery materialized views (simple GROUP BY, no window functions).
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS `skintific-data-warehouse.sfa_step.agg_weekly_store_sku_demand`
PARTITION BY week_start_date
CLUSTER BY client_code, product_code
AS
SELECT
  DATE_TRUNC(demand_date, WEEK(MONDAY)) AS week_start_date,
  client_code, product_code, region, brand, category,
  SUM(demand_quantity) AS demand_quantity,
  SUM(demand_amount) AS demand_amount,
  COUNT(DISTINCT demand_date) AS active_days
FROM `skintific-data-warehouse.sfa_step.fact_daily_store_sku_demand`
GROUP BY week_start_date, client_code, product_code, region, brand, category;

CREATE MATERIALIZED VIEW IF NOT EXISTS `skintific-data-warehouse.sfa_step.agg_monthly_store_sku_demand`
PARTITION BY month_start_date
CLUSTER BY client_code, product_code
AS
SELECT
  DATE_TRUNC(demand_date, MONTH) AS month_start_date,
  client_code, product_code, region, brand, category,
  SUM(demand_quantity) AS demand_quantity,
  SUM(demand_amount) AS demand_amount,
  COUNT(DISTINCT demand_date) AS active_days
FROM `skintific-data-warehouse.sfa_step.fact_daily_store_sku_demand`
GROUP BY month_start_date, client_code, product_code, region, brand, category;

-- =============================================================================
-- PROPOSAL TABLES (the only WRITE path in the Store Opportunity module)
-- =============================================================================

-- Proposal number sequence — a lightweight control table that tracks the latest
-- running number per (distributor_code, year_month), avoiding sequence gaps
-- from concurrent inserts without needing a Postgres SEQUENCE or OLTP-style lock.
-- The STEP backend pre-claims a number (READ then UPSERT), so a BigQuery
-- row-append pattern is safe at the concurrency level of a Distributor Admin UI.
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.proposal_seq` (
  distributor_code   STRING  NOT NULL,
  year_month         STRING  NOT NULL OPTIONS(description="Format YYYYMM, e.g. '202607'"),
  last_seq           INT64   NOT NULL,
  updated_at         TIMESTAMP NOT NULL,
  PRIMARY KEY (distributor_code, year_month) NOT ENFORCED
)
OPTIONS (description = "Proposal number sequence tracker — one row per distributor per month, last claimed running number.");

-- Recommendation threshold configuration — allows business rules tuning without a
-- schema migration or code deploy.
CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.recommendation_threshold` (
  config_id          STRING    NOT NULL OPTIONS(description="GENERATE_UUID()"),
  distributor_code   STRING             OPTIONS(description="NULL = applies to all distributors; non-null overrides for a specific distributor"),
  weight_quantity    FLOAT64 DEFAULT 0.30 NOT NULL OPTIONS(description="Weight for demand_quantity_norm in opportunity_score formula"),
  weight_frequency   FLOAT64 DEFAULT 0.25 NOT NULL OPTIONS(description="Weight for demand_frequency_norm"),
  weight_growth      FLOAT64 DEFAULT 0.25 NOT NULL OPTIONS(description="Weight for growth_pct_norm"),
  weight_weekly_avg  FLOAT64 DEFAULT 0.20 NOT NULL OPTIONS(description="Weight for weekly_avg_norm"),
  score_recommended  FLOAT64 DEFAULT 70 NOT NULL   OPTIONS(description="Minimum score for 'Recommended' classification"),
  score_potential    FLOAT64 DEFAULT 40 NOT NULL   OPTIONS(description="Minimum score for 'Potential'; below this = 'Monitor'"),
  growth_threshold   FLOAT64 DEFAULT 5 NOT NULL    OPTIONS(description="abs(growth_pct) below this = 'Stable' trend; must be positive"),
  suggested_qty_uplift FLOAT64 DEFAULT 1.1 NOT NULL  OPTIONS(description="Multiplier for Suggested Quantity = ROUND(monthly_avg × uplift)"),
  valid_from         DATE      NOT NULL,
  valid_to           DATE               OPTIONS(description="NULL = currently active"),
  created_by         STRING    NOT NULL,
  created_at         TIMESTAMP NOT NULL,
  PRIMARY KEY (config_id) NOT ENFORCED
)
OPTIONS (description = "Configurable recommendation rules — change weights and thresholds without a code deploy.");

CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.proposal_header` (
  proposal_id        STRING    NOT NULL OPTIONS(description="GENERATE_UUID()"),
  proposal_number    STRING    NOT NULL OPTIONS(description="Format: PROP/{dist_code}/{YYYYMM}/{seq:04d}, e.g. PROP/DST171/202607/0001 — unique per distributor+month; generated by fn_proposal_number()"),
  proposal_date      DATE      NOT NULL,
  valid_until        DATE      NOT NULL OPTIONS(description="proposal_date + 30 days by default; overridable before printing"),
  distributor_code   STRING    NOT NULL,
  distributor_name   STRING,
  store_code         STRING    NOT NULL OPTIONS(description="client_code from demand tables"),
  store_name         STRING,
  region             STRING,
  salesman_code      STRING,
  salesman_name      STRING,
  generated_by       STRING    NOT NULL OPTIONS(description="STEP user id of the Distributor Admin who generated this proposal"),
  notes              STRING             OPTIONS(description="Free-text proposal notes, visible in the printed document"),
  status             STRING DEFAULT 'draft' NOT NULL OPTIONS(description="draft | generated | printed | exported | cancelled"),
  total_sku_count    INT64,
  total_quantity     FLOAT64,
  print_count        INT64 DEFAULT 0 NOT NULL,
  last_printed_at    TIMESTAMP,
  created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
  updated_at         TIMESTAMP,
  is_deleted         BOOL DEFAULT FALSE NOT NULL,
  PRIMARY KEY (proposal_id) NOT ENFORCED
)
PARTITION BY proposal_date
CLUSTER BY distributor_code, store_code
OPTIONS (description = "Offer Proposal header — one row per proposal. Written by the STEP app (the only write path in this module).");

CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.proposal_item` (
  item_id              STRING    NOT NULL OPTIONS(description="GENERATE_UUID()"),
  proposal_id          STRING    NOT NULL OPTIONS(description="FK to proposal_header"),
  product_code         STRING    NOT NULL,
  product_name         STRING,
  brand                STRING,
  category             STRING,
  historical_demand    FLOAT64             OPTIONS(description="Total demand quantity for this store×sku over the look-back window used when generating the proposal"),
  weekly_avg_demand    FLOAT64,
  monthly_avg_demand   FLOAT64,
  recommended_quantity FLOAT64   NOT NULL  OPTIONS(description="Final quantity as confirmed by the Distributor Admin before printing — may differ from the system-suggested quantity"),
  opportunity_score    FLOAT64,
  trend_direction      STRING,
  item_order           INT64               OPTIONS(description="Display sequence in the printed proposal, user-reorderable"),
  created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
  PRIMARY KEY (item_id) NOT ENFORCED
)
CLUSTER BY proposal_id
OPTIONS (description = "Offer Proposal line items — one row per SKU included in the proposal.");

CREATE TABLE IF NOT EXISTS `skintific-data-warehouse.sfa_step.proposal_history` (
  history_id     STRING    NOT NULL OPTIONS(description="GENERATE_UUID()"),
  proposal_id    STRING    NOT NULL OPTIONS(description="FK to proposal_header"),
  action         STRING    NOT NULL OPTIONS(description="generated | printed | exported | cancelled | reprinted | re-exported"),
  action_by      STRING,
  action_at      TIMESTAMP NOT NULL,
  export_format  STRING             OPTIONS(description="pdf | excel — only populated for printed/exported/reprinted/re-exported actions"),
  PRIMARY KEY (history_id) NOT ENFORCED
)
PARTITION BY DATE(action_at)
CLUSTER BY proposal_id
OPTIONS (description = "Audit history of every action taken on a proposal — including reprints.");

-- =============================================================================
-- PROPOSAL UDF — deterministic proposal number, same pattern as fn_po_number
-- =============================================================================
CREATE OR REPLACE FUNCTION `skintific-data-warehouse.sfa_step.fn_proposal_number`(distributor_code STRING, proposal_date DATE, seq INT64)
RETURNS STRING
AS (
  CONCAT('PROP/', IFNULL(distributor_code, 'NA'), '/', FORMAT_DATE('%Y%m', proposal_date), '/', LPAD(CAST(seq AS STRING), 4, '0'))
);

-- =============================================================================
-- VIEWS — Store Opportunity module reporting layer
-- =============================================================================

-- Store Performance List (Page 2) — joined with dimension views for display-ready row.
-- Note: vw_dim_store_demand, vw_dim_salesman_demand, fact_daily_store_demand, and
-- agg_weekly/monthly_store_demand are defined in sfa_step_demand_report.sql.
-- This view adds the opportunity_score from the new store-sku grain.
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_store_performance_list` AS
WITH scored AS (
  SELECT
    client_code,
    AVG(opportunity_score) AS avg_opportunity_score,
    MAX(trend_direction) AS trend_direction
  FROM `skintific-data-warehouse.sfa_step.fact_daily_store_sku_demand`
  WHERE demand_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY client_code
)
SELECT
  d.client_code, dim.store_name, dim.region, dim.channel, dim.store_grade,
  dc.RepresentativeName AS salesman_name, dc.representative_code AS salesman_code,
  SUM(d.demand_quantity) AS total_demand_quantity,
  SUM(d.sku_count) AS total_skus_requested,
  MAX(d.demand_date) AS last_demand_date,
  ANY_VALUE(w.demand_quantity) AS weekly_avg_demand,
  ANY_VALUE(m.demand_quantity) AS monthly_avg_demand,
  COALESCE(sc.avg_opportunity_score, 0) AS opportunity_score,
  COALESCE(sc.trend_direction, 'No Data') AS trend_direction
FROM `skintific-data-warehouse.sfa_step.fact_daily_store_demand` d
LEFT JOIN `skintific-data-warehouse.sfa_step.vw_dim_store_demand` dim ON dim.client_code = d.client_code
LEFT JOIN `skintific-data-warehouse.repsly.ind_dim_clients` dc ON dc.code = d.client_code
LEFT JOIN (
  SELECT client_code, demand_quantity FROM `skintific-data-warehouse.sfa_step.agg_weekly_store_demand`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY client_code ORDER BY week_start_date DESC) = 1
) w ON w.client_code = d.client_code
LEFT JOIN (
  SELECT client_code, demand_quantity FROM `skintific-data-warehouse.sfa_step.agg_monthly_store_demand`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY client_code ORDER BY month_start_date DESC) = 1
) m ON m.client_code = d.client_code
LEFT JOIN scored sc ON sc.client_code = d.client_code
GROUP BY d.client_code, dim.store_name, dim.region, dim.channel, dim.store_grade,
         dc.RepresentativeName, dc.representative_code, sc.avg_opportunity_score, sc.trend_direction;

-- SKU Drill-Down (Page 4) — per store × sku, joined with product master.
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_store_sku_drilldown` AS
WITH latest_weekly AS (
  SELECT client_code, product_code, demand_quantity AS weekly_avg
  FROM `skintific-data-warehouse.sfa_step.agg_weekly_store_sku_demand`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY client_code, product_code ORDER BY week_start_date DESC) = 1
),
latest_monthly AS (
  SELECT client_code, product_code, demand_quantity AS monthly_avg
  FROM `skintific-data-warehouse.sfa_step.agg_monthly_store_sku_demand`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY client_code, product_code ORDER BY month_start_date DESC) = 1
),
sku_totals AS (
  SELECT
    client_code, product_code,
    SUM(demand_quantity) AS total_demand_quantity,
    COUNT(DISTINCT demand_date) AS demand_frequency_days,
    MAX(demand_date) AS last_demand_date,
    AVG(opportunity_score) AS opportunity_score,
    ANY_VALUE(trend_direction) AS trend_direction,
    ANY_VALUE(brand) AS brand,
    ANY_VALUE(category) AS category
  FROM `skintific-data-warehouse.sfa_step.fact_daily_store_sku_demand`
  GROUP BY client_code, product_code
)
SELECT
  t.client_code, t.product_code, p.product_name, t.brand, t.category,
  t.total_demand_quantity, t.demand_frequency_days, t.last_demand_date,
  COALESCE(w.weekly_avg, 0) AS weekly_avg_demand,
  COALESCE(m.monthly_avg, 0) AS monthly_avg_demand,
  t.opportunity_score, t.trend_direction
FROM sku_totals t
LEFT JOIN `skintific-data-warehouse.sfa_step.vw_dim_product_demand` p ON p.product_code_bridge = REGEXP_REPLACE(t.product_code, r'^IND-', '')
LEFT JOIN latest_weekly w ON w.client_code = t.client_code AND w.product_code = t.product_code
LEFT JOIN latest_monthly m ON m.client_code = t.client_code AND m.product_code = t.product_code;

-- Opportunity Recommendation (Page 5) — classified tiers from the shared view above.
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_opportunity_recommendations` AS
WITH config AS (
  SELECT score_recommended, score_potential, suggested_qty_uplift
  FROM `skintific-data-warehouse.sfa_step.recommendation_threshold`
  WHERE distributor_code IS NULL AND (valid_to IS NULL OR valid_to >= CURRENT_DATE())
  ORDER BY valid_from DESC LIMIT 1
)
SELECT
  d.client_code, d.product_code, d.product_name, d.brand, d.category,
  d.total_demand_quantity, d.demand_frequency_days, d.weekly_avg_demand, d.monthly_avg_demand,
  d.opportunity_score, d.trend_direction, d.last_demand_date,
  CASE
    WHEN d.opportunity_score >= c.score_recommended AND d.trend_direction = 'Increasing' THEN 'Recommended'
    WHEN d.opportunity_score >= c.score_potential THEN 'Potential'
    ELSE 'Monitor'
  END AS recommendation_class,
  ROUND(d.monthly_avg_demand * c.suggested_qty_uplift) AS suggested_quantity
FROM `skintific-data-warehouse.sfa_step.vw_store_sku_drilldown` d
CROSS JOIN config c;

-- Proposal List (for Proposal History page)
CREATE OR REPLACE VIEW `skintific-data-warehouse.sfa_step.vw_proposal_list` AS
SELECT
  h.proposal_id, h.proposal_number, h.proposal_date, h.valid_until,
  h.distributor_code, h.distributor_name, h.store_code, h.store_name,
  h.region, h.salesman_name, h.total_sku_count, h.total_quantity,
  h.generated_by, h.status, h.print_count, h.last_printed_at,
  h.notes, h.created_at, h.updated_at
FROM `skintific-data-warehouse.sfa_step.proposal_header` h
WHERE h.is_deleted = FALSE;
