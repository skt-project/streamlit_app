# STEP — Demand Monitoring Report Functional Specification

**Status:** Analysis + design, per the explicit "analyze before implementing" gate. This replaces the earlier Purchase Order / Invoicing module proposal (reverted — see [[project_step_platform]] memory) — STEP is purely a reporting/monitoring layer here, no transaction creation, editing, or workflow.

---

## 1. Business Objective

Distributor Admins currently can't see demand generated from their downline stores in one place. This report gives them: which stores/SKUs/salesmen are driving demand, how it's distributed, and whether it's trending up or down — for supply, replenishment, and sales-monitoring decisions. No write path — STEP never creates or edits a demand record, only reports on what SFA already captured.

## 2. User Stories

- As a Distributor Admin, I want to see total demand, active stores, and active SKUs for today/this week/this month, so I can gauge current activity at a glance.
- As a Distributor Admin, I want to drill from my distributor down to a specific store and see its demand history and trend, so I know which stores need replenishment attention.
- As a Distributor Admin, I want to compare this week's demand to last week's (and this month to last month), so I can tell whether a store or region is growing or declining.
- As a Distributor Admin, I want to rank my salesmen by demand generated and stores covered, so I can identify coverage gaps.
- As a Distributor Admin, I want to export any report view to Excel/CSV for offline planning.
- As a Distributor Admin, I want to see *only* my own distributor's data — never another distributor's.

---

## 3. Source Data Model Analysis

### 3.1 Demand source: `repsly.ind_purchase_orders`

Confirmed real, live, large: **13,937,216 rows, 5.7GB**, `document_date` ranging 2025-08-01 to "2056-04-05" (the latter is a confirmed data-entry typo, not real — see §3.3). One row per line item, header fields denormalized across every line of the same `purchase_order_id`. ~50-60K distinct purchase orders/month, ~1.1-1.5M line items/month.

| Column | Role |
|---|---|
| `purchase_order_id` | Groups line items into one "order" (in SFA terms, the demand captured during one visit/document) |
| `visit_id` | FK toward the visit that generated this demand (same concept validated in the earlier PO analysis pass) |
| `document_date`, `due_date` | Demand date |
| `client_code`, `client_name` | Store — FK toward `repsly.ind_dim_clients.code` |
| `representative_code`, `representative_name` | Salesman — FK toward `repsly.master_representative.code`, confirmed **93.6% match rate** (3,824 of 4,087 distinct codes in the PO data resolve) |
| `product_code`, `product_name` | SKU |
| `quantity`, `unit_price`, `discount_amount`, `tax_amount`, `total_amount` | Demand amount |
| `transaction_type` | `VPNAR` (~99.5% of rows) vs `VPNARSTO` (~0.5%) — meaning not confirmed from data alone; both are netted into demand totals rather than guessed at and filtered out (see §3.3) |
| `document_status` | **Unusable** — blank/NULL on 99.97% of rows (confirmed by direct query in the prior PO analysis pass). Per your earlier decision, every row is treated as already-finalized by construction (this is itself a downstream synced extract — "update_by: Repsly ETL Airflow" — not a live transactional store), and this gap is documented rather than papered over with an invented filter |

### 3.2 Master data

| Entity | Source | Notes |
|---|---|---|
| **Store** | `repsly.ind_dim_clients` (12,636 rows) | `territory` is a delimited hierarchy string (`IND>Region>Province>City`) — parsed, not a normalized hierarchy table. `store_category` ('GT'/'MT') = Channel. `business_region` = Region. No `subchannel` or `distributor` column exists on this table — both NULL/best-effort in this design, not fabricated. Brand-specific supervisor columns (`skt_tph_kae_spv_name` vs `g2g_fcr_kae_spv_name`) confirm the same Skintific/G2G brand-group split already built into the STEP prototype is a real org structure, not a prototype invention. **Data quality**: contains literal test rows (`store_name = 'test Store'`, `'GZ HO TEST'`) — filtered out by name pattern in the sync, not left in to inflate counts. |
| **Salesman** | `repsly.master_representative` (6,969 rows) | No direct `team`/`supervisor` column. Supervisor is **derived**, not joined: the mode (most frequent) SPV name across the stores a rep has generated demand for, computed in the fact ETL — flagged as best-effort, not a clean source field, anywhere this is surfaced. |
| **Product** | `gt_schema.master_product` (714 rows) | Clean `sku`/`category`/`brand`/`pack_size`. **Unconfirmed bridge**: `master_product.sku` format (`G2G-186`) does not visually match `ind_purchase_orders.product_code` format (`IND-SKINTIFIC-315`) — joined via `product_code` with the `IND-` prefix stripped as a best-effort transformation; the match rate is reported in the reconciliation query (sync script §4) rather than assumed. |
| **Region hierarchy** | Parsed from `ind_dim_clients.territory` | No separate normalized hierarchy table exists; building one is out of scope here (would duplicate `territory`'s information) — the daily fact tables carry `region`/`area` as flat columns derived from the string split. |

### 3.3 Data quality concerns and duplicate risk (real, measured — not hypothetical)

These are the load-bearing findings for the whole ETL design in §6/sync script:

1. **`line_no` is not a reliable item-level key.** A single `purchase_order_id` (17081666) has 129 rows all sharing `line_no = 130` — but every one of those 129 rows is a **different `product_code`** (legitimate distinct line items, e.g. `IND-NJM102001`, `IND-G2G-213`, `IND-G2G-104`, ...). This is a source-side numbering bug for large consolidated documents, not real duplication. Confirmed by checking the actual rows, not inferred from the row count alone.
2. **Genuine ETL re-sync duplicates also exist, separately.** A different group (`purchase_order_id=16911659, line_no=7`) has exactly 2 rows, **fully identical** on `product_code`/`visit_id`/`quantity`/`total_amount`, differing only by `update_time` (25 seconds apart) — this is the same source record synced twice by the "Repsly ETL Airflow" job, not a business event. A 5%-sample exact-duplicate check found 1,328 fully-identical rows (≈26,500 extrapolated across the full table).
3. **Negative quantities/amounts are real and present** (e.g. `quantity: -2.0, total_amount: -100300.0`) — almost certainly returns or cancellations folded into the same table rather than a separate document type. Net demand is computed by `SUM(quantity)` including these (negatives naturally reduce the total), not by filtering them out — excluding them would overstate gross demand and silently hide returns. This is a modeling decision, documented here so it's never mistaken for an oversight.
4. **198 rows have a `document_date` outside any sane range** (e.g. year 2056) — confirmed typos, not real future orders. Filtered with a `document_date BETWEEN '2020-01-01' AND CURRENT_DATE()` guard in every fact-building query.

**Resulting dedup rule** (implemented in the sync script, not just described here): rows are deduplicated on the full business-column tuple — `purchase_order_id, product_code, client_code, representative_code, document_date, quantity, unit_price, total_amount` — explicitly **excluding** `line_no` (unreliable, per #1) and `update_time`/`date_and_time` (volatile re-sync metadata, per #2). `QUALIFY ROW_NUMBER() OVER (PARTITION BY <business tuple> ORDER BY update_time DESC) = 1` keeps exactly one row per genuine business fact.

---

## 4. Reporting Specifications

### 4.1 Demand Overview Dashboard
Total Demand Quantity, Total Demanding Stores, Total Demanded SKUs, Total Salesmen with Demand, Demand Trend, Top Regions/Stores/SKUs by Demand — sourced from `agg_daily_distributor_demand` rolled up to whatever date range is selected (Today/Yesterday/Last 7/Last 30/Custom), never from the raw 13.9M-row table directly (see architecture doc §3 for why).

### 4.2 Store / Salesman Demand Reports, Detail Views
Per the brief's field list exactly — backed by `fact_daily_store_demand`/`fact_daily_salesman_demand` rolled up to the requested grain, joined to the store/salesman dimensions in §3.2.

### 4.3 Drill-down path
Distributor → Region → Salesman → Store → SKU — implemented as progressively-filtered queries against the same daily fact table (filter columns exist at every level: `distributor_code` is NULL/best-effort per §3.2, so in practice the top level of the real, populated hierarchy is **Region**, not Distributor, until a real distributor field is found — flagged, not hidden).

### 4.4 Comparative analytics (DoD/WoW/MoM)
Computed via self-joins of the daily/weekly/monthly aggregate views against the same view shifted by one period (`LAG()` window functions over the aggregate grain, not over raw data) — see `sfa_step_demand_report.sql` for the actual view definitions.

### 4.5 Security rules
Distributor Admin visibility is enforced at two layers: (1) the API layer resolves the logged-in admin's `distributor_code`(s) from STEP's own user/role system (not something BigQuery can know natively) and always injects that as a query parameter; (2) a BigQuery **row access policy** on the fact tables provides defense-in-depth so a query that *forgot* the filter still can't see other distributors' rows. Since `distributor_code` isn't populated on the store dimension today (§3.2), this policy currently has limited practical effect until that gap closes — documented as a real, current limitation, not hidden behind a policy that looks complete but isn't.

### 4.6 Export requirements
Excel/CSV for every list view, per the brief — an API/app-layer concern (streaming the same query result through a formatter), not a database design concern; no schema implications.

---

## 5. Forecasting readiness (not built now, designed for)

`fact_daily_store_demand`/`fact_daily_salesman_demand`/`fact_daily_sku_demand` are kept at the **finest practical grain** (day × store × SKU, day × salesman × SKU) specifically so a future forecasting/anomaly-detection job can read from them directly rather than needing to re-derive daily granularity from raw transactional data — this is the entire reason the brief's requested fact-table list exists as *daily* tables rather than going straight to weekly/monthly aggregates.

---

## 6. Related Documents

[`../database/schema/sfa_step_demand_report.sql`](../database/schema/sfa_step_demand_report.sql) · [`../database/schema/sfa_step_demand_sync.sql`](../database/schema/sfa_step_demand_sync.sql) · [`step_demand_report_api.md`](step_demand_report_api.md) · [`step_demand_report_architecture.md`](step_demand_report_architecture.md)
