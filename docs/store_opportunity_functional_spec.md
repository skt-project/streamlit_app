# STEP — Store Opportunity Module Functional Specification

**Status:** Design phase. This document covers the full functional specification for the new `Store Opportunity` top-level module and the complementary restructure of the existing `Reports` menu.

---

## 1. Navigation Restructure Rationale

**Before:**
```
Dashboard | Route Evaluate | Route Planner | Store & Salesman | Manajemen Target | Reports | Administrasi
```

**After (this brief):**
```
Dashboard | Store Opportunity | Route Evaluate | Route Planner | Store & Salesman | Manajemen Target | Reports | Master Data | Administration
```

**Why:** "Store Performance" analytics inside a generic Reports menu requires a Distributor Admin to navigate two levels deep just to reach actionable demand data. The Offer Proposal feature (a downstream sales action) was similarly disconnected — a standalone module with no visual link to the demand data that should trigger it. Merging both into a single `Store Opportunity` module makes the full workflow linear: monitor → analyze → recommend → propose, without ever leaving one area of the app. `Reports` is kept but repositioned as purely analytical/informational (executive summaries, cross-channel trend views, territory analytics) — no operational workflows, no action triggers.

---

## 2. User Persona

**Distributor Admin** — operates under a specific distributor code (e.g. `DST171`), has direct visibility into their own stores and salesmen, wants to understand which stores are demanding which products and quickly generate a commercial offer proposal for a salesman to take to a high-opportunity store. Distributor scope is enforced at every layer — this user never sees another distributor's data.

---

## 3. Module Structure

```
Store Opportunity
├── Page 1 — Store Dashboard (overview / KPI cards)
├── Page 2 — Store Performance List (searchable, sortable table)
├── Page 3 — Store Detail (profile + demand summary + trend)
│   └── Page 4 — SKU Drill-Down (expandable from Store Detail)
├── Page 5 — Opportunity Recommendation (auto-classified SKUs + scoring)
└── Page 6 — Offer Proposal (generate → preview → print/export)
    └── Proposal History (searchable, reprintable)
```

---

## 4. Page Specifications

### Page 1 — Store Dashboard

**Purpose:** At-a-glance demand health for this Distributor Admin's distributor.

**KPI cards:**
| KPI | Source |
|---|---|
| Total Stores with Demand | `COUNT(DISTINCT client_code)` from `fact_daily_store_demand` for date range |
| Total Demand Quantity | `SUM(demand_quantity)` |
| Total Demanded SKUs | `COUNT(DISTINCT product_code)` from `fact_daily_sku_demand` |
| Weekly Average Demand | `demand_quantity / active_weeks` from `agg_weekly_store_demand` |
| Monthly Average Demand | from `agg_monthly_store_demand` |
| Stores with Growing Demand | `COUNT` where WoW `pct_diff > 0` via `vw_store_demand_wow` |
| Stores with Declining Demand | `COUNT` where WoW `pct_diff < 0` |

**Filters (applied to all Page 1 content):** Date Range (Today/Last 7/Last 30/Custom), Distributor (admin scope — always pre-filtered, displayed for context), Region, Area, Salesman, Channel, Store.

---

### Page 2 — Store Performance List

**Purpose:** Ranked, searchable list of stores — the primary operational view for a Distributor Admin scanning for high-opportunity stores.

**Columns:**
| Column | Source |
|---|---|
| Store Name | `vw_dim_store_demand.store_name` |
| Store Code | `client_code` |
| Salesman | resolved from `ind_dim_clients.RepresentativeName` |
| Region | `region` |
| Total Demand Quantity | `SUM(demand_quantity)` |
| Number of Requested SKUs | `SUM(sku_count)` / distinct from staging |
| Last Demand Date | `MAX(demand_date)` |
| Weekly Average Demand | from `agg_weekly_store_demand` |
| Monthly Average Demand | from `agg_monthly_store_demand` |
| **Opportunity Score** | Computed — see §5 |

**Features:** Search (store name/code/salesman), Filter (region/channel/date), Sort (any column), Pagination (50/page default, 200 max), Export (Excel/CSV — same endpoint and format as `GET /demand/export`).

**Row click → Page 3 (Store Detail).**

---

### Page 3 — Store Detail

**Purpose:** Full demand profile for one store — the bridge between analysis and action.

**Sections:**

**Store Profile**
- Store Name, Store Code, Distributor, Region, Area, Channel, Store Grade
- Assigned Salesman: name, contact info

**Demand Summary** (for selected date range)
- Total Demand Quantity, Demanded SKUs, Weekly/Monthly Average, Last Demand Date

**Demand Trends**
- Daily demand quantity line chart (from `fact_daily_store_demand`)
- Weekly demand bar chart (from `agg_weekly_store_demand`)
- Monthly demand bar chart (from `agg_monthly_store_demand`)
- Growth indicators: DoD %, WoW %, MoM % (from `vw_store_demand_dod/wow/mom`)

**Action buttons:** `View SKU Details` (→ Page 4), `Generate Opportunity` (→ Page 5), `Create Proposal` (→ Page 6).

---

### Page 4 — SKU Drill-Down

**Purpose:** Which specific products is this store demanding, at what frequency, and at what growth trajectory?

**Display (one row per SKU per store):**
| Column | Source |
|---|---|
| SKU Code | `product_code` |
| SKU Name | `product_name` via master_product |
| Brand | `brand` |
| Category | `category` |
| Demand Quantity | `SUM(quantity)` from `stg_demand_daily` per store+sku |
| Demand Frequency (days with demand) | `COUNT(DISTINCT document_date)` |
| Weekly Average Demand | from `agg_weekly_store_sku_demand` |
| Monthly Average Demand | from `agg_monthly_store_sku_demand` |
| **Growth %** | WoW or MoM, from the sku-store comparative view |
| **Opportunity Score** | see §5 |
| Last Demand Date | `MAX(document_date)` |

**Features:** Search (SKU code/name), Filter (brand/category/date range), Sort (any column), Ranking (by demand_quantity / opportunity_score), Export.

---

### Page 5 — Opportunity Recommendation

**Purpose:** Auto-classify each store's SKUs into commercial opportunity tiers, with configurable thresholds and generated reasons — the analytical bridge to proposal creation.

**Classification logic:**

| Class | Label | Condition (defaults — all configurable via `recommendation_threshold`) |
|---|---|---|
| **A** | Recommended | `opportunity_score >= 70` AND `growth_pct > 0` |
| **B** | Potential | `opportunity_score >= 40` AND `opportunity_score < 70` |
| **C** | Monitor | `opportunity_score < 40` OR `demand_frequency_days <= 1` |

**Opportunity Score formula (default weights, configurable):**
```
opportunity_score =
    (demand_quantity_norm × 0.30)
  + (demand_frequency_norm × 0.25)
  + (growth_pct_norm × 0.25)
  + (weekly_avg_norm × 0.20)
```
Where each `_norm` is min-max normalized within the current distributor's store-sku universe (so "high demand" is relative to peers, not absolute values).

**Recommendation Reason** (generated as a human-readable string, e.g.):
- "Strong demand (450 units) with consistent weekly ordering (4/4 weeks active) and +23% MoM growth."
- "Moderate demand but flat trend — may benefit from a targeted offer."
- "Single occurrence demand — monitor before prioritizing."

**Suggested Quantity:** `ROUND(monthly_avg × 1.1)` by default (10% uplift) — override per SKU before generating proposal.

**Action:** Select SKUs → `Generate Offer Proposal` (→ Page 6).

---

### Page 6 — Offer Proposal

**Workflow:**
```
Store Performance (Page 2)
    ↓ Click store
Store Detail (Page 3)
    ↓ "View SKU Details" or "Generate Opportunity"
SKU Demand Analysis (Page 4) → Opportunity Recommendation (Page 5)
    ↓ Select SKUs + adjust quantities
Generate Offer Proposal
    ↓ Preview
Print / Export PDF
```

**Proposal generation inputs:**
- Pre-populated from the selected store's Page 5 output
- Distributor Admin can adjust: remove/add SKUs, override recommended quantities, add notes

**Header fields (from `proposal_header`):**
- Distributor Logo (configurable per distributor), Distributor Name/Address, Proposal Number (auto-generated: `PROP/{dist_code}/{YYYYMM}/{seq}`), Proposal Date, Valid Until (default +30 days)

**Store Information block:** Store Name, Store Code, Region, Assigned Salesman

**Product Table (from `proposal_item`):** SKU Code, SKU Name, Historical Demand Quantity, Weekly Avg, Monthly Avg, Recommended Quantity (editable before printing)

**Summary:** Total Recommended SKUs, Total Recommended Quantity, Notes (free text)

**Footer:** Distributor Representative, Salesman Signature area, Customer Acknowledgement area, Terms & Conditions (configurable template)

---

### Proposal History

Part of Page 6 — accessible via a "History" tab within the Offer Proposal page. Backed by `proposal_header` JOIN `proposal_history`.

**Display:** Proposal Number, Date, Store Name, Salesman, SKU Count, Generated By, Export Status (Printed/Exported/Not Yet), Last Updated.
**Features:** Search, Filter (date/store/salesman), Reprint (re-generate the same PDF with the same data), Re-export, View Details (read-only modal showing the full proposal as generated).

---

## 5. Opportunity Score — Design Notes

The opportunity score is computed **per (store, sku)** pair and stored in `fact_daily_store_sku_demand.opportunity_score` (updated daily by the ETL). Key properties:
- **0–100 scale**, higher = better opportunity.
- **Distributor-relative normalization** — a score of 80 means "top 20% of this distributor's store-sku combinations by opportunity," not an absolute threshold. This avoids a small distributor's genuinely-active stores all scoring low just because they're smaller than a large city distributor.
- **Configurable weights** via `recommendation_threshold` table — allows the business to prioritize frequency over quantity (or vice versa) without a schema change.
- **Trend direction flag** (`Increasing/Stable/Declining`) stored separately from score so sort/filter can use it independently.

---

## 6. Reports Menu — Restructured Content

**What moves OUT of Reports:** Store Performance (moves to Store Opportunity → Page 2). No other existing report sections are removed — they continue in Reports, repositioned as pure analysis.

**New Reports structure:**
- **Demand Overview** — overall demand trend, regional/brand/category breakdown (backed by `vw_demand_overview` + `vw_demand_heatmap_region` already built)
- **Salesman Analytics** — demand contribution by salesman, productivity, territory coverage (backed by `vw_salesman_demand_report` + `vw_salesman_detail_summary`)
- **Product Analytics** — SKU demand ranking, brand/category analysis (backed by `fact_daily_sku_demand` + `agg_weekly_sku_demand`)
- **Trend Analytics** — daily/weekly/monthly trends, DoD/WoW/MoM comparatives (backed by `vw_store_demand_dod/wow/mom`)
- **Executive Dashboard** — KPI summary cards, top opportunities (backed by `vw_kpi_demand` + `vw_kpi_active_entities`)

Reports is analytical and informational only — no action buttons, no proposal generation, no edit workflows.

---

## 7. Security Rules

- Distributor Admin sees ONLY their own `distributor_code`'s stores/salesmen/demand/proposals.
- Enforced at API layer (auth session → distributor_code → injected into every query).
- Additional BigQuery row access policy on `proposal_header` for defense-in-depth.
- Proposal generation (write to `proposal_header`/`proposal_item`) is the only WRITE path in this module — all other pages are read-only.

---

## 8. Related Documents

[`store_opportunity_architecture.md`](store_opportunity_architecture.md) · [`store_opportunity_api.md`](store_opportunity_api.md) · [`store_opportunity_proposal_template.md`](store_opportunity_proposal_template.md) · [`../database/schema/store_opportunity.sql`](../database/schema/store_opportunity.sql) · [`step_demand_report_functional_spec.md`](step_demand_report_functional_spec.md)
