# sfa_step Data Dictionary

**Project:** `skintific-data-warehouse` · **Schema:** `sfa_step` · **Engine:** BigQuery
**Scope:** Core slice (Outlet, Salesman, Route Plan, Visit/Call, Management Target, SPV Target) — see [`sfa_step_architecture.md`](sfa_step_architecture.md) for what's deferred to a later phase.

Every fact in this dictionary was confirmed by directly querying the real warehouse on 2026-06-29 (read-only service account, `BigQuery Data Viewer` role) — row counts, date ranges, and join-match rates are measured, not estimated, unless explicitly marked "assumed."

---

## 1. Table Overview

| Table | Type | Rows (real, measured) | Owner | Refresh |
|---|---|---|---|---|
| `dim_outlet` | Dimension (federated) | ~31,261 (GT only in core slice) | STEP app team (sync logic), GT data owner (source) | Daily, incremental |
| `dim_salesman` | Dimension (federated) | ~317 (GT_MAPPING) | STEP app team (sync logic), GT data owner (source) | Daily, incremental |
| `fact_route_plan_pjp` | Fact (recurring pattern) | ~27,915 | GT data owner (source, batch upload) | Weekly, full reload |
| `fact_visit` | Fact (event) | ~872,873 (SADATA_BA) + ~11,616 (REPSLY_HISTORICAL, frozen) | sadata app owner (source), STEP app team (sync) | Daily (SADATA_BA only); REPSLY_HISTORICAL is one-time |
| `dim_outlet_location` | Dimension (derived) | One row per outlet with ≥1 check-in | STEP app team (fully derived — no external owner) | Daily, full re-aggregate, after `fact_visit` |
| `fact_management_target` | Fact (snapshot) | ~44,827 source rows → ~3x after brand unpivot | GT planning/finance team (source), STEP app team (sync) | Daily, incremental |
| `fact_spv_target` | Fact (operational, STEP-native) | 0 (new) | STEP app (writes directly — not synced) | Real-time (app writes) |
| `sync_watermark` | Control | 4 seed rows | STEP app team | Updated by every sync run |
| `sync_log` | Control (audit) | 0 (new) | STEP app team | Appended by every sync run |

**Views:** `vw_outlet_active`, `vw_salesman_active` (operational — hide soft-deleted rows), `vw_outlet_location_best` (operational — COALESCEs `dim_outlet_location` over `dim_outlet`'s sparse master-data lat/long), `vw_target_comply` (reporting — Comply% per brand/month), `vw_route_compliance` (reporting — planned vs actual visits per salesman per ISO week), `vw_salesman_360_summary` (dashboard aggregate).

---

## 2. Table Detail

### 2.1 `dim_outlet`

**Purpose:** Federated store/outlet roster. **Not** a single conformed dimension — see §4 (Relationships) for why.

| Column | Type | Notes |
|---|---|---|
| `outlet_sk` | STRING | PK (not enforced). `SHA256(source_system \|\| source_outlet_code)` |
| `source_system` | STRING | `'GT'` in this slice (REPSLY/SADATA outlet rows are a Phase 2 addition — see architecture doc §6) |
| `source_outlet_code` | STRING | `cust_id` from `gt_schema.master_store_database` |
| `master_entity_id` | STRING | Reserved for future MDM reconciliation. **Always NULL today** — do not build logic that assumes it's populated. |
| `brand` | STRING | Real column from source (`Skintific`/`Glad2Glow`/`Timephoria`/etc.) — confirms STEP's brand-group concept already exists in the business, not an invention of the prototype |
| `channel` | STRING | `'GT'` or `'BA'`, derived from source's `ba_non_ba` flag |
| `store_grade` | STRING | `COALESCE(sktf_store_grade_q1_25, g2g_store_grade_q1_25)` — maps to STEP's Tier S/A/B/C/D concept |
| `repsly_client_code`, `sadata_store_id`, `skintific_code` | STRING | Cross-reference bridge columns. **Confirmed sparse**: only 157/3,922 (4%) of `repsly.dim_clients_t` rows have a non-empty `store_code_sadata`. Treat as best-effort, not authoritative. |
| `is_deleted` | BOOL | Soft delete — set when the source row disappears from `gt_schema.master_store_database` |

**Source:** `gt_schema.master_store_database` (31,261 rows, 14.3MB, has a real `input_date` watermark column).

### 2.2 `dim_salesman`

**Purpose:** Federated field-personnel roster.

| Column | Type | Notes |
|---|---|---|
| `salesman_sk` | STRING | PK. `SHA256(source_system \|\| source_salesman_code)` |
| `source_system` | STRING | `'GT_MAPPING'` in this slice |
| `source_salesman_code` | STRING | `salesman_id` from `gt_schema.gt_salesman_mapping` — the **only** GT salesman table with a clean ID |
| `spv_name`, `asm_name`, `region` | STRING | Enriched from `gt_schema.gt_master_salesman` by **exact name match** — confirmed 247/317 = **78% match rate**. The remaining 22% will have these fields NULL. This is the table's single biggest data-quality caveat; do not treat a NULL here as "no SPV," treat it as "enrichment join missed." |

**Source:** `gt_schema.gt_salesman_mapping` (317 rows) + `gt_schema.gt_master_salesman` (318 rows, name-keyed enrichment).

### 2.3 `fact_route_plan_pjp`

**Purpose:** GT's recurring journey plan (PJP). Modeled at its **native grain** — day-of-week + odd/even-week + frequency code — not exploded into calendar dates, because the source isn't date-grained either.

| Column | Type | Notes |
|---|---|---|
| `visit_day_of_week` | STRING | Raw Indonesian day name (e.g. `'Senin'`) — not translated to an ISO weekday number in this slice |
| `visit_week_pattern` | STRING | e.g. `'Minggu Ganjil'` (odd week) / `'Minggu Genap'` (even week) |
| `visit_frequency_code` | STRING | e.g. `'F2'` — raw source code, meaning not decoded in this slice |
| `salesman_sk`, `outlet_sk` | STRING | **Assumed**, not confirmed, to resolve cleanly — `kode_toko` (PJP) vs `cust_id` (master_store_database) look format-compatible by visual sampling (`"ICWJ01164"` vs `"IWJA00173"`) but this was not validated by a full join during this design pass. **Run the validation query in `sfa_step_sync.sql` §1c before trusting this table's FK fill rate in production.** |

**Source:** `gt_schema.gt_master_salesman_pjp` (27,915 rows, 4.6MB, `uploaded_at` confirmed current as of this design — i.e. an actively maintained table, not abandoned).

### 2.4 `fact_visit`

**Purpose:** Actual visit execution — this is "Call" in STEP's Route Evaluate terminology.

| Column | Type | Notes |
|---|---|---|
| `source_system` | STRING | `'SADATA_BA'` (primary — through 2025-08-07 as of this design) or `'REPSLY_HISTORICAL'` (**frozen** — max date confirmed 2024-01-06, never re-synced) |
| `is_call` | BOOL | `TRUE` iff `check_in_at IS NOT NULL` — directly answers the source Excel's core limitation (it cannot distinguish "not visited" from "visited, no sale") |
| `is_effective` | BOOL, nullable | **NULL by design** in this slice. No table found in this investigation reliably links a specific visit to a specific order at store+rep+date grain. Do not backfill this with a heuristic — wait for a real source (see `effective_source` below) or accept it stays NULL. |
| `effective_source` | STRING | Lineage tag for `is_effective` once it's populated from a real source — e.g. `'SFA_HANDHELDV2_VISIT_ITEMS'` once that system goes live (it already has a purpose-built `effective_call` field — zero rows today). **NULL for every row today.** |

**Sources:**
- `sadata.fact_ba_attendance_t` (872,873 rows, 198.2MB — the volume driver of this entire slice; date range 2024-01-01 to 2025-08-07 as last confirmed).
- `repsly.fact_visits_t` (11,616 rows, 3.5MB; date range 2023-08-16 to **2024-01-06 — confirmed stale**, treat as historical archive only).

### 2.4b `dim_outlet_location`

**Purpose:** An outlet's real-world GPS coordinate, derived from where field reps actually checked in (`fact_visit.check_in_latitude/longitude`) — **not** geocoded from an address, and deliberately a separate table from `dim_outlet` rather than a column on it. Added because `dim_outlet.latitude/longitude` (sourced from `gt_schema.master_store_database`) is confirmed only **12.3% populated** (3,855/31,261 rows) by direct query — real field-visit GPS data is both more complete (grows as visits accumulate) and more trustworthy (it's where a rep actually stood, not a geocoded building centroid).

| Column | Type | Notes |
|---|---|---|
| `outlet_sk` | STRING | PK. One row per outlet with ≥1 GPS-tagged check-in in `fact_visit` — not every outlet will have a row |
| `latitude`, `longitude` | FLOAT64 | **Median** (via `APPROX_QUANTILES`), not average, of all observed `check_in_latitude/longitude` for that outlet — robust to the occasional bad GPS fix or a rep checking in from the wrong spot |
| `observation_count` | INT64 | How many check-ins contributed. Treat 1-2 as low-confidence |
| `location_stddev_m` | FLOAT64 | Spread (via `ST_DISTANCE`) of observed points around the median, in meters — high spread suggests a large venue (mall, multi-floor) or a data-quality issue, not a precise point |
| `source` | STRING | Always `'HANDHELD_CHECKIN_DERIVED'` today |

**Source:** derived entirely from `sfa_step.fact_visit` itself (no external table) — a full re-aggregate, not incremental, run daily right after `fact_visit`'s own sync. See `vw_outlet_location_best` for a single COALESCEd answer (`dim_outlet_location` preferred, `dim_outlet`'s master-data lat/long as fallback) when a consumer just wants the best available point regardless of source.

### 2.5 `fact_management_target`

**Purpose:** Real top-down per-brand target — this is what the STEP prototype had to invent as a hardcoded `MANAGEMENT_TARGET_BY_BRAND` constant; here it's a real, synced number.

| Column | Type | Notes |
|---|---|---|
| `brand` | STRING | `'Skintific'` / `'Glad2Glow'` / `'Timephoria'` — one row per brand, unpivoted from the source's 3 separate target columns |
| `management_target_amount` | FLOAT64 | From `skintific_target` / `g2g_target` / `timephoria_target` respectively |
| `weekly_visit_target` | INT64 | Source also carries a visit-frequency target — not currently consumed by any view in this slice, carried through for Phase 2 (Route Evaluate target-vs-actual visit count) |

**Source:** `gt_schema.fact_gt_target_v2_t` (44,827 rows, 2024-01-01 to 2026-05-01). **Data quality confirmed:** 31,997/44,827 (71%) rows have a non-null `customer_id`; 44,272/44,827 (99%) have at least one non-null target value. The sync scripts filter `customer_id IS NOT NULL` — the remaining ~13K customer-less rows are excluded by design, not lost by accident.

### 2.6 `fact_spv_target`

**Purpose:** SPV-proposed target distribution — **genuinely new data**, not sourced from any existing warehouse table (none of the systems investigated capture "what target did the SPV themselves propose," only the top-down Management figure exists). Written directly by the STEP application as part of its target-approval workflow; never touched by a sync job.

| Column | Type | Notes |
|---|---|---|
| `approval_status` | STRING | `draft` \| `submitted` \| `approved` \| `rejected` — `vw_target_comply` only counts `approved` rows |

---

## 3. Views

| View | Business purpose | Refresh |
|---|---|---|
| `vw_outlet_active` / `vw_salesman_active` | Operational — current roster, hides soft-deleted rows | Always current (view, not materialized) |
| `vw_target_comply` | Reporting — `Comply % = SUM(SPV Target) / SUM(Management Target) × 100` per brand/month, matching the STEP prototype's Target Management formula exactly | Always current |
| `vw_route_compliance` | Reporting — planned (PJP pattern) vs actual (`fact_visit`) per salesman per ISO week. **Approximation**, documented inline in the view: `fact_route_plan_pjp` is a recurring pattern, not a per-week exploded calendar | Always current |
| `vw_salesman_360_summary` | Dashboard — one row per salesman, latest-known Route Compliance + Effective Call Rate, the query STEP's Dashboard/Salesman 360 hexagon chart would read | Always current |

None of these are materialized views in this first pass — at the confirmed real data volumes (low hundreds of MB total across the whole slice), a standard view recomputed on read is cheap enough that materialization would add refresh-staleness risk for no measurable performance benefit. Revisit if/when SFA-Handheldv2 goes live and volume grows by an order of magnitude or more (see architecture doc §7).

---

## 4. Relationships & Why This Isn't a Single Conformed Dimension

```
dim_outlet (1) ──< fact_route_plan_pjp >── (1) dim_salesman
dim_outlet (1) ──< fact_visit          >── (1) dim_salesman
dim_outlet (1) ──< fact_management_target
dim_salesman (1) ──< fact_spv_target
fact_management_target ─┐
fact_spv_target ─────────┴──> vw_target_comply (brand + month grain, not a stored FK)
fact_route_plan_pjp ─────┐
fact_visit ───────────────┴──> vw_route_compliance (salesman + ISO week grain, not a stored FK)
```

**Why `dim_outlet`/`dim_salesman` are federated rosters, not conformed dimensions:** three real systems (`gt_schema`, `repsly`, `sadata`) each have their own identity for what is conceptually "the same kind of entity" (a store, a field worker), with **no reliable shared key**, confirmed by direct sampling:

- `sadata.fact_ba_attendance_t.store_id` values: `"000267"`, `"116SU17102023"`, `"12octgen8"`
- `gt_schema.master_store_database.cust_id` / `gt_schema.fact_gt_target_v2_t.customer_id` values: `"IWJA00173"`, `"IWSN00016"`

These are visibly different ID systems — no naive string-match bridge exists between them. `repsly.dim_clients_t` has columns (`skintific_code`, `store_code_sadata`) that *look* like a ready-made bridge, but are only populated for 4% (store_code_sadata) to 77% (skintific_code) of rows, and `store_code_sadata`'s actual values weren't even confirmed non-empty beyond a 5-row sample. **Building automated identity resolution across these three systems is a real Master Data Management project, not a column-mapping exercise — explicitly out of scope for this core slice.** `master_entity_id` is reserved on both dimensions so that work can land without a schema migration later.

---

## 5. Data Ownership

| Domain | Source-of-record owner | sfa_step owner |
|---|---|---|
| GT store master, GT salesman master/mapping, PJP route plans, GT brand targets | GT planning/commercial team (existing `gt_schema` pipeline) | STEP app team owns the *sync logic*, not the data itself — any correction belongs upstream in `gt_schema` |
| BA attendance/visit data | `sadata` app owner | STEP app team owns the *sync logic* |
| Repsly historical visits | Frozen — pipeline owner unknown/inactive as of this design (confirmed stale since 2024-01-06) | STEP app team (one-time backfill only, no ongoing ownership relationship needed) |
| SPV-proposed targets | **STEP app team — sole owner**, no upstream source | STEP app team |

---

## 6. Related Documents

[`sfa_step_ddl.sql`](sfa_step_ddl.sql) · [`sfa_step_sync.sql`](sfa_step_sync.sql) · [`sfa_step_architecture.md`](sfa_step_architecture.md) · prototype reference: [`../../prototype/target-management.html`](../../prototype/target-management.html), [`../../prototype/route-evaluation.html`](../../prototype/route-evaluation.html)
