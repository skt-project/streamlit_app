# STEP — Demand Monitoring Report API

Read-only API — every endpoint here is a `GET`. There is no write path for demand data anywhere in this module (per the explicit "no transaction creation, editing, approval workflow" requirement). All endpoints back directly onto the views/tables in [`sfa_step_demand_report.sql`](../database/schema/sfa_step_demand_report.sql).

---

## 1. Conventions

### 1.1 Pagination

All list endpoints (`/demand/stores`, `/demand/salesmen`, `/demand/sku`) share one pagination contract:

```
?page=1&page_size=50
```

- `page_size` max `200`, default `50`.
- Response always includes a `pagination` object:

```json
{
  "data": [ ... ],
  "pagination": { "page": 1, "page_size": 50, "total_records": 4231, "total_pages": 85 }
}
```

### 1.2 Date range parameter (shared by every endpoint)

```
?date_range=today | yesterday | last_7_days | last_30_days | custom
&start_date=2026-06-01&end_date=2026-06-30   (required only when date_range=custom)
```

Server-side validation: `start_date <= end_date`, both within `2025-08-01..today` (the confirmed real data floor — see functional spec §3.1; a request outside this range returns an empty result set with a `warning`, not a 400, since "no data yet" isn't a client error).

### 1.3 Distributor Admin scoping (every endpoint)

The API layer resolves the caller's `distributor_code`(s) from STEP's own auth/session (never trusts a client-supplied `distributor_code` query param for filtering — if one is supplied, it's validated against the caller's actual allowed set, not used to widen access). Per functional spec §4.5/§3.2, `distributor_code` isn't populated on the store dimension today — until that's resolved, **region** is the real, currently-enforceable scope boundary; this is stated explicitly in every response via a `scope` block:

```json
"scope": { "distributor_codes": ["DST001"], "regions_resolved": ["JABODETABEK MT", "Central Java"], "distributor_field_available": false }
```

### 1.4 Error format (all endpoints)

```json
{ "error": { "code": "INVALID_DATE_RANGE", "message": "start_date must be on or before end_date", "field": "start_date" } }
```

| HTTP | code | When |
|---|---|---|
| 400 | `INVALID_DATE_RANGE` | `start_date > end_date`, or `custom` selected without both dates |
| 400 | `INVALID_PAGE_SIZE` | `page_size > 200` |
| 403 | `FORBIDDEN_SCOPE` | caller's resolved region/distributor set is empty (no access configured) |
| 404 | `NOT_FOUND` | `/demand/stores/{id}` or `/demand/salesmen/{id}` with an unknown code |
| 500 | `INTERNAL_ERROR` | unexpected — logged with a request id, never exposes raw BigQuery error text to the client |

---

## 2. Endpoints

### `GET /demand/overview`

Demand Overview Dashboard — KPI cards + top regions/stores/SKUs + trend.

**Query params:** `date_range`, `start_date`/`end_date` (§1.2), `compare_to` (`previous_period` | `none`, default `previous_period` — drives the DoD/WoW/MoM block).

**Response:**
```json
{
  "kpi": {
    "total_demand_quantity": 1407178, "total_demanding_stores": 8211,
    "total_demanded_skus": 412, "total_salesmen_with_demand": 1893,
    "vs_previous_period_pct": 12.4, "trend_direction": "Increasing"
  },
  "trend": [ { "date": "2026-06-01", "demand_quantity": 41203 }, ... ],
  "top_regions": [ { "region": "JABODETABEK MT", "demand_quantity": 312044 }, ... ],
  "top_stores": [ { "client_code": "IND-IEBB01067", "store_name": "...", "demand_quantity": 8420 }, ... ],
  "top_skus": [ { "product_code": "IND-SKINTIFIC-315", "demand_quantity": 19022 }, ... ],
  "scope": { ... }
}
```
Backed by `vw_demand_overview` + `vw_kpi_demand` joined to a `LIMIT 10` ranking query against `fact_daily_store_demand`/`fact_daily_sku_demand` for the same date range.

### `GET /demand/stores`

Store Demand Report list. Supports `search` (matches `store_name`/`client_code`), `filter[region]`, `filter[channel]`, `sort` (`total_demand_quantity` | `last_demand_date` | `skus_requested_sum`, with `_desc` suffix for descending), plus §1.1/§1.2.

**Response row shape** (from `vw_store_demand_report`):
```json
{ "client_code": "IND-IEBB01067", "store_name": "Rumah Carissa Uluwatu", "region": "Bali Nusa Tenggara",
  "distributor": null, "skus_requested_sum": 14, "total_demand_quantity": 842, "last_demand_date": "2026-06-15" }
```
`distributor: null` is real and expected — see functional spec §3.2; the API does not fabricate a value.

### `GET /demand/stores/{id}`

Store Detail View. `{id}` = `client_code`.

**Response:**
```json
{
  "profile": { "client_code": "...", "store_name": "...", "region": "...", "channel": "GT", "store_grade": "C",
               "assigned_salesman": { "representative_code": "...", "representative_name": "..." } },
  "demand_history": [ { "date": "2026-06-15", "demand_quantity": 120, "demand_amount": 4867200 }, ... ],
  "demanded_skus": [ { "product_code": "...", "product_name": "...", "demand_quantity": 12 }, ... ],
  "last_demand_date": "2026-06-15",
  "trend": { "dod": {...}, "wow": {...}, "mom": {...} }
}
```
`demand_history` from `vw_store_detail_demand_history`; `trend` from `vw_store_demand_dod`/`_wow`/`_mom` filtered to this `client_code`.

### `GET /demand/salesmen`

Salesman Demand Report list. Same search/filter/sort/pagination pattern as `/demand/stores`, backed by `vw_salesman_demand_report`. Additional `rank=true` param returns rows pre-sorted by `total_demand_quantity DESC` with a `rank` field added (per the brief's "Ranking" feature, distinct from generic sort since rank is 1-indexed and gap-free).

### `GET /demand/salesmen/{id}`

Salesman Detail View. `{id}` = `representative_code`. Backed by `vw_salesman_detail_summary` (total managed stores, active days) + top-stores/top-SKUs sub-queries against `fact_daily_salesman_demand`/`stg_demand_daily`.

### `GET /demand/sku`

SKU-level analytics list — daily/weekly/monthly demand, store/salesman counts, trend, top regions/stores per SKU. Backed by `fact_daily_sku_demand` + `agg_weekly_sku_demand`/`agg_monthly_sku_demand` depending on the requested `granularity` param (`daily` | `weekly` | `monthly`, default `daily`).

### `GET /demand/export`

Streams the *same* result set as whichever list endpoint the caller specifies, formatted as a file.

**Query params:** `report` (`stores` | `salesmen` | `sku`, required), `format` (`excel` | `csv`, required), plus that report's normal filter/date-range params.

**Response:** `Content-Disposition: attachment` with the rendered file — not a JSON body. A request exceeding ~50,000 rows returns `400 EXPORT_TOO_LARGE` with a suggestion to narrow the date range, rather than silently truncating data (a truncated "export" that looks complete is worse than an explicit rejection).

---

## 3. Validation Rules Summary

| Rule | Applies to |
|---|---|
| `date_range=custom` requires both `start_date` and `end_date` | all endpoints |
| `start_date <= end_date` | all endpoints |
| Date range within `2025-08-01..today` | all endpoints (outside range → empty result + warning, not an error) |
| `page_size <= 200` | list endpoints |
| `sort` field must be one of the endpoint's allowed sort fields | list endpoints |
| `report` and `format` required, from a fixed enum | `/demand/export` |
| Result row count `<= 50,000` | `/demand/export` |

---

## 4. Related Documents

[`step_demand_report_functional_spec.md`](step_demand_report_functional_spec.md) · [`step_demand_report_architecture.md`](step_demand_report_architecture.md) · [`../database/schema/sfa_step_demand_report.sql`](../database/schema/sfa_step_demand_report.sql)
