# STEP — Store Opportunity API

Read-mostly module — all `/store-opportunity/` analytics endpoints are `GET`. Only `/proposals/` endpoints make writes (see §3). Builds on the pagination, date-range, distributor-scoping, and error-format conventions already established in [`step_demand_report_api.md`](step_demand_report_api.md) — read that first for shared conventions.

---

## 1. Store Opportunity Analytics Endpoints

### `GET /store-opportunity/dashboard`
Store Dashboard (Page 1) KPIs + trend.

**Query params:** `date_range`, `start_date`/`end_date`, `region`, `area`, `salesman_code`, `channel`, `client_code`.

**Response:**
```json
{
  "kpis": {
    "stores_with_demand": 412, "total_demand_quantity": 182430,
    "total_demanded_skus": 87, "weekly_avg_demand": 40620, "monthly_avg_demand": 162480,
    "stores_growing": 204, "stores_declining": 89
  },
  "demand_trend": [ { "date": "2026-06-01", "demand_quantity": 5832 }, ... ],
  "scope": { "distributor_code": "DST171", "distributor_name": "CV MAJU BERSAMA", "regions_resolved": ["WEST JAVA"] }
}
```

### `GET /store-opportunity/stores`
Store Performance List (Page 2). Same filter/sort/pagination contract as `GET /demand/stores` (which also backs the Reports version). Additional param: `sort=opportunity_score` (Descending by default for this endpoint, matching the "find highest opportunity" use case).

**Response row shape** (from `vw_store_performance_list`):
```json
{ "client_code": "IND-IEBB01067", "store_name": "...", "salesman_name": "...", "region": "...",
  "total_demand_quantity": 842, "total_skus_requested": 14, "last_demand_date": "2026-06-15",
  "weekly_avg_demand": 210, "monthly_avg_demand": 842, "opportunity_score": 73.4, "trend_direction": "Increasing" }
```

### `GET /store-opportunity/stores/{client_code}`
Store Detail (Page 3) — profile, demand summary, trend data. `client_code` = the raw Repsly client code (e.g. `IND-IEBB01067`).

**Response:**
```json
{
  "profile": { "client_code": "...", "store_name": "...", "region": "...", "channel": "GT",
               "store_grade": "C", "distributor": null, "salesman_code": "...", "salesman_name": "..." },
  "demand_summary": { "total_quantity": 842, "sku_count": 14, "weekly_avg": 210, "monthly_avg": 842, "last_demand_date": "2026-06-15" },
  "trends": {
    "daily": [ { "date": "2026-06-15", "quantity": 120 }, ... ],
    "weekly": [ { "week_start": "2026-06-08", "quantity": 280 }, ... ],
    "monthly": [ { "month_start": "2026-06-01", "quantity": 842 }, ... ],
    "dod": { "pct_diff": 5.2, "trend_direction": "Increasing" },
    "wow": { "pct_diff": 12.4, "trend_direction": "Increasing" },
    "mom": { "pct_diff": -3.1, "trend_direction": "Declining" }
  }
}
```

### `GET /store-opportunity/stores/{client_code}/skus`
SKU Drill-Down (Page 4) for a specific store. Backed by `vw_store_sku_drilldown`.

**Query params:** Standard `date_range`, `brand`, `category`, `sort` (`demand_quantity`|`opportunity_score`|`growth_pct`, default `opportunity_score`), pagination.

**Response row:** `{ "product_code": "IND-SKINTIFIC-315", "product_name": "...", "brand": "Skintific", "category": "...", "total_demand_quantity": 24, "demand_frequency_days": 7, "weekly_avg_demand": 6, "monthly_avg_demand": 24, "growth_pct": 18.5, "opportunity_score": 81.2, "trend_direction": "Increasing", "last_demand_date": "2026-06-15" }`.

### `GET /store-opportunity/stores/{client_code}/recommendations`
Opportunity Recommendation (Page 5). Backed by `vw_opportunity_recommendations` scoped to `client_code`.

**Query params:** `recommendation_class` (filter: `Recommended`|`Potential`|`Monitor`, multi-select), `sort`, pagination.

**Response row:** Same shape as SKU Drill-Down plus `recommendation_class: "Recommended"`, `suggested_quantity: 26`.

---

## 2. Common Endpoints (Shared with Reports)

`GET /demand/stores` and `GET /demand/stores/{id}` are both re-exposed under Store Opportunity routing (same backing views — no duplication). This allows the Store Opportunity frontend to link out to the demand-report API without needing different routes.

---

## 3. Proposal Endpoints (Write-enabled)

### `POST /proposals/generate`
Create a new offer proposal from a set of selected SKU recommendations.

**Request body:**
```json
{
  "client_code": "IND-IEBB01067",
  "valid_until": "2026-07-31",
  "notes": "Proposed based on strong G2G demand in June",
  "items": [
    { "product_code": "IND-SKINTIFIC-315", "recommended_quantity": 26, "item_order": 1 },
    { "product_code": "IND-G2G-213",       "recommended_quantity": 12, "item_order": 2 }
  ]
}
```

**Validation:** at least 1 item required; `recommended_quantity > 0` for each item; `client_code` must be within the caller's distributor scope; `valid_until >= today`.

**Response:** `{ "proposal_id": "...", "proposal_number": "PROP/DST171/202607/0001", "status": "draft" }` — `201 Created`.

### `GET /proposals`
Proposal History list — backed by `vw_proposal_list`. Params: `date_range`, `client_code`, `status`, `sort`, pagination.

### `GET /proposals/{proposal_id}`
Full proposal detail — joins `proposal_header` + `proposal_item` + `proposal_history`. Returns print-ready structured data.

### `GET /proposals/{proposal_id}/pdf`
Generate PDF. Server renders the proposal using the template in [`store_opportunity_proposal_template.md`](store_opportunity_proposal_template.md). Response: `Content-Disposition: attachment; filename="PROP_DST171_202607_0001.pdf"`. Also inserts a `proposal_history` row (`action='printed'`) and increments `print_count`.

### `POST /proposals/{proposal_id}/reprint`
Re-generate the same PDF (same data as originally saved, not re-queried). Inserts `proposal_history` (`action='reprinted'`).

### `PATCH /proposals/{proposal_id}/cancel`
Cancel a proposal. Validates `status` is not already `cancelled`. Inserts `proposal_history` (`action='cancelled'`).

### `GET /proposals/export`
Excel export of the proposal list. Same `EXPORT_TOO_LARGE` guard (50,000 rows max) as the demand report export endpoint.

---

## 4. Validation Rules

| Rule | Applies to |
|---|---|
| `client_code` must be within the caller's distributor scope | All store-level endpoints |
| `recommendation_class` filter values from a fixed enum | `GET .../recommendations` |
| Minimum 1 item in request body | `POST /proposals/generate` |
| `recommended_quantity > 0` per item | `POST /proposals/generate` |
| `valid_until >= today` | `POST /proposals/generate` |
| Can only cancel `draft`, `generated`, `printed`, or `exported` proposals | `PATCH .../cancel` |

---

## 5. Related Documents

[`store_opportunity_functional_spec.md`](store_opportunity_functional_spec.md) · [`store_opportunity_architecture.md`](store_opportunity_architecture.md) · [`store_opportunity_proposal_template.md`](store_opportunity_proposal_template.md) · [`step_demand_report_api.md`](step_demand_report_api.md)
