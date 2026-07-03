# Salesman PJP — STEP Integration Recommendations

**Classification legend:** A = Ready to migrate directly | B = Requires minor modification | C = Requires redesign | D = Not suitable for STEP (keep in Streamlit or retire)

---

## 1. Feature Classification

### Feature: Salesman List View
**Class: B — Requires minor modification**

The data already exists in `sfa_step.dim_salesman` (from `gt_schema.gt_salesman_mapping`). STEP's `outlet-salesman.html` already shows a read-only salesman list. Minor modification: scope by `distributor_code` (from the logged-in Distributor Admin's session), add the HR fields (phone, SPV internal/external, salary, status) that `dim_salesman` currently doesn't carry (they'd need to be added from `gt_master_salesman` via enrichment in the sync layer). No new BigQuery objects needed beyond extending the existing dim enrichment.

**Navigation placement:** `Administration → Master Data → Salesmen`, or a `Master Data` top-level module entry. NOT in Store Opportunity — the roster is operational data, not analytics.

### Feature: Add Salesman (new salesman record + new mapping)
**Class: C — Requires redesign**

The Streamlit implementation writes directly from the user's browser session to BigQuery (WRITE_APPEND, no OLTP-style transaction). In STEP, this must go through the API layer (not a direct BQ connection), with proper validation, ID generation server-side, and an atomic two-step insert (salesman record + mapping in a BEGIN...END scripted BQ transaction). The business logic (ID format, type enum, required fields) is Class A (directly portable) — only the delivery mechanism needs redesigning.

**Security fix required:** Salesman CRUD should be gated on `distributor_admin` role + `distributor_code` claim, not a hardcoded password dict.

**Navigation placement:** `Administration → Master Data → Salesmen → Add Salesman`.

### Feature: Edit Salesman (update HR fields)
**Class: C — Requires redesign**

Same reasoning as Add. Additionally: the current name-matching `UPDATE WHERE UPPER(TRIM(nama_salesman)) = ...` is fragile — STEP's version must use `salesman_id` (the immutable PK) as the target, not name. This requires `salesman_id` to be surfaced in the UI and passed in the API request, which it currently isn't in the Streamlit version.

**Business logic:** field list, allowed updates, phone normalization — Class A (directly portable).

### Feature: Replace Salesman (same ID, new person)
**Class: C — Requires redesign**

The 3-step sequence (deactivate old → insert new salesman record → insert new mapping) needs to be atomic. In STEP: a single `POST /administration/salesmen/{id}/replace` endpoint that executes all 3 writes in a BigQuery BEGIN...END transaction. The business rules themselves are Class A.

### Feature: Deactivate Salesman
**Class: B — Requires minor modification**

One BigQuery UPDATE (`is_active=FALSE`). Already has a safety gate (confirmation checkbox). In STEP, add a `PATCH /administration/salesmen/{id}/deactivate` endpoint — simple, minimal redesign needed.

### Feature: PJP Excel Template Download
**Class: B — Requires minor modification**

The Excel generation logic (`create_pjp_excel`, `_build_lookup_and_named_ranges`, `_attach_cascade_dvs`) is the most sophisticated part of the app. The core logic is directly portable as a Python server-side function — move it from Streamlit to a FastAPI endpoint (`GET /administration/pjp/template`). The API would call the same openpyxl-based generation code with the same lookup data, returning the file as an attachment. No redesign of the Excel structure needed — the business rules (cascading dropdown chain, protected cells, named ranges, 30K data rows) remain as-is.

**Navigation placement:** `Administration → PJP Management → Download Template`.

### Feature: Cascading Dropdown Logic in PJP Template
**Class: A — Ready to migrate directly**

The named-range + INDIRECT() formula approach works at the Excel/spreadsheet level — not web-rendered. It doesn't need to be reproduced as a web form (an online cascading dropdown form would be a separate feature). The existing approach is the right one for offline bulk data entry and should be kept as-is in the Excel template.

### Feature: PJP Bulk Upload + Validation
**Class: B — Requires minor modification**

The validation logic (`validate_pjp_df`, `validate_row_completeness`, `read_template_sheet`) is directly portable. The write flow (`delete_pjp_records` + `push_to_bigquery`) needs the same atomicity fix as the Replace flow — wrap in a BigQuery scripted transaction. In STEP: a `POST /administration/pjp/upload` endpoint accepting a multipart form upload.

**Navigation placement:** `Administration → PJP Management → Update PJP`.

### Feature: Per-Distributor Authentication (password gate)
**Class: D — Not suitable for STEP. Must be replaced.**

Plain-text hardcoded passwords are a critical security risk regardless of platform. STEP will use role-based access control (role=`distributor_admin`, claims include `distributor_code`). The equivalent of "password per distributor" becomes "an account with role=distributor_admin is provisioned with a specific distributor_code claim." This is a standard SSO/IAM problem, not a feature to migrate — it's a design to retire.

### Feature: INPUT_DEADLINE (monthly upload cutoff)
**Class: C — Requires redesign as a configurable setting**

In STEP, this becomes a row in an `Administration → Configuration` table (key: `pjp_input_deadline`, value: `YYYY-MM-DD`), editable by Head Office Admin via the STEP UI. Not a hardcoded constant.

---

## 2. Menu Placement Summary

```
Dashboard
Store Opportunity           ← new (this project)
Route Evaluate              ← existing
Route Planner               ← existing
Store & Salesman            ← existing (extend with demand enrichment)
Manajemen Target            ← existing
Reports                     ← restructured (this project)
Master Data                 ← NEW top-level module (from this recommendation)
    → Salesman Master       ← from salesman_pjp.py Kelola Salesman
    → PJP Management        ← from salesman_pjp.py PJP Template
Administration              ← existing
    → System Configuration  ← for INPUT_DEADLINE and other configurable rules
```

The Streamlit app's two pages map cleanly into a new **Master Data** module, with the security redesign handled at the auth/role layer rather than embedded in the feature itself.

---

## 3. What NOT to Migrate (Category D)

| Item | Reason |
|---|---|
| Hardcoded password dict | Replace with role-based auth — do not port to STEP |
| `get_credentials()` with local file fallback path | Environment-specific hack — not portable |
| `st.cache_data.clear()` global invalidation | Streamlit-specific; STEP will use targeted cache keys |
| `_fetch_salesman_detail` nested `@st.cache_data` pattern | Streamlit pattern — not applicable in a REST API backend |
