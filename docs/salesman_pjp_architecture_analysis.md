# Salesman PJP — Technical Architecture Analysis

**Source file analyzed:** `D:\GitHub\streamlit_app\salesman_pjp.py` (2,230 lines)
**Excel template analyzed:** `D:\Claude\PJP_Template_20260526.xlsx` (54K-row Lookup sheet, 30K-row data sheet)
**Analysis date:** 2026-07-01

---

## 1. Application Structure

```
salesman_pjp.py
├── CONSTANTS (lines 1–144)
│   ├── DISTRIBUTOR_PASSWORDS — ~140 entries, plain-text, hardcoded
│   └── INPUT_DEADLINE — hardcoded datetime
├── AUTH LAYER (lines 147–199)
│   ├── _get_password_for_distributor()
│   ├── _check_distributor_auth()
│   └── _render_password_gate()
├── BQ CREDENTIAL LAYER (lines 202–232)
│   └── get_credentials() — tries st.secrets, falls back to local file
├── DATA LOADING LAYER (lines 236–292)
│   ├── load_distributor_data() — @st.cache_data
│   └── load_store_data() — @st.cache_data
├── SALESMAN CRUD LAYER (lines 295–580)
│   ├── get_salesman_list() — read + cache
│   ├── get_latest_running_number() / generate_salesman_id()
│   ├── insert_salesman_record() / insert_mapping_record()
│   ├── update_salesman_record()
│   └── deactivate_previous_mapping() / deactivate_salesman_mapping()
├── PJP CRUD LAYER (lines 354–414, 1232–1263)
│   ├── get_pjp_list() — read, NOT cached
│   ├── push_to_bigquery() — generic WRITE_APPEND
│   └── delete_pjp_records() — DELETE with filter guard
├── EXCEL GENERATION LAYER (lines 634–1029)
│   ├── create_pjp_excel() — orchestrator
│   ├── _build_lookup_and_named_ranges() — builds hidden Lookup sheet
│   ├── _attach_cascade_dvs() — attaches 4-step cascading DataValidation
│   └── Excel style helpers (_thin_border, _fill, etc.)
├── VALIDATION LAYER (lines 1084–1176)
│   ├── validate_pjp_df()
│   ├── validate_row_completeness()
│   └── read_template_sheet()
├── SHARED UI COMPONENTS (lines 1265–1370)
│   ├── _render_salesman_form_fields()
│   ├── _build_salesman_data()
│   └── _validate_salesman_fields()
├── NAVIGATION (lines 1373–1448)
│   ├── PAGES = {"👥 Kelola Salesman": "salesman", "🗓️ PJP Template": "pjp_template"}
│   ├── Sidebar: distributor selector + page selector
│   └── Global data load + PASSWORD GATE
├── PAGE: SALESMAN (lines 1450–2009)
│   ├── Displays paginated salesman list
│   ├── Per-row actions: Edit, Replace, Deactivate (inline expanders)
│   └── Add Salesman form (bottom of page)
└── PAGE: PJP TEMPLATE (lines 2012–2230)
    ├── Tab 1: Download PJP Template (generate + download Excel)
    └── Tab 2: Update PJP (upload + validate + delete old + insert new)
```

---

## 2. Security Analysis

| Issue | Severity | Description |
|---|---|---|
| **Plain-text passwords hardcoded in source** | 🔴 Critical | ~140 distributor passwords stored as a Python dict literal in the source file. Anyone with repo access (or who can read the deployed code) has all passwords. No salting, no rotation mechanism, no expiry. |
| **No centralized identity management** | 🔴 Critical | Authentication is a per-session session_state flag, not tied to any user record. Sharing a password with multiple users is indistinguishable from one user. |
| **Fallback credential path hardcoded** | 🟡 High | `get_credentials()` falls back to `C:\Users\Bella Chelsea\Documents\...` — a local path that would silently fail or crash in any non-developer environment if `st.secrets` isn't configured. |
| **No audit trail on BigQuery writes** | 🟡 High | `push_to_bigquery()` appends rows with `uploaded_at`, but there's no record of *who* inserted them (which distributor admin, which session). `update_salesman_record()` and `delete_pjp_records()` similarly leave no caller-identity trail. |
| **Non-atomic PJP update** | 🟡 High | The update flow is `DELETE old records → INSERT new records`. A failure between these two steps leaves the data in a partial state with no rollback. The app catches the error and warns, but can't restore deleted records. |
| **Name-based UPDATE target** | 🟠 Medium | `update_salesman_record()` finds the row to update by `UPPER(TRIM(nama_salesman)) = ... AND UPPER(TRIM(kode_distributor)) = ...`. If two salesmen share a name within one distributor, both records are updated. |
| **WRITE_APPEND with no dedup** | 🟠 Medium | Every call to `push_to_bigquery()` appends rows without checking for existing records. Rapid re-uploads create duplicate rows in BigQuery (the existing dedup logic in `get_salesman_list` using `ROW_NUMBER()` compensates somewhat for gt_master_salesman, but not for gt_master_salesman_pjp). |
| **INPUT_DEADLINE hardcoded** | 🟢 Low | Business rule embedded as a constant, must be code-edited each month. Not a security issue but an operational risk. |

---

## 3. Performance Assessment

| Area | Current behavior | Risk | Recommendation |
|---|---|---|---|
| **load_distributor_data + load_store_data** | Cached via `@st.cache_data`. Cache survives across user sessions until Streamlit restarts. | First load per deployment instance hits BigQuery — warm-up latency. Multiple distributors hitting simultaneously = concurrent BQ queries | Cache is appropriate; add a TTL (e.g. `@st.cache_data(ttl=3600)`) to avoid serving stale master data across month boundaries |
| **get_salesman_list** | Cached per `distributor_code`. Uses `ROW_NUMBER()` + `LEFT JOIN` between two BQ tables — correct dedup pattern but queries two tables per call | Low risk — salesman roster is small (~5–30 per distributor) | Fine at current scale; consider a lower `ttl=600` since salesman data changes more frequently than distributor master |
| **get_pjp_list** | NOT cached. Reads `gt_master_salesman_pjp` (27,915 rows confirmed) filtering by distributor/salesman. Missing index on `kode_distributor` and `nama_salesman` (BigQuery doesn't have traditional indexes — clustering handles this). | Each page load re-queries full PJP table with a full scan if the table isn't clustered | Document that `gt_master_salesman_pjp` should be clustered on `kode_distributor` for this query pattern; add caching per distributor+salesman |
| **Excel generation** | Builds a 30,000-row Excel in memory via openpyxl. Tested file: 2.97MB, 54K-row Lookup + 30K data rows. Named range count grows with ASM × Region × Distributor hierarchy depth. | Memory spike per generation; slow for large distributor hierarchies (~234 named ranges in sample template) | Already cached via `_cached_pjp_excel()`. Acceptable for current scale; generate once per deployment session per distributor |
| **UPPER(TRIM()) normalization in every query** | All WHERE clauses use `UPPER(TRIM(x)) = UPPER(TRIM(@param))` | BigQuery can't use clustering effectively on computed expressions — full table scans on name-matched WHERE clauses | Normalize at write time (already done for some fields via `sanitize_salesman_name`) so indexed/clustered columns can be compared directly |

---

## 4. State Management

Streamlit's session state is used for:
- `auth_{dist_code}`: whether the current session has authenticated for a distributor
- `_prev_dist_code`: detect distributor switch → clear auth + cached salesman DF
- `salesman_df`: in-memory copy of the salesman DataFrame to avoid repeated BQ calls within a session
- `_cached_dist`: distributor context key for cache invalidation
- `show_add_form`: toggle state for the Add Salesman panel
- `action_mode`: which inline action panel is currently open (tuple of `(action_type, salesman_id)`)

**Caching pain points:** `get_salesman_list` is decorated with `@st.cache_data(show_spinner=False)` but takes a string argument — cache invalidation after a write requires explicit `st.cache_data.clear()` (done after successful CRUD operations). However, clearing the global cache evicts ALL cached data for ALL distributors (over-broad), causing the next request to re-fetch from BigQuery even for unrelated distributors.

---

## 5. Dependencies

| Dependency | Version constraint | Used for |
|---|---|---|
| `streamlit` | current | UI framework |
| `pandas` | current | DataFrame manipulation, BQ result processing |
| `google-cloud-bigquery` | current | All BQ read/write |
| `google-auth` | current | Service account credentials |
| `openpyxl` | current | Excel generation (named ranges, data validation, cell styling, sheet protection) |
| `io.BytesIO` | stdlib | In-memory Excel buffer |
| `re`, `unicodedata` | stdlib | Name sanitization, Excel named range key cleaning |
| `datetime` | stdlib | Timestamps, INPUT_DEADLINE |

No `requirements.txt` was found in the analyzed file's context — dependency versions are implicit from the environment.
