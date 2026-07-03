# Salesman PJP — Function Inventory

**Source file:** `D:\GitHub\streamlit_app\salesman_pjp.py` (2,230 lines, analyzed 2026-07-01)

---

## 1. Module Overview

| Item | Value |
|---|---|
| App title | G2G Template Manager — Salesman & PJP Template Manager |
| Framework | Streamlit |
| Primary users | Distributor Admin (one per distributor, ~140+ distributors) |
| Data channel | G2G (Glad2Glow / related brand) only — NOT Skintific or Timephoria |
| Pages | 2 (Kelola Salesman, PJP Template) |
| Auth model | Per-distributor passwords, session-state based |
| Write targets | BigQuery `gt_schema.gt_master_salesman`, `gt_schema.gt_salesman_mapping`, `gt_schema.gt_master_salesman_pjp` |
| Read sources | BigQuery `gt_schema.master_distributor`, `gt_schema.master_store_database_basis`, same 3 write targets |

---

## 2. Function Inventory

### Authentication Functions

| Function | Purpose | Inputs | Outputs | Notes |
|---|---|---|---|---|
| `_get_password_for_distributor(dist_code)` | Looks up plain-text password from hardcoded dict | `dist_code: str` | `str \| None` | **SECURITY RISK**: ~140 plain-text passwords hardcoded in source. No hashing, no rotation mechanism. |
| `_check_distributor_auth(dist_code)` | Checks session state for auth flag | `dist_code: str` | `bool` | |
| `_render_password_gate(dist_code, dist_name)` | Renders password input UI, validates, sets session flag | `dist_code: str`, `dist_name: str` | `bool` (is_authenticated) | Per-distributor session gate — survives page reload within same tab |

### Credential / BigQuery Functions

| Function | Purpose | Inputs | Outputs | Notes |
|---|---|---|---|---|
| `get_credentials()` | Returns BQ credentials from `st.secrets` or fallback local keyfile | none | `(credentials, project_id)` | Hardcoded fallback path (`C:\Users\Bella Chelsea\...`) — local dev only, would crash on cloud deployment if secrets not configured |

### Data Loading Functions

| Function | Purpose | Inputs | Outputs | Business Rules |
|---|---|---|---|---|
| `load_distributor_data()` | Loads all active G2G distributors + ASM/Region hierarchy from BQ | none | `pd.DataFrame` | Cached via `@st.cache_data`. Filters `region_g2g != ''` AND `status = 'Active'`. Returns columns: distributor_name, region, distributor_code, asm |
| `load_store_data()` | Loads all G2G stores with distributor name mapping | none | `pd.DataFrame` | Cached. Joins `master_store_database_basis` on `distributor_g2g` name to get distributor hierarchy. Returns: store_code (cust_id), store_name, distributor_name, store_label |
| `build_lookup_tables(dist_df)` | Derives dictionaries and option lists from distributor data | `dist_df: DataFrame` | `(distributor_map, asm_options, region_options)` | `distributor_map` = `{distributor_code: distributor_name}` |
| `get_salesman_list(distributor_code)` | Loads all salesmen for a distributor, enriched with gt_master_salesman details | `distributor_code: str` | `pd.DataFrame` | Cached per distributor_code. Uses `ROW_NUMBER()` dedup on gt_master_salesman (same upload-at pattern as sfa_step analysis). Returns: salesman_id, salesman_type, distributor_code, salesman, is_active, no_hp, status_salesman, region, asm |
| `get_pjp_list(distributor_code, salesman_name)` | Loads PJP routes for a distributor (optionally filtered by salesman) | `distributor_code: str (opt)`, `salesman_name: str (opt)` | `pd.DataFrame` | NOT cached. Returns all columns from gt_master_salesman_pjp. Both params optional — one or both must be set |

### Salesman CRUD Functions

| Function | Purpose | Inputs | Outputs | Business Rules |
|---|---|---|---|---|
| `generate_salesman_id(dist_code, salesman_type)` | Generates next salesman ID: `{type}{dist_code}{seq:03d}` | `distributor_code, salesman_type (GTI/MIX/MTI)` | `str` | Reads current max from BQ first. IDs are immutable once created. |
| `get_latest_running_number(dist_code, salesman_type)` | Gets current max seq from gt_salesman_mapping | `distributor_code, salesman_type` | `int` | Returns 0 if none found |
| `insert_salesman_record(salesman_data)` | Appends new row to gt_master_salesman | `dict` | `(bool, str)` | WRITE_APPEND — no dedup check before insert |
| `insert_mapping_record(salesman_id, dist_code, type, name)` | Appends new active mapping row to gt_salesman_mapping | 4 args | `(bool, str)` | Assumes caller has already deactivated previous mapping if replacing |
| `update_salesman_record(nama_salesman, dist_code, updated_fields)` | UPDATE in gt_master_salesman matching by name+distributor | `str, str, dict` | `(bool, str)` | Targets by NAME (string matching, not by a unique ID) — fragile if two salesmen have identical names |
| `deactivate_previous_mapping(salesman_id)` | Sets `is_active=FALSE` for all active mappings with that salesman_id | `str` | `(bool, str)` | Called before inserting a replacement mapping |
| `deactivate_salesman_mapping(salesman_id)` | Alias for `deactivate_previous_mapping` | `str` | `(bool, str)` | Redundant wrapper |

### PJP Management Functions

| Function | Purpose | Inputs | Outputs | Business Rules |
|---|---|---|---|---|
| `delete_pjp_records(dist_code, salesman_name)` | DELETE from gt_master_salesman_pjp matching dist+optionally salesman | `str, str (opt)` | `(bool, str)` | Refuses to execute without at least one filter (safety guard). Non-reversible. |
| `push_to_bigquery(df, col_map, table_id)` | Generic WRITE_APPEND to any BQ table | `DataFrame, dict, str` | `(bool, str)` | Adds `uploaded_at` timestamp. WRITE_APPEND — no dedup/merge |

### Excel Generation Functions

| Function | Purpose | Inputs | Outputs | Notes |
|---|---|---|---|---|
| `create_pjp_excel(df, distributor_map, dist_df, store_df)` | Generates the full PJP Excel template with cascading dropdowns | 4 args | `BytesIO` | The most complex function in the file — creates a Lookup sheet (hidden, named ranges) + PJP Template sheet (30,000 data rows, protected formula cells, cascading DVs) |
| `_build_lookup_and_named_ranges(wb, dist_df, store_df)` | Builds the hidden Lookup sheet + Excel named ranges for the cascading dropdown chain | Workbook + 2 DataFrames | None (mutates wb) | Creates ranges: ALL_ASM, one per ASM (region list), one per ASM+Region (distributor list), DIST_LOOKUP (name↔code), STORE_LOOKUP (code↔name), one per distributor (store code list) |
| `_attach_cascade_dvs(ws, col_names, first_data, last_data)` | Attaches Excel DataValidation objects for the 4-step cascade | worksheet, col list, row range | None (mutates ws) | Cascade: ASM → Region (`INDIRECT("NR_"&ASM)`) → Distributor (`INDIRECT("NR_"&ASM&"_"&Region)`) → Store Code (`INDIRECT("NR_STORE_"&DistName)`) |
| `_safe_name(text)` | Sanitizes a string to a valid Excel named range identifier | `str` | `str` | Normalizes unicode, removes special chars, adds "NR_" prefix |
| `_indirect_clean(cell_ref)` | Builds a SUBSTITUTE() chain to clean a cell reference for INDIRECT() | `str` | `str` | Handles spaces, hyphens, slashes, brackets, etc. |

### Validation Functions

| Function | Purpose | Inputs | Outputs |
|---|---|---|---|
| `validate_pjp_df(df, distributor_map, store_df)` | Full validation of an uploaded PJP sheet: required columns, one-distributor-per-file rule, valid store codes, valid dropdown values | 3 args | `(errors[], warnings[])` |
| `validate_row_completeness(df, required_cols, label)` | Checks that no row is partially filled (all-empty or all-filled, never partial) | 3 args | `errors[]` |
| `read_template_sheet(file, sheet, header_row, distributor_map, store_df)` | Reads and normalizes an uploaded Excel template sheet | 5 args | `pd.DataFrame` | Normalizes Hari (Title case), Minggu (Title case), Frekuensi (UPPER), derives Kode Distributor + Nama Toko from lookups, normalizes phone numbers |

### Helper Functions

| Function | Purpose |
|---|---|
| `normalize_phone_id(phone)` | Normalizes Indonesian phone numbers to +62XXXXXXXXX format |
| `sanitize_salesman_name(name)` | UPPER + collapse multiple spaces |
| `_is_empty(val)` | Returns True if NaN or blank string |
| `_get_unique_distributors(df)` | Gets distinct non-null distributor codes from a DataFrame column |

### Shared UI Helper Functions

| Function | Purpose |
|---|---|
| `_render_salesman_form_fields(key_prefix)` | Renders all 16 salesman form fields (two-column layout). Returns a dict of entered values. Reused by Add, Edit, and Replace panels |
| `_build_salesman_data(fields, dist_df, dist_code, dist_name)` | Converts form-field values to a BigQuery-ready dict (normalizes names, phone numbers, handles None for optional fields) |
| `_validate_salesman_fields(fields)` | Basic required-field validation for the salesman form (name, SPV, phone) |

### Excel Style Functions

| Function | Returns |
|---|---|
| `_thin_border()` | `Border` with thin gray sides |
| `_fill(hex)` | `PatternFill(solid, fgColor=hex)` |
| `_header_font()` | Bold white Calibri 10pt |
| `_note_font()` | Italic gray Calibri 9pt |
| `_req_font()` | Bold red Calibri 9pt (for "Wajib Diisi" labels) |
| `_center()` | Center+center+wrap alignment |
| `_vcenter(wrap)` | Left+center alignment |

---

## 3. Constants and Configuration

| Constant | Value | Notes |
|---|---|---|
| `DISTRIBUTOR_PASSWORDS` | ~140-entry dict | Plain-text passwords per distributor code — **MUST be replaced with a real auth system** |
| `INPUT_DEADLINE` | `datetime(2025, 6, 11).date()` | Monthly upload cutoff — hardcoded, must be updated manually each month |
| `MAPPING_TABLE` | `skintific-data-warehouse.gt_schema.gt_salesman_mapping` | |
| `SALESMAN_TABLE` | `skintific-data-warehouse.gt_schema.gt_master_salesman` | |
| `PJP_TABLE` | `skintific-data-warehouse.gt_schema.gt_master_salesman_pjp` | |
| `SALESMAN_TYPES` | `["GTI", "MIX", "MTI"]` | Determines salesman ID prefix |
| `DAY_OPTIONS` | `["Senin".."Sabtu"]` | 6-day week (Mon-Sat only) |
| `WEEK_OPTIONS` | Ganjil / Genap / Ganjil+Genap | Odd/even/both week patterns |
| `FREQUENCY_OPTIONS` | F4+, F4, F2, F1 | Visit frequency codes |
