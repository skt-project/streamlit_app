# Salesman PJP — Business Process Documentation

---

## 1. Business Problem Being Solved

The Streamlit app solves two distinct, related operational problems for the G2G (Glad2Glow) distribution channel:

**Problem 1 — Salesman Roster Management:** G2G distributors need to maintain their salesman headcount in a centralized system (gt_schema.gt_master_salesman + gt_salesman_mapping). Currently this requires the distributor admin to: know the right ID-generation format, manually maintain the active/inactive state across replace/deactivation events, and upload a complete data record (HR-level fields: salary, education, DOB, join date, phone) for each salesman. Without this app, distributors would submit data via Excel spreadsheets to an HO admin who manually uploads to BigQuery — a slow, error-prone process with no self-service visibility.

**Problem 2 — PJP (Permanent Journey Plan) Management:** Each salesman has a weekly route plan: which stores to visit, on which day, in which week pattern (odd/even/both), at what frequency (F1/F2/F4/F4+). This PJP is the operational plan used for route compliance monitoring (STEP's Route Evaluate), demand attribution, and territory coverage tracking. The app generates a formatted Excel template with cascading dropdowns (ASM→Region→Distributor→Store) so distributors can submit accurately-structured PJP data without knowing BigQuery schemas, then validates and loads the submission into BigQuery.

---

## 2. User Journey — Salesman Management

```
Distributor Admin logs in
    → Enters password for their distributor code
    → Password validated against DISTRIBUTOR_PASSWORDS dict
    → Auth flag set in session state
    ↓
Navigates to "Kelola Salesman"
    → Sees list of all active salesmen for their distributor
    → Columns: Salesman ID, Type, Name, Phone, Status, SPV, Region
    ↓
For each salesman, can:

 ┌─ EDIT (inline form)
 │   → Updates non-key fields (SPV, phone, salary, status, etc.)
 │   → Calls update_salesman_record() — UPDATE in gt_master_salesman
 │   → If name changed, also updates gt_salesman_mapping.salesman column
 │   → Success/failure feedback in UI

 ├─ REPLACE (change of personnel — same code, new person)
 │   → Inserts new salesman record (insert_salesman_record)
 │   → Deactivates old mapping (deactivate_previous_mapping)
 │   → Creates new mapping with same salesman_id (insert_mapping_record)
 │   → Net effect: same salesman ID, new person behind it

 └─ DEACTIVATE (salesman leaves)
     → Sets is_active=FALSE in gt_salesman_mapping
     → Does NOT delete from gt_master_salesman (historical HR record preserved)

Add New Salesman (bottom of page)
    → Selects salesman type (GTI / MIX / MTI)
    → System previews the ID that will be generated: {Type}{DistCode}{seq:03d}
    → Fills in all HR fields
    → System calls insert_salesman_record() then insert_mapping_record()
    → New salesman appears in list on next load
```

---

## 3. User Journey — PJP Template Download

```
Navigates to "PJP Template" → "Download Template" tab
    → System generates the Excel template (or serves from cache)
    → Template contains:
        - Hidden Lookup sheet with all named ranges (ASM/Region/Distributor/Store hierarchies)
        - PJP Template sheet with:
            - Row 1: Column usage notes (instruction text, gray italic)
            - Row 2: "Wajib Diisi" (mandatory field indicators, red bold)
            - Row 3: Column headers (blue/orange header row)
            - Rows 4–30003: 30,000 empty data rows with:
                - Cascading dropdown validations
                - Auto-populated formula cells (Kode Distributor from Nama Distributor, Nama Toko from Kode Toko)
                - Sheet protection (locked formula cells, user can edit data cells)
    → Distributor Admin downloads and fills offline
    → Cascade order is enforced by dropdown chain: ASM → Region → Distributor → Kode Toko
    → Formula cells auto-populate: Kode Distributor and Nama Toko
```

---

## 4. User Journey — PJP Update

```
Navigates to "PJP Template" → "Update PJP" tab
    ↓
Step 1: Choose scope
    Option A: "Distributor (semua salesman)" → delete ALL PJP for this distributor
    Option B: "Salesman tertentu" → delete PJP for one specific salesman only
    ↓
Step 2: Upload filled template (.xlsx)
    → System reads "PJP Template" sheet (skipping header rows)
    → Normalizes: Hari (Title Case), Minggu (Title Case), Frekuensi (UPPER)
    → Derives: Kode Distributor from Nama Distributor, Nama Toko from Kode Toko
    → Validates:
        - All required columns present
        - No partial rows (all mandatory fields must be filled if any are filled)
        - Only 1 distributor code per file
        - Distributor code must be in the master list
        - Store codes must exist in master store (warnings, not errors)
        - Hari/Minggu/Frekuensi must be valid dropdown values
    ↓
Step 3: Review validation results
    → Errors shown in red (blocking) — must fix before proceeding
    → Warnings shown in yellow (non-blocking) — proceeds with warning noted
    → Preview: "X baris valid, Y errors"
    ↓
Step 4: Confirm + Execute (if no errors)
    → Checkbox confirmation: "I understand old PJP will be deleted and replaced"
    → Click "Update PJP"
    → delete_pjp_records(distributor_code, [salesman_name]) — DELETE old
    → push_to_bigquery(df, _PJP_COL_MAP, PJP_TABLE) — INSERT new
    → Success or error feedback
```

---

## 5. Data Flow Diagram

```
BQ: gt_schema.master_distributor ──→ load_distributor_data()
                                       ↓
BQ: gt_schema.master_store_db ──────→ load_store_data()
                                       ↓
                                 build_lookup_tables()
                                       ↓
                            [Sidebar: Distributor selector]
                                       ↓
                            PASSWORD GATE per distributor
                                       ↓
                     ┌─────────────────┴────────────────────┐
              SALESMAN PAGE                            PJP TEMPLATE PAGE
                     │                                       │
     get_salesman_list(dist_code)                  Tab 1: Download
                     │                              create_pjp_excel(...)
     [display list + inline actions]                        ↓
                     │                               [Download .xlsx]
         ┌───────────┼──────────────┐
         │           │              │                Tab 2: Update
        EDIT       REPLACE      DEACTIVATE         [Upload .xlsx]
         │           │              │                     │
  update_         insert_      deactivate_        validate_pjp_df()
  salesman_       salesman_    previous_                  │
  record()        record() +   mapping()          [Confirm + Execute]
                  deactivate                              │
                  old +                        delete_pjp_records()
                  insert_                               +
                  mapping()                  push_to_bigquery()
```

---

## 6. Key Business Rules

| Rule | Where enforced |
|---|---|
| 1 file = 1 distributor per PJP upload | `validate_pjp_df()` — hard error if > 1 distinct distributor code found |
| All mandatory fields must be filled if any field is filled in a row | `validate_row_completeness()` — partial rows are errors |
| Salesman ID format: `{Type}{DistCode}{seq:03d}` | `generate_salesman_id()` — auto-generated, not manually entered |
| Replace = deactivate old mapping FIRST, then insert new mapping with SAME ID | `_render` inline replace panel + 3-step function chain |
| PJP update is DELETE + INSERT (not UPDATE) — all old PJP deleted before insert | `delete_pjp_records()` then `push_to_bigquery()` |
| Store code must exist in master store (warning, not blocking) | `validate_pjp_df()` — warning only, PJP can be submitted with unknown stores |
| Input deadline exists (currently hardcoded June 11, 2025) | `INPUT_DEADLINE` constant — referenced in UI but no hard enforcement found in the analyzed code |

---

## 7. Dependencies (External)

| Dependency | Description |
|---|---|
| `gt_schema.master_distributor` | Source of all distributor names, codes, regions, ASM assignments |
| `gt_schema.master_store_database_basis` | Source of all G2G store codes + names + distributor assignment |
| `gt_schema.gt_master_salesman` | HR-level salesman records (salary, education, DOB, phone, SPV) |
| `gt_schema.gt_salesman_mapping` | Active/inactive salesman-to-distributor-type mapping, is source of truth for salesman IDs |
| `gt_schema.gt_master_salesman_pjp` | The PJP route plan — the primary write target of the app |
