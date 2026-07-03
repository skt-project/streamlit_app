# Salesman PJP — STEP Gap Analysis

**Comparing:** existing `salesman_pjp.py` Streamlit capabilities vs. current STEP Web Application capabilities.

---

## 1. Feature Comparison Matrix

| Feature | Streamlit (salesman_pjp.py) | STEP (current) | Gap type |
|---|---|---|---|
| **Salesman list view** | ✅ Full list for one distributor, with ID/type/name/phone/SPV/region/status | ✅ Partial — `outlet-salesman.html` Salesman tab shows salesman list from `sfa_step.dim_salesman` (GT + SADATA sources) | Partial overlap — STEP has a read-only roster; Streamlit has an editable one |
| **Add salesman** | ✅ Full form with ID generation and BigQuery insert | ❌ Not in STEP | Gap — STEP has no write path for salesman data |
| **Edit salesman** | ✅ Update HR fields (SPV, phone, salary, status, education, DOB) | ❌ Not in STEP | Gap |
| **Replace salesman** | ✅ Full replace workflow (old deactivated, new mapped to same ID) | ❌ Not in STEP | Gap |
| **Deactivate salesman** | ✅ Sets is_active=FALSE in mapping | ❌ Not in STEP | Gap |
| **Salesman ID generation** | ✅ Auto-generates `{Type}{DistCode}{seq:03d}` | ❌ Not in STEP | Gap |
| **PJP view / browse** | ✅ Per-distributor PJP list read from BigQuery | ⚠️ Partial — `sfa_step.fact_route_plan_pjp` (sfa_step core slice) has PJP data, but no Browse UI | Partial — data exists in sfa_step, no browse UI |
| **PJP Excel template download** | ✅ Generates sophisticated template with cascading dropdowns | ❌ Not in STEP — STEP only has `import-export.html` Outlet Information upload | Gap — no PJP-specific template download |
| **PJP template cascading dropdowns** | ✅ 4-level cascade: ASM→Region→Distributor→Store with INDIRECT() formulae | ❌ Not in STEP | Gap (complex to replicate in a web form; Excel approach is deliberate for offline use) |
| **PJP bulk upload + validation** | ✅ Upload, validate, error report, confirm, delete+insert | ⚠️ STEP has a generic `Import & Export` module with Outlet Information, but not PJP-specific | Gap — PJP-specific upload validation rules not in STEP |
| **PJP per-distributor scoping** | ✅ Full — only shows/edits data for the authenticated distributor | ⚠️ Partial — STEP has brand-group scoping, not distributor-level row security yet | Gap — STEP's Distributor Admin row-level security currently only at Region level (documented in demand report architecture) |
| **Distributor-specific authentication** | ✅ Per-distributor password (plain-text, insecure) | ❌ STEP uses a shared role-switcher (no real auth in prototype) | Gap — both implementations have auth weaknesses; production STEP would need SSO or role-based auth |
| **Distributor selector** | ✅ Full dropdown of all distributors (role-scoped by password) | ⚠️ STEP has `step_brand_group` (brand switching) but no distributor-code selector | Gap — STEP doesn't currently model a single user having distributor-scoped access |
| **Demand visibility** | ❌ Not in Streamlit (separate Demand Monitoring Report Streamlit app handles this) | ✅ New — `sfa_step` Demand Monitoring Report built in this project | Gap the OTHER way — STEP now has demand analytics that Streamlit doesn't |
| **Route planning** | ❌ Not in Streamlit (separate or STEP) | ✅ `route-planner.html` — full route planning UI | Gap the OTHER way |
| **Offer proposals** | ❌ Not in Streamlit | ✅ New — Store Opportunity module designed in this project | Gap the OTHER way |

---

## 2. Redundant Features (Both Systems Have, Could Be Consolidated)

| Feature | Streamlit | STEP | Consolidation opportunity |
|---|---|---|---|
| Salesman roster display | Kelola Salesman page | `outlet-salesman.html` → Salesman tab | Merge into STEP's Salesman tab, enriched with edit capability |
| PJP data access | `get_pjp_list()` | `sfa_step.fact_route_plan_pjp` (read-only) + Route Planner's PJP usage | Consolidate: STEP's route planner already uses PJP data; add a PJP management view |
| Distributor-scoped data | Password-per-distributor | Brand-group + Region scope in sfa_step | Design a real Distributor Admin role in STEP to replace the Streamlit password gate |

---

## 3. Enhancement Opportunities When Moving to STEP

| Opportunity | Current Streamlit limitation | How STEP can improve |
|---|---|---|
| **Audit trail** | No record of who made which changes | STEP's existing `sfa_step.sync_log` + Audit Log pattern can log every salesman write with user identity |
| **Security** | Plain-text passwords hardcoded in source | Replace with STEP's role-based access control (role=`distributor_admin`, scoped to `distributor_code`) |
| **PJP atomicity** | DELETE then INSERT = data loss window if INSERT fails | STEP can wrap in a BigQuery scripted transaction (BEGIN...END with rollback on error) |
| **Salesman ID target** | UPDATE targets by name (fragile) | STEP can target by `salesman_id` (immutable PK) — cleaner and unambiguous |
| **INPUT_DEADLINE** | Hardcoded constant, must be code-edited monthly | STEP's Administration module can store this as a configurable setting |
| **Cache invalidation** | `st.cache_data.clear()` clears ALL distributors' data globally | STEP can invalidate cache per-distributor (a targeted cache key, not global clear) |
| **PJP drill-down** | Flat list only | STEP can show PJP alongside the Store Detail view (from Route Planner context) and the Route Evaluate module |

---

## 4. Summary Statistics

| Category | Count |
|---|---|
| Features unique to Streamlit (not in STEP) | 8 (salesman CRUD, PJP template, PJP upload, cascading dropdowns, distributor auth, distributor selector, per-dist password, INPUT_DEADLINE) |
| Features unique to STEP (not in Streamlit) | 6 (Demand Monitoring, Route Planner, Route Evaluate, Store Opportunity, Brand/Multi-Group, monthly/weekly analytics) |
| Overlapping features (both have some version) | 4 (salesman list, PJP data access, scoped data, distributor concept) |
| Enhancement opportunities in migration | 6 (audit, security, atomicity, ID targeting, config externalization, cache) |
