# Salesman PJP — STEP Integration Implementation Roadmap

**Purpose:** A phased implementation plan for migrating `salesman_pjp.py` functionality into the STEP Web Application. This is an analysis and planning document — no code exists yet for this roadmap. Approval is expected before any development begins.

---

## 1. Pre-requisites (Must happen first — not a Phase)

| Item | Owner | Why blocking |
|---|---|---|
| Define the `distributor_admin` STEP role with `distributor_code` claim | Security/Auth team | All salesman CRUD and PJP features require row-level scoping by distributor code — without this role definition, none of the Phase 1 features can be properly access-controlled |
| Retire plain-text passwords from `salesman_pjp.py` source | Current maintainer | **Critical security risk** — passwords should not remain in git history during the transition period. Even if the Streamlit app is retired, the passwords themselves should be rotated |
| Add `Master Data` to the STEP top-level navigation | STEP dev | Required before any Master Data sub-pages can be placed |

---

## 2. Phase 1 — Quick Wins (Estimated: 2–3 weeks)

Features that are either Class A (direct port) or Class B (minor modification) with high immediate business value.

### Feature 1.1 — Salesman List (Read-Only, Distributor-Scoped)
**Effort:** Low (3–5 days)
**Dependencies:** `distributor_admin` role + `distributor_code` claim
**What:** Extend `sfa_step.dim_salesman` sync to include HR fields from `gt_master_salesman` (SPV, phone, status, join date). Add a `Salesman Master` page under `Master Data` that shows the list filtered by the logged-in distributor's distributor_code.
**Business value:** Distributor Admin can see their salesman roster in one place, replacing the Streamlit read flow.
**Risk:** Low — read-only, no write path.

### Feature 1.2 — PJP Template Download (Server-Side Excel Generation)
**Effort:** Medium (5–7 days)
**Dependencies:** `gt_schema.master_distributor`, `gt_schema.master_store_database_basis` access from STEP API
**What:** Port `create_pjp_excel()` + helper functions to a Python FastAPI endpoint (`GET /administration/pjp/template`). Same openpyxl logic, same cascading dropdown structure. The endpoint generates the Excel on-demand, returning it as an attachment.
**Business value:** Distributor Admins get the same high-quality template without needing to open the Streamlit app. Template can be refreshed on-demand (no stale cached file).
**Risk:** Low. The Excel generation logic is self-contained and well-tested in Streamlit. The biggest risk is the openpyxl dependency needing to be added to the STEP API's dependencies.

### Feature 1.3 — PJP View (Read-Only, Per Distributor)
**Effort:** Low (2–3 days)
**Dependencies:** `sfa_step.fact_route_plan_pjp` already exists (from the sfa_step core slice)
**What:** Add a `PJP Management` sub-page under `Master Data` that shows the current PJP for the distributor's salesmen, sourced from `sfa_step.fact_route_plan_pjp`.
**Business value:** Distributor Admin can verify their current PJP data before uploading changes.
**Risk:** Low — read-only.

**Phase 1 estimated total effort:** 10–15 developer-days.
**Phase 1 estimated business value:** Covers the "view" side of both Streamlit pages without any security risk. Closes the current "must open a different app" friction.

---

## 3. Phase 2 — High-Value Enhancements (Estimated: 3–5 weeks)

Features requiring more backend work (Class C — Requires redesign) but with high operational impact.

### Feature 2.1 — PJP Bulk Upload + Validation
**Effort:** Medium (7–10 days)
**Dependencies:** Phase 1.2 (template download) should be live first (so users have the correct template)
**What:** `POST /administration/pjp/upload` — accepts .xlsx, runs the same validation logic as `validate_pjp_df()`, shows validation results, and on confirmation: deletes old PJP + inserts new PJP in a BigQuery BEGIN...END scripted transaction (atomicity fix vs the current Streamlit delete-then-insert risk).
**Business value:** Eliminates the data-loss window in the current PJP update flow. Distributor Admins can submit their PJP through STEP without switching apps.
**Risk:** Medium. The atomic transaction pattern (`BEGIN...END` in BigQuery scripting) needs careful testing. Also: the current STEP `import-export.html` module might be the better UI home rather than a new PJP-specific page — evaluate whether to reuse or create a new template entity.

### Feature 2.2 — Salesman Deactivate
**Effort:** Low (2–3 days)
**Dependencies:** Feature 1.1 live (salesman list)
**What:** `PATCH /administration/salesmen/{salesman_id}/deactivate` — sets `is_active=FALSE` in `gt_schema.gt_salesman_mapping`. Confirm modal in UI. Writes an audit log entry.
**Business value:** Safe, audited deactivation of departing salesmen through STEP.
**Risk:** Low — simple single-row UPDATE.

### Feature 2.3 — Salesman Add (New Salesman)
**Effort:** Medium (7–10 days)
**Dependencies:** `distributor_admin` role
**What:** Form under `Master Data → Salesman Master → Add Salesman`. Backend `POST /administration/salesmen` generates the ID server-side (`{Type}{DistCode}{seq:03d}` — port `generate_salesman_id()` logic), then executes `insert_salesman_record()` + `insert_mapping_record()` in a BQ scripted transaction. Full field validation. Audit log.
**Business value:** Distributor Admins can onboard new salesmen without waiting for an HO admin or opening Streamlit.
**Risk:** Medium. ID generation race condition (two concurrent adds for the same distributor) must be handled — use a BigQuery MERGE on `proposal_seq`-style table for sequence tracking (same pattern as the proposal_seq table in store_opportunity.sql).

### Feature 2.4 — INPUT_DEADLINE as Configuration
**Effort:** Low (2–3 days)
**Dependencies:** Administration module in STEP
**What:** Add a `Configuration` section to STEP's Administration module. Store `pjp_input_deadline` as a row in a `sfa_step.app_config` table (key/value). HO Admin can update it via a date picker without a code deploy.
**Business value:** Monthly deadline change no longer requires a code edit and redeployment.
**Risk:** Low.

**Phase 2 estimated total effort:** 18–26 developer-days.

---

## 4. Phase 3 — Advanced Capabilities (Estimated: 4–6 weeks)

Features with higher complexity and lower urgency — suitable after Phases 1 and 2 are proven stable.

### Feature 3.1 — Salesman Edit (Update HR Fields)
**Effort:** Medium (7–10 days)
**Dependencies:** Feature 2.3 (Add) live, ID targeting confirmed
**What:** `PATCH /administration/salesmen/{salesman_id}` — takes a partial update payload. Targets by `salesman_id` (not by name — fixing the fragile name-matching issue). Updates both `gt_master_salesman` AND `gt_salesman_mapping.salesman` atomically if name changed.
**Risk:** Medium — name synchronization between the two tables must be handled carefully.

### Feature 3.2 — Salesman Replace
**Effort:** Medium (8–12 days)
**Dependencies:** Feature 3.1 (Edit) live
**What:** `POST /administration/salesmen/{salesman_id}/replace` — full 3-step atomic operation in BQ scripted transaction: INSERT new salesman record → UPDATE old mapping to inactive → INSERT new mapping with same salesman_id.
**Risk:** Medium — 3-step atomicity requires rigorous testing.

### Feature 3.3 — PJP Management Advanced (Drill-Down from Route Planner / Route Evaluate)
**Effort:** High (10–14 days)
**Dependencies:** Phase 2 PJP upload live; Route Evaluate must show per-salesman PJP coverage
**What:** Surface PJP data in Store Opportunity (which stores are on which salesman's PJP), in Route Evaluate (planned route vs actual visits → PJP coverage), and as a configurable view in Master Data (PJP editor — not just upload, but inline row-level editing). This is a significant UX build, not just a backend port.
**Risk:** High — spans multiple modules.

---

## 5. Migration Strategy for the Streamlit App

| Milestone | When | Action |
|---|---|---|
| Phase 1 complete | After Phases 1.1–1.3 go live | Add a notice to the Streamlit app: "PJP Template download and view are now available in STEP." |
| Phase 2 complete | After 2.1–2.4 go live | Add: "PJP Update is now available in STEP. This app will be retired in 30 days." Notify all Distributor Admins. |
| Phase 3.1–3.2 complete | After Edit and Replace are live | Retire Streamlit app (or keep as read-only archive). Rotate all ~140 distributor passwords. |
| Phase 3.3 and beyond | Ongoing | Advanced integration — Streamlit app is already retired. |

**Critical note on timing:** The Streamlit app should NOT be retired until Phase 2.3 (Add Salesman) is live in STEP. If the Streamlit app is retired before that, distributors lose the ability to onboard new salesmen entirely.

---

## 6. Effort and Risk Summary

| Phase | Effort | Risk | Business Value |
|---|---|---|---|
| Pre-requisites | ~5 days (auth/role design) | Low (design, not code) | Foundational |
| Phase 1 | 10–15 dev-days | Low | High (immediate auth security fix + view parity) |
| Phase 2 | 18–26 dev-days | Medium | Very high (write parity + atomicity fix) |
| Phase 3 | 25–36 dev-days | Medium-High | High (full salesman lifecycle management) |
| **Total** | **~60–80 dev-days** | | Full replacement of Streamlit app + significant improvements |
