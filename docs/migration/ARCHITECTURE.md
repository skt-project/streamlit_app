# Architecture — Current State and Proposed Target

Companion to [MIGRATION_PLAN.md](MIGRATION_PLAN.md). Audited 2026-08-26, GCP project `skintific-data-warehouse`.

---

## 1. Current Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│  ~22 independent Streamlit Community Cloud apps (free tier)               │
│  one .py entry point per app, one shared repo-wide requirements.txt       │
│  each app = one small, idle-sleeping, non-autoscaled Python process        │
│                                                                            │
│   po_buffer.py          po_portal_suggestion.py        po_simulator_v2.py │
│   skt_area_execution_   salesman_pjp.py                noo_detector.py    │
│     capability_v2.py    smart_coverage.py               visit_validator/  │
│   sfa_attendance.py     store_channelization.py         time_study_       │
│   template_converter.py stock_opname_ssjabo.py            stopwatch.py    │
│   whitespace_map.py     skt_top_20_store_list_stock.py  whitespace_map_   │
│   noo_sku_mapping.py    (+ 5 superseded/dead files, not independently       lightweight.py │
│                          deployed — see MIGRATION_PLAN.md §2)             │
└───────────────┬───────────────────────────┬──────────────────────┬───────┘
                │                           │                      │
                │ google-cloud-bigquery      │ google-cloud-storage │ gspread
                │ (per-app st.secrets,        │ (uploads, some       │ (2 apps use
                │  plaintext SA key JSON)     │  blob.make_public()) │  Sheets as a
                ▼                           ▼                      ▼  system of record)
┌──────────────────────────────────────────────────────────────────────────┐
│                     GCP project: skintific-data-warehouse                 │
│  BigQuery: 41 datasets (rsa, dms, gt_schema, pbi_gt_dataset, ...)          │
│  GCS: 22 buckets (none structured for this portfolio)                     │
│  No Secret Manager use · No Cloud Run use · No custom domain · No CI/CD   │
└──────────────────────────────────────────────────────────────────────────┘
```

No API layer, no load balancer, no CDN, no monitoring stack exists for this portfolio today. Each app is a monolith: UI, business logic, and data access all live in one Python file (occasionally split into a local package, e.g. `noo_sku/`), executed top-to-bottom on every user interaction (Streamlit's standard model).

---

## 2. Proposed Target Architecture

```
                              skintific.io
                                   │
                         DNS (Cloud DNS, new)
                                   │
                 ┌─────────────────┴─────────────────┐
                 ▼                                   ▼
         app.skintific.io                    api.skintific.io
       Static frontend (GCS + Cloud CDN,       Cloud Run API service(s)
       or a Cloud Run service serving a         (FastAPI, following the
       built SPA — see §5)                       already-live step-api
                 │                                pattern in this project)
                 │  HTTPS/JSON, JWT in Authorization header
                 └────────────────────┬──────────────────┘
                                      ▼
                     ┌────────────────────────────────┐
                     │   Cloud Run API (one service     │
                     │   per app-group, not per app —   │
                     │   see grouping below)            │
                     │                                  │
                     │  Auth middleware (JWT verify)     │
                     │  Business logic (ported from      │
                     │    assessment_logic.py, noo_sku/,  │
                     │    and newly-extracted equivalents) │
                     └───────┬─────────────────┬─────────┘
                             ▼                 ▼
                        BigQuery            GCS
                    (same 41 datasets,   (new, purpose-built
                     same tables —        buckets, private by
                     no data fork)        default, signed URLs)
```

**Interim state (Option A, Phase 3 of the roadmap)** — not pictured above, but real and load-bearing: before the frontend split, every app is simply a **containerized Streamlit process on Cloud Run**, still talking directly to BigQuery/GCS, with no separate API layer yet. This interim architecture is:

```
Users → Cloud Run (Streamlit container, --concurrency=1, min-instances tuned per app)
              │
              ├──> BigQuery
              └──> GCS
```

This is the actual Phase-3 target, and it is intentionally simple — it is not a stepping stone that gets thrown away, it is the thing that fixes the reported memory/hosting problem, and it can be the **permanent** end state for any app where a full Option-B rewrite is never justified by usage/value.

**Suggested Cloud Run service grouping for the eventual API layer** (avoid one Cloud Run service per app — 22 tiny services is unnecessary operational overhead; avoid one monolith either — that recreates today's coupling problem):

| Service | Apps it serves | Rationale |
|---|---|---|
| `po-api` | `po_buffer.py`, `po_portal_suggestion.py`, `po_simulator_v2.py`'s Request-PO page | Shared domain (PO suggestion/tracking/simulation), shared tables (`rsa.*`, `po_portal_*`) |
| `assessment-api` | `skt_area_execution_capability_v2.py`, its allocation-upload feature | Self-contained domain, already has extracted pure logic (`assessment_logic.py`) to build on |
| `field-ops-api` | `sfa_attendance.py`, `time_study_stopwatch.py`, `visit_validator/`, `smart_coverage.py`, `stock_opname_ssjabo.py`, `skt_top_20_store_list_stock.py` | Shared "field visit" domain; note `sfa_attendance.py` and `time_study_stopwatch.py` need the session-as-transaction redesign (MIGRATION_PLAN.md §5, M-1) done here, not deferred |
| `master-data-api` | `salesman_pjp.py`, `store_channelization.py`, `template_converter.py`, `noo_detector.py`, `noo_sku_mapping.py` | Shared "reference/master data maintenance" domain |
| `analytics-api` | `whitespace_map.py` / `whitespace_map_lightweight.py` | Geospatial-specific; keep isolated since it has the heaviest, most distinct dependency footprint (geopandas, folium) |

---

## 3. Data Flow

### Upload flow (target)

```
1. Frontend requests a signed upload URL from the relevant Cloud Run API
   (POST /uploads/init — validates auth, file-type allowlist, returns a
   time-limited signed PUT URL + an upload_id)
2. Browser PUTs the file directly to GCS using that signed URL
   (Cloud Run never sees the file bytes)
3. Frontend calls POST /uploads/{upload_id}/process
4. Cloud Run reads the file from GCS (not from the request body), runs the
   same validation/transformation logic ported from the original app
   (e.g. noo_sku_mapping.py's pipeline: validate → enrich → normalize →
   identity → dedup → preview), and either:
     a. returns a preview + a confirm token (for flows that need a human
        confirm step today — most of them), or
     b. writes directly to BigQuery/Sheets (for flows with no confirm step)
5. Frontend calls POST /uploads/{upload_id}/confirm with the confirm token
6. Cloud Run performs the actual BigQuery/GCS/Sheets write, using the
   idempotent-write pattern from time_study_stopwatch.py (dedup key +
   retry/backoff) as the standard for every write, not just that one app
```

### Download/export flow (target)

```
1. Frontend calls the relevant "generate export" endpoint on explicit
   user action (button click) — never on every page render, fixing
   M-3 from MIGRATION_PLAN.md
2a. Small file: Cloud Run generates in-memory (same pandas/openpyxl/
    reportlab logic as today) and returns it directly in the response
2b. Large file: Cloud Run generates, writes to a short-TTL GCS object,
    and returns a signed download URL
3. Browser downloads directly (from the response or from GCS)
```

### Authentication flow (target)

```
1. Frontend POSTs credentials to a shared auth endpoint (one for the
   whole portfolio, not per-app — replacing the 5 incompatible schemes
   documented in MIGRATION_PLAN.md §12)
2. Auth service checks a bcrypt/argon2 hash against a BigQuery or
   Cloud SQL user table (consolidating the several different user
   tables found today: assessment_users, po_portal_distributor_users,
   noo_sku_distributor_user, salesman_pjp's hardcoded password dict —
   this consolidation is itself a design decision requiring product
   sign-off, not a mechanical merge, since the tables encode different
   role models today)
3. On success, issues a JWT (mirroring step-api's existing pattern:
   HS256, ~24h expiry) containing user identity + role + company/
   distributor scope
4. Frontend attaches the JWT as a Bearer token on every subsequent
   Cloud Run API call
5. Cloud Run API middleware verifies the JWT and enforces row/company
   scoping SERVER-SIDE in the query (fixing the client-side-pandas-
   filter pattern found in po_portal_suggestion.py) — never trusts a
   client-supplied company/role value
```

### API architecture (illustrative endpoint shape, per service group)

```
GET  /health                              — Cloud Run liveness/readiness
POST /auth/login                          — shared auth endpoint
POST /auth/change-password
GET  /po/suggestions?company=...          — server-side filtered, replaces
                                             po_portal_suggestion.py's
                                             fetch-all-then-pandas-filter
GET  /po/tracking?company=...&from=...&to=... — bounded by default date range
                                             (fixes the unbounded-scan finding)
POST /po/feedback/uploads/init            — signed URL issuance
POST /po/feedback/uploads/{id}/process
POST /po/feedback/uploads/{id}/confirm
GET  /assessment/{distributor}/{period}   — role-scoped server-side
POST /assessment/{distributor}/{period}/submit
POST /assessment/allocation/uploads/init  — same signed-URL pattern
GET  /field-ops/visits/active             — replaces sfa_attendance.py's
                                             session-state-only check-in
                                             record with a real server row
POST /field-ops/visits/checkin
POST /field-ops/visits/{id}/checkout
```

---

## 4. GCS Architecture

Proposed bucket/prefix layout (new buckets — none of the 22 existing buckets are structured for this and none should be repurposed):

```
gs://skintific-streamlit-uploads-{env}/
    {app-group}/{upload_id}/original.xlsx      -- short TTL, deleted after processing
gs://skintific-streamlit-exports-{env}/
    {app-group}/{export_id}/result.xlsx        -- short TTL (e.g. 24h lifecycle rule)
gs://skintific-streamlit-frontend-{env}/       -- static frontend assets (Option B),
                                                   fronted by Cloud CDN, NOT public-write
```

- `{env}` = `staging` / `prod`, matching the DUPLICATION_PLAN.md environment separation.
- All buckets **private by default**; every read/write goes through a signed URL or the API's own credentials — no `blob.make_public()` calls anywhere in the new architecture (fixing the finding in `stock_opname_ssjabo.py`/`skt_top_20_store_list_stock.py`).
- Lifecycle rules auto-delete upload/export objects after a short retention window (they are working files, not the system of record — BigQuery is).

---

## 5. BigQuery Integration

No new datasets are needed for the data itself (§6 of MIGRATION_PLAN.md — all 41 datasets already exist and already hold every table this portfolio touches). What changes is **how** the apps connect:

- **Today**: each Streamlit process holds its own `bigquery.Client()`, built from a per-app plaintext service-account key in `st.secrets`.
- **Target**: each Cloud Run service holds one `bigquery.Client()` per container (cold-start-once, `@lru_cache`-equivalent — the FastAPI analog of `st.cache_resource`), authenticated via **Workload Identity** where possible (no key file at all) or, where a key is still required, a Secret-Manager-held key referenced via `--set-secrets` — never an env var holding the raw key (see the `step-api` anti-pattern flagged in MIGRATION_PLAN.md §11, which the new architecture must not repeat).
- **Query discipline carried into every ported query**: parameterized always (no f-string interpolation — fixing the pattern found in `po_buffer.py`, `po_simulator*.py`, `noo_detector.py`, `store_channelization.py`); explicit `WHERE` + sensible default bounds on every read (fixing the unbounded-scan findings); row/company scoping enforced in the query itself, not after fetch.
- **Write discipline**: adopt `time_study_stopwatch.py`'s pattern as the house standard — an idempotency key, a pre-write dedup check, retry/backoff, and (where the target table supports it) partitioning/clustering matched to the actual query pattern.
