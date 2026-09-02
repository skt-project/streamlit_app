# Migration Plan — Streamlit Portfolio → Cloud Run / GCS / BigQuery

**Status: AUDIT + PLAN ONLY. No production code, data, or infrastructure was modified to produce this document.**
**Scope: the entire `D:\GitHub\streamlit_app` repository (github.com/skt-project/streamlit_app) — not one app.**
Audited 2026-08-26. GCP project: `skintific-data-warehouse`.

---

## 1. Executive Summary

This repository is not one Streamlit application — it is a **portfolio of ~22 independently-deployed Streamlit tools** sharing one GitHub repo, one unpinned `requirements.txt`, and Streamlit Community Cloud's free tier as the hosting model (one small shared instance per app, no autoscaling, idle-sleep). That hosting model — not any single line of application code — is the dominant explanation for the reported memory pressure, and it gets strictly worse as more tools are added or more users adopt them, independent of code quality.

The portfolio spans a very wide maturity range: from a well-structured, dependency-free reference app (`visit_validator/`) to a 2,735-line monolith with a disabled authentication gate (`po_simulator_v2.py`) to apps whose core transaction (a GPS check-in, a stopwatch timer) is held **only** in Streamlit's in-memory session state with no server-side record until the final write — a pattern fundamentally incompatible with Cloud Run's stateless, scale-to-zero execution model.

**Recommendation: do not attempt one big-bang rewrite.** Move the whole portfolio onto Cloud Run running containerized Streamlit first (Option A) — this alone fixes the actual reported symptom (memory/hosting limits) with low risk and no UI rewrite. Then decouple into a real Cloud Run API + static frontend (Option B) **app-by-app**, starting with the apps that are already stateless and dependency-light, while the apps with genuine architectural mismatches (session-state-as-transaction, disabled auth, non-atomic delete-then-insert writes) get a deliberate backend redesign — not a mechanical port — before their frontend is touched. Full reasoning in §9.

A separate, unrelated finding surfaced during the GCP audit and was already flagged to the user directly: the `step-api` Cloud Run service (a different project, SFA-STEP) stores a live BigQuery service-account private key and JWT secret in plaintext Cloud Run environment variables rather than Secret Manager. This is called out again in §12 because the new architecture must not repeat the pattern, but the rotation itself is independent of this migration's timeline.

---

## 2. Current Architecture

```
                     ┌───────────────────────────────────────────────┐
                     │        Streamlit Community Cloud (free)        │
                     │  ~22 independent apps, each = one .py file      │
                     │  in this repo, each its own small instance:     │
                     │  idle-sleep, cold-start, no autoscaling,        │
                     │  single shared Python process per app,          │
                     │  shared repo-wide `requirements.txt` installed  │
                     │  in full regardless of which app is deployed    │
                     └───────────────────────────┬─────────────────────┘
                                                  │ google-cloud-bigquery /
                                                  │ google-cloud-storage /
                                                  │ gspread, using per-app
                                                  │ st.secrets["connections"]
                                                  │ (plaintext SA key in
                                                  │ .streamlit/secrets.toml,
                                                  │ gitignored, not in
                                                  │ Secret Manager)
                     ┌───────────────────────────▼─────────────────────┐
                     │                skintific-data-warehouse          │
                     │  BigQuery (41 datasets) · GCS (22 buckets)       │
                     │  Google Sheets (gspread — 2 apps use Sheets      │
                     │  as a system of record, not just an import)     │
                     └───────────────────────────────────────────────┘
```

No Dockerfile, no CI/CD (`.github/workflows` absent), no load balancer, no custom domain, no centralized monitoring/logging, no Secret Manager usage anywhere in this repo. Deployment is direct GitHub→Streamlit-Cloud, apparently including edits made through GitHub's/Streamlit's own web editor (many commits are auto-generated "Update X.py" with no message).

**Portfolio inventory** (full detail in [FEATURE_MIGRATION_MATRIX.md](FEATURE_MIGRATION_MATRIX.md)):

| Tier | Apps | Characteristics |
|---|---|---|
| **Live production, business-critical** | `po_buffer.py`, `po_portal_suggestion.py`, `po_simulator_v2.py`, `skt_area_execution_capability_v2.py`, `salesman_pjp.py` | Real users, real writes, largest files, highest migration complexity |
| **Live production, self-contained** | `noo_detector.py`, `visit_validator/`, `sfa_attendance.py`, `smart_coverage.py`, `store_channelization.py`, `stock_opname_ssjabo.py`, `skt_top_20_store_list_stock.py`, `time_study_stopwatch.py`, `template_converter.py`, `whitespace_map.py`, `whitespace_map_lightweight.py` | Single-purpose, smaller, varying complexity |
| **Built, not yet live** | `noo_sku_mapping.py` + `noo_sku/` package | Dry-run verified, zero real production writes so far |
| **Superseded / test harness** | `po_portal_suggestion_v2.py` (fixes never merged to prod), `po_portal_suggestion_dev.py` (explicit test harness), `skt_area_execution_capability_mock.py`, `po_simulator.py` (quiet since 2026-06) | Do not treat as independent migration targets |
| **Dead — do not migrate** | `skt_area_execution_capability.py` (writes to the same live table as v2 with an incompatible schema if ever run — archive, don't port), `po_portal/`, `po_portal_mockup.zip` (source deleted, only cache artifacts remain) | Flag for archival |
| **Out of scope (not Streamlit apps)** | `docs_crawler.py`, `build_html_guide.py`, `build_pptx.py` | Internal doc-generation tooling |
| **Shared, reusable as-is** | `assessment_logic.py` (pure, 42 tests, 98% coverage), `assessment_email.py` (mostly pure), `noo_sku/` package (pure, 73 tests) | Port directly into a future backend — the only genuinely reusable business logic found in the whole portfolio |

---

## 3. Streamlit-Specific Dependency Analysis

Cross-cutting patterns found across all 22 apps, and how each maps onto the target architecture:

| Streamlit API | Where/how used today | Target replacement |
|---|---|---|
| `st.session_state` | Ranges from 2 keys (`po_portal_suggestion.py`) to dozens of dynamically f-string-templated keys (`po_simulator_v2.py`); **in two apps it is the ONLY record of an in-progress transaction** (`sfa_attendance.py`'s check-in, `time_study_stopwatch.py`'s running timer) with no BigQuery row until the final write | Short-lived UI state → frontend component state (React/Vue state or `localStorage`). Any state that represents a **transaction** (checked-in-but-not-out, a submitted-but-not-confirmed upload) must move to a server-side row (BigQuery draft row or Firestore) written at the *start* of the transaction, not just at the end — this is a required behavior change, not a lift-and-shift |
| `st.cache_data` | Universal; TTLs 30s–6h; several with **no `max_entries`** (`po_portal_suggestion.py` ×3) allowing unbounded per-key cache growth | Cloud Run: HTTP response caching (Cloud CDN / `Cache-Control` headers) or an explicit Redis/Memorystore layer if cross-instance cache sharing is needed; BigQuery result caching (native, free) covers the query-repeat case already |
| `st.cache_resource` | Correctly used for the BigQuery client in 8 of ~15 files that need it; **missing in `po_portal_suggestion.py` and `skt_area_execution_capability_v2.py` — both live production** (client rebuilt, and service-account key re-parsed, on every single user interaction) | A singleton BigQuery client constructed once per Cloud Run container at cold start (module-level in the FastAPI app), reused across requests within that instance |
| `st.file_uploader` | Universal; **no app in the portfolio sets an explicit max-size guard** — all rely on Streamlit's global 200MB default | Browser → direct-to-GCS upload via a signed URL (see §6 Excel Upload Flow) with an explicit, enforced size cap set server-side when the signed URL is issued |
| `st.download_button` | Universal for Excel/PDF exports, several regenerated **unconditionally on every script rerun** regardless of whether the user clicks download (`po_portal_suggestion.py` ×2) | An explicit "generate export" API call, triggered only on user action, returning either the file directly (small) or a signed GCS download URL (large) |
| `st.form` | Used for CRUD edit panels (`salesman_pjp.py`), login forms, upload confirmations | Standard HTML form + client-side validation, POSTing to a Cloud Run endpoint |
| `st.rerun()` | Used for page-nav-via-sidebar, post-login refresh, and — critically — **inside two busy-loops** (`time_study_stopwatch.py`'s 1-second timer poll and GPS-acquisition poll) that hold a live WebSocket/container open for the duration of a store visit (potentially hours) | Client-side JS state transitions (React re-render) for navigation; the timer/GPS polling loops must move to client-side `setInterval`, hitting the server only at Start/Stop boundaries — this is a required redesign, not a mechanical port (see §7, Finding M-1) |
| `st.secrets` | Universal for BigQuery service-account JSON; several files **also** have a hardcoded local Windows-path credential fallback (`C:\script\...`, `C:\Users\Bella Chelsea\...`) that must never reach a shared repo/CI | GCP Secret Manager, referenced via `--set-secrets` in the Cloud Run deploy, never baked into the image or committed to source |
| `st.query_params` | Not used anywhere in the portfolio | N/A |
| Auth (no single Streamlit mechanism — 5 incompatible schemes coexist) | See §12 Security | A single, shared auth service (JWT-based, following the `step-api` precedent already live in this GCP project) issuing tokens the frontend attaches to every Cloud Run API call |

---

## 4. Excel Upload — Deep Dive

Every upload flow in the portfolio follows the same shape today: `st.file_uploader` → full in-memory `pd.read_excel`/`pd.read_csv` → in-process pandas validation/transformation → BigQuery insert or GCS upload — **no app streams or chunks a file, and none writes to a local temp file** (confirmed across all 22 apps). This is actually good news for migration: there is no local-disk-dependent Excel processing anywhere to re-platform.

```
CURRENT (all 22 apps, same shape):
  Browser → Streamlit server (single process) → pandas (full file in RAM)
          → validate/transform → BigQuery insert_rows_json / Sheets append / GCS blob

PROPOSED:
  Browser → signed GCS upload URL (issued by Cloud Run API) → GCS
          → Cloud Run "process upload" endpoint reads from GCS (not from the
            request body) → pandas validate/transform → BigQuery / Sheets write
          → status returned to frontend (sync for small files, or a
            job-id + polling/webhook for large ones)
```

**Do not send files through Cloud Run's request body by default** — Cloud Run has a 32MB request-body limit as of typical default configuration and holds the container's memory for the request duration; direct-to-GCS via signed URL avoids both constraints and matches the task's own stated preference. The one exception worth keeping simple: files that are already small and processed synchronously today (e.g., `smart_coverage.py`'s invoice photos, already uploaded straight to GCS via a server-side call — that pattern is fine to keep, since the file goes to GCS either way, just via the Cloud Run process today vs. a signed URL tomorrow).

**Per-app specifics worth carrying over or fixing during the rewrite** (not exhaustive — see the matrix for all 22):
- `noo_sku_mapping.py`: two-key duplicate detection (identity key + content hash, deliberately excluding volatile master-data fields) is a genuinely well-designed idempotency pattern — port the logic as-is into the backend.
- `time_study_stopwatch.py`'s BigQuery write path (idempotent `row_ids`, client-side pre-dedup, retry/backoff, partitioned+clustered target table) is the **best-built write path in the whole portfolio** — use it as the template for every other app's insert logic, most of which have no dedup guard at all.
- `salesman_pjp.py`'s PJP replace flow does a **non-atomic delete-then-insert** (its own error string admits data loss if the insert fails after the delete succeeds) — this must become a single `MERGE` or a transactional multi-statement script in the backend rewrite, not carried forward as-is.
- `sfa_attendance.py`'s checkout write has **no idempotency key** (`visit_id` is freshly generated on every render) — a double-click or retry after a network blip creates a duplicate visit row today; fix during the rewrite regardless of hosting change.
- Five apps (`po_buffer.py`, `po_simulator.py`, `po_simulator_v2.py`, `noo_detector.py`, one query in `store_channelization.py`) build BigQuery `WHERE`/`IN` clauses via f-string interpolation rather than parameters. None are currently exploitable (inputs come from selectboxes or internal codes, not free-text fields), but this pattern should not be carried into new backend code, and `store_channelization.py`'s `check_duplicate_cust_ids` is the one case where the interpolated values **do** originate from an uploaded file — fix this one specifically before any migration testing against real data.

---

## 5. Memory / Scalability Audit

Ranked findings, ordered by how directly each explains "memory limitations on the current free-tier host":

| # | Finding | App(s) | Current behavior | Root cause | Priority |
|---|---|---|---|---|---|
| M-1 | **Server-held polling loops** | `time_study_stopwatch.py` | `time.sleep(1)` + `st.rerun()` runs every second for the duration of an active timer or a GPS-acquisition wait (up to 15s), holding a live WebSocket/process thread open for potentially hours per active field visit | Streamlit's rerun model used for a client-side concern (a stopwatch face) | **Highest** — this pattern alone defeats Cloud Run's scale-to-zero economics and its request-duration limits; must be redesigned (client-side JS timer) before or during migration, independent of hosting |
| M-2 | **Eager, whole-country data load** | `whitespace_map.py` | Loads a full-Indonesia GeoDataFrame, a full Nielsen workbook, and an unfiltered national store table into `st.cache_resource`/`st.cache_data` **at module level, before any user selection** | No request-scoped filtering; assumes one long-lived warm process | High — this exact problem was already solved once in this same codebase (see below) |
| M-3 | **Unconditional per-row/per-rerun heavy work** | `po_buffer.py` (PDF generation for every visible store row, every rerun, not gated behind a button); `po_portal_suggestion.py` (two Excel exports regenerated every rerun) | CPU-bound work repeats on every filter change whether or not the user asked for the output | Export/generation code not gated behind an explicit action | High — cheap fix, already fixed with a precedent pattern in `po_portal_suggestion_v2.py` |
| M-4 | **Unbounded/unfiltered BigQuery reads** | `po_portal_suggestion.py` (both suggestion queries, no WHERE/LIMIT, **now run twice per load** since the 2026-08-20 dynamic-table promotion added a second query without removing the first); `po_buffer.py`'s full buffer load; `whitespace_map.py`'s national join; `noo_detector.py`'s full store table; `skt_top_20_store_list_stock.py`'s **uncached** PO-suggestion query (re-scans on every single interaction) | Full-table fetch into pandas, filtered client-side (or not filtered at all) | Missing server-side `WHERE`, missing cache decorator | High — grows worse automatically as tables accumulate rows, with zero code change |
| M-5 | **Uncached BigQuery client construction** | `po_portal_suggestion.py`, `po_portal_suggestion_dev.py`, `skt_area_execution_capability_v2.py` (all three currently in production) | Client + credential parsing rebuilt on every widget interaction | Missing `@st.cache_resource` | Medium — cheap, mechanical fix; precedent already exists in 8+ sibling files in this same repo |
| M-6 | **Uncapped per-key cache growth** | `po_portal_suggestion.py` (`load_po_tracking` keyed by company, no `max_entries`) | One cache entry per distinct company seen in the TTL window, including a full-table "Admin" entry | Missing `max_entries` | Medium |
| M-7 | **One shared `requirements.txt` for all 22 apps** | Every deployment | Every app's Streamlit Cloud instance installs `geopandas`, `folium`, `matplotlib`, `reportlab`, `pyarrow`, `gspread`, etc. regardless of whether that specific app needs them | No per-app dependency isolation | Medium — resolved automatically by containerizing each app with its own `requirements.txt`/Dockerfile in Phase 3 |

**What is already proven to work, in this exact codebase**: `whitespace_map_lightweight.py` is a real, already-shipped rewrite of `whitespace_map.py` that eliminates M-2 entirely — it replaced the live national BigQuery join + whole-country `gpd.sjoin` with an offline pre-processing step (not present in the repo, needs to be located) that writes small, pre-aggregated, per-Kabupati parquet files to GCS, which the app then reads lazily and narrowly. **This is the pattern to generalize**: push expensive joins/aggregations out of the request path into a scheduled batch job (Airflow, already in use elsewhere per prior audits), and have the Cloud Run app do cheap, narrowly-scoped reads. Recommend applying this same technique to `po_portal_suggestion.py`'s and `po_buffer.py`'s unfiltered queries.

**What is not a memory problem, for balance**: session-state usage itself is minimal-to-moderate in most apps and not a source of memory growth on its own (only `po_simulator_v2.py`'s dozens of dynamically-keyed per-file/per-SKU keys come close to being a concern, and even there it's bounded by how many files one user uploads in one session, not unbounded). No N+1 query pattern was found anywhere in the portfolio.

---

## 6. GCP Infrastructure Audit

Full detail in [GCP_REQUIREMENTS.md](GCP_REQUIREMENTS.md). Summary:

| Category | Finding | Verdict |
|---|---|---|
| Cloud Run | No Streamlit-related service exists yet. `step-api` (SFA-STEP, a different project/domain) is a live, working precedent for this exact target pattern — FastAPI on Cloud Run, min-scale=1/max-scale=100, CORS to a separate frontend | **NEW** services needed; reuse `step-api`'s deployment shape as a template |
| GCS | 22 buckets exist, none structured for this portfolio's uploads or for a static frontend; no bucket has website-hosting configured; `public_skintific_storage` is intentionally `allUsers`-readable (fine for its purpose, not a template to copy) | **NEW** buckets needed, least-privilege from the start |
| BigQuery | 41 datasets already hold every table this portfolio touches (`rsa`, `dms`, `gt_schema`, `pbi_gt_dataset`, etc.) | **REUSE** entirely — no new datasets needed for the data itself |
| Service accounts | 29 exist; `readonly@` and `sfa-web-api@` show the right least-privilege shape (`bigquery.dataViewer` + `bigquery.jobUser`) but none is scoped to this portfolio | **NEW** — one read SA and one write SA minimum, ideally per app-group, following the existing least-privilege pattern |
| Secret Manager | 6 secrets exist, all unrelated; **zero** Streamlit app credentials are here today (they live in `.streamlit/secrets.toml`, gitignored, on Streamlit Cloud's own store) | **NEW** — every credential this portfolio uses must move here before or during migration |
| Artifact Registry | Several auto-created `cloud-run-source-deploy` repos exist for other apps | **NEW** repo(s) for this portfolio |
| Cloud Build | 3 triggers exist, none for this repo | **NEW** trigger(s) |
| Cloud DNS | **No managed zones exist at all** — `skintific.io` is not configured anywhere in this project yet | **NEW** — fully greenfield, confirms §10 domain work has no legacy constraint to work around |
| Load Balancer / SSL | None exist | **NEW**, only if fronting multiple Cloud Run services + a GCS static bucket under one apex domain (see §10) |
| Monitoring/Logging | Only 1 unrelated uptime check exists; only default logging sinks, no custom export/alerting | **NEW** — 100% of observability for this portfolio needs to be built |
| APIs enabled | `run`, `secretmanager`, `dns`, `compute`, `cloudbuild`, `artifactregistry` are **already enabled** on the project | **REUSE** — no new API enablement blocking the start of this work |

---

## 7. Target Architecture Options

### Option A — Minimal Migration (containerize Streamlit as-is on Cloud Run)

```
Users → Cloud Run (Streamlit in a container, per app or per app-group)
              │
              ├──> BigQuery (unchanged queries, fixed per §5/§3 findings)
              └──> GCS (unchanged upload/download calls)
```

- **Advantages**: solves the actual reported problem (memory/hosting limits) directly — Cloud Run offers configurable memory (512Mi–32Gi) and real horizontal autoscaling vs. Streamlit Cloud's fixed small instance; no UI rewrite; every app can move independently, in any order; lowest risk per app; the M-3/M-5/M-6 fixes from §5 are small, mechanical, and several already have a working precedent elsewhere in this same repo.
- **Disadvantages**: does not fix M-1 (server-held polling loops) or the session-state-as-transaction pattern in `sfa_attendance.py`/`time_study_stopwatch.py` — Streamlit's own execution model still requires sticky WebSocket sessions, which constrains Cloud Run's autoscaling and concurrency settings (must pin `--concurrency=1` per Streamlit's own session model, or accept session pinning quirks); does not address the 5-incompatible-auth-scheme problem; does not give a modern, fast, mobile-friendly UI.
- **Migration effort**: Low-Medium per app. A Dockerfile + `requirements.txt` scoped per app + moving secrets to Secret Manager + fixing the caching/query bugs already identified.
- **Scalability**: Materially better than today (real autoscaling, no idle-sleep cold starts if min-instances ≥ 1), but still fundamentally bounded by Streamlit's single-process-per-session model — it will not scale a single app to thousands of concurrent interactive users the way a stateless API + static frontend can.
- **Risk**: Low. Each app is a near-1:1 port; rollback is "point the DNS/link back at Streamlit Cloud."

### Option B — Full Decoupled Architecture (Cloud Run API + static frontend)

```
Browser → Static Frontend (GCS + Cloud CDN, or Cloud Run serving a built SPA)
              │
              ▼
        Cloud Run API (FastAPI, following the step-api precedent)
              │
        ┌─────┴─────┐
        ▼           ▼
    BigQuery       GCS
```

- **Advantages**: true statelessness — scales to any number of concurrent users with per-request billing; modern UI/UX possible; a single shared auth/session layer replaces the 5 incompatible schemes; direct-to-GCS signed uploads remove the file-size ceiling entirely; enables reuse of one API by multiple frontends (web, potentially mobile) exactly as `step-api`/`sfa-mobile` already demonstrate elsewhere in this org.
- **Disadvantages**: a full rewrite of every UI — 22 apps' worth of forms, tables, filters, wizards, and dialogs need a new frontend implementation; the apps with session-state-as-transaction (`sfa_attendance.py`, `time_study_stopwatch.py`) and multi-step stateful wizards (`po_simulator_v2.py`'s RSA page, `skt_area_execution_capability_v2.py`'s confirm-dialog-then-insert pattern, `salesman_pjp.py`'s inline CRUD) need genuine backend redesign (a staged draft/confirm/commit pattern), not a mechanical translation — this is real engineering, not busywork.
- **Migration effort**: High. Realistically a multi-month program across 22 apps if done for all of them at once.
- **Scalability**: Best available — matches the target architecture diagram in the task brief exactly.
- **Risk**: Medium-High if attempted all at once; Low-Medium if done incrementally, one app at a time, after Option A has already stabilized hosting.

### Recommendation

**Do Option A first, for the whole portfolio, then Option B incrementally, app-by-app, starting with the lowest-complexity apps.** Justification:

1. The reported pain (memory limits) is a **hosting-model** problem first and a **code** problem second (§5, §2). Option A fixes the hosting model in weeks, not months, for all 22 apps simultaneously, with low risk per app.
2. A big-bang Option B rewrite of 22 apps — several of which need genuine architectural redesign, not just a UI port — is a multi-month program with real risk of stalling mid-migration, leaving some apps half-migrated and users confused about which URL is authoritative.
3. Doing Option A first buys time to design the shared pieces Option B needs regardless of order: one auth service, one Secret Manager migration, one Excel-upload-via-signed-URL pattern, one BigQuery-client-per-container pattern — all of which are also required for Option A and are directly reusable when Option B work begins.
4. Suggested Option-B ordering once Option A is stable, cheapest/lowest-risk first: `visit_validator/` (already has zero coupling to the rest of the repo — literally the reference template), `template_converter.py`, `noo_detector.py` (fix the two flagged issues first), `smart_coverage.py`, `stock_opname_ssjabo.py`, `whitespace_map_lightweight.py` → then the medium-complexity apps → then the four apps flagged "Needs redesign" (`time_study_stopwatch.py`, `sfa_attendance.py`, `po_simulator_v2.py`'s wizard flows, `skt_area_execution_capability_v2.py`) last, each preceded by its own dedicated design pass, not a mechanical port.

---

## 8. Duplication Strategy

Full detail in [DUPLICATION_PLAN.md](DUPLICATION_PLAN.md). Summary: yes, this can be duplicated safely without touching production, using a new git branch/fork, a read-only BigQuery service account for all query testing, and either a separate BigQuery dataset or row-level test markers for anything that needs to exercise a write path. Production Streamlit Cloud deployments are untouched throughout — nothing in this plan requires disabling or redirecting them until Phase 9.

---

## 9. Migration Roadmap

| Phase | Objective | Key tasks | Risk | Rollback | Success criteria |
|---|---|---|---|---|---|
| **0 — Audit** | Understand the system completely | This document + its 4 companions | None (read-only) | N/A | This document exists and is reviewed |
| **1 — Duplicate** | Stand up an isolated copy | Fork/branch the repo, provision a read-only BQ SA, stand up a test dataset/GCS prefix (see DUPLICATION_PLAN.md) | Low | Delete the fork | A second, isolated environment exists; prod untouched |
| **2 — Isolated test env** | Deploy the duplicate somewhere real | Containerize 2-3 pilot apps (start with `visit_validator/`, `template_converter.py`, `noo_detector.py`) onto Cloud Run in a `-staging`-suffixed service, behind no custom domain yet | Low | Delete the Cloud Run service | Pilot apps run on Cloud Run against read-only/test data, side-by-side with production Streamlit Cloud |
| **3 — Backend/Cloud Run for the rest** | Containerize remaining Option-A candidates | Per-app Dockerfile + scoped `requirements.txt`; move all secrets to Secret Manager; fix M-3/M-5/M-6 findings during the port (cheap, high-value) | Medium | Redirect users back to the Streamlit Cloud URL (still live) | Every app runs on Cloud Run with equal or better behavior than today, verified side-by-side |
| **4 — Excel upload/download to GCS** | Remove the Cloud-Run-request-body bottleneck | Implement signed-URL direct upload for the highest-volume upload flows first (`po_portal_suggestion.py` feedback, `salesman_pjp.py` PJP template, `skt_area_execution_capability_v2.py` allocation) | Medium | Fall back to server-side upload handling (already works, just less scalable) | Large files no longer risk hitting a Cloud Run request-size/memory ceiling |
| **5 — Build frontend for pilot apps** | Prove Option B on the lowest-risk apps | Real SPA/static frontend for `visit_validator/`, `template_converter.py`, `noo_detector.py`, calling a new Cloud Run API | Medium | Keep serving the Option-A containerized Streamlit version of these same apps | Pilot users can complete the same task via the new frontend with no data discrepancy vs. Streamlit |
| **6 — Internal testing** | Validate before any real user sees the new frontend | Internal team dogfoods pilot apps for 1-2 weeks; compare outputs row-for-row against Streamlit's outputs on the same inputs | Low | N/A (pre-external) | Zero data discrepancies found; internal sign-off |
| **7 — Pilot users** | Real users, small blast radius | A handful of real distributors/SPVs use the new frontend for pilot apps only, Streamlit Cloud stays live as the default for everyone else | Medium | Redirect pilot users back to Streamlit Cloud | Pilot users complete real workflows successfully; support load is manageable |
| **8 — Gradual migration** | Expand app-by-app | Repeat Phases 5-7 for the next tranche of apps, in the ordering given in §7's recommendation, saving the 4 "needs redesign" apps for last with their own dedicated design pass first | Medium-High (rises with each "needs redesign" app) | Per-app rollback to either Cloud-Run-Streamlit (Option A) or original Streamlit Cloud | Each migrated app matches or beats its predecessor's functionality |
| **9 — Production cutover** | Make the new frontend the default | DNS points `app.skintific.io`/`api.skintific.io` at the new stack; Streamlit Cloud links are relabeled "legacy/fallback" | High (this is the point of no easy return for user habit, not for data) | DNS revert (see §10) — data itself was never at risk since BigQuery/GCS are shared, not duplicated, once cutover happens | New stack is the default entry point for all migrated apps; support tickets return to baseline within an agreed window |
| **10 — Fallback/decommission** | Retire Streamlit only once proven stable | Keep Streamlit Cloud deployments live but unlinked for an agreed soak period (recommend minimum 4-6 weeks per app after its own cutover); decommission only after that period shows no rollback need | Low if the soak period is honored | Re-link Streamlit Cloud, which was never turned off | Decommission decision is made per-app, not portfolio-wide, and only after its own soak period |

---

## 10. Rollback Strategy

Production Streamlit Cloud deployments are **never disabled** during Phases 0-9 — they keep running, unlinked-but-live, throughout. Rollback at any phase is therefore always available and cheap:

- **Phases 0-8**: nothing user-facing has changed yet for apps not yet in their own Phase 5-7; rollback is simply "don't proceed to the next phase for that app."
- **Phase 9 (cutover)**: rollback is a DNS/link change — point users back at the original `*.streamlit.app` URLs, which are still running. No data migration needs to be undone, because BigQuery and GCS are the **same** underlying data throughout (see §11) — the new stack reads/writes the same tables, it does not fork them.
- **Phase 10 (decommission)**: only happens after a soak period with no rollback need; if a rollback is needed after decommissioning one app, redeploying that one app's original `.py` file to Streamlit Community Cloud is a same-day operation (it's still in git history).

---

## 11. Security

Findings, current state → target state:

| Area | Current state | Target state |
|---|---|---|
| Credentials | Plaintext service-account JSON in `.streamlit/secrets.toml` per app (gitignored, but present on every developer machine and Streamlit Cloud's own store); several files additionally hardcode a local Windows credential-file fallback path | Google Secret Manager, referenced via Cloud Run `--set-secrets`, never baked into an image or committed |
| Passwords | **5 incompatible schemes coexist**: (1) no auth at all — most apps; (2) plaintext comparison against a BigQuery column literally named `password_hash` (`po_portal_suggestion.py` and all its variants); (3) a disabled/dead primary auth check plus one shared secret gating only one page (`po_simulator_v2.py`); (4) bcrypt-hashed passwords in a dedicated table (`noo_sku_mapping.py` — the **one correct implementation** in the portfolio); (5) ~140 hardcoded plaintext distributor passwords directly in source (`salesman_pjp.py`) | One shared auth service issuing JWTs (mirroring the already-live `step-api` pattern in this same GCP project), bcrypt/argon2 password storage universally, no hardcoded credentials in source under any circumstance |
| Authorization | Role/company scoping enforced **client-side in pandas after an unfiltered fetch** in several apps (`po_portal_suggestion.py`'s pre-2026-08-20 RLS, still true for its dynamic-matrix path) rather than in the query or at an API boundary | Row/company scoping enforced server-side, in the query or in the API layer, never trusted from client state |
| File upload | No app enforces an explicit size cap; no malicious-file-content scanning anywhere in the portfolio; `noo_detector.py` exposes NIK/NPWP (PII) with zero authentication | Explicit size caps on every signed upload URL; basic content-type/structure validation before processing; auth added to any tool handling PII before it leaves an internal-only network model |
| GCS bucket permissions | `stock_opname_ssjabo.py` and `skt_top_20_store_list_stock.py` call `blob.make_public()` on every uploaded supporting document (photos/invoices), making them world-readable via a predictable path | Private-by-default buckets with signed URLs for any legitimate read access; remove `make_public()` calls during migration regardless of hosting change |
| SQL construction | Parameterized correctly in the newest/best-audited apps (`skt_area_execution_capability_v2.py`, `noo_sku/sources.py`, most of `salesman_pjp.py`); f-string interpolation (not currently exploitable, but not defense-in-depth) in `po_buffer.py`, `po_simulator*.py`, `noo_detector.py`, one query each in `store_channelization.py` (fed by an uploaded file — the one genuinely concerning instance) | Parameterized queries everywhere, no exceptions, in any new/rewritten backend code |
| Secrets in Cloud Run (separate, already-flagged finding) | `step-api` (a different, already-deployed service in this same project) stores a full BigQuery SA private key and a JWT secret as plaintext Cloud Run env vars, readable by anyone with `run.services.get` | Do not repeat this pattern in the new architecture; recommend (independently of this migration's timeline) rotating that specific key/secret and moving both to Secret Manager |

**Never expose in frontend code** (per task requirement): none of the 22 apps currently embed a service-account key or secret in anything that reaches a browser — all credential handling happens server-side in the Streamlit process today, and this property must be preserved (not regressed) when splitting into a frontend + Cloud Run API, since a naive "just call BigQuery from the browser" shortcut would violate it.

---

## 12. Scalability

Covered in depth in §5 and §7. One-line summary: Option A alone converts the hosting model from "one small fixed instance, shared across all users of one app, idle-sleeping between visits" to "autoscaled Cloud Run instances with configurable memory," which is the single highest-leverage, lowest-risk change available — do it first, portfolio-wide, before any frontend decoupling work begins.

---

## 13. Estimated Complexity

| Complexity tier | Apps | Why |
|---|---|---|
| **Easy** | `visit_validator/app.py`, `template_converter.py` (with minor fixes), `stock_opname_ssjabo.py` | Stateless, no/minimal auth complexity, no destructive writes, small |
| **Easy-Medium** | `po_buffer.py`, `po_simulator.py`, `noo_detector.py` (after two flagged fixes), `skt_top_20_store_list_stock.py` | Read-mostly or single-writer-path, moderate size |
| **Medium** | `smart_coverage.py`, `po_portal_suggestion_v2.py` (already fixed, but feature-behind), `whitespace_map_lightweight.py` | Some auth/RLS or moderate business logic, but no fundamental architecture mismatch |
| **Medium-Hard** | `noo_sku_mapping.py` + `noo_sku/`, `store_channelization.py` | Best-modularized code in the portfolio, but the hard question is architectural (Sheets-as-system-of-record) not code quality |
| **Hard / Needs redesign** | `po_portal_suggestion.py`, `po_simulator_v2.py`, `skt_area_execution_capability_v2.py`, `salesman_pjp.py`, `time_study_stopwatch.py`, `sfa_attendance.py`, `whitespace_map.py` | Dual read/write schemas, disabled auth, multi-role stateful wizards, non-atomic writes, session-state-as-transaction, or (for `whitespace_map.py`) a whole-country eager-load pattern that needs the same fix already proven in its own `_lightweight` sibling |
| **Retire, don't migrate** | `skt_area_execution_capability.py`, `po_portal/`, `po_portal_mockup.zip` | Dead code pointing at live tables/credentials, or already-abandoned prototypes |

Full per-feature detail: [FEATURE_MIGRATION_MATRIX.md](FEATURE_MIGRATION_MATRIX.md).
