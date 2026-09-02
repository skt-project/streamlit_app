# GCP Requirements

Companion to [MIGRATION_PLAN.md](MIGRATION_PLAN.md). Based on a read-only audit of the `skintific-data-warehouse` project on 2026-08-26 (no infrastructure was created, modified, or deleted to produce this document).

---

## 1. Required GCP services

| Service | Purpose in target architecture | Already enabled? |
|---|---|---|
| Cloud Run | Hosts every app (Option A) and every API service (Option B) | ✅ `run.googleapis.com` enabled |
| BigQuery | Unchanged — same 41 datasets, same tables | ✅ In use project-wide |
| Cloud Storage | Uploads, exports, static frontend hosting | ✅ In use project-wide (22 buckets, none scoped to this portfolio) |
| Secret Manager | Every credential this portfolio uses, replacing `.streamlit/secrets.toml` | ✅ `secretmanager.googleapis.com` enabled, **but zero secrets from this portfolio exist there today** |
| Cloud DNS | `skintific.io` and subdomains | ✅ `dns.googleapis.com` enabled, **but no managed zone exists — fully greenfield** |
| Artifact Registry | Container images for every Cloud Run service | ✅ `artifactregistry.googleapis.com` enabled, several repos exist for other apps, none for this portfolio |
| Cloud Build | CI/CD — build + deploy on push | ✅ `cloudbuild.googleapis.com` enabled, 3 triggers exist, none for this repo |
| Compute Engine (Load Balancer components) | Only needed if a single HTTPS Load Balancer fronts both the static frontend and multiple Cloud Run services under one apex domain | ✅ `compute.googleapis.com` enabled, no load balancer resources exist yet |
| Cloud Monitoring / Logging | Uptime checks, error tracking, Cloud Run metrics | Enabled by default project-wide, but **zero configuration exists for this portfolio** (1 unrelated uptime check total in the whole project) |
| Cloud CDN | Fronting the static frontend bucket for performance | Not separately checked — provisioned alongside the Load Balancer if Option B's static frontend goes that route |

**No new API enablement is a blocker to starting this work** — every API the target architecture needs is already turned on for this project.

---

## 2. Existing services — reuse, new, modify, deprecate

| Resource | Current state | Verdict |
|---|---|---|
| BigQuery datasets (`rsa`, `dms`, `gt_schema`, `pbi_gt_dataset`, `sadata`, `repsly`, etc.) | Hold every table this portfolio reads/writes | **REUSE** — no schema changes required by the migration itself (some apps need bug fixes to their write logic, e.g. adding a dedup key, but that's an app-level fix, not a new dataset) |
| `step-api` Cloud Run service | Live, unrelated (SFA-STEP), but architecturally the exact pattern this migration should copy (FastAPI, JWT auth, CORS to a separate frontend) | **REUSE as a reference pattern only** — do not touch this service; do not deploy this portfolio's apps into it |
| 29 existing service accounts | `readonly@`, `sfa-web-api@` show the correct least-privilege shape; none are scoped to this portfolio | **NEW** accounts needed (see DUPLICATION_PLAN.md §3 for the staging pair; production needs its own equivalent pair per app-group, not shared across groups, so an incident in one group's credentials doesn't expose another's data) |
| 22 GCS buckets | None structured for this portfolio; `public_skintific_storage` is intentionally public (not a template); no bucket has website-hosting configured | **NEW** buckets (see ARCHITECTURE.md §4 for the proposed layout); do not repurpose any existing bucket |
| 6 Secret Manager secrets | All unrelated (`irwan-ai-*`, GitHub OAuth tokens) | **NEW** secrets — every BigQuery service-account key and app-level secret this portfolio uses today needs a Secret Manager entry |
| Artifact Registry repos | Several `cloud-run-source-deploy` auto-repos exist for other apps; one `gcr.io` legacy repo | **NEW** repo(s), one per app-group is reasonable (`po-api`, `assessment-api`, `field-ops-api`, `master-data-api`, `analytics-api`), or one shared repo with per-service image paths |
| Cloud Build triggers | 3 exist, all for unrelated apps (`ar-aging-project`, `accountingerp`) | **NEW** trigger(s), one per Cloud Run service, on push to the migration branch (staging) and on merge to main (production) |
| Cloud DNS | **No managed zone exists** | **NEW** — create a managed zone for `skintific.io` once the domain is purchased/transferred; nothing to migrate away from |
| Load Balancer / SSL certs | None exist | **NEW**, only if the domain routing design in §5 needs one (see recommendation below) |
| Monitoring dashboards / alerting / uptime checks | 1 unrelated uptime check, default logging sinks only | **NEW** — full observability stack needs to be built for this portfolio, none of it exists to reuse |
| `.streamlit/secrets.toml` (per-app credential files, gitignored, on every dev machine + Streamlit Cloud's own secret store) | Live, working, but plaintext and per-app | **DEPRECATE** once every app has a Secret-Manager-backed equivalent; do not delete until the corresponding app has fully cut over (keep it live as the Streamlit-fallback's credential source through Phase 10) |
| Streamlit Community Cloud deployments (22 apps) | Live, in current use | **DEPRECATE** — but only per-app, only after its own soak period (MIGRATION_PLAN.md §9-10), never as a portfolio-wide switch-off |

---

## 3. IAM requirements

Follow the least-privilege shape already correctly used by `readonly@`/`sfa-web-api@` in this project — do not grant `roles/bigquery.admin`, `roles/owner`, or `roles/editor` to any new service account.

| Principal | Roles | Scope |
|---|---|---|
| Migration read SA (staging) | `roles/bigquery.dataViewer`, `roles/bigquery.jobUser` | Project-wide (read-only is low-risk project-wide; matches the existing `readonly@` pattern) |
| Migration write SA (staging) | `roles/bigquery.dataEditor` | Staging datasets only (`*_staging`), never production datasets |
| Per-app-group Cloud Run runtime SA (production, created at Phase 3/9, one per group) | `roles/bigquery.dataViewer` + `roles/bigquery.jobUser` project-wide; `roles/bigquery.dataEditor` scoped to only the specific tables that group's apps write to (dataset-level IAM or, where available, table-level IAM) | Narrowest scope that still lets the app function — audit each group's actual write tables (see FEATURE_MIGRATION_MATRIX.md) before granting |
| Per-app-group Cloud Run runtime SA — GCS | `roles/storage.objectAdmin` scoped to that group's specific upload/export bucket prefixes only, never project-wide `roles/storage.admin` | Bucket-level IAM condition on prefix where supported |
| Cloud Build service account | `roles/run.admin` (deploy), `roles/artifactregistry.writer` (push images), `roles/iam.serviceAccountUser` (act as the runtime SA) | Scoped to this portfolio's Cloud Run services/repos only |
| Human operators (migration team) | `roles/run.developer` for deploy/rollback, `roles/bigquery.dataViewer` for debugging — **not** `roles/run.admin` broadly and **not** direct production write access unless actively performing an approved write-path cutover | — |

**Cross-cutting IAM note from the audit**: `step-api`'s Cloud Run service currently exposes a full BigQuery service-account private key and a JWT secret via plaintext environment variables, readable by anyone holding `run.services.get` on the project — a much broader circle than intended credential holders. This is unrelated to the Streamlit portfolio and was already flagged directly to the user; it is repeated here only as a **negative example**: no service account key or app secret for this migration should ever be passed via `--set-env-vars`. Always use `--set-secrets` referencing Secret Manager, or Workload Identity with no key file at all.

---

## 4. Secrets

Every one of the following currently lives in plaintext, in `.streamlit/secrets.toml` (gitignored) or a hardcoded local-path fallback, and needs a Secret Manager entry before or during its app's migration:

- BigQuery service-account JSON (one per app today, several apps share the same underlying key — worth consolidating to one key per app-group during migration rather than preserving today's ad hoc per-app duplication)
- `po_portal_suggestion_dev.py`'s **second, separate** refresh credential (`st.secrets["refresh"]`)
- SMTP credentials (`assessment_email.py`)
- The shared password secret gating `po_simulator_v2.py`'s RSA page (`st.secrets["glowithyou"]`)
- The ~140 hardcoded plaintext distributor passwords in `salesman_pjp.py` — these are not currently secrets at all (they're in source); migrating them into Secret Manager is a stopgap at best, the real fix is replacing them with the shared auth service (MIGRATION_PLAN.md §11)
- Google Sheets API credentials used by `gspread` (`noo_sku_mapping.py`, `po_portal_suggestion_dev.py`, `smart_coverage.py`'s reference-data loader)

---

## 5. DNS / domain configuration

No DNS work is proposed to happen now (per the task's explicit instruction) — this is the design to execute once the domain is purchased and the team is ready for Phase 9.

```
skintific.io                 (Cloud DNS managed zone, new)
    │
    ├── app.skintific.io  →  frontend (GCS+CDN, or a Cloud Run service serving
    │                        a built SPA — decide based on how dynamic the
    │                        frontend needs to be; a pure static SPA calling
    │                        the API needs no server-rendering, favoring
    │                        GCS+CDN for lower cost and higher cache-ability)
    │
    └── api.skintific.io  →  Cloud Run API service(s) via Cloud Run domain
                             mapping, or via a Load Balancer if multiple
                             services need to share api.skintific.io under
                             different paths (e.g. /po/*, /assessment/*)
```

**Recommendation**: start with Cloud Run's built-in domain mapping (`gcloud run domain-mappings create`) for `api.skintific.io` pointed at a single API service per the ARCHITECTURE.md grouping, and a separate mapping or GCS+CDN setup for `app.skintific.io`. Only introduce a full HTTPS Load Balancer if/when the API needs to be split across multiple Cloud Run services under one hostname with path-based routing — domain mapping alone cannot do path-based fan-out to multiple services. Given the ARCHITECTURE.md recommendation of 5 app-group services, a Load Balancer with URL maps (`/po/*` → `po-api`, `/assessment/*` → `assessment-api`, etc.) is the likely eventual need — plan for it, but don't build it before the second Cloud Run API service exists.

HTTPS: Cloud Run domain mappings provision a managed certificate automatically; a Load Balancer setup needs an explicit `gcloud compute ssl-certificates create --domains=` managed certificate. Either path avoids needing Cloudflare or another third-party proxy layer, since the domain will already sit inside the same GCP project as everything else.

CORS: mirror `step-api`'s existing, working pattern — an explicit `CORS_ORIGINS` allowlist (that service currently lists its Vercel frontend + several localhost ports; the equivalent for this portfolio would list `https://app.skintific.io` plus local dev origins, never a wildcard `*`).

---

## 6. Estimated resource requirements

Rough starting points, to be tuned from real usage once Phase 2/3 is live — no production traffic data exists yet to size this precisely, so these are conservative defaults, not measurements:

| Cloud Run service | Suggested memory | Suggested CPU | min-instances | max-instances | Concurrency |
|---|---|---|---|---|---|
| Option-A containerized Streamlit apps (per app) | 512Mi–1Gi to start; raise for `whitespace_map.py`/`po_simulator_v2.py` specifically given their in-memory data volume (2-4Gi) | 1 vCPU | 0 for low-traffic internal tools, 1 for `po_buffer.py`/`po_portal_suggestion.py`-tier apps to avoid cold-start on every distributor visit | 2-5 (Streamlit's session model doesn't benefit from high fan-out per app) | **1** (Streamlit requires sticky WebSocket sessions — do not set higher) |
| `po-api`, `assessment-api`, `field-ops-api`, `master-data-api` (Option B) | 512Mi (stateless FastAPI, matches `step-api`'s existing 512Mi allocation) | 1 vCPU | 1 (avoid cold-start on the highest-traffic domains) | 100 (matches `step-api`'s existing ceiling) | 80 (FastAPI default concurrency, stateless requests) |
| `analytics-api` (geospatial) | 2Gi (geopandas/folium have a real memory footprint even server-side) | 1-2 vCPU | 0 | 10 | Lower, e.g. 10-20, given per-request memory use |

BigQuery: no new slot reservations needed — existing on-demand billing already serves this portfolio; the migration's query fixes (bounded WHERE clauses, parameterization) should **reduce** bytes-scanned/cost, not increase it.

GCS: new buckets sized by upload/export volume, which is currently unmeasured (no monitoring exists today, per §6 of MIGRATION_PLAN.md) — start with standard storage class + a 24-48h lifecycle-delete rule on the uploads/exports buckets, revisit after Phase 3 shows real volume.
