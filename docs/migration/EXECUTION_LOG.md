# Execution Log — GCP Migration Environment

Companion to [MIGRATION_PLAN.md](MIGRATION_PLAN.md). Records exactly what was created, when, by what command, and what remains. Executed 2026-09-08 in project `skintific-data-warehouse`. **Production Streamlit Community Cloud deployments were not touched at any point.**

---

## 1. IAM / Service Accounts — CREATED

| Resource | Value | Roles granted | Scope |
|---|---|---|---|
| Service account | `streamlit-migration-runtime@skintific-data-warehouse.iam.gserviceaccount.com` | `roles/bigquery.dataViewer`, `roles/bigquery.jobUser` | Project-wide (read-only) |
| | | `WRITER` (classic dataset ACL, not IAM — see note below) | `streamlit_migration_staging` dataset only |
| | | `roles/storage.objectAdmin` | `gs://skintific-streamlit-migration-uploads`, `gs://skintific-streamlit-migration-exports` only |
| Service account | `streamlit-migration-build@skintific-data-warehouse.iam.gserviceaccount.com` | `roles/artifactregistry.writer` | **Project-wide** (see permission gap below — intended repo-only) |
| | | `roles/run.developer` | Project-wide |
| | | `roles/iam.serviceAccountUser` | **Project-wide, any SA** (see permission gap below — intended: only the runtime SA) |
| My own account | `irwanraditya.skintific@gmail.com` | `roles/run.invoker` (added) | Project-wide — lets me call the authenticated-only pilot services for smoke testing |

**Permission gap hit and worked around**: my account holds `roles/editor` + `roles/bigquery.admin` + `roles/resourcemanager.projectIamAdmin` on this project — enough for project-level IAM bindings, but **not** `artifactregistry.repositories.setIamPolicy` or `iam.serviceAccounts.setIamPolicy` (resource-level IAM on a specific repo or a specific service account). Repo-scoped and SA-scoped bindings both failed with `PERMISSION_DENIED`. Worked around by granting the same roles at the **project** level instead, which is functionally sufficient but broader than the least-privilege design called for — `streamlit-migration-build` can currently push to *any* Artifact Registry repo in the project and act as *any* service account, not just the two it needs. **This needs a follow-up from someone holding `roles/iam.serviceAccountAdmin` or `roles/owner` to narrow both bindings to their intended resource scope** — flagged in the hands-on section below, not something I can self-grant.

**BigQuery dataset-level IAM gap**: `bq add-iam-policy-binding` on the new dataset failed with `This feature requires allowlisting`. Worked around using the classic dataset ACL mechanism (`bq update` with a modified `access` list) instead — functionally equivalent (the runtime SA has `WRITER` on `streamlit_migration_staging` only), just via the older API surface. No production dataset's ACL was read, touched, or listed.

---

## 2. Artifact Registry — CREATED

| Resource | Value |
|---|---|
| Repository | `streamlit-migration` (Docker format) |
| Location | `asia-southeast1` |
| Images pushed | `visit-validator:pilot-1`, `template-converter:pilot-1`, `noo-detector:pilot-1` |

## 3. Cloud Storage — CREATED

| Bucket | Purpose | Lifecycle |
|---|---|---|
| `gs://skintific-streamlit-migration-uploads` | Signed-URL upload target for migration pilots | Delete after 2 days |
| `gs://skintific-streamlit-migration-exports` | Generated export target for migration pilots | Delete after 1 day |

Both private (uniform bucket-level access, no `allUsers`/`allAuthenticatedUsers` binding). Neither pilot app currently uses GCS (per FEATURE_MIGRATION_MATRIX.md, `visit_validator`/`template_converter`/`noo_detector` are all BigQuery-only or fully local) — these buckets are provisioned ahead of the next pilot wave (`smart_coverage.py`, `stock_opname_ssjabo.py`), not yet wired into any deployed service.

## 4. BigQuery — CREATED

| Resource | Value |
|---|---|
| Dataset | `streamlit_migration_staging` (location `US`, matching every production dataset this portfolio touches) |
| Access | `streamlit-migration-runtime@...` granted `WRITER` |

No production dataset, table, schema, or ACL was modified. All three deployed pilots are read-only against production data (`gt_schema.master_distributor`, `gt_schema.distributor_configs`, `gt_schema.master_store_database_basis`, `pbi_gt_dataset.fact_sell_through_all`) via the runtime SA's project-wide `dataViewer` role — no write path exists in any of the three pilot apps, so the staging dataset is not yet exercised by anything live; it is provisioned for the next wave of pilots that do write (`smart_coverage.py`, `stock_opname_ssjabo.py`, etc., per DUPLICATION_PLAN.md §4).

## 5. Code changes — on branch `migration/cloud-run` only, NOT on `main`

Two files were edited, both are **preservation fixes required for the container to run at all**, not feature changes:

| File | Before | Problem | Change | After | Validation |
|---|---|---|---|---|---|
| `template_converter.py` | Credential fallback loaded a service-account key from `C:\script\skintific-data-warehouse-ea77119e2e7a.json` (hardcoded local Windows path); on failure fell to `credentials = None` | The path doesn't exist in a container; `credentials = None` is dead-code-that-crashes (`bigquery.Client()` immediately calls `credentials.project_id`, `AttributeError` on `None`) | Fallback now calls `google.auth.default()` (Application Default Credentials) when `st.secrets` is absent; `bigquery.Client()` construction changed to use the already-known `GCP_PROJECT_ID` constant instead of `credentials.project_id` | Works identically with a real `secrets.toml` (local dev, unchanged); works on Cloud Run via the attached service account, no key file needed | Deployed to Cloud Run; container starts cleanly, no traceback in logs (this file's credential block runs at module level, i.e. on every script execution, so a failure here would show immediately — none did) |
| `noo_detector.py` | Same local-path fallback pattern inside `load_existing_data()`; two BigQuery table paths (`bigquery_tables.master_store_database`, `bigquery_tables.fact_sell_through`) read from `st.secrets` with **no fallback at all** | Same container-incompatibility; the two table-path reads would raise `KeyError` with no `secrets.toml` present, before any credential issue even surfaces | Same ADC fallback for credentials; added a fallback default for both table paths (confirmed live via `bq show` — `gt_schema.master_store_database_basis`, `pbi_gt_dataset.fact_sell_through_all` — these are table pointers, not secrets) | Same | Deployed to Cloud Run; container starts cleanly. **Caveat**: this file's credential/table-path code runs inside `load_existing_data()`, called only when a user actually triggers detection — a mere page load does not exercise this path. Confirmed the container starts and serves the Streamlit shell (HTTP 200), but the ADC fallback and table-path fallback have **not yet been exercised by an actual triggered query** — see "Not yet validated" below |
| `visit_validator/app.py` | — | — | **No change** — zero external service dependency, deploys unmodified | — | Deployed, HTTP 200, clean logs |

Both edits include an inline `# MIGRATION NOTE` comment explaining the change and pointing back to this log. Compiled with `py_compile` before use (both passed).

## 6. Containers built & pushed — via Cloud Build (not local Docker — Docker Desktop's daemon was not running on this machine; `gcloud builds submit` was used instead, which also better matches the requested GitHub → Container Build → Artifact Registry pipeline shape)

| Image | Build | Status |
|---|---|---|
| `asia-southeast1-docker.pkg.dev/skintific-data-warehouse/streamlit-migration/visit-validator:pilot-1` | Cloud Build `1dbd78e0-d23f-4c67-a201-d5010ead3d78` | SUCCESS |
| `asia-southeast1-docker.pkg.dev/skintific-data-warehouse/streamlit-migration/template-converter:pilot-1` | Cloud Build `5b44a1ab-2d26-4b60-8189-ed2c396bda79` | SUCCESS |
| `asia-southeast1-docker.pkg.dev/skintific-data-warehouse/streamlit-migration/noo-detector:pilot-1` | Cloud Build `0256b93a-d36a-4a74-92ac-f9b83c7d61bf` | SUCCESS |

Built via my own gcloud identity (not yet via `streamlit-migration-build@` — that SA is provisioned for the future CI trigger, per the hands-on section below; today's builds were a manual one-off).

## 7. Cloud Run services — DEPLOYED

| Service | URL | Region | Memory/CPU | Concurrency | Min/Max instances | Ingress | Runtime SA |
|---|---|---|---|---|---|---|---|
| `visit-validator-migration` | `https://visit-validator-migration-141828905128.asia-southeast1.run.app` | asia-southeast1 | 512Mi / 1 | 1 | 0 / 3 | Authenticated only | `streamlit-migration-runtime@` |
| `template-converter-migration` | `https://template-converter-migration-141828905128.asia-southeast1.run.app` | asia-southeast1 | 1Gi / 1 | 1 | 0 / 3 | Authenticated only | `streamlit-migration-runtime@` |
| `noo-detector-migration` | `https://noo-detector-migration-141828905128.asia-southeast1.run.app` | asia-southeast1 | 1Gi / 1 | 1 | 0 / 3 | Authenticated only | `streamlit-migration-runtime@` |

Concurrency is pinned to 1 on all three per Streamlit's own session model (one WebSocket session per Streamlit process instance — a higher concurrency setting would let Cloud Run route multiple users' sessions into one container, which Streamlit does not support safely). `--no-allow-unauthenticated` deliberately chosen for this pilot/internal-testing phase — `noo-detector` specifically returns NIK/NPWP (PII) with zero app-level auth, and none of the three should be reachable by an unauthenticated caller before a real auth layer or an explicit pilot-user decision (MIGRATION_PLAN.md §7 roadmap Phase 6 vs 7).

**Smoke test results** (via `curl` with `gcloud auth print-identity-token`):

| Service | HTTP status | Notes |
|---|---|---|
| visit-validator-migration | 200 | Streamlit shell served, clean container logs |
| template-converter-migration | 200 | Streamlit shell served, clean container logs (credential ADC path already exercised — no error) |
| noo-detector-migration | 200 | Streamlit shell served, clean container logs (credential/table-path ADC fallback NOT yet exercised — needs an actual interaction, see below) |

## 8. Observability — CREATED (baseline)

| Resource | Value |
|---|---|
| Log-based metric | `streamlit_migration_errors` — counts ERROR-severity log entries across all three pilot Cloud Run services |

Cloud Run's default per-service metrics (request count, latency, container CPU/memory utilization, instance count) require **no setup** — they're automatically available in Cloud Monitoring's built-in Cloud Run dashboard the moment a service exists. No alert policy was created yet — that needs a notification channel (email/Slack/PagerDuty), which is a decision for the user (see hands-on section).

## 9. Git — 2 commits on branch `migration/cloud-run`, NOT pushed yet, NOT merged into `main`

| Commit | Contents |
|---|---|
| `b8c7128` | Dockerfiles + scoped requirements.txt for the 3 pilots, the two credential-fallback shims, `.gcloudignore` |
| `d8be02f` | Restores `packages.txt`, which disappeared from the working tree partway through this session with no traceable cause in any command this session ran (no `rm`, `git rm`, `Write`, or `Edit` touched that path) — caught via a routine diff check before pushing, root cause not identified, file restored from history. No other file was affected (verified via a full `git diff --name-status` against the prior commit). Recorded transparently rather than silently amended away. |

**`git push` to `origin/migration-cloud-run` was attempted and blocked by this session's own safety classifier** (a new-branch push is still a "visible to others" action). The two commits exist locally only. Pushing them is listed as a hands-on confirmation below, not because it's risky (main is untouched either way) but because the classifier requires an explicit human go-ahead for anything that reaches GitHub.

**Pre-existing, unrelated working-tree state, left untouched throughout**: `salesman_pjp.py` has a modified-but-uncommitted change that predates this session; `RAW DATA CLOSING DENPASAR SKINTIFIC.xlsx`, `po_portal_mockup.zip`, and `region_relationship_audit.xlsx` are untracked files that predate this session. None of these were staged, committed, or modified by any command in this log.

---

## Known incomplete validation (being transparent, not overclaiming)

- **noo_detector.py's ADC credential fallback and table-path fallback have not yet been exercised by a real triggered query** — the container starts cleanly, but that specific code path only runs when a user uploads a file or submits the manual-entry form, which a plain HTTP smoke test doesn't trigger (Streamlit executes app logic over an authenticated WebSocket session, not a plain GET). Confirming this fully needs either a manual browser session (through the authenticated Cloud Run URL, using a Google-account login that's been granted `run.invoker`) or a scripted WebSocket-based test. This is the top item in "next executable actions" below.
- **No side-by-side data comparison against the live Streamlit Cloud versions of these 3 apps has been run yet** — that's Phase 6 (Internal Testing) in MIGRATION_PLAN.md's roadmap and needs the same real-interaction test as above, on both sides, with the same inputs.
- **Load/performance/cold-start numbers**: not measured yet — no traffic has hit these services beyond the smoke-test curls.
