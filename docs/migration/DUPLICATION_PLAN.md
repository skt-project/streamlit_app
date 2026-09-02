# Duplication Plan — Safe Parallel Environment

Companion to [MIGRATION_PLAN.md](MIGRATION_PLAN.md). Answers: **can this portfolio be duplicated without touching production, and exactly how?** Yes — nothing below requires disabling, redirecting, or modifying any of the 22 live Streamlit Community Cloud deployments, any production BigQuery table, or any production GCS object.

---

## 1. Code duplication

```
D:\GitHub\streamlit_app          (existing repo — UNTOUCHED)
        │
        │  git clone / git branch (choose one — see below)
        ▼
migration/ (new branch) or a separate fork under skt-project org
        │
        ├── Dockerfiles + per-app requirements.txt (new, additive)
        ├── docs/migration/ (this document set — already additive, already done)
        └── (later) fastapi_backend/, frontend/ — new directories, new code
```

- **Recommended**: a new long-lived branch (`migration/cloud-run`) in the same repo, not a separate fork — keeps history/blame intact and makes it trivial to cherry-pick a bug fix discovered during migration back onto whatever apps stay on Streamlit Cloud in the interim. A separate fork is only worth it if the team wants a hard organizational boundary (different reviewers, different CI) — not needed here.
- **Nothing on this branch is auto-deployed anywhere.** Streamlit Community Cloud's existing deployments are configured (in Streamlit Cloud's own dashboard, not in this repo) to track specific files on `main`/`master` — a new branch is invisible to them until someone explicitly repoints a deployment at it, which this plan never does before Phase 9.

## 2. Configuration duplication

Every credential currently lives in `.streamlit/secrets.toml` (gitignored) or hardcoded fallback paths. For the duplicate environment:

| Item | Production | Duplicate/staging |
|---|---|---|
| BigQuery service account | Whatever key is currently in each app's `secrets.toml` (full read/write scope, per app, undocumented exact permission set) | **New**, purpose-built service account(s) — see §3 |
| GCS bucket | Various existing buckets (ad hoc per app) | **New** `*-staging` buckets, separate from anything production touches |
| Google Sheets (2 apps: `noo_sku_mapping.py`, `po_portal_suggestion_dev.py`) | Live tracker Sheet, live production tabs | Either a **copied Sheet** (recommended — same structure, fake/sample rows) or continued read-only access to the real Sheet with writes hard-disabled (`write_enabled=false`, already the default for `noo_sku_mapping.py` today) |
| Streamlit secrets / env vars | `.streamlit/secrets.toml` per app | Same file format for local dev; **Secret Manager** for anything deployed to Cloud Run (see MIGRATION_PLAN.md §11 — do this now, don't defer it to "later cleanup") |

## 3. Service accounts for the duplicate environment

Provision two new, narrowly-scoped service accounts before any duplicate deployment goes live, following the least-privilege shape already used correctly elsewhere in this project (`readonly@`, `sfa-web-api@`: `bigquery.dataViewer` + `bigquery.jobUser`, nothing more):

1. **`streamlit-migration-readonly@skintific-data-warehouse.iam.gserviceaccount.com`** — `roles/bigquery.dataViewer` + `roles/bigquery.jobUser` on the production project, **no write role anywhere**. Use this for every read-only query path during Phases 1-6 (which is most of the portfolio's queries — the audit found writes are concentrated in a known, small set of insert/update paths per app, listed in FEATURE_MIGRATION_MATRIX.md).
2. **`streamlit-migration-writer@skintific-data-warehouse.iam.gserviceaccount.com`** — scoped `roles/bigquery.dataEditor` **on specific staging/test datasets only** (see §4), never on the production datasets the live apps write to (`gt_schema`, `rsa`, `dms`, `pbi_gt_dataset`, etc.). This account should not exist in a form capable of writing to any table the live Streamlit apps write to until a specific, reviewed cutover step explicitly grants it (Phase 9, per-app, not portfolio-wide).

Both accounts are new IAM principals — creating them does not touch any existing service account's permissions, and both can be deleted cleanly if the migration is abandoned.

## 4. Data safety — read vs. write duplication strategy

Every BigQuery/GCS/Sheets operation across all 22 apps was classified in the per-group audits (full detail in FEATURE_MIGRATION_MATRIX.md). Summary strategy:

| Operation class | Example apps | Duplication strategy |
|---|---|---|
| **READ-ONLY** (the majority of operations across the portfolio) | Almost every query in `po_buffer.py`, `po_simulator*.py`, `noo_detector.py`, `visit_validator/`, dashboard/lookup queries everywhere | Point the duplicate straight at **production tables** using `streamlit-migration-readonly@` — this is safe by construction (the account cannot write) and gives the most realistic test data with zero duplication effort |
| **WRITE, append-only, low blast radius** | `smart_coverage.py`, `stock_opname_ssjabo.py`, `skt_top_20_store_list_stock.py` inserts | Point at a **dedicated staging dataset** (`gt_schema_staging` or similar, new, empty) with the same schema as production, created via `bq cp --schema_only`. Never point these at production during testing. |
| **WRITE, no dedup guard (highest pollution risk)** | `skt_area_execution_capability_v2.py`'s `insert_allocation_rows()`, `sfa_attendance.py`'s checkout insert | Staging dataset only, and **add the missing dedup guard as part of the port**, not after — testing against a real dedup-free write path is how you'd actually create the exact production incident this plan is trying to avoid |
| **WRITE, non-atomic delete-then-insert** | `salesman_pjp.py`'s PJP replace | Staging dataset only, until the atomic `MERGE` rewrite (MIGRATION_PLAN.md §5) is in place — never exercise the current delete-then-insert code path against anything that matters |
| **DESTRUCTIVE (WRITE_TRUNCATE)** | `po_portal_suggestion_dev.py`'s refresh buttons | Staging dataset only, always — this pattern is destructive even in today's production usage (it already shares tables with a scheduled DAG per prior incident history) and must never run against `po_portal_suggestion_matrix`/`_matrix_schema`/`po_portal_suggestion_dev` from a migration-test context |
| **Live stored procedure trigger** | `po_buffer.py`'s auto-triggered `CALL rsa.inventory_buffer_sp()` | Stub or disable the auto-trigger entirely in the duplicate (call it manually/on a schedule instead) — do not let a migration-test page load accidentally fire a production stored procedure |
| **GCS uploads currently made public** | `stock_opname_ssjabo.py`, `skt_top_20_store_list_stock.py` | Duplicate uses a private staging bucket; this is also the point at which the `make_public()` call gets removed for good, not carried into the new code |

## 5. Isolated test data for destructive operations

For every app in the "WRITE" rows above, before any duplicate deployment is exercised:

1. `bq mk --dataset` a `*_staging` counterpart for each production dataset the app writes to (e.g. `gt_schema_staging`, `rsa_staging`).
2. `bq cp --schema_only <prod_table> <staging_table>` to replicate structure without data, or copy a small, already-anonymized sample if realistic test volume is needed for a specific check (e.g. testing `store_channelization.py`'s duplicate-detection logic needs *some* existing rows to detect duplicates against).
3. Point the duplicate app's write path at the staging dataset via an environment variable / config flag — **the same code should never hardcode which dataset it writes to**; this is also good practice for the eventual Cloud Run deploy (dev/staging/prod distinguished by config, not by code branch).

## 6. Separate deployment for the duplicate

Per MIGRATION_PLAN.md Phase 2: stand up 2-3 pilot apps as Cloud Run services with a `-staging` suffix (e.g. `visit-validator-staging`), in the same GCP project (no need for a separate project — IAM scoping via the dedicated service accounts above is sufficient isolation), with:

- Their own Artifact Registry image tags (`:staging`, never overwriting `:prod` once that exists).
- No custom domain yet — use the auto-generated `*.run.app` URL for internal testing.
- `min-instances=0` (fine for staging — cold starts are an acceptable tradeoff pre-launch, unlike production).

This can be built, tested, and iterated on indefinitely without any production Streamlit Cloud app, BigQuery table, or GCS object ever being touched.

## 7. When production data safely becomes reachable from the duplicate

Only two things change between "isolated test environment" and "internal testing with real data confidence" (Phase 5-6 in the roadmap), and neither requires touching production write paths:

1. Swap `streamlit-migration-readonly@` in for read paths once confident in query correctness (already safe from day one, per §4).
2. Compare the duplicate's read-only output **row-for-row** against the live Streamlit app's output for the same filters/inputs — this is the actual verification step, and it requires zero writes.

Write-path confidence only needs to be built against the staging dataset (§4-5); production write access for the new Cloud Run/API code should not be granted until the specific per-app cutover step in Phase 9, and even then, ideally after a peer review of that app's write logic against the checklist in MIGRATION_PLAN.md §4 (idempotency key, atomic replace, parameterized query).
