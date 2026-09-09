# Checkpoint 1 — after Pilots 1–6

6 apps deployed and validated on Cloud Run: `visit_validator`, `template_converter`,
`noo_detector`, `stock_opname_ssjabo`, `skt_top_20_store_list_stock`,
`store_channelization`. This is a pause point before Pilot 7, per the migration's own
incremental-evidence approach.

## What patterns are now proven

- **Read-only BigQuery via ADC**: 6/6 pilots. Every app's queries return real production
  data through `streamlit-migration-runtime@`'s Application Default Credentials — no key
  file, no secrets.toml, no code path other than the one small fallback shim.
- **BigQuery writes via two different APIs**: `insert_rows_json` (streaming — Pilots 4, 5)
  and `load_table_from_dataframe` (batch load job — Pilot 6), both proven working under the
  same ADC identity, both independently verified via direct `bq query` against the actual
  staging table, not just trusting the app's own success message.
- **GCS-free apps deploy with zero extra IAM**: `visit_validator` needed no service-account
  permissions beyond what Cloud Run gives by default.
- **The reusable template works unmodified across different app shapes**: Pilots 5 and 6
  both built and deployed through `deploy_pilot.sh` without touching
  `deploy/_template/Dockerfile` or `deploy/cloudbuild-template.yaml` — only a per-app
  `requirements.txt` and (where needed) a credential shim were added.
- **Cascading multi-level selects work fine under Cloud Run**, including a 4-level chain
  (Region → SPV → Distributor → Store, Pilot 5) and a 2-level one with a cross-check against
  a second BigQuery table (Pilot 6's duplicate-detection against a staging table).
- **Write-safety pattern holds across every write-capable pilot**: none of the 3 write
  pilots' fallback config points at a real, unconfirmed production table — all three route to
  `streamlit_migration_staging.*`, created ahead of time with a schema matching the app's own
  insert payload.

## What assumptions were invalidated

- **"HTTP 200 means it works" — false, confirmed repeatedly.** Every pilot's static shell
  loads fine before any Python code has actually run a query; the real proof only comes from
  driving an actual session (WebSocket) and checking the rendered data or an independent `bq
  query`. This was true for the very first pilot and remained true through all 6.
- **"The CLI proxy is a faithful stand-in for a real browser" — mostly true, one exception.**
  `gcloud run services proxy` + Playwright reliably exercised page loads, cascading selects,
  and BigQuery reads/writes triggered by button clicks — but could **not** carry a real file
  upload through to the app (Pilot 6), even after the underlying Cloud Run session-affinity
  issue was fixed. Treat proxy-based testing as reliable for everything except
  `st.file_uploader` flows.
- **"Coordinate-based clicking is good enough" — false past 2 cascading levels.** It worked
  for the first 4 pilots' simpler layouts but broke as soon as a selection shifted the page
  height unpredictably (Pilot 5). Keyboard Tab+type+Enter navigation is layout-independent
  and is now the standard approach (documented in DEPLOYMENT_TEMPLATE.md §6).
- **"Concurrency=1 is the only Cloud Run setting Streamlit needs" — incomplete.**
  `--session-affinity` is also required for any app with `st.file_uploader`, discovered only
  when Pilot 6 actually exercised that code path. Now applied to every pilot by default via
  `deploy_pilot.sh`, whether or not the app uploads files.

## Common infrastructure components (now shared, not duplicated)

`deploy/_template/Dockerfile`, `deploy/cloudbuild-template.yaml`, `deploy/deploy_pilot.sh`,
one Artifact Registry repo, one runtime service account, one staging BigQuery dataset. Zero
new infrastructure was created for Pilots 5 and 6 beyond their own staging tables and
`deploy/<app>/requirements.txt`.

## Common application issue found in every single pilot so far (6/6)

Every app's original BigQuery credential fallback pointed at a hardcoded **local Windows
file path** (`C:\script\...`, `C:\Users\Bella Chelsea\...`, `D:\script\...` — three different
paths across six files, all equally unusable in a container). This is clearly a
repo-wide pattern from local development, not a one-off. Expect the same fix to be needed
in most of the remaining ~16 apps — the shim recipe in DEPLOYMENT_TEMPLATE.md §3 should
apply directly.

## Cloud Run configuration lessons

- `--concurrency=1` is correct and non-negotiable (Streamlit's session model).
- `--session-affinity` should be applied to every pilot by default now, not just ones with
  visible file uploaders (cheap, and an app's upload surface can grow later without a
  redeploy-config reminder).
- `--no-allow-unauthenticated` is right for this phase but is a dead end for real pilot
  users until an IAP or app-level-auth decision is made (see MIGRATION_PLAN.md's
  WebSocket/IAM-auth finding) — this hasn't blocked anything yet because internal validation
  uses the CLI proxy, but it will block Phase 7.

## BigQuery lessons

- Table names are often **not derivable from the app's own column names alone** — Pilot 5's
  `STORE_TABLE` needed an `INFORMATION_SCHEMA.COLUMNS` search across the whole dataset to
  find (`skt_top_20_store_list` has no obvious naming link to the app's filename-adjacent
  guesses). Budget for this step on every future write-adjacent app.
- Two different write APIs (`insert_rows_json` vs `load_table_from_dataframe`) both work
  identically under ADC — no reason to expect a third, undiscovered API to behave
  differently, but don't assume without checking which one a given app actually uses.
- When the UI can't exercise a write path (Pilot 6), **impersonating the runtime service
  account from a throwaway local script** is a fully valid, fast substitute — it tests the
  exact credential and exact BigQuery call the deployed app makes, just without going through
  Streamlit's UI at all.

## GCS lessons

None of the 6 pilots have had their GCS upload path exercised yet end-to-end (Pilot 4/5's
optional upload fields were left empty during testing; Pilot 6's couldn't be reached due to
the file-upload proxy limitation). This is a genuine open item, not a proven pattern —
flagged explicitly rather than assumed to work by extension from the BigQuery write proof.

## Security lessons

- The plaintext-local-credential-path pattern (see "common application issue" above) is
  repo-wide, not isolated — worth flagging to whoever manages this repo's local dev
  conventions independent of the migration.
- `public_skintific_storage` (a real, already-public production bucket) very nearly became
  a write target for Pilot 7's fallback — caught and redirected to the migration's own
  bucket before any deploy. Always check whether a fallback bucket name is one that's
  actually public before treating it as a safe default.

## Performance observations

Not yet measured — no load testing has been performed, and there's been no real traffic
beyond validation sessions. This remains an open item for a later phase, not something this
checkpoint can speak to honestly.

## Remaining architectural risks

1. **GCS upload path is unproven** (see above) — should be closed out on the next pilot that
   has one and can actually be driven through the proxy, or via the impersonation pattern if
   not.
2. **Google Sheets integrations are a new dependency class** (Pilot 7) — proven read patterns
   don't cover this; Sheets access is governed by its own sharing model, not GCP IAM, and is
   the first genuine hands-on blocker in the whole migration so far.
3. **The WebSocket/Cloud-Run-IAM-auth incompatibility is still unresolved** for any real
   pilot user — internal testing has a working substitute (the CLI proxy), pilot users will
   not.
4. **`sfa_attendance.py` and `time_study_stopwatch.py` remain correctly un-started** — nothing
   in Pilots 1–6 changes the assessment that they need a redesign, not a lift-and-shift.

## Verdict

No serious architectural problem has been discovered. Proceeding to Pilot 7 (pending the
Google Sheet access hands-on step below).
