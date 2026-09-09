# Deployment Template — reusable pattern for every migration pilot

Companion to [EXECUTION_LOG.md](EXECUTION_LOG.md). Extracted after 4 validated pilots
(`visit_validator`, `template_converter`, `noo_detector`, `stock_opname_ssjabo`) so the
remaining ~18 apps don't each reinvent the deployment mechanism. Read this before
containerizing the next app.

```
Common infrastructure                App-specific configuration        Cloud Run service
deploy/_template/Dockerfile     +    deploy/<app>/requirements.txt  →  <app>-migration
deploy/cloudbuild-template.yaml      (+ a credential/config shim,
deploy/deploy_pilot.sh                 only if the app needs one —
                                        see "Credential shim recipe")
```

---

## 1. The three proven infrastructure patterns

| Pattern | Proven by | Shape |
|---|---|---|
| **Read-only** | `template_converter.py`, `noo_detector.py` | Cloud Run → BigQuery `dataViewer`/`jobUser` only. No write role needed at all. |
| **File-based, no external service** | `visit_validator/app.py` | Cloud Run with zero GCP client setup — pure upload/compute/download in memory. No service account permissions beyond the default needed. |
| **Read + Write (BigQuery and/or GCS)** | `stock_opname_ssjabo.py` | Cloud Run → BigQuery read (production tables, `dataViewer`) → BigQuery write (staging dataset only, until the real production target is confirmed) → optional GCS write (migration bucket, until confirmed). |

Classify every new app into one of these three **before** writing its Dockerfile — it determines whether it needs any credential shim at all (file-based apps need none).

## 2. Standard build + deploy (do this, don't hand-roll gcloud commands)

```bash
deploy/deploy_pilot.sh <app-slug> <entrypoint_file.py> deploy/<app>/requirements.txt [memory] [cpu]
```

This runs the shared `deploy/_template/Dockerfile` (parameterized via `--build-arg`, not
forked per app) through `deploy/cloudbuild-template.yaml`, then deploys with the fixed,
non-negotiable settings established across all 4 pilots:

- Region `asia-southeast1`, service account `streamlit-migration-runtime@`
- `--no-allow-unauthenticated` (internal-testing phase only — see the WebSocket/IAM-auth
  finding in MIGRATION_PLAN.md; this is not the final answer for real pilot users)
- `--concurrency=1` (Streamlit's session model — never raise this)
- `--min-instances=0 --max-instances=3` (tune per app only if load testing says otherwise)

**When to deviate from the shared Dockerfile**: an app with a local helper package/subfolder
(like `visit_validator/utils/`, `visit_validator/components/`) needs a small custom
Dockerfile with extra `COPY` lines — copy `deploy/_template/Dockerfile` as a starting point
for that one app rather than trying to force the single-file template to handle it.

## 3. Credential shim recipe (apply only when the app needs BigQuery/GCS and has no ADC path already)

Every app so far follows the same broken pattern: `st.secrets["connections"]["bigquery"]`,
falling back on failure to a **hardcoded local Windows file path**
(`C:\script\skintific-data-warehouse-ea77119e2e7a.json` or similar) that cannot exist in a
container. The fix is always the same shape:

```python
import google.auth
...
try:
    gcp_secrets = st.secrets["connections"]["bigquery"]
    # ... existing service_account.Credentials.from_service_account_info(...) ...
    # ... existing config var assignments from st.secrets ...
except Exception:
    credentials, _adc_project = google.auth.default()
    # ... hardcoded fallback config vars (see step 4) ...
```

And wherever the code does `bigquery.Client(credentials=credentials, project=credentials.project_id)`,
change it to `project=<the already-known project-id constant>` — `credentials.project_id`
doesn't exist on ADC credential objects and this line is dead-code-that-crashes today anyway
(the original `except: credentials = None` fallback would hit the same `AttributeError`).

**Before making this edit**: `grep -n "st.secrets\|service_account\|bigquery.Client\|storage.Client" <file>.py` to find every touch point — don't assume the pattern is identical to a previous app; `stock_opname_ssjabo.py`'s original fallback was `st.stop()`, not a crash, for example.

## 4. Config-value fallback rule — read vs. write, this matters

For every `st.secrets[...]` value that isn't credential material (table names, dataset
names, bucket names):

- **Read-only values**: safe to hardcode a real, confirmed value as the fallback (verify with
  `bq show`/`gsutil ls` first — never guess blind). Wrong guesses here just produce a
  "table not found" error, not data risk.
- **Write-target values** (anything that becomes the destination of `insert_rows_json`,
  `load_table_from_dataframe`, a GCS `blob.upload_from_*`, etc.): **never guess**. If this
  machine's local `.streamlit/secrets.toml` doesn't have the app's real config (check first —
  most apps won't), fall back to this migration's own isolated resources instead:
  - BigQuery write → `streamlit_migration_staging.<app>_pilot` (create the table first with a
    schema matching the app's insert payload — `bq mk --table`)
  - GCS write → `skintific-streamlit-migration-uploads`
  - This sometimes means splitting one shared `DATASET` variable into a separate
    `OUTPUT_DATASET` so reads and writes can point at different places without changing
    behavior when real secrets ARE present — see `stock_opname_ssjabo.py`'s diff for the
    exact pattern.

**Non-secret config is not off-limits to read locally.** If you need to confirm a real
table/bucket name and it's present in `.streamlit/secrets.toml` on this machine, it's fine to
grep the *specific non-credential keys* (e.g. `grep "output_table\|bucket_name" .streamlit/secrets.toml`) —
just never grep/read/print the `private_key` or other credential fields, and never paste
secret values into any document, commit, or chat output.

## 5. Validation standard — an app is not "migrated" until all 10 pass

HTTP 200 on a plain `curl` only proves the static shell loaded — it does **not** prove the
Python script ran, because Streamlit executes over a WebSocket session that a plain GET
never touches. Every pilot needs:

1. **Container** — build succeeds (`gcloud builds submit`)
2. **Deployment** — Cloud Run service starts (`gcloud run deploy` succeeds, revision serving)
3. **Dependencies** — no `ModuleNotFoundError` in Cloud Run logs
4. **Authentication** — the ADC fallback actually authenticates (see §6, how to actually trigger it)
5. **Functional** — the main user workflow completes, driven through a real session (see §6)
6. **Data** — BigQuery read results look right (spot-check against what the field means)
7. **Write** (if applicable) — independently re-query the write target after submitting; don't
   trust the app's own "success" message alone
8. **GCS** (if applicable) — confirm the object actually landed (`gsutil ls`/`stat`)
9. **Reliability** — reload the page at least once (this has caught a real, recurring
   transient issue — see §7); multiple sequential requests; a redeploy is itself a restart test
10. **Logs** — `gcloud run services logs read <service>` clean, no tracebacks

## 6. How to actually trigger a real session (not just curl)

Cloud Run's `--no-allow-unauthenticated` cannot be satisfied by a browser's native WebSocket
handshake (browsers can't attach an `Authorization` header to it — confirmed directly, see
MIGRATION_PLAN.md). For internal validation, use the authenticated local tunnel instead:

```bash
gcloud run services proxy <service> --region=asia-southeast1 --project=skintific-data-warehouse --port=<local-port>
```

then drive `http://127.0.0.1:<local-port>` with a headless Playwright session (already
available in this environment — `py -3 -c "import playwright"` confirms it). Reference
scripts from the first 4 pilots' validation runs are in this session's scratchpad; the
pattern that reliably works:

```python
page.goto(URL, wait_until="networkidle", timeout=60000)
page.wait_for_timeout(10000)
page.reload(wait_until="networkidle", timeout=60000)   # see step 7 - do this before interacting
page.wait_for_timeout(10000)
# then interact: page.mouse.click(x, y) on selectboxes (Streamlit's BaseWeb
# components resist standard CSS-text locators in headless Chromium through
# this proxy - coordinate clicks + page.keyboard.type()/press("Enter") is
# the pattern that actually worked across all 4 pilots)
```

**Stop the proxy when done** (`TaskStop` on its background task ID, or `Ctrl+C` if run in a
real terminal) — don't leave tunnels open across sessions.

## 7. Known transient quirk — always reload once before interacting

Every pilot so far shows the same symptom on first load through the local proxy:
`TypeError: Failed to fetch dynamically imported module: .../static/js/<Component>.<hash>.js`
for whichever widget (Selectbox, Radio, Metric, etc.) renders first. **A single page reload
always clears it.** This is specific to the local `gcloud run services proxy` tunnel's
static-asset handling, not a real production issue (nothing suggests this would happen for a
real user hitting the actual Cloud Run URL directly) — but budget for it in every validation
script: `goto` → wait → `reload` → wait → *then* interact.

## 8. Security non-negotiables (apply to every future pilot without exception)

- No service-account JSON files added to the repo, ever
- No credentials embedded in source code, ever
- No personal credentials used as a Cloud Run service's runtime identity
- `--no-allow-unauthenticated` by default; only reconsider per-app once a real pilot-user
  access decision has been made (IAP vs. app-level login — see MIGRATION_PLAN.md)
- Any app returning PII (NIK/NPWP, personal names, addresses) gets flagged in its scorecard
  row regardless of how it's deployed — `noo_detector.py` is the current example
