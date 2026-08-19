# NOO & SKU Mapping — UAT Runbook

**Status:** READY FOR CONTROLLED UAT. **No production write has ever been executed.**
**Date:** 2026-08-19
**Script:** `scripts/run_noo_sku_uat.py`
**Companions:** [audit](streamlit_noo_sku_mapping_audit.md) · [implementation](streamlit_noo_sku_mapping_implementation.md) · [design](streamlit_noo_sku_mapping_design.md)

---

## 1. Prerequisites

- Python 3.11+ with `requirements.txt` installed.
- A Google service account with **Editor** on the tracker spreadsheet and read
  access to BigQuery `gt_schema` / `dms`.
- Both pool worksheets present with their current headers (the script verifies
  this and refuses on any drift).

## 2. Environment setup

```bash
cd /path/to/streamlit_app
pip install -r requirements.txt
```

## 3. Required credentials

Never place a key in source. The script resolves credentials through
`noo_sku.sources.load_credentials`, which reads `st.secrets` first and falls back
to a key file path in the environment.

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/secure/path/service-account.json
```

Required scopes (already requested by the code): `spreadsheets`,
`drive.readonly`, `bigquery`. Omitting the BigQuery scope authenticates fine and
then fails at query time with `ACCESS_TOKEN_SCOPE_INSUFFICIENT`.

## 4. Required Google Sheet access

| Spreadsheet | Access needed |
|---|---|
| `1bchAAMuXOT1lzuAB-KbrrAwpIrL1_MG3Hzcq823PAN4` (NOO TRACKER GT) | **Editor** — appends to the two pools; reads DIST DATABASE, SKU MAPPING and the three brand trackers |
| `1UObRQCPBB3grWvGcbe3S9F-gW8LWS_Pk` (SKU template) | Viewer |
| `1Yt6vRRVSz2-mm59KzVsq32MrwqmzDoYB` (NOO template) | Viewer |

## 5. Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | — | service-account key path (fallback when `st.secrets` is absent) |
| `APP_MODE` | `dry-run` | `dry-run` \| `pilot` \| `production`; `--mode` overrides |
| `WRITE_ENABLED` | `false` | must be `true` before any write can occur |
| `PILOT_MAX_ROWS` | `3` | ceiling for PILOT mode |
| `APP_ENV` | `dev` | environment label shown in the UI |
| `TRACKER_SPREADSHEET_ID` | the ID above | override only if pointing at a copy |

Writing requires **both** a non-dry-run mode **and** `WRITE_ENABLED=true`. Either
one alone leaves the run as a dry run.

## 6. Dry-run command

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/secure/path/service-account.json
python scripts/run_noo_sku_uat.py \
    --mode dry-run \
    --file uat_noo_sample.xlsx \
    --distributor DST082
```

Reads, validates, enriches, classifies duplicates, prints the preview and the
per-row mapping source, writes a report — and **writes nothing**.

## 7. Pilot command

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/secure/path/service-account.json
export WRITE_ENABLED=true
export PILOT_MAX_ROWS=3

python scripts/run_noo_sku_uat.py \
    --mode pilot \
    --file uat_noo_sample.xlsx \
    --distributor DST082
```

Prompts `Continue with write? [YES/NO]:`, writes at most 3 rows, then reads them
back and verifies. Add `--yes` only for scripted runs.

## 8. Production command

```bash
export WRITE_ENABLED=true
python scripts/run_noo_sku_uat.py \
    --mode production \
    --file noo_batch.xlsx \
    --distributor DST082 \
    --confirm-production
```

`--confirm-production` is mandatory; without it the script exits 2 having done
nothing.

## 9. Expected output

```
============================================================
NOO/SKU MAPPING UAT
============================================================

Mode:            PILOT
Distributor:     DST082
Distributor Name: CV CECE
Upload Type:     NOO
Input File:      uat_noo_sample.xlsx
Rows in File:    3
Valid Rows:      3
Invalid Rows:    0
Duplicates:      0
New Rows:        3
Rows To Write:   3
Fallback Mappings: 0
Target Spreadsheet: 1bchAAMuXOT1lzuAB-KbrrAwpIrL1_MG3Hzcq823PAN4
Target Tab:      POOL NOO STREAMLIT

============================================================
MAPPING SOURCE PER ROW
============================================================
  Baris 4 [SKINTIFIC] dist=MASTER_DISTRIBUTOR fallback=- store=MASTER_STORE_BASIS on=cust_id SE='...'

Continue with write? [YES/NO]: YES

============================================================
WRITE VERIFICATION
============================================================
Rows Written:  3
Rows Verified: 3
Verification:  PASS
```

### Exit codes

| Code | Meaning |
|---|---|
| 0 | success (dry run OK, or written **and** verified) |
| 1 | validation / duplicate check failed — nothing written |
| 2 | configuration, credential or spreadsheet-access problem |
| 3 | aborted at the prompt |
| 4 | write failed |
| 5 | **written but verification FAILED — inspect immediately** |

## 10. Verification procedure

The script verifies automatically after a write, matching on `input_time` +
`customer_branch_code`. Confirm by hand as well:

1. Open the tracker → `POOL NOO STREAMLIT` (or `POOL SKU STREAMLIT`).
2. Sort/filter by `input_time` — the batch's exact value is printed under
   **BATCH REFERENCE** and stored in the report.
3. Confirm the row count matches **Rows Written**.
4. Spot-check column alignment: `customer_branch_code` must be your distributor,
   `branch_name` the distributor name, `input_time` today in WIB.
5. Confirm the enrichment columns (`se_kae`, `spv`, `aom`, `area`, `province`)
   are either populated or intentionally blank for brand-new stores.
6. Confirm **no other tab changed**.

## 11. Rollback procedure

The pool has **no `upload_id` column** and adding one was explicitly out of scope
(decision B1). The batch key is therefore:

> **`input_time` + `customer_branch_code`**

`input_time` is taken once per upload, so every row of one batch carries the
identical timestamp to the second. Collision with another batch would require the
same distributor uploading twice in the same second.

**To identify pilot rows**

1. Open the target pool tab.
2. Filter `customer_branch_code` = the pilot distributor.
3. Filter `input_time` = the value printed under **BATCH REFERENCE** (also in the
   report as `input_time`, and `batch_reference` carries the run's short id).

**To remove them if UAT fails**

1. Verify the filter matches exactly the expected row count.
2. Select those rows and delete them.
3. Re-run verification to confirm the pool is back to its prior row count.

Because every write is an append and nothing is ever updated in place, deleting
the batch restores the previous state exactly. **Never** clear the sheet or
delete the header row.

## 12. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `REFUSED: --mode production requires --confirm-production` | safety guard | add the flag, or use `--mode pilot` |
| `WARNING: mode is 'pilot' but WRITE_ENABLED is not true` | write gate closed | `export WRITE_ENABLED=true` |
| `Struktur kolom '...' berbeda dari yang diharapkan` | pool header changed | stop; re-run the audit and update `config.POOL_*_HEADERS` |
| `ACCESS_TOKEN_SCOPE_INSUFFICIENT` | BigQuery scope missing | re-issue credentials with the BigQuery scope |
| `Distributor ... tidak ditemukan` | code absent from `DIST DATABASE` | check the code, or ask BD Support to add it |
| `mode PILOT hanya mengizinkan N baris` | more eligible rows than the ceiling | shrink the file, or raise `PILOT_MAX_ROWS` deliberately |
| `Template tidak dikenali` | wrong or edited template | re-download from the app |
| Verification FAILED | append succeeded but rows not found | inspect the pool immediately; use the rollback filter |

## 13. Known limitations

- **No `upload_id` / `row_type` column** in the pools — batch identification uses
  `input_time` + `customer_branch_code` (approved, decision B1).
- **Store-level enrichment is unavailable for genuinely new stores.** Expected
  and non-blocking (decision B2); BD Support completes those cells.
- **FACERINNA distributor coverage is thin** (`asm_fr` ~52%, `spv_fr` ~45%). The
  app falls back to the brand-neutral column and labels it
  `BRAND-NEUTRAL FALLBACK` in the preview and report.
- **303 `reference_id` values map to more than one store.** Those rows are left
  unenriched and flagged `AMBIGU — PERLU DITINJAU`, never auto-resolved.
- **`DST325` suffix is contested** — see §16.
- The pools accept appends only; the app can never repair a bad row, only add.

---

## 14. Duplicate detection and the hash

Identity — NOO: `customer_branch_code` + `customer_store_code`.
SKU: `customer_branch_code` + `product_code` + `customer_product_code`.

**Fields IN the content hash** (business data the admin controls):

| NOO | SKU |
|---|---|
| `store_id`, `store_name`, `channel_name`, `customer_code`, `customer_branch_code`, `customer_store_code`, `customer_store_name`, `city`, `store_address`, `store_type` | `customer_code`, `product_code`, `customer_branch_code`, `product_name`, `customer_product_code`, `customer_product_name`, `specification` |

**Fields excluded, and why:**

| Field | Business meaning | Why excluded |
|---|---|---|
| `input_time` | when the upload happened | Upload-event data, not business data. Including it would make every re-upload look new — the exact failure the brief warns about. |
| `se_kae`, `spv`, `aom`, `asm_name`, `asm_kam`, `asm` | people assigned to the store/distributor | Not user input. A pure function of (identity, master state at lookup time). A staff reassignment would otherwise re-hash a distributor's whole history into false CORRECTIONs. |
| `area`, `province`, `region` | geography from master | Same reasoning — derived, not submitted. |
| `branch_name`, `customer_name` | the distributor's own name | Constant for every row of a given distributor, and the hash is already scoped by distributor, so they add **zero** discriminating power — while a rename in master would re-hash everything. |
| `longitude`, `latitude`, ratings, `tl`, `pm`, `md/smd`, `ba1`–`ba4`, `group_*`, `nik`, `npwp`, `remark`, `barcode`, `description` | unused tracker columns | Measured at ≤0.1% fill across 3,859 real rows; permanently blank. Including them would silently invalidate every stored hash if they were ever populated. |

**Worked examples**

- *Example A* — same business data, uploaded at T1 and T2 → **same hash** →
  `EXACT_DUPLICATE`, skipped. ✅
- *Example B* — same identity, `store_name` ABC → XYZ → **different hash** →
  `CORRECTION`, inserted as a new row. ✅ `store_name` is meaningful business
  content and stays in the hash.
- *Example C* — same submission, but the store's SE changed in the master between
  uploads → **same hash** → `EXACT_DUPLICATE`. Correct: the admin submitted
  identical business data; only the master's rendering moved.

## 15. Branch Name / Customer Branch Code

Three categories:

| Category | Fields | Behaviour |
|---|---|---|
| **USER-PROVIDED** | `store_name`, `channel_name`, `customer_store_code`, `city`, `store_address`, `store_type`, `customer_code`, `store_id`, all SKU DB fields | Taken from the file, validated, written as submitted. `store_type` and `city` keep the admin's vocabulary exactly (decision B3). |
| **VALIDATED** | `Branch Name`, `Customer Branch Code` | Still in the template. Read from the file and **compared** against the authenticated distributor. A mismatch is a **hard validation error** — never silently accepted. |
| **SYSTEM-AUTHORITATIVE** | the value actually written for those two, plus `input_time`, `region`, ASM/SPV/SE/AOM, `customer_code` (SKU) | Always sourced from session/master. The uploaded file can never override the authenticated identity. |

> **Guideline update needed.** BD Support's NOO template guideline still tells
> admins to type Branch Name (item 6) and Customer Branch Code (item 7). Those
> values are now validated and system-controlled. The guideline should say they
> must match the login account and are filled by the system.

## 16. DST325 — evidence and decision

| Source | Value | Recency | Volume |
|---|---|---|---|
| `DIST DATABASE` col AO | **SJMA** | current reference | authoritative sheet |
| `SKINTIFIC NEW` | **SJMA** | last 31-Jul-2026 | 129 rows |
| `TIMEPHORIA NEW` | **SJMA** | last 31-Jul-2026 | 127 rows |
| `FACERINNA NEW` | **SJMA** | last 31-Jul-2026 | 127 rows |
| `SKU MAPPING` | **SJM** | last 12-Aug-2026 | 19 rows |
| BigQuery `dms.gt_po_tracking_all_mv` | **SJM** | last 12-Aug-2026 | 2,020 orders |

Distributor: `PT SUKSES JAYA MAKMUR ABADI - BANDA ACEH`, Active.

**This is not a stale value — it is a domain split.** The NOO side (383 tracker
rows plus BD Support's own reference) consistently uses `SJMA`. The
SKU/transactional side (SKU mapping plus 2,020 DMS purchase orders) consistently
uses `SJM`. Both are current: the newest evidence on each side is within two
weeks of the other.

**Current behaviour:** `DIST DATABASE` has top priority, so the app resolves
`SJMA` for **both** pools, and `SuffixResolution.conflict` flags the disagreement
so it is visible rather than silent.

**Impact**

- *SJMA everywhere (current):* NOO pool matches its 383 precedents ✅; SKU pool
  writes `11SJMA` where the main SKU tracker and DMS use `11SJM` — a downstream
  join on `customer_code` would miss for this distributor ⚠️.
- *SJM everywhere:* SKU side consistent ✅; NOO pool contradicts 383 existing rows ⚠️.
- *Domain-specific (SJMA for NOO, SJM for SKU):* both sides internally consistent,
  but it means the "distributor abbreviation" is not a single value, which no
  current documentation states.

**[NEEDS CONFIRMATION]** — BD Support must state whether `DST325` has one
abbreviation or two by domain. Until then the app keeps `SJMA` and flags the
conflict. **Do not run a SKU pilot for DST325**; use a distributor with no
conflict (e.g. `DST082`). `DST121` has a similar, smaller conflict
(`DIST DATABASE`/PO = `KIS`, SKU history = `SDS`).

## 17. FACERINNA fallback visibility

Coverage in `master_distributor` for FACERINNA is thin: `asm_fr` ~52%,
`spv_fr` ~45%. When the per-brand column is empty the app uses the brand-neutral
`asm`/`aom` column — and says so.

Every row in the preview and the report carries:

| Field | Example |
|---|---|
| Distributor | `CV CECE` |
| Brand | `FACERINNA` |
| ASM / SPV | resolved values |
| Mapping Source (Distributor) | `MASTER_DISTRIBUTOR` or **`BRAND-NEUTRAL FALLBACK`** |
| Mapping Source (Store) | `MASTER_STORE_BASIS` or `NOT AVAILABLE` |
| Matched On | `cust_id` / `reference_id_skt` / `-` |
| Fallback | `-`, `YA`, or `AMBIGU — PERLU DITINJAU` |

Plus counters: `Fallback Mappings` and `Ambiguous Mappings`. A fallback is never
silent.

## 18. Production safety controls

1. Default mode is `dry-run`; `WRITE_ENABLED` defaults to false.
2. Writing needs a non-dry-run mode **and** `WRITE_ENABLED=true`.
3. `--mode production` additionally needs `--confirm-production`.
4. PILOT refuses — rather than truncates — when more rows than the ceiling qualify.
5. `assert_layout()` re-reads the live header before every write, in every mode.
6. Append is the only write verb in the codebase; no update, clear, or delete.
7. `valueInputOption="RAW"` blocks formula injection and numeric coercion.
8. Interactive `YES/NO` confirmation before any write.
9. Post-write read-back verification with a distinct exit code on failure.
10. Reports contain no credentials.

---

## 19. UAT procedure

| Step | Action |
|---|---|
| 1 | Pick a pilot distributor with **no** suffix conflict — `DST082` is verified clean. Avoid `DST325` and `DST121`. |
| 2 | Build a 2–3 row file from the app's own template. Use store codes prefixed with the distributor code. |
| 3 | Run **dry-run**. |
| 4 | Review: validation, mapping source, fallback flags, duplicate counts, target tab, preview rows. |
| 5 | Set `WRITE_ENABLED=true` and run **pilot**. |
| 6 | Answer `YES` at the prompt; 2–3 rows are written. |
| 7 | Verify in the spreadsheet using §10. |
| 8 | Compare expected vs actual column by column. |
| 9 | Keep the JSON report from `reports/`. |
| 10 | Only after a clean verification consider wider rollout. |

Re-running the same file after a successful pilot should classify every row as
`EXACT_DUPLICATE` and write nothing — a useful end-to-end confirmation that
duplicate detection works against real pool data.
