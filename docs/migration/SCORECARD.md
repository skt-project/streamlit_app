# Migration Scorecard

Updated 2026-09-09. All work on branch `migration/cloud-run`, pushed. Production Streamlit Community Cloud deployments untouched throughout.

| App | Type | Container | Cloud Run | BQ Read | BQ Write | GCS | Functional | Data Validation | Status |
|---|---|---|---|---|---|---|---|---|---|
| visit_validator | File-based | ✅ | ✅ | N/A | N/A | N/A | ✅ | N/A | **VALIDATED** |
| template_converter | Read | ✅ | ✅ | ✅ | N/A | N/A | ✅ | ✅ | **VALIDATED** |
| noo_detector | Read | ✅ | ✅ | ✅ | N/A | N/A | ✅ | ✅ | **VALIDATED** |
| stock_opname_ssjabo | Read+Write | ✅ | ✅ | ✅ | ✅ (staging) | ✅ (staging) | ✅ | ✅ independently verified | **VALIDATED** |
| skt_top_20_store_list_stock | Read+Write | ✅ | ✅ | ✅ | ✅ (staging) | N/A | ✅ | ✅ independently verified | **VALIDATED** |
| store_channelization | Read+Write | ✅ | ✅ | ✅ | ✅ (staging, via impersonation) | N/A | ✅ (read/export) | ✅ independently verified | **VALIDATED** (upload UI untestable via proxy — tooling limit, not a defect) |
| po_buffer.py | Read+DDL-guarded | ✅ | ✅ | ✅ | N/A (SP auto-exec disabled) | N/A | ✅ | ✅ (362 stores, real scale) | **VALIDATED** — 2 real pre-existing bugs found+fixed |
| whitespace_map_lightweight | Read (GCS-only) | ✅ | ✅ | N/A | N/A | ✅ | ✅ | ✅ | **VALIDATED** |
| po_simulator | Read | ✅ | ✅ | ✅ | N/A | N/A | ✅ | — | **VALIDATED** |
| whitespace_map | Read | ✅ | ✅ | ✅ | N/A | ✅ | ✅ | ✅ | **VALIDATED** — updates "Hard" rating (works fine at 2Gi) |
| skt_area_execution_capability_v2 | Read+Write, multi-role | ✅ | ✅ | ✅ | ✅ (staging) | N/A | ✅ (login+role routing) | ✅ | **VALIDATED** — deep per-role/allocation-write not exercised |
| po_portal_suggestion.py | Read+Write, LIVE PROD | ✅ | ✅ | ✅ | ✅ (staging) | N/A | ✅ (login+dynamic schema) | ✅ | **VALIDATED** — feedback upload not exercised |
| po_simulator_v2.py | Read, largest file | ✅ | ✅ | ✅ | N/A | N/A | ✅ | ✅ | **VALIDATED** — RSA-gated page inaccessible (no real secret, by design) |
| smart_coverage.py | Read (Sheets)+Write | ✅ (code ready) | — | — | — | — | — | — | **BLOCKED** — needs Sheet access (hands-on sent) |
| noo_sku_mapping.py + noo_sku/ | Read (Sheets)+Write | ✅ (code ready) | — | — | — | — | — | — | **BLOCKED** — needs a *different* Sheet's access (hands-on below) |
| salesman_pjp.py | Read+Write | — | — | — | — | — | — | — | **SKIPPED** — active concurrent edits on this file by another session; will resume once clear |
| **sfa_attendance.py** | — | — | — | — | — | — | — | — | **NEEDS REDESIGN** — session-state-only transaction, per explicit instruction not to lift-and-shift |
| **time_study_stopwatch.py** | — | — | — | — | — | — | — | — | **NEEDS REDESIGN** — server-held polling loop, per explicit instruction not to lift-and-shift |
| skt_area_execution_capability.py | — | — | — | — | — | — | — | — | **DEAD** — do not migrate (writes same table as v2 with incompatible schema) |
| skt_area_execution_capability_mock.py | — | — | — | — | — | — | — | — | **NOT A TARGET** — test/demo harness |
| po_portal_suggestion_v2.py | — | — | — | — | — | — | — | — | **NOT A TARGET** — superseded, fixes never merged to prod |
| po_portal_suggestion_dev.py | — | — | — | — | — | — | — | — | **NOT A TARGET** — explicit test harness |
| po_portal/, po_portal_mockup.zip | — | — | — | — | — | — | — | — | **DEAD** — source already deleted |
| docs_crawler.py, build_html_guide.py, build_pptx.py | — | — | — | — | — | — | — | — | **OUT OF SCOPE** — not Streamlit apps |

**13 of ~22 apps fully containerized, deployed to Cloud Run, and functionally validated with real production data** (7 of those also have a validated write path, 4 independently re-verified at the database level, 1 via service-account impersonation). 2 are code-ready and blocked purely on external Sheet-sharing steps. 1 is deliberately skipped to avoid colliding with someone else's concurrent work. 2 are correctly excluded pending a redesign, per explicit instruction. The remainder are dead code or out of scope.

## Pending hands-on actions

```text
HANDS-ON REQUIRED #1 (sent earlier, still open)

Google Sheet: Smart Coverage tab, spreadsheet 1E90Ogzx7VeD9E68Qq5OHqqf31T9scqIqE3QzyobdcbU
Share with: streamlit-migration-runtime@skintific-data-warehouse.iam.gserviceaccount.com
Permission: Viewer
Then tell me: "Done"
```

```text
HANDS-ON REQUIRED #2 (new)

Google Sheet: "NOO TRACKER GT", spreadsheet 1bchAAMuXOT1lzuAB-KbrrAwpIrL1_MG3Hzcq823PAN4
https://docs.google.com/spreadsheets/d/1bchAAMuXOT1lzuAB-KbrrAwpIrL1_MG3Hzcq823PAN4/edit

Share with: streamlit-migration-runtime@skintific-data-warehouse.iam.gserviceaccount.com
Permission: Viewer (read-only)

Reason: noo_sku_mapping.py reads region/distributor identity from this
Sheet's "DIST DATABASE" tab and the NOO/SKU pool tabs. Writes to the pool
tabs are already gated off in this app (write_enabled=false by design,
per this codebase's own convention) - Viewer access is sufficient and
matches that existing safety gate; no write-capable Sheets access is
being requested.

Then tell me: "Done"
```
