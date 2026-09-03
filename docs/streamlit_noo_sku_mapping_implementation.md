# Streamlit NOO & SKU Mapping — Implementation

**Status:** IMPLEMENTED, dry-run verified against live data. **Not deployed. No production write has been performed.**
**Date:** 2026-08-19
**Repository:** `skt-project/streamlit_app`
**Entry point:** `noo_sku_mapping.py`
**Companion:** [`streamlit_noo_sku_mapping_design.md`](streamlit_noo_sku_mapping_design.md)

Every structural claim below was verified by read-only inspection of the live
spreadsheets on 2026-08-19. Where a fact contradicts the earlier design
document, this file supersedes it and the correction is called out.

---

## 1. Spreadsheet audit

### 1.1 Access

Both service accounts can now read all three files.

| Source | ID | Drive type | Title |
|---|---|---|---|
| Tracker | `1bchAAMuXOT1lzuAB-KbrrAwpIrL1_MG3Hzcq823PAN4` | **native Google Sheet** | `NOO TRACKER GT` |
| SKU source | `1UObRQCPBB3grWvGcbe3S9F-gW8LWS_Pk` | **.xlsx file in Drive** | `SKU MAPPING_TEMPLATE FOR STREAMLIT` |
| NOO source | `1Yt6vRRVSz2-mm59KzVsq32MrwqmzDoYB` | **.xlsx file in Drive** | `NOO MAPPING_TEMPLATE FOR STREAMLIT` |

> The two "mapping sources" are **not** Google Sheets. They are Excel files
> stored in Drive, so `gspread` cannot open them at all. They are fetched with
> Drive `files().get_media()` and read with `openpyxl`. This is handled in
> `noo_sku/sources.py:download_template`.

### 1.2 What each source actually is

Answering the brief's "master / output tracker / both" question from the data,
not from the filename:

| Source | Verdict | Evidence |
|---|---|---|
| `NOO TRACKER GT` | **Output tracker + master** | Holds the pools, the three brand trackers, and `DIST DATABASE` (the distributor master) |
| `NOO MAPPING_TEMPLATE FOR STREAMLIT` | **Blank template only** | 0 data rows; contains `Template`, `Guideline`, and a `City & Store Type` reference sheet |
| `SKU MAPPING_TEMPLATE FOR STREAMLIT` | **Blank template only** | 0 data rows; contains `SKU TEMPLATE FOR STREAMLIT` and `GUIDELINE` |

Neither template file is written to. They are the artefacts DB admins download.

### 1.3 The tracker's tabs

`gid=2087836837` in the supplied URL is **`SKU MAPPING`** — the BD Support main
SKU tracker, **not** a pool tab. The gid simply reflects whichever tab was
active when the link was copied. Both pool tabs exist under different gids.

| gid | Tab | Size | Hidden | Role |
|---|---|---|---|---|
| 848056557 | `SKINTIFIC NEW` | 4297×177 | no | NOO main tracker, brand 11 |
| 623358114 | `TIMEPHORIA NEW` | 4044×177 | no | NOO main tracker, brand 13 |
| 1247943160 | `FACERINNA NEW` | 4035×177 | no | NOO main tracker, brand 1A |
| **2087836837** | **`SKU MAPPING`** | 6202×31 | no | SKU main tracker (5,353 data rows) |
| 1421740146 | `DIST DATABASE` | 1635×47 | no | Distributor master (331 rows) |
| **557889479** | **`POOL NOO STREAMLIT`** | 1000×26 | no | **Empty — Streamlit owns it** |
| **654605989** | **`POOL SKU STREAMLIT`** | 1000×26 | no | **Empty — Streamlit owns it** |
| 283935608 | `ASM/SPV/SE` | 1000×26 | no | Reference |
| 956332658 | `Acuan City, Store Type` | 1000×26 | no | Reference |
| 0, 349145311, 336483192 | `* ARCHIVE` | — | yes | Historical |
| 612071998, 1027891763, 757753634, 460752218, 5973957, 1258628885, 455723114, 57865724 | helpers / `Sheet15` / `RATU *` / `SPAM` | — | yes | Scratch |

**Both pool tabs are completely empty — not even a header row.** The app writes
the header once, and only into a tab it finds empty
(`noo_sku/writer.py:ensure_headers`).

---

## 2. Actual column mappings

### 2.1 `DIST DATABASE` — the distributor master

Row 1 is a merged grouping banner; the **real header is on row 2**, data from
row 3.

| Col | Header | Used for |
|---|---|---|
| A | `ID CODE` | — |
| B | `distributor_company_name` | fallback display name |
| C | `Distributor Name ` *(trailing space)* | **Distributor Name** |
| D | `Status` | **login gate** (`Active`) |
| Y | `Region` | pool `Region` column |
| AB | `Distributor Code Fix` | **login identity** (`DST082`) |
| **AO** | **`Customer Branch Code`** | **the DB abbreviation** (`CEC`) |

> **Correction to the design document.** The design said `master_distributor`
> in BigQuery has no abbreviation column and that PO history was the only
> source. Column **AO of `DIST DATABASE` is the authoritative abbreviation**,
> and it reproduces both MoM worked examples exactly: `DST082 CV CECE → CEC`
> (→ `11CEC`) and `DST164/165 PT KARYA ANANDA SUKSES → KAS` (→ `1AKAS`).

> **Correction to the design document — login population.** The design proposed
> gating login on BigQuery `gt_schema.master_distributor`. That table has only
> **100** Active distributors; `DIST DATABASE` has **215** and is a strict
> superset (intersection 100, sheet-only 115, BigQuery-only 0). Gating on
> BigQuery would lock out 115 of 215 real admins, so **login is gated on
> `DIST DATABASE`**.

### 2.2 `SKU MAPPING` — 15 columns, header row 1, 5,353 data rows

| Col | Declared header | **Actual content** | Fill |
|---|---|---|---|
| A | `DMS` | BD Support processing flag (`DONE`) | 99.9% |
| B | `ASM` | ASM name | 99.2% |
| C | `RSA` | RSA name | 99.2% |
| D | `Region` | Region | 99.2% |
| E | `Date` | **M/D/YYYY** | 99.9% |
| F | `Customer Code` | `11xxx`/`13xxx`/`1Axxx` | 99.9% |
| G | `Customer Name` | **the DISTRIBUTOR's name** | 99.9% |
| H | `Product` | **SKU Code Principal** | 99.9% |
| **I** | `Product Chinese Short Name` | **⚠ DISTRIBUTOR CODE** | 99.3% |
| J | `Product English Name` | *(unused)* | 0.1% |
| K | `Customer Product Code` | **SKU Code DB** | 99.9% |
| L | `Customer Product Name` | **SKU Name DB** | 99.9% |
| M | `Specification` | *(unused)* — Size | 0.0% |
| N | `Barcode` | *(unused)* | 0.0% |
| O | `Description` | *(unused)* | 0.0% |

**Column I is mislabelled.** Its declared header says "Product Chinese Short
Name", but 5,316 of 5,316 populated values match `^DST`. It holds the
Distributor Code. The code reads this column **by position, never by name**
(`config.SKU_MAP_COL_DIST_CODE = 8`).

Customer Code prefix distribution across all 5,353 rows — confirming the
three-brand scope with nothing else present: `11` ×3,132, `13` ×1,872, `1A` ×346.

**There is no column for SKU Name Principal.** The MoM asks admins to supply it
and the template collects it, but the main tracker has nowhere to store it
(column J is empty). The pool keeps it in its own `Principal Product Name`
column; see risk R3.

### 2.3 `SKINTIFIC NEW` — the NOO main tracker (28 leading columns)

`DMS`, `BASIS`, `ASM`, `RSA`, `BD Support`, `Date`, `Branch Name`, `Region`,
`NOO/Existing`, `Store ID`, `Store Name`, `Channel`, `Customer Code`,
`Customer Branch Code`, `Customer Store Code`, `Customer Store Name`, `City`,
`Store Address`, `Longitude`, `Latitude`, `Store Type`, `Visibility Rating`,
`Location Rating`, `ASM/KAM`, `SPV`, `SE/KAE`, `AOM`, `TL`.

Date format here is **`DD-Mmm-YYYY`** (`16-Mar-2026`) — different from the SKU
tab's `M/D/YYYY`. Each pool follows the format of the tab it feeds.

### 2.4 The real upload templates

Both differ from the MoM, and neither puts its header on row 1.

**NOO — sheet `Template`.** Banner row 1, **header row 2**, `CONTOH` example
row 3, data from row 4. **10 columns, not the MoM's 7.**

| Col | Header | MoM? |
|---|---|---|
| A | `Store ID (Opsional)` | yes |
| B | `Store Name` | yes |
| C | `Channel (GT / MTi)` | yes |
| D | **`Branch Name`** | **new** |
| E | `Customer Code ` | yes |
| F | **`Customer Branch Code`** | **new** |
| G | `Customer Store Code` | yes |
| H | `City` | yes |
| I | `Store Address` | renamed |
| J | **`Store Type`** | **new** |

**SKU — sheet `SKU TEMPLATE FOR STREAMLIT`.** Banner row 1, blank row 2,
**header row 3**, `CONTOH` marker row 4, example values row 5, data from row 6.
Five columns, renamed from the MoM: `Principal Product Code`,
`Principal Product Name`, `Product Size (ml/g)`,
`Customer Product Code ( Di isi oleh Distributor)`,
`Customer Product Name  ( Di isi oleh Distributor)` *(note the double space)*.

The NOO template also ships a `City & Store Type` reference sheet: 35 provinces,
513 cities, and store types constrained per channel —
GT → `ATC`, `Cosmetic Store`, `Pharmacy`, `Retail Store`;
MTI → `Large Supermarket`, `Minimarket`, `Premium SPM`, `Regular SPM`, `Specialty`.

---

## 3. Data ownership

| Spreadsheet | Tab | Purpose | Streamlit access |
|---|---|---|---|
| `NOO TRACKER GT` | `POOL NOO STREAMLIT` | NOO submissions | **READ + APPEND** |
| `NOO TRACKER GT` | `POOL SKU STREAMLIT` | SKU submissions | **READ + APPEND** |
| `NOO TRACKER GT` | `DIST DATABASE` | Distributor master, login, abbreviation | READ ONLY |
| `NOO TRACKER GT` | `SKU MAPPING` | SKU history → duplicate ledger, suffix source | READ ONLY |
| `NOO TRACKER GT` | `SKINTIFIC NEW` / `TIMEPHORIA NEW` / `FACERINNA NEW` | NOO history → duplicate ledger | READ ONLY |
| `NOO TRACKER GT` | all other tabs | — | **NOT ACCESSED** |
| `NOO MAPPING_TEMPLATE...` | `Template`, `Guideline` | template served to admins | READ ONLY |
| `NOO MAPPING_TEMPLATE...` | `City & Store Type` | city + store-type validation | READ ONLY |
| `SKU MAPPING_TEMPLATE...` | all | template served to admins | READ ONLY |
| BigQuery | `gt_schema.master_product` | SKU validity, brand, pack size | READ ONLY |
| BigQuery | `dms.gt_po_tracking_all_mv` | suffix fallback | READ ONLY |

**Append is the only write verb in the codebase.** There is no call to
`values.update`, `values.clear`, `batchUpdate`, or any delete anywhere in
`noo_sku/`. The single exception, `ensure_headers`, writes a header row **only
into a tab it has just read as empty**.

---

## 4. Data flow

```
                        DIST DATABASE (215 active)
                                 │  code → name, region, abbreviation, status
                                 ▼
  Distributor login ────► session identity  DSTxxx     ◄── password from st.secrets
                                 │
             ┌───────────────────┴───────────────────┐
             ▼                                       ▼
     NOO template (.xlsx)                    SKU template (.xlsx)
     downloaded from Drive                   downloaded from Drive
             │                                       │
             ▼                                       ▼
     parse (header row 2,                    parse (header row 3,
     drop CONTOH row 3)                      drop CONTOH rows 4-5)
             │                                       │
             ▼                                       ▼
     wrong-template check  ◄── header signature ──►  wrong-template check
             │                                       │
             ▼                                       ▼
     row validation                          row validation
     · Store ID optional                     · SKU must exist in master_product
     · Channel ∈ {GT, MTI}                   · brand must be in scope
     · Store Type ∈ channel set              · name/size vs principal (warn)
     · Customer Code = this DB's             · Customer Code DERIVED, not typed
     · Store Code starts with DSTxxx
     · Branch Name / Customer Branch Code OVERWRITTEN from session
             │                                       │
             ▼                                       ▼
     ledger: POOL NOO + 3 brand tabs         ledger: POOL SKU + SKU MAPPING
             │                                       │
             ▼                                       ▼
     classify NEW / CORRECTION / EXACT_DUPLICATE / DUPLICATE_IN_FILE
             │                                       │
             ▼                                       ▼
     build pool row                          build pool row
     Date = DD-Mmm-YYYY (WIB)                Date = M/D/YYYY (WIB)
             │                                       │
             ▼                                       ▼
     append → POOL NOO STREAMLIT             append → POOL SKU STREAMLIT
                          (blocked unless WRITE_ENABLED=true)
                                 │
                                 ▼
                    BD Support daily check → 3 brand tabs / SKU MAPPING
```

---

## 5. Distributor resolution

`DIST DATABASE` is the single source for identity:

```
DSTxxx ──► Distributor Name   (col C, fallback col B)
       ──► Status             (col D)  → only "Active" may log in
       ──► Region             (col Y)  → written to the SKU pool
       ──► Customer Branch Code (col AO) → the abbreviation
```

BigQuery `gt_schema.master_distributor` is **not** used for login (see the
correction in §2.1). It remains in use only for `master_product` and the PO
fallback, which are different tables.

---

## 6. Customer Code logic

```
Customer Code = brand_prefix(brand_of(SKU Code Principal)) + db_suffix(Distributor Code)
                └── 11 / 13 / 1A ──┘                        └──── 3-4 chars ────┘
```

**Brand is looked up, never parsed.** SKU code shapes are not uniform — SKINTIFIC
alone uses `SKINTIFIC-01`, `SKT-65+77`, `RMD-G-05+...` and `SX-6`; TIMEPHORIA
uses `TCC102001`; FACERINNA uses `F116`. The code reads
`gt_schema.master_product.brand` for the SKU and maps that to a prefix.

**Suffix resolution is three-tier**, highest priority first:

| # | Source | Covers (of 215 active) | Agreement with tier 1 |
|---|---|---|---|
| 0 | `st.secrets` override | manual | — |
| 1 | `DIST DATABASE!AO` | 97 | — |
| 2 | `SKU MAPPING` history | 28 (1 new) | 26 / 27 |
| 3 | BigQuery PO history | 212 (117 new) | 63 / 63 |
| | **Union** | **215 — all of them** | |

> **Correction to an interim finding.** An earlier pass reported that 91 active
> distributors could not resolve. That was an artefact of the `bq` command-line
> client's default 100-row output cap, which silently truncated the PO-history
> export. Queried through the API without a cap, PO history covers 212 of 215,
> and **the union of all three sources resolves every active distributor —
> 215/215, zero unresolved.** The SKU section does not need to be blocked for
> anyone.

Two genuine conflicts remain, both resolved in favour of `DIST DATABASE` and
both surfaced rather than hidden (`SuffixResolution.conflict`):

| Distributor | DIST DATABASE | SKU history | PO history | Chosen |
|---|---|---|---|---|
| `DST121` | `KIS` | `SDS` | `KIS` | `KIS` |
| `DST325` | `SJMA` | `SJM` | `SJM` | `SJMA` |

`DST325` is the one where the sheet stands alone against both historical
sources — see question Q2.

---

## 7. Duplicate detection

Two keys, both scoped to the session's distributor so one DB can never collide
with another.

| | Identity key | Content hash |
|---|---|---|
| **NOO** | `DSTxxx` + `Customer Store Code` | SHA-256 of 8 template fields + `DSTxxx` |
| **SKU** | `DSTxxx` + `Principal Product Code` + `Customer Product Code` | SHA-256 of 5 template fields + `DSTxxx` |

> The NOO identity key is **confirmed by BD Support's own guideline**, which
> states: *"Jika satu store dengan customer store code yang sama 'not exist'
> pada lebih dari satu brand, hanya perlu menginput satu kali."* Customer Store
> Code is the store identity. This closes design question O4.

Ledger sources — the design flagged that BD Support *moves* rows out of the
pool, which would destroy history. Implemented mitigation: the ledger is the
union of the pool **and** the main trackers.

| Upload | Identities from | Content hashes from |
|---|---|---|
| NOO | `POOL NOO STREAMLIT` + `SKINTIFIC/TIMEPHORIA/FACERINNA NEW` (col N/O) | pool only |
| SKU | `POOL SKU STREAMLIT` + `SKU MAPPING` (col H/I/K) | pool only |

Content hashes cannot be rebuilt from `SKU MAPPING` because it has no Principal
Product Name column, so a mapping already in the main tracker classifies as
`CORRECTION` rather than `EXACT_DUPLICATE`. That is the safe direction — it is
inserted for BD Support to adjudicate rather than silently dropped — but it is a
real limitation. See risk R3.

Verified ledger sizes for `DST082`: NOO 88 identities, SKU 240 identities.

**File-level verdict**

| Composition | Behaviour |
|---|---|
| 100% duplicates | **Reject.** *"File ini berisi data yang sudah pernah diupload sebelumnya."* |
| Any duplicates + some new | **Block, show the breakdown, require an explicit tick-box** before appending only the new/corrected rows |
| All new / corrections | Proceed |

---

## 8. Security

| Control | Where |
|---|---|
| Distributor Code comes only from `st.session_state` | `noo_sku_mapping.py` |
| `Branch Name` / `Customer Branch Code` in the file are **overwritten** from the session, with a warning | `validators.validate_noo` |
| `Customer Code` must belong to the logged-in DB | `validators.validate_noo` |
| `Customer Store Code` must be prefixed with the session's DST code | `validators.validate_noo` |
| Ledger reads filtered to the session's DB — no cross-DB visibility | `sources.load_*_ledger` |
| Passwords in `st.secrets`, never in source | `noo_sku_mapping.py:_passwords` |
| `valueInputOption="RAW"` blocks formula injection and numeric coercion | `sources.SheetsClient.append_column_span` |
| Exceptions never surface as tracebacks | `noo_sku_mapping.py:_handle_upload` |

Verified in the live dry run: a row claiming `PT PENYUSUP` / `DST999` while
logged in as `DST082` produced warnings and was written as `CV CECE` / `DST082`.

> **Conflict flagged, not silently resolved.** BD Support's NOO guideline
> instructs admins to *type* Branch Name (#6) and Customer Branch Code (#7),
> while the standing instruction is that distributor fields must never be
> enterable. The implementation keeps the columns — so the template BD Support
> is training admins on stays valid — but treats them as system-owned and
> overwrites them. Both requirements are satisfied. See question Q1.

---

## 9. Environment configuration

Nothing is hardcoded; write access is off unless explicitly enabled.

```toml
# .streamlit/secrets.toml   (gitignored)
[app]
env = "dev"              # or "production"
write_enabled = false    # MUST be true for any spreadsheet write
tracker_spreadsheet_id = "1bchAAMuXOT1lzuAB-KbrrAwpIrL1_MG3Hzcq823PAN4"

[connections.bigquery]
# existing service-account block, reused unchanged

[distributor_passwords]
DST082 = "..."

[distributor_suffix_overrides]
# only if BD Support needs to force an abbreviation
```

Environment variables are the fallback: `APP_ENV`, `WRITE_ENABLED`,
`TRACKER_SPREADSHEET_ID`, `GOOGLE_APPLICATION_CREDENTIALS`.

| Mode | `APP_ENV` | `WRITE_ENABLED` | Behaviour |
|---|---|---|---|
| **DEV / dry run** (default) | `dev` | `false` | Reads live data, validates, classifies duplicates, builds rows, shows exactly what *would* be written. **No spreadsheet write.** |
| **PRODUCTION** | `production` | `true` | As above, then appends. |

A missing or malformed setting fails closed to dry run.

The service account also needs the BigQuery scope — omitting it authenticates
fine and then fails at query time with `ACCESS_TOKEN_SCOPE_INSUFFICIENT`. All
three scopes are set in `sources.SCOPES_RW`.

---

## 10. Testing

`python run_all_tests.py` — or `--sanity` for the fast subset.

**73 tests for this feature, all passing** (55 in the sanity subset).

| Area | Tests | Covers |
|---|---|---|
| Parsing | 13 | banner/header/CONTOH handling, canonical keys, wrong template both ways, missing column, empty file, corrupt file, blank rows |
| Customer Code | 10 | all 3 brands, both MoM worked examples, tier priority, gap-filling, conflict visibility, unresolved → no guess |
| Normalisation | 6 | blank-spelling collapse, order sensitivity, header quirks, both date formats, WIB offset |
| NOO validation | 11 | optional Store ID, channel, store-type-per-channel, cross-DB customer code, store-code prefix, city warning, identity injection |
| SKU validation | 6 | unknown SKU, out-of-scope brand, required DB fields, strict/lenient name & size |
| Duplicates | 10 | all four buckets, cross-DB isolation, all three file verdicts |
| Writing | 9 | pool row shape/order, derived Customer Code, dry-run blocks append, write appends once, default is dry run, header written only into an empty tab |

### Two real bugs the tests caught

1. **SKU example row ingested as data.** The SKU template puts `CONTOH` alone on
   row 4 and the sample values on row 5. The original parser only skipped rows
   containing the literal `CONTOH`, so BD Support's example mapping
   (`TYY114002`) would have been written to the pool on **every** SKU upload.
   Fixed in `parsers._is_marker_only_row`; pinned by
   `test_sku_sample_values_row_is_not_ingested_as_data`.

2. **Canonical column keys.** `clean()` collapses whitespace, so the sheet
   header `Customer Product Name  (...)` (double space) never matched the
   constant. Every SKU upload would have failed with a spurious "column is
   empty" error on that field. Found by the live dry run, not by the unit tests
   — the tests had been feeding validators hand-built dicts rather than parser
   output. Fixed by re-keying in `parse_upload`; pinned by
   `test_parsed_rows_are_keyed_by_canonical_column_names`, and the test gap
   closed by routing two tests through the parser.

### Dry runs against live data (read-only, nothing written)

| Check | Result |
|---|---|
| Real NOO template parsed | `Template`, header row 2, 0 data rows, 0 missing columns |
| Real SKU template parsed | `SKU TEMPLATE FOR STREAMLIT`, header row 3, 0 data rows, 0 missing columns |
| Cross-upload detection | correct message in both directions |
| Login `DST082` | `CV CECE`, Active, `Northern Sulawesi` |
| Suffix | `CEC` from `DIST_DATABASE` → `11CEC` / `13CEC` / `1ACEC` |
| Coverage over 215 active | 215 resolved, 0 unresolved, 2 conflicts surfaced |
| NOO pipeline (4 rows: 2 good, 1 in-file dup, 1 invalid) | 5 errors + 3 warnings; identity injection confirmed; verdict `confirm`; 2 rows built |
| SKU pipeline (4 rows: 2 good, 1 out-of-scope, 1 unknown SKU) | 3 errors; `11CEC` / `13CEC` derived per brand; date `8/19/2026`; verdict `proceed` |
| **Pools after both dry runs** | **still completely empty** |

### Pre-existing unrelated failure

`tests/test_assessment_logic.py::TestBadStockGradeForYtd::test_just_under_80_percent_is_grade_c`
fails on a clean checkout. It belongs to the Distributor Assessment app;
`assessment_logic.py` and its tests are untouched by this work
(`git diff` is empty for both). Not fixed here — out of scope.

---

## 11. Deployment procedure

1. Merge to `main`. Streamlit Community Cloud deploys `noo_sku_mapping.py`.
2. Set secrets in the Streamlit Cloud UI with `write_enabled = false`.
3. Share `NOO TRACKER GT` with the service account as **Editor**.
4. **Smoke test in dry run**: log in as a pilot DB, download both templates,
   upload a filled file, confirm the preview and that both pools stay empty.
5. Have BD Support confirm the two pool header layouts (Q3) **before** any write.
6. Flip `write_enabled = true`, `env = "production"`.
7. First real upload: one pilot distributor, ≤5 rows. Confirm the appended rows
   land in the right columns and that BD Support can copy them across.
8. Roll out to the ~90 admins.

### Rollback

| Situation | Action |
|---|---|
| Bad data written | Delete the affected rows by `Upload ID` — every row carries one. |
| Systemic problem | Set `write_enabled = false`. The app degrades to dry run and stays usable for validation. |
| App broken | Revert the commit; no other app in the repo is affected. |
| Pool header wrong | Clear the pool's row 1 while the pool is still empty; `ensure_headers` rewrites it on the next upload. |

Because every write is an append tagged with an `Upload ID`, a rollback is a
filter-and-delete on that column — no restore from backup is needed.

---

## 12. Files created / modified

### Created

| Path | Lines | Purpose |
|---|---|---|
| `noo_sku_mapping.py` | ~420 | Streamlit entry point |
| `noo_sku/__init__.py` | 18 | package marker |
| `noo_sku/config.py` | ~185 | verified IDs, tabs, column positions, `Settings` |
| `noo_sku/normalize.py` | ~90 | cleaning, hashing, WIB dates |
| `noo_sku/customer_code.py` | ~145 | three-tier suffix resolution |
| `noo_sku/parsers.py` | ~185 | template parsing + wrong-template detection |
| `noo_sku/validators.py` | ~250 | NOO and SKU business rules |
| `noo_sku/duplicates.py` | ~135 | identity/content classification |
| `noo_sku/writer.py` | ~150 | pool row building + guarded append |
| `noo_sku/sources.py` | ~265 | Sheets / Drive / BigQuery readers |
| `tests/noo_sku_fixtures.py` | ~110 | template builders, fake client |
| `tests/test_noo_sku_parsers.py` | ~120 | 13 parsing tests |
| `tests/test_noo_sku_logic.py` | ~330 | 60 logic tests |
| `docs/streamlit_noo_sku_mapping_implementation.md` | this file | |

### Modified

| Path | Change |
|---|---|
| `run_all_tests.py` | coverage extended to the `noo_sku` package |
| `docs/streamlit_noo_sku_mapping_design.md` | corrections banner pointing here |

### Untouched

No existing application was modified. `requirements.txt` needs no change —
`streamlit`, `pandas`, `openpyxl`, `xlsxwriter`, `google-cloud-bigquery`,
`google-auth`, `gspread` and `pytz` are all already present. The app uses the
Sheets REST API via `googleapiclient` (a `google-cloud-bigquery` dependency)
rather than `gspread`, because it must also read the two `.xlsx` sources from
Drive, which `gspread` cannot do.

---

## 13. Remaining risks

| # | Risk | Impact | Mitigation |
|---|---|---|---|
| **R1** | Pool header layouts are the app's proposal, not BD Support's | Rows land in an order BD Support cannot copy straight across | Confirm Q3 before the first write; the pool is empty so the fix is free |
| **R2** | BD Support may delete rows from the pool | Content-hash history lost | Ledger already falls back to the main trackers for identities; `DMS` column left for a status flag instead of deletion |
| **R3** | `SKU MAPPING` has no Principal Product Name column | Re-uploading an identical mapping already promoted to the main tracker reads as `CORRECTION`, not `EXACT_DUPLICATE` | Errs toward inserting for BD Support to judge; resolved permanently once pool rows accumulate |
| **R4** | `Customer Store Code` quality is unproven | Weak NOO identity key | Guideline mandates the format and validation enforces the DST prefix; `noo_detector.py`'s fuzzy scorer is the fallback |
| **R5** | Read-then-append race at monthly closing | Two identical simultaneous uploads from one DB could both pass | Accepted at this scale; BD Support reviews daily |
| **R6** | Customer Code is many-to-one (`DST111/112/113` all → `CSA`) | It cannot identify a distributor | Pool carries `Distributor Code` as its own column |
| **R7** | 118 of 215 active distributors have a blank `Customer Branch Code` | Reliance on derived tiers | All resolve via PO history today; ask BD Support to backfill column AO |

---

## 14. Open questions

| # | Question | Current behaviour | Needs |
|---|---|---|---|
| **Q1** | The NOO guideline tells admins to type `Branch Name` and `Customer Branch Code`, but these must not be user-settable. Keep the columns, or remove them from the template? | Columns kept, values overwritten from the session with a warning | **[NEEDS CONFIRMATION]** |
| **Q2** | `DST325`: `DIST DATABASE` says `SJMA`, both historical sources say `SJM` | `SJMA` (sheet wins) | **[NEEDS CONFIRMATION]** |
| **Q3** | Exact pool header names and column order — should they mirror `SKINTIFIC NEW` / `SKU MAPPING` position-for-position? | SKU pool mirrors `SKU MAPPING` order exactly; NOO pool mirrors `SKINTIFIC NEW` naming | **[NEEDS CONFIRMATION] — blocks first write** |
| **Q4** | Can one DB map one `Principal Product Code` to several `Customer Product Code`s? | Yes — both codes are in the SKU identity key | **[NEEDS CONFIRMATION]** |
| **Q5** | Should a name/size mismatch against the principal master block the upload? The guideline says the values must match exactly. | Warning; `strict_names` / `strict_size` flags flip it to error | **[NEEDS CONFIRMATION]** |
| **Q6** | Partial-duplicate handling | Block, show breakdown, require explicit confirmation | **[NEEDS CONFIRMATION]** |
| **Q7** | Will BD Support mark rows processed in `DMS` rather than deleting them? | Pool treated as append-only; `DMS` left blank for them | **[NEEDS CONFIRMATION]** |
| **Q8** | `SKU Name Principal` has nowhere to go in the main tracker | Kept in the pool only | **[NEEDS CONFIRMATION]** |
| **Q9** | Max rows per upload | 5,000 | Confirm |

---

## 15. Production readiness

**Not production-ready.** Verified so far:

- ✅ Spreadsheet structure verified against the live sources
- ✅ Validation tested (73 automated tests + live dry runs)
- ✅ Duplicate detection tested (all four buckets, all three verdicts)
- ✅ Dry run tested end to end against live data, pools confirmed untouched
- ⬜ **Production write behaviour reviewed** — no write has ever been executed
- ⬜ Pool header layout signed off by BD Support (Q3)
- ⬜ Q1, Q2, Q4–Q9 answered
- ⬜ UAT with pilot distributor admins

The blocking item is Q3: the first write freezes the pool layout, and BD Support
copies ranges by position.


---

# 16. Part 2 — 2026-08-19 refactor

## 16.1 What changed

| Area | Before | After |
|---|---|---|
| Pool layout | app-defined 15 / 18 columns | **live 36 / 13 columns, read from the sheet** |
| Write guard | header written into an empty tab | `assert_layout()` re-reads the live header before **every** write (dry run included) and refuses on any mismatch |
| Enrichment | none | `enrichment.py` — distributor, store, product resolvers |
| Pipeline order | validate → dedup on raw input | **validate → enrich → normalize → identity → dedup**, enforced in `pipeline.py` |
| Duplicate input | raw user rows | **enriched pool rows** |
| Distributor mismatch in file | warning | **error** (brief §4) |
| Partial duplicates | whole file gated | eligible rows proceed; duplicates skipped and reported |
| `input_time` | not mapped | written to the real column, Asia/Jakarta, excluded from hashing |
| Guideline | English expander | **Indonesian expander + PDF download**, one source of truth |

## 16.2 Pipeline

```
raw upload
  -> parse (banner / header / CONTOH handling)
  -> wrong-template + column checks
  -> row validation            errors drop the row here; it never reaches the sheet
  -> ENRICHMENT                distributor / store / product
  -> normalisation             trim, collapse, blank-spelling
  -> identity resolution       computed on the enriched row
  -> duplicate classification  NEW / CORRECTION / EXACT_DUPLICATE / DUPLICATE_IN_FILE
  -> preview + warnings
  -> explicit user confirmation
  -> append eligible enriched rows to the existing pool
```

## 16.3 Enrichment rules (decisions B1–B3 as approved)

- **Missing required user input → ERROR.** The row is dropped before enrichment.
- **Master unavailable for a genuinely new store → INFO.** The row is kept and
  the enrichment columns are left blank. Verified live: a new store code produced
  blank `se_kae`/`area`/`province` and was still eligible.
- **Master available → populated automatically.** Verified live: real store codes
  resolved to `se_kae = Mohammad Fikram Dam`, `area`, `province`.
- **Ambiguous master match → never auto-selected.** Fields stay blank and the row
  is flagged for review (303 such reference ids exist).
- **`store_type` and `city` keep the user's exact vocabulary.** No translation
  between `Regular SPM` and `Regular Supermarket`; master is used only to fill a
  blank. Source master data is never modified.

## 16.4 Duplicate hashing

Identity — NOO: `customer_branch_code` + `customer_store_code`.
SKU: `customer_branch_code` + `product_code` + `customer_product_code`.

Content hash excludes:

1. `input_time` — brief §11/§13.
2. **Volatile store enrichment** (`se_kae`, `spv`, `aom`, `asm*`, `area`,
   `province`, `region`). *Design decision:* these are looked up at submission
   time and their availability drifts as master data catches up with new stores.
   Hashing them would turn an unchanged re-submission into a spurious
   `CORRECTION` the week after the store lands in the master. Identity and every
   user-entered business field remain hashed. **Flagged for confirmation.**
3. Permanently-blank pool columns, which would otherwise silently change every
   stored hash if they were ever populated.

## 16.5 Dry-run results (live data, nothing written)

| Check | Result |
|---|---|
| `assert_layout` vs live `POOL NOO STREAMLIT` | 36 = 36 ✅ |
| `assert_layout` vs live `POOL SKU STREAMLIT` | 13 = 13 ✅ |
| Store enrichment, real code, no `store_id` | resolved via `reference_id_skt` ✅ |
| Store enrichment, real code + `store_id` | resolved via `cust_id` ✅ |
| Genuinely new store | `se_kae`/`area`/`province` blank, row still eligible ✅ |
| Distributor-level fallback | `spv`/`aom` filled from `master_distributor` even when the store is unknown ✅ |
| Foreign distributor code in file | **rejected** with 2 errors ✅ |
| In-file duplicate | flagged, only the first row processed ✅ |
| SKU: master_product authoritative | user's wrong name/size warned, master values written ✅ |
| SKU: Customer Code per brand | `11CEC` / `13CEC` ✅ |
| SKU: out-of-scope + unknown SKU | both rejected ✅ |
| Row width written | 36 and 13 ✅ |
| **Both pools after all dry runs** | **still header-only — untouched** ✅ |

## 16.6 Files

**New:** `noo_sku/enrichment.py`, `noo_sku/pipeline.py`, `noo_sku/guideline.py`,
`tests/test_noo_sku_enrichment.py`, `docs/streamlit_noo_sku_mapping_audit.md`.

**Modified:** `noo_sku/config.py` (real layouts, unused/volatile/timestamp sets),
`noo_sku/writer.py` (rebuilt builders, `assert_layout`, `to_values`),
`noo_sku/duplicates.py` (enriched-row hashing), `noo_sku/sources.py`
(master_distributor + store basis readers, rebuilt ledger),
`noo_sku/validators.py` (mismatch severity), `noo_sku/normalize.py`
(`format_input_time`), `noo_sku_mapping.py` (pipeline + preview/confirm + PDF),
`tests/noo_sku_fixtures.py`, `tests/test_noo_sku_logic.py`.

**Untouched:** every other application in the repo.
