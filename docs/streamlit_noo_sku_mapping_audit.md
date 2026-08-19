# NOO & SKU Mapping — Pre-Refactor Audit

**Status:** AUDIT ONLY — no code or data changed in this pass
**Date:** 2026-08-19
**Scope:** existing app (`noo_sku_mapping.py` + `noo_sku/`), spreadsheet `1bchAAMuXOT1lzuAB-KbrrAwpIrL1_MG3Hzcq823PAN4`, BigQuery `gt_schema` masters
**Companion docs:** [design](streamlit_noo_sku_mapping_design.md) · [implementation](streamlit_noo_sku_mapping_implementation.md)

> **Headline: the two pool worksheets now have headers, and they do not match what
> the application writes.** At the previous pass both pools were completely empty
> and the app defined its own layout. They have since been populated with a
> 36-column (NOO) and 13-column (SKU) snake_case header. The current writer would
> append 15 values into a 36-column sheet. **The write path is broken and must be
> refactored before any production write.**
>
> No harm has occurred: `WRITE_ENABLED` defaults to false and no production write
> has ever executed.

---

## 1. Current architecture

```
noo_sku_mapping.py        Streamlit UI: login gate, two sections, upload, preview
noo_sku/
  config.py               spreadsheet IDs, tab names, column positions, Settings
  normalize.py            cleaning, SHA-256 hashing, Asia/Jakarta dates
  customer_code.py        3-tier distributor-abbreviation resolution
  parsers.py              reads BD Support's .xlsx templates
  validators.py           row-level business rules
  duplicates.py           identity / content classification
  writer.py               pool row construction + guarded append
  sources.py              Sheets / Drive / BigQuery readers
tests/                    73 tests, all passing
```

Separation is already close to what the brief asks for: authentication, validation,
duplicate detection and write are distinct modules, and everything except
`sources.py` is pure and unit-tested. **Enrichment is the missing layer** — there
is no enrichment module at all today.

## 2. Current upload flow

`parse → wrong-template check → column check → row validation → ledger read →
duplicate classify → verdict → build rows → append`

**Gap vs the brief:** enrichment does not happen, so the brief's mandated order
(`validate → enrich → normalize → final identity → duplicate check`) is not met.
Duplicates are currently classified on raw user input.

## 3. Current authentication flow

Login reads `DIST DATABASE` (not BigQuery), validates code + `Status = Active` +
a password from `st.secrets`, and stores the record in `st.session_state`.
Distributor identity is already authoritative: `validate_noo` overwrites
`Branch Name` and `Customer Branch Code` from the session.

**Gap:** a mismatched distributor code in the file is a *warning*, not an error.
The brief requires **reject** (§4). This is a one-line severity change.

## 4. Current Google Sheets flow

| Operation | Target | Verb |
|---|---|---|
| Read distributor master | `DIST DATABASE` | `values.get` |
| Read SKU history | `SKU MAPPING` | `values.get` |
| Read NOO history | 3 brand tabs (cols N:O) | `values.batchGet` |
| Read ledger | both pools | `values.get` |
| Write | both pools **only** | `values.append` |

Audited: `append` is the **only** write verb in the codebase. No `update`,
`clear`, `batchUpdate`, or delete anywhere. Tracker sheets are already read-only
in practice. `ensure_headers` writes a header **only into a tab it reads as
empty** — which is now a no-op, correctly.

## 5. Current BigQuery flow

`master_product` (SKU validity/brand/pack_size) and `dms.gt_po_tracking_all_mv`
(abbreviation fallback). `master_distributor` is **not** currently used.
`master_store_database_basis` is **not** currently used. Both are required by the
brief.

---

## 6. Existing pool structures — ACTUAL, as read

### 6.1 `POOL NOO STREAMLIT` (gid 557889479) — 36 columns, header row only, **no data**

`asm_name`, `input_time`, `branch_name`, `region`, `store_id`, `store_name`,
`channel_name`, `customer_code`, `customer_branch_code`, `customer_store_code`,
`customer_store_name`, `city`, `store_address`, `longitude`, `latitude`,
`store_type`, `visibility_rating`, `location_rating`, `asm_kam`, `spv`, `se_kae`,
`aom`, `tl`, `pm`, `md/smd`, `ba1`, `ba2`, `ba3`, `ba4`, `group_branch_blank`,
`group_name`, `nik`, `npwp`, `remark`, `area`, `province`

### 6.2 `POOL SKU STREAMLIT` (gid 654605989) — 13 columns, header row only, **no data**

`asm`, `region`, `input_time`, `customer_code`, `customer_name`, `product_code`,
`customer_branch_code`, `product_name`, `customer_product_code`,
`customer_product_name`, `specification`, `barcode`, `description`

### 6.3 Relationship to the main trackers

The pools are the main trackers minus BD-Support-only columns, with `Date`
replaced by `input_time`:

| Pool | Mirrors | Dropped |
|---|---|---|
| `POOL NOO STREAMLIT` A–AJ | `SKINTIFIC NEW` C–AO | `DMS`, `BASIS`, `RSA`, `BD Support`, `NOO/Existing`, `Remarks` |
| `POOL SKU STREAMLIT` A–M | `SKU MAPPING` B–O | `DMS`, `RSA` |

**Corroboration:** the SKU pool names its 7th column `customer_branch_code`,
independently confirming the earlier finding that `SKU MAPPING` column I — declared
"Product Chinese Short Name" — actually holds the distributor code. The pool also
adds `product_name`, giving Principal Product Name a home for the first time and
closing risk **R3** from the implementation doc.

---

## 7. Column usage — what BD Support actually fills

Measured over 3,859 rows of `SKINTIFIC NEW` (the pool's direct precedent):

| Fill | Columns |
|---|---|
| **≥95% — genuinely required** | `asm_name`, `input_time`, `branch_name`, `region`, `store_id`, `store_name`, `channel_name`, `customer_code`, `customer_branch_code`, `customer_store_code`, `customer_store_name`, `store_address`, `store_type`, `asm_kam`, `spv`, `se_kae`, `aom` |
| **92–95% — mostly filled** | `city`, `area`, `province` |
| **≤0.1% — essentially unused (16 columns)** | `longitude`, `latitude`, `visibility_rating`, `location_rating`, `tl`, `pm`, `md/smd`, `ba1`–`ba4`, `group_branch_blank`, `group_name`, `nik`, `npwp`, `remark` |

**Recommendation:** write blank to the 16 unused columns. Populating them would
introduce data BD Support neither expects nor uses, and `group_name` is only 4.5%
available in the master anyway.

---

## 8. BigQuery join-key findings

### 8.1 Store enrichment — `master_store_database_basis`

Two candidate keys tested against 3,857 real tracker rows:

| Key | Direction | Match rate | Ambiguity |
|---|---|---|---|
| `store_id` → `cust_id` | primary | **99.9%** (3,853/3,857) | unique key |
| `customer_store_code` → `reference_id_skt` | fallback | **98.2%** (3,788/3,857) | 303 refs map to >1 basis row |
| **Composite** (`cust_id` first, then unique `reference_id`) | recommended | **99.9%** (3,854/3,857) | ambiguous refs skipped, not guessed |

Per-distributor fallback rates: DST111 100%, DST117 100%, DST121 100%,
DST325 99.2%, DST333 99.3%, DST332 97.8%.

**Recommended join:** `cust_id` when `store_id` is supplied; otherwise
`reference_id_{brand}` **only when it resolves to exactly one row**. Ambiguous
matches (303 refs globally) must be flagged, never arbitrarily picked.

### 8.2 Enrichment availability among matched stores

| Field | Available |
|---|---|
| `spv_skt` | 100.0% |
| `area_coverage` | 100.0% |
| `customer_type` | 99.4% |
| `province` | 98.5% |
| `se_skt` | **95.3%** |
| `aom_skt` | 81.3% |
| `group_name` | 4.5% |

> An earlier whole-table reading suggested SE was only ~50% available. That is the
> figure across all 59,217 basis rows; among the stores these trackers actually
> reference it is **95.3%**. SE enrichment from basis is viable.

### 8.3 Distributor enrichment — `master_distributor`

**`master_distributor` has no SE column of any kind.** Confirmed across all 38
columns. This settles the brief's §6 hierarchy: SE **cannot** come from
master_distributor and must come from `master_store_database_basis`
(`se_skt` / `se_tph` / `se_fcr`).

Per-brand availability among the 100 Active rows:

| Field | SKT | TPH | FR |
|---|---|---|---|
| `asm_*` | 91.6% | 88.8% | **52.1%** |
| `spv_*` | 61.9% | — | **44.7%** |
| `aom_*` | 45.1% | — | **45.1%** |
| `pm` | 100% (brand-neutral) | | |

FACERINNA distributor-level coverage is weak. Where `master_distributor` is thin,
`master_store_database_basis` is the better source for `spv`/`aom` too (100% /
81.3% among matched stores).

### 8.4 Product enrichment — `master_product`

748 rows, `sku` unique, join key `UPPER(TRIM(sku))` ↔ `Principal Product Code`.
Supplies `product_name` and `pack_size` → `specification`. `barcode` is INT64 and
predominantly `0`, so it is **not** a usable source for the pool's `barcode`
column.

---

## 9. FIELD LINEAGE MATRIX

`USER` = from the upload template · `SESSION` = authenticated identity ·
`MD` = `master_distributor` · `BASIS` = `master_store_database_basis` ·
`MP` = `master_product` · `SYS` = system-generated

### 9.1 `POOL NOO STREAMLIT`

| # | Pool column | Source | Join key | Transformation | Req? | Fallback | Validation | Dup role |
|---|---|---|---|---|---|---|---|---|
| A | `asm_name` | MD `asm_{brand}` | `distributor_code` ← SESSION | brand from `customer_code` prefix | Yes | BASIS `asm_{brand}` | non-blank | — |
| B | `input_time` | SYS | — | now(Asia/Jakarta) | Yes | — | — | **EXCLUDED** |
| C | `branch_name` | MD `distributor` | `distributor_code` ← SESSION | direct | Yes | `DIST DATABASE` col C | non-blank | content |
| D | `region` | MD `region` | `distributor_code` ← SESSION | direct | Yes | `region_g2g`, `DIST DATABASE` col Y | non-blank | content |
| E | `store_id` | USER | — | trim/upper | **No** | — | `^IE[A-Z]{2}\d{3,6}$` if present | content |
| F | `store_name` | USER | — | trim | Yes | — | non-blank | content |
| G | `channel_name` | USER | — | upper | Yes | — | ∈ {GT, MTI} | content |
| H | `customer_code` | USER (validated) | — | upper | Yes | — | prefix ∈ {11,13,1A} + session suffix | content |
| I | `customer_branch_code` | **SESSION** | — | overwrite | Yes | — | must equal session code | **identity** |
| J | `customer_store_code` | USER | — | upper | Yes | — | must start with session code | **identity** |
| K | `customer_store_name` | USER | — | = `store_name` | Yes | — | non-blank | content |
| L | `city` | USER | — | trim | Yes | BASIS `city` | warn if not in reference list | content |
| M | `store_address` | USER | — | trim | Yes | — | non-blank | content |
| N | `longitude` | — | — | **leave blank** (0.0% used) | No | — | — | — |
| O | `latitude` | — | — | **leave blank** (0.0% used) | No | — | — | — |
| P | `store_type` | USER | — | trim | Yes | BASIS `customer_type` | ∈ channel's allowed set | content |
| Q | `visibility_rating` | — | — | **leave blank** | No | — | — | — |
| R | `location_rating` | — | — | **leave blank** | No | — | — | — |
| S | `asm_kam` | MD `asm_{brand}` | as A | same as `asm_name` | Yes | BASIS | non-blank | — |
| T | `spv` | **BASIS** `spv_{brand}` | composite store key | 100% among matched | Yes* | MD `spv_{brand}` | — | — |
| U | `se_kae` | **BASIS** `se_{brand}` | composite store key | `se_fcr` for FACERINNA | Yes* | *(none — MD has no SE)* | — | — |
| V | `aom` | MD `aom_{brand}` | `distributor_code` | — | Yes* | BASIS `aom_{brand}` | — | — |
| W–AH | `tl`, `pm`, `md/smd`, `ba1`–`ba4`, `group_branch_blank`, `group_name`, `nik`, `npwp`, `remark` | — | — | **leave blank** (all ≤0.1% used) | No | — | — | — |
| AI | `area` | BASIS `area_coverage` | composite store key | 100% among matched | No | MD `area_coverage` | — | — |
| AJ | `province` | BASIS `province` | composite store key | 98.5% among matched | No | MD `province` | — | — |

\* **Blocked pending decision B2** — see §12.

### 9.2 `POOL SKU STREAMLIT`

| # | Pool column | Source | Join key | Transformation | Req? | Fallback | Validation | Dup role |
|---|---|---|---|---|---|---|---|---|
| A | `asm` | MD `asm_{brand}` | `distributor_code` ← SESSION | brand from resolved SKU | Yes | BASIS | non-blank | — |
| B | `region` | MD `region` | `distributor_code` ← SESSION | direct | Yes | `region_g2g` | non-blank | content |
| C | `input_time` | SYS | — | now(Asia/Jakarta) | Yes | — | — | **EXCLUDED** |
| D | `customer_code` | **DERIVED** | — | `brand_prefix(MP.brand) + suffix(session)` | Yes | — | must resolve | content |
| E | `customer_name` | MD `distributor` | `distributor_code` ← SESSION | direct | Yes | `DIST DATABASE` col C | non-blank | content |
| F | `product_code` | USER | `UPPER(TRIM(sku))` → MP | validated against MP | Yes | — | must exist in MP; brand in scope | **identity** |
| G | `customer_branch_code` | **SESSION** | — | overwrite | Yes | — | must equal session code | **identity** |
| H | `product_name` | **MP** `product_name` | `sku` | authoritative over user input | Yes | USER value | — | content |
| I | `customer_product_code` | USER | — | trim | Yes | — | non-blank | **identity** |
| J | `customer_product_name` | USER | — | trim | Yes | — | non-blank | content |
| K | `specification` | **MP** `pack_size` | `sku` | authoritative over user input | No | USER value | — | content |
| L | `barcode` | — | — | **leave blank** (MP barcode is INT64, mostly 0) | No | — | — | — |
| M | `description` | — | — | **leave blank** | No | — | — | — |

---

## 10. Existing tracker usage (read-only, unchanged)

| Tab | Used for | Verb |
|---|---|---|
| `SKINTIFIC NEW` / `TIMEPHORIA NEW` / `FACERINNA NEW` | NOO identity history (cols N:O) | read |
| `SKU MAPPING` | SKU identity history; abbreviation source | read |
| `DIST DATABASE` | login population, distributor name/region/abbreviation | read |

## 11. Current duplicate logic

Already row-level and already matches the brief's classification
(`NEW` / `CORRECTION` / `EXACT_DUPLICATE` / `DUPLICATE_IN_FILE`), with identity +
content hashes scoped per distributor and `input_time` not part of any hash.

| Brief requirement | Status |
|---|---|
| Per-row detection | ✅ already |
| `EXACT_DUPLICATE` skipped | ✅ already |
| `CORRECTION` inserted as a new row | ✅ already |
| In-file duplicates warn | ✅ already |
| Partial duplicates don't block new rows | ⚠️ **partly** — the current `verdict()` blocks the whole file when *any* duplicate is present, pending a checkbox. The brief wants eligible rows to flow with a warning. Behaviour is close but the wording/flow needs to match §16. |
| Enrichment before duplicate check | ❌ **not met** — no enrichment layer exists |
| `input_time` excluded from hash | ✅ already |

---

## 12. Blocking issues — implementation stops here pending decisions

### **B1 — Missing pool columns (report, do not add)**

Per brief §2, I am reporting rather than adding:

| Missing field | Why it was wanted | Impact of absence | Safest option |
|---|---|---|---|
| `upload_id` | tags every row of one submission; makes rollback a filter-and-delete | **No clean rollback.** A bad batch must be identified by `input_time` + distributor instead | Use `input_time` (second precision) + `customer_branch_code` as the de-facto batch key. Workable — no column needed. **Recommend accepting this.** |
| `row_type` (`NEW`/`CORRECTION`) | lets BD Support see which rows are corrections | BD Support cannot tell a correction from a new row in the pool | Surface the classification in the app's preview and export, not in the sheet. **Recommend accepting.** |
| separate `date` | trackers carry `Date` distinct from a timestamp | none — `input_time` carries the date | Derive display date from `input_time`. **No action needed.** |

None of these blocks the build. **I recommend proceeding without any column
change**, but flagging so the decision is explicit.

### **B2 — Enrichment is impossible for genuinely new stores** ⛔ *needs your decision*

43% of tracker NOO rows are `NOO -> Create ID` — genuinely new outlets. A new
store **does not exist in `master_store_database_basis` at submission time**, so
`se_kae`, `spv`, `aom`, `area`, `province` cannot be resolved for exactly the rows
the NOO process is designed to handle.

The 99.9% match rate in §8.1 is measured on *historical* rows, which basis has
since caught up with. It is **not** the rate a live submission will see.

Brief §23 says a row missing a mandatory enrichment must be rejected. Applied
literally to `se_kae`, that would reject most genuine NOO submissions.

| Option | Behaviour | Trade-off |
|---|---|---|
| **(A) Recommended** — treat `se_kae`/`spv`/`aom`/`area`/`province` as **enrich-if-available, else blank** | New stores submit successfully with those cells blank; BD Support completes them as they already do | Matches the observed workflow; pool rows are partially enriched by design |
| (B) Mandatory | Reject any row that cannot resolve SE | Blocks the majority of real NOO submissions |
| (C) Mandatory only for existing stores | Reject when the store *is* in basis but SE is blank (4.7% of matched) | More complex; small benefit |

**Recommendation: (A).** It matches how BD Support demonstrably works — `se_kae`
is 96.7% filled in the tracker *after* their processing, not at submission.

### **B3 — `store_type` vocabulary mismatch** ⛔ *needs your decision*

The template dropdown and the master use different words for the same thing:

| Template (`City & Store Type`) | `basis.customer_type` |
|---|---|
| `Regular SPM` | `Regular Supermarket` |
| `Premium SPM` | `Premium Supermarket` |

`basis` is also dirty: `Cosmetic Store ` (trailing space, 122 rows),
`RETAIL STORE` (86), and 91 rows with `GT` + `Minimarket`, which violates the
template's own channel constraint.

**Recommendation:** the user's template value is authoritative for `store_type`
(they pick from the controlled dropdown); do **not** overwrite it from basis and
do **not** auto-translate between vocabularies. Use basis only when the user
value is absent. Needs confirmation.

---

## 13. Required changes

| # | Change | Files | Risk |
|---|---|---|---|
| 1 | Replace `POOL_*_HEADERS` with the actual 36/13-column layouts; read the live header and assert a match before writing | `config.py`, `writer.py` | **Critical — current writer misaligns** |
| 2 | Add an `enrichment.py` module (distributor, store, product resolvers) | new | Medium |
| 3 | Re-order the pipeline to `validate → enrich → normalize → duplicate` | `noo_sku_mapping.py` | Medium |
| 4 | Rebuild `build_noo_rows` / `build_sku_rows` against the real layouts | `writer.py` | High |
| 5 | Distributor mismatch in file → **error**, not warning | `validators.py` | Low |
| 6 | Rebuild ledger column offsets for the new pool layouts | `sources.py` | High |
| 7 | Partial-duplicate flow: warn + proceed with eligible rows per §16 | `noo_sku_mapping.py`, `duplicates.py` | Low |
| 8 | Add `master_distributor` + `master_store_database_basis` readers | `sources.py` | Medium |
| 9 | Indonesian guideline expander + PDF download | `noo_sku_mapping.py`, new | Low |
| 10 | Tests for the 30 scenarios in brief §28 | `tests/` | Low |
| 11 | Assert-before-write: refuse if the live header differs from the expected layout | `writer.py` | **Safety-critical** |

## 14. Risks

| # | Risk | Mitigation |
|---|---|---|
| R1 | Pool header changes again between now and go-live | Change 11: read and compare the header on every write; refuse on mismatch |
| R2 | Enrichment silently produces blanks | Preview shows per-column enrichment status before confirmation |
| R3 | 303 ambiguous `reference_id` values | Never auto-pick; skip enrichment and flag |
| R4 | FACERINNA distributor coverage 44–52% | Fall back to basis; surface unresolved fields in preview |
| R5 | Brand-conditional column selection (`_skt`/`_tph`/`_fcr`) is easy to get wrong | Table-driven per brand + unit tests per brand |
| R6 | `DIST DATABASE` (215 active) vs `master_distributor` (100 active) disagree | Login stays on `DIST DATABASE`; enrich from `master_distributor` where present, `DIST DATABASE` otherwise |

## 15. Recommended implementation plan

| Phase | Work | Gate |
|---|---|---|
| **0** | Decisions **B1, B2, B3** | ⛔ blocks all writing |
| 1 | Real pool layouts in config + header-assert guard | — |
| 2 | `enrichment.py` + per-brand resolution + unit tests | — |
| 3 | Rewire pipeline order; rebuild row builders and ledger offsets | Phase 1–2 |
| 4 | Distributor-mismatch error; partial-duplicate flow | — |
| 5 | Indonesian guideline + PDF | — |
| 6 | Full test suite to brief §28 | Phases 1–5 |
| 7 | Dry run against live pools; verify column alignment | Phase 6 |
| 8 | Controlled first write: 2–3 rows, one pilot distributor, verify, report | ⛔ explicit approval |

---

## 16. Production readiness

**NOT READY.**

| Criterion | Status |
|---|---|
| Pool structure preserved, not modified | ✅ verified — headers read, nothing written |
| Tracker sheets read-only | ✅ verified — `append` is the only write verb, targeting pools only |
| No new spreadsheet created | ✅ |
| Join keys documented and validated | ✅ §8 |
| All pool columns have documented lineage | ✅ §9 |
| Login determines authoritative distributor | ✅ (mismatch severity needs change 5) |
| Enrichment before duplicate check | ❌ not implemented |
| Writer matches actual pool layout | ❌ **broken — 15 vs 36 columns** |
| `input_time` populated, WIB, excluded from hash | ⚠️ generated correctly; not yet mapped to the real column |
| Indonesian guideline + PDF | ❌ expander exists, PDF does not |
| Dry run against real layout | ❌ pending refactor |

Blocking: **B2** and **B3** are business decisions; **change 1/4/6** is the
technical blocker.
