# Streamlit NOO & SKU Mapping — Design Document

**Status:** DESIGN — not implemented, not deployed
**Source of truth:** `MOM STREAMLIT NOO & SKU.docx` (BD Support x System Expert, Friday 14 August 2026, Symwhite 8th floor)
**Date:** 2026-08-18
**Repository:** `skt-project/streamlit_app`

> **SUPERSEDED IN PART — read
> [`streamlit_noo_sku_mapping_design`'s companion](streamlit_noo_sku_mapping_implementation.md)
> first.** This document was written before the real spreadsheets were
> available. It has since been implemented, and live inspection corrected
> several assumptions here. Where the two disagree, the implementation document
> wins. The corrections that matter:
>
> | § | This document says | Actually |
> |---|---|---|
> | 6.1 | NOO template has 7 columns | **10** — plus `Branch Name`, `Customer Branch Code`, `Store Type`; header is on **row 2**, not row 1 |
> | 6.2 | SKU headers named as in the MoM | Renamed — `Principal Product Code` etc.; header on **row 3** |
> | 10 | No abbreviation column exists anywhere | **`DIST DATABASE!AO "Customer Branch Code"`** is the authoritative source |
> | 10 | Login gated on BigQuery `master_distributor` | That table has only **100** active; `DIST DATABASE` has **215** and is a strict superset — login uses the sheet |
> | 11.4 | 3 distributors unresolvable, later revised to 91 | **0** — all 215 resolve. Both earlier figures were measurement artefacts (a join fan-out, then the `bq` CLI's 100-row cap) |
> | 12.3 | Spreadsheet inaccessible | Access granted; all three inspected |
> | 20-O4 | NOO duplicate key unconfirmed | **Confirmed** by BD Support's own guideline: Customer Store Code |
>
> Items marked **[NEEDS CONFIRMATION]** are business rules the MoM does not
> settle. The still-open ones are tracked as Q1–Q9 in the implementation document.

---

## 1. Business requirement

One Streamlit application serving **two** upload functions for Distributor (DB) admins:

1. **NOO / Store Mapping** — submit new stores
2. **SKU Mapping** — submit principal-SKU ↔ DB-SKU mappings

Scale and constraints, straight from the MoM:

| Item | Value |
|---|---|
| Users | < ~90 DB admins (Region Intan 13, Region Mba Surti 20, others TBC) |
| Concurrency | Low, except monthly closing |
| Identity | Distributor Code (`DSTxxx`) supplied at login |
| Destination | `POOL NOO STREAMLIT` / `POOL SKU STREAMLIT` tabs in the **NOO TRACKER GT** spreadsheet |
| Downstream | BD Support checks the pool daily and moves valid rows into the 3 brand tabs |
| App scope | Login, guideline, template, upload, validation, pool insertion, status |
| Explicitly out of scope | Approval, reconciliation, automatic promotion into the main brand tabs |

The application is an **operational upload tool**, not a dashboard and not an approval system.

### Brands in scope

The tracker has `SKINTIFIC NEW`, `TIMEPHORIA NEW`, `FACERINNA NEW` tabs, and the MoM lists
`11 Skintific / 13 Timephoria / 1A Facerinna`. **G2G (12) and BODIBREZE/NEXTPRIME (17) are
out of scope** for this app even though they exist in the customer-code space (see §11).

---

## 2. User flow

```
┌──────────────────────────────────────────────────────────────┐
│ LOGIN                                                        │
│  Distributor Code (DSTxxx) + password                        │
│  → resolved against gt_schema.master_distributor             │
└───────────────────────────┬──────────────────────────────────┘
                            │  st.session_state["distributor_code"]  (authoritative)
                            │  st.session_state["distributor_name"]  (derived)
                            │  st.session_state["db_suffix"]         (derived)
                            ▼
┌──────────────────────────────────────────────────────────────┐
│ HOME — "Welcome, PT XXX (DST123)"                            │
│  Sidebar: [NOO / Store Mapping] [SKU Mapping]                │
└───────────┬──────────────────────────────┬───────────────────┘
            ▼                              ▼
   ┌────────────────────┐        ┌────────────────────┐
   │ NOO / STORE MAPPING│        │ SKU MAPPING        │
   │  1. Guideline      │        │  1. Guideline      │
   │  2. Download templ.│        │  2. Download templ.│
   │  3. Bulk upload    │        │  3. Bulk upload    │
   │  4. Validation     │        │  4. Validation     │
   │  5. Result / errors│        │  5. Result / errors│
   └─────────┬──────────┘        └─────────┬──────────┘
             ▼                             ▼
   POOL NOO STREAMLIT            POOL SKU STREAMLIT
             └──────────────┬──────────────┘
                            ▼
              BD Support daily check → 3 brand tabs
```

---

## 3. Login flow

### 3.1 Requirement

- The DB admin logs in with their **Distributor Code**.
- That code becomes the session identity for every subsequent action.
- Distributor Code is **never** entered in a template and **never** read from an uploaded file.
- Distributor Name is resolved from master data, never typed.

### 3.2 Mechanism (reuse)

`salesman_pjp.py` in this repo already implements exactly this pattern:

- `load_distributor_data()` — queries `gt_schema.master_distributor` for
  `distributor_code`, `distributor` (name), `region_g2g`, `asm_g2g`, filtered to `status = 'Active'`
- `_render_password_gate(dist_code, dist_name)` — per-distributor password gate
- `_check_distributor_auth(dist_code)` — session-state guard keyed `auth_{dist_code}`

We reuse the shape of this flow, with two corrections:

1. **Passwords move out of source code.** `salesman_pjp.py` hardcodes a
   `DISTRIBUTOR_PASSWORDS` dict of ~90 plaintext `DSTxxx: password` pairs in a file that is
   committed to GitHub. The new app reads credentials from `st.secrets`
   (`[distributor_passwords]` table), which is gitignored. `bcrypt` is already in
   `requirements.txt` if hashing is wanted later.
2. **Login is the entry gate**, not a per-section gate. The user authenticates once; the whole
   app sits behind it.

### 3.3 Login validation rules

| Check | Behaviour on failure |
|---|---|
| Code matches `^DST\d{3}$` (case-insensitive, trimmed) | "Format kode distributor tidak valid. Contoh: DST123." |
| Code exists in `master_distributor` | "Kode distributor tidak dikenali." |
| `status = 'Active'` | "Distributor ini tidak aktif. Hubungi BD Support." |
| Password matches the secrets entry | "Password salah." |
| No secrets entry for the code | "Password belum dikonfigurasi. Hubungi administrator." |

On success the session stores `distributor_code`, `distributor_name`, `region`, `asm`,
`db_suffix` (§11), and `login_at`.

---

## 4. NOO workflow

```
Guideline read → Download template → Fill offline → Upload .xlsx
      ↓
STAGE 1  File-level validation   (readable, correct template, has data)
      ↓
STAGE 2  Column-level validation (required columns present + named correctly)
      ↓
STAGE 3  Row-level validation    (formats, allowed values, mandatory fields)
      ↓
STAGE 4  Duplicate classification against the submission ledger
      ↓
STAGE 5  Enrichment (Date, Distributor Code, Distributor Name, Upload ID)
      ↓
STAGE 6  Append to POOL NOO STREAMLIT
      ↓
Result screen (success summary or downloadable error report)
```

Nothing is written to the pool unless **all** of stages 1–4 pass for the whole file.

---

## 5. SKU workflow

Identical staging, with two extra steps inside STAGE 5:

- **Brand resolution** — look up `SKU Code Principal` in `gt_schema.master_product` and read
  its `brand` column.
- **Customer Code derivation** — `brand_prefix(brand) + db_suffix(distributor_code)` (§11).

A SKU Code Principal that does not exist in `master_product` is a **row-level validation
failure**, not a silent pass: without it, brand — and therefore Customer Code — cannot be derived.

---

## 6. Template structure

Both templates are `.xlsx`, generated in-app with `xlsxwriter`, following the pattern already
proven in `store_channelization.py` (`create_excel_with_dropdown`): a data sheet plus a
`Panduan & Metadata` guideline sheet, header formatting, column widths, and dropdown
validation on constrained columns.

### 6.1 NOO template — sheet `NOO Mapping`

| # | Column header | Mandatory | Format / rule |
|---|---|---|---|
| 1 | `Store ID` | **No** (optional by design) | `IExxNNNNN`, e.g. `IEBB01234`, `IESP00987` |
| 2 | `Store Name` | Yes | Free text, ≤ 150 chars |
| 3 | `Channel` | Yes | Dropdown: `GT` or `MTI` |
| 4 | `Customer Code` | Yes | `11xxx` / `13xxx` / `1Axxx` — must belong to this DB |
| 5 | `Customer Store Code` | Yes | `DST` + 6 chars |
| 6 | `City` | Yes | Free text |
| 7 | `Address` | Yes | Free text |

`Store ID` is optional **on purpose**: the MoM records that BD Support mostly analyses it
themselves, but some DB admins need the column so their SPV can validate the Store ID.
A blank `Store ID` must never fail an otherwise valid file.

### 6.2 SKU template — sheet `SKU Mapping`

| # | Column header | Mandatory | Format / rule |
|---|---|---|---|
| 1 | `SKU Code Principal` | Yes | Must exist in `gt_schema.master_product.sku` |
| 2 | `SKU Name Principal` | Yes | Free text |
| 3 | `Size` | Yes | Product specification, ml/g |
| 4 | `SKU Code DB` | Yes | The DB's own internal SKU code, free text |
| 5 | `SKU Name DB` | Yes | The DB's own internal SKU name, free text |

> The MoM's SKU list ends with "SKU name principal" as item 5. Read in context (items 4 and 5
> are the DB-side pair), this is a typo for **SKU Name DB**. Adopted as `SKU Name DB` here.
> **[NEEDS CONFIRMATION]** — trivial, but worth one sentence from BD Support.

### 6.3 Columns deliberately absent from both templates

| Column | Why | Where it comes from |
|---|---|---|
| `Date` | MoM: "terinput automate by system di pool tracker" | System clock, Asia/Jakarta (§13) |
| `Distributor Code` | MoM: login already binds it; also a security control (§15) | Session |
| `Distributor Name` | MoM: "Nama DB juga akan mengikat ke DSTxxx" | `master_distributor` |
| `Customer Code` (SKU only) | MoM: "akan automate muncul di tracker pool" | Derived (§11) |

Each template also carries an **example row** (visually distinguished, stripped by the parser on
upload) and the full guideline text on the `Panduan & Metadata` sheet, satisfying the MoM's
"ditampilkan di streamlit dan/atau di dalam template juga".

---

## 7. Validation rules

### 7.1 Stage 1 — file level

| Rule | Error message |
|---|---|
| File is `.xlsx` / `.xls` and openable | "File tidak bisa dibaca. Pastikan file .xlsx dan tidak ter-password." |
| Expected sheet present (or first sheet used) | "Sheet tidak ditemukan. Gunakan template resmi." |
| At least 1 data row after the example row | "File tidak berisi data." |
| ≤ 5,000 rows | "File terlalu besar. Maksimal 5.000 baris per upload." **[NEEDS CONFIRMATION]** on the cap |

### 7.2 Stage 2 — wrong-template detection

An explicit MoM requirement: "DB yang salah upload template NOO ke section SKU mapping and vice
versa akan detected as error."

Detection is by **header signature**, evaluated before any other column check:

```
NOO_SIGNATURE = {"store name", "channel", "customer store code"}
SKU_SIGNATURE = {"sku code principal", "sku code db"}
```

Headers are normalised (lowercase, collapse whitespace, strip punctuation) before matching.

| Situation | Message |
|---|---|
| SKU signature found in the NOO section | "❌ Ini template **SKU Mapping**, bukan NOO Mapping. Silakan upload di section SKU Mapping." |
| NOO signature found in the SKU section | "❌ Ini template **NOO Mapping**, bukan SKU Mapping. Silakan upload di section NOO / Store Mapping." |
| Neither signature recognised | "❌ Template tidak dikenali. Silakan download template terbaru." |

### 7.3 Stage 2b — column checks

- All required headers present → otherwise list the missing ones by name.
- Unknown extra columns → **warning**, not an error; dropped before insertion.
- A `Distributor Code` / `Distributor Name` / `Date` column present in the file → **warning**,
  and the values are **ignored** (§15).

### 7.4 Stage 3 — row rules

**NOO**

| Column | Rule | Example message |
|---|---|---|
| `Store ID` | Optional. If present, must match `^IE[A-Z]{2}\d{3,6}$` | `Baris 12: Store ID "IEBB1" formatnya tidak valid (contoh: IEBB01234).` |
| `Store Name` | Required, non-blank | `Baris 17: Store Name wajib diisi.` |
| `Channel` | Required, ∈ {`GT`, `MTI`} (case-insensitive) | `Baris 23: Channel harus GT atau MTI.` |
| `Customer Code` | Required; prefix ∈ {`11`,`13`,`1A`}; suffix must equal this DB's suffix | `Baris 8: Customer Code "11ABC" bukan milik DST082 (yang valid: 11CEC, 13CEC, 1ACEC).` |
| `Customer Store Code` | Required, non-blank, `^DST` prefix | `Baris 31: Customer Store Code wajib diisi.` |
| `City` | Required, non-blank | `Baris 5: City wajib diisi.` |
| `Address` | Required, non-blank | `Baris 5: Address wajib diisi.` |
| whole row | Not entirely blank | blank rows skipped silently |

**SKU**

| Column | Rule | Example message |
|---|---|---|
| `SKU Code Principal` | Required; must exist in `master_product.sku` | `Baris 4: SKU Code Principal "SKINTIFIC-999" tidak ditemukan di master product.` |
| | Brand must be Skintific / Timephoria / Facerinna | `Baris 9: SKU "G2G-74" milik brand G2G, di luar cakupan mapping ini.` |
| `SKU Name Principal` | Required | `Baris 6: SKU Name Principal wajib diisi.` |
| `Size` | Required | `Baris 6: Size wajib diisi.` |
| `SKU Code DB` | Required, non-blank | `Baris 31: SKU Code DB wajib diisi.` |
| `SKU Name DB` | Required, non-blank | `Baris 31: SKU Name DB wajib diisi.` |

Optional soft check: warn when `SKU Name Principal` disagrees with `master_product.product_name`,
or `Size` disagrees with `master_product.pack_size`. Warning only — the principal master is the
reference, but a DB's cosmetic typo should not block a submission. **[NEEDS CONFIRMATION]**
whether BD Support wants this as a hard error instead.

---

## 8. Duplicate detection

### 8.1 The requirement, precisely

MoM: *"Jika admin DB mengikutsertakan toko / produk yang sebelumnya sudah pernah diupload …
atau dengan kata lain 100% semua, streamlit akan mendeteksi dan memberikan notice bahwa
sebelumnya sudah pernah diupload (file failed to upload)."*

MoM: *"Jika terkait human error admin DB (contoh: typo di reference ID) dan admin DB ingin
mengunggah ulang, akan tetap terinput di tracker."*

So there are two distinct comparisons:

- **A — Exact previous submission** → reject.
- **B — Same store/SKU, changed content** (a correction) → accept; BD Support picks the right one.

### 8.2 The comparison keys

Every comparison is **scoped to the logged-in Distributor Code**. One DB's submissions can never
collide with another's.

**NOO**

| Key | Definition | Purpose |
|---|---|---|
| Identity key | `distributor_code` + `UPPER(TRIM(customer_store_code))` | "Is this the same store?" |
| Content hash | SHA-256 of the normalised 7 template fields + `distributor_code` | "Is this identical to a previous submission?" |

**SKU**

| Key | Definition | Purpose |
|---|---|---|
| Identity key | `distributor_code` + `UPPER(TRIM(sku_code_principal))` + `UPPER(TRIM(sku_code_db))` | "Is this the same mapping?" |
| Content hash | SHA-256 of the normalised 5 template fields + `distributor_code` | Exact-duplicate test |

Normalisation before hashing: cast to string, `strip()`, collapse internal whitespace, uppercase,
treat `NaN` / `None` / `""` identically.

> **[NEEDS CONFIRMATION] — NOO identity key.** `Customer Store Code` is chosen because it is the
> DB's own mandatory store reference and `Store ID` is optional (so it cannot be the key).
> This assumes `Customer Store Code` is unique per store *within* one DB. In
> `gt_schema.master_store_database_basis`, the equivalent field (`reference_id_skt`) is **not**
> clean — 544 rows are literally `Unmapped`, and a bare `DST267` appears 538 times. If DB admins
> reuse or leave placeholder codes, this key degrades. Confirm with BD Support, or fall back to
> the composite `store_name + address + city` using the fuzzy scorer already written in
> `noo_detector.py`.

> **[NEEDS CONFIRMATION] — SKU identity key.** Is a DB allowed to map one `SKU Code Principal`
> to more than one `SKU Code DB` (and vice versa)? If the relationship is strictly 1:1, the
> identity key should be `distributor_code + sku_code_principal` alone, and a second
> `SKU Code DB` for the same principal SKU would be a **correction**, not a new row. As specified
> above (both codes in the key), a re-mapping reads as a brand-new row instead.

### 8.3 The ledger problem — CRITICAL

The MoM says BD Support "pindahkan ke main tracker" — **moves** rows out of the pool. If rows
leave `POOL NOO STREAMLIT`, then comparing an upload against the pool's current contents silently
loses all history, and duplicate detection stops working within one day of go-live.

Duplicate detection must therefore run against an **append-only submission ledger**, not against
the working pool.

**Recommended (primary):** keep the pool itself append-only. Add a `BD Status` column
(`NEW` / `PROCESSED` / `REJECTED`) that BD Support sets instead of deleting rows. The pool *is*
the ledger; no new infrastructure; BD Support keeps working in the same tab and filters on
`BD Status = NEW` for their daily check.

**Fallback:** if BD Support insists on clearing the pool, mirror every accepted row into BigQuery
`gt_schema.noo_sku_submission_log` (append-only, partitioned on submission date) and run
duplicate detection there. The repo already has the `insert_rows_json` pattern in eight apps, so
this is cheap — but it is a second store to keep in sync.

> **[NEEDS CONFIRMATION]** — which of the two. This decision changes the pool tab's column
> layout, so it must be settled **before** the pool headers are frozen.

### 8.4 Classification output

Each row lands in exactly one bucket:

| Bucket | Condition | Action |
|---|---|---|
| `NEW` | Identity key not seen before for this DB | Insert |
| `CORRECTION` | Identity key seen, content hash differs | Insert (MoM re-upload rule) |
| `EXACT_DUPLICATE` | Content hash already present for this DB | Do not insert |
| `DUPLICATE_IN_FILE` | Identity key appears twice inside the uploaded file itself | Do not insert |

`DUPLICATE_IN_FILE` also covers the MoM's multi-brand guidance: a store that exists under more
than one brand is entered **once**, so two rows for the same store inside one file are a user
error and are surfaced as such.

---

## 9. Re-upload behaviour

| File composition | Behaviour |
|---|---|
| 100% `EXACT_DUPLICATE` | **Reject the file.** "File ini berisi data yang sudah pernah diupload sebelumnya." Matches the MoM's "file failed to upload" verbatim. |
| 100% `NEW` / `CORRECTION` | Accept, insert all rows. |
| Mixed | See below — **[NEEDS CONFIRMATION]** |

### Partial duplicates — proposed safest behaviour

The MoM does not cover this case. The proposal, chosen to satisfy "do not silently insert
duplicate data" without forcing a full re-edit for one stale row:

1. **Block the automatic insert.** Nothing is written on the first click.
2. Show a per-row breakdown: `NEW = 42`, `CORRECTION = 3`, `EXACT_DUPLICATE = 5`.
3. Offer **two explicit choices**:
   - `Upload 45 baris baru saja (5 duplikat dilewati)` — inserts only `NEW` + `CORRECTION`
   - `Batalkan dan perbaiki file` — download the annotated error report, fix, re-upload
4. Whichever is chosen, log the decision with the upload ID.

This is never silent, never partial-by-accident, and never forces the admin to hand-remove rows
the system already identified. The strict alternative — reject any file containing a single
duplicate — is also defensible and is a one-line switch if BD Support prefers it.

---

## 10. Distributor master mapping

**Source of truth:** `skintific-data-warehouse.gt_schema.master_distributor`
(331 rows, 215 with `status = 'Active'`, partitioned on `join_date`).

Relevant columns:

| Column | Use |
|---|---|
| `distributor_code` | Login identity — `DSTxxx` |
| `distributor` | Distributor Name written to the pool |
| `distributor_company` | Legal entity name (alternative display) |
| `region`, `region_g2g` | Context / audit |
| `asm`, `asm_skt`, `asm_tph`, `asm_fr` | Context; per-brand ASM columns exist |
| `status` | Login gate — only `Active` may log in |

**Finding: `master_distributor` has no abbreviation column.** The full 38-column schema was
inspected; there is no `abbreviation`, `singkatan`, `initial`, or `db_code` field. The 3–4 letter
DB abbreviation the MoM refers to ("singkatan huruf DB", the `CEC` in `11CEC`) does **not** exist
in the distributor master. It is recovered from PO history instead — see §11.

The MoM says "Mas Irwan akan mengacu ke distributor database", and the tracker screenshot shows a
`DIST DATABASE` tab. That tab is the intended reference for the abbreviation and could not be
inspected — see §20-O1.

---

## 11. Customer Code automation

### 11.1 The rule, decoded

MoM: *"customer code (11/13/1AKAS) akan automate muncul di tracker pool ngiket dari SKU code
principal dan 3 huruf di belakangnya ngiket ke kode DB"*.

```
Customer Code  =  brand_prefix( brand_of( SKU Code Principal ) )  ||  db_suffix( Distributor Code )
                  └──────────── 2 chars ────────────┘                └── 3–4 chars ──┘
```

This was verified empirically against `skintific-data-warehouse.dms.gt_po_tracking_all_mv`, which
carries `customer_code`, `distributor_code`, `distributor_name` and `brand` on the same row — the
authoritative join.

### 11.2 Brand prefix table (verified, not assumed)

| Prefix | Brand | Distinct customer codes | In scope? |
|---|---|---|---|
| `11` | SKINTIFIC | 36 | ✅ |
| `12` | G2G | 70 | ❌ out of scope |
| `13` | TIMEPHORIA | 30 | ✅ |
| `17` | NEXTPRIME **and** BODIBREZE | 53 + 35 | ❌ out of scope |
| `1A` | FACERINNA | 28 | ✅ |

The three in-scope prefixes match the MoM's note exactly. Note that `17` maps to **two** brands —
it is not a clean 1:1 prefix space, which is one more reason to restrict this app to the three
tracker brands.

### 11.3 Brand resolution from SKU Code Principal — lookup, not regex

SKU code formats are **not** uniform per brand:

| Brand | Pattern | Examples |
|---|---|---|
| SKINTIFIC | `SKINTIFIC-nn` (+ bundle forms `SKT-`, `RMD-`, `SX-`) | `SKINTIFIC-01`, `SKT-65+77`, `RMD-G-05+07+12+31+48+77` |
| TIMEPHORIA | `Txxnnnnnn`, no dash | `TCC102001` |
| FACERINNA | `Fnnn` | `F116`, `F121` |
| G2G | `G2G-nn` | `G2G-01` |
| BODIBREZE | `BSRnnnnnn` | `BSR111001` |
| NEXTPRIME | `Nxxnnn` | `NGZ128`, `NHA115` |

Because of the bundle SKUs (`SKT-`, `RMD-`, `SX-` are all SKINTIFIC), **do not regex the SKU code
to guess the brand**. Look the SKU up in `gt_schema.master_product` and read the `brand` column.
That table also supplies `product_name` and `pack_size` for the soft cross-checks in §7.4.

### 11.4 DB suffix resolution

The suffix is derived from PO history:

```sql
SELECT DISTINCT distributor_code, SUBSTR(customer_code, 3) AS db_suffix
FROM `skintific-data-warehouse.dms.gt_po_tracking_all_mv`
WHERE customer_code IS NOT NULL AND customer_code != ''
  AND distributor_code IS NOT NULL
```

Verified examples: `DST082 CV CECE → CEC` (giving `11CEC`, matching the MoM's example verbatim),
`DST026 CV MITRA PEMENANG → MTP`, `DST081 PT KURNIA MAJU PERKASA → KMP`.

**Coverage and caveats — all four matter:**

| Finding | Number | Consequence |
|---|---|---|
| Active distributors with a resolvable suffix | 212 / 215 | Good |
| Active distributors with **no** suffix (no PO history) | **3** | Cannot auto-derive Customer Code. App must fail loudly, not guess. |
| Distributors with **more than one** suffix | **14** | Ambiguous — needs a tie-break rule |
| Suffixes shared by **multiple** distributor codes | **35** | Customer Code is many-to-one: it does **not** uniquely identify a DST |

The 14 ambiguous distributors are mostly a territory split in progress, e.g.
`DST338 PT BINTANG SINAR JAYA - BATULICIN → {BSJ, LIJ}`, `DST152 CV DIMAS MADIUN → {DIM, DIMM}`,
`DST274 PT YAFINDO MITRA PERMATA - PADANG → {YMP, YMPP}`. The old parent code (`LIJ`) and the new
branch code coexist in history.

The many-to-one finding is structural: `DST111/112/113` (Catur Sentosa Anugerah Bandar Lampung /
Bangka / Belitung) all share `CSA`, and `DST117/118/119/120` (Surya Donasin Bandung / Tasikmalaya
/ Cirebon / Serang) all share `SDS`. **Customer Code alone cannot be reversed back to a
Distributor Code**, so the pool must carry `Distributor Code` as its own column — it is not
recoverable from Customer Code.

> **[NEEDS CONFIRMATION] — O2.** Tie-break for the 14 ambiguous distributors. Proposal: take the
> suffix from the most recent PO (`MAX(order_date)`), and surface the resolved value on the login
> screen so the admin sees which Customer Code will be written. Do not silently pick one.

> **[NEEDS CONFIRMATION] — O3.** The 3 active distributors with no suffix. They cannot upload SKU
> mappings until BD Support supplies a suffix. Proposal: seed a small override table in
> `st.secrets` or a `DIST DATABASE` column, and block the SKU section for them with an explicit
> message rather than writing a blank or wrong Customer Code.

### 11.5 Recommended reference object

Rather than querying PO history live on every upload, build a small cached reference resolved at
login and cached with `@st.cache_data(ttl=3600)`:

```
dim_distributor_customer_code:
    distributor_code   (PK)
    distributor_name   ← master_distributor.distributor
    db_suffix          ← latest from gt_po_tracking_all_mv, else override
    customer_code_skt  = '11' + db_suffix
    customer_code_tph  = '13' + db_suffix
    customer_code_fr   = '1A' + db_suffix
    source             = 'PO_HISTORY' | 'OVERRIDE' | 'DIST_DATABASE'
```

Once access to the `DIST DATABASE` tab is granted, reconcile this against it and switch `source`
to the sheet where they disagree — the sheet is BD Support's stated reference.

### 11.6 Customer Code in the NOO template

Note the asymmetry: for **SKU** the Customer Code is derived by the system; for **NOO** it is a
column the DB admin fills in (the MoM lists it as NOO template field 4), because the NOO row's
brand is a business decision the admin makes, not something derivable from the row.

The app still uses the same mapping — as a **validator**. A `Customer Code` on a NOO row is
accepted only if it equals one of this DB's three valid values (`11{suffix}`, `13{suffix}`,
`1A{suffix}`). This catches typos and cross-DB paste errors for free, and gives the guideline a
concrete list to show the admin.

---

## 12. Pool tracker mapping

**Spreadsheet:** `NOO TRACKER GT` (Google Sheets)
**Tabs visible in the MoM screenshot:** `SKINTIFIC NEW`, `TIMEPHORIA NEW`, `FACERINNA NEW`,
**`POOL NOO STREAMLIT`**, `SKU MAPPING`, **`POOL SKU STREAMLIT`**, `DIST DATABASE`, `ASM/SPV/SE`,
plus further tabs off-screen.

Both pool tabs **already exist** and appear **empty** in the screenshot (cursor on `A1`, no data,
no headers). So the app does not create them — but their headers still need defining, which is an
open item (§20-O1).

### 12.1 Proposed `POOL NOO STREAMLIT` layout

| Col | Header | Source |
|---|---|---|
| A | `Date` | System, Asia/Jakarta (§13) |
| B | `Distributor Code` | Session |
| C | `Distributor Name` | `master_distributor.distributor` |
| D | `Store ID` | Template (may be blank) |
| E | `Store Name` | Template |
| F | `Channel` | Template |
| G | `Customer Code` | Template (validated) |
| H | `Customer Store Code` | Template |
| I | `City` | Template |
| J | `Address` | Template |
| K | `Upload ID` | System — UUID per file |
| L | `Row Type` | `NEW` / `CORRECTION` |
| M | `BD Status` | Default `NEW`, maintained by BD Support (§8.3) |

### 12.2 Proposed `POOL SKU STREAMLIT` layout

| Col | Header | Source |
|---|---|---|
| A | `Date` | System, Asia/Jakarta |
| B | `Distributor Code` | Session |
| C | `Distributor Name` | `master_distributor.distributor` |
| D | `Customer Code` | **Derived** (§11) |
| E | `SKU Code Principal` | Template |
| F | `SKU Name Principal` | Template |
| G | `Size` | Template |
| H | `SKU Code DB` | Template |
| I | `SKU Name DB` | Template |
| J | `Upload ID` | System — UUID per file |
| K | `Row Type` | `NEW` / `CORRECTION` |
| L | `BD Status` | Default `NEW` |

> **[NEEDS CONFIRMATION] — O1.** These layouts are proposals. If BD Support has already designed
> the pool headers — or has a convention in the `SKINTIFIC NEW` / `SKU MAPPING` tabs that the pool
> should mirror so rows can be copy-pasted straight across — the app must match theirs exactly,
> **column order included**, since BD Support copies ranges by position. Confirming this is the
> single highest-value unblock: it determines both the writer and the ledger design.

### 12.3 Access — a hard blocker

Neither service account available in this environment can see the spreadsheet:

| Service account | Drive search for "NOO TRACKER" |
|---|---|
| `readonly@skintific-data-warehouse.iam.gserviceaccount.com` | 0 results |
| `sfa-web-api@skintific-data-warehouse.iam.gserviceaccount.com` | 0 results |

The tracker's actual tab headers, `DIST DATABASE` contents, and existing row formats therefore
**could not be inspected**. Everything in §12.1 / §12.2 is a proposal built from the MoM text and
the screenshot, not from the live sheet.

A repo-wide search for `NOO TRACKER` / `POOL NOO` / `POOL SKU` across `D:\GitHub`, the Skintific
knowledge base, and `D:\Claude` returned **no pre-existing reference**, and neither of the two
spreadsheet IDs already present in this repo is the tracker. So the spreadsheet ID is not recorded
anywhere locally — there is nothing to look up, and it must be supplied by BD Support.

**Required action before implementation:** obtain the `NOO TRACKER GT` spreadsheet ID, share the
sheet with the app's service account as **Editor** (the app must append), and record the ID in
`st.secrets`.

### 12.4 Write mechanism

The repo has **no existing Google Sheets write path** — `smart_coverage.py` is the only app that
touches Sheets and it only *reads* (`gc.open_by_key(...).worksheet(...).get_all_records()`); all
eight write paths in the repo go to BigQuery via `insert_rows_json`. So the sheet writer is
genuinely new code.

Proposed: `gspread` (already in `requirements.txt`)

```python
ws.append_rows(rows, value_input_option="RAW", insert_data_option="INSERT_ROWS")
```

`value_input_option="RAW"` is deliberate — it stops Sheets from reinterpreting store codes and SKU
codes as numbers, dates, or formulas. One `append_rows` call per upload keeps the whole file a
single API round-trip and a single atomic server-side append, which is what makes the concurrency
story acceptable at monthly-closing peak (§14).

Auth reuses the pattern in `smart_coverage.py`: `gspread.service_account_from_dict(gcp_secrets)`
with a fallback to `credentials.with_scopes([spreadsheets, drive])`.

---

## 13. Automatic date

- Timezone: **Asia/Jakarta (WIB, UTC+7)** — never UTC. The business operates in Indonesia, and a
  UTC timestamp would date monthly-closing evening uploads to the wrong day.
- Precedent in the repo: `smart_coverage.py` already does `pendulum.timezone("Asia/Jakarta")`;
  `pendulum` and `pytz` are both in `requirements.txt`.
- Written value: date only (`YYYY-MM-DD`) in the pool's `Date` column, matching the MoM's "Date".
- The full timestamp (`YYYY-MM-DD HH:MM:SS+07:00`) is shown on the result screen and kept in the
  audit fields, so a same-day re-upload is distinguishable.
- The timestamp is taken **once per upload**, not per row, so every row in one file shares a date.

---

## 14. Data architecture

```
┌─────────────────────────── READ ────────────────────────────┐
│ BigQuery  gt_schema.master_distributor                      │  login, DB name, status
│ BigQuery  gt_schema.master_product                          │  SKU validity, brand, pack_size
│ BigQuery  dms.gt_po_tracking_all_mv                         │  db_suffix → Customer Code
│ Sheets    NOO TRACKER GT ▸ DIST DATABASE                    │  suffix reconciliation [blocked]
│ Sheets    NOO TRACKER GT ▸ POOL * STREAMLIT                 │  duplicate ledger
└─────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────▼─────────┐
                    │ Streamlit app     │  st.cache_data(ttl=3600) on all master reads
                    │ (Community Cloud) │  session_state holds the identity
                    └─────────┬─────────┘
                              │
┌─────────────────────────── WRITE ───────────────────────────┐
│ Sheets    NOO TRACKER GT ▸ POOL NOO STREAMLIT   (append)    │
│ Sheets    NOO TRACKER GT ▸ POOL SKU STREAMLIT   (append)    │
└─────────────────────────────────────────────────────────────┘
```

**Caching.** Master data is read once per hour per session (`@st.cache_data(ttl=3600)`), the
pattern already used across the repo. The **pool** is read fresh on every upload attempt — it is
the duplicate ledger and must never be stale.

**Concurrency.** At <90 users with low concurrency, no locking is designed in. The one real window
is monthly closing. `append_rows` is a single server-side append, so two simultaneous uploads
interleave as whole blocks rather than corrupting each other. What is *not* protected is the
read-then-append gap: two admins from the **same** DB uploading the same file within seconds could
both pass the duplicate check. This is judged acceptable — same-DB simultaneous upload of an
identical file is a rare operator error, and BD Support reviews the pool daily anyway. Mitigation
if it ever bites: re-read the last N pool rows immediately before appending.

**No new BigQuery table** is proposed unless the §8.3 fallback is chosen.

---

## 15. Security

| Threat | Control |
|---|---|
| Admin edits the file to submit as another DB | Distributor Code is **never** read from the uploaded file. It comes only from `st.session_state`, set at login. A `Distributor Code` column in the file is dropped with a warning. |
| Admin guesses another DB's code | Code alone is insufficient — a per-distributor password is required. |
| Session fixation / stale identity | The enrichment step re-reads `st.session_state["distributor_code"]` at write time, not a value passed through the form. |
| Credentials in source control | Passwords live in `st.secrets`, which `.gitignore` already excludes. **This is a change from `salesman_pjp.py`, which hardcodes ~90 plaintext `DSTxxx: password` pairs in a committed file — that file should be remediated separately.** |
| Service-account key exposure | Reuse the existing `[connections.bigquery]` secrets block; no key file paths in source. Several existing apps hardcode a local fallback such as `C:\script\...json`; the new app should not copy that fallback. |
| Cross-DB data leakage | Every pool read for duplicate detection is filtered to the session's `distributor_code` before display. An admin never sees another DB's rows. |
| Formula injection into Sheets | `value_input_option="RAW"` prevents a cell beginning with `=` from being evaluated. |

**Explicit invariant to test:** logged in as `DST001`, uploading a file whose rows contain
`DST002`, the pool must receive `DST001` on every row.

---

## 16. Error handling

**No raw Python exception ever reaches the user.** Every handler wraps its work and maps failures
to plain-language messages; the traceback goes to the server log only.

### Success screen

```
✅ Upload berhasil

Jenis upload      : NOO / Store Mapping
Distributor       : PT CECE MANDIRI SEJAHTERA (DST082)
Jumlah baris      : 45 baris masuk (42 baru, 3 koreksi)
Waktu upload      : 18 Agustus 2026, 14:32:07 WIB
Tujuan            : POOL NOO STREAMLIT
Upload ID         : 8f3c1e9a
```

### Failure screen

```
❌ Upload gagal — 4 baris bermasalah, tidak ada data yang masuk

Baris 17 : Customer Store Code wajib diisi.
Baris 23 : Channel harus GT atau MTI (ditemukan: "Gt Modern").
Baris 31 : SKU Code DB wajib diisi.
Baris 44 : Customer Code "11ABC" bukan milik DST082 (yang valid: 11CEC, 13CEC, 1ACEC).

[ ⬇ Download laporan error (.xlsx) ]
```

The downloadable error report is the **original file plus two appended columns** — `Status` and
`Keterangan Error` — so the admin fixes errors in place and re-uploads the same file rather than
cross-referencing row numbers by hand.

Row numbers shown are **spreadsheet row numbers as the admin sees them in Excel** (header = row 1,
example row accounted for), not zero-based DataFrame indices.

---

## 17. UI design

### Login

```
┌──────────────────────────────────────────────┐
│              [ Skintific logo ]              │
│         NOO & SKU Mapping Portal             │
│                                              │
│   Kode Distributor  [ DST___          ]      │
│   Password          [ ••••••••        ]      │
│                                              │
│              [    🔓 Masuk    ]              │
└──────────────────────────────────────────────┘
```

### Home (after login)

```
Selamat datang, PT CECE MANDIRI SEJAHTERA
Kode Distributor: DST082   ·   Region: Northern Sulawesi        [ Keluar ]
────────────────────────────────────────────────────────────────────────

  SIDEBAR                        MAIN
  ┌────────────────────┐         ┌──────────────────────────────────┐
  │ ▸ NOO / Store      │         │  📘 Guideline           [expand] │
  │   Mapping          │         │  ────────────────────────────────│
  │ ▸ SKU Mapping      │         │  ⬇ Download Template NOO         │
  │                    │         │  ────────────────────────────────│
  │ ─────────────      │         │  ⬆ Upload File                   │
  │ DST082             │         │     [ Drag & drop .xlsx ]        │
  │ PT CECE MANDIRI…   │         │                                  │
  │ [ Keluar ]         │         │     [ Validasi & Upload ]        │
  └────────────────────┘         └──────────────────────────────────┘
```

Design rules, per the MoM's "keep it simple and operational":

- Two sidebar entries only. No dashboard, no charts, no metric tiles.
- Guideline in an `st.expander`, open by default on first visit.
- One primary button per screen.
- All user-facing copy in **Bahasa Indonesia** — the users are DB admins, and every existing
  DB-facing app in this repo (`salesman_pjp.py`, `store_channelization.py`) is already in
  Indonesian. Column headers stay in English to match the tracker.
- Reuse `.streamlit/config.toml` theme (`primaryColor = "#E6656F"`) and `assets/skintific_logo.png`.

### Guideline content

**NOO guideline** — required vs optional fields; `Store ID` is optional and why; **a store that
exists under more than one brand is entered once**; use the current template only; Channel is `GT`
or `MTI`; this DB's three valid Customer Codes listed explicitly; what happens after upload (BD
Support checks daily); common errors.

**SKU guideline** — required fields; SKU Code Principal must exist in the principal master; DB
Code is taken from login and must not be typed; Customer Code is generated automatically and how
(`11`/`13`/`1A` + this DB's suffix, with this DB's actual values shown); brand mapping table; what
happens after upload; common errors.

---

## 18. Testing strategy

Existing convention: `pytest.ini` with `testpaths = tests`, a `sanity` marker for a fast
pre-deploy subset, and `run_all_tests.py` as the single entry point.
`tests/test_assessment_logic.py` is the model — pure functions tested with **no BigQuery
credentials and no Streamlit secrets**.

The new logic is therefore split so that everything testable is a pure function in `noo_sku/`, and
only the thin I/O shell touches the network.

| # | Area | Test | Marker |
|---|---|---|---|
| 1 | Login | Valid Distributor Code resolves | sanity |
| 2 | Login | Invalid / unknown code rejected with a clear message | sanity |
| 3 | Login | `Inactive` distributor rejected | |
| 4 | Login | Distributor Name auto-resolved from the code | sanity |
| 5 | Login | Distributor Code cannot be overridden by file content | **sanity** |
| 6 | NOO | Template downloads with exactly the 7 headers, no Date/DST columns | sanity |
| 7 | NOO | Valid file passes and produces the right pool row shape | sanity |
| 8 | NOO | Missing required column → named in the error | sanity |
| 9 | NOO | SKU template uploaded here → wrong-template error | **sanity** |
| 10 | NOO | Blank `Store ID` passes (optional) | **sanity** |
| 11 | NOO | Blank `Customer Store Code` fails with a row number | |
| 12 | NOO | `Channel` outside {GT, MTI} fails | |
| 13 | NOO | Customer Code from another DB fails | |
| 14 | NOO | Same store twice inside one file → `DUPLICATE_IN_FILE` | |
| 15 | NOO | Previously uploaded exact record → `EXACT_DUPLICATE`, file rejected | **sanity** |
| 16 | NOO | Corrected record (same identity, changed content) → `CORRECTION`, accepted | **sanity** |
| 17 | NOO | Date auto-generated in Asia/Jakarta, one value for the whole file | sanity |
| 18 | NOO | Accepted rows map to `POOL NOO STREAMLIT` columns in order | |
| 19 | SKU | Template downloads with exactly the 5 headers | sanity |
| 20 | SKU | Valid file passes | sanity |
| 21 | SKU | NOO template uploaded here → wrong-template error | **sanity** |
| 22 | SKU | Missing required column → named in the error | |
| 23 | SKU | Unknown SKU Code Principal fails | sanity |
| 24 | SKU | Out-of-scope brand (G2G / BODIBREZE) fails | |
| 25 | SKU | Duplicate mapping inside one file → `DUPLICATE_IN_FILE` | |
| 26 | SKU | Previously uploaded exact record → file rejected | **sanity** |
| 27 | SKU | Corrected record accepted | |
| 28 | SKU | Date auto-generated | sanity |
| 29 | SKU | DB Code derived from login, not from file | **sanity** |
| 30 | SKU | Customer Code = brand prefix + DB suffix — table-driven over all 3 brands | **sanity** |
| 31 | SKU | Distributor with no suffix → explicit block, never a blank/wrong code | |
| 32 | SKU | Distributor with ambiguous suffix → documented tie-break applied | |
| 33 | SKU | Distributor Name auto-resolved | sanity |
| 34 | SKU | Accepted rows map to `POOL SKU STREAMLIT` columns in order | |
| 35 | Cross | 100% duplicate file rejected, **zero** rows written | **sanity** |
| 36 | Cross | Mixed file writes nothing before explicit confirmation | **sanity** |
| 37 | Cross | Hash normalisation: trailing space / case / `NaN` vs `""` collapse identically | sanity |
| 38 | Cross | Error report is the original file + `Status` + `Keterangan Error` | |
| 39 | Cross | A corrupt / password-protected file yields a message, not a traceback | |

Fixtures: six small `.xlsx` files under `tests/fixtures/` — `noo_valid`, `noo_missing_column`,
`noo_wrong_template`, `sku_valid`, `sku_unknown_sku`, `mixed_duplicates`.

**Not covered, deliberately:** live integration against the real spreadsheet and BigQuery. That
needs a designated test distributor code and a scratch copy of the tracker — see the roadmap.

---

## 19. Implementation roadmap

| Phase | Work | Depends on | Est. |
|---|---|---|---|
| **0** | **Unblock** — share `NOO TRACKER GT` with the service account; confirm the two pool layouts; answer O1–O10 | BD Support | — |
| 1 | `noo_sku/` package skeleton + `dim_distributor_customer_code` builder + unit tests for the Customer Code rule | Phase 0 (O2, O3) | 0.5 d |
| 2 | Template generators (`.xlsx` + `Panduan & Metadata` sheet) for NOO and SKU | — | 0.5 d |
| 3 | Validators: file / wrong-template / column / row, with the Indonesian message catalogue | Phase 2 | 1 d |
| 4 | Duplicate engine: normalisation, hashing, 4-way classification, pool reader | Phase 0 (O1, §8.3) | 1 d |
| 5 | Sheets writer (`append_rows`, RAW) + enrichment + audit fields | Phase 0 | 0.5 d |
| 6 | Streamlit UI: login gate, two sections, guidelines, result screens, error-report download | Phases 1–5 | 1 d |
| 7 | Test suite to the §18 table; `run_all_tests.py --sanity` green | Phases 1–6 | 1 d |
| 8 | UAT on a **copy** of the tracker with 2–3 pilot DB admins | Phase 7 | 2 d |
| 9 | Guideline handover + BD Support briefing on the `BD Status` column | Phase 8 | 0.5 d |
| 10 | Production deploy — **after explicit approval** | Phase 9 | — |

Phases 2 and 3 can run in parallel with 1 and 4. Phase 0 blocks everything that touches the sheet.

---

## 20. Open questions

| # | Question | Why it blocks | Proposed default |
|---|---|---|---|
| **O1** | What are the exact headers and column order of `POOL NOO STREAMLIT` and `POOL SKU STREAMLIT`? Should they mirror `SKINTIFIC NEW` / `SKU MAPPING` so BD Support can copy ranges by position? | Determines the writer, the ledger, and the duplicate keys. Highest-value unblock. Also requires the sheet to be shared with the service account. | §12.1 / §12.2 layouts |
| **O2** | Tie-break for the 14 distributors with more than one historical `db_suffix` (e.g. `DST338 → {BSJ, LIJ}`) | Wrong Customer Code silently written into the pool | Most recent PO wins; show the resolved code at login |
| **O3** | The 3 active distributors with **no** PO history and therefore no derivable suffix — who supplies theirs? | They cannot use the SKU section at all | Block the SKU section for them with an explicit message; accept an override list |
| **O4** | Is the NOO identity key `Customer Store Code`? Is it guaranteed unique per store within a DB? | Determines whether duplicate detection works at all | Yes, with the `noo_detector.py` fuzzy scorer as fallback |
| **O5** | Can one DB map one `SKU Code Principal` to multiple `SKU Code DB`? | Changes the SKU identity key and what counts as a correction vs a new row | Yes (both codes in the key) |
| **O6** | Partial-duplicate behaviour — the MoM covers only the 100% case | Determines whether a mixed file is rejected or partially accepted | Block, show the breakdown, require explicit confirmation (§9) |
| **O7** | Does BD Support agree to mark rows `PROCESSED` instead of deleting them from the pool? | If rows are deleted, duplicate detection loses its history within a day (§8.3) | Yes — otherwise fall back to a BigQuery ledger |
| **O8** | Is MoM SKU field 5 ("SKU name principal") a typo for `SKU Name DB`? | Template header wording | Yes, `SKU Name DB` |
| **O9** | Max rows per upload | File-level guard | 5,000 |
| **O10** | Should a `SKU Name Principal` / `Size` mismatch against `master_product` be a warning or a hard error? | Strictness of SKU validation | Warning |

---

## 21. Files to create / modify

The repository is a **flat multi-app Streamlit Community Cloud repo** — each root-level `.py` is a
separately deployed app (`noo_detector.py`, `po_simulator_v2.py`, `salesman_pjp.py`, …). The
brief's suggested `app.py` + `pages/` layout would collide with that convention, since a root
`pages/` directory attaches itself to every app in the repo. So: **one root entry point** following
the existing naming, plus a small importable package for the logic that needs tests.

### New

| Path | Purpose |
|---|---|
| `noo_sku_mapping.py` | Streamlit entry point — login gate, two sections, all UI |
| `noo_sku/__init__.py` | Package marker |
| `noo_sku/config.py` | Constants: brand prefixes, channel values, headers, regexes, limits |
| `noo_sku/distributor_service.py` | `master_distributor` loader, login validation, `dim_distributor_customer_code` builder |
| `noo_sku/product_service.py` | `master_product` loader, SKU existence + brand resolution |
| `noo_sku/customer_code.py` | The §11 rule as one pure function |
| `noo_sku/noo_validator.py` | NOO file / template / column / row validation |
| `noo_sku/sku_validator.py` | SKU file / template / column / row validation |
| `noo_sku/duplicate_engine.py` | Normalisation, content hashing, 4-way classification |
| `noo_sku/tracker_service.py` | gspread reader/writer for both pool tabs |
| `noo_sku/templates.py` | xlsxwriter template builders (+ `Panduan & Metadata` sheet) |
| `noo_sku/messages.py` | Indonesian message catalogue — single place for all user-facing copy |
| `tests/test_customer_code.py` | §18 items 30–32 |
| `tests/test_noo_validator.py` | §18 items 6–18 |
| `tests/test_sku_validator.py` | §18 items 19–34 |
| `tests/test_duplicate_engine.py` | §18 items 14–16, 25–27, 35–37 |
| `tests/fixtures/*.xlsx` | Six upload fixtures |
| `docs/streamlit_noo_sku_mapping_design.md` | This document |

### Modified

| Path | Change |
|---|---|
| `.streamlit/secrets.toml` (local, gitignored) | Add `[noo_tracker] spreadsheet_id`, `pool_noo_tab`, `pool_sku_tab`, `dist_database_tab`; add `[distributor_passwords]` |
| Streamlit Cloud secrets | Same keys, set in the deployment UI |
| `requirements.txt` | No change needed — `streamlit`, `gspread`, `pandas`, `openpyxl`, `xlsxwriter`, `google-cloud-bigquery`, `google-auth`, `pendulum`, `bcrypt` are all present |
| `run_all_tests.py` | Extend `--cov` to the `noo_sku` package |
| `CHANGELOG.md` | Entry for the new app |

### Reused as-is (no modification)

| Source | What is reused |
|---|---|
| `salesman_pjp.py` | Distributor login gate shape; `master_distributor` query |
| `smart_coverage.py` | gspread auth pattern; Asia/Jakarta timezone handling |
| `store_channelization.py` | xlsxwriter template + guideline-sheet + dropdown pattern |
| `noo_detector.py` | Fuzzy store-matching scorer — fallback for O4, and the reference for what BD Support already does downstream |
| `tests/test_assessment_logic.py` | Test style: pure functions, no credentials |
| `.streamlit/config.toml`, `assets/skintific_logo.png` | Theme and branding |

---

## 22. Explicitly not in scope

Per the MoM, the app stops at the pool. It does **not**:

- move rows into `SKINTIFIC NEW` / `TIMEPHORIA NEW` / `FACERINNA NEW`
- approve, reject, or reconcile submissions
- create or update stores in `master_store_database_basis`
- create or update products in `master_product`
- notify BD Support (they check the pool daily)

BD Support remains responsible for checking the pool and choosing the correct row when a
correction has been submitted.
