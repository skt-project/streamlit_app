"""Configuration and constants for the NOO & SKU Mapping app.

Every literal that describes the real spreadsheets lives here, so that a tab
rename or a column move is a one-file change. Values were verified by read-only
inspection on 2026-08-19 — see docs/streamlit_noo_sku_mapping_implementation.md
for the audit that produced them.
"""
from __future__ import annotations

import os

# ─── Spreadsheets ─────────────────────────────────────────────────────────────
# Native Google Sheet. Read + append.
TRACKER_SPREADSHEET_ID = "1bchAAMuXOT1lzuAB-KbrrAwpIrL1_MG3Hzcq823PAN4"
TRACKER_TITLE = "NOO TRACKER GT"

# Both of these are .xlsx files stored in Drive, NOT native Google Sheets, so
# they must be fetched with Drive files().get_media() and opened with openpyxl.
# gspread cannot open them. Read-only: they are BD Support's blank templates.
SKU_TEMPLATE_FILE_ID = "1UObRQCPBB3grWvGcbe3S9F-gW8LWS_Pk"
NOO_TEMPLATE_FILE_ID = "1Yt6vRRVSz2-mm59KzVsq32MrwqmzDoYB"

# ─── Tracker tabs (verified) ──────────────────────────────────────────────────
TAB_POOL_NOO = "POOL NOO STREAMLIT"      # gid 557889479  — existing, header fixed, append only
TAB_POOL_SKU = "POOL SKU STREAMLIT"      # gid 654605989  — existing, header fixed, append only
TAB_SKU_MAPPING = "SKU MAPPING"          # gid 2087836837 — BD Support main SKU tracker
TAB_DIST_DATABASE = "DIST DATABASE"      # gid 1421740146 — distributor master
TAB_NOO_MAIN = {                         # BD Support main NOO trackers, per brand
    "SKINTIFIC": "SKINTIFIC NEW",
    "TIMEPHORIA": "TIMEPHORIA NEW",
    "FACERINNA": "FACERINNA NEW",
}

# ─── DIST DATABASE layout ─────────────────────────────────────────────────────
# Row 1 is a merged grouping banner; the real header is on row 2 and data starts
# on row 3. Indices below are 0-based offsets into a row read from column A.
DIST_HEADER_ROW = 2
DIST_COL_ID_CODE = 0        # "ID CODE"                e.g. 82
DIST_COL_COMPANY = 1        # "distributor_company_name"
DIST_COL_NAME = 2           # "Distributor Name "      (trailing space in sheet)
DIST_COL_STATUS = 3         # "Status"                 Active / Inactive
DIST_COL_REGION = 24        # "Region"
DIST_COL_CODE = 27          # "Distributor Code Fix"   e.g. DST082
DIST_COL_BRANCH_CODE = 40   # "Customer Branch Code"   e.g. CEC  <- the DB abbreviation

# ─── SKU MAPPING layout ───────────────────────────────────────────────────────
# Header on row 1, data from row 2. NOTE: the declared header of column I is
# "Product Chinese Short Name" but the column actually holds the Distributor
# Code (verified: 5316/5316 populated values match ^DST). The header is a legacy
# mislabel; we read by position, never by that name.
SKU_MAP_COL_DMS = 0          # "DMS"        BD Support processing flag ("DONE")
SKU_MAP_COL_DATE = 4         # "Date"       M/D/YYYY
SKU_MAP_COL_CUSTOMER_CODE = 5
SKU_MAP_COL_CUSTOMER_NAME = 6   # holds the DISTRIBUTOR name
SKU_MAP_COL_PRODUCT = 7         # SKU Code Principal
SKU_MAP_COL_DIST_CODE = 8       # mislabeled header — actually Distributor Code
SKU_MAP_COL_CUST_PROD_CODE = 10  # SKU Code DB
SKU_MAP_COL_CUST_PROD_NAME = 11  # SKU Name DB
SKU_MAP_COL_SPECIFICATION = 12   # Size — almost never populated historically

# ─── Brand prefixes (verified against 5,353 SKU MAPPING rows) ─────────────────
# Only these three brands appear in this tracker. 12=G2G and 17=NEXTPRIME/
# BODIBREZE exist in the wider customer-code space but are out of scope here.
BRAND_PREFIX = {
    "SKINTIFIC": "11",
    "TIMEPHORIA": "13",
    "FACERINNA": "1A",
}
IN_SCOPE_BRANDS = tuple(BRAND_PREFIX)
VALID_PREFIXES = tuple(BRAND_PREFIX.values())

# ─── Upload template layouts (verified against BD Support's real .xlsx) ───────
# NOO: row 1 is an instruction banner, header on row 2, row 3 is the "CONTOH"
# example row, real data starts on row 4.
NOO_SHEET_NAME = "Template"
NOO_HEADER_ROW = 2
NOO_EXAMPLE_ROW = 3
NOO_COLUMNS = [
    "Store ID (Opsional)",
    "Store Name",
    "Channel (GT / MTi)",
    "Branch Name",
    "Customer Code",          # sheet has a trailing space; we normalise on read
    "Customer Branch Code",
    "Customer Store Code",
    "City",
    "Store Address",
    "Store Type",
]
# Columns the system owns. Whatever the file contains here is IGNORED and
# replaced with session/master-derived values (see §Security).
NOO_SYSTEM_OWNED = ("Branch Name", "Customer Branch Code")

# SKU: row 1 instruction banner, row 2 blank, header on row 3, row 4 "CONTOH",
# row 5 example, real data from row 6.
SKU_SHEET_NAME = "SKU TEMPLATE FOR STREAMLIT"
SKU_HEADER_ROW = 3
SKU_EXAMPLE_ROW = 5
# MoM 31-Aug-2026 removed the gramasi / specification column: admins no longer
# enter a product size. The pool still has a `specification` column, which is now
# filled from master_product rather than from the upload.
SKU_COLUMNS = [
    "Principal Product Code",
    "Principal Product Name",
    "Customer Product Code ( Di isi oleh Distributor)",
    "Customer Product Name  ( Di isi oleh Distributor)",
]

# Header signatures for wrong-template detection. Normalised (see normalize.py).
NOO_SIGNATURE = {"store name", "customer store code", "store address"}
SKU_SIGNATURE = {"principal product code", "principal product name"}

# ─── Pool tab layouts — READ FROM THE LIVE SHEET, DO NOT CHANGE ──────────────
# Verified 2026-08-19 against gid 557889479 and gid 654605989. These worksheets
# already exist and already carry these headers. The application adapts to them:
# no column may be added, removed, renamed or reordered. writer.assert_layout()
# re-reads the live header before every write and refuses on any mismatch.
# Re-verified 2026-09-01: BD Support prepended five processing columns, so the
# tab is now 41 wide. The original 36 follow unchanged, shifted right by five.
POOL_NOO_HEADERS = [
    # BD Support's own processing columns — Streamlit never writes these.
    "DMS", "BASIS", "RSA Name", "BD Support", "NOO/Existing",
    "asm_name", "input_time", "branch_name", "region", "store_id", "store_name",
    "channel_name", "customer_code", "customer_branch_code",
    "customer_store_code", "customer_store_name", "city", "store_address",
    "longitude", "latitude", "store_type", "visibility_rating",
    "location_rating", "asm_kam", "spv", "se_kae", "aom", "tl", "pm", "md/smd",
    "ba1", "ba2", "ba3", "ba4", "group_branch_blank", "group_name", "nik",
    "npwp", "remark", "area", "province",
]
POOL_SKU_HEADERS = [
    "asm", "region", "input_time", "customer_code", "customer_name",
    "product_code", "customer_branch_code", "product_name",
    "customer_product_code", "customer_product_name", "specification",
    "barcode", "description",
]

# Columns measured at <=0.1% fill across 3,859 rows of SKINTIFIC NEW, the pool's
# direct precedent. Left deliberately blank: populating them would push data BD
# Support neither expects nor uses into an operational sheet.
POOL_NOO_UNUSED = frozenset({
    "longitude", "latitude", "visibility_rating", "location_rating", "tl", "pm",
    "md/smd", "ba1", "ba2", "ba3", "ba4", "group_branch_blank", "group_name",
    "nik", "npwp", "remark",
    # BD Support's processing columns, added 2026-09-01. Theirs to fill.
    "DMS", "BASIS", "RSA Name", "BD Support",
})
POOL_SKU_UNUSED = frozenset({"barcode", "description"})

# System-generated; never part of any comparison (brief 11 and 13).
TIMESTAMP_COLUMNS = frozenset({"input_time"})

# Store-level enrichment looked up from master data at submission time. Excluded
# from the content hash because their availability drifts: a store absent from
# master_store_database_basis today may be present next week, and that must not
# turn a genuine re-submission into a spurious CORRECTION. Identity and all
# user-entered business content remain hashed.
VOLATILE_ENRICHMENT_COLUMNS = frozenset({
    # People assignments and geography, resolved from master data at submission
    # time. None is user input; each is a function of (identity, master state),
    # so including them would make the hash depend on WHEN the row was looked
    # up rather than WHAT the admin submitted.
    "se_kae", "spv", "aom", "asm_name", "asm_kam", "asm", "area", "province",
    "region",
    # NOO/Existing — MoM 2026-08-31 §3/§4: the NOO Detector's Reference-ID
    # verdict. Derived from master_store_database_basis at submission time, so
    # it is exactly as volatile as se_kae/spv/etc for the same reason: another
    # admin's upload could change what "already exists" means for this store
    # between two uploads of otherwise-identical business data.
    "NOO/Existing",
    # The distributor's own name, rendered from master. It is constant for every
    # row of a given distributor, so it adds zero discriminating power to a hash
    # that is already scoped by distributor - while a rename in master would
    # re-hash that distributor's entire history into false CORRECTIONs.
    "branch_name", "customer_name",
})

# ─── Date / time formats ──────────────────────────────────────────────────────
# Both pools carry a single `input_time` column rather than a separate date, so
# the app writes one timestamp. Format follows the trackers' readable style.
INPUT_TIME_FORMAT = "%d-%b-%Y %H:%M:%S"
DATE_FORMAT_NOO = "%d-%b-%Y"
BUSINESS_TIMEZONE = "Asia/Jakarta"

# ─── Channel / store type (from the template's "City & Store Type" sheet) ─────
STORE_TYPES_BY_CHANNEL = {
    "GT": {"ATC", "Cosmetic Store", "Pharmacy", "Retail Store"},
    "MTI": {"Large Supermarket", "Minimarket", "Premium SPM", "Regular SPM",
            "Specialty"},
}
VALID_CHANNELS = tuple(STORE_TYPES_BY_CHANNEL)

# ─── Limits ───────────────────────────────────────────────────────────────────
MAX_UPLOAD_ROWS = 5000
STORE_ID_PATTERN = r"^IE[A-Z]{2}\d{3,6}$"
DIST_CODE_PATTERN = r"^DST[A-Z0-9]{2,6}$"


# ─── Runtime environment ──────────────────────────────────────────────────────
def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


MODE_DRY_RUN = "dry-run"
MODE_PILOT = "pilot"
MODE_PRODUCTION = "production"
MODES = (MODE_DRY_RUN, MODE_PILOT, MODE_PRODUCTION)

#: Hard ceiling on a PILOT write. The first controlled write is meant to be a
#: handful of rows that a human verifies by eye.
DEFAULT_PILOT_MAX_ROWS = 3


class Settings:
    """Runtime settings, resolved from st.secrets then environment.

    Three write modes, defaulting to the safest:

        dry-run     (default) read, validate, enrich, classify, preview. NO WRITE.
        pilot       write at most `pilot_max_rows` rows, then verify.
        production  controlled write, still requires write_enabled.

    Writing requires BOTH a non-dry-run mode AND `write_enabled`. A misconfigured
    or half-configured deployment therefore performs a dry run rather than
    touching a live operational spreadsheet.
    """

    def __init__(self, secrets=None, env=None, mode=None, pilot_max_rows=None):
        secrets = secrets or {}
        env = env if env is not None else os.environ
        app = secrets.get("app", {}) if hasattr(secrets, "get") else {}

        self.app_env = (app.get("env") or env.get("APP_ENV")
                        or "dev").strip().lower()

        raw_mode = (mode or app.get("mode") or env.get("APP_MODE")
                    or MODE_DRY_RUN)
        self.mode = str(raw_mode).strip().lower()
        if self.mode not in MODES:
            raise ValueError(
                f"Mode tidak dikenali: {raw_mode!r}. Pilih salah satu: "
                + ", ".join(MODES))

        raw_write = app.get("write_enabled")
        if raw_write is None:
            self.write_enabled = _as_bool(env.get("WRITE_ENABLED"),
                                          default=False)
        else:
            self.write_enabled = bool(raw_write)

        self.pilot_max_rows = int(
            pilot_max_rows if pilot_max_rows is not None
            else app.get("pilot_max_rows")
            or env.get("PILOT_MAX_ROWS")
            or DEFAULT_PILOT_MAX_ROWS)

        self.tracker_spreadsheet_id = (
            app.get("tracker_spreadsheet_id")
            or env.get("TRACKER_SPREADSHEET_ID")
            or TRACKER_SPREADSHEET_ID
        )

    @property
    def is_production(self) -> bool:
        return self.mode == MODE_PRODUCTION

    @property
    def is_pilot(self) -> bool:
        return self.mode == MODE_PILOT

    @property
    def dry_run(self) -> bool:
        """True when no write may reach a spreadsheet."""
        return self.mode == MODE_DRY_RUN or not self.write_enabled

    @property
    def max_rows(self):
        """Row ceiling for this mode, or None when uncapped."""
        return self.pilot_max_rows if self.is_pilot else None

    def __repr__(self):  # pragma: no cover - debug aid
        return (f"Settings(mode={self.mode!r}, app_env={self.app_env!r}, "
                f"write_enabled={self.write_enabled}, dry_run={self.dry_run}, "
                f"pilot_max_rows={self.pilot_max_rows})")
