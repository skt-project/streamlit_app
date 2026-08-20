import streamlit as st
import pandas as pd
import uuid
import time
import logging
import gspread
from io import BytesIO
from pendulum import now
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

log = logging.getLogger("po_portal_suggestion_dev")

# ====================================================================
# DEV BUILD
# ====================================================================
# This file is a duplicate of po_portal_suggestion.py used ONLY to validate
# the dynamic-matrix table against real production data before promotion.
#
# Write scope, precisely (revised 2026-08-20 — added the fourth table and
# the dynamic Upload Feedback submit path; see git history for the
# three-table version if needed):
# this file can write to EXACTLY FOUR tables — po_portal_suggestion_matrix,
# po_portal_suggestion_matrix_schema, po_portal_suggestion_dev, and
# po_portal_feedback_dev — and NOTHING else. All four are tables this DEV
# build already exclusively owns/feeds; none are read by production, and
# NONE of them is production's `po_portal_feedback` (122k+ real distributor
# submissions) — that table's insert path (production's, and the near-
# duplicate one in po_portal_suggestion_v2.py) is never called from this
# file, on any path. The Upload Feedback DYNAMIC path (active when
# USING_DYNAMIC is True) now performs a REAL write to po_portal_feedback_dev
# on submit — this is new as of this revision and is the one production-
# mutating-shaped statement in this file; the LEGACY fallback path (matrix
# tables empty) stays preview-only, submission physically absent, unchanged
# from before. Every write call in this file lives inside
# run_matrix_refresh_inline(), run_dev_refresh_inline(), or the Upload
# Feedback dynamic-submit block — all three grep-findable, all three using a
# SEPARATE credential (`st.secrets["refresh"]`) from the one powering every
# read in the rest of this app (`st.secrets["connections"]["bigquery"]`,
# still recommended read-only per the README) — a write-capable credential
# misconfigured or compromised on the read path still can't write, and vice
# versa. Search this file for DEV_MODE to see every place that differs from
# production.
#
# DEV_MODE exists for UI labeling only (banners, titles, captions). No
# conditional in this file uses DEV_MODE to gate a write.
DEV_MODE = True

# --------------------------------------------------
# Page Config
# --------------------------------------------------
st.set_page_config(page_title="PO Portal Suggestion (DEV)", layout="wide")

st.warning(
    "🧪 **DEV ENVIRONMENT** — this app reads real production BigQuery data "
    "(including the dynamic matrix table) to validate the dynamic PO Portal "
    "Suggestion table before promotion. **This build can write, but only to "
    "its own four isolated tables** (`po_portal_suggestion_matrix`, "
    "`_matrix_schema`, `po_portal_suggestion_dev`, `po_portal_feedback_dev`) "
    "— via the manual refresh buttons below, or (Upload Feedback, dynamic "
    "path only) the Submit button — **never to production tables, never "
    "automatically.** The legacy-fallback Upload Feedback path stays "
    "preview-only. PO Tracking Data is read exactly as in production. See "
    "`README.md` in `po_portal_dynamic_matrix/` for the full audit.",
    icon="🧪",
)

# --------------------------------------------------
# BigQuery Client
# --------------------------------------------------
# Recommended (see README §DEV deployment): point this app's Streamlit Cloud
# secrets at a BigQuery Data Viewer + Job User service account — the same
# role class as D:\Claude\bq-skintific-dwh-readonly.json — instead of
# whatever writer-capable account production uses. That makes "cannot write"
# true at the IAM layer too, not just in this file's code path.
gcp_secrets = dict(st.secrets["connections"]["bigquery"])
gcp_secrets["private_key"] = gcp_secrets["private_key"].replace("\\n", "\n")

credentials = service_account.Credentials.from_service_account_info(gcp_secrets)

PROJECT_ID = st.secrets["bigquery"]["project"]
DATASET = st.secrets["bigquery"]["dataset"]

# Isolated DEV table — NOT the production po_portal_suggestion table. Fed by
# DAG `GT_po_portal_suggestion_refresh_DEV` on its normal schedule, AND by
# this app's own manual "Refresh DEV data" button (run_dev_refresh_inline,
# below) — reading the same raw source spreadsheet as production but writing
# to its own destination, so this app is unaffected by whatever is currently
# racing against the production table (see the 2026-08-18 schema-race RCA).
PO_TABLE = "po_portal_suggestion_dev"
FEEDBACK_TABLE = "po_portal_feedback"  # read-only reference only — legacy-fallback preview path
# Isolated DEV write target for the DYNAMIC upload path only. Never production
# po_portal_feedback. Auto-created on first submit via _sync_bq_schema, same
# as po_portal_suggestion_matrix — no manual DDL required.
FEEDBACK_DEV_TABLE = "po_portal_feedback_dev"
USER_TABLE = "po_portal_distributor_users"

# Dynamic matrix tables. Fed by DAG `GT_po_portal_suggestion_matrix_refresh`
# on its normal schedule, AND by this app's own manual "Refresh dynamic
# matrix data" button (run_matrix_refresh_inline, below). All reads in this
# file (load_po_suggestion_dynamic, the mapping-detail expander) are still
# SELECT-only; only run_matrix_refresh_inline writes here, via the separate
# [refresh] credential, not the bq_client below. Independent of PO_TABLE and
# FEEDBACK_TABLE either way.
MATRIX_TABLE = "po_portal_suggestion_matrix"
MATRIX_SCHEMA_TABLE = "po_portal_suggestion_matrix_schema"

# Columns the PO Suggestion section depends on structurally:
# row-level security + the three cascading filters.
RLS_COL = "_rls_company"
FLT_REGION = "_flt_region"
FLT_BRANCH = "_flt_branch"
INTERNAL_COLS = [RLS_COL, FLT_REGION, FLT_BRANCH]

bq_client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)

# --------------------------------------------------
# Load PO Suggestion
# --------------------------------------------------
def check_login(username, password):
    """SELECT-only. Identical to production — reading a login record is not a mutation."""

    query = f"""
        SELECT distributor_company, password_hash
        FROM `{PROJECT_ID}.{DATASET}.{USER_TABLE}`
        WHERE username = @username
          AND is_active = TRUE
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("username", "STRING", username)
        ]
    )

    df = bq_client.query(query, job_config=job_config).to_dataframe()

    if df.empty:
        return None

    stored_password = str(df.loc[0, "password_hash"]).strip()

    if password == stored_password:
        return df.loc[0, "distributor_company"]

    return None

@st.cache_data(ttl=600)
def load_po_suggestion():
    """
    Legacy fixed-schema read.

    Still required: this is the source of the Upload Feedback Excel template
    used for the preview in this DEV build (submission itself is disabled).
    Deliberately NOT driven by the spreadsheet structure — same as production.
    """
    query = f"""
        SELECT
            sku_status,
            brand,
            region,
            distributor_company,
            distributor_branch,
            product_id,
            product_name,
            current_stock_friday,
            in_transit_stock,
            total_stock,
            moq,
            standard_woi,
            avg_weekly_st_l3m,
            avg_weekly_st_lm,
            current_woi,
            si_target,
            assortment,
            stock_wh_qty,
            avg_weekly_st_mtd,
            avg_weekly_so_mtd,
            recomended_qty,
            ideal_weekly_po_qty,
            max_weekly_po_qty,
            min_weekly_po_qty
        FROM `{PROJECT_ID}.{DATASET}.{PO_TABLE}`
    """
    df = bq_client.query(query).to_dataframe()

    for col in ["region", "distributor_branch", "distributor_company"]:
        df[col] = df[col].astype(str).str.strip()

    return df


# --------------------------------------------------
# Load PO Suggestion — DYNAMIC (spreadsheet-driven)
# --------------------------------------------------
def _fetch_matrix_schema_df():
    """
    SELECT-only, uncached (unlike load_po_suggestion_dynamic below) — callers
    that need the freshest possible schema, like Upload Feedback validation,
    should call this directly rather than going through the 600s cache.
    """
    schema_sql = f"""
        SELECT matrix_column, source_header, source_header_original, column_position
        FROM `{PROJECT_ID}.{DATASET}.{MATRIX_SCHEMA_TABLE}`
        ORDER BY column_position
    """
    return bq_client.query(schema_sql).to_dataframe()


def _build_matrix_labels(schema_df):
    """
    Shared by the display path (load_po_suggestion_dynamic) and the Upload
    Feedback dynamic path — they MUST produce identical labels, since Upload
    Feedback validates an uploaded file against exactly the columns the
    display/download path generated. Aliased by the SAFE matrix_N name in
    SQL — NOT by the pretty header text — because BigQuery's query-result
    field-name rules are stricter than what backtick-quoting allows in the
    query text itself: any header containing parentheses (e.g. "Price
    (SIP)", "Standard WOI (Lead Time + Assortment WOI Target)" — several real
    headers on this sheet) fails with "Invalid field name" even though the
    SQL parses fine. Confirmed by running the previously-generated query
    directly against BigQuery. Renaming to the pretty label happens AFTER
    the fetch, in pandas, which has no such character restriction.
    """
    seen, matrix_cols, display_cols = {}, [], []
    for _, r in schema_df.iterrows():
        raw = (r["source_header_original"] or r["source_header"] or r["matrix_column"])
        label = str(raw).strip() or str(r["matrix_column"])
        if label in seen:
            seen[label] += 1
            label = f"{label}_{seen[label]}"
        else:
            seen[label] = 1
        matrix_cols.append(r["matrix_column"])
        display_cols.append(label)
    return matrix_cols, display_cols


@st.cache_data(ttl=600)
def load_po_suggestion_dynamic():
    """
    Read the spreadsheet's CURRENT structure from the matrix tables.

    This is the function under test in DEV: it reads the real
    po_portal_suggestion_matrix / _matrix_schema tables (SELECT only) so the
    dynamic behavior can be validated against production data before the
    production frontend is updated to call it.

    Returns (dataframe, display_columns) or (None, None) if the matrix tables
    are not available yet — in which case the caller falls back to the legacy
    read, same fallback behavior as the production candidate.
    """
    schema_df = _fetch_matrix_schema_df()
    if schema_df.empty:
        return None, None

    matrix_cols, display_cols = _build_matrix_labels(schema_df)
    projection = ",\n".join(f"  {c}" for c in matrix_cols)

    query = f"""
        SELECT
          distributor_company AS {RLS_COL},
          region              AS {FLT_REGION},
          distributor_branch  AS {FLT_BRANCH},
{projection}
        FROM `{PROJECT_ID}.{DATASET}.{MATRIX_TABLE}`
        ORDER BY row_index
    """
    df = bq_client.query(query).to_dataframe()
    df = df.rename(columns=dict(zip(matrix_cols, display_cols)))

    for col in INTERNAL_COLS:
        df[col] = df[col].astype(str).str.strip()

    return df, display_cols


# --------------------------------------------------
# Load PO Tracking Data
# --------------------------------------------------
@st.cache_data(ttl=600)
def load_po_tracking(company):
    """SELECT-only against dms.gt_po_tracking_all_mv. Untouched vs production."""

    if company == "Admin":
        query = """
            SELECT
                order_date,
                distributor_name,
                customer_order_no,
                sku,
                product_name,
                order_qty,
                unit_price,
                subtotal
            FROM `dms.gt_po_tracking_all_mv`
        """
        df = bq_client.query(query).to_dataframe()

    else:
        query = """
            SELECT
                order_date,
                distributor_name,
                customer_order_no,
                sku,
                product_name,
                order_qty,
                unit_price,
                subtotal
            FROM `dms.gt_po_tracking_all_mv`
            WHERE LOWER(distributor_name)
                  LIKE CONCAT('%', LOWER(@company), '%')
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("company", "STRING", company)
            ]
        )

        df = bq_client.query(query, job_config=job_config).to_dataframe()

    # ✅ move conversion OUTSIDE if/else
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    return df


# --------------------------------------------------
# Manual data refresh (user-initiated only — never automatic on login/page-load)
# --------------------------------------------------
# 2026-08-19 design change: this used to trigger the Airflow DAGs via
# Airflow's REST API. Switched to running the same extraction/load logic
# INLINE, synchronously, right here, because the Airflow path turned out to
# depend on infrastructure this app has no way to reach or verify: the
# Airflow REST API returns 401 (a documented, unresolved auth-backend issue),
# fixing it needs Docker/server access nobody in this project currently has
# from a coding session, AND the Airflow host is a private IP
# (192.168.110.37) that Streamlit Cloud's servers may not even be able to
# reach over the network — untested, unconfirmed, a real risk the trigger
# design carried. Running inline avoids all of that.
#
# Explicitly, still NOT the same as "import and execute the DAG's Python
# files" — the DAG files depend on Airflow's own runtime
# (BigQueryHook/Variable.get() only resolve inside an Airflow worker) and
# were never meant to run standalone. The functions below are a deliberate,
# separately-maintained PORT of the same business logic (sheet read -> cast
# -> load), re-credentialed to run from Streamlit instead of an Airflow
# worker. This is real logic duplication — a genuine, named cost: the DAGs
# (dag_gt_po_portal_suggestion_matrix.py / dag_gt_po_portal_suggestion_dev.py)
# and the two functions below can drift out of sync if one is edited without
# the other. Kept as small and literal a port as possible to minimize that
# risk, and called out here so it isn't a silent trap for a future editor.
#
# Credential separation: uses st.secrets["refresh"], a SEPARATE service
# account from st.secrets["connections"]["bigquery"] (used for every read
# in this app). That account needs (a) Google Sheets read access to
# 16diDHZExJeeQlJT9b4knVaamSsFJ86-6nLi3ipwU_RY ("PO Portal Project") and
# (b) BigQuery write access scoped to gt_schema — ideally via IAM Conditions
# restricted to just po_portal_suggestion_matrix / _matrix_schema /
# po_portal_suggestion_dev, not a blanket dataset Editor role, though the
# simpler dataset-level grant is what's documented as the default in the
# README. Read lazily, inside the refresh functions, not at module import
# time — a missing/misconfigured [refresh] secrets block must only disable
# the refresh buttons, never crash the rest of the app for every visitor.
#
# Still deliberately NOT triggered automatically on login/page-load, for the
# same reason as before: `po_portal_suggestion` (production's table) has an
# active, unresolved race condition (2026-08-18 schema-race RCA); firing
# writes on every visit — even to DEV's own isolated tables — has no upside
# over an explicit button and just adds load for no reason.
#
# DEV still never exposes anything that can write GT_po_portal_suggestion's
# table or touch production in any way — same isolation boundary as before,
# just enforced by "the code that runs doesn't reference that table" instead
# of "the button that would call it doesn't exist."
#
# Synchronous, not async: unlike the old Airflow-trigger design (which
# returned near-instantly, with Airflow doing the real work in the
# background), a click here BLOCKS the page for as long as the real read+load
# takes — a ~34,000-row Sheets read plus a BigQuery load, realistically tens
# of seconds. Streamlit's own spinner communicates this; there is no
# instant-return "queued" state anymore because there's no queue.
REFRESH_COOLDOWN_S = 60  # blocks rapid double-clicks from firing overlapping loads

RAW_SPREADSHEET_ID   = '16diDHZExJeeQlJT9b4knVaamSsFJ86-6nLi3ipwU_RY'  # 'PO Portal Project'
RAW_SHEET_NAME       = 'WIP - List PO Suggestion'
RAW_HEADER_ROW_INDEX = 2   # 1-based; row 1 on this raw tab holds KPI/summary values, not headers

# ---- matrix refresh: mirrors dag_gt_po_portal_suggestion_matrix.py ----
MATRIX_BUSINESS_FIELDS = ['distributor_company', 'region', 'distributor_branch']
MATRIX_FIXED_SCHEMA = [
    bigquery.SchemaField('distributor_company', 'STRING'),
    bigquery.SchemaField('region',              'STRING'),
    bigquery.SchemaField('distributor_branch',  'STRING'),
    bigquery.SchemaField('row_index',           'INTEGER'),
    bigquery.SchemaField('loaded_at',           'TIMESTAMP'),
]
MATRIX_SCHEMA_TABLE_SCHEMA = [
    bigquery.SchemaField('matrix_column',          'STRING'),
    bigquery.SchemaField('source_header',          'STRING'),
    bigquery.SchemaField('source_header_original', 'STRING'),
    bigquery.SchemaField('column_position',        'INTEGER'),
    bigquery.SchemaField('loaded_at',              'TIMESTAMP'),
]
MATRIX_MIN_EXPECTED_ROWS = 1

# ---- feedback (dynamic): fixed columns for the DEV-isolated submission
# target. matrix_1..matrix_N are appended at submit time, additive-synced via
# _sync_bq_schema exactly like the matrix table — the feedback table's width
# always matches whatever the CURRENT sheet schema was at submission time.
FEEDBACK_DEV_FIXED_SCHEMA = [
    bigquery.SchemaField('distributor_company', 'STRING'),
    bigquery.SchemaField('submission_id',       'STRING'),
    bigquery.SchemaField('submitted_at',        'DATETIME'),
    bigquery.SchemaField('feedback_qty',        'INTEGER'),
]

# ---- dev refresh: mirrors dag_gt_po_portal_suggestion_dev.py ----
DEV_STRING_COLUMNS = [
    'sku_status', 'brand', 'region', 'distributor_company',
    'distributor_branch', 'product_id', 'product_name', 'assortment',
]
DEV_INTEGER_COLUMNS = [
    'current_stock_friday', 'in_transit_stock', 'total_stock', 'moq',
    'standard_woi', 'avg_weekly_st_l3m', 'avg_weekly_st_lm', 'si_target',
    'stock_wh_qty', 'recomended_qty', 'ideal_weekly_po_qty',
    'max_weekly_po_qty', 'min_weekly_po_qty',
]
DEV_FLOAT_COLUMNS = ['current_woi', 'avg_weekly_st_mtd', 'avg_weekly_so_mtd']
DEV_BQ_SCHEMA = [
    bigquery.SchemaField('sku_status',            'STRING'),
    bigquery.SchemaField('brand',                 'STRING'),
    bigquery.SchemaField('region',                'STRING'),
    bigquery.SchemaField('distributor_company',   'STRING'),
    bigquery.SchemaField('distributor_branch',    'STRING'),
    bigquery.SchemaField('product_id',            'STRING'),
    bigquery.SchemaField('product_name',          'STRING'),
    bigquery.SchemaField('current_stock_friday',  'INTEGER'),
    bigquery.SchemaField('in_transit_stock',      'INTEGER'),
    bigquery.SchemaField('total_stock',           'INTEGER'),
    bigquery.SchemaField('moq',                   'INTEGER'),
    bigquery.SchemaField('standard_woi',          'INTEGER'),
    bigquery.SchemaField('avg_weekly_st_l3m',     'INTEGER'),
    bigquery.SchemaField('avg_weekly_st_lm',      'INTEGER'),
    bigquery.SchemaField('current_woi',           'FLOAT'),
    bigquery.SchemaField('si_target',             'INTEGER'),
    bigquery.SchemaField('assortment',            'STRING'),
    bigquery.SchemaField('stock_wh_qty',          'INTEGER'),
    bigquery.SchemaField('avg_weekly_st_mtd',     'FLOAT'),
    bigquery.SchemaField('avg_weekly_so_mtd',     'FLOAT'),
    bigquery.SchemaField('recomended_qty',        'INTEGER'),
    bigquery.SchemaField('ideal_weekly_po_qty',   'INTEGER'),
    bigquery.SchemaField('max_weekly_po_qty',     'INTEGER'),
    bigquery.SchemaField('min_weekly_po_qty',     'INTEGER'),
]
DEV_EXPECTED_COLS = [f.name for f in DEV_BQ_SCHEMA]
# Raw tab header (normalised) -> this schema's legacy column name. Only
# entries that don't already match 1:1 are needed. No source exists for
# avg_weekly_st_mtd / avg_weekly_so_mtd anywhere — they stay null.
DEV_HEADER_RENAME_MAP = {
    'stock_on_hand':                                    'current_stock_friday',
    'current_woi_vs_l3m':                               'current_woi',
    'standard_woi_(lead_time_+_assortment_woi_target)': 'standard_woi',
    'avg_weekly_l13_week':                              'avg_weekly_st_l3m',
    'avg_weekly_l5_week':                               'avg_weekly_st_lm',
    'norm_qty':                                         'ideal_weekly_po_qty',
    'saran_order_qty_(monthly_po)':                     'recomended_qty',
    'lowest_saran_order_(monthly_po)':                  'min_weekly_po_qty',
    'highest_saran_order':                               'max_weekly_po_qty',
}


def _refresh_credentials():
    """
    Raises a clear exception (never a bare KeyError, never a cryptic PEM
    parse error) if [refresh] secrets are absent or malformed — caught by
    each run_*_refresh_inline() caller, never at import time, so a missing
    or bad block only disables the buttons.
    """
    try:
        raw = dict(st.secrets["refresh"])
    except KeyError as exc:
        raise RuntimeError(
            "No [refresh] section in secrets.toml — add a service account "
            "with Sheets read access to the raw spreadsheet and BigQuery "
            "write access to gt_schema (see README §6e)."
        ) from exc

    key = raw.get("private_key", "") or ""
    key = key.replace("\\n", "\n")
    # Diagnose each failure mode SEPARATELY and report which one specifically
    # fired, plus safe metadata (length, a short prefix — the PEM boilerplate
    # itself isn't sensitive, this reveals nothing about the real key
    # material) — rather than one generic message covering 4 different root
    # causes. Without this, "still broken" and "broken differently" look
    # identical from outside, and neither the user nor a future debugger can
    # tell which condition is actually firing.
    prefix = key[:15].replace('\n', '\\n') if key else '(empty)'
    if not key:
        raise RuntimeError(
            "[refresh].private_key is empty or missing entirely. Check the "
            "field name is exactly `private_key` (not PrivateKey, private-key, "
            "etc.) and that it's inside the [refresh] table in secrets.toml."
        )
    if len(key) < 200:
        raise RuntimeError(
            f"[refresh].private_key is only {len(key)} characters after "
            f"unescaping — a real RSA private key is normally 1,700+ "
            f"characters. Starts with: {prefix!r}. This usually means the "
            f"value got truncated during copy/paste, or a placeholder was "
            f"left in place instead of the real key."
        )
    if "-----BEGIN" not in key:
        raise RuntimeError(
            f"[refresh].private_key doesn't contain a PEM '-----BEGIN' "
            f"header. Starts with: {prefix!r} ({len(key)} chars). Copy the "
            f"private_key field's value exactly as it appears in the "
            f"downloaded service-account JSON, including the BEGIN/END lines."
        )
    if "..." in key:
        raise RuntimeError(
            f"[refresh].private_key still contains a literal '...' "
            f"placeholder ({len(key)} chars total) instead of the real "
            f"base64 key content. If you copied an example from README/docs, "
            f"replace the whole '...' with the actual key body from your "
            f"downloaded service-account JSON."
        )
    raw["private_key"] = key
    return raw


def _read_raw_sheet_rows():
    """Authenticates with [refresh] creds, reads the raw tab, drops row 1."""
    raw = _refresh_credentials()
    creds = service_account.Credentials.from_service_account_info(raw, scopes=[
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ])
    gc = gspread.authorize(creds)
    all_rows = gc.open_by_key(RAW_SPREADSHEET_ID).worksheet(RAW_SHEET_NAME).get_all_values()
    return all_rows[RAW_HEADER_ROW_INDEX - 1:]


def _refresh_bq_client():
    raw = _refresh_credentials()
    creds = service_account.Credentials.from_service_account_info(raw)
    return bigquery.Client(credentials=creds, project=PROJECT_ID)


def _normalise_header(header: str) -> str:
    return str(header).strip().lower().replace(' ', '_')


def _sync_bq_schema(client, table_id: str, desired: list) -> list:
    """Additive-only schema sync — identical contract to the matrix DAG's."""
    try:
        table = client.get_table(table_id)
    except NotFound:
        client.create_table(bigquery.Table(table_id, schema=desired))
        return list(desired)
    existing = list(table.schema)
    known = {f.name for f in existing}
    additions = [f for f in desired if f.name not in known]
    if additions:
        table.schema = existing + additions
        client.update_table(table, ['schema'])
        return existing + additions
    return existing


def run_matrix_refresh_inline():
    """
    Port of dag_gt_po_portal_suggestion_matrix.py's extract_and_load(),
    re-credentialed for Streamlit. Writes ONLY to po_portal_suggestion_matrix
    and po_portal_suggestion_matrix_schema. Never raises — every failure
    mode returns (False, reason).
    """
    try:
        rows = _read_raw_sheet_rows()
        if not rows or len(rows) < 2:
            return False, f"Sheet returned only {len(rows) if rows else 0} row(s) — nothing to load."

        header_row = list(rows[0])
        while header_row and not str(header_row[-1]).strip():
            header_row.pop()
        width = len(header_row)
        if width == 0:
            return False, "Sheet header row is empty."

        headers = [_normalise_header(h) or f'column_{i + 1}' for i, h in enumerate(header_row)]
        body = [(list(r) + [''] * width)[:width] for r in rows[1:]]
        matrix_cols = [f'matrix_{i + 1}' for i in range(width)]
        df = pd.DataFrame(body, columns=matrix_cols, dtype=str).fillna('')

        if len(df):
            non_blank = df.apply(lambda s: s.str.strip()).ne('').any(axis=1)
            df = df[non_blank].reset_index(drop=True)

        position = {h: i for i, h in enumerate(headers)}
        for field in MATRIX_BUSINESS_FIELDS:
            if field in position:
                df[field] = df[f'matrix_{position[field] + 1}'].str.strip()
            else:
                df[field] = ''

        now_ts = pd.Timestamp.utcnow()
        df['row_index'] = range(1, len(df) + 1)
        df['loaded_at'] = now_ts

        mapping = pd.DataFrame({
            'matrix_column':          matrix_cols,
            'source_header':          headers,
            'source_header_original': header_row,
            'column_position':        list(range(1, width + 1)),
            'loaded_at':              now_ts,
        })

        if len(df) < MATRIX_MIN_EXPECTED_ROWS:
            return False, f"Only {len(df)} data row(s) after cleaning — refused to truncate the table."

        client = _refresh_bq_client()
        matrix_id = f"{PROJECT_ID}.{DATASET}.{MATRIX_TABLE}"
        schema_id = f"{PROJECT_ID}.{DATASET}.{MATRIX_SCHEMA_TABLE}"
        desired = MATRIX_FIXED_SCHEMA + [bigquery.SchemaField(f'matrix_{i + 1}', 'STRING')
                                          for i in range(width)]

        effective = _sync_bq_schema(client, matrix_id, desired)
        frame = df.copy()
        for field in effective:
            if field.name not in frame.columns:
                frame[field.name] = None
        frame = frame[[f.name for f in effective]]
        client.load_table_from_dataframe(
            frame, matrix_id,
            job_config=bigquery.LoadJobConfig(schema=effective,
                                               write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE),
        ).result()

        _sync_bq_schema(client, schema_id, MATRIX_SCHEMA_TABLE_SCHEMA)
        client.load_table_from_dataframe(
            mapping, schema_id,
            job_config=bigquery.LoadJobConfig(schema=MATRIX_SCHEMA_TABLE_SCHEMA,
                                               write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE),
        ).result()

        return True, f"Loaded {len(df):,} rows × {width} columns into {MATRIX_TABLE}"
    except Exception as exc:  # noqa: BLE001 - must never raise into the caller
        return False, f"{type(exc).__name__}: {exc}"


def run_dev_refresh_inline():
    """
    Port of dag_gt_po_portal_suggestion_dev.py's extract_and_load(),
    re-credentialed for Streamlit. Writes ONLY to po_portal_suggestion_dev.
    Never raises — every failure mode returns (False, reason).
    """
    try:
        rows = _read_raw_sheet_rows()
        if not rows:
            return False, "Sheet is empty."

        df = pd.DataFrame(rows[1:], columns=rows[0])
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        df = df.rename(columns=DEV_HEADER_RENAME_MAP)

        for col in DEV_STRING_COLUMNS:
            df[col] = df[col].astype(str).str.strip() if col in df.columns else ''

        for col in DEV_FLOAT_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].replace('', None).astype(str).str.replace(',', '.', regex=False),
                    errors='coerce')
            else:
                df[col] = None

        for col in DEV_INTEGER_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].replace('', None).astype(str).str.replace(',', '', regex=False),
                    errors='coerce').fillna(0).astype(int)
            else:
                df[col] = 0

        for col in DEV_EXPECTED_COLS:
            if col not in df.columns:
                df[col] = None
        df = df[DEV_EXPECTED_COLS]
        df = df[df['product_id'].str.strip().ne('') & df['product_id'].notna()]

        if df.empty:
            return False, "No data rows survived filtering — refused to truncate the table."

        client = _refresh_bq_client()
        table_id = f"{PROJECT_ID}.{DATASET}.{PO_TABLE}"
        client.load_table_from_dataframe(
            df, table_id,
            job_config=bigquery.LoadJobConfig(schema=DEV_BQ_SCHEMA,
                                               write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE),
        ).result()

        return True, f"Loaded {len(df):,} rows into {PO_TABLE}"
    except Exception as exc:  # noqa: BLE001 - must never raise into the caller
        return False, f"{type(exc).__name__}: {exc}"


def run_all_refreshes_inline():
    """
    Single-button entry point: runs both flows from one click. Each is still
    called unconditionally — _matrix runs even if _dev raised or failed, and
    vice versa — preserving the same "one flow's failure can never block the
    other" guarantee the two-button design had. The two calls are just
    combined into one (ok, msg) result for one button/one status area
    instead of two, so a partial failure is reported explicitly (never
    collapsed into a generic "something failed") rather than hidden by only
    running one flow.
    """
    dev_ok, dev_msg = run_dev_refresh_inline()
    matrix_ok, matrix_msg = run_matrix_refresh_inline()
    combined_ok = dev_ok and matrix_ok
    combined_msg = (
        f"_dev: {'✅' if dev_ok else '❌'} {dev_msg}\n\n"
        f"_matrix: {'✅' if matrix_ok else '❌'} {matrix_msg}"
    )
    return combined_ok, combined_msg


def render_refresh_button(label: str, action_fn, state_key: str):
    """
    Renders one independent manual-refresh button (per-flow cooldown,
    session_state-tracked last result). action_fn is a zero-arg callable
    returning (ok, msg) — both run_matrix_refresh_inline and
    run_dev_refresh_inline already never raise, but this wraps the call in
    its own try/except anyway as an absolute last-resort guard, since this
    function runs once per flow in sequence and a stray exception here must
    never stop the next flow's button from rendering.
    """
    last = st.session_state.get(state_key)
    now_ts = time.time()
    cooling_down = last is not None and (now_ts - last["at"]) < REFRESH_COOLDOWN_S

    try:
        if cooling_down:
            remaining = int(REFRESH_COOLDOWN_S - (now_ts - last["at"]))
            st.button(label, disabled=True, key=f"{state_key}_btn",
                      help=f"Ran {int(now_ts - last['at'])}s ago — "
                           f"wait {remaining}s before running again.")
        elif st.button(label, key=f"{state_key}_btn"):
            with st.spinner(f"Running {label}… this reads the full sheet and "
                             f"reloads BigQuery, typically tens of seconds."):
                ok, msg = action_fn()

            st.session_state[state_key] = {"at": now_ts, "ok": ok, "msg": msg}
            if ok:
                log.info("[manual refresh] %s", msg)
            else:
                log.error("[manual refresh] %s", msg)

            # The refresh writes fresh rows to BigQuery, but load_po_suggestion()
            # / load_po_suggestion_dynamic() are @st.cache_data(ttl=600) — without
            # clearing that cache, the page would keep showing the pre-refresh
            # snapshot for up to 10 minutes even though BigQuery already has the
            # new data. Cleared unconditionally (not only on full success) since
            # a partial outcome (e.g. _dev succeeded, _matrix didn't) still means
            # SOME table has new data that should show immediately. st.rerun()
            # re-executes the script so the fresh read actually happens now
            # instead of waiting for the next unrelated interaction; the
            # success/error message above survives the rerun via session_state
            # and is still shown via the "Last attempt" caption below.
            st.cache_data.clear()
            st.rerun()
    except Exception as exc:  # noqa: BLE001 - last-resort guard, see docstring
        log.error("[manual refresh] unexpected error rendering %r: %s", state_key, exc)
        st.error(f"❌ Unexpected error running `{label}`: {type(exc).__name__}: {exc}")
        last = st.session_state.get(state_key)  # may be unset — re-read is safe

    if last is not None and not cooling_down:
        badge = "✅" if last["ok"] else "❌"
        st.caption(f"{badge} Last attempt: {last['msg']}")


def validate_feedback_dynamic(df_upload, schema_df):
    """
    Pure validation for a dynamic-shaped Upload Feedback file — no BigQuery
    calls, so this is testable in isolation with a plain DataFrame. Mirrors
    the legacy path's checks (missing columns, feedback_qty must be numeric)
    but computes the required column list from the CURRENT schema instead of
    a hardcoded list, since a dynamic download's columns can change any time
    the sheet does.

    Returns (ok, message, extra):
      ok=False -> extra is {} normally, or {'invalid_rows': <DataFrame>} when
                  the failure is specifically bad feedback_qty values, so the
                  caller can show exactly which rows without recomputing the
                  mask itself.
      ok=True  -> extra has 'cleaned_df' (feedback_qty coerced to int),
                  'matrix_cols' and 'display_cols' (the schema used — needed
                  by submit_feedback_dynamic to map columns back to matrix_N).
    """
    if schema_df is None or schema_df.empty:
        return False, ("Dynamic schema unavailable right now — cannot validate "
                        "this upload. Try again shortly."), {}

    matrix_cols, display_cols = _build_matrix_labels(schema_df)
    required_cols = display_cols + ["feedback_qty"]

    missing = [c for c in required_cols if c not in df_upload.columns]
    if missing:
        return False, (f"Missing columns: {missing}. The spreadsheet's "
                        f"structure may have changed since you downloaded "
                        f"this file — re-download and try again."), {}

    raw_feedback = df_upload["feedback_qty"].fillna("").astype(str).str.strip()
    raw_feedback = raw_feedback.str.replace(r"\.0$", "", regex=True)
    invalid_mask = raw_feedback.ne("") & ~raw_feedback.str.match(r"^\d+(,\d+)*$")
    if invalid_mask.any():
        return False, "Upload gagal: feedback_qty hanya boleh berisi ANGKA", {
            "invalid_rows": df_upload.loc[invalid_mask]
        }

    cleaned = df_upload.copy()
    cleaned["feedback_qty"] = (
        raw_feedback.replace("", "0")
        .str.replace(",", "", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0)
        .astype(int)
    )
    return True, f"{len(cleaned):,} row(s) validated.", {
        "cleaned_df": cleaned, "matrix_cols": matrix_cols, "display_cols": display_cols
    }


def submit_feedback_dynamic(cleaned_df, matrix_cols, display_cols, logged_company):
    """
    Writes a validated dynamic-shaped feedback upload to po_portal_feedback_dev
    — never production po_portal_feedback. Uses the SEPARATE [refresh] write
    credential via _refresh_bq_client(), the same isolation already proven
    for run_matrix_refresh_inline / run_dev_refresh_inline. Never raises —
    every failure mode returns (False, reason, {}).
    """
    label_to_matrix = dict(zip(display_cols, matrix_cols))
    submission_id = str(uuid.uuid4())
    submitted_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    records = []
    for _, row in cleaned_df.iterrows():
        rec = {
            "distributor_company": logged_company,
            "submission_id": submission_id,
            "submitted_at": submitted_at,
            "feedback_qty": int(row["feedback_qty"]),
        }
        for label, mcol in label_to_matrix.items():
            val = row.get(label)
            rec[mcol] = "" if pd.isna(val) else str(val).strip()
        records.append(rec)

    try:
        client = _refresh_bq_client()
        table_id = f"{PROJECT_ID}.{DATASET}.{FEEDBACK_DEV_TABLE}"
        desired_schema = FEEDBACK_DEV_FIXED_SCHEMA + [
            bigquery.SchemaField(mcol, "STRING") for mcol in matrix_cols
        ]
        _sync_bq_schema(client, table_id, desired_schema)
        errors = client.insert_rows_json(
            table_id, records,
            row_ids=[None] * len(records),
            skip_invalid_rows=True,
        )
        if errors:
            return False, "Failed to insert feedback", {"records": records, "errors": errors}
        return True, f"{len(records):,} row(s) submitted to {FEEDBACK_DEV_TABLE}", {"records": records}
    except Exception as exc:  # noqa: BLE001 - must never raise into the caller
        return False, f"{type(exc).__name__}: {exc}", {"records": records}


if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if not st.session_state.logged_in:
    st.title("🔐 PO Portal Distributor Login — 🧪 DEV")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        company = check_login(username, password)

        if company:
            st.session_state.logged_in = True
            st.session_state.distributor_company = company
            st.rerun()
        else:
            st.error("❌ Invalid username or password")

    st.stop()

_LEGACY_COLS = [
    "sku_status", "brand", "region", "distributor_company", "distributor_branch",
    "product_id", "product_name", "current_stock_friday", "in_transit_stock",
    "total_stock", "moq", "standard_woi", "avg_weekly_st_l3m", "avg_weekly_st_lm",
    "current_woi", "si_target", "assortment", "stock_wh_qty", "avg_weekly_st_mtd",
    "avg_weekly_so_mtd", "recomended_qty", "ideal_weekly_po_qty",
    "max_weekly_po_qty", "min_weekly_po_qty",
]

# Legacy frame — powers the Upload Feedback Excel template preview (unchanged contract).
# Reads the isolated PO_TABLE (po_portal_suggestion_dev), not production. That
# table only exists once `GT_po_portal_suggestion_refresh_DEV` has been deployed
# and has run at least once — until then, degrade to an empty frame with a clear
# explanation rather than crashing the page.
try:
    po_df = load_po_suggestion()
except Exception as exc:  # noqa: BLE001 - DEV table may not exist yet
    po_df = pd.DataFrame(columns=_LEGACY_COLS)
    st.warning(
        f"🧪 DEV table `{PO_TABLE}` isn't available yet ({type(exc).__name__}). "
        "This means `GT_po_portal_suggestion_refresh_DEV` "
        "(`dags/dag_gt_po_portal_suggestion_dev.py`) hasn't been deployed to "
        "Airflow yet, or hasn't completed its first run. Once it has, this "
        "table is created automatically on that first load — no manual "
        "BigQuery step needed. Showing an empty PO Suggestion / Upload "
        "Feedback template below in the meantime.",
        icon="🧪",
    )

# Dynamic frame — powers the on-screen table. Falls back to legacy if the matrix
# tables have not been created/populated yet.
try:
    dyn_df, dyn_cols = load_po_suggestion_dynamic()
except Exception as exc:  # noqa: BLE001 - degrade to legacy rather than 500 the page
    dyn_df, dyn_cols = None, None
    st.info(
        "Dynamic spreadsheet view unavailable — showing the standard column set. "
        f"({type(exc).__name__})"
    )

USING_DYNAMIC = dyn_df is not None and not dyn_df.empty

# 🔒 FORCE DATA BY LOGIN (ROW LEVEL SECURITY) — same rule as production
logged_company = st.session_state["distributor_company"]

st.caption(f"Logged in as: {logged_company}  ·  🧪 DEV build — real prod data reads, writes only to its own isolated tables")

# 🔒 Row level security
if logged_company != "Admin":
    po_df = po_df[
        po_df["distributor_company"] == logged_company
    ]
    if USING_DYNAMIC:
        dyn_df = dyn_df[dyn_df[RLS_COL] == logged_company]

st.title("📦 PO Portal Suggestion  🧪 DEV")

if USING_DYNAMIC:
    st.success(
        f"✅ Dynamic matrix source active — rendering {len(dyn_cols)} column(s) "
        "detected live from the spreadsheet via `po_portal_suggestion_matrix_schema`."
    )
else:
    st.info(
        "ℹ️ Matrix tables not found or empty — showing the fixed legacy column "
        "set. Run `GT_po_portal_suggestion_matrix_refresh` at least once to "
        "test the dynamic path."
    )

with st.expander("🔄 Manual data refresh", expanded=False):
    st.caption(
        "Runs the same read-sheet-and-load-BigQuery logic as the Airflow "
        "DAGs, inline, right now — instead of waiting for their normal "
        "30-minute schedule. Synchronous: this blocks for as long as the "
        "real loads take (typically tens of seconds each, so up to ~a "
        "minute total for both). User-initiated only — never runs "
        "automatically on login or page refresh. One button runs both "
        "flows (_dev and _matrix); each is still attempted independently — "
        "a failure in one never skips or blocks the other, and both "
        "outcomes are always reported separately below, not collapsed into "
        "one pass/fail. Writes only to `po_portal_suggestion_dev` / "
        "`po_portal_suggestion_matrix` / `_matrix_schema` — the three "
        "tables this DEV build already exclusively owns. Never touches "
        "production; this build has no code path that references "
        "`po_portal_suggestion` (production's table) at all."
    )
    render_refresh_button(
        "🔄 Refresh all data (_dev + _matrix)",
        run_all_refreshes_inline,
        "refresh_all",
    )


# --------------------------------------------------
# FILTERS (CASCADED)
# --------------------------------------------------
# Filter values are sourced from whichever frame is driving the table, so they
# always reflect the current spreadsheet. The same selections are applied to the
# legacy frame as well, keeping the Excel preview consistent with the view.
if USING_DYNAMIC:
    flt_df, C_REGION, C_COMPANY, C_BRANCH = dyn_df, FLT_REGION, RLS_COL, FLT_BRANCH
else:
    flt_df, C_REGION, C_COMPANY, C_BRANCH = (
        po_df, "region", "distributor_company", "distributor_branch")

with st.expander("🔍 Filter", expanded=True):

    col1, col2, col3 = st.columns(3)

    # ---------------------------
    # REGION FILTER
    # ---------------------------
    region_options = sorted(
        flt_df[C_REGION].dropna().unique()
    )

    selected_regions = col1.multiselect(
        "Region",
        options=region_options
    )

    # ---------------------------
    # DISTRIBUTOR COMPANY FILTER
    # (depends on Region)
    # ---------------------------
    if selected_regions:
        company_options = (
            flt_df[flt_df[C_REGION].isin(selected_regions)]
            [C_COMPANY]
            .dropna()
            .unique()
        )
    else:
        company_options = (
            flt_df[C_COMPANY]
            .dropna()
            .unique()
        )

    company_options = sorted(company_options)

    selected_companies = col2.multiselect(
        "Distributor Company",
        options=company_options
    )

    # ---------------------------
    # DISTRIBUTOR BRANCH FILTER
    # (depends on Region + Company)
    # ---------------------------
    temp_df = flt_df.copy()

    if selected_regions:
        temp_df = temp_df[temp_df[C_REGION].isin(selected_regions)]

    if selected_companies:
        temp_df = temp_df[temp_df[C_COMPANY].isin(selected_companies)]

    branch_options = (
        temp_df[C_BRANCH]
        .dropna()
        .unique()
    )

    branch_options = sorted(branch_options)

    selected_branches = col3.multiselect(
        "Distributor Branch",
        options=branch_options
    )

# --------------------------------------------------
# APPLY FILTER
# --------------------------------------------------
def _apply_filters(frame, c_region, c_company, c_branch):
    out = frame.copy()
    if selected_regions:
        out = out[out[c_region].isin(selected_regions)]
    if selected_companies:
        out = out[out[c_company].isin(selected_companies)]
    if selected_branches:
        out = out[out[c_branch].isin(selected_branches)]
    return out


# Legacy frame — drives the Excel preview / Upload Feedback template.
filtered_df = _apply_filters(
    po_df, "region", "distributor_company", "distributor_branch")

# --------------------------------------------------
# DISPLAY TABLE  (spreadsheet-driven when available)
# --------------------------------------------------
if USING_DYNAMIC:
    display_df = _apply_filters(dyn_df, FLT_REGION, RLS_COL, FLT_BRANCH)
    # Render exactly the spreadsheet's current columns, in its current order.
    display_df = display_df[[c for c in dyn_cols if c in display_df.columns]]
    st.caption(f"{len(display_df):,} rows · {len(display_df.columns)} columns (from spreadsheet)")
else:
    display_df = filtered_df.copy()
    display_df["feedback_qty"] = ""

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True
)

with st.expander("🧪 DEV — matrix column mapping detail", expanded=False):
    st.caption(
        "Live contents of po_portal_suggestion_matrix_schema. Use this to verify "
        "add/remove/rename/reorder scenarios: change the sheet, rerun "
        "GT_po_portal_suggestion_matrix_refresh, refresh this page, and confirm "
        "the mapping and the table above both reflect the change."
    )
    try:
        mapping_preview = bq_client.query(f"""
            SELECT column_position, matrix_column, source_header,
                   source_header_original, loaded_at
            FROM `{PROJECT_ID}.{DATASET}.{MATRIX_SCHEMA_TABLE}`
            ORDER BY column_position
        """).to_dataframe()
        st.dataframe(mapping_preview, use_container_width=True, hide_index=True)
    except Exception as exc:  # noqa: BLE001
        st.caption(f"Mapping table not available yet: {type(exc).__name__}")

# --------------------------------------------------
# DOWNLOAD EXCEL — mirrors the on-screen table exactly (display_df), whichever
# frame is currently driving it. Downloading a file is a read from BigQuery's
# perspective; it writes nothing.
# --------------------------------------------------
excel_df = display_df.copy()
excel_df["feedback_qty"] = ""

output = BytesIO()
with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
    excel_df.to_excel(writer, index=False, sheet_name="po_suggestion")

output.seek(0)

st.download_button(
    label="📥 Download PO Suggestion (Excel) — DEV preview",
    data=output,
    file_name="po_portal_suggestion_DEV.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# ==================================================
# PO TRACKING SECTION — read-only, byte-for-byte same queries as production
# ==================================================
st.divider()
st.header("📊 PO Tracking Data  🧪 DEV (read-only, unaffected by this change)")

tracking_df = load_po_tracking(logged_company)

# -------------------------
# FILTERS
# -------------------------
colA, colB, colC = st.columns(3)

# distributor filter
dist_options = sorted(
    tracking_df["distributor_name"].dropna().unique()
)

selected_dist = colA.multiselect(
    "Distributor Name",
    options=dist_options
)

# order filter
order_options = sorted(
    tracking_df["customer_order_no"].dropna().unique()
)

selected_orders = colB.multiselect(
    "Customer Order No",
    options=order_options
)

# date filter
date_range = colC.date_input(
    "Order Date",
    value=None,
    format="YYYY/MM/DD"
)

# -------------------------
# APPLY FILTER
# -------------------------
filtered_tracking = tracking_df.copy()

if selected_dist:
    filtered_tracking = filtered_tracking[
        filtered_tracking["distributor_name"].isin(selected_dist)
    ]

if selected_orders:
    filtered_tracking = filtered_tracking[
        filtered_tracking["customer_order_no"].isin(selected_orders)
    ]

if date_range:

    # single date selected
    if not isinstance(date_range, (list, tuple)):
        start = end = pd.to_datetime(date_range)

    # range selected
    elif len(date_range) == 2:
        start = pd.to_datetime(date_range[0])
        end = pd.to_datetime(date_range[1])

    else:
        start = end = None

    if start is not None:
        filtered_tracking = filtered_tracking[
            (filtered_tracking["order_date"] >= start) &
            (filtered_tracking["order_date"] <= end)
        ]

# -------------------------
# DISPLAY
# -------------------------
display_tracking = filtered_tracking.copy()
display_tracking["order_date"] = display_tracking["order_date"].dt.date

st.dataframe(
    display_tracking,
    use_container_width=True,
    hide_index=True
)

# -------------------------
# DOWNLOAD EXCEL
# -------------------------
tracking_output = BytesIO()

with pd.ExcelWriter(tracking_output, engine="xlsxwriter") as writer:
    display_tracking.to_excel(
        writer,
        index=False,
        sheet_name="po_tracking"
    )

tracking_output.seek(0)

st.download_button(
    label="📥 Download PO Tracking (Excel) — DEV preview",
    data=tracking_output,
    file_name="po_tracking_DEV.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# --------------------------------------------------
# UPLOAD FEEDBACK
# --------------------------------------------------
# Two independent paths, matching the two shapes excel_df can come in as
# (see DOWNLOAD EXCEL above, which always mirrors display_df):
#
#   USING_DYNAMIC True  -> DYNAMIC path. Validates against the CURRENT
#                          po_portal_suggestion_matrix_schema (fetched fresh,
#                          not the 600s-cached read) and, on submit, writes
#                          to the DEV-isolated po_portal_feedback_dev table
#                          via the SEPARATE [refresh] write credential — the
#                          same credential/isolation pattern already proven
#                          for run_matrix_refresh_inline / run_dev_refresh_inline.
#                          This is a REAL write, not a preview.
#   USING_DYNAMIC False -> legacy fallback path. Unchanged from before this
#                          revision: validates the fixed 25-column shape,
#                          preview only, submission stays physically absent.
#                          This is the degraded/rare case (matrix tables
#                          empty) and was never what this round-trip needed
#                          to prove, so it isn't extended here.
st.divider()
st.subheader("📤 Upload Feedback  🧪 DEV")

if USING_DYNAMIC:
    st.info(
        "Validates a filled Excel against the sheet's CURRENT structure — "
        "exactly the columns the download above just produced. **Submitting "
        "here writes to `po_portal_feedback_dev`, a DEV-isolated table — "
        "never production `po_portal_feedback`.**",
        icon="🧪",
    )
else:
    st.info(
        "Dynamic matrix tables aren't available right now, so this falls "
        "back to the legacy fixed-column template — **preview only, "
        "submission is physically removed from this path**, since it isn't "
        "the round trip this build validates.",
        icon="🚫",
    )

uploaded_file = st.file_uploader(
    "Upload filled Excel (feedback_qty)",
    type=["xlsx"]
)

if uploaded_file:
    df_upload = pd.read_excel(uploaded_file)

    if USING_DYNAMIC:
        # ---- DYNAMIC path: validate + REAL submit to po_portal_feedback_dev ----
        # Both steps are standalone functions (defined above, before the login
        # gate) precisely so they're unit-testable the same way
        # run_matrix_refresh_inline/run_dev_refresh_inline already are — this
        # script body is just UI glue around them, no business logic of its own.
        schema_df = _fetch_matrix_schema_df()
        ok, msg, extra = validate_feedback_dynamic(df_upload, schema_df)

        if not ok:
            st.error(f"❌ {msg}")
            if "invalid_rows" in extra:
                st.warning("Baris berikut mengandung huruf atau simbol:")
                st.dataframe(extra["invalid_rows"], use_container_width=True)
            st.stop()

        cleaned_df = extra["cleaned_df"]
        matrix_cols = extra["matrix_cols"]
        display_cols = extra["display_cols"]

        st.success(f"✅ File validated — {msg}")
        st.subheader("Preview")
        st.dataframe(cleaned_df, use_container_width=True, hide_index=True)

        if st.button("✅ Submit Feedback — writes to po_portal_feedback_dev"):
            sub_ok, sub_msg, sub_extra = submit_feedback_dynamic(
                cleaned_df, matrix_cols, display_cols, logged_company)
            if sub_ok:
                st.success(f"✅ {sub_msg} (DEV-isolated table).")
            else:
                st.error(f"❌ {sub_msg}")
                for err in sub_extra.get("errors", []):
                    st.write(err)

    else:
        # ---- Legacy fallback path — preview only, submission stays absent ----
        required_cols = [
            "sku_status",
            "brand",
            "region",
            "distributor_company",
            "distributor_branch",
            "product_id",
            "product_name",
            "current_stock_friday",
            "in_transit_stock",
            "total_stock",
            "moq",
            "standard_woi",
            "avg_weekly_st_l3m",
            "avg_weekly_st_lm",
            "current_woi",
            "si_target",
            "assortment",
            "stock_wh_qty",
            "avg_weekly_st_mtd",
            "avg_weekly_so_mtd",
            "recomended_qty",
            "ideal_weekly_po_qty",
            "max_weekly_po_qty",
            "min_weekly_po_qty",
            "feedback_qty"
        ]

        missing = [c for c in required_cols if c not in df_upload.columns]
        if missing:
            st.error(f"❌ Missing columns: {missing}")
            st.stop()

        raw_feedback = (
            df_upload["feedback_qty"]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        # hilangkan .0 di belakang angka (hasil dari Excel)
        raw_feedback = raw_feedback.str.replace(r"\.0$", "", regex=True)

        invalid_mask = (
            raw_feedback.ne("")   # kosong tetap boleh
            & ~raw_feedback.str.match(r"^\d+(,\d+)*$")
        )

        if invalid_mask.any():
            invalid_rows = df_upload.loc[
                invalid_mask,
                ["region", "distributor_branch", "product_id", "product_name", "feedback_qty"]
            ]

            st.error("❌ Upload gagal: feedback_qty hanya boleh berisi ANGKA")
            st.warning("Baris berikut mengandung huruf atau simbol:")

            st.dataframe(invalid_rows, use_container_width=True)

            st.stop()

        # --------------------------------------------------
        # CLEAN NUMERIC DATA (EXCEL SAFE) — in-memory only, identical to production
        # --------------------------------------------------
        INT_COLS = [
            "current_stock_friday",
            "in_transit_stock",
            "total_stock",
            "moq",
            "standard_woi",
            "avg_weekly_st_l3m",
            "avg_weekly_st_lm",
            "si_target",
            "stock_wh_qty",
            "recomended_qty",
            "ideal_weekly_po_qty",
            "max_weekly_po_qty",
            "min_weekly_po_qty",
            "feedback_qty"
        ]

        FLOAT_COLS = [
            "current_woi",
            "avg_weekly_st_mtd",
            "avg_weekly_so_mtd"
        ]

        def clean_numeric(df, cols, dtype):
            for col in cols:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(",", "", regex=False)
                    .pipe(pd.to_numeric, errors="coerce")
                    .fillna(0)
                    .astype(dtype)
                )

        clean_numeric(df_upload, INT_COLS, "Int64")
        clean_numeric(df_upload, FLOAT_COLS, "float64")

        st.success(f"✅ File validated — **{len(df_upload):,} row(s)** would be submitted in production.")
        st.subheader("Preview (read-only)")
        preview_cols = ["product_id", "product_name", "distributor_branch",
                        "recomended_qty", "feedback_qty"]
        st.dataframe(
            df_upload[[c for c in preview_cols if c in df_upload.columns]],
            use_container_width=True,
            hide_index=True,
        )

        # --------------------------------------------------
        # SUBMISSION — REMOVED for this path.
        #
        # Production's "Submit Feedback" button and its bq_client.insert_rows_json(...)
        # call intentionally do not exist below this line. The button is rendered
        # disabled so a tester sees where it would be, but no click handler and no
        # BigQuery write call are wired to it — there is nothing to bypass.
        # --------------------------------------------------
        st.button(
            "🚫 Submit Feedback — disabled (legacy fallback path)",
            disabled=True,
            help="Submission is physically removed from this path. "
                 "No row can be written to po_portal_feedback from here.",
        )
        st.caption(
            "In production, clicking Submit here would insert these rows into "
            f"`{PROJECT_ID}.{DATASET}.{FEEDBACK_TABLE}`. That call does not exist "
            "in this DEV file."
        )
