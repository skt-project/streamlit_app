import streamlit as st
import pandas as pd
import uuid
from io import BytesIO
from pendulum import now
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# --------------------------------------------------
# Page Config
# --------------------------------------------------
st.set_page_config(page_title="PO Portal Suggestion", layout="wide")

# --------------------------------------------------
# BigQuery Client
# --------------------------------------------------
gcp_secrets = dict(st.secrets["connections"]["bigquery"])
gcp_secrets["private_key"] = gcp_secrets["private_key"].replace("\\n", "\n")

credentials = service_account.Credentials.from_service_account_info(gcp_secrets)

PROJECT_ID = st.secrets["bigquery"]["project"]
DATASET = st.secrets["bigquery"]["dataset"]

PO_TABLE = "po_portal_suggestion"
FEEDBACK_TABLE = "po_portal_feedback"
USER_TABLE = "po_portal_distributor_users"

# Dynamic matrix tables (populated by GT_po_portal_suggestion_matrix_refresh).
# Read-only from this app. Independent of PO_TABLE and FEEDBACK_TABLE.
MATRIX_TABLE = "po_portal_suggestion_matrix"
MATRIX_SCHEMA_TABLE = "po_portal_suggestion_matrix_schema"

# Dynamic Upload Feedback write target — additive, alongside FEEDBACK_TABLE,
# never replacing it. A legacy-shaped upload still goes to FEEDBACK_TABLE
# exactly as before; only a dynamic-shaped upload goes here. Created via
# sql/02_create_po_portal_feedback_matrix.sql before this code path can be
# exercised for real — see _sync_bq_schema below for the additive-growth
# safety net (not relied on for initial creation).
FEEDBACK_MATRIX_TABLE = "po_portal_feedback_matrix"

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

    Still required: this is the source of the Upload Feedback Excel template,
    whose 25-column contract is validated on upload. It is deliberately NOT
    driven by the spreadsheet structure.
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
    (SIP)", "Standard WOI (Lead Time + Assortment WOI Target)" — real
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

    The column list is resolved at query time from po_portal_suggestion_matrix_schema,
    so a column added/removed/renamed/reordered in the sheet flows through with no
    code change. Nothing about the sheet's structure is hardcoded here.

    Returns (dataframe, display_columns) or (None, None) if the matrix tables are
    not available yet — in which case the caller falls back to the legacy read.
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


def _sync_bq_schema(client, table_id: str, desired: list) -> list:
    """
    Additive-only schema sync — identical contract to the matrix DAG's and to
    po_portal_suggestion_dev.py's own version. NOT relied on for
    po_portal_feedback_matrix's initial creation — that's
    sql/02_create_po_portal_feedback_matrix.sql's job, run via BigQuery
    Console before this code path can be exercised for real. Only a safety
    net here for the sheet growing wider later (adds new matrix_N columns).
    """
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


FEEDBACK_MATRIX_FIXED_SCHEMA = [
    bigquery.SchemaField('distributor_company', 'STRING'),
    bigquery.SchemaField('submission_id',       'STRING'),
    bigquery.SchemaField('submitted_at',        'DATETIME'),
    bigquery.SchemaField('feedback_qty',        'INTEGER'),
]

# Mirrors the legacy Upload Feedback required_cols list below verbatim — kept
# as its own module-level copy (not a shared reference) specifically so the
# shape-detection check below can run BEFORE we know which branch we're in,
# while the legacy branch's own internals stay byte-identical to what
# production has always run.
LEGACY_FEEDBACK_REQUIRED_COLS = [
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
    "feedback_qty",
]


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
    Writes a validated dynamic-shaped feedback upload to
    po_portal_feedback_matrix — never touches po_portal_feedback (the legacy
    table, still fed by the unchanged branch below). Uses the SAME bq_client
    this app already writes with today; production has never had a
    read/write credential split (unlike the DEV build), so this doesn't
    introduce one. Never raises — every failure mode returns (False, reason, {}).
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
        table_id = f"{PROJECT_ID}.{DATASET}.{FEEDBACK_MATRIX_TABLE}"
        desired_schema = FEEDBACK_MATRIX_FIXED_SCHEMA + [
            bigquery.SchemaField(mcol, "STRING") for mcol in matrix_cols
        ]
        _sync_bq_schema(bq_client, table_id, desired_schema)
        errors = bq_client.insert_rows_json(
            table_id, records,
            row_ids=[None] * len(records),
            skip_invalid_rows=True,
        )
        if errors:
            return False, "Failed to insert feedback", {"records": records, "errors": errors}
        return True, f"{len(records):,} row(s) submitted to {FEEDBACK_MATRIX_TABLE}", {"records": records}
    except Exception as exc:  # noqa: BLE001 - must never raise into the caller
        return False, f"{type(exc).__name__}: {exc}", {"records": records}


if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if not st.session_state.logged_in:
    st.title("🔐PO Portal Distributor Login")

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

# Legacy frame — powers the Upload Feedback Excel template (unchanged contract).
# Wrapped in try/except (2026-08-21, emergency fix): po_portal_suggestion is fed
# by a separate, now-deprecated spreadsheet whose formula has broken repeatedly.
# A failure here must never crash the whole app — the sheet that matters going
# forward is the one load_po_suggestion_dynamic() reads. Degrading to an empty
# frame means: RLS/Fix-B filter options go empty (distributors can't narrow the
# dynamic table by dropdown until this is resolved) but the dynamic table itself,
# its download, and both Upload Feedback paths (neither reads po_df) stay fully
# functional — restoring the core experience instead of a total outage.
try:
    po_df = load_po_suggestion()
except Exception:  # noqa: BLE001 - degrade rather than 500 the whole page
    po_df = pd.DataFrame(columns=LEGACY_FEEDBACK_REQUIRED_COLS[:-1])  # drop feedback_qty, not a queried column

# Dynamic frame — powers the on-screen table. Falls back to legacy if the matrix
# tables have not been created/populated yet, so this file is safe to deploy
# before the DAG lands.
try:
    dyn_df, dyn_cols = load_po_suggestion_dynamic()
except Exception:  # noqa: BLE001 - degrade to legacy rather than 500 the page
    dyn_df, dyn_cols = None, None

USING_DYNAMIC = dyn_df is not None and not dyn_df.empty

# 🔒 FORCE DATA BY LOGIN (ROW LEVEL SECURITY)
logged_company = st.session_state["distributor_company"]

st.caption(f"Logged in as: {logged_company}")

# 🔒 Row level security
if logged_company != "Admin":
    po_df = po_df[
        po_df["distributor_company"] == logged_company
    ]
    if USING_DYNAMIC:
        dyn_df = dyn_df[dyn_df[RLS_COL] == logged_company]

st.title("📦 PO Portal Suggestion")


# --------------------------------------------------
# FILTERS (CASCADED)
# --------------------------------------------------
# Filter widget OPTIONS always come from the legacy frame (po_df), regardless
# of which frame is driving the table display. If the dynamic (raw-sheet) and
# legacy (derived-sheet) sources have diverged -- plausible, given the
# derived sheet's documented fragility -- a distributor must never be able to
# select a filter value that doesn't exist in the legacy data, since the
# Excel export / Upload Feedback template always filters that same legacy
# frame. The dynamic table below is still filtered and rendered from dyn_df
# itself; only the available filter CHOICES are pinned to what's actually
# exportable, so a selection can never silently produce a 0-row export.
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


# Legacy frame — drives the Excel export / Upload Feedback template.
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

# --------------------------------------------------
# DOWNLOAD EXCEL — mirrors the on-screen table exactly (display_df), whichever
# frame is currently driving it. Upload Feedback below detects the uploaded
# file's shape rather than assuming which one this was, so a dynamic-shaped
# download here still round-trips correctly.
# --------------------------------------------------
excel_df = display_df.copy()
excel_df["feedback_qty"] = ""

output = BytesIO()
with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
    excel_df.to_excel(writer, index=False, sheet_name="po_suggestion")

output.seek(0)

st.download_button(
    label="📥 Download PO Suggestion (Excel)",
    data=output,
    file_name="po_portal_suggestion.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# ==================================================
# PO TRACKING SECTION
# ==================================================
st.divider()
st.header("📊 PO Tracking Data")

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
    label="📥 Download PO Tracking (Excel)",
    data=tracking_output,
    file_name="po_tracking.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# --------------------------------------------------
# UPLOAD FEEDBACK
# --------------------------------------------------
st.divider()
st.subheader("📤 Upload Feedback")

uploaded_file = st.file_uploader(
    "Upload filled Excel (feedback_qty)",
    type=["xlsx"]
)

if uploaded_file:
    df_upload = pd.read_excel(uploaded_file)

    # Shape-detection, not a live-flag gate: a legacy-shaped file (an old
    # download, or one taken during a DAG hiccup) always goes to the legacy
    # branch below regardless of what USING_DYNAMIC happens to be right now.
    # Only a file that ISN'T legacy-shaped is attempted as dynamic. This
    # removes a transition-window edge case a pure USING_DYNAMIC gate would
    # have (found during planning): USING_DYNAMIC flipping between a
    # distributor's download and their re-upload would otherwise route a
    # correctly-shaped file into the wrong validator.
    is_legacy_shape = all(c in df_upload.columns for c in LEGACY_FEEDBACK_REQUIRED_COLS)

    if is_legacy_shape:
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
        # CLEAN NUMERIC DATA (EXCEL SAFE)
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

        # --------------------------------------------------
        # ADD SUBMISSION METADATA
        # --------------------------------------------------
        submission_id = str(uuid.uuid4())
        submitted_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        df_upload["submission_id"] = submission_id
        df_upload["submitted_at"] = submitted_at

        final_cols = [
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
            "feedback_qty",
            "submission_id",
            "submitted_at"
        ]

        df_upload["feedback_qty"] = (
            df_upload["feedback_qty"]
            .fillna(0)
            .astype(int)
        )

        # Convert other NaN values to None for BigQuery
        df_upload = df_upload.fillna({
            "sku_status": "",
            "brand": "",
            "region": "",
            "distributor_company": "",
            "distributor_branch": "",
            "product_id": "",
            "product_name": "",
            "assortment": ""
        })

        # Prepare payload
        # Convert ke native Python types (WAJIB untuk BigQuery JSON)
        df_upload = df_upload.astype(object)

        # Replace NaN lagi (just in case)
        df_upload = df_upload.where(pd.notna(df_upload), None)

        records = df_upload[final_cols].to_dict("records")

        # --------------------------------------------------
        # INSERT TO BIGQUERY
        # --------------------------------------------------
        if st.button("Submit Feedback"):
            errors = bq_client.insert_rows_json(
                f"{PROJECT_ID}.{DATASET}.{FEEDBACK_TABLE}",
                records,
                row_ids=[None] * len(records),
                skip_invalid_rows=True
            )

            if errors:
                st.error("❌ Failed to insert feedback")
                for err in errors:
                    st.write(err)
            else:
                st.success("✅ Feedback successfully submitted")

    else:
        # ---- DYNAMIC path: file doesn't match the legacy shape ----
        # Both validation and submission are standalone functions (defined
        # above, before the login gate) — this script body is just UI glue
        # around them, same reasoning as the DEV build: inline script logic
        # can't be unit-tested, standalone functions can.
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
        st.dataframe(cleaned_df, use_container_width=True, hide_index=True)

        if st.button("Submit Feedback"):
            sub_ok, sub_msg, sub_extra = submit_feedback_dynamic(
                cleaned_df, matrix_cols, display_cols, logged_company)
            if sub_ok:
                st.success(f"✅ {sub_msg}")
            else:
                st.error(f"❌ {sub_msg}")
                for err in sub_extra.get("errors", []):
                    st.write(err)
