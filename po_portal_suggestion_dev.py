import streamlit as st
import pandas as pd
import uuid
from io import BytesIO
from pendulum import now
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery

# ====================================================================
# DEV / READ-ONLY BUILD
# ====================================================================
# This file is a duplicate of po_portal_suggestion.py used ONLY to validate
# the dynamic-matrix table against real production data before promotion.
#
# Hard guarantee: this file contains ZERO calls to insert_rows_json, UPDATE,
# DELETE, DROP, MERGE, or any BigQuery write/DML verb, ZERO GCS writes, and
# ZERO DAG-trigger calls. The single production-mutating statement that
# exists in po_portal_suggestion.py — the `bq_client.insert_rows_json(...)`
# in Upload Feedback — has been physically deleted below, not merely
# disabled behind a flag. Search this file for DEV_MODE to see every place
# that differs from production.
#
# DEV_MODE exists for UI labeling only (banners, titles, captions). No
# conditional in this file uses DEV_MODE to gate a write — there is no write
# to gate. Removing DEV_MODE entirely would not make any write possible.
DEV_MODE = True

# --------------------------------------------------
# Page Config
# --------------------------------------------------
st.set_page_config(page_title="PO Portal Suggestion (DEV)", layout="wide")

st.warning(
    "🧪 **DEV / READ-ONLY ENVIRONMENT** — this app reads real production "
    "BigQuery data (including the new dynamic matrix table) to validate the "
    "dynamic PO Portal Suggestion table before it is promoted. **No action "
    "on this page can write to production.** Upload Feedback submission is "
    "disabled; PO Tracking Data and Upload Feedback are otherwise read "
    "exactly as in production. See `README.md` in "
    "`po_portal_dynamic_matrix/` for the full audit.",
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

PO_TABLE = "po_portal_suggestion"
FEEDBACK_TABLE = "po_portal_feedback"  # read-only reference only — see Upload Feedback section
USER_TABLE = "po_portal_distributor_users"

# Dynamic matrix tables (populated by GT_po_portal_suggestion_matrix_refresh).
# Read-only from this app. Independent of PO_TABLE and FEEDBACK_TABLE.
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
    schema_sql = f"""
        SELECT matrix_column, source_header, source_header_original, column_position
        FROM `{PROJECT_ID}.{DATASET}.{MATRIX_SCHEMA_TABLE}`
        ORDER BY column_position
    """
    schema_df = bq_client.query(schema_sql).to_dataframe()
    if schema_df.empty:
        return None, None

    # Build the projection in spreadsheet order. Duplicate headers get a numeric
    # suffix so the SELECT stays valid and no column is silently lost.
    seen, selects, display_cols = {}, [], []
    for _, r in schema_df.iterrows():
        raw = (r["source_header_original"] or r["source_header"] or r["matrix_column"])
        label = str(raw).strip() or str(r["matrix_column"])
        if label in seen:
            seen[label] += 1
            label = f"{label}_{seen[label]}"
        else:
            seen[label] = 1
        selects.append(f"  {r['matrix_column']} AS `{label}`")
        display_cols.append(label)

    # Built outside the f-string: backslash escapes inside f-string expressions
    # are a SyntaxError before Python 3.12.
    projection = ",\n".join(selects)

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

# Legacy frame — powers the Upload Feedback Excel template preview (unchanged contract).
po_df = load_po_suggestion()

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

st.caption(f"Logged in as: {logged_company}  ·  🧪 DEV build — real prod data, no writes possible")

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
# DOWNLOAD EXCEL (preview only — same file production would generate)
# --------------------------------------------------
# Built from the LEGACY frame, not the dynamic one — mirrors production exactly.
# Downloading a file is a read from BigQuery's perspective; it writes nothing.
excel_df = filtered_df.copy()
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
# UPLOAD FEEDBACK — PREVIEW ONLY. SUBMISSION IS PHYSICALLY DISABLED.
# --------------------------------------------------
# Everything up to and including validation/preview is IN-MEMORY pandas work —
# no BigQuery call happens until (in production) the "Submit Feedback" button
# is clicked. In this DEV file, that button and the insert_rows_json call it
# used to trigger have been REMOVED, not merely hidden or gated behind a flag.
# There is no code path in this file that can write a row to po_portal_feedback.
st.divider()
st.subheader("📤 Upload Feedback  🧪 DEV — preview only, submission disabled")
st.info(
    "This section validates a filled Excel exactly like production, so the "
    "template produced by the dynamic table change can be checked end to end. "
    "**The actual BigQuery write has been removed from this build** — no "
    "button here can insert a row into `po_portal_feedback`.",
    icon="🚫",
)

uploaded_file = st.file_uploader(
    "Upload filled Excel (feedback_qty) — DEV preview, will NOT be submitted",
    type=["xlsx"]
)

if uploaded_file:
    df_upload = pd.read_excel(uploaded_file)

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
    # SUBMISSION — REMOVED IN DEV.
    #
    # Production's "Submit Feedback" button and its bq_client.insert_rows_json(...)
    # call intentionally do not exist below this line. The button is rendered
    # disabled so a tester sees where it would be, but no click handler and no
    # BigQuery write call are wired to it — there is nothing to bypass.
    # --------------------------------------------------
    st.button(
        "🚫 Submit Feedback — disabled in DEV",
        disabled=True,
        help="Submission is physically removed from this build. "
             "No row can be written to po_portal_feedback from here.",
    )
    st.caption(
        "In production, clicking Submit here would insert these rows into "
        f"`{PROJECT_ID}.{DATASET}.{FEEDBACK_TABLE}`. That call does not exist "
        "in this DEV file."
    )
