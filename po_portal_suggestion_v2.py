import time
import uuid
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# --------------------------------------------------
# Page Config
# --------------------------------------------------
st.set_page_config(page_title="PO Portal Suggestion", layout="wide")

PROJECT_ID = st.secrets["bigquery"]["project"]
DATASET = st.secrets["bigquery"]["dataset"]

PO_TABLE = "po_portal_suggestion"
FEEDBACK_TABLE = "po_portal_feedback"
USER_TABLE = "po_portal_distributor_users"


# --------------------------------------------------
# BigQuery Client
# --------------------------------------------------
# v2: was built inline at module level (re-run on every interaction,
# every user). Wrapping it in @st.cache_resource means the credential
# parsing + client construction happens once and is reused — same pattern
# already used in po_simulator.py, po_simulator_v2.py, and
# po_portal/utils/bq_ops.py in this repo.
@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    gcp_secrets = dict(st.secrets["connections"]["bigquery"])
    gcp_secrets["private_key"] = gcp_secrets["private_key"].replace("\\n", "\n")
    credentials = service_account.Credentials.from_service_account_info(gcp_secrets)
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


bq_client = get_bq_client()


# v2: small retry/backoff wrapper so a transient BigQuery hiccup surfaces
# as a friendly message + one retry instead of an immediate hard crash.
def run_query(query: str, job_config: bigquery.QueryJobConfig | None = None,
             retries: int = 2, backoff: float = 1.5) -> pd.DataFrame:
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return bq_client.query(query, job_config=job_config).to_dataframe()
        except Exception as exc:  # noqa: BLE001 - intentionally broad: any BQ failure should retry/report the same way
            last_exc = exc
            if attempt < retries:
                time.sleep(backoff ** attempt)
    st.error("⚠️ Unable to load data right now. Please try again in a moment.")
    raise last_exc


# --------------------------------------------------
# Login
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

    try:
        df = bq_client.query(query, job_config=job_config).to_dataframe()
    except Exception:
        st.error("⚠️ Unable to verify login right now. Please try again in a moment.")
        st.stop()

    if df.empty:
        return None

    # NOTE: this compares the input directly against a column named
    # "password_hash" — see analysis/streamlit_architecture.md for a
    # security observation about this. Left unchanged here: that's a
    # separate decision from the performance/stability scope of this
    # file, not something to silently alter.
    stored_password = str(df.loc[0, "password_hash"]).strip()

    if password == stored_password:
        return df.loc[0, "distributor_company"]

    return None


# --------------------------------------------------
# Load PO Suggestion
# --------------------------------------------------
# v2: now takes `company` and filters server-side (WHERE clause) instead
# of fetching every distributor's rows and filtering in pandas afterward.
# Pass company=None for Admin (all rows) — same pattern already used in
# po_portal/utils/bq_ops.py's load_po_suggestion().
@st.cache_data(ttl=600, max_entries=20)
def load_po_suggestion(company: str | None):
    where = "" if not company else "WHERE distributor_company = @company"
    params = [] if not company else [
        bigquery.ScalarQueryParameter("company", "STRING", company)
    ]

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
        {where}
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    df = run_query(query, job_config=job_config)

    for col in ["region", "distributor_branch", "distributor_company"]:
        df[col] = df[col].astype(str).str.strip()

    return df


# --------------------------------------------------
# Load PO Tracking Data
# --------------------------------------------------
# v2: accepts an optional date range and pushes it into the SQL query.
# Default usage (see the UI section below) bounds this to the last 90
# days instead of fetching the entire materialized view unconditionally.
# The leading-wildcard LIKE for non-Admin company matching is left as-is
# — replacing it needs confirmation that an exact-match company
# identifier exists in the schema, which isn't something to guess at here.
@st.cache_data(ttl=600, max_entries=20)
def load_po_tracking(company, start_date=None, end_date=None):
    conditions = []
    params = []

    if company and company != "Admin":
        conditions.append("LOWER(distributor_name) LIKE CONCAT('%', LOWER(@company), '%')")
        params.append(bigquery.ScalarQueryParameter("company", "STRING", company))

    if start_date:
        conditions.append("order_date >= @start_date")
        params.append(bigquery.ScalarQueryParameter("start_date", "DATE", start_date))

    if end_date:
        conditions.append("order_date <= @end_date")
        params.append(bigquery.ScalarQueryParameter("end_date", "DATE", end_date))

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    query = f"""
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
        {where}
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    df = run_query(query, job_config=job_config)

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    return df


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

logged_company = st.session_state["distributor_company"]
st.caption(f"Logged in as: {logged_company}")
st.caption("🆕 v2 — performance & reliability enhancements (see analysis/ for details)")

# 🔒 Row level security — now applied server-side inside load_po_suggestion()
po_df = load_po_suggestion(None if logged_company == "Admin" else logged_company)

st.title("📦 PO Portal Suggestion")



# --------------------------------------------------
# FILTERS (CASCADED)
# --------------------------------------------------
with st.expander("🔍 Filter", expanded=True):

    col1, col2, col3 = st.columns(3)

    # ---------------------------
    # REGION FILTER
    # ---------------------------
    region_options = sorted(
        po_df["region"].dropna().unique()
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
            po_df[po_df["region"].isin(selected_regions)]
            ["distributor_company"]
            .dropna()
            .unique()
        )
    else:
        company_options = (
            po_df["distributor_company"]
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
    temp_df = po_df.copy()

    if selected_regions:
        temp_df = temp_df[temp_df["region"].isin(selected_regions)]

    if selected_companies:
        temp_df = temp_df[temp_df["distributor_company"].isin(selected_companies)]

    branch_options = (
        temp_df["distributor_branch"]
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
filtered_df = po_df.copy()

if selected_regions:
    filtered_df = filtered_df[
        filtered_df["region"].isin(selected_regions)
    ]

if selected_companies:
    filtered_df = filtered_df[
        filtered_df["distributor_company"].isin(selected_companies)
    ]

if selected_branches:
    filtered_df = filtered_df[
        filtered_df["distributor_branch"].isin(selected_branches)
    ]

# --------------------------------------------------
# DISPLAY TABLE
# --------------------------------------------------
display_df = filtered_df.copy()
display_df["feedback_qty"] = ""

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True
)

# --------------------------------------------------
# DOWNLOAD EXCEL
# --------------------------------------------------
# v2: only generates the Excel file when explicitly requested, instead of
# on every single rerun (every filter change) regardless of use.
if st.button("📥 Prepare PO Suggestion Excel"):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        display_df.to_excel(writer, index=False, sheet_name="po_suggestion")
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

# v2: date range is captured BEFORE querying and pushed into the SQL
# WHERE clause, instead of fetching the entire table/view and filtering
# by date in pandas afterward. Defaults to the last 90 days; widen it if
# you need older history.
_default_end = datetime.now().date()
_default_start = _default_end - timedelta(days=90)

date_range = st.date_input(
    "Order Date Range (defaults to the last 90 days — widen if needed)",
    value=(_default_start, _default_end),
    format="YYYY/MM/DD"
)

if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
elif date_range:
    start_date = end_date = date_range
else:
    start_date = end_date = None

tracking_df = load_po_tracking(logged_company, start_date, end_date)

# -------------------------
# FILTERS
# -------------------------
colA, colB = st.columns(2)

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
# v2: same fix as the PO Suggestion export — only build the file on request.
if st.button("📥 Prepare PO Tracking Excel"):
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
        try:
            errors = bq_client.insert_rows_json(
                f"{PROJECT_ID}.{DATASET}.{FEEDBACK_TABLE}",
                records,
                row_ids=[None] * len(records),
                skip_invalid_rows=True
            )
        except Exception:
            st.error("⚠️ Unable to submit feedback right now. Please try again in a moment.")
            st.stop()

        if errors:
            st.error("❌ Failed to insert feedback")
            for err in errors:
                st.write(err)
        else:
            st.success("✅ Feedback successfully submitted")
