import streamlit as st
import pandas as pd
import uuid
from io import BytesIO
from pendulum import timezone, now
from google.oauth2 import service_account
from google.cloud import bigquery

# ------------------------------------
# Page Config
# ------------------------------------
st.set_page_config(page_title="PO Portal Suggestion", layout="wide")
jakarta_tz = timezone("Asia/Jakarta")

# ------------------------------------
# BigQuery Client
# ------------------------------------
gcp_secrets = dict(st.secrets["connections"]["bigquery"])
gcp_secrets["private_key"] = gcp_secrets["private_key"].replace("\\n", "\n")

credentials = service_account.Credentials.from_service_account_info(gcp_secrets)

PROJECT_ID = st.secrets["bigquery"]["project"]
DATASET = st.secrets["bigquery"]["dataset"]
PO_TABLE = "po_portal_suggestion"
FEEDBACK_TABLE = "po_portal_feedback"

bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# ------------------------------------
# Load PO Suggestion
# ------------------------------------
@st.cache_data(ttl=600)
def load_po_suggestion():
    query = f"""
        SELECT
            sku_status,
            brand,
            region,
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
            ideal_weekly_po_qty,
            max_weekly_po_qty,
            min_weekly_po_qty
        FROM `{PROJECT_ID}.{DATASET}.{PO_TABLE}`
        ORDER BY region, distributor_branch, product_name
    """
    return bq_client.query(query).to_dataframe()

po_df = load_po_suggestion()

st.title("📦 PO Portal Suggestion")

# ------------------------------------
# Filters (CASCADING)
# ------------------------------------
with st.expander("🔍 Filter", expanded=True):

    col1, col2 = st.columns(2)

    # ---- REGION FILTER ----
    with col1:
        region_options = sorted(po_df["region"].dropna().unique())
        filter_region = st.multiselect(
            "Region",
            options=region_options
        )

    # ---- APPLY REGION FILTER FIRST ----
    if filter_region:
        region_filtered_df = po_df[
            po_df["region"].isin(filter_region)
        ]
    else:
        region_filtered_df = po_df.copy()

    # ---- DISTRIBUTOR FILTER (DEPENDS ON REGION) ----
    with col2:
        distributor_options = sorted(
            region_filtered_df["distributor_branch"]
            .dropna()
            .unique()
        )

        filter_distributor = st.multiselect(
            "Distributor",
            options=distributor_options
        )

    # ---- FINAL FILTERING ----
    filtered_df = region_filtered_df.copy()

    if filter_distributor:
        filtered_df = filtered_df[
            filtered_df["distributor_branch"].isin(filter_distributor)
        ]

# ------------------------------------
# Prepare Download Data
# ------------------------------------
download_df = filtered_df.copy()
download_df["feedback_qty"] = ""

st.dataframe(
    download_df,
    use_container_width=True,
    hide_index=True
)

# ------------------------------------
# Download Excel
# ------------------------------------
output = BytesIO()
with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
    download_df.to_excel(writer, index=False, sheet_name="po_suggestion")

output.seek(0)

st.download_button(
    label="📥 Download PO Suggestion (Excel)",
    data=output,
    file_name="po_portal_suggestion.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# ------------------------------------
# Upload Feedback
# ------------------------------------
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
        "ideal_weekly_po_qty",
        "max_weekly_po_qty",
        "min_weekly_po_qty",
        "feedback_qty"
    ]

    missing = [c for c in required_cols if c not in df_upload.columns]
    if missing:
        st.error(f"❌ Missing columns: {missing}")
        st.stop()

    # ------------------------------------
    # CLEAN NUMERIC DATA
    # ------------------------------------
    int_cols = [
        "current_stock_friday",
        "in_transit_stock",
        "total_stock",
        "moq",
        "standard_woi",
        "avg_weekly_st_l3m",
        "avg_weekly_st_lm",
        "si_target",
        "ideal_weekly_po_qty",
        "max_weekly_po_qty",
        "min_weekly_po_qty",
        "feedback_qty"
    ]

    float_cols = ["current_woi"]

    for col in int_cols:
        df_upload[col] = (
            pd.to_numeric(df_upload[col], errors="coerce")
            .fillna(0)
            .astype(int)
        )

    for col in float_cols:
        df_upload[col] = (
            pd.to_numeric(df_upload[col], errors="coerce")
            .fillna(0.0)
            .astype(float)
        )

    # ------------------------------------
    # ADD SUBMISSION METADATA
    # ------------------------------------
    submission_id = str(uuid.uuid4())
    submitted_at = (
        now(jakarta_tz)
        .naive()
        .isoformat(sep=" ")
    )
    
    df_upload["submission_id"] = submission_id
    df_upload["submitted_at"] = submitted_at
    
    final_cols = [
        "submission_id",
        "submitted_at",
        "sku_status",
        "brand",
        "region",
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
        "ideal_weekly_po_qty",
        "max_weekly_po_qty",
        "min_weekly_po_qty",
        "feedback_qty"
    ]

    records = df_upload[final_cols].to_dict("records")

    # ------------------------------------
    # INSERT TO BIGQUERY
    # ------------------------------------
    errors = bq_client.insert_rows_json(
        f"{PROJECT_ID}.{DATASET}.{FEEDBACK_TABLE}",
        records,
        row_ids=[None] * len(records)
    )

    if errors:
        st.error(errors)
    else:
        st.success("✅ Feedback successfully submitted to BigQuery")
        st.dataframe(df_upload, use_container_width=True)
