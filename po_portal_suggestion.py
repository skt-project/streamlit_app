import streamlit as st
import pandas as pd
import uuid
from io import BytesIO
from pendulum import now
from google.oauth2 import service_account
from google.cloud import bigquery

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

bq_client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)

# --------------------------------------------------
# Load PO Suggestion
# --------------------------------------------------
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
    """
    df = bq_client.query(query).to_dataframe()

    # normalize text columns (IMPORTANT for filters)
    for col in ["region", "distributor_branch"]:
        df[col] = df[col].astype(str).str.strip()

    return df


po_df = load_po_suggestion()

st.title("📦 PO Portal Suggestion")

# --------------------------------------------------
# FILTERS (CASCADED)
# --------------------------------------------------
with st.expander("🔍 Filter", expanded=True):

    col1, col2 = st.columns(2)

    # REGION FILTER
    region_options = sorted(po_df["region"].dropna().unique())
    selected_regions = col1.multiselect(
        "Region",
        options=region_options
    )

    # DISTRIBUTOR FILTER (DEPEND ON REGION)
    if selected_regions:
        distributor_options = (
            po_df[po_df["region"].isin(selected_regions)]
            ["distributor_branch"]
            .dropna()
            .unique()
        )
    else:
        distributor_options = po_df["distributor_branch"].dropna().unique()

    distributor_options = sorted(distributor_options)

    selected_distributors = col2.multiselect(
        "Distributor",
        options=distributor_options
    )

# --------------------------------------------------
# APPLY FILTER
# --------------------------------------------------
filtered_df = po_df.copy()

if selected_regions:
    filtered_df = filtered_df[
        filtered_df["region"].isin(selected_regions)
    ]

if selected_distributors:
    filtered_df = filtered_df[
        filtered_df["distributor_branch"].isin(selected_distributors)
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

    raw_feedback = df_upload["feedback_qty"].astype(str).str.strip()

    invalid_mask = (
        raw_feedback.notna()
        & raw_feedback.ne("")
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
            df_upload[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0)
            .astype("int64")
        )

    for col in float_cols:
        df_upload[col] = (
            df_upload[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0.0)
            .astype("float64")
        )

    # --------------------------------------------------
    # ADD SUBMISSION METADATA
    # --------------------------------------------------
    submission_id = str(uuid.uuid4())
    submitted_at = (
        now("Asia/Jakarta")
        .naive()
        .format("YYYY-MM-DD HH:mm:ss")
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

    df_upload["feedback_qty"] = (
        df_upload["feedback_qty"]
        .fillna(0)
        .astype(int)
    )

    # Convert other NaN values to None for BigQuery
    df_upload = df_upload.where(pd.notna(df_upload), None)

    # Prepare payload
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
            st.json(errors)
        else:
            st.success("✅ Feedback successfully submitted")
