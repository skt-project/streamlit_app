import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery, storage
from pendulum import timezone, now
from io import BytesIO


# ------------------------------------
# Page Config
# ------------------------------------
st.set_page_config(page_title="Store Stock Form", layout="wide")
jakarta_tz = timezone("Asia/Jakarta")

# ------------------------------------
# Secrets & Client
# ------------------------------------
gcp_secrets = st.secrets["connections"]["bigquery"]

private_key = gcp_secrets["private_key"].replace("\\n", "\n")
st.write(
    st.secrets["connections"]["bigquery"]["private_key"]
    .startswith("-----BEGIN PRIVATE KEY-----\n")
)

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["connections"]["bigquery"]
)

PROJECT_ID = st.secrets["bigquery"]["project"]
DATASET = st.secrets["bigquery"]["dataset"]
STORE_TABLE = st.secrets["bigquery"]["store_table"]
OUTPUT_TABLE = st.secrets["bigquery"]["output_table"]
PO_SUGGESTION_TABLE = "skt_top20_po_suggestion"

BUCKET_NAME = st.secrets["gcs"]["bucket_name"]
FOLDER_PREFIX = st.secrets["gcs"]["folder_prefix"]

bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
gcs_client = storage.Client(credentials=credentials, project=PROJECT_ID)

# ------------------------------------
# TOP 20 SKU MASTER
# ------------------------------------
TOP20_SKU = [
    ("TIMEPHORIA","TCC102006","TIMEPHORIA STELLAR DUST LIP STAIN NEROSE"),
    ("TIMEPHORIA","TCC102007","TIMEPHORIA STELLAR DUST LIP STAIN QUANTA"),
    ("TIMEPHORIA","TQD116003","TIMEPHORIA TIMELESS LUMINA MATTE PERFECTION CUSHION 03 FAWN"),
    ("TIMEPHORIA","TCC103004","TIMEPHORIA ETERNAL LIP MATTE CHARM 04"),
    ("TIMEPHORIA","TCC103006","TIMEPHORIA ETERNAL LIP MATTE HEX 06"),
    ("TIMEPHORIA","TYX109001","TIMEPHORIA DUNE HYPER-PRECISION SUPERSTAY EYELINER BLACK"),
    ("TIMEPHORIA","TFB122002","TIMEPHORIA TIMELESS OPTIMA COVER-BLUR SKIN PERFECTION POWDER FOUNDATION 002 BIRCH"),
    ("TIMEPHORIA","TCC140006","TIMEPHORIA ALTERA BLURRING LIP TINT 006 WISP"),
    ("TIMEPHORIA","TCP133006","TIMEPHORIA ORBITA LIP AND CHEEK BLURRING POT 006 HALLEY"),
    ("SKINTIFIC","SKINTIFIC-39","SKINTIFIC Symwhite 377 Dark Spot Moisture Gel"),
    ("SKINTIFIC","SKINTIFIC-21","SKINTIFIC NIACINAMIDE BRIGHTENING MOISTURE GEL"),
    ("SKINTIFIC","SKINTIFIC-17","SKINTIFIC-SYMWHITE 377 DARK SPOT SERUM 20ML"),
    ("SKINTIFIC","SKINTIFIC-31001","NIACINAMIDE BRIGHTENING DAILY MASK"),
    ("SKINTIFIC","SKINTIFIC-3262","COVER GLOW PERFECT CUSHION 02 IVORY"),
    ("SKINTIFIC","SKINTIFIC-170","SKINTIFIC RADIANCE BOOSTER SERUM SPRAY"),
    ("SKINTIFIC","SKINTIFIC-04","SKINTIFIC 10% NIACINAMIDE BRIGHTENING SERUM 20ML"),
    ("SKINTIFIC","SKINTIFIC-3263","COVER GLOW PERFECT CUSHION 03 PETAL"),
    ("SKINTIFIC","SKINTIFIC-183","SKINTIFIC RETINOL SKIN RENEWAL MOISTURIZER"),
    ("SKINTIFIC","SKINTIFIC-180","SKINTIFIC RETINOL SKIN RENEWAL SERUM"),
]

product_df = pd.DataFrame(TOP20_SKU, columns=["brand","sku_id","sku_name"])

# ------------------------------------
# Load Store Mapping
# ------------------------------------
@st.cache_data(ttl=600)
def load_store():
    query = f"""
        SELECT DISTINCT
            UPPER(region) AS region,
            spv,
            dist_name,
            store_id_st,
            customer_registered_name AS store_name,
            brand
        FROM `{PROJECT_ID}.{DATASET}.{STORE_TABLE}`
        WHERE brand IS NOT NULL
    """
    return bq_client.query(query).to_dataframe()
def load_po_suggestion():
    query = f"""
        SELECT
            type,
            region,
            spv,
            customer_id,
            customer_name,
            customer_group,
            sku_code,
            sku_name,
            qty,
            st
        FROM `{PROJECT_ID}.{DATASET}.{PO_SUGGESTION_TABLE}`
        ORDER BY region, spv, customer_name
    """
    return bq_client.query(query).to_dataframe()

store_df = load_store()

if store_df.empty:
    st.error("❌ Store master kosong / tidak ditemukan")
    st.stop()

# ------------------------------------
# UI FILTER
# ------------------------------------

st.subheader("📊 PO Suggestion")

po_df = load_po_suggestion()

with st.expander("🔍 Lihat PO Suggestion", expanded=True):

    col1, col2, col3 = st.columns(3)

    with col1:
        filter_region = st.multiselect(
            "Filter Region",
            sorted(po_df["region"].unique())
        )

    with col2:
        filter_spv = st.multiselect(
            "Filter SPV",
            sorted(po_df["spv"].unique())
        )

    with col3:
        filter_customer = st.multiselect(
            "Filter Customer",
            sorted(po_df["customer_name"].unique())
        )

    filtered_po = po_df.copy()

    if filter_region:
        filtered_po = filtered_po[filtered_po["region"].isin(filter_region)]

    if filter_spv:
        filtered_po = filtered_po[filtered_po["spv"].isin(filter_spv)]

    if filter_customer:
        filtered_po = filtered_po[filtered_po["customer_name"].isin(filter_customer)]

    st.dataframe(
        filtered_po,
        use_container_width=True,
        hide_index=True
    )

    st.caption("📌 Data ini adalah referensi sebelum input stock opname")

st.title("📦Store Stock Form")

region = st.selectbox("Region", ["Pilih Region"] + sorted(store_df["region"].unique()))
df_region = store_df[store_df["region"] == region] if region != "-" else pd.DataFrame()

spv = st.selectbox("SPV", ["Pilih SPV"] + sorted(df_region["spv"].dropna().unique())) if not df_region.empty else "-"
df_spv = df_region[df_region["spv"] == spv] if spv != "-" else pd.DataFrame()

dist = st.selectbox("Distributor", ["Pilih Distributor"] + sorted(df_spv["dist_name"].dropna().unique())) if not df_spv.empty else "-"
df_dist = df_spv[df_spv["dist_name"] == dist] if dist != "-" else pd.DataFrame()

store_select = st.selectbox(
    "Store",
    ["-"] + df_dist.apply(lambda r: f"{r['store_id_st']} - {r['store_name']}", axis=1).tolist()
) if not df_dist.empty else "-"

# ------------------------------------
# SKU INPUT
# ------------------------------------
if store_select != "-":

    store_id, store_name = store_select.split(" - ", 1)
    store_brand = df_dist[df_dist["store_id_st"] == store_id]["brand"].iloc[0]

    st.success(f"Brand Store: **{store_brand}**")

    total_qty = 0
    sku_inputs = []

    for _, row in product_df[product_df["brand"] == store_brand].iterrows():
        qty = st.number_input(
            f"{row['sku_id']} - {row['sku_name']}",
            min_value=0,
            step=1,
            key=row["sku_id"]
        )
        if qty > 0:
            sku_inputs.append((row, qty))
            total_qty += qty

    st.metric("Total Qty", total_qty)

    uploaded_files = st.file_uploader("Upload Dokumen (Opsional)", type=["jpg","png","jpeg","pdf"], accept_multiple_files=True)

    if st.button("🚀 Submit"):
        if total_qty == 0:
            st.warning("Minimal 1 SKU diisi")
        else:
            submission_id = str(uuid.uuid4())
            submitted_at = now(jakarta_tz).to_datetime_string()

            doc_urls = []
            if uploaded_files:
                bucket = gcs_client.bucket(BUCKET_NAME)
                for f in uploaded_files:
                    blob = bucket.blob(f"{FOLDER_PREFIX}/{store_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{f.name}")
                    blob.upload_from_file(f, content_type=f.type)
                    blob.make_public()
                    doc_urls.append(blob.public_url)

            records = []
            for row, qty in sku_inputs:
                records.append({
                    "submission_id": submission_id,
                    "submitted_at": submitted_at,
                    "region": region,
                    "spv": spv,
                    "dist_name": dist,
                    "store_id": store_id,
                    "store_name": store_name,
                    "sku_id": row["sku_id"],
                    "sku_name": row["sku_name"],
                    "brand": row["brand"],
                    "quantity": int(qty),
                    "docs": ", ".join(doc_urls) if doc_urls else None
                })

            errors = bq_client.insert_rows_json(
                f"{PROJECT_ID}.{DATASET}.{OUTPUT_TABLE}",
                records
            )

            if errors:
                st.error(errors)
            else:
                st.success("🎉 Stock Submit Berhasil")

                result_df = pd.DataFrame(records)
                st.dataframe(result_df)

                # ------------------------------------
                # Download Excel
                # ------------------------------------
                output = BytesIO()
                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    result_df.to_excel(writer, index=False, sheet_name="stock_opname")

                output.seek(0)

                filename = f"stock_opname_{store_id}_{submission_id[:8]}.xlsx"

                st.download_button(
                    label="📥 Download Excel",
                    data=output,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
else:
    st.info("Pilih Region → SPV → Distributor → Store")
