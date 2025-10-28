import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery, storage
from pendulum import timezone, now

# ---------------------------
# Streamlit Config
# ---------------------------
st.set_page_config(page_title="📦 Stock Opname Entry", layout="wide")
jakarta_tz = timezone("Asia/Jakarta")

# ---------------------------
# Setup GCP Connections
# ---------------------------
try:
    # BigQuery
    gcp_secrets = st.secrets["connections"]["bigquery"]
    if "private_key" in gcp_secrets:
        gcp_secrets = dict(gcp_secrets)
        gcp_secrets["private_key"] = gcp_secrets["private_key"].replace("\\n", "\n")
    credentials = service_account.Credentials.from_service_account_info(gcp_secrets)

    PROJECT_ID = st.secrets["bigquery"]["project"]
    DATASET = st.secrets["bigquery"]["dataset"]
    STORE_TABLE = st.secrets["bigquery"]["store_table"]
    PRODUCT_TABLE = st.secrets["bigquery"]["product_table"]
    OUTPUT_TABLE = st.secrets["bigquery"]["output_table"]

    # GCS
    BUCKET_NAME = st.secrets["gcs"]["bucket_name"]

except Exception as e:
    st.error(f"Gagal membaca secrets.toml: {e}")
    st.stop()

bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
gcs_client = storage.Client(credentials=credentials, project=PROJECT_ID)

# ---------------------------
# Load Reference Data
# ---------------------------
@st.cache_data(ttl=600)
def load_store_data():
    query = f"""
        SELECT DISTINCT
            region, spv_skt, cust_id, store_name
        FROM `{PROJECT_ID}.{DATASET}.{STORE_TABLE}`
        WHERE region IN ('Southern Sumatera 1','Southern Sumatera 2','Jakarta (Csa)')
          AND cust_id IS NOT NULL
          AND store_name IS NOT NULL
    """
    return bq_client.query(query).to_dataframe()

@st.cache_data(ttl=600)
def load_product_data():
    query = f"""
        SELECT DISTINCT
            sku, product_name, brand
        FROM `{PROJECT_ID}.{DATASET}.{PRODUCT_TABLE}`
        WHERE brand IN ('SKINTIFIC','TIMEPHORIA','FACERINNA')
    """
    return bq_client.query(query).to_dataframe()

with st.spinner("🔄 Loading reference data from BigQuery..."):
    store_df = load_store_data()
    product_df = load_product_data()

# ---------------------------
# UI Selections
# ---------------------------
st.title("📦 Stock Opname Entry Form")

region = st.selectbox(
    "Pilih Region",
    options=["- Pilih Region -"] + sorted(store_df["region"].unique().tolist())
)
df_region = store_df[store_df["region"] == region] if region not in ["- Pilih Region -", ""] else pd.DataFrame()

# Exclude blank or null SPV
if not df_region.empty:
    valid_spv_list = sorted([spv for spv in df_region["spv_skt"].dropna().unique().tolist() if spv.strip() != ""])
else:
    valid_spv_list = []

spv = st.selectbox(
    "Pilih SPV",
    options=["- Pilih SPV -"] + valid_spv_list
) if valid_spv_list else "- Pilih SPV -"

df_spv = df_region[df_region["spv_skt"] == spv] if spv not in ["", "- Pilih SPV -"] else pd.DataFrame()

store_select = st.selectbox(
    "Pilih Store (Cust ID + Store Name)",
    options=["- Pilih Store -"] + [
        f"{r['cust_id']} - {r['store_name']}" for _, r in df_spv.iterrows()
    ]
) if not df_spv.empty else "- Pilih Store -"

# ---------------------------
# SKU Input
# ---------------------------
if all([
    region not in ["", "- Pilih Region -"],
    spv not in ["", "- Pilih SPV -"],
    store_select not in ["", "- Pilih Store -"]
]):
    st.success("✅ Semua pilihan lengkap. Silakan isi quantity SKU di bawah.")

    st.subheader("📥​ Input Quantity per SKU")

    sku_quantities = {}
    total_qty = 0

    # Group products by brand for easier navigation
    for brand in ["SKINTIFIC", "TIMEPHORIA", "FACERINNA"]:
        brand_products = product_df[product_df["brand"] == brand]

        if not brand_products.empty:
            st.markdown(f"### 🧴 {brand}")
            for _, row in brand_products.iterrows():
                sku_label = f"{row['sku']} - {row['product_name']}"
                qty = st.number_input(sku_label, min_value=0, value=0, step=1, key=row["sku"])
                sku_quantities[row["sku"]] = {
                    "product_name": row["product_name"],
                    "brand": row["brand"],
                    "quantity": int(qty)
                }
                total_qty += int(qty)

    st.metric("Total Quantity Input", total_qty)

    # ---------------------------
    # File Upload Section
    # ---------------------------
    st.subheader("📎 Upload Dokumen Pendukung (Opsional)")
    uploaded_files = st.file_uploader(
        "Unggah file (foto, dokumen, bukti, dll)",
        type=["jpg", "jpeg", "png", "pdf"],
        accept_multiple_files=True
    )

    # ---------------------------
    # Submit Button
    # ---------------------------
    if st.button("🚀 Submit Stock Opname"):
        if total_qty == 0:
            st.warning("⚠️ Harap isi minimal satu SKU dengan quantity > 0.")
        else:
            try:
                submission_id = str(uuid.uuid4())
                cust_id, store_name = store_select.split(" - ", 1)
                submitted_at = now(jakarta_tz).to_datetime_string()

                # Upload files to GCS
                doc_urls = []
                if uploaded_files:
                    bucket = gcs_client.bucket(BUCKET_NAME)
                    for file in uploaded_files:
                        filename = f"stock_opname/{cust_id}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}_{file.name}"
                        blob = bucket.blob(filename)
                        blob.upload_from_file(file, content_type=file.type)
                        blob.make_public()
                        doc_urls.append(blob.public_url)

                # Prepare records for BigQuery
                records = [
                    {
                        "submission_id": submission_id,
                        "submitted_at": submitted_at,
                        "region": region,
                        "spv": spv,
                        "cust_id": cust_id,
                        "store_name": store_name,
                        "sku": sku,
                        "product_name": data["product_name"],
                        "brand": data["brand"],
                        "quantity": data["quantity"],
                        "docs": ", ".join(doc_urls) if doc_urls else None
                    }
                    for sku, data in sku_quantities.items()
                    if data["quantity"] > 0
                ]

                # Insert into BigQuery
                table_id = f"{PROJECT_ID}.{DATASET}.{OUTPUT_TABLE}"
                errors = bq_client.insert_rows_json(table_id, records)
                if errors:
                    raise RuntimeError(errors)

                st.success(f"✅ Stock opname berhasil disubmit untuk {store_name}")
                with st.expander("📋 Detail Submission"):
                    st.write(pd.DataFrame(records))

            except Exception as e:
                st.error(f"Gagal submit ke BigQuery: {e}")

else:
    st.info("👆 Silakan pilih Region, SPV, dan Store terlebih dahulu.")

# ---------------------------
# Help Section
# ---------------------------
st.markdown("---")
with st.expander("ℹ️ Panduan Pengisian"):
    st.markdown("""
    ### Langkah Pengisian:
    1. Pilih **Region**
    2. Pilih **SPV**
    3. Pilih **Store**
    4. Isi **Quantity SKU**
    5. (Opsional) Upload **Dokumen Pendukung**
    6. Klik **Submit Stock Opname**
    
    File akan otomatis tersimpan di **GCS**, dan link-nya tercatat di BigQuery.
    """)
