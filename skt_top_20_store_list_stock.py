import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery, storage
from pendulum import timezone, now

# ------------------------------------
# Page Config
# ------------------------------------
st.set_page_config(page_title="Top 20 Store Stock Opname", layout="wide")
jakarta_tz = timezone("Asia/Jakarta")

# ------------------------------------
# Secrets & Client
# ------------------------------------
gcp_secrets = dict(st.secrets["connections"]["bigquery"])
gcp_secrets["private_key"] = gcp_secrets["private_key"].replace("\\n", "\n")
credentials = service_account.Credentials.from_service_account_info(gcp_secrets)

PROJECT_ID = st.secrets["bigquery"]["project"]
DATASET = "gt_schema"
STORE_TABLE = st.secrets["bigquery"]["store_table"]
OUTPUT_TABLE = st.secrets["bigquery"]["output_table"]

BUCKET_NAME = st.secrets["gcs"]["bucket_name"]
FOLDER_PREFIX = st.secrets["gcs"].get("folder_prefix", "stock_opname_top20")

bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
gcs_client = storage.Client(credentials=credentials, project=PROJECT_ID)

# ------------------------------------
# TOP20 SKU MASTER
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
    ("SKINTIFIC","SKINTIFIC-23","AHA BHA PHA EXFOLIATING PADS"),
    ("SKINTIFIC","SKINTIFIC-3261","COVER GLOW PERFECT CUSHION 01 VANILLA"),
    ("SKINTIFIC","SKINTIFIC-411","NIACINAMIDE BRIGHTENING MICELLAR WATER"),
    ("SKINTIFIC","SKINTIFIC-413","5X CERAMIDE BARRIER MICELLAR WATER"),
    ("SKINTIFIC","SKINTIFIC-17102","SKINTIFIC DAILY FILTER PERFECT SKIN TINT 02 IVORY"),
    ("SKINTIFIC","SKINTIFIC-1602","SKINTIFIC ULTRA COVER POWDER FOUNDATION 02 IVORY"),
    ("SKINTIFIC","SKINTIFIC-1603","SKINTIFIC ULTRA COVER POWDER FOUNDATION 03 PETAL"),
    ("SKINTIFIC","SKINTIFIC-17103","SKINTIFIC DAILY FILTER PERFECT SKIN TINT 03 PETAL"),
    ("SKINTIFIC","SKINTIFIC-161","SKINTIFIC LOCK THE LOOK SETTING SPRAY"),
    ("SKINTIFIC","SKINTIFIC-389","RADIANCE BOOST SERUM SPRAY"),
    ("SKINTIFIC","SKINTIFIC-3264","COVER GLOW PERFECT CUSHION 04 BEIGE"),
    ("SKINTIFIC","SKINTIFIC-17101","SKINTIFIC DAILY FILTER PERFECT SKIN TINT 01 VANILLA"),
    ("SKINTIFIC","SKINTIFIC-17103A","SKINTIFIC DAILY FILTER PERFECT SKIN TINT 03A ALMOND"),
    ("SKINTIFIC","SKINTIFIC-1603A","SKINTIFIC ULTRA COVER POWDER FOUNDATION 03A ALMOND"),
    ("SKINTIFIC","SKINTIFIC-1601","SKINTIFIC ULTRA COVER POWDER FOUNDATION 01 VANILLA"),
    ("SKINTIFIC","SKINTIFIC-17104","SKINTIFIC DAILY FILTER PERFECT SKIN TINT 04 BEIGE"),
    ("SKINTIFIC","SKINTIFIC-1604","SKINTIFIC ULTRA COVER POWDER FOUNDATION 04 BEIGE"),
    ("SKINTIFIC","SKINTIFIC-17100","SKINTIFIC DAILY FILTER PERFECT SKIN TINT 00 PORCELAIN"),
    ("SKINTIFIC","SKINTIFIC-1600","SKINTIFIC ULTRA COVER POWDER FOUNDATION 00 PORCELAIN"),
]

product_df = pd.DataFrame(TOP20_SKU, columns=["brand","sku","product_name"])

# ------------------------------------
# Load Store Mapping
# ------------------------------------
@st.cache_data(ttl=600)
def load_top20_store():
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

store_df = load_top20_store()

# ------------------------------------
# UI FILTER
# ------------------------------------
st.title("📦 Top 20 Store Stock Opname")

region = st.selectbox("Pilih Region", ["- Pilih Region -"] + sorted(store_df["region"].unique()))
df_region = store_df[store_df["region"] == region] if region != "- Pilih Region -" else pd.DataFrame()

spv = st.selectbox("Pilih SPV", ["- Pilih SPV -"] + sorted(df_region["spv"].dropna().unique())) if not df_region.empty else "- Pilih SPV -"
df_spv = df_region[df_region["spv"] == spv] if spv != "- Pilih SPV -" else pd.DataFrame()

dist = st.selectbox("Pilih Distributor", ["- Pilih Distributor -"] + sorted(df_spv["dist_name"].dropna().unique())) if not df_spv.empty else "- Pilih Distributor -"
df_dist = df_spv[df_spv["dist_name"] == dist] if dist != "- Pilih Distributor -" else pd.DataFrame()

store_select = st.selectbox("Pilih Store", ["- Pilih Store -"] + df_dist.apply(lambda r: f"{r['store_id_st']} - {r['store_name']}", axis=1).tolist()) if not df_dist.empty else "- Pilih Store -"

# ------------------------------------
# SKU INPUT
# ------------------------------------
if all([region != "- Pilih Region -", spv != "- Pilih SPV -", dist != "- Pilih Distributor -", store_select != "- Pilih Store -"]):

    store_id, store_name = store_select.split(" - ", 1)
    store_brand = df_dist[df_dist["store_id_st"] == store_id]["brand"].values[0]

    st.success(f"✅ Store siap input | Brand: {store_brand}")

    sku_quantities = {}
    total_qty = 0

    for _, row in product_df[product_df["brand"] == store_brand].iterrows():
        qty = st.number_input(f"{row['sku']} - {row['product_name']}", min_value=0, step=1, key=row["sku"])
        sku_quantities[row["sku"]] = {"product_name": row["product_name"],"brand": row["brand"],"quantity": int(qty)}
        total_qty += int(qty)

    st.metric("Total Qty", total_qty)

    uploaded_files = st.file_uploader("Upload Dokumen Pendukung", type=["jpg","png","jpeg","pdf"], accept_multiple_files=True)

    if st.button("🚀 Submit Stock Opname"):
        if total_qty == 0:
            st.warning("Isi minimal 1 SKU.")
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

            records = [{
                "submission_id": submission_id,
                "submitted_at": submitted_at,
                "region": region,
                "spv": spv,
                "dist_name": dist,
                "store_id": store_id,
                "store_name": store_name,
                "sku": k,
                "product_name": v["product_name"],
                "brand": v["brand"],
                "quantity": v["quantity"],
                "docs": ", ".join(doc_urls) if doc_urls else None
            } for k,v in sku_quantities.items() if v["quantity"] > 0]

            errors = bq_client.insert_rows_json(f"{PROJECT_ID}.{DATASET}.{OUTPUT_TABLE}", records)
            if errors:
                st.error(errors)
            else:
                st.success("🎉 Stock Opname Berhasil Disimpan")
                st.dataframe(pd.DataFrame(records))
else:
    st.info("Silakan pilih Region → SPV → Distributor → Store")
