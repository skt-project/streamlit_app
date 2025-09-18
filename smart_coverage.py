import streamlit as st
import pandas as pd
import uuid
import re
import time
from datetime import datetime, timezone
from google.oauth2 import service_account
from google.cloud import bigquery, storage
import gspread
from pendulum import timezone, now

# Set timezone to UTC+7
jakarta_tz = timezone("Asia/Jakarta")

st.set_page_config(page_title="TPH Smart Coverage", page_icon="📤", layout="wide")

# ---------------------------
# Config & Auth
# ---------------------------
try:
    gcp_secrets = st.secrets["connections"]["bigquery"]
    if "private_key" in gcp_secrets:
        gcp_secrets = dict(gcp_secrets)
        gcp_secrets["private_key"] = gcp_secrets["private_key"].replace("\\n", "\n")

    credentials = service_account.Credentials.from_service_account_info(gcp_secrets)
    GCP_PROJECT_ID = gcp_secrets.get("project_id") or st.secrets["bigquery"].get("project")
    BQ_DATASET = st.secrets["bigquery"]["dataset"]
    BQ_TABLE = st.secrets["bigquery"]["stock_analysis_table"]
    SPREADSHEET_KEY = st.secrets["spreadsheet"]["url"]
    _bucket_raw = st.secrets["bigquery"].get("public_skintific_storage", "public_skintific_storage/smart_coverage")
except Exception:
    GCP_CREDENTIALS_PATH = r"D:\script\skintific-data-warehouse-ea77119e2e7a.json"
    credentials = service_account.Credentials.from_service_account_file(GCP_CREDENTIALS_PATH)
    GCP_PROJECT_ID = "skintific-data-warehouse"
    BQ_DATASET = "gt_schema"
    BQ_TABLE = "smart_coverage"
    SPREADSHEET_KEY = "1E90Ogzx7VeD9E68Qq5OHqqf31T9scqIqE3QzyobdcbU"
    _bucket_raw = "public_skintific_storage/smart_coverage"

if "/" in _bucket_raw:
    BUCKET_NAME, BUCKET_PREFIX = _bucket_raw.split("/", 1)
else:
    BUCKET_NAME = _bucket_raw
    BUCKET_PREFIX = ""

# ---------------------------
# Clients
# ---------------------------
bq_client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)
gcs_client = storage.Client(credentials=credentials, project=GCP_PROJECT_ID)

try:
    if 'gcp_secrets' in locals() and isinstance(gcp_secrets, dict):
        gc = gspread.service_account_from_dict(gcp_secrets)
    else:
        gc = gspread.service_account(filename=GCP_CREDENTIALS_PATH)
except Exception:
    try:
        scoped_credentials = credentials.with_scopes([
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ])
        gc = gspread.authorize(scoped_credentials)
    except Exception as e:
        st.error(f"GSpread authorization failed: {e}")
        st.stop()

# ---------------------------
# Load reference data
# ---------------------------
@st.cache_data(ttl=300)
def load_reference_data():
    sh = gc.open_by_key(SPREADSHEET_KEY)
    ws = sh.worksheet("Smart Coverage")
    ref_df = pd.DataFrame(ws.get_all_records())

    required = ["region","distributor_company","distributor_name","cust_id","customer_name","spv","cluster"]
    missing = [c for c in required if c not in ref_df.columns]
    if missing:
        raise RuntimeError(f"Kolom wajib hilang: {missing}")

    def clean_col(s: pd.Series) -> pd.Series:
        return (s.astype(str)
                  .str.replace("\u00a0"," ",regex=False)
                  .str.strip()
                  .str.replace(r"\s+"," ",regex=True))

    for c in ["region","distributor_company","distributor_name","spv","customer_name","cluster"]:
        ref_df[c] = clean_col(ref_df[c])
    ref_df["cluster"] = ref_df["cluster"].str.title()
    ref_df["cust_id"] = ref_df["cust_id"].astype(str).str.strip()
    ref_df["CustKey"] = ref_df["cust_id"] + " - " + ref_df["customer_name"]
    return ref_df

with st.spinner("🔄 Mohon Tunggu..."):
    try:
        ref_df = load_reference_data()
    except Exception as e:
        st.error(str(e))
        st.stop()

# ---------------------------
# UI Title
# ---------------------------
st.title("📤 TPH Smart Coverage")
st.markdown("Silakan pilih **Region → Distributor Company → Distributor Name → SPV → Customer → Cluster → Input Quantity SKU** untuk upload dokumen.")

# ---------------------------
# SKU Reference
# ---------------------------
sku_data = """
TCC102006 TCC102007 TCC102004 TQD116002 TQD116003 TCC104001 TCC104002 TCC104009 TCC103004 TCC103005 TCC103006 TCC108005 TCC108012 TCC108001 TYX109001 TYX109002
Set 1 15 10 5 5 5 5 5 5 10 5 5 5 5 5 15 10
Set 2 10 10 0 0 0 5 0 0 10 5 0 5 5 0 5 5
Set 3 5 5 0 0 0 0 0 0 5 5 0 5 5 0 5 0
"""
def parse_sku_data(data_string: str):
    lines = [line.strip() for line in data_string.strip().split("\n") if line.strip()]
    sku_codes = lines[0].split()
    clusters = {}
    for line in lines[1:]:
        parts = line.split()
        if len(parts) >= 2:
            cluster_name = f"{parts[0]} {parts[1]}"
            quantities = [int(x) for x in parts[2:]]
            if len(quantities) < len(sku_codes):
                quantities = quantities + [0]*(len(sku_codes)-len(quantities))
            clusters[cluster_name] = quantities
    return sku_codes, clusters

sku_list, min_quantities_map = parse_sku_data(sku_data)

# SKU Names mapping
sku_names = {
    "TCC102006": "TIMEPHORIA STELLAR DUST LIP STAIN NEROSE",
    "TCC102007": "TIMEPHORIA STELLAR DUST LIP STAIN QUANTA",
    "TCC102004": "TIMEPHORIA STELLAR DUST LIP STAIN CHERRION",
    "TQD116002": "TIMEPHORIA TIMELESS LUMINA MATTE PERFECTION CUSHION 02 BIRCH",
    "TQD116003": "TIMEPHORIA TIMELESS LUMINA MATTE PERFECTION CUSHION 03 FAWN",
    "TCC104001": "TIMEPHORIA NEBULA VELVET LIP CREAM CORDELIA",
    "TCC104002": "TIMEPHORIA NEBULA VELVET LIP CREAM AURORA",
    "TCC104009": "TIMEPHORIA NEBULA VELVET LIP CREAM HELION",
    "TCC103004": "TIMEPHORIA ETERNAL LIP MATTE CHARM 04",
    "TCC103005": "TIMEPHORIA ETERNAL LIP MATTE ARCANE 05",
    "TCC103006": "TIMEPHORIA ETERNAL LIP MATTE HEX 06",
    "TCC108005": "TIMEPHORIA LUNARA FROST 3D LIP GLOSS ARTEMIS 005",
    "TCC108012": "TIMEPHORIA LUNARA FROST 3D LIP GLOSS DUNARA 012",
    "TCC108001": "TIMEPHORIA LUNARA FROST 3D LIP GLOSS MIRELLE 001",
    "TYX109001": "TIMEPHORIA DUNE HYPER-PRECISION SUPERSTAY EYELINER BLACK",
    "TYX109002": "TIMEPHORIA DUNE HYPER-PRECISION SUPERSTAY EYELINER BROWN"
}

# ---------------------------
# Dropdowns
# ---------------------------
region_opts = sorted(
    [r for r in ref_df["region"].dropna().unique().tolist() if r != "#N/A"]
)
region = st.selectbox("Pilih Region", ["- Pilih Region -"] + region_opts)

df_r = ref_df[ref_df["region"]==region] if region not in ["", "- Pilih Region -"] else pd.DataFrame()
distributor_company = st.selectbox("Pilih Distributor Company", ["- Pilih Distributor Company -"]+sorted(df_r["distributor_company"].unique())) if not df_r.empty else "- Pilih Distributor Company -"

df_dc = df_r[df_r["distributor_company"]==distributor_company] if distributor_company not in ["", "- Pilih Distributor Company -"] else pd.DataFrame()
distributor_name = st.selectbox("Pilih Distributor Name", ["- Pilih Distributor Name -"]+sorted(df_dc["distributor_name"].unique())) if not df_dc.empty else "- Pilih Distributor Name -"

df_dn = df_dc[df_dc["distributor_name"]==distributor_name] if distributor_name not in ["", "- Pilih Distributor Name -"] else pd.DataFrame()
spv = st.selectbox("Pilih SPV", ["- Pilih SPV -"]+sorted(df_dn["spv"].unique())) if not df_dn.empty else "- Pilih SPV -"

df_spv = df_dn[df_dn["spv"]==spv] if spv not in ["", "- Pilih SPV -"] else pd.DataFrame()
cust_key = st.selectbox("Pilih Customer ID & Name", ["- Pilih Customer -"]+sorted(df_spv["CustKey"].unique())) if not df_spv.empty else "- Pilih Customer -"

df_customer = df_spv[df_spv["CustKey"]==cust_key] if cust_key not in ["", "- Pilih Customer -"] else pd.DataFrame()

cluster = "- Pilih Cluster -"
if not df_customer.empty:
    current_cluster = df_customer["cluster"].iloc[0]
    if current_cluster=="Set 3":
        allowed_clusters=["Set 3","Set 2","Set 1"]
    elif current_cluster=="Set 2":
        allowed_clusters=["Set 2","Set 1"]
    else:
        allowed_clusters=["Set 1"]
    cluster = st.selectbox(f"Pilih Cluster (Toko ini ada di: {current_cluster})", ["- Pilih Cluster -"]+allowed_clusters)

# ---------------------------
# Input SKU & Upload
# ---------------------------
if all([region not in ["","- Pilih Region -"], distributor_company not in ["","- Pilih Distributor Company -"],
        distributor_name not in ["","- Pilih Distributor Name -"], spv not in ["","- Pilih SPV -"],
        cust_key not in ["","- Pilih Customer -"], cluster not in ["","- Pilih Cluster -"]]):

    st.success("✅ Semua pilihan sudah lengkap. Silakan Mengisi Quantity SKU Di Bawah.")
    selected_cluster = cluster
    cluster_minimums = min_quantities_map.get(selected_cluster,[0]*len(sku_list))

    sku_quantities={}
    validation_errors=[]
    st.subheader("📦 Input Quantity SKU ")
    for i,sku in enumerate(sku_list):
        min_qty=cluster_minimums[i]
        sku_label = f"{sku} - {sku_names.get(sku, '')}"
        qty=st.number_input(f"{sku_label} (Min {min_qty})",min_value=0,value=min_qty,step=1,key=f"sku_{sku}")
        sku_quantities[sku]=int(qty)
        if qty < min_qty:
            validation_errors.append(f"{sku}: {qty} < {min_qty}")
            st.error("SKU di bawah minimum")

    total_quantity = sum(sku_quantities.values())
    total_minimum = sum(cluster_minimums)
    st.metric("Total Quantity", total_quantity)
    st.metric("Total Minimum", total_minimum)

    # Valid jika tidak ada error
    is_valid_sku = len(validation_errors) == 0

    with st.form("upload_form"):
        uploaded_files = st.file_uploader(
            "Upload Dokumen (Invoice 1 : Mandatory, Invoice 2 : Opsional Lip Gloss)",
            type=["jpg", "jpeg", "png", "pdf"],
            accept_multiple_files=True
        )

        uploader_note = st.selectbox(
            "Catatan (Mandatory)",
            ["- Pilih Catatan -", "New Submit Store", "Resubmit, Perubahan Cluster", "Resubmit, Perubahan Quantity"]
        )

        # Validasi catatan wajib dipilih
        is_valid_note = uploader_note != "- Pilih Catatan -"

        submitted = st.form_submit_button("🚀 Submit")

    if submitted:
        if not is_valid_sku:
            st.error("❌ Quantity SKU masih ada yang di bawah minimum.")
        elif uploader_note == "- Pilih Catatan -":
            st.error("❌ Catatan wajib dipilih.")
        elif not uploaded_files:
            st.error("❌ Silakan upload minimal 1 file.")
        else:
                try:
                    submission_id = str(uuid.uuid4())
                    gcs_uris = []
                    for idx, uploaded in enumerate(uploaded_files, 1):
                        safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", uploaded.name)
                        obj_path = f"{submission_id}/{idx}_{safe_name}"
                        gcs_path = f"{BUCKET_PREFIX.rstrip('/')}/{obj_path}" if BUCKET_PREFIX else obj_path
                        blob = gcs_client.bucket(BUCKET_NAME).blob(gcs_path)
                        uploaded.seek(0)
                        blob.upload_from_file(uploaded, content_type=uploaded.type)
                        gcs_uris.append(f"https://storage.cloud.google.com/{BUCKET_NAME}/{gcs_path}")

                    preview = df_customer.iloc[0].copy()
                    preview["cluster"] = cluster
                    base_info = {
                        "submission_id": submission_id,
                        "submitted_at": now(jakarta_tz).to_datetime_string(),
                        "region": preview["region"],
                        "distributor_company": preview["distributor_company"],
                        "distributor_name": preview["distributor_name"],
                        "spv": preview["spv"],
                        "cust_id": preview["cust_id"],
                        "customer_name": preview["customer_name"],
                        "cluster": preview["cluster"],
                        "gcs_uri": ";".join(gcs_uris),
                        "uploader_note": uploader_note,
                    }

                    # include sku_name in records
                    records = [
                        {**base_info, "sku": sku, "sku_name": sku_names.get(sku, ""), "quantity": qty}
                        for sku, qty in sku_quantities.items()
                    ]
                    full_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
                    errors = bq_client.insert_rows_json(full_table_id, records)
                    if errors:
                        raise RuntimeError(errors)

                    st.success("✅ Data berhasil diupload!")
                    with st.expander("📋 Detail Submission"):
                        st.write(f"**Submission ID:** {submission_id}")
                        st.write(f"**Region:** {preview['region']}")
                        st.write(f"**Distributor Company:** {preview['distributor_company']}")
                        st.write(f"**Distributor Name:** {preview['distributor_name']}")
                        st.write(f"**SPV:** {preview['spv']}")
                        st.write(f"**Customer:** {preview['customer_name']}")
                        st.write(f"**Cluster:** {preview['cluster']}")
                except Exception as e:
                    st.error(f"Gagal upload: {e}")
# ---------------------------
# Help & Info
# ---------------------------
st.markdown("---")
with st.expander("ℹ️ Bantuan & Informasi"):
    st.markdown(
        """
        ### Cara Menggunakan Form:
        1. **Pilih Region** - Mulai dengan memilih region yang sesuai
        2. **Pilih Distributor Company** - Pilih Distributor Company
        3. **Pilih Distributor Name** - Pilih Nama Distributor
        4. **Pilih SPV** - Pilih Nama SPV
        5. **Pilih Customer** - Pilih Store ID & Name
        6. **Pilih Cluster** - Pilih Cluster untuk Store, Cluster pada masing - masing toko sudah di tentukan. Jika ingin upgrade toko ke Set yang lebih tinggi harap pilih Set diatas yang telah di tentukan. **Toko tidak bisa downgrade ke Set yang lebih rendah**
        7. **Isi Quantity SKU** - Silahkan isi Quantity SKU dengan syarat tidak boleh dibawah minimum yang telah ditentukan dan diperbolehkan jika lebih dari minimum. Jika Minimum Quantity tertera 0, Maka bisa untuk di input 0 atau bisa juga untuk mengisi quantity nya
        8. **Upload File** - Setelah semua dipilih, upload dokumen Anda

        ### Format File yang Didukung:
        - Gambar: JPG, JPEG, PNG
        - Dokumen: PDF

        ### Catatan:
        - Jika ingin re-submit toko yang sama, Harap masukkan catatan "Resubmit, Upgrade Cluster/Perubahan Quantity"
        - Data akan tercatat di database untuk tracking
        """
    )
