import streamlit as st
import pandas as pd
import json
import uuid
import re
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials
from google.cloud import bigquery, storage
import gspread

st.set_page_config(page_title="Region Upload Portal", page_icon="📤", layout="wide")

# ------------------------------------------------------------
# Config & Auth
# ------------------------------------------------------------
if "gcp" not in st.secrets:
    st.stop()  # secrets.toml is required

conf = st.secrets["gcp"]
PROJECT = conf["project"]
DATASET = conf["dataset"]
BUCKET = conf["bucket"]
SPREADSHEET_URL = conf["spreadsheet_url"]

# Parse service account credentials
service_info = json.loads(conf["service_account_json"])
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/devstorage.read_write",
]

creds = Credentials.from_service_account_info(service_info, scopes=SCOPES)

bq = bigquery.Client(project=PROJECT, credentials=creds)
gcs = storage.Client(project=PROJECT, credentials=creds)
gc = gspread.authorize(creds)

TABLE_ID = f"{PROJECT}.{DATASET}.doc_submissions"

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
@st.cache_data(ttl=300)
def load_reference_sheet(spreadsheet_url: str) -> pd.DataFrame:
    """Read the sheet into a DataFrame and prepare helper columns."""
    sh = gc.open_by_url(spreadsheet_url)
    ws = sh.sheet1  # or use .worksheet('Sheet1') if you want a specific tab
    rows = ws.get_all_records()  # list of dicts
    df = pd.DataFrame(rows)

    # Standardize column names (strip whitespace)
    df.columns = [c.strip() for c in df.columns]

    # Ensure required columns exist
    required = ["Region", "Distributor", "Distributor Name (basis)", "SPV", "Cust ID", "Customer Name", "Cluster"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in sheet: {missing}")

    # Build "CustKey" = "Cust ID - Customer Name"
    df["CustKey"] = df["Cust ID"].astype(str).str.strip() + " - " + df["Customer Name"].astype(str).str.strip()

    # Clean numeric columns (optional snapshot fields)
    for col in ["G2G Cleaner", "SKT", "TPH"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .replace({"": None, "None": None})
            )
            # to numeric (coerce errors -> NaN)
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

def slugify(text: str) -> str:
    # very simple slugify for paths
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")

def ensure_bq_table():
    """Create dataset/table if they don't exist (idempotent)."""
    # dataset
    ds_ref = bigquery.Dataset(f"{PROJECT}.{DATASET}")
    try:
        bq.get_dataset(ds_ref)
    except Exception:
        bq.create_dataset(ds_ref, exists_ok=True)

    # table
    schema = [
        bigquery.SchemaField("submission_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("submitted_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("region", "STRING"),
        bigquery.SchemaField("distributor", "STRING"),
        bigquery.SchemaField("distributor_name", "STRING"),
        bigquery.SchemaField("spv", "STRING"),
        bigquery.SchemaField("cust_id", "STRING"),
        bigquery.SchemaField("customer_name", "STRING"),
        bigquery.SchemaField("cluster", "STRING"),
        bigquery.SchemaField("g2g_cleaner", "NUMERIC"),
        bigquery.SchemaField("skt", "NUMERIC"),
        bigquery.SchemaField("tph", "NUMERIC"),
        bigquery.SchemaField("file_name", "STRING"),
        bigquery.SchemaField("file_type", "STRING"),
        bigquery.SchemaField("file_size_bytes", "INT64"),
        bigquery.SchemaField("gcs_uri", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("uploader_note", "STRING"),
    ]
    table = bigquery.Table(TABLE_ID, schema=schema)
    bq.create_table(table, exists_ok=True)

def upload_to_gcs(bucket_name: str, path: str, file_bytes: bytes, content_type: str) -> str:
    bucket = gcs.bucket(bucket_name)
    blob = bucket.blob(path)
    blob.upload_from_string(file_bytes, content_type=content_type)
    return f"gs://{bucket_name}/{path}"

def insert_row(record: dict):
    errors = bq.insert_rows_json(TABLE_ID, [record])
    if errors:
        raise RuntimeError(errors)

# ------------------------------------------------------------
# UI
# ------------------------------------------------------------
st.title("📤 Area Document Upload")

with st.expander("ℹ️ How it works", expanded=False):
    st.write("""
    1) Select Region → Distributor → SPV → **Cust ID – Customer Name** → Cluster (filtered automatically).  
    2) Upload your **image/PDF**.  
    3) We store the file in **Google Cloud Storage** and metadata in **BigQuery**.
    """)

ensure_bq_table()
ref_df = load_reference_sheet(SPREADSHEET_URL)

# ------------- filters -------------
col1, col2, col3 = st.columns(3)
with col1:
    regions = sorted(ref_df["Region"].dropna().unique().tolist())
    region = st.selectbox("Region", regions)

with col2:
    dist_opts = sorted(ref_df.query("Region == @region")["Distributor"].dropna().unique().tolist())
    distributor = st.selectbox("Distributor", dist_opts)

with col3:
    spv_opts = sorted(ref_df.query("Region == @region & Distributor == @distributor")["SPV"].dropna().unique().tolist())
    spv = st.selectbox("SPV", spv_opts)

col4, col5 = st.columns(2)
with col4:
    cust_opts = ref_df.query(
        "Region == @region & Distributor == @distributor & SPV == @spv"
    )["CustKey"].dropna().unique().tolist()
    cust_key = st.selectbox("Cust ID – Customer Name", sorted(cust_opts))

# derive cluster choices based on selection
sub_df = ref_df.query(
    "Region == @region & Distributor == @distributor & SPV == @spv & CustKey == @cust_key"
)
cluster_opts = sorted(sub_df["Cluster"].dropna().unique().tolist())

with col5:
    cluster = st.selectbox("Cluster", cluster_opts)

# Optional free-text note from uploader
uploader_note = st.text_input("Note (optional)")

st.markdown("---")

uploaded = st.file_uploader(
    "Upload document (JPG, PNG, PDF)",
    type=["jpg", "jpeg", "png", "pdf"],
    accept_multiple_files=False
)

submit = st.button("Submit", type="primary", disabled=uploaded is None)

# ------------- on submit -------------
if submit:
    if uploaded is None:
        st.error("Please choose a file to upload.")
        st.stop()

    # Validate the selected row exists
    if sub_df.empty:
        st.error("The selected combination was not found in the reference sheet.")
        st.stop()

    ref_row = sub_df.iloc[0]

    # Build path in GCS (organize by region/cust_id/date)
    cust_id = str(ref_row["Cust ID"]).strip()
    cust_name = str(ref_row["Customer Name"]).strip()
    ext = uploaded.name.split(".")[-1].lower()
    submission_id = str(uuid.uuid4())

    path = (
        f"{slugify(region)}/"
        f"{slugify(distributor)}/"
        f"{slugify(spv)}/"
        f"{slugify(cust_id)}/"
        f"{datetime.now(timezone.utc):%Y/%m/%d}/"
        f"{submission_id}.{ext}"
    )

    # Upload to GCS
    gcs_uri = upload_to_gcs(
        BUCKET, path, uploaded.getvalue(), uploaded.type or "application/octet-stream"
    )

    # Prepare BQ record
    rec = {
        "submission_id": submission_id,
        "submitted_at": datetime.now(timezone.utc).isoformat(),
        "region": region,
        "distributor": distributor,
        "distributor_name": ref_row.get("Distributor Name (basis)", None),
        "spv": spv,
        "cust_id": cust_id,
        "customer_name": cust_name,
        "cluster": cluster,
        "g2g_cleaner": float(ref_row["G2G Cleaner"]) if "G2G Cleaner" in ref_row and pd.notna(ref_row["G2G Cleaner"]) else None,
        "skt": float(ref_row["SKT"]) if "SKT" in ref_row and pd.notna(ref_row["SKT"]) else None,
        "tph": float(ref_row["TPH"]) if "TPH" in ref_row and pd.notna(ref_row["TPH"]) else None,
        "file_name": uploaded.name,
        "file_type": uploaded.type,
        "file_size_bytes": uploaded.size,
        "gcs_uri": gcs_uri,
        "uploader_note": uploader_note or None,
    }

    # Insert into BigQuery
    try:
        insert_row(rec)
        st.success("Upload successful and saved to BigQuery ✅")
        st.write("**File stored at:**", gcs_uri)
        st.json(rec)
    except Exception as e:
        st.error(f"Failed to write to BigQuery: {e}")
