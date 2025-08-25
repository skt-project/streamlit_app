import io
from typing import Dict, List, Tuple, Optional
import streamlit as st
import pandas as pd
from difflib import get_close_matches
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account

# =========================
# Environment / Secrets
# =========================
# Set these as environment variables in your deployment
# GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
# GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
# OAUTH_REDIRECT_URI = os.getenv(
#     "OAUTH_REDIRECT_URI", ""
# )
# GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
# BQ_DATASET = os.getenv("BQ_DATASET", "dist_config")
# BQ_CONFIGS_TABLE = os.getenv("BQ_CONFIGS_TABLE", "distributor_configs")
# BQ_ROLES_TABLE = os.getenv("BQ_ROLES_TABLE", "app_roles")

try:
    # Use Streamlit secrets if available
    gcp_secrets = st.secrets["connections"]["bigquery"]
    private_key = gcp_secrets["private_key"].replace("\\n", "\n")
    credentials = service_account.Credentials.from_service_account_info({
        "type": gcp_secrets["type"],
        "project_id": gcp_secrets["project_id"],
        "private_key_id": gcp_secrets["private_key_id"],
        "private_key": private_key,
        "client_email": gcp_secrets["client_email"],
        "client_id": gcp_secrets["client_id"],
        "auth_uri": gcp_secrets["auth_uri"],
        "token_uri": gcp_secrets["token_uri"],
        "auth_provider_x509_cert_url": gcp_secrets["auth_provider_x509_cert_url"],
        "client_x509_cert_url": gcp_secrets["client_x509_cert_url"],
    })
    GCP_PROJECT_ID = st.secrets["bigquery"]["project"]
    BQ_DATASET = st.secrets["bigquery"]["dataset"]
    BQ_CONFIGS_TABLE = st.secrets["bigquery"]["config_table"]
except Exception:
    # Fallback to local key file
    GCP_CREDENTIALS_PATH = r"C:\script\skintific-data-warehouse-ea77119e2e7a.json"
    GCP_PROJECT_ID = "skintific-data-warehouse"
    BQ_DATASET = "gt_schema"
    BQ_CONFIGS_TABLE = "distributor_configs"
    credentials = service_account.Credentials.from_service_account_file(
        GCP_CREDENTIALS_PATH
    )

# =========================
# Master Schema
# =========================
MASTER_SCHEMA: List[str] = [
    "Customer Code",
    "Customer Name",
    "Customer Branch Code",
    "Customer Branch Name",
    "Customer Address",
    "PO Date",
    "PO Number",
    "Customer Store Code",
    "Customer Store Name",
    "Customer SKU Code",
    "Customer SKU Name",
    "Qty",
]

FIXED_FIRST_5 = MASTER_SCHEMA[:5]

# Hardcoded brand options with prefixes
BRAND_PREFIXES = {
    "SKINTIFIC": "11",
    "G2G": "12",
    "TIMEPHORIA": "13",
}
BRAND_OPTIONS = list(BRAND_PREFIXES.keys())


# =========================
# BigQuery Client
# =========================
@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    """Initializes and returns a BigQuery client."""
    # Explicitly load credentials from the file path
    return bigquery.Client(credentials=credentials, project=credentials.project_id)


# =========================
# BigQuery Bootstrap
# =========================
def ensure_bq_objects():
    client = get_bq_client()
    dataset_ref = bigquery.Dataset(f"{client.project}.{BQ_DATASET}")
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        client.create_dataset(dataset_ref)

    # distributor_configs: one row per distributor
    # - distributor STRING  (primary key)
    # - static_fields JSON  (first 5 columns)
    # - mapping JSON        (col map for other fields)
    # - updated_at TIMESTAMP
    # - updated_by STRING   (user email)
    schema_configs = [
        bigquery.SchemaField("distributor", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("static_fields", "JSON", mode="REQUIRED"),
        bigquery.SchemaField("mapping", "JSON", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("updated_by", "STRING", mode="REQUIRED"),
    ]
    table_configs_id = f"{client.project}.{BQ_DATASET}.{BQ_CONFIGS_TABLE}"
    try:
        client.get_table(table_configs_id)
    except NotFound:
        table = bigquery.Table(table_configs_id, schema=schema_configs)
        client.create_table(table)


# =========================
# BigQuery Helpers
# =========================
def list_distributors() -> List[str]:
    client = get_bq_client()
    sql = f"""
    SELECT distributor
    FROM `{client.project}.{BQ_DATASET}.{BQ_CONFIGS_TABLE}`
    ORDER BY distributor
    """
    rows = client.query(sql).result()
    return [r.distributor for r in rows]

def get_config(distributor: str) -> Optional[Dict]:
    client = get_bq_client()
    sql = f"""
    SELECT static_fields, mapping
    FROM `{client.project}.{BQ_DATASET}.{BQ_CONFIGS_TABLE}`
    WHERE distributor = @distributor
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("distributor", "STRING", distributor)
        ]
    )
    rows = list(client.query(sql, job_config=job_config).result())
    if not rows:
        return None
    # JSON type returns as dicts already
    return {
        "static_fields": rows[0].static_fields,
        "mapping": rows[0].mapping,
    }


# =========================
# Intelligent Mapping
# =========================
def intelligent_mapping(
    df: pd.DataFrame,
    static_fields: Dict[str, str],
    mapping: Dict[str, str],
    brand_prefix: str,
    enable_fuzzy: bool = True,
    fuzzy_cutoff: float = 0.6,
) -> Tuple[pd.DataFrame, Dict[str, str], List[str]]:
    """
    Returns (mapped_df, effective_mapping)
    effective_mapping includes columns used after fuzzy fallback.
    """
    out = pd.DataFrame()
    effective_mapping = {}
    failed_columns = []

    # Fill fixed fields for all rows
    for col in FIXED_FIRST_5:
        out[col] = [static_fields.get(col, "")] * len(df)

    # Apply Customer Code prefix
    customer_code_static = static_fields.get("Customer Code", "")
    out["Customer Code"] = brand_prefix + customer_code_static

    # Dynamic fields (rest of schema)
    needed = [c for c in MASTER_SCHEMA if c not in FIXED_FIRST_5]

    # 1) apply direct mapping where source exists
    for target in needed:
        src = mapping.get(target, "")
        if src and src in df.columns:
            # Special handling for PO Date: convert to datetime and format as date
            if target == "PO Date":
                out[target] = pd.to_datetime(df[src], errors="coerce").dt.strftime(
                    "%Y-%m-%d"
                )
            else:
                out[target] = df[src]
            effective_mapping[target] = src
        else:
            out[target] = None

    # 2) fuzzy fallback for any still empty target
    if enable_fuzzy:
        for target in needed:
            if out[target].isna().all():
                # find best source candidate by comparing target label to df columns
                guesses = get_close_matches(
                    target, df.columns.tolist(), n=1, cutoff=fuzzy_cutoff
                )
                if guesses:
                    src = guesses[0]
                    out[target] = df[src]
                    effective_mapping[target] = src
                else:
                    failed_columns.append(target)   

    # Get the prefix from the static fields
    branch_code_prefix = static_fields.get("Customer Branch Code", "")

    # Get the original 'Customer Store Code' from the effective mapping
    original_store_code_col = effective_mapping.get("Customer Store Code")

    if branch_code_prefix and original_store_code_col:
        # Create the new combined column
        out["Customer Store Code"] = (
            branch_code_prefix + out["Customer Store Code"].astype(str)
        )
        effective_mapping["Customer Store Code"] = (
            f"PREFIXED({branch_code_prefix}){original_store_code_col}"
        )

    # Ensure final order
    out = out[MASTER_SCHEMA]
    return out, effective_mapping, failed_columns


# =========================
# Utilities
# =========================
def read_any_table(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    elif name.endswith((".xlsx", ".xls")):
        return pd.read_excel(uploaded_file)
    else:
        st.error("Unsupported file type. Please upload a .csv, .xls, or .xlsx file.")
        return pd.DataFrame()


def to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Data") -> bytes:
    """
    Converts a pandas DataFrame to an in-memory Excel file (xlsx).

    The output is configured to have column headers in the first row,
    and the data starting on the second row.
    """
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, header=True, startrow=1, sheet_name=sheet_name)
    buf.seek(0)
    return buf.getvalue()


# =========================
# UI
# =========================
def main():
    st.set_page_config(
        page_title="Distributor Converter", page_icon="📦", layout="wide"
    )
    st.header("📂 Distributor Data Converter")
    st.markdown(
        "Upload your distributor's data file and it will be converted to the standard template."
    )

    # Ensure BQ objects exist
    try:
        ensure_bq_objects()
    except Exception as e:
        st.error(f"BigQuery setup error: {e}")
        st.stop()

    distributors = list_distributors()
    if not distributors:
        st.info(
            "No distributors configured yet. Please configure at least one in BigQuery directly."
        )
        return

    # User selects a distributor
    dist = st.selectbox("Select Distributor", distributors)
    brand = st.selectbox("Select Brand", BRAND_OPTIONS)
    brand_prefix = BRAND_PREFIXES.get(brand, "")

    uploaded = st.file_uploader(
        "Upload Distributor File (.xlsx/.csv)", type=["xlsx", "xls", "csv"]
    )
    if not uploaded:
        return

    st.write("Preview of uploaded data:")
    try:
        df = read_any_table(uploaded)
        st.dataframe(df.head())
    except Exception as e:
        st.error(f"Error reading the uploaded file: {e}")
        return

    cfg = get_config(dist)
    if not cfg:
        st.error(
            "Configuration for the selected distributor was not found. Please contact an administrator."
        )
        return

    # Intelligent map (apply config, fallback to fuzzy)
    try:
        mapped, effective_map, failed_columns = intelligent_mapping(
            df, cfg["static_fields"], cfg["mapping"], brand_prefix
        )
    except Exception as e:
        st.error(f"Error during data mapping: {e}")
        return

    st.success("Mapped to master schema.")
    st.write("Converted sample:")
    st.dataframe(mapped.head())

    st.subheader("Mapping Log")

    # Successful Mappings
    if effective_map:
        st.write("✅ **Successful Mappings:**")
        successful_log = []
        for target, source in effective_map.items():
            successful_log.append(
                {
                    "Target Column": target,
                    "Source Column": source,
                    "Status": "Mapped" if target in cfg["mapping"] else "Fuzzy Match",
                }
            )
        st.table(pd.DataFrame(successful_log))

    # Failed Mappings
    if failed_columns:
        st.write("❌ **Failed Mappings (Columns not found in the uploaded file):**")
        failed_log = []
        for target in failed_columns:
            failed_log.append(
                {
                    "Target Column": target,
                    "Expected Source Column (from config)": cfg["mapping"].get(
                        target, "N/A"
                    ),
                }
            )
        st.table(pd.DataFrame(failed_log))

    # Download Excel
    try:
        xlsx = to_excel_bytes(mapped, "MappedData")
        st.download_button(
            "📥 Download Converted Excel",
            data=xlsx,
            file_name=f"{dist}_converted.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        st.error(f"Error generating download file: {e}")


if __name__ == "__main__":
    main()
