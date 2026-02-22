import io
import re
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
try:
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

BRAND_PREFIXES = {
    "SKINTIFIC": "11",
    "G2G": "12",
    "TIMEPHORIA": "13",
    "FACERINNA": "1A",
    "BODIBREZE": "17",
}
BRAND_OPTIONS = list(BRAND_PREFIXES.keys())

# Distributors whose raw files require the 3M cleaning pipeline
# (matched via case-insensitive prefix)
M3_DISTRIBUTOR_PREFIX = "cv mitra makmur mandiri"


# =========================
# BigQuery Client
# =========================
@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
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
    return {
        "static_fields": rows[0].static_fields,
        "mapping": rows[0].mapping,
    }


# =========================
# 3M Daily ST Cleaning
# =========================
def is_3m_distributor(distributor_name: str) -> bool:
    """Returns True if the distributor should use the 3M cleaning pipeline."""
    return distributor_name.lower().startswith(M3_DISTRIBUTOR_PREFIX)


def clean_3m_daily_st(uploaded_file) -> pd.DataFrame:
    """
    Parse and flatten the 3M Daily ST Upload Template (.xlsx).

    The raw file has a report-style layout where each transaction block starts
    with a header line like:
        "No. Trans : JL/M3-26020183 [ 09-02-2026 ] - ONE MART"
    followed by product rows.  Column 7 of that header row carries the
    distributor store ID ("Store Code Suggestion").

    Returns a flat DataFrame with columns:
        Product Code | Product Name | Kuantitas | H.JUAL | JLH SBLM DISC |
        DISC | JUMLAH | No. TRANSAKSI | ID CUST DISTRIBUTOR | Customer Store Name
    """
    df = pd.read_excel(uploaded_file, sheet_name="TEMPLATE", header=None)

    records = []
    current_trans = None
    current_store_id = None
    current_store_name = None

    for _, row in df.iterrows():
        cell0 = str(row[0]).strip() if pd.notna(row[0]) else ""

        # ── Transaction header ────────────────────────────────────────────────
        if "No. Trans :" in cell0:
            match = re.match(
                r"No\.\s*Trans\s*:\s*(\S+)\s*\[.*?\]\s*-\s*(.+)", cell0
            )
            if match:
                current_trans = match.group(1).strip()
                current_store_name = match.group(2).strip()

            col7 = str(row[7]).strip() if pd.notna(row[7]) else ""
            # Leave blank for unregistered stores
            current_store_id = (
                "" if col7 in ("Not Registered", "nan", "") else col7
            )

        # ── Product row (col 0 is a numeric barcode ≥ 10 digits) ─────────────
        elif cell0.isdigit() and len(cell0) >= 10:
            records.append(
                {
                    "Product Code": str(int(cell0)),
                    "Product Name": row[1],
                    "Kuantitas": row[2],
                    "H.JUAL": row[3],
                    "JLH SBLM DISC": row[4],
                    "DISC": row[5],
                    "JUMLAH": row[6],
                    "No. TRANSAKSI": current_trans,
                    "ID CUST DISTRIBUTOR": current_store_id,
                    "Customer Store Name": current_store_name,
                }
            )

    result = pd.DataFrame(records)

    # Type cleanup
    for col in ["Kuantitas", "H.JUAL", "JLH SBLM DISC", "DISC", "JUMLAH"]:
        result[col] = pd.to_numeric(result[col], errors="coerce")

    return result


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
    out = pd.DataFrame()
    effective_mapping = {}
    failed_columns = []

    df.columns = [col.lower() for col in df.columns]
    mapping_lower = {k: v.lower() for k, v in mapping.items()}

    for col in FIXED_FIRST_5:
        out[col] = [static_fields.get(col, "")] * len(df)

    customer_code_static = static_fields.get("Customer Code", "")
    out["Customer Code"] = brand_prefix + customer_code_static

    needed = [c for c in MASTER_SCHEMA if c not in FIXED_FIRST_5]

    for target in needed:
        src = mapping_lower.get(target, "")
        if src and src in df.columns:
            if target == "PO Date":
                out[target] = pd.to_datetime(df[src], errors="coerce").dt.strftime(
                    "%Y-%m-%d"
                )
            else:
                out[target] = df[src]
            effective_mapping[target] = src
        else:
            out[target] = None

    if enable_fuzzy:
        for target in needed:
            if out[target].isna().all():
                guesses = get_close_matches(
                    target.lower(), df.columns.tolist(), n=1, cutoff=fuzzy_cutoff
                )
                if guesses:
                    src = guesses[0]
                    out[target] = df[src]
                    effective_mapping[target] = src
                else:
                    failed_columns.append(target)

    branch_code_prefix = static_fields.get("Customer Branch Code", "")
    original_store_code_col = effective_mapping.get("Customer Store Code")

    if branch_code_prefix and original_store_code_col:
        out["Customer Store Code"] = (
            branch_code_prefix + out["Customer Store Code"].astype(str)
        )
        effective_mapping["Customer Store Code"] = (
            f"PREFIXED({branch_code_prefix}){original_store_code_col}"
        )

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
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, header=True, startrow=1, sheet_name=sheet_name)
    buf.seek(0)
    return buf.getvalue()


# =========================
# UI – 3M pipeline section
# =========================
def render_3m_pipeline(dist: str, brand: str, brand_prefix: str):
    """Handles upload + conversion for CV MITRA MAKMUR MANDIRI distributors."""
    st.info(
        "ℹ️ This distributor uses the **3M Daily ST** format. "
        "Please upload the raw *Laporan Detail Penjualan* Excel file."
    )

    uploaded = st.file_uploader(
        "Upload 3M Daily ST File (.xlsx)", type=["xlsx", "xls"], key="m3_uploader"
    )
    if not uploaded:
        return

    # ── Parse & clean ────────────────────────────────────────────────────────
    with st.spinner("Parsing 3M report layout…"):
        try:
            cleaned = clean_3m_daily_st(uploaded)
        except Exception as e:
            st.error(f"Error parsing 3M file: {e}")
            return

    if cleaned.empty:
        st.warning("No product rows were found in the uploaded file.")
        return

    st.success(f"✅ Extracted **{len(cleaned):,}** product rows from the report.")
    st.write("Cleaned data preview:")
    st.dataframe(cleaned.head(10))

    # ── Mapping log ──────────────────────────────────────────────────────────
    st.subheader("Column Mapping")
    mapping_info = [
        {"Output Column": "Product Code",        "Source": "BARCODE (col 0)"},
        {"Output Column": "Product Name",         "Source": "NAMA PRODUK (col 1)"},
        {"Output Column": "Kuantitas",            "Source": "QTY (col 2)"},
        {"Output Column": "H.JUAL",               "Source": "H.JUAL (col 3)"},
        {"Output Column": "JLH SBLM DISC",        "Source": "JLH SBLM DISC (col 4)"},
        {"Output Column": "DISC",                 "Source": "DISC (col 5)"},
        {"Output Column": "JUMLAH",               "Source": "JUMLAH (col 6)"},
        {"Output Column": "No. TRANSAKSI",        "Source": "Parsed from transaction header"},
        {"Output Column": "ID CUST DISTRIBUTOR",  "Source": "Store Code Suggestion (col 7)"},
        {"Output Column": "Customer Store Name",  "Source": "Parsed from transaction header"},
    ]
    st.table(pd.DataFrame(mapping_info))

    # ── Unregistered stores summary ──────────────────────────────────────────
    unregistered = cleaned[cleaned["ID CUST DISTRIBUTOR"] == ""]
    if not unregistered.empty:
        unique_unregistered = unregistered["Customer Store Name"].unique()
        st.warning(
            f"⚠️ **{len(unique_unregistered)} store(s)** are marked *Not Registered* "
            "and will have an empty distributor ID:"
        )
        st.write(", ".join(sorted(unique_unregistered)))

    # ── Download ─────────────────────────────────────────────────────────────
    try:
        xlsx = to_excel_bytes(cleaned, "3M_Cleaned")
        st.download_button(
            label="📥 Download Cleaned Excel",
            data=xlsx,
            file_name=f"{dist}_3M_cleaned.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        st.error(f"Error generating download file: {e}")


# =========================
# UI – Standard pipeline
# =========================
def render_standard_pipeline(dist: str, brand: str, brand_prefix: str):
    """Handles upload + conversion for standard distributor formats."""
    uploaded = st.file_uploader(
        "Upload Distributor File (.xlsx/.csv)", type=["xlsx", "xls", "csv"]
    )
    if not uploaded:
        return

    st.write("Preview of uploaded data:")
    try:
        df = read_any_table(uploaded)
        df.columns = [col.lower() for col in df.columns]
        st.dataframe(df.head())
    except Exception as e:
        st.error(f"Error reading the uploaded file: {e}")
        return

    cfg = get_config(dist)
    if not cfg:
        st.error(
            "Configuration for the selected distributor was not found. "
            "Please contact an administrator."
        )
        return

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

    if effective_map:
        st.write("✅ **Successful Mappings:**")
        successful_log = [
            {
                "Target Column": target,
                "Source Column": source,
                "Status": "Mapped" if target in cfg["mapping"] else "Fuzzy Match",
            }
            for target, source in effective_map.items()
        ]
        st.table(pd.DataFrame(successful_log))

    if failed_columns:
        st.write("❌ **Failed Mappings (Columns not found in the uploaded file):**")
        failed_log = [
            {
                "Target Column": target,
                "Expected Source Column (from config)": cfg["mapping"].get(target, "N/A"),
            }
            for target in failed_columns
        ]
        st.table(pd.DataFrame(failed_log))

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


# =========================
# Main
# =========================
def main():
    st.set_page_config(
        page_title="Distributor Converter", page_icon="📦", layout="wide"
    )
    st.header("📂 Distributor Data Converter")
    st.markdown(
        "Upload your distributor's data file and it will be converted to the standard template."
    )

    try:
        ensure_bq_objects()
    except Exception as e:
        st.error(f"BigQuery setup error: {e}")
        st.stop()

    distributors = list_distributors()
    if not distributors:
        st.info(
            "No distributors configured yet. "
            "Please configure at least one in BigQuery directly."
        )
        return

    dist = st.selectbox("Select Distributor", distributors)
    brand = st.selectbox("Select Brand", BRAND_OPTIONS)
    brand_prefix = BRAND_PREFIXES.get(brand, "")

    # ── Route to the correct pipeline ────────────────────────────────────────
    if is_3m_distributor(dist):
        render_3m_pipeline(dist, brand, brand_prefix)
    else:
        render_standard_pipeline(dist, brand, brand_prefix)


if __name__ == "__main__":
    main()
