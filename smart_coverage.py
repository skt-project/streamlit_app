import io
import streamlit as st
import pandas as pd
from typing import List, Dict
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime

# =========================
# BigQuery and GCS Configuration
# =========================
# Use Streamlit secrets if available, fallback to local path
try:
    gcp_secrets = st.secrets["connections"]["bigquery"]
    private_key = gcp_secrets["private_key"].replace("\\n", "\n")
    credentials = service_account.Credentials.from_service_account_info(
        {
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
        }
    )
    GCP_PROJECT_ID = st.secrets["bigquery"]["project"]
    BQ_DATASET = st.secrets["bigquery"]["dataset"]
    BQ_TABLE = st.secrets["bigquery"]["stock_analysis_table"]
except Exception:
    # Fallback for local testing if secrets are not configured
    GCP_CREDENTIALS_PATH = r"C:\script\skintific-data-warehouse-ea77119e2e7a.json"
    GCP_PROJECT_ID = "skintific-data-warehouse"
    BQ_DATASET = "gt_schema"
    BQ_TABLE = "stock_analysis"
    credentials = service_account.Credentials.from_service_account_file(
        GCP_CREDENTIALS_PATH
    )

# BigQuery table containing store hierarchy data
MASTER_STORE_TABLE = "gt_schema.master_store_database_basis"

# BigQuery table to store the submitted data
SKU_QUANTITY_TABLE = "gt_schema.store_stock_data"

# Google Cloud Storage bucket for PDF uploads
GCS_BUCKET = "public_skintific_storage"
GCS_FOLDER = "smart_coverage"

# Hardcoded list of SKUs for quantity input
HARDCODED_SKUS = [
    "TCC102006",
    "TCC102007",
    "TCC102004",
    "TQD116002",
    "TQD116003",
    "TCC104001",
    "TCC104002",
    "TCC104009",
    "TCC103004",
    "TCC103005",
    "TCC103006",
    "TCC108005",
    "TCC108012",
    "TCC108001",
    "TYX109001",
    "TYX109002",
]

# Hardcoded list of SKUs for quantity input
SKU_SETS: Dict[str, Dict[str, int]] = {
    "Set 1": {
        "TCC102006": 5,
        "TCC102007": 5,
        "TQD116002": 5,
        "TQD116003": 5,
    },
    "Set 2": {
        "TCC104001": 5,
        "TCC104002": 5,
        "TCC104009": 5,
        "TCC103004": 5,
        "TCC103005": 5,
    },
    "Set 3": {
        "TCC102006": 5,
        "TCC102007": 5,
        "TCC103004": 5,
        "TCC103005": 5,
        "TCC108005": 5,
        "TCC108012": 5,
        "TYX109001": 5,
    },
}

@st.cache_resource
def get_bq_client():
    """Returns a BigQuery client."""
    return bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)


@st.cache_resource
def get_gcs_client():
    """Returns a Google Cloud Storage client."""
    return storage.Client(credentials=credentials, project=GCP_PROJECT_ID)


@st.cache_data
def get_regions() -> List[str]:
    """Fetches unique regions from BigQuery."""
    client = get_bq_client()
    query = f"SELECT DISTINCT UPPER(Region) AS Region FROM `{MASTER_STORE_TABLE}` ORDER BY Region"
    df = client.query(query).to_dataframe()
    return df["Region"].tolist()


@st.cache_data
def get_spvs(region: str) -> List[str]:
    """Fetches unique SPVs based on a selected region."""
    client = get_bq_client()
    query = f"""
        SELECT DISTINCT UPPER(spv_tph) AS SPV
        FROM `{MASTER_STORE_TABLE}`
        WHERE UPPER(Region) = @region
        ORDER BY SPV
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("region", "STRING", region),
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    return df["SPV"].tolist()


@st.cache_data
def get_distributors(spv: str) -> List[str]:
    """Fetches unique distributors based on a selected SPV."""
    client = get_bq_client()
    query = f"""
        SELECT DISTINCT UPPER(Distributor_tph) AS Distributor
        FROM `{MASTER_STORE_TABLE}`
        WHERE UPPER(spv_tph) = @spv
        ORDER BY Distributor
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("spv", "STRING", spv),
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    return df["Distributor"].tolist()


@st.cache_data
def get_stores(distributor: str) -> List[str]:
    """Fetches stores based on a selected distributor."""
    client = get_bq_client()
    query = f"""
        SELECT cust_id, store_name
        FROM `{MASTER_STORE_TABLE}`
        WHERE UPPER(Distributor_tph) = @distributor
        ORDER BY cust_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("distributor", "STRING", distributor),
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    df["display_name"] = df["cust_id"].astype(str) + " - " + df["store_name"]
    return df["display_name"].tolist()


def upload_pdf_to_gcs(uploaded_file, store_id):
    """Uploads a PDF file to a GCS bucket."""
    try:
        gcs_client = get_gcs_client()
        bucket = gcs_client.bucket(GCS_BUCKET)

        # Create a unique file path in GCS
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        gcs_path = f"{GCS_FOLDER}/{store_id}_{timestamp}_{uploaded_file.name}"
        blob = bucket.blob(gcs_path)

        # Upload the file
        blob.upload_from_file(uploaded_file, content_type="application/pdf")
        st.success(f"PDF uploaded successfully to GCS: {gcs_path}")
        return f"gs://{GCS_BUCKET}/{gcs_path}"
    except Exception as e:
        st.error(f"Error uploading PDF to GCS: {e}")
        return None


def insert_sku_data_to_bq(store_id, store_name, pdf_gcs_url, sku_data):
    """Inserts SKU quantity data into a BigQuery table."""
    try:
        bq_client = get_bq_client()
        table_ref = bq_client.dataset(SKU_QUANTITY_TABLE.split(".")[0]).table(
            SKU_QUANTITY_TABLE.split(".")[1]
        )

        rows_to_insert = []
        submission_timestamp = datetime.now().isoformat()

        for sku, quantity in sku_data.items():
            if quantity is not None and quantity > 0:
                rows_to_insert.append(
                    {
                        "store_id": store_id,
                        "store_name":store_name,
                        "sku": sku,
                        "quantity": quantity,
                        "pdf_url": pdf_gcs_url,
                        "submission_timestamp": submission_timestamp,
                    }
                )

        if rows_to_insert:
            errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
            if errors:
                st.error(f"Errors while inserting data: {errors}")
                return False
            else:
                st.success("SKU quantity data submitted to BigQuery successfully!")
                return True
        else:
            st.warning("No quantities were entered. No data submitted.")
            return False

    except Exception as e:
        st.error(f"Error inserting SKU data to BigQuery: {e}")
        return False


# =========================
# Streamlit App
# =========================
def main():
    st.title("Store Stock Management App")
    st.markdown(
        "Please select your store, upload a document, and fill in the stock quantities."
    )

    # 1. Hierarchical Dropdowns
    st.header("1. Store Selection")
    regions = get_regions()
    selected_region = st.selectbox("Select Region", options=regions)

    selected_store_display = None
    if selected_region:
        spvs = get_spvs(selected_region)
        selected_spv = st.selectbox("Select SPV", options=spvs)

        if selected_spv:
            distributors = get_distributors(selected_spv)
            selected_distributor = st.selectbox(
                "Select Distributor", options=distributors
            )

            if selected_distributor:
                stores = get_stores(selected_distributor)
                selected_store_display = st.selectbox("Select Store", options=stores)

    selected_sku_set = None
    if selected_store_display:
        st.markdown("---")
        st.header("1.1. Select SKU Set")
        selected_sku_set = st.selectbox("Select SKU Set", options=list(SKU_SETS.keys()))

    if selected_store_display and selected_sku_set:
        st.markdown("---")  # Separator after store is selected

        # 2. PDF Upload
        st.header("2. Document Upload")
        st.info("💡 Please upload the store's **stock analysis PDF** document.")
        uploaded_file = st.file_uploader("Upload a PDF document", type="pdf")

        # 3. SKU Quantity Input
        st.header("3. SKU Quantity Input")
        st.markdown("Please fill in the quantities for the following SKUs.")
        st.markdown(f"**Note:** SKUs in **{selected_sku_set}** have a minimum quantity requirement.")

        with st.form("sku_form"):
            sku_quantities = {}
            for sku in HARDCODED_SKUS:
                sku_quantities[sku] = st.number_input(
                    f"Quantity for {sku}", min_value=0, step=1, key=f"sku_{sku}"
                )

            submit_button = st.form_submit_button("Submit Data")

        if submit_button:
            validation_passed = True

            if not uploaded_file:
                st.error("Please upload a PDF document before submitting.")
                validation_passed = False

            # Validate SKUs in the selected set
            if selected_sku_set:
                required_skus = SKU_SETS[selected_sku_set]
                for sku, min_qty in required_skus.items():
                    if sku_quantities.get(sku) is None or sku_quantities[sku] < min_qty:
                        st.error(
                            f"Error: Quantity for {sku} must be at least {min_qty}."
                        )
                        validation_passed = False
                        
            if validation_passed:
                # Extract store ID and name from the display string
                parts = selected_store_display.split(" - ", 1)
                store_id = parts[0]
                store_name = parts[1] if len(parts) > 1 else "N/A"

                # Step 1: Upload PDF to GCS
                pdf_url = upload_pdf_to_gcs(uploaded_file, store_id)

                # Step 2: Insert data to BigQuery
                if pdf_url:
                    insert_sku_data_to_bq(store_id, store_name, pdf_url, sku_quantities)

if __name__ == "__main__":
    main()
