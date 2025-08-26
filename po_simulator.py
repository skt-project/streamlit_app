import io
import streamlit as st
import pandas as pd
import numpy as np
from typing import List
from openpyxl import Workbook
from openpyxl.styles import PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows

# --- BigQuery Imports ---
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account

# =========================
# BigQuery Configuration
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
    BQ_DATASET = "rsa"
    BQ_TABLE = "stock_analysis"
    credentials = service_account.Credentials.from_service_account_file(
        GCP_CREDENTIALS_PATH
    )


@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    """Initializes and returns a BigQuery client."""
    return bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)


@st.cache_data(show_spinner="Fetching distributor data from BigQuery...")
def get_distributor_data() -> List[str]:
    """Fetches unique customer and branch codes from BigQuery."""
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        query = f"""
        SELECT DISTINCT distributor
        FROM `{table_id}`
        ORDER BY distributor
        """
        rows = client.query(query).result()
        return [r.distributor for r in rows]
    except NotFound:
        st.error(f"Error: BigQuery table '{table_id}' not found.")
        return []
    except Exception as e:
        st.error(f"An error occurred while fetching data from BigQuery: {e}")
        return []


@st.cache_data(show_spinner="Fetching SKU data from BigQuery...")
def get_sku_data_from_bq(distributor_name: str, sku_list: List[str]) -> pd.DataFrame:
    """
    Fetches stock and sales data for a given list of SKUs from BigQuery.
    """
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    # Create a string of the SKUs for the IN clause
    sku_list_str = ", ".join([f"'{sku}'" for sku in sku_list])

    query = f"""
    SELECT
        distributor,
        sku,
        product_name,
        total_stock,
        buffer_plan_by_lm_qty_adj,
        avg_weekly_st_lm_qty,
        buffer_plan_by_lm_val_adj
    FROM `{table_id}`
    WHERE distributor = '{distributor_name}'
    AND sku IN ({sku_list_str})
    """
    try:
        df_sku_data = client.query(query).to_dataframe()
        return df_sku_data
    except Exception as e:
        st.error(f"Error fetching SKU data from BigQuery: {e}")
        return pd.DataFrame()

# --- Helper Functions ---
def calculate_woi(stock: pd.Series, po_qty: pd.Series, avg_weekly_sales: pd.Series) -> pd.Series:
    """
    Calculates Weeks of Inventory (WOI) based on the formula:
    (Stock + PO Quantity) / Average Weekly Sales LM
    """
    # Handle division by zero
    return (stock + po_qty) / avg_weekly_sales.replace(0, pd.NA).astype(float)

def to_excel_with_styling(df: pd.DataFrame) -> bytes:
    """
    Converts a pandas DataFrame to an Excel file with special styling for the first 4 columns.
    """
    output = io.BytesIO()
    wb = Workbook()
    ws = wb.active
    ws.title = "PO Simulation"

    # Define the fill style for the first 7 columns
    fill = PatternFill(start_color="D9EAD3", end_color="D9EAD3", fill_type="solid")

    # Write the dataframe to the worksheet
    rows = dataframe_to_rows(df, index=False, header=True)
    for r_idx, row in enumerate(rows, 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)
            # Apply color to the first 7 columns, including the header row
            if c_idx <= 7:
                cell.fill = fill

    wb.save(output)
    output.seek(0)
    return output.getvalue()


def main():
    st.set_page_config(page_title="PO Simulator", page_icon="🛒", layout="wide")
    st.title("🛒 PO Simulator")
    st.markdown(
        "Use this app to simulate Purchase Order data and decide on whether to Reject / Approve the PO."
    )

    # Hardcoded Reject List
    MANUAL_REJECT_SKUS = [

    ]

    st.header("1. Input Parameters")

    # Fetch data for dropdowns
    distributors = get_distributor_data()
    distributor_name = st.selectbox("Distributor Name", options=distributors)

    st.header("2. Upload PO Data")
    uploaded_file = st.file_uploader(
        "Upload a PO file (.xlsx/.csv) containing: 'PRODUCT CODE', 'DESCRIPTION', 'QTY', 'DPP', and 'TOTAL PRICE'.",
        type=["xlsx", "xls", "csv"],
    )

    if uploaded_file:
        try:
            required_cols = ["PRODUCT CODE", "DESCRIPTION", "QTY", "DPP"]

            # Read the uploaded file (Start from 8th row)
            if uploaded_file.name.endswith(".xlsx"):
                po_df = pd.read_excel(uploaded_file, header=7, engine="openpyxl")
            else:
                po_df = pd.read_csv(uploaded_file, header=7)
            st.success("File uploaded successfully!")

            # Drop columns that are all empty or have "Unnamed" in their header
            po_df.dropna(axis=1, how='all', inplace=True)
            po_df.drop(columns=[col for col in po_df.columns if 'Unnamed' in str(col)], inplace=True)

            # Ensure required columns are present in the uploaded file
            if not all(col in po_df.columns for col in required_cols):
                st.error("The uploaded file is missing one or more required columns.")
                st.write("Please check for these columns:", required_cols)
                return

            # Clean and filter the data
            # Ensure columns are of the correct type for calculations
            po_df["QTY"] = pd.to_numeric(po_df["QTY"], errors="coerce")
            po_df["DPP"] = pd.to_numeric(po_df["DPP"], errors="coerce")
            po_df.dropna(subset=["QTY", "DPP"], inplace=True)

            st.write("Preview of uploaded PO data:")
            st.dataframe(po_df.head())

            # Filter out SKUs with QTY of 0 or empty
            po_df = po_df[po_df["QTY"] > 0]
            po_df.dropna(subset=["PRODUCT CODE", "QTY", "DPP"], inplace=True)

            # Calculate the PO Value as requested: DPP * 1.11 (+ tax 11%) * QTY
            po_df["PO Value"] = po_df["DPP"] * 1.11 * po_df["QTY"]

            # Rename columns to match the rest of the script's expectations
            po_df.rename(
                columns={
                    "PRODUCT CODE": "Customer SKU Code",
                    "QTY": "PO Qty",
                },
                inplace=True,
            )

            # Keep only the required columns for the merge
            po_df = po_df[["Customer SKU Code", "PO Qty", "PO Value"]]

            # --- Fetch missing data from BigQuery ---
            sku_list = po_df["Customer SKU Code"].unique().tolist()
            sku_data_df = get_sku_data_from_bq(distributor_name, sku_list)

            if sku_data_df.empty:
                st.warning(
                    "Could not find stock and sales data for the uploaded SKUs in BigQuery. Please check the SKU codes."
                )
                return

            if "sku" in sku_data_df.columns:
                sku_data_df.rename(columns={"sku": "Customer SKU Code"}, inplace=True)

            # --- Data Processing and Calculation ---
            st.header("3. PO Simulation and Results")

            # Merge uploaded PO data with BigQuery SKU data
            # Use a left merge to keep all SKUs from the uploaded file
            result_df = pd.merge(po_df, sku_data_df, on="Customer SKU Code", how="left")

            # Fill NaN values with 0 for calculations if data was not found for some SKUs
            result_df[
                [
                    "total_stock",
                    "buffer_plan_by_lm_qty_adj",
                    "avg_weekly_st_lm_qty",
                    "buffer_plan_by_lm_val_adj",
                ]
            ] = result_df[
                [
                    "total_stock",
                    "buffer_plan_by_lm_qty_adj",
                    "avg_weekly_st_lm_qty",
                    "buffer_plan_by_lm_val_adj",
                ]
            ].fillna(
                0
            )

            # Add distributor_name column
            result_df["distributor_name"] = distributor_name

            # Calculate WOI Original
            result_df["WOI PO Original"] = calculate_woi(
                result_df["total_stock"],
                result_df["PO Qty"],
                result_df["avg_weekly_st_lm_qty"],
            )

            # Calculate WOI Suggest
            result_df["WOI Suggest"] = calculate_woi(
                result_df["total_stock"],
                result_df["buffer_plan_by_lm_qty_adj"],
                result_df["avg_weekly_st_lm_qty"],
            )

            # Conditions for np.select
            conditions = [
                # 1. Hardcoded Reject
                result_df["Customer SKU Code"].isin(MANUAL_REJECT_SKUS),
                # 2. Reject if suggested PO is 0
                (result_df["buffer_plan_by_lm_qty_adj"] == 0),
                # 3. PO Qty > Suggested PO Qty (Over-ordering)
                (result_df["PO Qty"] > result_df["buffer_plan_by_lm_qty_adj"]),
                # 4. PO Qty < Suggested PO Qty (Under-ordering)
                (result_df["PO Qty"] < result_df["buffer_plan_by_lm_qty_adj"]),
                # 5. PO Qty = Suggested PO Qty (Exact Match) - This is the default 'Proceed' if no other condition is met
                (result_df["PO Qty"] == result_df["buffer_plan_by_lm_qty_adj"]),
            ]

            # Corresponding values
            choices = [
                "Reject (Manual from Steve)",
                "Reject",
                "Reject with suggestion",
                "Proceed with suggestion",
                "Proceed",
            ]

            # Apply the conditions to create the 'Remark' column
            # The last choice "Proceed" catches the case PO Qty = Suggested PO Qty
            result_df["Remark"] = np.select(
                conditions, choices, default="N/A (Missing Data)"
            )

            new_column_names = {
                "distributor_name": "Distributor",
                "Customer SKU Code": "SKU",
                "product_name": "Product Name",
                "PO Qty": "PO Qty",
                "PO Value": "PO Value",
                "total_stock": "Total Stock Qty",
                "avg_weekly_st_lm_qty": "Avg Weekly Sales LM (Qty)",
                "buffer_plan_by_lm_qty_adj": "Suggested PO Qty",
                "buffer_plan_by_lm_val_adj": "Suggested PO Value",
                "WOI PO Original": "WOI (Stock + PO Ori)",
                "WOI Suggest": "WOI (Stock + Suggestion)",
            }

            # Rename the columns in the DataFrame
            result_df.rename(columns=new_column_names, inplace=True)

            # Final column order
            final_cols = [
                "Distributor",
                "SKU",
                "Product Name",
                "PO Qty",
                "PO Value",
                "WOI (Stock + PO Ori)",
                "Remark",
                "Suggested PO Qty",
                "Suggested PO Value",
                "WOI (Stock + Suggestion)",
            ]

            result_df = result_df.reindex(columns=final_cols)

            # Format 'PO Value' as currency with comma separators
            result_df["PO Value"] = result_df["PO Value"].apply(
                lambda x: f"{x:,.2f}" if pd.notnull(x) else ""
            )
            result_df["Suggested PO Value"] = result_df["Suggested PO Value"].apply(
                lambda x: f"{x:,.2f}" if pd.notnull(x) else ""
            )

            # Format 'WOI' columns to 2 decimal places
            result_df["WOI (Stock + PO Ori)"] = result_df["WOI (Stock + PO Ori)"].apply(
                lambda x: f"{x:.2f}" if pd.notnull(x) else ""
            )
            result_df["WOI (Stock + Suggestion)"] = result_df[
                "WOI (Stock + Suggestion)"
            ].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "")

            # --- Display Results ---
            st.subheader("Simulated PO Data")
            st.dataframe(result_df)

            # --- Download Button ---
            st.subheader("4. Download Result")
            xlsx_data = to_excel_with_styling(result_df)

            st.download_button(
                label="📥 Download PO Simulator Excel",
                data=xlsx_data,
                file_name="po_simulator_result.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        except Exception as e:
            st.error(f"An error occurred: {e}")
            st.info(
                "Please ensure the uploaded file is a valid .xlsx or .csv and contains all the required columns."
            )

if __name__ == "__main__":
    main()
