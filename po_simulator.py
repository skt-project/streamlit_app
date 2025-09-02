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
def get_sku_data(sku_list: List[str]) -> pd.DataFrame:
    """
    Fetches SKU data for a given list of SKUs from BigQuery.
    """
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.gt_schema.master_product"

    # Create a string of the SKUs for the IN clause
    sku_list_str = ", ".join([f"'{sku}'" for sku in sku_list])

    query = f"""
    SELECT
        sku,
        product_name,
        assortment
    FROM `{table_id}`
    WHERE sku IN ({sku_list_str})
    """
    try:
        df_sku_data = client.query(query).to_dataframe()
        return df_sku_data
    except Exception as e:
        st.error(f"Error fetching SKU data from BigQuery: {e}")
        return pd.DataFrame()


@st.cache_data(show_spinner="Fetching Stock Analysis data from BigQuery...")
def get_stock_data(distributor_name: str, sku_list: List[str]) -> pd.DataFrame:
    """
    Fetches stock and sales data for a given list of SKUs from BigQuery.
    """
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    # Create a string of the SKUs for the IN clause
    sku_list_str = ", ".join([f"'{sku}'" for sku in sku_list])

    query = f"""
    SELECT
        UPPER(region) AS region,
        distributor,
        sku,
        product_name,
        assortment,
        total_stock,
        buffer_plan_by_lm_qty_adj,
        avg_weekly_st_lm_qty,
        buffer_plan_by_lm_val_adj
    FROM `{table_id}`
    WHERE distributor = '{distributor_name}'
    AND (
        sku IN ({sku_list_str})
        OR buffer_plan_by_lm_qty_adj > 0
    )
    """
    try:
        df_sku_data = client.query(query).to_dataframe()
        return df_sku_data
    except Exception as e:
        st.error(f"Error fetching Stock Analysis data from BigQuery: {e}")
        return pd.DataFrame()


@st.cache_data(show_spinner="Fetching NPD PO tracking data...")
def get_npd_po_tracking_data_from_bq(
    distributor_name: str, sku_list: List[str]) -> pd.DataFrame:
    """
    Fetches PO tracking data for NPD SKUs from the specified BigQuery table.
    """
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.dms.gt_po_tracking_mtd_mv"

    sku_list_str = ", ".join([f"'{sku}'" for sku in sku_list])

    query = f"""
    SELECT
        region,
        UPPER(distributor_name) AS distributor,
        sku,
        SUM(order_qty) as total_ordered_qty,
        SUM(nett_amount_incl_ppn) as total_ordered_value
    FROM `{table_id}`
    WHERE UPPER(distributor_name) = '{distributor_name}'
    AND sku IN ({sku_list_str})
    GROUP BY region, sku, UPPER(distributor_name)
    """
    try:
        df_tracking_data = client.query(query).to_dataframe()
        return df_tracking_data
    except Exception as e:
        st.error(f"Error fetching NPD tracking data from BigQuery: {e}")
        return pd.DataFrame()


# --- Helper Functions ---
def calculate_woi(stock: pd.Series, po_qty: pd.Series, avg_weekly_sales: pd.Series) -> pd.Series:
    """
    Calculates Weeks of Inventory (WOI) based on the formula:
    (Stock + PO Quantity) / Average Weekly Sales LM
    """
    # Handle division by zero
    # return (stock + po_qty) / avg_weekly_sales.replace(0, pd.NA).astype(float)
    # Use np.where to handle division by zero
    return np.where(avg_weekly_sales > 0, (stock + po_qty) / avg_weekly_sales, 0)

def to_excel_with_styling(df: pd.DataFrame) -> bytes:
    """
    Converts a pandas DataFrame to an Excel file with special styling for the first 7 columns.
    """
    output = io.BytesIO()
    wb = Workbook()
    ws = wb.active
    ws.title = "PO Simulation"

    # Define the fill style for the SKU types
    po_fill = PatternFill(start_color="D9EAD3", end_color="D9EAD3", fill_type="solid")
    suggestion_fill = PatternFill(
        start_color="FFF2CC", end_color="FFF2CC", fill_type="solid"
    )

    # Store the `is_po_sku` Series and then drop the column from the DataFrame
    # so it does not appear in the final Excel file.
    is_po_sku_series = df["is_po_sku"]
    df_no_flag = df.drop("is_po_sku", axis=1)

    # Write the DataFrame (without the flag column) to the worksheet
    rows = dataframe_to_rows(df_no_flag, index=False, header=True)

    # Iterate over rows and apply styling based on the original Series
    for r_idx, row in enumerate(rows, 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)

            # Apply color to the first 8 columns for data rows only (r_idx > 1)
            if c_idx <= 8 and r_idx > 1:
                # Get the boolean value from the original `is_po_sku` Series
                # The index for the series is the row index in the original df
                original_row_index = (
                    r_idx - 2
                )  # Subtract 2 because header is row 1 and data starts at 0
                is_po_row = is_po_sku_series.iloc[original_row_index]

                # Set the fill based on the boolean flag
                if is_po_row:
                    cell.fill = po_fill
                else:
                    cell.fill = suggestion_fill

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
        'G2G-45', 'G2G-51', 'G2G-186', 'G2G-202', 'G2G-110', 'G2G-74', 'G2G-37',
        'G2G-103', 'G2G-36', 'G2G-230', 'G2G-235', 'G2G-18', 'G2G-70', 'G2G-44',
        'G2G-217', 'G2G-800', 'G2G-213', 'G2G-47', 'G2G-1440', 'G2G-1445', 'G2G-17',
        'G2G-1942', 'G2G-1943'
    ]

    # Hardcoded NPD SKUs and their allocation data
    # npd_allocation_data = {
    #     'Region': [
    #         'Bali Nusa Tenggara', 'Central Java', 'Central Sumatera', 'East Java',
    #         'East Kalimantan', 'Jakarta (Csa)', 'Northern Sumatera',
    #         'South Kalimantan', 'Southern Sumatera 1', 'Southern Sumatera 2',
    #         'Sulawesi 1', 'Sulawesi 2', 'West Java (Sd)', 'West Kalimantan'
    #     ],
    #     'G2G-20900': [504, 671, 630, 611, 301, 119, 439, 533, 314, 444, 626, 211, 1229, 1108],
    #     'G2G-20901': [2031, 2773, 2549, 2466, 1211, 472, 1792, 2118, 1270, 1766, 2630, 858, 4815, 5581],
    #     'G2G-20902': [7783, 11007, 9831, 9469, 4640, 1772, 7000, 7966, 4878, 6649, 10652, 3336, 17746, 27255],
    #     'G2G-20903': [7783, 11007, 9831, 9469, 4640, 1772, 7000, 7966, 4878, 6649, 10652, 3336, 17746, 27255],
    #     'G2G-20904': [2031, 2773, 2549, 2466, 1211, 472, 1792, 2118, 1270, 1766, 2630, 858, 4815, 5581]
    # }
    # npd_allocation_df_full = pd.DataFrame(npd_allocation_data)
    # npd_skus = [col for col in npd_allocation_df_full.columns if col != 'Region']

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

            # Add a flag to identify original PO SKUs
            po_df["is_po_sku"] = True

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
            po_df = po_df[["Customer SKU Code", "PO Qty", "PO Value", "is_po_sku"]]

            # --- Fetch missing data from BigQuery ---
            sku_list = po_df["Customer SKU Code"].unique().tolist()
            sku_data_df = get_stock_data(distributor_name, sku_list)

            if sku_data_df.empty:
                st.warning(
                    "Could not find stock and sales data for the uploaded SKUs in BigQuery. Please check the SKU codes."
                )
                return

            if "sku" in sku_data_df.columns:
                sku_data_df.rename(columns={"sku": "Customer SKU Code"}, inplace=True)

            # --- Data Processing and Calculation ---
            st.header("3. PO Simulation and Download Result")

            # Merge uploaded PO data with BigQuery SKU data
            # Use a left merge to keep all SKUs from the uploaded file
            result_df = pd.merge(po_df, sku_data_df, on="Customer SKU Code", how="outer")

            # Fallback for missing Product Name and Assortment
            missing_sku_list = result_df[result_df["product_name"].isnull()]["Customer SKU Code"].tolist()
            if missing_sku_list:
                fallback_sku_data = get_sku_data(missing_sku_list)
                if not fallback_sku_data.empty:
                    fallback_sku_data.rename(columns={"sku": "Customer SKU Code"}, inplace=True)
                    result_df.set_index("Customer SKU Code", inplace=True)
                    fallback_sku_data.set_index("Customer SKU Code", inplace=True)

                    # Update the missing values with data from the fallback table
                    result_df.update(fallback_sku_data)
                    result_df.reset_index(inplace=True)

            # Fill NaN values in 'is_po_sku' with False
            result_df["is_po_sku"] = result_df["is_po_sku"].fillna(False)

            # Fill NaN values with 0 for calculations if data was not found for some SKUs
            result_df[
                [
                    "PO Qty",
                    "PO Value",
                    "total_stock",
                    "buffer_plan_by_lm_qty_adj",
                    "avg_weekly_st_lm_qty",
                    "buffer_plan_by_lm_val_adj",
                ]
            ] = result_df[
                [
                    "PO Qty",
                    "PO Value",
                    "total_stock",
                    "buffer_plan_by_lm_qty_adj",
                    "avg_weekly_st_lm_qty",
                    "buffer_plan_by_lm_val_adj",
                ]
            ].fillna(
                0
            )

            # --- Handle NPD SKUs and Allocation ---
            # npd_tracking_df = get_npd_po_tracking_data_from_bq(
            #     distributor_name, npd_skus
            # )

            # st.dataframe(npd_tracking_df.head(5))

            # if npd_tracking_df.empty:
            #     st.warning(
            #         "No NPD tracking data found for this distributor. Continuing without NPD-specific logic."
            #     )
            #     selected_region = None
            # else:
            #     selected_region = npd_tracking_df["region"].iloc[0]

            # if (
            #     selected_region
            #     and selected_region in npd_allocation_df_full["Region"].values
            # ):
            #     npd_allocations = (
            #         npd_allocation_df_full[
            #             npd_allocation_df_full["Region"] == selected_region
            #         ]
            #         .drop("Region", axis=1)
            #         .iloc[0]
            #     )

            #     npd_tracking_df.rename(
            #         columns={"sku": "Customer SKU Code"}, inplace=True
            #     )

            #     # Merge NPD tracking data with the main result_df
            #     result_df = pd.merge(
            #         result_df,
            #         npd_tracking_df.drop(
            #             columns=["region", "distributor"]
            #         ),  # drop region and distributor from tracking df as they're redundant
            #         on="Customer SKU Code",
            #         how="left",
            #     )
            #     result_df[["total_ordered_qty", "total_ordered_value"]] = result_df[
            #         ["total_ordered_qty", "total_ordered_value"]
            #     ].fillna(0)

            #     # Apply NPD logic to the result_df
            #     for sku in npd_skus:
            #         mask = result_df["Customer SKU Code"] == sku
            #         if mask.any():

            #             allocation_qty = npd_allocations.get(sku, 0)

            #             row = result_df[mask]

            #             # Use DPP from the uploaded PO if available, otherwise get a default value
            #             dpp = (
            #                 row["DPP"].iloc[0]
            #                 if "DPP" in row.columns and not row["DPP"].isnull().iloc[0]
            #                 else 0
            #             )

            #             # Calculate suggested values based on allocation and existing orders
            #             current_ordered_qty = row["total_ordered_qty"].iloc[0]
            #             current_ordered_value = row["total_ordered_value"].iloc[0]

            #             po_qty_inputted = row["PO Qty"].iloc[0]

            #             result_df.loc[mask, "buffer_plan_by_lm_qty_adj"] = max(
            #                 0, allocation_qty - current_ordered_qty
            #             )
            #             result_df.loc[mask, "buffer_plan_by_lm_val_adj"] = max(
            #                 0, (allocation_qty * dpp * 1.11) - current_ordered_value
            #             )

            #             is_original_po_sku = row["is_po_sku"].iloc[0]

            #             if is_original_po_sku:
            #                 total_qty_after_po = current_ordered_qty + po_qty_inputted
            #                 if total_qty_after_po > allocation_qty:
            #                     result_df.loc[mask, "Remark"] = (
            #                         "Reject (NPD Allocation Exceeded)"
            #                     )
            #                 elif total_qty_after_po < allocation_qty:
            #                     result_df.loc[mask, "Remark"] = (
            #                         "Proceed with suggestion (NPD Under-ordered)"
            #                     )
            #                 else:
            #                     result_df.loc[mask, "Remark"] = "Proceed (NPD)"
            #             else:
            #                 result_df.loc[mask, "Remark"] = "NPD Suggestion"

            # # Drop unnecessary columns from the result_df before final calculations
            # if "total_ordered_qty" in result_df.columns and "total_ordered_value" in result_df.columns:
            #     result_df.drop(
            #         columns=["total_ordered_qty", "total_ordered_value"], inplace=True
            #     )

            # Filter to show only SKUs with PO Qty or a positive suggested qty
            result_df = result_df[
                (result_df["PO Qty"] > 0) | (result_df["buffer_plan_by_lm_qty_adj"] > 0)
            ]

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
                # 1. New condition for additional suggested SKUs
                (result_df["is_po_sku"] == False),
                # 2. Hardcoded Reject
                result_df["Customer SKU Code"].isin(MANUAL_REJECT_SKUS),
                # 3. Reject if suggested PO is 0
                (result_df["buffer_plan_by_lm_qty_adj"] == 0),
                # 4. PO Qty > Suggested PO Qty (Over-ordering)
                (result_df["PO Qty"] > result_df["buffer_plan_by_lm_qty_adj"]),
                # 5. PO Qty < Suggested PO Qty (Under-ordering)
                (result_df["PO Qty"] < result_df["buffer_plan_by_lm_qty_adj"]),
                # 6. PO Qty = Suggested PO Qty (Exact Match)
                (result_df["PO Qty"] == result_df["buffer_plan_by_lm_qty_adj"]),
            ]

            # Corresponding values
            choices = [
                "Additional Suggestion",
                "Reject",
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
                "assortment": "Assortment",
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

            # Sort the DataFrame: user SKUs first, then suggested SKUs
            result_df.sort_values(
                by=["is_po_sku", "SKU"], ascending=[False, True], inplace=True
            )

            # Reorder columns for display
            excel_cols = [
                "Distributor",
                "SKU",
                "Product Name",
                "Assortment",
                "PO Qty",
                "PO Value",
                "WOI (Stock + PO Ori)",
                "Remark",
                "Suggested PO Qty",
                "Suggested PO Value",
                "WOI (Stock + Suggestion)",
                "is_po_sku"
            ]

            result_df = result_df.reindex(columns=excel_cols)

            # Format 'PO Value' as currency with comma separators
            result_df["PO Value"] = result_df["PO Value"].apply(
                lambda x: f"{x:,.2f}" if pd.notnull(x) else ""
            )
            result_df["Suggested PO Value"] = result_df["Suggested PO Value"].apply(
                lambda x: f"{x:,.2f}" if pd.notnull(x) else ""
            )

            # Format 'WOI' columns to 2 decimal places
            result_df["WOI (Stock + PO Ori)"] = result_df[
                "WOI (Stock + PO Ori)"
            ].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "")
            result_df["WOI (Stock + Suggestion)"] = result_df[
                "WOI (Stock + Suggestion)"
            ].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "")

            # --- Download Button ---
            xlsx_data = to_excel_with_styling(result_df)

            st.download_button(
                label="📥 Download PO Simulator Excel",
                data=xlsx_data,
                file_name="po_simulator_result.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            # --- Display Results ---
            st.subheader("Simulated PO Data")

            # Reorder columns for display
            final_cols = [
                "Distributor",
                "SKU",
                "Product Name",
                "Assortment",
                "PO Qty",
                "PO Value",
                "WOI (Stock + PO Ori)",
                "Remark",
                "Suggested PO Qty",
                "Suggested PO Value",
                "WOI (Stock + Suggestion)",
            ]

            st.dataframe(result_df.reindex(columns=final_cols).reset_index(drop=True))

        except Exception as e:
            st.error(f"An error occurred: {e}")
            st.info(
                "Please ensure the uploaded file is a valid .xlsx or .csv and contains all the required columns."
            )

if __name__ == "__main__":
    main()
