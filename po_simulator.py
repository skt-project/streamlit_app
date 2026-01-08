import io
import streamlit as st
import pandas as pd
import numpy as np
from typing import List
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment
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


@st.cache_data(ttl=21600, show_spinner="Fetching distributor data from BigQuery...")
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


@st.cache_data(ttl=21600, show_spinner="Fetching SKU data from BigQuery...")
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
        price_for_distri
    FROM `{table_id}`
    WHERE sku IN ({sku_list_str})
    """
    try:
        df_sku_data = client.query(query).to_dataframe()
        return df_sku_data
    except Exception as e:
        st.error(f"Error fetching SKU data from BigQuery: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=21600, show_spinner="Fetching NPD Allocation data from BigQuery...")
def get_npd_data(sku_list: List[str]) -> pd.DataFrame:
    """
    Fetches NPD Allocation data for a given list of SKUs from BigQuery.
    """
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.gt_schema.npd_allocation"

    # Create a string of the SKUs for the IN clause
    sku_list_str = ", ".join([f"'{sku}'" for sku in sku_list])

    query = f"""
    SELECT
        calendar_date,
        region,
        sku
    FROM `{table_id}`
    WHERE sku IN ({sku_list_str})
    AND calendar_date = '2026-01-01'
    """
    try:
        df_sku_data = client.query(query).to_dataframe()
        return df_sku_data
    except Exception as e:
        st.error(f"Error fetching NPD data from BigQuery: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=21600, show_spinner="Fetching Stock Analysis data from BigQuery...")
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
        UPPER(distributor) AS distributor,
        sku,
        product_name,
        assortment,
        supply_control_status_gt,
        total_stock,
        buffer_plan_by_lm_qty_adj,
        avg_weekly_st_lm_qty,
        buffer_plan_by_lm_val_adj,
        remaining_allocation_qty_region,
        woi_end_of_month_by_lm
    FROM `{table_id}`
    WHERE UPPER(distributor) = '{distributor_name}'
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


def calculate_woi(stock: pd.Series, po_qty: pd.Series, avg_weekly_sales: pd.Series) -> pd.Series:
    """
    Calculates Weeks of Inventory (WOI) based on the formula:
    (Stock + PO Quantity) / Average Weekly Sales LM
    """
    # Use np.where to handle division by zero
    return np.where(avg_weekly_sales > 0, (stock + po_qty) / avg_weekly_sales, 0)


def apply_sku_rejection_rules(sku_list: List, df: pd.DataFrame, regions: List[str], is_in: bool) -> pd.DataFrame:
    """
    Auto-rejects specific SKUs based on a provided list and region rules.
    """
    # Convert regions list to lowercase for case-insensitive matching
    regions_lower = [r.lower() for r in regions]

    # Create the rejection condition
    # If is_in = False, then only allow for the regions in the list
    if not is_in:
        # Rejects if SKU is in the sku_list AND region is NOT in the regions list
        condition = (df["SKU"].isin(sku_list)) & (~df["region"].str.lower().isin(regions_lower))
    else:
        # Rejects if SKU is in the sku_list AND region is in the regions list
        condition = (df["SKU"].isin(sku_list)) & (df["region"].str.lower().isin(regions_lower))

    # Apply the rejection logic
    df.loc[condition, "Remark"] = "Reject (Stop by Steve)"

    st.info("Rejection rules for specific SKUs applied.")
    return df


def to_excel_with_styling(dfs: dict, npd_sku_list: List[str] = None) -> bytes:
    """
    Converts a pandas DataFrame to an Excel file with special styling for the first 7 columns.
    Applies specific color to 'Remaining Allocation (By Region)' column if SKU is in npd_sku_list.
    """
    output = io.BytesIO()
    wb = Workbook()

    # Remove the default sheet created on workbook initialization
    del wb["Sheet"]

    # Define the fill style for the SKU types
    po_fill = PatternFill(start_color="D9EAD3", end_color="D9EAD3", fill_type="solid")
    suggestion_fill = PatternFill(
        start_color="FFF2CC", end_color="FFF2CC", fill_type="solid"
    )
    npd_fill = PatternFill(start_color="B1DBF0", end_color="B1DBF0", fill_type="solid")

    # Define a style for the header row
    header_font = Font(bold=True)
    header_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
    header_alignment = Alignment(horizontal='left', vertical='center')

    # Define a style for the 'Remark' column
    proceed_font = Font(bold=True, color="54CE54")
    reject_font = Font(bold=True, color="D73E3E")
    suggest_font = Font(bold=True, color="F3C94C")

    for sheet_name, df in dfs.items():
        ws = wb.create_sheet(title=sheet_name[:31])  # Truncate sheet name to 31 chars

        # Store the `is_po_sku` Series and then drop the column from the DataFrame
        # so it does not appear in the final Excel file.
        is_po_sku_series = df["is_po_sku"]
        df_no_flag = df.drop("is_po_sku", axis=1)

        # Write the DataFrame (without the flag column) to the worksheet
        rows = dataframe_to_rows(df_no_flag, index=False, header=True)

        # Get the column names and their indices
        headers = list(df_no_flag.columns)
        # This is a mapping from column name to its 0-based index
        col_map = {col: i for i, col in enumerate(headers)}

        # Define the columns that need special number formatting
        currency_cols = ["PO Value", "Suggested PO Value"]
        integer_cols = ["Remaining Allocation (By Region)", "Avg Weekly Sales LM (Qty)"]
        decimal_cols = ["WOI (Stock + PO Ori)", "Current WOI", "WOI After Buffer (Stock + Suggested Qty)", "Stock + Suggested Qty WOI (Projection at EOM)"]

        # Iterate over rows and apply styling based on the original Series
        for r_idx, row in enumerate(rows, 1):
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx, value=value)

                # Apply header styling
                if r_idx == 1:
                    cell.font = header_font
                    cell.fill = header_fill
                    cell.alignment = header_alignment

                # Apply color to the first 10 columns for data rows only (r_idx > 1)
                if c_idx <= 11 and r_idx > 1:
                    # Get the boolean value from the original `is_po_sku` Series
                    # The index for the series is the row index in the original df
                    original_row_index = (r_idx - 2)  # Subtract 2 because header is row 1 and data starts at 0
                    is_po_row = is_po_sku_series.iloc[original_row_index]

                    # Set the fill based on the boolean flag
                    if is_po_row:
                        cell.fill = po_fill
                    else:
                        cell.fill = suggestion_fill

                # Apply number formatting based on column name for data rows
                if r_idx > 1:
                    col_name = headers[c_idx - 1]

                    if col_name in currency_cols:
                        cell.number_format = "#,##0.00"
                    elif col_name in integer_cols:
                        cell.number_format = "#,##0"
                    elif col_name in decimal_cols:
                        cell.number_format = "0.00"

                    # Apply font styling to the 'Remark' column based on value
                    if col_name == "Remark":
                        remark_value = row[c_idx - 1]
                        if "Proceed" in remark_value:
                            cell.font = proceed_font
                        elif "Reject" in remark_value:
                            cell.font = reject_font
                        elif "Additional" in remark_value:
                            cell.font = suggest_font

                    # Apply specific fill to 'Remaining Allocation (By Region)' if SKU is in npd_sku_list
                    if col_name in ["Remaining Allocation (By Region)", "Suggested PO Qty", "Suggested PO Value"] and npd_sku_list is not None:
                        sku_col_index = col_map.get("SKU")
                        if sku_col_index is not None:
                            sku_value = row[sku_col_index]
                            if sku_value in npd_sku_list:
                                cell.fill = npd_fill

    wb.save(output)
    output.seek(0)
    return output.getvalue()


def to_excel_single_sheet(df: pd.DataFrame, npd_sku_list: List[str] = None) -> bytes:
    """
    Converts a single pandas DataFrame (all distributors stacked) 
    to an Excel file with special styling.
    """
    output = io.BytesIO()
    wb = Workbook()
    ws = wb.active
    ws.title = "PO Simulator"

    # Define the fill style for the SKU types
    po_fill = PatternFill(start_color="D9EAD3", end_color="D9EAD3", fill_type="solid")
    suggestion_fill = PatternFill(
        start_color="FFF2CC", end_color="FFF2CC", fill_type="solid"
    )
    npd_fill = PatternFill(start_color="B1DBF0", end_color="B1DBF0", fill_type="solid")

    # Define a style for the header row
    header_font = Font(bold=True)
    header_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
    header_alignment = Alignment(horizontal='left', vertical='center')

    # Define a style for the 'Remark' column
    proceed_font = Font(bold=True, color="54CE54")
    reject_font = Font(bold=True, color="D73E3E")
    suggest_font = Font(bold=True, color="F3C94C")

    # Store the `is_po_sku` Series and then drop the column from the DataFrame
    is_po_sku_series = df["is_po_sku"]
    df_no_flag = df.drop("is_po_sku", axis=1)

    # Write the DataFrame (without the flag column) to the worksheet
    rows = dataframe_to_rows(df_no_flag, index=False, header=True)

    # Get the column names and their indices
    headers = list(df_no_flag.columns)
    # This is a mapping from column name to its 0-based index
    col_map = {col: i for i, col in enumerate(headers)}

    # Define the columns that need special number formatting
    currency_cols = ["PO Value", "Suggested PO Value"]
    integer_cols = ["Remaining Allocation (By Region)", "Avg Weekly Sales LM (Qty)"]
    decimal_cols = ["WOI (Stock + PO Ori)", "Current WOI", "WOI After Buffer (Stock + Suggested Qty)", "Stock + Suggested Qty WOI (Projection at EOM)"]

    # Iterate over rows and apply styling based on the original Series
    for r_idx, row in enumerate(rows, 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)

            # Apply header styling
            if r_idx == 1:
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = header_alignment

            # Apply color to the first 11 columns for data rows only (r_idx > 1)
            if c_idx <= 11 and r_idx > 1:
                # Get the boolean value from the original `is_po_sku` Series
                # The index for the series is the row index in the original df
                original_row_index = (r_idx - 2)  # Subtract 2 because header is row 1 and data starts at 0
                is_po_row = is_po_sku_series.iloc[original_row_index]

                # Set the fill based on the boolean flag
                if is_po_row:
                    cell.fill = po_fill
                else:
                    cell.fill = suggestion_fill

            # Apply number formatting based on column name for data rows
            if r_idx > 1:
                col_name = headers[c_idx - 1]

                if col_name in currency_cols:
                    cell.number_format = "#,##0.00"
                elif col_name in integer_cols:
                    cell.number_format = "#,##0"
                elif col_name in decimal_cols:
                    cell.number_format = "0.00"

                # Apply font styling to the 'Remark' column based on value
                if col_name == "Remark":
                    remark_value = row[c_idx - 1]
                    if "Proceed" in remark_value:
                        cell.font = proceed_font
                    elif "Reject" in remark_value:
                        cell.font = reject_font
                    elif "Additional" in remark_value:
                        cell.font = suggest_font

                # Apply specific fill to 'Remaining Allocation (By Region)' if SKU is in npd_sku_list
                if col_name in ["Remaining Allocation (By Region)", "Suggested PO Qty", "Suggested PO Value"] and npd_sku_list is not None:
                    sku_col_index = col_map.get("SKU")
                    if sku_col_index is not None:
                        sku_value = row[sku_col_index]
                        if sku_value in npd_sku_list:
                            cell.fill = npd_fill

    wb.save(output)
    output.seek(0)
    return output.getvalue()


def create_po_template_excel() -> bytes:
    """
    Creates a blank Excel template file with the required headers.
    """
    output = io.BytesIO()
    wb = Workbook()
    ws = wb.active
    ws.title = "PO Template"

    # Static headers
    ws["A2"] = "PUCHASE ORDER FORM"
    ws["A3"] = "CUSTOMER NAME :"
    ws["A4"] = "NPWP / ID CARD :"
    ws["A5"] = "ADDRESS :"
    ws["D3"] = "DATE :"
    ws["D4"] = "Berlaku Sampai"
    ws["D5"] = "Issued by"

    ws["A2"].font = Font(bold=True, size=16)

    # Define the headers
    headers = ["DISTRIBUTOR", "PRODUCT CODE", "DESCRIPTION", "QTY", "DPP", "TOTAL PRICE"]

    # Write headers starting at row 8 (index 7 in a 0-based array)
    for col_idx, header in enumerate(headers, 1):
        cell = ws.cell(row=8, column=col_idx, value=header)
        cell.font = Font(bold=True)

    # Freeze the first 8 rows to make the headers visible while scrolling
    ws.freeze_panes = "A9"

    wb.save(output)
    output.seek(0)
    return output.getvalue()


def main():
    st.set_page_config(page_title="PO Simulator", page_icon="🛒", layout="wide")

    st.title("🛒 PO Simulator")
    st.markdown(
        "Use this app to simulate Purchase Order data and decide on whether to Reject / Approve the PO."
    )

    # Create tabs
    tab1, tab2 = st.tabs(["📖 Guide & Rules", "🔍 Simulation & Results"])

    # Hardcoded Reject List
    MANUAL_REJECT_SKUS = [
    "G2G-252",
    "G2G-253",
    "G2G-29700",
    "G2G-27300",
    "G2G-29705",
    "G2G-224",
    "G2G-247",
    "G2G-225",
    "G2G-226",
    "G2G-228",
    "G2G-74",
    "G2G-186",
    "G2G-202",
    "G2G-840",
    "G2G-844",
    "G2G-841",
    "G2G-800",
    "G2G-213",
    "G2G-217",
    "G2G-27305"
]

    LIMITED_SKUS_QTY = [
    ]
    MAX_QTY_LIMIT = 500

    # Additional rejected SKUs based on region rules
    REJECTED_SKUS_1 = []
    REGION_LIST_1 = []

    REJECTED_SKUS_2 = []
    REGION_LIST_2 = []

    with tab1:
        st.header("How to Use the PO Simulator")
        with st.expander("📋 Step-by-Step Guide"):
            st.markdown("""
            1. **Upload PO Data**: Upload an Excel or CSV file containing the required columns: 'DISTRIBUTOR', 'PRODUCT CODE', 'DESCRIPTION', 'QTY'. The file should start from the 8th row.
            2. **Review Rejection Lists**: Check the manual rejection SKUs and region-based rejections if applicable.
            3. **Simulate and Analyze**: The app will fetch stock and sales data from BigQuery, perform calculations like Weeks of Inventory (WOI), and apply approval/rejection rules.
            4. **View Results**: Review the simulated data in the table, including remarks on whether to proceed, reject, or suggest adjustments.
            5. **Download Excel**: Click the download button to get a single Excel file with a separate sheet for each distributor's results, or a single sheet for all distributors.
            """)

        # Display Proceed / Reject Rules Explanation
        st.header("Rules & Calculations Logic")
        with st.expander("⚖️ Rules & Calculations Logic Explanation"):
            st.markdown("""
            The following rules are applied in order to determine the remark for each SKU. The rest are the calculations logic behind the results.

            1.  **Reject**: The SKU is rejected if any of these conditions are met:
                * It is on the **regional rejection list** (Stop by Steve), unless the region is allowed to order.
                * It is on the **manual rejection list** (Stop by Steve).
                * The **Remaining Allocation (By Region)** is **less than 0** (Negative Allocation).
                * The **Current WOI** is too high (Exceeded the WOI Standard).
                * The PO quantity is **greater than** the suggested PO quantity (over-ordering) -> **Reject with Suggestion**.

            2.  **Proceed**: The SKU is approved if any of these conditions are met:
                * The PO quantity is **less than** the suggested PO quantity (under-ordering) -> **Proceed with Suggestion**.
                * The PO quantity **exactly matches** the suggested PO quantity.
                * It's a new product (NPD) with remaining region allocation **greater than 0**, and the PO quantity is **less than or equal to** the remaining allocation by region.
                * It has no historical trend data, and the supply control status is **not** "STOP PO," "DISCONTINUED," or "OOS."

            3.  **Additional Suggestion**: The SKU is marked as an "Additional Suggestion" if:
                * It was not on the original PO but was **suggested by the system**.
            
            4.  **Suggested WOI (OH + IT + Suggested Qty + ST Projection until EOM)**: The suggested WOI is calculated based on the total stock + Suggested Qty + ST Projection until end of month
            """)

        # Display Manual Rejection SKUs in an expander
        st.header("Manual Rejection SKUs")
        with st.expander("🚫 List of SKUs that are manually rejected by Steve"):
            st.dataframe(pd.DataFrame(MANUAL_REJECT_SKUS, columns=["SKU"]).sort_values(by="SKU").reset_index(drop=True))    

        # Display Rejected SKUs by Region in a separate expander
        # with st.expander("🌏 List of Rejected SKUs by Region (Reject for 'Sulawesi 1', 'Southern Sumatera 1', 'Central Java', 'West Kalimantan')"):
        #     st.dataframe(pd.DataFrame(REJECTED_SKUS_2, columns=["SKU"]).sort_values(by="SKU").reset_index(drop=True))

    with tab2:
        st.header("1. Download PO Template")

        st.download_button(
            label="📥 Download PO Template",
            data=create_po_template_excel(),
            file_name="po_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        st.header("2. Upload PO Data")
        uploaded_file = st.file_uploader(
            "Upload a PO file (.xlsx/.csv) containing: 'PRODUCT CODE', 'DESCRIPTION', 'QTY', and 'DISTRIBUTOR'.",
            type=["xlsx", "xls", "csv"],
        )

        if uploaded_file:
            try:
                required_cols = ["PRODUCT CODE", "DESCRIPTION", "QTY", "DISTRIBUTOR"]

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
                po_df.dropna(subset=["QTY"], inplace=True)

                # Standardize the distributor name to all uppercase for consistency
                po_df["DISTRIBUTOR"] = po_df["DISTRIBUTOR"].str.upper()

                po_df["PRODUCT CODE"] = po_df["PRODUCT CODE"].astype(str).str.strip().str.upper()

                st.write("Preview of uploaded PO data:")
                st.dataframe(po_df.head())

                # Filter out SKUs with QTY of 0 or empty
                po_df = po_df[po_df["QTY"] > 0]
                po_df.dropna(subset=["PRODUCT CODE", "QTY", "DISTRIBUTOR"], inplace=True)

                # Add a flag to identify original PO SKUs
                po_df["is_po_sku"] = True

                # Rename columns to match the rest of the script's expectations
                po_df.rename(
                    columns={
                        "PRODUCT CODE": "Customer SKU Code",
                        "QTY": "PO Qty",
                        "DISTRIBUTOR": "Distributor"
                    },
                    inplace=True,
                )

                # Keep only the required columns for the merge
                po_df = po_df[["Distributor", "Customer SKU Code", "PO Qty", "is_po_sku"]]

                # --- Data Processing and Calculation ---
                st.header("3. PO Simulation")

                st.subheader("4. Select Output Format and Download")
                output_format = st.radio(
                    "Choose Excel Output Format:",
                    ["Separate Sheets (One per Distributor)", "Single Sheet (All Distributors Stacked)"],
                    index=0
                )

                progress = st.progress(0)
                # progress.progress(0.1, "Starting data processing...")
                progress_step = 1.0 / len(po_df["Distributor"].unique())

                # Master list to collect all NPD SKUs across all distributors
                all_npd_sku_list = []
                # Dictionary to hold dataframes for Excel sheets
                excel_dfs = {}
                # List to hold dataframes for on-screen display
                display_dfs = []

                # --- Loop through each distributor in the uploaded file ---
                uploaded_distributors = po_df["Distributor"].unique().tolist()

                for i, distributor_name in enumerate(uploaded_distributors):
                    st.subheader(f"Processing data for: **{distributor_name}**")
                    progress.progress((i * progress_step), f"Processing {distributor_name}...")

                    # Filter the uploaded data for the current distributor
                    current_po_df = po_df[po_df["Distributor"] == distributor_name].copy()
                    sku_list = current_po_df["Customer SKU Code"].unique().tolist()

                    # Fetch data from BigQuery for the current distributor

                    # --- Fetch missing data from BigQuery ---
                    sku_df = get_sku_data(sku_list)
                    sku_data_df = get_stock_data(distributor_name, sku_list)

                    sku_df.rename(
                        columns={
                            "sku": "Customer SKU Code",
                            "price_for_distri": "SIP",
                        },
                        inplace=True,
                    )

                    if sku_data_df.empty:
                        st.warning(
                            "Could not find stock and sales data for the uploaded Distributor/SKUs in BigQuery. Please check the Distributor Name or SKU codes."
                        )
                        return

                    if "sku" in sku_data_df.columns:
                        sku_data_df.rename(columns={"sku": "Customer SKU Code", "distributor": "Distributor"}, inplace=True)

                    # Merge uploaded PO data with SKU price data
                    result_df = pd.merge(current_po_df, sku_df, on="Customer SKU Code", how="left")

                    result_df["SIP"] = pd.to_numeric(result_df["SIP"], errors="coerce").fillna(0)

                    # Calculate the PO Value using price_for_distri
                    result_df["PO Value"] = result_df["SIP"] * result_df["PO Qty"]

                    # Merge uploaded PO data with BigQuery SKU data
                    # Use a left merge to keep all SKUs from the uploaded file
                    result_df = pd.merge(result_df, sku_data_df, on="Customer SKU Code", how="outer")
                    # progress.progress(0.3, "Data merged successfully")

                    result_sku_list = result_df["Customer SKU Code"].unique().tolist()

                    npd_df = get_npd_data(result_sku_list)
                    # NPD SKUs for the current distributor's results
                    current_npd_sku_list = npd_df['sku'].unique().tolist() if not npd_df.empty else []

                    # Add the current list of NPD SKUs to the master list
                    all_npd_sku_list.extend(current_npd_sku_list)

                    # Ensure the master list contains only unique SKUs
                    all_npd_sku_list = list(set(all_npd_sku_list))

                    # Fill NaN values in 'is_po_sku' with False
                    result_df["is_po_sku"] = result_df["is_po_sku"].astype("boolean")
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
                            "remaining_allocation_qty_region",
                            "woi_end_of_month_by_lm",
                        ]
                    ] = result_df[
                        [
                            "PO Qty",
                            "PO Value",
                            "total_stock",
                            "buffer_plan_by_lm_qty_adj",
                            "avg_weekly_st_lm_qty",
                            "buffer_plan_by_lm_val_adj",
                            "remaining_allocation_qty_region",
                            "woi_end_of_month_by_lm",
                        ]
                    ].fillna(
                        0
                    )

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

                    # Calculate Current WOI
                    result_df["Current WOI"] = calculate_woi(
                        result_df["total_stock"],
                        0,
                        result_df["avg_weekly_st_lm_qty"],
                    )
                    # progress.progress(0.6, "Calculations completed")

                    # Conditions for np.select

                    conditions = [
                        # Addtional: New condition for PO Qty > MAX_QTY_LIMIT for specific SKUs
                        (result_df["Customer SKU Code"].isin(LIMITED_SKUS_QTY)) & (result_df["PO Qty"] > MAX_QTY_LIMIT),
                        # 1. Reject if Remaining Allocation is less than 0
                        (result_df["remaining_allocation_qty_region"] < 0),
                        # 2. New condition for additional suggested SKUs
                        (result_df["is_po_sku"] == False),
                        # 3. Hardcoded Reject
                        result_df["Customer SKU Code"].isin(MANUAL_REJECT_SKUS),
                        # 3. Reject if the supply control status are ["STOP PO", "DISCONTINUED", "OOS"]
                        (result_df["supply_control_status_gt"].str.upper().isin(["STOP PO", "DISCONTINUED", "OOS", "UNAVAILABLE"])),
                        # 4. Proceed (ST LM = 0) -> NPD or there's no ST for LM
                        (
                            (result_df["avg_weekly_st_lm_qty"] == 0) &
                            (result_df["buffer_plan_by_lm_qty_adj"] == 0) &
                            (~result_df["Customer SKU Code"].str.upper().isin(all_npd_sku_list)) &
                            (~result_df["supply_control_status_gt"].str.upper().isin(["STOP PO", "DISCONTINUED", "OOS"]))
                        ),
                        # # 5. NPD with Allocation
                        # (
                        #     (result_df["remaining_allocation_qty_region"] > 0) &
                        #     (result_df["PO Qty"] <= result_df["remaining_allocation_qty_region"])
                        # ),
                        # 6. Reject if suggested PO is 0 or isin ["STOP PO", "DISCONTINUED", "OOS"]
                        (result_df["buffer_plan_by_lm_qty_adj"] == 0),
                        # 7. PO Qty > Suggested PO Qty (Over-ordering)
                        (result_df["PO Qty"] > result_df["buffer_plan_by_lm_qty_adj"]),
                        # 8. PO Qty < Suggested PO Qty (Under-ordering)
                        (result_df["PO Qty"] < result_df["buffer_plan_by_lm_qty_adj"]),
                        # 9. PO Qty = Suggested PO Qty (Exact Match)
                        (result_df["PO Qty"] == result_df["buffer_plan_by_lm_qty_adj"]),
                    ]

                    # Corresponding values
                    choices = [
                        f"Reject (Exceeds Qty Limit of {MAX_QTY_LIMIT})",
                        "Reject (Negative Allocation)",
                        "Additional Suggestion",
                        "Reject (Stop by Steve)",
                        "Reject",
                        "Proceed",
                        # "Proceed",
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
                    # progress.progress(0.9, "Rules applied")

                    new_column_names = {
                        "distributor_name": "Distributor",
                        "Customer SKU Code": "SKU",
                        "product_name": "Product Name",
                        "assortment": "Assortment",
                        "supply_control_status_gt": "Supply Control",
                        "PO Qty": "PO Qty",
                        "PO Value": "PO Value",
                        "total_stock": "Total Stock (Qty)",
                        "avg_weekly_st_lm_qty": "Avg Weekly Sales LM (Qty)",
                        "buffer_plan_by_lm_qty_adj": "Suggested PO Qty",
                        "buffer_plan_by_lm_val_adj": "Suggested PO Value",
                        "WOI PO Original": "WOI (Stock + PO Ori)",
                        "WOI Suggest": "WOI After Buffer (Stock + Suggested Qty)",
                        "woi_end_of_month_by_lm": "Stock + Suggested Qty WOI (Projection at EOM)",
                        "remaining_allocation_qty_region": "Remaining Allocation (By Region)",
                    }

                    # Rename the columns in the DataFrame
                    result_df.rename(columns=new_column_names, inplace=True)

                    # result_df = apply_sku_rejection_rules(REJECTED_SKUS_1, result_df, REGION_LIST_1, False)
                    # result_df = apply_sku_rejection_rules(REJECTED_SKUS_2, result_df, REGION_LIST_2, True)

                    # Sort the DataFrame: user SKUs first, then suggested SKUs
                    result_df.sort_values(
                        by=["is_po_sku", "SKU"], ascending=[False, True], inplace=True
                    )

                    result_df["RSA Notes"] = ""

                    # Reorder columns for display
                    excel_cols = [
                        "Distributor",
                        "SKU",
                        "Product Name",
                        "Assortment",
                        "Supply Control",
                        "Avg Weekly Sales LM (Qty)",
                        "Total Stock (Qty)",
                        "Current WOI",
                        "PO Qty",
                        "PO Value",
                        "WOI (Stock + PO Ori)",
                        "Remark",
                        "Suggested PO Qty",
                        "Suggested PO Value",
                        "WOI After Buffer (Stock + Suggested Qty)",
                        "Stock + Suggested Qty WOI (Projection at EOM)",
                        "Remaining Allocation (By Region)",
                        "is_po_sku",
                        "RSA Notes",
                    ]

                    result_df = result_df.reindex(columns=excel_cols)

                    # Append the processed DataFrame to the dictionary for Excel output
                    excel_dfs[distributor_name] = result_df.copy()

                    # Format 'PO Value' as currency with comma separators
                    result_df["PO Value"] = result_df["PO Value"].apply(
                        lambda x: f"{x:,.2f}" if pd.notnull(x) else 0
                    )
                    result_df["Suggested PO Value"] = result_df["Suggested PO Value"].apply(
                        lambda x: f"{x:,.2f}" if pd.notnull(x) else 0
                    )
                    result_df["Remaining Allocation (By Region)"] = result_df["Remaining Allocation (By Region)"].apply(
                        lambda x: f"{round(x):,d}" if pd.notnull(x) else 0
                    )
                    result_df["Avg Weekly Sales LM (Qty)"] = result_df["Avg Weekly Sales LM (Qty)"].apply(
                        lambda x: f"{round(x):,d}" if pd.notnull(x) else 0
                    )

                    # Format 'WOI' columns to 2 decimal places
                    result_df["WOI (Stock + PO Ori)"] = result_df[
                        "WOI (Stock + PO Ori)"
                    ].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "")

                    result_df["Stock + Suggested Qty WOI (Projection at EOM)"] = result_df[
                        "Stock + Suggested Qty WOI (Projection at EOM)"
                    ].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "")

                    result_df["Current WOI"] = result_df[
                        "Current WOI"
                    ].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "")

                    result_df["WOI After Buffer (Stock + Suggested Qty)"] = result_df[
                        "WOI After Buffer (Stock + Suggested Qty)"
                    ].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "")

                    # Append the processed DataFrame to the list for on-screen display
                    display_dfs.append(result_df)

                progress.progress(1.0, "Processing complete")

                # --- Download Button ---
                xlsx_data = to_excel_with_styling(excel_dfs, all_npd_sku_list)

                # --- Download Button (Conditional Logic) ---
                if output_format == "Single Sheet (All Distributors Stacked)":
                    final_excel_df = pd.concat(excel_dfs.values(), ignore_index=True)
                    xlsx_data = to_excel_single_sheet(final_excel_df, all_npd_sku_list)
                    file_name = "po_simulator_result_single_sheet.xlsx"
                else:
                    xlsx_data = to_excel_with_styling(excel_dfs, all_npd_sku_list)
                    file_name = "po_simulator_result_separate_sheets.xlsx"

                st.download_button(
                    label=f"📥 Download PO Simulator Excel ({output_format})",
                    data=xlsx_data,
                    file_name=file_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

                # --- Display Results ---
                st.subheader("Simulated PO Data")
                final_display_df = pd.concat(display_dfs, ignore_index=True)

                # Reorder columns for display
                final_cols = [
                    "Distributor",
                    "SKU",
                    "Product Name",
                    "Assortment",
                    "Supply Control",
                    "Avg Weekly Sales LM (Qty)",
                    "Total Stock (Qty)",
                    "Current WOI",
                    "PO Qty",
                    "PO Value",
                    "WOI (Stock + PO Ori)",
                    "Remark",
                    "Suggested PO Qty",
                    "Suggested PO Value",
                    "WOI After Buffer (Stock + Suggested Qty)",
                    "Stock + Suggested Qty WOI (Projection at EOM)",
                    "Remaining Allocation (By Region)",
                ]

                st.dataframe(final_display_df.reindex(columns=final_cols).reset_index(drop=True))

            except Exception as e:
                st.error(f"An error occurred: {e}")
                st.info(
                    "Please ensure the uploaded file is a valid .xlsx or .csv and contains all the required columns."
                )

if __name__ == "__main__":
    main()