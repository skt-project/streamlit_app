import streamlit as st
import pandas as pd
from datetime import datetime
import io
import zipfile
import math
import re
import base64
import urllib.request
from pathlib import Path
import openpyxl
import numpy as np
from typing import List
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
)
import os

BASE_DIR = os.path.dirname(__file__)

print("BASE_DIR:", BASE_DIR)
print("FILES:", os.listdir(BASE_DIR))

pdfmetrics.registerFont(
    TTFont('Trebuchet', os.path.join(BASE_DIR, 'trebuc.ttf'))
)

pdfmetrics.registerFont(
    TTFont('Trebuchet-Bold', os.path.join(BASE_DIR, 'trebucbd.ttf'))
)
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    MATPLOTLIB_OK = True
except ImportError:
    MATPLOTLIB_OK = False

st.set_page_config(
    page_title="PO Simulator — Glad2Glow",
    layout="wide",
    page_icon= '📁',
    initial_sidebar_state="expanded",
)

DASHBOARD_URL_DEFAULT = "https://po-simulator.streamlit.app/"

# Google Drive share link untuk po_template.xlsx
# Format: https://drive.google.com/file/d/FILE_ID/view?usp=sharing
TEMPLATE_DRIVE_URL = "https://docs.google.com/spreadsheets/d/1_4SFn2_SvGm1on0EJkntYjC2cLvNZyDjX54zcQAWRtQ/edit?gid=0#gid=0"
#"https://docs.google.com/spreadsheets/d/1FD2WN8PutkwzXXRYSj1jpA4EyxqzAfStyg2KC3grR30/edit?usp=sharing"
TEMPLATE_PO_URL="https://docs.google.com/spreadsheets/d/1_4SFn2_SvGm1on0EJkntYjC2cLvNZyDjX54zcQAWRtQ/edit?gid=0#gid=0"
BQ_DATASET = "rsa"
BQ_TABLE = "stock_analysis"

try:
    gcp_secrets = st.secrets["connections"]["bigquery"]
    private_key = gcp_secrets["private_key"].replace("\\n", "\n")
    _bq_credentials = service_account.Credentials.from_service_account_info(
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
    
except Exception:

    GCP_CREDENTIALS_PATH = r"C:\Users\Shaltsa Nadya\Documents\try python\streamlit\skintific-data-warehouse-ea77119e2e7a.json"
    GCP_PROJECT_ID = "skintific-data-warehouse"
    BQ_DATASET = "rsa"
    BQ_TABLE = "stock_analysis"
    _bq_credentials = service_account.Credentials.from_service_account_file(
    GCP_CREDENTIALS_PATH
    )



@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    """Initializes and returns a BigQuery client."""
    return bigquery.Client(credentials=_bq_credentials, project=GCP_PROJECT_ID)


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_customer_names() -> list:
    """Fetch distributor list from BigQuery master_distributor table."""
    try:
        client = get_bq_client()
        query = """
            SELECT distributor
            FROM `skintific-data-warehouse.gt_schema.master_distributor`
            ORDER BY distributor
        """
        rows = client.query(query).result()
        return [r.distributor for r in rows if r.distributor]
    except Exception as e:
        st.warning(f"⚠️ Gagal memuat daftar distributor dari BigQuery: {e}")
        return []


# Load customer names dynamically from BigQuery
CUSTOMER_NAMES = fetch_customer_names()


def _drive_to_direct(url: str) -> str:
    """Convert Google Drive / Google Sheets share URL → direct download URL."""
    # Google Sheets: export as xlsx
    m = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', url)
    if m:
        return f"https://docs.google.com/spreadsheets/d/{m.group(1)}/export?format=xlsx"
    # Google Drive file: use usercontent domain (works for public files)
    m = re.search(r'/file/d/([a-zA-Z0-9_-]+)', url)
    if m:
        return f"https://drive.usercontent.google.com/download?id={m.group(1)}&export=download&authuser=0"
    # Already a direct link — return as-is
    return url

@st.cache_data(show_spinner=False)
def _fetch_template_bytes(url: str) -> bytes:
    """Download template xlsx dari Google Drive/Sheets, cache hasilnya."""
    if not url or not url.strip():
        raise ValueError("TEMPLATE_DRIVE_URL belum diset. Isi konstanta di bagian atas app.py.")
    direct = _drive_to_direct(url)
    headers = {"User-Agent": "Mozilla/5.0"}
    req = urllib.request.Request(direct, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = resp.read()
    if not data or data[:4] != b'PK\x03\x04':
        raise ValueError(
            "File yang didownload bukan xlsx yang valid. "
            "Pastikan file bisa diakses publik ('Anyone with the link')."
        )
    return data

def _logo_src() -> str:
    local = Path(__file__).parent / "logo.png"
    if local.exists():
        b64 = base64.b64encode(local.read_bytes()).decode()
        return f"data:image/png;base64,{b64}"
    return "https://glad2glow.com/cdn/shop/files/logo.png?height=628&v=1745724802&width=1200"

LOGO_URL = _logo_src()

PO_TEMPLATE_COLS = [
    'Distributor', 'SKU', 'Product Name', 'Assortment', 'Supply Control',
    'Avg Weekly Sales LM (Qty)', 'Total Stock (Qty)', 'Current WOI',
    'PO Qty', 'PO Value', 'WOI (Stock + PO Ori)', 'Remark',
    'Suggested PO Qty', 'Suggested PO Value',
    'WOI After Buffer (Stock + Suggested Qty)',
    'Stock + Suggested Qty WOI (Projection at EOM)',
    'Remaining Allocation (By Region)', 'RSA Notes',
]

PO_IMG_COLS = PO_TEMPLATE_COLS[6:13]
PO_COLS_copy = PO_TEMPLATE_COLS[:13]


@st.cache_data(ttl=21600, show_spinner="Fetching SKU data from BigQuery...")
def get_sku_data(sku_list: List[str]) -> pd.DataFrame:
    if not sku_list:
        return pd.DataFrame()
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.gt_schema.master_product"
    sku_list_str = ", ".join([f"'{sku}'" for sku in sku_list])

    query = f"""
    SELECT
        sku,
        product_name,
        price_for_distri
    FROM `{table_id}`
    WHERE UPPER(sku) IN ({sku_list_str})
    """
    try:
        df_sku_data = client.query(query).to_dataframe()
        return df_sku_data
    except Exception as e:
        st.error(f"Error fetching SKU data from BigQuery: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=21600, show_spinner="Fetching NPD Allocation data from BigQuery...")
def get_npd_data(sku_list: List[str]) -> pd.DataFrame:
    if not sku_list:
        return pd.DataFrame()
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.gt_schema.npd_allocation"
    sku_list_str = ", ".join([f"'{sku}'" for sku in sku_list])

    query = f"""
    SELECT
        calendar_date,
        region,
        sku
    FROM `{table_id}`

    WHERE sku IN ({sku_list_str})
    AND calendar_date = '2026-04-01'
    """
    try:
        df_sku_data = client.query(query).to_dataframe()
        return df_sku_data
    except Exception as e:
        st.error(f"Error fetching NPD data from BigQuery: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=21600, show_spinner="Fetching SKU suggestions from BigQuery...")
def get_distributor_suggestions(distributor_name: str, brand_name: str = "All") -> pd.DataFrame:
    if not distributor_name:
        return pd.DataFrame()
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    _brand_filter = ""
    if brand_name and brand_name != "All":
        _brand_filter = f"AND UPPER(brand) = UPPER('{brand_name}')"
    
    query = f"""
    SELECT
        UPPER(region) AS REGION,
        UPPER(distributor) AS DISTRIBUTOR,
        UPPER(sku) AS SKU,
        ROUND(current_woi_by_lm, 2) AS CURRENT_WOI,
        buffer_plan_by_lm_qty_adj AS SUGGESTION_QTY,
        ROUND(
            SAFE_DIVIDE(
                COALESCE(total_stock, 0) + COALESCE(buffer_plan_by_lm_qty_adj, 0),
                NULLIF(avg_weekly_st_lm_qty, 0)
            ), 2
        ) AS WOI_AFTER_PO,
        remaining_allocation_qty_region AS REMAINING_ALLOCATION,
        CASE 
            WHEN remaining_allocation_qty_region >0 THEN 'Terdapat Alokasi' 
            else 'Alokasi Habis' 
        END AS STATUS_ALOKASI
    FROM `{table_id}`
    WHERE UPPER(distributor) = '{distributor_name.upper()}'
      AND buffer_plan_by_lm_qty_adj > 0
      {_brand_filter}
    ORDER BY SUGGESTION_QTY DESC
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error fetching distributor suggestions: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=21600, show_spinner="Fetching Stock Analysis data from BigQuery...")
def get_stock_data(distributor_name: str, sku_list: List[str]) -> pd.DataFrame:
    if not sku_list:
        return pd.DataFrame()
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    sku_list_str = ", ".join([f"'{sku}'" for sku in sku_list])

    query = f"""
    SELECT
        UPPER(region) AS region,
        UPPER(distributor) AS distributor,
        sku,
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
        UPPER(sku) IN ({sku_list_str})
        OR buffer_plan_by_lm_qty_adj > 0
    )
    """
    try:
        df_sku_data = client.query(query).to_dataframe()
        return df_sku_data
    except Exception as e:
        st.error(f"Error fetching Stock Analysis data from BigQuery: {e}")
        return pd.DataFrame()


#@st.cache_data(ttl=21600, show_spinner="Fetching SKU master list from BigQuery...")
#def get_sku_master_data(brand: str = None) -> pd.DataFrame:
#    """Fetches SKU master list from BigQuery, optionally filtered by brand."""
#    client = get_bq_client()
#    table_id = f"{GCP_PROJECT_ID}.gt_schema.master_product"
#    where_clause = f"WHERE LOWER(brand) = LOWER('{brand}')" if brand else ""
#    query = f"""
#    SELECT
#        sku,
#        product_name,
#        brand,
#        product_life_cycle,
#        price_for_distri
#    FROM `{table_id}`
#    {where_clause}
#    ORDER BY sku
#    """
#    try:
#        return client.query(query).to_dataframe()
#    except Exception as e:
#        st.error(f"Error fetching SKU master data: {e}")
#        return pd.DataFrame()
#

@st.cache_data(ttl=21600, show_spinner=False)
def get_brand_list() -> list:
    """Fetches distinct brand list from BigQuery master_product."""
    try:
        client = get_bq_client()
        query = f"""
        SELECT DISTINCT brand
        FROM `{GCP_PROJECT_ID}.gt_schema.master_product`
        WHERE brand IS NOT NULL
        ORDER BY brand
        """
        rows = client.query(query).result()
        return [r.brand for r in rows if r.brand]
    except Exception as e:
        return []


def calculate_woi(stock: pd.Series, po_qty: pd.Series, avg_weekly_sales: pd.Series) -> pd.Series:
    """
    Calculates Weeks of Inventory (WOI) based on the formula:
    (Stock + PO Quantity) / Average Weekly Sales LM
    """
    return np.where(avg_weekly_sales > 0, (stock + po_qty) / avg_weekly_sales, 0)


def apply_sku_rejection_rules(sku_list: List, df: pd.DataFrame, regions: List[str], is_in: bool) -> pd.DataFrame:
    """
    Auto-rejects specific SKUs based on a provided list and region rules.
    """
    regions_upper = [r.upper() for r in regions]

    if "SKU" not in df.columns or "region" not in df.columns:
        return df

    if not is_in:
        condition = (df["SKU"].isin(sku_list)) & (~df["region"].str.upper().isin(regions_upper))
    else:
        condition = (df["SKU"].isin(sku_list)) & (df["region"].str.upper().isin(regions_upper))

    df.loc[condition, "Remark"] = "Reject (Stop by Steve)"

    return df


def to_excel_with_styling(dfs: dict, npd_sku_list: List[str] = None) -> bytes:
    """
    Converts a pandas DataFrame to an Excel file with special styling for the first 7 columns.
    Applies specific color to 'Remaining Allocation (By Region)' column if SKU is in npd_sku_list.
    """
    output = io.BytesIO()
    wb = Workbook()

    del wb["Sheet"]

    po_fill = PatternFill(start_color="D9EAD3", end_color="D9EAD3", fill_type="solid")
    suggestion_fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")
    npd_fill = PatternFill(start_color="B1DBF0", end_color="B1DBF0", fill_type="solid")

    header_font = Font(bold=True)
    header_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
    header_alignment = Alignment(horizontal='left', vertical='center')

    proceed_font = Font(bold=True, color="54CE54")
    reject_font = Font(bold=True, color="D73E3E")
    suggest_font = Font(bold=True, color="F3C94C")

    for sheet_name, df in dfs.items():
        ws = wb.create_sheet(title=sheet_name[:31])

        is_po_sku_series = df["is_po_sku"]
        df_no_flag = df.drop("is_po_sku", axis=1)

        rows = dataframe_to_rows(df_no_flag, index=False, header=True)

        headers = list(df_no_flag.columns)
        col_map = {col: i for i, col in enumerate(headers)}

        currency_cols = ["PO Value", "Suggested PO Value"]
        integer_cols = ["Remaining Allocation (By Region)", "Avg Weekly Sales LM (Qty)"]
        decimal_cols = ["WOI (Stock + PO Ori)", "Current WOI", "WOI After Buffer (Stock + Suggested Qty)", "Stock + Suggested Qty WOI (Projection at EOM)"]

        for r_idx, row in enumerate(rows, 1):
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx, value=value)

                if r_idx == 1:
                    cell.font = header_font
                    cell.fill = header_fill
                    cell.alignment = header_alignment

                if c_idx <= 11 and r_idx > 1:
                    original_row_index = (r_idx - 2)
                    is_po_row = is_po_sku_series.iloc[original_row_index]

                    if is_po_row:
                        cell.fill = po_fill
                    else:
                        cell.fill = suggestion_fill

                if r_idx > 1:
                    col_name = headers[c_idx - 1]

                    if col_name in currency_cols:
                        cell.number_format = "#,##0.00"
                    elif col_name in integer_cols:
                        cell.number_format = "#,##0"
                    elif col_name in decimal_cols:
                        cell.number_format = "0.00"

                    if col_name == "Remark":
                        remark_value = row[c_idx - 1]
                        if "Proceed" in remark_value:
                            cell.font = proceed_font
                        elif "Reject" in remark_value:
                            cell.font = reject_font
                        elif "Additional" in remark_value:
                            cell.font = suggest_font

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

    po_fill = PatternFill(start_color="D9EAD3", end_color="D9EAD3", fill_type="solid")
    suggestion_fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")
    npd_fill = PatternFill(start_color="B1DBF0", end_color="B1DBF0", fill_type="solid")

    header_font = Font(bold=True)
    header_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
    header_alignment = Alignment(horizontal='left', vertical='center')

    proceed_font = Font(bold=True, color="54CE54")
    reject_font = Font(bold=True, color="D73E3E")
    suggest_font = Font(bold=True, color="F3C94C")

    is_po_sku_series = df["is_po_sku"]
    df_no_flag = df.drop("is_po_sku", axis=1)

    rows = dataframe_to_rows(df_no_flag, index=False, header=True)

    headers = list(df_no_flag.columns)
    col_map = {col: i for i, col in enumerate(headers)}

    currency_cols = ["PO Value", "Suggested PO Value"]
    integer_cols = ["Remaining Allocation (By Region)", "Avg Weekly Sales LM (Qty)"]
    decimal_cols = ["WOI (Stock + PO Ori)", "Current WOI", "WOI After Buffer (Stock + Suggested Qty)", "Stock + Suggested Qty WOI (Projection at EOM)"]

    for r_idx, row in enumerate(rows, 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)

            if r_idx == 1:
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = header_alignment

            if c_idx <= 11 and r_idx > 1:
                original_row_index = (r_idx - 2)
                is_po_row = is_po_sku_series.iloc[original_row_index]

                if is_po_row:
                    cell.fill = po_fill
                else:
                    cell.fill = suggestion_fill

            if r_idx > 1:
                col_name = headers[c_idx - 1]

                if col_name in currency_cols:
                    cell.number_format = "#,##0.00"
                elif col_name in integer_cols:
                    cell.number_format = "#,##0"
                elif col_name in decimal_cols:
                    cell.number_format = "0.00"

                if col_name == "Remark":
                    remark_value = row[c_idx - 1]
                    if "Proceed" in remark_value:
                        cell.font = proceed_font
                    elif "Reject" in remark_value:
                        cell.font = reject_font
                    elif "Additional" in remark_value:
                        cell.font = suggest_font

                if col_name in ["Remaining Allocation (By Region)", "Suggested PO Qty", "Suggested PO Value"] and npd_sku_list is not None:
                    sku_col_index = col_map.get("SKU")
                    if sku_col_index is not None:
                        sku_value = row[sku_col_index]
                        if sku_value in npd_sku_list:
                            cell.fill = npd_fill

    wb.save(output)
    output.seek(0)
    return output.getvalue()


def to_excel_single_sheet_with_sku(
    df: pd.DataFrame,
    npd_sku_list: List[str] = None,
    sku_master_df: pd.DataFrame = None,
) -> bytes:
    """
    Like to_excel_single_sheet but adds a 2nd sheet 'SKU Master List'
    populated from sku_master_df (filtered by brand).
    Sheet 1: PO Simulator (same styling as before)
    Sheet 2: SKU Master List (dark navy header, alternating rows, price formatted)
    """
    output = io.BytesIO()
    wb = Workbook()
    ws = wb.active
    ws.title = "PO Simulator"

    # ── Sheet 1 fills / fonts ──────────────────────────────────────────────
    po_fill         = PatternFill(start_color="D9EAD3", end_color="D9EAD3", fill_type="solid")
    suggestion_fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")
    npd_fill        = PatternFill(start_color="B1DBF0", end_color="B1DBF0", fill_type="solid")
    header_font     = Font(bold=True)
    header_fill     = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
    header_alignment = Alignment(horizontal='left', vertical='center')
    proceed_font    = Font(bold=True, color="54CE54")
    reject_font     = Font(bold=True, color="D73E3E")
    suggest_font    = Font(bold=True, color="F3C94C")

    is_po_sku_series = df["is_po_sku"]
    df_no_flag       = df.drop("is_po_sku", axis=1)
    rows             = dataframe_to_rows(df_no_flag, index=False, header=True)
    headers          = list(df_no_flag.columns)
    col_map          = {col: i for i, col in enumerate(headers)}

    currency_cols = ["PO Value", "Suggested PO Value"]
    integer_cols  = ["Remaining Allocation (By Region)", "Avg Weekly Sales LM (Qty)"]
    decimal_cols  = [
        "WOI (Stock + PO Ori)", "Current WOI",
        "WOI After Buffer (Stock + Suggested Qty)",
        "Stock + Suggested Qty WOI (Projection at EOM)",
    ]

    for r_idx, row in enumerate(rows, 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)

            if r_idx == 1:
                cell.font      = header_font
                cell.fill      = header_fill
                cell.alignment = header_alignment

            if c_idx <= 11 and r_idx > 1:
                original_row_index = r_idx - 2
                is_po_row = is_po_sku_series.iloc[original_row_index]
                cell.fill = po_fill if is_po_row else suggestion_fill

            if r_idx > 1:
                col_name = headers[c_idx - 1]

                if col_name in currency_cols:
                    cell.number_format = "#,##0.00"
                elif col_name in integer_cols:
                    cell.number_format = "#,##0"
                elif col_name in decimal_cols:
                    cell.number_format = "0.00"

                if col_name == "Remark":
                    remark_value = row[c_idx - 1]
                    if "Proceed" in remark_value:
                        cell.font = proceed_font
                    elif "Reject" in remark_value:
                        cell.font = reject_font
                    elif "Additional" in remark_value:
                        cell.font = suggest_font

                if (
                    col_name in ["Remaining Allocation (By Region)", "Suggested PO Qty", "Suggested PO Value"]
                    and npd_sku_list is not None
                ):
                    sku_col_index = col_map.get("SKU")
                    if sku_col_index is not None:
                        sku_value = row[sku_col_index]
                        if sku_value in npd_sku_list:
                            cell.fill = npd_fill

    # ── Sheet 2: SKU Master List ───────────────────────────────────────────
    #if sku_master_df is not None and not sku_master_df.empty:
    #    ws2 = wb.create_sheet(title="SKU Master List")
#
    #    # Header styling — dark navy background, white bold text
    #    sku_hdr_fill  = PatternFill(start_color="1A1A2E", end_color="1A1A2E", fill_type="solid")
    #    sku_hdr_font  = Font(bold=True, color="FFFFFF")
    #    sku_hdr_align = Alignment(horizontal="left", vertical="center")
#
    #    # Alternating row fill
    #    even_fill = PatternFill(start_color="EBF5FB", end_color="EBF5FB", fill_type="solid")
#
    #    # Rename columns for a clean display
    #    _display_df = sku_master_df.rename(columns={
    #        "sku":               "SKU",
    #        "product_name":      "Product Name",
    #        "brand":             "Brand",
    #        "product_life_cycle": "Product Life Cycle",
    #        "price_for_distri":  "Price (Distributor)",
    #    })
#
    #    sku_rows = list(dataframe_to_rows(_display_df, index=False, header=True))
    #    sku_headers = list(_display_df.columns)
#
    #    for r_idx, row in enumerate(sku_rows, 1):
    #        for c_idx, value in enumerate(row, 1):
    #            cell = ws2.cell(row=r_idx, column=c_idx, value=value)
#
    #            if r_idx == 1:
    #                # Header row
    #                cell.font      = sku_hdr_font
    #                cell.fill      = sku_hdr_fill
    #                cell.alignment = sku_hdr_align
    #            else:
    #                # Alternating rows
    #                if r_idx % 2 == 0:
    #                    cell.fill = even_fill
    #                # Number format for price column
    #                col_name = sku_headers[c_idx - 1]
    #                if col_name == "Price (Distributor)":
    #                    cell.number_format = "#,##0.00"
#
    #    # Auto-fit column widths (cap at 50)
    #    for col_cells in ws2.columns:
    #        max_len = max(
    #            (len(str(cell.value)) if cell.value is not None else 0)
    #            for cell in col_cells
    #        )
    #        ws2.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 50)
#
    #    # Freeze header row
    #    ws2.freeze_panes = "A2"
#
    wb.save(output)
    output.seek(0)
    return output.getvalue()
#

def check_password():

    def login_form():
        st.markdown("""
        <style>
        body, p, div, span, label, input, button, h1, h2, h3, h4, h5, h6 { font-family: 'Trebuchet MS', sans-serif; }
        html, body, [data-testid="stAppViewContainer"] {
            background: linear-gradient(145deg, #F13E93 40%,#FFA6A6 50%, #F26076 60%) !important;
            min-height: 50vh;
        }
        [data-testid="stSidebar"] { display: none !important; }
        .login-card {
            background: rgba(255,255,255,0.92);
            border-radius: 24px;
            padding: 2.5rem 2rem 2rem;
            box-shadow: 0 20px 60px rgba(202,97,128,0.25);
            border: 1px solid rgba(252,183,199,0.5);
            backdrop-filter: blur(12px);
        }
        .login-petal {
            position: fixed; pointer-events: none; font-size: 1.8rem; opacity: 0.18;
        }
        </style>
        <div class="login-petal" style="top:8%;left:5%;">🌸</div>
        <div class="login-petal" style="top:15%;right:8%;">🌺</div>
        <div class="login-petal" style="bottom:20%;left:10%;">🌸</div>
        <div class="login-petal" style="bottom:10%;right:6%;">💐</div>
        """, unsafe_allow_html=True)

        _, col, _ = st.columns([1, 1.5, 1])
        with col:

            st.markdown(
                f'<div style="text-align:center;padding:0.5rem 0 0.2rem;">'
                f'<img src="{LOGO_URL}" style="max-width:200px;height:auto;display:inline-block;background:transparent;" />'
                f'</div>',
                unsafe_allow_html=True
            )
            st.markdown("""
            <div style="text-align:center; margin: 1rem 0 1.5rem;">
                <div style="font-size:1.5rem; font-weight:700; color:#6E253A; letter-spacing:0.5px;">
                    DataFlow Automator
                </div>
                <div style="color:#000000; font-size:0.85rem; margin-top:0.3rem;">
                    Masukkan passwordmu
                </div>
            </div>
            """, unsafe_allow_html=True)

            with st.form("login_form"):
                password = st.text_input(
                    "🔒 Password",
                    type="password",
                    placeholder="Masukkan password...",
                )
                submitted = st.form_submit_button(
                    "Login", use_container_width=True
                )
                if submitted:
                    if password == st.secrets["glowithyou"]:
                        st.session_state["authenticated"] = True
                        st.rerun()
                    else:
                        st.error("❌ Password salah. Silakan coba lagi.")

            st.markdown("""
            <div style="text-align:center; margin-top:1.5rem; color:#6E253A; font-size:0.72rem;">
                Glad2Glow
            </div>
            </div>
            """, unsafe_allow_html=True)

    if st.session_state.get("authenticated"):
        return True

    login_form()
    return False

if not check_password():
    st.stop()

CUSTOM_CSS = """
<style>

body, p, div, span, label, input, textarea, select, button,
h1, h2, h3, h4, h5, h6, li, td, th, caption, small, strong, em {
    font-family: 'Trebuchet MS', sans-serif;
    box-sizing: border-box;
}

[data-testid="stSidebarCollapseButton"] { display: none !important; }

:root {
    --rose:      #CA6180;
    --rose-dark: #A84D6A;
    --blush:     #FCB7C7;
    --dark:      #BF3979;
    --dark2:     #751F58;
    --border:    rgba(0,0,0,0.08);
    --shadow:    0 2px 12px rgba(0,0,0,0.07);
    --shadow-lg: 0 8px 32px rgba(0,0,0,0.12);
    --g-rose:    linear-gradient(135deg, #751F58, #A84D6A);
}

h1,h2,h3,h4,h5,h6 { color: #FFFFFF !important; }

[data-testid="stSidebar"] {
    background: var(--dark) !important;
    border-right: 1px solid rgba(255,255,255,0.06) !important;
    box-shadow: 4px 0 24px rgba(0,0,0,0.35);
}
[data-testid="stSidebar"], [data-testid="stSidebar"] * { color: #FFFFFF !important; }
[data-testid="stSidebar"] input {
    background: rgba(255,255,255,0.1) !important;
    border: 1px solid rgba(255,255,255,0.2) !important;
    color: #2D1B26 !important; border-radius: 8px !important;
}
[data-testid="stSidebar"] input::placeholder { color: rgba(255,255,255,0.4) !important; }
[data-testid="stSidebar"] [data-baseweb="select"] > div {
    background: rgba(255,255,255,0.1) !important;
    border: 1px solid rgba(255,255,255,0.2) !important;
    color: #FFFFFF !important;
}
[data-testid="stSidebar"] hr { border-color: rgba(255,255,255,0.1) !important; }

[data-testid="stMain"] input, [data-testid="stMain"] textarea {
    color: #FFFFFF !important; background: rgba(255,255,255,0.15) !important;
    border: 1.5px solid rgba(255,255,255,0.35) !important; border-radius: 10px !important;
}
[data-testid="stMain"] input:focus {
    border-color: #FFFFFF !important;
    box-shadow: 0 0 0 3px rgba(255,255,255,0.15) !important;
}
[data-testid="stMain"] .stSelectbox > div > div {
    color: #FFFFFF !important; background: rgba(255,255,255,0.15) !important;
    border: 1.5px solid rgba(255,255,255,0.35) !important; border-radius: 10px !important;
}
[data-testid="stMain"] [data-baseweb="select"] span,
[data-testid="stMain"] [data-baseweb="select"] div { color: #FFFFFF !important; }

.hero-wrap { padding: 0.5rem 0 1.5rem; }
.hero-tag {
    display: inline-block;
    background: rgba(168,77,106,0.6);
    color: #FFFFFF !important;
    font-size: 0.72rem; font-weight: 700;
    letter-spacing: 1.5px; text-transform: uppercase;
    padding: 0.25rem 0.8rem; border-radius: 20px;
    border: 1px solid rgba(255,255,255,0.25);
    margin-bottom: 0.75rem;
}
.hero-title {
    font-size: 2rem; font-weight: 700; color: #FFFFFF !important;
    margin-bottom: 0.5rem; line-height: 1.25;
}
.hero-sub { color: rgba(255,255,255,0.85) !important; font-size: 0.92rem; line-height: 1.7; max-width: 600px; }

.pipeline-step {
    background: #A84D6A; border: none;
    border-radius: 12px; padding: 0.9rem 1.3rem;
    margin-bottom: 1rem;
    box-shadow: 0 2px 12px rgba(0,0,0,0.15);
}
.pipeline-step *, .pipeline-step strong, .pipeline-step code { color: #FFFFFF !important; }
.pipeline-step.active   { background: #8B2040; }
.pipeline-step.completed { background: #3E7D6A; }
.step-number {
    display: inline-flex; align-items: center; justify-content: center;
    width: 24px; height: 24px; border-radius: 50%;
    background: var(--g-rose); color: #FFF !important;
    font-weight: 700; font-size: 0.75rem;
    margin-right: 0.6rem; vertical-align: middle;
}

.metric-card {
    background: rgba(168,77,106,0.35); border: 1px solid rgba(255,255,255,0.2);
    border-radius: 12px; padding: 1.1rem 1.3rem;
    box-shadow: var(--shadow);
    transition: transform 0.2s, box-shadow 0.2s;
    position: relative; overflow: hidden;
}
.metric-card::before {
    content: ''; position: absolute; top: 0; left: 0;
    width: 3px; height: 100%; background: rgba(255,255,255,0.5);
    border-radius: 3px 0 0 3px;
}
.metric-card:hover { transform: translateY(-2px); box-shadow: var(--shadow-lg); }
.metric-label {
    color: rgba(255,255,255,0.7) !important; font-size: 0.68rem;
    text-transform: uppercase; letter-spacing: 1.1px; font-weight: 700; margin-bottom: 0.3rem;
}
.metric-value { font-size: 1.5rem; font-weight: 700; color: #FFFFFF !important; }
.metric-rose  .metric-value { color: var(--rose) !important; }
.metric-pink  .metric-value { color: #C96080 !important; }
.metric-muted .metric-value { color: var(--rose-dark) !important; }
.metric-blush .metric-value { color: #B05A77 !important; }

.feat-card {
    background: rgba(168,77,106,0.35); border: 1px solid rgba(255,255,255,0.2);
    border-radius: 16px; padding: 2rem 1.5rem;
    text-align: center; box-shadow: var(--shadow);
    transition: transform 0.2s, box-shadow 0.2s;
}
.feat-card:hover { transform: translateY(-3px); box-shadow: var(--shadow-lg); }
.feat-icon { font-size: 2.2rem; margin-bottom: 0.8rem; }
.feat-title { font-size: 0.95rem; font-weight: 700; color: #FFFFFF !important; margin-bottom: 0.4rem; }
.feat-desc  { font-size: 0.8rem; color: rgba(255,255,255,0.75) !important; line-height: 1.6; }

.badge {
    display: inline-block; padding: 0.18rem 0.65rem;
    border-radius: 20px; font-size: 0.65rem; font-weight: 700;
    text-transform: uppercase; letter-spacing: 0.7px;
}
.badge-success { background: rgba(5,150,105,0.25); color: #FFFFFF !important; border: 1px solid rgba(255,255,255,0.2); }
.badge-warning { background: rgba(168,77,106,0.4); color: #FFFFFF !important; border: 1px solid rgba(255,255,255,0.2); }
.badge-info    { background: rgba(255,255,255,0.2); color: #FFFFFF !important; border: 1px solid rgba(255,255,255,0.3); }

.stDownloadButton > button, .stButton > button {
    background: var(--g-rose) !important; color: #FFF !important;
    border: none !important; border-radius: 10px !important;
    font-weight: 700 !important; padding: 0.55rem 1.4rem !important;
    box-shadow: 0 2px 10px rgba(202,97,128,0.28) !important;
    transition: all 0.2s !important;
}
.stDownloadButton > button:hover, .stButton > button:hover {
    transform: translateY(-1px) !important;
    box-shadow: 0 5px 18px rgba(202,97,128,0.4) !important;
}

[data-testid="stDataFrame"] { border-radius: 10px !important; overflow: hidden !important; }
hr { border-color: #E8EAED !important; margin: 1.2rem 0 !important; }
.stAlert { border-radius: 10px !important; }
.stAlert * { color: inherit !important; }
.streamlit-expanderHeader {
    background: rgba(255,255,255,0.15) !important; border-radius: 10px !important;
    border: 1px solid rgba(255,255,255,0.25) !important; color: #FFFFFF !important;
}
.streamlit-expanderHeader * { color: #FFFFFF !important; }
.gsheet-card {
    background: rgba(255,255,255,0.1); border: 1.5px dashed rgba(255,255,255,0.3);
    border-radius: 14px; padding: 1.4rem;
}
.gsheet-card * { color: #FFFFFF !important; }
.split-info {
    background: rgba(168,77,106,0.3); border: 1px solid rgba(255,255,255,0.2);
    border-radius: 10px; padding: 0.85rem 1.1rem; margin: 0.5rem 0;
}
.split-info * { color: #FFFFFF !important; }
[data-testid="stMain"] [data-testid="stDateInput"] input {
    color: #FFFFFF !important; background: rgba(255,255,255,0.15) !important;
    border: 1.5px solid rgba(255,255,255,0.35) !important; border-radius: 10px !important;
}

.product-banner {
    border-radius: 20px; overflow: hidden;
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    padding: 3rem 2.5rem; margin-top: 2rem;
    display: flex; align-items: center; justify-content: space-between;
    position: relative; min-height: 180px;
}
.product-banner::before {
    content: ''; position: absolute; inset: 0;
    background: radial-gradient(ellipse 400px 300px at 80% 50%,
        rgba(202,97,128,0.18) 0%, transparent 70%);
    pointer-events: none;
}
.banner-left { flex: 1; z-index: 1; }
.banner-label {
    font-size: 0.7rem; font-weight: 700; letter-spacing: 2px;
    text-transform: uppercase; color: var(--blush) !important;
    margin-bottom: 0.6rem;
}
.banner-title {
    font-size: 1.6rem; font-weight: 700; color: #FFFFFF !important;
    line-height: 1.3; margin-bottom: 0.5rem;
}
.banner-sub { font-size: 0.85rem; color: rgba(255,255,255,0.6) !important; }
.banner-right { z-index: 1; text-align: right; }
.banner-stat { font-size: 3.5rem; font-weight: 700; color: #FFFFFF !important; line-height: 1; }
.banner-stat-label { font-size: 0.78rem; color: rgba(255,255,255,0.55) !important; margin-top: 0.2rem; }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
#----------------------------CHANGE TO DARK MODE-----------------------
LIGHT_CSS = """
<style>
/* ==== Light mode overrides ==== */
html, body, [data-testid="stAppViewContainer"], [data-testid="stMain"],
.main, .block-container { background: #FFFFFF !important; }
[data-testid="stHeader"] { background: #FFFFFF !important; }

/* Force ALL text elements dark — override global #FFFFFF rules */
[data-testid="stMain"] *:not(svg):not(path):not(button) {
    color: #1F1F1F !important;
}
[data-testid="stMain"] h1, [data-testid="stMain"] h2, [data-testid="stMain"] h3,
[data-testid="stMain"] h4, [data-testid="stMain"] h5, [data-testid="stMain"] h6,
[data-testid="stMain"] p, [data-testid="stMain"] span, [data-testid="stMain"] div,
[data-testid="stMain"] label, [data-testid="stMain"] li, [data-testid="stMain"] td,
[data-testid="stMain"] th, [data-testid="stMain"] strong, [data-testid="stMain"] em,
[data-testid="stMain"] small, [data-testid="stMain"] caption,
[data-testid="stMain"] [data-testid="stMarkdownContainer"],
[data-testid="stMain"] [data-testid="stMarkdownContainer"] * {
    color: #1F1F1F !important;
}
/* Muted / secondary text */
[data-testid="stMain"] .stCaption *,
[data-testid="stMain"] [data-testid="stCaptionContainer"] * { color: #5A5A5A !important; }

[data-testid="stMain"] input, [data-testid="stMain"] textarea {
    color: #1F1F1F !important; background: #FFFFFF !important;
    border: 1.5px solid #C9C9C9 !important;
}
[data-testid="stMain"] input::placeholder,
[data-testid="stMain"] textarea::placeholder { color: #8A8A8A !important; }
[data-testid="stMain"] .stSelectbox > div > div {
    color: #1F1F1F !important; background: #FFFFFF !important;
    border: 1.5px solid #C9C9C9 !important;
}
[data-testid="stMain"] [data-baseweb="select"] span,
[data-testid="stMain"] [data-baseweb="select"] div { color: #1F1F1F !important; }

/* Gray buttons with visible dark text */
[data-testid="stMain"] .stDownloadButton > button,
[data-testid="stMain"] .stButton > button {
    background: #D9D9D9 !important;
    color: #111111 !important;
    border: 1px solid #B0B0B0 !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.08) !important;
    font-weight: 700 !important;
}
[data-testid="stMain"] .stDownloadButton > button:hover,
[data-testid="stMain"] .stButton > button:hover {
    background: #C2C2C2 !important;
    color: #000000 !important;
    box-shadow: 0 3px 10px rgba(0,0,0,0.12) !important;
}

/* Hero / pipeline */
[data-testid="stMain"] .hero-title { color: #1F1F1F !important; }
[data-testid="stMain"] .hero-sub { color: rgba(31,31,31,0.75) !important; }
[data-testid="stMain"] .hero-tag {
    background: #F7C8D6 !important; color: #6E253A !important;
    border: 1px solid #E0B4C4 !important;
}

/* Cards */
[data-testid="stMain"] .metric-card, [data-testid="stMain"] .feat-card {
    background: #FAFAFA !important; border: 1px solid #E0E0E0 !important;
}
[data-testid="stMain"] .metric-label { color: #ffffff !important; }
[data-testid="stMain"] .metric-value { color: #1F1F1F !important; }
[data-testid="stMain"] .feat-title { color: #1F1F1F !important; }
[data-testid="stMain"] .feat-desc  { color: #5A5A5A !important; }

/* Badges */
[data-testid="stMain"] .badge-info {
    background: #EEE !important; color: #1F1F1F !important;
    border: 1px solid #CCC !important;
}

/* Dividers & containers */
[data-testid="stMain"] hr { border-color: #E0E0E0 !important; }
[data-testid="stMain"] [data-testid="stVerticalBlockBorderWrapper"] {
    border-color: #E0E0E0 !important;
}

/* Expander header + content */
[data-testid="stMain"] details summary { color: #1F1F1F !important; }
[data-testid="stMain"] [data-testid="stExpander"] { background: #FAFAFA !important; }

/* Captions */
[data-testid="stMain"] [data-testid="stCaptionContainer"],
[data-testid="stMain"] .stCaption { color: #5A5A5A !important; }

/* File uploader dropzone */
[data-testid="stMain"] [data-testid="stFileUploaderDropzone"],
[data-testid="stMain"] [data-testid="stFileUploader"] section {
    background: #FAFBFC !important;
    border: 1.5px dashed #D8DEE5 !important;
    border-radius: 10px !important;
    color: #1F1F1F !important;
    padding: 0.8rem 1rem !important;
}
[data-testid="stMain"] [data-testid="stFileUploaderDropzone"] *,
[data-testid="stMain"] [data-testid="stFileUploader"] section * {
    color: #1F1F1F !important;
}
[data-testid="stMain"] [data-testid="stFileUploaderDropzone"] small,
[data-testid="stMain"] [data-testid="stFileUploader"] small { color: #5A5A5A !important; }
[data-testid="stMain"] [data-testid="stFileUploaderDropzone"] button {
    background: #D9D9D9 !important; color: #111111 !important;
    border: 1px solid #B0B0B0 !important; border-radius: 8px !important;
    font-weight: 700 !important;
}
[data-testid="stMain"] [data-testid="stFileUploaderDropzone"] button:hover {
    background: #C2C2C2 !important;
}
/* Uploaded file card */
[data-testid="stMain"] [data-testid="stFileUploader"] *:not(svg):not(path) {
    background: #F5F7FA !important;
    background-color: #F5F7FA !important;
    color: #1F1F1F !important;
}
[data-testid="stMain"] [data-testid="stFileUploader"] section {
    background: #FAFBFC !important;
    background-color: #FAFBFC !important;
    border: 1.5px dashed #D8DEE5 !important;
}
[data-testid="stMain"] [data-testid="stFileUploaderFile"],
[data-testid="stMain"] [data-testid="stFileUploader"] ul li {
    border: 1px solid #D5DAE0 !important;
    border-radius: 8px !important;
}
[data-testid="stMain"] [data-testid="stFileUploader"] svg,
[data-testid="stMain"] [data-testid="stFileUploader"] svg path {
    fill: #8B2040 !important; color: #8B2040 !important;
}
[data-testid="stMain"] [data-testid="stFileUploader"] svg,
[data-testid="stMain"] [data-testid="stFileUploader"] [data-testid="stFileUploaderFile"] > div:first-child,
[data-testid="stMain"] [data-testid="stFileUploaderFile"] div:has(> svg) {
    background: transparent !important;
    background-color: transparent !important;
}
[data-testid="stMain"] [data-testid="stFileUploaderDeleteBtn"],
[data-testid="stMain"] [data-testid="stFileUploaderFile"] button {
    background: transparent !important;
    background-color: transparent !important;
    color: #1F1F1F !important; border: none !important;
}
[data-testid="stMain"] [data-testid="stFileUploaderDropzone"] button {
    background: #D9D9D9 !important; background-color: #D9D9D9 !important;
    color: #111111 !important; border: 1px solid #B0B0B0 !important;
}

/* Preview data (dataframe) */
[data-testid="stMain"] [data-testid="stDataFrame"],
[data-testid="stMain"] [data-testid="stDataFrameResizable"],
[data-testid="stMain"] [data-testid="stDataFrame"] > div {
    background: #F0F2F5 !important;
    border: 1px solid #D5DAE0 !important;
    border-radius: 8px !important;
}
[data-testid="stMain"] [data-testid="stDataFrame"] * { color: #F0F2F5 !important; }
[data-testid="stMain"] [data-testid="stDataFrame"] [role="gridcell"],
[data-testid="stMain"] [data-testid="stDataFrame"] [role="columnheader"] {
    background: #F0F2F5 !important; color: #F0F2F5 !important;
}
/* Expander */
[data-testid="stMain"] [data-testid="stExpander"],
[data-testid="stMain"] details {
    background: #F0F2F5 !important;
    border: 1px solid #D5DAE0 !important;
    border-radius: 8px !important;
}
[data-testid="stMain"] details summary { background: #F0F2F5 !important; color: #D5DAE0 !important; }

/* Tighter spacing */
[data-testid="stMain"] .pipeline-step {
    margin-bottom: 0.5rem !important;
    padding: 0.65rem 1rem !important;
    background: #F5EEF2 !important;
    border: 1px solid #E6D7DF !important;
    box-shadow: none !important;
}
[data-testid="stMain"] .pipeline-step.active { background: #F0D9E2 !important; border-color: #E0BDCC !important; }
[data-testid="stMain"] .pipeline-step.completed { background: #DFF0E7 !important; border-color: #BFDCCB !important; }
[data-testid="stMain"] .pipeline-step,
[data-testid="stMain"] .pipeline-step *,
[data-testid="stMain"] .pipeline-step strong,
[data-testid="stMain"] .pipeline-step code { color: #5A1E38 !important; }
[data-testid="stMain"] .pipeline-step .step-number {
    background: #8B2040 !important; color: #FFFFFF !important;
}
[data-testid="stMain"] .hero-wrap { padding: 0.3rem 0 0.6rem !important; }
[data-testid="stMain"] hr { margin: 0.6rem 0 !important; }
[data-testid="stMain"] [data-testid="stVerticalBlock"] { gap: 0.5rem !important; }

/* Number input steppers */
[data-testid="stMain"] [data-testid="stNumberInput"] button {
    background: #D9D9D9 !important; color: #111111 !important;
    border: 1px solid #B0B0B0 !important;
}
[data-testid="stMain"] [data-testid="stNumberInput"] input {
    background: #FFFFFF !important; color: #1F1F1F !important;
}

/* Text area */
[data-testid="stMain"] [data-testid="stTextArea"] textarea {
    background: #FFFFFF !important; color: #1F1F1F !important;
    border: 1.5px solid #C9C9C9 !important;
}

/* Tabs */
[data-testid="stMain"] [data-baseweb="tab-list"] { border-bottom-color: #D0D0D0 !important; }
[data-testid="stMain"] [data-baseweb="tab"] { color: #5A5A5A !important; }
[data-testid="stMain"] [data-baseweb="tab"][aria-selected="true"] { color: #1F1F1F !important; }

/* Alerts */
[data-testid="stMain"] .stAlert, [data-testid="stMain"] .stAlert * {
    color: #1F1F1F !important;
}
</style>
"""

#if st.session_state.get("light_mode", False):
#    st.markdown(LIGHT_CSS, unsafe_allow_html=True)
# Force light mode selalu aktif
st.markdown(LIGHT_CSS, unsafe_allow_html=True)

def detect_date_columns(df: pd.DataFrame) -> list:
    date_cols = []
    for col in df.columns:
        if df[col].dtype in ['datetime64[ns]', 'datetime64[ns, UTC]']:
            date_cols.append(col)
            continue
        if df[col].dtype == object:
            sample = df[col].dropna().head(50)
            if len(sample) == 0:
                continue
            try:
                parsed = pd.to_datetime(sample, errors='coerce')
                if parsed.notna().sum() / len(sample) > 0.6:
                    date_cols.append(col)
            except Exception:
                pass
    return date_cols

def safe_to_datetime(series: pd.Series) -> pd.Series:
    try:
        return pd.to_datetime(series, errors='coerce')
    except Exception:
        return series

def _convert_via_excel(fbytes: bytes, src_ext: str, dst_ext: str) -> bytes | None:
    _fmt_map = {"xlsx": 51, "xls": 56}
    if dst_ext not in _fmt_map:
        return None
    try:
        import tempfile, os
        import win32com.client as _wc  # type: ignore
        import pythoncom  # type: ignore
    except Exception:
        return None
    _fin_path = _fout_path = None
    _xl = None
    try:
        pythoncom.CoInitialize()
        with tempfile.NamedTemporaryFile(suffix=f".{src_ext}", delete=False) as _fin:
            _fin.write(fbytes)
            _fin_path = _fin.name
        _fout_path = _fin_path.rsplit(".", 1)[0] + f"_out.{dst_ext}"
        _xl = _wc.DispatchEx("Excel.Application")
        _xl.Visible = False
        _xl.DisplayAlerts = False
        _wb = _xl.Workbooks.Open(_fin_path, UpdateLinks=0, ReadOnly=True)
        _wb.SaveAs(_fout_path, FileFormat=_fmt_map[dst_ext])
        _wb.Close(SaveChanges=False)
        with open(_fout_path, "rb") as _f:
            return _f.read()
    except Exception:
        return None
    finally:
        try:
            if _xl is not None:
                _xl.Quit()
        except Exception:
            pass
        for _p in (_fin_path, _fout_path):
            if _p:
                try:
                    os.remove(_p)
                except Exception:
                    pass
        try:
            pythoncom.CoUninitialize()
        except Exception:
            pass

def _xls_to_xlsx_via_excel(fbytes: bytes) -> bytes | None:
    return _convert_via_excel(fbytes, "xls", "xlsx")

def _sanitize_xlsx_bytes(xlsx_bytes: bytes) -> bytes:
    """Strip external links & dangling externalReference entries that trigger
    Excel's 'We found a problem with some content' recovery prompt after an
    openpyxl re-save."""
    import zipfile, re
    _src = io.BytesIO(xlsx_bytes)
    _dst = io.BytesIO()
    try:
        with zipfile.ZipFile(_src, "r") as _zin:
            names = _zin.namelist()
            rels_name = "xl/_rels/workbook.xml.rels"
            drop_rids: set[str] = set()
            if rels_name in names:
                _rels_txt = _zin.read(rels_name).decode("utf-8", "ignore")
                for _m in re.finditer(
                    r'<Relationship\b[^>]*?Id="([^"]+)"[^>]*?Type="[^"]*externalLink[^"]*"[^>]*/>',
                    _rels_txt,
                ):
                    drop_rids.add(_m.group(1))
            with zipfile.ZipFile(_dst, "w", zipfile.ZIP_DEFLATED) as _zout:
                for name in names:
                    if name.startswith("xl/externalLinks/"):
                        continue
                    data = _zin.read(name)
                    if name == "xl/workbook.xml":
                        _txt = data.decode("utf-8", "ignore")
                        _txt = re.sub(
                            r"<externalReferences>.*?</externalReferences>",
                            "",
                            _txt,
                            flags=re.DOTALL,
                        )
                        data = _txt.encode("utf-8")
                    elif name == rels_name and drop_rids:
                        _txt = data.decode("utf-8", "ignore")
                        for _rid in drop_rids:
                            _txt = re.sub(
                                rf'<Relationship\b[^>]*?Id="{re.escape(_rid)}"[^>]*/>',
                                "",
                                _txt,
                            )
                        data = _txt.encode("utf-8")
                    _zout.writestr(name, data)
        return _dst.getvalue()
    except Exception:
        return xlsx_bytes

def _edit_qty_via_excel_com(xlsx_bytes: bytes, sheet_name: str, hdr_row_0: int,
                            sku_col_name: str, qty_col_name: str,
                            cell_writer) -> tuple[bytes, int] | None:
    """Edit qty cells via Excel COM (pywin32). Preserves all formatting,
    formulas, images. cell_writer(sku_val, qty_val) → new_qty | None.
    Returns (bytes, changed_count) on success, None on failure."""
    try:
        import tempfile, os
        import win32com.client as _wc  # type: ignore
        import pythoncom  # type: ignore
    except Exception:
        return None
    _fin_path = None
    _xl = None
    try:
        pythoncom.CoInitialize()
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as _fin:
            _fin.write(xlsx_bytes)
            _fin_path = _fin.name
        _xl = _wc.DispatchEx("Excel.Application")
        _xl.Visible = False
        _xl.DisplayAlerts = False
        _wb = _xl.Workbooks.Open(_fin_path, UpdateLinks=0)
        _ws = None
        for _s in _wb.Worksheets:
            if _s.Name == sheet_name:
                _ws = _s
                break
        if _ws is None:
            _ws = _wb.Worksheets(1)
        _hdr_row = hdr_row_0 + 1
        _used = _ws.UsedRange
        _max_col = _used.Columns.Count + _used.Column - 1
        _max_row = _used.Rows.Count + _used.Row - 1
        _sku_ci = _qty_ci = None
        for _c in range(1, _max_col + 1):
            _v = _ws.Cells(_hdr_row, _c).Value
            if _v == sku_col_name:
                _sku_ci = _c
            elif _v == qty_col_name:
                _qty_ci = _c
        changed = 0
        if _sku_ci and _qty_ci:
            for _r in range(_hdr_row + 1, _max_row + 1):
                _sv = _ws.Cells(_r, _sku_ci).Value
                _sv_str = str(_sv or "").strip()
                _qv = _ws.Cells(_r, _qty_ci).Value
                _new = cell_writer(_sv_str, _qv)
                if _new is not None:
                    _ws.Cells(_r, _qty_ci).Value = _new
                    changed += 1
        _wb.Save()
        _wb.Close(SaveChanges=False)
        with open(_fin_path, "rb") as _f:
            return _sanitize_xlsx_bytes(_f.read()), changed
    except Exception:
        return None
    finally:
        try:
            if _xl is not None:
                _xl.Quit()
        except Exception:
            pass
        if _fin_path:
            try:
                os.remove(_fin_path)
            except Exception:
                pass
        try:
            pythoncom.CoUninitialize()
        except Exception:
            pass

def _convert_to_xlsx(fname: str, fbytes: bytes) -> tuple[str, bytes]:
    """Convert xls/csv to xlsx bytes. Returns (new_fname, xlsx_bytes)."""
    ext = fname.rsplit(".", 1)[-1].lower()
    base = fname.rsplit(".", 1)[0]
    if ext == "xlsx":
        return fname, fbytes
    elif ext == "xls":
        _hifi = _xls_to_xlsx_via_excel(fbytes)
        if _hifi is not None:
            return base + ".xlsx", _hifi
        try:
            import xlrd as _xlrd
            _book = _xlrd.open_workbook(file_contents=fbytes)
            _wb = openpyxl.Workbook()
            _wb.remove(_wb.active)
            _vis_map = {0: 'visible', 1: 'hidden', 2: 'veryHidden'}
            for _si in range(_book.nsheets):
                _ws_old = _book.sheet_by_index(_si)
                _ws_new = _wb.create_sheet(title=_ws_old.name)
                _vis = getattr(_book, 'sheet_visibility', None)
                _ws_new.sheet_state = _vis_map.get(_vis[_si] if _vis else 0, 'visible')
                for _r in range(_ws_old.nrows):
                    for _c in range(_ws_old.ncols):
                        _ws_new.cell(row=_r + 1, column=_c + 1, value=_ws_old.cell_value(_r, _c))
            _buf = io.BytesIO()
            _wb.save(_buf)
            return base + ".xlsx", _buf.getvalue()
        except Exception:
            pass
        _all_sheets = pd.read_excel(io.BytesIO(fbytes), sheet_name=None, dtype=object)
        _buf = io.BytesIO()
        with pd.ExcelWriter(_buf, engine="openpyxl") as _wr:
            for _nm, _df in _all_sheets.items():
                _df.to_excel(_wr, sheet_name=str(_nm)[:31], index=False)
        return base + ".xlsx", _buf.getvalue()
    elif ext == "csv":
        _df = pd.read_csv(io.BytesIO(fbytes), dtype=str, encoding_errors="replace")
        _buf = io.BytesIO()
        _df.to_excel(_buf, index=False, engine="openpyxl")
        return base + ".xlsx", _buf.getvalue()
    return fname, fbytes

def _append_to_template(df: pd.DataFrame, template_bytes: bytes, start_row: int = 9) -> bytes:
    wb = openpyxl.load_workbook(io.BytesIO(template_bytes), keep_links=False)
    ws = wb.active
    header_row_idx = start_row - 1
    tpl_headers = {}
    for col_idx in range(1, ws.max_column + 1):
        val = ws.cell(row=header_row_idx, column=col_idx).value
        if val is not None:
            tpl_headers[str(val).strip().lower()] = col_idx
    for row in ws.iter_rows(min_row=start_row):
        for cell in row:
            cell.value = None
    df_cols = [str(c).strip() for c in df.columns]
    for r_offset, row_data in enumerate(df.itertuples(index=False)):
        cur_row = start_row + r_offset
        for c_idx, col_name in enumerate(df_cols):
            tpl_col = tpl_headers.get(col_name.lower())
            if tpl_col is None:
                continue
            val = row_data[c_idx]
            ws.cell(row=cur_row, column=tpl_col).value = (
                None if (val is None or str(val).strip() in ("", "nan")) else val
            )
    buf = io.BytesIO()
    wb.save(buf)
    return _sanitize_xlsx_bytes(buf.getvalue())

def to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Sheet1") -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl', datetime_format='YYYY-MM-DD HH:mm:SS') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]
        for col_cells in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col_cells)
            header_len = len(str(col_cells[0].value or ""))
            ws.column_dimensions[col_cells[0].column_letter].width = min(max(max_len, header_len) + 3, 50)
    return buf.getvalue()

def create_zip_of_files(file_dict: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for fname, data in file_dict.items():
            zf.writestr(fname, data)
    return buf.getvalue()

def split_dataframe(df: pd.DataFrame, max_rows: int = 7500) -> list:
    if len(df) <= max_rows:
        return [df]
    n_chunks = math.ceil(len(df) / max_rows)
    return [df.iloc[i * max_rows:(i + 1) * max_rows].reset_index(drop=True) for i in range(n_chunks)]

def split_by_po_groups(df: pd.DataFrame, po_column: str | None, max_rows: int = 7500) -> list:
    if po_column is None or po_column not in df.columns:
        return split_dataframe(df, max_rows)
    if len(df) <= max_rows:
        return [df.reset_index(drop=True)]

    po_order = [v for v in df[po_column].drop_duplicates().tolist() if v == v and v is not None]
    po_sizes = df.groupby(po_column, sort=False).size()

    chunks = []
    current_pos: list = []
    current_size = 0

    for po_val in po_order:
        po_size = int(po_sizes.get(po_val, 0))
        if po_size == 0:
            continue
        if current_pos and current_size + po_size > max_rows:
            mask = df[po_column].isin(current_pos)
            chunks.append(df[mask].reset_index(drop=True))
            current_pos = []
            current_size = 0
        current_pos.append(po_val)
        current_size += po_size

    if current_pos:
        mask = df[po_column].isin(current_pos)
        chunks.append(df[mask].reset_index(drop=True))

    return chunks if chunks else [df.reset_index(drop=True)]

def gsheet_to_csv_url(url: str):
    match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', url)
    if not match:
        return None, None
    sheet_id = match.group(1)
    gid_match = re.search(r'gid=(\d+)', url)
    gid = gid_match.group(1) if gid_match else '0'
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    return csv_url, sheet_id

def attach_po_counts(df: pd.DataFrame, po_column: str) -> tuple:
    grouped = df.groupby(po_column).size().reset_index(name='count')
    grouped = grouped.sort_values('count', ascending=False).reset_index(drop=True)
    return df.merge(grouped, on=po_column, how='left'), grouped

def numeric_coerce(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        try:
            numeric_vals = pd.to_numeric(df[col], errors='coerce')
            if numeric_vals.notna().sum() / max(df[col].notna().sum(), 1) > 0.8:
                df[col] = numeric_vals
        except Exception:
            pass
    return df

_REMARK_STYLES = [
    ('reject with suggestion', '#FFF3CD', '#856404'),
    ('reject (stop by steve',  '#F8D7DA', '#721C24'),
    ('reject',                 '#F8D7DA', '#721C24'),
    ('proceed with suggestion','#D4EDDA', '#155724'),
    ('proceed',                '#D4EDDA', '#155724'),
    ('additional suggestion',  '#D1ECF1', '#0C5460'),
]

def df_to_image_bytes(df: pd.DataFrame, title: str = "") -> bytes:
    if not MATPLOTLIB_OK:
        raise RuntimeError("matplotlib tidak tersedia.")
    n_rows, n_cols = df.shape
    col_width = 2.0
    fig_w = max(16, n_cols * col_width)
    fig_h = max(2.5, n_rows * 0.38 + 1.6)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    ax.axis('off')
    if title:
        ax.set_title(title, fontsize=11, fontweight='bold', color='#1a1a2e', pad=10)

    cell_text = [[str(v) if pd.notna(v) else '' for v in row] for row in df.values]
    tbl = ax.table(
        cellText=cell_text,
        colLabels=df.columns.tolist(),
        loc='center',
        cellLoc='left',
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(7.5)
    tbl.auto_set_column_width(col=list(range(n_cols)))

    for j in range(n_cols):
        tbl[0, j].set_facecolor('#1a1a2e')
        tbl[0, j].set_text_props(color='white', fontweight='bold')

    remark_idx = df.columns.tolist().index('Remark') if 'Remark' in df.columns else None

    for i in range(1, n_rows + 1):
        row_bg = '#EBF5FB' if i % 2 == 0 else '#FFFFFF'
        for j in range(n_cols):
            tbl[i, j].set_facecolor(row_bg)
            tbl[i, j].set_text_props(color='#1a1a2e')
        if remark_idx is not None:
            val = str(df.iloc[i - 1, remark_idx]).lower().strip()
            for keyword, bg, fg in _REMARK_STYLES:
                if keyword in val:
                    tbl[i, remark_idx].set_facecolor(bg)
                    tbl[i, remark_idx].set_text_props(color=fg, fontweight='bold')
                    break

    fig.patch.set_facecolor('white')
    buf = io.BytesIO()
    plt.savefig(buf, format='jpg', bbox_inches='tight', dpi=150, facecolor='white')
    buf.seek(0)
    plt.close(fig)
    return buf.getvalue()

def validate_po_template(df: pd.DataFrame) -> tuple[bool, list]:
    missing = [c for c in PO_TEMPLATE_COLS if c not in df.columns]
    return len(missing) == 0, missing

def _get_sheet_names(file_bytes: bytes, engine: str) -> list[str]:
    """Kembalikan daftar sheet yang visible/unhide saja."""
    try:
        if engine == 'xlrd':
            import xlrd
            book = xlrd.open_workbook(file_contents=file_bytes)
            if hasattr(book, 'sheet_visibility'):
                return [book.sheet_name(i) for i in range(book.nsheets)
                        if book.sheet_visibility[i] == 0]
            else:
                return [book.sheet_name(i) for i in range(book.nsheets)
                        if getattr(book.sheet_by_index(i), 'visibility', 0) == 0]
        else:
            wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
            sheets = [ws.title for ws in wb.worksheets if ws.sheet_state == 'visible']
            wb.close()
            return sheets
    except Exception:
        return []

def _excel_engine(fname: str) -> str:
    if fname.lower().endswith('.xls'):
        import importlib.util
        if importlib.util.find_spec('xlrd') is None:
            raise ImportError(
                "Library `xlrd` tidak ditemukan di environment ini. "
                "Jalankan `pip install xlrd>=2.0.1` di terminal yang sama dengan Streamlit, "
                "atau konversi file .xls ke .xlsx terlebih dahulu."
            )
        return 'xlrd'
    return 'openpyxl'

def detect_header_row(file_bytes: bytes, fname: str = "", max_scan: int = 15, sheet_name=0) -> int:
    engine = _excel_engine(fname)
    df_raw = pd.read_excel(
        io.BytesIO(file_bytes), sheet_name=sheet_name, header=None,
        engine=engine, dtype=str, nrows=max_scan
    )
    best_row, best_score = 0, -1
    for i in range(len(df_raw)):
        vals = [str(v).strip() for v in df_raw.iloc[i].values if pd.notna(v) and str(v).strip()]
        text_count = sum(
            1 for v in vals
            if not v.replace('.', '', 1).replace(',', '', 1).lstrip('-').isdigit()
        )
        score = text_count * 10 + len(vals)
        if score > best_score:
            best_score = score
            best_row = i
    return best_row

with st.sidebar:

    st.markdown(
                f'<div style="text-align:center;padding:0.5rem 0 0.2rem;">'
                f'<img src="{LOGO_URL}" style="max-width:200px;height:auto;display:inline-block;background:transparent;" />'
                f'</div>',
                unsafe_allow_html=True
            )
    if 'page' not in st.session_state:
        st.session_state['page'] = 'extractor'

    
    #st.session_state.get("light_mode", False)
    #_prev_light= st.session_state.get("light_mode", False)
    #_new_light = st.toggle(
    #    "☀️ Light Mode" if _prev_light else "🌙 Dark Mode",
    #    value=_prev_light, key="light_mode",
    #)
    #if _new_light != _prev_light:
    #    st.rerun()

    st.markdown(
        "<div style='padding:0 0.4rem 0.3rem;;text-align:center;font-size:1rem;font-weight:700;"
        "letter-spacing:2.5px;color:rgba(255,255,255,0.35);text-transform:uppercase;'>MENU</div>",
        unsafe_allow_html=True
    )
    st.markdown("<div style='height:50px;'></div>", unsafe_allow_html=True)

    #if st.button("Data Extractor", use_container_width=True, key="nav_extractor"):
    #    st.session_state['page'] = 'extractor'
    #    st.rerun()
    if st.button("Request PO", use_container_width = True, key="nav_spv"):
        st.session_state['page'] = 'spv'
        st.rerun()
    if st.button("PO Changer", use_container_width=True, key="nav_po"):
        st.session_state['page'] = 'po_changer'
        st.rerun()
    

    #st.divider()
    #st.markdown(
    #    "<div style='padding:0 0.6rem;font-size:0.6rem;font-weight:700;letter-spacing:2px;"
    #    "color:rgba(255,255,255,0.35);text-transform:uppercase;margin-bottom:0.6rem;'>CONFIGURATION</div>",
    #    unsafe_allow_html=True
    #)
    #max_rows_per_file = st.number_input(
    #    "Max rows per file",
    #    min_value=100, max_value=100000, value=7500, step=500,
    #    help="Batas baris per file output"
    #)
    #po_col_override = st.text_input(
    #    "Nama kolom PO (opsional)",
    #    placeholder="Auto-detect",
    #    help="Kosongkan untuk auto-detect kolom PO"
    #)
    st.markdown("<div style='height:200px;'></div>", unsafe_allow_html=True)
    #st.divider()

    st.divider()
    if st.button("Logout", use_container_width=True):
        st.session_state["authenticated"] = False
        for k in ("raw_df", "data_source", "source_type"):
            st.session_state.pop(k, None)
        st.rerun()

    st.markdown(
        "<div style='text-align:center;color:rgba(255,255,255,0.25);font-size:0.62rem;margin-top:1rem;'>DataFlow v1.0 · Glad2Glow",
        unsafe_allow_html=True
    )


if st.session_state.get('page') == 'po_changer':
    st.markdown("""
    <div class="hero-wrap">
        <div class="hero-tag">✦ PO Management</div>
        <div class="hero-title">PO Changer</div>
    </div>
    """, unsafe_allow_html=True)

    st.divider()
    with st.popover("ⓘ Info Tutorial"):
        st.markdown("""
    **Tentang PO File:**
    1. Klik **Make a Copy** setelah pilih Distributor.
    2. Copy SKU dan QTY untuk dimasukkan dalam Spreadsheet.
    3. Buat Share File jadi **Anyone with Link - View** - Wajib.
    4. Paste link Spreadsheet yang sudah di buat **Make Copy**.
    5. Pilih Distributor (Jika belum ada Distributor dalam spreadsheet) dan Nama RSA yang akan di assign.
    6. Lakukan Preview File terlebih dahulu untuk memastikan ketepatan data.
    7. Export File bisa dalam bentuk PDF atau Excel.
    
    📌 **Template PO:** [Klik di sini](https://docs.google.com/spreadsheets/d/1_4SFn2_SvGm1on0EJkntYjC2cLvNZyDjX54zcQAWRtQ/copy)
    """)
    _INVALID_QTY = {"-", "null", "none", "", "0", "0.0"}

    def _read_one(fname: str, fbytes: bytes, sheet_name=0):
        ext = fname.rsplit(".", 1)[-1].lower()
        if ext == "csv":
            raw_preview = pd.read_csv(io.BytesIO(fbytes), header=None, dtype=str, nrows=15, encoding_errors="replace")
            best_row, best_score = 0, -1
            for i in range(len(raw_preview)):
                vals = [str(v).strip() for v in raw_preview.iloc[i].values if pd.notna(v) and str(v).strip()]
                text_count = sum(1 for v in vals if not v.replace('.','',1).replace(',','',1).lstrip('-').isdigit())
                score = text_count * 10 + len(vals)
                if score > best_score:
                    best_score, best_row = score, i
            df = pd.read_csv(io.BytesIO(fbytes), header=best_row, dtype=str, encoding_errors="replace")
            return df, best_row
        else:
            engine = _excel_engine(fname)
            hrow = detect_header_row(fbytes, fname, sheet_name=sheet_name)
            df = pd.read_excel(io.BytesIO(fbytes), sheet_name=sheet_name, header=hrow, engine=engine, dtype=str)
            return df, hrow

    def _parse_idx(rng: str):
        rng = rng.strip()
        if not rng or rng == ":":
            return None, None
        if ":" not in rng:
            n = int(rng)
            return n, n + 1
        left, right = rng.split(":", 1)
        start = int(left.strip()) if left.strip() else None
        end   = int(right.strip()) if right.strip() else None
        stop  = (end + 1) if end is not None else None
        return start, stop

    def _apply_range_r(df: pd.DataFrame, rng_r: str, rng_c: str) -> pd.DataFrame:
        if rng_r.strip():
            try:
                rs, re = _parse_idx(rng_r)
                df = df.iloc[rs:re]
            except Exception:
                pass
        if rng_c.strip():
            try:
                cs, ce = _parse_idx(rng_c)
                df = df.iloc[:, cs:ce]
            except Exception:
                pass
        return df

    folder_files = st.file_uploader(
        "📁 Upload File PO (.xlsx / .xls / .csv / .zip)",
        type=["xlsx", "xls", "csv", "zip"],
        accept_multiple_files=True,
        key="po_folder",
    )

    if folder_files:

        raw_entries = []
        _converted_names = []
        for uf in folder_files:
            fb = uf.read()
            if uf.name.lower().endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(fb)) as zf:
                    for zname in zf.namelist():
                        ext = zname.rsplit(".", 1)[-1].lower() if "." in zname else ""
                        if ext in ("xlsx", "xls", "csv"):
                            _new_name, _new_bytes = _convert_to_xlsx(zname, zf.read(zname))
                            raw_entries.append((_new_name, _new_bytes))
                            if ext != "xlsx":
                                _converted_names.append(f"{zname} → {_new_name}")
            else:
                _new_name, _new_bytes = _convert_to_xlsx(uf.name, fb)
                raw_entries.append((_new_name, _new_bytes))
                if uf.name.rsplit(".", 1)[-1].lower() != "xlsx":
                    _converted_names.append(f"{uf.name} → {_new_name}")

        if _converted_names:
            st.caption("🔄 Auto-convert: " + "  ·  ".join(_converted_names))

        st.markdown("""
        <div class="pipeline-step active">
            <span class="step-number">1</span>
            <strong>Konfigurasi per File</strong>
        </div>
        """, unsafe_allow_html=True)

        parsed = []
        for idx, (fname, fbytes) in enumerate(raw_entries):
            ext = fname.rsplit(".", 1)[-1].lower()
            with st.container(border=True):
                hc1, hc2 = st.columns([2, 1])
                with hc1:
                    st.markdown(f"**#{idx+1} &nbsp; {fname}**")
                with hc2:
                    try:
                        _wb = openpyxl.load_workbook(io.BytesIO(fbytes), data_only=True)
                        sheets = [ws.title for ws in _wb.worksheets if ws.sheet_state == 'visible']
                        _wb.close()
                    except Exception:
                        sheets = []
                    if len(sheets) > 1:
                        sheet_sel = st.selectbox(
                            "Sheet:", options=sheets,
                            key=f"fs_{idx}_{fname}",
                            label_visibility="collapsed",
                        )
                    elif sheets:
                        sheet_sel = sheets[0]
                        st.caption(f"📄 `{sheets[0]}`")
                    else:
                        sheet_sel = 0
                        st.caption("⚠️ Tidak ada sheet visible")

                try:
                    df_f, hrow = _read_one(fname, fbytes, sheet_sel)
                    df_f = df_f.loc[:, ~df_f.columns.astype(str).str.startswith('Unnamed')].dropna(how='all')
                    df_f = df_f[df_f.apply(lambda r: r.astype(str).str.strip().ne('').any(), axis=1)]
                    parse_err = None
                except Exception as e:
                    df_f, hrow, parse_err = None, "-", str(e)

                if df_f is not None:
                    df_f.columns = [str(c).strip().upper() for c in df_f.columns]

                    _qty_col = next((c for c in df_f.columns if c.strip().upper() in ("QTY", "QUANTITY")), None)
                    _before_qty = len(df_f)
                    if _qty_col:
                        def _qty_valid(v):
                            s = str(v).strip().lower()
                            if s in _INVALID_QTY:
                                return False
                            try:
                                return float(s.replace(",", ".")) > 0
                            except ValueError:
                                return False
                        df_f = df_f[df_f[_qty_col].apply(_qty_valid)].reset_index(drop=True)
                        _qty_removed = _before_qty - len(df_f)
                        if _qty_removed:
                            st.caption(f"🗑 {_qty_removed:,} baris dibuang (QTY tidak valid — kolom **{_qty_col}**)")

                    _dist_cols = [c for c in df_f.columns if "DISTRIBUTOR" in c.upper()]
                    _has_dist = len(_dist_cols) > 0

                    dc1, dc2 = st.columns([1, 2])
                    with dc1:
                        st.caption("Distributor" + (" *(sudah ada di kolom)*" if _has_dist else ""))
                    with dc2:
                        dist_val = st.selectbox(
                            "Distributor",
                            options=["(Pilih)"] + CUSTOMER_NAMES,
                            key=f"dist_{idx}_{fname}",
                            label_visibility="collapsed",
                        )

                    _df_preview = df_f.copy()
                    if dist_val not in ("", "(Pilih)"):
                        if _has_dist:
                            _df_preview["DISTRIBUTOR"] = dist_val
                        else:
                            _df_preview.insert(0, "DISTRIBUTOR", dist_val)
                    with st.expander(f"👁 Lihat isi  ·  header row {hrow}  ·  {len(_df_preview)} baris  ·  {_df_preview.shape[1]} kolom  ·  indeks 0–{_df_preview.shape[1]-1}", expanded=False):
                        st.dataframe(_df_preview.iloc[:,:6].reset_index(drop=True), use_container_width=True)

                    rc1, rc2 = st.columns(2)
                    with rc1:
                        row_rng = st.text_input("Row Range", value="",
                                                key=f"row_{idx}_{fname}",
                                                placeholder="5:10 | 5: | :10 | 5")
                    with rc2:
                        col_rng = st.text_input("Column Range", value="",
                                                key=f"col_{idx}_{fname}",
                                                placeholder="0:3 | 2: | :5 | 2")

                    if row_rng.strip() or col_rng.strip():
                        _pv = _apply_range_r(df_f.copy(), row_rng, col_rng)
                        st.caption(f"📍 Preview rentang: {len(_pv)} baris × {_pv.shape[1]} kolom")
                        st.dataframe(_pv, use_container_width=True, hide_index=True)

                    parsed.append({
                        "name": fname, "df": df_f,
                        "row_rng": row_rng, "col_rng": col_rng,
                        "dist_val": dist_val, "has_dist": _has_dist,
                        "error": None,
                    })
                else:
                    st.error(f"❌ {parse_err}")
                    parsed.append({"name": fname, "df": None, "row_rng": "", "col_rng": "",
                                   "dist_val": "", "has_dist": False, "error": parse_err})

        ready = [p for p in parsed if p["df"] is not None]
        st.divider()
        if st.button("🔎 Cek Semua File", disabled=not ready,
                     use_container_width=True, key="folder_concat_btn"):
            _frames = []
            for p in ready:
                _df = _apply_range_r(p["df"].copy(), p["row_rng"], p["col_rng"])
                if p["dist_val"] not in ("", "(Pilih)"):
                    if p["has_dist"]:
                        _df["DISTRIBUTOR"] = p["dist_val"]
                    else:
                        _df.insert(0, "DISTRIBUTOR", p["dist_val"])
                _frames.append(_df)

            try:
                _tpl_bytes = _fetch_template_bytes(TEMPLATE_DRIVE_URL)
            except Exception:
                _tpl_bytes = None

            # ── Build ref_cols dari kolom-kolom semua file (union, urutan terjaga) ──
            _seen = set()
            _ref_cols = []
            for f in _frames:
                for c in f.columns:
                    c_str = str(c).strip()
                    if c_str and c_str not in _seen:
                        _seen.add(c_str)
                        _ref_cols.append(c_str)

            # Reindex semua frame ke ref_cols yang sama, lalu concat
            _frames = [f.reindex(columns=_ref_cols) for f in _frames]
            combined_df = pd.concat(_frames, ignore_index=True)

            # Drop duplicate columns (safety net)
            combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]
            st.session_state["folder_result"] = {"df": combined_df, "tpl_bytes": _tpl_bytes}
            st.session_state.pop("sim_result", None)
            st.rerun()

        _res = st.session_state.get("folder_result")
        if _res is not None:
            combined_df = _res["df"]
            _dist_label = st.session_state.get("po_distributor") or ""
            if not _dist_label or _dist_label == "(Pilih Distributor)":
                _dist_label = "Combined"

            st.success(f"✅ **{len(combined_df):,}** baris · {combined_df.shape[1]} kolom dari {len(ready)} file")
            for col in ['QTY', 'DPP', 'TOTAL PRICE']:
                if col in combined_df.columns:
                    combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
            st.subheader("📊 Hasil Gabungan")
            st.dataframe(combined_df, use_container_width=True, hide_index=True)

            _ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            _fname_final = f"PO_{_dist_label}_{_ts}.xlsx"

    # ─────────────────────────────────────────────────────────────────────────
    # PO SIMULATOR — BigQuery integration
    # ─────────────────────────────────────────────────────────────────────────

    _MANUAL_REJECT_APPROVAL = ["G2G-252", "G2G-253"]
    _MANUAL_REJECT_NO_TOL = [
        "G2G-29705", "G2G-224", "G2G-247", "G2G-225", "G2G-226",
        "G2G-228", "G2G-74", "G2G-186", "G2G-202", "G2G-840",
        "G2G-844", "G2G-841", "G2G-800", "G2G-213", "G2G-217",
        "G2G-27305", "G2G-30701", "G2G-30702", "G2G-30703", "G2G-30704",
        "G2G-201", "G2G-31",
    ]
    _MANUAL_REJECT_ALL   = _MANUAL_REJECT_APPROVAL + _MANUAL_REJECT_NO_TOL
    _LIMITED_SKUS_QTY    = []
    _MAX_QTY_LIMIT       = 500
    _REJECTED_SKUS_1     = ["G2G-29700", "G2G-27300"]
    _REGION_LIST_1       = [
        "Central Sumatera", "Northern Sumatera", "Jakarta (Csa)",
        "West Kalimantan", "South Kalimantan", "East Kalimantan",
    ]
    _REJECTED_SKUS_2     = []
    _REGION_LIST_2       = []
    _WOI_STANDARD        = 12

    _folder_res = st.session_state.get("folder_result")
    if _folder_res is not None:
        _sim_df       = _folder_res["df"].copy()
        _sku_col_sim  = next((c for c in _sim_df.columns if c.upper() in ("SKU", "PRODUCT CODE")), None)
        _qty_col_sim  = next((c for c in _sim_df.columns if c.upper() in ("QTY", "QUANTITY")), None)
        _dist_col_sim = next((c for c in _sim_df.columns if "DISTRIBUTOR" in c.upper()), None)

        if _sku_col_sim and _qty_col_sim and _dist_col_sim:
            st.divider()
            if st.session_state.get("sim_result") is None:
                _sim_df[_qty_col_sim] = pd.to_numeric(_sim_df[_qty_col_sim], errors="coerce")
                _sim_df = _sim_df.dropna(subset=[_qty_col_sim])
                _sim_df = _sim_df[_sim_df[_qty_col_sim] > 0].copy()
                _sim_df[_dist_col_sim] = _sim_df[_dist_col_sim].astype(str).str.strip().str.upper()
                _sim_df[_sku_col_sim]  = _sim_df[_sku_col_sim].astype(str).str.strip().str.upper()
                _sim_df = _sim_df.rename(columns={
                    _dist_col_sim: "Distributor",
                    _sku_col_sim:  "Customer SKU Code",
                    _qty_col_sim:  "PO Qty",
                })
                _sim_df["is_po_sku"] = True
                _sim_df = _sim_df[["Distributor", "Customer SKU Code", "PO Qty", "is_po_sku"]]

                _all_npd     = []
                _excel_dfs   = {}
                _prog        = st.progress(0)
                _distributors = _sim_df["Distributor"].unique().tolist()

                for _di, _dist_name in enumerate(_distributors):
                    _prog.progress((_di + 1) / len(_distributors), f"Processing {_dist_name}...")
                    _cur_po   = _sim_df[_sim_df["Distributor"] == _dist_name].copy()
                    _sku_list = _cur_po["Customer SKU Code"].unique().tolist()

                    _sku_df   = get_sku_data(tuple(_sku_list))
                    _stock_df = get_stock_data(_dist_name, tuple(_sku_list))

                    if _sku_df.empty and _stock_df.empty:
                        st.warning(f"Tidak ada data untuk distributor: {_dist_name}")
                        continue

                    _sku_df = _sku_df.rename(columns={
                        "sku": "Customer SKU Code",
                        "price_for_distri": "SIP",
                        "product_name": "Product Name",
                    })
                    if "Customer SKU Code" in _sku_df.columns:
                        _sku_df["Customer SKU Code"] = _sku_df["Customer SKU Code"].astype(str).str.strip().str.upper()
                    if "sku" in _stock_df.columns:
                        _stock_df = _stock_df.rename(columns={"sku": "Customer SKU Code"})
                    if "Customer SKU Code" in _stock_df.columns:
                        _stock_df["Customer SKU Code"] = _stock_df["Customer SKU Code"].astype(str).str.strip().str.upper()
                    _stock_df = _stock_df.drop(columns=["distributor", "Distributor", "product_name"], errors="ignore")

                    _skus_in_sku   = set(_sku_df["Customer SKU Code"].tolist()) if not _sku_df.empty else set()
                    _skus_in_stock = set(_stock_df["Customer SKU Code"].tolist()) if not _stock_df.empty else set()
                    _skus_not_found = set(_sku_list) - (_skus_in_sku | _skus_in_stock)
                    if _skus_not_found:
                        st.warning(f"SKU tidak ditemukan ({_dist_name}): {', '.join(sorted(_skus_not_found))}")

                    _res_df = pd.merge(_cur_po, _sku_df, on="Customer SKU Code", how="left")
                    _sip_series = _res_df["SIP"] if "SIP" in _res_df.columns else pd.Series([0]*len(_res_df), index=_res_df.index)
                    _res_df["SIP"] = pd.to_numeric(_sip_series, errors="coerce").fillna(0)
                    _res_df["PO Value"] = _res_df["SIP"] * _res_df["PO Qty"]
                    _res_df = pd.merge(_res_df, _stock_df, on="Customer SKU Code", how="outer")
                    _res_df["Distributor"] = _dist_name

                    _miss_pn = _res_df["Product Name"].isna()
                    if _miss_pn.any():
                        _extra_skus = _res_df.loc[_miss_pn, "Customer SKU Code"].unique().tolist()
                        _extra_df   = get_sku_data(tuple(_extra_skus))
                        if not _extra_df.empty:
                            _extra_df = _extra_df.rename(columns={
                                "sku": "Customer SKU Code",
                                "product_name": "Product Name",
                                "price_for_distri": "SIP",
                            })
                            _extra_df["Customer SKU Code"] = _extra_df["Customer SKU Code"].astype(str).str.strip().str.upper()
                            _name_map = _extra_df.set_index("Customer SKU Code")["Product Name"].to_dict()
                            _res_df.loc[_miss_pn, "Product Name"] = _res_df.loc[_miss_pn, "Customer SKU Code"].map(_name_map)
                            if "SIP" in _extra_df.columns:
                                _sip_map  = _extra_df.set_index("Customer SKU Code")["SIP"].to_dict()
                                _miss_sip = _res_df["SIP"].isna() | (_res_df["SIP"] == 0)
                                _res_df.loc[_miss_sip, "SIP"] = _res_df.loc[_miss_sip, "Customer SKU Code"].map(_sip_map)

                    _all_sku_list = _res_df["Customer SKU Code"].unique().tolist()
                    _npd_df   = get_npd_data(tuple(_all_sku_list))
                    _cur_npd  = _npd_df["sku"].unique().tolist() if not _npd_df.empty else []
                    _all_npd  = list(set(_all_npd + _cur_npd))

                    _res_df["is_po_sku"] = _res_df["is_po_sku"].astype("boolean").fillna(False)
                    for _fc in ["PO Qty", "PO Value", "total_stock", "buffer_plan_by_lm_qty_adj",
                                "avg_weekly_st_lm_qty", "buffer_plan_by_lm_val_adj",
                                "remaining_allocation_qty_region", "woi_end_of_month_by_lm"]:
                        if _fc in _res_df.columns:
                            _res_df[_fc] = pd.to_numeric(_res_df[_fc], errors="coerce").fillna(0)

                    _res_df["SIP"] = pd.to_numeric(_res_df["SIP"], errors="coerce").fillna(0)
                    _zero_sugg_val = (
                        (_res_df["buffer_plan_by_lm_val_adj"] == 0) &
                        (_res_df["buffer_plan_by_lm_qty_adj"] > 0) &
                        (_res_df["SIP"] > 0)
                    )
                    _res_df.loc[_zero_sugg_val, "buffer_plan_by_lm_val_adj"] = (
                        _res_df.loc[_zero_sugg_val, "SIP"] *
                        _res_df.loc[_zero_sugg_val, "buffer_plan_by_lm_qty_adj"]
                    )

                    _bp = _res_df["buffer_plan_by_lm_qty_adj"] if "buffer_plan_by_lm_qty_adj" in _res_df.columns else pd.Series([0]*len(_res_df), index=_res_df.index)
                    _res_df = _res_df[(_res_df["PO Qty"] > 0) | (_bp > 0)].copy()

                    _sugg_mask = _res_df["is_po_sku"] == False
                    _sc_s = _res_df["supply_control_status_gt"] if "supply_control_status_gt" in _res_df.columns else pd.Series([""]*len(_res_df), index=_res_df.index)
                    _ra_s = _res_df["remaining_allocation_qty_region"] if "remaining_allocation_qty_region" in _res_df.columns else pd.Series([0]*len(_res_df), index=_res_df.index)
                    _bp_s = _res_df["buffer_plan_by_lm_qty_adj"] if "buffer_plan_by_lm_qty_adj" in _res_df.columns else pd.Series([0]*len(_res_df), index=_res_df.index)

                    _npd_sku_upper = [s.upper() for s in _cur_npd]
                    _excl = (
                        _res_df["Customer SKU Code"].isin(_skus_not_found) |
                        _res_df["Customer SKU Code"].isin(_MANUAL_REJECT_ALL) |
                        (_ra_s < 0) |
                        _sc_s.str.upper().isin(["STOP PO", "DISCONTINUEDD", "OOS", "UNAVAILABLE"]) |
                        (
                            _res_df["Customer SKU Code"].isin(_LIMITED_SKUS_QTY) &
                            (_bp_s > _MAX_QTY_LIMIT)
                        ) |
                        (_bp_s == 0)
                    )
                    if _REJECTED_SKUS_1:
                        _reg_up = [r.upper() for r in _REGION_LIST_1]
                        _reg_s  = _res_df["region"] if "region" in _res_df.columns else pd.Series([""]*len(_res_df), index=_res_df.index)
                        _excl   = _excl | (_res_df["Customer SKU Code"].isin(_REJECTED_SKUS_1) & ~_reg_s.str.upper().isin(_reg_up))
                    _res_df = _res_df[~(_sugg_mask & _excl)].copy()

                    _avg  = _res_df["avg_weekly_st_lm_qty"] if "avg_weekly_st_lm_qty" in _res_df.columns else pd.Series([0]*len(_res_df), index=_res_df.index)
                    _stk  = _res_df["total_stock"] if "total_stock" in _res_df.columns else pd.Series([0]*len(_res_df), index=_res_df.index)
                    _bp2  = _res_df["buffer_plan_by_lm_qty_adj"] if "buffer_plan_by_lm_qty_adj" in _res_df.columns else pd.Series([0]*len(_res_df), index=_res_df.index)

                    _res_df["WOI PO Original"] = calculate_woi(_stk, _res_df["PO Qty"], _avg)
                    _res_df["WOI Suggest"]     = calculate_woi(_stk, _bp2, _avg)
                    _res_df["Current WOI"]     = calculate_woi(_stk, 0, _avg)

                    _ra2  = _res_df["remaining_allocation_qty_region"] if "remaining_allocation_qty_region" in _res_df.columns else pd.Series([0]*len(_res_df), index=_res_df.index)
                    _sc2  = _res_df["supply_control_status_gt"] if "supply_control_status_gt" in _res_df.columns else pd.Series([""]*len(_res_df), index=_res_df.index)
                    _bp3  = _res_df["buffer_plan_by_lm_qty_adj"] if "buffer_plan_by_lm_qty_adj" in _res_df.columns else pd.Series([0]*len(_res_df), index=_res_df.index)
                    _avg2 = _res_df["avg_weekly_st_lm_qty"] if "avg_weekly_st_lm_qty" in _res_df.columns else pd.Series([0]*len(_res_df), index=_res_df.index)
                    _cur_woi = _res_df["Current WOI"]

                    _conds = [
                        _res_df["Customer SKU Code"].isin(_skus_not_found),
                        _res_df["Customer SKU Code"].isin(_LIMITED_SKUS_QTY) & (_res_df["PO Qty"] > _MAX_QTY_LIMIT),
                        _ra2 < 0,
                        _res_df["is_po_sku"] == False,
                        _res_df["Customer SKU Code"].isin(_MANUAL_REJECT_APPROVAL),
                        _res_df["Customer SKU Code"].isin(_MANUAL_REJECT_NO_TOL),
                        _sc2.str.upper().isin(["STOP PO", "DISCONTINUEDD", "OOS", "UNAVAILABLE"]),
                        (
                            (_avg2 == 0) &
                            (_bp3 == 0) &
                            ~_res_df["Customer SKU Code"].str.upper().isin(_npd_sku_upper) &
                            ~_sc2.str.upper().isin(["STOP PO", "DISCONTINUEDD", "OOS"])
                        ),
                        _bp3 == 0,
                        _res_df["PO Qty"] > _bp3,
                        _res_df["PO Qty"] < _bp3,
                        _res_df["PO Qty"] == _bp3,
                    ]
                    _choices = [
                        "Reject (SKU Not Found in System)",
                        f"Reject (Exceeds Qty Limit of {_MAX_QTY_LIMIT})",
                        "Reject (Negative Allocation)",
                        "Additional Suggestion",
                        "Reject (Stop by Steve - Need approval email)",
                        "Reject (Stop by Steve - No tolerance to open)",
                        "Reject",
                        "Proceed",
                        "Reject",
                        "Reject with suggestion",
                        "Proceed with suggestion",
                        "Proceed",
                    ]

                    _res_df["Remark"] = np.select(_conds, _choices, default="N/A (Missing Data)")
                    _res_df = _res_df.rename(columns={
                        "distributor_name": "Distributor",
                        "Customer SKU Code": "SKU",
                        "assortment": "Assortment",
                        "supply_control_status_gt": "Supply Control",
                        "total_stock": "Total Stock (Qty)",
                        "avg_weekly_st_lm_qty": "Avg Weekly Sales LM (Qty)",
                        "buffer_plan_by_lm_qty_adj": "Suggested PO Qty",
                        "buffer_plan_by_lm_val_adj": "Suggested PO Value",
                        "WOI PO Original": "WOI (Stock + PO Ori)",
                        "WOI Suggest": "WOI After Buffer (Stock + Suggested Qty)",
                        "woi_end_of_month_by_lm": "Stock + Suggested Qty WOI (Projection at EOM)",
                        "remaining_allocation_qty_region": "Remaining Allocation (By Region)",
                    })
                    if _REJECTED_SKUS_1:
                        _res_df = apply_sku_rejection_rules(_REJECTED_SKUS_1, _res_df, _REGION_LIST_1, is_in=False)
                    if _REJECTED_SKUS_2:
                        _res_df = apply_sku_rejection_rules(_REJECTED_SKUS_2, _res_df, _REGION_LIST_2, is_in=False)

                    _res_df["RSA Notes"] = ""
                    _out_cols = [
                        "Distributor", "SKU", "Product Name", "Assortment", "Supply Control",
                        "Avg Weekly Sales LM (Qty)", "Total Stock (Qty)", "Current WOI",
                        "PO Qty", "PO Value", "WOI (Stock + PO Ori)", "Remark",
                        "Suggested PO Qty", "Suggested PO Value",
                        "WOI After Buffer (Stock + Suggested Qty)",
                        "Stock + Suggested Qty WOI (Projection at EOM)",
                        "Remaining Allocation (By Region)", "is_po_sku", "RSA Notes",
                    ]
                    _res_df = _res_df.reindex(columns=_out_cols)
                    _res_df.sort_values(by=["is_po_sku", "SKU"], ascending=[False, True], inplace=True)

                    _zero_sugg = _res_df[
                        (_res_df["Suggested PO Qty"] == 0) & (_res_df["is_po_sku"] == True)
                    ][["SKU", "Product Name", "PO Qty", "Suggested PO Qty", "Remark"]]
                    if not _zero_sugg.empty:
                        st.warning(
                            f"⚠️ **{_dist_name}** — {len(_zero_sugg)} SKU memiliki **Suggested PO Qty = 0** "
                            f"(data `buffer_plan_by_lm_qty_adj` tidak ada / 0 di BigQuery):"
                        )
                        st.dataframe(_zero_sugg, use_container_width=True, hide_index=True)

                    _excel_dfs[_dist_name] = _res_df.copy()

                _prog.progress(1.0, "Selesai")
                st.session_state["sim_result"] = {"dfs": _excel_dfs, "npd": _all_npd}
                st.rerun()

            # ── Display simulation results ────────────────────────────────
            _sim_out = st.session_state.get("sim_result")
            if _sim_out is not None:
                _e_dfs = _sim_out["dfs"]
                _e_npd = _sim_out["npd"]

                if _e_dfs:
                    st.success(f"Simulasi selesai — {len(_e_dfs)} distributor")
                    _final_disp = pd.concat(_e_dfs.values(), ignore_index=True)

                    _combined_raw = _folder_res["df"].copy()
                    _combined_raw[_sku_col_sim]  = _combined_raw[_sku_col_sim].astype(str).str.strip().str.upper()
                    _combined_raw[_dist_col_sim] = _combined_raw[_dist_col_sim].astype(str).str.strip().str.upper()
                    _combined_raw = _combined_raw.rename(columns={_sku_col_sim: "SKU", _dist_col_sim: "Distributor"})
                    _sim_col_names_upper = {c.upper() for c in _final_disp.columns}
                    _exclude     = {"SKU", "DISTRIBUTOR", _qty_col_sim.upper()}
                    _extra_cols  = [
                        c for c in _combined_raw.columns
                        if c.upper() not in _exclude and c.upper() not in _sim_col_names_upper
                    ]
                    if _extra_cols:
                        _combined_agg = (
                            _combined_raw.groupby(["SKU", "Distributor"], as_index=False)[_extra_cols].first()
                        )
                        _final_disp = _final_disp.merge(_combined_agg, on=["SKU", "Distributor"], how="left")

                    woi_col = next((c for c in _final_disp.columns if "woi" in c.lower() and "stock" in c.lower()), None)
                    if woi_col is None:
                        woi_col = next((c for c in _final_disp.columns if "woi" in c.lower()), "Current WOI")

                    st.markdown("""
                    <div class="pipeline-step active">
                        <span class="step-number">2</span>
                        <strong>Preview Data — Top 10 WOI</strong>
                    </div>
                    """, unsafe_allow_html=True)

                    _prev_df = _final_disp[_final_disp["Remark"].str.contains("Reject", na=False)].copy()
                    _prev_df[woi_col] = pd.to_numeric(_prev_df[woi_col], errors="coerce")
                    _top10 = _prev_df.nlargest(10, woi_col)[PO_IMG_COLS].reset_index(drop=True)
                    _styled = _top10.style.set_properties(**{
                        "background-color": "#D6EAF8",
                        "color": "#1a1a2e",
                        "border": "1px solid #AED6F1",
                    }).format(na_rep="-")
                    st.dataframe(_styled, use_container_width=True, hide_index=True)
                    _final_disp = pd.concat(_e_dfs.values(), ignore_index=True)

                    # Formatted copy for display only
                    _final_disp_fmt = _final_disp.copy()
                    _final_disp_fmt["PO Value"] = _final_disp_fmt["PO Value"].apply(
                        lambda x: f"{x:,.2f}" if pd.notnull(x) else 0)
                    _final_disp_fmt["Suggested PO Value"] = _final_disp_fmt["Suggested PO Value"].apply(
                        lambda x: f"{x:,.2f}" if pd.notnull(x) else 0)
                    _final_disp_fmt["Remaining Allocation (By Region)"] = _final_disp_fmt["Remaining Allocation (By Region)"].apply(
                        lambda x: f"{round(x):,d}" if pd.notnull(x) else 0)
                    _final_disp_fmt["Avg Weekly Sales LM (Qty)"] = _final_disp_fmt["Avg Weekly Sales LM (Qty)"].apply(
                        lambda x: f"{round(x):,d}" if pd.notnull(x) else 0)
                    _final_disp_fmt["WOI (Stock + PO Ori)"] = _final_disp_fmt["WOI (Stock + PO Ori)"].apply(
                        lambda x: f"{x:.2f}" if pd.notnull(x) else "")
                    _final_disp_fmt["Stock + Suggested Qty WOI (Projection at EOM)"] = _final_disp_fmt["Stock + Suggested Qty WOI (Projection at EOM)"].apply(
                        lambda x: f"{x:.2f}" if pd.notnull(x) else "")
                    _final_disp_fmt["Current WOI"] = _final_disp_fmt["Current WOI"].apply(
                        lambda x: f"{x:.2f}" if pd.notnull(x) else "")
                    _final_disp_fmt["WOI After Buffer (Stock + Suggested Qty)"] = _final_disp_fmt["WOI After Buffer (Stock + Suggested Qty)"].apply(
                        lambda x: f"{x:.2f}" if pd.notnull(x) else "")

                    st.caption(f"Menampilkan 10 baris dengan nilai **{woi_col}** terbesar · kolom Distributor s/d Remark")

                    alloc_col = next((c for c in _final_disp.columns if "allocation" in c.lower()), None)
                    if alloc_col:
                        st.markdown("""
                        <div class="pipeline-step active">
                            <span class="step-number">3</span>
                            <strong>Remaining Allocation (per Distributor)</strong>
                        </div>
                        """, unsafe_allow_html=True)
                        _alloc_df = _final_disp.copy()
                        _alloc_df[alloc_col] = pd.to_numeric(_alloc_df[alloc_col], errors="coerce")
                        _alloc_df = _alloc_df[_alloc_df[alloc_col].notna() & (_alloc_df[alloc_col] != 0)]
                        _show_cols = list(dict.fromkeys([c for c in PO_IMG_COLS + [alloc_col] if c in _alloc_df.columns]))
                        if _alloc_df.empty:
                            st.info("Tidak ada baris dengan allocation tersedia.")
                        else:
                            for _dist_alloc, _grp in _alloc_df.groupby("Distributor"):
                                with st.expander(f"📦 {_dist_alloc} — {len(_grp)} baris", expanded=True):
                                    st.dataframe(_grp[_show_cols].reset_index(drop=True), use_container_width=True, hide_index=True)

                    _final_step = 3 + (1 if alloc_col else 0)

                    # ── Brand selector for Sheet 2 ────────────────────────
                    #st.markdown(f"""
                    #<div class="pipeline-step active">
                    #    <span class="step-number">{_final_step}</span>
                    #    <strong>SKU Master List — Sheet 2 (Filter by Brand)</strong>
                    #</div>
                    #""", unsafe_allow_html=True)
#
                    #_brand_options_raw = get_brand_list()
                    #_selected_brand = st.selectbox(
                    #    "Filter SKU Master by Brand (akan ditambahkan sebagai Sheet 2 di Excel):",
                    #    options=["(All Brands)"] + _brand_options_raw,
                    #    key="sku_master_brand_sel",
                    #)
                    #_brand_filter = None if _selected_brand == "(All Brands)" else _selected_brand
                    #_sku_master_df = get_sku_master_data(_brand_filter)
#
                    #if not _sku_master_df.empty:
                    #    st.caption(
                    #        f"📋 **{len(_sku_master_df):,} SKU** ditemukan"
                    #        f"{'  ·  Brand: **' + _selected_brand + '**' if _brand_filter else ' (semua brand)'}"
                    #        f" — akan ditulis ke Sheet 2"
                    #    )
                    #    with st.expander("👁 Preview SKU Master List", expanded=False):
                    #        st.dataframe(_sku_master_df.head(50), use_container_width=True, hide_index=True)
                    #else:
                    #    st.info("Tidak ada data SKU master yang ditemukan untuk brand ini.")

                    # ── Download button ───────────────────────────────────
                    _dl_data = to_excel_single_sheet_with_sku(
                        _final_disp,
                        _e_npd,
                        #_sku_master_df if not _sku_master_df.empty else None,
                    )
                    st.download_button(
                        label=f"Download PO Result.xlsx ({len(_final_disp)} baris · 2 sheets)",
                        data=_dl_data,
                        file_name=f"PO Result {datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )

                    # ── Image categorisation ──────────────────────────────
                    st.markdown(f"""
                    <div class="pipeline-step active">
                        <span class="step-number">{_final_step + 1}</span>
                        <strong>Auto Kategorisasi &amp; Download Gambar</strong>
                    </div>
                    """, unsafe_allow_html=True)

                    if not MATPLOTLIB_OK:
                        st.error("❌ Library matplotlib tidak tersedia. Tambahkan ke requirements.txt.")
                        st.stop()

                    _img_df      = _final_disp[PO_COLS_copy].copy()
                    _remark_col  = "Remark"
                    _stop_keywords = ["STOP PO", "OOS", "DISCONTINUED", "UNAVAILABLE"]
                    _sc_col = next((c for c in _img_df.columns if "supply" in c.lower() and "control" in c.lower()), None)
                    _stop_mask = _img_df[_sc_col].str.strip().str.upper().isin([k.upper() for k in _stop_keywords])
                    _stop_df = _img_df[_stop_mask].reset_index(drop=True)

                    _cat1_col, _cat2_col, _cat3_col = st.columns(3)

                    with _cat1_col:
                        st.markdown("""
                        <div class="metric-card" style="text-align:center;border-left:4px solid #CA6180;margin-bottom:0.6rem;">
                            <div style="font-size:1.6rem;">🚫</div>
                            <div style="font-weight:700;color:#CA6180;">Product Stop PO</div>
                            <div style="color:#A8849A;font-size:0.78rem;">Supply Control: STOP PO / OOS / DISCONTINUED</div>
                        </div>""", unsafe_allow_html=True)
                        if _stop_df.empty:
                            st.info("Tidak ada data kategori ini.")
                        else:
                            st.caption(f"{len(_stop_df)} baris ditemukan")
                            try:
                                _img_bytes = df_to_image_bytes(_stop_df, title="Product Stop PO")
                                st.download_button(
                                    label='Download product stop po (.png)',
                                    data=_img_bytes,
                                    file_name=f"product_stop_po_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png",
                                    mime="image/png",
                                    use_container_width=True,
                                )
                            except Exception as _e:
                                st.error(f"Gagal generate gambar: {_e}")

                    _non_stop_df = _img_df[~_stop_mask].copy()
                    _steve_mask  = (
                        _non_stop_df[_remark_col].str.lower().str.contains("reject (stop by steve", na=False, regex=False)
                        | _non_stop_df[_remark_col].str.lower().str.contains("reject (negative allocation)", na=False, regex=False)
                    )
                    _steve_df = _non_stop_df[_steve_mask].reset_index(drop=True)

                    with _cat2_col:
                        st.markdown("""
                        <div class="metric-card" style="text-align:center;border-left:4px solid #FCB7C7;margin-bottom:0.6rem;">
                            <div style="font-size:1.6rem;">❌</div>
                            <div style="font-weight:700;color:#CA6180;">Reject by Steve</div>
                            <div style="color:#A8849A;font-size:0.78rem;">Remark: reject by steve</div>
                        </div>""", unsafe_allow_html=True)
                        if _steve_df.empty:
                            st.info("Tidak ada data kategori ini.")
                        else:
                            st.caption(f"{len(_steve_df)} baris ditemukan")
                            try:
                                _img_bytes = df_to_image_bytes(_steve_df, title="Reject by Steve")
                                st.download_button(
                                    label='Download reject by steve (.png)',
                                    data=_img_bytes,
                                    file_name=f"reject_by_steve_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png",
                                    mime="image/png",
                                    use_container_width=True,
                                )
                            except Exception as _e:
                                st.error(f"Gagal generate gambar: {_e}")

                    _approval_mask = (
                        _non_stop_df[_remark_col].str.strip().str.lower().isin(["reject", "reject with suggestion"])
                        & ~_steve_mask
                    )
                    _approval_df = _non_stop_df[_approval_mask].reset_index(drop=True)

                    with _cat3_col:
                        st.markdown("""
                        <div class="metric-card" style="text-align:center;border-left:4px solid #A84D6A;margin-bottom:0.6rem;">
                            <div style="font-size:1.6rem;">⚠️</div>
                            <div style="font-weight:700;color:#CA6180;">Products Need Approval</div>
                            <div style="color:#A8849A;font-size:0.78rem;">Remark: reject / reject by suggestion</div>
                        </div>""", unsafe_allow_html=True)
                        if _approval_df.empty:
                            st.info("Tidak ada data kategori ini.")
                        else:
                            st.caption(f"{len(_approval_df)} baris ditemukan")
                            try:
                                _img_bytes = df_to_image_bytes(_approval_df, title="Products Need Approval")
                                st.download_button(
                                    label='Download products need approval (.png)',
                                    data=_img_bytes,
                                    file_name=f"products_need_approval_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png",
                                    mime="image/png",
                                    use_container_width=True,
                                )
                            except Exception as _e:
                                st.error(f"Gagal generate gambar: {_e}")

                    st.markdown(f"""
                    <div class="pipeline-step active">
                        <span class="step-number">{_final_step + 2}</span>
                        <strong>Product Code yang Harus Dihapus</strong>
                    </div>
                    """, unsafe_allow_html=True)

                    _sku_series = [_sdf["SKU"].dropna().astype(str).str.strip() for _sdf in (_stop_df, _steve_df) if "SKU" in _sdf.columns]
                    _remark_series = []
                    if "Supply Control" in _stop_df.columns:
                        _remark_series.append(_stop_df["Supply Control"].dropna().astype(str).str.strip())
                    if "Remark" in _steve_df.columns:
                        _remark_series.append(_steve_df["Remark"].dropna().astype(str).str.strip())

                    _sku_all = (
                        pd.concat(_sku_series).pipe(lambda s: s[s != ""]).drop_duplicates().sort_values().reset_index(drop=True)
                        if _sku_series else pd.Series(dtype=str)
                    )
                    _remark_all = (
                        pd.concat(_remark_series, ignore_index=True).astype(str).str.strip()
                        .loc[lambda s: s.ne("")].drop_duplicates().sort_values().reset_index(drop=True)
                        if _remark_series else pd.Series(dtype="string")
                    )
                    if _sku_all.empty:
                        st.info("Tidak ada product code yang perlu dihapus.")
                    else:
                        _pc1, _pc2 = st.columns([1, 1])
                        with _pc1:
                            st.dataframe(pd.DataFrame({"Remark/Supply Control": _remark_all}), use_container_width=True, hide_index=True)
                        with _pc2:
                            with st.expander("📋 Copy SKU"):
                                st.code("\n".join(_sku_all.tolist()), language=None)
                            # ─────────────────────────────────────────────────────
                            # POINT 7 — Summary PO per Distributor
                            # ─────────────────────────────────────────────────────
                    st.markdown(f"""
                    <div class="pipeline-step active">
                        <span class="step-number">{_final_step + 3}</span>
                        <strong>Summary PO</strong>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    _summary_df = _final_disp.copy()
                    _summary_df["PO Value"] = pd.to_numeric(_summary_df["PO Value"], errors="coerce").fillna(0)
                    _summary_df["Supply Control"] = _summary_df["Supply Control"].fillna("").astype(str)
                    _summary_df["Remark"] = _summary_df["Remark"].fillna("").astype(str)
                    
                    _po_date_str = datetime.now().strftime("%d %B %Y")
                    _stop_keywords_sum = ["STOP PO", "OOS", "DISCONTINUED", "UNAVAILABLE"]
                    
                    def _rp(v):
                        try:
                            return f"Rp {v:,.0f}".replace(",", ".")
                        except Exception:
                            return "Rp 0"
                    
                    for _dist_sum in sorted(_summary_df["Distributor"].dropna().unique()):
                        _grp = _summary_df[_summary_df["Distributor"] == _dist_sum]
                        _grp_po = _grp[_grp["is_po_sku"] == True] if "is_po_sku" in _grp.columns else _grp
                    
                        # Total SKU
                        _total_sku = _grp_po["SKU"].nunique()
                    
                        # Discontinue / Stop PO
                        _stop_mask_sum = _grp_po["Supply Control"].str.strip().str.upper().isin(
                            [k.upper() for k in _stop_keywords_sum]
                        )
                        _stop_grp   = _grp_po[_stop_mask_sum]
                        _stop_count = _stop_grp["SKU"].nunique()
                        _stop_value = _stop_grp["PO Value"].sum()
                    
                        # Reject by Steve
                        _steve_mask_sum = (
                            _grp_po["Remark"].str.lower().str.contains("reject (stop by steve", na=False, regex=False)
                            | _grp_po["Remark"].str.lower().str.contains("reject (negative allocation)", na=False, regex=False)
                        )
                        _steve_grp   = _grp_po[_steve_mask_sum]
                        _steve_count = _steve_grp["SKU"].nunique()
                        _steve_value = _steve_grp["PO Value"].sum()
                    
                        # Need approval
                        _approval_mask_sum = (
                            _grp_po["Remark"].str.strip().str.lower().isin(["reject", "reject with suggestion"])
                            & ~_steve_mask_sum
                            & ~_stop_mask_sum
                        )
                        _approval_grp   = _grp_po[_approval_mask_sum]
                        _approval_count = _approval_grp["SKU"].nunique()
                        _approval_value = _approval_grp["PO Value"].sum()
                    
                        # Total reduction
                        _total_reduction = _stop_value + _steve_value + _approval_value
                        _grand_total_po  = _grp_po["PO Value"].sum()
                        # ── Label dinamis dari kategori yang muncul di data ──
                        _stop_categories = (
                            _stop_grp["Supply Control"]
                            .dropna()
                            .astype(str)
                            .str.strip()
                            .str.title()
                            .replace({"Stop Po": "Stop PO", "Oos": "OOS"})
                            .unique()
                            .tolist()
                        )
                        _stop_categories = [c for c in _stop_categories if c]
                        _stop_label = ", ".join(_stop_categories) if _stop_categories else "Discontinued / Stop PO"

                        _steve_categories = []
                        if _steve_count > 0:
                            if _grp_po["Remark"].str.lower().str.contains("reject (stop by steve", na=False, regex=False).any():
                                _steve_categories.append("Stop by Steve")
                            if _grp_po["Remark"].str.lower().str.contains("reject (negative allocation)", na=False, regex=False).any():
                                _steve_categories.append("Negative Allocation")
                        _steve_label = ", ".join(_steve_categories) if _steve_categories else "Reject by Steve"

                        _grand_total_after = _grand_total_po - _total_reduction
                    
                        # ── Visual card (HTML, biar enak dilihat) ──
                        _summary_html = f"""
                        <div style="background:#FFF5F8;border:1px solid #F0C8D6;border-radius:12px;
                                    padding:1rem 1.2rem;margin:0.6rem 0 0.4rem;">
                            <div style="font-size:0.95rem;font-weight:700;color:#8B2040;margin-bottom:0.5rem;">
                                Summary PO dari Distributor <strong>{_dist_sum}</strong> — PO Date: <strong>{_po_date_str}</strong>
                            </div>
                            <ul style="margin:0;padding-left:1.2rem;color:#1F1F1F;font-size:0.88rem;line-height:1.7;">
                                <li>Total SKU: <strong>{_total_sku:,}</strong></li>
                                <li>Grand Total PO (sebelum pengurangan): <strong>{_rp(_grand_total_po)}</strong></li>
                                <li>{_stop_label}: <strong>{_stop_count:,}</strong> SKU dengan value <strong>{_rp(_stop_value)}</strong></li>
                                <li>{_steve_label}: <strong>{_steve_count:,}</strong> SKU dengan value <strong>{_rp(_steve_value)}</strong></li>
                                <li>Total pengurangan: <strong>{_rp(_total_reduction)}</strong></li>
                                <li>Grand Total setelah pengurangan: <strong>{_rp(_grand_total_after)}</strong></li>
                            </ul>
                        </div>
                        """
                        st.markdown(_summary_html, unsafe_allow_html=True)
                    
                        # ── Copy-friendly version (plain text dengan tombol copy bawaan st.code) ──
                        # Ambil kategori Supply Control yang benar-benar muncul di _stop_grp
                        _stop_categories = (
                            _stop_grp["Supply Control"]
                            .dropna()
                            .astype(str)
                            .str.strip()
                            .str.title()           # "STOP PO" → "Stop Po"; lihat fix di bawah
                            .replace({"Stop Po": "Stop PO", "Oos": "OOS"})  # rapikan kapitalisasi
                            .unique()
                            .tolist()
                        )
                        _stop_categories = [c for c in _stop_categories if c]  # buang string kosong
                        _stop_label = ", ".join(_stop_categories) if _stop_categories else "Discontinued / Stop PO"
                        # Sama untuk Steve (kategori dari Remark)
                        _steve_categories = []
                        if _steve_count > 0:
                            if _grp_po["Remark"].str.lower().str.contains("reject (stop by steve", na=False, regex=False).any():
                                _steve_categories.append("Stop by Steve")
                            if _grp_po["Remark"].str.lower().str.contains("reject (negative allocation)", na=False, regex=False).any():
                                _steve_categories.append("Negative Allocation")
                        _steve_label = ", ".join(_steve_categories) if _steve_categories else "Reject by Steve"
                        _summary_text = (
                        f"Summary PO dari Distributor {_dist_sum} - PO Date: {_po_date_str}\n"
                        f"\n"
                        f"• Total SKU: {_total_sku:,}\n"
                        f"• Current Grand Total PO: {_rp(_grand_total_po)}\n"
                        f"\n"
                        f"• {_stop_label}: {_stop_count:,} SKU dengan value {_rp(_stop_value)}\n"
                        f"• {_steve_label}: {_steve_count:,} SKU dengan value {_rp(_steve_value)}\n"
                        #f"• Need Approval (Reject / Reject with Suggestion): {_approval_count:,} SKU "
                        #f"dengan value {_rp(_approval_value)}\n"
                        f"\n"
                        f"• Total pengurangan: {_rp(_total_reduction)}\n"
                        f"• Grand Total Setelah Pengurangan: {_rp(_grand_total_after)}"
                    )
                    
                        #with st.expander(f"📋 Copy Summary — {_dist_sum}", expanded=False):
                        #        st.code(_summary_text, language=None)
    
# ─────────────────────────────────────────────────────────────────────────
    # Hapus SKU dari File PO (pakai file dari Upload File PO di atas)
    # ─────────────────────────────────────────────────────────────────────────
    #st.markdown("""
    #<div class="hero-wrap">
    #    <div class="hero-tag">✦ Edit PO</div>
    #   <div class="hero-title">EDIT QTY dari File PO</div>
    #</div>
    #""", unsafe_allow_html=True)

    if not folder_files:
        st.info("ℹ️ Upload file PO di section atas terlebih dahulu untuk modifikasi.")
        st.stop()

    st.markdown("""
     <div class="pipeline-step active">
         <span class="step-number">1</span>
         <strong>Pilih File untuk Modifikasi</strong>
     </div>
     """, unsafe_allow_html=True)

   # Loop pakai raw_entries dari section Upload File PO di atas
    for _fi, (tpl_fname, tpl_orig_bytes) in enumerate(raw_entries):
        with st.container(border=True):
            st.markdown(f"**#{_fi+1} &nbsp; {tpl_fname}**")

            tpl_orig_ext = tpl_fname.rsplit(".", 1)[-1].lower()
            _tpl_name, tpl_bytes = _convert_to_xlsx(tpl_fname, tpl_orig_bytes)
            if tpl_orig_ext != "xlsx":
                st.caption(f"🔄 Auto-convert: {tpl_fname} → {_tpl_name}")
            _tpl_engine = "openpyxl"
            tpl_sheets = _get_sheet_names(tpl_bytes, _tpl_engine)
            if not tpl_sheets:
                st.warning("⚠️ Tidak ada sheet visible.")
                continue
            sc1, sc2 = st.columns([2, 1])
            with sc1:
                if len(tpl_sheets) > 1:
                    tpl_selected_sheet = st.selectbox(
                        f"Sheet ({len(tpl_sheets)} visible):",
                        options=tpl_sheets,
                        key=f"tpl_sheet_sel_{_fi}",
                    )
                else:
                    tpl_selected_sheet = tpl_sheets[0]
                    st.caption(f"📄 Sheet: **{tpl_selected_sheet}**")
            with sc2:
                _auto_hrow  = detect_header_row(tpl_bytes, _tpl_name, sheet_name=tpl_selected_sheet)
                _hrow_input = st.number_input(
                    "Header row (baris ke-)", min_value=1,
                    value=int(_auto_hrow) + 1, step=1,
                    key=f"tpl_hrow_{_fi}",
                )
            _tpl_hrow = int(_hrow_input) - 1

            try:
                tpl_df = pd.read_excel(io.BytesIO(tpl_bytes), sheet_name=tpl_selected_sheet,
                                       header=_tpl_hrow, engine=_tpl_engine, dtype=str)
                tpl_df = tpl_df.loc[:, ~tpl_df.columns.str.startswith('Unnamed')]
                tpl_df = tpl_df.dropna(how='all').reset_index(drop=True)
            except Exception as e:
                st.error(f"❌ Gagal membaca file: {e}")
                continue

            st.caption(f"**{len(tpl_df):,} baris · {len(tpl_df.columns)} kolom**")
            with st.expander("👁 Preview data", expanded=False):
                st.dataframe(tpl_df, use_container_width=True, hide_index=True)

            qty_col_t = next((c for c in tpl_df.columns
                              if any(k in c.lower() for k in ['qty', 'quantity'])), None)
            sku_col_t = next((c for c in tpl_df.columns
                              if any(k in c.lower() for k in ['sku', 'product code', 'kode', 'code'])), None)

            if qty_col_t and sku_col_t:
                st.markdown("""
                <div class="pipeline-step active">
                    <span class="step-number">2</span>
                    <strong>Modifikasi Quantity per Product Code</strong>
                </div>
                """, unsafe_allow_html=True)

                def _save_tpl_file(tpl_selected_sheet, _tpl_hrow, cell_writer):
                    _com_res = _edit_qty_via_excel_com(
                        tpl_bytes, tpl_selected_sheet, _tpl_hrow,
                        sku_col_t, qty_col_t, cell_writer,
                    )
                    if _com_res is not None:
                        return _com_res
                    out_buf = io.BytesIO()
                    changed = 0
                    _wb = openpyxl.load_workbook(io.BytesIO(tpl_bytes), data_only=False)
                    _ws = next(
                        (s for s in _wb.worksheets if s.title == tpl_selected_sheet),
                        _wb.active,
                    )
                    _hdr_row = _tpl_hrow + 1
                    _hdrs = {
                        _ws.cell(row=_hdr_row, column=c).value: c
                        for c in range(1, _ws.max_column + 1)
                    }
                    _sku_ci = _hdrs.get(sku_col_t)
                    _qty_ci = _hdrs.get(qty_col_t)
                    if _sku_ci and _qty_ci:
                        for _row in _ws.iter_rows(min_row=_hdr_row + 1, max_row=_ws.max_row):
                            _sv   = str(_row[_sku_ci - 1].value or "").strip()
                            _qcell = _row[_qty_ci - 1]
                            _qv   = _qcell.value
                            _new  = cell_writer(_sv, _qv)
                            if _new is not None:
                                _qcell.value = _new
                                changed += 1
                    _wb.save(out_buf)
                    return out_buf.getvalue(), changed

                _out_ext  = "xlsx"
                _out_mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

                with st.container(border=True):
                    st.caption(f"SKU: **{sku_col_t}** · Quantity: **{qty_col_t}**")
                    reduce_codes = st.text_area(
                        "Daftar Product Code (satu per baris)",
                        placeholder="SKU001\nSKU-ABC\nPROD123",
                        height=150, key=f"tpl_reduce_codes_{_fi}",
                    )
                    load_skus_btn = st.button(
                        "📋 Tampilkan SKU",
                        use_container_width=True, key=f"tpl_load_skus_{_fi}"
                    )

                    if load_skus_btn and reduce_codes.strip():
                        _parsed = [c.strip() for c in reduce_codes.strip().splitlines() if c.strip()]
                        st.session_state[f"reduce_skus_{_fi}"] = _parsed
                    elif load_skus_btn:
                        st.warning("⚠️ Tidak ada SKU yang valid")

                _skus_r = st.session_state.get(f"reduce_skus_{_fi}", [])
                if _skus_r:
                    st.markdown("**Atur quantity baru per Product Code:**")
                    _sku_qty_map = (
                        tpl_df[[sku_col_t, qty_col_t]]
                        .dropna(subset=[sku_col_t])
                        .assign(**{sku_col_t: lambda d: d[sku_col_t].astype(str).str.strip()})
                        .set_index(sku_col_t)[qty_col_t]
                        .to_dict()
                    )
                    for _sku_r in _skus_r:
                        _cur_q = _sku_qty_map.get(_sku_r, None)
                        try:
                            _cur_q_int = int(float(_cur_q)) if _cur_q not in (None, "") else 0
                        except (ValueError, TypeError):
                            _cur_q_int = 0
                        _def_key = f"edit_val_{_fi}_{_sku_r}"
                        if _def_key not in st.session_state:
                            st.session_state[_def_key] = _cur_q_int
                        with st.container(border=True):
                            _rc1, _rc2, _rc3 = st.columns([3, 2, 3])
                            with _rc1:
                                st.markdown(f"**{_sku_r}**")
                            with _rc2:
                                st.caption(f"QTY saat ini")
                                st.markdown(f"**{_cur_q if _cur_q is not None else '-'}**")
                            with _rc3:
                                st.number_input(
                                    "Quantity baru",
                                    min_value=0,
                                    step=1,
                                    key=_def_key,
                                )

                    reduce_apply = st.button(
                        "Change QTY",
                        use_container_width=True,
                        key=f"tpl_reduce_apply_{_fi}",
                    )
                    if reduce_apply:
                        _edit_map = {
                            _sku_r: st.session_state.get(f"edit_val_{_fi}_{_sku_r}", 0)
                            for _sku_r in _skus_r
                        }
                        mask_t = tpl_df[sku_col_t].astype(str).str.strip().isin(set(_skus_r))

                        def _edit_writer(sku_val, qty_val):
                            if sku_val not in _edit_map:
                                return None
                            _new = float(_edit_map[sku_val])
                            return int(_new) if _new == int(_new) else _new

                        _buf, _cnt = _save_tpl_file(tpl_selected_sheet, _tpl_hrow, _edit_writer)
                        st.session_state[f"tpl_out_{_fi}"] = {
                            "buf": _buf, "mask": mask_t,
                            "sku": sku_col_t, "qty": qty_col_t,
                            "cleared": _cnt, "df": tpl_df,
                            "ext": _out_ext, "mime": _out_mime,
                            "edit_map": _edit_map,
                        }

                _res_t = st.session_state.get(f"tpl_out_{_fi}")
                if _res_t:
                    st.success(f"✅ Quantity diubah untuk **{_res_t['cleared']}** baris.")
                    customer_name = st.selectbox(
                        "Distributor",
                        options=["(Pilih)"] + CUSTOMER_NAMES,
                        key=f"tpl_cust_{_fi}",
                        label_visibility="collapsed",
                    )
                    file_label = re.sub(r'[\\/*?:"<>|]', "", (customer_name or "").strip()) or "Unnamed_Customer"
                    _dl_ext  = _res_t.get("ext", "xlsx")
                    _dl_mime = _res_t.get("mime", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    st.download_button(
                        label=f"⬇ Download Hasil Modifikasi (.{_dl_ext})",
                        data=_res_t["buf"],
                        file_name=f"Form PO {file_label}.{_dl_ext}",
                        mime=_dl_mime,
                        use_container_width=True,
                        key=f"tpl_dl_{_fi}",
                    )
            else:
                st.info("ℹ️ Kolom SKU / QTY tidak terdeteksi. Periksa header row.")

    st.stop()

# =============================================================================
# DATA EXTRACTOR PAGE
# =============================================================================

st.markdown("""
    <div class="hero-wrap">
        <div class="hero-tag">✦ PO REQUEST</div>
        <div class="hero-title">PO REQUEST</div>
        <div class="hero-sub"></div>
    </div>
    """, unsafe_allow_html=True)
st.divider()

st.markdown("""
<div class="pipeline-step active">
    <span class="step-number">1</span>
    <strong>ISI DATA</strong>
    <span class="badge badge-info" style="margin-left:0.8rem;">Mandatory</span>
</div>
""", unsafe_allow_html=True)
#---------------------------SPV SIMULATOR------------------
st.markdown("<br>", unsafe_allow_html=True)
# ─────────────────────────────────────────────────────────────────────────
# DRILL-DOWN per Customer (BigQuery Suggestions)
# ─────────────────────────────────────────────────────────────────────────
#st.markdown("""
#<div class="pipeline-step active">
#    <span class="step-number">2</span>
#    <strong>Drill Down per Customer</strong>
#    <span class="badge badge-info" style="margin-left:0.8rem;">Optional</span>
#</div>
#""", unsafe_allow_html=True)

with st.container(border=True):
    _drill_col1, _drill_col2 = st.columns([2, 1])

    with _drill_col1:
        _drill_dist = st.selectbox(
            "Pilih Distributor untuk lihat suggestion SKU dari BigQuery",
            options=["(Pilih Distributor)"] + CUSTOMER_NAMES,
            key="drill_distri",
        )

    with _drill_col2:
        _brand_options = get_brand_list()
        _drill_brand = st.selectbox(
            "Filter Brand",
            options=["All"] + _brand_options,
            key="drill_brand",
        )

    if _drill_dist and _drill_dist != "(Pilih Distributor)":
        _drill_df = get_distributor_suggestions(_drill_dist, _drill_brand)

        if _drill_df.empty:
            st.info(f"ℹ️ Tidak ada suggestion SKU untuk **{_drill_dist}**.")
        else:
           
            _drill_agg = (
                _drill_df.groupby("SKU", as_index=False)
                .agg(
                    SUGGESTION_QTY=("SUGGESTION_QTY", "sum"),
                    REMAINING_ALLOCATION=("REMAINING_ALLOCATION", "sum"),
                    CURRENT_WOI=("CURRENT_WOI", "first"),
                    WOI_AFTER_PO=("WOI_AFTER_PO", "first"),
                    STATUS_ALOKASI=("STATUS_ALOKASI", "first"),
                )
                .sort_values("SUGGESTION_QTY", ascending=False)
                .reset_index(drop=True)
            )

            # Lookup product names
            # Lookup product names
            _drill_skus = _drill_agg["SKU"].astype(str).str.upper().tolist()
            _drill_names = get_sku_data(tuple(_drill_skus))
            if not _drill_names.empty:
                _drill_names["sku"] = _drill_names["sku"].astype(str).str.upper()
                _drill_agg["SKU"] = _drill_agg["SKU"].astype(str).str.upper()
                _drill_agg = _drill_agg.merge(
                    _drill_names[["sku", "product_name"]].rename(columns={"sku": "SKU", "product_name": "PRODUCT_NAME"}),
                    on="SKU",
                    how="left"
                )
            else:
                _drill_agg["PRODUCT_NAME"] = ""

            st.caption(
                f"📦 **{_drill_dist}** — {len(_drill_agg):,} SKU dengan suggestion QTY > 0"
            )

            # Build TEMPLATE PO copy URL
            _tpl_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', TEMPLATE_PO_URL)
            _copy_url = (
                f"https://docs.google.com/spreadsheets/d/{_tpl_match.group(1)}/copy"
                if _tpl_match else TEMPLATE_PO_URL
            )

            # Pagination — show 20 per page
            _page_df = _drill_agg.reset_index(drop=True)

            # Header row
            _display_cols = ["SKU", "PRODUCT_NAME", "SUGGESTION_QTY", "CURRENT_WOI", "WOI_AFTER_PO", "REMAINING_ALLOCATION", "STATUS_ALOKASI"]
            _display_cols = [c for c in _display_cols if c in _page_df.columns]
            _tbl_df = _page_df[_display_cols].copy()
            if "SUGGESTION_QTY" in _tbl_df.columns:
                _tbl_df["SUGGESTION_QTY"] = _tbl_df["SUGGESTION_QTY"].apply(
                    lambda x: int(x) if pd.notna(x) else 0
                )
            if "REMAINING_ALLOCATION" in _tbl_df.columns:
                _tbl_df["REMAINING_ALLOCATION"] = _tbl_df["REMAINING_ALLOCATION"].apply(
                    lambda x: int(x) if pd.notna(x) else 0
                )
            def _highlight_alokasi(val):
                if val == "Terdapat Alokasi":
                    return "background-color: #D6EAF8; color: #1A5490; font-weight: 600;"
                elif val == "Alokasi Habis":
                    return "background-color: #FADBD8; color: #922B21; font-weight: 600;"
                return ""

            _decimal_cols = [c for c in ["CURRENT_WOI", "WOI_AFTER_PO"] if c in _tbl_df.columns]
            _format_dict = {c: "{:.2f}" for c in _decimal_cols}

            if "STATUS_ALOKASI" in _tbl_df.columns:
                _styled_tbl = _tbl_df.style.map(
                    _highlight_alokasi, subset=["STATUS_ALOKASI"]
                ).format(_format_dict)
            else:
                _styled_tbl = _tbl_df.style.format(_format_dict)

            st.dataframe(_styled_tbl, use_container_width=True, hide_index=True)

            #with st.expander("📋 Lihat data lengkap (semua region)", expanded=False):
            #    st.dataframe(_drill_df, use_container_width=True, hide_index=True)

            # ── Make a Copy Template PO ──
            st.markdown(
                f"""
                <div style="margin-top:1rem;padding:0.9rem 1.2rem;
                            background:#FFF5F8;border:1px solid #F0C8D6;
                            border-radius:10px;display:flex;align-items:center;
                            justify-content:space-between;">
                    <div style="font-size:0.88rem;color:#5A1E38;">
                        📄 Gunakan template PO ini, lalu isi dengan data suggestion di atas
                    </div>
                    <a href="{_copy_url}" target="_blank"
                       style="background:#F49CB6;color:#fff !important;
                              text-decoration:none;padding:0.45rem 1.1rem;
                              border-radius:8px;font-size:0.85rem;font-weight:700;
                              white-space:nowrap;margin-left:1rem;">
                        📝 Make a Copy Template PO
                    </a>
                </div>
                """,
                unsafe_allow_html=True,
            )

st.divider()

RSA = ['Aqil', 'Alfaradi', 'Erliana', 'Rizky', 'Geirda', 'Rintan', 'Shaltsa', 'Daffa']

tabs = st.tabs(["🔗 Google Spreadsheet"])

with tabs[0]:
    st.session_state["extractor"] = "google_sheet"

    gsheet_url = st.text_input(
        "Google Spreadsheet URL",
        placeholder="https://docs.google.com/spreadsheets/d/...",
        help="Pastikan sheet sudah public (view access)",
        label_visibility="collapsed"
    )

    #sheet2_gid = st.text_input(
    #    "GID Sheet ke-2 (opsional)",
    #    placeholder="",
    #    help="Isi GID tab sheet lain yang ingin ditampilkan. Bisa dilihat di URL setelah gid=",
    #)
    if st.button("Load Data"):
        if not gsheet_url.strip():
            st.warning("Masukkan link Google Spreadsheet dulu.")
        else:
            csv_url, sheet_id = gsheet_to_csv_url(gsheet_url)
            if csv_url is None:
                st.error("Link tidak valid.")
            else:
                with st.spinner("🌸 Loading data..."):
                    try:
                        direct_url = _drive_to_direct(csv_url)
                        headers = {"User-Agent": "Mozilla/5.0"}
                        req = urllib.request.Request(csv_url, headers=headers)
                        
                        with urllib.request.urlopen(req, timeout=30) as resp:
                            data = resp.read()
                
                        def _read_csv_safe(raw: bytes, **kwargs) -> pd.DataFrame:
                            for _enc in ("utf-8", "cp1252", "latin-1", "iso-8859-1"):
                                try:
                                    return pd.read_csv(
                                        io.BytesIO(raw), encoding=_enc,
                                        on_bad_lines='skip',
                                        engine='python',   # <-- lebih toleran dari C engine
                                        **kwargs
                                    )
                                except UnicodeDecodeError:
                                    continue
                                except Exception:
                                    continue
                            return pd.read_csv(
                                io.BytesIO(raw), encoding="utf-8",
                                encoding_errors="replace",
                                on_bad_lines='skip',
                                engine='python',
                                **kwargs
                            )
                
                        df_loaded = _read_csv_safe(data, dtype=str)      # <-- hasil ganti
                        df_column = _read_csv_safe(data, header=8)       # <-- hasil ganti
                        df_loaded = numeric_coerce(df_loaded)
                                        
                        # Store in session state only after successful load
                        st.session_state['raw_df']      = df_loaded
                        st.session_state['df']          = df_column
                        st.session_state['gsheet_url']  = gsheet_url
                        st.session_state['data_source'] = f"Google Sheet ({sheet_id[0]}...)"
                        st.session_state['source_type'] = 'GSHEET'
                        st.session_state.pop('export_bytes', None)
                        
                        st.success("✅ Data berhasil dimuat!")
                        st.rerun() # Refresh to clear the 'st.stop()' below
                        
                    except Exception as e:
                        # This block is what was missing!
                        st.error(f"⚠️ Gagal memuat data dari Google Sheets: {e}")
                        st.info("Pastikan link sudah diset ke 'Anyone with the link' (Public).")

    # The app will pause here until 'df' exists in session state
    if 'df' not in st.session_state:
        st.stop()

    df = st.session_state['df'].copy()
    # ── Validasi PRODUCT CODE & QTY ───────────────────────────────────────
    _val_errors = []

    # 1. Cek kolom wajib ada
    _has_product_code = 'PRODUCT CODE' in df.columns
    _has_qty = 'QTY' in df.columns

    if not _has_product_code:
        _val_errors.append("❌ Kolom **PRODUCT CODE** tidak ditemukan di spreadsheet.")
    if not _has_qty:
        _val_errors.append("❌ Kolom **QTY** tidak ditemukan di spreadsheet.")

    if _has_qty:
        # 2. Cek QTY format — harus numeric
        _df_check = df.copy()
        _df_check['_qty_num'] = pd.to_numeric(
            _df_check['QTY'].astype(str)
            .str.replace('.', '', regex=False)
            .str.replace(',', '.', regex=False)
            .str.strip(),
            errors='coerce'
        )
        _invalid_qty = _df_check[
            _df_check['QTY'].notna() &
            _df_check['QTY'].astype(str).str.strip().ne('') &
            _df_check['_qty_num'].isna()
        ]['QTY'].unique().tolist()

        if _invalid_qty:
            _val_errors.append(
                f"❌ Kolom **QTY** memiliki nilai yang bukan angka: "
                f"`{'`, `'.join([str(v) for v in _invalid_qty[:10]])}`"
                + (" ..." if len(_invalid_qty) > 10 else "")
            )

    if _has_product_code:
        # 3. Cek format PRODUCT CODE — harus match pola G2G-xxx
        _df_pc = df['PRODUCT CODE'].dropna().astype(str).str.strip()
        _df_pc = _df_pc[_df_pc != '']
        _invalid_pc = _df_pc[
            ~_df_pc.str.upper().str.match(r'^G2G-\S+$')
        ].unique().tolist()

        if _invalid_pc:
            _val_errors.append(
                f"⚠️ **PRODUCT CODE** berikut tidak sesuai format (contoh: G2G-223): "
                f"`{'`, `'.join([str(v) for v in _invalid_pc[:10]])}`"
                + (" ..." if len(_invalid_pc) > 10 else "")
            )

        # 4. Cross-check PRODUCT CODE vs BigQuery SKU master
        _pc_list = _df_pc.str.upper().unique().tolist()
        if _pc_list:
            with st.spinner("🔍 Memvalidasi PRODUCT CODE ke BigQuery..."):
                _bq_check = get_sku_data(tuple(_pc_list))
            if not _bq_check.empty:
                _found_skus = set(_bq_check['sku'].astype(str).str.upper().tolist())
                _not_found = [p for p in _pc_list if p not in _found_skus]
                if _not_found:
                    _val_errors.append(
                        f"❌ **PRODUCT CODE** berikut tidak ditemukan di sistem (BigQuery): "
                        f"`{'`, `'.join(_not_found[:10])}`"
                        + (" ..." if len(_not_found) > 10 else "")
                    )
            else:
                _val_errors.append(
                    "⚠️ Tidak dapat memvalidasi PRODUCT CODE ke BigQuery"
                    
                )

    # 5. Tampilkan semua error/warning
    if _val_errors:
        st.warning("**⚠️ Data perlu dicek ulang sebelum di-generate:**")
        for _err in _val_errors:
            st.markdown(f"- {_err}")
        st.info("💡 Pastikan PRODUCT CODE sesuai format sistem (contoh: **G2G-223**) "
                "dan kolom QTY berisi angka saja.")
    else:
        st.success("✅ Validasi OK — PRODUCT CODE dan QTY sesuai format.")



    
    # ── Input: Distributor & RSA ───────────────────────────────────────────
    with st.container(border=True):
        col1, col2 = st.columns([1, 1])
        with col1:
            st.markdown("**DISTRIBUTOR**")
            pilih = st.selectbox(
                "",
                options=["(Pilih)"] + CUSTOMER_NAMES,
                key="distri",
                label_visibility="collapsed"
            )

        with col2:
            st.markdown("**RSA NAME**")
            rsa_pilih = st.selectbox(
                "",
                options=["(Pilih)"] + RSA,
                key="rsa",
                label_visibility="collapsed"
            )

    if pilih == "(Pilih)":
        st.info("Silakan pilih Distributor terlebih dahulu.")
        st.stop()

    # ── Isi kolom DISTRIBUTOR ──────────────────────────────────────────────
    if 'DISTRIBUTOR' not in df.columns:
        df['DISTRIBUTOR'] = pilih
    else:
        df['DISTRIBUTOR'] = df['DISTRIBUTOR'].fillna(pilih)

    # ── Bersihkan & konversi TOTAL PRICE ──────────────────────────────────
    df['TOTAL PRICE'] = (
        df['TOTAL PRICE']
        .astype(str)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False)
        .astype(float)
    )

    discount    = 0
    sub_total   = df['TOTAL PRICE'].sum()
    tax         = sub_total * 0.11
    grand_total = sub_total - discount + tax
    count_sku   = df['PRODUCT CODE'].notna().sum()

    summary = pd.DataFrame([
        {"PRODUCT CODE": "", "DESCRIPTION": "", "QTY": "SUB-TOTAL",  "DPP": "", "TOTAL PRICE": sub_total},
        {"PRODUCT CODE": "", "DESCRIPTION": "", "QTY": "DISCOUNTS",  "DPP": "", "TOTAL PRICE": -discount},
        {"PRODUCT CODE": "", "DESCRIPTION": "", "QTY": "Tax (11%)",  "DPP": "", "TOTAL PRICE": tax},
        {"PRODUCT CODE": "", "DESCRIPTION": "", "QTY": "GRAND TOTAL","DPP": "", "TOTAL PRICE": grand_total},
    ])

    df_final = pd.concat([df, summary], ignore_index=True).iloc[:, 0:6]
    with st.expander(
            f"👁 Lihat isi · {len(df_final)} baris · {df_final.shape[1]} kolom · indeks 0–{df_final.shape[1]-1}",
            expanded=False):
            st.dataframe(df_final.iloc[:, :6].reset_index(drop=True), use_container_width=True)
            st.success(f"GRAND TOTAL: Rp {grand_total:,.0f} | Total SKU: {count_sku}")

        # ── Lookup LIFECYCLE STATUS dari Product List ──────────────────────────
    PRODUCT_LIST_URL = "https://docs.google.com/spreadsheets/d/1_4SFn2_SvGm1on0EJkntYjC2cLvNZyDjX54zcQAWRtQ/export?format=csv&gid=91084545"

    @st.cache_data(ttl=3600, show_spinner=False)
    def load_product_list() -> pd.DataFrame:
        try:
            df_raw = pd.read_csv(PRODUCT_LIST_URL, header=None, nrows=10)
            header_row = 0
            for i, row in df_raw.iterrows():
                row_str = ' '.join(row.astype(str).str.upper().tolist())
                if 'PRODUCT' in row_str or 'SKU' in row_str or 'CODE' in row_str:
                    header_row = i
                    break
            df_p = pd.read_csv(PRODUCT_LIST_URL, header=header_row)
            df_p.columns = [str(c).strip().upper() for c in df_p.columns]
            return df_p
        except Exception as e:
            st.warning(f"Gagal load Product List: {e}")
            return pd.DataFrame()
    df_product = load_product_list()
    lifecycle_col    = next((c for c in df_product.columns if any(k in c for k in ['LIFECYCLE', 'LIFE CYCLE', 'LIFESTYLE', 'STATUS'])), None)
    product_code_col = next((c for c in df_product.columns if any(k in c for k in ['PRODUCT CODE', 'SKU', 'CODE'])), None)
    if lifecycle_col and product_code_col:
    # Pastikan tipe data sama sebelum merge
        df['PRODUCT CODE'] = df['PRODUCT CODE'].astype(str).str.strip()
        df_product[product_code_col] = df_product[product_code_col].astype(str).str.strip()

        df = df.merge(
            df_product[[product_code_col, lifecycle_col]].rename(columns={
                product_code_col: 'PRODUCT CODE',
                lifecycle_col:    'LIFECYCLE STATUS',
            }),
            on='PRODUCT CODE',
            how='left'
        )
        _preview_cols = [c for c in ['PRODUCT CODE', 'DESCRIPTION', 'LIFECYCLE STATUS'] if c in df.columns]
        _df_preview   = df[_preview_cols].dropna(subset=['PRODUCT CODE']).copy()
        _matched      = _df_preview['LIFECYCLE STATUS'].notna().sum()
        _unmatched    = _df_preview['LIFECYCLE STATUS'].isna().sum()
        with st.expander(
            f"🔗 Preview Lifecycle Status · {_matched} matched · {_unmatched} not found",
            expanded=False):
            st.dataframe(_df_preview.reset_index(drop=True), use_container_width=True, hide_index=True)
    else:
        st.warning(f"Kolom lifecycle/product tidak ditemukan di Product List — cek: {df_product.columns.tolist()}")
    st.divider()
    # ── Helper: Export to template ─────────────────────────────────────────
    @st.cache_data(show_spinner=False)
    def fetch_template_xlsx(url: str) -> bytes | None:
        m = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', url)
        if not m:
            return None
        gid_match  = re.search(r'gid=(\d+)', url)
        gid        = gid_match.group(1) if gid_match else '0'
        export_url = (
            f"https://docs.google.com/spreadsheets/d/{m.group(1)}"
            f"/export?format=xlsx&gid={gid}&single=true"
        )
        req = urllib.request.Request(export_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()

        wb = openpyxl.load_workbook(io.BytesIO(raw), data_only=False, keep_links=False)
        for sheet_name in wb.sheetnames[2:]:
            del wb[sheet_name]
        # ── Truncate: max 200 rows × kolom A–H ─────────────────────────────
        ws = wb.active
        MAX_ROW = 200
        MAX_COL = 8  # A=1, B=2, ..., H=8

        # Hapus baris di atas 200
        if ws.max_row > MAX_ROW:
            ws.delete_rows(MAX_ROW + 1, ws.max_row - MAX_ROW)

        # Hapus kolom setelah H
        if ws.max_column > MAX_COL:
            ws.delete_cols(MAX_COL + 1, ws.max_column - MAX_COL)

        buf = io.BytesIO()
        wb.save(buf)
        return buf.getvalue()

    
    def export_to_template(
        df_data: pd.DataFrame,
        template_bytes: bytes,
        distributor: str,
        rsa_name: str,
        discount: float,
    ) -> bytes:
        wb = openpyxl.load_workbook(io.BytesIO(template_bytes), data_only=False, keep_links=False)
        ws = wb.active

        # Isi header
        ws['B3'] = distributor
        ws['E3'] = datetime.now().strftime("%d %B %Y")
        ws['E5'] = rsa_name

        COL_MAP = {
            'DISTRIBUTOR':  1,  # A
            'PRODUCT CODE': 2,  # B
            'DESCRIPTION':  3,  # C
            'QTY':          4,  # D
            'DPP':          5,  # E
            'TOTAL PRICE':  6,  # F
        }
        START_ROW      = 10
        SUMMARY_LABELS = ['SUB-TOTAL', 'DISCOUNTS', 'Tax (11%)', 'GRAND TOTAL']

        df_export = df_data[~df_data['QTY'].astype(str).isin(SUMMARY_LABELS)].copy()

        # Tulis data + formula TOTAL PRICE = QTY * DPP
        for r_offset, (_, row) in enumerate(df_export.iterrows()):
            excel_row = START_ROW + r_offset
            for col_name, col_idx in COL_MAP.items():
                if col_name == 'TOTAL PRICE':
                    ws.cell(row=excel_row, column=col_idx, value=f"=D{excel_row}*E{excel_row}")
                else:
                    val = row.get(col_name, "")
                    if pd.isna(val) or str(val).strip() in ('', 'nan', 'None'):
                        val = None
                    ws.cell(row=excel_row, column=col_idx, value=val)

        # Tulis summary dengan formula Excel
        last_data_row = START_ROW + len(df_export) - 1
        summary_start = last_data_row + 2
        sub_row, disc_row, tax_row, grand_row = (
            summary_start, summary_start + 1, summary_start + 2, summary_start + 3
        )

        for row_idx, label, formula in [
            (sub_row,   "SUB-TOTAL",   f"=SUM(F{START_ROW}:F{last_data_row})"),
            (disc_row,  "DISCOUNTS",   f"=0"),
            (tax_row,   "Tax (11%)",   f"=F{sub_row}*0.11"),
            (grand_row, "GRAND TOTAL", f"=F{sub_row}-F{disc_row}+F{tax_row}"),
        ]:
            ws.cell(row=row_idx, column=4, value=label)
            ws.cell(row=row_idx, column=6, value=formula)
# ── Tulis sheet ke-2 dari Google Sheet ──
        _other = st.session_state.get('other_sheets', {})
        if _other:
            _sheet2_name = list(_other.keys())[0]
            _sheet2_df   = _other[_sheet2_name]

            # Kalau template sudah punya sheet ke-2, pakai itu
            # Kalau belum, buat baru
            if len(wb.sheetnames) >= 2:
                ws2 = wb[wb.sheetnames[1]]
                # Kosongkan dulu
                for row in ws2.iter_rows():
                    for cell in row:
                        cell.value = None
            else:
                ws2 = wb.create_sheet(title=_sheet2_name[:31])

            # Tulis header
            for c_idx, col_name in enumerate(_sheet2_df.columns, 1):
                ws2.cell(row=1, column=c_idx, value=col_name)

            # Tulis data
            for r_idx, row_data in enumerate(_sheet2_df.itertuples(index=False), 2):
                for c_idx, val in enumerate(row_data, 1):
                    ws2.cell(row=r_idx, column=c_idx, value=(
                        None if pd.isna(val) or str(val).strip() in ('', 'nan') else val
                    ))

        buf = io.BytesIO()
        wb.save(buf)
        return buf.getvalue()
    def excel_to_pdf(
        df_data: pd.DataFrame,
        distributor: str,
        rsa_name: str,
        sub_total: float,
        tax: float,
        grand_total: float,
        discount: float = 0,
    ) -> bytes:
        """Generate PDF PO menggunakan reportlab (pure Python)."""
        SUMMARY_LABELS = ['SUB-TOTAL', 'DISCOUNTS', 'Tax (11%)', 'GRAND TOTAL']
        df_clean = df_data[~df_data['QTY'].astype(str).isin(SUMMARY_LABELS)].copy()

        buf = io.BytesIO()
        doc = SimpleDocTemplate(
            buf, pagesize=A4,
            leftMargin=15*mm, rightMargin=15*mm,
            topMargin=15*mm, bottomMargin=15*mm,
        )
        elements = []
        styles   = getSampleStyleSheet()

        # ── Title & header ─────────────────────────────────────────────────
        title_style = ParagraphStyle(
            'Title', parent=styles['Title'],fontName='Trebuchet-Bold',
            fontSize=12 , textColor=colors.HexColor("#B53473"),
            alignment=1, spaceAfter=12,
        )
        elements.append(Paragraph("PURCHASE ORDER", title_style))

        # Info header (Distributor, Date, RSA)
        info_data = [
            ['Distributor:',  distributor,
             'Date:',         datetime.now().strftime("%d %B %Y")],
            ['RSA:',          rsa_name,
             '',              ''],
        ]
        info_tbl = Table(info_data, colWidths=[20*mm, 85*mm, 20*mm, 50*mm])
        info_tbl.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Trebuchet'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('FONTNAME', (0, 0), (0, -1), 'Trebuchet-Bold'),
            ('FONTNAME', (2, 0), (2, -1), 'Trebuchet-Bold'),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        elements.append(info_tbl)
        elements.append(Spacer(1, 8*mm))

        # ── Data table ─────────────────────────────────────────────────────
        header = ['No', 'PRODUCT CODE', 'DESCRIPTION', 'QTY', 'DPP', 'TOTAL PRICE']
        data   = [header]
        for i, (_, row) in enumerate(df_clean.iterrows(), start=1):
            qty   = row.get('QTY', '')
            dpp   = row.get('DPP', '')
            total = row.get('TOTAL PRICE', 0)
            data.append([
                str(i),
                str(row.get('PRODUCT CODE', '')),
                str(row.get('DESCRIPTION', ''))[:40],
                f"{qty}" if pd.notna(qty) else "",
                f"{dpp:,.0f}" if isinstance(dpp, (int, float)) else str(dpp),
                f"{total:,.0f}" if isinstance(total, (int, float)) else str(total),
            ])

        # Summary rows
        data.append(['', '', '', '', 'SUB-TOTAL',   f"{sub_total:,.0f}"])
        data.append(['', '', '', '', 'DISCOUNTS',   f"-{discount:,.0f}"])
        data.append(['', '', '', '', 'Tax (11%)',   f"{tax:,.0f}"])
        data.append(['', '', '', '', 'GRAND TOTAL', f"{grand_total:,.0f}"])

        tbl = Table(data, colWidths=[10*mm, 25*mm, 70*mm, 20*mm, 20*mm, 30*mm])
        tbl.setStyle(TableStyle([
            # Header
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#BF3979')),
            ('FONTNAME',   (4, -4), (-1, -1), 'Trebuchet-Bold'),
            ('TEXTCOLOR',  (0, 0), (-1, 0), colors.white),
            ('FONTSIZE',   (0, 0), (-1, 0), 7),
            ('ALIGN',      (0, 0), (-1, 0), 'CENTER'),
            # Body
            ('FONTNAME',   (0, 1), (-1, -5), 'Trebuchet'),
            ('FONTSIZE',   (0, 1), (-1, -1), 6),
            ('ALIGN',      (3, 1), (-1, -1), 'RIGHT'),
            ('ALIGN',      (0, 1), (0, -5),  'CENTER'),
            ('VALIGN',     (0, 0), (-1, -1), 'MIDDLE'),
            # Grid
            ('GRID',       (0, 0), (-1, -5), 0.4, colors.HexColor('#FFB6C1')),
            # Summary section
            ('FONTNAME',   (4, -4), (-1, -1), 'Trebuchet-Bold'),
            ('LINEABOVE',  (4, -4), (-1, -4), 0.8, colors.HexColor('#FFB6C1')),
            ('BACKGROUND', (4, -1), (-1, -1), colors.HexColor('#FFB6C1')),
            ('TEXTCOLOR',  (4, -1), (-1, -1), colors.black),
            # Padding
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING',    (0, 0), (-1, -1), 4),
            # Row banding
            ('ROWBACKGROUNDS', (0, 1), (-1, -5),
             [colors.white, colors.HexColor('#FAFAFA')]),
        ]))
        elements.append(tbl)

        doc.build(elements)
        return buf.getvalue()
    # ── Export section ─────────────────────────────────────────────────────
    if st.button("🔄 Generate", use_container_width=True):
        with st.spinner("Prepare file..."):
            try:
                tpl_bytes    = fetch_template_xlsx(st.session_state['gsheet_url'])
                export_bytes = export_to_template(df_final, tpl_bytes, pilih, rsa_pilih, discount)
                st.session_state['export_bytes'] = export_bytes
            except Exception as e:
              
                st.error(f"Gagal generate file: {e}")
                st.session_state.pop('export_bytes', None)
   

    if 'export_bytes' in st.session_state:
        prog = st.progress(0)
        for i in range(101):
            prog.progress(i, text="Processing complete" if i == 100 else f"Loading... {i}%")

        # ── Custom CSS pink mentah ─────────────────────────────────────────
        st.markdown("""
        <style>
        div[data-testid="stDownloadButton"] button {
            background-color: #FFB6C1 !important;
            color: #000000 !important;
            border: 1px solid #FF8FA3 !important;
            font-weight: 600 !important;
        }
        div[data-testid="stDownloadButton"] button:hover {
            background-color: #FF99B0 !important;
            color: #000000 !important;
        }
        div[data-testid="stDownloadButton"] button * {
            color: #000000 !important;
        }
        </style>
        """, unsafe_allow_html=True)

        # ── Two-button layout ──────────────────────────────────────────────
        dl_col1, dl_col2 = st.columns(2)

        with dl_col1:
            st.download_button(
                label="Export Excel",
                data=st.session_state['export_bytes'],
                file_name=f"PO_{pilih}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="dl_excel",
            )

        with dl_col2:
            try:
                pdf_bytes = excel_to_pdf(
                    df_final, pilih, rsa_pilih,
                    sub_total, tax, grand_total, discount,
                )
                st.download_button(
                    label="📄 Export PDF",
                    data=pdf_bytes,
                    file_name=f"PO_{pilih}_{datetime.now().strftime('%Y%m%d')}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    key="dl_pdf",
                )
            except Exception as e:
                st.error(f"Gagal generate PDF: {e}")
