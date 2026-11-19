"""
PO Suggestion Tool - Production Release
========================================
Version: 2.1.0
Last Updated: 2026-11-19

A production-ready Streamlit application for generating Purchase Order suggestions
based on inventory buffer analysis from BigQuery.

Features:
- Automated inventory refresh (2-hour intervals)
- Per-store PDF download and bulk download (one ZIP, one PDF per store)
- Advanced filtering and selection
- Real-time data synchronization
- Download tracking per store + admin dashboard (login required)
- Comprehensive error handling and logging

Supporting modules (pure logic, unit-tested without Streamlit):
- po_buffer_downloads.py  file naming, bulk plan and ZIP assembly
- po_buffer_tracking.py   persistent download tracking + dashboard aggregation
- po_buffer_admin.py      admin credentials and login state
"""

import streamlit as st
import pandas as pd
import pytz
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from google.oauth2 import service_account
from google.cloud import bigquery
from google.api_core import exceptions as google_exceptions
import io
from reportlab.lib.pagesizes import A4, portrait
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

import po_buffer_admin as admin_auth
import po_buffer_tracking as tracking
from po_buffer_downloads import (
    build_bulk_zip,
    build_pdf_filename,
    build_store_info,
    build_unique_filenames,
    get_store_detail,
    jakarta_date_str,
    plan_bulk_download,
)

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Application configuration constants"""
    APP_TITLE = "PO Suggestion Tool"
    APP_ICON = "📦"
    VERSION = "2.1.0"
    BRAND_NAME = "Glad2Glow"
    
    # BigQuery Configuration
    BQ_PROJECT = "skintific-data-warehouse"
    BQ_DATASET = "rsa"
    BQ_TABLE = "inventory_buffer"
    BQ_STAGING_DATASET = "gt_schema"
    BQ_STAGING_TABLE = "po_store_suggestion_stagging"
    BQ_MASTER_TABLE = "master_distributor"
    BQ_STORED_PROC = f"`{BQ_PROJECT}.{BQ_DATASET}.inventory_buffer_sp`"
    
    # Timing Configuration
    CACHE_TTL_SECONDS = 600  # 10 minutes
    REFRESH_INTERVAL_SECONDS = 7200  # 2 hours
    
    # Priority Thresholds
    PRIORITY_HIGH_THRESHOLD = 5_000_000
    PRIORITY_MEDIUM_THRESHOLD = 2_000_000
    
    # Timezone
    TIMEZONE = "Asia/Jakarta"

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging():
    """Configure application logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title=Config.APP_TITLE,
    page_icon=Config.APP_ICON,
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ============================================================================
# CUSTOM CSS (Consolidated)
# ============================================================================

def load_custom_css():
    """Load custom CSS styling"""
    st.markdown("""
<style>
    /* Hide default streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Custom header styling */
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem 2rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .sync-status {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        background: rgba(255,255,255,0.2);
        padding: 0.4rem 1rem;
        border-radius: 20px;
        font-size: 0.85rem;
        color: white;
        margin-top: 0.5rem;
    }
    
    .sync-dot {
        width: 8px;
        height: 8px;
        background: #4ade80;
        border-radius: 50%;
        animation: pulse 2s infinite;
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }
    
    .selection-title {
        color: #5b6abf;
        font-size: 1.1rem;
        font-weight: 600;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .metric-group {
        display: flex;
        flex-direction: column;
    }

    .metric-label {
        font-size: 0.7rem;
        color: #94a3b8;
        text-transform: uppercase;
        font-weight: 700;
        margin-bottom: 2px;
    }

    .metric-value {
        font-size: 0.9rem;
        font-weight: 600;
        color: #334155;
    }

    .value-blue {
        color: #3b82f6;
    }
    
    /* Streamlit button override */
    .stButton > button {
        width: 100%;
        background: #5b6abf;
        color: white;
        border: none;
        padding: 0.65rem 1.5rem;
        border-radius: 8px;
        font-weight: 600;
        transition: all 0.2s;
    }
    
    .stButton > button:hover {
        background: #4a5aa0;
        box-shadow: 0 4px 8px rgba(91,106,191,0.3);
    }
    
    .stDownloadButton > button {
        background: #10b981 !important;
        color: white !important;
    }
    
    .stDownloadButton > button:hover {
        background: #059669 !important;
    }
</style>
""", unsafe_allow_html=True)

load_custom_css()

# ============================================================================
# BIGQUERY CLIENT INITIALIZATION
# ============================================================================

# po_store_suggestion_stagging is a Drive-backed external table. BigQuery can
# read it only when the job credentials also carry a Drive scope.
BQ_SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/drive",
]

@st.cache_resource
def get_bigquery_client() -> bigquery.Client:
    """
    Initialize BigQuery client with credentials
    
    Returns:
        bigquery.Client: Authenticated BigQuery client
        
    Raises:
        RuntimeError: If credentials cannot be loaded from any source
    """
    logger.info("Initializing BigQuery client...")
    
    # Try multiple credential sources in order of preference
    credential_sources = [
        ("Streamlit secrets (connections.bigquery)", lambda: _load_from_streamlit_secrets("connections")),
        ("Streamlit secrets (bigquery)", lambda: _load_from_streamlit_secrets("bigquery")),
        ("Local credentials file", _load_from_local_file),
    ]
    
    for source_name, loader_func in credential_sources:
        try:
            logger.info(f"Attempting to load credentials from: {source_name}")
            credentials, project_id = loader_func()
            client = bigquery.Client(credentials=credentials, project=project_id)
            logger.info(f"✓ Successfully initialized BigQuery client using {source_name}")
            return client
        except Exception as e:
            logger.warning(f"Failed to load from {source_name}: {str(e)[:200]}")
            continue
    
    # If all sources failed
    error_msg = """
    ❌ Failed to initialize BigQuery client from all sources.
    
    Please configure credentials using one of these methods:
    1. Streamlit Cloud: Add secrets in .streamlit/secrets.toml
    2. Local Development: Place credentials JSON file in project directory
    
    See documentation for setup instructions.
    """
    logger.error(error_msg)
    raise RuntimeError(error_msg)

def _load_from_streamlit_secrets(key: str) -> Tuple:
    """Load credentials from Streamlit secrets"""
    if key == "connections":
        gcp_secrets = dict(st.secrets["connections"]["bigquery"])
    else:
        gcp_secrets = dict(st.secrets[key])
    
    # Fix private key formatting
    if "private_key" in gcp_secrets:
        gcp_secrets["private_key"] = gcp_secrets["private_key"].replace("\\n", "\n")
    
    credentials = service_account.Credentials.from_service_account_info(
        gcp_secrets,
        scopes=BQ_SCOPES,
    )
    project_id = gcp_secrets.get("project_id") or gcp_secrets.get("project") or Config.BQ_PROJECT
    
    return credentials, project_id

def _load_from_local_file() -> Tuple:
    """Load credentials from local file"""
    import os
    
    possible_paths = [
        "skintific-data-warehouse-ea77119e2e7a.json",
        "credentials.json",
        "../credentials.json",
        os.path.expanduser("~/credentials.json"),
    ]
    
    credentials_path = None
    for path in possible_paths:
        if os.path.exists(path):
            credentials_path = path
            break
    
    if not credentials_path:
        raise FileNotFoundError("No credentials file found in expected locations")
    
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=BQ_SCOPES,
    )
    project_id = Config.BQ_PROJECT
    
    return credentials, project_id

# ============================================================================
# TIME UTILITIES
# ============================================================================

def get_jkt_now() -> datetime:
    """Get current time in Jakarta timezone"""
    return datetime.now(pytz.timezone(Config.TIMEZONE))

def format_jkt_time(dt: datetime, format_str: str = "%d %b %Y, %H:%M WIB") -> str:
    """Format datetime in Jakarta timezone"""
    if dt is None:
        return "-"
    
    jkt_tz = pytz.timezone(Config.TIMEZONE)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    
    dt_jkt = dt.astimezone(jkt_tz)
    return dt_jkt.strftime(format_str)
    
def safe_str(value, default: str = "-") -> str:
    """
    Safely convert any value to string, handling None and NaN
    
    Args:
        value: Any value to convert
        default: Default string if value is None/NaN
        
    Returns:
        str: String representation or default
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    return str(value)

# ============================================================================
# PDF GENERATION FUNCTIONS
# ============================================================================

def generate_po_pdf(
    store_info: Dict,
    store_detail: pd.DataFrame,
    brand_name: str = Config.BRAND_NAME
) -> io.BytesIO:
    """
    Generate PO PDF for a single store
    
    Args:
        store_info: Store metadata dictionary
        store_detail: DataFrame with product details
        brand_name: Brand name for PDF header
        
    Returns:
        BytesIO: PDF file buffer
    """
    try:
        logger.info(f"Generating PDF for store: {store_info['store_code']}")
        
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=portrait(A4),
            rightMargin=25,
            leftMargin=25,
            topMargin=25,
            bottomMargin=25
        )
        
        styles = getSampleStyleSheet()
        elements = _generate_store_page_elements(store_info, store_detail, styles, brand_name)
        
        doc.build(elements)
        buffer.seek(0)
        
        logger.info(f"✓ PDF generated successfully for {store_info['store_code']}")
        return buffer
        
    except Exception as e:
        logger.error(f"Failed to generate PDF for {store_info.get('store_code', 'unknown')}: {str(e)}")
        raise

def _generate_store_page_elements(
    store_data: Dict,
    sku_data: pd.DataFrame,
    styles,
    brand_name: str
) -> List:
    elements = []
    table_font_size = 8.5
    
    sku_data_filtered = sku_data[sku_data['buffer_plan'].notna()].copy()
    
    # Define styles
    cell_style = ParagraphStyle(
        'TableCell',
        parent=styles['Normal'],
        fontSize=table_font_size,
        leading=table_font_size + 2,
        alignment=TA_CENTER,
        wordWrap='LTR'
    )
    
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.HexColor('#D8A7A7'),
        alignment=TA_CENTER,
        spaceAfter=10
    )
    
    # Header
    elements.append(Paragraph(brand_name, title_style))
    elements.append(Paragraph("<b>📋 FORMULIR PEMBELIAN (PO)</b>", styles['Heading2']))
    elements.append(Spacer(1, 8))
    
    # Store Information
    store_info_data = [
    [f"Toko: {safe_str(store_data.get('store_name'))}",
     f"Kode: {safe_str(store_data.get('store_code'))}"],

    [f"Region: {safe_str(store_data.get('region'))}",
     f"Distributor: {safe_str(store_data.get('distributor_g2g'))}"],

    [f"Tgl: {get_jkt_now().strftime('%d/%b/%Y')}",
     "Sales: _________________"]
    ]

    
    info_table = Table(store_info_data, colWidths=[4.0 * inch, 3.5 * inch])
    info_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    elements.append(info_table)
    elements.append(Spacer(1, 10))
    
    # ✅ Summary - NOW CALCULATED ON FILTERED DATA
    total_out_of_stock = len(sku_data_filtered[sku_data_filtered['estimated_actual_stock_adj'] == 0])
    total_suggested = int(sku_data_filtered['buffer_plan'].fillna(0).sum())
    total_value = int(
    sku_data_filtered['buffer_plan_value']
    .fillna(0)
    .sum()
    )


    
    summary_table = Table(
        [
            ["SKU HABIS", "SARAN QTY", "EST. NILAI ORDER"],
            [str(total_out_of_stock), str(total_suggested), f"Rp {total_value:,.0f}"]
        ],
        colWidths=[2.5 * inch] * 3
    )
    
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, 0), colors.HexColor('#E74C3C')),
        ('BACKGROUND', (1, 0), (1, 0), colors.HexColor('#27AE60')),
        ('BACKGROUND', (2, 0), (2, 0), colors.HexColor('#3498DB')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 1), (-1, 1), 12),
        ('GRID', (0, 0), (-1, -1), 1, colors.white),
    ]))
    
    elements.append(summary_table)
    elements.append(Spacer(1, 12))
    
    # Product Table
    table_data = [
        ["SKU / Produk", "Stok\nToko\nEst", "SO\nBulanan",
         "Dist.\nStok", "Saran\n(Qty)", "Nilai Order\n(Rp)", "Order\nAktual"]
    ]
    
    dist_stock_map = {
        "Stok Tersedia": "Ada",
        "Stok Menipis": "Menipis"
    }
    
    # ✅ Use filtered data for stock_date too
    stock_date_val = sku_data_filtered['stock_date'].iloc[0] if not sku_data_filtered.empty else None

    stock_date_str = (
        stock_date_val.strftime('%d %b %Y')
        if isinstance(stock_date_val, (datetime, pd.Timestamp))
        else safe_str(stock_date_val)
    )

    
    # ✅ Loop through filtered data - NO MORE if buffer_qty == 0 CHECK NEEDED
    for _, row in sku_data_filtered.iterrows():
        buffer_qty = int(row['buffer_plan']) if pd.notna(row['buffer_plan']) else 0
        actual_stock = int(row['estimated_actual_stock_adj']) if not pd.isna(row['estimated_actual_stock_adj']) else 0
        buffer_val = (
            int(row['buffer_plan_value'].item())
            if hasattr(row['buffer_plan_value'], "item")
            else int(row['buffer_plan_value'])
            if not pd.isna(row['buffer_plan_value'])
            else 0
        )
        
        # ✅ FIX: Handle NaN in SO_Bulanan - convert to 0 instead of '-'
        so_bulanan = row.get('SO_Bulanan')
        so_bulanan_display = (
            str(int(so_bulanan)) 
            if pd.notna(so_bulanan) 
            else "0"
        )
        
        prod_style = ParagraphStyle('ProdStyle', parent=cell_style, alignment=TA_LEFT)
        priority_label = safe_str(row.get('priority_label', ''), default='')
        priority_line = f"<br/>{priority_label}" if priority_label else ""

        merged_product = Paragraph(
            f"<b>{row['product_code']}</b><br/>{row['product_name']}{priority_line}",
        prod_style
        )
        
        raw_dist_stock = str(row.get('status_stok_distributor', '-'))
        dist_stock_display = dist_stock_map.get(raw_dist_stock, raw_dist_stock)
        
        value_style = ParagraphStyle('ValStyle', parent=cell_style, alignment=TA_RIGHT)
        
        table_data.append([
            merged_product,
            str(actual_stock),
            so_bulanan_display,  # ✅ Use the handled value
            Paragraph(dist_stock_display, cell_style),
            str(buffer_qty),
            Paragraph(f"{buffer_val:,}", value_style),
            "____"
        ])
    
    # ... rest of the function remains the same
    
    col_widths = [
        2.30 * inch, 0.50 * inch, 0.60 * inch, 0.75 * inch,
        0.65 * inch, 1.20 * inch, 0.95 * inch
    ]
    
    product_table = Table(table_data, colWidths=col_widths, repeatRows=1)
    product_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2C3E50')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), table_font_size),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ('TOPPADDING', (-1, 1), (-1, -1), 8),
        ('BOTTOMPADDING', (-1, 1), (-1, -1), 8),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1),
         [colors.white, colors.HexColor('#F8F9FA')]),
    ]))
    
    elements.append(product_table)
    elements.append(Spacer(1, 10))
    elements.append(Paragraph(
        f"<b>Data stok toko terakhir diperbarui: {stock_date_str}</b>",
        styles['Normal']
    ))
    elements.append(Spacer(1, 10))
    elements.append(Paragraph(
        f"<i>* System Generated: {get_jkt_now().strftime('%d %b %Y, %H:%M')}</i>",
        styles['Italic']
    ))
    
    return elements

def generate_store_pdf(store_info: Dict, store_detail: pd.DataFrame) -> bytes:
    """
    Generate the PO PDF of exactly one store, as bytes.

    Thin wrapper over generate_po_pdf (unchanged) used by both the individual
    download button and the bulk ZIP, so a store gets the same PDF either way.
    """
    return generate_po_pdf(store_info, store_detail, brand_name=Config.BRAND_NAME).getvalue()

# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

def combined_inventory_sql() -> str:
    """
    Inventory buffer unioned with the current-month store suggestion staging
    table, collapsed to one row per store_code + id_st + product_code.

    Numeric, qty, value, and date columns use MAX. Text and bool columns use
    ANY_VALUE. Only rows with buffer_plan IS NOT NULL are returned.
    Staging rows are limited to Kalimantan and Sulawesi. Their store_code is
    taken from inventory for the same id_st, preferring the code equal to
    id_st, so a plan is not copied across store codes that share an id_st.
    """
    inventory_table = f"`{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}`"
    staging_table = (
        f"`{Config.BQ_PROJECT}.{Config.BQ_STAGING_DATASET}.{Config.BQ_STAGING_TABLE}`"
    )
    return f"""
    SELECT *
    FROM (
    SELECT
        id_st,
        product_code,
        MAX(inner_pcs) AS inner_pcs,
        MAX(price_for_store) AS price_for_store,
        MAX(actual_stock) AS actual_stock,
        MAX(stock_date) AS stock_date,
        MAX(avg_daily_qty) AS avg_daily_qty,
        MAX(so_since_stock_date) AS so_since_stock_date,
        MAX(estimated_actual_stock) AS estimated_actual_stock,
        MAX(topup_qty_allocated) AS topup_qty_allocated,
        MAX(estimated_actual_stock_adj) AS estimated_actual_stock_adj,
        MAX(SO_Bulanan) AS SO_Bulanan,
        MAX(days_of_inventory) AS days_of_inventory,
        MAX(standard_doi) AS standard_doi,
        MAX(stok_distributor) AS stok_distributor,
        MAX(buffer_plan) AS buffer_plan,
        MAX(buffer_plan_ver2) AS buffer_plan_ver2,
        MAX(last_updated_jkt) AS last_updated_jkt,
        MAX(buffer_plan_value) AS buffer_plan_value,
        MAX(buffer_plan_value_ver2) AS buffer_plan_value_ver2,
        ANY_VALUE(product_life_cycle) AS product_life_cycle,
        ANY_VALUE(Stok_Principal) AS Stok_Principal,
        ANY_VALUE(assortment) AS assortment,
        ANY_VALUE(priority_label) AS priority_label,
        ANY_VALUE(product_name) AS product_name,
        store_code,
        ANY_VALUE(distributor_g2g) AS distributor_g2g,
        ANY_VALUE(region) AS region,
        ANY_VALUE(store_name) AS store_name,
        ANY_VALUE(address) AS address,
        ANY_VALUE(status_stok_distributor) AS status_stok_distributor,
        ANY_VALUE(is_topup) AS is_topup
    FROM (
        SELECT
            store_code,
            product_code,
            product_life_cycle,
            Stok_Principal,
            assortment,
            priority_label,
            inner_pcs,
            price_for_store,
            product_name,
            actual_stock,
            stock_date,
            avg_daily_qty,
            so_since_stock_date,
            estimated_actual_stock,
            topup_qty_allocated,
            estimated_actual_stock_adj,
            SO_Bulanan,
            days_of_inventory,
            standard_doi,
            id_st,
            distributor_g2g,
            region,
            store_name,
            address,
            stok_distributor,
            status_stok_distributor,
            is_topup,
            buffer_plan,
            buffer_plan_ver2,
            last_updated_jkt,
            buffer_plan_value,
            buffer_plan_value_ver2
        FROM {inventory_table}
        UNION ALL
        SELECT
            map.store_code AS store_code,
            staging.product_code,
            CAST(NULL AS STRING) AS product_life_cycle,
            CAST(NULL AS STRING) AS Stok_Principal,
            CAST(NULL AS STRING) AS assortment,
            CAST(NULL AS STRING) AS priority_label,
            CAST(NULL AS INT64) AS inner_pcs,
            staging.price_for_store,
            staging.product_name,
            CAST(NULL AS FLOAT64) AS actual_stock,
            staging.date AS stock_date,
            CAST(NULL AS FLOAT64) AS avg_daily_qty,
            CAST(NULL AS FLOAT64) AS so_since_stock_date,
            CAST(NULL AS FLOAT64) AS estimated_actual_stock,
            CAST(NULL AS FLOAT64) AS topup_qty_allocated,
            CAST(NULL AS FLOAT64) AS estimated_actual_stock_adj,
            CAST(NULL AS INT64) AS SO_Bulanan,
            CAST(NULL AS FLOAT64) AS days_of_inventory,
            CAST(NULL AS INT64) AS standard_doi,
            staging.id_st,
            staging.distributor_g2g,
            staging.region,
            staging.store_name,
            CAST(NULL AS STRING) AS address,
            CAST(NULL AS FLOAT64) AS stok_distributor,
            CAST(NULL AS STRING) AS status_stok_distributor,
            CAST(NULL AS BOOL) AS is_topup,
            staging.buffer_plan,
            CAST(NULL AS FLOAT64) AS buffer_plan_ver2,
            CAST(NULL AS DATETIME) AS last_updated_jkt,
            staging.buffer_plan_value,
            CAST(NULL AS FLOAT64) AS buffer_plan_value_ver2
        FROM {staging_table} AS staging
        LEFT JOIN (
            SELECT id_st, store_code
            FROM {inventory_table}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY id_st
                ORDER BY IF(store_code = id_st, 0, 1), store_code
            ) = 1
        ) AS map
            ON map.id_st = staging.id_st
        WHERE staging.date >= DATE_TRUNC(CURRENT_DATE(), MONTH)
          AND staging.date < DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH)
          AND (
              UPPER(staging.region) LIKE '%KALIMANTAN%'
              OR UPPER(staging.region) LIKE '%SULAWESI%'
          )
    ) AS combined_data
    GROUP BY
        store_code,
        id_st,
        product_code
    ) AS grouped_inventory
    WHERE buffer_plan IS NOT NULL
    """

@st.cache_data(ttl=60)
def get_inventory_last_updated_jkt() -> Optional[datetime]:
    """
    Get last update timestamp from inventory buffer table
    
    Returns:
        datetime: Last update time in Jakarta timezone, or None if unavailable
    """
    try:
        client = get_bigquery_client()
        query = f"""
            SELECT MAX(last_updated_jkt) AS last_updated_jkt
            FROM `{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}`
        """
        
        df = client.query(query).to_dataframe()
        
        if df.empty or pd.isna(df.loc[0, "last_updated_jkt"]):
            logger.warning("No last_updated_jkt value found in database")
            return None
        
        ts = df.loc[0, "last_updated_jkt"]
        jkt_tz = pytz.timezone(Config.TIMEZONE)
        
        # Handle naive datetime (assume UTC)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=pytz.UTC)
        
        result = ts.astimezone(jkt_tz)
        logger.info(f"Last inventory update: {format_jkt_time(result)}")
        return result
        
    except Exception as e:
        logger.error(f"Failed to get last_updated_jkt: {str(e)}")
        return None

def execute_stored_procedure() -> bool:
    """
    Execute inventory buffer stored procedure
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info("Executing stored procedure...")
        client = get_bigquery_client()
        query = f"CALL {Config.BQ_STORED_PROC}();"
        
        job = client.query(query)
        job.result()  # Wait for completion
        
        logger.info("✓ Stored procedure executed successfully")
        return True
        
    except google_exceptions.GoogleAPIError as e:
        logger.error(f"BigQuery API error during stored procedure execution: {str(e)}")
        st.error(f"❌ Database error: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error executing stored procedure: {str(e)}")
        st.error(f"❌ Error executing stored procedure: {str(e)}")
        return False

def check_and_execute_sp() -> Optional[datetime]:
    """
    Check if stored procedure needs to run and execute if necessary
    
    Returns:
        datetime: Last update timestamp after check/execution
    """
    now_jkt = get_jkt_now()
    last_updated_jkt = get_inventory_last_updated_jkt()
    
    # First-time initialization
    if last_updated_jkt is None:
        logger.info("First-time initialization detected")
        with st.spinner("🔄 Initializing inventory data..."):
            if execute_stored_procedure():
                st.cache_data.clear()
                st.toast("Inventory initialized", icon="✅")
                return get_inventory_last_updated_jkt()
        return None
    
    # Check if refresh needed
    time_diff = now_jkt - last_updated_jkt
    should_refresh = time_diff.total_seconds() >= Config.REFRESH_INTERVAL_SECONDS
    
    if should_refresh:
        logger.info(f"Auto-refresh triggered (last update: {format_jkt_time(last_updated_jkt)})")
        with st.spinner("🔄 Updating inventory buffer (scheduled refresh)..."):
            if execute_stored_procedure():
                st.cache_data.clear()
                st.toast("Inventory refreshed", icon="✅")
                return get_inventory_last_updated_jkt()
    
    return last_updated_jkt

@st.cache_data(ttl=Config.CACHE_TTL_SECONDS)
def load_active_distributors() -> pd.DataFrame:
    """Active master distributors whose Glad2Glow region is filled in."""
    client = get_bigquery_client()
    query = f"""
    SELECT DISTINCT
        TRIM(region_g2g) AS region_g2g,
        TRIM(distributor) AS distributor
    FROM `{Config.BQ_PROJECT}.{Config.BQ_STAGING_DATASET}.{Config.BQ_MASTER_TABLE}`
    WHERE UPPER(TRIM(status)) IN ('ACTIVE', 'AKTIF')
      AND region_g2g IS NOT NULL
      AND TRIM(region_g2g) != ''
      AND distributor IS NOT NULL
      AND TRIM(distributor) != ''
    """
    df = client.query(query).to_dataframe()
    for column in ("region_g2g", "distributor"):
        df[column] = df[column].astype(str).str.strip()
    return df.drop_duplicates()

def _distributors_for_filter(selected_region: str, selected_distributor: str) -> List[str]:
    master = load_active_distributors()
    scoped = master
    if selected_region != "All":
        scoped = scoped[scoped["region_g2g"] == selected_region]
    if selected_distributor != "All":
        scoped = scoped[scoped["distributor"] == selected_distributor]
    return sorted(scoped["distributor"].dropna().unique().tolist())

@st.cache_data(ttl=Config.CACHE_TTL_SECONDS)
def load_store_summary_filtered(selected_region="All", selected_distributor="All") -> pd.DataFrame:
    empty = pd.DataFrame(columns=[
        "store_code", "store_name", "region", "distributor", "stock_date",
        "total_skus", "total_buffer_qty", "est_order_value", "priority",
    ])
    distributors = _distributors_for_filter(selected_region, selected_distributor)
    if not distributors:
        return empty

    client = get_bigquery_client()
    query = f"""
    WITH combined_data AS (
        {combined_inventory_sql()}
    )
    SELECT
        ib.store_code,
        ANY_VALUE(ib.store_name) AS store_name,
        ANY_VALUE(ib.region) AS region,
        ANY_VALUE(TRIM(ib.distributor_g2g)) AS distributor,
        MAX(ib.stock_date) AS stock_date,
        COUNT(DISTINCT CASE WHEN ib.buffer_plan IS NOT NULL THEN ib.product_code END) AS total_skus,
        COALESCE(SUM(CASE WHEN ib.buffer_plan IS NOT NULL THEN ib.buffer_plan ELSE 0 END), 0) AS total_buffer_qty,
        COALESCE(SUM(CASE WHEN ib.buffer_plan IS NOT NULL THEN ib.buffer_plan_value ELSE 0 END), 0) AS est_order_value
    FROM combined_data ib
    WHERE TRIM(ib.distributor_g2g) IN UNNEST(@distributors)
    GROUP BY ib.store_code
    HAVING total_skus > 0
    ORDER BY est_order_value DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("distributors", "STRING", distributors),
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()

    def classify_priority(val):
        if val > Config.PRIORITY_HIGH_THRESHOLD:
            return "High"
        elif val > Config.PRIORITY_MEDIUM_THRESHOLD:
            return "Medium"
        return "Low"

    df["priority"] = df["est_order_value"].apply(classify_priority)
    return df
        
    # except google_exceptions.GoogleAPIError as e:
    #     logger.error(f"BigQuery API error loading store summary: {str(e)}")
    #     raise
    # except Exception as e:
    #     logger.error(f"Failed to load store summary: {str(e)}")
    #     raise

@st.cache_data(ttl=Config.CACHE_TTL_SECONDS)
def load_inventory_buffer_data() -> pd.DataFrame:
    """
    Load inventory rows from rsa.inventory_buffer unioned with the current
    month of gt_schema.po_store_suggestion_stagging, one row per store_code + id_st + product_code.
    
    Returns:
        pd.DataFrame: Combined inventory buffer data
        
    Raises:
        Exception: If query fails
    """
    try:
        logger.info("Loading combined inventory buffer data...")
        client = get_bigquery_client()
        
        query = f"""
        SELECT *
        FROM (
            {combined_inventory_sql()}
        ) AS ib
        ORDER BY
            ib.region,
            ib.store_name,
            STRPOS('Must Have SKU, NPD, Discontinued', ib.priority_label),
            ib.SO_Bulanan DESC,
            ib.buffer_plan_value DESC
        """
        
        df = client.query(query).to_dataframe()
        
        logger.info(f"✓ Loaded {len(df)} combined inventory records")
        return df
        
    except google_exceptions.GoogleAPIError as e:
        logger.error(f"BigQuery API error loading inventory data: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Failed to load inventory data: {str(e)}")
        raise

# ============================================================================
# DOWNLOAD TRACKING
# ============================================================================

@st.cache_resource
def get_tracking_store() -> tracking.TrackingStore:
    """Process-wide tracking store (BigQuery table, local-file fallback)."""
    return tracking.TrackingStore(client_factory=get_bigquery_client)

@st.cache_data(ttl=30, show_spinner=False)
def load_tracking_data() -> Tuple[pd.DataFrame, str, Optional[str]]:
    """All tracking events as (events, source, warning); cleared by Refresh/record."""
    result = tracking.fetch_tracking_data(get_tracking_store())
    return result.events, result.source, result.warning

def record_download(download_type: str, stores: List[Dict], event_id: str) -> None:
    """
    on_click handler of every PO download button - the ONLY place a download
    is counted.

    Streamlit calls an on_click callback exactly once per click, before the
    script reruns, so building/rendering a PDF (which happens on every rerun)
    never counts. event_id is created when the button is rendered; recording
    the same id twice is a no-op in the session and in BigQuery, which makes
    a replayed click harmless. Tracking problems never block the download:
    the user already has the file, so failures are only logged and toasted.
    """
    try:
        result = tracking.record_download_event(
            get_tracking_store(),
            download_type,
            stores,
            event_id,
            seen_event_ids=st.session_state.po_recorded_event_ids,
        )
        if not result.duplicate:
            load_tracking_data.clear()
        if result.warning:
            st.toast(f"⚠️ {result.warning}", icon="⚠️")
    except Exception:
        logger.exception("Failed to record %s download %s", download_type, event_id)
        st.toast("⚠️ File terunduh, tetapi pencatatan tracking gagal.", icon="⚠️")

def _admin_credentials() -> admin_auth.AdminCredentials:
    return admin_auth.get_admin_credentials(secrets=st.secrets, environ=os.environ)

# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================

def init_session_state():
    """Initialize Streamlit session state variables"""
    defaults = {
        "prev_region": None,
        "prev_distributor": None,
        "region_select": "All",
        "distributor_select": "All",
        "store_select": [],
        "sp_checked": False,
        "po_recorded_event_ids": set(),
        "po_bulk_prepared": None,
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

init_session_state()

# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    # The admin page needs neither the inventory data nor the stored procedure,
    # so it is handled before any BigQuery inventory work starts.
    if render_page_switcher() == PAGE_ADMIN:
        render_admin_page()
        return

    # Check and execute stored procedure if needed
    if not st.session_state.sp_checked:
        last_exec_time = check_and_execute_sp()
        st.session_state.sp_checked = True
    else:
        last_exec_time = get_inventory_last_updated_jkt()

    try:
        with st.spinner("🔄 Loading data from BigQuery..."):
            # 1️⃣ Load base inventory data FIRST
            df_inventory_buffer = load_inventory_buffer_data()

    except Exception as e:
        st.error(f"❌ Error loading inventory data: {str(e)}")
        st.stop()

    # Header
    render_header(last_exec_time)

    # 2️⃣ Render filters → NOW variables exist
    selected_region, selected_distributor, selected_stores = render_filters(
        df_inventory_buffer
    )

    try:
        # 3️⃣ Load store summary using selected filters
        df_store_summary = load_store_summary_filtered(
            selected_region,
            selected_distributor
        )
    except Exception as e:
        st.error(f"❌ Error loading store summary: {str(e)}")
        st.stop()

    # 4️⃣ Apply store-level filters
    filtered_stores = apply_filters(
        df_store_summary,
        selected_region,
        selected_distributor,
        selected_stores
    )

    # Bulk download (one ZIP, one PDF per store - also for a single-store filter)
    if not filtered_stores.empty:
        render_bulk_download(filtered_stores, df_inventory_buffer, last_exec_time)

    render_active_filters(selected_distributor, selected_stores)
    render_store_list(filtered_stores, df_inventory_buffer)
    render_footer()


# ============================================================================
# UI RENDERING FUNCTIONS
# ============================================================================

def render_header(last_exec_time: Optional[datetime]):
    if last_exec_time and isinstance(last_exec_time, datetime):
        sync_time_str = last_exec_time.strftime("%I:%M %p")
        now_jkt = get_jkt_now()
        time_since_sync = now_jkt - last_exec_time
    
        seconds = int(time_since_sync.total_seconds())
        hours_since = seconds // 3600
        minutes_since = (seconds % 3600) // 60
        
        # Fixed the indentation for this block
        if hours_since > 0:
            sync_status = f"({hours_since}h {minutes_since}m ago)"
        else:
            sync_status = f"({minutes_since}m ago)"
    else:
        sync_status = "Syncing data..."

    
    st.markdown(f"""
    <div class="main-header">
        <div style="display: flex; justify-content: space-between; align-items: flex-start;">
            <div>
                <h1>{Config.APP_ICON} {Config.APP_TITLE}</h1>
                <p>Data-Driven Purchase Order Recommendations (Inventory Buffer)</p>
                <p style="font-size: 0.75rem; opacity: 0.8; margin-top: 0.5rem;">v{Config.VERSION}</p>
            </div>
            <div style="text-align: right;">
                <div class="sync-status">
                    <span class="sync-dot"></span>
                    <span>{sync_status}</span>
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

def render_filters(df_inventory_buffer: pd.DataFrame) -> Tuple[str, str, List[str]]:
    """
    Render filter controls and return selections
    
    Args:
        df_inventory_buffer: Full inventory data
        
    Returns:
        Tuple of (selected_region, selected_distributor, selected_stores)
    """
    col1, col2 = st.columns(2)
    
    # Region filter
    with col1:
        st.markdown(
            '<div class="selection-title" style="font-size:0.9rem;">📍 Region</div>',
            unsafe_allow_html=True
        )
        master = load_active_distributors()
        regions = ["All"] + sorted(master["region_g2g"].unique())
        selected_region = st.selectbox(
            "Region",
            regions,
            label_visibility="collapsed",
            key="region_select"
        )
    
    # Distributor filter
    with col2:
        st.markdown(
            '<div class="selection-title" style="font-size:0.9rem;">🏢 Distributor</div>',
            unsafe_allow_html=True
        )
        scoped = master if selected_region == "All" else master[master["region_g2g"] == selected_region]
        distributors = ["All"] + sorted(scoped["distributor"].unique())
        selected_distributor = st.selectbox(
            "Distributor",
            distributors,
            label_visibility="collapsed",
            key="distributor_select"
        )
    
    # Reset store selection when upper filters change
    if (st.session_state.prev_region != selected_region or
        st.session_state.prev_distributor != selected_distributor):
        st.session_state.store_select = []
    
    st.session_state.prev_region = selected_region
    st.session_state.prev_distributor = selected_distributor
    
    st.markdown("<hr style='margin:1rem 0; opacity:0.3;'>", unsafe_allow_html=True)
    
    # Store filter follows the master distributor list for the selected region.
    allowed_distributors = set(_distributors_for_filter(selected_region, selected_distributor))
    dist_key = df_inventory_buffer["distributor_g2g"].fillna("").astype(str).str.strip()
    df_filtered_stores = df_inventory_buffer.loc[dist_key.isin(allowed_distributors)].copy()
    
    store_options = (
    df_filtered_stores[["store_code", "store_name"]]
    .drop_duplicates()
    .assign(label=lambda x: x["store_code"] + " - " + x["store_name"])
    .sort_values("label")
    )
    
    # ✅ Filter out invalid store codes from session state
    valid_store_codes = store_options["store_code"].tolist()
    
    # Update session state directly if needed
    if st.session_state.store_select:
        st.session_state.store_select = [
            s for s in st.session_state.store_select 
            if s in valid_store_codes
        ]
    
    selected_stores = st.multiselect(
        "Store Selection",
        options=valid_store_codes,
        format_func=lambda x: store_options.set_index("store_code").loc[x, "label"],
        placeholder="Select one or more stores…",
        label_visibility="collapsed",
        key="store_select"
    )
    
    return selected_region, selected_distributor, selected_stores

    
    # Store selection header with buttons
    title_col, btn_col1, btn_col2 = st.columns([6, 1.2, 1], gap="small")
    
    with title_col:
        st.markdown(
            '<div style="padding-top: 5px;"><span style="font-size:0.95rem; font-weight:600;">🏪 Store Selection</span></div>',
            unsafe_allow_html=True
        )
    
    with btn_col1:
        if st.button("Select All", key="all_btn", use_container_width=True):
            st.session_state.store_select = store_options
            st.rerun()
    
    with btn_col2:
        if st.button("Clear", key="clr_btn", use_container_width=True):
            st.session_state.store_select = []
            st.rerun()
    
    # Store multiselect
    selected_stores = st.multiselect(
        "Store Selection",
        options=store_options,
        default=st.session_state.store_select,
        placeholder="Select one or more stores…",
        label_visibility="collapsed",
        key="store_select"
    )
    
    return selected_region, selected_distributor, selected_stores

def apply_filters(
    df_store_summary: pd.DataFrame,
    selected_region: str,
    selected_distributor: str,
    selected_stores: List[str]
) -> pd.DataFrame:
    filtered = df_store_summary.copy()
    allowed_distributors = set(_distributors_for_filter(selected_region, selected_distributor))
    distributor_key = filtered["distributor"].fillna("").astype(str).str.strip()
    filtered = filtered.loc[distributor_key.isin(allowed_distributors)]

    if selected_stores:
        filtered = filtered[filtered["store_code"].isin(selected_stores)]

    priority_order = {"High": 0, "Medium": 1, "Low": 2}
    filtered["priority_rank"] = filtered["priority"].map(priority_order)

    filtered = (
        filtered
        .sort_values(["priority_rank", "est_order_value"], ascending=[True, False])
        .drop_duplicates(subset=["store_code"])
        .drop(columns="priority_rank")
    )

    return filtered


def render_bulk_download(
    filtered_stores: pd.DataFrame,
    df_inventory_buffer: pd.DataFrame,
    last_exec_time: Optional[datetime]
):
    """
    Bulk download: ONE ZIP holding exactly ONE PDF per store of the current filter.

    Every store of the filter is accounted for - either it gets a PDF, or it is
    listed with the reason it was left out. Nothing is dropped silently.
    """
    st.markdown("---")

    date_str = jakarta_date_str()
    plan = plan_bulk_download(filtered_stores, df_inventory_buffer, date_str)
    eligible = [item for item in plan if item.will_generate]
    skipped = [item for item in plan if not item.will_generate]

    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 12px;
        text-align: center;
        margin-bottom: 1rem;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.25);
    ">
        <h2 style="color: white; margin: 0; font-size: 1.5rem;">📋 Bulk Download Order Suggestion</h2>
        <p style="color: rgba(255,255,255,0.9); margin: 0.5rem 0 0 0; font-size: 1rem;">
            Satu file ZIP berisi <b>satu PDF untuk setiap toko</b> hasil filter.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Shown BEFORE anything is generated, so the user knows what to expect.
    st.info(
        f"🏪 **{len(plan)} toko** hasil filter  →  📄 **{len(eligible)} file PDF** "
        f"akan dibuat dalam 1 file ZIP."
    )
    if skipped:
        st.warning(
            f"{len(skipped)} toko tidak akan dibuatkan PDF karena data tidak valid. "
            "Detailnya ada di bawah."
        )
        with st.expander(f"Lihat {len(skipped)} toko yang dilewati", expanded=False):
            st.dataframe(
                pd.DataFrame(
                    [
                        {"Kode Toko": i.store_code or "-", "Nama Toko": i.store_name,
                         "Alasan": i.skip_reason}
                        for i in skipped
                    ]
                ),
                hide_index=True,
                use_container_width=True,
            )

    if not eligible:
        st.error("Tidak ada toko dengan data valid untuk dibuatkan PDF.")
        st.markdown("---")
        return

    signature = (date_str, tuple(sorted(i.store_code for i in plan)), str(last_exec_time))

    if st.button(
        f"📦 Generate ZIP ({len(eligible)} PDF, 1 file per toko)",
        key="bulk_zip_btn",
        use_container_width=True,
        type="primary"
    ):
        with st.spinner(f"Sedang memproses {len(eligible)} PDF..."):
            try:
                result = build_bulk_zip(
                    plan,
                    df_inventory_buffer,
                    generate_store_pdf,
                    date_str,
                    store_rows={
                        str(row["store_code"]): row
                        for row in filtered_stores.to_dict("records")
                    },
                )
                st.session_state.po_bulk_prepared = {
                    "signature": signature,
                    "result": result,
                }
                logger.info(
                    f"Bulk ZIP generated: {result.zip_name} "
                    f"({result.file_count} PDFs, {len(result.excluded)} excluded)"
                )
            except Exception as e:
                st.session_state.po_bulk_prepared = None
                st.error(f"❌ Failed to generate bulk ZIP: {str(e)}")
                logger.error(f"Bulk ZIP generation failed: {str(e)}")

    prepared = st.session_state.po_bulk_prepared
    if prepared and prepared["signature"] != signature:
        # Filter or data changed since the ZIP was built: do not offer a stale file.
        st.session_state.po_bulk_prepared = prepared = None

    if prepared:
        result = prepared["result"]
        if result.excluded:
            st.warning(
                f"ZIP berisi {result.file_count} dari {len(plan)} toko. "
                f"{len(result.excluded)} toko dilewati:"
            )
            st.dataframe(
                pd.DataFrame(
                    [
                        {"Kode Toko": i.store_code or "-", "Nama Toko": i.store_name,
                         "Alasan": i.skip_reason}
                        for i in result.excluded
                    ]
                ),
                hide_index=True,
                use_container_width=True,
            )
        if result.zip_bytes is None:
            st.error("Tidak ada PDF yang berhasil dibuat.")
        else:
            st.success(f"ZIP siap: {result.file_count} file PDF - {result.zip_name}")
            # Counted on click only (see record_download), never on generation.
            st.download_button(
                label=f"📥 Klik di Sini untuk Mengunduh ZIP ({result.file_count} PDF)",
                data=result.zip_bytes,
                file_name=result.zip_name,
                mime="application/zip",
                key="dl_bulk_zip",
                use_container_width=True,
                on_click=record_download,
                args=(
                    tracking.DOWNLOAD_BULK,
                    [item.as_store_record() for item in result.included],
                    uuid.uuid4().hex,
                ),
            )

    st.markdown("---")

def render_active_filters(selected_distributor: str, selected_stores: List[str]):
    """Render active filters display"""
    active_filters = []
    
    if selected_distributor != "All":
        active_filters.append(f"🏪 {selected_distributor}")
    
    if selected_stores:
        active_filters.append(f"🎯 {len(selected_stores)} specific store(s)")
    
    if active_filters:
        st.markdown(f"""
        <div style="background: #f8fafc; padding: 0.75rem 1rem; border-radius: 8px; 
                    margin-bottom: 1rem; border-left: 4px solid #667eea;">
            <span style="color: #64748b; font-size: 0.85rem; font-weight: 600;">Active Filters: </span>
            <span style="color: #334155; font-size: 0.85rem;">{' • '.join(active_filters)}</span>
        </div>
        """, unsafe_allow_html=True)

def render_store_list(filtered_stores: pd.DataFrame, df_inventory_buffer: pd.DataFrame):
    """Render list of stores with download buttons"""
    st.markdown(f"### Store List ({len(filtered_stores)} stores)")
    st.markdown(
        '<p style="color: #64748b; font-size: 0.85rem; margin-top: -1rem;">'
        'PO suggestions prioritized by sales velocity - from most sold to less sold items</p>',
        unsafe_allow_html=True
    )
    
    if len(filtered_stores) == 0:
        st.markdown("""
        <div style="text-align: center; padding: 4rem 2rem; color: #94a3b8;">
            <div style="font-size: 4rem; margin-bottom: 1rem;">🔍</div>
            <h3 style="color: #64748b;">No stores found</h3>
            <p>No stores match your current filters.</p>
            <p style="font-size: 0.85rem; margin-top: 1rem;">
                Try adjusting your filters or selecting "All" in some dropdowns
            </p>
        </div>
        """, unsafe_allow_html=True)
        return
    
    # Same naming (and same duplicate-name handling) as the files inside the bulk ZIP.
    date_str = jakarta_date_str()
    file_names = build_unique_filenames(filtered_stores.to_dict("records"), date_str)

    for _, row in filtered_stores.iterrows():
        unique_key = row["store_code"]

        # ✅ FIX: Get stock_date_str for THIS store
        stock_date_val = row.get('stock_date')
        stock_date_str = (
            stock_date_val.strftime('%d %b %Y')
            if isinstance(stock_date_val, (datetime, pd.Timestamp))
            else str(stock_date_val) if stock_date_val else '-'
        )
        
        # Store header
        st.markdown(f"""
        <div style="display: flex; align-items: center; gap: 10px; margin-top: 1.5rem; margin-bottom: 0.5rem;">
            <span style="font-size: 1.15rem; font-weight: 700; color: #1e293b;">{row['store_name']}</span>
            <span style="background: #f1f5f9; color: #64748b; padding: 2px 10px; 
                         border-radius: 12px; font-size: 0.75rem; font-weight: 600;">
                {row['store_code']}
            </span>
        </div>
        """, unsafe_allow_html=True)
        
        # Metrics and download button
        m1, m2, m3, m4, btn_download = st.columns([2, 2.5, 2, 2.5, 1.5])
        
        with m1:
            st.markdown(
                f'<div class="metric-group">'
                f'<span class="metric-label">Branch</span>'
                f'<span class="metric-value">{row["region"]}</span></div>',
                unsafe_allow_html=True
            )
        
        with m2:
            st.markdown(
                f"""
                <div class="metric-group">
                    <span class="metric-label">🗓️ Last Stock Update</span>
                    <span class="metric-value">{stock_date_str}</span>
                </div>
                """,
                unsafe_allow_html=True
            )

        
        with m3:
            st.markdown(
                f'<div class="metric-group">'
                f'<span class="metric-label">Total SKUs</span>'
                f'<span class="metric-value">{int(row["total_skus"])} products</span></div>',
                unsafe_allow_html=True
            )
        
        with m4:
            order_value = f"Rp {row['est_order_value']/1000000:.1f}M"
            st.markdown(
                f'<div class="metric-group">'
                f'<span class="metric-label">Est. Order Value</span>'
                f'<span class="metric-value value-blue">{order_value}</span></div>',
                unsafe_allow_html=True
            )
        
        # Download button
        with btn_download:
            st.markdown('<div style="padding-top: 0.5rem;"></div>', unsafe_allow_html=True)
            
            try:
                store_detail = get_store_detail(df_inventory_buffer, row['store_code'])
                store_info = build_store_info(row)

                pdf_bytes = generate_store_pdf(store_info, store_detail)
                file_name = (
                    file_names.get(str(row['store_code']).strip())
                    or build_pdf_filename(row['store_name'], date_str)
                )

                # Counted on click only (see record_download), never on generation.
                st.download_button(
                    label="📄 Download PDF",
                    data=pdf_bytes,
                    file_name=file_name,
                    mime="application/pdf",
                    key=f"dl_pdf_{unique_key}",
                    use_container_width=True,
                    on_click=record_download,
                    args=(
                        tracking.DOWNLOAD_INDIVIDUAL,
                        [{
                            'store_code': row['store_code'],
                            'store_name': row['store_name'],
                            'region': row['region'],
                            'distributor': row['distributor'],
                            'file_name': file_name,
                        }],
                        uuid.uuid4().hex,
                    ),
                )

            except Exception as e:
                st.error(f"Error generating PDF: {str(e)}")
                logger.error(f"PDF generation failed for {unique_key}: {str(e)}")
        
        st.markdown('<hr style="margin: 1rem 0; border: none; border-top: 1px solid #f1f5f9;">', 
                   unsafe_allow_html=True)

def render_footer():
    """Render application footer"""
    st.markdown("---")
    st.markdown(f"""
    <div style="text-align: center; color: #94a3b8; font-size: 0.85rem; padding: 1rem;">
        💡 PO suggestions are automatically calculated based on 2-month sales data, DOI standards, and stock levels<br>
        <span style="font-size: 0.75rem; opacity: 0.7;">Version {Config.VERSION} | {Config.BRAND_NAME}</span>
    </div>
    """, unsafe_allow_html=True)

# ============================================================================
# ADMIN: LOGIN + DOWNLOAD TRACKING DASHBOARD
# ============================================================================

PAGE_PO = "📦 PO Suggestion"
PAGE_ADMIN = "🔐 Admin Tracking"


def render_page_switcher() -> str:
    """Top navigation between the PO tool and the admin tracking page."""
    nav_col, logout_col = st.columns([5, 1])
    with nav_col:
        page = st.radio(
            "Halaman",
            [PAGE_PO, PAGE_ADMIN],
            horizontal=True,
            key="page_view",
            label_visibility="collapsed",
        )
    with logout_col:
        if admin_auth.is_admin_authenticated(st.session_state):
            if st.button("🚪 Logout", key="admin_logout_nav_btn", use_container_width=True):
                admin_auth.logout_admin(st.session_state)
                st.rerun()
    return page


def render_admin_login() -> None:
    """Username/password form. The tracking page is not rendered before success."""
    st.markdown("### 🔐 Login Admin")
    st.caption("Halaman tracking download hanya dapat dibuka oleh admin.")

    with st.form("admin_login_form", clear_on_submit=True):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login", use_container_width=True)

    if submitted:
        result = admin_auth.attempt_admin_login(
            st.session_state, username, password, _admin_credentials()
        )
        if result.ok:
            st.rerun()
        else:
            st.error(result.message)


def render_admin_page() -> None:
    """Gate: login form until authenticated, then the dashboard."""
    if not admin_auth.is_admin_authenticated(st.session_state):
        render_admin_login()
        return
    render_admin_dashboard()


def _format_jkt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True).dt.tz_convert(
        pytz.timezone(Config.TIMEZONE)
    ).dt.strftime("%Y-%m-%d %H:%M:%S")


def render_admin_dashboard() -> None:
    """Download-tracking dashboard. Refuses to render without an admin session."""
    if not admin_auth.is_admin_authenticated(st.session_state):
        st.error("Akses ditolak. Silakan login sebagai admin.")
        return

    st.markdown("## 📊 Tracking Download PO")
    user = st.session_state.get(admin_auth.SESSION_USER_KEY, "admin")
    st.caption(f"Login sebagai **{user}** · waktu ditampilkan dalam {Config.TIMEZONE}")

    if _admin_credentials().is_default:
        st.warning(
            "Kredensial admin default masih dipakai. Repository ini publik, jadi "
            "atur `[po_buffer_admin]` di Streamlit secrets atau variabel environment "
            "`PO_BUFFER_ADMIN_USERNAME` / `PO_BUFFER_ADMIN_PASSWORD_HASH`."
        )

    if st.button("🔄 Refresh Data", key="admin_refresh_btn"):
        load_tracking_data.clear()
        st.rerun()

    events, source, warning = load_tracking_data()
    if warning:
        st.warning(warning)

    if events.empty:
        st.info(
            "Belum ada download yang tercatat. Hitungan bertambah hanya saat tombol "
            "download ditekan, bukan saat PDF dibuat."
        )
        return

    # --- Filters -----------------------------------------------------------
    f1, f2 = st.columns([1, 2])
    with f1:
        all_dates = st.checkbox("Semua tanggal", value=True, key="admin_all_dates")
        start_date = end_date = None
        if not all_dates:
            today = get_jkt_now().date()
            picked = st.date_input(
                "Rentang tanggal",
                value=(today - timedelta(days=30), today),
                key="admin_date_range",
            )
            if isinstance(picked, (tuple, list)):
                if len(picked) == 2:
                    start_date, end_date = picked
                elif len(picked) == 1:
                    start_date = end_date = picked[0]
            else:
                start_date = end_date = picked
    with f2:
        store_labels = (
            events[["store_code", "store_name"]]
            .drop_duplicates("store_code")
            .assign(label=lambda d: d["store_code"] + " - " + d["store_name"].fillna(""))
            .set_index("store_code")["label"]
            .sort_values()
        )
        store_codes = st.multiselect(
            "Toko",
            options=store_labels.index.tolist(),
            format_func=lambda code: store_labels[code],
            key="admin_store_filter",
        )
    f3, f4 = st.columns(2)
    with f3:
        regions = st.multiselect(
            "Region",
            sorted(events["region"].dropna().unique()),
            key="admin_region_filter",
        )
    with f4:
        distributors = st.multiselect(
            "Distributor",
            sorted(events["distributor"].dropna().unique()),
            key="admin_distributor_filter",
        )

    filtered = tracking.filter_events(
        events,
        start_date=start_date,
        end_date=end_date,
        store_codes=store_codes,
        regions=regions,
        distributors=distributors,
    )

    # --- Totals ------------------------------------------------------------
    totals = tracking.summarize_totals(filtered)
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Download", f"{totals['total_downloads']:,}")
    m2.metric("Individual", f"{totals['individual_downloads']:,}")
    m3.metric("Via Bulk (per toko)", f"{totals['bulk_downloads']:,}")
    m4.metric("Aksi Bulk", f"{totals['bulk_actions']:,}")
    m5.metric("Toko Diunduh", f"{totals['stores_downloaded']:,}")
    st.caption(
        "Total download dihitung per toko: bulk ZIP berisi 5 toko menambah 5, "
        "masing-masing +1."
    )

    if filtered.empty:
        st.info("Tidak ada data untuk filter yang dipilih.")
        return

    # --- Per-store table ---------------------------------------------------
    st.markdown("### Download per Toko")
    per_store = tracking.aggregate_by_store(filtered)
    shown = per_store.assign(last_download_at=_format_jkt(per_store["last_download_at"]))
    st.dataframe(
        shown.rename(columns={
            "store_code": "Store Code",
            "store_name": "Nama Toko",
            "region": "Region",
            "distributor": "Distributor",
            "individual_downloads": "Individual",
            "bulk_downloads": "Bulk",
            "total_downloads": "Total",
            "last_download_at": "Download Terakhir (WIB)",
        }),
        hide_index=True,
        use_container_width=True,
    )

    stamp = jakarta_date_str()
    e1, e2 = st.columns(2)
    with e1:
        st.download_button(
            "⬇️ Export ringkasan per toko (CSV)",
            data=tracking.to_csv_bytes(per_store),
            file_name=f"PO_Tracking_Per_Toko_{stamp}.csv",
            mime="text/csv",
            key="admin_export_summary",
            on_click="ignore",
            use_container_width=True,
        )
    with e2:
        st.download_button(
            "⬇️ Export log download (CSV)",
            data=tracking.to_csv_bytes(filtered),
            file_name=f"PO_Tracking_Log_{stamp}.csv",
            mime="text/csv",
            key="admin_export_events",
            on_click="ignore",
            use_container_width=True,
        )

    # --- Bulk membership + raw log ----------------------------------------
    bulk = tracking.bulk_event_details(filtered)
    with st.expander(f"Detail aksi bulk ({len(bulk)})"):
        if bulk.empty:
            st.caption("Belum ada bulk download pada filter ini.")
        else:
            st.dataframe(
                bulk.assign(downloaded_at=_format_jkt(bulk["downloaded_at"])).rename(columns={
                    "event_id": "Event ID",
                    "downloaded_at": "Waktu (WIB)",
                    "store_count": "Jumlah Toko",
                    "stores": "Toko dalam ZIP",
                    "files": "File",
                }),
                hide_index=True,
                use_container_width=True,
            )

    with st.expander("Log download terbaru (200 baris)"):
        log = filtered.sort_values("downloaded_at", ascending=False).head(200)
        st.dataframe(
            log.assign(downloaded_at=_format_jkt(log["downloaded_at"])),
            hide_index=True,
            use_container_width=True,
        )

# ============================================================================
# APPLICATION ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    try:
        logger.info(f"Starting {Config.APP_TITLE} v{Config.VERSION}")
        main()
        logger.info("Application rendered successfully")
    except Exception as e:
        logger.critical(f"Critical application error: {str(e)}", exc_info=True)
        #st.error(f"❌ Critical Error: {str(e)}")
        #st.info("Please contact support if this issue persists.")
