"""
PO Suggestion Tool - Production Release
========================================
Version: 2.0.0
Last Updated: 2024-02-06

A production-ready Streamlit application for generating Purchase Order suggestions
based on inventory buffer analysis from BigQuery.

Features:
- Automated inventory refresh (2-hour intervals)
- Multi-store PDF generation
- Advanced filtering and selection
- Real-time data synchronization
- Comprehensive error handling and logging
"""

import streamlit as st
import pandas as pd
import pytz
import logging
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from google.oauth2 import service_account
from google.cloud import bigquery
from google.api_core import exceptions as google_exceptions
import io
from reportlab.lib.pagesizes import A4, portrait
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle, PageBreak
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Application configuration constants"""
    APP_TITLE = "PO Suggestion Tool"
    APP_ICON = "📦"
    VERSION = "2.0.0"
    BRAND_NAME = "Glad2Glow"
    
    # BigQuery Configuration
    BQ_PROJECT = "skintific-data-warehouse"
    BQ_DATASET = "rsa"
    BQ_TABLE = "inventory_buffer"
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
    
    credentials = service_account.Credentials.from_service_account_info(gcp_secrets)
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
    
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
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
    
    # ✅ FILTER FIRST - Only products with buffer_plan > 0
    sku_data_filtered = sku_data[
        (sku_data['buffer_plan_ver2'].notna()) & 
        (sku_data['buffer_plan_ver2'] > 0)
    ].copy()
    
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
        fontSize=22,
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
        [f"Toko: {store_data['store_name']}", f"Kode: {store_data['store_code']}"],
        [f"Region: {store_data.get('region', '-')}", f"Distributor: {store_data.get('distributor_g2g', '-')}"],
        [f"Tgl: {datetime.now().strftime('%d/%b/%Y')}", "Sales: _________________"]
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
    total_out_of_stock = len(sku_data_filtered[sku_data_filtered['actual_stock'] == 0])
    total_suggested = int(sku_data_filtered['buffer_plan_ver2'].sum())
    total_value = sku_data_filtered['buffer_plan_value_ver2'].sum()
    
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
        ('FONTSIZE', (0, 1), (-1, 1), 14),
        ('GRID', (0, 0), (-1, -1), 1, colors.white),
    ]))
    
    elements.append(summary_table)
    elements.append(Spacer(1, 15))
    
    # Product Table
    table_data = [
        ["SKU / Produk", "Stok\nToko", "SO\nBulanan",
         "Dist.\nStok", "Saran\n(Qty)", "Nilai Order\n(Rp)", "Order\nAktual"]
    ]
    
    dist_stock_map = {
        "Stok Tersedia": "Ada",
        "Stok Menipis": "Menipis"
    }
    
    # ✅ Use filtered data for stock_date too
    stock_date_val = sku_data_filtered['stock_date'].iloc[0] if not sku_data_filtered.empty else "-"
    stock_date_str = (
        stock_date_val.strftime('%d %b %Y')
        if isinstance(stock_date_val, datetime)
        else str(stock_date_val)
    )
    
    # ✅ Loop through filtered data - NO MORE if buffer_qty == 0 CHECK NEEDED
    for _, row in sku_data_filtered.iterrows():
        buffer_qty = int(row['buffer_plan_ver2'])  # Already filtered, must be > 0
        actual_stock = int(row['actual_stock']) if not pd.isna(row['actual_stock']) else 0
        buffer_val = int(row['buffer_plan_value_ver2']) if not pd.isna(row['buffer_plan_value_ver2']) else 0
        
        prod_style = ParagraphStyle('ProdStyle', parent=cell_style, alignment=TA_LEFT)
        merged_product = Paragraph(
            f"<b>{row['product_code']}</b><br/>{row['product_name']}",
            prod_style
        )
        
        raw_dist_stock = str(row.get('status_stok_distributor', '-'))
        dist_stock_display = dist_stock_map.get(raw_dist_stock, raw_dist_stock)
        
        value_style = ParagraphStyle('ValStyle', parent=cell_style, alignment=TA_RIGHT)
        
        table_data.append([
            merged_product,
            str(actual_stock),
            str(row.get('SO_Bulanan', '-')),
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
        f"<i>* System Generated: {datetime.now().strftime('%d %b %Y, %H:%M')}</i>",
        styles['Italic']
    ))
    
    return elements

def generate_multi_store_pdf(
    stores_data: pd.DataFrame,
    df_inventory_buffer: pd.DataFrame,
    brand_name: str = Config.BRAND_NAME
) -> io.BytesIO:
    """
    Generate combined PDF for multiple stores
    
    Args:
        stores_data: DataFrame with store summary data
        df_inventory_buffer: DataFrame with inventory details
        brand_name: Brand name for PDF header
        
    Returns:
        BytesIO: Combined PDF file buffer
    """
    try:
        logger.info(f"Generating multi-store PDF for {len(stores_data)} stores")
        
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=portrait(A4),
            rightMargin=25, leftMargin=25, topMargin=25, bottomMargin=25
        )
        elements = []
        styles = getSampleStyleSheet()
        
        stores_included = 0
        for idx, (_, store_row) in enumerate(stores_data.iterrows()):
            store_detail = df_inventory_buffer[
                df_inventory_buffer['store_code'] == store_row['store_code']
            ].copy()
            
            if store_detail['buffer_plan_ver2'].sum() == 0:
                continue
            
            store_info = {
                'store_code': store_row['store_code'],
                'store_name': store_row['store_name'],
                'distributor_g2g': store_row.get('distributor', '-'),
                'region': store_row.get('region', '-'),
            }
            
            store_elements = _generate_store_page_elements(store_info, store_detail, styles, brand_name)
            elements.extend(store_elements)
            
            if stores_included < len(stores_data) - 1:
                elements.append(PageBreak())
            
            stores_included += 1
        
        doc.build(elements)
        buffer.seek(0)
        
        logger.info(f"✓ Multi-store PDF generated successfully ({stores_included} stores)")
        return buffer
        
    except Exception as e:
        logger.error(f"Failed to generate multi-store PDF: {str(e)}")
        raise

# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

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
def load_store_summary_filtered(selected_region="All", selected_distributor="All") -> pd.DataFrame:
    client = get_bigquery_client()

    filters = []
    if selected_region != "All":
        filters.append(f"ib.region = '{selected_region}'")
    if selected_distributor != "All":
        filters.append(f"ib.distributor_g2g = '{selected_distributor}'")

    where_sql = ""
    if filters:
        where_sql = "AND " + " AND ".join(filters)

    query = f"""
    WITH latest_stock AS (
        SELECT
            store_code,
            MAX(stock_date) AS latest_stock_date
        FROM `{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}`
        GROUP BY store_code
    ),
    filtered_data AS (
        -- ✅ First, get only the rows with the latest stock_date per store
        SELECT
            ib.store_code,
            ib.store_name,
            ib.region,
            ib.distributor_g2g,
            ib.product_code,
            ib.buffer_plan_ver2,
            ib.buffer_plan_value_ver2,
            ls.latest_stock_date AS stock_date
        FROM `{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}` ib
        INNER JOIN latest_stock ls
            ON ib.store_code = ls.store_code
            AND ib.stock_date = ls.latest_stock_date
        WHERE
        {where_sql}
    )
    -- ✅ Then, aggregate by store_code only
    SELECT
        store_code,
        ANY_VALUE(store_name) AS store_name,
        ANY_VALUE(region) AS region,
        ANY_VALUE(distributor_g2g) AS distributor,
        ANY_VALUE(stock_date) AS stock_date,
        COUNT(DISTINCT product_code) AS total_skus,
        SUM(buffer_plan_ver2) AS total_buffer_qty,
        SUM(buffer_plan_value_ver2) AS est_order_value
    FROM filtered_data
    GROUP BY store_code  -- ✅ Only group by store_code, not stock_date
    HAVING total_buffer_qty > 0
    ORDER BY est_order_value DESC
    """

    df = client.query(query).to_dataframe()

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
    Load full inventory buffer data from BigQuery
    
    Returns:
        pd.DataFrame: Complete inventory buffer data
        
    Raises:
        Exception: If query fails
    """
    try:
        logger.info("Loading inventory buffer data...")
        client = get_bigquery_client()
        
        query = f"""
        SELECT 
            store_code, product_code, product_life_cycle, assortment,
            inner_pcs, price_for_store, product_name, actual_stock,
            stock_date, avg_daily_qty, days_of_inventory, standard_doi,
            id_st, distributor_g2g, region, store_name, address,
            stok_distributor, status_stok_distributor, buffer_plan,
            buffer_plan_ver2, buffer_plan_value, buffer_plan_value_ver2,
            SO_Bulanan
        FROM `{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}`
        ORDER BY region, store_name, SO_Bulanan, buffer_plan_value_ver2 DESC
        """
        
        df = client.query(query).to_dataframe()
        
        logger.info(f"✓ Loaded {len(df)} inventory records")
        return df
        
    except google_exceptions.GoogleAPIError as e:
        logger.error(f"BigQuery API error loading inventory data: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Failed to load inventory data: {str(e)}")
        raise

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
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

init_session_state()

# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
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

    # Bulk download
    if len(filtered_stores) > 1:
        render_bulk_download(filtered_stores, df_inventory_buffer, last_exec_time)

    render_active_filters(selected_distributor, selected_stores)
    render_store_list(filtered_stores, df_inventory_buffer)
    render_footer()


# ============================================================================
# UI RENDERING FUNCTIONS
# ============================================================================

def render_header(last_exec_time: Optional[datetime]):
    """Render application header with sync status"""
    if last_exec_time:
        sync_time_str = last_exec_time.strftime("%I:%M %p")
        now_jkt = get_jkt_now()
        time_since_sync = now_jkt - last_exec_time
        
        seconds = int(time_since_sync.total_seconds())
        hours_since = seconds // 3600
        minutes_since = (seconds % 3600) // 60
        
        if hours_since > 0:
            sync_status = f"Last Sync: {sync_time_str} WIB ({hours_since}h {minutes_since}m ago)"
        else:
            sync_status = f"Last Sync: {sync_time_str} WIB ({minutes_since}m ago)"
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
        regions = ["All"] + sorted(df_inventory_buffer["region"].dropna().unique())
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
        df_filtered = df_inventory_buffer.copy()
        if selected_region != "All":
            df_filtered = df_filtered[df_filtered["region"] == selected_region]
        
        distributors = ["All"] + sorted(df_filtered["distributor_g2g"].dropna().unique())
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
    
    # Store filter
    df_filtered_stores = df_filtered.copy()
    if selected_distributor != "All":
        df_filtered_stores = df_filtered_stores[
            df_filtered_stores["distributor_g2g"] == selected_distributor
        ]
    
    store_options = (
    df_filtered_stores[["store_code", "store_name"]]
    .drop_duplicates()
    .assign(label=lambda x: x["store_code"] + " - " + x["store_name"])
    .sort_values("label")
    )
    
    # ✅ Filter out invalid store codes from session state
    valid_store_codes = store_options["store_code"].tolist()
    default_stores = [
        s for s in st.session_state.store_select 
        if s in valid_store_codes
    ]
    
    selected_stores = st.multiselect(
        "Store Selection",
        options=valid_store_codes,
        format_func=lambda x: store_options.set_index("store_code").loc[x, "label"],
        default=default_stores,  # ✅ Now using validated defaults
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

    if selected_region != "All":
        filtered = filtered[filtered["region"] == selected_region]

    if selected_distributor != "All":
        filtered = filtered[filtered["distributor"] == selected_distributor]

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
    """Render bulk download section for multiple stores"""
    st.markdown("---")
    
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
            Otomatis membuat satu dokumen PDF berisi <b>{len(filtered_stores)} toko</b> yang terpilih.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    if st.button(
        "📄 Generate Combined PDF for All Selected Stores",
        key="multi_pdf_btn",
        use_container_width=True,
        type="primary"
    ):
        with st.spinner(f"Sedang memproses PDF untuk {len(filtered_stores)} toko..."):
            try:
                pdf_buffer = generate_multi_store_pdf(
                    filtered_stores,
                    df_inventory_buffer,
                    brand_name=Config.BRAND_NAME
                )
                
                timestamp = format_jkt_time(last_exec_time or get_jkt_now(), "%Y-%m-%d_%H%M_WIB")
                filename = f"PO_Bulk_{len(filtered_stores)}_Stores_{timestamp}.pdf"
                
                st.download_button(
                    label=f"📥 Klik di Sini untuk Mengunduh PDF ({len(filtered_stores)} Toko)",
                    data=pdf_buffer,
                    file_name=filename,
                    mime="application/pdf",
                    key="dl_multi_pdf",
                    use_container_width=True
                )
                
                logger.info(f"Bulk PDF generated: {filename}")
                
            except Exception as e:
                st.error(f"❌ Failed to generate bulk PDF: {str(e)}")
                logger.error(f"Bulk PDF generation failed: {str(e)}")
    
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
    
    # filtered_stores = (
    #     filtered_stores
    #     .sort_values("est_order_value", ascending=False)
    #     .drop_duplicates(subset=["store_code"], keep="first")
    # )

    for _, row in filtered_stores.iterrows():
        unique_key = row["store_code"]

        
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
                f'<div class="metric-group">'
                f'<span class="metric-label">🗓️ Last Stock Update</span>'
                f'<span class="metric-value">{row["stock_date"]}</span></div>',
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
                store_detail = df_inventory_buffer[
                    df_inventory_buffer['store_code'] == row['store_code']
                ].copy()
                
                store_info = {
                    'store_code': row['store_code'],
                    'store_name': row['store_name'],
                    'distributor_g2g': row['distributor'],
                    'region': row['region'],
                    'address': '-'
                }
                
                pdf_buffer = generate_po_pdf(store_info, store_detail, brand_name=Config.BRAND_NAME)
                
                st.download_button(
                    label="📄 Download PDF",
                    data=pdf_buffer,
                    file_name=f"PO_{row['store_code']}_{datetime.now().strftime('%Y%m%d')}.pdf",
                    mime="application/pdf",
                    key=f"dl_pdf_{unique_key}",
                    use_container_width=True
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
        📊 Data source: <code>{Config.BQ_PROJECT}.{Config.BQ_DATASET}.{Config.BQ_TABLE}</code><br>
        <span style="font-size: 0.75rem; opacity: 0.7;">Version {Config.VERSION} | {Config.BRAND_NAME}</span>
    </div>
    """, unsafe_allow_html=True)

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
        st.error(f"❌ Critical Error: {str(e)}")
        st.info("Please contact support if this issue persists.")
