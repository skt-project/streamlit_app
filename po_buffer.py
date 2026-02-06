import streamlit as st
import pandas as pd
import pytz
from datetime import datetime, timedelta
from google.oauth2 import service_account
from google.cloud import bigquery
import io
from reportlab.lib.pagesizes import A4, portrait, landscape
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle, PageBreak, KeepTogether
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

# Page config
st.set_page_config(
    page_title="PO Suggestion Tool",
    page_icon="📦",
    layout="wide"
)

# Custom CSS for styling
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
    
    /* Horizontal Store Card Layout */
    .store-card-horizontal {
        background: #0b1220;
        padding: 1rem 1.5rem;
        border-bottom: 1px solid #1e293b;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }

    .store-main-info {
        flex: 2;
    }

    .store-metrics {
        flex: 3;
        display: flex;
        justify-content: space-around;
        align-items: center;
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
            
   /* =========================
   Store Card – Dark Theme
   ========================= */

.store-card {
    background: #0f172a; /* deep slate */
    border-radius: 14px;
    padding: 1.6rem;
    margin-bottom: 1.2rem;
    border: 1px solid #1e293b;
    box-shadow: 0 0 0 1px rgba(255,255,255,0.02);
    transition: all 0.25s ease;
}


/* =========================
   Header
   ========================= */
.store-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 1.1rem;
}

.store-name {
    font-size: 1.45rem;  
    font-weight: 900;     
    color: #ffffff;     
    margin-bottom: 0.35rem;
    letter-spacing: 0.2px;
}


.store-code {
    background: #1e293b;
    color: #f8fafc;            /* brighter */
    padding: 0.35rem 1rem;
    border-radius: 999px;
    font-size: 0.85rem;        /* ⬆ bigger */
    font-weight: 800;
}

/* =========================
   Store Info Grid
   ========================= */
.store-info {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1.2rem;
    margin-top: 1.2rem;
    padding-top: 1.2rem;
    border-top: 1px solid #1e293b;
}

.info-item {
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
}

/* Labels = soft but readable */
.info-label {
    font-size: 1.2rem;         /* ⬆ bigger */
    color: #cbd5f5;            /* brighter muted */
    text-transform: uppercase;
    font-weight: 800;
    letter-spacing: 0.1em;
}

/* Values = high contrast */
.info-value {
    font-size: 1.2rem;        /* ⬆ noticeably bigger */
    color: #ffffff;            /* max contrast */
    font-weight: 700;
    line-height: 1.4;
}

/* Highlighted values */
.value-highlight {
    color: #f472b6;            /* brighter pink */
    font-size: 1.2rem;         /* ⬆ bigger */
    font-weight: 900;
}

.store-card {
    background: #0b1220;       /* slightly darker for contrast */
    border-radius: 16px;
    padding: 1.8rem;           /* ⬆ more breathing room */
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
    
    /* Priority badge styling */
    .priority-high {
        background: #ef4444;
        color: white;
        padding: 0.25rem 0.75rem;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    
    .priority-urgent {
        background: #dc2626;
        color: white;
        padding: 0.25rem 0.75rem;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    
    .priority-standard {
        background: #f59e0b;
        color: white;
        padding: 0.25rem 0.75rem;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    
    .priority-low {
        background: #64748b;
        color: white;
        padding: 0.25rem 0.75rem;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    
    .subtitle {
        color: #64748b;
        font-size: 0.9rem;
        margin-top: -0.5rem;
        margin-bottom: 1.5rem;
    }
    
</style>


""", unsafe_allow_html=True)

# ---------------------------
# PDF Generation Functions (Portrait & Merged Columns)
# ---------------------------
def generate_po_pdf(store_info, store_detail, brand_name="Glad2Glow"):
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

    elements = generate_store_page_elements(
        store_data=store_info,
        sku_data=store_detail,
        styles=styles,
        brand_name=brand_name
    )

    doc.build(elements)
    buffer.seek(0)
    return buffer

def generate_store_page_elements(store_data, sku_data, styles, brand_name="Glad2Glow"):
    elements = []

    # --- Styles ---
    table_font_size = 8.5

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

    # --- Header ---
    elements.append(Paragraph(brand_name, title_style))
    elements.append(Paragraph("<b>📋 FORMULIR PEMBELIAN (PO)</b>", styles['Heading2']))
    elements.append(Spacer(1, 8))

    # --- Store Information ---
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

    # --- Summary ---
    total_out_of_stock = len(sku_data[sku_data['actual_stock'] == 0])
    total_suggested = int(sku_data['buffer_plan_ver2'].sum())
    total_value = sku_data['buffer_plan_value_ver2'].sum()

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

    # --- Product Table ---
    table_data = [
        ["SKU / Produk", "Stok\nToko", "SO\nBulanan",
         "Dist.\nStok", "Saran\n(Qty)", "Nilai Order\n(Rp)", "Order\nAktual"]
    ]

    dist_stock_map = {
        "Stok Tersedia": "Ada",
        "Stok Menipis": "Menipis"
    }

    stock_date_val = sku_data['stock_date'].iloc[0] if not sku_data.empty else "-"
    stock_date_str = (
        stock_date_val.strftime('%d %b %Y')
        if isinstance(stock_date_val, datetime)
        else str(stock_date_val)
    )

    for _, row in sku_data.iterrows():
        buffer_qty = int(row['buffer_plan_ver2']) if not pd.isna(row['buffer_plan_ver2']) else 0
        if buffer_qty == 0:
            continue

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
            "____"   # Order Aktual
        ])

    # --- Column widths (Order Aktual tightened) ---
    col_widths = [
        2.30 * inch,  # SKU / Produk
        0.50 * inch,  # Stok Toko
        0.60 * inch,  # SO Bulanan
        0.75 * inch,  # Dist. Stok
        0.65 * inch,  # Saran Qty
        1.20 * inch,  # Nilai Order
        0.95 * inch   # Order Aktual (fits 4 digits neatly)
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

        # Extra writing comfort for Order Aktual only
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



def generate_multi_store_pdf(stores_data, df_inventory_buffer, brand_name="Glad2Glow"):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=portrait(A4),
        rightMargin=25, leftMargin=25, topMargin=25, bottomMargin=25
    )
    elements = []
    styles = getSampleStyleSheet()
    
    for idx, (_, store_row) in enumerate(stores_data.iterrows()):
        store_detail = df_inventory_buffer[df_inventory_buffer['store_code'] == store_row['store_code']].copy()
        
        # Skip this store if total buffer_plan_ver2 is 0
        if store_detail['buffer_plan_ver2'].sum() == 0:
            continue
            
        store_info = {
            'store_code': store_row['store_code'],
            'store_name': store_row['store_name'],
            'distributor_g2g': store_row.get('distributor', '-'),
            'region': store_row.get('region', '-'),
        }
        store_elements = generate_store_page_elements(store_info, store_detail, styles, brand_name)
        elements.extend(store_elements)
        if idx < len(stores_data) - 1:
            elements.append(PageBreak())
    
    doc.build(elements)
    buffer.seek(0)
    return buffer

# ---------------------------
# BigQuery Setup
# ---------------------------
@st.cache_resource
def get_bigquery_client():
    """Initialize BigQuery client with credentials"""
    try:
        # Try Streamlit secrets first (for cloud deployment)
        gcp_secrets = st.secrets["connections"]["bigquery"]
        if "private_key" in gcp_secrets:
            gcp_secrets = dict(gcp_secrets)
            gcp_secrets["private_key"] = gcp_secrets["private_key"].replace("\\n", "\n")
        credentials = service_account.Credentials.from_service_account_info(gcp_secrets)
        project_id = gcp_secrets.get("project_id") or st.secrets["bigquery"].get("project")
        return bigquery.Client(credentials=credentials, project=project_id)
    except Exception as e1:
        try:
            # Try alternative Streamlit secrets structure
            gcp_secrets = st.secrets["bigquery"]
            if "private_key" in gcp_secrets:
                gcp_secrets = dict(gcp_secrets)
                gcp_secrets["private_key"] = gcp_secrets["private_key"].replace("\\n", "\n")
            credentials = service_account.Credentials.from_service_account_info(gcp_secrets)
            project_id = gcp_secrets.get("project_id") or gcp_secrets.get("project")
            return bigquery.Client(credentials=credentials, project=project_id)
        except Exception as e2:
            try:
                # Fallback to local credentials file
                credentials_path = "skintific-data-warehouse-ea77119e2e7a.json"
                
                import os
                if not os.path.exists(credentials_path):
                    possible_paths = [
                        r"D:\script\skintific-data-warehouse-ea77119e2e7a.json",
                        r"C:\Users\Bella Chelsea\Documents\skintific-data-warehouse-ea77119e2e7a.json",
                        "credentials.json",
                        "../credentials.json"
                    ]
                    for path in possible_paths:
                        if os.path.exists(path):
                            credentials_path = path
                            break
                
                credentials = service_account.Credentials.from_service_account_file(credentials_path)
                project_id = "skintific-data-warehouse"
                return bigquery.Client(credentials=credentials, project=project_id)
            except Exception as e3:
                error_msg = f"""
                Failed to initialize BigQuery client. Tried:
                1. Streamlit secrets (connections.bigquery): {str(e1)[:100]}
                2. Streamlit secrets (bigquery): {str(e2)[:100]}
                3. Local credentials file: {str(e3)[:100]}
                
                Please ensure either:
                - .streamlit/secrets.toml is configured with BigQuery credentials
                - Local credentials JSON file exists in the project directory
                """
                raise RuntimeError(error_msg)

# ---------------------------
# Stored Procedure Execution
# ---------------------------
# ---------------------------
# Stored Procedure Execution
# (REFRESH BASED ON last_updated_jkt)
# ---------------------------

def get_jkt_now():
    """Get current time in Jakarta (timezone-aware)"""
    return datetime.now(pytz.timezone("Asia/Jakarta"))

@st.cache_data(ttl=60)
def get_inventory_last_updated_jkt():
    """
    Read last_updated_jkt and correctly convert to Asia/Jakarta
    Handles both UTC and naive datetimes safely
    """
    client = get_bigquery_client()
    query = """
        SELECT
            MAX(last_updated_jkt) AS last_updated_jkt
        FROM `skintific-data-warehouse.rsa.inventory_buffer`
    """

    try:
        df = client.query(query).to_dataframe()

        if df.empty or pd.isna(df.loc[0, "last_updated_jkt"]):
            return None

        ts = df.loc[0, "last_updated_jkt"]
        jkt_tz = pytz.timezone("Asia/Jakarta")

        # Case 1: BigQuery returned naive datetime → ASSUME UTC
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=pytz.UTC)

        # Convert to Jakarta time
        return ts.astimezone(jkt_tz)

    except Exception as e:
        st.error(f"❌ Failed to read last_updated_jkt: {e}")
        return None



def execute_stored_procedure():
    """Execute inventory buffer stored procedure"""
    client = get_bigquery_client()
    query = "CALL `skintific-data-warehouse.rsa.inventory_buffer_sp`();"

    try:
        job = client.query(query)
        job.result()
        return True
    except Exception as e:
        st.error(f"❌ Error executing stored procedure: {e}")
        return False


def check_and_execute_sp():
    """
    Auto refresh rule:
    - Uses last_updated_jkt from inventory_buffer
    - Refresh if data is older than 2 hours
    """
    now_jkt = get_jkt_now()
    last_updated_jkt = get_inventory_last_updated_jkt()

    # First-time / empty table scenario
    if last_updated_jkt is None:
        with st.spinner("🔄 Initializing inventory data..."):
            if execute_stored_procedure():
                st.cache_data.clear()
                st.toast("Inventory initialized", icon="✅")
                return get_inventory_last_updated_jkt()
        return None

    # Time difference
    time_diff = now_jkt - last_updated_jkt
    should_refresh = time_diff.total_seconds() >= 7200  # 2 hours

    if should_refresh:
        with st.spinner("🔄 Updating inventory buffer (scheduled refresh)..."):
            if execute_stored_procedure():
                st.cache_data.clear()
                st.toast("Inventory refreshed", icon="✅")
                return get_inventory_last_updated_jkt()

    return last_updated_jkt


# ---------------------------
# Data Loading Functions
# ---------------------------
@st.cache_data(ttl=600)
def load_store_summary():
    client = get_bigquery_client()

    query = """
    SELECT
        region,
        store_code,
        store_name,
        distributor_g2g AS distributor,
        MAX(stock_date) stock_date,
        COUNT(product_code) AS total_skus,
        SUM(buffer_plan_ver2) AS total_buffer_qty,
        SUM(buffer_plan_value_ver2) AS est_order_value,
        SUM(actual_stock) AS current_stock_qty
    FROM `skintific-data-warehouse.rsa.inventory_buffer`
    GROUP BY region, store_code, store_name, distributor
    HAVING SUM(buffer_plan_ver2) > 0
    """

    df = client.query(query).to_dataframe()

    def priority(v):
        if v > 5_000_000:
            return "High"
        elif v > 2_000_000:
            return "Medium"
        return "Low"

    df["priority"] = df["est_order_value"].apply(priority)
    return df

@st.cache_data(ttl=600)
def load_inventory_buffer_data():
    """Load full inventory buffer data from BigQuery"""
    client = get_bigquery_client()
    
    query = """
    SELECT 
        store_code,
        product_code,
        product_life_cycle,
        assortment,
        inner_pcs,
        price_for_store,
        product_name,
        actual_stock,
        stock_date,
        avg_daily_qty,
        days_of_inventory,
        standard_doi,
        id_st,
        distributor_g2g,
        region,
        store_name,
        address,
        stok_distributor,
        status_stok_distributor,
        buffer_plan,
        buffer_plan_ver2,
        buffer_plan_value,
        buffer_plan_value_ver2,
        SO_Bulanan
    FROM `skintific-data-warehouse.rsa.inventory_buffer`
    ORDER BY 
        region,
        store_name,
        SO_Bulanan,
        buffer_plan_value_ver2 DESC
    """
    
    df = client.query(query).to_dataframe()
    return df


# ---------------------------
# Session State Init (Cascading Filters)
# ---------------------------
for key, default in {
    "prev_region": None,
    "prev_distributor": None,
    "region_select": "All",
    "distributor_select": "All",
    "store_select": [],
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

if "sp_checked" not in st.session_state:
    st.session_state.sp_checked = False

if not st.session_state.sp_checked:
    last_exec_time = check_and_execute_sp()
    st.session_state.sp_checked = True
else:
    last_exec_time = get_inventory_last_updated_jkt()




# ---------------------------
# Load Data
# ---------------------------
df_inventory_buffer = None
df_store_summary = None
df_master_store = None

# Execute stored procedure if needed (every 2 hours)
last_exec_time = check_and_execute_sp()

try:
    with st.spinner("🔄 Loading data from BigQuery..."):
        df_store_summary = load_store_summary()
        df_master_store = df_store_summary
        df_inventory_buffer = load_inventory_buffer_data()

except Exception as e:
    st.error(f"❌ Error loading data: {e}")
    st.info("💡 Make sure the stored procedure has been executed and credentials are configured correctly.")
    
    with st.expander("🔍 Debug Information"):
        st.code(str(e))
        import traceback
        st.code(traceback.format_exc())
    
    st.warning("⚠️ Running in demo mode with sample data...")
    
def get_display_timestamp():
    return last_exec_time.strftime("%d %b %Y, %H:%M WIB") if last_exec_time else "-"

# ---------------------------
# Header
# ---------------------------
# Format last execution time for display
if last_exec_time:
    # last_exec_time is already in JKT from the function above
    sync_time_str = last_exec_time.strftime("%I:%M %p")
    sync_date_str = last_exec_time.strftime("%b %d, %Y")
    
    # Calculate time since last sync
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
    sync_time_str = get_jkt_now().strftime("%I:%M %p")


st.markdown(f"""
<div class="main-header">
    <div style="display: flex; justify-content: space-between; align-items: flex-start;">
        <div>
            <h1>📦 PO Suggestion Tool</h1>
            <p>Data-Driven Purchase Order Recommendations (Inventory Buffer)</p>
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
# ---------------------------
# Safety Check
# ---------------------------
if df_inventory_buffer is None or df_store_summary is None:
    st.error("❌ No data available. Please check your BigQuery connection.")
    st.stop()

# ---------------------------
# Filter Selection (UX Friendly – Vertical Flow)
# ---------------------------
# st.markdown('<div class="selection-box">', unsafe_allow_html=True)
# st.markdown('<div class="selection-title">📍 Filter Selection</div>', unsafe_allow_html=True)

# ---------- REGION & DISTRIBUTOR ----------
col1, col2 = st.columns(2)

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

with col2:
    st.markdown(
        '<div class="selection-title" style="font-size:0.9rem;">🏢 Distributor</div>',
        unsafe_allow_html=True
    )
    df_f1 = df_inventory_buffer.copy()
    if selected_region != "All":
        df_f1 = df_f1[df_f1["region"] == selected_region]

    distributors = ["All"] + sorted(df_f1["distributor_g2g"].dropna().unique())
    selected_distributor = st.selectbox(
        "Distributor",
        distributors,
        label_visibility="collapsed",
        key="distributor_select"
    )

# ---------- RESET STORE WHEN UPPER FILTER CHANGES ----------
if (
    st.session_state.prev_region != selected_region
    or st.session_state.prev_distributor != selected_distributor
):
    st.session_state.store_select = []

st.session_state.prev_region = selected_region
st.session_state.prev_distributor = selected_distributor

st.markdown("<hr style='margin:1rem 0; opacity:0.3;'>", unsafe_allow_html=True)

# ---------- STORE SELECTION ----------
# Ensure session state is initialized
if "store_select" not in st.session_state:
    st.session_state.store_select = []

# Filter store options logic
df_f2 = df_f1.copy()
if selected_distributor != "All":
    df_f2 = df_f2[df_f2["distributor_g2g"] == selected_distributor]

store_options = sorted(df_f2["store_name"].dropna().unique())
selected_count = len(st.session_state.store_select)

# Unified Header Row: Title | Select All | Clear
# Adjusting the ratio to [6, 1.2, 1] to keep buttons tight on the right
title_col, btn_col1, btn_col2 = st.columns([6, 1.2, 1], gap="small")

with title_col:
    st.markdown(
        f"""
        <div style="padding-top: 5px;">
            <span style="font-size:0.95rem; font-weight:600;">🏪 Store Selection</span>
            <span style="font-size:0.8rem; color:gray; margin-left:5px;">({selected_count} selected)</span>
        </div>
        """,
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

# Multiselect Widget
# We use the 'store_select' key directly to link it to the button actions
selected_stores = st.multiselect(
    "Store Selection",
    options=store_options,
    default=st.session_state.store_select,
    placeholder="Select one or more stores…",
    label_visibility="collapsed",
    key="store_multiselect_widget"
)

# Update the session state with the current selection from the widget
st.session_state.store_select = selected_stores

# ---------------------------
# Final Data Filtering (SINGLE SOURCE OF TRUTH)
# ---------------------------
filtered_stores = df_store_summary.copy()

if selected_region != "All":
    filtered_stores = filtered_stores[
        filtered_stores["region"] == selected_region
    ]

if selected_distributor != "All":
    filtered_stores = filtered_stores[
        filtered_stores["distributor"] == selected_distributor
    ]

if selected_stores:
    filtered_stores = filtered_stores[
        filtered_stores["store_name"].isin(selected_stores)
    ]

# Priority sorting
priority_order = {"High": 0, "Medium": 1, "Low": 2}
filtered_stores["priority_rank"] = filtered_stores["priority"].map(priority_order)

filtered_stores = (
    filtered_stores
    .sort_values(
        by=["priority_rank", "est_order_value"],
        ascending=[True, False]
    )
    .drop(columns="priority_rank")
)


# ---------------------------
# Multi-Store PDF Download Button (Full Width Version)
# ---------------------------
if len(filtered_stores) > 1:
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
    
    if st.button("📄 Generate Combined PDF for All Selected Stores", key="multi_pdf_btn", use_container_width=True, type="primary"):
        with st.spinner(f"Sedang memproses PDF untuk {len(filtered_stores)} toko..."):
            pdf_buffer = generate_multi_store_pdf(filtered_stores, df_inventory_buffer, brand_name="Glad2Glow")
            
            timestamp = last_exec_time.strftime("%Y-%m-%d_%H%M_WIB")
            filename = f"PO_Bulk_{len(filtered_stores)}_Stores_{timestamp}.pdf"
            
            st.download_button(
                label=f"📥 Klik di Sini untuk Mengunduh PDF ({len(filtered_stores)} Toko)",
                data=pdf_buffer,
                file_name=filename,
                mime="application/pdf",
                key="dl_multi_pdf",
                use_container_width=True
            )
    st.markdown("---")

# ---------------------------
# Show active filters
# ---------------------------
active_filters = []
if selected_distributor != "All":
    active_filters.append(f"🏪 {selected_distributor}")
if selected_stores:
    active_filters.append(f"🎯 {len(selected_stores)} specific store(s)")

if active_filters:
    st.markdown(f"""
    <div style="background: #f8fafc; padding: 0.75rem 1rem; border-radius: 8px; margin-bottom: 1rem; border-left: 4px solid #667eea;">
        <span style="color: #64748b; font-size: 0.85rem; font-weight: 600;">Active Filters: </span>
        <span style="color: #334155; font-size: 0.85rem;">{' • '.join(active_filters)}</span>
    </div>
    """, unsafe_allow_html=True)

# ---------------------------
# Display Store Cards (Horizontal UI)
# ---------------------------
st.markdown(f"### Store List ({len(filtered_stores)} stores)")
st.markdown('<p style="color: #64748b; font-size: 0.85rem; margin-top: -1rem;">PO suggestions prioritized by sales velocity - from most sold to less sold items</p>', unsafe_allow_html=True)

# Use enumerate to get a unique numeric 'i'
for i, (idx, row) in enumerate(filtered_stores.iterrows()):
    # 1. Store Title & Code
    st.markdown(f"""
        <div style="display: flex; align-items: center; gap: 10px; margin-top: 1.5rem; margin-bottom: 0.5rem;">
            <span style="font-size: 1.15rem; font-weight: 700; color: #1e293b;">{row['store_name']}</span>
            <span style="background: #f1f5f9; color: #64748b; padding: 2px 10px; border-radius: 12px; font-size: 0.75rem; font-weight: 600;">{row['store_code']}</span>
        </div>
    """, unsafe_allow_html=True)

    # 2. Metrics & Buttons as Columns
    m1, m2, m3, m4, btn_download = st.columns([2, 2.5, 2, 2.5, 1.5])

    with m1:
        st.markdown(f'<div class="metric-group"><span class="metric-label">Branch</span><span class="metric-value">{row["region"]}</span></div>', unsafe_allow_html=True)
    
    with m2:
        st.markdown(f'<div class="metric-group"><span class="metric-label">🗓️ Last Stock Update</span><span class="metric-value">{row["stock_date"]}</span></div>', unsafe_allow_html=True)
    
    with m3:
        st.markdown(f'<div class="metric-group"><span class="metric-label">Total SKUs</span><span class="metric-value">{int(row["total_skus"])} products</span></div>', unsafe_allow_html=True)
    
    with m4:
        order_value = f"Rp {row['est_order_value']/1000000:.1f}M"
        st.markdown(f'<div class="metric-group"><span class="metric-label">Est. Order Value</span><span class="metric-value value-blue">{order_value}</span></div>', unsafe_allow_html=True)

    # Get store detail data
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

    # --- DOWNLOAD BUTTON ---
    with btn_download:
        st.markdown('<div style="padding-top: 0.5rem;"></div>', unsafe_allow_html=True)
        # Generate PDF directly
        pdf_buffer = generate_po_pdf(store_info, store_detail, brand_name="Glad2Glow")
        st.download_button(
            label="📄 Download PDF",
            data=pdf_buffer,
            file_name=f"PO_{row['store_code']}_{datetime.now().strftime('%Y%m%d')}.pdf",
            mime="application/pdf",
            key=f"dl_pdf_{row['store_code']}_{i}",
            use_container_width=True
        )

    st.markdown('<hr style="margin: 1rem 0; border: none; border-top: 1px solid #f1f5f9;">', unsafe_allow_html=True)

# ---------------------------
# Empty State
# ---------------------------
if len(filtered_stores) == 0:
    filter_msg = "No stores match your current filters."
    if active_filters:
        filter_msg = f"No stores found with filters: {', '.join(active_filters)}"
    
    st.markdown(f"""
    <div style="text-align: center; padding: 4rem 2rem; color: #94a3b8;">
        <div style="font-size: 4rem; margin-bottom: 1rem;">🔍</div>
        <h3 style="color: #64748b;">No stores found</h3>
        <p>{filter_msg}</p>
        <p style="font-size: 0.85rem; margin-top: 1rem;">Try adjusting your filters or selecting "All" in some dropdowns</p>
    </div>
    """, unsafe_allow_html=True)

# ---------------------------
# Footer
# ---------------------------
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #94a3b8; font-size: 0.85rem; padding: 1rem;">
    💡 PO suggestions are automatically calculated based on 2-month sales data, DOI standards, and stock levels<br>
    📊 Data source: <code>skintific-data-warehouse.rsa.inventory_buffer</code>
</div>
""", unsafe_allow_html=True)