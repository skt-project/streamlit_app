import streamlit as st
import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from io import BytesIO
from datetime import datetime

# Store channel options with definitions
STORE_CHANNEL_OPTIONS = ["Cosmetic Store", "Retail/Grocery", "Pharmacy", "ATC"]

STORE_CHANNEL_DEFINITIONS = {
    "Cosmetic Store": {
        "Kontribusi kosmetik": "> 50%",
        "Beauty Advisory": "Ada",
        "Area display utama": "Rak khusus per brand",
        "Tipe beauty visibility": "Backwall, floor display, kasir, banner, billboard",
        "Rekomendasi Produk": "Semua SKU"
    },
    "Retail/Grocery": {
        "Kontribusi kosmetik": "< 50%",
        "Beauty Advisory": "Tidak ada",
        "Area display utama": "Rak multi-brand",
        "Tipe beauty visibility": "Kasir",
        "Rekomendasi Produk": "Cleanser, sunscreen, micellar water, body lotion, and compact powder"
    },
    "Pharmacy": {
        "Kontribusi kosmetik": "< 5%",
        "Beauty Advisory": "Tidak ada",
        "Area display utama": "Rak multi-brand",
        "Tipe beauty visibility": "Kasir",
        "Rekomendasi Produk": "Acne and sensitive series"
    },
    "ATC": {
        "Kontribusi kosmetik": "Channel alternatif selain GT dan MT",
        "Beauty Advisory": "-",
        "Area display utama": "-",
        "Tipe beauty visibility": "-",
        "Rekomendasi Produk": "-"
    }
}

# --- BigQuery Setup ---
def get_credentials():
    """Get GCP credentials from secrets or local file"""
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
        master_store_table_path = st.secrets["bigquery_tables"]["master_store_database"]
    except Exception:
        # Fallback to local key file (silent fallback for local development)
        SERVICE_ACCOUNT_FILE = r'C:\Users\Bella Chelsea\Documents\skintific-data-warehouse-ea77119e2e7a.json'
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE
        )
        master_store_table_path = "skintific-data-warehouse.gt_schema.master_store_database_basis"
    
    return credentials, master_store_table_path

@st.cache_data(ttl=3600)
def load_store_data(region_filter):
    """Load GT store data from BigQuery with optional region filter"""
    credentials, master_store_table_path = get_credentials()
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    where_clause = "WHERE (customer_category = 'GT' OR customer_category IS NULL)"
    if region_filter and region_filter != "All Regions":
        where_clause += f" AND region = '{region_filter}'"
    
    query = f"""
        SELECT 
            customer_category,
            region,
            cust_id,
            reference_id_g2g,
            store_name
        FROM `{master_store_table_path}`
        {where_clause}
        ORDER BY region, cust_id
    """
    
    df = client.query(query).to_dataframe()
    
    # Apply IFNULL logic in pandas
    df['customer_category'] = df['customer_category'].fillna('GT')
    
    return df, credentials

def get_available_regions():
    """Get list of unique regions from BigQuery for GT stores only"""
    credentials, master_store_table_path = get_credentials()
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    query = f"""
        SELECT DISTINCT region 
        FROM `{master_store_table_path}`
        WHERE (customer_category = 'GT' OR customer_category IS NULL)
            AND region IS NOT NULL
        ORDER BY region
    """
    
    regions_df = client.query(query).to_dataframe()
    return ["All Regions"] + regions_df['region'].tolist()

def create_excel_with_dropdown(df):
    """Create Excel file with dropdown validation for store_channel column"""
    output = BytesIO()
    
    # Create Excel writer
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Write Store Data sheet
        df.to_excel(writer, index=False, sheet_name='Store Data')
        
        # Add instructions sheet
        instructions_df = pd.DataFrame({
            'Instructions': [
                '1. Fill the store_channel column by selecting one of the dropdown options: Cosmetic Store, Retail/Grocery, Pharmacy, ATC',
                '2. Do NOT modify customer_category, region, cust_id, reference_id_g2g, or store_name columns',
                '3. Save the file and upload it back through the "Upload Updated Data" tab',
                '4. All rows must have a value in the store_channel column'
            ]
        })
        instructions_df.to_excel(writer, index=False, sheet_name='Instructions')
        
        # Add store channel definitions sheet as a structured table matching the image
        definitions_rows = [
            {
                'Category': 'GT',
                'Channel': 'Cosmetic Store',
                'Kontribusi kosmetik': '> 50%',
                'Beauty Advisory': 'Ada',
                'Area display utama': 'Rak khusus per brand',
                'Tipe beauty visibility': 'Backwall, floor display, kasir, banner, billboard',
                'Rekomendasi Produk': 'Semua SKU'
            },
            {
                'Category': 'GT',
                'Channel': 'Retail/Grocery',
                'Kontribusi kosmetik': '< 50%',
                'Beauty Advisory': 'Tidak ada',
                'Area display utama': 'Rak multi-brand',
                'Tipe beauty visibility': 'Kasir',
                'Rekomendasi Produk': 'Cleanser, sunscreen, micellar water, body lotion, and compact powder'
            },
            {
                'Category': 'GT',
                'Channel': 'Pharmacy',
                'Kontribusi kosmetik': '< 5%',
                'Beauty Advisory': 'Tidak ada',
                'Area display utama': 'Rak multi-brand',
                'Tipe beauty visibility': 'Kasir',
                'Rekomendasi Produk': 'Acne and sensitive series'
            },
            {
                'Category': 'Alternative Channel',
                'Channel': 'ATC',
                'Kontribusi kosmetik': 'Channel alternatif selain GT dan MT',
                'Beauty Advisory': '',
                'Area display utama': '',
                'Tipe beauty visibility': '',
                'Rekomendasi Produk': ''
            }
        ]
        
        definitions_df = pd.DataFrame(definitions_rows)
        definitions_df.to_excel(writer, index=False, sheet_name='Channel Definitions')
        
        # Get the xlsxwriter workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets['Store Data']
        definitions_sheet = writer.sheets['Channel Definitions']
        
        # Format the definitions sheet
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#4472C4',
            'font_color': 'white',
            'border': 1,
            'align': 'center',
            'valign': 'vcenter'
        })
        
        cell_format = workbook.add_format({
            'border': 1,
            'align': 'left',
            'valign': 'top',
            'text_wrap': True
        })
        
        # Set column widths for definitions sheet
        definitions_sheet.set_column(0, 0, 20)  # Category
        definitions_sheet.set_column(1, 1, 20)  # Channel
        definitions_sheet.set_column(2, 2, 20)  # Kontribusi kosmetik
        definitions_sheet.set_column(3, 3, 18)  # Beauty Advisory
        definitions_sheet.set_column(4, 4, 25)  # Area display utama
        definitions_sheet.set_column(5, 5, 45)  # Tipe beauty visibility
        definitions_sheet.set_column(6, 6, 50)  # Rekomendasi Produk
        
        # Set row height for better readability
        definitions_sheet.set_row(1, 30)
        definitions_sheet.set_row(2, 30)
        definitions_sheet.set_row(3, 30)
        definitions_sheet.set_row(4, 30)
        
        # Find the store_channel column index
        # Columns: customer_category (0), region (1), cust_id (2), reference_id_g2g (3), store_name (4), store_channel (5)
        store_channel_col = 5
        
        # Add data validation for dropdown
        worksheet.data_validation(
            1, store_channel_col,
            10000, store_channel_col,
            {
                'validate': 'list',
                'source': STORE_CHANNEL_OPTIONS,
                'input_title': 'Select Store Channel',
                'input_message': 'Choose one: ' + ', '.join(STORE_CHANNEL_OPTIONS),
                'error_title': 'Invalid Entry',
                'error_message': 'Please select a value from the dropdown list',
                'show_error': True
            }
        )
        
        # Set column widths for Store Data sheet
        worksheet.set_column(0, 0, 20)  # customer_category
        worksheet.set_column(1, 1, 20)  # region
        worksheet.set_column(2, 2, 15)  # cust_id
        worksheet.set_column(3, 3, 18)  # reference_id_g2g
        worksheet.set_column(4, 4, 35)  # store_name
        worksheet.set_column(5, 5, 18)  # store_channel

    output.seek(0)
    return output

def insert_to_bigquery(df, credentials, table_name):
    """Insert combined data to BigQuery staging table"""
    try:
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        
        # Define schema with reference_id_g2g
        schema = [
            bigquery.SchemaField("cust_id", "STRING"),
            bigquery.SchemaField("reference_id_g2g", "STRING"),
            bigquery.SchemaField("store_name", "STRING"),
            bigquery.SchemaField("customer_category", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("store_channel", "STRING"),
            bigquery.SchemaField("upload_timestamp", "TIMESTAMP"),
        ]
        
        df['upload_timestamp'] = datetime.now()
        
        # Include reference_id_g2g in upload
        upload_df = df[['cust_id', 'reference_id_g2g', 'store_name', 'customer_category', 'region', 
                        'store_channel', 'upload_timestamp']]
        
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
        )
        
        job = client.load_table_from_dataframe(upload_df, table_name, job_config=job_config)
        job.result()
        
        return True, f"Successfully loaded {len(upload_df)} rows to {table_name}"
    except Exception as e:
        return False, str(e)

# --- Streamlit UI ---
st.set_page_config(page_title="Store Update Manager", page_icon="📊", layout="wide")
st.title("📊 Store Channel Update Manager")

st.markdown("""
This application manages the GT store update workflow:
1. **Export** GT store data filtered by region
2. **Fill** the store_channel column using dropdown options in Excel
3. **Upload** completed files and automatically inject to Database staging table
""")

# Create tabs for different actions
tab1, tab2 = st.tabs(["📥 Export Data", "📤 Upload & Submit to Database"])

with tab1:
    st.header("Step 1: Export GT Store Data")
    
    st.info("📌 **Note**: Only GT stores (customer_category = 'GT' or NULL) will be exported")
    
    # Instructions box
    st.markdown("### 📋 Instructions for Filling the Excel File")
    st.markdown(f"""
    After downloading the Excel file, please follow these steps:
    
    1. **Store Channel Column** (Column F):
       - Click on any cell in the `store_channel` column
       - A dropdown arrow will appear - click it to see options
       - Select one of: **{', '.join(STORE_CHANNEL_OPTIONS)}**
       - ⚠️ Do NOT type manually - use the dropdown only
       - 📖 See the "Channel Definitions" sheet in Excel for detailed descriptions
    
    2. **Important Reminders**:
       - ❌ Do NOT modify: `customer_category`, `region`, `cust_id`, `reference_id_g2g`, or `store_name` columns
       - ✅ All rows must have a value in the `store_channel` column
       - 💾 Save the file after completing all entries
       - 📤 Upload the completed file in the "Upload & Submit to Database" tab
    """)
    
    # Display channel definitions
    st.markdown("### 📖 Store Channel Definitions")
    
    # Create a dataframe for display matching the image structure
    definitions_display_df = pd.DataFrame([
        {
            'Category': 'GT',
            'Channel': 'Cosmetic Store',
            'Kontribusi kosmetik': '> 50%',
            'Beauty Advisory': 'Ada',
            'Area display utama': 'Rak khusus per brand',
            'Tipe beauty visibility': 'Backwall, floor display, kasir, banner, billboard',
            'Rekomendasi Produk': 'Semua SKU'
        },
        {
            'Category': 'GT',
            'Channel': 'Retail/Grocery',
            'Kontribusi kosmetik': '< 50%',
            'Beauty Advisory': 'Tidak ada',
            'Area display utama': 'Rak multi-brand',
            'Tipe beauty visibility': 'Kasir',
            'Rekomendasi Produk': 'Cleanser, sunscreen, micellar water, body lotion, and compact powder'
        },
        {
            'Category': 'GT',
            'Channel': 'Pharmacy',
            'Kontribusi kosmetik': '< 5%',
            'Beauty Advisory': 'Tidak ada',
            'Area display utama': 'Rak multi-brand',
            'Tipe beauty visibility': 'Kasir',
            'Rekomendasi Produk': 'Acne and sensitive series'
        },
        {
            'Category': 'Alternative Channel',
            'Channel': 'ATC',
            'Kontribusi kosmetik': 'Channel alternatif selain GT dan MT',
            'Beauty Advisory': '',
            'Area display utama': '',
            'Tipe beauty visibility': '',
            'Rekomendasi Produk': ''
        }
    ])
    
    st.dataframe(definitions_display_df, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # Get available regions
    with st.spinner("Loading regions..."):
        available_regions = get_available_regions()
    
    # Region filter
    selected_region = st.selectbox(
        "Select Region to Export",
        available_regions,
        key="region_export"
    )
    
    if st.button("Export Store Data", type="primary"):
        with st.spinner("Fetching GT store data from BigQuery..."):
            store_df, credentials = load_store_data(selected_region)
        
        if store_df.empty:
            st.warning("No GT stores found for the selected region.")
        else:
            st.success(f"✅ Found {len(store_df)} GT stores")
            
            store_df['store_channel'] = ""
            store_df = store_df[['customer_category', 'region', 'cust_id', 'reference_id_g2g', 'store_name', 'store_channel']]
            
            st.subheader("Data Preview")
            st.dataframe(store_df.head(10), use_container_width=True)
            
            st.info(f"**Total Records:** {len(store_df)}")
            
            with st.spinner("Creating Excel file with dropdown validation..."):
                excel_output = create_excel_with_dropdown(store_df)
            
            region_suffix = selected_region.replace(" ", "_").replace("(", "").replace(")", "") if selected_region != "All Regions" else "All_Regions"
            filename = f"gt_store_export_{region_suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            
            st.download_button(
                label="📥 Download Excel Template (with Dropdown)",
                data=excel_output.getvalue(),
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Download the Excel file with dropdown options for store_channel. Fill the column and upload back."
            )
            
            st.success("✅ Excel file ready! The store_channel column has dropdown options built-in.")

with tab2:
    st.header("Step 2: Upload Updated Store Data & Submit to Database")
    
    st.info(f"""
    **Before uploading, ensure:**
    - ✅ The `store_channel` column is filled using the **dropdown options** in Excel
    - ❌ You have NOT modified the `cust_id`, `reference_id_g2g`, `store_name`, `customer_category`, or `region` columns
    - ✅ All rows have values in the store_channel column
    - ✅ All stores exist in the master database
    
    **Store Channel Options (available in dropdown):**
    {', '.join(STORE_CHANNEL_OPTIONS)}
    """)
    
    # Display channel definitions in upload tab too
    with st.expander("📖 View Store Channel Definitions"):
        # Create a dataframe for display
        definitions_rows = []
        for channel, attributes in STORE_CHANNEL_DEFINITIONS.items():
            definitions_rows.append({
                'Channel': channel,
                'Kontribusi kosmetik': attributes['Kontribusi kosmetik'],
                'Beauty Advisory': attributes['Beauty Advisory'],
                'Area display utama': attributes['Area display utama'],
                'Tipe beauty visibility': attributes['Tipe beauty visibility'],
                'Rekomendasi Produk': attributes['Rekomendasi Produk']
            })
        
        definitions_display_df = pd.DataFrame(definitions_rows)
        st.dataframe(definitions_display_df, use_container_width=True, hide_index=True)
    
    uploaded_file = st.file_uploader(
        "Upload completed Excel file",
        type=["xlsx"],
        help="Select the Excel file with filled store_channel column"
    )
    
    if uploaded_file:
        try:
            updated_df = pd.read_excel(uploaded_file, sheet_name='Store Data')
            
            required_cols = ['customer_category', 'region', 'cust_id', 'reference_id_g2g', 'store_name', 'store_channel']
            missing_cols = [col for col in required_cols if col not in updated_df.columns]
            
            if missing_cols:
                st.error(f"❌ Missing required columns: {', '.join(missing_cols)}")
            else:
                st.success("✅ File structure is valid")
                
                st.markdown("---")
                st.subheader("📊 Channel Assignment Summary")
                
                total_stores = len(updated_df)
                filled_channel = updated_df['store_channel'].notna() & (updated_df['store_channel'] != '')
                filled_count = filled_channel.sum()
                empty_count = total_stores - filled_count
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Stores", total_stores)
                with col2:
                    st.metric("Filled", filled_count, delta=None if filled_count == total_stores else f"-{empty_count} empty")
                
                st.markdown("#### Channel Distribution")
                channel_counts = updated_df['store_channel'].value_counts()
                
                cols = st.columns(4)
                for idx, channel in enumerate(STORE_CHANNEL_OPTIONS):
                    with cols[idx]:
                        count = channel_counts.get(channel, 0)
                        percentage = (count / total_stores * 100) if total_stores > 0 else 0
                        st.metric(channel, f"{count} ({percentage:.1f}%)")
                
                st.markdown("---")
                
                uploaded_regions = updated_df['region'].dropna().unique().tolist()
                
                if not uploaded_regions:
                    st.error("❌ No valid region found in the uploaded file.")
                    st.stop()
                
                st.subheader("🔍 Validating Store List Against Database")
                
                all_valid = True
                stores_not_in_db = []
                
                for region in uploaded_regions:
                    with st.spinner(f"Checking stores for region: {region}..."):
                        bq_stores_df, _ = load_store_data(region)
                        bq_cust_ids = set(bq_stores_df['cust_id'].tolist())
                        
                        excel_region_df = updated_df[updated_df['region'] == region]
                        excel_cust_ids = set(excel_region_df['cust_id'].tolist())
                        
                        missing_in_excel = bq_cust_ids - excel_cust_ids
                        extra_in_excel = excel_cust_ids - bq_cust_ids
                        
                        # Track stores not in database
                        if extra_in_excel:
                            stores_not_in_db.extend(extra_in_excel)
                        
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric(f"Region: {region}", "")
                        with col2:
                            st.metric("In Database", len(bq_cust_ids))
                        with col3:
                            st.metric("In Excel", len(excel_cust_ids))
                        with col4:
                            if len(missing_in_excel) == 0 and len(extra_in_excel) == 0:
                                st.metric("Status", "✅ Match")
                            else:
                                st.metric("Status", "❌ Mismatch")
                                all_valid = False
                        
                        if missing_in_excel:
                            st.error(f"❌ **{len(missing_in_excel)} stores from Database are MISSING in Excel for region: {region}**")
                            with st.expander(f"Show missing stores ({len(missing_in_excel)})"):
                                missing_stores_df = bq_stores_df[bq_stores_df['cust_id'].isin(missing_in_excel)]
                                st.dataframe(missing_stores_df, use_container_width=True)
                        
                        if extra_in_excel:
                            st.error(f"❌ **{len(extra_in_excel)} stores in Excel are NOT in Database for region: {region}**")
                            st.error("**FILE REJECTED**: All stores must exist in the master database before submission.")
                            with st.expander(f"Show stores not in database ({len(extra_in_excel)})"):
                                extra_stores_df = excel_region_df[excel_region_df['cust_id'].isin(extra_in_excel)]
                                st.dataframe(extra_stores_df, use_container_width=True)
                
                # If there are stores not in database, stop processing
                if stores_not_in_db:
                    st.error(f"""
                    ❌ **VALIDATION FAILED - FILE REJECTED**
                    
                    Found {len(stores_not_in_db)} store(s) that do not exist in the master database.
                    
                    **Action Required:**
                    - Remove these stores from your Excel file, OR
                    - Add these stores to the master database first, then re-upload
                    
                    All stores must be registered in the master database before channelization.
                    """)
                    st.stop()
                
                if not all_valid:
                    st.error("❌ **Validation Failed**: The Excel file does not match the Database store list.")
                    st.stop()
                
                st.success("✅ All stores validated successfully against Database!")
                st.markdown("---")
                
                # Validate store_channel values
                invalid_store_channels = updated_df[
                    (~updated_df['store_channel'].isin(STORE_CHANNEL_OPTIONS)) & 
                    (updated_df['store_channel'].notna()) & 
                    (updated_df['store_channel'] != '')
                ]
                
                if not invalid_store_channels.empty:
                    st.error(f"❌ Found {len(invalid_store_channels)} rows with invalid store_channel values")
                    st.write("Valid options are:", ', '.join(STORE_CHANNEL_OPTIONS))
                    with st.expander("Show rows with invalid store_channel"):
                        st.dataframe(invalid_store_channels[required_cols], use_container_width=True)
                    st.stop()
                else:
                    st.success("✅ All store_channel values are valid")
                
                empty_channel = updated_df['store_channel'].isna() | (updated_df['store_channel'] == '')
                
                if empty_channel.any():
                    st.error(f"❌ Found {empty_channel.sum()} rows with missing store_channel values. All stores must have a channel assigned!")
                    
                    with st.expander("📋 Show rows with missing data", expanded=True):
                        missing_data = updated_df[empty_channel][required_cols]
                        st.dataframe(missing_data, use_container_width=True)
                    
                    st.warning("""
                    ⚠️ **Action Required:**
                    - Please fill in the missing store_channel values in your Excel file
                    - Use the dropdown options in the store_channel column
                    - Save the file and re-upload
                    """)
                    
                    st.stop()
                else:
                    st.success("✅ All required fields are filled")
                
                st.markdown("---")
                
                st.subheader("📈 Data Preview")
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Stores", len(updated_df))
                with col2:
                    st.metric("Unique Channels Used", updated_df['store_channel'].nunique())
                
                st.subheader("Store Channel Distribution")
                store_channel_dist = updated_df['store_channel'].value_counts().reset_index()
                store_channel_dist.columns = ['Store Channel', 'Count']
                st.dataframe(store_channel_dist, use_container_width=True)
                
                st.subheader("Data Preview (First 10 Rows)")
                st.dataframe(updated_df.head(10), use_container_width=True)
                
                st.success("✅ **All Validations Passed!**")
                
                st.markdown("---")
                
                st.subheader("🚀 Submit to Database")
                
                # BigQuery table input
                
                st.info(f"""
                **Ready to submit:**
                - File: `{uploaded_file.name}`
                - Regions: {', '.join(uploaded_regions)}
                - Total Stores: {len(updated_df)}
                - All validations: ✅ Passed
                
                Click the button below to submit this data directly to the Database.
                """)
                
                if st.button("Submit", type="primary"):
                    with st.spinner("Uploading to Database..."):
                        credentials, _ = get_credentials()
                        success, message = insert_to_bigquery(updated_df, credentials, staging_table)
                    
                    if success:
                        st.success(f"✅ {message}")
                        st.balloons()
                        
                        st.info(f"""
                        **Upload Summary:**
                        - Table: `{staging_table}`
                        - Records: {len(updated_df)}
                        - Regions: {', '.join(uploaded_regions)}
                        - Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                        """)
                    else:
                        st.error(f"❌ Database upload failed: {message}")
        
        except Exception as e:
            st.error(f"❌ Error reading file: {str(e)}")

# Footer
st.markdown("---")
st.caption("Store Channel & Category Update Manager - Powered by Streamlit")