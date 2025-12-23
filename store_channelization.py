import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from io import BytesIO
from datetime import datetime

STORE_CHANNEL_OPTIONS = ["Cosmetic Store", "Retail/Grocery", "Pharmacy", "ATC"]

def get_credentials():
    try:
        gcp_secrets = st.secrets["connections"]["bigquery"]
        private_key = gcp_secrets["private_key"].replace("\\n", "\n")
        credentials = service_account.Credentials.from_service_account_info({
            "type": gcp_secrets["type"], "project_id": gcp_secrets["project_id"], "private_key_id": gcp_secrets["private_key_id"],
            "private_key": private_key, "client_email": gcp_secrets["client_email"], "client_id": gcp_secrets["client_id"],
            "auth_uri": gcp_secrets["auth_uri"], "token_uri": gcp_secrets["token_uri"],
            "auth_provider_x509_cert_url": gcp_secrets["auth_provider_x509_cert_url"],
            "client_x509_cert_url": gcp_secrets["client_x509_cert_url"]
        })
        master_store_table_path = st.secrets["bigquery_tables"]["master_store_database"]
        staging_table_path = st.secrets["bigquery_tables"]["staging_table"]
    except Exception:
        SERVICE_ACCOUNT_FILE = r'C:\Users\Bella Chelsea\Documents\skintific-data-warehouse-ea77119e2e7a.json'
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE
            )
        master_store_table_path = "skintific-data-warehouse.gt_schema.master_store_database_basis"
        staging_table_path = "skintific-data-warehouse.staging.gt_store_channel_staging"
    return credentials, master_store_table_path, staging_table_path
    
@st.cache_data(ttl=3600)
def load_store_data(region_filter, distributor_filter):
    credentials, master_store_table_path, _ = get_credentials()  # FIXED: unpack 3 values
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    where_clause = "WHERE (customer_category = 'GT' OR customer_category IS NULL OR customer_category = '')"
    if region_filter and region_filter != "Semua Region":
        where_clause += f" AND region = '{region_filter}'"
    if distributor_filter and distributor_filter != "Semua Distributor":
        where_clause += f" AND UPPER(distributor_g2g) = '{distributor_filter.upper()}'"
    query = f"""
        SELECT customer_category, region, UPPER(distributor_g2g) AS distributor, 
               dst_id_g2g, cust_id, reference_id_g2g AS reference_id, store_name
        FROM `{master_store_table_path}` {where_clause}
        ORDER BY region, distributor_g2g, cust_id
    """
    df = client.query(query).to_dataframe()
    df['customer_category'] = df['customer_category'].fillna('GT')
    dst_id = df['dst_id_g2g'].iloc[0] if not df.empty and 'dst_id_g2g' in df.columns else 'UNKNOWN'
    return df, credentials, dst_id

def get_available_regions():
    credentials, master_store_table_path, _ = get_credentials()  # FIXED: unpack 3 values
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    query = f"""
        SELECT DISTINCT region FROM `{master_store_table_path}`
        WHERE (customer_category = 'GT' OR customer_category IS NULL) AND region IS NOT NULL
        ORDER BY region
    """
    regions_df = client.query(query).to_dataframe()
    return ["Semua Region"] + regions_df['region'].tolist()

def get_available_distributors(region_filter=None):
    credentials, master_store_table_path, _ = get_credentials()  # FIXED: unpack 3 values
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    where_clause = "WHERE (customer_category = 'GT' OR customer_category IS NULL) AND distributor_g2g IS NOT NULL"
    if region_filter and region_filter != "Semua Region":
        where_clause += f" AND region = '{region_filter}'"
        
    query = f"""
        SELECT DISTINCT UPPER(distributor_g2g) as distributor_g2g
        FROM `{master_store_table_path}`
        {where_clause} 
        ORDER BY distributor_g2g
    """
    distributors_df = client.query(query).to_dataframe()
    return ["Semua Distributor"] + distributors_df['distributor_g2g'].tolist(), distributors_df

def create_excel_with_dropdown(df, region, distributor):
    output = BytesIO()
    
    # 1. Create a copy and remove the ID column
    df_for_excel = df.copy()
    if 'dst_id_g2g' in df_for_excel.columns:
        df_for_excel = df_for_excel.drop(columns=['dst_id_g2g'])
    
    if 'store_channel' not in df_for_excel.columns:
        df_for_excel['store_channel'] = ""

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_for_excel.to_excel(writer, index=False, sheet_name='Data Toko')
        
        workbook = writer.book
        worksheet = writer.sheets['Data Toko']
        info_sheet = workbook.add_worksheet('Panduan & Metadata')
        
        # Formats
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
        title_fmt = workbook.add_format({'bold': True, 'font_size': 14})
        cell_fmt = workbook.add_format({'border': 1})
        instruction_fmt = workbook.add_format({'italic': True, 'font_color': 'red'})

        # --- SECTION 1: INSTRUKSI PENGISIAN ---
        info_sheet.write(0, 0, "PANDUAN PENGISIAN FILE", title_fmt)
        
        instruksi = [
            "1. Buka sheet 'Data Toko'.",
            "2. Fokus pada kolom 'store_channel' (kolom terakhir).",
            "3. Klik pada sel kosong di kolom tersebut dan pilih kategori dari daftar dropdown yang muncul.",
            "4. JANGAN mengubah data pada kolom lain (cust_id, store_name, dll) agar tidak error saat upload.",
            "5. JANGAN mengubah nama file ini karena sistem mendeteksi ID Distributor dari nama file.",
            "6. Setelah selesai, simpan (Save) dan unggah kembali ke aplikasi Streamlit."
        ]
        
        for i, text in enumerate(instruksi):
            info_sheet.write(i + 2, 0, text)

        # --- SECTION 2: METADATA ---
        metadata_start_row = 10
        info_sheet.write(metadata_start_row, 0, "METADATA EXPORT", header_fmt)
        metadata = [
            ['Field', 'Value'],
            ['Region', region],
            ['Distributor', distributor],
            ['Total Toko', len(df_for_excel)],
            ['Tanggal Export', datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        ]
        for r, row in enumerate(metadata):
            info_sheet.write_row(metadata_start_row + 1 + r, 0, row, cell_fmt)

        # --- SECTION 3: DEFINISI CHANNEL (In Bahasa Indonesia) ---
        def_start_row = 17
        info_sheet.write(def_start_row, 0, "DEFINISI STORE CHANNEL", header_fmt)
        headers_def = [
            'Category', 'Channel', 'Kontribusi Kosmetik', 'Beauty Advisory (BA)', 
            'Area Display Utama', 'Tipe Visibility', 'Rekomendasi Produk'
        ]
        
        definitions_rows = [
            ['GT', 'Cosmetic Store', '> 50%', 'Ada', 'Rak khusus per brand', 'Backwall, floor display, kasir', 'Semua SKU'],
            ['GT', 'Retail/Grocery', '< 50%', 'Tidak ada', 'Rak multi-brand', 'Area Kasir', 'Cleanser, sunscreen, micellar, lotion'],
            ['GT', 'Pharmacy', '< 5%', 'Tidak ada', 'Rak multi-brand', 'Area Kasir', 'Acne and sensitive series'],
            ['Alternative', 'ATC', 'Channel alternatif (Non-GT/MT)', '-', '-', '-', '-']
        ]
        
        info_sheet.write_row(def_start_row + 1, 0, headers_def, header_fmt)
        for r, row in enumerate(definitions_rows):
            info_sheet.write_row(def_start_row + 2 + r, 0, row, cell_fmt)

        info_sheet.set_column(0, 0, 45) # Width for instruction column
        info_sheet.set_column(1, 6, 20)

        # --- Dropdown Logic ---
        store_channel_idx = df_for_excel.columns.get_loc('store_channel')
        worksheet.data_validation(1, store_channel_idx, 10000, store_channel_idx, {
            'validate': 'list',
            'source': STORE_CHANNEL_OPTIONS,
            'input_title': 'Pilih Channel',
            'input_message': 'Pilih salah satu: ' + ', '.join(STORE_CHANNEL_OPTIONS),
            'error_title': 'Input Salah',
            'error_message': 'Mohon pilih kategori dari daftar dropdown.',
            'show_error': True
        })
        worksheet.set_column(store_channel_idx, store_channel_idx, 25)
        
    output.seek(0)
    return output

def check_duplicate_cust_ids(df, credentials, staging_table_path):
    """Check if any cust_id from the dataframe already exists in staging table"""
    try:
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        
        # Get all cust_ids from the upload dataframe
        upload_cust_ids = df['cust_id'].tolist()
        
        # Query staging table to check for existing cust_ids
        cust_ids_str = "', '".join(upload_cust_ids)
        query = f"""
            SELECT DISTINCT cust_id, store_name, region, distributor, upload_timestamp
            FROM `{staging_table_path}`
            WHERE cust_id IN ('{cust_ids_str}')
        """
        
        existing_df = client.query(query).to_dataframe()
        
        if not existing_df.empty:
            return True, existing_df  # Duplicates found
        else:
            return False, None  # No duplicates
            
    except Exception as e:
        # If table doesn't exist yet, that's fine - no duplicates
        if "Not found" in str(e):
            return False, None
        else:
            raise e

def insert_to_bigquery(df, credentials, table_name, dst_id):
    try:
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        schema = [
            bigquery.SchemaField("cust_id", "STRING"), bigquery.SchemaField("reference_id", "STRING"),
            bigquery.SchemaField("store_name", "STRING"), bigquery.SchemaField("customer_category", "STRING"),
            bigquery.SchemaField("region", "STRING"), bigquery.SchemaField("distributor", "STRING"),
            bigquery.SchemaField("dst_id_g2g", "STRING"), bigquery.SchemaField("store_channel", "STRING"),
            bigquery.SchemaField("upload_timestamp", "TIMESTAMP")
        ]
        df['upload_timestamp'] = datetime.now()
        df['dst_id_g2g'] = dst_id
        upload_df = df[['cust_id', 'reference_id', 'store_name', 'customer_category', 'region', 
                        'distributor', 'dst_id_g2g', 'store_channel', 'upload_timestamp']]
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(upload_df, table_name, job_config=job_config)
        job.result()
        return True, f"Berhasil memuat {len(upload_df)} baris ke {table_name}"
    except Exception as e:
        return False, str(e)

st.set_page_config(page_title="Store Channelization", page_icon="📊", layout="wide")
st.title("📊 Store Channelization")

st.header("📥 Export Data")

with st.spinner("Memuat filter..."):
    available_regions = get_available_regions()

col1, col2 = st.columns(2)
with col1:
    selected_region = st.selectbox("Pilih Region", available_regions)
with col2:
    available_distributors, dist_df = get_available_distributors(selected_region)
    selected_distributor = st.selectbox("Pilih Distributor", available_distributors)

if st.button("Export", type="primary"):
    if selected_region == "Semua Region" or selected_distributor == "Semua Distributor":
        st.error("❌ Harap pilih Region dan Distributor spesifik (bukan 'Semua')")
    else:
        with st.spinner("Mengambil data..."):
            store_df, credentials, dst_id = load_store_data(selected_region, selected_distributor)
        if store_df.empty:
            st.warning("Tidak ada toko ditemukan")
        else:
            st.success(f"✅ Ditemukan {len(store_df)} toko | DST ID: {dst_id}")
            store_df['store_channel'] = ""
            # Display preview
            display_df = store_df[['customer_category', 'region', 'distributor', 'cust_id', 'reference_id', 'store_name', 'store_channel']]
            st.dataframe(display_df.head(6), use_container_width=True)
            
            excel_output = create_excel_with_dropdown(store_df, selected_region, selected_distributor)
            filename = f"toko_{selected_region.replace(' ','_')}_{selected_distributor.replace(' ','_')}_DST{dst_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            st.download_button("📥 Unduh File Excel", excel_output.getvalue(), filename, 
                             "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.markdown("---")

st.header("📤 Upload File yang Sudah Diisi")
st.info("⚠️ **PENTING**: File harus untuk SATU region dan distributor saja. Jangan gabungkan beberapa distributor.")

uploaded_file = st.file_uploader("Upload file Excel", type=["xlsx"])

if uploaded_file:
    error_log = []
    error_data = {}
    try:
        updated_df = pd.read_excel(uploaded_file, sheet_name='Data Toko')
        
        # 1. Parse filename as the SOLE SOURCE for dst_id
        filename = uploaded_file.name
        upload_dst_id = None
        if '_DST' in filename:
            try:
                upload_dst_id = filename.split('_DST')[1].split('_')[0]
            except Exception:
                error_log.append("❌ Format nama file tidak valid - tidak dapat mengekstrak DST ID")
        else:
            error_log.append("❌ Nama file tidak mengandung tag '_DST'. Gunakan file asli hasil ekspor.")

        # 2. Extract Metadata (Region & Distributor)
        try:
            info_sheet = pd.read_excel(uploaded_file, sheet_name='Panduan & Metadata')
            file_region = info_sheet[info_sheet['Field'] == 'Region']['Value'].values[0] if 'Region' in info_sheet['Field'].values else None
            file_distributor = info_sheet[info_sheet['Field'] == 'Distributor']['Value'].values[0] if 'Distributor' in info_sheet['Field'].values else None
        except Exception:
            file_region = None
            file_distributor = None
        
        # 3. Basic Column Validation
        required_cols = ['customer_category', 'region', 'distributor', 'cust_id', 'reference_id', 'store_name', 'store_channel']
        missing_cols = [col for col in required_cols if col not in updated_df.columns]
        
        if missing_cols:
            error_log.append(f"❌ Kolom hilang di Excel: {', '.join(missing_cols)}")
        elif not upload_dst_id:
            st.error("Gagal memproses: DST ID tidak ditemukan dalam nama file.")
            st.stop()
        else:
            # 4. Consistency Validation
            unique_regions = updated_df['region'].dropna().unique()
            unique_distributors = updated_df['distributor'].dropna().unique()
            
            if len(unique_regions) > 1:
                error_log.append(f"❌ File berisi {len(unique_regions)} region berbeda. Hanya boleh 1.")
            if len(unique_distributors) > 1:
                error_log.append(f"❌ File berisi {len(unique_distributors)} distributor berbeda. Hanya boleh 1.")
            
            if len(unique_regions) == 1 and len(unique_distributors) == 1:
                upload_region = unique_regions[0]
                upload_distributor = unique_distributors[0]
                
                if file_region and file_region != upload_region:
                    error_log.append(f"❌ Region di data ({upload_region}) ≠ metadata ({file_region})")
                if file_distributor and file_distributor != upload_distributor:
                    error_log.append(f"❌ Distributor di data ({upload_distributor}) ≠ metadata ({file_distributor})")
                
                st.success(f"✅ Identitas File: Region={upload_region}, Distributor={upload_distributor}, DST_ID={upload_dst_id}")
                
                # 5. Row Content Validation
                total = len(updated_df)
                
                # Database Cross-Check
                with st.spinner("Validasi terhadap database..."):
                    bq_df, credentials_check, _ = load_store_data(upload_region, upload_distributor)
                    bq_ids = set(bq_df['cust_id'])
                    excel_ids = set(updated_df['cust_id'])
                    
                    missing_in_excel = bq_ids - excel_ids
                    extra_in_excel = excel_ids - bq_ids
                    
                    if missing_in_excel:
                        error_log.append(f"❌ {len(missing_in_excel)} toko dari database HILANG di Excel")
                        error_data['missing_stores'] = bq_df[bq_df['cust_id'].isin(missing_in_excel)]
                    if extra_in_excel:
                        error_log.append(f"❌ {len(extra_in_excel)} toko di Excel TIDAK ADA di database")
                        error_data['extra_stores'] = updated_df[updated_df['cust_id'].isin(extra_in_excel)]

                # Invalid value validation
                invalid = updated_df[(~updated_df['store_channel'].isin(STORE_CHANNEL_OPTIONS)) & 
                                    (updated_df['store_channel'].notna()) & (updated_df['store_channel'] != '')]
                if not invalid.empty:
                    error_log.append(f"❌ {len(invalid)} baris dengan store_channel tidak valid")
                    error_data['invalid_channel'] = invalid

                # Empty value validation
                empty = updated_df['store_channel'].isna() | (updated_df['store_channel'] == '')
                if empty.any():
                    error_log.append(f"❌ {empty.sum()} baris dengan store_channel kosong")
                    error_data['empty_channel'] = updated_df[empty]

                # Check for duplicate cust_ids in staging table
                with st.spinner("Memeriksa duplikasi di database..."):
                    credentials, _, staging_table_path = get_credentials()
                    has_duplicates, duplicate_df = check_duplicate_cust_ids(updated_df, credentials, staging_table_path)
                    
                    if has_duplicates:
                        error_log.append(f"❌ {len(duplicate_df)} toko sudah ada di database (duplikasi cust_id)")
                        error_data['duplicate_staging'] = duplicate_df

                # --- Error Reporting (CONSOLIDATED TABLE) ---
                if error_log:
                    st.error("### ⚠️ LOG KESALAHAN VALIDASI")
                    for idx, msg in enumerate(error_log, 1):
                        st.markdown(f"**{idx}. {msg}**")
                    
                    if error_data:
                        st.markdown("---")
                        st.subheader("📋 Detail Baris Bermasalah")
                        
                        all_errors = []
                        # Helper to copy and drop dst_id_g2g if it exists
                        def prep_error_df(df, issue_label):
                            temp_df = df.copy()
                            if 'dst_id_g2g' in temp_df.columns:
                                temp_df = temp_df.drop(columns=['dst_id_g2g'])
                            temp_df['Issue_Type'] = issue_label
                            return temp_df

                        if 'missing_stores' in error_data:
                            all_errors.append(prep_error_df(error_data['missing_stores'], "HILANG DI EXCEL"))
                        
                        if 'extra_stores' in error_data:
                            all_errors.append(prep_error_df(error_data['extra_stores'], "TIDAK ADA DI DB"))
                        
                        if 'invalid_channel' in error_data:
                            all_errors.append(prep_error_df(error_data['invalid_channel'], "CHANNEL TIDAK VALID"))
                        
                        if 'empty_channel' in error_data:
                            all_errors.append(prep_error_df(error_data['empty_channel'], "CHANNEL KOSONG"))
                        
                        if 'duplicate_staging' in error_data:
                            temp_df = error_data['duplicate_staging'].copy()
                            # Format upload_timestamp to be more readable
                            if 'upload_timestamp' in temp_df.columns:
                                temp_df['upload_timestamp'] = pd.to_datetime(temp_df['upload_timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
                            temp_df['Issue_Type'] = "SUDAH ADA DI DATABASE"
                            all_errors.append(temp_df)

                        if all_errors:
                            # Merge all problematic rows into one table
                            report_df = pd.concat(all_errors, ignore_index=True)
                            
                            # Move 'Issue_Type' to the first column for better visibility
                            cols = ['Issue_Type'] + [c for c in report_df.columns if c != 'Issue_Type']
                            st.dataframe(report_df[cols], use_container_width=True)
                    
                    st.stop()
                
                # 6. Final Submission
                st.success("✅ Semua validasi berhasil!")
                if st.button("Upload", type="primary"):
                    with st.spinner("Mengunggah..."):
                        credentials, _, staging_table_path = get_credentials()  # FIXED: unpack 3 values and use staging_table_path
                        success, message = insert_to_bigquery(updated_df, credentials, staging_table_path, upload_dst_id)
                    if success:
                        st.success(f"✅ {message}")
                        st.snow()
                    else:
                        st.error(f"❌ Gagal: {message}")

    except Exception as e:
        st.error(f"❌ Terjadi kesalahan saat membaca file: {str(e)}")

st.markdown("---")
st.caption("Store Channelization App")