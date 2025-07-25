import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from rapidfuzz import fuzz
from haversine import haversine, Unit
from io import BytesIO
import re

# Define required columns for the Excel template
REQUIRED_COLUMNS = [
    "Store Name",
    "Region",
    "City",
    "Address",
    "Latitude",
    "Longitude",
    "Reference ID",
    "NIK",
    "NPWP"
]

# --- BigQuery Setup ---
@st.cache_data(ttl=3600)
def load_existing_data():
    try:
        # Use Streamlit secrets if available
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
    except Exception:
        # Fallback to local key file
        SERVICE_ACCOUNT_FILE = r'C:\script\skintific-data-warehouse-ea77119e2e7a.json'
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE
        )

    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # 1. Fetch master store data from store basis
    master_store_table_path = st.secrets["bigquery_tables"]["master_store_database"]
    query = f"""
        SELECT
            cust_id,
            store_name,
            region,
            city,
            address,
            longitude,
            latitude,
            nik,
            npwp,
            reference_id_skt,
            reference_id_g2g,
            reference_id_tph
        FROM `{master_store_table_path}`
    """
    existing_df = client.query(query).to_dataframe()

    # 2. Fetch last 6 months sell-through data
    sell_through_table_path = st.secrets["bigquery_tables"]["fact_sell_through"]
    sell_through_query = f"""
        SELECT
            cust_id,
            DATE_TRUNC(calendar_date, MONTH) AS month_,
            SUM(value) AS monthly_st_value
        FROM `{sell_through_table_path}`
        WHERE calendar_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 6 MONTH)
            AND calendar_date <= LAST_DAY(CURRENT_DATE(), MONTH)
        GROUP BY cust_id, month_
    """
    sell_through_df = client.query(sell_through_query).to_dataframe()

    # 3. Pivot the monthly sell-through data in Pandas
    if not sell_through_df.empty:
        # Convert month_ to datetime
        if not sell_through_df.empty:
            sell_through_df['month_'] = pd.to_datetime(sell_through_df['month_'])

        # Format month as YYYY-MM for column names
        sell_through_df['month_str'] = sell_through_df['month_'].dt.strftime('%Y-%m')
        
        # Pivot the table
        pivoted_st_df = sell_through_df.pivot_table(
            index='cust_id',
            columns='month_str',
            values='monthly_st_value',
            fill_value=0 # Fill NaN with 0 for months without data
        ).reset_index()

        # Rename columns to be more descriptive (e.g., 'ST Value 2024-01')
        pivoted_st_df.columns.name = None # Remove the column name from the index
        pivoted_st_df = pivoted_st_df.rename(
            columns={col: f"ST Value {col}" for col in pivoted_st_df.columns if col != 'cust_id'}
        )

        # 4. Merge pivoted sell-through data with existing_df
        merged_df = pd.merge(
            existing_df,
            pivoted_st_df,
            on='cust_id',
            how='left'
        )
        # Ensure that newly added ST columns are filled with 0 if no match (left join)
        st_cols = [col for col in merged_df.columns if col.startswith('ST Value')]
        merged_df[st_cols] = merged_df[st_cols].fillna(0)
    else:
        # If no sell-through data is returned, initialize the columns with 0
        merged_df = existing_df.copy()
        # Create column names for the last 6 months dynamically and MTD
        today = pd.to_datetime(pd.Timestamp.now().date())
        for i in range(7):
            # We want the *last* 6 full months. So 1 month ago, 2 months ago, etc.
            # Example: If today is July 16, 2025, we want June, May, April, March, Feb, Jan 2025
            month_date = today - pd.DateOffset(months=i+1)
            col_name = f"ST Value {month_date.strftime('%Y-%m')}"
            merged_df[col_name] = 0

    return merged_df

# --- Normalize Helper ---
def normalize(text: str, text_type: str):
    if not text or not text_type:
        return ""
    
    text = str(text).lower()
    
    # Normalize for store name
    if text_type == 'store_name':
        return text.strip()
    # Normalize for store address
    elif text_type == 'address':
        text = text.replace("jl.", "").replace("no.", "").replace("jl", "").replace("jalan", "").replace("no", "").replace("jalan.", "")
        text = re.sub(r"[^a-z0-9\s]", " ", text)  # Remove punctuation
        text = re.sub(r"\brt\b|\brw\b", "", text)  # Remove rt/rw
        text = re.sub(r"\s+", " ", text).strip()
    # Normalize for store city
    elif text_type == 'city':
        text = re.sub(r"[^a-z0-9\s]", " ", text)  # Remove punctuation
        text = re.sub(r"\s+", " ", text).strip()

    return text

# --- Matching Logic ---
def match_store(new_store, existing_stores, return_all=False):
    results = []
    for _, store in existing_stores.iterrows():
        score = 0
        log_lines = []

        # Name similarity
        name_score = max(
            fuzz.ratio(normalize(new_store['Store Name'], "store_name"), normalize(store['store_name'], "store_name")),
            fuzz.token_set_ratio(normalize(new_store['Store Name'], "store_name"), normalize(store['store_name'], "store_name")),
            fuzz.partial_ratio(normalize(new_store['Store Name'], "store_name"), normalize(store['store_name'], "store_name"))
        )
        name_weight = 35
        name_score_scaled = (name_score / 100) * name_weight
        score += name_score_scaled
        log_lines.append(f"• Name Similarity: {name_score} → Score: {name_score_scaled:.1f} / {name_weight}")

        # Address and City Scoring Logic
        new_address = new_store['Address']
        existing_address = store['address']
        new_city = new_store['City']
        existing_city = store['city']

        # Check if both new_city and existing_city are filled (not NaN and not empty string after strip)
        if pd.notna(new_city) and str(new_city).strip() != "" and \
           pd.notna(existing_city) and str(existing_city).strip() != "":
            # Both cities are present, split the 25 points: 20 for address, 5 for city
            address_weight_current = 20
            city_weight_current = 10

            address_score = max(
                fuzz.ratio(normalize(new_address, "address"), normalize(existing_address, "address")),
                fuzz.token_set_ratio(normalize(new_address, "address"), normalize(existing_address, "address")),
                fuzz.partial_ratio(normalize(new_address, "address"), normalize(existing_address, "address"))
            )
            city_score = max(
                fuzz.ratio(normalize(new_city, "city"), normalize(existing_city, "city")),
                fuzz.token_set_ratio(normalize(new_city, "city"), normalize(existing_city, "city")),
                fuzz.partial_ratio(normalize(new_city, "city"), normalize(existing_city, "city"))
            )

            address_score_scaled = (address_score / 100) * address_weight_current
            city_score_scaled = (city_score / 100) * city_weight_current
            
            score += address_score_scaled + city_score_scaled
            log_lines.append(f"• Address Similarity: {address_score} → Score: {address_score_scaled:.1f} / {address_weight_current}")
            log_lines.append(f"• City Similarity: {city_score} → Score: {city_score_scaled:.1f} / {city_weight_current}")
        else:
            # City is blank in one or both, assign full 25 points to address
            address_weight_current = 25

            address_score = max(
                fuzz.ratio(normalize(new_address, "address"), normalize(existing_address, "address")),
                fuzz.token_set_ratio(normalize(new_address, "address"), normalize(existing_address, "address")),
                fuzz.partial_ratio(normalize(new_address, "address"), normalize(existing_address, "address"))
            )
            address_score_scaled = (address_score / 100) * address_weight_current
            
            score += address_score_scaled
            log_lines.append(f"• Address Similarity: {address_score} → Score: {address_score_scaled:.1f} / {address_weight_current} (City data missing)")

        # Location proximity
        dist = None
        try:
            if pd.notna(new_store['Latitude']) and pd.notna(store['latitude']):
                dist = haversine(
                    (float(new_store['Latitude']), float(new_store['Longitude'])),
                    (float(store['latitude']), float(store['longitude'])),
                    unit=Unit.METERS
                )
                if dist < 50:
                    score += 20
                    log_lines.append(f"• Distance: {dist:.2f} m ✅ (+20)")
                else:
                    log_lines.append(f"• Distance: {dist:.2f} m ❌")
        except:
            log_lines.append("• Distance: N/A")

        # NIK / NPWP
        nik_score = 0
        npwp_score = 0

        nik_new = str(new_store.get("NIK", "")).strip()
        nik_existing = str(store.get("nik", "")).strip()
        npwp_new = str(new_store.get("NPWP", "")).strip()
        npwp_existing = str(store.get("npwp", "")).strip()

        if nik_new and nik_existing and nik_new[-8:] == nik_existing[-8:]:
            nik_score = 5
            log_lines.append("• NIK match ✅ (+5)")
        else:
            log_lines.append("• NIK match ❌")

        if npwp_new and npwp_existing and npwp_new[-8:] == npwp_existing[-8:]:
            npwp_score = 5
            log_lines.append("• NPWP match ✅ (+5)")
        else:
            log_lines.append("• NPWP match ❌")

        score += nik_score + npwp_score

        # Reference ID check
        input_ref_id = str(new_store.get("Reference ID", "")).strip().upper()
        if input_ref_id and input_ref_id in [
            str(store.get("reference_id_skt", "")).strip().upper(),
            str(store.get("reference_id_g2g", "")).strip().upper(),
            str(store.get("reference_id_tph", "")).strip().upper()
        ]:
            score += 10
            log_lines.append("• Reference ID match ✅ (+10)")
        else:
            log_lines.append("• Reference ID match ❌")

        log_lines.append(f"• Total Score: {score}")

        if score >= 70 or (return_all and score >= 50):
            result = store.to_dict()
            result["Match Score"] = score
            result["Match Log"] = "\n".join(log_lines)
            result["New Store Name"] = new_store["Store Name"]
            result["New Address"] = new_store["Address"]
            results.append(result)

    return results

# --- Streamlit UI ---
st.set_page_config("Duplicate Store Checker", page_icon="🔍")
st.title("🔍 Duplicate Store Detection")

existing_df = load_existing_data()

option = st.radio("Input Type", ["Upload Excel", "Manual Entry"])

if option == "Upload Excel":
    st.markdown("---")
    st.subheader("1. Download Excel Template")
    # Create a dummy DataFrame for the template
    template_df = pd.DataFrame(columns=REQUIRED_COLUMNS)
    template_excel_buffer = BytesIO()
    template_df.to_excel(template_excel_buffer, index=False, engine='openpyxl')
    template_excel_buffer.seek(0) # Rewind the buffer to the beginning

    st.download_button(
        label="Download New Store Data Template (Excel)",
        data=template_excel_buffer,
        file_name="new_store_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # Notes on required columns for the template
    st.info(
        f"**Important:** When filling the template, all the following columns are required:\n"
        f"- **Store Name**\n"
        f"- **Region**\n"
        f"- **Address**\n"
        f"- **Reference ID**\n"
        f"- **Latitude**\n"
        f"- **Longitude**"
    )

    st.markdown("---")

    st.subheader("2. Upload Your New Store Data")

    # Notes for Excel upload guidelines
    st.info(
        "**Please follow these important guidelines for your Excel upload:**\n"
        "\n"
        "- Your Excel file must contain **only one sheet/tab**.\n"
        "- All column headers should begin in cell **A1** of that single sheet.\n"
        "- Each uploaded Excel file should include data for **only one region**. "
        "If you have data for multiple regions, please create separate Excel files for each region and upload them individually."
    )

    uploaded = st.file_uploader("Upload new store Excel file", type=["xlsx"])
    if uploaded:
        new_stores = pd.read_excel(uploaded)

        # Validate uploaded file columns
        uploaded_columns = new_stores.columns.tolist()
        if sorted(uploaded_columns) != sorted(REQUIRED_COLUMNS):
            st.error(
                f"The uploaded Excel file does not match the template. "
                f"Expected columns (case-sensitive, order doesn't matter): {', '.join(REQUIRED_COLUMNS)}. "
                f"Found columns: {', '.join(uploaded_columns)}."
            )
            st.stop() # Stop execution if validation fails

        # Normalize the columns in the new_stores DataFrame
        new_stores['Region'] = new_stores['Region'].apply(lambda x: normalize(x, 'region') if pd.notna(x) else x)
        new_stores['City'] = new_stores['City'].apply(lambda x: normalize(x, 'city') if pd.notna(x) else x)
        new_stores['Store Name'] = new_stores['Store Name'].apply(lambda x: normalize(x, 'store_name') if pd.notna(x) else x)
        new_stores['Address'] = new_stores['Address'].apply(lambda x: normalize(x, 'address') if pd.notna(x) else x)

        # Validate and get unique region(s) from uploaded file
        input_regions = new_stores['Region'].dropna().unique().tolist()
        
        if not input_regions:
            st.error("No valid region found in the 'Region' column of the uploaded Excel file. Please ensure the column is populated.")
            st.stop()
        
        # Check for single region in uploaded Excel as per requirement
        if len(input_regions) > 1:
            st.error("The uploaded Excel file must contain data for only one region. Multiple regions found: " + ", ".join(input_regions))
            st.stop()
        
        # Now we know there's exactly one region to filter by
        selected_input_region = input_regions[0]
        
        # Filter existing_df based on the input region
        filtered_existing_df = existing_df[existing_df['region'].apply(lambda x: normalize(x, 'region')) == selected_input_region]

        if filtered_existing_df.empty:
            st.warning(f"No existing stores found in the specified region: '{selected_input_region}'. Cannot perform matching.")
        else:
            all_matches = []
            for _, new_store in new_stores.iterrows():
                matches = match_store(new_store, filtered_existing_df, return_all=True)
                all_matches.extend(matches)

            if all_matches:
                result_df = pd.DataFrame(all_matches)
                st.write("### Possible Duplicates")
                
                st_value_cols = [col for col in result_df.columns if col.startswith('ST Value')]
                display_columns = [
                    "New Store Name", "New Address", "cust_id", "store_name", "region", 
                    "city", "address", "latitude", "longitude", "reference_id_skt", "reference_id_g2g",
                    "reference_id_tph", "nik", "npwp", "Match Score"
                    ] + sorted(st_value_cols)

                # Create a copy for display to avoid modifying the original result_df
                display_df = result_df[display_columns].copy()

                # Rename columns for display
                rename_map = {
                    "New Store Name": "Input Store Name",
                    "New Address": "Input Address",
                    "cust_id": "Matched Customer ID",
                    "store_name": "Matched Store Name",
                    "region": "Region",
                    "city": "City",
                    "address": "Matched Address",
                    "latitude": "Latitude",
                    "longitude": "Longitude",
                    "reference_id_skt": "Ref ID SKT",
                    "reference_id_g2g": "Ref ID G2G",
                    "reference_id_tph": "Ref ID TPH",
                    "nik": "NIK",
                    "npwp": "NPWP",
                    "Match Score": "Similarity Score"
                }

                # Apply renaming
                display_df.rename(columns=rename_map, inplace=True)

                # Apply formatting to ST Value columns for display
                for col in st_value_cols:
                    # Ensure the column is numeric before formatting, handle potential NaN by showing empty string
                    display_df[col] = pd.to_numeric(display_df[col], errors='coerce').apply(
                        lambda x: f"{x:,.2f}" if pd.notna(x) else ""
                    )

                st.dataframe(display_df)

                for i, row in result_df.iterrows():
                    with st.expander(f"🔍 Log for match #{i+1} - {row['store_name']}"):
                        st.text(row["Match Log"])

                output = BytesIO()
                display_df.to_excel(output, index=False, engine='openpyxl')
                st.download_button("Download Results", data=output.getvalue(), file_name="duplicates.xlsx")
            else:
                st.success("No duplicates found.")
else:
    with st.form("manual_form"):
        store_name = st.text_input("Store Name")
        region = st.text_input("Region")
        city = st.text_input("City")
        address = st.text_input("Address")
        lat = st.text_input("Latitude")
        lon = st.text_input("Longitude")
        ref_id = st.text_input("Reference ID")
        nik = st.text_input("NIK")
        npwp = st.text_input("NPWP")
        submitted = st.form_submit_button("Check for Duplicates")

    if submitted:
        new_store = {
            "Store Name": normalize(store_name, 'store_name'),
            "Region": normalize(region, 'region'),
            "City": normalize(city, 'city'),
            "Address": normalize(address, 'address'),
            "Latitude": lat,
            "Longitude": lon,
            "Reference ID": ref_id,
            "NIK": nik,
            "NPWP": npwp
        }

        # Extract region from manual input and validate
        input_region = new_store.get('Region')
        if not input_region:
            st.error("Region must be provided for manual entry.")
            st.stop()

        # Filter existing_df based on the input region
        filtered_existing_df = existing_df[existing_df['region'].apply(lambda x: normalize(x, 'region')) == input_region]

        if filtered_existing_df.empty:
            st.warning(f"No existing stores found in region: '{input_region}'. Cannot perform matching.")
        else:
            matches = match_store(new_store, filtered_existing_df, return_all=True)

            if matches:
                result_df = pd.DataFrame(matches).sort_values(by="Match Score", ascending=False)
                st.write("### All Matches (Sorted by Score)")
                
                st_value_cols = [col for col in result_df.columns if col.startswith('ST Value')]
                display_columns = [
                    "cust_id", "store_name", "region", "city", "address", 
                    "latitude", "longitude", "reference_id_skt", "reference_id_g2g",
                    "reference_id_tph", "nik", "npwp", "Match Score"
                ] + sorted(st_value_cols)

                # Create a copy for display to avoid modifying the original result_df
                display_df = result_df[display_columns].copy()

                # Rename columns for display
                rename_map = {
                    "cust_id": "Matched Customer ID",
                    "store_name": "Matched Store Name",
                    "region": "Region",
                    "city": "City",
                    "address": "Matched Address",
                    "latitude": "Latitude",
                    "longitude": "Longitude",
                    "reference_id_skt": "Ref ID SKT",
                    "reference_id_g2g": "Ref ID G2G",
                    "reference_id_tph": "Ref ID TPH",
                    "nik": "NIK",
                    "npwp": "NPWP",
                    "Match Score": "Similarity Score"
                }

                # Apply renaming
                display_df.rename(columns=rename_map, inplace=True)

                # Apply formatting to ST Value columns for display
                for col in st_value_cols:
                    # Ensure the column is numeric before formatting, handle potential NaN by showing empty string
                    display_df[col] = pd.to_numeric(display_df[col], errors='coerce').apply(
                        lambda x: f"{x:,.2f}" if pd.notna(x) else ""
                    )

                st.dataframe(display_df)

                for i, row in result_df.iterrows():
                    with st.expander(f"🔍 Log for match #{i+1} - {row['store_name']}"):
                        st.text(row["Match Log"])

                output = BytesIO()
                display_df.to_excel(output, index=False, engine='openpyxl')
                st.download_button("Download Possible Duplicate", data=output.getvalue(), file_name="manual_all_scores.xlsx")
            else:
                st.success("No potential duplicates found.")