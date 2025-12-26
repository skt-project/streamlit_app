# streamlit_app_lightweight.py
import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
from google.cloud import storage
from google.oauth2 import service_account
from folium import GeoJsonTooltip
from streamlit_folium import st_folium
from io import BytesIO
import json
import ast
import os
from datetime import datetime

# --- Streamlit App Config ---
st.set_page_config(layout="wide")
st.title("Indonesia Nielsen Store Map")

# --- Configuration ---
STORE_GRADE_COLORS = {
    "S": "#006400", "A": "#32CD32", "B": "#FF8C00",
    "C": "#FFD700", "D": "#B22222", "Other": "#66B2FD"
}

DISTRIBUTOR_BRAND_COLORS = {
    "SKT_ONLY": "lightblue",
    "G2G_ONLY": "pink",
    "BOTH_SKT_G2G": "purple",
    "OTHER": "gray"
}

# --- Initialize GCS Client ---
@st.cache_resource
def get_gcs_client():
    """Initialize GCS client"""
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
        BUCKET_NAME = st.secrets["gcs"]["data"]
    except Exception as e:
        # Local fallback with environment variable
        credentials = service_account.Credentials.from_service_account_file(
            r"C:\Users\Bella Chelsea\Documents\skintific-data-warehouse-ea77119e2e7a.json"
        )
        BUCKET_NAME = "public_skintific_storage"
    
    return storage.Client(credentials=credentials), BUCKET_NAME

# --- Load Region Index ---
@st.cache_data(ttl=86400)  # Cache for 24 hours
def load_region_index(_gcs_client, bucket_name):
    """Load the region index from GCS"""
    try:
        bucket = _gcs_client.bucket(bucket_name)
        blob = bucket.blob("external_dataset/processed_data/latest/region_index.json")
        
        if blob.exists():
            content = blob.download_as_text()
            full_index = json.loads(content)

            if 'data' in full_index:
                return full_index['data']
            else:
                st.error("Warning: 'data' key missing in region index file.")
                return {}
        else:
            st.error("Region index not found. Please run the pre-processing pipeline.")
            return {}
    except Exception as e:
        st.error(f"Error loading region index: {e}")
        return {}

# --- Load Processed Data for Specific Region/Kabupaten ---
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_processed_data(_gcs_client, bucket_name, region, kabupaten):
    """Load pre-processed data for specific region and kabupaten"""

    # Clean folder names
    region_folder = region.replace(" ", "_").replace("/", "_").lower()
    kabupaten_folder = kabupaten.replace(" ", "_").replace("/", "_").lower()

    base_path = f"external_dataset/processed_data/latest/{region_folder}/{kabupaten_folder}"

    try:
        bucket = _gcs_client.bucket(bucket_name)

        # Load village data with store counts
        village_blob = bucket.blob(f"{base_path}/villages_with_counts.parquet")
        if village_blob.exists():
            village_bytes = village_blob.download_as_bytes()
            villages_gdf = gpd.read_parquet(BytesIO(village_bytes))
            
            # Validate CRS
            if villages_gdf.crs is None:
                st.warning("Village data has no CRS, assuming EPSG:4326")
                villages_gdf.set_crs(epsg=4326, inplace=True)
        else:
            st.warning(f"No village data found for {region}/{kabupaten}")
            return None

        # Load stores
        stores_gdf = None
        stores_blob = bucket.blob(f"{base_path}/stores.parquet")
        if stores_blob.exists():
            stores_bytes = stores_blob.download_as_bytes()
            stores_gdf = gpd.read_parquet(BytesIO(stores_bytes))

        # Load distributors
        distributors_df = None
        dist_blob = bucket.blob(f"{base_path}/distributors.parquet")
        if dist_blob.exists():
            dist_bytes = dist_blob.download_as_bytes()
            distributors_df = pd.read_parquet(BytesIO(dist_bytes))

        # Load potential stores
        potential_df = None
        potential_blob = bucket.blob(f"{base_path}/potential_stores.parquet")
        if potential_blob.exists():
            potential_bytes = potential_blob.download_as_bytes()
            potential_df = pd.read_parquet(BytesIO(potential_bytes))

        # Load metadata
        metadata = {}
        metadata_blob = bucket.blob(f"{base_path}/metadata.json")
        if metadata_blob.exists():
            metadata_content = metadata_blob.download_as_text()
            metadata = json.loads(metadata_content)

        return {
            'villages': villages_gdf,
            'stores': stores_gdf,
            'distributors': distributors_df,
            'potential_stores': potential_df,
            'metadata': metadata
        }

    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

# --- Map Creation Functions ---
def create_legend():
    """Generate custom HTML legend"""
    legend_html = """
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 260px; 
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:darkgray; opacity:0.9;
                max-height: 80vh; overflow-y: auto;">
    &nbsp; <b>Map Legend</b> <br>
    &nbsp; <i style="background:#d2af13; border: 1px solid black; width: 10px; height: 10px; display: inline-block;"></i> Gold Tier (With stores) <br>
    &nbsp; <i style="background:darkred; border: 1px solid black; width: 10px; height: 10px; display: inline-block;"></i> Gold Tier (No stores)<br>
    &nbsp; <i style="background:#61615F; border: 1px solid black; width: 10px; height: 10px; display: inline-block;"></i> Other Tier<br>
    <hr style="margin: 2px 0;">
    &nbsp; <b>Store Points</b> <br>
    &nbsp; <i style="background:#006400; border: 1px solid black; width: 10px; height: 10px; display: inline-block; border-radius: 50%;"></i> Store Grade S <br>
    &nbsp; <i style="background:#32CD32; border: 1px solid black; width: 10px; height: 10px; display: inline-block; border-radius: 50%;"></i> Store Grade A <br>
    &nbsp; <i style="background:#FF8C00; border: 1px solid black; width: 10px; height: 10px; display: inline-block; border-radius: 50%;"></i> Store Grade B <br>
    &nbsp; <i style="background:#FFD700; border: 1px solid black; width: 10px; height: 10px; display: inline-block; border-radius: 50%;"></i> Store Grade C <br>
    &nbsp; <i style="background:#B22222; border: 1px solid black; width: 10px; height: 10px; display: inline-block; border-radius: 50%;"></i> Store Grade D <br>
    &nbsp; <i style="background:#66B2FD; border: 1px solid black; width: 10px; height: 10px; display: inline-block; border-radius: 50%;"></i> Store Grade Other <br>
    <hr style="margin: 2px 0;">
    &nbsp; <b>Distributor Branches</b> <br>
    &nbsp; <span style="color: lightblue; font-size: 18px">&#9830;</span> SKT <br>
    &nbsp; <span style="color: pink; font-size: 18px">&#9830;</span> G2G <br>
    &nbsp; <span style="color: purple; font-size: 18px">&#9830;</span> SKT & G2G <br>
    &nbsp; <i style="background:cadetblue; border: 1px solid black; width: 10px; height: 10px; display: inline-block;"></i> Potential Store (Shopping Cart) <br>
    </div>
    """
    return legend_html


def get_distributor_color(brand_string):
    """Determine distributor color based on brand"""
    if pd.isna(brand_string):
        return DISTRIBUTOR_BRAND_COLORS["OTHER"]
    
    brand_upper = str(brand_string).upper()
    has_skt = "SKT" in brand_upper
    has_g2g = "G2G" in brand_upper
    
    if has_skt and has_g2g:
        return DISTRIBUTOR_BRAND_COLORS["BOTH_SKT_G2G"]
    elif has_skt:
        return DISTRIBUTOR_BRAND_COLORS["SKT_ONLY"]
    elif has_g2g:
        return DISTRIBUTOR_BRAND_COLORS["G2G_ONLY"]
    else:
        return DISTRIBUTOR_BRAND_COLORS["OTHER"]

# --- Create Summary Table from Pre-processed Data ---
def create_summary_table(villages_gdf, stores_gdf):
    """
    Creates the summary table by expanding the dictionary in the 
    'store_grade' column into separate grade count columns (S, A, B, C, D, Other).
    """
    if villages_gdf.empty:
        return pd.DataFrame()

    # Validate required columns exist
    required_cols = ['Region', 'Kabupaten', 'Kecamatan', 'Kelurahan']
    missing_cols = [col for col in required_cols if col not in villages_gdf.columns]
    if missing_cols:
        st.error(f"Missing required columns: {missing_cols}")
        return pd.DataFrame()

    # Ensure 'store_count' exists
    if 'store_count' not in villages_gdf.columns:
        villages_gdf['store_count'] = 0

    # Check if store_grade column exists
    if 'store_grade' not in villages_gdf.columns:
        st.warning("'store_grade' column not found. Creating default summary.")
        summary_cols = required_cols + ['store_count']
        summary_df = villages_gdf[summary_cols].copy()
        # Add empty grade columns
        for grade in ['S', 'A', 'B', 'C', 'D', 'Other']:
            summary_df[f'Grade {grade}'] = 0
    else:
        summary_cols = required_cols + ['store_count', 'store_grade']
        summary_df = villages_gdf[summary_cols].copy()

        # 1. Standardize and Parse the 'store_grade' column
        def parse_grade_dict(data):
            if data is None or data == '' or (isinstance(data, str) and data.strip() == ''):
                return {}
            
            if isinstance(data, dict):
                return data
            
            if isinstance(data, str):
                try:
                    # Try JSON parsing first
                    return json.loads(data)
                except json.JSONDecodeError:
                    try:
                        # Try Python literal evaluation
                        return ast.literal_eval(data)
                    except (ValueError, SyntaxError):
                        st.warning(f"Could not parse grade data: {data[:50]}...")
                        return {}
            
            return {}

        # Apply the parsing function
        summary_df['store_grade_dict'] = summary_df['store_grade'].apply(parse_grade_dict)

        # 2. Expand the dictionary into new columns
        grade_counts_df = summary_df['store_grade_dict'].apply(pd.Series).fillna(0).astype(int)

        # 3. Standardize Grade Columns
        all_grades = ['S', 'A', 'B', 'C', 'D', 'Other']
        grade_cols_final = [f"Grade {g}" for g in all_grades]

        # Rename the new columns and ensure we have all required columns
        grade_mapping = {g: f"Grade {g}" for g in grade_counts_df.columns}
        grade_counts_df = grade_counts_df.rename(columns=grade_mapping)

        # 4. Merge/Concatenate the expanded grades back to the main DataFrame
        summary_df = pd.concat([summary_df.drop(columns=['store_grade', 'store_grade_dict']), grade_counts_df], axis=1)

        # 5. Ensure all required columns exist
        for col in grade_cols_final:
            if col not in summary_df.columns:
                summary_df[col] = 0
            summary_df[col] = summary_df[col].astype(int)

    # 6. Final cleanup and sorting
    grade_cols = [f"Grade {g}" for g in ['S', 'A', 'B', 'C', 'D', 'Other']]
    existing_grade_cols = [col for col in grade_cols if col in summary_df.columns]
    summary_df['Number of Stores'] = summary_df[existing_grade_cols].sum(axis=1)

    for col_name in ["Kabupaten", "Kecamatan", "Kelurahan"]:
        if col_name in summary_df.columns:
            summary_df[col_name] = summary_df[col_name].apply(str.title)

    # Reorder columns
    final_cols_order = required_cols + ['Number of Stores'] + existing_grade_cols
    summary_df = summary_df[final_cols_order].copy()

    # Sort by number of stores
    summary_df = summary_df.sort_values('Number of Stores', ascending=False)

    return summary_df

# --- Main App Logic ---
def main():
    # Initialize GCS client
    gcs_client, BUCKET_NAME = get_gcs_client()

    # Load region index
    st.header("🗺️ Filter Map")

    region_index = load_region_index(gcs_client, BUCKET_NAME)

    if not region_index:
        st.error("No data available. Please run the pre-processing pipeline first.")
        return

    # Region selection
    regions = sorted(region_index.keys())
    selected_region = st.selectbox("Select Region", ["--- Select Region ---"] + regions, index=0)
    
    if selected_region == "--- Select Region ---":
        st.info("Please select a Region to begin.")
        return

    # Validate region exists in index
    if selected_region not in region_index:
        st.error(f"Region '{selected_region}' not found in index.")
        return

    # Kabupaten selection
    kabupaten = sorted(region_index[selected_region])
    kabupaten_titled = sorted([k.title() for k in kabupaten])

    selected_kabupaten = st.selectbox(
        "Select Kabupaten", 
        ["--- Select Kabupaten ---"] + kabupaten_titled, 
        index=0
    )
    
    if selected_kabupaten == "--- Select Kabupaten ---":
        st.info("Please select a Kabupaten to view the map.")
        return

    with st.spinner("Loading map data..."):
        # Load pre-processed data
        processed_data = load_processed_data(
            gcs_client, BUCKET_NAME, selected_region, selected_kabupaten
        )

        if processed_data is None:
            st.error("Failed to load data. Please try again.")
            return

        villages_gdf = processed_data['villages']
        stores_gdf = processed_data['stores']
        distributors_df = processed_data['distributors']
        potential_df = processed_data['potential_stores']

        # Format geographical columns for consistent display
        for col_name in ["Kabupaten", "Kecamatan", "Kelurahan"]:
            if col_name in villages_gdf.columns:
                villages_gdf[col_name] = villages_gdf[col_name].apply(str.title)

        # Display metadata
        metadata = processed_data['metadata']

        st.subheader("📊 Area Statistics")

        # Use 4 columns for the metrics
        cols = st.columns(4)

        with cols[0]:
            st.metric("Kelurahan (Villages)", metadata.get('village_count', 0))

        with cols[1]:
            st.metric("Total Stores", metadata.get('store_count', 0))

        with cols[2]:
            st.metric("Distributors", metadata.get('distributor_count', 0))

        with cols[3]:
            st.metric("Potential Stores", metadata.get('potential_store_count', 0))

        # Format timestamp properly
        timestamp_str = metadata.get('processing_timestamp', 'Unknown')
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, AttributeError):
            formatted_time = timestamp_str.split('.')[0].split('+')[0].replace('T', ' ')
        st.caption(f"Last updated: {formatted_time}")

        # --- Create Map ---
        if not villages_gdf.empty:
            # Calculate accurate map center
            try:
                if villages_gdf.crs and villages_gdf.crs != 'EPSG:4326':
                    gdf_wgs84 = villages_gdf.to_crs(epsg=4326)
                else:
                    gdf_wgs84 = villages_gdf
                
                union_geom = gdf_wgs84.geometry.unary_union
                centroid = union_geom.centroid
                center = [centroid.y, centroid.x]
            except Exception as e:
                st.warning(f"Error calculating map center: {e}")
                # Fallback to bounds center
                bounds = villages_gdf.total_bounds
                center = [(bounds[1] + bounds[3]) / 2, (bounds[0] + bounds[2]) / 2]
            
            m = folium.Map(location=center, zoom_start=11, tiles="CartoDB positron")

            # Style function
            def style_function(feature):
                nielsen_tier = feature['properties'].get('Nielsen Tier', '')
                has_store = feature['properties'].get('store_count', 0) > 0

                if nielsen_tier == "Gold":
                    fill_color = "#d2af13" if has_store else "darkred"
                else:
                    fill_color = "#61615F"

                return {
                    "fillColor": fill_color,
                    "color": "black",
                    "weight": 1,
                    "fillOpacity": 0.7,
                }

            # Add villages GeoJSON
            folium.GeoJson(
                villages_gdf,
                name=f"{selected_kabupaten} Villages",
                style_function=style_function,
                tooltip=GeoJsonTooltip(
                    fields=['Region', 'Kabupaten', 'Kecamatan', 'Kelurahan', 
                           'Nielsen Tier', 'store_count'],
                    aliases=['Region:', 'Kabupaten:', 'Kecamatan:', 'Kelurahan:', 
                            'Nielsen Tier:', 'Store Count:'],
                    localize=True
                )
            ).add_to(m)

            # Add store markers
            if stores_gdf is not None and not stores_gdf.empty:
                for _, store in stores_gdf.iterrows():
                    grade = store.get('store_grade', 'Other')
                    color = STORE_GRADE_COLORS.get(grade, STORE_GRADE_COLORS['Other'])

                    folium.CircleMarker(
                        location=[store['latitude'], store['longitude']],
                        radius=6,
                        color='black',
                        weight=1,
                        fill=True,
                        fill_color=color,
                        fill_opacity=1.0,
                        tooltip=f"Store: {store.get('cust_id', 'N/A')} - {store.get('store_name', 'N/A')} (Grade: {grade})"
                    ).add_to(m)

            # Add distributor markers
            if distributors_df is not None and not distributors_df.empty:
                for _, dist in distributors_df.iterrows():
                    color = get_distributor_color(dist.get('Brand'))
                    folium.Marker(
                        location=[dist['Latitude'], dist['Longitude']],
                        icon=folium.Icon(color=color, icon="diamond", prefix="fa"),
                        tooltip=f"Distributor: {dist.get('Distributor Name', 'N/A')}"
                    ).add_to(m)

            # Add potential store markers
            if potential_df is not None and not potential_df.empty:
                for _, pot in potential_df.iterrows():
                    folium.Marker(
                        location=[pot['latitude'], pot['longitude']],
                        icon=folium.Icon(color="cadetblue", icon="shopping-cart", prefix="fa"),
                        tooltip=f"Potential Store: {pot.get('name', 'N/A')}"
                    ).add_to(m)

            # Add legend and layer control
            m.get_root().html.add_child(folium.Element(create_legend()))
            folium.LayerControl().add_to(m)

            # Display map
            st_map = st_folium(m, width=1200, height=700, returned_objects=[])

            # --- Display Summary Table ---
            st.subheader("📊 Area Summary")
            summary_df = create_summary_table(villages_gdf, stores_gdf)

            if not summary_df.empty:
                st.dataframe(summary_df, use_container_width=True, hide_index=True)

                # Create Excel download
                output = BytesIO()
                
                try:
                    summary_df.to_excel(output, index=False, sheet_name='Summary', engine='xlsxwriter')
                    excel_data = output.getvalue()

                    st.download_button(
                        label="📥 Download Excel",
                        data=excel_data,
                        file_name=f"summary_{selected_region}_{selected_kabupaten}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                except Exception as e:
                    st.error(f"Error creating Excel file: {e}")
            else:
                st.info("No summary data available.")

        else:
            st.warning("No village data available for the selected area.")

if __name__ == "__main__":
    main()