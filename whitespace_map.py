import os
import pandas as pd
import geopandas as gpd
import folium
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from folium.plugins import MarkerCluster
from folium import GeoJsonTooltip
import streamlit as st
from streamlit_folium import st_folium
from io import BytesIO

# --- Streamlit App Config ---
st.set_page_config(layout="wide")
st.title("Indonesia Nielsen Store Map")

# --- Configuration for Store Grade Colors ---
STORE_GRADE_COLORS = {
    "S": "#006400",  # DarkGreen
    "A": "#32CD32",  # LimeGreen
    "B": "#FF8C00",  # Orange
    "C": "#FFD700",  # OrangeRed
    "D": "#B22222",  # Red
    "Other": "#66B2FD",  # Default/Missing Grade (Blue)
}

# --- Configuration for Distributor Brand Colors (Add this to the config section) ---
DISTRIBUTOR_BRAND_COLORS = {
    "SKT_ONLY": "lightblue",
    "G2G_ONLY": "pink",
    "BOTH_SKT_G2G": "purple",
    "OTHER": "gray"
}

# --- Step 1: Authenticate BigQuery ---
@st.cache_resource(show_spinner="Authenticating with Google Cloud...")
def get_google_client():
    """Initializes and returns both BigQuery and GCS client, handling both cloud and local environments."""
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
                "auth_provider_x509_cert_url": gcp_secrets[
                    "auth_provider_x509_cert_url"
                ],
                "client_x509_cert_url": gcp_secrets["client_x509_cert_url"],
            }
        )
        GCP_PROJECT_ID = st.secrets["bigquery"]["project"]
        BQ_DATASET = st.secrets["bigquery"]["dataset"]
        REPSLY_DATASET = st.secrets["bigquery"]["repsly_dataset"]
        BASIS_TABLE = st.secrets["bigquery"]["basis"]
        WHITESPACE_TABLE = st.secrets["bigquery"]["whitespace"]
        REPSLY_TABLE = st.secrets["bigquery"]["repsly"]
        POTENTIAL_STORES_TABLE = st.secrets["bigquery"]["potential_stores"]
        BUCKET_NAME = st.secrets["gcs"]["data"]
    except Exception:
        # --- Local fallback for development ---
        GCP_CREDENTIALS_PATH = r"C:\script\skintific-data-warehouse-ea77119e2e7a.json"
        GCP_PROJECT_ID = "skintific-data-warehouse"
        BQ_DATASET = "gt_schema"
        REPSLY_DATASET = "repsly"
        BASIS_TABLE = "master_store_database_basis"
        WHITESPACE_TABLE = "whitespace_long_lat"
        REPSLY_TABLE = "ind_dim_clients"
        POTENTIAL_STORES_TABLE = "indonesia_cosmetic_stores"
        BUCKET_NAME = "public_skintific_storage"
        credentials = service_account.Credentials.from_service_account_file(
            GCP_CREDENTIALS_PATH
        )

    # Initialize both clients
    bq_client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)
    gcs_client = storage.Client(credentials=credentials, project=GCP_PROJECT_ID)

    return (
        bq_client,
        gcs_client,
        GCP_PROJECT_ID,
        BQ_DATASET,
        REPSLY_DATASET,
        BASIS_TABLE,
        WHITESPACE_TABLE,
        REPSLY_TABLE,
        POTENTIAL_STORES_TABLE,
        BUCKET_NAME
    )


# --- Step 2: Load GeoJSON & Nielsen Data ---
@st.cache_data(show_spinner="Loading GeoJSON from GCS...")
def load_geodata(_gcs_client, bucket_name: str, blob_path: str):
    """Loads geospatial data from a parquet file in GCS."""
    bucket = _gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    # Download to bytes and read
    data_bytes = blob.download_as_bytes()
    return gpd.read_parquet(BytesIO(data_bytes))


@st.cache_data(show_spinner="Loading Nielsen data from GCS...")
def load_nielsen(_gcs_client, bucket_name: str, blob_path: str):
    """Loads Nielsen data from an Excel file in GCS."""
    bucket = _gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    # Download to bytes and read
    data_bytes = blob.download_as_bytes()
    return pd.read_excel(BytesIO(data_bytes), sheet_name="Data by Kelurahan")


@st.cache_data(show_spinner="Fetching stores from BigQuery...")
def load_stores(project, bq_dataset, repsly_dataset, basis, whitespace, repsly):
    """Fetches store data with geocoordinates and store grade from BigQuery."""
    query = f"""
    SELECT
        b.city,
        b.region,
        b.cust_id,
        b.store_name,
        COALESCE(NULLIF(b.store_grade_g2g_qtd, ''), 'Other') AS store_grade,
        COALESCE(SAFE_CAST(c.longitude AS FLOAT64), w.longitude, SAFE_CAST(b.longitude AS FLOAT64)) AS longitude,
        COALESCE(SAFE_CAST(c.latitude AS FLOAT64), w.latitude, SAFE_CAST(b.latitude AS FLOAT64)) AS latitude
    FROM `{project}.{bq_dataset}.{basis}` AS b
    LEFT JOIN `{project}.{repsly_dataset}.{repsly}` AS c
        ON UPPER(b.cust_id) = UPPER(REGEXP_EXTRACT(c.code, r'^[^-]*-(.*)'))
    LEFT JOIN `{project}.{bq_dataset}.{whitespace}` AS w
        ON UPPER(b.cust_id) = UPPER(w.store_id)
    WHERE
        COALESCE(SAFE_CAST(c.longitude AS FLOAT64), w.longitude, SAFE_CAST(b.latitude AS FLOAT64)) IS NOT NULL
        AND COALESCE(SAFE_CAST(c.latitude AS FLOAT64), w.latitude, SAFE_CAST(b.latitude AS FLOAT64)) IS NOT NULL
    """
    bq_client, _, _, _, _, _, _, _, _, _ = get_google_client()
    df = bq_client.query(query).to_dataframe()
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df.dropna(subset=["latitude", "longitude"], inplace=True)
    df["store_grade"] = df["store_grade"].astype(str).str.upper().str.strip()
    return df


@st.cache_data(show_spinner="Loading distributor data from GCS...")
def load_distributors(_gcs_client, bucket_name: str, blob_path: str):
    """Loads distributor branch data from an Excel file in GCS."""
    bucket = _gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    data_bytes = blob.download_as_bytes()
    df = pd.read_excel(BytesIO(data_bytes))
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df.dropna(subset=["Latitude", "Longitude"], inplace=True)
    return df


@st.cache_data(show_spinner="Fetching potential stores from BigQuery...")
def load_potential_stores(project, bq_dataset, potential_stores_table):
    """Fetches potential store data from BigQuery."""
    query = f"""
    SELECT
        name,
        latitude,
        longitude,
        province,
        kabupaten,
        region
    FROM `{project}.{bq_dataset}.{potential_stores_table}`
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    bq_client, _, _, _, _, _, _, _, _, _ = get_google_client()
    df = bq_client.query(query).to_dataframe()
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df.dropna(subset=["latitude", "longitude"], inplace=True)
    return df

# --- Step 3: Load All Data Once ---
(
    bq_client,
    gcs_client,
    GCP_PROJECT_ID,
    BQ_DATASET,
    REPSLY_DATASET,
    BASIS_TABLE,
    WHITESPACE_TABLE,
    REPSLY_TABLE,
    POTENTIAL_STORES_TABLE,
    BUCKET_NAME,
) = get_google_client()

# --- Path helper ---
JSON_BLOB_PATH = "external_dataset/indonesia_villages_border_simplified.parquet"
NIELSEN_BLOB_PATH = "external_dataset/Data Nielsen by Kelurahan in Indonesia.xlsx"
DISTRIBUTOR_BLOB_PATH = "external_dataset/Distributor Long Lat.xlsx"

gdf_subdistricts = load_geodata(gcs_client, BUCKET_NAME, JSON_BLOB_PATH)
nielsen_df = load_nielsen(gcs_client, BUCKET_NAME, NIELSEN_BLOB_PATH)
store_df = load_stores(
    GCP_PROJECT_ID,
    BQ_DATASET,
    REPSLY_DATASET,
    BASIS_TABLE,
    WHITESPACE_TABLE,
    REPSLY_TABLE,
)
distributor_df = load_distributors(gcs_client, BUCKET_NAME, DISTRIBUTOR_BLOB_PATH)
potential_stores_df = load_potential_stores(GCP_PROJECT_ID, BQ_DATASET, POTENTIAL_STORES_TABLE)

# --- Step 4: Create Composite Keys & Merge ---
nielsen_df["geo_id"] = (
    nielsen_df["Kabupaten"].astype(str).str.upper()
    + "_"
    + nielsen_df["Kecamatan"].astype(str).str.upper()
    + "_"
    + nielsen_df["Kelurahan"].astype(str).str.upper()
)

merged_gdf = gdf_subdistricts.merge(nielsen_df, on="geo_id", how="left")

# Filter only "Gold" Nielsen Tier
gold_gdf = merged_gdf[merged_gdf["Nielsen Tier"] == "Gold"].copy()
gold_gdf["Nielsen Tier Value"] = gold_gdf["Nielsen Tier"].map({"Gold": 2})

# --- Step 5: Dropdown for Region ---
st.header("🗺️ Filter Map")
available_regions = merged_gdf["Region"].dropna().unique()
# Insert a placeholder at the start of the list for 'no selection'
sorted_regions = ["--- Select Region ---"] + sorted(available_regions)
# Set index=0 to default to the placeholder
selected_region_placeholder = st.selectbox(
    "Select Region",
    sorted_regions,
    index=0  # Default to the placeholder
)

if selected_region_placeholder != "--- Select Region ---":
    selected_region = selected_region_placeholder
    # --- Step 6: Dropdown for Kabupaten ---
    available_kabupaten = merged_gdf[merged_gdf["Region"].astype(str).str.upper() == selected_region.upper()]["Kabupaten"].dropna().str.title().unique()
    sorted_kabupaten = ["--- Select Kabupaten ---"] + sorted(available_kabupaten)

    # Set index=0 to default to the placeholder
    selected_kabupaten_placeholder = st.selectbox(
        "Select Kabupaten", sorted_kabupaten, index=0  # Default to the placeholder
    )

    if selected_kabupaten_placeholder != "--- Select Kabupaten ---":
        selected_kabupaten = selected_kabupaten_placeholder
        with st.spinner(text="Generating Map... This may take a moment."):
            # Filter by both region and kabupaten
            region_gdf = gold_gdf[
                gold_gdf["Region"].astype(str).str.upper() == selected_region.upper()
            ].copy()
            region_stores = store_df[
                store_df["region"].astype(str).str.upper() == selected_region.upper()
            ].copy()
            distributor_df = distributor_df[
                distributor_df["Region"].astype(str).str.upper() == selected_region.upper()
            ]
            potential_stores_df = potential_stores_df[
                potential_stores_df["kabupaten"].astype(str).str.upper() == selected_kabupaten.upper()
            ]

            # --- Step 6: Perform Whitespace Analysis ---
            @st.cache_data(show_spinner="Analyzing whitespace...")
            def analyze_whitespace(_geodataframe, stores_df):
                """
                Performs a spatial analysis to identify 'whitespace' areas (gold-tier subdistricts
                that do not contain any stores).
                """
                if stores_df.empty or _geodataframe.empty:
                    _geodataframe["has_store"] = False
                    return _geodataframe

                # Convert store DataFrame to a GeoDataFrame
                stores_gdf = gpd.GeoDataFrame(
                    stores_df,
                    geometry=gpd.points_from_xy(stores_df.longitude, stores_df.latitude),
                    crs="EPSG:4326",
                )

                # Perform a spatial join to link subdistricts with stores
                sjoin_gdf = gpd.sjoin(_geodataframe, stores_gdf, how="left", predicate="intersects")

                # Identify which subdistricts have at least one store
                _geodataframe["has_store"] = _geodataframe.index.isin(
                    sjoin_gdf.dropna(subset=["cust_id"]).index
                )

                return _geodataframe

            region_gdf_analyzed = analyze_whitespace(region_gdf, region_stores)

            # --- Step 7: Build Folium Map ---
            def style_function(feature):
                """
                Defines the styling for each GeoJSON feature based on its properties.
                Now includes logic for 'whitespace' areas.
                """
                nielsen_tier = feature["properties"].get("Nielsen Tier")
                has_store = feature["properties"].get("has_store")

                if nielsen_tier == "Gold":
                    if has_store:
                        # Gold tier with stores
                        fill_color = "#d2af13"  # Gold
                    else:
                        # Gold tier 'whitespace' (no stores)
                        fill_color = "darkred"
                else:
                    fill_color = "lightgray"  # Other tiers

                return {
                    "fillColor": fill_color,
                    "color": "black",
                    "weight": 0.5,
                    "fillOpacity": 0.7,
                }

            # Define a function to determine the distributor color
            def get_distributor_color(brand_string):
                """Determines the color based on brand presence (SKT and/or G2G)."""
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

            def selected_kabupaten_style_function(feature):
                style = style_function(feature)
                style["color"] = "#4b5004d5"
                style["weight"] = 2.5
                return style

            def other_kabupaten_style_function(feature):
                style = style_function(feature)
                style["fillOpacity"] = 0.2  # lower opacity for other kabupaten
                return style

            def create_legend():
                """Generates a custom HTML legend for the map."""
                legend_html = """
                <div style="position: fixed; 
                            bottom: 50px; left: 50px; width: 260px; 
                            border:2px solid grey; z-index:9999; font-size:14px;
                            background-color:darkgray; opacity:0.9;">
                &nbsp; <b>Map Legend</b> <br>
                &nbsp; <i style="background:#d2af13; border: 1px solid black; width: 10px; height: 10px; display: inline-block;"></i> Gold Tier (With stores) <br>
                &nbsp; <i style="background:darkred; border: 1px solid black; width: 10px; height: 10px; display: inline-block;"></i> Gold Tier (No stores)<br>
                &nbsp; <i style="background:white; border: 1px solid black; width: 10px; height: 10px; display: inline-block;"></i> Other Tier<br>
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
                # &nbsp; <span style="background:transparent; border: 1px solid black; width: 10px; height: 10px; display: inline-block;"></span> <span style="font-size: 10px;">HeatMap shows Grade S density</span> <br>
                return legend_html

            if not region_gdf_analyzed.empty:
                # Create new columns with title-cased names for the tooltip
                region_gdf_analyzed['Region_title'] = region_gdf_analyzed['Region'].str.title()
                region_gdf_analyzed['Kabupaten_title'] = region_gdf_analyzed['Kabupaten'].str.title()
                region_gdf_analyzed['Kecamatan_title'] = region_gdf_analyzed['Kecamatan'].str.title()
                region_gdf_analyzed['Kelurahan_title'] = region_gdf_analyzed['Kelurahan'].str.title()

                # Filter for selected and other Kabupaten
                kabupaten_gdf = region_gdf_analyzed[region_gdf_analyzed["Kabupaten"].astype(str).str.upper() == selected_kabupaten.upper()].copy()
                other_kabupaten_gdf = region_gdf_analyzed[region_gdf_analyzed["Kabupaten"].astype(str).str.upper() != selected_kabupaten.upper()].copy()

                # Get map center
                try:
                    # Initialize center with a safe fallback
                    center = [-2.5, 118.0]  # Default to Indonesia center
                    zoom_start = 9

                    target_gdf = None
                    target_zoom = 9

                    if not kabupaten_gdf.empty:
                        target_gdf = kabupaten_gdf
                        target_zoom = 11
                    elif not region_gdf_analyzed.empty:
                        # Fallback to region center if selected Kabupaten is empty
                        target_gdf = region_gdf_analyzed
                        target_zoom = 9

                    if target_gdf is not None:
                        # 1. Check for valid geometry before projection/centroid calculation
                        target_gdf = target_gdf[target_gdf.geometry.is_valid]

                        if not target_gdf.empty:
                            target_gdf_proj = target_gdf.to_crs(epsg=3857)
                            centroid_proj = target_gdf_proj.geometry.centroid
                            centroid_wgs84 = centroid_proj.to_crs(epsg=4326)

                            # 2. Check if the resulting centroid is valid
                            if not centroid_wgs84.empty:
                                lat = centroid_wgs84.y.iloc[0]
                                lon = centroid_wgs84.x.iloc[0]

                                if pd.notna(lat) and pd.notna(lon):
                                    center = [lat, lon]
                                    zoom_start = target_zoom
                                # Else: Fallback center remains [-2.5, 118.0]

                except Exception as e:
                    # Log the error for debugging, but keep the fallback center
                    st.error(f"Error calculating map center: {e}")

                m = folium.Map(location=center, zoom_start=zoom_start, tiles="CartoDB positron")

                # Add the GeoJSON for other Kabupaten first
                if not other_kabupaten_gdf.empty:
                    folium.features.GeoJson(
                        other_kabupaten_gdf,
                        name="Other Kabupaten",
                        style_function=other_kabupaten_style_function,
                        tooltip=GeoJsonTooltip(
                            fields=[
                                "Region_title",
                                "Kabupaten_title",
                                "Kecamatan_title",
                                "Kelurahan_title",
                                "Nielsen Tier",
                            ],
                            aliases=[
                                "Region:",
                                "Kabupaten:",
                                "Kecamatan:",
                                "Kelurahan:",
                                "Nielsen Tier:",
                            ],
                            localize=True,
                            sticky=False,
                            labels=True,
                        ),
                    ).add_to(m)

                # Add the GeoJSON for the selected Kabupaten with its unique style
                if not kabupaten_gdf.empty:
                    folium.features.GeoJson(
                        kabupaten_gdf,
                        name=f"Selected Kabupaten: {selected_kabupaten}",
                        style_function=selected_kabupaten_style_function,
                        tooltip=GeoJsonTooltip(
                            fields=[
                                "Region_title",
                                "Kabupaten_title",
                                "Kecamatan_title",
                                "Kelurahan_title",
                                "Nielsen Tier",
                            ],
                            aliases=[
                                "Region:",
                                "Kabupaten:",
                                "Kecamatan:",
                                "Kelurahan:",
                                "Nielsen Tier:",
                            ],
                            localize=True,
                            sticky=False,
                            labels=True,
                        ),
                    ).add_to(m)

                # Filter distributor and potential stores to the visible map area (Kabupaten boundary)
                # This requires spatial filtering, which is skipped for simplicity but recommended for performance.

                # Add Distributor markers (Diamond icon)
                for _, row in distributor_df.iterrows():
                    brand_string = row.get("Brand")
                    dist_color = get_distributor_color(brand_string) # Get the color based on Brand

                    folium.Marker(
                        location=[row["Latitude"], row["Longitude"]],
                        icon=folium.Icon(color=dist_color, icon="diamond", prefix="fa"),
                        tooltip=f"Distributor Branch: {row.get('Distributor Name', 'N/A')} ({brand_string})",
                    ).add_to(m)

                # Add Potential Store markers (Custom Icon)
                for _, row in potential_stores_df.iterrows():
                    folium.Marker(
                        location=[row["latitude"], row["longitude"]],
                        icon=folium.Icon(
                            color="cadetblue",
                            icon="shopping-cart",  # Font Awesome Icon for potential store
                            prefix="fa",
                        ),
                        tooltip=f"Potential Store: {row['name']}",
                    ).add_to(m)

                # Add HeatMap for Grade S concentration
                # grade_s_stores = region_stores[region_stores['store_grade'] == 'S']
                # if not grade_s_stores.empty:
                #     s_store_data = grade_s_stores[['latitude', 'longitude']].values.tolist()
                #     HeatMap(s_store_data, name='Grade S Store Density (HeatMap)', radius=15).add_to(m)

                # Add store markers
                # marker_cluster = MarkerCluster(name="Store by Grade").add_to(m)
                for _, row in region_stores.iterrows():
                    store_grade = row.get('store_grade', 'Other')
                    grade_color = STORE_GRADE_COLORS.get(store_grade, STORE_GRADE_COLORS['Other'])

                    folium.CircleMarker(
                        location=[row["latitude"], row["longitude"]],
                        radius=6,
                        color="black",
                        weight=1,
                        fill=True,
                        fill_color=grade_color,
                        fill_opacity=1.0,
                        tooltip=f"Store: {row['cust_id']} - {row['store_name']} (Grade: {store_grade})",
                    ).add_to(m)

                # Add the legend to the map
                m.get_root().html.add_child(folium.Element(create_legend()))
                folium.LayerControl().add_to(m)

                # --- Step 8: Show Map in Streamlit ---
                st_map = st_folium(m, width=1200, height=700, returned_objects=[])

                # --- Step 9: Create Summary Table ---
                st.subheader("📊 Area Summary")

                @st.cache_data(show_spinner="Generating summary table...")
                def create_summary_table(_region_gdf, _region_stores, selected_kabupaten):
                    """Creates a summary table with store counts by area."""
                    # Create a copy of the region data
                    summary_gdf = _region_gdf.copy()

                    grade_columns = list(STORE_GRADE_COLORS.keys())

                    # Convert stores to GeoDataFrame for spatial analysis
                    if not _region_stores.empty:
                        stores_gdf = gpd.GeoDataFrame(
                            _region_stores,
                            geometry=gpd.points_from_xy(_region_stores.longitude, _region_stores.latitude),
                            crs="EPSG:4326",
                        )

                        # Perform spatial join to count stores per area
                        joined_gdf = gpd.sjoin(summary_gdf, stores_gdf, how="left", predicate="intersects")

                        # Count stores per area
                        store_counts = joined_gdf.groupby(joined_gdf.index).size().fillna(0)
                        summary_gdf["Number of Stores"] = store_counts

                        # Calculate Grade Counts
                        for grade in grade_columns:
                            # Filter stores for the current grade (case-insensitive)
                            grade_filtered_gdf = joined_gdf[joined_gdf['store_grade'].str.upper() == grade.upper()]
                            # Count the occurrences of this grade for each Kelurahan (index)
                            grade_counts = grade_filtered_gdf.groupby(grade_filtered_gdf.index).size().reindex(summary_gdf.index, fill_value=0)
                            summary_gdf[f'Stores Grade {grade}'] = grade_counts
                    else:
                        summary_gdf["Number of Stores"] = 0
                        for grade in grade_columns:
                            summary_gdf[f"Stores Grade {grade}"] = 0

                    # Group by area and sum store counts/grades
                    group_cols = ['Region', 'Kabupaten', 'Kecamatan', 'Kelurahan']
                    agg_dict = {'Number of Stores': 'sum'}
                    for grade in grade_columns:
                        agg_dict[f'Stores Grade {grade}'] = 'sum'

                    summary_df = summary_gdf.groupby(group_cols).agg(agg_dict).reset_index()

                    # Filter by selected kabupaten if specified
                    if selected_kabupaten:
                        summary_df = summary_df[summary_df['Kabupaten'].astype(str).str.upper() == selected_kabupaten.upper()]

                    # Sort by number of stores (descending)
                    summary_df = summary_df.sort_values('Number of Stores', ascending=False)

                    # Select and rename columns for display
                    display_cols = ['Region', 'Kabupaten', 'Kecamatan', 'Kelurahan', 'Number of Stores'] + [f'Stores Grade {g}' for g in grade_columns]
                    summary_df = summary_df[display_cols]

                    return summary_df

                # Generate summary table
                summary_df = create_summary_table(region_gdf_analyzed, region_stores, selected_kabupaten)

                if not summary_df.empty:
                    # Configure column display
                    column_config_dict = {
                        "Region": st.column_config.TextColumn("Region", width="medium"),
                        "Kabupaten": st.column_config.TextColumn("Kabupaten", width="medium"),
                        "Kecamatan": st.column_config.TextColumn("Kecamatan", width="medium"),
                        "Kelurahan": st.column_config.TextColumn("Kelurahan", width="medium"),
                        "Number of Stores": st.column_config.NumberColumn("Total Stores", width="small")
                    }
                    # Add grade columns to config
                    for grade in list(STORE_GRADE_COLORS.keys()):
                        column_config_dict[f'Stores Grade {grade}'] = st.column_config.NumberColumn(f'Grade {grade}', width="small")

                    # Display the table
                    st.dataframe(
                        summary_df,
                        column_config=column_config_dict,
                        hide_index=True,
                        use_container_width=True
                    )

                    # --- Step 10: Excel Download Functionality ---
                    def get_excel_download_link(df):
                        """Generate Excel download link."""
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            df.to_excel(writer, sheet_name='Store Summary', index=False)
                        output.seek(0)
                        return output

                    # Create download button
                    excel_data = get_excel_download_link(summary_df)
                    st.download_button(
                        label="📥 Download Excel",
                        data=excel_data,
                        file_name=f"store_summary_{selected_region}_{selected_kabupaten or 'all'}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                else:
                    st.info("No data available for the summary table.")

            else:
                st.warning(f"No data available for {selected_region}")
    else:
        # Inform the user to select a Kabupaten
        st.info("Please select a Kabupaten to view the map and area summary.")
else:
    # Inform the user to select a Region (although selected_region will likely not be None
    # if available_regions is not empty, this is for robustness)
    st.info("Please select a Region.")
