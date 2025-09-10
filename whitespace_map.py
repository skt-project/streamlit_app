import pandas as pd
import geopandas as gpd
import folium
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from folium.plugins import MarkerCluster
from folium import GeoJsonTooltip
import streamlit as st
from streamlit_folium import st_folium

# --- Streamlit App Config ---
st.set_page_config(layout="wide")
st.title("Indonesia Nielsen Store Map")

# --- Step 1: Authenticate BigQuery ---
@st.cache_resource(show_spinner="Authenticating with BigQuery...")
def get_bigquery_client():
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
        BQ_TABLE = st.secrets["bigquery"]["bq_table"]

    except Exception:
        # --- Local fallback for development ---
        GCP_CREDENTIALS_PATH = r"C:\script\skintific-data-warehouse-ea77119e2e7a.json"
        GCP_PROJECT_ID = "skintific-data-warehouse"
        BQ_DATASET = "gt_schema"
        BQ_TABLE = "master_store_database_basis"
        credentials = service_account.Credentials.from_service_account_file(
            GCP_CREDENTIALS_PATH
        )

    client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)
    return client, GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE

# --- Step 2: Load GeoJSON & Nielsen Data ---
@st.cache_data(show_spinner="Loading GeoJSON...")
def load_geojson(path: str):
    with open(path, "r", encoding="utf-8") as f:
        geojson_data = json.load(f)
    gdf = gpd.GeoDataFrame.from_features(geojson_data["features"])
    gdf.set_crs(epsg=4326, inplace=True)
    return gdf


@st.cache_data(show_spinner="Loading Nielsen data...")
def load_nielsen(path: str):
    return pd.read_excel(path, sheet_name="Data by Kelurahan")

@st.cache_data(show_spinner="Fetching stores from BigQuery...")
def load_stores(project, dataset, table):
    query = f"""
    SELECT region, cust_id, store_name, longitude, latitude
    FROM `{project}.{dataset}.{table}`
    WHERE (longitude IS NOT NULL AND longitude <> "")
    AND (latitude IS NOT NULL AND latitude <> "")
    """
    client, _, _, _ = get_bigquery_client()
    df = client.query(query).to_dataframe()
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df.dropna(subset=["latitude", "longitude"], inplace=True)
    return df

# --- Step 3: Load All Data Once ---
client, GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE = get_bigquery_client()
gdf_subdistricts = load_geojson(r"C:\script\indonesia_villages_border_simplified.json")
nielsen_df = load_nielsen(r"C:\Users\Mikael Andrew\Downloads\Excel\Data Nielsen by Kelurahan in Indonesia.xlsx")
# gdf_subdistricts = load_geojson("indonesia_villages_border_simplified.json")
# nielsen_df = load_nielsen("Data Nielsen by Kelurahan in Indonesia.xlsx")
store_df = load_stores(GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE)

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
available_regions = merged_gdf["Region"].dropna().unique()
selected_region = st.selectbox("Select Region", sorted(available_regions))

region_gdf = gold_gdf[gold_gdf["Region"].astype(str).str.upper() == selected_region.upper()].copy()
region_stores = store_df[store_df["region"].astype(str).str.upper() == selected_region.upper()].copy()

# --- Step 6: Build Folium Map ---
# m = folium.Map(location=[-2.5, 118.0], zoom_start=5, tiles="CartoDB positron")

def style_function(feature):
    nielsen_tier = feature["properties"].get("Nielsen Tier")
    fill_color = "#d2af13" if nielsen_tier == "Gold" else "darkgray"
    return {
        "fillColor": fill_color,
        "color": "black",
        "weight": 0.5,
        "fillOpacity": 0.7,
    }

if not region_gdf.empty:
    # Get map center from region centroid
    try:
        region_gdf_proj = region_gdf.to_crs(epsg=3857)
        centroid_proj = region_gdf_proj.geometry.centroid
        centroid_wgs84 = centroid_proj.to_crs(epsg=4326)
        center = [centroid_wgs84.y.iloc[0], centroid_wgs84.x.iloc[0]]
    except Exception:
        center = [-2.5, 118.0]  # fallback to Indonesia center

    m = folium.Map(location=center, zoom_start=9, tiles="CartoDB positron")

    folium.features.GeoJson(
        region_gdf,
        name="Nielsen Tiers",
        style_function=style_function,
        tooltip=GeoJsonTooltip(
            fields=["Region", "Kabupaten", "Kecamatan", "Kelurahan", "Nielsen Tier"],
            aliases=["Region:", "Kabupaten:", "Kecamatan:", "Kelurahan:", "Nielsen Tier:"],
            localize=True,
            sticky=False,
            labels=True,
        ),
    ).add_to(m)

    # Add store markers
    marker_cluster = MarkerCluster().add_to(m)
    for _, row in region_stores.iterrows():
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=4,
            color="black",
            fill=True,
            fill_color="blue",
            fill_opacity=1.0,
            tooltip=f"Store: {row['cust_id']} - {row['store_name']}",
        ).add_to(marker_cluster)

    folium.LayerControl().add_to(m)

    # --- Step 7: Show Map in Streamlit ---
    st_map = st_folium(m, width=1200, height=700, returned_objects=[])

else:
    st.warning(f"No data available for {selected_region}")
