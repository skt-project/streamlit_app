import streamlit as st
import pandas as pd
import uuid
import math
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------
st.set_page_config(page_title="SFA Attendance App", layout="wide")

st.title("📍 SFA Salesman Attendance")

# --------------------------------------------------
# BIGQUERY CONNECTION
# --------------------------------------------------
gcp_secrets = dict(st.secrets["connections"]["bigquery"])
gcp_secrets["private_key"] = gcp_secrets["private_key"].replace("\\n", "\n")

credentials = service_account.Credentials.from_service_account_info(gcp_secrets)

PROJECT_ID = st.secrets["bigquery"]["project"]
DATASET = st.secrets["bigquery"]["dataset"]
ATT_TABLE = "sfa_attendance"

bq_client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)

# --------------------------------------------------
# LOAD STORE MASTER
# --------------------------------------------------
@st.cache_data(ttl=600)
def load_store_master():
    query = f"""
        SELECT
            cust_id,
            store_name,
            IFNULL(NULLIF(se_skt, ''), se_tph) AS salesman_name,
            latitude,
            longitude
        FROM `{PROJECT_ID}.{DATASET}.master_store_database_basis`
        WHERE customer_status = 'Active'
    """
    df = bq_client.query(query).to_dataframe()

    df = df.dropna(subset=["salesman_name", "latitude", "longitude"])

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    df = df.dropna(subset=["latitude", "longitude"])

    return df

store_df = load_store_master()

# --------------------------------------------------
# LOGIN SIMPLE (Salesman Name Based)
# --------------------------------------------------
salesman_list = sorted(store_df["salesman_name"].unique())

salesman = st.selectbox("Select Your Name", salesman_list)

if not salesman:
    st.stop()

# Filter stores by salesman
salesman_stores = store_df[
    store_df["salesman_name"] == salesman
]

store_options = salesman_stores["store_name"].unique()

selected_store = st.selectbox("Select Store to Visit", store_options)

store_data = salesman_stores[
    salesman_stores["store_name"] == selected_store
].iloc[0]

store_lat = store_data["latitude"]
store_lon = store_data["longitude"]

# --------------------------------------------------
# DEVICE GPS INPUT
# --------------------------------------------------
st.subheader("📱 Capture Your Current Location")

device_lat = st.number_input("Your Latitude")
device_lon = st.number_input("Your Longitude")

# --------------------------------------------------
# HAVERSINE DISTANCE FUNCTION
# --------------------------------------------------
def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371000  # meters

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)

    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(delta_lambda / 2) ** 2

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance

# --------------------------------------------------
# SESSION STATE
# --------------------------------------------------
if "checkin_time" not in st.session_state:
    st.session_state.checkin_time = None

# --------------------------------------------------
# CHECK-IN BUTTON
# --------------------------------------------------
if st.button("✅ Check In"):

    if device_lat == 0 or device_lon == 0:
        st.error("Please input your GPS location")
        st.stop()

    distance = calculate_distance(
        store_lat, store_lon,
        device_lat, device_lon
    )

    if distance > 100:
        st.error(f"❌ You are too far from store ({round(distance,2)} meters)")
        st.stop()

    st.session_state.checkin_time = datetime.now()

    st.success(f"Checked in successfully! Distance: {round(distance,2)} meters")

# --------------------------------------------------
# CHECK-OUT BUTTON
# --------------------------------------------------
if st.session_state.checkin_time:

    st.info(f"Checked in at: {st.session_state.checkin_time}")

    if st.button("🏁 Check Out"):

        checkout_time = datetime.now()
        duration = (
            checkout_time - st.session_state.checkin_time
        ).total_seconds() / 60

        distance = calculate_distance(
            store_lat, store_lon,
            device_lat, device_lon
        )

        visit_id = str(uuid.uuid4())

        record = {
            "visit_id": visit_id,
            "visit_date": datetime.now().date().isoformat(),
            "salesman_name": salesman,
            "cust_id": store_data["cust_id"],
            "store_name": selected_store,
            "store_latitude": store_lat,
            "store_longitude": store_lon,
            "checkin_time": st.session_state.checkin_time.isoformat(),
            "checkout_time": checkout_time.isoformat(),
            "visit_duration_minutes": duration,
            "checkin_latitude": device_lat,
            "checkin_longitude": device_lon,
            "distance_meters": distance,
            "is_valid_visit": distance <= 100
        }

        errors = bq_client.insert_rows_json(
            f"{PROJECT_ID}.{DATASET}.{ATT_TABLE}",
            [record]
        )

        if errors:
            st.error(errors)
        else:
            st.success("✅ Visit saved successfully")

        st.session_state.checkin_time = None
