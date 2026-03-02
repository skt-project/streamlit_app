import streamlit as st
import pandas as pd
import uuid
import math
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
import streamlit.components.v1 as components

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
ATT_TABLE = st.secrets["bigquery"]["attendance_table"]

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
# SALESMAN SELECT
# --------------------------------------------------
salesman_list = sorted(store_df["salesman_name"].unique())
salesman = st.selectbox("Select Your Name", salesman_list)

salesman_stores = store_df[
    store_df["salesman_name"] == salesman
]

selected_store = st.selectbox(
    "Select Store to Visit",
    salesman_stores["store_name"].unique()
)

store_data = salesman_stores[
    salesman_stores["store_name"] == selected_store
].iloc[0]

store_lat = float(store_data["latitude"])
store_lon = float(store_data["longitude"])

# --------------------------------------------------
# AUTO GPS CAPTURE (JAVASCRIPT)
# --------------------------------------------------
st.subheader("📱 Capture Your Real-Time Location")

gps_html = """
<script>
navigator.geolocation.getCurrentPosition(
    function(position) {
        const latitude = position.coords.latitude;
        const longitude = position.coords.longitude;
        const accuracy = position.coords.accuracy;

        const data = {
            latitude: latitude,
            longitude: longitude,
            accuracy: accuracy
        };

        window.parent.postMessage(
            { type: "streamlit:setComponentValue", value: data },
            "*"
        );
    },
    function(error) {
        alert("Please enable location access.");
    }
);
</script>
"""

gps_data = components.html(gps_html, height=0)

if gps_data:
    device_lat = gps_data["latitude"]
    device_lon = gps_data["longitude"]
    accuracy = gps_data["accuracy"]

    st.success(f"📍 Location Captured (Accuracy: {round(accuracy,1)}m)")
    st.write(f"Latitude: {device_lat}")
    st.write(f"Longitude: {device_lon}")
else:
    st.warning("Waiting for GPS permission...")

# --------------------------------------------------
# DISTANCE FUNCTION
# --------------------------------------------------
def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi/2)**2 + \
        math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2

    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

# --------------------------------------------------
# SESSION STATE
# --------------------------------------------------
if "checkin_time" not in st.session_state:
    st.session_state.checkin_time = None

# --------------------------------------------------
# CHECK IN
# --------------------------------------------------
if gps_data and st.button("✅ Check In"):

    distance = calculate_distance(
        store_lat, store_lon,
        device_lat, device_lon
    )

    if distance > 100:
        st.error(f"❌ Too far from store ({round(distance,2)} meters)")
        st.stop()

    st.session_state.checkin_time = datetime.now()
    st.session_state.checkin_lat = device_lat
    st.session_state.checkin_lon = device_lon

    st.success("✅ Check-in successful")

# --------------------------------------------------
# CHECK OUT
# --------------------------------------------------
if st.session_state.checkin_time:

    st.info(f"Checked in at {st.session_state.checkin_time}")

    if st.button("🏁 Check Out"):

        checkout_time = datetime.now()
        duration = (
            checkout_time - st.session_state.checkin_time
        ).total_seconds() / 60

        distance = calculate_distance(
            store_lat, store_lon,
            st.session_state.checkin_lat,
            st.session_state.checkin_lon
        )

        record = {
            "visit_id": str(uuid.uuid4()),
            "visit_date": datetime.now().date().isoformat(),
            "salesman_name": salesman,
            "cust_id": store_data["cust_id"],
            "store_name": selected_store,
            "store_latitude": store_lat,
            "store_longitude": store_lon,
            "checkin_time": st.session_state.checkin_time.isoformat(),
            "checkout_time": checkout_time.isoformat(),
            "visit_duration_minutes": duration,
            "checkin_latitude": st.session_state.checkin_lat,
            "checkin_longitude": st.session_state.checkin_lon,
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
            st.success("🎉 Visit saved successfully")

        st.session_state.checkin_time = None
