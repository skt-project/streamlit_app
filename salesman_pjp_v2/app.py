"""
Salesman PJP v2 — Entry point.

Run with:
    streamlit run D:/GitHub/streamlit_app/salesman_pjp_v2/app.py

Secrets required in .streamlit/secrets.toml (or Streamlit Cloud):
    [connections.bigquery]
    type = "service_account"
    project_id = "skintific-data-warehouse"
    private_key_id = "..."
    private_key = "..."
    client_email = "..."
    client_id = "..."
    auth_uri = "https://accounts.google.com/o/oauth2/auth"
    token_uri = "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
    client_x509_cert_url = "..."

    [distributor_passwords]
    DST171 = "password_here"
    # ... add all distributors

    pjp_input_deadline = "2026-07-31"   # update monthly; leave blank to disable lock
"""

import uuid
import streamlit as st

# ── Page config (must be first Streamlit call) ─────────────────────────────────
st.set_page_config(
    page_title="Salesman & PJP Template v2",
    page_icon="📋",
    layout="wide",
)

# ── Session ID (for audit log) ─────────────────────────────────────────────────
if "_v2_session_id" not in st.session_state:
    st.session_state["_v2_session_id"] = str(uuid.uuid4())[:8]

# ── Imports (after page config) ────────────────────────────────────────────────
from salesman_pjp_v2.auth import render_password_gate, render_deadline_gate
from salesman_pjp_v2.data_loaders import load_distributor_data, load_store_data, build_lookup_tables
from salesman_pjp_v2.pages import salesman_page, pjp_page

# ── Sidebar: navigation + distributor selector ─────────────────────────────────
with st.sidebar:
    st.title("📋 G2G Template Manager v2")
    st.markdown("---")
    selected_page = st.radio(
        "Navigasi",
        ["👥 Kelola Salesman", "🗓️ PJP Template"],
        label_visibility="collapsed",
    )
    st.markdown("---")

# ── Load shared reference data ─────────────────────────────────────────────────
try:
    dist_df      = load_distributor_data()
    store_df     = load_store_data()
    distributor_map, _, _ = build_lookup_tables(dist_df)
except Exception as e:
    st.error(f"Gagal memuat data dari Database: {e}")
    st.stop()

# ── Distributor selector ───────────────────────────────────────────────────────
dist_labels = [
    f"{row['distributor_code']} — {row['distributor_name']}"
    for _, row in dist_df.sort_values("distributor_name").iterrows()
]
code_from_label = {
    f"{row['distributor_code']} — {row['distributor_name']}": row["distributor_code"]
    for _, row in dist_df.iterrows()
}

with st.sidebar:
    st.markdown("### 🏢 Pilih Distributor")
    selected_label = st.selectbox(
        "Distributor",
        ["— Pilih distributor —"] + dist_labels,
        key="v2_dist_selector",
        label_visibility="collapsed",
    )

if selected_label == "— Pilih distributor —":
    st.title("📋 Salesman & PJP Template Manager v2")
    st.info("👈 Pilih distributor di sidebar untuk melanjutkan.")
    st.stop()

selected_dist_code = code_from_label[selected_label]
selected_dist_name = dist_df.loc[
    dist_df["distributor_code"] == selected_dist_code, "distributor_name"
].iloc[0]

# Clear auth + salesman cache on distributor switch
prev = st.session_state.get("v2_prev_dist_code")
if prev is not None and prev != selected_dist_code:
    st.session_state.pop(f"v2_auth_{selected_dist_code}", None)
    st.session_state.pop("v2_salesman_df",  None)
    st.session_state.pop("v2_cached_dist",  None)
    st.session_state.pop("v2_eff_df",       None)
    st.session_state.pop("v2_eff_dist",     None)
st.session_state["v2_prev_dist_code"] = selected_dist_code

with st.sidebar:
    st.success(f"**{selected_dist_name}**\n\n`{selected_dist_code}`")
    st.markdown("---")
    st.caption("Salesman & PJP Template Manager v2 · G2G")

# ── Gates: password → deadline ─────────────────────────────────────────────────
if not render_password_gate(selected_dist_code, selected_dist_name):
    st.stop()

if not render_deadline_gate():
    st.stop()

# ── Page routing ───────────────────────────────────────────────────────────────
if selected_page == "👥 Kelola Salesman":
    salesman_page.render(selected_dist_code, selected_dist_name, dist_df)
else:
    pjp_page.render(
        selected_dist_code, selected_dist_name,
        dist_df, store_df, distributor_map,
    )
