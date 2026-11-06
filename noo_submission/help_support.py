"""Help & Support: popup tiket yang dipakai bersama oleh halaman login, app_submission, dan app_main."""
import datetime
import re
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import streamlit as st

import shared as _shared
from shared import sb_insert, catat_riwayat

WIB = ZoneInfo("Asia/Jakarta")
TABLE_HELP = "help_support"
# key = nama kolom di Supabase (sesuaikan dengan schema-mu)
HELP_KOLOM = {
    "issue": "issue", "username": "username", "nomor_wa": "nomor_wa",
    "generated_at": "generated_at", "priority": "priority", "status": "status",
    "title": "title",
}
OPSI_PRIORITAS = ["Low", "Medium", "High"]
OPSI_TITLE = [
    "Account Issue",
    "Password Issue",
    "Upload / File Issue",
    "NOO Submission Issue",
    "SKU Mapping Issue",
    "SPV Confirmation Issue",
    "Store ID / Generate ID Issue",
    "Distributor / Mapping Issue",
    "Data Not Showing / Incorrect Data",
    "Bug / Error Message",
    "Feature Request",
    "Other",
]

@st.cache_data(ttl=30, show_spinner=False)
def ambil_tiket_help(username: str) -> pd.DataFrame:
    r = requests.get(
        f"{_shared.SUPABASE_URL}/rest/v1/{TABLE_HELP}",
        headers={**_shared._SB_HEADERS, "Prefer": ""},
        params={"select": "*", HELP_KOLOM["username"]: f"eq.{username}",
                "order": f"{HELP_KOLOM['generated_at']}.desc", "limit": 200},
        timeout=20)
    r.raise_for_status()
    return pd.DataFrame(r.json())

@st.dialog("Help & Support", width="small")
def dialog_help_support():
    ss   = st.session_state
    # tampilan ringkas (juga menimpa gaya input besar milik halaman login)
    st.markdown("""<style>
[data-testid="stDialog"] [data-testid="stForm"] { padding: 12px 14px !important; }
[data-testid="stDialog"] [data-testid="stVerticalBlock"] { gap: 0.5rem !important; }
[data-testid="stDialog"] [data-testid="stWidgetLabel"] p { font-size: 12px !important; }
[data-testid="stDialog"] [data-baseweb="input"], [data-testid="stDialog"] [data-baseweb="select"] > div {
  min-height: 34px !important; height: 34px !important; border-radius: 8px !important; }
div[data-testid="stDialog"] [data-testid="stTextInput"] div[data-baseweb="input"] { height: 36px !important; min-height: 36px !important; }
div[data-testid="stDialog"] [data-testid="stTextInput"] input { font-size: 13px !important; }
[data-testid="stDialog"] [data-baseweb="select"] div { font-size: 13px !important; }
[data-testid="stDialog"] textarea { font-size: 13px !important; min-height: 0 !important; }
[data-testid="stDialog"] [data-testid="stFormSubmitButton"] button { height: 34px !important; min-height: 34px !important; }
</style>""", unsafe_allow_html=True)
    user = ss.get("user") or {}
    uname = str(user.get("username", "")).strip()
    sudah_login = bool(uname)
    if sudah_login:
        tab_baru, tab_riwayat = st.tabs([":material/edit_note: New Ticket", ":material/history: My Tickets"])
    else:                                   # dibuka dari halaman login: belum ada username -> diisi manual
        (tab_baru,) = st.tabs([":material/edit_note: New Ticket"])
        tab_riwayat = None

    with tab_baru:
        with st.form("form_help_support", clear_on_submit=True):
            if not sudah_login:
                uname_input = st.text_input("Your Username *", placeholder="Username you use to log in")
            c1, c2 = st.columns([2, 1])
            nomor_wa  = c1.text_input("WhatsApp Number *", placeholder="62xxxxxxxxxx", max_chars=13,
                                      help="Numbers only, 11-13 digits.")
            prioritas = c2.selectbox("Priority *", OPSI_PRIORITAS, index=1)
            judul     = st.selectbox("Title *", OPSI_TITLE, index=None, placeholder="Select the issue category")
            issue     = st.text_area("Issue *", height=90,
                                     placeholder="Jelaskan kendala / pertanyaan, langkah yang dilakukan, kode toko terkait, dll.")
            kirim = st.form_submit_button(":material/send: Submit", type="primary", width="stretch")

        if kirim:
            if not sudah_login:
                uname = uname_input.strip().lower()
            wa_input = nomor_wa.strip()
            wa = wa_input
            if wa.startswith("0"):
                wa = "62" + wa[1:]                     # 08xx -> 628xx
            if not issue.strip() or not wa_input or not judul or not uname:
                st.warning("Username, Title, WhatsApp Number and Issue are required.")
            elif not (wa_input.isascii() and wa_input.isdigit()):
                st.warning("WhatsApp Number must contain digits only (no spaces, +, or dashes).")
            elif not (11 <= len(wa_input) <= 13):
                st.warning("WhatsApp Number must be 11-13 digits.")
            else:
                row = {
                    HELP_KOLOM["title"]:        judul,
                    HELP_KOLOM["issue"]:        issue.strip(),
                    HELP_KOLOM["username"]:     uname,
                    HELP_KOLOM["nomor_wa"]:     wa,
                    HELP_KOLOM["generated_at"]: datetime.datetime.now(WIB).isoformat(timespec="seconds"),  # WIB (+07:00)
                    HELP_KOLOM["priority"]:     prioritas,
                    HELP_KOLOM["status"]:       "In Progress",
                }
                with st.spinner("Sending..."):
                    result = sb_insert(TABLE_HELP, [row])
                if isinstance(result, dict) and "_error" in result:
                    st.error(f"Failed to send: {result['_error']}")
                else:
                    ambil_tiket_help.clear()
                    catat_riwayat("Help & Support", f"{judul} | {prioritas} - {issue.strip()[:40]}", pengguna=uname)
                    ss.pesan_toast = "Ticket submitted. Thank you!"
                    st.rerun()      # menutup dialog

    if tab_riwayat is None:
        return
    with tab_riwayat:
        try:
            df = ambil_tiket_help(uname)
        except Exception as e:
            st.error(f"Failed to load tickets: {e}"); return
        if df.empty:
            st.info("No tickets yet."); return
        kg = HELP_KOLOM["generated_at"]
        if kg in df.columns:
            df[kg] = (pd.to_datetime(df[kg], errors="coerce", utc=True, format="ISO8601")
                        .dt.tz_convert(WIB).dt.strftime("%d %b %Y %H:%M WIB"))
        kolom = [HELP_KOLOM[k] for k in ("generated_at","title","issue","nomor_wa","priority","status")
                 if HELP_KOLOM[k] in df.columns]
        st.dataframe(df[kolom], hide_index=True, width="stretch")
