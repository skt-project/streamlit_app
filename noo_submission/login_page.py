import base64
import re
from pathlib import Path

import datetime

import bcrypt
import requests
import streamlit as st
import streamlit.components.v1 as components

from shared import SUPABASE_URL, _SB_HEADERS, sb_insert, muat_distri_code_map

TABLE_USER = "users"        
TAGGING_RSA = "RSA"
TAGGING_BD = "BD_SUPPORT"
TAGGING_ADMIN_DISTRI = "Admin Distributor"
TAGGING_USER = "user"   

VIDEO_URL = (
    "https://id.glad2glow.com/cdn/shop/videos/c/vp/d5b2903bb1aa433c8941c13212b84286/d5b2903bb1aa433c8941c13212b84286.HD-1080p-2.5Mbps-90325375.mp4?v=0"
)
FORGOT_FORM_URL = "https://docs.google.com/forms/d/1E5qhMSQ8kSu6WJQzIGbpeBVr0oMU8mjkWyI00VdRUyI/viewform"

def validasi_username(username: str) -> str:
    u = username.strip().upper()
    if u == "RSA":
        return TAGGING_RSA
    if u == "BD_SUPPORT":
        return TAGGING_BD
    try:
        kode_valid = set(muat_distri_code_map())
    except Exception as e:
        raise ValueError(f"Gagal memuat daftar distributor: {e}")
    if u in kode_valid:
        return TAGGING_ADMIN_DISTRI
    raise ValueError("Username not valid")


def _logo_src() -> str:
    local = Path(__file__).parent / "logo.png"
    if local.exists():
        b64 = base64.b64encode(local.read_bytes()).decode()
        return f"data:image/png;base64,{b64}"
    return "https://glad2glow.com/cdn/shop/files/logo-svg.svg?v=1777280269&width=280"


def _cari_user(**filter_eq) -> list:
    params = {"select": "username,password,email,tagging", "limit": 1}
    params.update({k: f"eq.{v}" for k, v in filter_eq.items()})
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{TABLE_USER}",
        headers={**_SB_HEADERS, "Prefer": ""}, params=params, timeout=10,
    )
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------- CSS
CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

:root{
  --rose:#c98283; --rose-deep:#a95e69; --gold:#bd9a65;
  --ink:#45343b; --muted:#8c7a80; --line:rgba(174,126,132,.28);
  --veil:.15;
}

/* ---- page: putih, tanpa chrome streamlit ---- */
html, body, [data-testid="stApp"], [data-testid="stAppViewContainer"]{
  background:#ffffff !important;
  font-family:Inter, system-ui, sans-serif;
}
footer, [data-testid="stSidebar"]{ display:none !important; }
header[data-testid="stHeader"]{ background:transparent !important; }
[data-testid="stMainBlockContainer"], .block-container{
  max-width:1080px !important; padding:32px 18px !important;
}

/* ---- shell (card) ---- */
.st-key-shell{
  border:1px solid #f1e4e4; border-radius:30px; overflow:hidden;
  box-shadow:0 28px 90px rgba(120,74,78,.14); background:#fff;
  animation:fadeUp .8s ease both;
}
.st-key-shell [data-testid="stHorizontalBlock"]{ gap:0 !important; align-items:stretch; }
.st-key-shell [data-testid="stColumn"]{ padding:0 !important; }
.st-key-shell [data-testid="stVerticalBlock"]{ gap:0 !important; }

/* ---- brand panel (kiri) ---- */
.st-key-brand{ position:relative; min-height:820px; overflow:hidden; background:#f7e9e8; }
.st-key-brand iframe{ position:absolute; inset:0; width:100%; height:100% !important; border:0; }
.st-key-brand .bg-video{
  position:absolute; inset:0; width:100%; height:100%; object-fit:cover; z-index:0;
}
.st-key-brand .veil{
  position:absolute; inset:0; z-index:1;
  background:linear-gradient(150deg, rgba(255,248,245,var(--veil)), rgba(237,205,207,calc(var(--veil) * .6)));
}
.brand-inner{
  position:relative; z-index:2; min-height:650px; padding:clamp(32px,5vw,64px);
  display:flex; flex-direction:column; justify-content:space-between;
}
.brand-inner img.logo{ height:44px; width:auto; display:block; }
.tagline{ font-size:11px; text-transform:uppercase; letter-spacing:3px; margin-top:12px; color:#7d5f64; }
.brand-copy{ max-width:390px; }
.eyebrow{ text-transform:uppercase; letter-spacing:3px; font-size:10px; color:#8f6656; font-weight:700; }
.brand-copy h1{
  font-size:clamp(35px,4.4vw,54px); line-height:1.08; letter-spacing:-2px; font-weight:550;
  margin:18px 0; padding:0; color:var(--ink);
}
.brand-copy h1 em{ font-family:Georgia,serif; font-weight:400; color:var(--rose-deep); }
.brand-copy p{ color:#6f5a60; line-height:1.8; font-size:15px; max-width:330px; margin:0; }
.brand-footer{
  color:#7d5f64; font-size:10px; letter-spacing:2.6px; text-transform:uppercase;
  display:flex; align-items:center; gap:12px;
}
.brand-footer::after{ content:""; width:50px; height:1px; background:var(--gold); }

/* ---- form panel (kanan) ---- */
.st-key-formp{ padding:clamp(28px,5vw,64px) clamp(28px,5vw,64px); background:#fff; min-height:650px; justify-content:center; }
.form-logo{ height:70px; width:auto; display:block; margin:0 auto 28px; }
.welcome{ font-size:12px; letter-spacing:1.8px; text-transform:uppercase; color:var(--rose-deep); font-weight:700; }
.form-title{ font-size:35px; letter-spacing:-1.2px; margin:12px 0 8px; font-weight:560; color:var(--ink); line-height:1.15; }
.subtitle{ margin:0 0 30px; color:var(--muted); font-size:14px; line-height:1.6; }
.fine-print{ text-align:center; font-size:11px; color:#a39498; margin-top:27px; line-height:1.6; }
.fine-print a{ color:var(--rose-deep); text-decoration:none; }

/* inputs */
.st-key-formp [data-testid="stTextInput"] input,
div[data-testid="stDialog"] [data-testid="stTextInput"] input{
  height:58px; padding:0 16px; border-radius:13px; font-size:14px; color:var(--ink);
  background:rgba(255,255,255,.9); border:1px solid var(--line);
}
.st-key-formp [data-testid="stTextInput"] div[data-baseweb="input"],
div[data-testid="stDialog"] [data-testid="stTextInput"] div[data-baseweb="input"]{
  height:58px !important; border:1px solid var(--line) !important;
  border-radius:13px !important; background:rgba(255,255,255,.9) !important;
}
.st-key-formp [data-testid="stTextInput"] input,
div[data-testid="stDialog"] [data-testid="stTextInput"] input{
  height:100% !important; border:0 !important;
}
.st-key-formp [data-testid="stVerticalBlock"]{ gap:.75rem !important; }
.st-key-formp [data-testid="stTextInput"] input:focus,
div[data-testid="stDialog"] [data-testid="stTextInput"] input:focus{
  border-color:#c98283; box-shadow:0 0 0 4px rgba(201,130,131,.12);
}
[data-testid="stTextInput"]{ margin-bottom:6px; }

/* checkbox */
.st-key-formp [data-testid="stCheckbox"] label p{ font-size:12px; color:var(--muted); }

/* forgot link (button tampil seperti link) */
.st-key-forgot button{
  border:0 !important; background:transparent !important; box-shadow:none !important;
  color:var(--rose-deep) !important; font-size:12px; padding:0; min-height:0;
}
.st-key-forgot [data-testid="stHorizontalBlock"]{
  flex-wrap:nowrap !important; align-items:center;
}
.st-key-forgot [data-testid="stColumn"]{ min-width:0 !important; }
.st-key-forgot [data-testid="stColumn"]:first-child [data-testid="stElementContainer"]{
  display:flex; justify-content:flex-start;
}
.st-key-forgot [data-testid="stColumn"]:last-child [data-testid="stElementContainer"]{
  display:flex; justify-content:flex-end;
}
.st-key-forgot button:hover{ text-decoration:underline; transform:none; }
.st-key-forgot{ margin-top:18px; }
.st-key-forgot a{
  border:0 !important; background:transparent !important; box-shadow:none !important;
  color:var(--rose-deep) !important; font-size:12px; padding:0 !important; min-height:0 !important;
}
.st-key-forgot a:hover{ text-decoration:underline; }

/* form: tanpa border */
.st-key-formp [data-testid="stForm"]{ border:0; padding:0; }
/* pesan error/success di bawah form */
.st-key-formp [data-testid="stAlert"]{
  margin-top:16px;
  margin-bottom:8px;
}
.st-key-formp [data-testid="stAlert"] > div{
  padding:12px 16px !important;
  border-radius:13px !important;
  height:auto !important;
  min-height:44px !important;
  align-items:center;
}
.st-key-formp [data-testid="stAlert"] p{
  font-size:13px;
  line-height:1.4;
}

/* tombol Login (primary) */
.st-key-formp button[data-testid="stBaseButton-primaryFormSubmit"],
.st-key-formp button[kind="primaryFormSubmit"],
div[data-testid="stDialog"] button[data-testid="stBaseButton-primary"],
div[data-testid="stDialog"] button[kind="primary"]{
  width:100%; min-height:52px; border:0; border-radius:13px; color:#fff; font-weight:650; font-size:14px;
  background:linear-gradient(110deg,#bd7b82,#d5a18f); box-shadow:0 10px 22px rgba(183,112,119,.22);
  transition:transform .2s ease, box-shadow .2s ease;
}
.st-key-formp button[data-testid="stBaseButton-primaryFormSubmit"]:hover,
div[data-testid="stDialog"] button[data-testid="stBaseButton-primary"]:hover{
  transform:translateY(-2px); box-shadow:0 14px 28px rgba(183,112,119,.32); color:#fff;
}

/* tombol Sign Up (secondary) */
.st-key-signup_btn button{
  width:100%; min-height:52px; border-radius:13px; font-weight:650; font-size:14px;
  color:#9e6670; background:#fff; border:1px solid rgba(189,123,130,.42);
  transition:transform .2s ease, box-shadow .2s ease;
}
.st-key-signup_btn button:hover{
  transform:translateY(-2px); box-shadow:0 8px 18px rgba(183,112,119,.1);
  border-color:rgba(189,123,130,.7); color:#9e6670;
}
.st-key-signup_btn{ margin-top:22px; }

/* dialog */
div[data-testid="stDialog"] div[role="dialog"]{ border-radius:24px; background:#fffaf8; }

@keyframes fadeUp{ from{opacity:0; transform:translateY(12px)} to{opacity:1; transform:translateY(0)} }

@media (max-width:640px){
  .st-key-brand, .brand-inner{ min-height:260px; }
  .brand-inner{ padding:26px 28px 20px; }
  .brand-footer{ display:none; }
  .brand-copy h1{ font-size:34px; margin:12px 0; }
  .st-key-formp{ min-height:unset; padding:30px 28px 32px; }
  .form-title{ font-size:31px; }
}
</style>
"""
VIDEO_HTML = f"""
<style>
html,body{{margin:0;height:100%;overflow:hidden;background:#f7e9e8}}
video{{width:100%;height:100%;object-fit:cover;display:block}}
</style>
<video autoplay muted loop playsinline src="{VIDEO_URL}"></video>
"""

FORM_HEAD_HTML = f"""
<img class="form-logo" src="{_logo_src()}" alt="Glad2Glow">
<div class="welcome">Welcome back</div>
<div class="form-title">Let’s glow in.</div>
<p class="subtitle">Sign in to continue to your Glad2Glow workspace.</p>
"""

FINE_PRINT_HTML = """
<p class="fine-print">By continuing, you agree to our <a href="#">Terms of Service</a>
and <a href="#">Privacy Policy</a>.<br>Made for your glow, with love ♡</p>
"""


# --------------------------------------------------------------- auth logic
def do_login(username: str, password: str):
    validasi_username(username)
    rows = _cari_user(username=username.strip().lower())
    if not rows or not bcrypt.checkpw(password.encode(), rows[0]["password"].encode()):
        raise ValueError("Username atau password salah.")
    r = rows[0]
    st.session_state.user = {"username": r["username"], "email": r["email"], "tagging": r["tagging"]}


def do_signup(username, email, password, tagging=None):
    if tagging is None:                   
        tagging = validasi_username(username)
    if tagging in (TAGGING_RSA, TAGGING_BD) and _cari_user(tagging=tagging):
        raise ValueError(f"Akun {tagging} sudah terdaftar.")
    username, email = username.strip().lower(), email.strip().lower()
    if _cari_user(username=username):
        raise ValueError("Username sudah dipakai.")
    if _cari_user(email=email):
        raise ValueError("Email sudah terdaftar.")
    result = sb_insert(TABLE_USER, {
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "username":   username,
        "password":   bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode(),
        "tagging":    tagging,
        "email":      email,
    })
    if not result or (isinstance(result, dict) and "_error" in result):
        raise ValueError(result.get("_error") if isinstance(result, dict) else f"Insert failed: {result!r}")

@st.dialog("Create your account")
def signup_dialog():
    st.caption("Join the Glad2Glow community.")
    username = st.text_input("Username", placeholder="Username", label_visibility="collapsed")
    email = st.text_input("Email", placeholder="Email", label_visibility="collapsed")
    pw = st.text_input("Password", placeholder="Password (min. 8 characters)", type="password", label_visibility="collapsed")
    pw2 = st.text_input("Confirm Password", placeholder="Confirm Password", type="password", label_visibility="collapsed")

    if st.button("Create Account →", type="primary", width="stretch"):
        if not all([username, email, pw, pw2]):
            st.error("Semua field wajib diisi.")
        elif not re.fullmatch(r"[A-Za-z0-9_.]{3,20}", username):
            st.error("Username 3-20 karakter: huruf, angka, _ atau .")
        elif len(pw) < 8:
            st.error("Password minimal 8 karakter.")
        elif pw != pw2:
            st.error("Password tidak sama.")
        else:
            try:
                do_signup(username, email, pw)
                st.success("Akun berhasil dibuat. Silakan login.")
            except Exception as e:
                st.error(f"Gagal daftar: {e}")



# ---------------------------------------------------------------------- UI
def require_login():
    """Panggil setelah st.set_page_config(). Halaman di bawahnya hanya jalan jika sudah login."""
    if st.session_state.get("user") is not None:
        return st.session_state.user

    st.markdown(CSS, unsafe_allow_html=True)

    with st.container(key="shell"):
        col_l, col_r = st.columns(2, gap="small")

        with col_l:
            with st.container(key="brand"):
                components.html(VIDEO_HTML, height=820)

        with col_r:
            with st.container(key="formp"):
                st.markdown(FORM_HEAD_HTML, unsafe_allow_html=True)

                with st.form("login_form", border=False):
                    username = st.text_input("Username", placeholder="Username", label_visibility="collapsed")
                    password = st.text_input("Password", placeholder="Password", type="password", label_visibility="collapsed")
                    st.checkbox("Remember me")
                    submitted = st.form_submit_button("Login →", type="primary", width="stretch")

                if submitted:
                    if not username or not password:
                        st.error("Isi username dan password.")
                    else:
                        try:
                            do_login(username, password)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Login gagal: {e}")

                with st.container(key="forgot"):
                    c_chat, c_forgot = st.columns([1.3, 1], vertical_alignment="center")
                    c_chat.link_button("📞Chat admin", FORGOT_FORM_URL)
                    if c_forgot.button("Forgot password?"):
                        st.toast("Hubungi admin untuk reset password.", icon="ℹ️")

                with st.container(key="signup_btn"):
                    if st.button("Sign Up", width="stretch"):
                        signup_dialog()

                st.markdown(FINE_PRINT_HTML, unsafe_allow_html=True)

    st.stop()

