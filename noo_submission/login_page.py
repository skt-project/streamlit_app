import base64
import hashlib
import hmac
import json
import re
import time
from pathlib import Path

import datetime

import bcrypt
import requests
import streamlit as st
import streamlit.components.v1 as components

from streamlit.noo_submission.shared import SUPABASE_URL, SUPABASE_KEY, _SB_HEADERS, sb_insert, muat_distri_code_map

TABLE_USER = "users"        
TAGGING_RSA = "RSA"
TAGGING_BD = "BD_SUPPORT"
TAGGING_ADMIN_DISTRI = "Admin Distributor"
TAGGING_USER = "user"   

VIDEO_URL = (
    "https://id.glad2glow.com/cdn/shop/videos/c/vp/d5b2903bb1aa433c8941c13212b84286/d5b2903bb1aa433c8941c13212b84286.HD-1080p-2.5Mbps-90325375.mp4?v=0"
)

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
  max-width:880px !important; padding:20px 16px !important;
  /* kartu login tepat di tengah layar (vertikal & horizontal) */
  min-height:100vh; box-sizing:border-box; display:flex !important; flex-direction:column; justify-content:center;
}
[data-testid="stMainBlockContainer"] > [data-testid="stVerticalBlock"]{ width:100%; flex:0 0 auto; }

/* ---- shell (card) ---- */
.st-key-shell{
  border:1px solid #f1e4e4; border-radius:22px; overflow:hidden;
  box-shadow:0 18px 56px rgba(120,74,78,.13); background:#fff;
  animation:fadeUp .8s ease both;
}
.st-key-shell [data-testid="stHorizontalBlock"]{ gap:0 !important; align-items:stretch !important; }
.st-key-shell [data-testid="stColumn"]{ align-self:stretch !important; }   /* kolom video ikut setinggi kolom form */
.st-key-shell [data-testid="stColumn"]{ padding:0 !important; }
.st-key-shell [data-testid="stVerticalBlock"]{ gap:0 !important; }

/* video memenuhi SELURUH kolom kiri (kolom sudah setinggi kartu): iframe diposisikan relatif ke kolom, bukan ke panel */
.st-key-shell [data-testid="stColumn"]:has(.st-key-brand){
  position:relative; overflow:hidden; margin:0 !important; padding:0 !important;
  border-radius:22px 0 0 22px;      /* sudut kiri video mengikuti sudut kartu */
}
.st-key-shell [data-testid="stColumn"]:has(.st-key-brand) [data-testid="stElementContainer"],
.st-key-shell [data-testid="stColumn"]:has(.st-key-brand) iframe{ border-radius:22px 0 0 22px; }
.st-key-brand{ position:static !important; overflow:visible !important; }

/* ---- brand panel (kiri) ---- */
.st-key-brand{ position:relative; min-height:560px; overflow:hidden; background:#f7e9e8; }
.st-key-brand iframe{ position:absolute; inset:0; width:100% !important; height:100% !important; border:0; }
.st-key-brand .bg-video{
  position:absolute; inset:0; width:100%; height:100%; object-fit:cover; z-index:0;
}
.st-key-brand .veil{
  position:absolute; inset:0; z-index:1;
  background:linear-gradient(150deg, rgba(255,248,245,var(--veil)), rgba(237,205,207,calc(var(--veil) * .6)));
}
.brand-inner{
  position:relative; z-index:2; min-height:560px; padding:clamp(32px,5vw,64px);
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
.st-key-formp{ padding:clamp(22px,3vw,38px) clamp(34px,6vw,76px); background:#fff; min-height:560px; justify-content:center; }
.form-logo{ height:64px; width:auto; display:block; margin:0 auto 22px; }
.welcome{ font-size:9.5px; letter-spacing:1.6px; text-transform:uppercase; color:var(--rose-deep); font-weight:700; }
.form-title{ font-size:22px; letter-spacing:-.8px; margin:8px 0 4px; font-weight:560; color:var(--ink); line-height:1.15; }
.subtitle{ margin:0 0 14px; color:var(--muted); font-size:11px; line-height:1.6; }
.fine-print{ text-align:center; font-size:9px; color:#a39498; margin-top:14px; line-height:1.6; }
.fine-print a{ color:var(--rose-deep); text-decoration:none; }

/* teks judul/subjudul/fine print: spesifisitas tinggi supaya menang atas ukuran paragraf bawaan Streamlit */
.st-key-formp [data-testid="stMarkdownContainer"] .form-title{ font-size:19px !important; margin:6px 0 3px; }
.st-key-formp [data-testid="stMarkdownContainer"] p.subtitle{ font-size:10.5px !important; margin:0 0 12px; line-height:1.5; }
.st-key-formp [data-testid="stMarkdownContainer"] p.fine-print{ font-size:8.5px !important; line-height:1.5; margin-top:12px; }
.st-key-formp [data-testid="stMarkdownContainer"] .welcome{ font-size:9px !important; }

/* jarak antar blok (margin paragraf di dalam markdown tidak terbawa keluar, jadi diatur di wadahnya) */
.st-key-formp [data-testid="stElementContainer"]:has(.subtitle){ margin-bottom:22px; }
.st-key-formp [data-testid="stElementContainer"]:has(.fine-print){ margin-top:18px; }
.st-key-formp [data-testid="stForm"] [data-testid="stCheckbox"]{ margin:2px 0 8px; }

/* inputs */
.st-key-formp [data-testid="stTextInput"] input,
div[data-testid="stDialog"] [data-testid="stTextInput"] input{
  height:36px; padding:0 12px; border-radius:8px; font-size:12px; color:var(--ink);
  background:rgba(255,255,255,.9); border:1px solid var(--line);
}
.st-key-formp [data-testid="stTextInput"] div[data-baseweb="input"],
div[data-testid="stDialog"] [data-testid="stTextInput"] div[data-baseweb="input"]{
  height:36px !important; border:1px solid var(--line) !important;
  border-radius:8px !important; background:rgba(255,255,255,.9) !important;
}
.st-key-formp [data-testid="stTextInput"] input,
div[data-testid="stDialog"] [data-testid="stTextInput"] input{
  height:100% !important; border:0 !important;
}
.st-key-formp [data-testid="stVerticalBlock"]{ gap:.7rem !important; }
.st-key-formp [data-testid="stTextInput"] input:focus,
div[data-testid="stDialog"] [data-testid="stTextInput"] input:focus{
  border-color:#c98283; box-shadow:0 0 0 4px rgba(201,130,131,.12);
}
[data-testid="stTextInput"]{ margin-bottom:2px; }

/* teks bawaan Streamlit di panel form (rem) ikut diperkecil */
.st-key-formp .stButton button p{ font-size:12px; }

/* checkbox "Remember me": kotak, teks, dan ikon ? sejajar di satu garis tengah (tanpa pembesaran bawaan) */
.st-key-formp [data-testid="stCheckbox"] label{ display:flex !important; align-items:center !important; gap:8px; }
.st-key-formp [data-testid="stCheckbox"] label > span:first-child{ transform:none !important; margin:0 !important; flex-shrink:0; }
.st-key-formp [data-testid="stCheckbox"] label p{ margin:0 !important; line-height:1.2 !important; }
.st-key-formp [data-testid="stCheckbox"] [data-testid="stTooltipIcon"],
.st-key-formp [data-testid="stCheckbox"] [data-testid="stTooltipHoverTarget"]{ display:flex; align-items:center; margin:0 0 0 2px; }
.st-key-formp [data-testid="stCheckbox"] [data-testid="stWidgetLabel"]{ display:flex; align-items:center; }

/* checkbox */
.st-key-formp [data-testid="stCheckbox"] label p{ font-size:10px; color:var(--muted); }

/* forgot link (button tampil seperti link) */
.st-key-forgot button{
  border:0 !important; background:transparent !important; box-shadow:none !important;
  color:var(--rose-deep) !important; font-size:10.5px; padding:0; min-height:0;
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
.st-key-forgot{ margin-top:14px; }
.st-key-forgot a{
  border:0 !important; background:transparent !important; box-shadow:none !important;
  color:var(--rose-deep) !important; font-size:10.5px; padding:0 !important; min-height:0 !important;
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
  font-size:12px;
  line-height:1.4;
}

/* tombol Login (primary) */
.st-key-formp button[data-testid="stBaseButton-primaryFormSubmit"],
.st-key-formp button[kind="primaryFormSubmit"],
div[data-testid="stDialog"] button[data-testid="stBaseButton-primary"],
div[data-testid="stDialog"] button[kind="primary"]{
  width:100%; min-height:36px; height:36px; border:0; border-radius:8px; color:#fff; font-weight:650; font-size:12px;
  background:linear-gradient(110deg,#bd7b82,#d5a18f); box-shadow:0 10px 22px rgba(183,112,119,.22);
  transition:transform .2s ease, box-shadow .2s ease;
}
.st-key-formp button[data-testid="stBaseButton-primaryFormSubmit"]:hover,
div[data-testid="stDialog"] button[data-testid="stBaseButton-primary"]:hover{
  transform:translateY(-2px); box-shadow:0 14px 28px rgba(183,112,119,.32); color:#fff;
}

/* tombol Sign Up (secondary) */
.st-key-signup_btn button{
  width:100%; min-height:36px; height:36px; border-radius:8px; font-weight:650; font-size:12px;
  color:#9e6670; background:#fff; border:1px solid rgba(189,123,130,.42);
  transition:transform .2s ease, box-shadow .2s ease;
}
.st-key-signup_btn button:hover{
  transform:translateY(-2px); box-shadow:0 8px 18px rgba(183,112,119,.1);
  border-color:rgba(189,123,130,.7); color:#9e6670;
}
.st-key-signup_btn{ margin-top:14px; }

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
            except Exception as e:
                st.error(f"Gagal daftar: {e}")
            else:
                st.session_state["pesan_toast"] = "Akun berhasil dibuat. Silakan login."
                st.session_state["pesan_toast_icon"] = ":material/check_circle:"
                st.rerun()   # menutup dialog; toast muncul di require_login()



# ---------------------------------------------------------------------- UI
# ------------------------------------------------- login bertahan saat refresh (cookie bertanda tangan)
COOKIE_LOGIN = "skp_auth"
MASA_LOGIN_DETIK = 30 * 60        # tanpa "Remember me": tetap login 30 menit sejak aktivitas terakhir
MASA_INGAT_DETIK = 24 * 60 * 60   # dengan "Remember me": tetap login 1 hari sejak aktivitas terakhir
_JEDA_PERBARUI_COOKIE = 300       # perpanjang cookie paling cepat tiap 5 menit (hemat render)


def _kunci_cookie() -> bytes:
    try:
        return str(st.secrets["auth_secret"]).encode()    # disarankan: tambahkan auth_secret di secrets
    except Exception:
        return ("skp-auth|" + str(SUPABASE_KEY)).encode()


def _buat_token(user: dict, ingat: bool = False) -> str:
    masa = MASA_INGAT_DETIK if ingat else MASA_LOGIN_DETIK
    isi = {"u": user.get("username", ""), "e": user.get("email", ""), "t": user.get("tagging", ""),
           "r": bool(ingat), "x": int(time.time()) + masa}
    b = base64.urlsafe_b64encode(json.dumps(isi).encode()).decode()
    return f"{b}.{hmac.new(_kunci_cookie(), b.encode(), hashlib.sha256).hexdigest()}"


def _baca_token(token: str):
    try:
        b, sig = token.rsplit(".", 1)
        if not hmac.compare_digest(sig, hmac.new(_kunci_cookie(), b.encode(), hashlib.sha256).hexdigest()):
            return None
        isi = json.loads(base64.urlsafe_b64decode(b.encode()))
        if int(isi["x"]) < time.time():
            return None
        return {"username": isi["u"], "email": isi["e"], "tagging": isi["t"], "_r": bool(isi.get("r", False))}
    except Exception:
        return None


def _js_cookie(token: str, detik: int):
    """Tulis/hapus cookie lewat JS (iframe komponen same-origin dengan halaman Streamlit)."""
    components.html(
        f"""<script>
        const sec = (location.protocol === 'https:') ? '; SameSite=None; Secure' : '; SameSite=Lax';
        window.parent.document.cookie = "{COOKIE_LOGIN}={token}; max-age={detik}; path=/" + sec;
        </script>""", height=0)


def keluar():
    """Logout: kosongkan sesi + hapus cookie login. Panggil lalu st.rerun()."""
    ss = st.session_state
    ss.clear()
    ss["_auth_dicoba"] = True      # cookie lama masih terbaca di sesi ini -> jangan dipulihkan lagi
    ss["_hapus_cookie"] = True


def require_login():
    """Panggil setelah st.set_page_config(). Halaman di bawahnya hanya jalan jika sudah login."""
    ss = st.session_state
    if ss.get("user") is None and not ss.get("_auth_dicoba"):
        ss["_auth_dicoba"] = True                       # pulihkan dari cookie hanya sekali per sesi browser
        try:
            token = st.context.cookies.get(COOKIE_LOGIN)
        except Exception:
            token = None
        pulih = _baca_token(token) if token else None
        if pulih:
            ss["_ingat"] = pulih.pop("_r", False)           # pilihan "Remember me" ikut dipulihkan dari cookie
            ss.user = pulih
    if ss.get("user") is not None:
        ulang = ss.get("_cookie_ulang", 0)
        if ulang > 0 or time.time() - ss.get("_cookie_ts", 0) > _JEDA_PERBARUI_COOKIE:
            ingat = bool(ss.get("_ingat"))
            _js_cookie(_buat_token(ss.user, ingat), MASA_INGAT_DETIK if ingat else MASA_LOGIN_DETIK)
            ss["_cookie_ts"] = time.time()
            # Setelah login, tulis cookie di beberapa run pertama: kalau run pertama keburu di-rerun sebelum iframe
            # sempat menjalankan script, run berikutnya mencoba lagi (bukan menunggu 5 menit).
            ss["_cookie_ulang"] = max(0, ulang - 1)
        return ss.user
    # slot tetap (selalu ada, kadang kosong): jumlah elemen di halaman login SAMA di setiap run, jadi tidak ada
    # elemen sisa (kartu login ganda) saat Streamlit mencocokkan elemen run lama dengan run baru
    slot_cookie = st.empty()
    if ss.pop("_hapus_cookie", False):
        with slot_cookie:
            _js_cookie("", 0)

    css_ph = st.empty()       # CSS login (termasuk latar halaman) di placeholder: dihapus seketika saat login berhasil
    css_ph.markdown(CSS, unsafe_allow_html=True)
    if ss.get("pesan_toast"):                      # mis. "Ticket submitted" dari popup Help & Support
        st.toast(ss.pop("pesan_toast"), icon=ss.pop("pesan_toast_icon", ":material/check_circle:"))

    login_ph = st.empty()     # dibungkus placeholder supaya bisa dikosongkan seketika saat login (anti ghost)
    with login_ph.container(), st.container(key="shell"):
        col_l, col_r = st.columns(2, gap="small")

        with col_l:
            with st.container(key="brand"):
                components.html(VIDEO_HTML, height=560)

        with col_r:
            with st.container(key="formp"):
                st.markdown(FORM_HEAD_HTML, unsafe_allow_html=True)

                with st.form("login_form", border=False):
                    username = st.text_input("Username", placeholder="Username", label_visibility="collapsed")
                    password = st.text_input("Password", placeholder="Password", type="password", label_visibility="collapsed")
                    ingat_saya = st.checkbox("Remember me", help="Stay signed in for 1 day on this browser.")
                    submitted = st.form_submit_button("Login →", type="primary", width="stretch")

                if submitted:
                    if not username or not password:
                        st.error("Isi username dan password.")
                    else:
                        try:
                            do_login(username, password)
                            ss["_ingat"] = bool(ingat_saya)
                            ss["_cookie_ts"] = 0          # tulis cookie segera dengan masa berlaku yang sesuai
                            ss["_cookie_ulang"] = 3       # dan ulangi di 3 run pertama (lihat require_login)
                            login_ph.empty()      # hapus halaman login dari layar sebelum app dimuat
                            css_ph.empty()        # latar/gaya login ikut dihapus, kalau tidak tertinggal saat app memuat
                            st.rerun()
                        except Exception as e:
                            st.error(f"Login gagal: {e}")

                with st.container(key="forgot"):
                    c_chat, c_forgot = st.columns([1.3, 1], vertical_alignment="center")
                    if c_chat.button(":material/help: Help & Support", key="login_help"):
                        from streamlit.noo_submission.help_support import dialog_help_support      # popup tiket (tanpa Google Form)
                        dialog_help_support()
                    if c_forgot.button("Forgot password?"):
                        st.toast("Hubungi admin untuk reset password.", icon=":material/info:")

                with st.container(key="signup_btn"):
                    if st.button("Sign Up", width="stretch"):
                        signup_dialog()

                st.markdown(FINE_PRINT_HTML, unsafe_allow_html=True)

    st.stop()

