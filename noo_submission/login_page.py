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
from shared import SUPABASE_URL, SUPABASE_KEY, _SB_HEADERS, sb_insert, muat_distri_code_map
#from shared import SUPABASE_URL, SUPABASE_KEY, _SB_HEADERS, sb_insert, muat_distri_code_map

TABLE_USER = "users"        
TAGGING_RSA = "RSA"
TAGGING_BD = "BD_SUPPORT"
TAGGING_ADMIN_DISTRI = "Admin Distributor"
TAGGING_USER = "user"
TAGGING_SPV = "SPV"      # akun ke-2 per kode distributor (menjawab "Are you SPV? Yes"): bisa SPV Confirmation

VIDEO_URL = (
    "https://id.glad2glow.com/cdn/shop/videos/c/vp/d5b2903bb1aa433c8941c13212b84286/d5b2903bb1aa433c8941c13212b84286.HD-1080p-2.5Mbps-90325375.mp4?v=0"
)

USERNAME_TESTING = "TESTING"      # akun uji coba (mode testing): diperlakukan seperti kode distributor (login, Forgot password,
TESTING_AKTIF = True              # Sign Up + aturan maks. 2 akun/SPV). Set False untuk MEMATIKAN mode testing (mis. saat rilis).


def validasi_username(username: str, izinkan_testing: bool = False) -> str:
    u = username.strip().upper()
    if izinkan_testing and TESTING_AKTIF and u == USERNAME_TESTING:
        return TAGGING_ADMIN_DISTRI
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


# ------------------------------------------------ TABEL PERMINTAAN: account_requests
# SATU tabel untuk dua jenis permintaan, dibedakan kolom `type`:
#   signup_approval   = akun ke-2 (SPV) untuk kode yang sudah terdaftar; baru jadi akun (tabel users) setelah di-approve
#   password_approval = permintaan reset password (Forgot password)
# SQL pembuatan/migrasi tabel: sql_account_requests.sql. Tabel users TIDAK diubah.
TABLE_REQ = "account_requests"
TIPE_SIGNUP, TIPE_PASSWORD = "signup_approval", "password_approval"
MAKS_AKUN_PER_KODE = 2
STATUS_AKTIF, STATUS_PENDING = "active", "pending"       # STATUS_AKTIF: hasil do_signup (akun langsung aktif)


def _get_req(params: dict) -> list:
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{TABLE_REQ}", headers={**_SB_HEADERS, "Prefer": ""},
                     params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def _semua_user(username: str, dengan_password: bool = False) -> list:
    """SEMUA akun di tabel users dengan username (= kode) tertentu (satu kode boleh punya 2 akun)."""
    kolom = "username,email,tagging" + (",password" if dengan_password else "")
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{TABLE_USER}", headers={**_SB_HEADERS, "Prefer": ""},
                     params={"select": kolom, "username": f"eq.{username}"}, timeout=10)
    r.raise_for_status()
    return r.json()


def _signup_pending_kode(username: str) -> list:
    return _get_req({"select": "id,username,email,password_hash,status", "type": f"eq.{TIPE_SIGNUP}",
                     "username": f"eq.{username}", "status": f"eq.{STATUS_PENDING}"})


def signup_daftar_pending() -> list:
    return _get_req({"select": "id,username,email,tagging,created_at", "type": f"eq.{TIPE_SIGNUP}",
                     "status": f"eq.{STATUS_PENDING}", "order": "created_at.asc"})


def signup_putuskan(req_id, setuju: bool, oleh: str = ""):
    """Approve -> akun dibuat di tabel users, permintaan ditandai approved. Reject -> ditandai rejected (slot kosong lagi).
    Mengembalikan (berhasil, pesan_error)."""
    baris = _get_req({"select": "username,email,tagging,password_hash", "id": f"eq.{req_id}"})
    if not baris:
        return False, "Request not found."
    q = baris[0]
    if setuju:
        hasil = sb_insert(TABLE_USER, {
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "username": q["username"], "password": q["password_hash"], "tagging": q["tagging"], "email": q["email"]})
        if not hasil or (isinstance(hasil, dict) and "_error" in hasil):
            err = hasil.get("_error") if isinstance(hasil, dict) else f"Insert failed: {hasil!r}"
            if "duplicate" in str(err).lower() or "23505" in str(err):
                err += (" -> the UNIQUE constraint on users.username must be dropped so one code can have 2 accounts "
                        "(see sql_account_requests.sql).")
            return False, err
    return reset_ubah_status(req_id, "approved" if setuju else "rejected", oleh), ""


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

/* Forgot password: tombol Back seperti link + label field */
.st-key-reset_back button{ border:0 !important; background:transparent !important; box-shadow:none !important;
  color:var(--rose-deep) !important; font-size:11px; padding:0 !important; min-height:0 !important; }
.st-key-reset_back button:hover{ text-decoration:underline; }
.field-label{ font-size:11px; font-weight:600; color:var(--ink); margin:0 0 4px; }
.rst-center{ text-align:center; }
/* ---- layar Forgot password: kolom input putih, sudut membulat + shadow lembut, tombol modern ---- */
.st-key-formp [data-testid="stForm"][class*="reset"] [data-testid="stVerticalBlock"],
.st-key-reset_form [data-testid="stVerticalBlock"], .st-key-reset_cek_form [data-testid="stVerticalBlock"]{ gap:.35rem !important; }
.st-key-reset_form [data-testid="stTextInput"] div[data-baseweb="input"],
.st-key-reset_cek_form [data-testid="stTextInput"] div[data-baseweb="input"]{
  height:48px !important; border:0 !important; border-radius:14px !important; background-color:#fff !important;
  box-shadow:0 8px 22px rgba(120,74,78,.14), 0 1px 3px rgba(120,74,78,.10) !important;
  transition:box-shadow .2s ease, transform .2s ease;
}
.st-key-reset_form [data-testid="stTextInput"] div[data-baseweb="input"]:focus-within,
.st-key-reset_cek_form [data-testid="stTextInput"] div[data-baseweb="input"]:focus-within{
  box-shadow:0 0 0 3px rgba(201,130,131,.28), 0 10px 26px rgba(120,74,78,.18) !important; transform:translateY(-1px);
}
.st-key-reset_form [data-testid="stTextInput"] input,
.st-key-reset_cek_form [data-testid="stTextInput"] input{ padding-left:44px !important; font-size:13px; }
.st-key-reset_form [data-testid="stTextInput"] div[data-baseweb="input"],
.st-key-reset_cek_form [data-testid="stTextInput"] div[data-baseweb="input"]{
  background-repeat:no-repeat !important; background-position:15px center !important;
  background-image:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='18' height='18' viewBox='0 0 24 24' fill='none' stroke='%238c7a80' stroke-width='1.8' stroke-linecap='round' stroke-linejoin='round'><rect x='3' y='5' width='18' height='14' rx='2'/><path d='M3 7l9 6 9-6'/></svg>") !important;
}
.st-key-reset_form [data-testid="stElementContainer"]:has(input[aria-label="Username"]) div[data-baseweb="input"],
.st-key-reset_cek_form [data-testid="stElementContainer"]:has(input[aria-label="Username"]) div[data-baseweb="input"]{
  background-image:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='18' height='18' viewBox='0 0 24 24' fill='none' stroke='%238c7a80' stroke-width='1.8' stroke-linecap='round' stroke-linejoin='round'><circle cx='12' cy='8' r='4'/><path d='M4 21c0-4.4 3.6-7 8-7s8 2.6 8 7'/></svg>") !important;
}
/* username tampil HURUF BESAR saat diketik (nilai tetap diproses huruf kecil) */
.st-key-reset_form input[aria-label="Username"], .st-key-reset_cek_form input[aria-label="Username"]{ text-transform:uppercase; }
.st-key-reset_form input[aria-label="Username"]::placeholder, .st-key-reset_cek_form input[aria-label="Username"]::placeholder{ text-transform:none; }
/* teks SEMUA tombol di halaman login = font tombol sidebar di app_main (Source Sans, tebal 700) */
.st-key-formp button, .st-key-formp button p, .st-key-formp button div,
div[data-testid="stDialog"] button, div[data-testid="stDialog"] button p{
  font-family:"Source Sans","Source Sans Pro",ui-sans-serif,system-ui,sans-serif !important;
  font-weight:700 !important; letter-spacing:0 !important;
}
.st-key-formp button p, div[data-testid="stDialog"] button p{ font-size:12px !important; }
/* tombol utama (Lanjutkan / Cek status): gradasi + shadow, naik sedikit saat di-hover */
.st-key-reset_form button[data-testid="stBaseButton-primaryFormSubmit"],
.st-key-reset_cek_form button[data-testid="stBaseButton-primaryFormSubmit"]{
  height:48px; min-height:48px; border-radius:14px; font-size:14px; font-weight:700; letter-spacing:.3px; margin-top:6px;
  background:linear-gradient(110deg,#b86b78,#d89a86); box-shadow:0 12px 26px rgba(183,112,119,.38);
}
.st-key-reset_form button[data-testid="stBaseButton-primaryFormSubmit"]:hover,
.st-key-reset_cek_form button[data-testid="stBaseButton-primaryFormSubmit"]:hover{
  transform:translateY(-2px); box-shadow:0 16px 32px rgba(183,112,119,.46);
}
.st-key-reset_form button[data-testid="stBaseButton-primaryFormSubmit"]:active{ transform:translateY(0); }
/* tombol sekunder: "Sudah pernah mengajukan? </br>Cek status" */
.st-key-reset_sudah button{
  height:46px; border:0 !important; border-radius:14px; background:#fff !important; color:#a95e69 !important;
  font-weight:650; box-shadow:0 8px 22px rgba(120,74,78,.12), 0 1px 3px rgba(120,74,78,.10);
  transition:transform .2s ease, box-shadow .2s ease;
}
.st-key-reset_sudah button:hover{ transform:translateY(-2px); box-shadow:0 12px 28px rgba(120,74,78,.2); color:#8e4753 !important; }
.st-key-reset_sudah button p{ font-size:12.5px !important; }
/* latar panel Forgot password pink muda supaya kolom & tombol putih terlihat jelas */
.st-key-formp:has(.st-key-reset_back){ background:linear-gradient(180deg,#fdf0f1 0%,#f8dfe1 100%) !important; }
.st-key-formp:has(.st-key-reset_back) .fine-print{ color:#8c6f75; }
/* animasi penunjuk: panah di kanan bergerak maju-mundur + cincin denyut halus supaya tombol terlihat */
.st-key-reset_sudah button{ position:relative; animation:rstPulse 2.4s ease-out infinite;
  height:auto !important; min-height:42px; padding:6px 34px 6px 12px !important; overflow:visible !important; }
.st-key-reset_sudah button > div, .st-key-reset_sudah button p{
  white-space:nowrap !important; overflow:visible !important; text-overflow:clip !important;
  line-height:1.2 !important; margin:0 !important; text-align:center; font-size:11.5px !important; }
.st-key-reset_sudah button::after{
  content:""; position:absolute; right:10px; top:50%; width:16px; height:16px; margin-top:-8px;
  background:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='%23a95e69' stroke-width='2.4' stroke-linecap='round' stroke-linejoin='round'><path d='M5 12h14M13 6l6 6-6 6'/></svg>") center/contain no-repeat;
  animation:rstNudge 1.2s ease-in-out infinite;
}
.st-key-reset_sudah button:hover{ animation:none; }
.st-key-reset_sudah button:hover::after{ animation-duration:.6s; }
@keyframes rstNudge{ 0%,100%{ transform:translateX(0); } 50%{ transform:translateX(7px); } }
@keyframes rstPulse{
  0%{ box-shadow:0 8px 22px rgba(120,74,78,.12), 0 0 0 0 rgba(201,130,131,.45); }
  70%{ box-shadow:0 8px 22px rgba(120,74,78,.12), 0 0 0 12px rgba(201,130,131,0); }
  100%{ box-shadow:0 8px 22px rgba(120,74,78,.12), 0 0 0 0 rgba(201,130,131,0); }
}
@media (prefers-reduced-motion:reduce){ .st-key-reset_sudah button, .st-key-reset_sudah button::after{ animation:none; } }
/* tombol di layar status (Check status, Send a new request) */
.st-key-reset_cek button, .st-key-reset_ulang button{
  height:48px; border-radius:14px; font-weight:700; box-shadow:0 12px 26px rgba(183,112,119,.38);
}
.field-label{ font-size:12px; font-weight:650; color:var(--ink); margin:6px 0 2px; }
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
.st-key-formp button[data-testid="stBaseButton-primary"],
div[data-testid="stDialog"] button[data-testid="stBaseButton-primary"],
div[data-testid="stDialog"] button[kind="primary"]{
  width:100%; min-height:36px; height:36px; border:0; border-radius:8px; color:#fff; font-weight:650; font-size:12px;
  background:linear-gradient(110deg,#bd7b82,#d5a18f); box-shadow:0 10px 22px rgba(183,112,119,.22);
  transition:transform .2s ease, box-shadow .2s ease;
}
.st-key-formp button[data-testid="stBaseButton-primaryFormSubmit"]:hover,
.st-key-formp button[data-testid="stBaseButton-primary"]:hover,
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
    validasi_username(username, izinkan_testing=True)
    uname = username.strip().lower()
    # satu kode bisa punya sampai 2 akun: cari akun yang password-nya cocok
    cocok = next((x for x in _semua_user(uname, dengan_password=True)
                  if bcrypt.checkpw(password.encode(), x["password"].encode())), None)
    if cocok is None:
        # belum jadi akun: mungkin akun ke-2 (SPV) yang masih menunggu persetujuan admin
        if any(bcrypt.checkpw(password.encode(), x["password_hash"].encode()) for x in _signup_pending_kode(uname)):
            raise ValueError("Akun kamu masih menunggu persetujuan admin (Waiting approval). "
                             "Silakan login setelah disetujui.")
        raise ValueError("Username atau password salah.")
    st.session_state.user = {"username": cocok["username"], "email": cocok["email"], "tagging": cocok["tagging"]}


def jumlah_akun_kode(username: str) -> int:
    """Akun yang sudah ada + permintaan akun ke-2 yang masih menunggu persetujuan."""
    u = username.strip().lower()
    return len(_semua_user(u)) + len(_signup_pending_kode(u))


def do_signup(username, email, password, tagging=None, spv=False) -> str:
    """Buat akun. Mengembalikan 'active' (akun langsung jadi) atau 'pending' (akun ke-2 SPV: masuk account_requests,
    jadi akun setelah admin approve)."""
    if tagging is None:
        tagging = validasi_username(username, izinkan_testing=True)
    if tagging in (TAGGING_RSA, TAGGING_BD) and _cari_user(tagging=tagging):
        raise ValueError(f"Akun {tagging} sudah terdaftar.")
    username, email = username.strip().lower(), email.strip().lower()
    status = STATUS_AKTIF
    if tagging == TAGGING_ADMIN_DISTRI:                  # pendaftaran mandiri dengan kode distributor
        n = jumlah_akun_kode(username)
        if n >= MAKS_AKUN_PER_KODE:
            raise ValueError(f"Kode {username.upper()} sudah mencapai batas maksimal {MAKS_AKUN_PER_KODE} akun.")
        if n >= 1:
            if not spv:
                raise ValueError("Kode ini sudah terdaftar. Hanya SPV yang dapat mendaftarkan akun ke-2.")
            status = STATUS_PENDING
    elif _cari_user(username=username):                  # akun yang dibuat admin / RSA / BD: username tetap unik
        raise ValueError("Username sudah dipakai.")
    if _cari_user(email=email) or _get_req({"select": "id", "type": f"eq.{TIPE_SIGNUP}", "email": f"eq.{email}",
                                            "status": f"eq.{STATUS_PENDING}", "limit": 1}):
        raise ValueError("Email sudah terdaftar.")
    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    if status == STATUS_PENDING:                         # akun ke-2: simpan sebagai permintaan, BUKAN di tabel users
        result = sb_insert(TABLE_REQ, {"type": TIPE_SIGNUP, "username": username, "email": email, "tagging": TAGGING_SPV,
                                       "password_hash": hashed, "status": STATUS_PENDING, "created_at": now})
    else:
        result = sb_insert(TABLE_USER, {"created_at": now, "username": username, "password": hashed,
                                        "tagging": tagging, "email": email})
    if not result or (isinstance(result, dict) and "_error" in result):
        raise ValueError(result.get("_error") if isinstance(result, dict) else f"Insert failed: {result!r}")
    return status


@st.dialog("Create your account")
def signup_dialog():
    ss = st.session_state
    st.caption("Join the Glad2Glow community.")
    username = st.text_input("Username", placeholder="Username", label_visibility="collapsed")
    email = st.text_input("Email", placeholder="Email", label_visibility="collapsed")
    pw = st.text_input("Password", placeholder="Password (min. 8 characters)", type="password", label_visibility="collapsed")
    pw2 = st.text_input("Confirm Password", placeholder="Confirm Password", type="password", label_visibility="collapsed")

    def _selesai(status):
        ss.pop("_spv_tanya", None)
        ss["pesan_sukses"] = ("Akun berhasil dibuat. Waiting approval: kamu baru bisa login setelah admin menyetujui."
                              if status == STATUS_PENDING else "Akun berhasil dibuat. Silakan login.")
        st.rerun()

    if st.button("Create Account →", type="primary", width="stretch"):
        ss.pop("_spv_tanya", None)
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
                tag = validasi_username(username, izinkan_testing=True)
                n = jumlah_akun_kode(username) if tag == TAGGING_ADMIN_DISTRI else 0
                if n >= MAKS_AKUN_PER_KODE:
                    st.error(f"Kode {username.strip().upper()} sudah mencapai batas maksimal {MAKS_AKUN_PER_KODE} akun.")
                elif n >= 1:
                    ss["_spv_tanya"] = username.strip().lower()     # kode sudah dipakai -> tanya dulu: SPV?
                else:
                    _selesai(do_signup(username, email, pw))
            except Exception as e:
                st.error(f"Gagal daftar: {e}")

    # kode sudah terdaftar 1 akun: hanya SPV yang boleh menambah akun ke-2 (menunggu persetujuan admin)
    if ss.get("_spv_tanya") and ss["_spv_tanya"] == username.strip().lower():
        st.warning(f"Kode **{username.strip().upper()}** sudah terdaftar. Akun ke-2 hanya untuk **SPV** dan perlu "
                   "persetujuan admin sebelum bisa login.")
        st.markdown("**Are you SPV?**")
        c_yes, c_no = st.columns(2)
        if c_yes.button("Yes, I'm SPV", type="primary", width="stretch", key="spv_yes"):
            try:
                _selesai(do_signup(username, email, pw, spv=True))
            except Exception as e:
                st.error(f"Gagal daftar: {e}")
        if c_no.button("No", width="stretch", key="spv_no"):
            ss.pop("_spv_tanya", None)
            st.error("Kode yang sudah terdaftar hanya bisa ditambah akun oleh SPV.")


# ------------------------------------------------------------ FORGOT PASSWORD
# Alur: user kirim permintaan (status pending) -> admin BD Support approve di Settings -> user bisa isi password baru.
# Tabel Supabase: account_requests, type = password_approval (SQL ada di sql_account_requests.sql).
TABLE_RESET = TABLE_REQ
RESET_PENDING, RESET_APPROVED, RESET_REJECTED, RESET_DONE = "pending", "approved", "rejected", "done"


def reset_cari_permintaan(username: str):
    """Permintaan reset TERBARU milik username yang belum selesai (pending / approved / rejected), atau None."""
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{TABLE_RESET}",
        headers={**_SB_HEADERS, "Prefer": ""},
        params={"select": "id,username,status,created_at", "type": f"eq.{TIPE_PASSWORD}", "username": f"eq.{username}",
                "status": f"neq.{RESET_DONE}", "order": "created_at.desc", "limit": 1}, timeout=10)
    r.raise_for_status()
    rows = r.json()
    return rows[0] if rows else None


def reset_buat_permintaan(username: str, email: str):
    hasil = sb_insert(TABLE_RESET, {"type": TIPE_PASSWORD, "username": username, "email": email, "status": RESET_PENDING,
                                    "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat()})
    if isinstance(hasil, dict) and "_error" in hasil:
        raise ValueError(hasil["_error"])


def reset_daftar(status: str = RESET_PENDING) -> list:
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{TABLE_RESET}",
        headers={**_SB_HEADERS, "Prefer": ""},
        params={"select": "id,username,email,status,created_at,decided_by,decided_at", "type": f"eq.{TIPE_PASSWORD}",
                "status": f"eq.{status}", "order": "created_at.asc"}, timeout=10)
    r.raise_for_status()
    return r.json()


def reset_ubah_status(req_id, status: str, oleh: str = "") -> bool:
    patch = {"status": status}
    if status in (RESET_APPROVED, RESET_REJECTED):
        patch.update({"decided_by": oleh, "decided_at": datetime.datetime.now(datetime.timezone.utc).isoformat()})
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{TABLE_RESET}",
        headers={**_SB_HEADERS, "Prefer": "return=representation"},
        params={"id": f"eq.{req_id}"}, json=patch, timeout=10)
    return r.ok and len(r.json()) == 1


def reset_password_user(username: str, pw_baru: str) -> bool:
    hashed = bcrypt.hashpw(pw_baru.encode(), bcrypt.gensalt()).decode()
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{TABLE_USER}",
        headers={**_SB_HEADERS, "Prefer": "return=representation"},
        params={"username": f"eq.{username}"}, json={"password": hashed}, timeout=10)
    return r.ok and len(r.json()) >= 1          # satu kode bisa punya 2 akun: password baru berlaku untuk keduanya


def _svg_status(jenis: str) -> str:
    """Ilustrasi status: 'wait' (jam pasir), 'ok' (centang), 'no' (silang)."""
    warna, isi = {
        "wait": ("#bd9a65", '<path d="M40 30h40M40 90h40M46 30c0 24 28 18 28 30s-28 6-28 30M74 30c0 24-28 18-28 30s28 6 28 30" '
                            'fill="none" stroke="#a95e69" stroke-width="5" stroke-linecap="round"/>'
                            '<path d="M50 38c4 8 16 8 20 0z M52 88c2-10 14-10 16 0z" fill="#c98283"/>'),
        "ok":   ("#6aa77f", '<path d="M40 62l14 14 28-30" fill="none" stroke="#3f8f5a" stroke-width="7" '
                            'stroke-linecap="round" stroke-linejoin="round"/>'),
        "no":   ("#d98585", '<path d="M44 44l32 32M76 44L44 76" fill="none" stroke="#c0504d" stroke-width="7" '
                            'stroke-linecap="round"/>'),
    }[jenis]
    return (f'<div style="text-align:center;margin:6px 0 10px"><svg width="132" height="132" viewBox="0 0 120 120">'
            f'<circle cx="60" cy="60" r="54" fill="{warna}" fill-opacity=".14"/>'
            f'<circle cx="60" cy="60" r="44" fill="{warna}" fill-opacity=".14"/>{isi}</svg></div>')


def _svg_shield() -> str:
    """Ilustrasi perisai + gembok + '****' (layar Forgot password)."""
    return ('<div style="text-align:center;margin:2px 0 6px"><svg width="150" height="150" viewBox="0 0 150 150">'
            '<path d="M75 10 L128 30 V74 C128 104 105 126 75 140 C45 126 22 104 22 74 V30 Z" fill="#fbeeee" '
            'stroke="#c98283" stroke-width="7" stroke-linejoin="round"/>'
            '<path d="M75 22 L117 38 V74 C117 98 99 116 75 128 C51 116 33 98 33 74 V38 Z" fill="#fff" fill-opacity=".7"/>'
            '<path d="M62 62 V52 a13 13 0 0 1 26 0 V62" fill="none" stroke="#45343b" stroke-width="5" stroke-linecap="round"/>'
            '<rect x="56" y="60" width="38" height="28" rx="5" fill="#fff" stroke="#45343b" stroke-width="5"/>'
            '<circle cx="75" cy="72" r="4" fill="#45343b"/><rect x="73" y="73" width="4" height="9" rx="2" fill="#45343b"/>'
            '<rect x="34" y="92" width="82" height="24" rx="6" fill="#fff" stroke="#bd9a65" stroke-width="3"/>'
            '<text x="75" y="113" text-anchor="middle" font-size="24" font-weight="700" fill="#45343b" '
            'font-family="Inter,sans-serif" letter-spacing="3">****</text></svg></div>')


def _judul(judul: str, sub: str):
    st.markdown(f'<div class="form-title rst-center">{judul}</div><p class="subtitle rst-center">{sub}</p>',
                unsafe_allow_html=True)


def _keluar_reset():
    for k in ("reset_view", "reset_user"):
        st.session_state.pop(k, None)


def tampil_reset_password():
    """Layar Forgot password di panel form: forgot -> waiting -> newpw. Dipanggil dari require_login."""
    ss = st.session_state
    if st.button(":material/arrow_back: Back", key="reset_back"):
        _keluar_reset()
        st.rerun()
    view = ss.get("reset_view", "forgot")

    # 1. isi username
    if view == "forgot":
        _hl = 'style="color:#a95e69;font-weight:700"'          # penanda (highlight) kata kunci
        st.markdown(_svg_shield(), unsafe_allow_html=True)
        _judul("Lupa Password", f"Masukkan <span {_hl}>email</span> kamu, kami akan mengirim permintaan ke "
                                f"<span {_hl}>admin</span>. Setelah <span {_hl}>disetujui</span>, "
                                "kamu bisa membuat password baru.")
        with st.form("reset_form", border=False):
            st.markdown('<div class="field-label">Username</div>', unsafe_allow_html=True)
            un = st.text_input("Username", placeholder="Kode distributor terdaftar", label_visibility="collapsed")
            st.markdown('<div class="field-label">Email address</div>', unsafe_allow_html=True)
            em = st.text_input("Email address", placeholder="Masukkan email kamu...", label_visibility="collapsed")
            kirim = st.form_submit_button("Lanjutkan", type="primary", width="stretch")
        st.markdown('<p class="fine-print"><b>Info:</b> username yang bisa dipakai hanya <b>kode distributor yang terdaftar</b>. '
                    ,
                    unsafe_allow_html=True)
        if st.button(":material/history: Sudah pernah mengajukan? Cek status", key="reset_sudah", width="stretch"):
            ss.reset_view = "cek"
            st.rerun()
        if kirim:
            un, em = un.strip().lower(), em.strip().lower()
            if not un or not em:
                st.error("Isi username dan email.")
            elif not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", em):
                st.error("Format email tidak valid.")
            else:
                try:
                    pesan = ""
                    if un.upper() in ("RSA", "BD_SUPPORT"):
                        pesan = "Akun RSA dan BD_SUPPORT tidak bisa reset password lewat halaman ini."
                    else:
                        try:
                            validasi_username(un, izinkan_testing=True)   # username tetap harus kode distributor yang valid (kecuali "testing")
                        except ValueError as e:
                            pesan = str(e)
                    user = [] if pesan else _cari_user(username=un)
                    if not pesan and user and user[0].get("tagging") in (TAGGING_RSA, TAGGING_BD):
                        pesan = "Akun RSA dan BD_SUPPORT tidak bisa reset password lewat halaman ini."
                    # email TIDAK perlu sama dengan data lama: hanya kontak yang dicatat di permintaan
                    if not pesan and not user:
                        pesan = "Username tidak ditemukan."
                    if pesan:
                        st.error(pesan)
                    else:
                        u = user[0]["username"]
                        if not reset_cari_permintaan(u):          # sudah ada permintaan aktif -> jangan dobel
                            reset_buat_permintaan(u, em)
                        ss.reset_user, ss.reset_view = u, "waiting"
                        st.rerun()
                except Exception as e:
                    st.error(f"Gagal mengirim permintaan: {e}")
        return

    # 1b. sudah pernah mengajukan: cukup username, langsung ke status permintaan
    if view == "cek":
        st.markdown(_svg_status("wait"), unsafe_allow_html=True)
        _judul("Cek status permintaan", "Masukkan <span style=\"color:#a95e69;font-weight:700\">username</span> "
                                        "yang kamu pakai saat mengajukan reset password.")
        with st.form("reset_cek_form", border=False):
            st.markdown('<div class="field-label">Username</div>', unsafe_allow_html=True)
            un = st.text_input("Username", placeholder="Kode distributor terdaftar",
                               label_visibility="collapsed")
            cek = st.form_submit_button("Cek status", type="primary", width="stretch")
        if cek:
            un = un.strip().lower()
            try:
                if not un:
                    st.error("Isi username.")
                elif un.upper() in ("RSA", "BD_SUPPORT"):
                    st.error("Akun RSA dan BD_SUPPORT tidak bisa reset password lewat halaman ini.")
                elif reset_cari_permintaan(un) is None:
                    st.error("Belum ada permintaan aktif untuk username ini. Kembali dan ajukan permintaan baru.")
                else:
                    ss.reset_user, ss.reset_view = un, "waiting"
                    st.rerun()
            except Exception as e:
                st.error(f"Gagal memeriksa status: {e}")
        return

    u = ss.get("reset_user", "")
    try:
        req = reset_cari_permintaan(u)
    except Exception as e:
        st.error(f"Could not check the request status: {e}")
        return
    if req is None:                                     # tidak ada permintaan aktif (mis. sudah dipakai)
        _keluar_reset()
        st.rerun()
    status = req["status"]
    if status == RESET_APPROVED:
        ss.reset_view = view = "newpw"

    # 2. menunggu / ditolak
    if view == "waiting":
        if status == RESET_REJECTED:
            st.markdown(_svg_status("no"), unsafe_allow_html=True)
            _judul("Request declined", "The admin declined your reset request. Please contact the admin for help.")
            if st.button("Send a new request", type="primary", width="stretch", key="reset_ulang"):
                reset_ubah_status(req["id"], RESET_DONE)       # tutup yang ditolak, user boleh mengajukan lagi
                ss.reset_view = "forgot"
                st.rerun()
            return
        st.markdown(_svg_status("wait"), unsafe_allow_html=True)
        _judul("Waiting for approval", "Your request has been sent to the admin. Come back and check the status once it has been approved.")
        if st.button(":material/refresh: Check status", type="primary", width="stretch", key="reset_cek"):
            st.rerun()
        return

    # 3. disetujui -> password baru
    st.markdown(_svg_status("ok"), unsafe_allow_html=True)
    _judul("Create new password", "Your request was approved. Your new password must be different from previously "
                                  "used passwords.")
    with st.form("newpw_form", border=False):
        pw1 = st.text_input("New password", placeholder="Password (min. 8 characters)", type="password",
                            label_visibility="collapsed")
        pw2 = st.text_input("Confirm password", placeholder="Confirm password", type="password",
                            label_visibility="collapsed")
        simpan = st.form_submit_button("Reset password", type="primary", width="stretch")
    if simpan:
        if len(pw1) < 8:
            st.error("Password must be at least 8 characters.")
        elif pw1 != pw2:
            st.error("Both passwords must match.")
        else:
            try:
                lama = _cari_user(username=u)
                if lama and bcrypt.checkpw(pw1.encode(), lama[0]["password"].encode()):
                    st.error("New password must be different from the previous one.")
                elif not reset_password_user(u, pw1):
                    st.error("Could not update the password. Please contact the admin.")
                else:
                    reset_ubah_status(req["id"], RESET_DONE)
                    _keluar_reset()
                    ss["pesan_sukses"] = "Password updated. Please log in with your new password."
                    st.rerun()
            except Exception as e:
                st.error(f"Could not update the password: {e}")


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
    components.html(
        f"""<script>
        const sec = (location.protocol === 'https:') ? '; SameSite=None; Secure' : '; SameSite=Lax';
        window.parent.document.cookie = "{COOKIE_LOGIN}={token}; max-age={detik}; path=/" + sec;
        </script>""", height=0)


def keluar():
    ss = st.session_state
    ss.clear()
    ss["_auth_dicoba"] = True      # cookie lama masih terbaca di sesi ini -> jangan dipulihkan lagi
    ss["_hapus_cookie"] = True


def require_login(app: str = ""):
    # `app` memisahkan cookie login per aplikasi (mis. "submission" / "validation"): cookie dibagi ke semua port di
    # host yang sama, jadi tanpa ini login di satu aplikasi otomatis ikut masuk di aplikasi lain.
    global COOKIE_LOGIN
    COOKIE_LOGIN = f"skp_auth_{app}" if app else "skp_auth"
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
                if ss.get("reset_view"):
                    tampil_reset_password()
                else:
                    st.markdown(FORM_HEAD_HTML, unsafe_allow_html=True)

                    if ss.get("pesan_sukses"):
                        st.success(ss["pesan_sukses"])

                    with st.form("login_form", border=False):
                        username = st.text_input("Username", placeholder="Username", label_visibility="collapsed")
                        password = st.text_input("Password", placeholder="Password", type="password", label_visibility="collapsed")
                        ingat_saya = st.checkbox("Remember me", help="Stay signed in for 1 day on this browser.")
                        submitted = st.form_submit_button("Login →", type="primary", width="stretch")

                    if submitted:
                        ss.pop("pesan_sukses", None)   # banner hilang setelah user mencoba login
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
                        c_chat, c_tut, c_forgot = st.columns([1.2, 0.85, 1.1], vertical_alignment="center")
                        if c_chat.button(":material/help: Help & Support", key="login_help"):
                            from help_support import dialog_help_support     # popup tiket (tanpa Google Form)
                            dialog_help_support()
                        if c_tut.button(":material/school: Tutorial", key="login_tutorial"):
                            from tutorial import buka_kunci
                            buka_kunci("login", manual=True)
                        if c_forgot.button("Forgot password?"):
                            ss.reset_view = "forgot"
                            st.rerun()

                    with st.container(key="signup_btn"):
                        if st.button("Sign Up", width="stretch"):
                            signup_dialog()

                    st.markdown(FINE_PRINT_HTML, unsafe_allow_html=True)

    # tutorial halaman login: TIDAK otomatis (supaya tidak mengganggu), hanya saat user klik tombol Tutorial
    from tutorial import tampilkan_tutorial_jika_aktif
    tampilkan_tutorial_jika_aktif()
    st.stop()

