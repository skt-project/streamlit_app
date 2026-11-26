"""Pop-up Welcome (sekali per login) untuk SEMUA peran: Admin Distributor, SPV, BD Support, RSA.
Distributor dicari dari kode (= username) akun lewat sheet Distributor Mapping. Dipakai oleh app_submission & app_main."""
import html as _h
import random

import streamlit as st

import shared as _shared
from avatar_hewan import _HEWAN_SVG, _HEWAN_URUT

PERAN_INTERNAL = ("RSA", "BD_SUPPORT", "BD SUPPORT")      # melihat semua distributor


def _tentukan(user: dict):
    """-> (judul, sub, peran, region, tampil_kode)"""
    kode = str(user.get("username", "")).strip().upper()
    tagging = user.get("tagging", "")
    if tagging in PERAN_INTERNAL:
        peran = "RSA" if tagging == "RSA" else "BD Support"
        return (kode or peran), "All distributors", peran, "", False
    try:
        info = _shared.muat_distri_code_map().get(kode)
    except Exception:
        info = None
    distri = info["distributor"] if info else ""
    peran = "SPV" if tagging == "SPV" else "Admin"
    region = (_shared.region_grup(distri) if distri else "") or _shared.region_dari_kode(kode)
    sub = "" if distri else f"Code {kode} is not linked to an active distributor yet."
    return (distri or "Your account"), sub, peran, region, True


def _hewan_besar() -> str:
    """Hewan kawaii ACAK setiap kali login (dipilih sekali per sesi, supaya tidak berganti saat dialog digambar ulang);
    digerakkan lewat CSS (.wl-hewan)."""
    ss = st.session_state
    if ss.get("_welcome_hewan") not in _HEWAN_SVG:
        ss["_welcome_hewan"] = random.choice(_HEWAN_URUT)
    h = ss["_welcome_hewan"]
    return (f'<svg class="wl-hewan" width="132" height="132" viewBox="0 0 64 64" xmlns="http://www.w3.org/2000/svg">'
            f'{_HEWAN_SVG[h]}</svg>')


_HATI = ('<svg viewBox="0 0 24 24" width="{u}" height="{u}"><path fill="{w}" d="M12 21s-7.5-4.6-9.6-9.2C.9 8.4 2.6 5 5.9 5c2 0 3.4 1.1 4.1 2.4'
         'C10.7 6.1 12.1 5 14.1 5c3.3 0 5 3.4 3.5 6.8C19.5 16.4 12 21 12 21z"/></svg>')

# ikon bulat oranye di sekeliling maskot (toko, kotak SKU, centang)
_IKON = {
    "toko": '<svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 9l1.5-5h13L20 9"/><path d="M4 9v11h16V9"/><path d="M4 9c0 2 3 2 4 0 1 2 3 2 4 0 1 2 3 2 4 0 1 2 4 2 4 0"/></svg>',
    "kotak": '<svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="5" rx="1"/><path d="M5 9v10h14V9"/><path d="M10 13h4"/></svg>',
    "centang": '<svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="#fff" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M8 12.5l3 3 5-6"/></svg>',
}


@st.dialog(" ", width="small")
def dialog_welcome(user: dict):
    kode = str(user.get("username", "")).strip().upper()
    judul, sub, peran, region, tampil_kode = _tentukan(user)
    hewan = _hewan_besar()
    hati = "".join(f'<span class="wl-love" style="left:{x}%;animation-delay:{d}s;--u:{u}px">{_HATI.format(u=u, w=w)}</span>'
                   for x, d, u, w in ((14, 0.0, 15, "#f9a8b8"), (30, 1.1, 11, "#fb7185"), (66, 0.5, 13, "#f9a8b8"),
                                      (84, 1.6, 16, "#fb7185"), (50, 2.2, 11, "#f9a8b8")))
    chips = "".join(f'<span class="wl-chip">{_h.escape(t)}</span>' for t in
                    ([f"Code {kode}"] if (kode and tampil_kode) else []) + [peran] + ([region] if region else []))
    if peran == "Admin":
        info = "Submit NOO &amp; SKU Mapping and track your submissions in History."
    elif peran == "SPV":
        info = "Submit NOO &amp; SKU Mapping, confirm outlet types, and track everything in History."
    else:
        info = "Validate stores, manage SKU Mapping and follow the process across all distributors."
    st.markdown("""<style>
/* tanpa latar berwarna: kartu krem polos, ilustrasi besar di lingkaran lembut */
[data-testid="stDialog"] div[role="dialog"]{ border-radius:26px; background:#fbf8f3 !important; padding-top:0 !important; }
[data-testid="stDialog"] [data-testid="stDialogHeader"], [data-testid="stDialog"] h2{ display:none; }
.wl-card{ text-align:center; padding:0 4px; }
.wl-hi{ font-size:24px; font-weight:800; color:#3b2a1e; line-height:1.15; margin-top:4px; }
.wl-name{ font-size:24px; font-weight:800; color:#b45309; line-height:1.2; margin:2px 0 6px; }
.wl-sub{ font-size:13px; color:#7a6a5d; }
.wl-stage{ position:relative; height:250px; margin:6px 0 2px; }
.wl-disc{ position:absolute; left:50%; top:18px; width:206px; height:206px; margin-left:-103px; border-radius:50%; background:#fff;
          display:flex; align-items:center; justify-content:center; animation:wlBob 3s ease-in-out infinite; }
.wl-hewan{ transform-origin:50% 90%; animation:wlWave 3s ease-in-out infinite; }
.wl-ikon{ position:absolute; width:46px; height:46px; border-radius:50%; background:#f97316; display:flex; align-items:center; justify-content:center;
          box-shadow:0 8px 18px rgba(249,115,22,.30); animation:wlFloat 3.4s ease-in-out infinite; }
.wl-ring{ position:absolute; width:16px; height:16px; border:3px solid #a78bfa; border-radius:50%; }
.wl-squig{ position:absolute; }
.wl-love{ position:absolute; bottom:30px; width:var(--u); height:var(--u); opacity:0; animation:wlLove 3.6s ease-in infinite; }
@keyframes wlBob{ 0%,100%{ transform:translateY(0) } 50%{ transform:translateY(-6px) } }
@keyframes wlWave{ 0%,100%{ transform:rotate(-4deg) } 50%{ transform:rotate(4deg) scale(1.03) } }
@keyframes wlFloat{ 0%,100%{ transform:translateY(0) } 50%{ transform:translateY(-8px) } }
@keyframes wlLove{ 0%{ transform:translateY(0) scale(.6); opacity:0 } 15%{ opacity:.95 } 100%{ transform:translateY(-110px) scale(1.15); opacity:0 } }
@media (prefers-reduced-motion:reduce){ .wl-disc,.wl-hewan,.wl-ikon,.wl-love{ animation:none } .wl-love{ opacity:0 } }
.wl-chips{ display:flex; justify-content:center; flex-wrap:wrap; gap:6px; margin:10px 0 8px; }
.wl-chip{ font-size:11.5px; font-weight:700; padding:3px 12px; border-radius:99px; background:#f1e7da; color:#6b4a2f; }
.wl-info{ font-size:13px; color:#7a6a5d; line-height:1.55; margin:2px 10px 14px; }
/* tombol Get Started: pil cokelat tua + panah */
.st-key-btn_welcome_mulai button{ height:54px; border-radius:99px !important; background:#4a3426 !important; border:0 !important;
  box-shadow:0 12px 24px rgba(74,52,38,.30); }
.st-key-btn_welcome_mulai button p{ color:#fff !important; font-size:15px !important; font-weight:700 !important; letter-spacing:.3px; }
.st-key-btn_welcome_mulai button:hover{ background:#3b2a1e !important; transform:translateY(-1px); }
</style>""", unsafe_allow_html=True)
    st.markdown(
        '<div class="wl-card">'
        '<div class="wl-hi">Welcome back,</div>'
        f'<div class="wl-name">{_h.escape(judul)}</div>'
        + (f'<div class="wl-sub">{_h.escape(sub)}</div>' if sub else '') +
        '<div class="wl-stage">'
        f'{hati}'
        f'<div class="wl-disc">{hewan}</div>'
        f'<div class="wl-ikon" style="left:6%;top:48px">{_IKON["toko"]}</div>'
        f'<div class="wl-ikon" style="right:5%;top:22px;animation-delay:.6s">{_IKON["kotak"]}</div>'
        f'<div class="wl-ikon" style="right:10%;bottom:34px;animation-delay:1.2s">{_IKON["centang"]}</div>'
        '<span class="wl-ring" style="left:15%;bottom:62px"></span>'
        '<svg class="wl-squig" style="left:40%;top:0" width="64" height="18" viewBox="0 0 64 18" fill="none" stroke="#f5c518" stroke-width="3.5" stroke-linecap="round">'
        '<path d="M3 12c5-9 8 6 13-2s8 6 13-2 8 6 13-2 8 6 13-2"/></svg>'
        '<svg class="wl-squig" style="left:22%;bottom:4px" width="20" height="20" viewBox="0 0 20 20" fill="none" stroke="#f5c518" stroke-width="3.5" stroke-linecap="round">'
        '<path d="M4 3a9 9 0 0 0 0 14"/></svg>'
        '</div>'
        f'<div class="wl-chips">{chips}</div><div class="wl-info">{info}</div></div>', unsafe_allow_html=True)
    if st.button("Get Started  →", type="primary", width="stretch", key="btn_welcome_mulai"):
        st.rerun()
