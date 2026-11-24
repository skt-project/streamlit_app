import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import streamlit as st

import shared as _shared
from shared import sb_insert

TABLE_USER = "users"
TABLE_TUT_FB = "tutorial_feedback"
ZONA = ZoneInfo("Asia/Jakarta")
HARI_AKUN_BARU = 3                                      # akun baru: popup otomatis dari hari daftar (H) sampai H+3
PERAN_TANPA_TUTORIAL = ("BD_SUPPORT", "BD SUPPORT")     # tidak pernah dapat popup otomatis
# masa peluncuran: SEMUA akun (kecuali BD Support) dapat popup otomatis sekali pada rentang tanggal ini. Setelahnya normal:
# hanya akun baru (H s/d H+3). Ubah tanggalnya di sini.
PELUNCURAN_MULAI = datetime.date(2026, 11, 26)
PELUNCURAN_AKHIR = datetime.date(2026, 11, 27)

from tutorial_isi import TUTORIAL, ILUSTRASI, CSS_ILUSTRASI      # teks & ilustrasi tutorial (bahasa Indonesia)

TINGGI_ISI = 420          # tinggi area isi tutorial (px), sama untuk semua langkah; isi lebih panjang di-scroll
_HALAMAN_KE_TUTORIAL = {"pengajuan": "pengajuan", "spv": "spv", "history": "intro"}


# ===================================================================== STATUS (Supabase)
def _muat_seen(username: str, tagging: str = ""):
    # tutorial yang sudah dijawab feedback-nya (baris tabel tutorial_feedback) dianggap sudah dilihat dan sudah ditanya.
    # Gagal baca = set kosong (tutorial tampil sekali per sesi).
    try:
        filt = f"(user.eq.{username},user.like.RSA - *)" if str(tagging).strip().upper() == "RSA" else f"(user.eq.{username})"
        r = requests.get(f"{_shared.SUPABASE_URL}/rest/v1/{TABLE_TUT_FB}",
                         headers={**_shared._SB_HEADERS, "Prefer": ""},
                         params={"select": "tutorial_key", "or": filt, "limit": 300}, timeout=10)
        r.raise_for_status()
        seen = set()
        for x in r.json():
            kunci = str(x.get("tutorial_key") or "")
            if kunci in TUTORIAL:
                seen.update({kunci, f"{kunci}_fb"})
        return seen
    except Exception:
        return set()


def _muat_info(username: str):
    # (created_at tanggal WIB atau None, tagging) dari tabel users; kolom ini sudah ada di tabel users.
    try:
        r = requests.get(f"{_shared.SUPABASE_URL}/rest/v1/{TABLE_USER}",
                         headers={**_shared._SB_HEADERS, "Prefer": ""},
                         params={"select": "created_at,tagging", "username": f"eq.{username}", "limit": 1}, timeout=10)
        r.raise_for_status()
        rows = r.json()
        if not rows:
            return None, ""
        dibuat = pd.to_datetime(rows[0].get("created_at"), utc=True, errors="coerce")
        tgl = None if pd.isna(dibuat) else dibuat.tz_convert(ZONA).date()
        return tgl, str(rows[0].get("tagging") or "")
    except Exception:
        return None, ""


def _berhak_otomatis() -> bool:
    # Aturan popup otomatis: bukan BD Support, dan (masa peluncuran ATAU akun dibuat dalam H..H+3).
    ss = st.session_state
    tgl_dibuat, tagging = ss.get("_tut_info", (None, ""))
    tagging = (tagging or str((ss.get("user") or {}).get("tagging", ""))).strip().upper()
    if tagging in PERAN_TANPA_TUTORIAL:
        return False
    hari_ini = datetime.datetime.now(ZONA).date()
    if PELUNCURAN_MULAI <= hari_ini <= PELUNCURAN_AKHIR:
        return True
    return tgl_dibuat is not None and 0 <= (hari_ini - tgl_dibuat).days <= HARI_AKUN_BARU


def _username() -> str:
    return str((st.session_state.get("user") or {}).get("username", "")).strip().lower()


def _seen_user():
    ss = st.session_state
    u = _username()
    # muat ulang kalau user berganti ATAU salah satu kunci hilang (mis. skrip terhenti di tengah, atau state sisa versi kode lama)
    if ss.get("_tut_user") != u or "_tut_seen" not in ss or "_tut_info" not in ss:
        info = _muat_info(u) if u else (None, "")
        seen = _muat_seen(u, info[1]) if u else None      # None hanya kalau belum login
        ss["_tut_info"], ss["_tut_seen"], ss["_tut_user"] = info, seen, u     # _tut_user terakhir: ditulis hanya kalau semuanya sudah siap
    return ss["_tut_seen"]


# ===================================================================== PEMICU OTOMATIS
def picu_tutorial_otomatis(halaman: str):
    # Popup otomatis (sekali per tutorial, bisa di-Skip) hanya untuk akun yang berhak, lihat _berhak_otomatis().
    ss = st.session_state
    if ss.get("tut_aktif") or ss.get("tut_fb"):      # feedback tutorial yang sedang menunggu dijawab dulu, baru tutorial berikutnya
        return
    seen = _seen_user()
    if seen is None or not _berhak_otomatis():
        return
    tutup = ss.setdefault("_tut_tutup", set())
    for kunci in ("intro", _HALAMAN_KE_TUTORIAL.get(halaman)):
        if kunci and kunci in TUTORIAL and kunci not in seen and kunci not in tutup:
            ss["tut_aktif"], ss["tut_langkah"], ss["tut_manual"] = kunci, 0, False
            return


def buka_kunci(kunci: str, manual: bool = True):
    # Buka tutorial berdasarkan kunci (dipakai halaman login, sebelum ada user).
    ss = st.session_state
    ss["tut_aktif"], ss["tut_langkah"], ss["tut_manual"] = kunci, 0, manual


def buka_tutorial(halaman: str):
    ss = st.session_state
    ss["tut_aktif"], ss["tut_langkah"], ss["tut_manual"] = _HALAMAN_KE_TUTORIAL.get(halaman, "intro"), 0, True


def _selesai():
    ss = st.session_state
    kunci = ss.pop("tut_aktif", None)
    ss.pop("tut_langkah", None)
    manual = ss.pop("tut_manual", False)
    if not kunci or manual:
        return
    ss.setdefault("_tut_tutup", set()).add(kunci)
    seen = ss.get("_tut_seen")
    if seen is not None and kunci not in seen:
        seen.add(kunci)


def _selesai_dan_tanya():
    # tutorial diselesaikan (bukan dilewati): tutup tutorial, lalu arahkan user mengisi feedback tutorial (sekali per tutorial per akun)
    ss = st.session_state
    kunci = ss.get("tut_aktif")
    _selesai()
    seen = ss.get("_tut_seen")
    sudah = ss.setdefault("_tut_fb_tutup", set())
    if kunci and _username() and seen is not None and f"{kunci}_fb" not in seen and kunci not in sudah:
        ss["tut_fb"], ss["tut_fb_pilih"] = kunci, None
        ss.pop("tut_fb_selesai", None)


def _tandai_fb_ditanya():
    ss = st.session_state
    kunci = ss.get("tut_fb")
    if not kunci:
        return
    ss.setdefault("_tut_fb_tutup", set()).add(kunci)
    seen = ss.get("_tut_seen")
    if seen is not None and f"{kunci}_fb" not in seen:
        seen.add(f"{kunci}_fb")


def _tutup_fb():
    _tandai_fb_ditanya()
    ss = st.session_state
    for k in ("tut_fb", "tut_fb_pilih", "tut_fb_selesai", "tut_fb_komentar"):
        ss.pop(k, None)


def _wajah_kawaii(jenis: str, ukuran: int = 76) -> str:
    # wajah kawaii: "ya" = senang mata tertutup + pipi merah muda, "belum" = bingung manis
    if jenis == "ya":
        warna, mata, mulut = "#ffb3c9", '<path d="M25 36q5-6 10 0M45 36q5-6 10 0" stroke="#5a2a3c" stroke-width="3" fill="none" stroke-linecap="round"/>', '<path d="M32 46q8 9 16 0z" fill="#5a2a3c"/>'
    else:
        warna, mata, mulut = "#d9d2f5", '<circle cx="30" cy="36" r="3.4" fill="#4a3f6b"/><circle cx="50" cy="36" r="3.4" fill="#4a3f6b"/>', '<path d="M34 50q6-5 12 0" stroke="#4a3f6b" stroke-width="3" fill="none" stroke-linecap="round"/>'
    return (f'<svg width="{ukuran}" height="{ukuran}" viewBox="0 0 80 80"><circle cx="40" cy="42" r="30" fill="{warna}"/>'
            f'<circle cx="22" cy="46" r="5" fill="#ff8fb0" opacity=".55"/><circle cx="58" cy="46" r="5" fill="#ff8fb0" opacity=".55"/>'
            f'{mata}{mulut}</svg>')


_CSS_FB_TUT = (
    '<style>@keyframes tf-bob{0%,100%{transform:translateY(0)}50%{transform:translateY(-6px)}}'
    '@keyframes tf-twinkle{0%,100%{opacity:.3;transform:scale(.8)}50%{opacity:1;transform:scale(1.2)}}'
    '@keyframes tf-rise{0%{transform:translateY(10px);opacity:0}30%{opacity:1}100%{transform:translateY(-34px);opacity:0}}'
    '@keyframes tf-beat{0%,100%{transform:scale(1)}30%{transform:scale(1.15)}60%{transform:scale(.96)}}'
    '.tf-bob{display:inline-block;animation:tf-bob 2.2s ease-in-out infinite}'
    '.tf-judul{font-size:17px;font-weight:800;color:#3a2a35;text-align:center;margin:6px 0 2px}'
    '.tf-sub{font-size:12px;color:#8c7a80;text-align:center;margin-bottom:10px}'
    '.tf-pil{border:2px solid #f1e3e8;border-radius:16px;padding:8px 4px 2px;text-align:center;background:#fffafb}'
    '.tf-pil.on{border-color:#ff7aa2;background:#fff0f5;box-shadow:0 4px 14px rgba(255,122,162,.25)}'
    '.tf-thx{text-align:center;padding:6px 0 2px}'
    '.tf-thx .jd{font-size:26px;font-weight:800;color:#e8508a;margin-top:6px;letter-spacing:-.5px}'
    '.tf-thx .ps{font-size:12.5px;color:#6b5b63;margin:4px 0 4px}'
    '.tf-hati{position:absolute;color:#ff7aa2;animation:tf-rise 2.4s ease-in infinite}'
    '.st-key-tut_fb_dlg button{min-height:30px !important;border-radius:9px !important}'
    '.st-key-tut_fb_dlg button p{font-size:12px !important;font-weight:700 !important}</style>')


def _isi_fb_tutorial():
    ss = st.session_state
    st.markdown(_CSS_FB_TUT, unsafe_allow_html=True)
    with st.container(key="tut_fb_dlg"):
        if ss.get("tut_fb_selesai"):
            hati = "".join(f'<span class="tf-hati" style="left:{x}%;animation-delay:{d}s;font-size:{z}px">&#10084;</span>'
                           for x, d, z in ((18, 0, 14), (36, .6, 10), (62, 1.1, 16), (80, 1.7, 11)))
            kilau = "".join(f'<span style="position:absolute;color:#f5b400;font-size:{z}px;left:{x}%;top:{y}%;animation:tf-twinkle 1.6s ease-in-out {d}s infinite">&#9733;</span>'
                            for x, y, z, d in ((14, 20, 14, 0), (82, 14, 11, .5), (74, 62, 13, 1), (22, 66, 10, 1.4)))
            st.markdown(
                '<div class="tf-thx"><div style="position:relative;height:120px;display:flex;justify-content:center;align-items:center">'
                f'{hati}{kilau}<span style="animation:tf-beat 1.4s ease-in-out infinite;display:inline-block">'
                '<svg width="104" height="96" viewBox="0 0 104 96"><path d="M52 90C18 64 4 46 4 28 4 14 15 4 29 4c9 0 17 5 23 13C58 9 66 4 75 4c14 0 25 10 25 24 0 18-14 36-48 62z" fill="#ff7aa2"/>'
                '<circle cx="36" cy="38" r="4" fill="#fff"/><circle cx="68" cy="38" r="4" fill="#fff"/>'
                '<path d="M42 52q10 10 20 0" stroke="#fff" stroke-width="4" fill="none" stroke-linecap="round"/>'
                '<circle cx="26" cy="48" r="5" fill="#ffc2d6" opacity=".8"/><circle cx="78" cy="48" r="5" fill="#ffc2d6" opacity=".8"/></svg></span></div>'
                '<div class="jd">Thanks ya!</div>'
                '<div class="ps">Masukanmu bikin tutorial ini makin baik dan makin gampang dipahami.</div></div>',
                unsafe_allow_html=True)
            if st.button("Tutup", key="tut_fb_tutup", type="primary", width="stretch"):
                _tutup_fb()
                st.rerun()
            return

        st.markdown(f'<div style="text-align:center"><span class="tf-bob">{_wajah_kawaii("ya", 64)}</span></div>'
                    '<div class="tf-judul">Apakah tutorial ini membantumu?</div>'
                    '<div class="tf-sub">Jawabanmu membantu kami memperbaiki tutorial.</div>', unsafe_allow_html=True)
        pilih = ss.get("tut_fb_pilih")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f'<div class="tf-pil{" on" if pilih == "ya" else ""}">{_wajah_kawaii("ya")}</div>', unsafe_allow_html=True)
            if st.button("Membantu banget", key="tut_fb_ya", width="stretch"):
                ss["tut_fb_pilih"] = "ya"
                st.rerun(scope="fragment")
        with c2:
            st.markdown(f'<div class="tf-pil{" on" if pilih == "belum" else ""}">{_wajah_kawaii("belum")}</div>', unsafe_allow_html=True)
            if st.button("Belum membantu", key="tut_fb_belum", width="stretch"):
                ss["tut_fb_pilih"] = "belum"
                st.rerun(scope="fragment")
        if pilih:
            ph = "Bagian mana yang paling membantu? (opsional)" if pilih == "ya" else "Bagian mana yang kurang jelas? (opsional)"
            komentar = st.text_area("Komentar", key="tut_fb_komentar", height=70, placeholder=ph, label_visibility="collapsed")
        else:
            komentar = ""
        c_lewat, c_kirim = st.columns(2)
        if c_lewat.button("Nanti saja", key="tut_fb_nanti", width="stretch"):
            _tutup_fb()
            st.rerun()
        if c_kirim.button("Kirim", key="tut_fb_kirim", type="primary", width="stretch", disabled=not pilih):
            kunci_tut = ss.get("tut_fb")
            nama = ss.get("fb_nama_rsa") or _username()      # akun RSA: pakai nama "RSA - <nama>" kalau sudah pernah diisi di feedback
            baris = {
                "tutorial_key": kunci_tut,
                "tutorial_judul": TUTORIAL.get(kunci_tut, ("Tutorial",))[0],
                "membantu": pilih == "ya",
                "komentar": komentar.strip()[:1000] or None,
                "user": nama,
            }
            with st.spinner("Mengirim..."):
                hasil = sb_insert(TABLE_TUT_FB, [baris])
            if isinstance(hasil, dict) and "_error" in hasil:
                st.error(f"Gagal mengirim: {hasil['_error']}")
            else:
                _tandai_fb_ditanya()
                ss["tut_fb_selesai"] = True
                st.rerun(scope="fragment")


@st.dialog("Gimana tutorialnya?", width="small", on_dismiss=_tutup_fb)
def _dialog_fb_tutorial():
    _isi_fb_tutorial()


# ===================================================================== TAMPILAN
def _isi_tutorial_inner():
    ss = st.session_state
    kunci = ss.get("tut_aktif")
    if kunci not in TUTORIAL:
        _selesai()
        return
    langkah = TUTORIAL[kunci][1]
    n = len(langkah)
    i = min(max(ss.get("tut_langkah", 0), 0), n - 1)
    nama_ilustrasi, judul, isi = langkah[i]
    st.markdown(
        '<style>.tut-bar{height:5px;border-radius:5px;background:#f1e9ee;overflow:hidden;margin:2px 0 12px}'
        '.tut-bar i{display:block;height:100%;background:linear-gradient(90deg,#ff4f8b,#ff8a5c);border-radius:5px}'
        '.tut-no{font-size:11px;color:#9a93ad;font-weight:600}.tut-j{font-size:16px;font-weight:800;color:#1c1633;margin:2px 0 6px}'
        + CSS_ILUSTRASI + '</style>'
        f'<div class="tut-no">Langkah {i + 1} dari {n}</div><div class="tut-bar"><i style="width:{(i + 1) / n * 100:.0f}%"></i></div>',
        unsafe_allow_html=True)
    # area isi berukuran tetap di semua langkah (dialog tidak naik-turun); kalau lebih panjang, bisa di-scroll ke bawah
    with st.container(height=TINGGI_ISI, border=False):
        st.markdown(ILUSTRASI.get(nama_ilustrasi, ""), unsafe_allow_html=True)
        st.markdown(f'<div class="tut-j">{judul}</div>', unsafe_allow_html=True)
        st.markdown(isi)

    c_skip, c_back, c_next = st.columns([1, 1, 1.3])
    if c_skip.button("Lewati", key="tut_skip", width="stretch"):
        _selesai()
        st.rerun()
    if c_back.button("Kembali", key="tut_back", width="stretch", disabled=i == 0):
        ss["tut_langkah"] = i - 1
        st.rerun(scope="fragment")
    if i < n - 1:
        if c_next.button("Lanjut", key="tut_next", type="primary", width="stretch"):
            ss["tut_langkah"] = i + 1
            st.rerun(scope="fragment")
    elif c_next.button("Selesai", key="tut_fin", type="primary", width="stretch"):
        _selesai_dan_tanya()
        st.rerun()


_CSS_DIALOG = (
    '<style>div[data-testid="stDialog"] div[role="dialog"]{width:620px !important;max-width:92vw !important}'
    '.st-key-tut_dlg p,.st-key-tut_dlg li{font-size:13px !important;line-height:1.45 !important;margin-bottom:.35rem !important}'
    '.st-key-tut_dlg button{min-height:30px !important;height:30px !important;padding:0 12px !important;border-radius:8px !important}'
    '.st-key-tut_dlg button p{font-size:12px !important;margin:0 !important;font-weight:600 !important}'
    '.st-key-tut_dlg [data-testid="stHorizontalBlock"]{gap:.5rem !important}</style>')


def _isi_tutorial():
    st.markdown(_CSS_DIALOG, unsafe_allow_html=True)
    with st.container(key="tut_dlg"):
        _isi_tutorial_inner()


@st.dialog("Tutorial", width="medium", on_dismiss=_selesai)
def _dialog_tutorial():
    _isi_tutorial()


def tampilkan_tutorial_jika_aktif():
    if st.session_state.get("tut_aktif"):
        _dialog_tutorial()
    elif st.session_state.get("tut_fb"):
        _dialog_fb_tutorial()
