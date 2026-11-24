import requests
import streamlit as st

import shared as _shared
from shared import sb_insert

TABLE_FEEDBACK = "feedback"
KOLOM_USER = "user"
DAFTAR_RSA = ["Aqil", "Geirda", "Kayla", "Erliana", "Shaltsa", "Furqan", "Miftah", "Others"]
# akun yang TIDAK diminta feedback (RSA sekarang ikut diminta)
PERAN_TANPA_FEEDBACK = ("BD_SUPPORT", "BD SUPPORT")

# rating -> (label, interpretasi)
RATING_INFO = {
    1: ("Very Dissatisfied", "Sangat sulit dan banyak kendala"),
    2: ("Dissatisfied",      "Kurang praktis dan banyak hal perlu diperbaiki"),
    3: ("Neutral",           "Cukup baik, tetapi masih ada kendala"),
    4: ("Satisfied",         "Mudah digunakan dengan sedikit kekurangan"),
    5: ("Very Satisfied",    "Sangat mudah, efisien, dan nyaman digunakan"),
}
LABEL_ID = {1: "Sangat Tidak Puas", 2: "Tidak Puas", 3: "Cukup", 4: "Puas", 5: "Sangat Puas"}

OPSI_KATEGORI = [
    "Kemudahan Penggunaan", "Proses Terlalu Rumit", "Proses Memakan Waktu", "Navigasi & Petunjuk",
    "Performa Sistem", "Fitur & Fungsionalitas", "Lainnya",
]


def _username() -> str:
    return str((st.session_state.get("user") or {}).get("username", "")).strip()


def _sudah_kasih_feedback(username: str):
    ss = st.session_state
    if str((ss.get("user") or {}).get("tagging") or "").strip().upper() == "RSA":
        username = ss.get("fb_nama_rsa") or ""             # di tabel, user RSA tersimpan sebagai nama RSA
        if not username:
            ss["_fb_rows"] = 0
            return False
    # SELALU cek langsung ke tabel Supabase (tanpa cache session): kalau baris feedback user ini sudah dihapus,
    # statusnya otomatis kembali "belum pernah" dan popup wajib muncul lagi.
    try:
        r = requests.get(
            f"{_shared.SUPABASE_URL}/rest/v1/{TABLE_FEEDBACK}",
            headers={**_shared._SB_HEADERS, "Prefer": ""},
            params={"select": "rating", KOLOM_USER: f"eq.{username}", "limit": 1,
                    "or": "(kategori.is.null,kategori.neq.Tutorial)"}, timeout=10)   # feedback Tutorial tidak dihitung
        r.raise_for_status()
        rows = r.json()
        ada = bool(rows)
        ss["_fb_rows"] = len(rows) if isinstance(rows, list) else rows
    except Exception as e:
        detail = ""
        try:
            detail = e.response.text[:200]                  # isi error Supabase (mis. kolom `user` belum ada / RLS)
        except Exception:
            pass
        ss.pesan_toast = f"Feedback check failed: {e} {detail}"[:300]
        ss.pesan_toast_icon = ":material/warning:"
        return None
    return ada


def minta_feedback():
    ss = st.session_state
    user = ss.get("user") or {}
    username = _username()
    if not username or user.get("tagging") in PERAN_TANPA_FEEDBACK:
        # ===== DEBUG TOAST (dimatikan): alasan popup tidak muncul - akun dikecualikan / username kosong =====
        # _debug(f"skip: user='{username}', tagging='{user.get('tagging')}' dikecualikan / username kosong")
        return
    sudah = _sudah_kasih_feedback(username)
    if sudah is None:                                      # gagal cek ke Supabase -> jangan menghalangi user
        return
    # sudah pernah kasih feedback: popup (boleh di-skip) cukup MUNCUL SEKALI per sesi login, di submit mana pun yang
    # lebih dulu (NOO / SKU Mapping / SPV Confirmation). Submit berikutnya di sesi yang sama tidak memunculkannya lagi.
    if sudah and ss.get("fb_sudah_ditawari"):
        # ===== DEBUG TOAST (dimatikan): alasan popup tidak muncul - sudah pernah isi & sudah ditawarkan di sesi ini =====
        # _debug(f"skip: user '{username}' sudah pernah isi feedback & popup sudah ditawarkan di sesi ini")
        return
    if sudah:
        ss["fb_sudah_ditawari"] = True
    # belum pernah kasih feedback -> wajib (tanpa silang/skip), muncul di setiap submit sampai diisi
    # ===== DEBUG TOAST (dimatikan): info user, project Supabase, jumlah baris feedback, WAJIB / boleh skip =====
    # _debug(f"user='{username}' | project={_shared.SUPABASE_URL.split('//')[-1].split('.')[0]} | "
    #     f"baris feedback user ini: {ss.get('_fb_rows')} -> {'boleh skip' if sudah else 'WAJIB'}")
    ss["fb_wajib"] = True
    ss["fb_boleh_skip"] = bool(sudah)
    ss.pop("fb_selesai", None)
    for k in ("fb_bintang", "fb_kategori", "fb_lainnya", "fb_keterangan", "fb_rsa", "fb_rsa_lain"):
        ss.pop(k, None)


DEBUG_FEEDBACK = False     # ===== DEBUG TOAST: True = tampilkan toast [feedback] ...; juga hapus tanda # pada baris _debug(...) di atas =====


def _debug(teks: str):
    if DEBUG_FEEDBACK and not st.session_state.get("pesan_toast_debug_lock"):
        st.session_state.pesan_toast = f"[feedback] {teks}"
        st.session_state.pesan_toast_icon = ":material/info:"


def tampilkan_feedback_jika_diminta():
    if st.session_state.get("fb_wajib"):
        (dialog_feedback_skip if st.session_state.get("fb_boleh_skip") else dialog_feedback_wajib)()


def _tutup_feedback():
    # Dipakai tombol Skip dan tombol silang (on_dismiss): popup ditutup tanpa menyimpan apa pun.
    ss = st.session_state
    for k in ("fb_wajib", "fb_selesai", "fb_bintang", "fb_kategori", "fb_lainnya", "fb_keterangan", "fb_rsa", "fb_rsa_lain"):
        ss.pop(k, None)


def _tampilan_terima_kasih():
    st.markdown(
        '<style>'
        '@keyframes fb-pop{0%{transform:scale(0) rotate(-25deg);opacity:0}60%{transform:scale(1.25) rotate(8deg);opacity:1}'
        '100%{transform:scale(1) rotate(0)}}'
        '@keyframes fb-wiggle{0%,100%{transform:rotate(0)}25%{transform:rotate(-14deg) translateY(-3px)}'
        '50%{transform:rotate(8deg)}75%{transform:rotate(-6deg)}}'
        '@keyframes fb-draw{from{stroke-dashoffset:90}to{stroke-dashoffset:0}}'
        '.fb-thumb{display:inline-block;transform-origin:20% 90%;animation:fb-pop .6s ease-out both,'
        'fb-wiggle 1.1s ease-in-out .7s 2}'
        '.fb-thumb path{stroke-dasharray:90;animation:fb-draw .9s ease-out both}'
        '</style>'
        '<div style="padding:8px 0 4px"><span class="fb-thumb">'
        '<svg width="56" height="56" viewBox="0 0 24 24" fill="none" stroke="#e8202a" stroke-width="1.6" '
        'stroke-linecap="round" stroke-linejoin="round"><path d="M7 10v11H3V10h4z"/>'
        '<path d="M7 10l4-8a2.5 2.5 0 0 1 2.5 2.5V8H19a2 2 0 0 1 2 2.3l-1.4 9A2 2 0 0 1 17.6 21H7"/></svg></span>'
        '<div style="font-size:34px;font-weight:500;margin:14px 0 4px;text-shadow:2px 3px 6px rgba(0,0,0,.28)">Thank You</div>'
        '<div style="font-size:13px;font-weight:600;color:#1e9e4a">Your Feedback was Successfully Submitted</div>'
        '</div>', unsafe_allow_html=True)
    st.markdown('<style>.st-key-fb_home button{background:#e8202a !important;border:1px solid #e8202a !important;'
                'border-radius:8px !important;padding:2px 16px !important;min-height:32px !important;'
                'width:auto !important}'
                '.st-key-fb_home button p{color:#fff !important;font-weight:700 !important;font-size:13px !important}'
                '.st-key-fb_home button:hover{background:#c91a23 !important;border-color:#c91a23 !important}'
                '</style>', unsafe_allow_html=True)
    if st.button("Back to home", key="fb_home"):
        st.session_state.pop("fb_selesai", None)
        st.session_state.pop("fb_wajib", None)
        st.rerun()                                          # menutup dialog


@st.dialog("Feedback", width="small", dismissible=False)    # tanpa tombol silang, klik di luar tidak menutup
def dialog_feedback_wajib():
    _isi_dialog(boleh_skip=False)


@st.dialog("Feedback", width="small", on_dismiss=_tutup_feedback)   # ada silang; silang = skip
def dialog_feedback_skip():
    _isi_dialog(boleh_skip=True)


def _isi_dialog(boleh_skip: bool):
    if st.session_state.get("fb_selesai"):
        _tampilan_terima_kasih()
        return
    if boleh_skip:
        st.caption("Anda sudah pernah mengirim feedback, jadi popup ini boleh di-skip.")
    is_rsa = str((st.session_state.get("user") or {}).get("tagging") or "").strip().upper() == "RSA"
    nama_rsa = ""
    if is_rsa:                                              # akun RSA: wajib pilih nama RSA
        st.markdown("**Nama RSA**")
        pilih_rsa = st.selectbox("Nama RSA", DAFTAR_RSA, index=None, key="fb_rsa", placeholder="Pilih nama RSA…",
                                 label_visibility="collapsed")
        nama_rsa = pilih_rsa or ""
        if pilih_rsa == "Others":
            nama_rsa = st.text_input("Nama lainnya", key="fb_rsa_lain", placeholder="Ketik nama Anda…").strip()
    st.markdown("**Bagaimana pengalaman Anda menggunakan sistem ini?**")
    st.caption("Berikan penilaian dari 1–5 bintang")
    idx = st.feedback("stars", key="fb_bintang")           # None atau 0..4
    rating = (idx + 1) if idx is not None else None
    if rating:
        st.caption(f"{LABEL_ID[rating]} - {RATING_INFO[rating][1]}")

    kategori, lainnya, keterangan = [], "", ""
    if rating is not None:
        if rating <= 3:
            judul = "Apa yang perlu ditingkatkan?"
        else:
            judul = "Apa yang sudah baik / masih bisa ditingkatkan?"
        st.divider()
        st.markdown(f"**{judul}**")
        st.caption("Pilih satu atau lebih kategori")
        kategori = st.pills("Kategori", OPSI_KATEGORI, selection_mode="multi", key="fb_kategori",
                            label_visibility="collapsed") or []
        if "Lainnya" in kategori:
            lainnya = st.text_input("Lainnya", key="fb_lainnya", placeholder="Tuliskan kategori lain…")
        st.markdown("**Ceritakan pengalaman atau kendala Anda**")
        keterangan = st.text_area(
            "Keterangan", key="fb_keterangan", height=100, label_visibility="collapsed",
            placeholder="Contoh: Proses upload membutuhkan waktu lama dan terlalu banyak tahapan…")

    # semua wajib: rating, minimal 1 kategori, "Lainnya" harus dijelaskan, dan cerita pengalaman
    kurang = []
    if is_rsa and not nama_rsa:
        kurang.append("nama RSA")
    if rating is None:
        kurang.append("rating bintang")
    else:
        if not kategori:
            kurang.append("kategori")
        if "Lainnya" in kategori and not lainnya.strip():
            kurang.append("isian Lainnya")
        if not keterangan.strip():
            kurang.append("cerita pengalaman")
    if kurang:
        st.caption("Wajib diisi: " + ", ".join(kurang))
    if boleh_skip:
        c_skip, c_kirim = st.columns(2)
        if c_skip.button("Skip", width="stretch", key="fb_skip"):
            _tutup_feedback()
            st.rerun()                                      # menutup dialog
    else:
        c_kirim = st.container()
    if c_kirim.button("Submit", type="primary", width="stretch", key="fb_kirim", disabled=bool(kurang)):
        row = {
            "rating":         rating,
            "kategori":       ", ".join(k for k in kategori if k != "Lainnya" or not lainnya.strip()) or None,
            "interpretasi":   RATING_INFO[rating][1],
            "keterangan":     keterangan.strip(),
            "other_feedback": lainnya.strip() or None,
            KOLOM_USER:       _username(),
        }
        if is_rsa:                                          # akun RSA: kolom user diganti nama RSA yang dipilih/diketik
            row[KOLOM_USER] = f"RSA - {nama_rsa}"
        with st.spinner("Sending..."):
            result = sb_insert(TABLE_FEEDBACK, [row])
        if isinstance(result, dict) and "_error" in result:
            st.error(f"Gagal mengirim feedback: {result['_error']}")
        else:
            for k in ("fb_bintang", "fb_kategori", "fb_lainnya", "fb_keterangan", "fb_rsa", "fb_rsa_lain"):
                st.session_state.pop(k, None)
            if is_rsa:
                st.session_state["fb_nama_rsa"] = f"RSA - {nama_rsa}"  # dipakai untuk cek "sudah pernah kasih feedback"
            st.session_state.fb_selesai = True
            st.rerun(scope="fragment")                      # render ulang dialog saja -> tampil Thank You
