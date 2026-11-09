"""SKP JBP / JCP Periode 2026 — Streamlit entry point.

Run locally:  streamlit run app.py
Configuration lives in Streamlit Secrets (see README.md); no credentials in code.
"""
from __future__ import annotations

import logging

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="SKP JBP / JCP 2026",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="collapsed",
)

from src import page_admin, page_upload, ui  # noqa: E402  (set_page_config must run first)
from src.runtime import get_backends, load_source  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

PAGES = ("home", "upload", "admin", "success")


def go(page: str) -> None:
    st.session_state["page"] = page
    st.query_params["page"] = page


def landing() -> None:
    ui.wordmark()
    st.markdown(
        '<div class="skp-hero"><h1>SKP JBP / JCP<br>Periode 2026</h1>'
        "<p>Upload dokumen SKP (Surat Kesepakatan Program) JBP dan JCP per toko, brand, dan kuartal. "
        "Dokumen tersimpan rapi dan dapat dipantau oleh tim admin.</p></div>",
        unsafe_allow_html=True,
    )
    steps = ["Pilih region, lalu toko.", "Upload PDF untuk setiap SKP (maks 1 MB).",
             "Periksa rangkuman.", "Konfirmasi submit."]
    st.markdown(
        '<div class="skp-steps">' + "".join(
            f'<div class="skp-step"><b>{i}</b><div>{ui.esc(s)}</div></div>' for i, s in enumerate(steps, 1)
        ) + "</div>",
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns(2, gap="large")
    with c1:
        st.markdown(
            '<div class="skp-card"><div class="icon">📤</div><h3>Upload SKP JBP atau JCP</h3>'
            "<p>Pilih region dan toko, lalu upload PDF untuk setiap SKP yang tercatat di spreadsheet. "
            "Tidak perlu login.</p></div>",
            unsafe_allow_html=True,
        )
        if st.button("Mulai upload →", type="primary", width="stretch", key="go_upload"):
            go("upload")
            st.rerun()
    with c2:
        st.markdown(
            '<div class="skp-card"><div class="icon">🗂️</div><h3>Admin dan Dokumentasi</h3>'
            "<p>Dashboard, compliance, galeri dokumen per toko, dan download ZIP. Khusus admin.</p></div>",
            unsafe_allow_html=True,
        )
        if st.button("Masuk admin →", width="stretch", key="go_admin"):
            go("admin")
            st.rerun()


def success_page() -> None:
    result = st.session_state.get("last_result")
    if not result:
        go("home")
        st.rerun()
    st.title("✅ Dokumen berhasil disimpan")
    store = result["store"]
    st.success(f"{len(result['saved'])} dokumen untuk **{store.name}** ({store.code}) berhasil disimpan "
               f"dan metadata sudah dicatat. PIC: {result['pic']}.")
    st.dataframe(pd.DataFrame([{
        "Brand": r["Brand"], "Program": r["Jenis Program"], "Kuartal": r["Kuartal"], "Nomor SKP": r["Nomor SKP"],
        "Nama file": r["Nama File"], "Status": r["Status"], "Lokasi": r["GCS Path"],
    } for r in result["saved"]]), hide_index=True, width="stretch")
    if result.get("removed"):
        st.caption("File lama dengan nama berbeda untuk kombinasi yang sama sudah diganti: " + ", ".join(result["removed"]))
    c1, c2 = st.columns(2)
    if c1.button("📤 Upload dokumen lain", type="primary", width="stretch"):
        st.session_state.pop("last_result", None)
        go("upload")
        st.rerun()
    if c2.button("🏠 Kembali ke beranda", width="stretch"):
        st.session_state.pop("last_result", None)
        go("home")
        st.rerun()


def warm_up() -> None:
    """Once per browser session: loading popup while the Google clients and the
    store list are prepared, so the upload page opens instantly afterwards.
    Failures are not shown here; each page reports its own friendly error."""
    if st.session_state.get("booted"):
        return
    with ui.LoadingOverlay("Menyiapkan aplikasi…", fast_seconds=1.0, min_seconds=1.0):
        try:
            load_source(get_backends())
        except Exception:  # noqa: BLE001
            logging.getLogger("skp").info("Warm-up incomplete; the page will show the error")
    st.session_state["booted"] = True


def main() -> None:
    ui.inject_css()
    warm_up()
    if "page" not in st.session_state:
        requested = st.query_params.get("page", "home")
        st.session_state["page"] = requested if requested in ("home", "upload", "admin") else "home"
    page = st.session_state["page"]

    if page == "home":
        landing()
        return
    if page == "success":
        success_page()
        return
    try:
        backends = get_backends()
    except Exception:  # noqa: BLE001 - never show stack traces / secrets to users
        logging.getLogger("skp").exception("Backend initialisation failed")
        st.error("Aplikasi belum dapat terhubung ke Google. Periksa konfigurasi Secrets (lihat README).")
        if st.button("← Kembali ke beranda"):
            go("home")
            st.rerun()
        return
    if page == "upload":
        page_upload.render(backends)
    elif page == "admin":
        page_admin.render(backends)
    else:
        go("home")
        st.rerun()


main()
