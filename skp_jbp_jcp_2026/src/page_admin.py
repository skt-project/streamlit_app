"""Admin & Dokumentasi: login, dashboard, compliance, gallery per toko, bulk ZIP."""
from __future__ import annotations

import hmac
import time

import pandas as pd
import streamlit as st

from src import ui
from src.runtime import Backends, DataUnavailable, clear_all_caches, load_logs, load_source
from src.service import (
    STATUS_NOT_UPLOADED,
    apply_filters,
    compliance_frame,
    count_by,
    dashboard_stats,
)
from src.sheets import COL_BRAND, COL_CODE, COL_NAME, COL_PROGRAM, COL_REGION, expected_documents, now_wib
from src.storage import StorageError, build_zip, object_name
from src.validation import BRANDS, PROGRAMS, QUARTERS

MAX_ATTEMPTS = 5
LOCKOUT_SECONDS = 60
GALLERY_PAGE_SIZE = 10


def _go(page: str) -> None:
    st.session_state["page"] = page
    st.query_params["page"] = page


def render(backends: Backends) -> None:
    if not st.session_state.get("admin_ok"):
        _login(backends)
        return

    top_l, top_r = st.columns([3, 1], vertical_alignment="center")
    top_l.title("🗂️ Admin & Dokumentasi")
    if top_r.button("Keluar", width="stretch"):
        st.session_state.pop("admin_ok", None)
        _go("home")
        st.rerun()
    ui.mode_banner(backends.mode, backends.source.label if backends.source else "-",
                   backends.storage.label if backends.storage else "-", backends.logs.label if backends.logs else "-")

    try:
        logs = load_logs(backends)
    except DataUnavailable as exc:
        st.error(f"Log upload tidak dapat dibaca. {exc}")
        logs = pd.DataFrame()
    try:
        source_df, _ = load_source(backends)
        expected = expected_documents(source_df)
    except DataUnavailable as exc:
        st.warning(f"Spreadsheet sumber tidak dapat dibaca, kolom 'Belum diupload' tidak tersedia. {exc}")
        expected = pd.DataFrame()

    full = compliance_frame(logs, expected)
    view = _filters(full)
    uploaded_view = view[view["Status"] != STATUS_NOT_UPLOADED]

    tabs = st.tabs(["📊 Dashboard", "✅ Compliance", "🏪 Gallery per toko", "📦 Bulk download", "⚙️ Status sistem"])
    with tabs[0]:
        _dashboard(uploaded_view, view)
    with tabs[1]:
        _compliance(view)
    with tabs[2]:
        _gallery(uploaded_view, backends)
    with tabs[3]:
        _bulk_download(uploaded_view, backends)
    with tabs[4]:
        _system_status(backends)


# --------------------------------------------------------------------------

def _login(backends: Backends) -> None:
    if st.button("← Kembali ke beranda"):
        _go("home")
        st.rerun()
    st.title("🔐 Login Admin")
    settings = backends.settings
    if not settings.admin_configured:
        st.error("Login admin belum dikonfigurasi. Isi ADMIN_USERNAME dan ADMIN_PASSWORD di Streamlit Secrets.")
        return
    locked_until = st.session_state.get("admin_locked_until", 0.0)
    if time.time() < locked_until:
        st.error(f"Terlalu banyak percobaan gagal. Coba lagi dalam {int(locked_until - time.time()) + 1} detik.")
        return
    with st.form("admin_login", border=True):
        user = st.text_input("Username", autocomplete="username")
        password = st.text_input("Password", type="password", autocomplete="current-password")
        submitted = st.form_submit_button("Masuk", type="primary", width="stretch")
    if submitted:
        ok = hmac.compare_digest(user.strip().encode(), settings.admin_username.encode()) & \
            hmac.compare_digest(password.encode(), settings.admin_password.encode())
        if ok:
            st.session_state["admin_ok"] = True
            st.session_state["admin_attempts"] = 0
            st.rerun()
        attempts = st.session_state.get("admin_attempts", 0) + 1
        st.session_state["admin_attempts"] = attempts
        if attempts >= MAX_ATTEMPTS:
            st.session_state["admin_locked_until"] = time.time() + LOCKOUT_SECONDS
            st.session_state["admin_attempts"] = 0
        st.error("Username atau password salah.")


def _options(df: pd.DataFrame, column: str, preferred: tuple[str, ...] = ()) -> list[str]:
    present = {v for v in df[column].unique() if v} if not df.empty else set()
    ordered = [p for p in preferred if p in present]
    return ordered + sorted(present - set(ordered))


def _filters(df: pd.DataFrame) -> pd.DataFrame:
    with st.container(border=True):
        st.markdown("**Filter** — berlaku untuk semua tab")
        c1, c2, c3 = st.columns(3)
        regions = c1.multiselect("Region", _options(df, COL_REGION), placeholder="Semua region", key="f_region")
        quarters = c2.multiselect("Kuartal", _options(df, "Kuartal", QUARTERS), placeholder="Semua kuartal", key="f_quarter")
        programs = c3.multiselect("Program", _options(df, COL_PROGRAM, PROGRAMS), placeholder="Semua program", key="f_program")
        c4, c5, c6 = st.columns(3)
        brands = c4.multiselect("Brand", _options(df, COL_BRAND, BRANDS), placeholder="Semua brand", key="f_brand")
        store_q = c5.text_input("Nama / kode toko", placeholder="Ketik sebagian nama", key="f_store")
        status = c6.selectbox("Status", ["Semua", "Sudah upload", "Belum upload"], key="f_status")
    return apply_filters(df, regions, quarters, programs, brands, store_q, status)


def _dashboard(uploaded: pd.DataFrame, view: pd.DataFrame) -> None:
    stats = dashboard_stats(uploaded)
    cols = st.columns(5)
    cols[0].metric("Total upload", stats["uploads"], help="Jumlah sesi submit (terbaru per kombinasi).", border=True)
    cols[1].metric("Total toko", stats["stores"], border=True)
    cols[2].metric("Total dokumen", stats["documents"], border=True)
    cols[3].metric("Total region", stats["regions"], border=True)
    cols[4].metric("Total brand", stats["brands"], border=True)

    expected_total = len(view)
    if expected_total:
        done = int((view["Status"] != STATUS_NOT_UPLOADED).sum())
        pct = done / expected_total
        st.markdown(f"**Kelengkapan dokumen (sesuai filter): {done} dari {expected_total} ({pct:.0%})**")
        st.progress(min(pct, 1.0))

    if uploaded.empty:
        st.info("Belum ada dokumen yang diupload untuk filter ini.")
        return
    grid = [("Kuartal", "Dokumen per kuartal"), (COL_PROGRAM, "Dokumen per program"),
            (COL_BRAND, "Dokumen per brand"), (COL_REGION, "Dokumen per region")]
    for row in (grid[:2], grid[2:]):
        cols = st.columns(2)
        for col, (column, title) in zip(cols, row):
            data = count_by(uploaded, column)
            col.markdown(f"**{title}**")
            col.dataframe(
                data,
                hide_index=True,
                width="stretch",
                column_config={"Dokumen": st.column_config.ProgressColumn(
                    "Dokumen", format="%d", min_value=0, max_value=int(data["Dokumen"].max() or 1))},
            )


def _compliance(view: pd.DataFrame) -> None:
    table = view.rename(columns={COL_NAME: "Toko", COL_PROGRAM: "Program", "Nama File": "Nama file"})
    columns = ["Toko", COL_CODE, COL_REGION, COL_BRAND, "Program", "Kuartal", "Nomor SKP", "Nama file",
               "PIC Submit", "Timestamp", "Status"]
    st.caption(f"{len(table)} baris. ‘Belum diupload’ = nomor SKP ada di spreadsheet tetapi dokumennya belum diupload.")
    st.dataframe(table[columns], hide_index=True, width="stretch", height=520)
    st.download_button("⬇️ Download tabel (CSV)", table[columns].to_csv(index=False).encode("utf-8-sig"),
                       file_name=f"SKP_compliance_{now_wib()[:10]}.csv", mime="text/csv", on_click="ignore")


def _gallery(uploaded: pd.DataFrame, backends: Backends) -> None:
    if uploaded.empty:
        st.info("Belum ada dokumen untuk filter ini.")
        return
    stores = (uploaded.groupby(COL_CODE, sort=False)
              .agg(name=(COL_NAME, "first"), region=(COL_REGION, "first"), docs=("GCS Path", "count"))
              .reset_index().sort_values("name"))
    pages = max(1, -(-len(stores) // GALLERY_PAGE_SIZE))
    page = 1
    if pages > 1:
        page = st.number_input(f"Halaman (1–{pages})", min_value=1, max_value=pages, value=1, step=1, key="gallery_page")
    st.caption(f"{len(stores)} toko · {len(uploaded)} dokumen")
    can_link = backends.storage is not None and backends.storage.supports_links
    if not can_link:
        st.caption("Link buka dokumen hanya tersedia jika file tersimpan di GCS.")
    for _, s in stores.iloc[(page - 1) * GALLERY_PAGE_SIZE: page * GALLERY_PAGE_SIZE].iterrows():
        docs = uploaded[uploaded[COL_CODE] == s[COL_CODE]].copy()
        brands = ", ".join(sorted(docs[COL_BRAND].unique()))
        with st.expander(f"🏪 {s['name']} · {s[COL_CODE]} · {s['region'] or 'Tanpa region'} — {s['docs']} dokumen"):
            ui.facts([("Nama toko", s["name"]), ("Region", s["region"]), ("Brand", brands)])
            table = docs[[COL_BRAND, COL_PROGRAM, "Kuartal", "Nomor SKP", "Nama File", "GCS Path"]].rename(
                columns={COL_PROGRAM: "Program", "Nama File": "Nama dokumen"})
            config = {}
            if can_link:
                table["Buka"] = [backends.storage.signed_url(object_name(p)) for p in docs["GCS Path"]]
                config["Buka"] = st.column_config.LinkColumn("Buka", display_text="Buka PDF ↗",
                                                             help="Link berlaku 15 menit")
            st.dataframe(table, hide_index=True, width="stretch", column_config=config)


def _bulk_download(uploaded: pd.DataFrame, backends: Backends) -> None:
    paths = sorted(p for p in uploaded["GCS Path"].unique() if p) if not uploaded.empty else []
    st.markdown("ZIP berisi dokumen sesuai filter dengan struktur folder `2026 Qx/jbp|jcp/{region}/{file}.pdf`. "
                "ZIP dibuat di memori dan tidak disimpan ke GCS.")
    if not paths:
        st.info("Tidak ada dokumen untuk filter ini.")
        return
    if backends.storage is None:
        st.error("Penyimpanan dokumen belum dikonfigurasi (GCS_BUCKET).")
        return
    signature = hash(tuple(paths))
    cached = st.session_state.get("zip_cache")
    if cached and cached["sig"] != signature:
        st.session_state.pop("zip_cache", None)
        cached = None
    st.write(f"**{len(paths)} dokumen** siap dikumpulkan.")
    if st.button("📦 Siapkan ZIP", type="primary", width="stretch"):
        bar = st.progress(0.0, text="Mengunduh dokumen…")
        try:
            result = build_zip(paths, backends.storage, progress=lambda i, n: bar.progress(i / n, text=f"{i}/{n} dokumen"))
        except StorageError as exc:
            bar.empty()
            st.error(str(exc))
            return
        bar.empty()
        cached = {"sig": signature, "data": result.data, "added": len(result.added), "missing": result.missing,
                  "name": f"SKP_JBP_JCP_2026_{now_wib().replace(':', '').replace(' ', '_')}.zip"}
        st.session_state["zip_cache"] = cached
    if cached:
        st.success(f"ZIP siap: {cached['added']} dokumen ({len(cached['data']) / (1024 * 1024):.1f} MB).")
        if cached["missing"]:
            st.warning(f"{len(cached['missing'])} file tercatat di log tetapi tidak ada di penyimpanan:")
            st.code("\n".join(cached["missing"][:50]))
        st.download_button("⬇️ Download ZIP", cached["data"], file_name=cached["name"], mime="application/zip",
                           type="primary", width="stretch", on_click="ignore")


def _system_status(backends: Backends) -> None:
    s = backends.settings
    ui.facts([
        ("Mode", backends.mode.name), ("Service account", s.service_account_email or "—"),
        ("Bucket GCS", s.gcs_bucket or "—"), ("Sheet sumber", s.source_sheet_name),
        ("Sheet log", s.log_sheet_name), ("Spreadsheet ID", s.source_spreadsheet_id or "—"),
    ])
    c1, c2 = st.columns(2)
    if c1.button("🔄 Muat ulang data dari spreadsheet", width="stretch"):
        clear_all_caches()
        st.rerun()
    if c2.button("🩺 Cek koneksi", width="stretch"):
        try:
            _, index = load_source(backends)
            st.success(f"Sheet sumber terbaca: {len(index)} toko.")
        except DataUnavailable as exc:
            st.error(f"Sheet sumber: {exc}")
        if backends.storage is not None:
            ok, msg = backends.storage.check()
            (st.success if ok else st.error)(msg)
        else:
            st.error("Penyimpanan belum dikonfigurasi.")
