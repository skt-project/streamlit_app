"""Tab Admin: login, kelengkapan input per region / per store, dan galeri evidence.

Tab ini hanya MEMBACA (BigQuery dan foto di GCS). Tidak ada tombol hapus/ubah apa pun.
"""
from __future__ import annotations

import logging
import math
import time
from datetime import date, datetime
from typing import Callable

import pandas as pd
import streamlit as st

from admin_auth import credentials_match, lock_remaining, record_failure, reset
from bigquery_store import BigQueryError
from completeness import (
    STATUS_COMPLETE, STATUS_NONE, STATUS_PARTIAL, STATUSES, StoreStatus, compute_completeness,
    evidence_items, region_of, summarize_by_region,
)
from gcs_evidence import EvidenceStorageError
from mobile import keep_row
from timeutil import WIB, fmt_wib

log = logging.getLogger("store_sales.admin")

PAGE_SIZE = 8                              # 2 kolom x 4 baris: pas untuk layar HP
ALL_REGIONS = "Semua region"
ALL_STORES = "(Semua store sesuai filter)"
_STATUS_ICON = {STATUS_COMPLETE: "🟢", STATUS_PARTIAL: "🟡", STATUS_NONE: "🔴"}


def _logout():
    st.session_state["admin_ok"] = False


def _login(cfg) -> None:
    ss = st.session_state
    state = ss.setdefault("admin_throttle", {})
    st.caption("Khusus admin. Masuk untuk melihat kelengkapan input dan galeri evidence.")
    with st.form("admin_login"):
        username = st.text_input("Username", key="adm_user")
        password = st.text_input("Password", type="password", key="adm_pass")
        submitted = st.form_submit_button("Login")
    if not submitted:
        return
    wait = lock_remaining(state, time.monotonic())
    if wait > 0:
        st.error(f"Terlalu banyak percobaan gagal. Coba lagi dalam {math.ceil(wait)} detik.")
        return
    if credentials_match(username, password, cfg.admin_username, cfg.admin_password):
        reset(state)
        ss["admin_ok"] = True
        st.rerun()
    record_failure(state, time.monotonic())
    log.warning("Login admin gagal")
    time.sleep(1)                           # memperlambat tebak-tebakan
    st.error("Username atau password salah.")


def _photo_card(evidence, it) -> None:
    url = evidence.view_url(it.file_id)
    try:
        if url:
            st.image(url, width="stretch")
        else:
            st.image(evidence.read_bytes(it.file_id), width="stretch")
    except EvidenceStorageError as exc:
        st.caption(f"Foto tidak dapat dimuat: {exc}")
    except Exception:
        log.exception("Gagal menampilkan foto %s", it.file_id)
        st.caption("Foto tidak dapat dimuat.")
    st.markdown(f"**{it.store_id}** · {it.store_name}  \n"
                f"{it.sku_code} · {it.sku_name}  \n"
                f"Sell Out **{it.sell_out_qty}** · Stock **{it.stock_qty}**  \n"
                f"{it.supervisor or '-'} · {fmt_wib(it.updated_at) if it.updated_at else '-'}")
    if url:
        st.markdown(f"[Buka ukuran penuh]({url})")


def _store_table(statuses: list[StoreStatus]) -> pd.DataFrame:
    return pd.DataFrame([{
        "Status": f"{_STATUS_ICON[s.status]} {s.status}",
        "Region": s.region, "Store ID": s.store.store_id, "Store": s.store.store_name,
        "SKU terisi": f"{s.sku_filled}/{s.sku_total}", "Foto": s.photo_count,
        "Supervisor": s.supervisor or "",
        "Waktu submit terakhir (WIB)": fmt_wib(s.last_submit) if s.last_submit else "",
    } for s in statuses])


def _step(delta: int) -> None:
    """Callback tombol Prev/Next: geser halaman galeri, tetap dalam rentang 1..jumlah halaman."""
    ss = st.session_state
    ss["adm_page"] = min(max(1, ss.get("adm_page", 1) + delta), ss.get("adm_pages", 1))


def _pager(pages: int, page: int, where: str) -> None:
    """Kontrol halaman: [◀ Prev] [halaman] [Next ▶]. Dipasang di atas dan di bawah galeri."""
    cols = st.columns(3, vertical_alignment="center")
    with cols[0]:
        keep_row()
        st.button("◀ Prev", key=f"adm_prev_{where}", on_click=_step, args=(-1,), disabled=page <= 1, width="stretch")
    with cols[1]:
        if where == "top":                          # di atas: bisa loncat langsung ke halaman tertentu
            st.selectbox("Halaman", list(range(1, pages + 1)), key="adm_page", label_visibility="collapsed",
                         format_func=lambda n: f"Hal. {n} / {pages}")
        else:
            st.markdown(f"<div style='text-align:center;font-weight:700;margin:0;line-height:1'>Hal. {page} / {pages}</div>",
                        unsafe_allow_html=True)
    cols[2].button("Next ▶", key=f"adm_next_{where}", on_click=_step, args=(1,), disabled=page >= pages,
                   width="stretch")


def render_admin(be, master, fetch_rows: Callable[[str, int], list[dict]]) -> None:
    cfg, ss = be.config, st.session_state
    if not (cfg.admin_username and cfg.admin_password):
        st.info("Tab admin belum aktif: isi bagian `[admin]` (username dan password) di secrets.")
        return
    if not ss.get("admin_ok"):
        _login(cfg)
        return

    head = st.columns([5, 1], vertical_alignment="center")
    with head[0]:
        keep_row("button")
        st.subheader("Kelengkapan input & evidence")
    head[1].button("Logout", key="adm_logout", on_click=_logout)

    f1, f2 = st.columns(2)
    sales_date = f1.date_input("Tanggal Penjualan", value=datetime.now(WIB).date(), format="YYYY-MM-DD",
                               key="adm_date")
    regions = sorted({region_of(s) for s in master.stores.values()})
    region = f2.selectbox("Region", [ALL_REGIONS, *regions], key="adm_region")
    wanted = st.multiselect("Status", list(STATUSES), default=list(STATUSES), key="adm_status")
    if st.button("Muat ulang data", key="adm_refresh"):
        ss["adm_nonce"] = ss.get("adm_nonce", 0) + 1
    if not isinstance(sales_date, date):
        st.info("Pilih tanggal penjualan.")
        return

    try:
        rows = fetch_rows(sales_date.isoformat(), ss.get("adm_nonce", 0))
    except BigQueryError as exc:
        st.error(str(exc))
        return

    statuses = compute_completeness(master.stores, master.skus, rows)
    scoped = [s for s in statuses if region in (ALL_REGIONS, s.region)]
    shown = [s for s in scoped if s.status in wanted]

    row1, row2 = st.columns(2), st.columns(2)          # metrik 2x2, tetap berdampingan di HP
    with row1[0]:
        keep_row()
        st.metric("Total store", len(scoped))
    row1[1].metric(f"{_STATUS_ICON[STATUS_COMPLETE]} {STATUS_COMPLETE}", sum(s.status == STATUS_COMPLETE for s in scoped))
    with row2[0]:
        keep_row()
        st.metric(f"{_STATUS_ICON[STATUS_PARTIAL]} {STATUS_PARTIAL}", sum(s.status == STATUS_PARTIAL for s in scoped))
    row2[1].metric(f"{_STATUS_ICON[STATUS_NONE]} {STATUS_NONE}", sum(s.status == STATUS_NONE for s in scoped))
    st.caption(f"Lengkap = semua {len(master.skus)} SKU terisi (Sell Out + Stock) **dan** minimal 1 foto evidence. "
               f"Tanggal penjualan {sales_date.isoformat()}.")

    st.markdown("##### Per region")
    st.dataframe(pd.DataFrame(summarize_by_region(statuses)), hide_index=True, width="stretch",
                 column_config={"% Lengkap": st.column_config.ProgressColumn(
                     "% Lengkap", min_value=0, max_value=100, format="%.1f%%")})

    st.markdown("##### Per store")
    table = _store_table(shown)
    st.dataframe(table, hide_index=True, width="stretch")
    st.download_button("Unduh CSV (per store)", table.to_csv(index=False).encode("utf-8-sig"),
                       file_name=f"kelengkapan_{sales_date.isoformat()}.csv", mime="text/csv", key="adm_csv")

    # Detail + galeri
    st.markdown("##### Galeri evidence")
    by_label = {f"{s.store.store_id} - {s.store.store_name}": s for s in shown}
    pick = st.selectbox("Store", [ALL_STORES, *by_label], key=f"adm_store_{region}_{sales_date.isoformat()}")
    chosen = by_label.get(pick)
    if chosen is not None:
        sid = chosen.store.store_id
        by_sku = {r["sku_code"]: r for r in rows if r["store_id"] == sid}
        st.caption(f"{_STATUS_ICON[chosen.status]} {chosen.status} · {chosen.sku_filled}/{chosen.sku_total} SKU · "
                   f"{chosen.photo_count} foto · supervisor {chosen.supervisor or '-'}")
        st.dataframe(pd.DataFrame([{
            "SKU": f"{sku.code} - {sku.name}",
            "Sell Out": (by_sku.get(sku.code) or {}).get("sell_out_qty"),
            "Stock": (by_sku.get(sku.code) or {}).get("stock_qty"),
            "Isian": "Terisi" if sku.code in by_sku else "Belum terisi",
            "Foto": "Ada" if (by_sku.get(sku.code) or {}).get("evidence_file_id") else "-",
        } for sku in master.skus.values()]), hide_index=True, width="stretch")

    allowed = {s.store.store_id for s in shown} if chosen is None else {chosen.store.store_id}
    items = [i for i in evidence_items(master.stores, master.skus, rows) if i.store_id in allowed]
    if not items:
        st.info("Belum ada foto evidence untuk filter ini.")
        return
    pages = max(1, math.ceil(len(items) / PAGE_SIZE))
    sig = (region, pick, sales_date.isoformat(), tuple(sorted(wanted)))
    if ss.get("adm_sig") != sig:                    # filter berubah → kembali ke halaman 1
        ss["adm_sig"], ss["adm_page"] = sig, 1
    ss["adm_pages"] = pages
    ss["adm_page"] = min(max(1, ss.get("adm_page", 1)), pages)
    page = ss["adm_page"]
    first, last = (page - 1) * PAGE_SIZE, min(page * PAGE_SIZE, len(items))
    st.caption(f"{len(items)} foto" + (f" · menampilkan {first + 1}–{last}" if pages > 1 else ""))
    if pages > 1:
        _pager(pages, page, "top")
    cols = st.columns(2)
    with cols[0]:
        keep_row()
    for idx, it in enumerate(items[first:last]):
        with cols[idx % 2]:
            _photo_card(be.evidence, it)
    if pages > 1:
        st.divider()
        _pager(pages, page, "bottom")
