"""Tab Admin: login, kelengkapan, perbandingan stok, dan galeri foto.

Tab ini hanya MEMBACA. Tidak ada tombol hapus atau ubah.
"""
from __future__ import annotations

import logging
import math
import time
from datetime import datetime, timedelta
from typing import Callable

import pandas as pd
import streamlit as st

from admin_auth import credentials_match, lock_remaining, record_failure, reset
from bigquery_store import BigQueryError
from completeness import (
    STATUS_COMPLETE, STATUS_NONE, STATUS_PARTIAL, STATUSES, DistributorStatus,
    change_marker, compare_sku, compute_completeness, evidence_items, region_of, summarize_by_region,
)
from gcs_evidence import EvidenceStorageError
from mobile import keep_row
from timeutil import WIB, fmt_wib

log = logging.getLogger("timephoria.admin")

MAX_RANGE_DAYS = 92
PAGE_SIZE = 8
ALL_REGIONS = "Semua region"
ALL_DISTRIBUTORS = "(Semua distributor sesuai filter)"
_STATUS_ICON = {STATUS_COMPLETE: "🟢", STATUS_PARTIAL: "🟡", STATUS_NONE: "🔴"}


def _logout():
    st.session_state["admin_ok"] = False


def _login(cfg) -> None:
    ss = st.session_state
    state = ss.setdefault("admin_throttle", {})
    st.caption("Khusus admin. Masuk untuk melihat kelengkapan input dan galeri foto.")
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
    time.sleep(1)
    st.error("Username atau password salah.")


def _photo_card(evidence, item) -> None:
    url = evidence.view_url(item.file_id)
    try:
        if url:
            st.image(url, width="stretch")
        else:
            st.image(evidence.read_bytes(item.file_id), width="stretch")
    except EvidenceStorageError as exc:
        st.caption(f"Foto tidak dapat dimuat: {exc}")
    except Exception:
        log.exception("Gagal menampilkan foto %s", item.file_id)
        st.caption("Foto tidak dapat dimuat.")
    when = item.tanggal_stock.isoformat() if item.tanggal_stock else "-"
    st.markdown(
        f"**{item.distributor_code}** · {item.distributor_name}  \n"
        f"Tanggal {when}  \n"
        f"{item.spu_code} · {item.spu_name}  \n"
        f"Stock **{item.stock_qty}**  \n"
        f"{item.supervisor or '-'} · {fmt_wib(item.updated_at) if item.updated_at else '-'}"
    )
    if url:
        st.markdown(f"[Buka ukuran penuh]({url})")


def _distributor_table(statuses: list[DistributorStatus]) -> pd.DataFrame:
    return pd.DataFrame([{
        "Status": f"{_STATUS_ICON[s.status]} {s.status}",
        "Region": s.region,
        "Kode": s.distributor.distributor_code,
        "Distributor": s.distributor.distributor_name,
        "Kunjungan": s.visits,
        "Kunjungan terakhir": s.last_visit.isoformat() if s.last_visit else "",
        "SPU terisi (terbaik)": f"{s.sku_filled}/{s.sku_total}",
        "Foto": s.photo_count,
        "Supervisor": s.supervisor or "",
        "Waktu submit terakhir (WIB)": fmt_wib(s.last_submit) if s.last_submit else "",
    } for s in statuses])


def _step(delta: int) -> None:
    ss = st.session_state
    ss["adm_page"] = min(max(1, ss.get("adm_page", 1) + delta), ss.get("adm_pages", 1))


def _pager(pages: int, page: int, where: str) -> None:
    cols = st.columns(3, vertical_alignment="center")
    with cols[0]:
        keep_row()
        st.button("◀ Prev", key=f"adm_prev_{where}", on_click=_step, args=(-1,), disabled=page <= 1, width="stretch")
    with cols[1]:
        if where == "top":
            st.selectbox(
                "Halaman", list(range(1, pages + 1)), key="adm_page", label_visibility="collapsed",
                format_func=lambda n: f"Hal. {n} / {pages}",
            )
        else:
            st.markdown(
                f"<div style='text-align:center;font-weight:700;margin:0;line-height:1'>Hal. {page} / {pages}</div>",
                unsafe_allow_html=True,
            )
    cols[2].button("Next ▶", key=f"adm_next_{where}", on_click=_step, args=(1,), disabled=page >= pages, width="stretch")


def render_admin(be, master, fetch_rows: Callable[[str, str, int], list[dict]],
                 fetch_previous: Callable[[str, str, int], list[dict]]) -> None:
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
        st.subheader("Kelengkapan input & foto")
    head[1].button("Logout", key="adm_logout", on_click=_logout)

    today = datetime.now(WIB).date()
    f1, f2 = st.columns(2)
    picked = f1.date_input(
        "Rentang Tanggal Stock", value=(today - timedelta(days=6), today),
        format="YYYY-MM-DD", key="adm_range",
    )
    regions = sorted({region_of(d) for d in master.distributors.values()})
    region = f2.selectbox("Region", [ALL_REGIONS, *regions], key="adm_region")
    wanted = st.multiselect("Status", list(STATUSES), default=list(STATUSES), key="adm_status")
    threshold = st.number_input("Ambang perubahan (%)", min_value=0.0, value=50.0, step=1.0, key="adm_threshold")
    if st.button("Muat ulang data", key="adm_refresh"):
        ss["adm_nonce"] = ss.get("adm_nonce", 0) + 1
    if not (isinstance(picked, (tuple, list)) and len(picked) == 2):
        st.info("Pilih tanggal mulai dan tanggal akhir.")
        return
    start, end = picked
    if (end - start).days > MAX_RANGE_DAYS:
        st.warning(f"Rentang maksimal {MAX_RANGE_DAYS} hari. Persempit rentang tanggalnya.")
        return

    try:
        rows = fetch_rows(start.isoformat(), end.isoformat(), ss.get("adm_nonce", 0))
    except BigQueryError as exc:
        st.error(str(exc))
        return

    statuses = compute_completeness(master.distributors, master.skus, rows)
    scoped = [s for s in statuses if region in (ALL_REGIONS, s.region)]
    shown = [s for s in scoped if s.status in wanted]

    row1, row2 = st.columns(2), st.columns(2)
    with row1[0]:
        keep_row()
        st.metric("Total", len(scoped))
    row1[1].metric(f"{_STATUS_ICON[STATUS_COMPLETE]} {STATUS_COMPLETE}", sum(s.status == STATUS_COMPLETE for s in scoped))
    with row2[0]:
        keep_row()
        st.metric(f"{_STATUS_ICON[STATUS_PARTIAL]} {STATUS_PARTIAL}", sum(s.status == STATUS_PARTIAL for s in scoped))
    row2[1].metric(f"{_STATUS_ICON[STATUS_NONE]} {STATUS_NONE}", sum(s.status == STATUS_NONE for s in scoped))
    st.caption(
        f"Rentang {start.isoformat()} s/d {end.isoformat()}. **Lengkap** = pada minimal satu tanggal kunjungan "
        f"dalam rentang, semua {len(master.skus)} SPU terisi, dan bila total Stock tanggal itu lebih dari 0 ada "
        f"minimal 1 foto. Total Stock 0 tidak membutuhkan foto."
    )

    st.markdown("##### Per region")
    st.dataframe(
        pd.DataFrame(summarize_by_region(statuses)), hide_index=True, width="stretch",
        column_config={"% Lengkap": st.column_config.ProgressColumn("% Lengkap", min_value=0, max_value=100, format="%.1f%%")},
    )

    st.markdown("##### Per distributor")
    table = _distributor_table(shown)
    st.dataframe(table, hide_index=True, width="stretch")
    st.download_button(
        "Unduh CSV (per distributor)", table.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"kelengkapan_{start.isoformat()}_{end.isoformat()}.csv", mime="text/csv", key="adm_csv",
    )

    st.markdown("##### Detail distributor")
    span = f"{start.isoformat()}_{end.isoformat()}"
    by_label = {f"{s.distributor.distributor_code} - {s.distributor.distributor_name}": s for s in shown}
    pick = st.selectbox("Detail distributor", [ALL_DISTRIBUTORS, *by_label], key=f"adm_dist_{region}_{span}")
    chosen = by_label.get(pick)
    if chosen is not None:
        code = chosen.distributor.distributor_code
        st.caption(
            f"{_STATUS_ICON[chosen.status]} {chosen.status} · {chosen.visits} kunjungan dalam rentang · "
            f"{chosen.photo_count} foto · supervisor terakhir {chosen.supervisor or '-'}"
        )
        dates = sorted({r["tanggal_stock"] for r in rows if r["distributor_code"] == code}, reverse=True)
        if dates:
            default = dates.index(chosen.best_date) if chosen.best_date in dates else 0
            visit = st.selectbox(
                "Tanggal kunjungan", dates, index=default, key=f"adm_visit_{code}_{span}",
                format_func=lambda d: d.isoformat(),
            )
            by_sku = {r["spu_code"]: r for r in rows if r["distributor_code"] == code and r["tanggal_stock"] == visit}
            st.dataframe(pd.DataFrame([{
                "SPU": f"{sku.code} - {sku.name}" if sku.name else sku.code,
                "Stock": (by_sku.get(sku.code) or {}).get("stock_qty"),
                "Isian": "Terisi" if sku.code in by_sku else "Belum",
                "Foto": "Ada" if (by_sku.get(sku.code) or {}).get("evidence_file_id") else "-",
            } for sku in master.skus.values()]), hide_index=True, width="stretch")

            try:
                previous_rows = fetch_previous(code, visit.isoformat(), ss.get("adm_nonce", 0))
            except BigQueryError as exc:
                st.error(str(exc))
                previous_rows = []
            prev_by_sku = {r["spu_code"]: r for r in previous_rows}
            prev_date = previous_rows[0]["tanggal_stock"] if previous_rows else None
            st.markdown("##### Perbandingan dengan submit sebelumnya")
            if prev_date:
                st.caption(f"Tanggal sebelumnya: {prev_date.isoformat()} (boleh di luar rentang filter).")
            else:
                st.caption("Belum ada submit sebelumnya untuk distributor ini.")
            compared = []
            for sku in master.skus.values():
                current = (by_sku.get(sku.code) or {}).get("stock_qty")
                prev = (prev_by_sku.get(sku.code) or {}).get("stock_qty")
                change = compare_sku(current, prev)
                compared.append({
                    "SPU": sku.code,
                    "Stock sekarang": change["sekarang"],
                    "Stock sebelumnya": change["sebelumnya"],
                    "Selisih": change["selisih"],
                    "Perubahan": change["perubahan"],
                    "Tanda": change_marker(change["perubahan"], float(threshold)),
                })
            st.dataframe(pd.DataFrame(compared), hide_index=True, width="stretch")
        else:
            st.info("Distributor ini belum submit pada rentang tanggal ini.")

    st.markdown("##### Galeri foto")
    allowed = {s.distributor.distributor_code for s in shown} if chosen is None else {chosen.distributor.distributor_code}
    items = [i for i in evidence_items(master.distributors, master.skus, rows) if i.distributor_code in allowed]
    if not items:
        st.info("Belum ada foto untuk filter ini.")
        return
    pages = max(1, math.ceil(len(items) / PAGE_SIZE))
    sig = (region, pick, span, tuple(sorted(wanted)))
    if ss.get("adm_sig") != sig:
        ss["adm_sig"], ss["adm_page"] = sig, 1
    ss["adm_pages"] = pages
    ss["adm_page"] = min(max(1, ss.get("adm_page", 1)), pages)
    page = ss["adm_page"]
    first, last = (page - 1) * PAGE_SIZE, min(page * PAGE_SIZE, len(items))
    st.caption(f"{len(items)} foto · menampilkan {first + 1}–{last}")
    if pages > 1:
        _pager(pages, page, "top")
    cols = st.columns(2)
    with cols[0]:
        keep_row()
    for idx, item in enumerate(items[first:last]):
        with cols[idx % 2]:
            _photo_card(be.evidence, item)
    if pages > 1:
        st.divider()
        _pager(pages, page, "bottom")
