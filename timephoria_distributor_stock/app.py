"""TIMEPHORIA Smart Coverage Tester Feedback — stok tester di gudang distributor.

BigQuery = source of truth, Google Sheets = reporting mirror, Google Cloud Storage = foto evidence.
"""
from __future__ import annotations

import logging
from datetime import date, datetime

import pandas as pd
import streamlit as st

from admin_view import render_admin
from bigquery_store import BigQueryError
from clients import build_backends
from config import ConfigError
from gcs_evidence import EvidenceStorageError
from image_processor import MAX_BYTES, ImageError, process_photo
from master_data import MasterError, load_master, load_master_with_fallback
from mobile import apply_mobile_layout, keep_row
from opening_stock import HoStockError, load_ho_stock, lookup_ho_qty
from service import SalesSubmitter, SubmissionFailed, SubmissionRejected, plan_actions
from timeutil import WIB, fmt_wib
from validation import LineInput, validate_submission

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("timephoria.app")

PAGE_TITLE = "TIMEPHORIA Smart Coverage Tester Feedback"
SKU_HEADING = "DAFTAR SKU"
SKU_NOTE = (
    "Isi jumlah stok Tester Smart Coverage yang tersisa di Distributor dan sertakan bukti fotonya (wajib)"
)
STOCK_DIFF_NOTE = (
    "Jika ada selisih antara Stok Distributor dan Stok versi Perusahaan, "
    "maka Perusahaan akan follow-up ke pihak terkait untuk validasi lebih lanjut"
)
HO_STOCK_LABEL = "Data Stock yang dimiliki tim Head Office"
QTY_PLACEHOLDER = "isi quantity disini"
_REQUIRED_SUFFIX = "wajib diisi."

st.set_page_config(page_title=PAGE_TITLE, layout="centered", initial_sidebar_state="collapsed")


def _plain(obj):
    if hasattr(obj, "items"):
        return {k: _plain(v) for k, v in obj.items()}
    return obj


@st.cache_resource(show_spinner="Menghubungkan ke Google...")
def get_backends():
    return build_backends(_plain(st.secrets))


@st.cache_data(ttl=600, show_spinner="Memuat master distributor & SKU...")
def get_master(project: str, dataset: str, sku_key: str, nonce: int):
    be = get_backends()
    codes = tuple(part for part in sku_key.split("|") if part)
    return load_master_with_fallback(lambda: load_master(be.bq_client, project, dataset, codes))


@st.cache_data(ttl=30, show_spinner=False)
def get_existing(distributor_code: str, iso_date: str, nonce: int):
    return get_backends().bq_store.fetch_existing(date.fromisoformat(iso_date), distributor_code)


@st.cache_data(ttl=60, show_spinner=False)
def get_range_rows(start_iso: str, end_iso: str, nonce: int):
    return get_backends().bq_store.fetch_by_date_range(date.fromisoformat(start_iso), date.fromisoformat(end_iso))


@st.cache_data(ttl=600, show_spinner=False)
def get_ho_stock(sheet_id: str, worksheet: str, nonce: int):
    return load_ho_stock(get_backends().gc, sheet_id, worksheet)


@st.cache_data(ttl=60, show_spinner=False)
def get_previous(distributor_code: str, iso_date: str, nonce: int):
    return get_backends().bq_store.fetch_previous(distributor_code, date.fromisoformat(iso_date))


@st.cache_data(show_spinner=False, max_entries=64)
def compress(data: bytes, name: str | None):
    try:
        photo = process_photo(data, name)
        return photo.data, None, photo.original_size, photo.size
    except ImageError as exc:
        return None, str(exc), len(data), 0


def _kb(n: int) -> str:
    return f"{n / 1024:.0f} KB" if n < 1024 * 1024 else f"{n / 1024 / 1024:.1f} MB"


def _init_state():
    ss = st.session_state
    ss.setdefault("gen", 0)
    ss.setdefault("next_uid", 1)
    ss.setdefault("rows", [])
    ss.setdefault("row_fixed", {})
    ss.setdefault("rows_ready", False)
    ss.setdefault("attempted", False)
    ss.setdefault("nonce", 0)
    ss.setdefault("master_nonce", 0)
    ss.setdefault("last_result", None)


def _add_row() -> int:
    uid = st.session_state.next_uid
    st.session_state.next_uid += 1
    st.session_state.rows.append(uid)
    return uid


def _k(kind: str, uid: int) -> str:
    return f"{kind}_{st.session_state.gen}_{uid}"


def _fill_all_sku_rows(skus):
    ss = st.session_state
    ss.rows = []
    ss.row_fixed = {}
    for sku in skus:
        uid = _add_row()
        ss.row_fixed[uid] = sku.code
    ss.rows_ready = True


def _reset_rows():
    st.session_state.gen += 1
    st.session_state.rows_ready = False
    st.session_state.attempted = False


def _mark_attempted():
    st.session_state.attempted = True


def _text_filled(value) -> bool:
    return value is not None and str(value).strip() != ""


def render_result(res):
    st.success(f"Tersimpan: {res.inserted} baris baru (INSERT), {res.updated} diperbarui (UPDATE).")
    if res.tanggal_stock and res.submitted_at:
        st.caption(
            f"Tanggal stock: **{res.tanggal_stock.isoformat()}**  ·  "
            f"Waktu submit: **{fmt_wib(res.submitted_at)}**"
        )
    c1, c2 = st.columns(2)
    with c1:
        keep_row()
        st.metric("Status BigQuery", res.bigquery_status)
    c2.metric("Status Google Sheets mirror", "OK" if res.sheets_status == "OK" else "GAGAL")
    if res.sheets_status != "OK":
        st.caption(res.sheets_status)
    for warning in res.warnings:
        st.warning(warning)
    df = pd.DataFrame([{
        "SKU": f"{ln.sku_code} - {ln.sku_name}" if ln.sku_name else ln.sku_code,
        "Stock": ln.stock_qty,
        "Aksi": ln.action,
        "Transaction ID": ln.transaction_id,
        "Evidence": ln.evidence_url,
    } for ln in res.lines])
    st.dataframe(
        df, hide_index=True, width="stretch",
        column_config={"Evidence": st.column_config.LinkColumn("Evidence", display_text="Buka foto")},
    )


def main():
    _init_state()
    ss = st.session_state
    apply_mobile_layout()
    st.title(PAGE_TITLE)

    try:
        be = get_backends()
        cfg = be.config
        sku_key = "|".join(cfg.sku_codes)
        mres = get_master(cfg.bigquery_project, cfg.bigquery_dataset, sku_key, ss.master_nonce)
    except (ConfigError, MasterError) as exc:
        st.error(str(exc))
        st.stop()
    except Exception as exc:
        log.exception("Inisialisasi gagal")
        st.error(f"Gagal terhubung ke layanan Google: {exc.__class__.__name__}. Periksa secrets dan akses service account.")
        st.stop()
    master = mres.data

    if mres.stale:
        st.warning(
            "Master distributor/SKU sedang bermasalah, jadi aplikasi memakai data terakhir yang valid, "
            f"dibaca {fmt_wib(mres.loaded_at)}, penyebab: {mres.error}"
        )

    if not ss.rows_ready:
        _fill_all_sku_rows(list(master.skus.values()))

    with st.sidebar:
        st.caption(f"Master dibaca: {fmt_wib(mres.loaded_at)}" + (" (data lama)" if mres.stale else ""))
        if st.button("Muat ulang master"):
            ss.master_nonce += 1
            get_master.clear()
            st.rerun()
        st.button("Reset daftar SKU (kosongkan isian)", on_click=_reset_rows)
        missing = [s.code for s in master.skus.values() if not s.name]
        if missing:
            st.warning("Ada SKU tanpa nama di master produk: " + ", ".join(missing))
        with st.expander("Info master"):
            for line in master.diagnostics:
                st.caption(line)

    tab_input, tab_admin = st.tabs(["📝 Input Stock", "🔐 Admin"])
    with tab_input:
        render_input(be, master)
    with tab_admin:
        render_admin(be, master, get_range_rows, get_previous)


def _load_ho_reference(be, nonce: int) -> dict:
    sheet_id = getattr(be.config, "output_sheet_id", "") or ""
    worksheet = getattr(be.config, "ho_stock_worksheet", "Opening Stock")
    if not sheet_id:
        return {"url": "", "stocks": {}}
    try:
        return get_ho_stock(sheet_id, worksheet, nonce)
    except HoStockError as exc:
        st.warning(str(exc))
    except Exception as exc:
        log.exception("Stok Head Office gagal dimuat")
        st.warning(f"Data stok Head Office tidak dapat dimuat ({exc.__class__.__name__}).")
    return {"url": "", "stocks": {}}


def _ho_caption(ho: dict, distributor, sku_code: str | None) -> str:
    if distributor is None or not sku_code:
        shown = "—"
    else:
        qty = lookup_ho_qty(ho.get("stocks") or {}, distributor.distributor_name, sku_code)
        shown = "tidak ada" if qty is None else str(qty)
    return f"**{HO_STOCK_LABEL}:** {shown}"


def render_input(be, master):
    ss = st.session_state
    if ss.last_result is not None:
        render_result(ss.last_result)
        st.divider()

    regions = sorted({d.region for d in master.distributors.values()})
    region = st.selectbox("Region", regions, index=None, placeholder="Pilih region dulu", key="region")
    spv_options = sorted({d.spv for d in master.distributors.values() if d.region == region}) if region else []
    spv = st.selectbox(
        "Nama Supervisor", spv_options, index=None, key=f"spv_{region}",
        disabled=region is None,
        placeholder="Pilih region dulu" if region is None else "Pilih supervisor",
    )
    in_scope = [
        d for d in master.distributors.values() if region and spv and d.region == region and d.spv == spv
    ]
    in_scope.sort(key=lambda d: d.label)
    by_label = {d.label: d for d in in_scope}
    label = st.selectbox(
        "Distributor", list(by_label), index=None, key=f"dist_{region}_{spv}",
        disabled=spv is None,
        placeholder="Pilih supervisor dulu" if spv is None else "Pilih distributor",
    )
    distributor = by_label.get(label) if label else None
    tanggal = st.date_input("Tanggal Stock", value=datetime.now(WIB).date(), format="YYYY-MM-DD")

    existing: list[dict] = []
    if distributor and tanggal:
        try:
            existing = get_existing(distributor.distributor_code, tanggal.isoformat(), ss.nonce)
        except BigQueryError as exc:
            st.error(str(exc))
        if existing:
            st.info(
                f"Sudah ada {len(existing)} SKU tercatat untuk distributor ini pada {tanggal.isoformat()}."
            )
            with st.expander("Data yang sudah ada", expanded=False):
                st.dataframe(pd.DataFrame([{
                    "SKU": f"{r['sku_code']} - {r['sku_name']}",
                    "Stock": r["stock_qty"],
                    "Supervisor": r["supervisor_name"],
                    "Evidence": r["evidence_file_url"],
                    "Tanggal Stock": r["tanggal_stock"].isoformat() if hasattr(r["tanggal_stock"], "isoformat") else r["tanggal_stock"],
                    "Waktu Submit Pertama": fmt_wib(r["created_at"]),
                    "Terakhir Diperbarui": fmt_wib(r["updated_at"]),
                } for r in existing]), hide_index=True, width="stretch",
                    column_config={"Evidence": st.column_config.LinkColumn("Evidence", display_text="Buka foto")})

    st.subheader(SKU_HEADING)
    st.markdown(SKU_NOTE)
    st.markdown(f"**{STOCK_DIFF_NOTE}**")
    ho = _load_ho_reference(be, ss.master_nonce)
    if ho.get("url"):
        st.markdown(
            f"{HO_STOCK_LABEL} untuk setiap SKU ada di [spreadsheet ini]({ho['url']})."
        )
    sku_by_code = master.skus
    slots: dict[int, st.delta_generator.DeltaGenerator] = {}
    inputs: list[LineInput] = []
    for i, uid in enumerate(list(ss.rows), start=1):
        with st.container(border=True):
            fixed_code = ss.row_fixed.get(uid)
            sku = sku_by_code.get(fixed_code) if fixed_code else None
            name = sku.name if sku else ""
            st.markdown(f"**{i}. {fixed_code}** {name}".rstrip())
            sku_code = fixed_code if sku else None
            st.markdown(_ho_caption(ho, distributor, sku_code))
            stock_raw = st.text_input("Stock", key=_k("st", uid), placeholder=QTY_PLACEHOLDER)
            with st.expander("📷 Foto SKU (opsional)"):
                cam = st.camera_input("Ambil foto", key=_k("cam", uid))
                upl = st.file_uploader("…atau upload dari galeri (JPG/JPEG)", key=_k("upl", uid))
            photo, photo_error = None, None
            chosen = cam or upl
            if chosen is not None:
                jpeg, photo_error, orig, final = compress(chosen.getvalue(), None if chosen is cam else chosen.name)
                photo = jpeg
                if jpeg:
                    note = "  · kamera & upload sama-sama terisi, foto kamera yang dipakai" if cam and upl else ""
                    st.caption(f"✅ Foto siap — {_kb(orig)} → {_kb(final)} (JPG){note}")
                else:
                    st.caption(f"❌ {photo_error}")
            else:
                st.caption("Belum ada foto")
            slots[i] = st.empty()
            inputs.append(LineInput(
                row_no=i, sku_code=sku_code, stock_raw=stock_raw, photo=photo, photo_error=photo_error,
            ))

    result = validate_submission(
        distributor_code=distributor.distributor_code if distributor else None,
        tanggal_stock=tanggal,
        supervisor_name=spv,
        lines=inputs,
        distributors=master.distributors,
        skus=master.skus,
    )

    def visible(issue) -> bool:
        return ss.attempted or not issue.message.endswith(_REQUIRED_SUFFIX)

    has_input = any(_text_filled(ln.stock_raw) or ln.photo or ln.photo_error for ln in inputs)

    row_errors: dict[int, list[str]] = {}
    for issue in result.errors:
        if issue.row_no and issue.row_no in slots and visible(issue):
            row_errors.setdefault(issue.row_no, []).append(issue.message)
    for row_no, msgs in row_errors.items():
        slots[row_no].error("\n\n".join(msgs))
    if not result.errors:
        row_warnings: dict[int, list[str]] = {}
        for issue in result.warnings:
            if issue.row_no and issue.row_no in slots:
                row_warnings.setdefault(issue.row_no, []).append(issue.message)
        for row_no, msgs in row_warnings.items():
            slots[row_no].warning("\n\n".join(msgs))

    if result.lines:
        actions = plan_actions(result.lines, existing)
        old = {r["sku_code"]: r for r in existing}
        st.dataframe(pd.DataFrame([{
            "SKU": f"{ln.sku_code} - {ln.sku_name}" if ln.sku_name else ln.sku_code,
            "Stock": ln.stock_qty,
            "Aksi": actions[ln.sku_code],
            "Stock lama": (old.get(ln.sku_code) or {}).get("stock_qty"),
            "Foto": "Baru" if ln.photo else ("Tetap" if (old.get(ln.sku_code) or {}).get("evidence_file_id") else "-"),
        } for ln in result.lines]), hide_index=True, width="stretch")
    no_photo = [w for w in result.warnings if w.message.startswith("PERINGATAN")]
    if result.ok and no_photo:
        st.warning(
            f"⚠️ {len(no_photo)} SKU dengan Stock > 0 belum punya foto sendiri. "
            "Submit tetap boleh selama minimal satu foto terlampir."
        )
    for warning in result.warnings:
        if not warning.message.startswith("PERINGATAN"):
            st.warning(str(warning))
    if has_input or ss.attempted:
        for issue in result.errors:
            if not issue.row_no:
                st.error(issue.message)

    if st.button("Submit", type="primary", width="stretch", on_click=_mark_attempted):
        if not result.ok:
            st.error("Perbaiki isian di atas terlebih dahulu. Tidak ada data yang disimpan.")
        elif distributor is None or tanggal is None or not spv:
            st.error("Region, supervisor, distributor, dan tanggal wajib diisi.")
        else:
            submitter = SalesSubmitter(be.bq_store, be.evidence, be.mirror)
            try:
                with st.spinner("Menyimpan..."):
                    out = submitter.submit(
                        distributor=distributor, tanggal_stock=tanggal,
                        supervisor_name=spv, lines=result.lines,
                    )
            except SubmissionRejected as exc:
                for issue in exc.issues:
                    st.error(str(issue))
            except (SubmissionFailed, EvidenceStorageError, BigQueryError) as exc:
                log.exception("Submit gagal")
                st.error(str(exc))
            else:
                ss.last_result = out
                ss.gen += 1
                ss.rows_ready = False
                ss.attempted = False
                ss.nonce += 1
                get_existing.clear()
                st.rerun()


main()
