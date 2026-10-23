"""Store Sales Input — pencatatan penjualan store ke end consumer oleh supervisor.

BigQuery = source of truth, Google Sheets = reporting mirror, Google Cloud Storage = evidence foto.
"""
from __future__ import annotations

import logging
from datetime import date, datetime

import pandas as pd
import streamlit as st

from bigquery_store import BigQueryError
from admin_view import render_admin
from clients import build_backends
from config import ConfigError
from gcs_evidence import EvidenceStorageError
from image_processor import MAX_BYTES, ImageError, process_photo
from master_cache import load_master_with_fallback
from mobile import apply_mobile_layout, keep_row
from service import SalesSubmitter, SubmissionFailed, SubmissionRejected, plan_actions
from sheets_master import MasterError, load_master
from timeutil import WIB, fmt_wib
from validation import LineInput, validate_submission

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("store_sales.app")

st.set_page_config(page_title="Store Sales Input", page_icon="🛒", layout="centered",
                   initial_sidebar_state="collapsed")

NO_REGION = "(Tanpa Region)"
_REQUIRED_SUFFIX = "wajib diisi."


def _plain(obj):
    """Salin st.secrets (AttrDict) menjadi dict biasa."""
    if hasattr(obj, "items"):
        return {k: _plain(v) for k, v in obj.items()}
    return obj


@st.cache_resource(show_spinner="Menghubungkan ke Google...")
def get_backends():
    return build_backends(_plain(st.secrets))


@st.cache_data(ttl=600, show_spinner="Memuat master Store & SKU...")
def get_master(master_id: str, store_ws: str, sku_ws: str, nonce: int):
    """Master dengan cadangan: bila spreadsheet bermasalah, data terakhir yang valid dipakai."""
    be = get_backends()
    return load_master_with_fallback(lambda: load_master(be.gc, master_id, store_ws, sku_ws))


@st.cache_data(ttl=30, show_spinner=False)
def get_existing(store_id: str, iso_date: str, nonce: int):
    return get_backends().bq_store.fetch_existing(date.fromisoformat(iso_date), store_id)


@st.cache_data(ttl=60, show_spinner=False)
def get_day_rows(iso_date: str, nonce: int):
    """Transaksi satu tanggal penjualan, untuk tab Admin."""
    return get_backends().bq_store.fetch_by_date(date.fromisoformat(iso_date))


@st.cache_data(show_spinner=False, max_entries=64)
def compress(data: bytes, name: str | None):
    """(jpeg_bytes, error, ukuran_asli, ukuran_akhir). Di-cache agar rerun tidak mengompres ulang."""
    try:
        p = process_photo(data, name)
        return p.data, None, p.original_size, p.size
    except ImageError as exc:
        return None, str(exc), len(data), 0


def _kb(n: int) -> str:
    return f"{n / 1024:.0f} KB" if n < 1024 * 1024 else f"{n / 1024 / 1024:.1f} MB"


# ── State ───────────────────────────────────────────────────────────────────

def _init_state():
    ss = st.session_state
    ss.setdefault("gen", 0)               # naik → semua widget baris di-reset
    ss.setdefault("next_uid", 1)
    ss.setdefault("rows", [])
    ss.setdefault("row_fixed", {})        # uid → label SKU untuk baris dari template (semua SKU)
    ss.setdefault("rows_ready", False)    # False → daftar diisi ulang dengan SEMUA SKU
    ss.setdefault("attempted", False)     # True setelah klik Submit → error "wajib diisi" ikut tampil
    ss.setdefault("nonce", 0)             # naik setelah submit → baca ulang data lama
    ss.setdefault("master_nonce", 0)
    ss.setdefault("last_result", None)


def _add_row() -> int:
    uid = st.session_state.next_uid
    st.session_state.next_uid += 1
    st.session_state.rows.append(uid)
    return uid


def _delete_row(uid: int):
    st.session_state.rows = [r for r in st.session_state.rows if r != uid]
    st.session_state.row_fixed.pop(uid, None)


def _k(kind: str, uid: int) -> str:
    return f"{kind}_{st.session_state.gen}_{uid}"


def _fill_all_sku_rows(skus):
    """Template awal: satu baris untuk SETIAP SKU di master (urutan master)."""
    ss = st.session_state
    ss.rows = []
    ss.row_fixed = {}
    for sku in skus:
        uid = _add_row()
        ss.row_fixed[uid] = sku.label
    ss.rows_ready = True


def _reset_rows():
    st.session_state.gen += 1
    st.session_state.rows_ready = False
    st.session_state.attempted = False


def _mark_attempted():
    st.session_state.attempted = True


# ── Tampilan hasil ──────────────────────────────────────────────────────────

def render_result(res):
    st.success(f"Tersimpan: {res.inserted} baris baru (INSERT), {res.updated} baris diperbarui (UPDATE).")
    if res.sales_date and res.submitted_at:
        st.caption(f"Tanggal penjualan: **{res.sales_date.isoformat()}**  ·  "
                   f"Waktu submit: **{fmt_wib(res.submitted_at)}**")
    c1, c2 = st.columns(2)
    with c1:
        keep_row()
        st.metric("Status BigQuery", res.bigquery_status)
    c2.metric("Status Google Sheets mirror", "OK" if res.sheets_status == "OK" else "GAGAL")
    if res.sheets_status != "OK":
        st.caption(res.sheets_status)
    for w in res.warnings:
        st.warning(w)
    df = pd.DataFrame([{
        "SKU": f"{ln.sku_code} - {ln.sku_name}", "Sell Out": ln.sell_out_qty, "Stock": ln.stock_qty,
        "Aksi": ln.action,
        "Transaction ID": ln.transaction_id, "Evidence": ln.evidence_url,
    } for ln in res.lines])
    st.dataframe(df, hide_index=True, width="stretch",
                 column_config={"Evidence": st.column_config.LinkColumn("Evidence", display_text="Buka foto")})


# ── Halaman ─────────────────────────────────────────────────────────────────

def main():
    _init_state()
    ss = st.session_state
    apply_mobile_layout()
    st.title("🛒 Store Sales Input")
    st.caption("Catat Sell Out dan Stock per SKU di store. Submit ulang untuk tanggal + store + SKU yang sama "
               "memperbarui data lama (tanpa duplikat).")

    try:
        be = get_backends()
        mres = get_master(be.config.master_sheet_id, be.config.store_master_worksheet,
                          be.config.sku_master_worksheet, ss.master_nonce)
    except (ConfigError, MasterError) as exc:
        st.error(str(exc))
        st.stop()
    except Exception as exc:
        log.exception("Inisialisasi gagal")
        st.error(f"Gagal terhubung ke layanan Google: {exc.__class__.__name__}. Periksa secrets dan akses service account.")
        st.stop()
    master = mres.data

    if mres.stale:
        st.warning(f"⚠️ Master Store/SKU di spreadsheet sedang bermasalah, jadi aplikasi memakai **data master "
                   f"terakhir yang valid** (dibaca {fmt_wib(mres.loaded_at)}). Perubahan terbaru di spreadsheet "
                   f"belum terpakai. Penyebab: {mres.error}")

    if not ss.rows_ready:
        _fill_all_sku_rows(list(master.skus.values()))

    with st.sidebar:
        st.caption(f"Evidence: gs://{be.config.evidence_bucket}")
        st.caption(f"Master dibaca: {fmt_wib(mres.loaded_at)}" + (" (data lama)" if mres.stale else ""))
        if st.button("Muat ulang master Store/SKU"):
            ss.master_nonce += 1
            st.rerun()
        st.button("Reset daftar SKU (kosongkan isian)", on_click=_reset_rows)
        with st.expander("Info master"):
            for line in master.diagnostics:
                st.caption(line)

    tab_input, tab_admin = st.tabs(["📝 Input Penjualan", "🔐 Admin"])
    with tab_input:
        render_input(be, master)
    with tab_admin:
        render_admin(be, master, get_day_rows)


def render_input(be, master):
    """Form input penjualan (tidak bergantung pada tab Admin)."""
    ss = st.session_state
    if ss.last_result is not None:
        render_result(ss.last_result)
        st.divider()

    # 1–3. Region → Store, tanggal, supervisor
    region_of = {s.store_id: (s.region or NO_REGION) for s in master.stores.values()}
    regions = sorted(set(region_of.values()))
    region = st.selectbox("Region", regions, index=None, placeholder="Pilih region dulu")
    in_region = sorted((s for s in master.stores.values() if region_of[s.store_id] == region),
                       key=lambda s: s.label) if region else []
    by_label = {s.label: s for s in in_region}
    label = st.selectbox("Selected Store", list(by_label), index=None, key=f"store_{region}",
                         disabled=region is None,
                         placeholder="Ketik ID atau nama store (hanya Non BA)" if region else "Pilih region dulu")
    store = by_label.get(label) if label else None
    if store:
        st.caption(f"Region: {store.region or '-'} · Group: {store.group or '-'} · {store.ba_status}")
    sales_date = st.date_input("Tanggal Penjualan", value=datetime.now(WIB).date(), format="YYYY-MM-DD")
    supervisor = st.text_input("Nama Supervisor", max_chars=100)

    # Data lama: hanya dibaca setelah store + tanggal dipilih.
    existing: list[dict] = []
    if store and sales_date:
        try:
            existing = get_existing(store.store_id, sales_date.isoformat(), ss.nonce)
        except BigQueryError as exc:
            st.error(str(exc))
        if existing:
            st.info(f"Sudah ada {len(existing)} SKU tercatat untuk store ini pada {sales_date.isoformat()}. "
                    "Submit akan memperbarui SKU yang sama.")
            with st.expander("Data yang sudah ada", expanded=False):
                st.dataframe(pd.DataFrame([{
                    "SKU": f"{r['sku_code']} - {r['sku_name']}", "Sell Out": r["sell_out_qty"], "Stock": r["stock_qty"],
                    "Supervisor": r["supervisor_name"], "Evidence": r["evidence_file_url"],
                    "Tanggal Penjualan": r["tanggal_penjualan"].isoformat(),
                    "Waktu Submit Pertama": fmt_wib(r["created_at"]),
                    "Terakhir Diperbarui": fmt_wib(r["updated_at"]),
                } for r in existing]), hide_index=True, width="stretch",
                    column_config={"Evidence": st.column_config.LinkColumn("Evidence", display_text="Buka foto")})

    # 4. Daftar SKU (template awal = semua SKU)
    st.subheader("Daftar SKU")
    st.caption(f"{len(master.skus)} SKU tampil otomatis. Isi **Sell Out** (penjualan store ke end consumer) dan "
               f"**Stock** untuk setiap SKU, boleh 0. **Foto evidence wajib: minimal satu foto di setiap submit** "
               f"(otomatis dikompres, maksimal {MAX_BYTES // 1_000_000} MB).")
    sku_labels = [s.label for s in master.skus.values()]
    sku_by_label = {s.label: s for s in master.skus.values()}
    slots: dict[int, st.delta_generator.DeltaGenerator] = {}
    inputs: list[LineInput] = []
    for i, uid in enumerate(list(ss.rows), start=1):
        with st.container(border=True):
            fixed_label = ss.row_fixed.get(uid)
            head = st.columns([6, 1], vertical_alignment="center")
            with head[0]:
                keep_row("icon")
                if fixed_label:                                   # SKU dari template: teks tetap, hemat layar HP
                    sku = sku_by_label.get(fixed_label)
                    st.markdown(f"**{i}. {sku.code}**  \n{sku.name}" if sku else f"**{i}.** {fixed_label}")
                    sku_label = fixed_label
                else:                                             # baris tambahan: pilih SKU sendiri
                    sku_label = st.selectbox(f"SKU #{i}", sku_labels, index=None, key=_k("sku", uid),
                                             placeholder="Ketik kode atau nama SKU")
            head[1].button("🗑", key=_k("del", uid), help="Hapus baris", on_click=_delete_row, args=(uid,))
            qty = st.columns(2)
            with qty[0]:
                keep_row()
                sell_out_raw = st.text_input("Sell Out", key=_k("so", uid), placeholder="0",
                                             help="Penjualan store ke end consumer (pcs)")
            stock_raw = qty[1].text_input("Stock", key=_k("st", uid), placeholder="0",
                                          help="Stock SKU di store (pcs)")
            with st.expander("📷 Foto SKU (opsional)"):
                cam = st.camera_input("Ambil foto", key=_k("cam", uid))
                upl = st.file_uploader("…atau upload dari galeri (JPG/JPEG)", key=_k("upl", uid))
            photo, photo_error = None, None
            chosen = cam or upl
            if chosen is not None:
                jpeg, photo_error, orig, final = compress(chosen.getvalue(), None if chosen is cam else chosen.name)
                photo = jpeg
                if jpeg:
                    st.caption(f"✅ Foto siap — {_kb(orig)} → {_kb(final)} (JPG)"
                               + ("  · kamera & upload sama-sama terisi, foto kamera yang dipakai" if cam and upl else ""))
                else:
                    st.caption(f"❌ {photo_error}")
            else:
                st.caption("Belum ada foto")
            slots[i] = st.empty()
            inputs.append(LineInput(row_no=i, sku_code=sku_by_label[sku_label].code if sku_label in sku_by_label else None,
                                    sell_out_raw=sell_out_raw, stock_raw=stock_raw,
                                    photo=photo, photo_error=photo_error))
    st.button("➕ Tambah SKU", on_click=_add_row, width="stretch")

    # Validasi live
    result = validate_submission(
        store_id=store.store_id if store else None, sales_date=sales_date, supervisor_name=supervisor,
        lines=inputs, stores=master.stores, skus=master.skus)

    def visible(issue) -> bool:
        """Error 'wajib diisi' di baris baru muncul setelah klik Submit, supaya halaman awal tidak merah semua."""
        return ss.attempted or not issue.message.endswith(_REQUIRED_SUFFIX)

    # Nama supervisor sengaja tidak dihitung: ia tetap terisi setelah submit, padahal form sudah kosong lagi.
    has_input = any(
        (ln.sell_out_raw or "").strip() or (ln.stock_raw or "").strip() or ln.photo or ln.photo_error
        for ln in inputs)

    # Satu slot per baris hanya bisa menampung satu elemen, jadi pesan satu baris digabung.
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
            "SKU": f"{ln.sku_code} - {ln.sku_name}", "Sell Out": ln.sell_out_qty, "Stock": ln.stock_qty,
            "Aksi": actions[ln.sku_code],
            "Sell Out lama": (old.get(ln.sku_code) or {}).get("sell_out_qty"),
            "Stock lama": (old.get(ln.sku_code) or {}).get("stock_qty"),
            "Foto": "Baru" if ln.photo else ("Tetap (foto lama)" if (old.get(ln.sku_code) or {}).get("evidence_file_id") else "-"),
        } for ln in result.lines]), hide_index=True, width="stretch")
    no_photo = [w for w in result.warnings if w.message.startswith("PERINGATAN")]
    if no_photo:
        st.warning(f"⚠️ {len(no_photo)} SKU dengan Sell Out > 0 belum punya foto evidence. "
                   "Submit tetap boleh selama minimal satu foto terlampir.")
    for w in result.warnings:
        if not w.message.startswith("PERINGATAN"):
            st.warning(str(w))
    if has_input or ss.attempted:
        for issue in result.errors:
            if not issue.row_no:
                st.error(issue.message)

    # 5. Submit
    if st.button("Submit", type="primary", width="stretch", on_click=_mark_attempted):
        if not result.ok:
            st.error("Perbaiki isian di atas terlebih dahulu. Tidak ada data yang disimpan.")
        elif store is None or sales_date is None:
            st.error("Store dan tanggal wajib diisi.")
        else:
            submitter = SalesSubmitter(be.bq_store, be.evidence, be.mirror)
            try:
                with st.spinner("Menyimpan..."):
                    out = submitter.submit(store=store, sales_date=sales_date, supervisor_name=supervisor,
                                           lines=result.lines)
            except SubmissionRejected as exc:
                for i in exc.issues:
                    st.error(str(i))
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
