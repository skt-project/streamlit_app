"""Form Upload Visibility Travel Pack — pengganti Google Form untuk ASM (dipakai dari HP).

Alur: Nama ASM -> toko ASM itu yang Ach SKU (MOQ) kolom K >= 4 -> Nama SPV -> foto display -> Submit.
Foto masuk Google Cloud Storage, 1 baris per submit masuk tab hasil di Google Sheets.
"""
from __future__ import annotations

import hashlib
import html
import logging

import pandas as pd
import streamlit as st
from streamlit.errors import StreamlitSecretNotFoundError

import clients
import stores
from config import ConfigError
from mobile import apply_mobile_layout
from photos import ACCEPTED_TYPES, PhotoError, human_size, inspect_photo, thumbnail
from responses import ResponseSheetError
from service import MAX_SPV_LEN, SubmitFailed, SubmitRejected
from stores import StoreListError, fmt_number

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("travel_pack.app")

st.set_page_config(page_title="Upload Visibility Travel Pack", page_icon="📸", layout="centered",
                   initial_sidebar_state="collapsed")
apply_mobile_layout()


def _plain(obj):
    """Salin st.secrets (AttrDict) menjadi dict biasa."""
    if hasattr(obj, "items"):
        return {k: _plain(v) for k, v in obj.items()}
    return obj


@st.cache_resource(show_spinner="Menghubungkan ke Google...")
def get_backends():
    try:
        secrets = _plain(st.secrets)
    except StreamlitSecretNotFoundError:
        secrets = {}                   # -> ConfigError "Secrets [app] belum lengkap"
    return clients.build_backends(secrets)


@st.cache_resource
def _last_good() -> dict:
    """Daftar toko terakhir yang berhasil dibaca (dipakai bila spreadsheet sementara bermasalah)."""
    return {}


@st.cache_data(ttl=300, show_spinner="Memuat daftar toko...")
def get_store_list(nonce: int):
    be = get_backends()
    cfg = be.config
    return stores.load_store_list(be.gc, cfg.store_sheet_id, worksheet=cfg.store_worksheet,
                                  ach_index=cfg.ach_index, ach_column=cfg.ach_column, min_ach=cfg.min_ach,
                                  overrides=cfg.header_overrides)


@st.cache_data(ttl=60, show_spinner=False)
def get_upload_index(nonce: int):
    return get_backends().responses.upload_index()


@st.cache_data(show_spinner=False, max_entries=8)
def get_preview(data: bytes):
    """(info, thumbnail_bytes, error). Di-cache agar rerun tidak memproses ulang foto yang sama."""
    try:
        info = inspect_photo(data)
    except PhotoError as exc:
        return None, None, str(exc)
    try:
        return info, thumbnail(data), None
    except Exception:
        return info, None, None


def esc(text: str) -> str:
    return html.escape(str(text or ""))


def _init_state():
    ss = st.session_state
    ss.setdefault("gen", 0)            # naik setelah submit sukses -> uploader & SPV direset
    ss.setdefault("nonce", 0)          # naik setelah submit / muat ulang -> status dibaca ulang
    ss.setdefault("last_result", None)
    ss.setdefault("sent", set())       # hash (toko + foto) yang sudah terkirim di sesi ini


def _load_store_list():
    try:
        sl = get_store_list(st.session_state.nonce)
        _last_good()["store_list"] = sl
        return sl
    except StoreListError as exc:
        cached = _last_good().get("store_list")
        if cached is None:
            st.error(str(exc))
            st.stop()
        st.warning(f"{exc} Menampilkan daftar toko terakhir yang berhasil dimuat.")
        return cached


def _show_last_result():
    """Konfirmasi submit terakhir. Pilihan toko sudah direset, jadi halaman memendek dan pesan ini
    tampil kira-kira di posisi tombol Submit tadi (di HP layar tidak melompat ke atas)."""
    res = st.session_state.last_result
    if not res:
        return
    st.toast(f"Terkirim: {res['code']}", icon="✅")
    st.success(f"Terkirim: **{res['code']}** · {res['name']} — {res['at']} WIB. Pilih toko berikutnya.")
    st.caption(f"ID submit: {res['id'][:8]} · {res['file']}")
    st.session_state.last_result = None


def main():
    _init_state()
    try:
        be = get_backends()
    except ConfigError as exc:
        st.error(f"Konfigurasi aplikasi belum lengkap: {exc}")
        st.stop()
    except Exception as exc:
        log.exception("Gagal membuat koneksi Google")
        st.error(f"Gagal terhubung ke Google ({exc.__class__.__name__}). Coba muat ulang halaman.")
        st.stop()
    cfg = be.config
    ss = st.session_state

    st.title(cfg.title)
    store_list = _load_store_list()
    _sidebar(store_list)
    min_txt = fmt_number(store_list.min_ach)

    # 1. Nama ASM (bisa dibuka langsung lewat link ?asm=Nama)
    names = store_list.asm_names()
    if not names:
        st.error("Daftar toko tidak berisi nama ASM.")
        st.stop()
    wanted = (st.query_params.get("asm") or "").casefold()
    default = next((i for i, n in enumerate(names) if n.casefold() == wanted), None)
    asm = st.selectbox("Nama ASM *", names, index=default, placeholder="Pilih nama ASM", key="asm")
    if not asm:
        st.info(f"Pilih nama ASM. Yang muncul hanya toko dengan Ach SKU (MOQ) ≥ {min_txt}.")
        st.stop()
    if st.query_params.get("asm") != asm:
        st.query_params["asm"] = asm

    eligible = store_list.eligible_for(asm)
    if not eligible:
        total = len(store_list.stores_for(asm))
        st.warning(f"Belum ada toko {asm} dengan Ach SKU (MOQ) ≥ {min_txt} (dari {total} toko).")
        st.stop()

    status_ok = True
    try:
        index = get_upload_index(ss.nonce)
    except ResponseSheetError as exc:
        index, status_ok = {}, False
        st.warning(f"Status upload belum bisa dibaca: {exc}")

    done = sum(1 for s in eligible if s.code in index)
    st.progress(done / len(eligible), text=f"{done} dari {len(eligible)} toko sudah upload")
    with st.expander("Lihat daftar toko"):
        st.dataframe(
            pd.DataFrame([{
                "Status": (f"✅ {index[s.code].last_at}" if s.code in index else "Belum") if status_ok else "?",
                "Kode": s.code, "Nama Toko": s.name, "Ach SKU": fmt_number(s.ach),
            } for s in eligible]),
            hide_index=True, width="stretch")

    # 2. Toko (belum upload di atas). Key memuat `gen` -> pilihan kosong lagi setelah submit sukses.
    _show_last_result()
    by_code = {s.code: s for s in eligible}
    options = sorted(by_code, key=lambda c: (c in index, by_code[c].name.casefold(), c))
    code = st.selectbox(
        "Toko *", options, index=None, placeholder="Ketik kode / nama toko",
        format_func=lambda c: ("✅ " if c in index else "") + by_code[c].label, key=f"store_{asm}_{ss.gen}")
    if not code:
        st.caption(f"{len(eligible)} toko memenuhi syarat Ach SKU (MOQ) ≥ {min_txt}.")
        st.stop()
    store = by_code[code]
    meta = [f"Kode ST Toko: <b>{esc(store.code)}</b>", f"Ach SKU (MOQ): <b>{esc(fmt_number(store.ach))}</b>"]
    if store.region:
        meta.append(esc(store.region))
    st.markdown(f'<div class="tp-card"><div class="tp-name">{esc(store.name or store.code)}</div>'
                f'<div class="tp-meta">{" · ".join(meta)}</div></div>', unsafe_allow_html=True)
    if code in index:
        prev = index[code]
        st.info(f"Toko ini sudah upload {prev.count}x (terakhir {prev.last_at} WIB). Submit lagi akan "
                "menambah foto baru; foto lama tetap tersimpan.")

    # 3. Nama SPV (terisi dari daftar toko bila ada, boleh diubah)
    spv = st.text_input("Nama SPV", value=store.spv, max_chars=MAX_SPV_LEN, placeholder="Nama SPV toko ini",
                        key=f"spv_{code}_{ss.gen}")

    # 4. Foto
    photo = st.file_uploader("Foto display toko pakai GPS Camera *", type=ACCEPTED_TYPES, key=f"photo_{ss.gen}")
    st.caption("1 foto JPG/PNG, maks 10 MB. Foto pakai aplikasi GPS Camera (lokasi & waktu tercetak di foto), "
               "lalu pilih dari galeri.")
    data = photo.getvalue() if photo is not None else None
    if data:
        info, thumb, err = get_preview(data)
        if err:
            st.error(err)
        else:
            if thumb:
                st.image(thumb, width="stretch")
            extra = [f"{info.width}×{info.height}", human_size(info.size)]
            if info.taken_at:
                extra.append(f"diambil {info.taken_at}")
            st.caption(" · ".join(extra) + (f" · GPS {info.gps}" if info.gps else ""))

    if st.button("Submit", type="primary", width="stretch"):
        digest = hashlib.sha256(code.encode() + b"|" + (data or b"")).hexdigest()
        if data and digest in ss.sent:
            st.warning("Foto ini sudah terkirim untuk toko ini.")
            st.stop()
        try:
            with st.spinner("Mengirim foto... jangan tutup halaman ini."):
                result = be.submitter.submit(store_list=store_list, asm=asm, store_code=code, spv=spv, photo=data)
        except (SubmitRejected, SubmitFailed) as exc:
            st.error(str(exc))
            st.stop()
        ss.sent.add(digest)
        ss.gen += 1
        ss.nonce += 1
        get_upload_index.clear()          # status upload segar juga untuk sesi ASM lain
        ss.last_result = {"code": result.store.code, "name": result.store.name, "at": result.submitted_at,
                          "id": result.submission_id, "file": result.photo.file_name}
        st.rerun()


def _sidebar(store_list):
    with st.sidebar:
        st.subheader("Info data")
        for line in store_list.diagnostics:
            st.caption(line)
        if st.button("Muat ulang daftar toko & status", width="stretch"):
            st.session_state.nonce += 1
            st.rerun()


main()
