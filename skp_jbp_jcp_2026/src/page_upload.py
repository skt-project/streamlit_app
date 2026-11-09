"""Upload flow: cari toko -> pilih brand/program/kuartal -> upload PDF ->
rangkuman -> dialog konfirmasi -> GCS + SKP_UPLOAD_LOG."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from src import ui
from src.runtime import Backends, DataUnavailable, clear_log_cache, load_logs, load_source
from src.service import PlannedDocument, plan_document, resolve_path_collisions, submit_documents, uploads_for_store
from src.sheets import Detection, Store, detect_programs, search_stores_by_code, search_stores_by_name, store_from_index
from src.validation import (
    BRANDS,
    PROGRAMS,
    QUARTERS,
    build_target_filename,
    clean_cell,
    format_size,
    validate_pic,
)

SEARCH_LIMIT = 50


def _nonce() -> int:
    return st.session_state.setdefault("form_nonce", 0)


def _k(name: str) -> str:
    return f"{name}__{_nonce()}"


def _clear_search() -> None:
    st.session_state[_k("q_code")] = ""
    st.session_state[_k("q_name")] = ""


def _go(page: str) -> None:
    st.session_state["page"] = page
    st.query_params["page"] = page


def render(backends: Backends) -> None:
    if st.button("← Kembali ke beranda"):
        _go("home")
        st.rerun()
    st.title("📤 Upload SKP JBP / JCP")
    ui.mode_banner(backends.mode, backends.source.label if backends.source else "-",
                   backends.storage.label if backends.storage else "-", backends.logs.label if backends.logs else "-")

    last = st.session_state.pop("submit_problem", None)
    if last:
        st.error(last)

    try:
        source_df, index = load_source(backends)
    except DataUnavailable as exc:
        st.error(f"Daftar toko tidak dapat dimuat. {exc}")
        return
    try:
        logs_df = load_logs(backends)
    except DataUnavailable as exc:
        st.warning(f"Riwayat upload tidak dapat dibaca ({exc}). Upload tetap bisa dilakukan.")
        logs_df = pd.DataFrame()

    store = _search_store(index)
    if store is None:
        return
    detection = detect_programs(source_df, store.code)
    previous = uploads_for_store(logs_df, store.code)
    _store_card(store, detection)

    combos = _pick_programs(store, detection)
    if not combos:
        st.info("Pilih minimal satu brand, program, dan kuartal untuk menampilkan area upload.")
        return

    docs, missing = _upload_documents(store, detection, combos, previous)
    resolve_path_collisions(store, docs, logs_df)
    _summary_and_submit(backends, store, combos, docs, missing, detection)


# --------------------------------------------------------------------------

def _search_store(index: pd.DataFrame) -> Store | None:
    ui.section(1, "Cari toko")
    st.caption("Isi **salah satu** saja: Kode atau Nama. Kolom yang lain otomatis terkunci.")
    code_key, name_key = _k("q_code"), _k("q_name")
    code_val = clean_cell(st.session_state.get(code_key, ""))
    name_val = clean_cell(st.session_state.get(name_key, ""))
    c1, c2 = st.columns(2)
    c1.text_input("🔎 Kode Toko/Group", key=code_key, disabled=bool(name_val),
                  placeholder="contoh: ICES02525 atau nama group", max_chars=60)
    c2.text_input("🏪 Nama Toko/Group", key=name_key, disabled=bool(code_val),
                  placeholder="contoh: Beauty", max_chars=80)
    if code_val or name_val:
        st.button("✖ Kosongkan pencarian", on_click=_clear_search)
    if not (code_val or name_val):
        st.info("Ketik kode atau nama toko, lalu tekan Enter.")
        return None

    by_code = bool(code_val)
    query = code_val if by_code else name_val
    results = (search_stores_by_code if by_code else search_stores_by_name)(index, query, SEARCH_LIMIT)
    if not results:
        st.warning(f"Tidak ada toko dengan {'kode' if by_code else 'nama'} yang mengandung “{query}”.")
        return None
    note = f" (ditampilkan {SEARCH_LIMIT} teratas — ketik lebih spesifik)" if len(results) >= SEARCH_LIMIT else ""
    st.caption(f"{len(results)} toko ditemukan{note}.")
    labels = {s.code: s.label for s in results}
    picked = st.selectbox(
        "Pilih toko",
        options=list(labels),
        format_func=lambda c: labels[c],
        index=0 if len(results) == 1 else None,
        placeholder="Pilih toko dari hasil pencarian…",
        key=_k(f"pick_{'code' if by_code else 'name'}_{query.upper()}"),
    )
    if not picked:
        return None
    return store_from_index(index, picked)


def _store_card(store: Store, detection: Detection) -> None:
    with st.container(border=True):
        ui.facts([
            ("Kode Toko/Group", store.code),
            ("Nama Toko/Group", store.name),
            ("Region", store.region or "Belum diisi"),
        ])
        if store.aliases:
            st.caption("Nama lain di spreadsheet: " + "; ".join(store.aliases))
        if not store.region:
            st.warning("Region toko kosong di spreadsheet. Dokumen akan disimpan di folder “TANPA REGION”.")
        programs = sorted(detection.programs, key=lambda bp: (BRANDS.index(bp[0]), PROGRAMS.index(bp[1])))
        if programs:
            st.markdown("**Program terdeteksi**<br>" + ui.chips([f"{b} · {p}" for b, p in programs]),
                        unsafe_allow_html=True)
            rows = [[b, p, *[detection.skp_number(b, p, q) or "—" for q in QUARTERS]] for b, p in programs]
            st.markdown("**Nomor SKP per kuartal**")
            ui.scroll_table(["Brand", "Program", *QUARTERS], rows)
        else:
            st.info("Belum ada program/nomor SKP untuk toko ini di spreadsheet. Anda tetap bisa memilih manual.")
        for conflict in detection.conflicts:
            st.warning(conflict)


def _pick_programs(store: Store, detection: Detection) -> list[tuple[str, str, str]]:
    ui.section(2, "Pilih brand, program, dan kuartal")
    st.caption("Tanda ✓ = terdeteksi dari spreadsheet dan sudah dipilih otomatis. Anda boleh menambah atau mengurangi pilihan.")
    sk = f"{_nonce()}_{store.code}"
    brand_cols = st.columns(len(BRANDS))
    chosen_brands = [
        brand for col, brand in zip(brand_cols, BRANDS)
        if col.checkbox(f"{brand}{' ✓' if brand in detection.brands else ''}", value=brand in detection.brands,
                        key=f"brand_{sk}_{brand}")
    ]
    combos: list[tuple[str, str, str]] = []
    for brand in chosen_brands:
        with st.container(border=True):
            st.markdown(f"**{brand}**")
            for program in PROGRAMS:
                detected = (brand, program) in detection.programs
                on = st.checkbox(f"{program}{' ✓ terdeteksi' if detected else ''}", value=detected,
                                 key=f"prog_{sk}_{brand}_{program}")
                if not on:
                    continue
                picked = st.pills(
                    f"Kuartal {brand} {program}",
                    QUARTERS,
                    selection_mode="multi",
                    default=detection.quarters_for(brand, program),
                    format_func=lambda q, b=brand, p=program: f"{q} ✓" if detection.skp_number(b, p, q) else q,
                    key=f"qtr_{sk}_{brand}_{program}",
                    label_visibility="collapsed",
                ) or []
                if not picked:
                    st.caption(f"Pilih kuartal untuk {program}.")
                combos.extend((brand, program, q) for q in QUARTERS if q in picked)
    return combos


def _upload_documents(store: Store, detection: Detection, combos, previous) -> tuple[list[PlannedDocument], list]:
    ui.section(3, "Upload dokumen PDF")
    st.caption("Satu file PDF per kombinasi, maksimal 1 MB. Upload ulang untuk kombinasi yang sama akan menimpa file lama.")
    st.text_input("👤 PIC Submit (wajib)", key=_k("pic"), max_chars=80, placeholder="Nama lengkap yang mengupload")
    sk = f"{_nonce()}_{store.code}"
    docs: list[PlannedDocument] = []
    missing: list[tuple[str, str, str]] = []
    for brand, program, quarter in combos:
        skp = detection.skp_number(brand, program, quarter)
        target = build_target_filename(store.name, skp, brand, program, quarter)
        with st.container(border=True):
            st.markdown(f"**{brand} · {program} · {quarter}**")
            if skp:
                st.markdown(f"Nomor SKP: **{ui.esc(skp)}**", unsafe_allow_html=True)
            else:
                st.caption("⚠️ Nomor SKP tidak ada di spreadsheet (pilihan manual). Upload tetap diperbolehkan.")
            st.markdown(f'Disimpan sebagai: <span class="skp-filename">{ui.esc(target)}</span>', unsafe_allow_html=True)
            prev = previous.get((brand, program, quarter))
            if prev:
                st.info(f"Sudah pernah diupload {prev.get('Timestamp', '')} oleh {prev.get('PIC Submit', '')}. "
                        "Upload baru akan menimpa file dan metadata lama.")
            uploaded = st.file_uploader(
                "File PDF (maks 1 MB)",
                type=["pdf"],
                key=f"file_{sk}_{brand}_{program}_{quarter}",
                max_upload_size=1,
                help="Hanya PDF, maksimal 1 MB.",
            )
            st.caption("File bertanda merah / pesan *“File must be 1.0MB or smaller”* = ukuran lebih dari 1 MB "
                       "dan ditolak. Kompres PDF terlebih dahulu, lalu upload ulang.")
            if uploaded is None:
                missing.append((brand, program, quarter))
                continue
            doc = plan_document(store, brand, program, quarter, skp, uploaded.name, uploaded.getvalue())
            for err in doc.check.errors:
                st.error(err)
            for warn in doc.check.warnings:
                st.warning(warn)
            if doc.check.ok and not doc.check.warnings:
                st.success(f"✅ {uploaded.name} · {format_size(len(doc.data))}")
            docs.append(doc)
    return docs, missing


def _summary_rows(docs: list[PlannedDocument], missing) -> list[dict]:
    rows = [{
        "Brand": d.brand, "Program": d.program, "Kuartal": d.quarter, "Nomor SKP": d.skp_number or "—",
        "Nama file": d.target_filename, "File asli": d.original_filename, "Status validasi": d.check.label,
    } for d in docs]
    rows += [{
        "Brand": b, "Program": p, "Kuartal": q, "Nomor SKP": "", "Nama file": "—", "File asli": "—",
        "Status validasi": "⏭️ Belum ada file (dilewati)",
    } for b, p, q in missing]
    order = {(b, p, q): i for i, (b, p, q) in enumerate(
        (b, p, q) for b in BRANDS for p in PROGRAMS for q in QUARTERS)}
    return sorted(rows, key=lambda r: order[(r["Brand"], r["Program"], r["Kuartal"])])


def _summary_and_submit(backends: Backends, store: Store, combos, docs, missing, detection: Detection) -> None:
    ui.section(4, "Periksa rangkuman")
    pic = clean_cell(st.session_state.get(_k("pic"), ""))
    with st.container(border=True):
        ui.facts([
            ("Nama toko", store.name), ("Kode toko", store.code), ("Region", store.region or "Belum diisi"),
            ("PIC Submit", pic or "Belum diisi"), ("Dokumen siap", f"{sum(d.check.ok for d in docs)} dari {len(combos)}"),
        ])
        st.dataframe(pd.DataFrame(_summary_rows(docs, missing)), hide_index=True, width="stretch")

    blockers: list[str] = []
    if not backends.mode.can_upload or backends.storage is None or backends.logs is None:
        blockers.append("Upload dinonaktifkan karena konfigurasi penyimpanan belum lengkap (lihat pesan di atas).")
    pic_error = validate_pic(pic)
    if pic_error:
        blockers.append(pic_error)
    if not docs:
        blockers.append("Belum ada file PDF yang dipilih.")
    if any(not d.check.ok for d in docs):
        blockers.append("Perbaiki atau hapus file bertanda ❌ terlebih dahulu.")
    if docs and missing:
        st.info(f"{len(missing)} kombinasi belum memiliki file dan akan dilewati.")
    for blocker in blockers:
        st.warning(blocker)

    if st.button("Submit dan tampilkan konfirmasi", type="primary", disabled=bool(blockers), width="stretch"):
        st.session_state["pending_submit"] = {"store": store, "pic": pic, "docs": docs}
        _confirm_dialog(backends)


@st.dialog("Konfirmasi terakhir", width="large")
def _confirm_dialog(backends: Backends) -> None:
    pending = st.session_state.get("pending_submit")
    if not pending:
        st.write("Tidak ada data untuk dikirim.")
        return
    store: Store = pending["store"]
    docs: list[PlannedDocument] = pending["docs"]
    ui.facts([("Nama toko", store.name), ("Kode toko", store.code), ("Region", store.region or "Belum diisi"),
              ("PIC Submit", pending["pic"]), ("Jumlah dokumen", str(len(docs)))])
    st.dataframe(pd.DataFrame([{
        "Brand": d.brand, "Program": d.program, "Kuartal": d.quarter, "Nomor SKP": d.skp_number or "—",
        "Nama file": d.target_filename, "Catatan": "⚠️ cek nama file" if d.name_warning else "",
    } for d in docs]), hide_index=True, width="stretch")
    if backends.mode.mock:
        st.warning("🧪 Mode lokal (mock): file PDF dan metadata hanya disimpan di komputer ini "
                   f"({backends.storage.label}; {backends.logs.label}), **bukan** ke GCS / spreadsheet.")
    else:
        st.warning(f"📦 File PDF akan disimpan ke Google Cloud Storage ({backends.storage.label}). "
                   "File lama untuk kombinasi yang sama akan ditimpa.")
        st.warning(f"📝 Metadata akan ditulis ke spreadsheet ({backends.logs.label}). "
                   "Data lama untuk kombinasi yang sama akan diperbarui, bukan diduplikasi.")

    c1, c2 = st.columns(2)
    if c1.button("Batalkan", width="stretch", key="dlg_cancel"):
        st.session_state.pop("pending_submit", None)
        st.rerun()
    if c2.button("Konfirmasi & Upload", type="primary", width="stretch", key="dlg_confirm"):
        with st.spinner("Menyimpan dokumen…"):
            try:
                result = submit_documents(store, pending["pic"], docs, backends.storage, backends.logs)
            except ValueError as exc:
                st.error(str(exc))
                return
        clear_log_cache()
        st.session_state.pop("pending_submit", None)
        if result.ok:
            st.session_state["last_result"] = {"store": store, "pic": pending["pic"], "saved": result.saved,
                                               "removed": result.removed_old}
            st.session_state["form_nonce"] = _nonce() + 1  # fresh, empty form next time
            _go("success")
        else:
            lines = [f"• {d.brand} {d.program} {d.quarter}: {msg}" for d, msg in result.failed]
            if result.log_error:
                lines.insert(0, result.log_error)
            saved = len(result.saved)
            head = f"{saved} dokumen tersimpan, {len(result.failed)} gagal." if saved else "Upload gagal."
            st.session_state["submit_problem"] = head + "  \n" + "  \n".join(lines) + \
                "  \nFile Anda masih terisi di form; tekan Submit lagi untuk mencoba ulang (tidak akan dobel)."
        st.rerun()
