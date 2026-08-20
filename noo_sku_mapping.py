"""NOO & SKU Mapping portal for distributor admins.

Login with a Distributor Code, download BD Support's template, upload a filled
file. The app validates, enriches from master data, classifies duplicates per
row, shows a preview, and only after explicit confirmation appends the eligible
enriched rows to the existing pool worksheet.

Write access is off unless explicitly enabled. In dry-run mode every step runs
for real — including the pool layout check — except the final append.
"""
from __future__ import annotations

import io

import pandas as pd
import streamlit as st

from noo_sku import (config, duplicates, enrichment, guideline, parsers,
                     pipeline, sources, validators, writer)
from noo_sku.customer_code import CustomerCodeResolver
from noo_sku.normalize import norm_key, now_business

st.set_page_config(page_title="NOO & SKU Mapping", page_icon="📦", layout="wide")

SECTION_NOO = "NOO / Store Mapping"
SECTION_SKU = "SKU Mapping"


# ─── Settings & clients ───────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def _settings():
    return config.Settings(secrets=st.secrets)


@st.cache_resource(show_spinner="Menyiapkan koneksi...")
def _clients():
    creds, project = sources.load_credentials(st.secrets)
    settings = config.Settings(secrets=st.secrets)
    return creds, project, sources.SheetsClient(creds,
                                                settings.tracker_spreadsheet_id)


@st.cache_data(ttl=3600, show_spinner="Memuat data distributor...")
def _distributors():
    _, _, client = _clients()
    return sources.load_distributors(client)


@st.cache_data(ttl=3600, show_spinner="Memuat master produk...")
def _products():
    creds, project, _ = _clients()
    return sources.load_products(creds, project)


@st.cache_data(ttl=3600, show_spinner="Memuat master distributor...")
def _master_distributor():
    creds, project, _ = _clients()
    try:
        return sources.load_master_distributor(creds, project)
    except Exception:
        return {}


@st.cache_data(ttl=1800, show_spinner="Memuat master toko...")
def _store_basis(distributor_code):
    creds, project, _ = _clients()
    try:
        return sources.load_store_basis(creds, project, [distributor_code])
    except Exception:
        return {}, {"skt": {}, "tph": {}, "fcr": {}}


@st.cache_data(ttl=3600, show_spinner="Menyiapkan kode customer...")
def _resolver():
    creds, project, client = _clients()
    distributors = sources.load_distributors(client)
    try:
        po = sources.suffixes_from_po_history(creds, project)
    except Exception:
        po = {}
    return CustomerCodeResolver(
        dist_database=sources.suffixes_from_dist_database(distributors),
        sku_history=sources.suffixes_from_sku_history(client),
        po_history=po,
        overrides=dict(st.secrets.get("distributor_suffix_overrides", {})))


@st.cache_data(ttl=86400, show_spinner=False)
def _cities():
    creds, _, _ = _clients()
    try:
        return sources.load_city_reference(creds)
    except Exception:
        return set()


@st.cache_data(ttl=86400, show_spinner=False)
def _template_bytes(file_id):
    creds, _, _ = _clients()
    return sources.download_template(creds, file_id)


@st.cache_data(ttl=86400, show_spinner=False)
def _guideline_pdf():
    return guideline.build_pdf()


# ─── Login ────────────────────────────────────────────────────────────────────
def render_login():
    st.title("📦 NOO & SKU Mapping")
    st.caption("Portal upload untuk admin distributor")
    if _settings().dry_run:
        st.info("Mode **DRY RUN** — file divalidasi penuh, tetapi tidak ditulis "
                "ke tracker.", icon="🧪")

    with st.form("login"):
        code = st.text_input("Kode Distributor", placeholder="DST123").strip()
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("🔓 Masuk", type="primary",
                                          use_container_width=True)
    if not submitted:
        return

    code = norm_key(code)
    if not code:
        st.error("Kode distributor wajib diisi.")
        return

    creds, project, _ = _clients()
    dataset = st.secrets["bigquery"]["dataset"]
    try:
        account = sources.check_login(creds, project, dataset, code, password)
    except Exception:
        st.error("Tidak bisa memverifikasi login saat ini. Coba lagi atau "
                 "hubungi administrator.")
        return
    if account is None:
        st.error("Kode distributor atau password salah, atau kode Anda belum "
                 "terdaftar. Hubungi BD Support.")
        return

    # DIST DATABASE remains the authority on name, region and active status.
    record = _distributors().get(account["distributor_code"])
    if record is None:
        st.error(f"Kode distributor **{code}** tidak ada di "
                 f"{config.TAB_DIST_DATABASE}. Hubungi BD Support.")
        return
    if not record["active"]:
        st.error(f"Distributor **{code}** berstatus "
                 f"{record['status'] or 'tidak aktif'}. Hubungi BD Support.")
        return

    st.session_state["auth"] = True
    st.session_state["distributor"] = record
    st.rerun()


# ─── Rendering helpers ────────────────────────────────────────────────────────
def _issue_frame(issues):
    return pd.DataFrame([{
        "Baris": i.row, "Kolom": i.column, "Masalah": i.problem,
        "Saran Perbaikan": i.suggestion,
    } for i in issues])


def _error_report_bytes(result):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xl:
        _issue_frame(result.errors).to_excel(xl, index=False,
                                             sheet_name="Error")
        if result.warnings:
            _issue_frame(result.warnings).to_excel(xl, index=False,
                                                   sheet_name="Peringatan")
        details = pipeline.duplicate_details(result)
        if details:
            pd.DataFrame(details).to_excel(xl, index=False,
                                           sheet_name="Duplikat")
    buf.seek(0)
    return buf.getvalue()


def _render_preview(result, kind):
    st.subheader("Ringkasan Upload")
    s = result.summary
    cols = st.columns(6)
    cols[0].metric("Total baris", s["total"])
    cols[1].metric("Baris baru", s["new"])
    cols[2].metric("Koreksi", s["correction"])
    cols[3].metric("Duplikat", s["exact_duplicate"] + s["duplicate_in_file"])
    cols[4].metric("Error", s["error"])
    cols[5].metric("Fallback", result.fallback_count)

    if s["exact_duplicate"]:
        st.warning(f"⚠️ {s['exact_duplicate']} baris sudah pernah diupload "
                   "sebelumnya dan tidak akan dimasukkan kembali.")
    if s["duplicate_in_file"]:
        st.warning(f"⚠️ File berisi {s['duplicate_in_file']} baris ganda. "
                   "Mohon periksa kembali input Anda — hanya baris pertama "
                   "yang diproses.")
    if s["correction"]:
        st.info(f"ℹ️ {s['correction']} baris terdeteksi sebagai koreksi dan "
                "akan dimasukkan sebagai baris baru.")

    details = pipeline.duplicate_details(result)
    if details:
        with st.expander(f"Lihat {len(details)} baris duplikat"):
            st.dataframe(pd.DataFrame(details), use_container_width=True,
                         hide_index=True)

    mapping = pipeline.mapping_sources(result)
    if mapping:
        label = f"Sumber mapping per baris ({len(mapping)})"
        if result.fallback_count:
            label += f" — {result.fallback_count} memakai fallback"
        if result.ambiguous_count:
            label += f", {result.ambiguous_count} ambigu"
        with st.expander(label, expanded=bool(result.fallback_count
                                              or result.ambiguous_count)):
            st.caption("Kolom *Fallback* menandai baris yang tidak mendapat "
                       "mapping langsung dari master brand-nya.")
            st.dataframe(pd.DataFrame(mapping), use_container_width=True,
                         hide_index=True)

    notes = pipeline.enrichment_details(result)
    if notes:
        blank = sum(1 for n in result.enrichment_notes
                    if n.status == enrichment.STATUS_NEW_STORE)
        ambiguous = sum(1 for n in result.enrichment_notes
                        if n.status == enrichment.STATUS_AMBIGUOUS)
        label = f"Lihat {len(notes)} catatan enrichment"
        if blank:
            label += f" — {blank} toko baru (kolom SE/SPV/AOM dikosongkan)"
        if ambiguous:
            label += f", {ambiguous} perlu ditinjau"
        with st.expander(label):
            st.caption("Enrichment yang tidak tersedia tidak membatalkan baris. "
                       "Kolom terkait dikosongkan dan dilengkapi BD Support.")
            st.dataframe(pd.DataFrame(notes), use_container_width=True,
                         hide_index=True)

    if result.eligible_rows:
        with st.expander(f"Pratinjau {len(result.eligible_rows)} baris yang "
                         "akan ditulis"):
            headers = writer.pool_headers_for(
                parsers.UPLOAD_NOO if kind == SECTION_NOO else parsers.UPLOAD_SKU)
            st.dataframe(pd.DataFrame(writer.to_values(result.eligible_rows,
                                                       headers),
                                      columns=headers),
                         use_container_width=True, hide_index=True)


def _render_result(result, write_result, dist, kind):
    if not write_result.ok:
        st.error(f"✗ Upload gagal — {write_result.message}")
        return
    icon = "🧪" if write_result.dry_run else "✅"
    title = "Dry run selesai" if write_result.dry_run else "Upload berhasil"
    st.success(f"{icon} **{title}**")
    left, right = st.columns(2)
    with left:
        st.markdown(
            f"**Distributor**  \n{dist['distributor_name']} "
            f"({dist['distributor_code']})\n\n**Jenis upload**  \n{kind}\n\n"
            f"**Referensi batch**  \n`{write_result.upload_id}`")
    with right:
        st.markdown(
            f"**Baris ditulis**  \n{write_result.rows_written} "
            f"({result.summary['new']} baru, "
            f"{result.summary['correction']} koreksi)\n\n"
            f"**input_time**  \n{result.when:%d-%b-%Y %H:%M:%S} WIB\n\n"
            f"**Tujuan**  \n`{write_result.destination}`")
    if write_result.dry_run:
        st.info(write_result.message, icon="🧪")


# ─── Upload handling ──────────────────────────────────────────────────────────
def _build_pipeline_result(kind, dist, uploaded):
    expected = (parsers.UPLOAD_NOO if kind == SECTION_NOO
                else parsers.UPLOAD_SKU)
    try:
        parsed = parsers.parse_upload(uploaded)
    except parsers.ParseError as exc:
        st.error(f"✗ {exc}")
        return None

    wrong = parsers.check_template_kind(parsed, expected)
    if wrong:
        st.error(f"✗ {wrong}")
        return None
    missing = parsers.missing_columns(parsed, expected)
    if missing:
        st.error("✗ Kolom wajib tidak ditemukan: **" + "**, **".join(missing)
                 + "**. Gunakan template resmi tanpa mengubah urutan kolom.")
        return None
    if not parsed.rows:
        st.error("✗ File tidak berisi data. Isi minimal satu baris di bawah "
                 "baris CONTOH.")
        return None
    if len(parsed.rows) > config.MAX_UPLOAD_ROWS:
        st.error(f"✗ File berisi {len(parsed.rows)} baris, melebihi batas "
                 f"{config.MAX_UPLOAD_ROWS} baris per upload.")
        return None

    _, _, client = _clients()
    resolver = _resolver()
    dist_code = dist["distributor_code"]
    dist_enricher = enrichment.DistributorEnricher(
        master_distributor=_master_distributor(), dist_database=_distributors())

    try:
        if expected == parsers.UPLOAD_NOO:
            by_cust, by_ref = _store_basis(dist_code)
            return pipeline.run_noo(
                parsed, distributor=dist, resolver=resolver,
                dist_enricher=dist_enricher,
                store_enricher=enrichment.StoreEnricher(by_cust, by_ref),
                ledger=sources.load_noo_ledger(client, dist_code),
                known_cities=_cities(), when=now_business())

        resolution = resolver.resolve(dist_code)
        if not resolution.resolved:
            st.error(
                f"✗ Kode singkatan untuk **{dist_code}** belum terdaftar, "
                "sehingga Customer Code tidak bisa dibuat otomatis. Hubungi "
                "BD Support untuk melengkapi kolom *Customer Branch Code* "
                f"pada sheet `{config.TAB_DIST_DATABASE}`.")
            return None
        return pipeline.run_sku(
            parsed, distributor=dist, resolver=resolver,
            dist_enricher=dist_enricher,
            product_enricher=enrichment.ProductEnricher(_products()),
            ledger=sources.load_sku_ledger(client, dist_code),
            product_lookup=_products(), when=now_business())
    except Exception as exc:
        st.error("✗ Gagal memproses file. Tidak ada data yang tersimpan. "
                 "Silakan coba lagi atau hubungi BD Support.")
        st.caption(f"Referensi teknis: {type(exc).__name__}")
        return None


def _handle_section(kind, dist):
    state_key = f"result_{kind}"
    uploaded = st.file_uploader("Upload file yang sudah diisi", type=["xlsx"],
                                key=f"upload_{kind}")

    if uploaded and st.button("1️⃣ Validasi & Pratinjau", type="primary",
                              key=f"check_{kind}"):
        st.session_state[state_key] = _build_pipeline_result(kind, dist,
                                                             uploaded)

    result = st.session_state.get(state_key)
    if result is None:
        return

    if result.errors:
        st.error(f"**{len(result.errors)} kesalahan ditemukan.** Perbaiki file "
                 "terlebih dahulu — baris bermasalah tidak akan diupload.")
        st.dataframe(_issue_frame(result.errors), use_container_width=True,
                     hide_index=True)
    if result.warnings:
        with st.expander(f"⚠️ {len(result.warnings)} peringatan "
                         "(tidak memblokir upload)"):
            st.dataframe(_issue_frame(result.warnings),
                         use_container_width=True, hide_index=True)
    if result.errors:
        st.download_button("⬇ Download laporan error (.xlsx)",
                           data=_error_report_bytes(result),
                           file_name="laporan_error.xlsx",
                           mime="application/vnd.openxmlformats-officedocument"
                                ".spreadsheetml.sheet", key=f"err_{kind}")

    _render_preview(result, kind)

    if result.decision == "reject":
        st.error(f"✗ {result.message}")
        return

    st.divider()
    st.markdown(f"**{result.message}**")
    confirmed = st.checkbox(
        "Saya sudah memeriksa ringkasan di atas dan setuju melanjutkan upload.",
        key=f"confirm_{kind}")
    if not st.button("2️⃣ Konfirmasi & Upload", type="primary",
                     disabled=not confirmed, key=f"go_{kind}"):
        return

    expected = (parsers.UPLOAD_NOO if kind == SECTION_NOO
                else parsers.UPLOAD_SKU)
    _, _, client = _clients()
    try:
        write_result = writer.append_rows(
            client, writer.pool_tab_for(expected), result.eligible_rows,
            headers=writer.pool_headers_for(expected), settings=_settings(),
            upload_id=result.upload_id)
    except writer.LayoutMismatch as exc:
        st.error(f"✗ {exc}")
        return
    except Exception as exc:
        st.error("✗ Upload gagal saat menulis ke tracker. Tidak ada data "
                 "sebagian yang tersimpan. Silakan coba lagi atau hubungi "
                 "BD Support.")
        st.caption(f"Referensi teknis: {type(exc).__name__}")
        return

    _render_result(result, write_result, dist, kind)
    if not write_result.dry_run:
        st.session_state.pop(state_key, None)


# ─── Sections ─────────────────────────────────────────────────────────────────
def render_section(kind, dist):
    is_noo = kind == SECTION_NOO
    st.subheader(kind)

    with st.expander("📘 Panduan Penginputan", expanded=False):
        st.markdown(guideline.as_markdown())
        resolution = _resolver().resolve(dist["distributor_code"])
        if resolution.resolved:
            codes = ", ".join(f"`{c}`"
                              for c in resolution.all_customer_codes().values())
            st.markdown(f"**Customer Code yang berlaku untuk Anda:** {codes}")
        else:
            st.warning("Singkatan distributor Anda belum terdaftar di master. "
                       "Hubungi BD Support sebelum upload SKU.")
        pdf = _guideline_pdf()
        if pdf:
            st.download_button("⬇ Download Panduan PDF", data=pdf,
                               file_name="Panduan_NOO_SKU_Mapping.pdf",
                               mime="application/pdf", key=f"pdf_{kind}")

    file_id = (config.NOO_TEMPLATE_FILE_ID if is_noo
               else config.SKU_TEMPLATE_FILE_ID)
    name = ("NOO_MAPPING_TEMPLATE.xlsx" if is_noo
            else "SKU_MAPPING_TEMPLATE.xlsx")
    try:
        st.download_button(f"⬇ Download Template {'NOO' if is_noo else 'SKU'}",
                           data=_template_bytes(file_id), file_name=name,
                           mime="application/vnd.openxmlformats-officedocument"
                                ".spreadsheetml.sheet", key=f"tpl_{kind}")
    except Exception:
        st.warning("Template belum bisa diunduh saat ini. Hubungi BD Support.")

    st.divider()
    _handle_section(kind, dist)


def render_app():
    dist = st.session_state["distributor"]
    settings = _settings()

    with st.sidebar:
        st.markdown(f"### {dist['distributor_name']}")
        st.caption(f"Kode Distributor: **{dist['distributor_code']}**")
        if dist.get("region"):
            st.caption(f"Region: {dist['region']}")
        st.divider()
        section = st.radio("Menu", [SECTION_NOO, SECTION_SKU],
                           label_visibility="collapsed")
        st.divider()
        st.caption(f"Mode: `{settings.app_env}`"
                   + ("  ·  DRY RUN" if settings.dry_run else "  ·  WRITE"))
        if st.button("Keluar", use_container_width=True):
            st.session_state.clear()
            st.rerun()

    st.title("📦 NOO & SKU Mapping")
    st.caption(f"Selamat datang, **{dist['distributor_name']}** "
               f"({dist['distributor_code']})")
    if settings.dry_run:
        st.info("Mode **DRY RUN** aktif — validasi dan enrichment berjalan "
                "penuh, tetapi tidak ada data yang ditulis ke tracker.",
                icon="🧪")
    st.divider()
    render_section(section, dist)


if st.session_state.get("auth"):
    render_app()
else:
    render_login()
