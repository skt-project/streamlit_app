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
from pathlib import Path

import pandas as pd
import streamlit as st

from noo_sku import (auth, config, duplicates, enrichment, guideline,
                     parsers, pipeline, sources, validators, writer)
from noo_sku.customer_code import CustomerCodeResolver
from noo_sku.normalize import norm_key, now_business

st.set_page_config(page_title="NOO & SKU Mapping", page_icon="📦", layout="wide")

SECTION_NOO = "NOO / Store Mapping"
SECTION_SKU = "SKU Mapping"

LOGO_PATH = Path(__file__).parent / "assets" / "skintific_logo.png"

# SKINTIFIC light-blue palette, applied to the landing page only.
BRAND = {
    "primary": "#1BA5D6",
    "dark": "#0E7FA8",
    "tint": "#EAF7FC",
    "border": "#BFE6F4",
    "ink": "#123A4A",
    "muted": "#5B7C８A".replace("８", "8"),
}

LOGIN_CSS = f"""
<style>
  .stApp {{
    background: linear-gradient(160deg, {BRAND['tint']} 0%, #FFFFFF 62%);
  }}
  .brand-card {{
    max-width: 420px; margin: 1.5rem auto 0; padding: 2rem 2rem 1.25rem;
    background: #FFFFFF; border: 1px solid {BRAND['border']};
    border-radius: 16px; box-shadow: 0 8px 28px rgba(27,165,214,.10);
  }}
  .brand-title {{
    text-align: center; font-size: 1.45rem; font-weight: 700;
    color: {BRAND['ink']}; margin: .85rem 0 .15rem;
  }}
  .brand-sub {{
    text-align: center; color: {BRAND['muted']}; font-size: .92rem;
    margin-bottom: 1.35rem; letter-spacing: .02em;
  }}
  .brand-foot {{
    text-align: center; color: {BRAND['muted']}; font-size: .78rem;
    margin-top: 1.1rem;
  }}
  div[data-testid="stForm"] {{ border: none; padding: 0; }}
  .stButton > button[kind="primary"], .stFormSubmitButton > button {{
    background: {BRAND['primary']}; border: 1px solid {BRAND['primary']};
    color: #FFFFFF; font-weight: 600; border-radius: 9px;
  }}
  .stButton > button[kind="primary"]:hover, .stFormSubmitButton > button:hover {{
    background: {BRAND['dark']}; border-color: {BRAND['dark']}; color: #FFFFFF;
  }}
  .stTextInput input {{ border-radius: 9px; }}
  .stTextInput input:focus {{
    border-color: {BRAND['primary']};
    box-shadow: 0 0 0 2px rgba(27,165,214,.18);
  }}
</style>
"""


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
    """NOO's template only — still BD Support's live Drive-hosted file."""
    creds, _, _ = _clients()
    return sources.download_template(creds, file_id)


@st.cache_data(ttl=86400, show_spinner=False)
def _sku_template_bytes():
    """SKU's template — bundled locally as of the 2026-09-03 MoM, no Drive
    call. `prepare_sku_template` is kept as a defensive no-op: harmless if the
    bundled file never carries a size/gramasi column, still correct if it ever
    does again."""
    return sources.prepare_sku_template(sources.load_local_sku_template())


@st.cache_data(ttl=86400, show_spinner=False)
def _guideline_pdf(kind):
    """PDF for the selected function only — never a combined document."""
    return guideline.build_pdf(kind)


# ─── Login ────────────────────────────────────────────────────────────────────
def _dataset():
    return st.secrets["bigquery"]["dataset"]


def render_login():
    st.markdown(LOGIN_CSS, unsafe_allow_html=True)
    left, mid, right = st.columns([1, 1.35, 1])
    with mid:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), use_container_width=True)
        st.markdown('<div class="brand-title">NOO &amp; SKU Mapping</div>',
                    unsafe_allow_html=True)
        st.markdown('<div class="brand-sub">Distributor Management</div>',
                    unsafe_allow_html=True)

        with st.form("login"):
            code = st.text_input("Kode Distributor",
                                 placeholder="DST123").strip()
            password = st.text_input("Password", type="password",
                                     placeholder="••••••••")
            submitted = st.form_submit_button("Login", type="primary",
                                              use_container_width=True)

        if _settings().dry_run:
            st.caption("Mode DRY RUN — data divalidasi, tidak ditulis ke tracker.")
        st.markdown('<div class="brand-foot">SKINTIFIC · Distributor Portal</div>',
                    unsafe_allow_html=True)

    if not submitted:
        return

    code = norm_key(code)
    if not code:
        st.error("Kode distributor wajib diisi.")
        return
    if not password:
        st.error("Password wajib diisi.")
        return

    try:
        creds, project, _ = _clients()
        account = sources.load_account(creds, project, _dataset(), code)
    except Exception:
        st.error("Tidak bisa memverifikasi login saat ini. Coba lagi atau "
                 "hubungi administrator.")
        return

    # One message for unknown code, inactive account and wrong password, so the
    # form cannot be used to discover which distributor codes exist.
    if account is None or not auth.verify_password(password,
                                                   account["password_hash"]):
        st.error("Kode distributor atau password salah.")
        return

    record = _distributors().get(code)
    if record is None:
        st.error(f"Kode distributor **{code}** tidak ada di "
                 f"{config.TAB_DIST_DATABASE}. Hubungi BD Support.")
        return
    if not record["active"]:
        st.error(f"Distributor **{code}** berstatus "
                 f"{record['status'] or 'tidak aktif'}. Hubungi BD Support.")
        return

    sources.touch_last_login(creds, project, _dataset(), code)
    # The hash never enters session state.
    auth.establish_session(st.session_state, record, {
        "distributor_code": account["distributor_code"],
        "distributor_name": account["distributor_name"],
        "must_change_password": account["must_change_password"],
    })
    st.rerun()


def render_change_password(dist):
    """Self-service password change. Verifies the current password server-side."""
    st.subheader("Ubah Password")
    if st.session_state.get("must_change_password"):
        st.warning("Anda masih memakai password default. Demi keamanan, "
                   "silakan ganti password Anda sekarang.", icon="🔑")

    with st.form("change_password"):
        current = st.text_input("Password Saat Ini", type="password")
        new = st.text_input("Password Baru", type="password")
        confirm = st.text_input("Konfirmasi Password Baru", type="password")
        submitted = st.form_submit_button("Simpan Password Baru",
                                          type="primary")
    if not submitted:
        return

    code = auth.session_distributor_code(st.session_state)
    try:
        creds, project, _ = _clients()
        account = sources.load_account(creds, project, _dataset(), code)
    except Exception:
        st.error("Tidak bisa memproses permintaan saat ini. Coba lagi.")
        return
    if account is None:
        st.error("Akun tidak ditemukan atau tidak aktif.")
        return

    check = auth.validate_new_password(current, new, confirm,
                                       account["password_hash"])
    if not check.ok:
        st.error(check.message)
        return

    try:
        sources.set_password(creds, project, _dataset(), code,
                             auth.hash_password(new))
    except Exception:
        st.error("Gagal menyimpan password baru. Coba lagi atau hubungi "
                 "administrator.")
        return

    st.session_state["must_change_password"] = False
    st.success("Password berhasil diubah. Gunakan password baru pada login "
               "berikutnya.")


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

    distributors = _distributors()
    allowed = auth.authorized_branches(distributors, dist_code)
    company = (distributors.get(dist_code) or {}).get("company", "")

    try:
        if expected == parsers.UPLOAD_NOO:
            by_cust, by_ref = _store_basis(dist_code)
            return pipeline.run_noo(
                parsed, distributor=dist, resolver=resolver,
                dist_enricher=dist_enricher,
                store_enricher=enrichment.StoreEnricher(by_cust, by_ref),
                # Ledger spans every authorised branch so a duplicate filed
                # earlier under a sibling branch is still caught.
                ledger=sources.load_noo_ledger(client, set(allowed)),
                known_cities=_cities(), when=now_business(),
                allowed_branches=allowed, company_name=company)

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
            product_lookup=_products(), when=now_business(),
            company_name=company)
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

    kind_key = guideline.UPLOAD_NOO if is_noo else guideline.UPLOAD_SKU
    label = ("📘 Panduan Penginputan — NOO / Store Mapping" if is_noo
             else "📘 Panduan Penginputan — SKU Mapping")
    with st.expander(label, expanded=False):
        st.caption("Panduan di bawah ini hanya berisi ketentuan untuk "
                   f"**{kind}**.")
        st.markdown(guideline.as_markdown(kind_key))
        resolution = _resolver().resolve(dist["distributor_code"])
        if resolution.resolved:
            codes = ", ".join(f"`{c}`"
                              for c in resolution.all_customer_codes().values())
            st.markdown(f"**Customer Code yang berlaku untuk Anda:** {codes}")
        else:
            st.warning("Singkatan distributor Anda belum terdaftar di master. "
                       "Hubungi BD Support sebelum upload SKU.")
        pdf = _guideline_pdf(kind_key)
        if pdf:
            st.download_button(
                f"⬇ Download Panduan PDF ({'NOO' if is_noo else 'SKU'})",
                data=pdf,
                file_name=f"Panduan_{'NOO_Mapping' if is_noo else 'SKU_Mapping'}.pdf",
                mime="application/pdf", key=f"pdf_{kind}")

    name = ("NOO_MAPPING_TEMPLATE.xlsx" if is_noo
            else "SKU_MAPPING_TEMPLATE_2.0.xlsx")
    try:
        template_bytes = (_template_bytes(config.NOO_TEMPLATE_FILE_ID) if is_noo
                          else _sku_template_bytes())
        st.download_button(f"⬇ Download Template {'NOO' if is_noo else 'SKU'}",
                           data=template_bytes, file_name=name,
                           mime="application/vnd.openxmlformats-officedocument"
                                ".spreadsheetml.sheet", key=f"tpl_{kind}")
    except Exception:
        st.warning("Template belum bisa diunduh saat ini. Hubungi BD Support.")

    st.divider()
    _handle_section(kind, dist)


MENU_ACCOUNT = "Akun & Password"


def render_app():
    # Defence in depth: render_app is only reached through the guard below, but
    # it re-checks so a stray rerun can never paint the app unauthenticated.
    if not auth.is_authenticated(st.session_state):
        auth.clear_session(st.session_state)
        render_login()
        return

    dist = st.session_state["distributor"]
    settings = _settings()

    with st.sidebar:
        st.markdown(f"### {dist['distributor_name']}")
        st.caption(f"Kode Distributor: **{dist['distributor_code']}**")
        if dist.get("company"):
            branches = auth.authorized_branches(_distributors(),
                                                dist["distributor_code"])
            st.caption(f"Perusahaan: {dist['company']}")
            if len(branches) > 1:
                st.caption(f"Cabang yang dapat Anda input: **{len(branches)}**")
        if dist.get("region"):
            st.caption(f"Region: {dist['region']}")
        st.divider()
        section = st.radio("Menu", [SECTION_NOO, SECTION_SKU, MENU_ACCOUNT],
                           label_visibility="collapsed")
        st.divider()
        st.caption(f"Mode: `{settings.app_env}`"
                   + ("  ·  DRY RUN" if settings.dry_run else "  ·  WRITE"))
        if st.session_state.get("must_change_password"):
            st.warning("Password masih default", icon="🔑")
        if st.button("Keluar", use_container_width=True):
            auth.clear_session(st.session_state)
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
    if section == MENU_ACCOUNT:
        st.markdown(f"**Distributor**\n\n{dist['distributor_name']} "
                    f"({dist['distributor_code']})")
        st.divider()
        render_change_password(dist)
    else:
        render_section(section, dist)


if auth.is_authenticated(st.session_state):
    render_app()
else:
    auth.clear_session(st.session_state)
    render_login()
