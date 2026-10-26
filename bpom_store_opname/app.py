"""BPOM Stock Opname Toko.

Pengisi memilih company dan toko (daftar dari BD), men-download template yang sudah terisi SKU + nomor BPOM
resmi, mengisi QTY, lalu meng-upload-nya. Hasil disimpan ke satu Google Spreadsheet utama.
Tidak ada mode revisi: data yang sudah terkirim tidak bisa dikirim ulang dari form ini.

Jalankan:  streamlit run app.py
"""
from __future__ import annotations

import hashlib
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path

import gspread
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

import master_product
import reference_data
import sheets_store as store
import template_builder
import validation as val

log = logging.getLogger("bpom_opname")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
BQ_SCOPE = ["https://www.googleapis.com/auth/bigquery"]
PRODUCTS_RETRY_SECONDS = 60   # setelah BigQuery gagal, jangan coba lagi di setiap rerun
STORE_SEARCH_THRESHOLD = 1500   # company dengan store lebih banyak: pakai kotak cari (dropdown ribuan opsi berat dikirim ke HP)
STORE_SEARCH_MIN_CHARS = 3
STORE_SEARCH_RESULTS = 50
MAX_ISSUES_LISTED = 10
MAX_ISSUES_SHOWN = 300

APP_DIR = Path(__file__).parent
DISTRIBUTOR_FILE = APP_DIR / "reference" / "Daftar Distributor.xlsx"
STORE_FILE = APP_DIR / "reference" / "Daftar Store.xlsx"   # dari BD: Distributor Name, Store ID, Store Name
TEMPLATE_FILENAME = "Contoh Excel Response.xlsx"
TEMPLATE_CANDIDATES = (
    APP_DIR / "template" / TEMPLATE_FILENAME,  # salinan yang ikut ter-deploy
    Path(r"D:\Workspace\Company\Skintific\Data\BPOM Audit") / TEMPLATE_FILENAME,
)
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

# CSS global. Tombol download merah + tebal; di layar sempit (HP) tombol penuh lebar dan cukup besar untuk disentuh.
APP_CSS = """
<style>
div[data-testid="stDownloadButton"] button {
    background-color: #D32F2F !important;
    border: 2px solid #B71C1C !important;
    color: #FFFFFF !important;
    padding: 0.6rem 1.6rem !important;
}
div[data-testid="stDownloadButton"] button:hover { background-color: #B71C1C !important; }
div[data-testid="stDownloadButton"] button * { color: #FFFFFF !important; font-weight: 700 !important; }

@media (max-width: 640px) {
    div[data-testid="stMainBlockContainer"], .block-container { padding: 1rem 0.8rem 4rem 0.8rem !important; }
    h1 { font-size: 1.55rem !important; line-height: 1.25 !important; }
    h3 { font-size: 1.15rem !important; }
    div[data-testid="stElementContainer"]:has([data-testid="stDownloadButton"]),
    div[data-testid="stElementContainer"]:has([data-testid="stButton"]),
    div[data-testid="stElementContainer"]:has([data-testid="stFormSubmitButton"]),
    div[data-testid="stDownloadButton"], div[data-testid="stButton"], div[data-testid="stFormSubmitButton"] { width: 100% !important; }
    div[data-testid="stDownloadButton"] button, div[data-testid="stButton"] button,
    div[data-testid="stFormSubmitButton"] button {
        width: 100% !important; min-height: 3rem !important; font-size: 1.05rem !important;
    }
}
</style>
"""

GUIDE_RULES = """
**Pelaksanaan stock opname**
Toko wajib melakukan stock opname fisik terhadap produk yang terdaftar, dengan pemisahan data untuk setiap SKU dan
masing-masing nomor BPOM sesuai dengan data yang telah ditetapkan oleh prinsipal.

**Periode pelaksanaan dan pengisian form**
Stock opname dilakukan dengan menggunakan posisi stok fisik per tanggal yang ditentukan, dan hasilnya wajib diinput
ke dalam form yang disediakan oleh prinsipal pada periode **26 Oktober – 8 November 2026**.

**Kesesuaian SKU dan nomor BPOM**
Toko wajib memastikan bahwa setiap SKU dicatat berdasarkan nomor BPOM yang sesuai. Produk dengan SKU yang sama
tetapi memiliki nomor BPOM berbeda harus dicatat secara terpisah dan tidak boleh digabungkan.

**Kelengkapan dan keakuratan data**
Toko bertanggung jawab memastikan bahwa seluruh data yang diinput telah diperiksa, lengkap, akurat, dan sesuai
dengan kondisi persediaan fisik pada saat stock opname dilakukan.

**Batas waktu dan konfirmasi penyelesaian**
Seluruh pengisian form wajib diselesaikan paling lambat tanggal **8 November 2026**. Toko diharapkan memastikan
bahwa seluruh SKU dan nomor BPOM yang menjadi tanggung jawabnya telah dilaporkan sebelum batas waktu tersebut.

Apabila terdapat nomor BPOM tambahan, Toko diperbolehkan menambahkan baris baru pada form, dengan SKU tetap
mengacu pada SKU yang telah ditetapkan oleh prinsipal.

**Ketentuan Pengisian**

1. Pilih **Distributor Company** dan **Toko** (jika toko adalah grup, maka pilih nama grup), lalu isi **tanggal stock opname** dan **nama PIC** Stock Opname
2. **Download template** (kolom Distributor Branch akan terisi otomatis), isi kolom **QTY**, jangan mengubah header atau urutan kolom.
3. **Upload** file, tunggu pemeriksaan otomatis, perbaiki jika ada error.
4. Klik **Submit**. Data hanya disimpan jika seluruh isi file valid.

- Toko tidak diperkenankan mengubah header, nama kolom, atau urutan kolom. Baris judul di atas header boleh dibiarkan.
- Satu file = satu toko. Untuk toko lain, pilih toko tersebut lalu download template-nya.
- Kolom **Distributor Branch** sudah terisi otomatis sesuai distributor dari toko yang dipilih; jangan diubah.
- **PRODUCT ID, PRODUCT NAME, ISI (GR / ML), BPOM NUMBER** sudah terisi di template; jangan dikosongkan.
- Yang perlu Anda isi hanya **QTY** di setiap baris dengan bilangan bulat 0 atau lebih (tidak boleh negatif; tanpa desimal atau huruf).
- Satu kombinasi **PRODUCT ID + BPOM NUMBER** hanya boleh muncul satu kali dalam satu file.
"""
GUIDE_RULES = GUIDE_RULES.replace("**\n", "**  \n")  # judul + paragraf: pindah baris tanpa paragraf baru
GUIDE_EXTRA_BPOM = """
**Menambah SKU / nomor BPOM yang belum ada di template:** paling mudah lewat form **Tambah SKU / nomor BPOM langsung**
di bawah (pilih SKU dari master product, isi nomor BPOM dan QTY). Alternatifnya, tambahkan baris baru di bagian bawah data
Excel dengan **PRODUCT ID** yang sudah ada di template, lalu **PRODUCT NAME** dan **ISI (GR / ML)** (salin dari baris SKU
yang sama), **BPOM NUMBER** baru, dan **QTY**. Baris tambahan ditandai pada pratinjau setelah upload.
"""


# ── Konfigurasi & koneksi ──────────────────────────────────────────────────

@dataclass(frozen=True)
class Config:
    sheet_name: str
    worksheet_title: str
    sheet_id: str | None
    share_with: tuple[str, ...]
    template_path: Path | None
    master_product_table: str = master_product.DEFAULT_TABLE


def service_account_info() -> dict | None:
    """Credential service account dari secrets.

    Model utama: `[connections.bigquery]` (sama dengan aplikasi Streamlit lain di perusahaan).
    `[gcp_service_account]` tetap diterima sebagai alternatif.
    """
    try:
        if "connections" in st.secrets and "bigquery" in st.secrets["connections"]:
            return dict(st.secrets["connections"]["bigquery"])
        if "gcp_service_account" in st.secrets:
            return dict(st.secrets["gcp_service_account"])
    except Exception:  # secrets.toml tidak ada
        pass
    return None


def _sheet_id(value: str | None) -> str | None:
    """Terima ID spreadsheet atau URL lengkap; kembalikan ID-nya."""
    if not value:
        return None
    match = re.search(r"/d/([A-Za-z0-9_-]+)", value)
    return match.group(1) if match else value.strip()


def load_config() -> Config:
    """Baca `.streamlit/secrets.toml`. Berhenti dengan pesan jelas bila belum diatur."""
    if service_account_info() is None:
        st.error("Credential belum diatur. Salin `.streamlit/secrets.toml.example` menjadi "
                 "`.streamlit/secrets.toml`, lalu isi `[connections.bigquery]` (service account) dan `[app]`.")
        st.stop()
    app = st.secrets["app"] if "app" in st.secrets else {}
    configured = app.get("template_path")
    candidates = ((Path(configured),) if configured else ()) + TEMPLATE_CANDIDATES
    return Config(
        sheet_name=app.get("google_sheet_name", "BPOM Stock Opname Store"),
        worksheet_title=app.get("worksheet_name", "BPOM Stock Opname Store"),
        sheet_id=_sheet_id(app.get("google_sheet_id")),
        share_with=tuple(app.get("share_with", [])),
        template_path=next((p for p in candidates if p.is_file()), None),
        master_product_table=app.get("master_product_table", master_product.DEFAULT_TABLE),
    )


@st.cache_resource(show_spinner=False)
def get_gspread_client() -> gspread.Client:
    creds = service_account.Credentials.from_service_account_info(service_account_info(), scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    info = service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=BQ_SCOPE)
    return bigquery.Client(credentials=creds, project=info.get("project_id"))


# cache_resource = objek bersama semua sesi TANPA disalin per rerun (cache_data menyalin/pickle tiap rerun: 45 ms untuk
# 20.000 store). Isi cache ini tidak boleh diubah oleh pemakainya.
@st.cache_resource(ttl=3600, show_spinner=False)
def _fetch_products(table: str) -> tuple[list[master_product.Product], int]:
    return master_product.fetch_products(get_bq_client(), table)


_products_failure: tuple[float, str] | None = None   # per proses: 60 sesi tidak menyerbu BigQuery saat ia gagal


def load_products(table: str) -> tuple[list[master_product.Product], int, str | None]:
    """Daftar SKU dari master_product (cache 1 jam). Gagal -> ([], 0, pesan); hanya fitur tambah SKU yang nonaktif."""
    global _products_failure
    failure = _products_failure
    if failure and time.time() - failure[0] < PRODUCTS_RETRY_SECONDS:
        return [], 0, failure[1]
    try:
        products, skipped = _fetch_products(table)
    except Exception as exc:
        log.exception("Gagal membaca master_product")
        message = f"{type(exc).__name__}: {str(exc)[:300]}"
        _products_failure = (time.time(), message)
        return [], 0, message
    _products_failure = None
    return products, skipped, None


@st.cache_resource(show_spinner=False)
def load_distributors() -> list[tuple[str, str]]:
    return reference_data.load_distributors(DISTRIBUTOR_FILE)


@st.cache_resource(show_spinner=False)
def load_stores_by_company() -> dict[str, list[reference_data.Store]]:
    """{company: [store, ...]} - dibaca dari file BD sekali, lalu di-cache (dropdown tetap ringan)."""
    grouped: dict[str, list[reference_data.Store]] = {}
    for s in reference_data.load_stores(STORE_FILE, load_distributors()):
        grouped.setdefault(s.company, []).append(s)
    return dict(sorted(grouped.items()))


@st.cache_resource(show_spinner=False)
def load_skus(template_path: str) -> list[val.Sku]:
    return reference_data.load_catalogue(Path(template_path))


@st.cache_resource(show_spinner=False)
def template_bytes(template_path: str) -> bytes:
    return Path(template_path).read_bytes()


@st.cache_resource(max_entries=300, show_spinner=False)
def build_template_cached(template_path: str, store_label: str, distributor: str) -> bytes:
    """Template per store, dibangun sekali lalu di-cache (bukan di setiap rerun). Data dikirim langsung (bukan callable
    deferred): callable gagal bila halaman rerun tepat saat tombol ditekan ('Deferred file not found')."""
    return template_builder.build_template(template_bytes(template_path), store_label, distributor, load_skus(template_path))


@st.cache_resource(show_spinner=False)
def get_opname_store(sheet_id, sheet_name: str, worksheet_title: str, share_with: tuple) -> store.OpnameStore:
    """Satu handle Sheets per proses: spreadsheet, worksheet, dan header diperiksa SEKALI (bukan tiap submit)."""
    sh, created = store.open_spreadsheet(get_gspread_client(), sheet_name, sheet_id=sheet_id, share_with=share_with)
    ws = store.ensure_worksheet(sh, worksheet_title, fresh=created)
    return store.OpnameStore(sh, ws, assume_empty=created)


def parse_upload(data: bytes, distributor: str, skus: list[val.Sku]) -> val.ParseResult:
    """parse_excel sekali per (isi file, distributor): rerun karena widget lain tidak mengulang pembacaan file."""
    key = (hashlib.blake2b(data, digest_size=16).hexdigest(), distributor)
    memo = st.session_state.get("parse_memo")
    if memo and memo[0] == key:
        return memo[1]
    result = val.parse_excel(data, [distributor], skus)
    st.session_state["parse_memo"] = (key, result)
    return result


# ── Komponen UI ────────────────────────────────────────────────────────────

def show_issues(issues: list[val.Issue]) -> None:
    st.error(f"Ditemukan {len(issues)} masalah pada file. Upload dibatalkan: tidak ada data yang disimpan.")
    # Daftar teks (terbaca di layar HP); tabel lengkap ada di expander.
    for issue in issues[:MAX_ISSUES_LISTED]:
        where = f"**Baris {issue.row}** · {issue.column}" if issue.row else f"**{issue.column}**"
        st.markdown(f"- {where}: {issue.message}")
    if len(issues) > MAX_ISSUES_LISTED:
        shown = issues[:MAX_ISSUES_SHOWN]
        with st.expander(f"Lihat tabel lengkap ({len(shown)} dari {len(issues)} masalah)"):
            st.dataframe(
                pd.DataFrame({
                    "Baris Excel": [str(i.row) if i.row else "-" for i in shown],
                    "Kolom": [i.column for i in shown],
                    "Alasan": [i.message for i in shown],
                }),
                hide_index=True,
                width="stretch",
            )


def show_preview(rows: list[val.ExcelRow]) -> None:
    st.dataframe(
        pd.DataFrame({
            "Baris Excel": [str(r.excel_row) if r.excel_row else "Tambahan" for r in rows],
            "Distributor Branch": [r.distributor_name for r in rows],
            "PRODUCT ID": [r.product_id for r in rows],
            "PRODUCT NAME": [r.product_name for r in rows],
            "ISI (GR / ML)": [r.isi for r in rows],
            "BPOM NUMBER": [r.bpom_number for r in rows],
            "QTY": [r.qty for r in rows],
            "BPOM Tambahan": ["Ya" if r.additional else "" for r in rows],
        }),
        hide_index=True,
        width="stretch",
    )


def show_flash() -> None:
    flash = st.session_state.pop("flash", None)
    if flash:
        st.success(flash)


def template_button(cfg: Config, picked: reference_data.Store | None, skus: list[val.Sku]) -> None:
    if cfg.template_path is None:
        st.warning(f"File template '{TEMPLATE_FILENAME}' tidak ditemukan. Letakkan di folder `template/` "
                   "atau atur `template_path` di `[app]`.")
        return
    if not picked:
        st.info("Pilih Distributor Company dan Toko (Langkah 1) agar template bisa di-download.")
        return
    safe = re.sub(r'[\\/:*?"<>|]+', "_", picked.label).strip()
    path = str(cfg.template_path)

    data = build_template_cached(path, picked.label, picked.distributor)

    st.download_button(
        "**DOWNLOAD TEMPLATE**", data=data, on_click="ignore", file_name=f"BPOM Stock Opname - {safe}.xlsx",
        mime=XLSX_MIME,
        help="Template resmi untuk toko ini berisi daftar SKU + nomor BPOM. Isi kolom QTY saja.",
    )


def extras_section(cfg: Config, skus: list[val.Sku]) -> list[val.ExtraSku]:
    """Form tambah SKU + nomor BPOM langsung (SKU dari gt_schema.master_product). Daftar disimpan di session."""
    extras: list[val.ExtraSku] = st.session_state.setdefault("extras", [])
    st.markdown("**Tambah SKU / nomor BPOM langsung (opsional)**")
    st.caption("Untuk SKU + nomor BPOM yang tidak ada di template. Pilih SKU dari master product (format `SKU - nama produk`), "
               "isi nomor BPOM dan QTY, lalu **Tambah ke daftar**. Baris ini ikut tersimpan bersama isi file Excel.")
    flash = st.session_state.pop("extra_flash", None)
    if flash:
        st.success(flash)
    products, skipped, error = load_products(cfg.master_product_table)
    if error:
        st.warning("Daftar SKU dari master product tidak dapat dibaca, jadi fitur ini belum bisa dipakai. Anda masih bisa "
                   f"menambah baris langsung di Excel. Detail: {error}")
    else:
        with st.form("extra_form", clear_on_submit=True):
            product = st.selectbox("SKU", products, index=None, format_func=lambda p: p.label,
                                   placeholder=f"Ketik SKU / nama produk ({len(products)} SKU)")
            bpom = st.text_input("Nomor BPOM", max_chars=val.MAX_TEXT_LEN)
            qty = st.number_input("QTY", min_value=0, step=1, value=None, format="%d",
                                  placeholder="Bilangan bulat 0 atau lebih")
            if st.form_submit_button("Tambah ke daftar"):
                extra, err = val.build_extra(product.sku if product else "", bpom, qty, products, skus)
                if not err and any(val.row_key(e.product_id, e.bpom_number) == val.row_key(extra.product_id, extra.bpom_number)
                                   for e in extras):
                    err = f"{extra.product_id} + {extra.bpom_number} sudah ada di daftar tambahan."
                if err:
                    st.error(err)
                else:
                    extras.append(extra)
                    st.session_state["extra_flash"] = (f"Ditambahkan: {extra.product_id} - {extra.product_name} "
                                                       f"(BPOM {extra.bpom_number}, QTY {extra.qty}).")
                    st.rerun()
    if extras:
        st.markdown(f"**Daftar tambahan ({len(extras)} baris)**")
        for i, e in enumerate(extras):
            st.markdown(f"{i + 1}. **{e.product_id}** - {e.product_name} · ISI {e.isi} · BPOM **{e.bpom_number}** · QTY **{e.qty}**")
            if st.button("Hapus", key=f"del_extra_{i}"):
                extras.pop(i)
                st.rerun()
    return extras


# ── Submit ─────────────────────────────────────────────────────────────────

def save(cfg: Config, meta: store.OpnameMeta, rows: list[val.ExcelRow]) -> store.SaveSummary:
    opname = get_opname_store(cfg.sheet_id, cfg.sheet_name, cfg.worksheet_title, cfg.share_with)
    try:
        return opname.insert_rows(meta, rows)
    except store.DuplicateDataError:
        raise
    except (store.SheetsError, gspread.exceptions.APIError) as exc:
        status = store._status(exc) if isinstance(exc, gspread.exceptions.APIError) else 0
        if status != 429:          # kuota hanya antre; error lain (tab dihapus, dsb.) -> buka ulang handle di percobaan berikutnya
            get_opname_store.clear()
        raise


def handle_submit(cfg: Config, meta: store.OpnameMeta | None, form_errors: list[str],
                  parsed: val.ParseResult | None) -> None:
    if form_errors:
        for message in form_errors:
            st.error(message)
        return
    if parsed is None or not parsed.ok:
        st.error("File belum valid, upload dibatalkan. Perbaiki masalah di atas lalu upload ulang.")
        return

    try:
        with st.spinner("Menyimpan data... (bila banyak yang submit bersamaan, penyimpanan antre dan bisa memakan waktu sampai 1 menit; jangan tutup halaman)"):
            summary = save(cfg, meta, parsed.rows)
    except store.DuplicateDataError as exc:
        st.error(str(exc))
        st.dataframe(
            pd.DataFrame({"Baris Excel": [str(r.excel_row) if r.excel_row else "Tambahan" for r in exc.conflicts],
                          "Distributor Branch": [r.distributor_name for r in exc.conflicts],
                          "PRODUCT ID": [r.product_id for r in exc.conflicts],
                          "BPOM NUMBER": [r.bpom_number for r in exc.conflicts]}),
            hide_index=True, width="stretch")
        return
    except store.SheetsError as exc:
        st.error(str(exc))
        return
    except gspread.exceptions.APIError as exc:
        log.exception("Google Sheets API error")
        st.error(f"Google Sheets menolak permintaan, tidak ada perubahan yang disimpan. Detail: {exc}")
        return
    except Exception:
        log.exception("Unexpected error while saving")
        st.error("Terjadi kesalahan tak terduga saat menyimpan. Hubungi BD sebelum mencoba lagi "
                 "agar tidak terjadi duplikasi.")
        return

    msg = (f"Berhasil menyimpan {summary.inserted} baris stock opname untuk toko "
           f"{meta.store_id} - {meta.store_name}.")
    if summary.additional:
        msg += f" Termasuk {summary.additional} baris BPOM tambahan."
    msg += " Data yang sudah terkirim tidak dapat direvisi dari form ini; hubungi BD bila ada kesalahan."
    st.session_state["flash"] = msg
    st.session_state["extras"] = []
    st.session_state["upload_ver"] += 1   # kosongkan uploader
    st.rerun()


# ── Halaman ────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(page_title="BPOM Stock Opname Toko", layout="wide")
    st.markdown(APP_CSS, unsafe_allow_html=True)
    cfg = load_config()
    st.session_state.setdefault("upload_ver", 0)

    st.title("BPOM Stock Opname Toko")
    if not cfg.sheet_id:
        st.warning("`google_sheet_id` belum terbaca dari Secrets. Pastikan ada di bagian `[app]` "
                   "(`[app]` lalu baris `google_sheet_id = \"...\"`). Tanpa itu, aplikasi mencari spreadsheet "
                   f"berjudul '{cfg.sheet_name}' dan mencoba membuatnya, yang akan gagal.")
    show_flash()

    if cfg.template_path is None:
        st.error(f"File template '{TEMPLATE_FILENAME}' tidak ditemukan. Letakkan di folder `template/` "
                 "atau atur `template_path` di `[app]`.")
        st.stop()
    try:
        stores_by_company = load_stores_by_company()
        skus = load_skus(str(cfg.template_path))
    except reference_data.StoreListMissing:
        st.warning("**Daftar store belum tersedia.** Form belum bisa dipakai; hubungi BD untuk menyediakan "
                   "daftar store (kolom Distributor Name, Store ID, Store Name).")
        st.stop()
    except reference_data.ReferenceDataError as exc:
        st.error(f"Data referensi dari BD bermasalah: {exc}")
        st.stop()

    with st.expander("Ketentuan dan panduan pengisian", expanded=True):
        st.markdown(GUIDE_RULES)

    # Langkah 1 - distributor company -> store (daftar dari BD)
    st.subheader("Langkah 1 - Pilih Distributor Company dan Toko")
    st.caption("Pilih **Distributor Company**, lalu **Toko** (jika toko adalah grup, pilih nama grup) yang melakukan stock opname. Pilihan berasal dari daftar "
               "yang disediakan BD. Klik kolom lalu ketik ID atau nama toko untuk mencari.")
    company = st.selectbox("Distributor Company", list(stores_by_company), index=None,
                           placeholder="Pilih atau ketik untuk mencari")
    company_stores = stores_by_company.get(company, []) if company else []
    options, placeholder, searching = company_stores, "Pilih Distributor Company dulu", False
    if company and len(company_stores) > STORE_SEARCH_THRESHOLD:
        # Company dengan ribuan store: jangan kirim semua opsi ke browser di setiap rerun; cari dulu, tampilkan hasil teratas.
        searching = True
        query = st.text_input("Cari Toko", key=f"q_{company}", max_chars=60,
                              placeholder=f"Ketik ID atau nama toko (min. {STORE_SEARCH_MIN_CHARS} huruf) dari {len(company_stores)} toko"
                              ).strip().lower()
        options = ([s for s in company_stores if query in s.search_text][:STORE_SEARCH_RESULTS]
                   if len(query) >= STORE_SEARCH_MIN_CHARS else [])
        placeholder = (f"Ketik minimal {STORE_SEARCH_MIN_CHARS} huruf di kotak Cari Toko" if len(query) < STORE_SEARCH_MIN_CHARS
                       else f"{len(options)} hasil" + (f" (menampilkan {STORE_SEARCH_RESULTS} teratas; persempit pencarian)"
                                                      if len(options) == STORE_SEARCH_RESULTS else "")
                       if options else "Tidak ada toko yang cocok")
    elif company:
        placeholder = f"Ketik ID / nama toko ({len(company_stores)} toko)"
    picked = st.selectbox(
        "Toko", options, index=None, key=f"store_{company}", disabled=not company or (searching and not options),
        format_func=lambda s: f"{s.label}  ·  {s.distributor}", placeholder=placeholder)
    names = [picked.distributor] if picked else []

    # Langkah 2 - tanggal + PIC
    st.subheader("Langkah 2 - Isi Tanggal Stock Opname dan PIC")
    st.caption("**Tanggal Stock Opname** = tanggal perhitungan stock dilakukan. **Nama PIC** = nama pengisi.")
    col_date, col_pic = st.columns([1, 2])
    tanggal = col_date.date_input("Tanggal Stock Opname", value=None, format="DD/MM/YYYY")
    pic = col_pic.text_input("Nama PIC", max_chars=100)
    if tanggal:
        st.caption(f"Periode otomatis dari tanggal: **{val.periode_label(tanggal)}**")

    # Langkah 3 - template
    st.subheader("Langkah 3 - Download template dan isi hanya data QTY")
    st.caption(f"Template berisi {len(skus)} kombinasi SKU + nomor BPOM untuk satu toko. Isi hanya kolom "
               "**QTY** (bilangan bulat 0 atau lebih; tidak boleh negatif). Jangan mengubah header atau urutan kolom.")
    st.markdown(GUIDE_EXTRA_BPOM)
    template_button(cfg, picked, skus)
    extras = extras_section(cfg, skus)

    # Langkah 4 - upload
    st.subheader("Langkah 4 - Upload file Excel")
    st.caption("Upload file `.xlsx` yang sudah diisi. File diperiksa otomatis. Jika ada satu saja baris yang salah, "
               "seluruh upload dibatalkan dan tidak ada data yang masuk, sehingga perbaiki file lalu upload ulang.")
    uploaded = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"], key=f"upload_{st.session_state['upload_ver']}")

    parsed = None
    if uploaded is not None:
        if not picked:
            st.info("Pilih Distributor Company dan Toko dulu agar isi file bisa divalidasi.")
        else:
            parsed = val.merge_extras(parse_upload(uploaded.getvalue(), picked.distributor, skus), extras,
                                      picked.distributor, skus)
            for warning in parsed.warnings:
                st.warning(warning)
            if parsed.issues:
                show_issues(parsed.issues)
            else:
                extra = sum(r.additional for r in parsed.rows)
                n_extras = len(extras)
                st.success(f"File valid: {len(parsed.rows)} baris siap disimpan"
                           + (f" ({len(parsed.rows) - n_extras} dari file + {n_extras} SKU/BPOM tambahan dari form)"
                              if n_extras else "")
                           + (f", termasuk {extra} baris BPOM tambahan." if extra else "."))
                if parsed.missing:
                    st.warning(f"{len(parsed.missing)} kombinasi SKU + BPOM dari daftar prinsipal tidak ada di file. "
                               "Pastikan seluruh SKU dan nomor BPOM di toko ini sudah dilaporkan.")
                    with st.expander("Lihat yang belum ada di file"):
                        st.dataframe(pd.DataFrame({
                            "Distributor Branch": [n for n, _ in parsed.missing],
                            "PRODUCT ID": [k.product_id for _, k in parsed.missing],
                            "PRODUCT NAME": [k.product_name for _, k in parsed.missing],
                            "BPOM NUMBER": [k.bpom_number for _, k in parsed.missing],
                        }), hide_index=True, width="stretch")
                with st.expander("Pratinjau isi file", expanded=False):
                    show_preview(parsed.rows)

    if extras and uploaded is None:
        st.info(f"{len(extras)} SKU/BPOM tambahan sudah ada di daftar; upload file Excel agar semuanya diperiksa "
                "dan bisa disubmit.")

    # Langkah 5 - submit
    st.subheader("Langkah 5 - Submit")
    st.caption("Pastikan semua isian benar dan file sudah berstatus **valid**, lalu klik Submit satu kali dan tunggu "
               "sampai muncul pesan hasil. Data yang sudah terkirim **tidak dapat direvisi** dari form ini.")
    if st.button("Submit", type="primary"):
        form_errors = val.validate_form(company=company, store=picked, tanggal=tanggal, pic=pic,
                                        has_file=uploaded is not None)
        meta = None
        if not form_errors:
            meta = store.OpnameMeta(company=picked.company, store_id=picked.store_id, store_name=picked.store_name,
                                    tanggal=tanggal.isoformat(), pic=pic.strip())
        handle_submit(cfg, meta, form_errors, parsed)


if __name__ == "__main__":
    main()
