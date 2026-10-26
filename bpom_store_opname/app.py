"""BPOM Stock Opname Toko.

Pengisi memilih region dan toko (daftar dari BD), men-download template yang sudah terisi SKU + nomor BPOM
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
from google.oauth2 import service_account

import master_product
import reference_data
import sheets_store as store
import template_builder
import validation as val

log = logging.getLogger("bpom_opname")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]   # hanya Sheets: tanpa Drive (tidak ada cari/buat/share file)
STORE_SEARCH_THRESHOLD = 1500   # region dengan toko lebih banyak: pakai kotak cari (dropdown ribuan opsi berat dikirim ke HP)
STORE_SEARCH_MIN_CHARS = 3
STORE_SEARCH_RESULTS = 50
BQ_SCOPE = ["https://www.googleapis.com/auth/bigquery"]
PRODUCTS_RETRY_SECONDS = 60     # setelah BigQuery gagal, 60 detik tidak dicoba lagi (60+ sesi tidak menyerbunya)
STORE_SHEET_TTL = 600           # detik: perubahan di Google Sheet daftar toko muncul di aplikasi paling lambat segini
MAX_ISSUES_LISTED = 10
MAX_ISSUES_SHOWN = 300

APP_DIR = Path(__file__).parent
SKU_LIST_FILE = APP_DIR / "reference" / "Daftar SKU.xlsx"   # dari BD: SKU, Product Name, Pack Size (form tambah SKU langsung)
STORE_FILE = APP_DIR / "reference" / "Daftar Store.xlsx"   # dari BD: Region, Nama Group Toko (opsional), Nama Toko
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

1. Pilih **Region** dan **Toko** (jika toko adalah grup, maka pilih nama grup), lalu isi **tanggal stock opname** dan **nama PIC** Stock Opname
2. **Download template** (kolom Nama Toko (By Group) akan terisi otomatis), isi kolom **QTY**, jangan mengubah header atau urutan kolom.
3. **Upload** file, tunggu pemeriksaan otomatis, perbaiki jika ada error.
4. Klik **Submit**. Data hanya disimpan jika seluruh isi file valid.

- Toko tidak diperkenankan mengubah header, nama kolom, atau urutan kolom. Baris judul di atas header boleh dibiarkan.
- Satu file = satu toko. Untuk toko lain, pilih toko tersebut lalu download template-nya.
- Kolom **Nama Toko (By Group)** sudah terisi otomatis sesuai toko yang dipilih; jangan diubah.
- **PRODUCT ID, PRODUCT NAME, ISI (GR / ML), BPOM NUMBER** sudah terisi di template; jangan dikosongkan.
- Yang perlu Anda isi hanya **QTY** di setiap baris dengan bilangan bulat 0 atau lebih (tidak boleh negatif; tanpa desimal atau huruf).
- Satu kombinasi **PRODUCT ID + BPOM NUMBER** hanya boleh muncul satu kali dalam satu file.
"""
GUIDE_RULES = GUIDE_RULES.replace("**\n", "**  \n")  # judul + paragraf: pindah baris tanpa paragraf baru
GUIDE_EXTRA_BPOM = """
**Menambah SKU / nomor BPOM yang belum ada di template:** paling mudah lewat form **Tambah SKU / nomor BPOM langsung**
di bawah (pilih SKU dari daftar SKU, isi nomor BPOM dan QTY). Alternatifnya, tambahkan baris baru di bagian bawah data
Excel dengan **PRODUCT ID** yang sudah ada di template, lalu **PRODUCT NAME** dan **ISI (GR / ML)** (salin dari baris SKU
yang sama), **BPOM NUMBER** baru, dan **QTY**. Baris tambahan ditandai pada pratinjau setelah upload.
"""


# ── Konfigurasi & koneksi ──────────────────────────────────────────────────

@dataclass(frozen=True)
class Config:
    worksheet_title: str
    sheet_id: str | None
    template_path: Path | None
    # Daftar toko dari Google Sheet (opsional; bila kosong dipakai reference/Daftar Store.xlsx)
    store_sheet_id: str | None = None
    store_sheet_gid: int | None = None
    store_filter_value: str = "BA"              # hanya baris dengan nilai ini di kolom filter ("" = tanpa filter)
    store_filter_header: str = "BA Non BA"      # nama header kolom filter; bila tidak ada dipakai store_filter_column
    store_filter_column: str = "H"
    # Daftar SKU untuk form tambah SKU langsung: BigQuery master_product (brand terbatas), cadangan reference/Daftar SKU.xlsx
    master_product_table: str = master_product.DEFAULT_TABLE
    master_product_brands: tuple[str, ...] = master_product.DEFAULT_BRANDS


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


def _sheet_gid(value: str | None) -> int | None:
    """gid tab dari URL spreadsheet (`...edit?gid=123#gid=123`); None bila tidak ada."""
    match = re.search(r"[#&?]gid=(\d+)", value or "")
    return int(match.group(1)) if match else None


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
        worksheet_title=app.get("worksheet_name", "BPOM Stock Opname Store"),
        sheet_id=_sheet_id(app.get("google_sheet_id")),
        template_path=next((p for p in candidates if p.is_file()), None),
        store_sheet_id=_sheet_id(app.get("store_sheet_id")),
        store_sheet_gid=int(app["store_sheet_gid"]) if app.get("store_sheet_gid") not in (None, "") else _sheet_gid(app.get("store_sheet_id")),
        store_filter_value=str(app.get("store_filter_value", "BA")).strip(),
        store_filter_header=str(app.get("store_filter_header", "BA Non BA")).strip(),
        store_filter_column=str(app.get("store_filter_column", "H")).strip(),
        master_product_table=str(app.get("master_product_table", master_product.DEFAULT_TABLE)).strip(),
        master_product_brands=tuple(str(b).strip() for b in app.get("master_product_brands", master_product.DEFAULT_BRANDS)),
    )


@st.cache_resource(show_spinner=False)
def get_gspread_client() -> gspread.Client:
    creds = service_account.Credentials.from_service_account_info(service_account_info(), scopes=SCOPES)
    return gspread.authorize(creds)


# cache_resource = objek bersama semua sesi TANPA disalin per rerun (cache_data menyalin/pickle tiap rerun).
# Isi cache ini tidak boleh diubah oleh pemakainya.
@st.cache_resource(show_spinner=False)
def get_bq_client():
    from google.cloud import bigquery     # impor berat: hanya saat daftar SKU pertama kali dibaca
    info = service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=BQ_SCOPE)
    return bigquery.Client(credentials=creds, project=info.get("project_id"))


@st.cache_resource(ttl=3600, show_spinner=False)
def _fetch_products_bq(table: str, brands: tuple[str, ...]) -> tuple[list[master_product.Product], int]:
    return master_product.fetch_products(get_bq_client(), table, brands)


@st.cache_resource(show_spinner=False)
def _read_products_file() -> tuple[list[master_product.Product], int]:
    return master_product.load_products(SKU_LIST_FILE)


_bq_failure: tuple[float, str] | None = None     # per proses: sesi lain tidak ikut menyerbu BigQuery saat ia gagal


def load_products(cfg: Config) -> tuple[list[master_product.Product], int, str | None]:
    """Daftar SKU: BigQuery master_product (brand terbatas, cache 1 jam); bila gagal, file reference/Daftar SKU.xlsx.

    Gagal semua -> ([], 0, pesan); hanya fitur tambah SKU yang nonaktif, bagian lain tetap jalan.
    """
    global _bq_failure
    failure = _bq_failure
    bq_error = None
    if failure and time.time() - failure[0] < PRODUCTS_RETRY_SECONDS:
        bq_error = failure[1]
    else:
        try:
            products, skipped = _fetch_products_bq(cfg.master_product_table, cfg.master_product_brands)
            _bq_failure = None
            return products, skipped, None
        except Exception as exc:                  # izin, jaringan, tabel hilang, dsb.
            log.exception("Gagal membaca master_product dari BigQuery")
            bq_error = f"{type(exc).__name__}: {str(exc)[:200]}"
            _bq_failure = (time.time(), bq_error)
    try:
        products, skipped = _read_products_file()
        return products, skipped, None
    except (master_product.SkuListMissing, master_product.SkuListError):
        email = (service_account_info() or {}).get("client_email", "service account")
        return [], 0, (f"BigQuery master_product tidak terbaca ({bq_error}). Beri `{email}` peran BigQuery Data Viewer pada tabel "
                       "itu dan BigQuery Job User pada project, atau letakkan reference/Daftar SKU.xlsx.")


def _group_by_region(tokos) -> dict[str, list[reference_data.Toko]]:
    grouped: dict[str, list[reference_data.Toko]] = {}
    for t in tokos:
        grouped.setdefault(t.region, []).append(t)
    return {r: sorted(v, key=lambda t: t.name.casefold()) for r, v in sorted(grouped.items())}


@st.cache_resource(show_spinner=False)
def load_tokos_by_region() -> dict[str, list[reference_data.Toko]]:
    """{region: [toko, ...]} dari file BD (reference/Daftar Store.xlsx), sekali lalu di-cache (dropdown tetap ringan)."""
    return _group_by_region(reference_data.load_stores(STORE_FILE))


@st.cache_resource(ttl=STORE_SHEET_TTL, show_spinner=False)
def _tokos_from_sheet(sheet_id: str, gid: int | None, filter_header: str, filter_column: str,
                      filter_value: str) -> dict[str, list[reference_data.Toko]]:
    """Daftar toko dari Google Sheet: SATU pembacaan per proses tiap STORE_SHEET_TTL detik (bukan per sesi/rerun)."""
    gc = get_gspread_client()
    read = dict(retry_on=store._READ_RETRY, delays=store._BACKOFF)
    sh = store._retry(lambda: gc.open_by_key(sheet_id), **read)
    ws = sh.get_worksheet_by_id(gid) if gid is not None else sh.sheet1
    values = store._retry(lambda: ws.get_all_values(), **read)
    row_filter = (reference_data.RowFilter(equals=filter_value, header_names=(filter_header,) if filter_header else (),
                                           column_letter=filter_column) if filter_value else None)
    return _group_by_region(reference_data.load_stores_from_values(values, "Google Sheet daftar toko", row_filter=row_filter))


def load_tokos(cfg: Config) -> dict[str, list[reference_data.Toko]]:
    if cfg.store_sheet_id:
        return _tokos_from_sheet(cfg.store_sheet_id, cfg.store_sheet_gid, cfg.store_filter_header,
                                 cfg.store_filter_column, cfg.store_filter_value)
    return load_tokos_by_region()


@st.cache_resource(show_spinner=False)
def load_skus(template_path: str) -> list[val.Sku]:
    return reference_data.load_catalogue(Path(template_path))


@st.cache_resource(show_spinner=False)
def template_bytes(template_path: str) -> bytes:
    return Path(template_path).read_bytes()


@st.cache_resource(max_entries=300, show_spinner=False)
def build_template_cached(template_path: str, region: str, toko: str) -> bytes:
    """Template per store, dibangun sekali lalu di-cache (bukan di setiap rerun). Data dikirim langsung (bukan callable
    deferred): callable gagal bila halaman rerun tepat saat tombol ditekan ('Deferred file not found')."""
    return template_builder.build_template(template_bytes(template_path), region, toko, load_skus(template_path))


@st.cache_resource(show_spinner=False)
def get_opname_store(sheet_id, worksheet_title: str) -> store.OpnameStore:
    """Satu handle Sheets per proses: spreadsheet, worksheet, dan header diperiksa SEKALI (bukan tiap submit)."""
    sh = store.open_spreadsheet(get_gspread_client(), sheet_id)
    ws = store.ensure_worksheet(sh, worksheet_title)
    return store.OpnameStore(sh, ws)


def parse_upload(data: bytes, toko: str, skus: list[val.Sku]) -> val.ParseResult:
    """parse_excel sekali per (isi file, toko): rerun karena widget lain tidak mengulang pembacaan file."""
    key = (hashlib.blake2b(data, digest_size=16).hexdigest(), toko)
    memo = st.session_state.get("parse_memo")
    if memo and memo[0] == key:
        return memo[1]
    result = val.parse_excel(data, [toko], skus)
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
            "Nama Toko (By Group)": [r.toko_name for r in rows],
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


def template_button(cfg: Config, picked: reference_data.Toko | None, skus: list[val.Sku]) -> None:
    if cfg.template_path is None:
        st.warning(f"File template '{TEMPLATE_FILENAME}' tidak ditemukan. Letakkan di folder `template/` "
                   "atau atur `template_path` di `[app]`.")
        return
    if not picked:
        st.info("Pilih Region dan Toko (Langkah 1) agar template bisa di-download.")
        return
    safe = re.sub(r'[\\/:*?"<>|]+', "_", f"{picked.region} - {picked.name}").strip()
    path = str(cfg.template_path)

    data = build_template_cached(path, picked.region, picked.name)

    st.download_button(
        "**DOWNLOAD TEMPLATE**", data=data, on_click="ignore", file_name=f"BPOM Stock Opname - {safe}.xlsx",
        mime=XLSX_MIME,
        help="Template resmi untuk toko ini berisi daftar SKU + nomor BPOM. Isi kolom QTY saja.",
    )


def extras_section(cfg: Config, skus: list[val.Sku]) -> list[val.ExtraSku]:
    """Form tambah SKU + nomor BPOM langsung (SKU dari reference/Daftar SKU.xlsx). Daftar disimpan di session."""
    extras: list[val.ExtraSku] = st.session_state.setdefault("extras", [])
    st.markdown("**Tambah SKU / nomor BPOM langsung (opsional)**")
    st.caption("Untuk SKU + nomor BPOM yang tidak ada di template. Pilih SKU dari daftar SKU (format `SKU - nama produk`), "
               "isi nomor BPOM dan QTY, lalu **Tambah ke daftar**. Baris ini ikut tersimpan bersama isi file Excel.")
    flash = st.session_state.pop("extra_flash", None)
    if flash:
        st.success(flash)
    products, skipped, error = load_products(cfg)
    if error:
        st.warning("Daftar SKU tidak dapat dibaca, jadi fitur ini belum bisa dipakai. Anda masih bisa "
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
    opname = get_opname_store(cfg.sheet_id, cfg.worksheet_title)
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
                          "Nama Toko (By Group)": [r.toko_name for r in exc.conflicts],
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

    msg = f"Berhasil menyimpan {summary.inserted} baris stock opname untuk toko {meta.toko} (region {meta.region})."
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
                   "(`[app]` lalu baris `google_sheet_id = \"...\"`). Tanpa itu hasil tidak bisa disimpan.")
    show_flash()

    if cfg.template_path is None:
        st.error(f"File template '{TEMPLATE_FILENAME}' tidak ditemukan. Letakkan di folder `template/` "
                 "atau atur `template_path` di `[app]`.")
        st.stop()
    try:
        tokos_by_region = load_tokos(cfg)
        skus = load_skus(str(cfg.template_path))
    except reference_data.StoreListMissing:
        st.warning("**Daftar toko belum tersedia.** Form belum bisa dipakai; hubungi BD untuk menyediakan "
                   "daftar toko (kolom Region, Nama Group Toko, Nama Toko).")
        st.stop()
    except reference_data.ReferenceDataError as exc:
        st.error(f"Data referensi dari BD bermasalah: {exc}")
        st.stop()
    except (gspread.exceptions.GSpreadException, PermissionError) as exc:
        email = (service_account_info() or {}).get("client_email", "service account")
        log.exception("Gagal membaca daftar toko dari Google Sheet")
        st.error(f"Daftar toko dari Google Sheet tidak bisa dibaca ({type(exc).__name__}). Pastikan sheet itu di-share ke "
                 f"`{email}` (minimal Viewer) dan `store_sheet_id` benar.")
        st.stop()

    with st.expander("Ketentuan dan panduan pengisian", expanded=True):
        st.markdown(GUIDE_RULES)

    # Langkah 1 - region -> toko (daftar dari BD)
    st.subheader("Langkah 1 - Pilih Region dan Toko")
    st.caption("Pilih **Region**, lalu **Toko** (jika toko adalah grup, pilih nama grup) yang melakukan stock opname. "
               "Pilihan berasal dari daftar yang disediakan BD. Klik kolom lalu ketik nama toko untuk mencari.")
    region = st.selectbox("Region", list(tokos_by_region), index=None, placeholder="Pilih atau ketik untuk mencari")
    region_tokos = tokos_by_region.get(region, []) if region else []
    options, placeholder, searching = region_tokos, "Pilih Region dulu", False
    if region and len(region_tokos) > STORE_SEARCH_THRESHOLD:
        # Region dengan ribuan pilihan: jangan kirim semua opsi ke browser di setiap rerun; cari dulu, tampilkan hasil teratas.
        searching = True
        query = st.text_input("Cari Toko", key=f"q_{region}", max_chars=60,
                              placeholder=f"Ketik nama toko (min. {STORE_SEARCH_MIN_CHARS} huruf) dari {len(region_tokos)} pilihan"
                              ).strip().lower()
        options = ([s for s in region_tokos if query in s.search_text][:STORE_SEARCH_RESULTS]
                   if len(query) >= STORE_SEARCH_MIN_CHARS else [])
        placeholder = (f"Ketik minimal {STORE_SEARCH_MIN_CHARS} huruf di kotak Cari Toko" if len(query) < STORE_SEARCH_MIN_CHARS
                       else f"{len(options)} hasil" + (f" (menampilkan {STORE_SEARCH_RESULTS} teratas; persempit pencarian)"
                                                      if len(options) == STORE_SEARCH_RESULTS else "")
                       if options else "Tidak ada toko yang cocok")
    elif region:
        placeholder = f"Ketik nama toko ({len(region_tokos)} pilihan)"
    picked = st.selectbox(
        "Toko", options, index=None, key=f"toko_{region}", disabled=not region or (searching and not options),
        format_func=lambda t: t.label, placeholder=placeholder)

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
            st.info("Pilih Region dan Toko dulu agar isi file bisa divalidasi.")
        else:
            parsed = val.merge_extras(parse_upload(uploaded.getvalue(), picked.name, skus), extras,
                                      picked.name, skus)
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
                            "Nama Toko (By Group)": [n for n, _ in parsed.missing],
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
        form_errors = val.validate_form(region=region, toko=picked, tanggal=tanggal, pic=pic,
                                        has_file=uploaded is not None)
        meta = None
        if not form_errors:
            meta = store.OpnameMeta(region=picked.region, toko=picked.name, tanggal=tanggal.isoformat(), pic=pic.strip())
        handle_submit(cfg, meta, form_errors, parsed)


if __name__ == "__main__":
    main()
