"""BPOM Stock Opname Distributor.

Distributor memilih namanya (daftar dari BD), men-download template yang sudah terisi SKU + nomor BPOM
resmi, mengisi QTY, lalu meng-upload-nya. Hasil disimpan ke satu Google Spreadsheet utama.
Tidak ada mode revisi: data yang sudah terkirim tidak bisa dikirim ulang dari form ini.

Jalankan:  streamlit run app.py
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import gspread
import pandas as pd
import streamlit as st
from google.oauth2 import service_account

import reference_data
import sheets_store as store
import template_builder
import validation as val

log = logging.getLogger("bpom_opname")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
MAX_ISSUES_SHOWN = 300

APP_DIR = Path(__file__).parent
DISTRIBUTOR_FILE = APP_DIR / "reference" / "Daftar Distributor.xlsx"
SKU_FILE = APP_DIR / "reference" / "Data SKU BPOM.xlsx"
TEMPLATE_FILENAME = "Contoh Excel Response.xlsx"
TEMPLATE_CANDIDATES = (
    APP_DIR / "template" / TEMPLATE_FILENAME,  # salinan yang ikut ter-deploy
    Path(r"D:\Workspace\Company\Skintific\Data\BPOM Audit") / TEMPLATE_FILENAME,
)
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

# Tombol download dibuat merah + tebal agar mencolok.
DOWNLOAD_BUTTON_CSS = """
<style>
div[data-testid="stDownloadButton"] button {
    background-color: #D32F2F !important;
    border: 2px solid #B71C1C !important;
    color: #FFFFFF !important;
    padding: 0.6rem 1.6rem !important;
}
div[data-testid="stDownloadButton"] button:hover { background-color: #B71C1C !important; }
div[data-testid="stDownloadButton"] button * { color: #FFFFFF !important; font-weight: 700 !important; }
</style>
"""

GUIDE_RULES = """
**Pelaksanaan stock opname**
Distributor wajib melakukan stock opname fisik terhadap seluruh produk yang terdaftar, dengan pemisahan data untuk
setiap SKU dan masing-masing nomor BPOM sesuai dengan data yang telah ditetapkan oleh prinsipal.

**Periode pelaksanaan dan pengisian form**
Stock opname dilakukan dengan menggunakan posisi stok fisik per tanggal yang ditentukan, dan hasilnya wajib diinput
ke dalam form yang disediakan oleh prinsipal pada periode **23 Oktober – 8 November 2026**.

**Kesesuaian SKU dan nomor BPOM**
Distributor wajib memastikan bahwa setiap SKU dicatat berdasarkan nomor BPOM yang sesuai. Produk dengan SKU yang sama
tetapi memiliki nomor BPOM berbeda harus dicatat secara terpisah dan tidak boleh digabungkan.

**Kelengkapan dan keakuratan data**
Distributor bertanggung jawab memastikan bahwa seluruh data yang diinput telah diperiksa, lengkap, akurat, dan sesuai
dengan kondisi persediaan fisik pada saat stock opname dilakukan.

**Batas waktu dan konfirmasi penyelesaian**
Seluruh pengisian form wajib diselesaikan paling lambat tanggal **8 November 2026**. Distributor diharapkan memastikan
bahwa seluruh SKU dan nomor BPOM yang menjadi tanggung jawabnya telah dilaporkan sebelum batas waktu tersebut.

Apabila terdapat nomor BPOM tambahan, distributor diperbolehkan menambahkan baris baru pada form, dengan SKU tetap
mengacu pada SKU yang telah ditetapkan oleh prinsipal.

**Ketentuan Pengisian**

1. Pilih **distributor company**, **tanggal stock opname**, dan isi **nama PIC** Stock Opname
2. **Download template** (kolom nama distributor company akan terisi otomatis), isi kolom **QTY**, jangan mengubah header atau urutan kolom.
3. **Upload** file, tunggu pemeriksaan otomatis, perbaiki jika ada error.
4. Klik **Submit**. Data hanya disimpan jika seluruh isi file valid.

- Distributor tidak diperkenankan mengubah header, nama kolom, atau urutan kolom. Baris judul di atas header boleh dibiarkan.
- Kolom **Distributor Name (Company - Region)** sudah terisi otomatis untuk seluruh distributor milik company yang dipilih; jangan diubah.
- **PRODUCT ID, PRODUCT NAME, BPOM NUMBER** sudah terisi di template; jangan dikosongkan.
- Yang perlu Anda isi hanya **QTY** di setiap baris dengan bilangan bulat (boleh positif, 0, atau negatif; tanpa desimal atau huruf).
- Satu kombinasi **Distributor Name + PRODUCT ID + BPOM NUMBER** hanya boleh muncul satu kali dalam satu file.
"""
GUIDE_RULES = GUIDE_RULES.replace("**\n", "**  \n")  # judul + paragraf: pindah baris tanpa paragraf baru
GUIDE_EXTRA_BPOM = """
**Menambah nomor BPOM yang belum ada di template:** tambahkan baris baru di bagian bawah data. Isi kolom
*Distributor Name* dengan nama distributor yang memiliki stok tersebut (salin dari baris distributor itu), **PRODUCT ID** dengan SKU yang sudah ada di template, lalu
**PRODUCT NAME**, **BPOM NUMBER** baru, dan **QTY**. Baris tambahan ditandai pada pratinjau setelah upload.
"""


# ── Konfigurasi & koneksi ──────────────────────────────────────────────────

@dataclass(frozen=True)
class Config:
    sheet_name: str
    worksheet_title: str
    sheet_id: str | None
    share_with: tuple[str, ...]
    template_path: Path | None


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
        sheet_name=app.get("google_sheet_name", "BPOM Audit Stock"),
        worksheet_title=app.get("worksheet_name", "BPOM Audit Stock"),
        sheet_id=_sheet_id(app.get("google_sheet_id")),
        share_with=tuple(app.get("share_with", [])),
        template_path=next((p for p in candidates if p.is_file()), None),
    )


@st.cache_resource(show_spinner=False)
def get_gspread_client() -> gspread.Client:
    creds = service_account.Credentials.from_service_account_info(service_account_info(), scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_data(show_spinner=False)
def load_distributors() -> list[tuple[str, str]]:
    return reference_data.load_distributors(DISTRIBUTOR_FILE)


@st.cache_data(show_spinner=False)
def load_skus() -> list[val.Sku]:
    return reference_data.load_skus(SKU_FILE)


# ── Komponen UI ────────────────────────────────────────────────────────────

def show_issues(issues: list[val.Issue]) -> None:
    st.error(f"Ditemukan {len(issues)} masalah pada file. Upload dibatalkan: tidak ada data yang disimpan.")
    shown = issues[:MAX_ISSUES_SHOWN]
    st.dataframe(
        pd.DataFrame({
            "Baris Excel": [str(i.row) if i.row else "-" for i in shown],
            "Kolom": [i.column for i in shown],
            "Alasan": [i.message for i in shown],
        }),
        hide_index=True,
        width="stretch",
    )
    if len(issues) > len(shown):
        st.caption(f"Menampilkan {len(shown)} dari {len(issues)} masalah.")


def show_preview(rows: list[val.ExcelRow]) -> None:
    st.dataframe(
        pd.DataFrame({
            "Baris Excel": [r.excel_row for r in rows],
            "Distributor Name": [r.distributor_name for r in rows],
            "PRODUCT ID": [r.product_id for r in rows],
            "PRODUCT NAME": [r.product_name for r in rows],
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


def template_button(cfg: Config, company: str | None, names: list[str], skus: list[val.Sku]) -> None:
    if cfg.template_path is None:
        st.warning(f"File template '{TEMPLATE_FILENAME}' tidak ditemukan. Letakkan di folder `template/` "
                   "atau atur `template_path` di `[app]`.")
        return
    if not company:
        st.info("Pilih Distributor Company (Langkah 1) agar template bisa di-download "
                "dengan seluruh Distributor Name milik company tersebut.")
        return
    data = template_builder.build_template(cfg.template_path.read_bytes(), company, names, skus)
    safe = re.sub(r'[\\/:*?"<>|]+', "_", company).strip()
    st.markdown(DOWNLOAD_BUTTON_CSS, unsafe_allow_html=True)
    st.download_button(
        "**DOWNLOAD TEMPLATE**", data=data, file_name=f"BPOM Stock Opname - {safe}.xlsx", mime=XLSX_MIME,
        help="Template resmi berisi seluruh Distributor Name milik company Anda beserta daftar SKU + nomor BPOM. Isi kolom QTY saja.",
    )


# ── Submit ─────────────────────────────────────────────────────────────────

def save(cfg: Config, meta: store.OpnameMeta, rows: list[val.ExcelRow]) -> store.SaveSummary:
    sh, created = store.open_spreadsheet(
        get_gspread_client(), cfg.sheet_name, sheet_id=cfg.sheet_id, share_with=cfg.share_with)
    ws = store.ensure_worksheet(sh, cfg.worksheet_title, fresh=created)
    return store.insert_rows(sh, ws, meta, rows, check_duplicates=not created)


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
        with st.spinner("Menyimpan data..."):
            summary = save(cfg, meta, parsed.rows)
    except store.DuplicateDataError as exc:
        st.error(str(exc))
        st.dataframe(
            pd.DataFrame({"Baris Excel": [r.excel_row for r in exc.conflicts],
                          "Distributor Name": [r.distributor_name for r in exc.conflicts],
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

    msg = (f"Berhasil menyimpan {summary.inserted} baris stock opname untuk {meta.company} "
           f"({summary.distributors} distributor).")
    if summary.additional:
        msg += f" Termasuk {summary.additional} baris BPOM tambahan."
    msg += " Data yang sudah terkirim tidak dapat direvisi dari form ini; hubungi BD bila ada kesalahan."
    st.session_state["flash"] = msg
    st.session_state["upload_ver"] += 1   # kosongkan uploader
    st.rerun()


# ── Halaman ────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(page_title="BPOM Stock Opname", layout="wide")
    cfg = load_config()
    st.session_state.setdefault("upload_ver", 0)

    st.title("BPOM Stock Opname Distributor")
    if not cfg.sheet_id:
        st.warning("`google_sheet_id` belum terbaca dari Secrets. Pastikan ada di bagian `[app]` "
                   "(`[app]` lalu baris `google_sheet_id = \"...\"`). Tanpa itu, aplikasi mencari spreadsheet "
                   f"berjudul '{cfg.sheet_name}' dan mencoba membuatnya, yang akan gagal.")
    show_flash()

    try:
        distributors = load_distributors()
        skus = load_skus()
    except reference_data.ReferenceDataError as exc:
        st.error(f"Data referensi dari BD bermasalah: {exc}")
        st.stop()

    with st.expander("Ketentuan dan panduan pengisian", expanded=True):
        st.markdown(GUIDE_RULES)

    # Langkah 1 - distributor company (daftar dari BD)
    st.subheader("Langkah 1 - Pilih Distributor Company")
    st.caption("Pilih **Distributor Company**. Seluruh **Distributor Name** (cabang/region) milik company tersebut akan "
               "muncul di template saat di-download. Pilihan berasal dari daftar distributor yang disediakan BD. "
               "Klik kolom lalu ketik untuk mencari.")
    companies = sorted({c for c, _ in distributors})
    company = st.selectbox("Distributor Company", companies, index=None, placeholder="Pilih atau ketik untuk mencari")
    names = [n for c, n in distributors if c == company] if company else []
    if names:
        st.caption(f"{len(names)} Distributor Name pada company ini akan ada di template.")

    # Langkah 2 - tanggal + PIC
    st.subheader("Langkah 2 - Isi Tanggal Stock Opname dan PIC")
    st.caption("**Tanggal Stock Opname** = tanggal perhitungan stock dilakukan. **Nama PIC** = nama pengisi.")
    col_date, col_pic = st.columns([1, 2])
    tanggal = col_date.date_input("Tanggal Stock Opname", value=None, format="DD/MM/YYYY")
    pic = col_pic.text_input("Nama PIC Distributor", max_chars=100)
    if tanggal:
        st.caption(f"Periode otomatis dari tanggal: **{val.periode_label(tanggal)}**")

    # Langkah 3 - template
    st.subheader("Langkah 3 - Download template dan isi hanya data QTY")
    st.caption(f"Template berisi {len(skus)} kombinasi SKU + nomor BPOM untuk setiap Distributor Name. Isi hanya kolom "
               "**QTY** (bilangan bulat; boleh positif, 0, atau negatif). Jangan mengubah header atau urutan kolom.")
    st.markdown(GUIDE_EXTRA_BPOM)
    template_button(cfg, company, names, skus)

    # Langkah 4 - upload
    st.subheader("Langkah 4 - Upload file Excel")
    st.caption("Upload file `.xlsx` yang sudah diisi. File diperiksa otomatis. Jika ada satu saja baris yang salah, "
               "seluruh upload dibatalkan dan tidak ada data yang masuk, sehingga perbaiki file lalu upload ulang.")
    uploaded = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"], key=f"upload_{st.session_state['upload_ver']}")

    parsed = None
    if uploaded is not None:
        if not company:
            st.info("Pilih Distributor Company dulu agar isi file bisa divalidasi.")
        else:
            parsed = val.parse_excel(uploaded.getvalue(), names, skus)
            for warning in parsed.warnings:
                st.warning(warning)
            if parsed.issues:
                show_issues(parsed.issues)
            else:
                extra = sum(r.additional for r in parsed.rows)
                st.success(f"File valid: {len(parsed.rows)} baris siap disimpan"
                           + (f", termasuk {extra} baris BPOM tambahan." if extra else "."))
                if parsed.missing:
                    st.warning(f"{len(parsed.missing)} kombinasi Distributor Name + SKU + BPOM dari daftar prinsipal "
                               "tidak ada di file. Pastikan seluruh SKU dan nomor BPOM yang menjadi tanggung jawab "
                               "Anda sudah dilaporkan.")
                    with st.expander("Lihat yang belum ada di file"):
                        st.dataframe(pd.DataFrame({
                            "Distributor Name": [n for n, _ in parsed.missing],
                            "PRODUCT ID": [k.product_id for _, k in parsed.missing],
                            "PRODUCT NAME": [k.product_name for _, k in parsed.missing],
                            "BPOM NUMBER": [k.bpom_number for _, k in parsed.missing],
                        }), hide_index=True, width="stretch")
                with st.expander("Pratinjau isi file", expanded=False):
                    show_preview(parsed.rows)

    # Langkah 5 - submit
    st.subheader("Langkah 5 - Submit")
    st.caption("Pastikan semua isian benar dan file sudah berstatus **valid**, lalu klik Submit satu kali dan tunggu "
               "sampai muncul pesan hasil. Data yang sudah terkirim **tidak dapat direvisi** dari form ini.")
    if st.button("Submit", type="primary"):
        form_errors = val.validate_form(company=company, tanggal=tanggal, pic=pic,
                                        has_file=uploaded is not None)
        meta = None
        if not form_errors:
            meta = store.OpnameMeta(company=company, tanggal=tanggal.isoformat(), pic=pic.strip())
        handle_submit(cfg, meta, form_errors, parsed)


if __name__ == "__main__":
    main()
