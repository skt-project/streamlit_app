"""BPOM Audit Stock Distributor.

Distributor mengisi template Excel (PRODUCT ID + BPOM NUMBER + QTY), lalu meng-upload-nya.
Master distributor dibaca dari BigQuery; hasil disimpan ke satu Google Spreadsheet utama.

Jalankan:  streamlit run app.py
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, replace
from pathlib import Path

import gspread
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

import sheets_store as store
import template_builder
import validation as val

log = logging.getLogger("bpom_audit")

SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
BULAN = ["Januari", "Februari", "Maret", "April", "Mei", "Juni", "Juli", "Agustus",
         "September", "Oktober", "November", "Desember"]
# Periode audit yang dibuka saat ini (tahun, bulan). Tambahkan tuple baru untuk membuka periode berikutnya.
OPEN_PERIODS = ((2026, 10),)
MODES = ("Input", "Revisi")
MAX_ISSUES_SHOWN = 300

GUIDE_OVERVIEW = """
1. **Pilih distributor** (company lalu nama distributor), **periode audit**, **tanggal audit**, dan isi **nama PIC**.
2. **Pilih mode**: *Input* untuk pengiriman pertama, *Revisi* untuk memperbaiki data yang sudah terkirim.
3. **Download template** (kolom nama distributor sudah terisi otomatis), isi kolom **QTY**, jangan mengubah header atau urutan kolom.
4. **Upload** file, tunggu pemeriksaan otomatis, perbaiki jika ada error.
5. Klik **Submit**. Data hanya disimpan jika seluruh isi file valid.
"""
GUIDE_FILL = """
Aturan pengisian template:
- **Jangan** mengubah header, nama kolom, atau urutan kolom. Baris judul di atas header boleh dibiarkan.
- Kolom **Distributor Name (Company - Region)** sudah terisi otomatis sesuai distributor yang dipilih; jangan diubah.
- **PRODUCT ID, PRODUCT NAME, BPOM NUMBER** sudah terisi di template; jangan dikosongkan.
- Yang perlu Anda isi hanya **QTY** di setiap baris dengan bilangan bulat (boleh positif, 0, atau negatif; tanpa desimal atau huruf).
- Satu kombinasi **PRODUCT ID + BPOM NUMBER** hanya boleh muncul satu kali dalam satu file.
"""

APP_DIR = Path(__file__).parent
TEMPLATE_FILENAME = "Contoh Excel Response.xlsx"
TEMPLATE_CANDIDATES = (
    APP_DIR / "template" / TEMPLATE_FILENAME,  # salinan yang ikut ter-deploy
    Path(r"D:\Workspace\Company\Skintific\Data\BPOM Audit") / TEMPLATE_FILENAME,
)
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
_IDENT = re.compile(r"^[A-Za-z0-9_-]+$")


# ── Konfigurasi & koneksi ──────────────────────────────────────────────────

@dataclass(frozen=True)
class Config:
    sheet_name: str
    worksheet_title: str
    sheet_id: str | None
    share_with: tuple[str, ...]
    bq_project: str
    bq_dataset: str
    bq_table: str
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
    sa = service_account_info()
    if sa is None:
        st.error("Credential belum diatur. Salin `.streamlit/secrets.toml.example` menjadi "
                 "`.streamlit/secrets.toml`, lalu isi `[connections.bigquery]` (service account) dan `[app]`.")
        st.stop()
    app = st.secrets["app"] if "app" in st.secrets else {}

    configured = app.get("template_path")
    candidates = ((Path(configured),) if configured else ()) + TEMPLATE_CANDIDATES
    cfg = Config(
        sheet_name=app.get("google_sheet_name", "BPOM Audit Stock"),
        worksheet_title=app.get("worksheet_name", "BPOM Audit Stock"),
        sheet_id=_sheet_id(app.get("google_sheet_id")),
        share_with=tuple(app.get("share_with", [])),
        bq_project=app.get("bigquery_project") or sa["project_id"],
        bq_dataset=app.get("bigquery_dataset", "gt_schema"),
        bq_table=app.get("bigquery_table", "master_distributor"),
        template_path=next((p for p in candidates if p.is_file()), None),
    )
    for ident in (cfg.bq_project, cfg.bq_dataset, cfg.bq_table):
        if not _IDENT.match(ident):
            st.error(f"Nama project/dataset/table BigQuery tidak valid: {ident!r}")
            st.stop()
    return cfg


@st.cache_resource(show_spinner=False)
def get_credentials() -> service_account.Credentials:
    return service_account.Credentials.from_service_account_info(service_account_info(), scopes=SCOPES)


@st.cache_resource(show_spinner=False)
def get_gspread_client() -> gspread.Client:
    return gspread.authorize(get_credentials())


@st.cache_resource(show_spinner=False)
def get_bq_client(project: str) -> bigquery.Client:
    return bigquery.Client(project=project, credentials=get_credentials())


# ── Pembacaan data ─────────────────────────────────────────────────────────

@st.cache_data(ttl=600, show_spinner="Memuat master distributor dari BigQuery...")
def load_master(project: str, dataset: str, table: str) -> pd.DataFrame:
    """Pasangan distributor_company + distributor (unik) dari master, hanya yang status = Active."""
    sql = (
        "SELECT DISTINCT TRIM(distributor_company) AS distributor_company, TRIM(distributor) AS distributor "
        f"FROM `{project}.{dataset}.{table}` "
        "WHERE UPPER(TRIM(IFNULL(status, ''))) = 'ACTIVE' "
        "AND TRIM(IFNULL(distributor_company, '')) != '' AND TRIM(IFNULL(distributor, '')) != '' "
        "ORDER BY 1, 2"
    )
    rows = get_bq_client(project).query(sql).result()
    return pd.DataFrame([dict(r) for r in rows], columns=["distributor_company", "distributor"])


@st.cache_data(ttl=30, show_spinner="Membaca data lama dari Google Sheets...")
def load_existing(sheet_name: str, worksheet: str, sheet_id: str | None,
                  distributor_name: str, periode: str) -> list[dict]:
    """Mode Revisi saja: baris lama untuk distributor + periode."""
    return store.read_existing(get_gspread_client(), sheet_name, worksheet, distributor_name, periode,
                               sheet_id=sheet_id)


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
            "PRODUCT ID": [r.product_id for r in rows],
            "PRODUCT NAME": [r.product_name for r in rows],
            "BPOM NUMBER": [r.bpom_number for r in rows],
            "QTY": [r.qty for r in rows],
        }),
        hide_index=True,
        width="stretch",
    )


def show_flash() -> None:
    flash = st.session_state.pop("flash", None)
    if flash:
        st.success(flash)


def template_button(cfg: Config, distributor_name: str | None) -> None:
    if cfg.template_path is None:
        st.warning(f"File template '{TEMPLATE_FILENAME}' tidak ditemukan. Letakkan di folder `template/` "
                   "atau atur `template_path` di `[app]`.")
        return
    if not distributor_name:
        st.info("Pilih Distributor Company dan Distributor Name (Langkah 1) agar template bisa di-download "
                "dengan nama distributor yang sudah terisi.")
        return
    data = template_builder.build_prefilled_template(cfg.template_path.read_bytes(), distributor_name)
    safe = re.sub(r'[\\/:*?"<>|]+', "_", distributor_name).strip()
    st.download_button(
        "Download Template", data=data, file_name=f"BPOM Audit - {safe}.xlsx", mime=XLSX_MIME,
        help="Template resmi dengan kolom nama distributor sudah terisi. Struktur dan header tidak diubah.",
    )


# ── Submit ─────────────────────────────────────────────────────────────────

def _open_worksheet_for_revision(cfg: Config, gc: gspread.Client):
    sh, _ = store.open_spreadsheet(gc, cfg.sheet_name, sheet_id=cfg.sheet_id, create=False)
    if sh is None:
        raise store.SheetsError("Spreadsheet utama belum ada, jadi belum ada data yang bisa direvisi.")
    try:
        return sh, sh.worksheet(cfg.worksheet_title)
    except gspread.WorksheetNotFound:
        raise store.SheetsError(f"Worksheet '{cfg.worksheet_title}' belum ada, belum ada data yang bisa direvisi.") from None


def save(cfg: Config, mode: str, meta: store.AuditMeta, rows: list[val.ExcelRow]) -> store.SaveSummary:
    gc = get_gspread_client()
    if mode == "Input":
        sh, created = store.open_spreadsheet(
            gc, cfg.sheet_name, sheet_id=cfg.sheet_id, create=True, share_with=cfg.share_with)
        ws = store.ensure_worksheet(sh, cfg.worksheet_title, fresh=created)
        return replace(store.insert_rows(sh, ws, meta, rows, check_duplicates=not created),
                       spreadsheet_created=created)
    sh, ws = _open_worksheet_for_revision(cfg, gc)
    return store.upsert_rows(sh, ws, meta, rows)


def handle_submit(cfg: Config, mode: str, meta: store.AuditMeta | None, form_errors: list[str],
                  parsed: val.ParseResult | None) -> None:
    if form_errors:
        for message in form_errors:
            st.error(message)
        return
    if parsed is None or not parsed.ok:
        st.error("File belum valid, upload dibatalkan. Perbaiki masalah di atas lalu upload ulang.")
        return

    try:
        with st.spinner("Menyimpan ke Google Sheets..."):
            summary = save(cfg, mode, meta, parsed.rows)
    except store.DuplicateDataError as exc:
        st.error(str(exc))
        st.dataframe(
            pd.DataFrame({"Baris Excel": [r.excel_row for r in exc.conflicts],
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
        st.error("Terjadi kesalahan tak terduga saat menyimpan. Periksa spreadsheet sebelum mencoba lagi "
                 "agar tidak terjadi duplikasi.")
        return

    if mode == "Input":
        msg = f"Berhasil menyimpan {summary.inserted} baris untuk {meta.distributor_name} ({meta.periode})."
    else:
        msg = (f"Revisi berhasil untuk {meta.distributor_name} ({meta.periode}): "
               f"{summary.updated} baris diperbarui, {summary.inserted} baris baru ditambahkan.")
        if summary.duplicate_matches:
            msg += (f" Perhatian: {summary.duplicate_matches} baris lama kembar untuk key yang sama "
                    "ikut diperbarui; sebaiknya dibersihkan manual di spreadsheet.")
    if summary.spreadsheet_created:
        msg += " Spreadsheet utama baru dibuat."
    msg += f" [Buka spreadsheet]({summary.spreadsheet_url})"

    load_existing.clear()
    st.session_state["flash"] = msg
    st.session_state["upload_ver"] += 1   # kosongkan uploader
    st.rerun()


# ── Halaman ────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(page_title="BPOM Audit Stock", layout="wide")
    cfg = load_config()
    st.session_state.setdefault("upload_ver", 0)

    st.title("BPOM Audit Stock Distributor")
    if not cfg.sheet_id:
        st.warning("`google_sheet_id` belum terbaca dari Secrets. Pastikan ada di bagian `[app]` "
                   "(`[app]` lalu baris `google_sheet_id = \"...\"`). Tanpa itu, aplikasi mencari spreadsheet "
                   f"berjudul '{cfg.sheet_name}' dan mencoba membuatnya, yang akan gagal.")
    show_flash()
    with st.expander("Panduan singkat: alur pengisian", expanded=True):
        st.markdown(GUIDE_OVERVIEW)

    try:
        master = load_master(cfg.bq_project, cfg.bq_dataset, cfg.bq_table)
    except Exception as exc:
        log.exception("Failed to load master distributor")
        st.error(f"Gagal membaca master distributor dari BigQuery: {exc}")
        st.stop()
    if master.empty:
        st.error("Master distributor kosong.")
        st.stop()

    # 1-2. Distributor Company -> Distributor Name (bergantung pada company)
    st.subheader("Langkah 1 - Pilih distributor")
    st.caption("Pilih **Distributor Company** terlebih dahulu, lalu **Distributor Name** (cabang/region). "
               "Pilihan diambil dari master distributor dan tidak bisa diketik bebas. Klik kolom lalu ketik untuk mencari.")
    col_company, col_name = st.columns(2)
    company = col_company.selectbox("Distributor Company", master["distributor_company"].unique().tolist(),
                                    index=None, placeholder="Pilih atau ketik untuk mencari")
    branches = master.loc[master["distributor_company"] == company, "distributor"].tolist() if company else []
    distributor = col_name.selectbox("Distributor Name", branches, index=None, key=f"branch_{company}",
                                     disabled=not company, placeholder="Pilih Distributor Company dulu")

    # 3-5. Periode, tanggal, PIC
    st.subheader("Langkah 2 - Periode audit, tanggal audit, dan PIC")
    st.caption("**Periode Audit** = bulan dan tahun stock yang diaudit. **Tanggal Audit** = tanggal pengecekan "
               "fisik dilakukan. **Nama PIC** = nama Anda; akan tercatat sebagai pembuat/pengubah data.")
    col_period, col_date, col_pic = st.columns([2, 2, 3])
    period = col_period.selectbox("Periode Audit", OPEN_PERIODS, index=0 if len(OPEN_PERIODS) == 1 else None,
                                  format_func=lambda p: f"{BULAN[p[1] - 1]} {p[0]}", placeholder="Pilih periode")
    tanggal = col_date.date_input("Tanggal Audit", value=None, format="DD/MM/YYYY")
    pic = col_pic.text_input("Nama PIC Distributor", max_chars=100)

    periode = f"{period[0]}-{period[1]:02d}" if period else None
    if periode:
        st.caption(f"Periode Audit: {BULAN[period[1] - 1]} {period[0]} (disimpan sebagai `{periode}`)")

    # 6. Mode
    st.subheader("Langkah 3 - Pilih mode")
    st.caption("Pilih **Input** untuk mengirim hasil audit periode ini untuk pertama kali. Pilih **Revisi** hanya jika "
               "data periode ini sudah pernah dikirim dan ada yang perlu dikoreksi. Jangan memakai Input untuk "
               "mengirim ulang, karena akan membuat data ganda.")
    mode = st.radio("Mode", MODES, horizontal=True,
                    help="Input: data pertama. Revisi: memperbarui data yang sudah ada.")

    expected_name = distributor  # nilai master apa adanya

    # Mode Revisi: baca data lama (Input TIDAK membaca apa pun dari Google Sheets)
    existing: list[dict] = []
    if mode == "Revisi":
        st.subheader("Langkah 4 - Periksa data lama (khusus Revisi)")
        st.caption("Data yang sudah tersimpan untuk distributor dan periode ini ditampilkan di bawah. Periksa dulu "
                   "mana yang perlu dikoreksi. Baris dengan PRODUCT ID + BPOM NUMBER yang sama akan diperbarui; "
                   "kombinasi yang belum ada akan ditambahkan.")
        if not (expected_name and periode):
            st.info("Lengkapi Langkah 1 dan 2 (distributor dan periode) untuk menampilkan data lama.")
        else:
            try:
                existing = load_existing(cfg.sheet_name, cfg.worksheet_title, cfg.sheet_id, expected_name, periode)
            except Exception as exc:
                log.exception("Failed to read existing data")
                st.error(f"Gagal membaca data lama dari Google Sheets: {exc}")
                st.stop()
            if existing:
                st.caption(f"{len(existing)} baris ditemukan untuk {expected_name} periode {periode}.")
                st.dataframe(pd.DataFrame(existing, columns=store.HEADERS), hide_index=True, width="stretch")
            else:
                st.warning("Belum ada data untuk distributor dan periode ini. Gunakan mode Input.")

    # 7-8. Template & upload
    st.subheader(f"Langkah {5 if mode == 'Revisi' else 4} - Download template dan isi")
    st.markdown(GUIDE_FILL)
    if expected_name:
        st.caption(f"Kolom pertama template akan terisi otomatis: **{expected_name}**")
    template_button(cfg, expected_name)

    st.subheader(f"Langkah {6 if mode == 'Revisi' else 5} - Upload file Excel")
    st.caption("Upload file `.xlsx` yang sudah diisi. File diperiksa otomatis. Jika ada satu saja baris yang salah, "
               "seluruh upload dibatalkan dan tidak ada data yang masuk, sehingga perbaiki file lalu upload ulang. "
               + ("Pada mode Revisi, upload hanya baris yang ingin diperbarui atau ditambahkan."
                  if mode == "Revisi" else ""))
    uploaded = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"], key=f"upload_{st.session_state['upload_ver']}")

    file_bytes = parsed = None
    if uploaded is not None:
        file_bytes = uploaded.getvalue()
        if not expected_name:
            st.info("Pilih Distributor Company dan Distributor Name dulu agar isi file bisa divalidasi.")
        else:
            parsed = val.parse_excel(file_bytes, expected_name)
            for warning in parsed.warnings:
                st.warning(warning)
            if parsed.issues:
                show_issues(parsed.issues)
            else:
                st.success(f"File valid: {len(parsed.rows)} baris siap disimpan.")
                if mode == "Revisi" and existing:
                    old_keys = {val.row_key(e["PRODUCT ID"], e["BPOM NUMBER"]) for e in existing}
                    n_update = sum(r.key in old_keys for r in parsed.rows)
                    st.info(f"Revisi: {n_update} baris akan diperbarui, {len(parsed.rows) - n_update} baris baru "
                            "akan ditambahkan. Baris lama yang tidak ada di file tidak dihapus.")
                with st.expander("Pratinjau isi file", expanded=False):
                    show_preview(parsed.rows)

    # 9. Submit
    st.subheader(f"Langkah {7 if mode == 'Revisi' else 6} - Submit")
    st.caption("Pastikan semua isian benar dan file sudah berstatus **valid**, lalu klik Submit satu kali "
               "dan tunggu sampai muncul pesan hasil. Jangan menutup halaman saat proses berjalan.")
    if st.button("Submit", type="primary"):
        form_errors = val.validate_form(company=company, distributor=distributor, periode=periode,
                                        tanggal=tanggal, pic=pic, has_file=uploaded is not None)
        if mode == "Revisi" and not form_errors and not existing:
            form_errors.append("Belum ada data lama untuk distributor dan periode ini; gunakan mode Input.")
        meta = None
        if not form_errors:
            meta = store.AuditMeta(company=company, distributor_name=expected_name, periode=periode,
                                   tanggal_audit=tanggal.isoformat(), pic=pic.strip())
        handle_submit(cfg, mode, meta, form_errors, parsed)


if __name__ == "__main__":
    main()
