import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime
import re
import unicodedata
from google.oauth2 import service_account
from google.cloud import bigquery
from openpyxl import Workbook
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, Protection
from openpyxl.utils import get_column_letter
from openpyxl.workbook.defined_name import DefinedName

# ─── Page config (must be first) ──────────────────────────────────────────────

st.set_page_config(page_title="Salesman & PJP Template", page_icon="📋", layout="wide")

# ─── BigQuery credentials ─────────────────────────────────────────────────────

def get_credentials():
    try:
        gcp_secrets  = st.secrets["connections"]["bigquery"]
        private_key  = gcp_secrets["private_key"].replace("\\n", "\n")
        credentials  = service_account.Credentials.from_service_account_info({
            "type":                        gcp_secrets["type"],
            "project_id":                  gcp_secrets["project_id"],
            "private_key_id":              gcp_secrets["private_key_id"],
            "private_key":                 private_key,
            "client_email":                gcp_secrets["client_email"],
            "client_id":                   gcp_secrets["client_id"],
            "auth_uri":                    gcp_secrets["auth_uri"],
            "token_uri":                   gcp_secrets["token_uri"],
            "auth_provider_x509_cert_url": gcp_secrets["auth_provider_x509_cert_url"],
            "client_x509_cert_url":        gcp_secrets["client_x509_cert_url"],
        })
        project_id = gcp_secrets["project_id"]
    except Exception:
        SERVICE_ACCOUNT_FILE = r'C:\Users\Bella Chelsea\Documents\skintific-data-warehouse-ea77119e2e7a.json'
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
        project_id  = "skintific-data-warehouse"
    return credentials, project_id


# ─── BigQuery loaders ─────────────────────────────────────────────────────────

@st.cache_data(show_spinner="Memuat data distributor dari BigQuery...")
def load_distributor_data() -> pd.DataFrame:
    credentials, project_id = get_credentials()
    client = bigquery.Client(credentials=credentials, project=project_id)
    query = """
        SELECT
            UPPER(distributor)      AS distributor_name,
            UPPER(region_g2g)       AS region,
            UPPER(distributor_code) AS distributor_code,
            UPPER(asm_g2g)          AS asm
        FROM `gt_schema.master_distributor`
        WHERE asm_g2g != '' AND status = 'Active'
    """
    df = client.query(query).to_dataframe()
    df["distributor_code"] = df["distributor_code"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["distributor_code"]).reset_index(drop=True)
    return df


@st.cache_data(show_spinner="Memuat data toko dari Database...")
def load_store_data() -> pd.DataFrame:
    credentials, project_id = get_credentials()
    client = bigquery.Client(credentials=credentials, project=project_id)
    query = """
        WITH RAW AS (
            SELECT
                UPPER(distributor)      AS distributor_name,
                UPPER(region_g2g)       AS region,
                UPPER(distributor_code) AS distributor_code,
                UPPER(asm_g2g)          AS asm
            FROM `gt_schema.master_distributor`
            WHERE asm_g2g != '' AND status = 'Active'
        )
        SELECT
            UPPER(a.cust_id)           AS store_code,
            UPPER(a.store_name)        AS store_name,
            UPPER(a.distributor_g2g)   AS distributor_name
        FROM `gt_schema.master_store_database_basis` a
        LEFT JOIN RAW b ON UPPER(a.distributor_g2g) = UPPER(b.distributor_name)
    """
    df = client.query(query).to_dataframe()
    df = df.dropna(subset=["store_code", "store_name", "distributor_name"])
    df["store_code"]       = df["store_code"].astype(str).str.strip()
    df["store_name"]       = df["store_name"].astype(str).str.strip()
    df["distributor_name"] = df["distributor_name"].astype(str).str.strip()
    df["store_label"]      = df["store_code"] + " - " + df["store_name"]
    df = df.drop_duplicates(subset=["store_code"]).reset_index(drop=True)
    return df


def build_lookup_tables(dist_df: pd.DataFrame):
    distributor_map = dict(zip(dist_df["distributor_code"], dist_df["distributor_name"]))
    asm_options     = sorted(dist_df["asm"].dropna().unique().tolist())
    region_options  = sorted(dist_df["region"].dropna().unique().tolist())
    return distributor_map, asm_options, region_options


# ─── Salesman Mapping table helpers ──────────────────────────────────────────

MAPPING_TABLE  = "skintific-data-warehouse.gt_schema.gt_salesman_mapping"
SALESMAN_TABLE = "skintific-data-warehouse.gt_schema.gt_master_salesman"
PJP_TABLE      = "skintific-data-warehouse.gt_schema.gt_master_salesman_pjp"

SALESMAN_TYPES = ["GTI", "MIX", "MTI"]


def get_salesman_list(distributor_code: str) -> pd.DataFrame:
    try:
        credentials, project_id = get_credentials()
        client = bigquery.Client(credentials=credentials, project=project_id)
        query = f"""
            SELECT
                m.salesman_id,
                m.salesman_type,
                m.distributor_code,
                m.salesman,
                m.is_active,
                m.created_at,
                m.updated_at,
                s.nama_salesman,
                s.no_hp,
                s.status_salesman,
                s.region,
                s.asm
            FROM `{MAPPING_TABLE}` m
            LEFT JOIN `{SALESMAN_TABLE}` s
                ON UPPER(TRIM(m.salesman)) = UPPER(TRIM(s.nama_salesman))
            WHERE UPPER(m.distributor_code) = UPPER(@kode)
            ORDER BY m.salesman_id, m.created_at DESC
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("kode", "STRING", distributor_code)]
        )
        df = client.query(query, job_config=job_config).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Gagal memuat daftar salesman: {e}")
        return pd.DataFrame()


def get_latest_running_number(distributor_code: str, salesman_type: str) -> int:
    try:
        credentials, project_id = get_credentials()
        client = bigquery.Client(credentials=credentials, project=project_id)
        prefix = f"{salesman_type}{distributor_code}"
        query = f"""
            SELECT MAX(CAST(SUBSTR(salesman_id, {len(prefix) + 1}) AS INT64)) AS max_num
            FROM `{MAPPING_TABLE}`
            WHERE UPPER(distributor_code) = UPPER(@kode)
              AND UPPER(salesman_type)    = UPPER(@stype)
              AND STARTS_WITH(salesman_id, @prefix)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("kode",   "STRING", distributor_code),
                bigquery.ScalarQueryParameter("stype",  "STRING", salesman_type),
                bigquery.ScalarQueryParameter("prefix", "STRING", prefix),
            ]
        )
        result = client.query(query, job_config=job_config).to_dataframe()
        max_num = result["max_num"].iloc[0]
        return int(max_num) if pd.notna(max_num) else 0
    except Exception:
        return 0


def generate_salesman_id(distributor_code: str, salesman_type: str) -> str:
    latest = get_latest_running_number(distributor_code, salesman_type)
    next_num = latest + 1
    return f"{salesman_type}{distributor_code}{str(next_num).zfill(3)}"


def insert_salesman_record(salesman_data: dict) -> tuple[bool, str]:
    try:
        credentials, project_id = get_credentials()
        client = bigquery.Client(credentials=credentials, project=project_id)
        row = {**salesman_data, "uploaded_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        client.load_table_from_dataframe(pd.DataFrame([row]), SALESMAN_TABLE, job_config=job_config).result()
        return True, ""
    except Exception as e:
        return False, str(e)


def insert_mapping_record(salesman_id: str, distributor_code: str,
                          salesman_type: str, nama_salesman: str = "") -> tuple[bool, str]:
    try:
        credentials, project_id = get_credentials()
        client = bigquery.Client(credentials=credentials, project=project_id)
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        row = {
            "salesman_id":      salesman_id,
            "salesman_type":    salesman_type,
            "distributor_code": distributor_code,
            "salesman":         sanitize_salesman_name(nama_salesman) if nama_salesman else "",
            "is_active":        True,
            "created_at":       now,
            "updated_at":       now,
        }
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        client.load_table_from_dataframe(pd.DataFrame([row]), MAPPING_TABLE, job_config=job_config).result()
        return True, ""
    except Exception as e:
        return False, str(e)


def deactivate_previous_mapping(salesman_id: str) -> tuple[bool, str]:
    try:
        credentials, project_id = get_credentials()
        client = bigquery.Client(credentials=credentials, project=project_id)
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        query = f"""
            UPDATE `{MAPPING_TABLE}`
            SET is_active  = FALSE,
                updated_at = @updated_at
            WHERE salesman_id = @sid
              AND is_active   = TRUE
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sid",        "STRING", salesman_id),
                bigquery.ScalarQueryParameter("updated_at", "STRING", now),
            ]
        )
        client.query(query, job_config=job_config).result()
        return True, ""
    except Exception as e:
        return False, str(e)


def deactivate_salesman_mapping(salesman_id: str) -> tuple[bool, str]:
    return deactivate_previous_mapping(salesman_id)


def reactivate_salesman_mapping(salesman_id: str, distributor_code: str,
                                salesman_type: str, nama_salesman: str) -> tuple[bool, str]:
    return insert_mapping_record(salesman_id, distributor_code, salesman_type, nama_salesman)


def get_vacant_salesman_ids(distributor_code: str) -> pd.DataFrame:
    try:
        credentials, project_id = get_credentials()
        client = bigquery.Client(credentials=credentials, project=project_id)
        query = f"""
            SELECT
                m.salesman_id,
                m.salesman_type,
                MAX(m.updated_at) AS last_updated,
                MAX(m.salesman)   AS last_salesman
            FROM `{MAPPING_TABLE}` m
            WHERE UPPER(m.distributor_code) = UPPER(@kode)
            GROUP BY m.salesman_id, m.salesman_type
            HAVING LOGICAL_AND(m.is_active = FALSE)
            ORDER BY m.salesman_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("kode", "STRING", distributor_code)]
        )
        df = client.query(query, job_config=job_config).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Gagal memuat daftar ID salesman vakant: {e}")
        return pd.DataFrame()


# ─── Static option lists ──────────────────────────────────────────────────────

STATUS_OPTIONS    = ["Mix", "Eksklusif"]
GENDER_OPTIONS    = ["Male", "Female"]
EDUCATION_OPTIONS = ["SD", "SMP", "SMA", "S1", "S2"]
DAY_OPTIONS       = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu"]
WEEK_OPTIONS      = ["Minggu Ganjil", "Minggu Genap", "Minggu Ganjil + Genap"]
FREQUENCY_OPTIONS = ["F4+", "F4", "F2", "F1"]

SALESMAN_COLS = [
    ("Nama Salesman",                                       True,  "text"),
    ("Nama SPV External",                                   False, "text"),
    ("Nama SPV Internal",                                   True,  "text"),
    ("ASM",                                                 True,  "cascade"),
    ("Region",                                              True,  "cascade"),
    ("Nama Distributor",                                    True,  "cascade"),
    ("Kode Distributor",                                    True,  "auto"),
    ("Status Salesman",                                     True,  "dropdown"),
    ("Total Outlet Coverage PJP",                           True,  "numeric"),
    ("Gaji Pokok",                                          True,  "numeric"),
    ("Tunjangan dan insentif",                              True,  "numeric"),
    ("Tanggal Lahir",                                       True,  "date"),
    ("Jenis Kelamin",                                       True,  "dropdown"),
    ("Pendidikan Terakhir",                                 True,  "dropdown"),
    ("Pengalaman di Perusahaan Sebelumnya (Dalam Bulan)",   True,  "numeric"),
    ("Principal Lain yang Ditanggungjawabi",                False, "text"),
    ("No. HP",                                              True,  "text"),
    ("Tanggal Join di G2G",                                 True,  "date"),
]

PJP_COLS = [
    ("ASM",                                                 True,  "cascade"),
    ("Region",                                              True,  "cascade"),
    ("Nama Distributor",                                    True,  "cascade"),
    ("Kode Distributor",                                    True,  "auto"),
    ("Nama Salesman",                                       True,  "text"),
    ("Kode Toko",                                           True,  "store_cascade"),
    ("Nama Toko",                                           False, "auto"),
    ("Hari",                                                True,  "dropdown"),
    ("Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap",    True,  "dropdown"),
    ("Frekuensi",                                           True,  "dropdown"),
]

SALESMAN_REQUIRED = [c for c, r, _ in SALESMAN_COLS if r]
PJP_REQUIRED      = [c for c, r, _ in PJP_COLS if r]


# ─── Named-range key sanitiser ────────────────────────────────────────────────

def _safe_name(text: str) -> str:
    nfkd    = unicodedata.normalize("NFKD", text)
    ascii_s = nfkd.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z0-9]", "_", ascii_s)
    if not cleaned or cleaned[0].isdigit():
        cleaned = "_" + cleaned
    return ("NR_" + cleaned)[:255]


def _indirect_clean(cell_ref: str) -> str:
    special = [" ", "-", "/", "(", ")", "+", "&", ".", "'"]
    expr = cell_ref
    for ch in special:
        expr = f'SUBSTITUTE({expr},"{ch}","_")'
    return expr


# ─── Style helpers ────────────────────────────────────────────────────────────

def _thin_border():
    s = Side(style="thin", color="BFBFBF")
    return Border(left=s, right=s, top=s, bottom=s)

def _fill(hex_color):
    return PatternFill("solid", fgColor=hex_color)

def _header_font():
    return Font(bold=True, color="FFFFFF", size=10, name="Calibri")

def _note_font():
    return Font(italic=True, color="808080", size=9, name="Calibri")

def _req_font():
    return Font(bold=True, color="C00000", size=9, name="Calibri")

def _center():
    return Alignment(horizontal="center", vertical="center", wrap_text=True)

def _vcenter(wrap=False):
    return Alignment(vertical="center", wrap_text=wrap)


# ─── Build Lookup sheet + named ranges ───────────────────────────────────────

def _build_lookup_and_named_ranges(wb, dist_df, store_df=None):
    LK = "Lookup"
    lk = wb.create_sheet(LK)
    lk.sheet_state = "hidden"

    asm_list = sorted(dist_df["asm"].dropna().unique().tolist())
    cur_col  = 1

    lk.cell(row=1, column=cur_col, value="__ALL_ASM__")
    for i, asm in enumerate(asm_list, start=2):
        lk.cell(row=i, column=cur_col, value=asm)
    c  = get_column_letter(cur_col)
    nm = _safe_name("ALL_ASM")
    wb.defined_names[nm] = DefinedName(nm, attr_text=f"'{LK}'!${c}$2:${c}${1+len(asm_list)}")
    cur_col += 1

    for asm in asm_list:
        regions = sorted(dist_df.loc[dist_df["asm"] == asm, "region"].unique().tolist())
        lk.cell(row=1, column=cur_col, value=f"__ASM_{asm}__")
        for i, reg in enumerate(regions, start=2):
            lk.cell(row=i, column=cur_col, value=reg)
        c  = get_column_letter(cur_col)
        nm = _safe_name(asm)
        wb.defined_names[nm] = DefinedName(nm, attr_text=f"'{LK}'!${c}$2:${c}${1+len(regions)}")
        cur_col += 1

    for asm in asm_list:
        regions = sorted(dist_df.loc[dist_df["asm"] == asm, "region"].unique().tolist())
        for region in regions:
            mask  = (dist_df["asm"] == asm) & (dist_df["region"] == region)
            names = sorted(dist_df.loc[mask, "distributor_name"].unique().tolist())
            lk.cell(row=1, column=cur_col, value=f"__ASM_{asm}__REG_{region}__")
            for i, name in enumerate(names, start=2):
                lk.cell(row=i, column=cur_col, value=name)
            c  = get_column_letter(cur_col)
            nm = _safe_name(f"{asm}_{region}")
            wb.defined_names[nm] = DefinedName(nm, attr_text=f"'{LK}'!${c}$2:${c}${1+len(names)}")
            cur_col += 1

    name_col = cur_col
    code_col = cur_col + 1
    lk.cell(row=1, column=name_col, value="__DIST_NAME__")
    lk.cell(row=1, column=code_col, value="__DIST_CODE__")
    all_dists = (
        dist_df[["distributor_name", "distributor_code"]]
        .drop_duplicates(subset=["distributor_name"])
        .sort_values("distributor_name")
        .reset_index(drop=True)
    )
    for i, row in all_dists.iterrows():
        lk.cell(row=i + 2, column=name_col, value=row["distributor_name"])
        lk.cell(row=i + 2, column=code_col, value=row["distributor_code"])
    nc       = get_column_letter(name_col)
    kc       = get_column_letter(code_col)
    last_row = 1 + len(all_dists)
    wb.defined_names["NR_DIST_LOOKUP"] = DefinedName(
        "NR_DIST_LOOKUP",
        attr_text=f"'{LK}'!${nc}$2:${kc}${last_row}",
    )
    cur_col += 2

    if store_df is not None and not store_df.empty:
        dist_names_with_stores = sorted(store_df["distributor_name"].dropna().unique().tolist())
        for dist_name in dist_names_with_stores:
            codes = sorted(
                store_df.loc[store_df["distributor_name"] == dist_name, "store_code"]
                .dropna().unique().tolist()
            )
            lk.cell(row=1, column=cur_col, value=f"__STORE_{dist_name}__")
            for i, code in enumerate(codes, start=2):
                lk.cell(row=i, column=cur_col, value=code)
            c  = get_column_letter(cur_col)
            nm = _safe_name(f"STORE_{dist_name}")
            wb.defined_names[nm] = DefinedName(
                nm, attr_text=f"'{LK}'!${c}$2:${c}${1+len(codes)}"
            )
            cur_col += 1

        sc_col = cur_col
        sn_col = cur_col + 1
        lk.cell(row=1, column=sc_col, value="__STORE_CODE__")
        lk.cell(row=1, column=sn_col, value="__STORE_NAME__")
        all_stores = (
            store_df[["store_code", "store_name"]]
            .drop_duplicates(subset=["store_code"])
            .sort_values("store_code")
            .reset_index(drop=True)
        )
        for i, row in all_stores.iterrows():
            lk.cell(row=i + 2, column=sc_col, value=row["store_code"])
            lk.cell(row=i + 2, column=sn_col, value=row["store_name"])
        scc      = get_column_letter(sc_col)
        snc      = get_column_letter(sn_col)
        last_row = 1 + len(all_stores)
        wb.defined_names["NR_STORE_LOOKUP"] = DefinedName(
            "NR_STORE_LOOKUP",
            attr_text=f"'{LK}'!${scc}$2:${snc}${last_row}",
        )


# ─── Attach cascading DVs ─────────────────────────────────────────────────────

def _attach_cascade_dvs(ws, col_names, first_data, last_data):
    def cl(name):
        return get_column_letter(col_names.index(name) + 1)

    def sqref(name):
        c = cl(name)
        return f"{c}{first_data}:{c}{last_data}"

    asm_ref = f"{cl('ASM')}{first_data}"
    reg_ref = f"{cl('Region')}{first_data}"

    dv_asm = DataValidation(
        type="list", formula1=_safe_name("ALL_ASM"), allow_blank=True,
        showInputMessage=True, promptTitle="Langkah 1 - ASM",
        prompt="Pilih nama ASM. Region dan Distributor akan menyesuaikan.",
        showErrorMessage=True, errorTitle="Input Tidak Valid",
        error="Pilih ASM dari daftar.",
    )
    ws.add_data_validation(dv_asm)
    dv_asm.sqref = sqref("ASM")

    asm_clean = _indirect_clean(asm_ref)
    dv_reg = DataValidation(
        type="list", formula1=f'INDIRECT("NR_"&{asm_clean})', allow_blank=True,
        showInputMessage=True, promptTitle="Langkah 2 - Region",
        prompt="Pilih Region. Daftar disesuaikan dengan ASM yang dipilih.",
        showErrorMessage=True, errorTitle="Input Tidak Valid",
        error="Pilih Region dari daftar. Pastikan ASM sudah dipilih.",
    )
    ws.add_data_validation(dv_reg)
    dv_reg.sqref = sqref("Region")

    reg_clean = _indirect_clean(reg_ref)
    dv_nama = DataValidation(
        type="list",
        formula1=f'INDIRECT("NR_"&{asm_clean}&"_"&{reg_clean})',
        allow_blank=True,
        showInputMessage=True, promptTitle="Langkah 3 - Nama Distributor",
        prompt="Pilih Nama Distributor. Daftar disesuaikan dengan ASM dan Region.",
        showErrorMessage=True, errorTitle="Input Tidak Valid",
        error="Pilih Distributor dari daftar. Pastikan ASM dan Region sudah dipilih.",
    )
    ws.add_data_validation(dv_nama)
    dv_nama.sqref = sqref("Nama Distributor")

    if "Kode Toko" in col_names:
        nama_dist_ref = f"{cl('Nama Distributor')}{first_data}"
        dist_clean    = _indirect_clean(nama_dist_ref)
        dv_store = DataValidation(
            type="list",
            formula1=f'INDIRECT("NR_STORE_"&{dist_clean})',
            allow_blank=True,
            showInputMessage=True, promptTitle="Langkah 4 - Kode Toko",
            prompt="Pilih Kode Toko. Daftar disesuaikan dengan Distributor yang dipilih.",
            showErrorMessage=True, errorTitle="Input Tidak Valid",
            error="Pilih Kode Toko dari daftar. Pastikan Nama Distributor sudah dipilih.",
        )
        ws.add_data_validation(dv_store)
        dv_store.sqref = sqref("Kode Toko")


# ─── Salesman Excel ───────────────────────────────────────────────────────────

def create_salesman_excel(df, distributor_map, asm_options, region_options, dist_df) -> BytesIO:
    wb = Workbook()
    wb.remove(wb.active)
    _build_lookup_and_named_ranges(wb, dist_df)

    ws = wb.create_sheet("Salesman Template")

    col_names = [c for c, _, _ in SALESMAN_COLS]
    col_types = {c: t for c, _, t in SALESMAN_COLS}
    col_req   = {c: r for c, r, _ in SALESMAN_COLS}

    FIRST_DATA = 4
    LAST_DATA  = 30003

    CASCADE_COLS = {"ASM", "Region", "Nama Distributor", "Kode Distributor"}

    notes = {
        "Nama Salesman":            "Teks bebas",
        "Nama SPV External":        "Teks bebas (opsional)",
        "Nama SPV Internal":        "Teks bebas",
        "ASM":                      "Langkah 1 - Pilih ASM dari dropdown",
        "Region":                   "Langkah 2 - Pilih Region (mengikuti ASM)",
        "Nama Distributor":         "Langkah 3 - Pilih Distributor (mengikuti Region)",
        "Kode Distributor":         "Otomatis terisi dari Nama Distributor",
        "Status Salesman":          "Pilih: Mix atau Eksklusif",
        "Total Outlet Coverage PJP":"Angka bulat",
        "Gaji Pokok":               "Angka (Rupiah)",
        "Tunjangan dan insentif":   "Angka (Rupiah)",
        "Tanggal Lahir":            "Isi tanggal dengan format YYYY-MM-DD (contoh: 2001-01-25)",
        "Jenis Kelamin":            "Pilih: Male atau Female",
        "Pendidikan Terakhir":      "Pilih dari dropdown",
        "Pengalaman di Perusahaan Sebelumnya (Dalam Bulan)": "Angka (bulan)",
        "Principal Lain yang Ditanggungjawabi":
            "Teks bebas (opsional) — jika lebih dari satu, pisahkan dengan koma. Contoh: Unilever, P&G, Nestle",
        "No. HP":                   "Angka, tanpa tanda hubung",
        "Tanggal Join di G2G":      "Isi tanggal dengan format YYYY-MM-DD (contoh: 2026-01-25)",
    }

    for ci, cn in enumerate(col_names, 1):
        cell = ws.cell(row=1, column=ci, value=notes.get(cn, ""))
        cell.font = _note_font()
        cell.alignment = _vcenter(wrap=True)

    for ci, cn in enumerate(col_names, 1):
        if col_req[cn]:
            cell = ws.cell(row=2, column=ci, value="Wajib Diisi")
            cell.font = _req_font(); cell.alignment = _center()

    for ci, cn in enumerate(col_names, 1):
        cell = ws.cell(row=3, column=ci, value=cn)
        cell.font      = _header_font()
        cell.fill      = _fill("1A7A6E" if cn in CASCADE_COLS else "2E75B6")
        cell.alignment = _center()
        cell.border    = _thin_border()

    ws.row_dimensions[1].height = 42
    ws.row_dimensions[2].height = 16
    ws.row_dimensions[3].height = 44
    ws.freeze_panes = "A4"

    widths = [22, 22, 22, 22, 24, 30, 20, 16, 22, 16, 22, 18, 14, 20, 38, 42, 16, 20]
    for ci, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(ci)].width = w

    def col_letter(name):
        return get_column_letter(col_names.index(name) + 1)

    def dr(name):
        return f"{col_letter(name)}{FIRST_DATA}:{col_letter(name)}{LAST_DATA}"

    _attach_cascade_dvs(ws, col_names, FIRST_DATA, LAST_DATA)

    for col_name, opts, title, msg in [
        ("Status Salesman",    STATUS_OPTIONS,    "Status",        "Mix atau Eksklusif"),
        ("Jenis Kelamin",      GENDER_OPTIONS,    "Jenis Kelamin", "Male atau Female"),
        ("Pendidikan Terakhir",EDUCATION_OPTIONS, "Pendidikan",    "Pilih jenjang"),
    ]:
        dv = DataValidation(
            type="list", formula1='"' + ",".join(opts) + '"', allow_blank=True,
            showInputMessage=True, promptTitle=title, prompt=msg,
            showErrorMessage=True, errorTitle="Input Tidak Valid",
            error="Pilih nilai dari daftar dropdown.",
        )
        ws.add_data_validation(dv); dv.sqref = dr(col_name)

    for cn in ["Total Outlet Coverage PJP", "Gaji Pokok", "Tunjangan dan insentif",
               "Pengalaman di Perusahaan Sebelumnya (Dalam Bulan)"]:
        dv = DataValidation(
            type="whole", operator="greaterThanOrEqual", formula1="0",
            allow_blank=True, showErrorMessage=True,
            errorTitle="Input Tidak Valid", error="Hanya angka >= 0.",
        )
        ws.add_data_validation(dv); dv.sqref = dr(cn)

    hp_c = col_letter("No. HP")
    hp_dv = DataValidation(
        type="custom",
        formula1=f'AND(ISNUMBER(VALUE({hp_c}{FIRST_DATA})),LEN(SUBSTITUTE({hp_c}{FIRST_DATA}," ",""))>=8)',
        allow_blank=True,
        showInputMessage=True, promptTitle="No. HP",
        prompt="Masukkan nomor HP (hanya angka, tanpa tanda hubung atau spasi).",
        showErrorMessage=True, errorTitle="No. HP Tidak Valid",
        error="Hanya angka yang diperbolehkan. Contoh: 08123456789",
    )
    ws.add_data_validation(hp_dv); hp_dv.sqref = dr("No. HP")

    date_dv = DataValidation(
        type="date", operator="greaterThan", formula1="DATE(1900,1,1)",
        allow_blank=True,
        showInputMessage=True, promptTitle="Format Tanggal",
        prompt="Isi tanggal dengan format YYYY-MM-DD (contoh: 2001-01-25).",
        showErrorMessage=True, errorTitle="Tanggal Tidak Valid",
        error="Masukkan tanggal yang valid.",
    )
    ws.add_data_validation(date_dv)
    cl_lahir = col_letter("Tanggal Lahir")
    cl_join  = col_letter("Tanggal Join di G2G")
    date_dv.sqref = (
        f"{cl_lahir}{FIRST_DATA}:{cl_lahir}{LAST_DATA} "
        f"{cl_join}{FIRST_DATA}:{cl_join}{LAST_DATA}"
    )

    principal_cl = col_letter("Principal Lain yang Ditanggungjawabi")
    principal_dv = DataValidation(
        type="custom", formula1=f'LEN({principal_cl}{FIRST_DATA})>=0',
        allow_blank=True, showInputMessage=True,
        promptTitle="Principal Lain (opsional)",
        prompt="Jika lebih dari satu principal, pisahkan dengan koma.\nContoh: Unilever, P&G, Nestle",
        showErrorMessage=False,
    )
    ws.add_data_validation(principal_dv)
    principal_dv.sqref = dr("Principal Lain yang Ditanggungjawabi")

    nama_cl = col_letter("Nama Distributor")

    for excel_row in range(FIRST_DATA, LAST_DATA + 1):
        dfi      = excel_row - FIRST_DATA
        has_data = dfi < len(df)

        for ci, cn in enumerate(col_names, 1):
            cell  = ws.cell(row=excel_row, column=ci)
            ctype = col_types[cn]

            if cn == "Kode Distributor":
                cell.value = f'=IFERROR(VLOOKUP({nama_cl}{excel_row},NR_DIST_LOOKUP,2,0),"")'
                cell.fill       = _fill("D6E4F0")
                cell.font       = Font(italic=True, color="1A7A6E", size=10, name="Calibri")
                cell.alignment  = _vcenter()
                cell.border     = _thin_border()
                cell.number_format = "@"
                cell.protection = Protection(locked=True)
                continue

            if ctype == "date":
                cell.number_format = "YYYY-MM-DD"
                if has_data:
                    val = df.iloc[dfi].get(cn, "")
                    if pd.notna(val) and str(val).strip():
                        try:    cell.value = pd.to_datetime(val).date()
                        except: cell.value = str(val)
            elif ctype == "numeric":
                cell.number_format = "#,##0"
                if has_data:
                    val = df.iloc[dfi].get(cn, "")
                    if pd.notna(val) and str(val).strip():
                        try:    cell.value = float(str(val).replace(",", ""))
                        except: cell.value = str(val)
            else:
                cell.number_format = "@"
                if has_data:
                    val = df.iloc[dfi].get(cn, "")
                    cell.value = "" if pd.isna(val) else (str(val) if val != "" else "")

            cell.alignment  = _vcenter()
            cell.border     = _thin_border()
            cell.protection = Protection(locked=False)

    ws.protection.sheet               = True
    ws.protection.password            = "skintific"
    ws.protection.selectLockedCells   = False
    ws.protection.selectUnlockedCells = False

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    return output


# ─── PJP Excel ────────────────────────────────────────────────────────────────

def create_pjp_excel(df, distributor_map, dist_df, store_df) -> BytesIO:
    wb = Workbook()
    wb.remove(wb.active)
    _build_lookup_and_named_ranges(wb, dist_df, store_df)

    col_names = [c for c, _, _ in PJP_COLS]
    col_types = {c: t for c, _, t in PJP_COLS}
    col_req   = {c: r for c, r, _ in PJP_COLS}

    FIRST_DATA = 4
    LAST_DATA  = 30003

    CASCADE_COLS = {"ASM", "Region", "Nama Distributor", "Kode Distributor", "Kode Toko", "Nama Toko"}

    notes_pjp = {
        "ASM":              "Langkah 1 - Pilih ASM dari dropdown",
        "Region":           "Langkah 2 - Pilih Region (mengikuti ASM)",
        "Nama Distributor": "Langkah 3 - Pilih Distributor (mengikuti Region)",
        "Kode Distributor": "Otomatis terisi dari Nama Distributor",
        "Nama Salesman":    "Teks bebas",
        "Kode Toko":        "Langkah 4 - Pilih Kode Toko (mengikuti Distributor)",
        "Nama Toko":        "Otomatis terisi dari Kode Toko",
        "Hari":             "Drop down dengan opsi hari",
        "Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap": "Drop down: ganjil / genap / ganjil+genap",
        "Frekuensi":        "F4+ = >1x seminggu  |  F4 = 1 minggu sekali  |  F2 = 2 minggu sekali  |  F1 = 1 bulan sekali",
    }

    ws = wb.create_sheet("PJP Template")

    for ci, cn in enumerate(col_names, 1):
        cell = ws.cell(row=1, column=ci, value=notes_pjp.get(cn, ""))
        cell.font = _note_font()
        cell.alignment = _vcenter(wrap=True)

    for ci, cn in enumerate(col_names, 1):
        if col_req.get(cn):
            cell = ws.cell(row=2, column=ci, value="Wajib Diisi")
            cell.font = _req_font(); cell.alignment = _center()

    for ci, cn in enumerate(col_names, 1):
        cell = ws.cell(row=3, column=ci, value=cn)
        cell.font      = _header_font()
        cell.fill      = _fill("1A7A6E" if cn in CASCADE_COLS else "ED7D31")
        cell.alignment = _center()
        cell.border    = _thin_border()

    ws.row_dimensions[1].height = 42
    ws.row_dimensions[2].height = 16
    ws.row_dimensions[3].height = 44
    ws.freeze_panes = "A4"

    widths = [22, 24, 30, 20, 22, 18, 30, 12, 40, 22]
    for ci, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(ci)].width = w

    def col_letter(name):
        return get_column_letter(col_names.index(name) + 1)

    def dr(name):
        return f"{col_letter(name)}{FIRST_DATA}:{col_letter(name)}{LAST_DATA}"

    _attach_cascade_dvs(ws, col_names, FIRST_DATA, LAST_DATA)

    for col_name, opts in [
        ("Hari",                                              DAY_OPTIONS),
        ("Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap", WEEK_OPTIONS),
    ]:
        dv = DataValidation(
            type="list", formula1='"' + ",".join(opts) + '"', allow_blank=True,
            showInputMessage=True, promptTitle=col_name, prompt=f"Pilih {col_name}",
            showErrorMessage=True, errorTitle="Input Tidak Valid",
            error="Pilih nilai dari daftar dropdown.",
        )
        ws.add_data_validation(dv); dv.sqref = dr(col_name)

    frekuensi_dv = DataValidation(
        type="list", formula1='"' + ",".join(FREQUENCY_OPTIONS) + '"',
        allow_blank=True, showInputMessage=True, promptTitle="Frekuensi Kunjungan",
        prompt=(
            "Pilih frekuensi kunjungan:\n"
            "  F4+ = lebih dari 1 kali dalam seminggu\n"
            "  F4  = 1 minggu sekali\n"
            "  F2  = 2 minggu sekali\n"
            "  F1  = 1 bulan sekali"
        ),
        showErrorMessage=True, errorTitle="Input Tidak Valid",
        error="Pilih F4+, F4, F2, atau F1.",
    )
    ws.add_data_validation(frekuensi_dv)
    frekuensi_dv.sqref = dr("Frekuensi")

    nama_cl      = col_letter("Nama Distributor")
    kode_toko_cl = col_letter("Kode Toko")
    df_reindexed = df.reindex(columns=col_names)

    for excel_row in range(FIRST_DATA, LAST_DATA + 1):
        dfi      = excel_row - FIRST_DATA
        has_data = dfi < len(df_reindexed)

        for ci, cn in enumerate(col_names, 1):
            cell  = ws.cell(row=excel_row, column=ci)
            ctype = col_types.get(cn, "text")

            if cn == "Kode Distributor":
                cell.value = f'=IFERROR(VLOOKUP({nama_cl}{excel_row},NR_DIST_LOOKUP,2,0),"")'
                cell.fill       = _fill("D6E4F0")
                cell.font       = Font(italic=True, color="1A7A6E", size=10, name="Calibri")
                cell.alignment  = _vcenter()
                cell.border     = _thin_border()
                cell.number_format = "@"
                cell.protection = Protection(locked=True)
                continue

            if cn == "Nama Toko":
                cell.value = f'=IFERROR(VLOOKUP({kode_toko_cl}{excel_row},NR_STORE_LOOKUP,2,0),"")'
                cell.fill       = _fill("D6E4F0")
                cell.font       = Font(italic=True, color="1A7A6E", size=10, name="Calibri")
                cell.alignment  = _vcenter()
                cell.border     = _thin_border()
                cell.number_format = "@"
                cell.protection = Protection(locked=True)
                continue

            cell.number_format = "@"
            if has_data:
                val = df_reindexed.iloc[dfi].get(cn, "")
                cell.value = "" if pd.isna(val) else (str(val) if val != "" else "")

            cell.alignment  = _vcenter()
            cell.border     = _thin_border()
            cell.protection = Protection(locked=False)

    ws.protection.sheet               = True
    ws.protection.password            = "skintific"
    ws.protection.selectLockedCells   = False
    ws.protection.selectUnlockedCells = False

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    return output


# ─── Helpers ──────────────────────────────────────────────────────────────────

def normalize_phone_id(phone) -> str:
    if phone is None:
        return ""
    hp = str(phone).strip().replace(" ", "").replace("-", "").rstrip(".0")
    if hp.startswith("+62+62"):
        hp = hp[3:]
    if hp.startswith("+62"):
        digits_after = hp[3:]
        if digits_after.isdigit() and 8 <= len(digits_after) <= 13:
            return hp
        return hp
    if hp.startswith("62"):
        if hp[2:].isdigit() and 8 <= len(hp[2:]) <= 13:
            return "+" + hp
        return hp
    if hp.startswith("0"):
        if hp[1:].isdigit() and 8 <= len(hp[1:]) <= 13:
            return "+62" + hp[1:]
        return hp
    if hp.isdigit():
        if 8 <= len(hp) <= 13:
            return "+62" + hp
        return hp
    return hp


def sanitize_salesman_name(name: str) -> str:
    return re.sub(r"\s+", " ", str(name).strip().upper())


def _is_empty(val) -> bool:
    return pd.isna(val) or str(val).strip() == ""


def _get_unique_distributors(df, col="Kode Distributor") -> list:
    if col not in df.columns:
        return []
    return (
        df[col].dropna().astype(str).str.strip()
        .replace("", pd.NA).dropna().unique().tolist()
    )


def validate_row_completeness(df, required_cols, sheet_label) -> list:
    errors = []
    for i, row in df.iterrows():
        n = i + 4
        values    = {c: row.get(c, "") for c in required_cols}
        non_empty = [c for c, v in values.items() if not _is_empty(v)]
        empty     = [c for c, v in values.items() if _is_empty(v)]
        if non_empty and empty:
            errors.append(f"Baris {n}: kolom wajib belum terisi — {', '.join(empty)}")
    return errors


def validate_salesman_df(df, distributor_map, asm_options, region_options):
    errors, warnings = [], []
    missing = [c for c in SALESMAN_REQUIRED if c not in df.columns]
    if missing:
        errors.append(f"Kolom wajib tidak ditemukan: {', '.join(missing)}")
        return errors, warnings

    errors += validate_row_completeness(df, SALESMAN_REQUIRED, "Salesman")

    unique_dist = _get_unique_distributors(df)
    if len(unique_dist) > 1:
        errors.append(
            f"Sheet Salesman Template hanya boleh berisi 1 kode distributor per file. "
            f"Ditemukan {len(unique_dist)} kode: {', '.join(unique_dist)}"
        )
        return errors, warnings

    for i, row in df.iterrows():
        n = i + 4
        if pd.notna(row.get("ASM")) and row["ASM"] not in asm_options:
            warnings.append(f"Baris {n}: ASM '{row['ASM']}' tidak ada di daftar")
        if pd.notna(row.get("Region")) and row["Region"] not in region_options:
            warnings.append(f"Baris {n}: Region '{row['Region']}' tidak dikenali")

        kode = str(row.get("Kode Distributor", "")).strip()
        if kode and kode not in distributor_map:
            errors.append(f"Baris {n}: Kode Distributor '{kode}' tidak valid")

        for col, opts, label in [
            ("Status Salesman",    STATUS_OPTIONS,    "Mix/Eksklusif"),
            ("Jenis Kelamin",      GENDER_OPTIONS,    "Male/Female"),
            ("Pendidikan Terakhir",EDUCATION_OPTIONS, "/".join(EDUCATION_OPTIONS)),
        ]:
            val = row.get(col, "")
            if pd.notna(val) and str(val).strip() and val not in opts:
                errors.append(f"Baris {n}: '{col}' tidak valid - harus {label}")

        for nc in ["Total Outlet Coverage PJP", "Gaji Pokok", "Tunjangan dan insentif",
                   "Pengalaman di Perusahaan Sebelumnya (Dalam Bulan)"]:
            val = row.get(nc, "")
            if pd.notna(val) and str(val).strip():
                try:    float(str(val).replace(",", ""))
                except: errors.append(f"Baris {n}: '{nc}' harus berupa angka")

        hp = str(row.get("No. HP", "")).strip().rstrip(".0")
        if hp:
            hp_clean = hp
            if hp_clean.startswith("+62"):
                hp_clean = hp_clean[3:]
            elif hp_clean.startswith("62"):
                hp_clean = hp_clean[2:]
            if not hp_clean.isdigit():
                warnings.append(f"Baris {n}: No. HP '{hp}' harus hanya berisi angka (8-15 digit)")
            elif len(hp_clean) < 8 or len(hp_clean) > 13:
                warnings.append(f"Baris {n}: No. HP '{hp}' tidak valid (8-15 digit) - panjang {len(hp_clean)} digit")
            elif hp_clean.startswith("0"):
                warnings.append(f"Baris {n}: No. HP '{hp}' sebaiknya dimulai dengan '08' atau '+62'")

    return errors, warnings


def validate_pjp_df(df, distributor_map, store_df=None):
    errors, warnings = [], []
    missing = [c for c in PJP_REQUIRED if c not in df.columns]
    if missing:
        errors.append(f"Kolom wajib tidak ditemukan: {', '.join(missing)}")
        return errors, warnings

    errors += validate_row_completeness(df, PJP_REQUIRED, "PJP")

    unique_dist = _get_unique_distributors(df)
    if len(unique_dist) > 1:
        errors.append(
            f"Sheet PJP Template hanya boleh berisi 1 kode distributor per file. "
            f"Ditemukan {len(unique_dist)} kode: {', '.join(unique_dist)}"
        )
        return errors, warnings

    valid_store_codes = set()
    if store_df is not None and not store_df.empty:
        valid_store_codes = set(store_df["store_code"].dropna().tolist())

    for i, row in df.iterrows():
        n = i + 4
        kode = str(row.get("Kode Distributor", "")).strip()
        if kode and kode not in distributor_map:
            errors.append(f"Baris {n}: Kode Distributor '{kode}' tidak valid")

        kode_toko = str(row.get("Kode Toko", "")).strip()
        if kode_toko and valid_store_codes and kode_toko not in valid_store_codes:
            warnings.append(f"Baris {n}: Kode Toko '{kode_toko}' tidak ditemukan di master store")

        for col, opts in [
            ("Hari",                                              DAY_OPTIONS),
            ("Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap", WEEK_OPTIONS),
            ("Frekuensi",                                         FREQUENCY_OPTIONS),
        ]:
            val = row.get(col, "")
            if pd.notna(val) and str(val).strip() and val not in opts:
                errors.append(f"Baris {n}: '{col}' nilai tidak valid")

    return errors, warnings


def read_template_sheet(uploaded_file, sheet_name, header_row, distributor_map, store_df=None):
    df = pd.read_excel(uploaded_file, sheet_name=sheet_name, header=header_row)
    df = df.dropna(how="all")

    if "Hari" in df.columns:
        df["Hari"] = df["Hari"].astype(str).str.strip().str.title()

    if "Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap" in df.columns:
        df["Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap"] = (
            df["Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap"]
            .astype(str).str.strip().str.title()
        )

    if "Frekuensi" in df.columns:
        df["Frekuensi"] = df["Frekuensi"].astype(str).str.strip().str.upper()

    name_to_code = {v: k for k, v in distributor_map.items()}
    if "Nama Distributor" in df.columns:
        df["Kode Distributor"] = df["Nama Distributor"].apply(
            lambda x: name_to_code.get(str(x).strip(), "") if pd.notna(x) else ""
        )

    if store_df is not None and "Kode Toko" in df.columns:
        code_to_name = dict(zip(store_df["store_code"], store_df["store_name"]))
        df["Nama Toko"] = df["Kode Toko"].apply(
            lambda x: code_to_name.get(str(x).strip(), "") if pd.notna(x) else ""
        )

    if "No. HP" in df.columns:
        df["No. HP"] = df["No. HP"].apply(normalize_phone_id)

    return df


# ─── BigQuery writer ──────────────────────────────────────────────────────────

_SAL_COL_MAP = {
    "Nama Salesman":                                         "nama_salesman",
    "Nama SPV External":                                     "nama_spv_external",
    "Nama SPV Internal":                                     "nama_spv_internal",
    "ASM":                                                   "asm",
    "Region":                                                "region",
    "Nama Distributor":                                      "nama_distributor",
    "Kode Distributor":                                      "kode_distributor",
    "Status Salesman":                                       "status_salesman",
    "Total Outlet Coverage PJP":                             "total_outlet_coverage_pjp",
    "Gaji Pokok":                                            "gaji_pokok",
    "Tunjangan dan insentif":                                "tunjangan_dan_insentif",
    "Tanggal Lahir":                                         "tanggal_lahir",
    "Jenis Kelamin":                                         "jenis_kelamin",
    "Pendidikan Terakhir":                                   "pendidikan_terakhir",
    "Pengalaman di Perusahaan Sebelumnya (Dalam Bulan)":     "pengalaman_bulan",
    "Principal Lain yang Ditanggungjawabi":                  "principal_lain",
    "No. HP":                                                "no_hp",
    "Tanggal Join di G2G":                                   "tanggal_join_g2g",
}

_PJP_COL_MAP = {
    "ASM":                                               "asm",
    "Region":                                            "region",
    "Nama Distributor":                                  "nama_distributor",
    "Kode Distributor":                                  "kode_distributor",
    "Nama Salesman":                                     "nama_salesman",
    "Kode Toko":                                         "kode_toko",
    "Nama Toko":                                         "nama_toko",
    "Hari":                                              "hari",
    "Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap":  "minggu",
    "Frekuensi":                                         "frekuensi",
}


def push_to_bigquery(df, col_map, table_id) -> tuple[bool, str]:
    try:
        credentials, project_id = get_credentials()
        client = bigquery.Client(credentials=credentials, project=project_id)
        existing_cols = {c: col_map[c] for c in col_map if c in df.columns}
        bq_df = df[list(existing_cols.keys())].rename(columns=existing_cols).copy()
        bq_df["uploaded_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        job = client.load_table_from_dataframe(bq_df, table_id, job_config=job_config)
        job.result()
        return True, f"Berhasil menyimpan {len(bq_df)} baris ke Database."
    except Exception as e:
        return False, f"Gagal menyimpan ke Database: {e}"


# ─── Shared salesman form fields ──────────────────────────────────────────────

def _render_salesman_form_fields(key_prefix: str):
    c1, c2 = st.columns(2)
    with c1:
        nama       = st.text_input("Nama Salesman *", key=f"{key_prefix}_nama")
        spv_ext    = st.text_input("Nama SPV External", key=f"{key_prefix}_spv_ext")
        spv_int    = st.text_input("Nama SPV Internal *", key=f"{key_prefix}_spv_int")
        status_sal = st.selectbox("Status Salesman *", STATUS_OPTIONS, key=f"{key_prefix}_status")
        outlet_cov = st.number_input("Total Outlet Coverage PJP *", min_value=0, step=1, key=f"{key_prefix}_outlet")
        gaji       = st.number_input("Gaji Pokok (Rp) *", min_value=0, step=1000, key=f"{key_prefix}_gaji")
        tunjangan  = st.number_input("Tunjangan dan Insentif (Rp) *", min_value=0, step=1000, key=f"{key_prefix}_tunj")
    with c2:
        tgl_lahir  = st.date_input("Tanggal Lahir *", key=f"{key_prefix}_lahir")
        gender     = st.selectbox("Jenis Kelamin *", GENDER_OPTIONS, key=f"{key_prefix}_gender")
        pendidikan = st.selectbox("Pendidikan Terakhir *", EDUCATION_OPTIONS, key=f"{key_prefix}_pendidikan")
        pengalaman = st.number_input("Pengalaman Sebelumnya (bulan) *", min_value=0, step=1, key=f"{key_prefix}_exp")
        principal  = st.text_input("Principal Lain (opsional)", key=f"{key_prefix}_principal")
        no_hp      = st.text_input("No. HP *", placeholder="08123456789", key=f"{key_prefix}_hp")
        tgl_join   = st.date_input("Tanggal Join di G2G *", key=f"{key_prefix}_join")
    return {
        "nama": nama, "spv_ext": spv_ext, "spv_int": spv_int,
        "status_sal": status_sal, "outlet_cov": outlet_cov, "gaji": gaji,
        "tunjangan": tunjangan, "tgl_lahir": tgl_lahir, "gender": gender,
        "pendidikan": pendidikan, "pengalaman": pengalaman, "principal": principal,
        "no_hp": no_hp, "tgl_join": tgl_join,
    }


def _build_salesman_data(fields, dist_df, selected_dist_code, selected_dist_name) -> dict:
    hp_norm = normalize_phone_id(fields["no_hp"])
    return {
        "nama_salesman":             sanitize_salesman_name(fields["nama"]),
        "nama_spv_external":         fields["spv_ext"].strip().upper() if fields["spv_ext"].strip() else None,
        "nama_spv_internal":         fields["spv_int"].strip().upper(),
        "asm":                       dist_df.loc[dist_df["distributor_code"] == selected_dist_code, "asm"].iloc[0],
        "region":                    dist_df.loc[dist_df["distributor_code"] == selected_dist_code, "region"].iloc[0],
        "nama_distributor":          selected_dist_name,
        "kode_distributor":          selected_dist_code,
        "status_salesman":           fields["status_sal"],
        "total_outlet_coverage_pjp": int(fields["outlet_cov"]),
        "gaji_pokok":                float(fields["gaji"]),
        "tunjangan_dan_insentif":    float(fields["tunjangan"]),
        "tanggal_lahir":             str(fields["tgl_lahir"]),
        "jenis_kelamin":             fields["gender"],
        "pendidikan_terakhir":       fields["pendidikan"],
        "pengalaman_bulan":          int(fields["pengalaman"]),
        "principal_lain":            fields["principal"].strip() if fields["principal"].strip() else None,
        "no_hp":                     hp_norm,
        "tanggal_join_g2g":          str(fields["tgl_join"]),
    }


def _validate_salesman_fields(fields) -> list:
    errors = []
    if not fields["nama"].strip():    errors.append("Nama Salesman wajib diisi.")
    if not fields["spv_int"].strip(): errors.append("Nama SPV Internal wajib diisi.")
    if not fields["no_hp"].strip():   errors.append("No. HP wajib diisi.")
    return errors


# ══════════════════════════════════════════════════════════════════════════════
# NAVIGATION
# ══════════════════════════════════════════════════════════════════════════════

PAGES = {
    "👥 Kelola Salesman":    "salesman",
    "📋 Salesman Template":  "salesman_template",
    "🗓️ PJP Template":       "pjp_template",
}

# Sidebar navigation
with st.sidebar:
    st.title("📋 G2G Template Manager")
    st.markdown("---")
    selected_page = st.radio(
        "Navigasi",
        list(PAGES.keys()),
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.caption("Salesman & PJP Template Manager · G2G")

# ─── Load shared data ─────────────────────────────────────────────────────────

try:
    dist_df  = load_distributor_data()
    store_df = load_store_data()
    distributor_map, asm_options, region_options = build_lookup_tables(dist_df)
except Exception as e:
    st.error(f"Gagal memuat data dari Database: {e}")
    st.stop()

# ─── Distributor selector (shared across pages, stored in session_state) ──────

dist_labels = [
    f"{row['distributor_code']} — {row['distributor_name']}"
    for _, row in dist_df.sort_values("distributor_name").iterrows()
]
dist_code_from_label = {
    f"{row['distributor_code']} — {row['distributor_name']}": row["distributor_code"]
    for _, row in dist_df.iterrows()
}

with st.sidebar:
    st.markdown("### 🏢 Pilih Distributor")
    selected_label = st.selectbox(
        "Distributor",
        ["— Pilih distributor —"] + dist_labels,
        key="dist_selector",
        label_visibility="collapsed",
    )

if selected_label == "— Pilih distributor —":
    st.title("📋 Salesman & PJP Template Manager")
    st.info("👈 Pilih distributor di sidebar untuk melanjutkan.")
    st.stop()

selected_dist_code = dist_code_from_label[selected_label]
selected_dist_name = dist_df.loc[
    dist_df["distributor_code"] == selected_dist_code, "distributor_name"
].iloc[0]

with st.sidebar:
    st.success(f"**{selected_dist_name}**\n\n`{selected_dist_code}`")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: KELOLA SALESMAN
# ══════════════════════════════════════════════════════════════════════════════

if PAGES[selected_page] == "salesman":
    st.title("👥 Kelola Salesman")
    st.caption(f"Distributor: **{selected_dist_name}** ({selected_dist_code})")

    if "action_mode" not in st.session_state:
        st.session_state.action_mode = None

    with st.spinner("Memuat daftar salesman..."):
        salesman_df = get_salesman_list(selected_dist_code)

    col_search, col_filter, col_refresh = st.columns([3, 2, 1])
    with col_search:
        search_query = st.text_input(
            "🔍 Cari salesman:", key="search_salesman", placeholder="Nama atau ID salesman..."
        )
    with col_filter:
        filter_status = st.selectbox("Filter status:", ["Semua", "Aktif", "Tidak Aktif"], key="filter_status")
    with col_refresh:
        st.write("")
        if st.button("🔄 Refresh", use_container_width=True):
            st.cache_data.clear()
            st.session_state.action_mode = None
            st.rerun()

    if not salesman_df.empty:
        display_df = salesman_df.copy()

        if search_query.strip():
            q = search_query.strip().upper()
            mask = (
                display_df["salesman_id"].astype(str).str.upper().str.contains(q, na=False) |
                display_df["nama_salesman"].astype(str).str.upper().str.contains(q, na=False)
            )
            display_df = display_df[mask]

        if filter_status == "Aktif" and "is_active" in display_df.columns:
            display_df = display_df[display_df["is_active"] == True]
        elif filter_status == "Tidak Aktif" and "is_active" in display_df.columns:
            display_df = display_df[display_df["is_active"] == False]

        st.caption(f"Menampilkan **{len(display_df)}** salesman.")

        hcols = st.columns([1.2, 2.2, 1.2, 1.4, 1.2, 1.2, 0.9, 0.9, 0.9])
        headers = ["ID Salesman", "Nama", "Tipe", "No. HP", "Region", "ASM", "", "", ""]
        for hc, ht in zip(hcols, headers):
            hc.markdown(f"**{ht}**")
        st.divider()

        for _, row in display_df.iterrows():
            sal_id    = row["salesman_id"]
            is_active = row.get("is_active", True)

            rcols = st.columns([1.2, 2.2, 1.2, 1.4, 1.2, 1.2, 0.9, 0.9, 0.9])
            id_label = f"🟢 {sal_id}" if is_active else f"🔴 {sal_id}"
            rcols[0].markdown(id_label)
            rcols[1].markdown(row.get("nama_salesman", "-"))
            rcols[2].markdown(f"`{row.get('salesman_type', '-')}`")
            rcols[3].markdown(row.get("no_hp", "-") or "-")
            rcols[4].markdown(row.get("region", "-") or "-")
            rcols[5].markdown(row.get("asm", "-") or "-")

            if is_active:
                if rcols[6].button("✏️ Ganti", key=f"rep_{sal_id}", use_container_width=True):
                    st.session_state.action_mode = None if st.session_state.action_mode == ("replace", sal_id) else ("replace", sal_id)
                    st.rerun()
                if rcols[7].button("❌ Nonaktif", key=f"deact_{sal_id}", use_container_width=True):
                    st.session_state.action_mode = None if st.session_state.action_mode == ("deactivate", sal_id) else ("deactivate", sal_id)
                    st.rerun()
                rcols[8].markdown("—")
            else:
                rcols[6].markdown("—")
                rcols[7].markdown("—")
                if rcols[8].button("♻️ Aktifkan", key=f"react_{sal_id}", use_container_width=True):
                    st.session_state.action_mode = None if st.session_state.action_mode == ("reactivate", sal_id) else ("reactivate", sal_id)
                    st.rerun()

            # ── Inline Replace Panel ──────────────────────────────────────────
            if st.session_state.action_mode == ("replace", sal_id):
                with st.container(border=True):
                    st.markdown(f"#### 🔄 Ganti Salesman — `{sal_id}`")
                    st.info(
                        f"Mengganti: **{row.get('nama_salesman', '-')}** | Tipe: `{row.get('salesman_type', '-')}`\n\n"
                        "Mapping lama akan dinonaktifkan, lalu mapping baru dibuat dengan kode yang sama."
                    )
                    with st.form(f"form_replace_{sal_id}"):
                        st.markdown("**Data Salesman Pengganti**")
                        fields_r = _render_salesman_form_fields(f"rep_{sal_id}")
                        submitted_rep = st.form_submit_button("🔄 Simpan Penggantian", type="primary")

                    if submitted_rep:
                        errs = _validate_salesman_fields(fields_r)
                        if errs:
                            for e in errs: st.error(e)
                        else:
                            sal_data_r = _build_salesman_data(fields_r, dist_df, selected_dist_code, selected_dist_name)
                            with st.spinner("Menyimpan..."):
                                ok1, err1 = insert_salesman_record(sal_data_r)
                            if not ok1:
                                st.error(f"Gagal menyimpan data salesman: {err1}")
                            else:
                                with st.spinner("Menonaktifkan mapping lama..."):
                                    ok2, err2 = deactivate_previous_mapping(sal_id)
                                if not ok2:
                                    st.error(f"Data baru tersimpan, tapi gagal menonaktifkan mapping lama: {err2}")
                                else:
                                    with st.spinner("Membuat mapping baru..."):
                                        ok3, err3 = insert_mapping_record(
                                            sal_id, selected_dist_code,
                                            str(row.get("salesman_type", "")),
                                            nama_salesman=sanitize_salesman_name(fields_r["nama"]),
                                        )
                                    if not ok3:
                                        st.error(f"Mapping lama dinonaktifkan, tapi gagal membuat mapping baru: {err3}")
                                    else:
                                        st.success(f"✅ Salesman berhasil diganti! Kode `{sal_id}` kini dipegang oleh **{fields_r['nama'].strip().upper()}**.")
                                        st.session_state.action_mode = None
                                        st.cache_data.clear()
                                        st.rerun()

            # ── Inline Deactivate Panel ───────────────────────────────────────
            if st.session_state.action_mode == ("deactivate", sal_id):
                with st.container(border=True):
                    st.markdown(f"#### ❌ Non-Aktifkan — `{sal_id}`")
                    st.warning(
                        f"Anda akan menonaktifkan **{row.get('nama_salesman', sal_id)}**. "
                        "Tindakan ini akan menandai mapping sebagai tidak aktif."
                    )
                    dcols = st.columns([3, 1])
                    confirm = dcols[0].checkbox(f"Saya konfirmasi ingin menonaktifkan salesman ini", key=f"confirm_deact_{sal_id}")
                    if dcols[1].button("❌ Non-Aktifkan", key=f"do_deact_{sal_id}", type="primary", disabled=not confirm, use_container_width=True):
                        with st.spinner("Menonaktifkan salesman..."):
                            ok_d, err_d = deactivate_salesman_mapping(sal_id)
                        if ok_d:
                            st.success(f"✅ Salesman **{row.get('nama_salesman', sal_id)}** (`{sal_id}`) berhasil dinonaktifkan.")
                            st.session_state.action_mode = None
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(f"Gagal menonaktifkan salesman: {err_d}")

            # ── Inline Reactivate Panel ───────────────────────────────────────
            if st.session_state.action_mode == ("reactivate", sal_id):
                with st.container(border=True):
                    st.markdown(f"#### ♻️ Aktifkan Kembali — `{sal_id}`")
                    st.info(
                        f"Kode **`{sal_id}`** saat ini **tidak aktif** (pemegang sebelumnya: "
                        f"*{row.get('nama_salesman', '-')}*). "
                        "Isi data salesman baru yang akan menggunakan kode ini."
                    )
                    with st.form(f"form_reactivate_{sal_id}"):
                        st.markdown("**Data Salesman Baru (menggunakan kode yang sama)**")
                        fields_ra = _render_salesman_form_fields(f"react_{sal_id}")
                        submitted_ra = st.form_submit_button("♻️ Aktifkan & Simpan", type="primary")

                    if submitted_ra:
                        errs = _validate_salesman_fields(fields_ra)
                        if errs:
                            for e in errs: st.error(e)
                        else:
                            sal_data_ra = _build_salesman_data(fields_ra, dist_df, selected_dist_code, selected_dist_name)
                            with st.spinner("Menyimpan..."):
                                ok1, err1 = insert_salesman_record(sal_data_ra)
                            if not ok1:
                                st.error(f"Gagal menyimpan data salesman: {err1}")
                            else:
                                with st.spinner("Membuat mapping aktif baru..."):
                                    ok2, err2 = reactivate_salesman_mapping(
                                        sal_id, selected_dist_code,
                                        str(row.get("salesman_type", "")),
                                        nama_salesman=sanitize_salesman_name(fields_ra["nama"]),
                                    )
                                if not ok2:
                                    st.error(f"Data salesman tersimpan, tapi gagal membuat mapping aktif: {err2}")
                                else:
                                    st.success(f"✅ Kode `{sal_id}` berhasil diaktifkan kembali dan kini dipegang oleh **{fields_ra['nama'].strip().upper()}**.")
                                    st.session_state.action_mode = None
                                    st.cache_data.clear()
                                    st.rerun()

            st.divider()

    else:
        st.info("Belum ada data salesman untuk distributor ini.")

    # ── Bottom action buttons ─────────────────────────────────────────────────
    st.markdown("---")
    if "show_add_form" not in st.session_state:
        st.session_state.show_add_form = False
    if "show_vacant_form" not in st.session_state:
        st.session_state.show_vacant_form = False

    btn_col1, btn_col2 = st.columns(2)
    with btn_col1:
        add_btn_label = "➕ Tambah Salesman Baru" if not st.session_state.show_add_form else "✖ Tutup Form Tambah"
        if st.button(add_btn_label, type="primary", use_container_width=True):
            st.session_state.show_add_form = not st.session_state.show_add_form
            st.session_state.show_vacant_form = False
            st.session_state.action_mode = None
            st.rerun()
    with btn_col2:
        vacant_btn_label = "♻️ Aktifkan Kode Vakant" if not st.session_state.show_vacant_form else "✖ Tutup Form Aktifkan"
        if st.button(vacant_btn_label, use_container_width=True):
            st.session_state.show_vacant_form = not st.session_state.show_vacant_form
            st.session_state.show_add_form = False
            st.session_state.action_mode = None
            st.rerun()

    # ── Add New Salesman Form ─────────────────────────────────────────────────
    if st.session_state.show_add_form:
        with st.container(border=True):
            st.subheader("➕ Tambah Salesman Baru")
            st.info(
                "Sistem akan otomatis membuat **Salesman ID** baru berdasarkan:\n"
                "`Tipe Salesman + Kode Distributor + Nomor Urut`"
            )
            salesman_type_add = st.selectbox("Tipe Salesman *", SALESMAN_TYPES, key="add_type_bottom")
            preview_id = generate_salesman_id(selected_dist_code, salesman_type_add)
            st.info(f"ID yang akan dibuat: **`{preview_id}`**")

            with st.form("form_add_salesman_bottom"):
                st.markdown("**Data Salesman**")
                fields_add = _render_salesman_form_fields("add_bottom")
                submitted_add = st.form_submit_button("✅ Simpan Salesman Baru", type="primary")

            if submitted_add:
                errs = _validate_salesman_fields(fields_add)
                if errs:
                    for e in errs: st.error(e)
                else:
                    sal_data_add = _build_salesman_data(fields_add, dist_df, selected_dist_code, selected_dist_name)
                    salesman_id_new = generate_salesman_id(selected_dist_code, salesman_type_add)
                    with st.spinner("Menyimpan data salesman..."):
                        ok1, err1 = insert_salesman_record(sal_data_add)
                    if not ok1:
                        st.error(f"Gagal menyimpan ke tabel salesman: {err1}")
                    else:
                        with st.spinner("Membuat mapping salesman..."):
                            ok2, err2 = insert_mapping_record(
                                salesman_id_new, selected_dist_code, salesman_type_add,
                                nama_salesman=sanitize_salesman_name(fields_add["nama"]),
                            )
                        if not ok2:
                            st.error(f"Data salesman tersimpan, tapi gagal membuat mapping: {err2}")
                        else:
                            st.success(f"✅ Salesman baru berhasil ditambahkan!\n\n**ID Salesman: `{salesman_id_new}`**")
                            st.session_state.show_add_form = False
                            st.cache_data.clear()
                            st.rerun()

    # ── Reactivate Vacant Code Form ───────────────────────────────────────────
    if st.session_state.show_vacant_form:
        with st.container(border=True):
            st.subheader("♻️ Aktifkan Kembali Kode Salesman Vakant")
            st.info(
                "Kode **vakant** adalah ID salesman yang pernah digunakan tapi kini tidak aktif. "
                "Anda dapat menggunakan kembali kode yang sama untuk salesman baru."
            )
            with st.spinner("Memuat daftar kode vakant..."):
                vacant_df = get_vacant_salesman_ids(selected_dist_code)

            if vacant_df.empty:
                st.success("✅ Tidak ada kode salesman vakant untuk distributor ini.")
            else:
                vacant_options = {
                    f"{r['salesman_id']} [{r['salesman_type']}] — terakhir: {r.get('last_salesman', '-')}": r
                    for _, r in vacant_df.iterrows()
                }
                selected_vacant_label = st.selectbox("Pilih kode vakant:", list(vacant_options.keys()), key="vacant_selector")
                selected_vacant = vacant_options[selected_vacant_label]
                vacant_id   = selected_vacant["salesman_id"]
                vacant_type = selected_vacant["salesman_type"]

                st.markdown(
                    f"Kode dipilih: **`{vacant_id}`** | Tipe: `{vacant_type}` | "
                    f"Terakhir dipakai oleh: *{selected_vacant.get('last_salesman', '-')}*"
                )

                with st.form("form_reactivate_vacant"):
                    st.markdown("**Data Salesman Baru untuk Kode Ini**")
                    fields_rv = _render_salesman_form_fields("vacant_react")
                    submitted_rv = st.form_submit_button("♻️ Aktifkan & Simpan", type="primary")

                if submitted_rv:
                    errs = _validate_salesman_fields(fields_rv)
                    if errs:
                        for e in errs: st.error(e)
                    else:
                        sal_data_rv = _build_salesman_data(fields_rv, dist_df, selected_dist_code, selected_dist_name)
                        with st.spinner("Menyimpan data salesman baru..."):
                            ok1, err1 = insert_salesman_record(sal_data_rv)
                        if not ok1:
                            st.error(f"Gagal menyimpan data salesman: {err1}")
                        else:
                            with st.spinner("Membuat mapping aktif baru..."):
                                ok2, err2 = reactivate_salesman_mapping(
                                    vacant_id, selected_dist_code, vacant_type,
                                    nama_salesman=sanitize_salesman_name(fields_rv["nama"]),
                                )
                            if not ok2:
                                st.error(f"Data salesman tersimpan, tapi gagal membuat mapping aktif: {err2}")
                            else:
                                st.success(f"✅ Kode **`{vacant_id}`** berhasil diaktifkan kembali dan kini dipegang oleh **{fields_rv['nama'].strip().upper()}**.")
                                st.session_state.show_vacant_form = False
                                st.cache_data.clear()
                                st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: SALESMAN TEMPLATE
# ══════════════════════════════════════════════════════════════════════════════

elif PAGES[selected_page] == "salesman_template":
    st.title("📋 Salesman Template")
    st.caption(f"Distributor: **{selected_dist_name}** ({selected_dist_code})")

    tab_download, tab_upload = st.tabs(["📥 Download Template", "📤 Upload & Validasi"])

    with tab_download:
        st.subheader("📥 Download Salesman Template")

        with st.expander("📖 Panduan Pengisian", expanded=False):
            st.markdown("""
            ### ⚡ ATURAN DASAR:
            - **1 file = 1 distributor** (tidak boleh campur)
            - **Semua kolom "Wajib Diisi" harus terisi**
            - **Jangan edit kolom "Kode Distributor"** (otomatis)

            ### 🔄 URUTAN DROPDOWN BERTINGKAT (WAJIB!):
            1. **ASM** → 2. **Region** → 3. **Nama Distributor**

            ### ✅ FORMAT DATA YANG BENAR:
            - **Tanggal**: Format YYYY-MM-DD (contoh: 2001-01-25)
            - **Angka**: Hanya angka (contoh: 5000000)
            - **No. HP**: 8-13 digit, contoh: `08123456789`

            ### 📞 BUTUH BANTUAN?
            Hubungi tim support G2G
            """)

        sal_excel = create_salesman_excel(
            pd.DataFrame(), distributor_map, asm_options, region_options, dist_df
        )
        st.download_button(
            "⬇️ Download Salesman Template",
            data=sal_excel.getvalue(),
            file_name=f"Salesman_Template_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary",
        )

    with tab_upload:
        st.subheader("📤 Upload & Validasi Salesman Template")
        st.warning("""
        ⚠️ **PERHATIAN:**
        - 1 file = 1 distributor
        - **Data TIDAK BISA diupload jika masih ada error ATAU peringatan**
        - Perbaiki semua masalah di file Excel, lalu upload ulang
        """)

        uploaded = st.file_uploader("Pilih file Excel (.xlsx)", type=["xlsx"], key="sal_uploader")

        if uploaded:
            try:
                xl = pd.ExcelFile(uploaded)
            except Exception as e:
                st.error(f"Gagal membaca file: {e}")
                st.stop()

            if "Salesman Template" not in xl.sheet_names:
                st.error("Sheet 'Salesman Template' tidak ditemukan. Gunakan template resmi.")
                st.stop()

            st.success("Sheet `Salesman Template` ditemukan.")

            try:
                sal_df = read_template_sheet(uploaded, "Salesman Template", 2, distributor_map)
                sal_df = sal_df[sal_df["Nama Salesman"].notna() & (sal_df["Nama Salesman"] != "")]
                sal_df = sal_df.reset_index(drop=True)
            except Exception as e:
                st.error(f"Gagal membaca sheet: {e}")
                st.stop()

            if sal_df.empty:
                st.warning("Tidak ada data di sheet Salesman Template.")
                st.stop()

            with st.expander("👁️ Preview Data", expanded=True):
                st.dataframe(sal_df, use_container_width=True, hide_index=True)

            sal_errors, sal_warnings = validate_salesman_df(sal_df, distributor_map, asm_options, region_options)

            if sal_errors or sal_warnings:
                if sal_errors:
                    st.error(f"**❌ {len(sal_errors)} ERROR:**")
                    for e in sal_errors: st.markdown(f"- {e}")
                if sal_warnings:
                    st.warning(f"**⚠️ {len(sal_warnings)} PERINGATAN:**")
                    for w in sal_warnings: st.markdown(f"- {w}")
            else:
                st.success("✅ Validasi berhasil! Data siap diupload.")

                st.markdown("---")
                st.subheader("☁️ Upload ke Database")

                salesman_type_bulk = st.selectbox("Tipe Salesman untuk batch ini *", SALESMAN_TYPES, key="bulk_sal_type")
                if st.button("☁️ Simpan Salesman ke Database", key="bq_sal", type="primary"):
                    sal_df_upload = sal_df.copy()
                    sal_df_upload["Nama Salesman"] = sal_df_upload["Nama Salesman"].apply(
                        lambda x: sanitize_salesman_name(x) if pd.notna(x) else x
                    )
                    with st.spinner("Menyimpan data Salesman ke Database..."):
                        ok, msg = push_to_bigquery(sal_df_upload, _SAL_COL_MAP, SALESMAN_TABLE)
                    if ok:
                        mapping_errors = []
                        for _, row in sal_df_upload.iterrows():
                            sal_name = sanitize_salesman_name(row.get("Nama Salesman", ""))
                            new_id   = generate_salesman_id(selected_dist_code, salesman_type_bulk)
                            ok_m, err_m = insert_mapping_record(
                                new_id, selected_dist_code, salesman_type_bulk,
                                nama_salesman=sal_name,
                            )
                            if not ok_m:
                                mapping_errors.append(f"{sal_name}: {err_m}")
                        if mapping_errors:
                            st.warning(f"Data tersimpan tapi ada {len(mapping_errors)} mapping gagal dibuat.")
                        else:
                            st.success(f"✅ Berhasil menyimpan {len(sal_df_upload)} salesman + mapping ke Database.")
                        st.cache_data.clear()
                    else:
                        st.error(msg)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: PJP TEMPLATE
# ══════════════════════════════════════════════════════════════════════════════

elif PAGES[selected_page] == "pjp_template":
    st.title("🗓️ PJP Template")
    st.caption(f"Distributor: **{selected_dist_name}** ({selected_dist_code})")

    tab_download, tab_upload = st.tabs(["📥 Download Template", "📤 Upload & Validasi"])

    with tab_download:
        st.subheader("📥 Download PJP Template")

        with st.expander("📖 Panduan Pengisian", expanded=False):
            st.markdown("""
            ### ⚡ ATURAN DASAR:
            - **1 file = 1 distributor** (tidak boleh campur)
            - **Semua kolom "Wajib Diisi" harus terisi**
            - **Jangan edit kolom "Kode Distributor" dan "Nama Toko"** (otomatis)

            ### 🔄 URUTAN DROPDOWN BERTINGKAT (WAJIB!):
            1. **ASM** → 2. **Region** → 3. **Nama Distributor** → 4. **Kode Toko**

            ### ✅ FORMAT DATA YANG BENAR:
            - **Frekuensi PJP**: F4+ / F4 / F2 / F1
            - **Hari**: Pilih dari dropdown
            - **Minggu**: Pilih Ganjil / Genap / Ganjil+Genap

            ### 📞 BUTUH BANTUAN?
            Hubungi tim support G2G
            """)

        pjp_excel = create_pjp_excel(
            pd.DataFrame(columns=[c for c, _, _ in PJP_COLS]),
            distributor_map, dist_df, store_df,
        )
        st.download_button(
            "⬇️ Download PJP Template",
            data=pjp_excel.getvalue(),
            file_name=f"PJP_Template_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary",
        )

    with tab_upload:
        st.subheader("📤 Upload & Validasi PJP Template")
        st.warning("""
        ⚠️ **PERHATIAN:**
        - 1 file = 1 distributor
        - **Data TIDAK BISA diupload jika masih ada error ATAU peringatan**
        - Perbaiki semua masalah di file Excel, lalu upload ulang
        """)

        uploaded = st.file_uploader("Pilih file Excel (.xlsx)", type=["xlsx"], key="pjp_uploader")

        if uploaded:
            try:
                xl = pd.ExcelFile(uploaded)
            except Exception as e:
                st.error(f"Gagal membaca file: {e}")
                st.stop()

            if "PJP Template" not in xl.sheet_names:
                st.error("Sheet 'PJP Template' tidak ditemukan. Gunakan template resmi.")
                st.stop()

            st.success("Sheet `PJP Template` ditemukan.")

            try:
                pjp_df = read_template_sheet(uploaded, "PJP Template", 2, distributor_map, store_df)
                pjp_df = pjp_df[pjp_df["Nama Distributor"].notna() & (pjp_df["Nama Distributor"] != "")]
                pjp_df = pjp_df.reset_index(drop=True)
            except Exception as e:
                st.error(f"Gagal membaca sheet: {e}")
                st.stop()

            if pjp_df.empty:
                st.warning("Tidak ada data di sheet PJP Template.")
                st.stop()

            c1, c2 = st.columns([3, 1])
            c1.metric("Total Baris", len(pjp_df))
            c2.metric("Total Kolom", len(pjp_df.columns))

            with st.expander("👁️ Preview Data", expanded=True):
                st.dataframe(pjp_df, use_container_width=True, hide_index=True)

            pjp_errors, pjp_warnings = validate_pjp_df(pjp_df, distributor_map, store_df)

            if pjp_errors or pjp_warnings:
                if pjp_errors:
                    st.error(f"**❌ {len(pjp_errors)} ERROR:**")
                    for e in pjp_errors: st.markdown(f"- {e}")
                if pjp_warnings:
                    st.warning(f"**⚠️ {len(pjp_warnings)} PERINGATAN:**")
                    for w in pjp_warnings: st.markdown(f"- {w}")
            else:
                st.success("✅ Validasi berhasil! Data siap diupload.")

                st.markdown("---")
                st.subheader("☁️ Upload ke Database")

                if st.button("☁️ Simpan PJP ke Database", key="bq_pjp", type="primary"):
                    with st.spinner("Menyimpan data PJP ke Database..."):
                        ok, msg = push_to_bigquery(pjp_df, _PJP_COL_MAP, PJP_TABLE)
                    if ok:
                        st.success(f"✅ Berhasil menyimpan {len(pjp_df)} baris data PJP ke Database.")
                    else:
                        st.error(msg)
