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
    df = df.drop_duplicates(subset=["store_label"]).reset_index(drop=True)
    return df


def build_lookup_tables(dist_df: pd.DataFrame):
    distributor_map = dict(zip(dist_df["distributor_code"], dist_df["distributor_name"]))
    asm_options     = sorted(dist_df["asm"].dropna().unique().tolist())
    region_options  = sorted(dist_df["region"].dropna().unique().tolist())
    return distributor_map, asm_options, region_options


# ─── Static option lists ──────────────────────────────────────────────────────

STATUS_OPTIONS    = ["Mix", "Eksklusif"]
GENDER_OPTIONS    = ["Male", "Female"]
EDUCATION_OPTIONS = ["SD", "SMP", "SMA", "S1", "S2"]
DAY_OPTIONS       = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu"]
WEEK_OPTIONS      = ["Minggu Ganjil", "Minggu Genap", "Minggu Ganjil + Genap"]
FREQUENCY_OPTIONS = ["F4", "F2", "F1"]

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
    ("Toko",                                                True,  "store_cascade"),
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
    special = [" ", "-", "/", "(", ")", "+", "&", "."]
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

def _build_lookup_and_named_ranges(
    wb: Workbook,
    dist_df: pd.DataFrame,
    store_df: pd.DataFrame | None = None,
) -> None:
    LK = "Lookup"
    lk = wb.create_sheet(LK)
    lk.sheet_state = "hidden"

    asm_list = sorted(dist_df["asm"].dropna().unique().tolist())
    cur_col  = 1

    # Col A: all ASMs
    lk.cell(row=1, column=cur_col, value="__ALL_ASM__")
    for i, asm in enumerate(asm_list, start=2):
        lk.cell(row=i, column=cur_col, value=asm)
    c  = get_column_letter(cur_col)
    nm = _safe_name("ALL_ASM")
    wb.defined_names[nm] = DefinedName(nm, attr_text=f"'{LK}'!${c}$2:${c}${1+len(asm_list)}")
    cur_col += 1

    # One col per ASM: its regions
    for asm in asm_list:
        regions = sorted(dist_df.loc[dist_df["asm"] == asm, "region"].unique().tolist())
        lk.cell(row=1, column=cur_col, value=f"__ASM_{asm}__")
        for i, reg in enumerate(regions, start=2):
            lk.cell(row=i, column=cur_col, value=reg)
        c  = get_column_letter(cur_col)
        nm = _safe_name(asm)
        wb.defined_names[nm] = DefinedName(nm, attr_text=f"'{LK}'!${c}$2:${c}${1+len(regions)}")
        cur_col += 1

    # One col per (ASM, Region): distributor names
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

    # Two-col block: distributor_name | distributor_code (NR_DIST_LOOKUP)
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

    # Store named ranges: one col per distributor_name
    if store_df is not None and not store_df.empty:
        dist_names_with_stores = sorted(store_df["distributor_name"].dropna().unique().tolist())
        for dist_name in dist_names_with_stores:
            labels = sorted(
                store_df.loc[store_df["distributor_name"] == dist_name, "store_label"]
                .dropna().unique().tolist()
            )
            lk.cell(row=1, column=cur_col, value=f"__STORE_{dist_name}__")
            for i, lbl in enumerate(labels, start=2):
                lk.cell(row=i, column=cur_col, value=lbl)
            c  = get_column_letter(cur_col)
            nm = _safe_name(f"STORE_{dist_name}")
            wb.defined_names[nm] = DefinedName(
                nm, attr_text=f"'{LK}'!${c}$2:${c}${1+len(labels)}"
            )
            cur_col += 1


# ─── Attach cascading DVs ─────────────────────────────────────────────────────

def _attach_cascade_dvs(ws, col_names: list, first_data: int, last_data: int):
    def cl(name):
        return get_column_letter(col_names.index(name) + 1)

    def sqref(name):
        c = cl(name)
        return f"{c}{first_data}:{c}{last_data}"

    asm_ref = f"{cl('ASM')}{first_data}"
    reg_ref = f"{cl('Region')}{first_data}"

    # ASM
    dv_asm = DataValidation(
        type="list", formula1=_safe_name("ALL_ASM"), allow_blank=True,
        showInputMessage=True, promptTitle="Langkah 1 - ASM",
        prompt="Pilih nama ASM. Region dan Distributor akan menyesuaikan.",
        showErrorMessage=True, errorTitle="Input Tidak Valid",
        error="Pilih ASM dari daftar.",
    )
    ws.add_data_validation(dv_asm)
    dv_asm.sqref = sqref("ASM")

    # Region
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

    # Nama Distributor
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

    # Toko (PJP only)
    if "Toko" in col_names:
        nama_dist_ref = f"{cl('Nama Distributor')}{first_data}"
        dist_clean    = _indirect_clean(nama_dist_ref)
        dv_store = DataValidation(
            type="list",
            formula1=f'INDIRECT("NR_STORE_"&{dist_clean})',
            allow_blank=True,
            showInputMessage=True, promptTitle="Langkah 4 - Toko",
            prompt="Pilih Toko. Daftar disesuaikan dengan Distributor yang dipilih.",
            showErrorMessage=True, errorTitle="Input Tidak Valid",
            error="Pilih Toko dari daftar. Pastikan Nama Distributor sudah dipilih.",
        )
        ws.add_data_validation(dv_store)
        dv_store.sqref = sqref("Toko")


# ─── Salesman Excel ───────────────────────────────────────────────────────────

def create_salesman_excel(
    df: pd.DataFrame,
    distributor_map: dict,
    asm_options: list,
    region_options: list,
    dist_df: pd.DataFrame,
) -> BytesIO:
    wb = Workbook()
    wb.remove(wb.active)
    _build_lookup_and_named_ranges(wb, dist_df)

    ws = wb.create_sheet("Salesman Template")

    col_names = [c for c, _, _ in SALESMAN_COLS]
    col_types = {c: t for c, _, t in SALESMAN_COLS}
    col_req   = {c: r for c, r, _ in SALESMAN_COLS}

    FIRST_DATA = 4
    LAST_DATA  = 1003

    CASCADE_COLS = {"ASM", "Region", "Nama Distributor", "Kode Distributor"}

    notes = {
        "Nama Salesman":
            "Teks bebas",
        "Nama SPV External":
            "Teks bebas (opsional)",
        "Nama SPV Internal":
            "Teks bebas",
        "ASM":
            "Langkah 1 - Pilih ASM dari dropdown",
        "Region":
            "Langkah 2 - Pilih Region (mengikuti ASM)",
        "Nama Distributor":
            "Langkah 3 - Pilih Distributor (mengikuti Region)",
        "Kode Distributor":
            "Otomatis terisi dari Nama Distributor",
        "Status Salesman":
            "Pilih: Mix atau Eksklusif",
        "Total Outlet Coverage PJP":
            "Angka bulat",
        "Gaji Pokok":
            "Angka (Rupiah)",
        "Tunjangan dan insentif":
            "Angka (Rupiah)",
        "Tanggal Lahir":
            "Isi tanggal dengan format YYYY-MM-DD (contoh: 2001-01-25)",
        "Jenis Kelamin":
            "Pilih: Male atau Female",
        "Pendidikan Terakhir":
            "Pilih dari dropdown",
        "Pengalaman di Perusahaan Sebelumnya (Dalam Bulan)":
            "Angka (bulan)",
        # Comma-separation hint for multiple principals
        "Principal Lain yang Ditanggungjawabi":
            "Teks bebas (opsional) — jika lebih dari satu, pisahkan dengan koma. Contoh: Unilever, P&G, Nestle",
        "No. HP":
            "Angka, tanpa tanda hubung",
        "Tanggal Join di G2G":
            "Isi tanggal dengan format YYYY-MM-DD (contoh: 2026-01-25)",
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

    ws.row_dimensions[1].height = 42   # taller row to show wrapped note text
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
        prompt="Isi tanggal dengan format YYYY-MM-DD (contoh: 2001-01-25). Jangan gunakan format lain.",
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

    # Input-message tooltip on Principal Lain (comma-separation reminder)
    principal_cl = col_letter("Principal Lain yang Ditanggungjawabi")
    principal_dv = DataValidation(
        type="custom",
        formula1=f'LEN({principal_cl}{FIRST_DATA})>=0',  # always passes
        allow_blank=True,
        showInputMessage=True,
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

def create_pjp_excel(
    df: pd.DataFrame,
    distributor_map: dict,
    dist_df: pd.DataFrame,
    store_df: pd.DataFrame,
) -> BytesIO:
    wb = Workbook()
    wb.remove(wb.active)
    _build_lookup_and_named_ranges(wb, dist_df, store_df)

    col_names = [c for c, _, _ in PJP_COLS]
    col_types = {c: t for c, _, t in PJP_COLS}
    col_req   = {c: r for c, r, _ in PJP_COLS}

    FIRST_DATA = 4
    LAST_DATA  = 1003

    CASCADE_COLS = {"ASM", "Region", "Nama Distributor", "Kode Distributor", "Toko"}

    notes_pjp = {
        "ASM":
            "Langkah 1 - Pilih ASM dari dropdown",
        "Region":
            "Langkah 2 - Pilih Region (mengikuti ASM)",
        "Nama Distributor":
            "Langkah 3 - Pilih Distributor (mengikuti Region)",
        "Kode Distributor":
            "Otomatis terisi dari Nama Distributor",
        "Nama Salesman":
            "Teks bebas",
        "Toko":
            "Langkah 4 - Pilih Toko (mengikuti Distributor)",
        "Hari":
            "Drop down dengan opsi hari",
        "Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap":
            "Drop down: ganjil / genap / ganjil+genap",
        # F4/F2/F1 descriptions shown in the note row
        "Frekuensi":
            "F4 = 1 minggu sekali  |  F2 = 2 minggu sekali  |  F1 = 1 bulan sekali",
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

    ws.row_dimensions[1].height = 42   # taller row for wrapped note text
    ws.row_dimensions[2].height = 16
    ws.row_dimensions[3].height = 44
    ws.freeze_panes = "A4"

    widths = [22, 24, 30, 20, 22, 30, 12, 40, 22]
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

    # Frekuensi dropdown with F4/F2/F1 descriptions in the prompt tooltip
    frekuensi_dv = DataValidation(
        type="list",
        formula1='"' + ",".join(FREQUENCY_OPTIONS) + '"',
        allow_blank=True,
        showInputMessage=True,
        promptTitle="Frekuensi Kunjungan",
        prompt=(
            "Pilih frekuensi kunjungan:\n"
            "  F4 = 1 minggu sekali\n"
            "  F2 = 2 minggu sekali\n"
            "  F1 = 1 bulan sekali"
        ),
        showErrorMessage=True,
        errorTitle="Input Tidak Valid",
        error="Pilih F4, F2, atau F1.",
    )
    ws.add_data_validation(frekuensi_dv)
    frekuensi_dv.sqref = dr("Frekuensi")

    nama_cl      = col_letter("Nama Distributor")
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

def _is_empty(val) -> bool:
    return pd.isna(val) or str(val).strip() == ""

def _get_unique_distributors(df: pd.DataFrame, col: str = "Kode Distributor") -> list:
    if col not in df.columns:
        return []
    return (
        df[col].dropna().astype(str).str.strip()
        .replace("", pd.NA).dropna().unique().tolist()
    )


def validate_row_completeness(df: pd.DataFrame, required_cols: list, sheet_label: str) -> list:
    errors = []
    for i, row in df.iterrows():
        n = i + 1
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
        n = i + 1
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


def validate_pjp_df(df, distributor_map, store_df: pd.DataFrame | None = None):
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

    valid_store_labels = set()
    if store_df is not None and not store_df.empty:
        valid_store_labels = set(store_df["store_label"].dropna().tolist())

    for i, row in df.iterrows():
        n = i + 1
        kode = str(row.get("Kode Distributor", "")).strip()
        if kode and kode not in distributor_map:
            errors.append(f"Baris {n}: Kode Distributor '{kode}' tidak valid")

        toko = str(row.get("Toko", "")).strip()
        if toko and valid_store_labels and toko not in valid_store_labels:
            warnings.append(f"Baris {n}: Toko '{toko}' tidak ditemukan di master store")

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

    name_to_code = {v: k for k, v in distributor_map.items()}
    if "Nama Distributor" in df.columns:
        df["Kode Distributor"] = df["Nama Distributor"].apply(
            lambda x: name_to_code.get(str(x).strip(), "") if pd.notna(x) else ""
        )

    if "No. HP" in df.columns:
        df["No. HP"] = df["No. HP"].apply(normalize_phone_id)

    return df


# ─── BigQuery writer ──────────────────────────────────────────────────────────

_SAL_COL_MAP = {
    "Nama Salesman":                                     "nama_salesman",
    "Nama SPV External":                                 "nama_spv_external",
    "Nama SPV Internal":                                 "nama_spv_internal",
    "ASM":                                               "asm",
    "Region":                                            "region",
    "Nama Distributor":                                  "nama_distributor",
    "Kode Distributor":                                  "kode_distributor",
    "Status Salesman":                                   "status_salesman",
    "Total Outlet Coverage PJP":                         "total_outlet_coverage_pjp",
    "Gaji Pokok":                                        "gaji_pokok",
    "Tunjangan dan insentif":                            "tunjangan_dan_insentif",
    "Tanggal Lahir":                                     "tanggal_lahir",
    "Jenis Kelamin":                                     "jenis_kelamin",
    "Pendidikan Terakhir":                               "pendidikan_terakhir",
    "Pengalaman di Perusahaan Sebelumnya (Dalam Bulan)": "pengalaman_bulan",
    "Principal Lain yang Ditanggungjawabi":              "principal_lain",
    "No. HP":                                            "no_hp",
    "Tanggal Join di G2G":                               "tanggal_join_g2g",
}

_PJP_COL_MAP = {
    "ASM":                                               "asm",
    "Region":                                            "region",
    "Nama Distributor":                                  "nama_distributor",
    "Kode Distributor":                                  "kode_distributor",
    "Nama Salesman":                                     "nama_salesman",
    "Toko":                                              "kode_toko",
    "Hari":                                              "hari",
    "Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap":  "minggu",
    "Frekuensi":                                         "frekuensi",
}


def push_to_bigquery(df: pd.DataFrame, col_map: dict, table_id: str) -> tuple[bool, str]:
    try:
        credentials, project_id = get_credentials()
        client = bigquery.Client(credentials=credentials, project=project_id)
        existing_cols = {c: col_map[c] for c in col_map if c in df.columns}
        bq_df = df[list(existing_cols.keys())].rename(columns=existing_cols).copy()
        bq_df["uploaded_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        job = client.load_table_from_dataframe(bq_df, table_id, job_config=job_config)
        job.result()
        return True, f"Berhasil menyimpan {len(bq_df)} baris ke `{table_id}`."
    except Exception as e:
        return False, f"Gagal menyimpan ke Database: {e}"


def check_distributor_submitted(kode_distributor: str, table_id: str) -> tuple[bool, str]:
    try:
        credentials, project_id = get_credentials()
        client = bigquery.Client(credentials=credentials, project=project_id)
        query = f"""
            SELECT COUNT(*) AS cnt
            FROM `{table_id}`
            WHERE UPPER(kode_distributor) = UPPER(@kode)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("kode", "STRING", kode_distributor)]
        )
        result = client.query(query, job_config=job_config).result()
        cnt = next(iter(result))["cnt"]
        return (cnt > 0), ""
    except Exception as e:
        err = str(e)
        if "Not found" in err or "notFound" in err or "does not exist" in err.lower():
            return False, ""
        return False, f"Gagal memeriksa riwayat submission: {err}"


# ─── App ──────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="Salesman & PJP Template", page_icon="📋", layout="wide")
st.title("📋 Salesman & PJP Template Manager")
st.caption("Download template Excel, upload file yang sudah diisi, dan validasi data.")

try:
    dist_df  = load_distributor_data()
    store_df = load_store_data()
    distributor_map, asm_options, region_options = build_lookup_tables(dist_df)
except Exception as e:
    st.error(f"Gagal memuat data dari Database: {e}")
    st.stop()

with st.expander("📖 **PANDUAN SINGKAT**", expanded=False):
    st.markdown("""
    ### ⚡ ATURAN DASAR:
    - **1 file = 1 distributor** (tidak boleh campur)
    - **1 distributor = 1 kali submit** (tidak bisa ulang)
    - **Semua kolom "Wajib Diisi" harus terisi**
    - **Jangan edit kolom "Kode Distributor"** (otomatis)

    ### 🔄 URUTAN DROPDOWN BERTINGKAT (WAJIB!):
    1. **ASM** → 2. **Region** → 3. **Nama Distributor** → 4. **(PJP) Toko**

    ### ✅ FORMAT DATA YANG BENAR:
    - **Tanggal**: Isi manual dengan format YYYY-MM-DD (contoh: 2001-01-25)
    - **Angka**: Hanya angka (contoh: 5000000)
    - **No. HP**: 8-13 digit, contoh: `08123456789` atau `+628123456789`
    - **Principal Lain**: Jika lebih dari satu, pisahkan dengan koma — contoh: `Unilever, P&G, Nestle`
    - **Frekuensi PJP**: F4 = 1 minggu sekali | F2 = 2 minggu sekali | F1 = 1 bulan sekali

    ### ❌ UPLOAD DITOLAK JIKA:
    - Ada **error** (merah) ATAU **peringatan** (kuning)
    - Perbaiki semua masalah, lalu upload ulang

    ### 📞 BUTUH BANTUAN?
    Hubungi tim support G2G
    """)

tab_download, tab_upload = st.tabs(["📥 Download Template", "📤 Upload & Validasi"])

# ─── Tab: Download ────────────────────────────────────────────────────────────
with tab_download:
    st.header("📥 Download Template Excel")
    col_sal, col_pjp = st.columns(2)

    with col_sal:
        st.subheader("📋 Salesman Template")
        sal_excel = create_salesman_excel(
            pd.DataFrame(), distributor_map, asm_options, region_options, dist_df
        )
        st.download_button(
            "⬇️ Download Salesman Template",
            data=sal_excel.getvalue(),
            file_name=f"Salesman_Template_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True, type="primary",
        )

    with col_pjp:
        st.subheader("🗓️ PJP Template")
        pjp_excel = create_pjp_excel(
            pd.DataFrame(columns=[c for c, _, _ in PJP_COLS]),
            distributor_map, dist_df, store_df,
        )
        st.download_button(
            "⬇️ Download PJP Template",
            data=pjp_excel.getvalue(),
            file_name=f"PJP_Template_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True, type="primary",
        )

# ─── Tab: Upload & Validasi ───────────────────────────────────────────────────
with tab_upload:
    st.header("📤 Upload & Validasi Template")

    st.warning("""
    ⚠️ **PERHATIAN:**
    - 1 file = 1 distributor | 1 distributor = 1 kali submit
    - **Data TIDAK BISA diupload jika masih ada error ATAU peringatan**
    - Perbaiki semua masalah di file Excel, lalu upload ulang
    """)

    st.info(
        "Upload file Excel yang sudah diisi. "
        "Harus mengandung sheet **'Salesman Template'** dan/atau **'PJP Template'**."
    )

    uploaded = st.file_uploader("Pilih file Excel (.xlsx)", type=["xlsx"], key="main_uploader")

    if uploaded:
        try:
            xl = pd.ExcelFile(uploaded)
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            st.stop()

        has_salesman = "Salesman Template" in xl.sheet_names
        has_pjp      = "PJP Template"      in xl.sheet_names

        if not has_salesman and not has_pjp:
            st.error("Sheet 'Salesman Template' / 'PJP Template' tidak ditemukan. Gunakan template resmi.")
            st.stop()

        labels = []
        if has_salesman: labels.append("`Salesman Template`")
        if has_pjp:      labels.append("`PJP Template`")
        st.success(f"Sheet ditemukan: {' · '.join(labels)}")

        sal_errors = sal_warnings = pjp_errors = pjp_warnings = []
        sal_df = pjp_df = pd.DataFrame()
        sal_can_upload = pjp_can_upload = True

        if has_salesman:
            st.markdown("---")
            st.subheader("👤 Salesman Template")
            try:
                sal_df = read_template_sheet(uploaded, "Salesman Template", 2, distributor_map)
                sal_df = sal_df[sal_df["Nama Salesman"].notna() & (sal_df["Nama Salesman"] != "")]
                sal_df = sal_df.reset_index(drop=True)
            except Exception as e:
                st.error(f"Gagal membaca: {e}")
                sal_df = pd.DataFrame()

            if sal_df.empty:
                st.warning("Tidak ada data di sheet Salesman Template.")
                sal_can_upload = False
            else:
                with st.expander("👁️ Preview", expanded=True):
                    st.dataframe(sal_df, use_container_width=True, hide_index=True)

                sal_errors, sal_warnings = validate_salesman_df(
                    sal_df, distributor_map, asm_options, region_options
                )

                if sal_errors or sal_warnings:
                    sal_can_upload = False
                    if sal_errors:
                        st.error(f"**❌ {len(sal_errors)} ERROR - Data TIDAK BISA diupload:**")
                        for e in sal_errors: st.markdown(f"- {e}")
                    if sal_warnings:
                        st.warning(f"**⚠️ {len(sal_warnings)} PERINGATAN - Data TIDAK BISA diupload sampai peringatan diperbaiki:**")
                        for w in sal_warnings: st.markdown(f"- {w}")
                        if any("No. HP" in w for w in sal_warnings):
                            st.info("📝 **Cara memperbaiki No. HP:**\n"
                                    "- Gunakan format: 08123456789 atau +628123456789\n"
                                    "- Perbaiki di file Excel, lalu upload ulang")
                else:
                    st.success("✅ Validasi Salesman berhasil! Data siap diupload.")

        if has_pjp:
            st.markdown("---")
            st.subheader("🗓️ PJP Template")
            try:
                pjp_df = read_template_sheet(
                    uploaded, "PJP Template", 2, distributor_map, store_df
                )
                pjp_df = pjp_df[pjp_df["Nama Distributor"].notna() & (pjp_df["Nama Distributor"] != "")]
                pjp_df = pjp_df.reset_index(drop=True)
            except Exception as e:
                st.error(f"Gagal membaca: {e}")
                pjp_df = pd.DataFrame()

            if pjp_df.empty:
                st.warning("Tidak ada data di sheet PJP Template.")
                pjp_can_upload = False
            else:
                c1, c2 = st.columns([3, 1])
                c1.metric("Total Baris", len(pjp_df))
                c2.metric("Total Kolom", len(pjp_df.columns))
                with st.expander("👁️ Preview", expanded=True):
                    st.dataframe(pjp_df, use_container_width=True, hide_index=True)

                pjp_errors, pjp_warnings = validate_pjp_df(pjp_df, distributor_map, store_df)

                if pjp_errors or pjp_warnings:
                    pjp_can_upload = False
                    if pjp_errors:
                        st.error(f"**❌ {len(pjp_errors)} ERROR - Data TIDAK BISA diupload:**")
                        for e in pjp_errors: st.markdown(f"- {e}")
                    if pjp_warnings:
                        st.warning(f"**⚠️ {len(pjp_warnings)} PERINGATAN - Data TIDAK BISA diupload sampai peringatan diperbaiki:**")
                        for w in pjp_warnings: st.markdown(f"- {w}")
                else:
                    st.success("✅ Validasi PJP berhasil! Data siap diupload.")

        if has_salesman and has_pjp and not sal_df.empty and not pjp_df.empty:
            st.markdown("---")
            st.subheader("📊 Ringkasan")
            total_err  = len(sal_errors)  + len(pjp_errors)
            total_warn = len(sal_warnings) + len(pjp_warnings)
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Error",      total_err)
            c2.metric("Total Peringatan", total_warn)
            if total_err == 0 and total_warn == 0:
                c3.metric("Status", "✅ Siap Upload")
            else:
                c3.metric("Status", "❌ Ada Error/Peringatan - Perbaiki Data")

        st.markdown("---")
        st.subheader("☁️ Upload ke Database")

        if has_salesman and not sal_df.empty:
            sal_kode  = str(sal_df["Kode Distributor"].iloc[0]).strip()
            SAL_TABLE = "skintific-data-warehouse.gt_schema.gt_master_salesman"
            already_sal, chk_err_sal = check_distributor_submitted(sal_kode, SAL_TABLE)

            if chk_err_sal:
                st.error(chk_err_sal)
                sal_can_upload = False
            elif already_sal:
                st.error(
                    f"❌ **Submission ditolak:** Kode Distributor **{sal_kode}** "
                    f"sudah pernah mengirimkan data Salesman. "
                    f"Setiap distributor hanya diperbolehkan submit satu kali."
                )
                sal_can_upload = False
            else:
                if sal_can_upload and not sal_errors and not sal_warnings:
                    if st.button("☁️ Simpan Salesman ke Database", key="bq_sal", type="primary"):
                        with st.spinner("Menyimpan data Salesman ke Database..."):
                            ok, msg = push_to_bigquery(sal_df, _SAL_COL_MAP, SAL_TABLE)
                        if ok:
                            st.success(msg)
                            st.balloons()
                        else:
                            st.error(msg)
                else:
                    if sal_errors:
                        st.warning(f"⚠️ Data Salesman belum siap diupload. Terdapat {len(sal_errors)} error yang harus diperbaiki.")
                    elif sal_warnings:
                        st.warning(f"⚠️ Data Salesman belum siap diupload. Terdapat {len(sal_warnings)} peringatan yang harus diperbaiki.")
                        st.info("Perbaiki semua peringatan di file Excel, lalu upload ulang.")
                    else:
                        st.warning("⚠️ Data Salesman belum siap diupload. Perbaiki masalah yang ditemukan.")

        if has_pjp and not pjp_df.empty:
            pjp_kode  = str(pjp_df["Kode Distributor"].iloc[0]).strip()
            PJP_TABLE = "skintific-data-warehouse.gt_schema.gt_master_salesman_pjp"
            already_pjp, chk_err_pjp = check_distributor_submitted(pjp_kode, PJP_TABLE)

            if chk_err_pjp:
                st.error(chk_err_pjp)
                pjp_can_upload = False
            elif already_pjp:
                st.error(
                    f"❌ **Submission ditolak:** Kode Distributor **{pjp_kode}** "
                    f"sudah pernah mengirimkan data PJP. "
                    f"Setiap distributor hanya diperbolehkan submit satu kali."
                )
                pjp_can_upload = False
            else:
                if pjp_can_upload and not pjp_errors and not pjp_warnings:
                    if st.button("☁️ Simpan PJP ke Database", key="bq_pjp", type="primary"):
                        with st.spinner("Menyimpan data PJP ke Database..."):
                            ok, msg = push_to_bigquery(pjp_df, _PJP_COL_MAP, PJP_TABLE)
                        if ok:
                            st.success(msg)
                            st.balloons()
                        else:
                            st.error(msg)
                else:
                    if pjp_errors:
                        st.warning(f"⚠️ Data PJP belum siap diupload. Terdapat {len(pjp_errors)} error yang harus diperbaiki.")
                    elif pjp_warnings:
                        st.warning(f"⚠️ Data PJP belum siap diupload. Terdapat {len(pjp_warnings)} peringatan yang harus diperbaiki.")
                        st.info("Perbaiki semua peringatan di file Excel, lalu upload ulang.")
                    else:
                        st.warning("⚠️ Data PJP belum siap diupload. Perbaiki masalah yang ditemukan.")

st.markdown("---")
st.caption("Salesman & PJP Template Manager · G2G")
