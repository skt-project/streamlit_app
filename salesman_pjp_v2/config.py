"""
Central configuration for salesman_pjp_v2.
All constants come from st.secrets or module-level defaults — nothing hardcoded.
"""

import streamlit as st
from datetime import date

# ─── BigQuery table IDs ────────────────────────────────────────────────────────
PROJECT_ID        = "skintific-data-warehouse"
MAPPING_TABLE     = f"{PROJECT_ID}.gt_schema.gt_salesman_mapping"
# WRITES go to SALESMAN_TABLE (the append-only base); READS go to
# SALESMAN_VIEW, which resolves nama_distributor / region from
# master_distributor by distributor_code. The base table still holds
# the frozen historical spellings and the pre-September region, and is
# deliberately left that way.
SALESMAN_TABLE    = f"{PROJECT_ID}.gt_schema.gt_master_salesman"
SALESMAN_VIEW     = f"{PROJECT_ID}.gt_schema.gt_master_salesman_v"
PJP_TABLE         = f"{PROJECT_ID}.gt_schema.gt_master_salesman_pjp"
DISTRIBUTOR_TABLE = f"{PROJECT_ID}.gt_schema.master_distributor"
BASIS_TABLE       = f"{PROJECT_ID}.gt_schema.master_store_database_basis"
AUDIT_TABLE       = f"{PROJECT_ID}.sfa_step.audit_log"
SEQ_TABLE         = f"{PROJECT_ID}.sfa_step.salesman_id_seq"

# ─── Basis column names (AJ / AK / AL from master_store_database_basis) ───────
# These are the fallback salesman-per-brand columns.
# Adjust if the actual BQ column names differ from the spreadsheet headers.
BASIS_COL_SKT = "salesman_skt"   # AJ
BASIS_COL_G2G = "salesman_g2g"   # AK
BASIS_COL_TPH = "salesman_tph"   # AL

# ─── Salesman types & field options ───────────────────────────────────────────
SALESMAN_TYPES     = ["GTI", "MIX", "MTI"]
STATUS_OPTIONS     = ["Mix", "Eksklusif"]
GENDER_OPTIONS     = ["Male", "Female"]
EDUCATION_OPTIONS  = ["SD", "SMP", "SMA", "S1", "S2"]
DAY_OPTIONS        = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu"]
WEEK_OPTIONS       = ["Minggu Ganjil", "Minggu Genap", "Minggu Ganjil + Genap"]
FREQUENCY_OPTIONS  = ["F4+", "F4", "F2", "F1"]

# ─── Column specs for Excel templates ─────────────────────────────────────────
# (name, required, type)
SALESMAN_COLS = [
    ("Nama Salesman",                                              True,  "text"),
    ("Nama SPV External",                                          False, "text"),
    ("Nama SPV Internal",                                          True,  "text"),
    ("Nama SPV Internal 2",                                        False, "text"),
    ("ASM",                                                        True,  "cascade"),
    ("Region",                                                     True,  "cascade"),
    ("Nama Distributor",                                           True,  "cascade"),
    ("Kode Distributor",                                           True,  "auto"),
    ("Status Salesman",                                            True,  "dropdown"),
    ("Total Outlet Coverage PJP",                                  True,  "numeric"),
    ("Gaji Pokok",                                                 True,  "numeric"),
    ("Tunjangan dan insentif",                                     True,  "numeric"),
    ("Tanggal Lahir",                                              True,  "date"),
    ("Jenis Kelamin",                                              True,  "dropdown"),
    ("Pendidikan Terakhir",                                        True,  "dropdown"),
    ("Pengalaman di Perusahaan Sebelumnya (Dalam Bulan)",          True,  "numeric"),
    ("Principal Lain yang Ditanggungjawabi",                       False, "text"),
    ("No. HP",                                                     True,  "text"),
    ("Tanggal Join di G2G",                                        True,  "date"),
]
PJP_COLS = [
    ("ASM",                                                        True,  "cascade"),
    ("Region",                                                     True,  "cascade"),
    ("Nama Distributor",                                           True,  "cascade"),
    ("Kode Distributor",                                           True,  "auto"),
    ("Nama Salesman",                                              True,  "text"),
    ("Kode Toko",                                                  True,  "store_cascade"),
    ("Nama Toko",                                                  False, "auto"),
    ("Hari",                                                       True,  "dropdown"),
    ("Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap",          True,  "dropdown"),
    ("Frekuensi",                                                  True,  "dropdown"),
]

SALESMAN_REQUIRED = [c for c, r, _ in SALESMAN_COLS if r]
PJP_REQUIRED      = [c for c, r, _ in PJP_COLS if r]

_PJP_COL_MAP = {
    "ASM":                                               "asm",
    "Region":                                            "region",
    "Nama Distributor":                                  "nama_distributor",
    "Kode Distributor":                                  "kode_distributor",
    "Nama Salesman":                                     "nama_salesman",
    "Kode Toko":                                         "kode_toko",
    "Nama Toko":                                         "nama_toko",
    "Hari":                                              "hari",
    "Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap": "minggu",
    "Frekuensi":                                         "frekuensi",
}


# ─── Configurable deadline ─────────────────────────────────────────────────────

def get_input_deadline() -> date:
    """
    Monthly PJP submission deadline for SPV.
    Read from st.secrets so it can be changed without a code deploy.
    Falls back to a far-future date (open) if not configured.
    """
    raw = st.secrets.get("pjp_input_deadline", "")
    if raw:
        try:
            return date.fromisoformat(str(raw))
        except ValueError:
            pass
    # Default: open (no deadline enforced). Set pjp_input_deadline in secrets to lock.
    return date(9999, 12, 31)
