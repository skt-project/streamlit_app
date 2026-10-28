import datetime
import io
import re
import math
from zoneinfo import ZoneInfo
from urllib.parse import quote
import os
import numpy as np
import pandas as pd
import requests
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# ─────────────────────────────── ZONA & PENGGUNA ──────────────────────────────
ZONA_WAKTU = ZoneInfo("Asia/Jakarta")
PENGGUNA   = "Shaltsa"

# ─────────────────────────────── GOOGLE SHEETS ────────────────────────────────
HISTORY_GENERATOR = "https://docs.google.com/spreadsheets/d/1O5En6dz8AU5H6LEQCm2MGIxIF89VNv10Ehu96C26B7U/edit?gid=265011411#gid=265011411"
SHEET_DISTRI      = "Distributor Mapping!A2:H"
FILE_PREFIX_ID    = os.path.join(os.path.dirname(__file__), "Prefix id.xlsx")
HISTORY_NOO       = "https://docs.google.com/spreadsheets/d/1JZ025vZHwAo1Y3GljIvuXH0WC_pZeo5DfaXDqr6ANVk/edit?gid=0#gid=0"
SHEET_HISTORY     = "history!A1:G"
SHEET_PRODUCT     = "Product Mapping!A2:B"
SHEET_SALES       = "Sales Mapping"

# ─────────────────────────────── SUPABASE ─────────────────────────────────────
# SUPABASE_URL = st.secrets["SUPABASE_URL"]
# SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

SUPABASE_URL = "https://iarwfviiulnmseirfhlq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlhcndmdmlpdWxubXNlaXJmaGxxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3OTI0NjEyMTIsImV4cCI6MjEwODAzNzIxMn0.-LSVYGBxtUV2LWKkWddOBlqMBA60H1C15qsko0LFgFE"


_SB_HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=representation",
}

# ─────────────────────────────── TABLE NAMES ──────────────────────────────────
TABLE_SUBMIT_NOO   = "INPUT_NOO"
TABLE_RIWAYAT      = "riwayat_noo"
TABLE_HISTORY_NOO  = "NOO_GENERATOR_ID"
TABLE_SKU_MAPPING  = "sku_mapping"
BQ_MASTER_STORE    = "gt_schema.master_store_database_basis"
BQ_KOLOM_KODE_TOKO = "dst_id_g2g"   # = Customer Store Code (kode toko dari distributor)

# ─────────────────────────────── VALIDASI ─────────────────────────────────────
KOLOM_VALIDASI = ["Generate ID", "Need Review", "Existing"]
STATUS_VALID   = KOLOM_VALIDASI[0]
STATUS_REVIEW  = KOLOM_VALIDASI[1]

WARNA_VALIDASI = {
    "Generate ID": "background-color: #d3f9d8; color: #1b5e20",
    "Need Review": "background-color: #ffd6e0; color: #9c1c4a",
    "Existing":    "background-color: #fff3b0; color: #7a5b00",
}

#--------------------BIGQUERY---------------------

try:
    gcp_secrets = st.secrets["connections"]["bigquery"]
    private_key = gcp_secrets["private_key"].replace("\\n", "\n")
    _bq_credentials = service_account.Credentials.from_service_account_info({
        "type": gcp_secrets["type"],
        "project_id": gcp_secrets["project_id"],
        "private_key_id": gcp_secrets["private_key_id"],
        "private_key": private_key,
        "client_email": gcp_secrets["client_email"],
        "client_id": gcp_secrets["client_id"],
        "auth_uri": gcp_secrets["auth_uri"],
        "token_uri": gcp_secrets["token_uri"],
        "auth_provider_x509_cert_url": gcp_secrets["auth_provider_x509_cert_url"],
        "client_x509_cert_url": gcp_secrets["client_x509_cert_url"],
    })
    GCP_PROJECT_ID = st.secrets["bigquery"]["project"]
except Exception:
    GCP_CREDENTIALS_PATH = r"C:\Users\Shaltsa Nadya\Documents\try python\streamlit\skintific-data-warehouse-ea77119e2e7a.json"
    GCP_PROJECT_ID = "skintific-data-warehouse"
    _bq_credentials = service_account.Credentials.from_service_account_file(GCP_CREDENTIALS_PATH)


@st.cache_resource(show_spinner=False)
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(credentials=_bq_credentials, project=GCP_PROJECT_ID)


@st.cache_data(ttl=300, show_spinner=False)
def get_id() -> set:
    client = get_bq_client()
    query = f"""
        SELECT DISTINCT UPPER(TRIM(cust_id)) as cust_id
        FROM `{GCP_PROJECT_ID}.gt_schema.master_store_database_basis`
        WHERE cust_id IS NOT NULL AND TRIM(cust_id) != ''
    """
    return {r.cust_id for r in client.query(query).result()}

def bq_cari_duplikat_kode(kode_list) -> tuple:
    """Cek Customer Store Code ke kolom cust_id di BigQuery master store.
    Return (ditemukan, gagal_cek), sama seperti sb_cari_duplikat_kode."""
    kode_bersih = {str(k).strip() for k in kode_list if str(k).strip()}
    if not kode_bersih:
        return set(), set()
    try:
        client = get_bq_client()
        query = f"""
            SELECT DISTINCT UPPER(TRIM(CAST({BQ_KOLOM_KODE_TOKO} AS STRING))) AS kode
            FROM `{GCP_PROJECT_ID}.{BQ_MASTER_STORE}`
            WHERE UPPER(TRIM(CAST({BQ_KOLOM_KODE_TOKO} AS STRING))) IN UNNEST(@kode)
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ArrayQueryParameter("kode", "STRING", [k.upper() for k in kode_bersih])
        ])
        ketemu = {r.kode for r in client.query(query, job_config=cfg).result()}
        return {k for k in kode_bersih if k.upper() in ketemu}, set()
    except Exception:
        return set(), kode_bersih
#--------------------------------------------

def warnai_validasi(v):
    return WARNA_VALIDASI.get(v, "")


# ─────────────────────────────── STORE TYPES & KOTA ──────────────────────────
DATA_STORE_TYPE = [
    {"Channel": "GT",  "Store Type": "Cosmetic Store"},
    {"Channel": "GT",  "Store Type": "Retail Store"},
    {"Channel": "GT",  "Store Type": "Pharmacy"},
    {"Channel": "GT",  "Store Type": "ATC"},
    {"Channel": "MTI", "Store Type": "Large Supermarket"},
    {"Channel": "MTI", "Store Type": "Premium SPM"},
    {"Channel": "MTI", "Store Type": "Regular SPM"},
    {"Channel": "MTI", "Store Type": "Minimarket"},
    {"Channel": "MTI", "Store Type": "Specialty"},
    {"Channel": "MT",  "Store Type": "Hyper/Super"},
    {"Channel": "MT",  "Store Type": "Department Store"},
    {"Channel": "MT",  "Store Type": "Minis (Mini Supermarket)"},
    {"Channel": "MT",  "Store Type": "Online"},
    {"Channel": "MT",  "Store Type": "Drugstore"},
    {"Channel": "MT",  "Store Type": "Convenience Store"},
    {"Channel": "MT",  "Store Type": "Wholesale"},
]
JENIS_TOKO_DEFAULT = [d["Store Type"] for d in DATA_STORE_TYPE]

CITY = [
    "Badung","Bangli","Buleleng","Gianyar","Jembrana","Karangasem","Klungkung","Tabanan",
    "Kota Denpasar","Bima","Dompu","Lombok Barat","Lombok Tengah","Lombok Timur","Lombok Utara",
    "Sumbawa","Sumbawa Barat","Kota Bima","Kota Mataram","Alor","Belu","Ende","Flores Timur",
    "Kupang","Lembata","Malaka","Manggarai","Manggarai Barat","Manggarai Timur","Ngada","Nagekeo",
    "Rote Ndao","Sabu Raijua","Sikka","Sumba Barat","Sumba Barat Daya","Sumba Tengah","Sumba Timur",
    "Timor Tengah Selatan","Timor Tengah Utara","Kota Kupang","Banjarnegara","Banyumas","Batang",
    "Blora","Boyolali","Brebes","Cilacap","Demak","Grobogan","Jepara","Karanganyar","Kebumen",
    "Kendal","Klaten","Kudus","Magelang","Pati","Pekalongan","Pemalang","Purbalingga","Purworejo",
    "Rembang","Semarang","Sragen","Sukoharjo","Tegal","Temanggung","Wonogiri","Wonosobo",
    "Kota Magelang","Kota Pekalongan","Kota Salatiga","Kota Semarang","Kota Surakarta","Kota Tegal",
    "Bantul","Gunungkidul","Kulon Progo","Sleman","Kota Yogyakarta","Agam","Dharmasraya",
    "Kepulauan Mentawai","Lima Puluh Kota","Padang Pariaman","Pasaman","Pasaman Barat",
    "Pesisir Selatan","Sijunjung","Solok","Solok Selatan","Tanah Datar","Kota Bukittinggi",
    "Kota Padang","Kota Padang Panjang","Kota Pariaman","Kota Payakumbuh","Kota Sawahlunto",
    "Kota Solok","Bengkalis","Indragiri Hilir","Indragiri Hulu","Kampar","Kepulauan Meranti",
    "Kuantan Singingi","Pelalawan","Rokan Hilir","Rokan Hulu","Siak","Kota Dumai","Kota Pekanbaru",
    "Bintan","Karimun","Kepulauan Anambas","Lingga","Natuna","Kota Batam","Kota Tanjungpinang",
    "Bangkalan","Banyuwangi","Blitar","Bojonegoro","Bondowoso","Gresik","Jember","Jombang",
    "Kediri","Lamongan","Lumajang","Madiun","Magetan","Malang","Mojokerto","Nganjuk","Ngawi",
    "Pacitan","Pamekasan","Pasuruan","Ponorogo","Probolinggo","Sampang","Sidoarjo","Situbondo",
    "Sumenep","Trenggalek","Tuban","Tulungagung","Kota Batu","Kota Blitar","Kota Kediri",
    "Kota Madiun","Kota Malang","Kota Mojokerto","Kota Pasuruan","Kota Probolinggo","Kota Surabaya",
    "Berau","Kutai Barat","Kutai Kartanegara","Kutai Timur","Mahakam Ulu","Paser",
    "Penajam Paser Utara","Kota Balikpapan","Kota Bontang","Kota Samarinda","Bulungan","Malinau",
    "Nunukan","Tana Tidung","Kota Tarakan","Jakarta Barat","Jakarta Pusat","Jakarta Selatan",
    "Jakarta Timur","Jakarta Utara","Kepulauan Seribu","Aceh Barat","Aceh Barat Daya","Aceh Besar",
    "Aceh Jaya","Aceh Selatan","Aceh Singkil","Aceh Tamiang","Aceh Tengah","Aceh Tenggara",
    "Aceh Timur","Aceh Utara","Bener Meriah","Bireuen","Gayo Lues","Nagan Raya","Pidie",
    "Pidie Jaya","Simeulue","Kota Banda Aceh","Kota Langsa","Kota Lhokseumawe","Kota Sabang",
    "Kota Subulussalam","Asahan","Batu Bara","Dairi","Deli Serdang","Humbang Hasundutan","Karo",
    "Labuhanbatu","Labuhanbatu Selatan","Labuhanbatu Utara","Langkat","Mandailing Natal","Nias",
    "Nias Barat","Nias Selatan","Nias Utara","Padang Lawas","Padang Lawas Utara","Pakpak Bharat",
    "Samosir","Serdang Bedagai","Simalungun","Tapanuli Selatan","Tapanuli Tengah","Tapanuli Utara",
    "Toba Samosir","Kota Binjai","Kota Gunungsitoli","Kota Medan","Kota Padang Sidimpuan",
    "Kota Pematangsiantar","Kota Sibolga","Kota Tanjung Balai","Kota Tebing Tinggi","Balangan",
    "Banjar","Barito Kuala","Hulu Sungai Selatan","Hulu Sungai Tengah","Hulu Sungai Utara",
    "Kotabaru","Tabalong","Tanah Bumbu","Tanah Laut","Tapin","Kota Banjarbaru","Kota Banjarmasin",
    "Barito Selatan","Barito Timur","Barito Utara","Gunung Mas","Kapuas","Katingan",
    "Kotawaringin Barat","Kotawaringin Timur","Lamandau","Murung Raya","Pulang Pisau","Sukamara",
    "Seruyan","Kota Palangkaraya","Banyuasin","Empat Lawang","Lahat","Muara Enim","Musi Banyuasin",
    "Musi Rawas","Musi Rawas Utara","Ogan Ilir","Ogan Komering Ilir","Ogan Komering Ulu",
    "Ogan Komering Ulu Selatan","Ogan Komering Ulu Timur","Penukal Abab Lematang Ilir",
    "Kota Lubuklinggau","Kota Pagar Alam","Kota Palembang","Kota Prabumulih","Batanghari","Bungo",
    "Kerinci","Merangin","Muaro Jambi","Sarolangun","Tanjung Jabung Barat","Tanjung Jabung Timur",
    "Tebo","Kota Jambi","Kota Sungai Penuh","Bengkulu Selatan","Bengkulu Tengah","Bengkulu Utara",
    "Kaur","Kepahiang","Lebong","Mukomuko","Rejang Lebong","Seluma","Kota Bengkulu","Bangka",
    "Bangka Barat","Bangka Selatan","Bangka Tengah","Belitung","Belitung Timur","Kota Pangkalpinang",
    "Lampung Tengah","Lampung Utara","Lampung Selatan","Lampung Barat","Lampung Timur","Mesuji",
    "Pesawaran","Pesisir Barat","Pringsewu","Tulang Bawang","Tulang Bawang Barat","Tanggamus",
    "Way Kanan","Kota Bandar Lampung","Kota Metro","Bantaeng","Barru","Bone","Bulukumba","Enrekang",
    "Gowa","Jeneponto","Kepulauan Selayar","Luwu","Luwu Timur","Luwu Utara","Maros",
    "Pangkajene dan Kepulauan","Pinrang","Sidenreng Rappang","Sinjai","Soppeng","Takalar",
    "Tana Toraja","Toraja Utara","Wajo","Kota Makassar","Kota Palopo","Kota Pare pare","Bombana",
    "Buton","Buton Selatan","Buton Tengah","Buton Utara","Kolaka","Kolaka Timur","Kolaka Utara",
    "Konawe","Konawe Kepulauan","Konawe Selatan","Konawe Utara","Muna","Muna Barat","Wakatobi",
    "Kota Bau Bau","Kota Kendari","Banggai","Banggai Kepulauan","Banggai Laut","Buol","Donggala",
    "Morowali","Morowali Utara","Parigi Moutong","Poso","Sigi","Tojo Una-Una","ToliToli","Kota Palu",
    "Majene","Mamasa","Mamuju","Mamuju Tengah","Mamuju Utara","Polewali Mandar","Boalemo",
    "Bone Bolango","Gorontalo","Gorontalo Utara","Pahuwato","Kota Gorontalo","Bolaang Mongondow",
    "Bolaang Mongondow Selatan","Bolaang Mongondow Timur","Bolaang Mongondow Utara",
    "Kepulauan Sangihe","Kep. Siau Tagulandang Biaro","Kepulauan Talaud","Minahasa",
    "Minahasa Selatan","Minahasa Tenggara","Minahasa Utara","Kota Bitung","Kota Kotamobagu",
    "Kota Manado","Kota Tomohon","Buru","Buru Selatan","Kepulauan Aru","Maluku Barat Daya",
    "Maluku Tengah","Maluku Tenggara","Maluku Tenggara Barat","Seram Bagian Barat",
    "Seram Bagian Timur","Kota Ambon","Kota Tual","Halmahera Barat","Halmahera Tengah",
    "Halmahera Utara","Halmahera Selatan","Kepulauan Sula","Halmahera Timur","Pulau Morotai",
    "Pulau Taliabu","Kota Ternate","Kota Tidore Kepulauan","Asmat","Biak Numfor","Boven Digoel",
    "Deiyai","Dogiyai","Intan Jaya","Jayapura","Jayawijaya","Keerom","Kepulauan Yapen","Lanny Jaya",
    "Mamberamo Raya","Mamberamo Tengah","Mappi","Merauke","Mimika","Nabire","Nduga","Paniai",
    "Pegunungan Bintang","Puncak","Puncak Jaya","Sarmi","Supiori","Tolikara","Waropen","Yahukimo",
    "Yalimo","Kota Jayapura","Fakfak","Kaimana","Manokwari","Manokwari Selatan","Teluk Bintuni",
    "Teluk Wondama","Pegunungan Arfak","Maybrat","Raja Ampat","Kota Sorong","Sorong Selatan",
    "Tambrauw","Lebak","Pandeglang","Serang","Tangerang","Kota Cilegon","Kota Serang",
    "Kota Tangerang","Kota Tangerang Selatan","Bandung","Bandung Barat","Bekasi","Bogor","Ciamis",
    "Cianjur","Cirebon","Garut","Indramayu","Karawang","Kuningan","Majalengka","Pangandaran",
    "Purwakarta","Subang","Sukabumi","Sumedang","Tasikmalaya","Kota Bandung","Kota Banjar",
    "Kota Bekasi","Kota Bogor","Kota Cimahi","Kota Cirebon","Kota Depok","Kota Sukabumi",
    "Kota Tasikmalaya","Bengkayang","Kapuas Hulu","Kayong Utara","Ketapang","Kubu Raya","Landak",
    "Melawi","Mempawah","Sambas","Sanggau","Sekadau","Sintang","Kota Pontianak","Kota Singkawang",
]

# ─────────────────────────────── KOLOM NOO ────────────────────────────────────
KOLOM_NOO_OUTPUT = [
    "Store ID","Store Name","Channel","Customer Code","Customer Branch Code",
    "Customer Store Code","Customer Store Name","City","Store Address","Longitude",
    "Latitude","Store Type","Visibility Rating","Location Rating","ASM/KAM","SPV",
    "SE/KAE","AOM","TL","PM","MD/SMD","BA1 Name","BA2 Name","BA3 Name","BA4 Name",
    "Group/Branch/Blank","Group Name","NIK","NPWP","Remark",
]

class channel:
    """Auto GT di kolom Channel."""
    def __init__(self, nilai):
        self.nilai = nilai

MAPPING_KOLOM_NOO = {
    "Store ID":            None,
    "Store Name":          "Customer Store Name",
    "Channel":             channel("GT"),
    "Customer Code":       "Customer Code",
    "Customer Branch Code":"Customer Branch Code",
    "Customer Store Code": "Customer Store ID",
    "Customer Store Name": "Customer Branch Name",
    "City":                None,
    "Store Address":       None,
    "Longitude":           None,
    "Latitude":            None,
    "Store Type":          None,
    "Visibility Rating":   None,
    "Location Rating":     None,
    "ASM/KAM":             None,
    "SPV":                 None,
    "SE/KAE":              "Salesman",
    "AOM":                 None,
    "TL":                  None,
    "PM":                  None,
    "MD/SMD":              None,
    "BA1 Name":            None,
    "BA2 Name":            None,
    "BA3 Name":            None,
    "BA4 Name":            None,
    "Group/Branch/Blank":  None,
    "Group Name":          "Customer Branch Name",
    "NIK":                 None,
    "NPWP":                None,
    "Remark":              None,
}
KOLOM_KUNCI_UNIK_NOO = "Customer Store ID"

# ─────────────────────────────── KOLOM SKU ────────────────────────────────────
KOLOM_SKU_OUTPUT = [
    "Customer Code","Customer Name","Product","Product Chinese Short Name",
    "Product English Name","Customer Product Code","Customer Product Name",
    "Specification","Barcode","Description",
]
MAPPING_KOLOM_SKU = {
    "Customer Code":             "Customer Code",
    "Customer Name":             "Customer Branch Name",
    "Product":                   "Customer SKU Code",
    "Product Chinese Short Name": None,
    "Product English Name":       None,
    "Customer Product Code":     "Customer SKU Code",
    "Customer Product Name":     "Customer SKU Name",
    "Specification":             None,
    "Barcode":                   None,
    "Description":               None,
}
KOLOM_KUNCI_UNIK_SKU = "Customer SKU Code"
KEMUNGKINAN_KOLOM_KETERANGAN_FAILURE = [
    "失败原因\nFailure Reasons",
    "失败原因",
    "Failure Reasons",
]
KATA_KUNCI_NOO = "Store not exist, Please config Store mapping"
KATA_KUNCI_SKU = "SKU not exist, Please config SKU mapping"

# ─────────────────────────────── KOLOM TAMPIL (untuk app_main) ────────────────
KOLOM_DISTRI = [
    "distributor_company_name","Distributor Name","Status",
    "Area / Coverage","Area Coverage for NOO","Region",
]
KOLOM_GENERATOR = [
    "Registered Date","Reference ID","Store Name","Distributor",
    "Region","Coverage","Store ID [copy paste value]",
]
MAPPED_KOLOM = {
    "Registered Date": "registered_date",
    "Reference ID":    "reference_id",
    "Store Name":      "store_name",
    "Distributor":     "distributor",
    "Region":          "region",
    "Coverage":        "coverage",
    "Store ID [copy paste value]": "store_id",
}

MAPPING_SUPABASE_KE_VALIDASI = {
    "customer_code":       "Customer Code",
    "store_id":            "Store ID",
    "store_name":          "Store Name",
    "customer_store_code": "Customer Store Code",
    "customer_store_name": "Customer Store Name",
    "city":                "City",
    "store_address":       "Store Address",
    "longitude":           "Longitude",
    "latitude":            "Latitude",
    "store_type":          "Store Type",
    "created_at":          "Created At",
    "generated_at":        "Generated At",
}
KOLOM = [
    "Validasi","Store ID","Distributor","Store Name","Customer Code",
    "Customer Store Code","Customer Store Name","City","Store Address",
    "Longitude","Latitude","Store Type","Created At","Generated At",
]
KOLOM_EDIT   = KOLOM
WAJIB_ISI    = KOLOM[1:]
KOLOM_TAMPIL = ["#"] + KOLOM + ["_sb_id"]

PANJANG_PREFIX = 4
PANJANG_ANGKA  = 5

SKU_KOLOM_DB = {
    "Customer Code":              "customer_code",
    "Customer Name":              "customer_name",
    "Product":                    "product",
    "Customer Product Code":      "customer_product_code",
    "Customer Product Name":      "customer_product_name",
    "Product Chinese Short Name": "product_chinese_short_name",
    "Product English Name":       "product_english_name",
    "Specification":              "specification",
    "Barcode":                    "barcode",
    "Description":                "description",
}
KOLOM_SKU_OPSIONAL = [
    "Product Chinese Short Name","Product English Name","Specification","Barcode","Description",
]
KOLOM_SKU_STAGED = [
    "Pilih","Distributor","Customer Code","Customer Name","Product",
    "Customer Product Code","Customer Product Name",
] + KOLOM_SKU_OPSIONAL
KOLOM_SKU_TAMPIL = [c for c in KOLOM_SKU_STAGED if c not in KOLOM_SKU_OPSIONAL]

# ═════════════════════════════════════════════════════════════════════════════
# SUPABASE HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def sb_insert(table: str, row):
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            json=row, headers=_SB_HEADERS, timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        detail = e.response.text[:300] if e.response else str(e)
        return {"_error": f"HTTP {e.response.status_code}: {detail}"}
    except Exception as e:
        return {"_error": str(e)}

def sb_select(table: str, order: str = "created_at.desc", limit: int = 50):
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={**_SB_HEADERS, "Prefer": ""},
            params={"order": order, "limit": limit},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return []

def sb_patch_by_kode_nama(table: str, kode: str, nama: str, patch: dict) -> bool:
    try:
        r = requests.patch(
            f"{SUPABASE_URL}/rest/v1/{table}",
            params={"Customer Store Code": f"eq.{kode}", "Customer Store Name": f"eq.{nama}"},
            json=patch, headers=_SB_HEADERS, timeout=10,
        )
        if not r.ok:
            st.session_state.err_tagging = f"FAILED {table} ({kode}/{nama}): HTTP {r.status_code} — {r.text[:300]}"
            return False
        if r.json() == []:
            st.session_state.err_tagging = f"0 rows updated in {table} ({kode}/{nama})"
            return False
        return True
    except Exception as e:
        st.session_state.err_tagging = f"EXCEPTION {table} ({kode}/{nama}): {e}"
        return False

def sb_update(table: str, row_id, patch: dict):
    try:
        r = requests.patch(
            f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{row_id}",
            json=patch, headers=_SB_HEADERS, timeout=10,
        )
        if not r.ok:
            st.session_state.err_tagging = f"FAILED {table} (id={row_id}): HTTP {r.status_code} — {r.text[:300]}"
            return False
        if r.json() == []:
            st.session_state.err_tagging = f"0 rows updated in {table} (id={row_id})"
            return False
        return True
    except Exception as e:
        st.session_state.err_tagging = f"EXCEPTION {table} (id={row_id}): {e}"
        return False

def sb_cari_duplikat_kode(table: str, kode_list, kolom: str = "Customer Store Code"):
    kode_bersih = sorted({str(k).strip() for k in kode_list if str(k).strip()})
    ditemukan, gagal_cek = set(), set()
    for kode in kode_bersih:
        try:
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/{table}",
                headers={**_SB_HEADERS, "Prefer": ""},
                params={"select": f'"{kolom}"', kolom: f"ilike.{kode}", "limit": 1},
                timeout=10,
            )
            if not r.ok:
                gagal_cek.add(kode); continue
            if r.json():
                ditemukan.add(kode)
        except Exception:
            gagal_cek.add(kode)
    return ditemukan, gagal_cek

def sb_select_batch(table: str, created_at_iso: str) -> list:
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={**_SB_HEADERS, "Prefer": ""},
            params={"select": "*", "created_at": f"eq.{created_at_iso}"},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return []

def sb_cari_duplikat_sku(table: str, pasangan):
    kol_cc = SKU_KOLOM_DB["Customer Code"]
    kol_pr = SKU_KOLOM_DB["Product"]
    per_kode = {}
    for cc, pr in pasangan:
        per_kode.setdefault(_sku_str(cc), set()).add(_sku_str(pr))
    ditemukan, gagal_cek = set(), set()
    for cc, produk_set in per_kode.items():
        daftar = ",".join('"' + p.replace("\\","\\\\").replace('"','\\"') + '"' for p in sorted(produk_set))
        try:
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/{table}",
                headers={**_SB_HEADERS, "Prefer": ""},
                params={"select": f"{kol_cc},{kol_pr}", kol_cc: f"eq.{cc}", kol_pr: f"in.({daftar})"},
                timeout=10,
            )
            if not r.ok:
                gagal_cek.add(cc); continue
            for row in r.json():
                ditemukan.add(_sku_kunci(row.get(kol_cc), row.get(kol_pr)))
        except Exception:
            gagal_cek.add(cc)
    return ditemukan, gagal_cek

@st.cache_data(ttl=15, show_spinner=False)
def ambil_raw_noo() -> list:
    return sb_select(TABLE_SUBMIT_NOO, order="created_at.desc", limit=500)

def sb_max_angka_store_id() -> dict:
    """Max angka per prefix dari SEMUA Store ID di INPUT_NOO (tanpa batas 500 baris)."""
    hasil, start, step = {}, 0, 1000
    while True:
        try:
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/{TABLE_SUBMIT_NOO}",
                headers={**_SB_HEADERS, "Prefer": "", "Range-Unit": "items",
                         "Range": f"{start}-{start+step-1}"},
                params={"select": '"Store ID"'}, timeout=15)
            if r.status_code == 416:
                break
            r.raise_for_status()
            batch = r.json()
        except Exception:
            break
        for row in batch:
            sid = str(row.get("Store ID") or "").strip().upper()
            p, a = sid[:PANJANG_PREFIX], sid[PANJANG_PREFIX:]
            if a.isdigit():
                hasil[p] = max(hasil.get(p, 0), int(a))
        if len(batch) < step:
            break
        start += step
    return hasil

# ═════════════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS LOADERS
# ═════════════════════════════════════════════════════════════════════════════

def _id_spreadsheet(url_atau_id: str) -> str:
    m = re.search(r"/d/([a-zA-Z0-9_-]+)", url_atau_id)
    return m.group(1) if m else url_atau_id

@st.cache_data(ttl=600, show_spinner=False)
def muat_distributor_dari_sheet() -> pd.DataFrame:
    nama_sheet = SHEET_DISTRI.split("!")[0]
    url = (f"https://docs.google.com/spreadsheets/d/{_id_spreadsheet(HISTORY_GENERATOR)}/gviz/tq"
           f"?tqx=out:csv&sheet={quote(nama_sheet)}&headers=1")
    df = pd.read_csv(url, dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]
    df = df[KOLOM_DISTRI]
    df = df[df["Distributor Name"].str.strip().ne("") & df["Distributor Name"].ne("Distributor Name")]
    return df.reset_index(drop=True)

def daftar_distributor(aktif_saja: bool = True) -> list:
    try:
        df = muat_distributor_dari_sheet()
    except Exception as e:
        st.warning(f"Failed to read Distributor Mapping sheet: {e}")
        return []
    if aktif_saja:
        df = df[df["Status"].str.strip().str.lower() == "active"]
    return sorted(df["Distributor Name"].str.strip().unique())

@st.cache_data(ttl=600, show_spinner=False)
def muat_coverage_mapping() -> pd.DataFrame:
    mentah = pd.read_excel(FILE_PREFIX_ID, header=None, dtype=str, engine="openpyxl").fillna("")
    baris_header = None
    for i in range(min(5, len(mentah))):
        nilai_baris = mentah.iloc[i].astype(str).str.strip().tolist()
        if "Area Coverage" in nilai_baris and "Combine" in nilai_baris:
            baris_header = i
            break
    if baris_header is None:
        raise KeyError("Header 'Area Coverage' / 'Combine' tidak ditemukan di file Prefix id.xlsx.")
    header = mentah.iloc[baris_header].astype(str).str.strip().tolist()
    df = mentah.iloc[baris_header + 1:].reset_index(drop=True)
    seen = {}
    kolom_final = []
    for h in header:
        if h in seen:
            seen[h] += 1
            kolom_final.append(f"{h}__{seen[h]}")
        else:
            seen[h] = 0
            kolom_final.append(h)
    df.columns = kolom_final
    df = df[["Area Coverage","Combine"]]
    df = df[df["Area Coverage"].str.strip().ne("") & df["Combine"].str.strip().ne("")]
    return df.reset_index(drop=True)

@st.cache_data(ttl=600, show_spinner=False)
def muat_prefix_per_distributor() -> dict:
    df_distri = muat_distributor_dari_sheet()
    df_cov    = muat_coverage_mapping()
    df_distri = df_distri.copy()
    df_distri["_join_area"] = df_distri["Area Coverage for NOO"].str.strip()
    kosong = df_distri["_join_area"].eq("")
    df_distri.loc[kosong, "_join_area"] = df_distri.loc[kosong, "Area / Coverage"].str.strip()
    df_distri["_key"] = df_distri["_join_area"].str.strip().str.lower()
    df_cov = df_cov.copy()
    df_cov["_key"] = df_cov["Area Coverage"].str.strip().str.lower()
    df_cov = df_cov.drop_duplicates("_key")
    gabung = df_distri.merge(df_cov[["_key","Combine"]], on="_key", how="left")
    gabung = gabung[gabung["Combine"].notna() & gabung["Combine"].str.strip().ne("")]
    return dict(zip(gabung["Distributor Name"].str.strip(), gabung["Combine"].str.strip()))

@st.cache_data(ttl=300, show_spinner=False)
def max_angka_per_prefix() -> dict:
    hasil = {}
    for sid in get_id():
        prefix, angka_str = sid[:PANJANG_PREFIX], sid[PANJANG_PREFIX:]
        if angka_str.isdigit():
            hasil[prefix] = max(hasil.get(prefix, 0), int(angka_str))
    return hasil


    # sheet_name = SHEET_HISTORY.split("!")[0]
    # url = (f"https://docs.google.com/spreadsheets/d/{_id_spreadsheet(HISTORY_NOO)}/gviz/tq"
    #        f"?tqx=out:csv&sheet={quote(sheet_name)}&headers=1")
    # try:
    #     df = pd.read_csv(url, dtype=str).fillna("")
    #     df.columns = [c.strip() for c in df.columns]
    #     ids = df["Store ID [copy paste value]"].str.strip()
    #     ids = ids[ids.ne("")]
    #     hasil = {}
    #     for sid in ids:
    #         prefix   = sid[:PANJANG_PREFIX]
    #         angka_str = sid[PANJANG_PREFIX:]
    #         if angka_str.isdigit():
    #             hasil[prefix] = max(hasil.get(prefix, 0), int(angka_str))
    #     return hasil
    # except Exception:
    #     return {}



@st.cache_data(ttl=600, show_spinner=False)
def muat_distri_code_map() -> dict:
    nama_sheet = SHEET_DISTRI.split("!")[0]
    url = (f"https://docs.google.com/spreadsheets/d/{_id_spreadsheet(HISTORY_GENERATOR)}/gviz/tq"
           f"?tqx=out:csv&sheet={quote(nama_sheet)}&headers=1")
    df = pd.read_csv(url, dtype=str).fillna("")
    if df.shape[1] < 8:
        return {}
    hasil = {}
    for _, r in df.iterrows():
        nama = r.iloc[1].strip()
        if not nama or nama == df.columns[1] or nama == "Distributor Name":
            continue
        if r.iloc[2].strip().lower() != "active":
            continue
        h = r.iloc[7].strip().upper()
        if not h:
            continue
        g = r.iloc[6].strip()
        if g[:2] in ("12","17"):
            g = g[2:]
        hasil[h] = {"distributor": nama, "company_code": g}
    return hasil

@st.cache_data(ttl=600, show_spinner=False)
def muat_mapping_distributor_name() -> dict:
    df = muat_distributor_dari_sheet()
    df = df[df["Status"].str.strip().str.lower() == "active"]
    return {row["Distributor Name"].strip().upper(): row["Distributor Name"].strip()
            for _, row in df.iterrows()}

def resolve_distributor(nama_toko: str, kota: str = "") -> str:
    if not nama_toko or not str(nama_toko).strip():
        return ""
    kunci = str(nama_toko).strip().upper()
    mapping = muat_mapping_distributor_name()
    return mapping.get(kunci, "")

def region_distributor(nama_distributor: str) -> tuple:
    if not nama_distributor:
        return ()
    try:
        df = muat_distributor_dari_sheet()
    except Exception:
        return ()
    d = df[df["Distributor Name"].str.strip() == nama_distributor.strip()]
    return tuple(sorted({r.strip() for r in d["Region"] if r.strip()}))

@st.cache_data(ttl=600, show_spinner=False)
def muat_distri_customer_code_raw() -> dict:
    nama_sheet = SHEET_DISTRI.split("!")[0]
    url = (f"https://docs.google.com/spreadsheets/d/{_id_spreadsheet(HISTORY_GENERATOR)}/gviz/tq"
           f"?tqx=out:csv&sheet={quote(nama_sheet)}&headers=1")
    df = pd.read_csv(url, dtype=str).fillna("")
    if df.shape[1] < 7:
        return {}
    hasil = {}
    for _, r in df.iterrows():
        nama = r.iloc[1].strip()
        if not nama or nama == "Distributor Name" or nama == df.columns[1]:
            continue
        g = r.iloc[6].strip()
        if nama not in hasil or (not hasil[nama] and g):
            hasil[nama] = g
    return hasil

@st.cache_data(ttl=600, show_spinner=False)
def muat_distri_customer_name() -> dict:
    nama_sheet = SHEET_DISTRI.split("!")[0]
    url = (f"https://docs.google.com/spreadsheets/d/{_id_spreadsheet(HISTORY_GENERATOR)}/gviz/tq"
           f"?tqx=out:csv&sheet={quote(nama_sheet)}&headers=1")
    df = pd.read_csv(url, dtype=str).fillna("")
    if df.shape[1] < 2:
        return {}
    hasil = {}
    for _, r in df.iterrows():
        nama = r.iloc[1].strip()
        if not nama or nama == "Distributor Name" or nama == df.columns[1]:
            continue
        a = r.iloc[0].strip()
        if nama not in hasil or (not hasil[nama] and a):
            hasil[nama] = a
    return hasil

def _kunci_produk(v) -> str:
    return " ".join(_sku_str(v).upper().split())

@st.cache_data(ttl=600, show_spinner=False)
def muat_produk_lookup(nama_sheet: str) -> dict:
    url = (f"https://docs.google.com/spreadsheets/d/{_id_spreadsheet(HISTORY_GENERATOR)}/gviz/tq"
           f"?tqx=out:csv&sheet={quote(nama_sheet.split('!')[0])}&headers=1")
    df = pd.read_csv(url, dtype=str, header=None).fillna("")
    if df.shape[1] < 2:
        return {}
    hasil = {}
    for _, r in df.iterrows():
        a = _kunci_produk(r.iloc[0])
        b = _sku_str(r.iloc[1])
        if not a or not b or a in ("PRODUCT CODE","PRODUCT"):
            continue
        hasil.setdefault(a, b)
    return hasil

def cari_nama_produk(product: str, lookup: dict) -> str:
    return lookup.get(_kunci_produk(product), "")

@st.cache_data(ttl=600, show_spinner=False)
def muat_spv_sales_lookup() -> dict:
    url = (f"https://docs.google.com/spreadsheets/d/{_id_spreadsheet(HISTORY_GENERATOR)}/gviz/tq"
           f"?tqx=out:csv&sheet={quote(SHEET_SALES)}&headers=1")
    try:
        df = pd.read_csv(url, dtype=str).fillna("")
    except Exception:
        return {}
    if df.shape[1] < 3:
        return {}
    hasil = {}
    for _, r in df.iterrows():
        distri = r.iloc[0].strip().upper()
        sales  = r.iloc[1].strip()
        spv    = r.iloc[2].strip()
        if not distri or distri == "NAMA_DISTRIBUTOR" or not spv:
            continue
        lst = hasil.setdefault(distri, {}).setdefault(spv, [])
        if sales and sales not in lst:
            lst.append(sales)
    return hasil

def input_spv_sales(distri: str, kunci: str, c_spv, c_sales, lookup: dict) -> tuple:
    lbl  = "\u200b"
    peta = lookup.get(str(distri).strip().upper(), {})
    if not peta:
        peta = {}
        for d in lookup.values():
            for s, lst in d.items():
                peta.setdefault(s, [])
                peta[s] = sorted(set(peta[s]) | set(lst))
    daftar_spv = sorted(peta)
    if len(daftar_spv) == 1:
        spv = daftar_spv[0]
        c_spv.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{spv}</div>', unsafe_allow_html=True)
    else:
        spv = c_spv.selectbox(lbl, [""] + daftar_spv, label_visibility="collapsed", key=f"{kunci}_spv")
    daftar_sales = sorted(peta.get(spv, []))
    if len(daftar_sales) == 1:
        sales = daftar_sales[0]
        c_sales.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{sales}</div>', unsafe_allow_html=True)
    else:
        sales = c_sales.selectbox(lbl, [""] + daftar_sales, label_visibility="collapsed",
                                  key=f"{kunci}_sales_{spv}", disabled=not spv)
    return spv, sales

# ═════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═════════════════════════════════════════════════════════════════════════════

def ke_wib_naive(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        dt = pd.to_datetime(s, errors="coerce")
    else:
        dt = pd.to_datetime(s, errors="coerce", utc=True)
    if dt.dt.tz is not None:
        dt = dt.dt.tz_convert(ZONA_WAKTU).dt.tz_localize(None)
    return dt.astype("datetime64[ns]")

def waktu_sekarang() -> str:
    return datetime.datetime.now(ZONA_WAKTU).strftime("%d %b %Y %H:%M")

def hitung_validasi(df: pd.DataFrame) -> pd.Series:
    lon    = pd.to_numeric(df["Longitude"], errors="coerce")
    lat    = pd.to_numeric(df["Latitude"],  errors="coerce")
    alamat = df["Store Address"].fillna("").astype(str).str.strip()
    nama   = df["Store Name"].fillna("").astype(str).str.strip()
    kosong = lon.isna() | lat.isna() | alamat.eq("") | nama.eq("")
    return pd.Series(np.where(kosong, STATUS_REVIEW, STATUS_VALID), index=df.index)

def hasil_ke_excel(df: pd.DataFrame) -> bytes:
    from openpyxl.styles import Font, Border
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, startrow=1, sheet_name="Sheet1")
        ws = writer.sheets["Sheet1"]
        for sel in ws[2]:
            sel.font  = Font(bold=False)
            sel.border = Border()
    return buffer.getvalue()

def catat_riwayat(aksi: str, detail: str = ""):
    ss    = st.session_state
    df    = ss.get("df_toko", pd.DataFrame())
    total = len(df)
    ss.setdefault("riwayat", []).insert(0, {
        "Waktu": waktu_sekarang(), "Pengguna": PENGGUNA,
        "Aksi": aksi, "Detail": detail, "Total": total,
    })
    sb_insert(TABLE_RIWAYAT, {
        "waktu":    datetime.datetime.now(ZONA_WAKTU).isoformat(timespec="seconds"),
        "pengguna": PENGGUNA,
        "aksi":     aksi,
        "detail":   detail,
        "total":    total,
    })

def set_keterangan_noo(jenis: str, teks: str):
    ss = st.session_state
    ss.noo_keterangan     = f"{jenis}:{teks}"
    ss.pesan_toast        = teks
    ss.pesan_toast_icon   = {"success":"✅","warning":"⚠️","error":"❌"}.get(jenis, "ℹ️")

def tampilkan_keterangan_noo():
    ss = st.session_state
    if ss.get("noo_keterangan"):
        jenis, _, teks = ss.noo_keterangan.partition(":")
        {"success": st.success, "warning": st.warning, "error": st.error}.get(jenis, st.caption)(teks)

def noo_row_ke_supabase(row: pd.Series, waktu_submit: str) -> dict:
    def val(k):
        v = row.get(k, "")
        if pd.isna(v):
            return ""
        return v.item() if hasattr(v, "item") else v
    data = {k: val(k) for k in KOLOM_NOO_OUTPUT if k != "Distributor"}
    data["Channel"]      = data["Channel"] or "GT"
    data["Longitude"]    = data["Longitude"] or None
    data["Latitude"]     = data["Latitude"]  or None
    data["created_at"]   = waktu_submit
    data["tagging"]      = STATUS_REVIEW
    data["generated_at"] = None
    return data

def judul_opsi(nomor: int, judul: str, sub: str = "") -> str:
    sub_html = f'<span class="sub-opsi">· {sub}</span>' if sub else ""
    return (f'<div class="judul-opsi"><span class="nomor-opsi">{nomor}</span>'
            f'<span class="teks-opsi">{judul}</span>{sub_html}</div>')

# ═════════════════════════════════════════════════════════════════════════════
# PIVOT FAILURE LIST (shared by both apps, mostly used in submission)
# ═════════════════════════════════════════════════════════════════════════════

def cari_kolom_keterangan_failure(df_mentah: pd.DataFrame) -> str:
    for nama in KEMUNGKINAN_KOLOM_KETERANGAN_FAILURE:
        if nama in df_mentah.columns:
            return nama
    raise KeyError("Failure reason column not found. Columns searched: "
                   + ", ".join(KEMUNGKINAN_KOLOM_KETERANGAN_FAILURE))

def ambil_baris_unik(df: pd.DataFrame, kolom_kunci: str) -> pd.DataFrame:
    df = df.copy()
    df["_kunci_bersih"] = df[kolom_kunci].astype(str).str.strip().str.upper()
    df = df[df["_kunci_bersih"] != ""]
    df = df.drop_duplicates(subset=["_kunci_bersih"], keep="first")
    return df.drop(columns="_kunci_bersih").reset_index(drop=True)

def pivot_sku_mapping(df_mentah: pd.DataFrame) -> pd.DataFrame:
    kolom_keterangan = cari_kolom_keterangan_failure(df_mentah)
    keterangan = df_mentah[kolom_keterangan].astype(str)
    baris_sku  = df_mentah[keterangan.str.contains(KATA_KUNCI_SKU, case=False, na=False)]
    baris_sku  = ambil_baris_unik(baris_sku, KOLOM_KUNCI_UNIK_SKU)
    hasil = pd.DataFrame()
    for kolom_output, kolom_sumber in MAPPING_KOLOM_SKU.items():
        hasil[kolom_output] = baris_sku[kolom_sumber] if kolom_sumber else ""
    return hasil[KOLOM_SKU_OUTPUT]

def pivot_noo(df_mentah: pd.DataFrame) -> pd.DataFrame:
    kolom_keterangan = cari_kolom_keterangan_failure(df_mentah)
    keterangan = df_mentah[kolom_keterangan].astype(str)
    baris_noo  = df_mentah[keterangan.str.contains(KATA_KUNCI_NOO, case=False, na=False)]
    baris_noo  = ambil_baris_unik(baris_noo, KOLOM_KUNCI_UNIK_NOO)
    hasil = pd.DataFrame()
    for kolom_output, kolom_sumber in MAPPING_KOLOM_NOO.items():
        if kolom_sumber is None:
            hasil[kolom_output] = ""
        elif isinstance(kolom_sumber, channel):
            hasil[kolom_output] = kolom_sumber.nilai
        else:
            hasil[kolom_output] = baris_noo[kolom_sumber]
    return hasil[KOLOM_NOO_OUTPUT]

# ═════════════════════════════════════════════════════════════════════════════
# SKU HELPER FUNCTIONS
# ═════════════════════════════════════════════════════════════════════════════

def _sku_str(v) -> str:
    return "" if v is None or (not isinstance(v, str) and pd.isna(v)) else str(v).strip()

def _sku_kunci(customer_code, product) -> tuple:
    return (_kunci_produk(customer_code), _kunci_produk(product))

def bangun_customer_code(raw: str, product: str) -> str:
    raw = _sku_str(raw)
    if not raw:
        return ""
    sisa = re.sub(r"^\s*\d+\s*/\s*", "", raw)
    if sisa == raw:
        sisa = re.sub(r"^(12|17)", "", raw)
    sisa = sisa.strip()
    if not sisa:
        return ""
    prefix = "12" if "G2G" in _sku_str(product).upper() else "17"
    return prefix + sisa
