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
# URL & key dibaca dari .streamlit/secrets.toml (lokal, sudah di-gitignore) atau Secrets di hosting. Jangan ditulis di kode.
def _baca_kredensial_supabase() -> tuple:
    try:
        return st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"]
    except Exception:
        pass
    if os.environ.get("SUPABASE_URL") and os.environ.get("SUPABASE_KEY"):
        return os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"]
    try:
        import tomllib
        folder = os.path.dirname(os.path.abspath(__file__))
        for _ in range(4):
            f = os.path.join(folder, ".streamlit", "secrets.toml")
            if os.path.exists(f):
                with open(f, "rb") as fh:
                    d = tomllib.load(fh)
                if d.get("SUPABASE_URL") and d.get("SUPABASE_KEY"):
                    return d["SUPABASE_URL"], d["SUPABASE_KEY"]
            folder = os.path.dirname(folder)
    except Exception:
        pass
    return None, None

SUPABASE_URL, SUPABASE_KEY = _baca_kredensial_supabase()
if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("SUPABASE_URL / SUPABASE_KEY not found. Add them to .streamlit/secrets.toml (local) or the hosting Secrets (production).")
    st.stop()
SUPABASE_URL = SUPABASE_URL.strip().rstrip("/")
SUPABASE_KEY = SUPABASE_KEY.strip()


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
# >>> DAFTAR STATUS DROPDOWN "Validation" (Store Validation) <<<
# Urutan 0 dan 1 JANGAN diubah (dipakai sebagai STATUS_VALID / STATUS_REVIEW). Status baru ditambah di belakang.
KOLOM_VALIDASI = ["Generate ID", "Need Review", "Existing", "DMS Done", "DMS & Basis Done"]   # HANYA 5 status, jangan tambah
STATUS_VALID   = KOLOM_VALIDASI[0]
STATUS_REVIEW  = KOLOM_VALIDASI[1]
STATUS_DMS     = "DMS Done"
STATUS_BASIS   = "DMS & Basis Done"
# Status "final": kalau dipilih, baris DIKUNCI (Store ID dkk tidak bisa diedit lagi di Store Validation).
STATUS_TERKUNCI = (STATUS_DMS, STATUS_BASIS)
# Nama lama status (versi sebelumnya) -> nama sekarang, supaya data yang sudah tersimpan tetap terbaca.
ALIAS_STATUS = {"DMS Uploaded": STATUS_DMS, "Basis Updated": STATUS_BASIS, "Basis Done": STATUS_BASIS}

WARNA_VALIDASI = {
    "Generate ID": "background-color: #d3f9d8; color: #1b5e20",   # hijau
    "Need Review": "background-color: #ffd6e0; color: #9c1c4a",   # merah muda
    "Existing":    "background-color: #fff3b0; color: #7a5b00",   # kuning
    "DMS Done":    "background-color: #cfe8ff; color: #0b4a8f",   # biru
    "DMS & Basis Done": "background-color: #e6d9ff; color: #5b21b6",   # ungu
}

# >>> LABEL STATUS DI HALAMAN HISTORY (app_submission) <<<
# Kiri = nilai status asli di Supabase (kolom tagging), kanan = teks yang dilihat user/distributor di History.
# Mau ganti istilahnya? Ubah sisi kanan saja (dan warnanya di WARNA_HISTORY di bawah).
LABEL_HISTORY = {
    "Need Review":   "Under Review",         # NOO baru masuk, masih dicek
    "Existing":      "Existing Store",       # bukan NOO: toko sudah ada
    "Generate ID":   "Awaiting DMS Upload",  # Store ID sudah dibuat, menunggu di-upload ke DMS
    "DMS Done":      "Ready in DMS",         # sudah di-upload ke DMS
    "DMS & Basis Done": "Fully Processed",   # sudah masuk DMS dan basis, semua tahap selesai
    "Basis Done":    "Fully Processed",      # nama lama status (kompatibilitas)
    "DMS Uploaded":  "Ready in DMS",         # nama lama status (kompatibilitas)
    "Basis Updated": "Fully Processed",      # nama lama status (kompatibilitas)
}
WARNA_HISTORY = {
    "Under Review":        "background-color: #fff3cd; color: #8a5a00; font-weight: 600",   # kuning
    "Existing Store":      "background-color: #e5e7eb; color: #374151; font-weight: 600",   # abu
    "Awaiting DMS Upload": "background-color: #d3f9d8; color: #1b5e20; font-weight: 600",   # hijau (sama dgn Generate ID)
    "Ready in DMS":        "background-color: #cfe8ff; color: #0b4a8f; font-weight: 600",   # biru (sama dgn DMS Done)
    "Fully Processed":     "background-color: #e6d9ff; color: #5b21b6; font-weight: 600",   # ungu (sama dgn Basis Done)
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
    def __init__(self, nilai):
        self.nilai = nilai

MAPPING_KOLOM_NOO = {
    "Store ID":            None,
    "Store Name":          "Customer Store Name",
    "Channel":             channel("GT"),
    "Customer Code":       "Customer Code",
    "Customer Branch Code":"Customer Branch Code",
    "Customer Store Code": "Customer Store ID",
    "Customer Store Name": "Customer Store Name",   # nama toko (sama dengan Store Name), distributor ada di Group Name
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
    "Customer Name":             "Customer Name",
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
        # `if e.response` salah: Response bernilai False untuk status 4xx/5xx, jadi isi error Supabase tidak pernah terbaca
        detail = e.response.text[:300] if e.response is not None else str(e)
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

def sb_baca_status_id(pasangan) -> dict:
    pasangan = {(str(k).strip(), str(n).strip()) for k, n in pasangan if str(k).strip()}
    if not pasangan:
        return {}
    kode_list = sorted({k for k, _ in pasangan})
    hasil = {}
    try:
        for i in range(0, len(kode_list), 100):
            daftar = ",".join('"' + k.replace('"', '') + '"' for k in kode_list[i:i + 100])
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/{TABLE_SUBMIT_NOO}",
                headers={**_SB_HEADERS, "Prefer": ""},
                params={"select": '"Customer Store Code","Customer Store Name",tagging,"Store ID"',
                        "Customer Store Code": f"in.({daftar})"},
                timeout=15)
            r.raise_for_status()
            for row in r.json():
                kunci = (str(row.get("Customer Store Code") or "").strip(), str(row.get("Customer Store Name") or "").strip())
                if kunci in pasangan:
                    hasil[kunci] = (str(row.get("tagging") or "").strip(), str(row.get("Store ID") or "").strip())
        return hasil
    except Exception:
        return None

@st.cache_data(ttl=5, show_spinner=False)
def sb_baca_status_id_singkat(pasangan: tuple):
    return sb_baca_status_id(pasangan)

def sb_delete(table: str, filters: dict):
    if not filters or any(str(v).strip() == "" for v in filters.values()):
        return 0, "filter kosong, penghapusan dibatalkan"
    try:
        r = requests.delete(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={**_SB_HEADERS, "Prefer": "return=representation"},
            params={k: f"eq.{v}" for k, v in filters.items()}, timeout=15,
        )
        if not r.ok:
            return 0, f"HTTP {r.status_code}: {r.text[:200]}"
        return len(r.json()), ""
    except Exception as e:
        return 0, str(e)

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


# ══════════════════════════════════════════════
# CACHE "STALE-WHILE-REVALIDATE": halaman tidak pernah menunggu fetch ulang setelah TTL habis
# ══════════════════════════════════════════════
import copy as _copy
import threading as _threading
import time as _time

_SWR_STATE: dict = {}
_SWR_LOCK = _threading.RLock()

_PREWARM_SUDAH: set = set()

def prewarm(*fungsi):
    baru = [f for f in fungsi if f.__name__ not in _PREWARM_SUDAH]
    if not baru:
        return
    _PREWARM_SUDAH.update(f.__name__ for f in baru)
    def kerja():
        for f in baru:
            try:
                f()
            except Exception:
                _PREWARM_SUDAH.discard(f.__name__)
    _threading.Thread(target=kerja, daemon=True).start()

def _ada_isi(v) -> bool:
    if hasattr(v, "empty"):                 # DataFrame
        return not v.empty
    try:
        return bool(v)
    except Exception:
        return True

def swr_cache(ttl: float, salin: bool = True):
    def deco(fn):
        # state disimpan di level modul (kunci = nama fungsi): file app dijalankan ulang tiap rerun sehingga
        # fungsi di dalamnya dibuat ulang; state di closure akan ikut hilang
        st_ = _SWR_STATE.setdefault(fn.__module__ + "." + fn.__qualname__,
                                    {"val": None, "ada": False, "ts": 0.0, "sibuk": False,
                                     "awal": _threading.Lock()})
        kunci = _SWR_LOCK

        def _segarkan():
            try:
                baru = fn()
                with kunci:
                    # fetch gagal biasanya mengembalikan kosong -> jangan timpa data bagus yang sudah ada
                    if _ada_isi(baru) or not st_["ada"]:
                        st_["val"], st_["ada"] = baru, True
                    st_["ts"] = _time.time()
            except Exception:
                with kunci:
                    st_["ts"] = _time.time() - ttl / 2          # coba lagi sebentar lagi
            finally:
                with kunci:
                    st_["sibuk"] = False

        def wrapper():
            with kunci:
                ada, umur, sibuk = st_["ada"], _time.time() - st_["ts"], st_["sibuk"]
            if not ada:
                with st_["awal"]:                       # satu fetch awal saja; yang lain menunggu hasilnya
                    with kunci:
                        sudah = st_["ada"]
                    if not sudah:
                        baru = fn()
                        with kunci:
                            st_["val"], st_["ada"], st_["ts"] = baru, True, _time.time()
            elif umur > ttl and not sibuk:
                with kunci:
                    st_["sibuk"] = True
                _threading.Thread(target=_segarkan, daemon=True).start()
            with kunci:
                return _copy.deepcopy(st_["val"]) if salin else st_["val"]

        def clear():
            with kunci:
                st_["ada"], st_["val"], st_["ts"] = False, None, 0.0
        wrapper.clear = clear
        wrapper.__name__ = fn.__name__
        return wrapper
    return deco

@swr_cache(ttl=30)
def ambil_raw_noo() -> list:
    return sb_select(TABLE_SUBMIT_NOO, order="created_at.desc", limit=500)

# KEMBALIKAN KE INI
def sb_max_angka_store_id() -> dict:
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

@st.cache_resource(ttl=600, show_spinner=False)  # read-only dict: tanpa salinan/pickle tiap dipanggil
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

# ─────────────────────────────── DATA MERGE (ID yang sudah pernah dipakai) ────
# >>> SUMBER TAMBAHAN Store ID existing (selain BigQuery & Supabase) <<<
# Tiap baris = (link/ID spreadsheet, nama tab, nama kolom header berisi Store ID). Mau tambah/kurangi sumber: edit daftar ini.
# Spreadsheet harus bisa dibaca lewat link (sama seperti Distributor Mapping yang sudah dipakai app).
DATA_MERGE_SUMBER = [
    # "DATA GENERATOR" tidak lagi dibaca per-ID (33rb baris, lambat) -> pakai pivot max per prefix, lihat DATA_MERGE_PIVOT.
    (HISTORY_GENERATOR, "id dms",         "store_code"),                    # ID yang sudah ada di DMS
    # Kolom berupa ANGKA = dibaca per nomor kolom (tanpa header). ID hasil Apps Script di tab NEW Generator (kolom G).
    (HISTORY_GENERATOR, "NEW Generator",  7),
]
# Pola ID valid: 4 huruf + angka (mis. IESP01256). ID lain (kosong, 5 huruf, teks bebas) diabaikan.
POLA_STORE_ID = re.compile(r"^[A-Z]{%d}\d+$" % PANJANG_PREFIX)

@st.cache_data(ttl=120, show_spinner=False)
def muat_id_data_merge() -> tuple:
    hasil, galat = set(), []
    for url_sheet, tab, kolom in DATA_MERGE_SUMBER:
        try:
            if isinstance(kolom, int):      # per nomor kolom; baris non-ID (header/teks) dibuang oleh POLA_STORE_ID
                url = (f"https://docs.google.com/spreadsheets/d/{_id_spreadsheet(url_sheet)}/gviz/tq"
                       f"?tqx=out:csv&sheet={quote(tab)}&headers=0")
                df = pd.read_csv(url, dtype=str, header=None, usecols=[kolom - 1])
            else:
                url = (f"https://docs.google.com/spreadsheets/d/{_id_spreadsheet(url_sheet)}/gviz/tq"
                       f"?tqx=out:csv&sheet={quote(tab)}&headers=1")
                df = pd.read_csv(url, dtype=str, usecols=lambda c: str(c).strip() == kolom)
            for v in df.iloc[:, 0].dropna():
                v = str(v).strip().upper()
                if POLA_STORE_ID.match(v):
                    hasil.add(v)
        except Exception as e:
            galat.append(f"{tab}: {type(e).__name__}")
    return frozenset(hasil), ("Could not read ID source " + ", ".join(galat)
                              + " — new IDs may duplicate existing ones.") if galat else ""

# Pivot max per prefix yang sudah dihitung di sheet (tab DATA GENERATOR, kolom L = PREFIX, M = NUMBER).
# Cuma 2 kolom & beberapa ratus baris, jadi jauh lebih ringan daripada baca 33rb Store ID.
DATA_MERGE_PIVOT = (HISTORY_GENERATOR, "DATA GENERATOR", "L", "M")

@st.cache_data(ttl=120, show_spinner=False)
def muat_max_pivot() -> dict:
    url_sheet, tab, kol_prefix, kol_angka = DATA_MERGE_PIVOT
    url = (f"https://docs.google.com/spreadsheets/d/{_id_spreadsheet(url_sheet)}/gviz/tq"
           f"?tqx=out:csv&sheet={quote(tab)}&tq={quote(f'select {kol_prefix}, {kol_angka} where {kol_prefix} is not null')}")
    hasil = {}
    try:
        df = pd.read_csv(url, dtype=str)
        for p, a in zip(df.iloc[:, 0].fillna(""), df.iloc[:, 1].fillna("")):
            p = str(p).strip().upper()
            a = str(a).strip().replace(",", "")
            if re.fullmatch(r"[A-Z]{%d}" % PANJANG_PREFIX, p) and a.isdigit():
                hasil[p] = max(hasil.get(p, 0), int(a))
    except Exception:
        return {}
    return hasil

def max_angka_data_merge() -> dict:
    hasil = dict(muat_max_pivot())
    for sid in muat_id_data_merge()[0]:
        p, a = sid[:PANJANG_PREFIX], sid[PANJANG_PREFIX:]
        hasil[p] = max(hasil.get(p, 0), int(a))
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



@st.cache_resource(ttl=600, show_spinner=False)  # read-only dict: tanpa salinan/pickle tiap dipanggil
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

@st.cache_resource(ttl=600, show_spinner=False)  # read-only dict: tanpa salinan/pickle tiap dipanggil
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

@st.cache_resource(ttl=600, show_spinner=False)  # read-only dict: tanpa salinan/pickle tiap dipanggil
def muat_distri_customer_code_raw() -> dict:
    nama_sheet = SHEET_DISTRI.split("!")[0]
    url = (f"https://docs.google.com/spreadsheets/d/{_id_spreadsheet(HISTORY_GENERATOR)}/gviz/tq"
           f"?tqx=out:csv&sheet={quote(nama_sheet)}&headers=1")
    df = pd.read_csv(url, dtype=str).fillna("")
    if df.shape[1] < 7:
        return {}
    kol_status = next((c for c in df.columns if str(c).strip().lower() == "status"), None)
    hasil = {}
    for _, r in df.iterrows():
        nama = r.iloc[1].strip()
        if not nama or nama == "Distributor Name" or nama == df.columns[1]:
            continue
        if kol_status is not None and str(r[kol_status]).strip().lower() != "active":
            continue
        g = r.iloc[6].strip()
        if nama not in hasil or (not hasil[nama] and g):
            hasil[nama] = g
    return hasil

@st.cache_resource(ttl=600, show_spinner=False)  # read-only dict: tanpa salinan/pickle tiap dipanggil
def muat_distri_customer_name() -> dict:
    nama_sheet = SHEET_DISTRI.split("!")[0]
    url = (f"https://docs.google.com/spreadsheets/d/{_id_spreadsheet(HISTORY_GENERATOR)}/gviz/tq"
           f"?tqx=out:csv&sheet={quote(nama_sheet)}&headers=1")
    df = pd.read_csv(url, dtype=str).fillna("")
    if df.shape[1] < 2:
        return {}
    kol_status = next((c for c in df.columns if str(c).strip().lower() == "status"), None)
    hasil = {}
    for _, r in df.iterrows():
        nama = r.iloc[1].strip()
        if not nama or nama == "Distributor Name" or nama == df.columns[1]:
            continue
        if kol_status is not None and str(r[kol_status]).strip().lower() != "active":
            continue
        a = r.iloc[0].strip()
        if nama not in hasil or (not hasil[nama] and a):
            hasil[nama] = a
    return hasil

def _kunci_produk(v) -> str:
    return " ".join(_sku_str(v).upper().split())
GID_PRODUCT_DESCRIPTION = "2106824930"

@st.cache_resource(ttl=600, show_spinner=False)  # read-only dict: tanpa salinan/pickle tiap dipanggil
def muat_deskripsi_produk() -> dict:
    url = (f"https://docs.google.com/spreadsheets/d/{_id_spreadsheet(HISTORY_GENERATOR)}"
           f"/export?format=csv&gid={GID_PRODUCT_DESCRIPTION}")
    df = pd.read_csv(url, dtype=str, header=None).fillna("")
    if df.shape[1] < 2:
        return {}
    hasil = {}
    for _, r in df.iterrows():
        kode = _sku_str(r.iloc[0])
        desk = _sku_str(r.iloc[1])
        if not kode or not desk or kode.strip().upper() in ("PRODUCT CODE", "PRODUCT"):
            continue
        hasil.setdefault(desk, kode)
    return hasil

@st.cache_resource(ttl=600, show_spinner=False)  # read-only dict: tanpa salinan/pickle tiap dipanggil
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

def _rapikan_nama(t) -> str:
    return " ".join(str(t or "").split())

def _kunci_nama(t) -> str:
    return _rapikan_nama(t).upper()

def _gabung_unik(kumpulan) -> list:
    lihat, hasil = set(), []
    for nama in kumpulan:
        k = _kunci_nama(nama)
        if k and k not in lihat:
            lihat.add(k); hasil.append(_rapikan_nama(nama))
    return hasil

@st.cache_resource(ttl=600, show_spinner=False)  # read-only dict: tanpa salinan/pickle tiap dipanggil
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
        sales  = _rapikan_nama(r.iloc[1])
        spv    = _rapikan_nama(r.iloc[2])
        if not distri or distri == "NAMA_DISTRIBUTOR" or not spv:
            continue
        peta_d = hasil.setdefault(distri, {})
        # ejaan sama (beda huruf besar/kecil atau spasi) dianggap satu: pakai penulisan yang pertama muncul
        spv = next((k for k in peta_d if _kunci_nama(k) == _kunci_nama(spv)), spv)
        lst = peta_d.setdefault(spv, [])
        if sales and _kunci_nama(sales) not in {_kunci_nama(x) for x in lst}:
            lst.append(sales)
    return hasil

def _norm_distri(t: str) -> str:
    t = re.sub(r"[^A-Z0-9 ]", " ", str(t).upper())
    t = re.sub(r"\b(PT|CV|UD|TOKO)\b", " ", t)
    return re.sub(r"\s+", " ", t).strip()

def _cocokkan_distributor_spv(distri: str, lookup: dict) -> dict:
    kunci = _norm_distri(distri)
    if not kunci:
        return {}
    norm = {d: _norm_distri(d) for d in lookup}
    # 1) sama setelah dinormalisasi
    for d, n in norm.items():
        if n == kunci:
            return lookup[d]
    # 2) salah satu nama memuat yang lain (mis. "JAYATAMA" vs "CV JAYATAMA - BALI")
    kand = [d for d, n in norm.items() if n and (n in kunci or kunci in n)]
    if len(kand) == 1:
        return lookup[kand[0]]
    # 3) kemiripan tinggi
    try:
        from rapidfuzz import fuzz, process
        hit = process.extractOne(kunci, list(norm.values()), scorer=fuzz.token_set_ratio, score_cutoff=90)
        if hit:
            for d, n in norm.items():
                if n == hit[0]:
                    return lookup[d]
    except Exception:
        pass
    return {}

def peta_spv_sales(distri: str, lookup: dict) -> dict:
    peta = lookup.get(str(distri).strip().upper(), {})
    if not peta:
        peta = _cocokkan_distributor_spv(distri, lookup)
    if not peta:
        gab = {}
        for d in lookup.values():
            for s, lst in d.items():
                ks = _kunci_nama(s)
                nama_spv, daftar = gab.setdefault(ks, (_rapikan_nama(s), []))
                daftar.extend(lst)
        peta = {nama_spv: _gabung_unik(daftar) for nama_spv, daftar in gab.values()}
    return peta

def input_spv_sales(distri: str, kunci: str, c_spv, c_sales, lookup: dict) -> tuple:
    lbl  = "​"
    peta = peta_spv_sales(distri, lookup)
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

def hasil_ke_excel(df: pd.DataFrame, startrow: int = 1) -> bytes:
    from openpyxl.styles import Font, Border
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, startrow=startrow, sheet_name="Sheet1")
        ws = writer.sheets["Sheet1"]
        for sel in ws[startrow + 1]:       # baris header = startrow+1 (1-indexed): tanpa tebal & tanpa garis
            sel.font   = Font(bold=False)
            sel.border = Border()
    return buffer.getvalue()

def nama_pengguna_sesi() -> str:
    user  = st.session_state.get("user") or {}
    uname = str(user.get("username", "")).strip()
    if not uname:
        return PENGGUNA
    if user.get("tagging") == "Admin Distributor":
        return f"Admin - {uname.upper()}"
    return uname

# Region distributor -> RSA yang meng-handle (nama region harus sama persis dengan sheet)
REGION_TO_RSA = {
    "East Kalimantan": "erlin", "South Kalimantan": "erlin", "West Kalimantan": "erlin",
    "Maluku Papua": "shaltsa", "Northern Sulawesi": "shaltsa", "Southern Sulawesi": "shaltsa",
    "Bali Nusa Tenggara": "geirda", "Northern East Java": "geirda", "Southern East Java": "geirda",
    "Central Java 1": "furqan", "Central Java 2": "furqan", "Central Java 3": "furqan",
    "Jabodetabek": "furqan",
    "West Java": "kayla", "Central Sumatera": "kayla",
    "Southern Sumatera 1": "aqil", "Southern Sumatera 2": "aqil", "Northern Sumatera": "aqil",
    # Penulisan region lain yang benar-benar ada di sheet Distributor Mapping (kolom F).
    # Dipetakan ke RSA dari grup region yang sejenis — mohon dicek.
    "Sulawesi 1": "shaltsa", "Sulawesi 2": "shaltsa", "Papua Maluku": "shaltsa",
    "East Java": "geirda",
    "Central Java": "furqan", "South Central Java": "furqan", "North Central Java": "furqan",
    "Jakarta (Csa)": "furqan",
    "West Java (Sd)": "kayla",
    "Southern Sumatera": "aqil",
}

def rsa_identifier(nama_distributor: str, fallback: str = "rsa") -> str:
    nama = " ".join(str(nama_distributor or "").split()).lower()
    if not nama:
        return fallback
    try:
        df = muat_distributor_dari_sheet()
    except Exception:
        return fallback
    peta = {" ".join(k.split()).lower(): v for k, v in REGION_TO_RSA.items()}   # abaikan beda huruf/spasi
    cocok = df[df["Distributor Name"].map(lambda s: " ".join(str(s).split()).lower()) == nama]
    for rg in cocok["Region"]:
        rsa = peta.get(" ".join(str(rg).split()).lower())
        if rsa:
            return f"rsa_{rsa}"
    return fallback

def catat_riwayat(aksi: str, detail: str = "", pengguna: str = None, distributor: str = None):
    ss    = st.session_state
    df    = ss.get("df_toko", pd.DataFrame())
    total = len(df)
    pengguna = pengguna or nama_pengguna_sesi()
    # user RSA: jejak submit dicatat spesifik per RSA sesuai region distributor yang disubmit
    if distributor and (ss.get("user") or {}).get("tagging") == "RSA":
        pengguna = rsa_identifier(distributor, fallback=pengguna)
    ss.setdefault("riwayat", []).insert(0, {
        "Waktu": waktu_sekarang(), "Pengguna": pengguna,
        "Aksi": aksi, "Detail": detail, "Total": total,
    })
    sb_insert(TABLE_RIWAYAT, {
        "waktu":    datetime.datetime.now(ZONA_WAKTU).isoformat(timespec="seconds"),
        "pengguna": pengguna,
        "aksi":     aksi,
        "detail":   detail,
        "total":    total,
    })

def tampilkan_toast_warna(teks: str, icon: str = ":material/check_circle:"):
    import html as _html
    gagal = ("warning" in icon) or ("error" in icon)
    bg, bd, fg, simbol = (("#fde2e4","#f5a3ab","#8a1c27","warning") if gagal
                          else ("#dcf5e3","#9fd8b0","#17612f","check_circle"))
    st.markdown(
        f'<style>@keyframes toastWarna{{0%{{opacity:0;transform:translateY(-8px)}}6%{{opacity:1;transform:none}}'
        f'90%{{opacity:1}}100%{{opacity:0;visibility:hidden}}}}</style>'
        f'<div style="position:fixed;top:64px;right:24px;z-index:999999;max-width:360px;padding:12px 16px;'
        f'border-radius:10px;background:{bg};border:1px solid {bd};color:{fg};font-size:14px;'
        f'box-shadow:0 4px 14px rgba(0,0,0,.12);animation:toastWarna 5s ease forwards;pointer-events:none">'
        f'<b>{"&#9888;" if gagal else "&#10003;"}</b>&nbsp; {_html.escape(str(teks))}</div>',
        unsafe_allow_html=True)

def set_keterangan_noo(jenis: str, teks: str):
    ss = st.session_state
    ss.noo_keterangan     = f"{jenis}:{teks}"
    ss.pesan_toast        = teks
    ss.pesan_toast_icon   = {"success":":material/check_circle:","warning":":material/warning:",
                              "error":":material/error:"}.get(jenis, ":material/info:")

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

# ─────────────────────────────── KIRIM NOO KE GOOGLE SHEET ────────────────────
# Spreadsheet tujuan salinan data NOO Submission (sheet "test"). Spreadsheet harus di-share (Editor) ke
# noo-spreadsheet@project-noo-515103.iam.gserviceaccount.com.
# >>> GANTI LINK SPREADSHEET DI SINI <<<
# NOO_SHEET_ID  = potongan URL di antara "/d/" dan "/edit", contoh:
#   https://docs.google.com/spreadsheets/d/<<< INI ID-NYA >>>/edit?gid=123
# Spreadsheet baru juga harus di-share (Editor) ke no-spreadsheet@skintific-data-warehouse.iam.gserviceaccount.com
NOO_SHEET_ID   = "1EMgQSe9V15HtOg8qyjT-pjfWw1vEjiDd9D0aRz5IKhc"
# NOO_SHEET_TAB = nama tab (sheet) tujuan di spreadsheet tersebut, harus sama persis dengan nama tab-nya
NOO_SHEET_TAB  = "RAW"
# fallback lokal kalau st.secrets["noo_sheet"] tidak terbaca (mis. app dijalankan dari folder lain); jangan di-commit
NOO_SHEET_KEY_FILES = [os.path.join(os.path.dirname(__file__), "noo-spreadsheet.json"),
                       os.path.join(os.path.dirname(os.path.dirname(__file__)), "noo-spreadsheet.json")]
# >>> TAMBAH / KURANGI KOLOM DI SINI YAAA -SHALTSA<<<
# Daftar ini = urutan kolom di sheet (kiri ke kanan) sekaligus header baris 1. Header otomatis ditulis ulang
# kalau daftar ini berubah. Untuk mengurangi: hapus namanya. Untuk menambah: tulis namanya, persis sama (huruf besar/kecil).
# Nama yang tersedia:
#   - "Submitted At", "Source", "Submitted By"   (diisi otomatis oleh kirim_noo_ke_sheet)
#   - kolom NOO: "Store ID","Store Name","Channel","Customer Code","Customer Branch Code","Customer Store Code",
#     "Customer Store Name","City","Store Address","Longitude","Latitude","Store Type","Visibility Rating",
#     "Location Rating","ASM/KAM","SPV","SE/KAE","AOM","TL","PM","MD/SMD","BA1 Name","BA2 Name","BA3 Name",
#     "BA4 Name","Group/Branch/Blank","Group Name","NIK","NPWP","Remark"  (sama dengan KOLOM_NOO_OUTPUT)
#   - "created_at", "tagging"
# Catatan: kalau kolom diubah setelah sheet berisi data, baris lama tidak ikut bergeser: bersihkan sheet-nya dulu.
NOO_SHEET_KOLOM = [
    "Submitted At", "Source", "Submitted By", "Store ID", "Store Name", "Channel", "Customer Code",
    "Customer Branch Code", "Customer Store Code", "Customer Store Name", "City", "Store Address",
    "Longitude", "Latitude", "Store Type", "Visibility Rating", "Location Rating", "ASM/KAM", "SPV", "SE/KAE",
    "Generate ID", "generated_at",   # dua kolom ini kosong saat submit, diisi setelah Generate ID + Save changes (update_generate_id_di_sheet)
    # Jejak perubahan terakhir (diisi hanya kalau status / Store ID BENAR-BENAR berubah dibanding data tersimpan di Supabase):
    "Remark_status",   # contoh: "Need Review > Generate ID"
    "Remark_ID",       # contoh: "null > IESM07800", "IESM07800 > null", "IESM07800 > IESM07801"
    "Remark_at",       # waktu (WIB) perubahan terakhir
]

def _noo_sheet_credentials():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    try:
        info = dict(st.secrets["noo_sheet"])
        info["private_key"] = info["private_key"].replace("\\n", "\n")
        return service_account.Credentials.from_service_account_info(info, scopes=scopes)
    except Exception:
        for f in NOO_SHEET_KEY_FILES:
            if os.path.exists(f):
                return service_account.Credentials.from_service_account_file(f, scopes=scopes)
        raise FileNotFoundError("Service account key not found: add [noo_sheet] to secrets.toml or put "
                                "noo-spreadsheet.json in the project folder")

@st.cache_resource(show_spinner=False)
def _noo_worksheet():
    import gspread
    return gspread.authorize(_noo_sheet_credentials()).open_by_key(NOO_SHEET_ID).worksheet(NOO_SHEET_TAB)

def kirim_noo_ke_sheet(rows: list, sumber: str = "", pengguna: str = "") -> tuple:
    if not rows:
        return True, ""
    try:
        ws = _noo_worksheet()
        if ws.row_values(1) != NOO_SHEET_KOLOM:            # header belum ada / beda -> tulis di baris 1
            ws.update(range_name="A1", values=[NOO_SHEET_KOLOM])
        waktu = datetime.datetime.now(ZONA_WAKTU).strftime("%Y-%m-%d %H:%M:%S")
        data = []
        for r in rows:
            r = {**r, "Submitted At": waktu, "Source": sumber, "Submitted By": pengguna}
            data.append(["" if r.get(k) is None else str(r.get(k)) for k in NOO_SHEET_KOLOM])
        ws.append_rows(data, value_input_option="USER_ENTERED", table_range="A1")
        return True, ""
    except Exception as e:
        _noo_worksheet.clear()
        return False, f"{type(e).__name__}: {e}"

def update_generate_id_di_sheet(items: list) -> tuple:
    if not items:
        return True, ""
    try:
        import gspread
        ws = _noo_worksheet()
        if ws.row_values(1) != NOO_SHEET_KOLOM:
            ws.update(range_name="A1", values=[NOO_SHEET_KOLOM])
        c_kode, c_nama = NOO_SHEET_KOLOM.index("Customer Store Code"), NOO_SHEET_KOLOM.index("Customer Store Name")
        c_id, c_waktu = NOO_SHEET_KOLOM.index("Generate ID") + 1, NOO_SHEET_KOLOM.index("generated_at") + 1
        semua = ws.get_all_values()
        norm = lambda v: str(v or "").strip().upper()
        posisi = {}
        for n, baris in enumerate(semua[1:], start=2):
            if len(baris) > max(c_kode, c_nama):
                posisi.setdefault((norm(baris[c_kode]), norm(baris[c_nama])), []).append(n)
        update, tambah = [], []
        for it in items:
            baris_sheet = posisi.get((norm(it["kode"]), norm(it["nama"])))
            if not baris_sheet:
                # baris belum ada di sheet (mis. NOO masuk sebelum integrasi sheet, atau barisnya terhapus):
                # TAMBAHKAN sebagai baris baru lengkap dengan Generate ID + generated_at (bukan error)
                data = {**it.get("data", {}), **it.get("nilai", {}), "Customer Store Code": it["kode"],
                        "Customer Store Name": it["nama"], "Generate ID": it["store_id"], "generated_at": it["generated_at"]}
                tambah.append(["" if data.get(k) is None else str(data.get(k)) for k in NOO_SHEET_KOLOM])
                continue
            for n in baris_sheet:
                update.append({"range": gspread.utils.rowcol_to_a1(n, c_id), "values": [[it["store_id"]]]})
                update.append({"range": gspread.utils.rowcol_to_a1(n, c_waktu), "values": [[it["generated_at"]]]})
                # kolom lain yang ikut diedit (Store Name, City, Store Address, Longitude, Latitude, Store Type, ...)
                for kol, nilai in it.get("nilai", {}).items():
                    if kol in NOO_SHEET_KOLOM:
                        update.append({"range": gspread.utils.rowcol_to_a1(n, NOO_SHEET_KOLOM.index(kol) + 1),
                                       "values": [["" if nilai is None else str(nilai)]]})
        if update:
            ws.batch_update(update, value_input_option="USER_ENTERED")
        if tambah:
            ws.append_rows(tambah, value_input_option="USER_ENTERED", table_range="A1")
        return True, ""
    except Exception as e:
        _noo_worksheet.clear()
        return False, f"{type(e).__name__}: {e}"

# ─────────────────────────────── KUNCI GENERATE ID (Streamlit <-> Apps Script) ─
# Tab "LOCK" di spreadsheet NOO: tiap sesi Streamlit yang punya ID BELUM tersimpan menulis 1 baris (owner, waktu UTC, user).
# Apps Script harus berhenti (return) selama ada baris yang umurnya < NOO_LOCK_MENIT menit. Baris dihapus setelah Save /
# Refresh; baris yang lebih tua dari batas dianggap basi (supaya script tidak macet selamanya kalau browser ditutup).
NOO_LOCK_TAB   = "LOCK"
NOO_LOCK_MENIT = 10
NOO_LOCK_HEADER = ["owner", "locked_at_utc", "user"]

def _pemilik_kunci() -> str:
    ss = st.session_state
    if "_pemilik_kunci" not in ss:
        ss["_pemilik_kunci"] = os.urandom(4).hex()
    return ss["_pemilik_kunci"]

@st.cache_resource(show_spinner=False)
def _noo_lock_worksheet():
    import gspread
    book = gspread.authorize(_noo_sheet_credentials()).open_by_key(NOO_SHEET_ID)
    try:
        return book.worksheet(NOO_LOCK_TAB)
    except gspread.WorksheetNotFound:
        ws = book.add_worksheet(NOO_LOCK_TAB, rows=50, cols=3)
        ws.update(range_name="A1", values=[NOO_LOCK_HEADER])
        return ws

def kunci_generate_pasang(pengguna: str = "") -> tuple:
    try:
        ws = _noo_lock_worksheet()
        owner = _pemilik_kunci()
        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        kol = ws.col_values(1)
        if kol[:1] != ["owner"]:
            ws.insert_row(NOO_LOCK_HEADER, 1)
            kol = ws.col_values(1)
        if owner in kol:
            n = kol.index(owner) + 1
            ws.update(range_name=f"B{n}:C{n}", values=[[ts, pengguna]])
        else:
            ws.append_row([owner, ts, pengguna], value_input_option="RAW")
        st.session_state["_kunci_aktif"] = True
        return True, ""
    except Exception as e:
        _noo_lock_worksheet.clear()
        return False, f"{type(e).__name__}: {e}"

def kunci_generate_lepas() -> tuple:
    if not st.session_state.get("_kunci_aktif"):
        return True, ""          # sesi ini tidak pernah memasang kunci -> tidak perlu menyentuh Google Sheet
    try:
        ws = _noo_lock_worksheet()
        kol = ws.col_values(1)
        owner = _pemilik_kunci()
        if owner in kol:
            ws.delete_rows(kol.index(owner) + 1)
        st.session_state["_kunci_aktif"] = False
        return True, ""
    except Exception as e:
        _noo_lock_worksheet.clear()
        return False, f"{type(e).__name__}: {e}"

# ─────────────────────────────── COUNTER PUSAT STORE ID (Supabase RPC) ────────
# Streamlit dan Apps Script sama-sama mengambil nomor lewat fungsi ambil_nomor di Supabase (atomik), jadi tidak bisa kembar.
# Kalau fungsinya belum dibuat / gagal dipanggil, generate_store_ids otomatis kembali ke hitungan lokal (max+1).
PAKAI_COUNTER_PUSAT = True

def sb_ambil_nomor(prefix: str, jumlah: int, floor: int):
    if not PAKAI_COUNTER_PUSAT or jumlah < 1:
        return None
    try:
        r = requests.post(f"{SUPABASE_URL}/rest/v1/rpc/ambil_nomor", headers={**_SB_HEADERS, "Prefer": ""},
                          json={"p_prefix": prefix, "p_jumlah": int(jumlah), "p_floor": int(floor)}, timeout=15)
        r.raise_for_status()
        d = r.json()
        d = d[0] if isinstance(d, list) else d
        return int(d["dari"]), int(d["sampai"])
    except Exception:
        return None

def sb_id_terpakai() -> dict:
    hasil, start, step = {}, 0, 1000
    try:
        while True:
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/{TABLE_SUBMIT_NOO}",
                headers={**_SB_HEADERS, "Prefer": "", "Range-Unit": "items", "Range": f"{start}-{start+step-1}"},
                params={"select": '"Store ID","Customer Store Code","Customer Store Name"'}, timeout=15)
            if r.status_code == 416:
                break
            r.raise_for_status()
            batch = r.json()
            for row in batch:
                sid = str(row.get("Store ID") or "").strip().upper()
                if sid:
                    hasil[sid] = (str(row.get("Customer Store Code") or "").strip(), str(row.get("Customer Store Name") or "").strip())
            if len(batch) < step:
                break
            start += step
        return hasil
    except Exception:
        return None

def hapus_noo_di_sheet(items: list) -> tuple:
    if not items:
        return True, ""
    try:
        ws = _noo_worksheet()
        c_kode, c_nama = NOO_SHEET_KOLOM.index("Customer Store Code"), NOO_SHEET_KOLOM.index("Customer Store Name")
        norm = lambda v: str(v or "").strip().upper()
        target = {(norm(it["kode"]), norm(it["nama"])) for it in items}
        baris_hapus = []
        for n, baris in enumerate(ws.get_all_values()[1:], start=2):      # baris 1 = header
            if len(baris) > max(c_kode, c_nama) and (norm(baris[c_kode]), norm(baris[c_nama])) in target:
                baris_hapus.append(n)
        if baris_hapus:
            # hapus dari baris paling bawah dulu supaya nomor baris di atasnya tidak bergeser
            ws.spreadsheet.batch_update({"requests": [
                {"deleteDimension": {"range": {"sheetId": ws.id, "dimension": "ROWS",
                                               "startIndex": n - 1, "endIndex": n}}}
                for n in sorted(baris_hapus, reverse=True)]})
        return True, ""
    except Exception as e:
        _noo_worksheet.clear()
        return False, f"{type(e).__name__}: {e}"

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

def ambil_baris_unik(df: pd.DataFrame, kolom_kunci: str, kolom_brand: str = None) -> pd.DataFrame:
    df = df.copy()
    df["_kunci_bersih"] = df[kolom_kunci].astype(str).str.strip().str.upper()
    df = df[df["_kunci_bersih"] != ""]
    if kolom_brand and kolom_brand in df.columns:
        # toko yang sama di brand berbeda (Customer Code 12 = G2G, 17 = NXP/BB) dianggap 2 baris berbeda
        df["_kunci_bersih"] = df["_kunci_bersih"] + "|" + df[kolom_brand].astype(str).str.strip().str[:2]
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
    baris_noo  = ambil_baris_unik(baris_noo, KOLOM_KUNCI_UNIK_NOO, "Customer Code")
    hasil = pd.DataFrame()
    for kolom_output, kolom_sumber in MAPPING_KOLOM_NOO.items():
        if kolom_output == "Store Address":
            # ambil kolom alamat dari file (mis. "Customer Address") kalau ada (nama kolom dicari longgar); kalau tidak ada -> kosong
            norm = lambda x: re.sub(r"[^a-z0-9]", "", str(x).lower())
            asli = next((c for c in baris_noo.columns if norm(c) in ("customeraddress", "storeaddress", "customerstoreaddress", "address", "alamat", "alamattoko")), None)
            hasil[kolom_output] = baris_noo[asli] if asli else ""
        elif kolom_sumber is None:
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