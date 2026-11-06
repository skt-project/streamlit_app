
import os
import datetime
import re
import time

import numpy as np
import pandas as pd
import streamlit as st
from rapidfuzz import fuzz, process
import komponen
import shared as _shared
from login_page import require_login, keluar
from help_support import dialog_help_support
from shared import (
    # Constants
    CITY, JENIS_TOKO_DEFAULT, KOLOM_NOO_OUTPUT, KOLOM_SKU_OUTPUT, KOLOM_SKU_STAGED,
    KOLOM_SKU_TAMPIL, KOLOM_SKU_OPSIONAL, SKU_KOLOM_DB, TABLE_SUBMIT_NOO,
    ZONA_WAKTU, PENGGUNA, STATUS_REVIEW, STATUS_VALID,
    # Supabase
    sb_insert, sb_select, sb_cari_duplikat_kode, sb_cari_duplikat_sku, sb_select_batch, ambil_raw_noo,
    sb_patch_by_kode_nama,
    # BigQuery
    bq_cari_duplikat_kode,
    # GSheets
    daftar_distributor, muat_distri_code_map, muat_distri_customer_code_raw,
    muat_distri_customer_name, muat_produk_lookup, muat_spv_sales_lookup,
    muat_deskripsi_produk,
    region_distributor, muat_distributor_dari_sheet,
    # Pivot / helpers
    pivot_sku_mapping, pivot_noo, noo_row_ke_supabase,
    hasil_ke_excel, catat_riwayat, waktu_sekarang,
    set_keterangan_noo, tampilkan_keterangan_noo,
    bangun_customer_code, cari_nama_produk, _kunci_produk, _sku_str, _sku_kunci,
    input_spv_sales, judul_opsi,
    ke_wib_naive,
    SHEET_PRODUCT,
)
from zoneinfo import ZoneInfo
import requests

# ═════════════════════════════════════════════════════════════════════════════
# NAVIGASI
# ═════════════════════════════════════════════════════════════════════════════
NAVIGASI = [
    ("NOO & SKU Submission",    ":material/description:", "pengajuan"),
    ("SPV Confirmation",  ":material/verified_user:", "spv"),
    ("History",           ":material/history:", "history"),
]

# peran yang boleh melihat semua distributor; akun lain (Admin Distributor, username = kode distributor)
# hanya melihat distributornya sendiri
PERAN_SEMUA_DISTRIBUTOR = ("RSA", "BD_SUPPORT", "BD SUPPORT")

def distributor_terkunci(user: dict):
    """None kalau user boleh melihat semua distributor. Selain itu nama distributor milik akun
    (username = kode distributor di sheet Distributor Mapping), atau "" kalau kodenya tidak ditemukan."""
    if (user or {}).get("tagging") in PERAN_SEMUA_DISTRIBUTOR:
        return None
    kode = str((user or {}).get("username", "")).strip().upper()
    try:
        info = _shared.muat_distri_code_map().get(kode)
    except Exception:
        info = None
    return info["distributor"] if info else ""

def _ingat_scroll_tabel():
    """Tabel lebar (scroll horizontal) kembali ke kolom 1 tiap halaman dirender ulang. Script ini mengingat posisi
    scroll tiap tabel dan mengembalikannya setelah rerun (dipasang sekali di window induk)."""
    st.components.v1.html("""<script>
(function(){
  var W = window.parent; if (!W || W.__skpScrollInit) return; W.__skpScrollInit = true;
  var D = W.document, simpan = {}, KUNCI = ['tabel_spv_scroll','tabel_noo_scroll','tabel_manual_scroll'];
  function pasang(){
    KUNCI.forEach(function(k){
      var el = D.querySelector('.st-key-' + k); if (!el) return;
      if (!el.__skpBound) {
        el.__skpBound = true;
        el.__skpUser = 0;
        ['wheel','pointerdown','touchstart','keydown'].forEach(function(ev){
          el.addEventListener(ev, function(){ el.__skpUser = Date.now(); }, {passive:true}); });
        // nilai 0 hanya dicatat kalau user yang menggeser (bukan reset otomatis saat rerun)
        el.addEventListener('scroll', function(){
          if (el.scrollLeft > 0 || Date.now() - el.__skpUser < 1500) simpan[k] = el.scrollLeft; }, {passive:true});
      }
      {
        var x = simpan[k] || 0;
        if (x > 0 && el.scrollLeft === 0 && !el.__skpRestoring) {
          el.__skpRestoring = true; var n = 0;
          (function ulang(){ el.scrollLeft = x; if (++n < 12 && Math.abs(el.scrollLeft - x) > 2) setTimeout(ulang, 60);
                             else el.__skpRestoring = false; })();
        }
      }
    });
  }
  new W.MutationObserver(function(){ W.requestAnimationFrame(pasang); }).observe(D.body, {childList:true, subtree:true});
  pasang();
})();
</script>""", height=0)


def tuju_halaman(nama):
    ss = st.session_state
    if ss.get("halaman_aktif") == "pengajuan" and nama != "pengajuan":
        for k in ["bulk_data","bulk_kode_noo","bulk_nama_noo","noo_keterangan",
                  "noo_batch_terkirim","unggah_pengajuan","sku_last_batch","sku_last_n","sku_msg"]:
            ss.pop(k, None)
    ss.halaman_aktif = nama

def inisialisasi_state():
    ss = st.session_state
    ss.setdefault("halaman_aktif", "pengajuan")
    ss.setdefault("riwayat", [])
    ss.setdefault("terakhir_update", waktu_sekarang())
    ss.setdefault("pesan_toast", None)
    ss.setdefault("pesan_toast_icon", ":material/check_circle:")
    # SPV
    ss.setdefault("spv_tampil_error", False)
    ss.setdefault("baris_spv", [])
    ss.setdefault("sim_tanda", None)
    ss.setdefault("sim_hasil", {})

# ═════════════════════════════════════════════════════════════════════════════
# SIMILARITY (untuk SPV Confirmation)
# ═════════════════════════════════════════════════════════════════════════════
# MIFTAH EDIT: KOLOM_MASTER lama (Customer Store Code, Store Name, Store Address, City, Region) DIGANTI.
#   Kolom kode sekarang "Cust ID" (dari cust_id BigQuery) + 3 kolom reference_id_* khusus similarity.
KOLOM_REF    = ["reference_id_skt", "reference_id_g2g", "reference_id_tph"]   # MIFTAH EDIT: baru
KOLOM_DIST   = ["distributor_skt", "distributor_g2g", "distributor_tph"]   # untuk lookup region distributor
KOLOM_MASTER = ["Cust ID", "Store Name", "Store Address", "City", "Region"] + KOLOM_REF + KOLOM_DIST   # MIFTAH EDIT: ganti
BOBOT_NAMA    = 0.4; BOBOT_ALAMAT = 0.4; BOBOT_KOTA = 0.1; BOBOT_REGION = 0.1
AMBANG_NAMA   = 80;  AMBANG_ALAMAT = 60; AMBANG_GABUNGAN = 75
AMBANG_NAMA_SAJA = 90; AMBANG_WILAYAH = 90
AMBANG_NAMA_MIN  = 70   # kandidat minimal: skor nama >= ini, lalu diurutkan by skor gabungan tertinggi
MIN_HURUF_ALAMAT = 4    # alamat NOO yang lebih pendek (mis. "A") dianggap tidak ada, supaya tidak menipu skor
KATA_UMUM_NAMA   = {"TOKO","KIOS","KIOSK","UD","CV","PT","WARUNG","TK","SWALAYAN","MINIMARKET","SUPERMARKET",
                    "TOSERBA","GROSIR","AGEN","DEPO","DEPOT"}   # kata jenis usaha: tidak membedakan toko
SINGKATAN_ALAMAT = {"JL":"JALAN","JLN":"JALAN","GG":"GANG"}
KATA_BUANG_ALAMAT  = {"NO","NOMOR"}
KATA_BUANG_WILAYAH = {"KOTA","KAB","KABUPATEN"}

def _bersihkan(teks) -> str:
    s = re.sub(r"[\"'`´‘’“”]", "", str(teks or "").upper())      # tanda kutip dibuang dulu (D'LAUT -> DLAUT)
    s = re.sub(r"[^A-Z0-9 ]"," ", s)                             # simbol lain jadi spasi (TOKO-ABC -> TOKO ABC)
    return re.sub(r"\s+", " ", s).strip()

def normalisasi_nama(teks) -> str:
    bersih = _bersihkan(teks)
    token  = [t for t in bersih.split() if t not in KATA_UMUM_NAMA]
    return " ".join(token) if token else bersih

def normalisasi_alamat(teks, kota="") -> str:
    s = re.sub(r"\b(RT|RW)\.?\s*\d+\b"," ", str(teks or "").upper())
    kata_kota = set(_bersihkan(kota).split())
    token = []
    for t in _bersihkan(s).split():
        if t in KATA_BUANG_ALAMAT or t in kata_kota:
            continue
        token.append(SINGKATAN_ALAMAT.get(t, t))
    return " ".join(token)

def normalisasi_wilayah(teks) -> str:
    return " ".join(t for t in _bersihkan(teks).split() if t not in KATA_BUANG_WILAYAH)

# MIFTAH EDIT (DIHAPUS): REGION_DUMMY, master dummy (6 toko tetap + 300 acak + kode CSC-xxxxx), PAKAI_MASTER_DUMMY.
# MIFTAH EDIT (BARU): master toko sekarang diambil dari BigQuery (gt_schema.master_store_database_basis).
@st.cache_data(ttl=600, show_spinner=False)
def ambil_master_toko() -> pd.DataFrame:
    client = _shared.get_bq_client()
    query = f"""
        SELECT
          CAST(cust_id AS STRING)          AS `Cust ID`,
          CAST(store_name AS STRING)       AS `Store Name`,
          CAST(address AS STRING)          AS `Store Address`,
          CAST(city AS STRING)             AS `City`,
          CAST(region AS STRING)           AS `Region`,
          CAST(reference_id_skt AS STRING) AS `reference_id_skt`,
          CAST(reference_id_g2g AS STRING) AS `reference_id_g2g`,
          CAST(reference_id_tph AS STRING) AS `reference_id_tph`,
          CAST(distributor_skt AS STRING)  AS `distributor_skt`,
          CAST(distributor_g2g AS STRING)  AS `distributor_g2g`,
          CAST(distributor_tph AS STRING)  AS `distributor_tph`
        FROM `{_shared.GCP_PROJECT_ID}.{_shared.BQ_MASTER_STORE}`
        WHERE cust_id IS NOT NULL AND TRIM(CAST(cust_id AS STRING)) != ''
    """
    rows = [dict(r) for r in client.query(query).result()]
    df = pd.DataFrame(rows, columns=KOLOM_MASTER).fillna("")
    return df.astype(str)

def _normalkan_master(m: pd.DataFrame) -> pd.DataFrame:
    m = m.copy()
    m["_nama"]   = m["Store Name"].map(normalisasi_nama)
    m["_alamat"] = [normalisasi_alamat(a,k) for a,k in zip(m["Store Address"], m["City"])]
    m["_kota"]   = m["City"].map(normalisasi_wilayah)
    m["_region"] = m["Region"].map(normalisasi_wilayah)
    m["_dist"]   = [{re.sub(r"[^A-Z0-9]", "", str(v).upper()) for v in vs} - {"", "-"}
                    for vs in zip(*(m[k] for k in KOLOM_DIST))]   # distributor skt/g2g/tph baris master
    return m

# MIFTAH EDIT (BARU): pencocokan kode pasti. Customer Store Code NOO dicek ke reference_id_skt/g2g/tph di master,
#   kalau ketemu -> tampilkan cust_id + store_name baris master itu.
def _kunci_kode(v) -> str:
    return str(v or "").strip().upper()

@st.cache_data(ttl=600, show_spinner=False)
def _indeks_kode_master() -> dict:
    """kode referensi (skt/g2g/tph) -> (cust_id, store_name, address, region)"""
    m = ambil_master_toko()
    indeks = {}
    for kol in KOLOM_REF:
        for cid, nama, alamat, region, ref in zip(m["Cust ID"], m["Store Name"], m["Store Address"], m["Region"], m[kol]):
            k = _kunci_kode(ref)
            if k and k not in indeks:
                indeks[k] = (str(cid), str(nama), str(alamat), str(region))
    return indeks

@st.cache_data(ttl=600, show_spinner=False)
def _master_ternormalisasi() -> pd.DataFrame:
    return _normalkan_master(ambil_master_toko())

MAKS_GABUNG_TOKEN = 3   # maksimal berapa kata bersebelahan yang boleh digabung saat mencocokkan

def _skor_token_nama(a: str, b: str) -> float:
    """Cocokkan nama TOKEN per TOKEN tanpa peduli urutan. Kalau satu kata tidak ada pasangannya, kata itu
    boleh DIGABUNG dengan kata di sebelahnya (maks. 3 kata) lalu dicocokkan lagi, mis. "BAU BAU" ~ "BAUBAU",
    "SEKA WAN" ~ "SEKAWAN". Pasangan (kata atau gabungan) paling mirip dipilih lebih dulu.
    - bagian yang memuat angka harus sama persis ("12" != "1112")
    - kemiripan huruf di bawah 60 dianggap beda
    - skor = total kemiripan pasangan / jumlah kata sisi terpendek, lalu kelebihan kata di salah satu
      sisi hanya menurunkan sedikit (mis. nama master + nama kota)."""
    ta, tb = a.split(), b.split()
    if not ta or not tb:
        return 0.0

    def rentang(t):
        for i in range(len(t)):
            for k in range(1, MAKS_GABUNG_TOKEN + 1):
                if i + k <= len(t):
                    yield i, k, "".join(t[i:i + k])

    pasangan = []
    for i, ka, x in rentang(ta):
        for j, kb, y in rentang(tb):
            if x == y:
                sk = 100.0
            elif any(c.isdigit() for c in x + y):
                sk = 0.0
            else:
                sk = float(fuzz.ratio(x, y))
                if sk < 60:
                    sk = 0.0
            if sk:
                pasangan.append((sk, -(ka + kb), i, ka, j, kb))   # skor sama -> utamakan kata tunggal, bukan gabungan
    pasangan.sort(reverse=True)
    pakai_a, pakai_b, total, n_pasang = set(), set(), 0.0, 0
    for sk, _, i, ka, j, kb in pasangan:
        ra, rb = set(range(i, i + ka)), set(range(j, j + kb))
        if ra & pakai_a or rb & pakai_b:
            continue
        pakai_a |= ra; pakai_b |= rb
        total += sk; n_pasang += 1
    # satu gabungan dihitung satu pasangan, jadi menggabungkan kata tidak menghukum skor
    la = n_pasang + (len(ta) - len(pakai_a))
    lb = n_pasang + (len(tb) - len(pakai_b))
    dasar = total / min(la, lb)
    return dasar * (min(la, lb) / max(la, lb)) ** 0.25

def _hitung_skor(nama, alamat, kota, master, region_noo=()):
    n = len(master)
    nama_n   = normalisasi_nama(nama)
    alamat_n = normalisasi_alamat(alamat, kota)
    kota_n   = normalisasi_wilayah(kota)
    region_n = [r for r in (normalisasi_wilayah(x) for x in region_noo) if r]
    skor_nama   = process.cdist([nama_n], master["_nama"].tolist(), scorer=fuzz.token_set_ratio, workers=1)[0]
    # pass 1 (token_set_ratio) hanya menyaring kandidat; skor akhir dari pass 2 (token per token, angka harus sama)
    top = np.argsort(-skor_nama)[:100]
    nama_master = master["_nama"].to_numpy()
    halus = np.zeros(n)
    for j in top:
        if skor_nama[j] >= AMBANG_NAMA_MIN:
            halus[j] = _skor_token_nama(nama_n, nama_master[j])
    skor_nama = halus
    ada_alamat  = (len(alamat_n) >= MIN_HURUF_ALAMAT) & master["_alamat"].ne("").to_numpy()
    skor_alamat = (process.cdist([alamat_n], master["_alamat"].tolist(), scorer=fuzz.token_set_ratio, workers=1)[0]
                   if ada_alamat.any() else np.zeros(n))
    ada_kota    = bool(kota_n) & master["_kota"].ne("").to_numpy()
    skor_kota   = (process.cdist([kota_n], master["_kota"].tolist(), scorer=fuzz.ratio,
                                  score_cutoff=AMBANG_WILAYAH, workers=1)[0]
                   if ada_kota.any() else np.zeros(n))
    ada_region  = bool(region_n) & master["_region"].ne("").to_numpy()
    skor_region = (np.max([process.cdist([r], master["_region"].tolist(), scorer=fuzz.ratio,
                                          score_cutoff=AMBANG_WILAYAH, workers=1)[0] for r in region_n], axis=0)
                   if ada_region.any() else np.zeros(n))
    bobot   = BOBOT_NAMA + BOBOT_ALAMAT*ada_alamat + BOBOT_KOTA*ada_kota + BOBOT_REGION*ada_region
    jumlah  = (BOBOT_NAMA*skor_nama + BOBOT_ALAMAT*skor_alamat*ada_alamat
               + BOBOT_KOTA*skor_kota*ada_kota + BOBOT_REGION*skor_region*ada_region)
    gabungan = jumlah / bobot
    lolos = np.where(
        ada_alamat,
        (skor_nama>=AMBANG_NAMA) & (skor_alamat>=AMBANG_ALAMAT) & (gabungan>=AMBANG_GABUNGAN),
        (skor_nama>=AMBANG_NAMA_SAJA) & (gabungan>=AMBANG_GABUNGAN),
    )
    return {"nama":skor_nama,"alamat":skor_alamat,"kota":skor_kota,"region":skor_region,
            "ada_alamat":ada_alamat,"ada_kota":ada_kota,"ada_region":ada_region,
            "gabungan":gabungan,"lolos":lolos}

def cari_kemiripan(nama, alamat, kota, master, region_noo=(), nama_min=AMBANG_NAMA_MIN):
    if master is None or master.empty or not normalisasi_nama(nama):
        return None
    h = _hitung_skor(nama, alamat, kota, master, region_noo)
    kandidat = h["nama"] >= nama_min
    if not kandidat.any():
        return None
    # tampilkan master dengan skor gabungan TERTINGGI (tidak lagi harus lolos semua ambang)
    terbaik = int(np.argmax(np.where(kandidat, h["gabungan"], -1.0)))
    baris   = master.iloc[terbaik]
    # MIFTAH EDIT: sebelumnya return baris["Customer Store Code"] (kode dummy), sekarang cust_id dari master asli.
    # alamat master (kolom `address`) + skor kemiripan alamat, untuk kolom Similarity (Address)
    alamat = str(baris["Store Address"])
    if alamat.strip() and h["ada_alamat"][terbaik]:
        alamat = f"{alamat} ({h['alamat'][terbaik]:.0f}%)"
    region = str(baris["Region"])
    if region.strip() and h["ada_region"][terbaik]:
        region = f"{region} ({h['region'][terbaik]:.0f}%)"
    return str(baris["Cust ID"]), f'{baris["Store Name"]} ({h["gabungan"][terbaik]:.0f}%)', alamat, region

def region_dari_master(distributor: str, min_porsi: float = 0.05) -> tuple:
    """Region distributor dari kolom `region` master store, dicari lewat kolom distributor_skt/g2g/tph
    yang sama dengan distributor terpilih. Region yang porsinya < min_porsi dibuang (anggap salah input).
    Kosong kalau tidak ketemu -> pemanggil pakai region dari sheet Distributor Mapping."""
    kunci = re.sub(r"[^A-Z0-9]", "", str(distributor or "").upper())
    if not kunci:
        return ()
    try:
        m = ambil_master_toko()
    except Exception:
        return ()
    cocok = None
    for kol in KOLOM_DIST:
        mk = m[kol].str.upper().str.replace(r"[^A-Z0-9]", "", regex=True) == kunci
        cocok = mk if cocok is None else (cocok | mk)
    hitung = m.loc[cocok, "Region"].str.strip()
    hitung = hitung[hitung != ""].value_counts()
    if hitung.empty:
        return ()
    return tuple(sorted(r for r, n in hitung.items() if n / hitung.sum() >= min_porsi))

def _batasi_region(master, region_noo, distributor=""):
    """Persempit master ke region distributor terpilih. Region kosong -> semua region.
    Region cocok kalau sama persis / mirip (fuzz.ratio >= AMBANG_WILAYAH) setelah dinormalisasi."""
    region_n = {r for r in (normalisasi_wilayah(x) for x in region_noo) if r}
    kunci_d  = re.sub(r"[^A-Z0-9]", "", str(distributor or "").upper())
    if master is None or not region_n:
        return master
    # sheet Distributor Mapping menulis region bernomor ("Sulawesi 1"), master BigQuery bisa tanpa nomor ("Sulawesi")
    dasar = lambda t: re.sub(r"\s*\d+$", "", t).strip()
    cocok = {mr for mr in master["_region"].unique()
             if mr and any(mr == r or dasar(mr) == dasar(r) or fuzz.ratio(mr, r) >= AMBANG_WILAYAH for r in region_n)}
    # toko master yang distributornya SAMA dengan distributor baris ini ikut jadi kandidat walau region-nya
    # tertulis beda di master (mis. "Southern Sulawesi" vs "Sulawesi 1")
    milik = master["_dist"].map(lambda d: kunci_d in d) if kunci_d else False
    return master[master["_region"].isin(cocok) | milik]

def terapkan_similarity(baris_spv, region_noo=()):
    try:
        master = _master_ternormalisasi()
        indeks = _indeks_kode_master()
    except Exception as e:
        st.warning(f"Gagal memuat master toko: {e}")
        master, indeks = None, {}
    master_per_region = {}                            # cache master yang sudah dipersempit, per region
    for b in baris_spv:
        rg = tuple(b.get("region_noo") or region_noo)  # region distributor BARIS ini
        kk = (rg, b["distributor"])
        if master is not None and kk not in master_per_region:
            master_per_region[kk] = _batasi_region(master, rg, b["distributor"])
        master_b = master_per_region.get(kk)
        hit = indeks.get(_kunci_kode(b["store_code"]))            # 1) cocok kode
        if not hit and master_b is not None:                       # 2) fallback fuzzy (hanya di region distributor baris ini)
            hit = cari_kemiripan(b["nama"], b["alamat"], b.get("kota",""), master_b, rg)
        b["similarity_kode"], b["similarity_nama"], b["similarity_alamat"], b["similarity_region"] =             hit if hit else ("","","","")

def isi_similarity_spv(baris, region_noo):
    ss    = st.session_state
    tanda = (tuple((b["id"],b["store_code"],b["nama"],b["alamat"],b["kota"],tuple(b.get("region_noo") or ())) for b in baris), tuple(region_noo))
    if ss.get("sim_tanda") != tanda:
        terapkan_similarity(baris, region_noo)
        ss.sim_tanda = tanda
        ss.sim_hasil = {b["id"]:(b["similarity_kode"],b["similarity_nama"],b["similarity_alamat"],b["similarity_region"]) for b in baris}
    else:
        for b in baris:
            hasil = tuple(ss.sim_hasil.get(b["id"],())) + ("","","","")   # toleran thd cache sesi lama
            b["similarity_kode"], b["similarity_nama"], b["similarity_alamat"], b["similarity_region"] = hasil[:4]

def _teks(v) -> str:
    return "" if pd.isna(v) else str(v).strip()

def bangun_baris_spv(distributor: str = "") -> list:
    rows = ambil_raw_noo()
    if not rows:
        return []
    norm  = lambda s: re.sub(r"[^a-z0-9]","",str(s).lower())
    hasil, kemunculan = [], {}
    for i, row in enumerate(rows, 1):
        peta = {norm(k):v for k,v in row.items()}
        # id STABIL (bukan nomor urut): pilihan NOO/Existing & Store ID yang sudah diisi tetap menempel ke
        # baris yang sama walau data dimuat ulang (cache 15 dtk) dan ada baris baru yang masuk
        dasar_id = re.sub(r"[^A-Za-z0-9]", "", f"{peta.get('customerstorecode','')}_{peta.get('customerstorename','')}")[:60] or "baris"
        kemunculan[dasar_id] = kemunculan.get(dasar_id, 0) + 1
        id_stabil = dasar_id if kemunculan[dasar_id] == 1 else f"{dasar_id}_{kemunculan[dasar_id]}"
        hasil.append({
            "id":               id_stabil,
            "no":               i,
            "store_code":       peta.get("customerstorecode","") or "",
            # distributor = Group Name (Customer Branch Name); baris lama: Customer Store Name berisi distributor
            "distributor":      peta.get("groupname","") or peta.get("customerstorename","") or "",
            "csn":              peta.get("customerstorename","") or "",   # Customer Store Name asli (kunci update)
            "nama":             peta.get("storename","") or "",
            "alamat":           peta.get("storeaddress","") or "",
            "kota":             peta.get("city","") or "",
            "jenis_outlet":     None,
            "store_id_validasi":"",
            "keterangan":       "",
            "similarity_kode":  "",
            "similarity_nama":  "",
            "similarity_alamat":"",
            "similarity_region":"",
            "tagging":          peta.get("tagging","") or "",   # ← tambah ini
            # penanda "sudah dikonfirmasi SPV" (kolom opsional spv_confirmed di INPUT_NOO)
            "ada_kolom_spv":    "spvconfirmed" in peta,
            "spv_confirmed":    str(peta.get("spvconfirmed","")).strip().lower() in ("true","t","1","yes"),
            # Brand dari 2 digit awal Customer Code yang tercatat: 12 = G2G, 17 = NXP/BB
            "brand":            {"12": "G2G", "17": "NXP/BB"}.get(str(peta.get("customercode","") or "").strip()[:2], ""),
        })
    return hasil

# ═════════════════════════════════════════════════════════════════════════════
# MANUAL SKU MAPPING
# ═════════════════════════════════════════════════════════════════════════════

def _sku_peringatan(teks: str):
    ss = st.session_state
    ss.sku_msg        = ("warning", teks)
    ss.pesan_toast    = teks
    ss.pesan_toast_icon = ":material/warning:"

def _sku_tutup_hasil():
    st.session_state.sku_last_batch = None

def _sku_tambah_baris(map_raw: dict, map_produk: dict):
    ss = st.session_state
    ss.sku_last_batch = None
    distri  = _sku_str(ss.get("sku_in_distri",""))
    produk  = _sku_str(ss.get("sku_in_produk",""))
    kode_sku = _sku_str(ss.get("sku_in_kode",""))
    if not (distri and produk and kode_sku):
        ss.sku_msg = ("warning","Distributor, Product, and Customer Product Code are required.")
        return
    cc = bangun_customer_code(map_raw.get(distri,""), produk)
    if not cc:
        ss.sku_msg = ("error",f"Customer Code (column G) is empty for distributor: {distri}")
        return
    kunci_baru = _sku_kunci(cc, produk)
    if any(_sku_kunci(r["Customer Code"],r["Product"]) == kunci_baru for r in ss.sku_staged):
        _sku_peringatan(f"Duplicate: {cc} / {produk} is already in the staged rows.")
        return
    dup_db, _ = sb_cari_duplikat_sku("sku_mapping", [(cc, produk)])
    if kunci_baru in dup_db:
        _sku_peringatan(f"Duplicate: {cc} / {produk} already exists in sku_mapping.")
        return
    nama_cust = muat_distri_customer_name().get(distri,"")
    if not nama_cust:
        ss.sku_msg = ("error",f"Customer Name is empty for distributor: {distri}")
        return
    nama_produk = cari_nama_produk(produk, map_produk)
    if not nama_produk:
        contoh = ", ".join(list(map_produk)[:3])
        ss.sku_msg = ("error",f"Product '{produk}' not found in {SHEET_PRODUCT}. "
                              f"{len(map_produk)} products loaded (e.g. {contoh}).")
        return
    ss.sku_staged.append({
        "Pilih":False,"Distributor":distri,"Customer Code":cc,"Customer Name":nama_cust,
        "Product":produk,"Customer Product Code":kode_sku,"Customer Product Name":nama_produk,
        **{k:"" for k in KOLOM_SKU_OPSIONAL},
    })
    ss.sku_editor_key += 1
    ss.sku_in_kode = ""
    ss.sku_msg     = None

def render_manual_sku():
    ss = st.session_state
    ss.setdefault("sku_staged",[])
    ss.setdefault("sku_editor_key",0)
    ss.setdefault("sku_last_batch",None)
    ss.setdefault("sku_last_n",0)
    try:
        map_raw = muat_distri_customer_code_raw()
    except Exception as e:
        st.error(f"Failed to read Distributor Mapping sheet: {e}"); return
    if not map_raw:
        st.warning("Distributor Mapping sheet is empty or column G is missing."); return
    map_nama = muat_distri_customer_name()
    try:
        map_produk = muat_produk_lookup(SHEET_PRODUCT)
    except Exception as e:
        st.error(f"Failed to read {SHEET_PRODUCT}: {e}"); return
    if not map_produk:
        st.warning(f"{SHEET_PRODUCT} is empty."); return

    with st.expander(":material/edit_note: Manual Input, 1 by 1 row",
                     expanded=bool(ss.sku_staged) or ss.sku_last_batch is not None):
        if ss.sku_last_batch is not None:
            st.success(f"{ss.sku_last_n} rows saved to sku_mapping")
            if ss.sku_last_batch:
                df_batch = pd.DataFrame(ss.sku_last_batch)
                if "created_at" in df_batch.columns:
                    df_batch["created_at"] = ke_wib_naive(df_batch["created_at"]).dt.strftime("%d %b %Y %H:%M:%S")
                kolom = [c for c in list(SKU_KOLOM_DB.values())+["created_at"] if c in df_batch.columns]
                st.dataframe(df_batch[kolom], hide_index=True, width="stretch")
            st.button("Dismiss", key="sku_tutup", on_click=_sku_tutup_hasil)
            st.markdown("---")

        produk_now = _sku_str(ss.get("sku_in_produk",""))
        c1,c2,c3  = st.columns([3,2.2,1.2])
        distri    = c1.selectbox("Distributor",[""] + sorted(map_raw), key="sku_in_distri")
        c2.text_input("Customer Name", value=map_nama.get(distri,"") if distri else "", disabled=True)
        preview_cc = bangun_customer_code(map_raw.get(distri,""), produk_now) if distri else ""
        c3.text_input("Customer Code", value=preview_cc, disabled=True)

        c4,c5,c6,c7 = st.columns([2.0,1.4,2.2,0.9])
        produk = c4.selectbox("Product",[""] + sorted(map_produk), key="sku_in_produk")
        c5.text_input("Customer Product Code", key="sku_in_kode", placeholder="Product code...")
        c6.text_input("Customer Product Name", value=cari_nama_produk(produk,map_produk), disabled=True)
        with c7:
            st.markdown('<div style="height:1.7rem"></div>', unsafe_allow_html=True)
            st.button(":material/add: Add Row", key="sku_tambah", width="stretch",
                      on_click=_sku_tambah_baris, args=(map_raw,map_produk))

        pesan = ss.pop("sku_msg",None)
        if pesan:
            {"warning":st.warning,"error":st.error}.get(pesan[0],st.info)(pesan[1])

        if not ss.sku_staged:
            st.info("No staged rows yet."); return

        st.caption(f"Step 2 — Review staged rows ({len(ss.sku_staged)}). Edit or tick to delete.")
        df_base = pd.DataFrame(ss.sku_staged, columns=KOLOM_SKU_STAGED)
        kolom_teks = [c for c in KOLOM_SKU_STAGED if c != "Pilih"]
        df_base[kolom_teks] = df_base[kolom_teks].fillna("").astype(str)
        df_base["Pilih"]    = df_base["Pilih"].fillna(False).astype(bool)
        hasil = st.data_editor(
            df_base, key=f"sku_editor_{ss.sku_editor_key}",
            column_order=KOLOM_SKU_TAMPIL,
            column_config={
                "Pilih":                      st.column_config.CheckboxColumn("Select",width=50,default=False),
                "Distributor":                st.column_config.SelectboxColumn("Distributor",options=sorted(map_raw),width=190),
                "Customer Code":              st.column_config.TextColumn("Customer Code",width=130,disabled=True),
                "Customer Name":              st.column_config.TextColumn("Customer Name",width=180,disabled=True),
                "Product":                    st.column_config.SelectboxColumn("Product",options=sorted(map_produk),width=200),
                "Customer Product Code":      st.column_config.TextColumn("Customer Product Code",width=200),
                "Customer Product Name":      st.column_config.TextColumn("Customer Product Name",width=240,disabled=True),
                "Product Chinese Short Name": st.column_config.TextColumn("Product Chinese Short Name",width=190),
                "Product English Name":       st.column_config.TextColumn("Product English Name",width=190),
                "Specification":              st.column_config.TextColumn("Specification",width=150),
                "Barcode":                    st.column_config.TextColumn("Barcode",width=150),
                "Description":                st.column_config.TextColumn("Description",width=200),
            },
            disabled=["Customer Code","Customer Name","Customer Product Name"],
            hide_index=True, num_rows="dynamic", width="stretch",
        )

        rows_now = []
        for _, r in hasil.iterrows():
            d = _sku_str(r["Distributor"])
            p = _sku_str(r["Product"])
            rows_now.append({
                "Pilih":                bool(r["Pilih"]) if pd.notna(r["Pilih"]) else False,
                "Distributor":          d,
                "Customer Code":        bangun_customer_code(map_raw.get(d,""),p),
                "Customer Name":        map_nama.get(d,""),
                "Product":              p,
                "Customer Product Code": _sku_str(r["Customer Product Code"]),
                "Customer Product Name": cari_nama_produk(p, map_produk),
                **{k: _sku_str(r[k]) for k in KOLOM_SKU_OPSIONAL},
            })
        if rows_now != ss.sku_staged:
            ss.sku_staged     = rows_now
            ss.sku_editor_key += 1
            st.rerun()

        c_hapus, c_kosongkan, _ = st.columns([1.6,1.6,5])
        if c_hapus.button(":material/delete: Delete Selected", key="sku_hapus", width="stretch"):
            if any(r["Pilih"] for r in rows_now):
                ss.sku_staged = [r for r in rows_now if not r["Pilih"]]
                ss.sku_editor_key += 1
                st.rerun()
            else:
                st.warning("No rows selected.")
        if c_kosongkan.button("Clear All", key="sku_kosongkan", width="stretch"):
            ss.sku_staged = []
            ss.sku_editor_key += 1
            st.rerun()

        masalah = []
        for n, r in enumerate(rows_now,1):
            kurang = [k for k in ("Distributor","Customer Code","Customer Name","Product",
                                  "Customer Product Code","Customer Product Name") if not r[k]]
            if kurang:
                masalah.append(f"Row {n}: missing {', '.join(kurang)}")
        posisi = {}
        for n, r in enumerate(rows_now,1):
            if r["Customer Code"] and r["Product"]:
                posisi.setdefault(_sku_kunci(r["Customer Code"],r["Product"]),[]).append(n)
        for (cc_k,pr_k), nomor in posisi.items():
            if len(nomor) > 1:
                masalah.append(f"Duplicate {cc_k}/{pr_k} in rows {', '.join(map(str,nomor))}")
        if masalah:
            st.warning("Fix before saving: " + " | ".join(masalah))

        if st.button(":material/send: Save All to Supabase", type="primary", width="stretch",
                     key="sku_submit", disabled=bool(masalah)):
            with st.spinner("Checking for duplicates..."):
                dup_db, gagal_cek = sb_cari_duplikat_sku(
                    "sku_mapping", [(r["Customer Code"],r["Product"]) for r in rows_now])
            if gagal_cek:
                ss.sku_msg = ("error","Save cancelled — could not check duplicates for: "
                                      + ", ".join(sorted(gagal_cek)))
                st.rerun()
            baris_dup = [r for r in rows_now if _sku_kunci(r["Customer Code"],r["Product"]) in dup_db]
            if baris_dup:
                daftar_dup = ", ".join(f"{r['Customer Code']}/{r['Product']}" for r in baris_dup)
                ss.sku_msg = ("error",f"Save cancelled — already in sku_mapping: {daftar_dup}")
                ss.pesan_toast = "Duplicate found. Nothing saved."; ss.pesan_toast_icon = ":material/warning:"
                st.rerun()
            ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
            payload = [{**{SKU_KOLOM_DB[k]:(r[k] or None) for k in SKU_KOLOM_DB},"created_at":ts}
                       for r in rows_now]
            with st.spinner("Saving to Supabase..."):
                result = sb_insert("sku_mapping", payload)
            if isinstance(result,dict) and "_error" in result:
                st.error(f"Failed to save: {result['_error']}")
            else:
                batch = sb_select_batch("sku_mapping", ts)
                if not batch and isinstance(result,list):
                    batch = result
                ss.sku_last_batch = batch
                ss.sku_last_n     = len(payload)
                ss.sku_staged     = []
                ss.sku_editor_key += 1
                catat_riwayat("Submit Manual SKU Mapping", f"{len(payload)} rows")
                ss.pesan_toast = f"Manual SKU Mapping saved — {len(payload)} rows"
                st.rerun()
def _set_produk_dari_deskripsi(produk_key: str, desc_key: str, desc_map: dict):
    deskripsi = st.session_state.get(desc_key, "")
    if deskripsi and deskripsi in desc_map:
        st.session_state[produk_key] = desc_map[deskripsi]
    elif not deskripsi:
        # deskripsi dikosongkan -> buang pilihan Product manual; dropdown Product kembali ke kode semula
        # (hasil auto-match dari file) saat dirender ulang
        st.session_state.pop(produk_key, None)

def _ambil_kode_depan(teks: str) -> str:
    """Ambil kata pertama sebelum spasi, misal 'G2G-32601' dari
    'G2G-32601 GLAD2GLOW PERFECT BLURRING POWDER ...'."""
    t = _sku_str(teks)
    return t.split()[0] if t else ""

def _bulk_sku_cocokkan(teks: str, map_produk: dict) -> tuple:
    """Return (produk_di_dropdown | None, status)."""
    k = _kunci_produk(teks)
    if k in map_produk:
        return k, "Exact"
    hit = process.extractOne(k, list(map_produk), scorer=fuzz.WRatio, score_cutoff=60)
    if hit:
        return hit[0], "Suggested"
    return None, "Not found"

def render_bulk_sku():
    ss = st.session_state
    ss.setdefault("sku_bulk_rows", [])
    ss.setdefault("sku_bulk_ver", 0)
    ss.setdefault("sku_staged", [])
    ss.setdefault("sku_editor_key", 0)
    try:
        map_raw    = muat_distri_customer_code_raw()
        map_nama   = muat_distri_customer_name()
        map_produk = muat_produk_lookup(SHEET_PRODUCT)
    except Exception as e:
        st.error(f"Failed to load mapping for Bulk SKU Paste: {e}"); return
    if not map_raw or not map_produk:
        return
    opsi_produk = sorted(map_produk)

    with st.expander(":material/content_paste: Paste All SKU", expanded=bool(ss.sku_bulk_rows)):
        st.caption("Step 1 — Select distributor, paste Product and Customer Product Code (one per line)")
        distri = st.selectbox("Distributor", [""] + sorted(map_raw), key="sku_bulk_distri")
        c_p, c_k = st.columns(2)
        t_produk = c_p.text_area("Product", height=120, key="sku_bulk_produk",
                                 placeholder="G2G-XXX\nNXP-YYY\n...")
        t_kode   = c_k.text_area("Customer Product Code", height=120, key="sku_bulk_kode",
                                 placeholder="SKU001\nSKU002\n...")

        if st.button("Apply Paste", key="sku_bulk_apply"):
            l_produk = [x.strip() for x in t_produk.splitlines() if x.strip()]
            l_kode   = [x.strip() for x in t_kode.splitlines() if x.strip()]
            if not l_produk:
                st.warning("Product is required.")
            elif len(l_produk) != len(l_kode):
                st.error(f"Product Code Salah: {len(l_produk)} products dari {len(l_kode)} kode.")
            else:
                rows = []
                for p, kd in zip(l_produk, l_kode):
                    hasil_p, status = _bulk_sku_cocokkan(p, map_produk)
                    rows.append({"Pasted": p, "Match": status,
                                 "Product": hasil_p, "Customer Product Code": kd})
                ss.sku_bulk_rows = rows
                ss.sku_bulk_ver += 1
                st.rerun()

        if not ss.sku_bulk_rows:
            return

        st.caption("Step 2 — Fix any wrong Product using the dropdown, then add to staged rows")
        hasil = st.data_editor(
            pd.DataFrame(ss.sku_bulk_rows), key=f"sku_bulk_editor_{ss.sku_bulk_ver}",
            column_order=["Pasted", "Match", "Product", "Customer Product Code"],
            column_config={
                "Pasted":  st.column_config.TextColumn("Pasted Product", width=200),
                "Match":   st.column_config.TextColumn("Match", width=100),
                "Product": st.column_config.SelectboxColumn("Product (fix here)", options=opsi_produk, width=240),
                "Customer Product Code": st.column_config.TextColumn("Customer Product Code", width=200),
            },
            disabled=["Pasted", "Match"], hide_index=True, num_rows="fixed", width="stretch",
        )

        belum = [n for n, (p, kd) in enumerate(zip(hasil["Product"], hasil["Customer Product Code"]), 1)
                 if _sku_str(p) not in map_produk or not _sku_str(kd)]
        if belum:
            st.warning("Fix Product / Customer Product Code on row: " + ", ".join(map(str, belum)))
        elif (hasil["Match"] == "Suggested").any():
            st.info("Rows marked Suggested were auto-matched. Please double-check them.")

        c_add, c_clear, _ = st.columns([1.8, 1.2, 5])
        tambah = c_add.button(":material/add: Add to Staged", type="primary", key="sku_bulk_add",
                              width="stretch", disabled=bool(belum) or not distri)
        if c_clear.button("Clear", key="sku_bulk_clear", width="stretch"):
            ss.sku_bulk_rows = []
            ss.sku_bulk_ver += 1
            st.rerun()
        if not distri:
            st.caption("Select a distributor first.")

        if tambah:
            nama_cust = map_nama.get(distri, "")
            if not nama_cust:
                st.error(f"Customer Name is empty for distributor: {distri}"); return
            baru, lewat = [], []
            ada = {_sku_kunci(r["Customer Code"], r["Product"]) for r in ss.sku_staged}
            kandidat = []
            for _, r in hasil.iterrows():
                p  = _sku_str(r["Product"])
                cc = bangun_customer_code(map_raw.get(distri, ""), p)
                if not cc:
                    st.error(f"Customer Code (column G) is empty for distributor: {distri}"); return
                kandidat.append((cc, p, _sku_str(r["Customer Product Code"])))
            dup_db, gagal_cek = sb_cari_duplikat_sku("sku_mapping", [(cc, p) for cc, p, _ in kandidat])
            if gagal_cek:
                st.error("Could not check duplicates for: " + ", ".join(sorted(gagal_cek))); return
            for cc, p, kd in kandidat:
                k = _sku_kunci(cc, p)
                if k in ada or k in dup_db:
                    lewat.append(f"{cc}/{p}"); continue
                ada.add(k)
                baru.append({
                    "Pilih": False, "Distributor": distri, "Customer Code": cc,
                    "Customer Name": nama_cust, "Product": p,
                    "Customer Product Code": kd,
                    "Customer Product Name": cari_nama_produk(p, map_produk),
                    **{k2: "" for k2 in KOLOM_SKU_OPSIONAL},
                })
            ss.sku_staged.extend(baru)
            ss.sku_editor_key += 1
            ss.sku_bulk_rows = []
            ss.sku_bulk_ver += 1
            ss.pesan_toast = (f"{len(baru)} rows added to staged"
                              + (f", {len(lewat)} duplicates skipped: {', '.join(lewat[:5])}" if lewat else ""))
            ss.pesan_toast_icon = ":material/warning:" if lewat else ":material/check_circle:"
            st.rerun()
# HALAMAN: NOO SUBMISSION
# ═════════════════════════════════════════════════════════════════════════════
def cc_salah(df: pd.DataFrame) -> list:
    """Return [(index, product, customer_code, prefix_benar)] untuk baris yang prefix-nya salah."""
    hasil = []
    if df.empty or not {"Product", "Customer Code"} <= set(df.columns):
        return hasil
    for i, r in df.iterrows():
        p  = _sku_str(r["Product"]).upper()
        cc = _sku_str(r["Customer Code"])
        if not p or not cc:
            continue
        benar = "12" if p.startswith("G2G-") else "17"
        if cc[:2] != benar:
            hasil.append((i, p, cc, benar))
    return hasil

def perbaiki_cc(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for i, p, cc, benar in cc_salah(df):
        df.at[i, "Customer Code"] = benar + cc[2:]
    return df

def _aktifkan_fix_cc():
    st.session_state.sku_fix_on = True

def halaman_pengajuan_noo():
    ss = st.session_state
    st.markdown(komponen.html_breadcrumb("NOO & SKU Submission"), unsafe_allow_html=True)
    st.markdown(komponen.html_header("NOO & SKU Submission","Upload Failure List file to process data.",":material/description:"),
                unsafe_allow_html=True)

    with st.container(key="kotak_unggah"):
        daftar_berkas = st.file_uploader(
            "Upload Failure List (.xlsx / .xls, max 10 MB per file)",
            type=["xlsx","xls"], key="unggah_pengajuan", accept_multiple_files=True)

    tanda_file = tuple((b.name, b.size) for b in (daftar_berkas or []))
    if ss.get("tanda_file_upload") != tanda_file:
        for k in ["sku_staged", "sku_last_batch", "sku_last_n", "sku_msg",
                  "sku_fix_on", "noo_batch_terkirim",
                  "_sku_upload_tanda", "_sku_upload_baris"]:
            ss.pop(k, None)
        ss.sku_editor_key        = ss.get("sku_editor_key", 0) + 1
        ss.sku_upload_editor_key = ss.get("sku_upload_editor_key", 0) + 1
        ss.tanda_file_upload = tanda_file

    file_siap  = False
    data_sku   = pd.DataFrame(columns=KOLOM_SKU_OUTPUT)
    data_noo   = pd.DataFrame(columns=KOLOM_NOO_OUTPUT)
    info_alamat = []             # ringkasan per file (untuk caption diagnosa)
    alamat_valid_kode = set()    # Customer Store Code dari file yang kolom alamatnya bervariasi (> 2 nilai unik)

    if daftar_berkas:
        kebesaran = [b for b in daftar_berkas if b.size > 10*1024*1024]
        if kebesaran:
            nama_k = ", ".join(b.name for b in kebesaran)
        
            st.markdown(komponen.html_error_unggah(f"Exceeds 10 MB: {nama_k}"), unsafe_allow_html=True)
        else:
            try:
                semua = []
                for berkas in daftar_berkas:
                    semua.append(pd.read_excel(berkas))
                    uk = (f"{berkas.size/1024/1024:.1f} MB" if berkas.size > 1024*1024
                          else f"{max(1,round(berkas.size/1024))} KB")
                    st.markdown(komponen.html_info_file(berkas.name, uk), unsafe_allow_html=True)
                df_gabungan = pd.concat(semua, ignore_index=True)
                data_sku  = pivot_sku_mapping(df_gabungan)
                if ss.get("sku_fix_on"):
                    data_sku = perbaiki_cc(data_sku)
                data_noo  = pivot_noo(df_gabungan)
                # aturan alamat dinilai PER FILE upload: file dengan > 2 alamat unik (baris NOO, tidak kosong)
                # boleh mengisi Store Address otomatis; file lain dikosongkan
                for berkas_f, df_f in zip(daftar_berkas, semua):
                    try:
                        noo_f = pivot_noo(df_f)
                        # hitung alamat unik dari SELURUH kolom alamat di file (bukan hanya baris NOO),
                        # lalu terapkan ke baris NOO dari file itu
                        _norm = lambda x: re.sub(r"[^a-z0-9]", "", str(x).lower())
                        kol_alamat = next((c for c in df_f.columns if _norm(c) in (
                            "customeraddress", "storeaddress", "customerstoreaddress", "address", "alamat", "alamattoko")), None)
                        if kol_alamat is None:
                            info_alamat.append(f"{berkas_f.name}: kolom alamat tidak ditemukan di file -> alamat dikosongkan")
                            continue
                        al = df_f[kol_alamat].map(
                            lambda v: "" if str(v).strip() in ("nan", "None", "NaN") else str(v).strip())
                        n_unik = int(al[al != ""].nunique())
                        if n_unik > 2:
                            alamat_valid_kode |= set(noo_f["Customer Store Code"].astype(str).str.strip())
                        # info_alamat.append(f"{berkas_f.name}: kolom '{kol_alamat}' punya {n_unik} alamat unik, "
                        #                    f"{len(noo_f)} baris NOO -> "
                        #                    + ("alamat diisi otomatis" if n_unik > 2 else "alamat dikosongkan"))
                    except Exception as e_f:
                        info_alamat.append(f"{berkas_f.name}: gagal membaca alamat ({type(e_f).__name__}: {e_f})")
                file_siap = True
            except Exception as e:
                st.markdown(komponen.html_error_unggah(f"Failed to read file: {e}"), unsafe_allow_html=True)
    if not daftar_berkas:
        ss.sku_fix_on = False
    salah_cc = cc_salah(data_sku)
    st.markdown('<hr class="pemisah-tipis">', unsafe_allow_html=True)

    # ── SKU MAPPING ──────────────────────────────────────────────────────────
    with st.container(key="kartu_sku"):
        st.markdown(komponen.html_header_pengajuan(":material/inventory_2:","SKU Mapping","Pivot based on Customer SKU data."),
                    unsafe_allow_html=True)
        st.markdown(komponen.html_judul_preview(len(data_sku) if file_siap else None), unsafe_allow_html=True)
        st.markdown(judul_opsi(1,"Manual","select Distributor & fill Product, Customer Product Code"),
                    unsafe_allow_html=True)
        render_manual_sku()
        render_bulk_sku()
        st.markdown('<hr class="pemisah-opsi">', unsafe_allow_html=True)

        st.markdown(judul_opsi(2,"Upload Option","extracted from Failure List file"),
                    unsafe_allow_html=True)
        produk_kosong = False
        if file_siap and len(data_sku):
            try:
                map_produk_upload = muat_produk_lookup(SHEET_PRODUCT)
            except Exception:
                map_produk_upload = {}
            try:
                map_deskripsi_upload = muat_deskripsi_produk()
            except Exception as e:
                st.warning(f"Failed to load Product Description list: {e}")
                map_deskripsi_upload = {}
            st.caption(f"DEBUG: {len(map_deskripsi_upload)} descriptions loaded")
            ss.setdefault("sku_upload_editor_key", 0)
            tanda_upload = tuple(zip(
                data_sku["Product"].tolist(), data_sku["Customer Product Name"].tolist()
            ))
            if ss.get("_sku_upload_tanda") != tanda_upload:
                baris_cocok = []
                for _, r in data_sku.iterrows():
                    produk_raw = _sku_str(r["Product"])
                    kunci_raw  = _kunci_produk(produk_raw)
                    if kunci_raw in map_produk_upload:
                        hasil_p, status = kunci_raw, "Exact"
                    else:
                        # ambil kata pertama (sebelum spasi) dari Customer SKU Name,
                        # cek exact match saja — tidak ada fuzzy sama sekali
                        kode_depan  = _ambil_kode_depan(r["Customer Product Name"])
                        kunci_depan = _kunci_produk(kode_depan) if kode_depan else ""
                        if kunci_depan in map_produk_upload:
                            hasil_p, status = kunci_depan, "Exact"
                        else:
                            hasil_p, status = None, "Not found"
                    baris_cocok.append({"_match": status, "product_fix": hasil_p or produk_raw})
                ss._sku_upload_baris = baris_cocok
                ss._sku_upload_tanda = tanda_upload
                ss.sku_upload_editor_key += 1
            df_edit = data_sku.copy()
            df_edit["Customer SKU Name"] = data_sku["Customer Product Name"]  # ← teks asli dari file, hanya referensi tampilan
            for i, b in enumerate(ss.get("_sku_upload_baris", [])):
                if i < len(df_edit):
                    df_edit.loc[df_edit.index[i], "Product"] = b["product_fix"]

            if ss.get("sku_fix_on"):
                df_edit = perbaiki_cc(df_edit)     # tampilan Customer Code ikut terkoreksi
            LEBAR_SKU_UPLOAD = [1.0, 1.2, 1.8, 2.0, 1.2, 1.3, 3.0]
            kolom_tampil = ["Customer Code", "Customer Name", "Customer SKU Name",
                            "Product Description (Reference)", "Product", "Customer Product Code",
                            "Customer Product Name"]

            tabel_sku_upload = st.container(key="tabel_sku_upload_scroll")
            tabel_sku_upload.__enter__()
            kepala = st.columns(LEBAR_SKU_UPLOAD)
            for kolom, teks in zip(kepala, kolom_tampil):
                kolom.markdown(
                    f'<b style="font-size:0.78rem; white-space:nowrap">{teks}</b>',
                    unsafe_allow_html=True)

            CELL_STYLE = ("font-size:0.78rem; white-space:nowrap; overflow:hidden; "
                          "text-overflow:ellipsis; display:block;")
            CELL_STYLE_WRAP = ("font-size:0.78rem; white-space:normal; word-break:break-word; "
                               "line-height:1.3; display:block;")
            opsi_deskripsi = [""] + sorted(map_deskripsi_upload.keys())

            produk_terupdate = []
            for i, r in df_edit.reset_index(drop=True).iterrows():
                c_cc, c_cn, c_skuname, c_desk, c_produk, c_kode, c_namaprod = st.columns(LEBAR_SKU_UPLOAD)
                c_cc.markdown(
                    f'<span style="{CELL_STYLE}">{r["Customer Code"]}</span>',
                    unsafe_allow_html=True)
                c_cn.markdown(
                    f'<span style="{CELL_STYLE_WRAP}">{r["Customer Name"]}</span>',
                    unsafe_allow_html=True)
                c_skuname.markdown(
                    f'<span style="font-size:0.68rem; white-space:normal; '
                    f'word-break:break-word; line-height:1.25; color:#64748b">'
                    f'{r["Customer SKU Name"]}</span>',
                    unsafe_allow_html=True)

                desc_key   = f"sku_upload_desc_{ss.sku_upload_editor_key}_{i}"
                produk_key = f"sku_upload_produk_{ss.sku_upload_editor_key}_{i}"
                c_desk.selectbox(
                    "", opsi_deskripsi, label_visibility="collapsed",
                    key=desc_key,
                    on_change=_set_produk_dari_deskripsi,
                    args=(produk_key, desc_key, map_deskripsi_upload),
                )

                opsi = [""] + sorted(map_produk_upload)          # "" = belum ada produk yang cocok (kode semula tidak ketemu)
                idx  = opsi.index(r["Product"]) if r["Product"] in opsi else 0
                produk_pilih = c_produk.selectbox(
                    "", opsi, index=idx, label_visibility="collapsed",
                    key=produk_key,
                )
                c_kode.markdown(
                    f'<span style="{CELL_STYLE}">{r["Customer Product Code"]}</span>',
                    unsafe_allow_html=True)
                nama_produk = cari_nama_produk(produk_pilih, map_produk_upload)
                c_namaprod.markdown(
                    f'<span style="font-size:0.78rem; white-space:nowrap;">{nama_produk}</span>',
                    unsafe_allow_html=True)
                produk_terupdate.append(produk_pilih)
            tabel_sku_upload.__exit__(None, None, None)

            data_sku = df_edit.drop(columns=["Customer SKU Name"]).reset_index(drop=True)
            data_sku["Product"] = produk_terupdate
            data_sku["Customer Product Name"] = [
                cari_nama_produk(p, map_produk_upload) for p in produk_terupdate
            ]
            if ss.get("sku_fix_on"):
                # Product bisa berubah setelah auto-match / dropdown, jadi prefix harus dihitung ulang
                # terhadap Product FINAL (sebelumnya hanya terhadap Product mentah dari file)
                data_sku = perbaiki_cc(data_sku)
            produk_kosong = any(not _sku_str(p) for p in produk_terupdate)
            if produk_kosong:
                n_kosong = sum(not _sku_str(p) for p in produk_terupdate)
                st.warning(f"{n_kosong} row(s) have no Product yet. Pick the Product Description or Product, "
                           f"Submit is locked until every row has a Product.")
            salah_cc = cc_salah(data_sku)
            if salah_cc:
                contoh = ", ".join(f"{p}: {cc}→{b}…" for _, p, cc, b in salah_cc[:5])
                st.error(f"{len(salah_cc)} row(s) have an invalid Customer Code prefix "
                         f"(G2G- products must use 12, all others must use 17). Examples: {contoh}. "
                         f"Submit is locked until this is fixed.")
                st.button(":material/build: Fix Customer Code", key="sku_fix", on_click=_aktifkan_fix_cc)
            elif ss.get("sku_fix_on"):
                st.success("Customer Code has been corrected to match the product brand.")
        else:
            st.markdown('<div class="preview-kosong">No data yet, please upload a file first</div>',
                        unsafe_allow_html=True)

    if st.button(":material/send: Submit SKU Mapping", disabled=not file_siap or bool(salah_cc) or produk_kosong,
                 type="primary", width="stretch", key="submit_sku"):
        if cc_salah(data_sku):          
            st.error("Submit cancelled: some Customer Codes still have an invalid prefix.")
            st.stop()
        rows = data_sku.fillna("").to_dict("records")
        with st.spinner("Checking duplicates..."):
            dup_db, gagal_cek = sb_cari_duplikat_sku(
                "sku_mapping", [(_sku_str(r["Customer Code"]), _sku_str(r["Product"])) for r in rows])
        if gagal_cek:
            ss.pesan_toast = "Cancelled — could not check duplicates: " + ", ".join(sorted(gagal_cek))
            ss.pesan_toast_icon = ":material/warning:"
            st.rerun()
        seen, baru = set(), []
        for r in rows:
            k = _sku_kunci(r["Customer Code"], r["Product"])
            if k in dup_db or k in seen:
                continue
            seen.add(k); baru.append(r)
        if not baru:
            ss.pesan_toast = "All rows already exist in sku_mapping. Nothing saved."
            ss.pesan_toast_icon = ":material/warning:"
            st.rerun()
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        kolom_ada = [k for k in SKU_KOLOM_DB if k in data_sku.columns]
        payload = [{**{SKU_KOLOM_DB[k]: (_sku_str(r[k]) or None) for k in kolom_ada},
                    "created_at": ts} for r in baru]
        with st.spinner("Saving to Supabase..."):
            result = sb_insert("sku_mapping", payload)
        if isinstance(result, dict) and "_error" in result:
            st.error(f"Failed to save: {result['_error']}")
        else:
            catat_riwayat("Submit SKU Mapping", f"{len(payload)} rows (skipped: {len(rows)-len(baru)})")
            ss.pesan_toast = (f"SKU Mapping saved — {len(payload)} rows"
                              + (f", {len(rows)-len(baru)} duplicates skipped" if len(baru) < len(rows) else ""))
            ss.pesan_toast_icon = ":material/warning:" if len(baru) < len(rows) else ":material/check_circle:"
            st.rerun()
    st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)

    # ── NOO ──────────────────────────────────────────────────────────────────
    with st.container(key="kartu_noo"):
        lbl = "\u200b"
        st.markdown(komponen.html_header_pengajuan(":material/storefront:","NOO","Pivot based on outlet data."),
                    unsafe_allow_html=True)
        st.markdown(komponen.html_judul_preview(len(data_noo) if file_siap else None), unsafe_allow_html=True)

        # Opsi 1: Manual bulk paste
        st.markdown(judul_opsi(1,"Manual","paste Customer Store Code & Store Name"), unsafe_allow_html=True)
        with st.expander(":material/edit_note: Manual Input and Paste Customer Store Code", expanded=bool(ss.get("bulk_data"))):
            st.caption("Step 1 — Paste Customer Store Code and Store Name (one per line)")
            c_bulk_kode, c_bulk_nama = st.columns(2)
            bulk_kode = c_bulk_kode.text_area("Customer Store Code",height=120,key="bulk_kode_noo",
                                               placeholder="DST001\nDST002\n...")
            bulk_nama = c_bulk_nama.text_area("Store Name",height=120,key="bulk_nama_noo",
                                               placeholder="STORE A\nSTORE B\n...")
            if st.button("Apply Paste", key="terapkan_bulk"):
                baris_kode = [k.strip().upper() for k in bulk_kode.splitlines() if k.strip()]   # kode toko selalu huruf besar
                baris_nama = [k.strip() for k in bulk_nama.splitlines() if k.strip()]
                if not baris_kode:
                    st.warning("Customer Store Code is required.")
                elif baris_nama and len(baris_nama) != len(baris_kode):
                    st.error(f"Mismatch: {len(baris_nama)} names vs {len(baris_kode)} codes.")
                else:
                    ss.bulk_data = [{"Customer Store Code":baris_kode[n],
                                     "Store Name":baris_nama[n] if baris_nama else ""}
                                    for n in range(len(baris_kode))]
                    st.rerun()

            if ss.get("bulk_data"):
                st.markdown("---")
                st.caption("Step 2 — Select distributor, then complete details for each outlet")
                distri_map      = muat_distri_code_map()
                prefix_map      = {"NXP":"17","G2G":"12"}
                LEBAR_MANUAL    = [1.2,1.2,1.3,0.8,0.9,0.9,1.5,1.1,1.1,1.2]
                spv_sales_lookup = muat_spv_sales_lookup()
                tabel_manual = st.container(key="tabel_manual_scroll")
                tabel_manual.__enter__()
                h = st.columns(LEBAR_MANUAL)
                for col, (teks, wajib) in zip(h,[
                    ("Store Code",False),("Store Name",False),("Distributor",False),
                    ("NXP/G2G",True),("Customer Code",False),("Branch Code",False),
                    ("Store Address",True),("SPV",True),("Sales Name",True),("Store Type",True)]):
                    tanda = ' <b style="color:#ef4f58">*</b>' if wajib else ""
                    col.markdown(f"<b>{teks}</b>{tanda}", unsafe_allow_html=True)

                bulk_semua_terisi = True
                tidak_ketemu = []
                for idx, baris in enumerate(ss.bulk_data):
                    c1,c2,c3,c4,c5,c6,c7,c8,c9,c10 = st.columns(LEBAR_MANUAL)
                    kode_toko   = str(baris["Customer Store Code"]).strip().upper()   # juga untuk data paste lama di sesi
                    baris["Customer Store Code"] = kode_toko
                    branch_code = kode_toko[:6]
                    info        = distri_map.get(branch_code.upper())
                    distri_nama = info["distributor"] if info else ""
                    company_code = info["company_code"] if info else ""
                    if not info:
                        tidak_ketemu.append(kode_toko)
                    c1.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{kode_toko}</div>', unsafe_allow_html=True)
                    c2.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{baris["Store Name"]}</div>', unsafe_allow_html=True)
                    c3.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{distri_nama or "not found"}</div>', unsafe_allow_html=True)
                    prefix_pilih  = c4.selectbox(lbl,["","NXP","G2G"],label_visibility="collapsed",key=f"bulk_prefix_{idx}")
                    custcode_final = prefix_map[prefix_pilih]+company_code if (prefix_pilih and company_code) else ""
                    c5.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{custcode_final or "-"}</div>', unsafe_allow_html=True)
                    c6.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{branch_code or "-"}</div>', unsafe_allow_html=True)
                    alamat = c7.text_input(lbl,label_visibility="collapsed",key=f"bulk_alamat_{idx}",placeholder="Enter address...")
                    spv, sales = input_spv_sales(distri_nama, f"bulk_{idx}", c8, c9, spv_sales_lookup)
                    tipe = c10.selectbox(lbl,[""] + JENIS_TOKO_DEFAULT,label_visibility="collapsed",key=f"bulk_tipe_{idx}")
                    baris.update({
                        "Customer Code":baris.get("Customer Code","") or custcode_final,
                        "Customer Branch Code":branch_code,
                        "Customer Store Name":distri_nama,
                        "Group Name":distri_nama,
                        "Channel":"GT",
                        "Store Address":alamat,
                        "SPV":spv,
                        "SE/KAE":sales,
                        "Store Type":tipe,
                    })
                    if not (info and prefix_pilih and alamat.strip() and spv.strip() and sales.strip() and tipe):
                        bulk_semua_terisi = False
                tabel_manual.__exit__(None, None, None)

                if tidak_ketemu:
                    st.warning("Distributor code not found for: " + ", ".join(tidak_ketemu))

                df_bulk = pd.DataFrame(ss.bulk_data)
                for k in KOLOM_NOO_OUTPUT:
                    if k not in df_bulk.columns:
                        df_bulk[k] = ""
                df_bulk = df_bulk[KOLOM_NOO_OUTPUT]

                if st.button(":material/send: Submit Manual NOO", disabled=not bulk_semua_terisi,
                             type="primary", width="stretch", key="submit_bulk_noo"):
                    kode_list  = df_bulk["Customer Store Code"].astype(str).str.strip().tolist()
                    tanda_batch = ("bulk",) + tuple(sorted(kode_list))
                    if ss.get("noo_batch_terkirim") == tanda_batch:
                        set_keterangan_noo("warning","Data already submitted.")
                    else:
                        with st.spinner("Checking duplicates..."):
                            dup_in, g_in   = sb_cari_duplikat_kode(TABLE_SUBMIT_NOO, kode_list,"Customer Store Code")
                            dup_hi, g_hi   = bq_cari_duplikat_kode(kode_list)
                            duplikat = dup_in | dup_hi
                            gagal    = g_in  | g_hi
                        if gagal:
                            set_keterangan_noo("error","Submission cancelled — failed to check: " + ", ".join(sorted(gagal)))
                        else:
                            mask_dup   = df_bulk["Customer Store Code"].astype(str).str.strip().isin(duplikat)
                            data_bersih   = df_bulk[~mask_dup]
                            data_dilewati = df_bulk[mask_dup]
                            if data_bersih.empty:
                                set_keterangan_noo("error","All codes already exist: " + ", ".join(sorted(duplikat)))
                            else:
                                with st.spinner("Sending..."):
                                    wkt = datetime.datetime.now(ZONA_WAKTU).isoformat(timespec="seconds")
                                    result = sb_insert(TABLE_SUBMIT_NOO,
                                                       [noo_row_ke_supabase(b,wkt) for _,b in data_bersih.iterrows()])
                                if isinstance(result,dict) and "_error" in result:
                                    set_keterangan_noo("error",f"Failed: {result['_error']}")
                                else:
                                    ss.noo_batch_terkirim = tanda_batch
                                    ambil_raw_noo.clear()
                                    sisa = set(data_dilewati["Customer Store Code"].astype(str).str.strip())
                                    ss.bulk_data = [b for b in ss.bulk_data
                                                    if str(b["Customer Store Code"]).strip() in sisa] or None
                                    catat_riwayat("Submit Manual NOO",
                                                  f"{len(data_bersih)} rows (skipped: {len(data_dilewati)})")
                                    if data_dilewati.empty:
                                        set_keterangan_noo("success",f"Submitted — {len(data_bersih)} rows")
                                    else:
                                        kode_skip = ", ".join(sorted(data_dilewati["Customer Store Code"].astype(str)))
                                        set_keterangan_noo("warning",
                                                           f"Sent {len(data_bersih)} rows. Skipped: {kode_skip}")
                                    st.rerun()
            tampilkan_keterangan_noo()

        st.markdown('<hr class="pemisah-opsi">', unsafe_allow_html=True)

        # Opsi 2: dari file upload
        st.markdown(judul_opsi(2,"Upload Option","extracted from Failure List file"), unsafe_allow_html=True)
        noo_siap = False
        if not file_siap or data_noo.empty:
            st.markdown('<div class="preview-kosong">No data yet, please upload a file first</div>',
                        unsafe_allow_html=True)
            ss["_noo_siap_btn"] = False
        else:
            # Fragment: mengetik/memilih di tabel ini hanya me-render ulang tabel, bukan seluruh halaman (tanpa loading panjang)
            @st.fragment
            def _tabel_noo_upload(data_noo_in):
                ss = st.session_state
                LEBAR_NOO = [1.7,1.6,1.6,1.0,1.5,1.5,2.0,2.0,1.3,0.8]
                tabel_noo = st.container(key="tabel_noo_scroll")
                tabel_noo.__enter__()
                header = st.columns(LEBAR_NOO)
                for kolom, (teks, wajib) in zip(header,[
                    ("Customer Store Code",True),("Store Name",False),("Customer Store Name",True),
                    ("Brand",False),("City",False),("Store Address",True),("SPV",True),("Sales Name",True),("Store Type",True),
                    ("Channel",True)]):
                    tanda = ' <b style="color:#ef4f58">*</b>' if wajib else ""
                    kolom.markdown(
                        f'<div style="font-size:13px; line-height:1.3; '
                        f'margin-bottom:6px; word-break:break-word;">{teks}{tanda}</div>',
                        unsafe_allow_html=True)
                spv_sales_lookup = muat_spv_sales_lookup()
                semua_terisi = True
                data_noo = data_noo_in.reset_index(drop=True)
                OPSI_TIPE = [""] + JENIS_TOKO_DEFAULT
                IDX_TIPE_DEFAULT = next(
                    (n for n, o in enumerate(OPSI_TIPE) if str(o).strip().lower() == "retail store"), 0)
                for _c in ["Customer Store Code","City","Store Address","SPV","SE/KAE","Store Type","Channel"]:
                    if _c not in data_noo.columns:
                        data_noo[_c] = ""
                    data_noo[_c] = data_noo[_c].astype("object")
                for _baris_info in info_alamat:
                    st.caption("" + _baris_info)
                # Store Address dari file hanya dipakai untuk file yang alamatnya bervariasi (> 2 nilai unik, dinilai per file
                # upload, lihat alamat_valid_kode). File lain dikosongkan (tetap bisa diisi/edit).
                # key widget memuat versi file + aturan: tanpa ini Streamlit memakai nilai lama dari upload sebelumnya
                ver_file = abs(hash((tanda_file, len(alamat_valid_kode)))) % 10**8
                distri_kode_map = muat_distri_code_map()
                for i, baris in data_noo.iterrows():
                    # distributor (untuk lookup SPV/Sales) = Customer Branch Name dari file, disimpan di Group Name
                    csn = str(baris.get("Group Name","") or baris.get("Customer Store Name","")).strip().upper()
                    # Nama distributor di file bisa berubah -> utamakan lookup dari 6 karakter awal Customer Store Code
                    # ke sheet Distributor Mapping (kolom H), baru cocokkan SPV/Sales di Sales Mapping.
                    _info_distri = distri_kode_map.get(str(baris["Customer Store Code"]).strip()[:6].upper())
                    if _info_distri:
                        csn = _info_distri["distributor"].strip().upper()
                        data_noo.loc[i, "Group Name"] = _info_distri["distributor"].strip()   # ikut tersimpan ke Supabase
                    #EDIT DISINI KOLOM NOO
                    c_kode,c_nama,c_csn,c_brand,c_city,c_alamat,c_spv,c_sales,c_tipe,c_channel = st.columns(LEBAR_NOO)
                    #-------------
                    kode_val = str(baris["Customer Store Code"])
                    c_kode.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{kode_val}</div>', unsafe_allow_html=True)
                    c_nama.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{baris["Store Name"]}</div>', unsafe_allow_html=True)
                    # Customer Store Name otomatis terisi dari Store Name, tetap bisa diedit
                    csn_noo = c_csn.text_input(lbl, value=str(baris["Store Name"]).strip(), label_visibility="collapsed",
                                               key=f"noo_csn_{i}_{ver_file}", placeholder="Customer store name...").strip()
                    # Brand dari 2 digit awal Customer Code di file: 12 = G2G, 17 = NXP/BB
                    _cc2 = str(baris.get("Customer Code","")).strip()[:2]
                    brand_val = {"12": "G2G", "17": "NXP/BB"}.get(_cc2, "-")
                    c_brand.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{brand_val}</div>', unsafe_allow_html=True)
                    city_noo = c_city.selectbox(lbl,[""] + CITY,label_visibility="collapsed",key=f"noo_city_{i}")
                    alamat_raw = str(baris.get("Store Address","") or "")
                    alamat_val = ("" if kode_val.strip() not in alamat_valid_kode or alamat_raw.strip() in ("nan","None","","NaN")
                                  else alamat_raw.strip())
                    # text_area: alamat panjang membungkus ke baris berikutnya, tidak terpotong; tetap bisa diedit
                    alamat_noo = c_alamat.text_area(lbl,value=alamat_val,label_visibility="collapsed",height=68,
                                                    key=f"noo_alamat_{i}_{ver_file}",placeholder="Store address...")
                    alamat_noo = " ".join(str(alamat_noo).split())   # Enter di text_area = baris baru -> jadikan spasi
                    spv_noo, sales_noo = input_spv_sales(csn, f"noo_{i}", c_spv, c_sales, spv_sales_lookup)
                    tipe_noo = c_tipe.selectbox(lbl,OPSI_TIPE,index=IDX_TIPE_DEFAULT,
                                                label_visibility="collapsed",key=f"noo_tipe_{i}")
                    channel_noo = c_channel.selectbox(lbl,["GT","MTI"],label_visibility="collapsed",key=f"noo_channel_{i}")
                    data_noo.loc[i,"Customer Store Code"] = kode_val
                    data_noo.loc[i,"Customer Store Name"] = csn_noo
                    data_noo.loc[i,"City"]       = city_noo
                    data_noo.loc[i,"Store Address"] = alamat_noo
                    data_noo.loc[i,"SE/KAE"]     = sales_noo
                    data_noo.loc[i,"SPV"]        = spv_noo
                    data_noo.loc[i,"Store Type"] = tipe_noo
                    data_noo.loc[i,"Channel"] = channel_noo
                    if not (kode_val.strip() and csn_noo and tipe_noo and spv_noo.strip() and sales_noo.strip()):
                        semua_terisi = False
                tabel_noo.__exit__(None, None, None)
                # status tombol Submit NOO ada di luar fragment: kalau berubah, segarkan seluruh halaman sekali
                ss['_noo_data_final'] = data_noo
                if ss.get('_noo_siap_btn') != semua_terisi:
                    ss['_noo_siap_btn'] = semua_terisi
                    st.rerun(scope='app')
            _tabel_noo_upload(data_noo)

    if st.button(":material/send: Submit NOO", disabled=not (file_siap and ss.get("_noo_siap_btn", False)),
                 type="primary", width="stretch", key="submit_noo"):
        data_noo = ss["_noo_data_final"]
        kode_list   = data_noo["Customer Store Code"].astype(str).str.strip().tolist()
        tanda_batch = tuple(sorted(kode_list))
        if ss.get("noo_batch_terkirim") == tanda_batch:
            set_keterangan_noo("warning","Already submitted. Modify data or upload new file.")
        else:
            with st.spinner("Checking duplicates..."):
                dup_in, g_in = sb_cari_duplikat_kode(TABLE_SUBMIT_NOO, kode_list,"Customer Store Code")
                dup_hi, g_hi = bq_cari_duplikat_kode(kode_list)
                duplikat = dup_in | dup_hi
                gagal    = g_in  | g_hi
            if gagal:
                set_keterangan_noo("error","Cancelled — failed to verify: " + ", ".join(sorted(gagal)))
            else:
                mask_dup      = data_noo["Customer Store Code"].astype(str).str.strip().isin(duplikat)
                data_bersih   = data_noo[~mask_dup]
                data_dilewati = data_noo[mask_dup]
                if data_bersih.empty:
                    set_keterangan_noo("error","All codes already exist: " + ", ".join(sorted(duplikat)))
                else:
                    with st.spinner("Sending..."):
                        wkt    = datetime.datetime.now(ZONA_WAKTU).isoformat(timespec="seconds")
                        result = sb_insert(TABLE_SUBMIT_NOO,
                                           [noo_row_ke_supabase(b,wkt) for _,b in data_bersih.iterrows()])
                    if isinstance(result,dict) and "_error" in result:
                        set_keterangan_noo("error",f"Failed: {result['_error']}")
                    else:
                        ss.noo_batch_terkirim = tanda_batch
                        ambil_raw_noo.clear()
                        catat_riwayat("Submit NOO", f"{len(data_bersih)} rows (skipped: {len(data_dilewati)})")
                        if data_dilewati.empty:
                            set_keterangan_noo("success",f"NOO submitted — {len(data_bersih)} rows")
                        else:
                            kode_skip = ", ".join(sorted(data_dilewati["Customer Store Code"].astype(str)))
                            set_keterangan_noo("warning",f"Sent {len(data_bersih)} rows. Skipped: {kode_skip}")
                        st.rerun()
    tampilkan_keterangan_noo()

# ═════════════════════════════════════════════════════════════════════════════
# HALAMAN: SPV CONFIRMATION
# ═════════════════════════════════════════════════════════════════════════════
OPSI_JENIS_SPV  = ["NOO","Existing"]
LABEL_KOSONG    = "\u200b"
LEBAR_KOLOM_SPV = [0.5,2.2,2.2,1.6,1.1,2.2,2.8,2.4,2.0,2.8,2.2,2.0,2.0,2.2]
IDX_JENIS, IDX_STORE_ID = 11, 12

def _kid(baris) -> str:
    return re.sub(r"[^A-Za-z0-9_]","_",str(baris["id"]))

def _masalah_baris_spv(b) -> dict:
    return {
        "jenis":    b["jenis_outlet"] not in OPSI_JENIS_SPV,
        "store_id": b["jenis_outlet"] == "Existing" and not str(b["store_id_validasi"]).strip(),
    }

def _css_status_spv(baris_tampil, tampil_error: bool) -> str:
    aturan = []
    for b in baris_tampil:
        k = _kid(b)
        m = _masalah_baris_spv(b)
        sel = f'.st-key-spv_jenis_{k} [data-baseweb="select"] > div'
        if b["jenis_outlet"] == "NOO":
            aturan.append(f"{sel}{{background:#d3f9d8;border-color:#86d98f}}")
        elif b["jenis_outlet"] == "Existing":
            aturan.append(f"{sel}{{background:#fff3b0;border-color:#e6c84a}}")
        elif tampil_error:
            aturan.append(f"{sel}{{border:1.5px solid #ef4f58;background:#fef2f2}}")
        else:
            aturan.append(f"{sel}{{border:1.5px dashed #cbd5e1;background:#ffffff}}")
        if tampil_error and m["store_id"]:
            aturan.append(f'.st-key-spv_sid_{k} [data-baseweb="input"]'
                          f'{{border:1.5px solid #ef4f58;background:#fef2f2}}')
    return "<style>" + "".join(aturan) + "</style>"

def _isi_store_id_otomatis(pasangan):
    """Callback tombol bulk: salin kode dari kolom Similarity (Code) ke Store ID (Validation).
    Hanya baris yang dipilih Existing dan Store ID-nya masih kosong (ketikan user tidak ditimpa)."""
    ss = st.session_state
    for k, kode in pasangan:
        if ss.get(f"spv_jenis_{k}") == "Existing" and not str(ss.get(f"spv_sid_{k}", "") or "").strip():
            ss[f"spv_sid_{k}"] = kode

def _tagging_dari_jenis(jenis: str) -> str:
    """Pemetaan hasil SPV Confirmation -> kolom 'tagging' di Supabase.
    'NOO'      -> STATUS_REVIEW (tetap menunggu; Generate ID hanya dijalankan manual oleh user di Store Validation)
    'Existing' -> 'Existing' apa adanya (Store ID sudah final, diisi manual oleh SPV)
    """
    return STATUS_REVIEW if jenis == "NOO" else jenis

def simpan_konfirmasi_spv(baris: list) -> tuple[int, list]:
    """Push hasil SPV Confirmation ke Supabase.
    PENTING: fungsi ini HANYA boleh dipanggil dari handler tombol Submit,
    setelah seluruh baris lolos _masalah_baris_spv (tidak ada yang 'jenis'
    kosong atau 'Existing' tanpa Store ID). Jangan dipanggil dari dalam
    loop render (per keystroke/selectbox change) — itu yang menyebabkan
    status & Store ID "ke-push" ke Supabase sebelum sempat divalidasi.
    """
    berhasil, gagal = 0, []
    for b in baris:
        kode = str(b["store_code"]).strip()
        nama = str(b.get("csn", b["distributor"])).strip()   # Customer Store Name asli (kunci update)
        if not kode or not nama:
            gagal.append(f"baris #{b['no']} (kode/nama kosong)")
            continue
        patch = {"tagging": _tagging_dari_jenis(b["jenis_outlet"])}
        if b.get("ada_kolom_spv"):
            patch["spv_confirmed"] = True          # NOO & Existing sama-sama dianggap sudah dikonfirmasi
        if b["jenis_outlet"] == "Existing":
            patch["Store ID"]     = str(b["store_id_validasi"]).strip()
            patch["generated_at"] = None
        if sb_patch_by_kode_nama(TABLE_SUBMIT_NOO, kode, nama, patch):
            berhasil += 1
            st.session_state.setdefault("spv_selesai", set()).add(b["id"])   # langsung hilang dari daftar
        else:
            gagal.append(f"{kode} / {nama}")
    return berhasil, gagal

def halaman_konfirmasi_spv():
    ss = st.session_state
    ss.setdefault("spv_tampil_error", False)
    st.markdown(komponen.html_breadcrumb("SPV Confirmation"), unsafe_allow_html=True)
    st.markdown(komponen.html_header("SPV Confirmation",
                                      "Confirm outlet type and validate data before submission.",":material/verified_user:"),
                unsafe_allow_html=True)

    kunci_distri = distributor_terkunci(ss.get("user"))
    c_distributor, c_region, c_refresh_spv, _ = st.columns([1.6,1.4,0.8,1.2])
    with c_distributor:
        if kunci_distri is None:
            distributor = st.selectbox("Distributor *",[""] + daftar_distributor(), key="distributor_spv")
        elif kunci_distri:
            # akun distributor: terkunci ke distributornya sendiri
            st.selectbox("Distributor *", [kunci_distri], key="distributor_spv_terkunci", disabled=True)
            distributor = kunci_distri
        else:
            st.selectbox("Distributor *", [], key="distributor_spv_terkunci", disabled=True)
            st.warning("Your account code is not linked to any active distributor.")
            distributor = "__no_distributor__"      # tidak cocok dengan distributor mana pun -> tidak ada baris
        region_noo  = region_dari_master(distributor) or region_distributor(distributor)   # master BigQuery dulu, sheet sbg cadangan
    with c_region:
        st.markdown('<div style="height:1.7rem"></div>', unsafe_allow_html=True)
        st.markdown(komponen.html_info_region(region_noo), unsafe_allow_html=True)
    with c_refresh_spv:
        st.markdown('<div style="height:1.7rem"></div>', unsafe_allow_html=True)
        if st.button(":material/refresh: Refresh", key="refresh_spv", width="stretch"):
            ambil_raw_noo.clear()
            # MIFTAH EDIT (BARU): Refresh juga menghapus cache master BigQuery supaya data master terbaru terbaca.
            ambil_master_toko.clear(); _master_ternormalisasi.clear(); _indeks_kode_master.clear()
            ss.sim_tanda = None; ss.sim_hasil = {}
            ss.pesan_toast = "Data refreshed"
            st.rerun()

    semua = bangun_baris_spv(distributor)
    if distributor:
        _nk = lambda s: re.sub(r"[^A-Z0-9]", "", str(s or "").upper())   # abaikan spasi/tanda baca/huruf besar-kecil
        semua = [b for b in semua if _nk(b["distributor"]) == _nk(distributor)]
    _cache_rg = {}
    for b in semua:                      # region tiap baris dari distributor-nya sendiri (bukan hanya dropdown)
        d = b["distributor"]
        if d not in _cache_rg:
            _cache_rg[d] = region_dari_master(d) or region_distributor(d)
        b["region_noo"] = _cache_rg[d]
    for n, b in enumerate(semua, 1):
        b["no"] = n                      # nomor urut ulang; "id" tetap (key widget stabil)
    isi_similarity_spv(semua, region_noo)
    ss.baris_spv = semua
    selesai = ss.get("spv_selesai", set())
    baris_tampil = [b for b in semua if b.get("tagging","") == STATUS_REVIEW
                    and not b.get("spv_confirmed") and b["id"] not in selesai]
    tempat_ringkasan = st.empty()
    
    n_sudah = len(semua) - len(baris_tampil)
    if n_sudah and baris_tampil and distributor:
        st.success(f"{n_sudah} outlet(s) already confirmed, {len(baris_tampil)} remaining.")

    _ingat_scroll_tabel()
    with st.container(key="kartu_spv"):
        st.markdown(komponen.html_catatan_spv(), unsafe_allow_html=True)
        if not baris_tampil:
            st.info(f"No NOO outlet data for {distributor}." if distributor
                    else "No NOO outlet data to display.")
        else:
          # tombol bulk: hanya muncul kalau ada baris Existing yang punya kode di Similarity & Store ID masih kosong
          siap_isi = [(_kid(b), str(b.get("similarity_kode", "")).strip()) for b in baris_tampil
                      if ss.get(f"spv_jenis_{_kid(b)}") == "Existing"
                      and str(b.get("similarity_kode", "")).strip()
                      and not str(ss.get(f"spv_sid_{_kid(b)}", "") or "").strip()]
          if siap_isi:
              st.button(f":material/auto_fix_high: Auto-fill Store ID from Similarity ({len(siap_isi)})",
                        key="spv_isi_otomatis", on_click=_isi_store_id_otomatis, args=(siap_isi,))
          with st.container(key="tabel_spv_scroll"):
            kepala = st.columns(LEBAR_KOLOM_SPV)
            for kolom, (teks, wajib) in zip(kepala,[
                ("No.",False),("Customer Store Code",False),("Distributor",False),("Region",False),("Brand",False),
                ("Store Name",False),("Store Address",False),("Similarity (Name)",False),
                ("Similarity (Code)",False),("Similarity (Address)",False),("Similarity (Region)",False),("NOO / Existing",True),
                ("Store ID (Validation)",False),("Remarks",False)]):
                kolom.markdown(komponen.html_kepala_kolom(teks, wajib), unsafe_allow_html=True)

            for b in baris_tampil:
                k = _kid(b)
                kunci_jenis, kunci_sid, kunci_ket = f"spv_jenis_{k}",f"spv_sid_{k}",f"spv_ket_{k}"
                ss.setdefault(kunci_jenis, b["jenis_outlet"])
                ss.setdefault(kunci_sid, b["store_id_validasi"])
                ss.setdefault(kunci_ket, b["keterangan"])
                with st.container(key=f"baris_spv_{k}"):
                    c_no,c_kode,c_distri,c_region_asli,c_brand,c_nama,c_alamat,c_sim_nama,c_sim_kode,c_sim_alamat,c_sim_region,c_jenis,c_sid,c_ket = \
                        st.columns(LEBAR_KOLOM_SPV)
                    c_no.markdown(komponen.html_sel_teks(b["no"]), unsafe_allow_html=True)
                    c_kode.markdown(komponen.html_sel_teks(b["store_code"],"sel-baca"), unsafe_allow_html=True)
                    c_distri.markdown(komponen.html_sel_teks(b["distributor"]), unsafe_allow_html=True)
                    c_region_asli.markdown(komponen.html_sel_teks(", ".join(b.get("region_noo") or ()) or "-","sel-baca"), unsafe_allow_html=True)
                    c_brand.markdown(komponen.html_sel_teks(b.get("brand") or "-","sel-baca"), unsafe_allow_html=True)
                    c_nama.markdown(komponen.html_sel_teks(b["nama"],"sel-tebal"), unsafe_allow_html=True)
                    c_alamat.markdown(komponen.html_sel_teks(b["alamat"],"sel-baca"), unsafe_allow_html=True)
                    c_sim_nama.markdown(komponen.html_sel_teks(b.get("similarity_nama",""),"sel-baca"), unsafe_allow_html=True)
                    c_sim_kode.markdown(komponen.html_sel_teks(b.get("similarity_kode",""),"sel-baca"), unsafe_allow_html=True)
                    c_sim_alamat.markdown(komponen.html_sel_teks(b.get("similarity_alamat",""),"sel-baca"), unsafe_allow_html=True)
                    c_sim_region.markdown(komponen.html_sel_teks(b.get("similarity_region",""),"sel-baca"), unsafe_allow_html=True)
                    jenis    = c_jenis.selectbox(LABEL_KOSONG, OPSI_JENIS_SPV, index=None,
                                                 placeholder="Pilih", label_visibility="collapsed", key=kunci_jenis)
                    store_id = c_sid.text_input(LABEL_KOSONG, label_visibility="collapsed",
                                                key=kunci_sid, placeholder="Isi jika Existing")
                    ket      = c_ket.text_input(LABEL_KOSONG, label_visibility="collapsed",
                                                key=kunci_ket, placeholder="Add remark...")
                    b["jenis_outlet"] = jenis
                    b["store_id_validasi"] = store_id
                    b["keterangan"] = ket
                    if ss.spv_tampil_error:
                        m = _masalah_baris_spv(b)
                        if m["jenis"] or m["store_id"]:
                            e = st.columns(LEBAR_KOLOM_SPV)
                            if m["jenis"]:
                                e[IDX_JENIS].markdown(komponen.html_pesan_error("Wajib dipilih"), unsafe_allow_html=True)
                            if m["store_id"]:
                                e[IDX_STORE_ID].markdown(komponen.html_pesan_error("Store ID wajib diisi"), unsafe_allow_html=True)

            st.markdown(f'<div class="teks-showing">Showing 1 – {len(baris_tampil)} of {len(baris_tampil)}</div>',
                        unsafe_allow_html=True)

    total      = len(semua)
    n_noo      = sum(b["jenis_outlet"] == "NOO" for b in semua)
    n_existing = sum(b["jenis_outlet"] == "Existing" for b in semua)
    n_belum    = sum(b["jenis_outlet"] not in OPSI_JENIS_SPV for b in semua)
    n_kurang_id= sum(_masalah_baris_spv(b)["store_id"] for b in semua)
    n_lengkap  = sum(not any(_masalah_baris_spv(b).values()) for b in semua)
    tempat_ringkasan.markdown(
        komponen.html_ringkasan_spv(total,n_lengkap,n_noo,n_existing,n_belum,n_kurang_id)
        + _css_status_spv(baris_tampil, ss.spv_tampil_error),
        unsafe_allow_html=True)

    n_bermasalah = sum(any(_masalah_baris_spv(b).values()) for b in baris_tampil)
    if ss.spv_tampil_error and n_bermasalah:
        st.error(f"{n_bermasalah} outlet(s) incomplete, please fill all required fields.")
    st.markdown('<hr class="pemisah-tipis">', unsafe_allow_html=True)
    c_kembali,_,c_draft,c_submit = st.columns([1.2,5.2,1.5,1.9], gap="xxsmall")
    if c_kembali.button("← Kembali", key="spv_kembali"):
        tuju_halaman("pengajuan"); st.rerun()
    if c_draft.button(":material/save: Save as Draft", key="spv_draft", disabled=not semua, width="stretch"):
        with st.spinner("Saving draft..."):
            time.sleep(0.7)
        catat_riwayat("SPV - Save as Draft", f"{len(semua)} rows")
        ss.pesan_toast = "SPV Confirmation draft saved"
        st.rerun()
    if c_submit.button(":material/send: Submit Confirmation", type="primary", key="spv_submit", disabled=not baris_tampil, width="stretch"):
        if n_bermasalah:
            ss.spv_tampil_error = True; st.rerun()
        with st.spinner("Sending confirmation..."):
            berhasil, gagal = simpan_konfirmasi_spv(baris_tampil)
            if berhasil:
                ambil_raw_noo.clear()          # paksa reload supaya Store Validation lihat data terbaru
                ss.sim_tanda = None; ss.sim_hasil = {}
        ss.spv_tampil_error = False
        n_noo_kirim = sum(b["jenis_outlet"] == "NOO" for b in baris_tampil)
        n_exi_kirim = sum(b["jenis_outlet"] == "Existing" for b in baris_tampil)
        catat_riwayat("SPV - Submit Confirmation",
                      f"{berhasil} rows (NOO {n_noo_kirim}, Existing {n_exi_kirim})"
                      + (f", failed: {len(gagal)}" if gagal else ""))
        if gagal:
            ss.pesan_toast      = f"Submitted {berhasil} rows. Failed: {', '.join(gagal)}"
            ss.pesan_toast_icon = ":material/warning:"
        else:
            ss.pesan_toast = f"SPV Confirmation submitted — {berhasil} rows"
        st.rerun()

# ═════════════════════════════════════════════════════════════════════════════
# HISTORY (NOO yang sudah disubmit, per bulan)
# ═════════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=60, show_spinner=False)
def ambil_history_noo() -> pd.DataFrame:
    import requests
    rows, start, step = [], 0, 1000
    while True:
        r = requests.get(
            f"{_shared.SUPABASE_URL}/rest/v1/{TABLE_SUBMIT_NOO}",
            headers={**_shared._SB_HEADERS, "Prefer": "", "Range-Unit": "items",
                     "Range": f"{start}-{start+step-1}"},
            params={"select": "*", "order": "created_at.desc"}, timeout=20)
        if r.status_code == 416:
            break
        r.raise_for_status()
        batch = r.json()
        rows += batch
        if len(batch) < step:
            break
        start += step
    return pd.DataFrame(rows)

def halaman_history():
    ss = st.session_state
    st.markdown(komponen.html_breadcrumb("History"), unsafe_allow_html=True)
    st.markdown(komponen.html_header("History", "NOO data that has been submitted, month by month.", ":material/history:"),
                unsafe_allow_html=True)
    try:
        df = ambil_history_noo()
    except Exception as e:
        st.error(f"Failed to load history: {e}"); return
    if df.empty:
        st.info("No submitted data yet."); return

    norm = lambda x: re.sub(r"[^a-z0-9]", "", str(x).lower())
    peta = {norm(c): c for c in df.columns}
    ambil = lambda *nama: next((peta[norm(n)] for n in nama if norm(n) in peta), None)
    k_waktu, k_kode, k_sid = ambil("created_at"), ambil("Customer Store Code"), ambil("Store ID")
    k_nama, k_city, k_tag = ambil("Store Name"), ambil("City"), ambil("tagging")
    k_distri = ambil("Group Name", "Customer Store Name")

    # akun distributor hanya melihat data distributornya sendiri
    kunci_distri = distributor_terkunci(ss.get("user"))
    if kunci_distri is not None:
        nk = lambda v: re.sub(r"\s+", " ", str(v or "")).strip().upper()
        df = df[df[k_distri].map(nk) == nk(kunci_distri)] if (k_distri and kunci_distri) else df.iloc[0:0]
        st.caption(f"Showing data for distributor: {kunci_distri or '-'}")

    out = pd.DataFrame(index=df.index)
    if k_waktu:
        waktu = pd.to_datetime(df[k_waktu], errors="coerce", utc=True, format="ISO8601").dt.tz_convert(ZONA_WAKTU)
        out["Month"] = waktu.dt.strftime("%Y-%m").fillna("-")
        out["Submitted At"] = waktu.dt.strftime("%Y-%m-%d %H:%M").fillna("-")
    else:
        out["Month"] = "-"; out["Submitted At"] = "-"
    out["Customer Store Code"] = df[k_kode].fillna("").astype(str) if k_kode else ""
    out["Store ID"]            = df[k_sid].fillna("").astype(str) if k_sid else ""
    out["Store Name"]          = df[k_nama].fillna("").astype(str) if k_nama else ""
    out["Distributor"]         = df[k_distri].fillna("").astype(str) if k_distri else ""
    out["City"]                = df[k_city].fillna("").astype(str) if k_city else ""
    out["Status"]              = df[k_tag].fillna("").astype(str) if k_tag else ""

    f1, f2, f3 = st.columns([1.2, 2.4, 0.9], vertical_alignment="bottom")
    bulan = ["All months"] + sorted([m for m in out["Month"].unique() if m != "-"], reverse=True)
    pilih_bulan = f1.selectbox("Month", bulan, key="hist_bulan")
    cari = f2.text_input("Store Code / Store ID", key="hist_cari",
                         placeholder="Type a Customer Store Code or Store ID...")
    if f3.button(":material/refresh: Refresh", key="hist_refresh", width="stretch"):
        ambil_history_noo.clear(); st.rerun()

    tampil = out
    if pilih_bulan != "All months":
        tampil = tampil[tampil["Month"] == pilih_bulan]
    if cari.strip():
        q = cari.strip().upper()
        tampil = tampil[tampil["Customer Store Code"].str.upper().str.contains(q, regex=False)
                        | tampil["Store ID"].str.upper().str.contains(q, regex=False)]

    ringkas = (out.groupby("Month").size().rename("Submitted").reset_index()
               .sort_values("Month", ascending=False))
    with st.expander(":material/calendar_month: Submissions per month", expanded=False):
        st.dataframe(ringkas, hide_index=True, width="stretch")
    st.markdown(f'<div class="teks-showing">Showing {len(tampil):,} of {len(out):,} records</div>',
                unsafe_allow_html=True)
    if tampil.empty:
        st.info("No records match the filters."); return
    st.dataframe(tampil.drop(columns=["Month"]), hide_index=True, width="stretch",
                 height=min(len(tampil) * 35 + 38, 560))

# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════
def nama_pengguna(user: dict) -> str:
    uname = str(user.get("username", "")).strip()
    if user.get("tagging") in ("RSA", "BD SUPPORT"):
        return uname
    if user.get("tagging") == "Admin Distributor":
        return f"Admin - {uname.upper()}"
    return uname or "Admin"
def main():
    st.set_page_config(page_title="NOO & SKU Submission", page_icon=":material/inventory_2:", layout="wide")
    user = require_login()
    _shared.PENGGUNA = nama_pengguna(user)
    inisialisasi_state()
    ss = st.session_state
    CSS_PATH = os.path.join(os.path.dirname(__file__), "style_app.css")
    try:
        with open(CSS_PATH, encoding="utf-8") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        st.error(f"CSS tidak ketemu di: {CSS_PATH}")
    if ss.get("pesan_toast"):
        st.toast(ss.pop("pesan_toast"), icon=ss.pop("pesan_toast_icon",":material/check_circle:"))

    with st.sidebar:
        st.markdown('<div class="brand"><span class="mi">inventory_2</span> NOO & SKU Submission</div>'
                    '<div class="brand-sub">Submit and confirm NOO data.</div>',
                    unsafe_allow_html=True)
        for label, ikon, kunci in NAVIGASI:
            nama_tombol = f"navaktif_{kunci}" if ss.halaman_aktif == kunci else f"nav_{kunci}"
            st.button(label, icon=ikon, key=nama_tombol, on_click=tuju_halaman,
                      args=(kunci,), width="stretch")
        st.markdown(f'<div class="status-update">Last updated: {ss.terakhir_update}</div>',
                    unsafe_allow_html=True)
        with st.container(key="logout"):
            if st.button(":material/logout: Log Out", width="stretch"):
                keluar()
                st.rerun()

    # Help & Support: teks biasa di kanan atas, klik -> dialog
    with st.container(key="help_kanan"):
        if st.button("Help & Support", key="btn_help_support"):
            dialog_help_support()
  # nama + peran + menu (Log Out ada di dalam menu ini)

    {"pengajuan": halaman_pengajuan_noo, "spv": halaman_konfirmasi_spv,
     "history": halaman_history}[ss.halaman_aktif]()

if __name__ == "__main__":
    main()
