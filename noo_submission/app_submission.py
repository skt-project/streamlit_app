
import os
import datetime
import re
import time

import numpy as np
import pandas as pd
import streamlit as st
from rapidfuzz import fuzz, process

import komponen
from shared import (
    # Constants
    CITY, JENIS_TOKO_DEFAULT, KOLOM_NOO_OUTPUT, KOLOM_SKU_OUTPUT, KOLOM_SKU_STAGED,
    KOLOM_SKU_TAMPIL, KOLOM_SKU_OPSIONAL, SKU_KOLOM_DB, TABLE_SUBMIT_NOO, TABLE_HISTORY_ID,
    KOLOM_HISTORY_ID_REF, ZONA_WAKTU, PENGGUNA, STATUS_REVIEW, STATUS_VALID,
    # Supabase
    sb_insert, sb_select, sb_cari_duplikat_kode, sb_cari_duplikat_sku, sb_select_batch, ambil_raw_noo,
    sb_patch_by_kode_nama,
    # GSheets
    daftar_distributor, muat_distri_code_map, muat_distri_customer_code_raw,
    muat_distri_customer_name, muat_produk_lookup, muat_spv_sales_lookup,
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

# ═════════════════════════════════════════════════════════════════════════════
# NAVIGASI
# ═════════════════════════════════════════════════════════════════════════════
NAVIGASI = [
    ("NOO & SKU Submission",    "⭐", "pengajuan"),
    ("SPV Confirmation",  "⭐", "spv"),
]

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
    ss.setdefault("pesan_toast_icon", "✅")
    # SPV
    ss.setdefault("spv_tampil_error", False)
    ss.setdefault("baris_spv", [])
    ss.setdefault("sim_tanda", None)
    ss.setdefault("sim_hasil", {})

# ═════════════════════════════════════════════════════════════════════════════
# SIMILARITY (untuk SPV Confirmation)
# ═════════════════════════════════════════════════════════════════════════════
KOLOM_MASTER  = ["Customer Store Code","Store Name","Store Address","City","Region"]
BOBOT_NAMA    = 0.4; BOBOT_ALAMAT = 0.4; BOBOT_KOTA = 0.1; BOBOT_REGION = 0.1
AMBANG_NAMA   = 80;  AMBANG_ALAMAT = 60; AMBANG_GABUNGAN = 75
AMBANG_NAMA_SAJA = 90; AMBANG_WILAYAH = 90
KATA_UMUM_NAMA   = {"TOKO","KIOS","KIOSK","UD","CV","PT","WARUNG"}
SINGKATAN_ALAMAT = {"JL":"JALAN","JLN":"JALAN","GG":"GANG"}
KATA_BUANG_ALAMAT  = {"NO","NOMOR"}
KATA_BUANG_WILAYAH = {"KOTA","KAB","KABUPATEN"}

def _bersihkan(teks) -> str:
    s = re.sub(r"[^A-Z0-9 ]"," ", str(teks or "").upper())
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

REGION_DUMMY = {
    "KOLAKA":"SULAWESI TENGGARA","KENDARI":"SULAWESI TENGGARA",
    "MAKASSAR":"SULAWESI SELATAN","PALU":"SULAWESI TENGAH",
    "MANADO":"SULAWESI UTARA","GORONTALO":"GORONTALO",
}

@st.cache_data(ttl=600, show_spinner=False)
def ambil_master_toko() -> pd.DataFrame:
    rng   = np.random.default_rng(7)
    tetap = [
        ("JUM KOSMETIK","KOLAKA","KOLAKA"),("TOKO INTAN BIRU","JL. POROS KOLAKA NO. 2","KOLAKA"),
        ("MANDIRI MRT","JALAN PELITA 5","KOLAKA"),("BINTANG JAYA","JLN MERDEKA NO 8 RT 02","KOLAKA"),
        ("APOTEK SEHAT","JL. SUDIRMAN NO. 12","KOLAKA"),("RIZKY MART","JL. VETERAN NO. 17","KENDARI"),
    ]
    kata  = ["SARI","INDAH","MULIA","SEHAT","CANTIK","SEJAHTERA","TIRTA","CAHAYA","MAKMUR",
             "ABADI","SENTOSA","LESTARI","UTAMA","PRIMA","BERKAH","RAHAYU","SUBUR","AGUNG",
             "HARAPAN","BUNDA","ANUGERAH","MEKAR","KARYA","NUSA","MITRA","GEMILANG","FAJAR"]
    jalan = ["SUDIRMAN","PELITA","MERDEKA","KARTINI","VETERAN","DIPONEGORO","AHMAD YANI",
             "PAHLAWAN","MAWAR","ANGGREK","POROS","PASAR BARU"]
    kota_list = list(REGION_DUMMY)
    baris = list(tetap)
    for _ in range(300):
        nm  = f"{rng.choice(kata)} {rng.choice(kata)}"
        al  = f"JL. {rng.choice(jalan)} NO. {int(rng.integers(1,120))}"
        baris.append((nm, al, str(rng.choice(kota_list))))
    return pd.DataFrame({
        "Customer Store Code": [f"CSC-{i:05d}" for i in range(1, len(baris)+1)],
        "Store Name":  [b[0] for b in baris],
        "Store Address":[b[1] for b in baris],
        "City":        [b[2] for b in baris],
        "Region":      [REGION_DUMMY.get(b[2],"") for b in baris],
    })[KOLOM_MASTER]

PAKAI_MASTER_DUMMY = False

def _normalkan_master(m: pd.DataFrame) -> pd.DataFrame:
    m = m.copy()
    m["_nama"]   = m["Store Name"].map(normalisasi_nama)
    m["_alamat"] = [normalisasi_alamat(a,k) for a,k in zip(m["Store Address"], m["City"])]
    m["_kota"]   = m["City"].map(normalisasi_wilayah)
    m["_region"] = m["Region"].map(normalisasi_wilayah)
    return m

@st.cache_data(ttl=600, show_spinner=False)
def _master_ternormalisasi() -> pd.DataFrame:
    return _normalkan_master(ambil_master_toko())

def _hitung_skor(nama, alamat, kota, master, region_noo=()):
    n = len(master)
    nama_n   = normalisasi_nama(nama)
    alamat_n = normalisasi_alamat(alamat, kota)
    kota_n   = normalisasi_wilayah(kota)
    region_n = [r for r in (normalisasi_wilayah(x) for x in region_noo) if r]
    skor_nama   = process.cdist([nama_n], master["_nama"].tolist(), scorer=fuzz.token_sort_ratio, workers=1)[0]
    ada_alamat  = bool(alamat_n) & master["_alamat"].ne("").to_numpy()
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

def cari_kemiripan(nama, alamat, kota, master, region_noo=()):
    if master is None or master.empty or not normalisasi_nama(nama):
        return None
    h = _hitung_skor(nama, alamat, kota, master, region_noo)
    if not h["lolos"].any():
        return None
    terbaik = int(np.argmax(np.where(h["lolos"], h["gabungan"], -1.0)))
    baris   = master.iloc[terbaik]
    return str(baris["Customer Store Code"]), str(baris["Store Name"])

def terapkan_similarity(baris_spv, region_noo=()):
    try:
        master = _master_ternormalisasi()
    except Exception as e:
        st.warning(f"Gagal memuat master toko: {e}")
        master = None
    for b in baris_spv:
        hasil = (cari_kemiripan(b["nama"], b["alamat"], b.get("kota",""), master, region_noo)
                 if master is not None else None)
        b["similarity_kode"], b["similarity_nama"] = hasil if hasil else ("","")

def isi_similarity_spv(baris, region_noo):
    ss    = st.session_state
    tanda = (tuple((b["id"],b["nama"],b["alamat"],b["kota"]) for b in baris), tuple(region_noo))
    if ss.get("sim_tanda") != tanda:
        terapkan_similarity(baris, region_noo)
        ss.sim_tanda = tanda
        ss.sim_hasil = {b["id"]:(b["similarity_kode"],b["similarity_nama"]) for b in baris}
    else:
        for b in baris:
            b["similarity_kode"], b["similarity_nama"] = ss.sim_hasil.get(b["id"],("",""))

def _teks(v) -> str:
    return "" if pd.isna(v) else str(v).strip()

def bangun_baris_spv(distributor: str = "") -> list:
    rows = ambil_raw_noo()
    if not rows:
        return []
    norm  = lambda s: re.sub(r"[^a-z0-9]","",str(s).lower())
    hasil = []
    for i, row in enumerate(rows, 1):
        peta = {norm(k):v for k,v in row.items()}
        hasil.append({
            "id":               i,
            "no":               i,
            "store_code":       peta.get("customerstorecode","") or "",
            "distributor":      peta.get("customerstorename","") or "",
            "nama":             peta.get("storename","") or "",
            "alamat":           peta.get("storeaddress","") or "",
            "kota":             peta.get("city","") or "",
            "jenis_outlet":     None,
            "store_id_validasi":"",
            "keterangan":       "",
            "similarity_kode":  "",
            "similarity_nama":  "",
            "tagging":          peta.get("tagging","") or "",   # ← tambah ini
        })
    return hasil

# ═════════════════════════════════════════════════════════════════════════════
# MANUAL SKU MAPPING
# ═════════════════════════════════════════════════════════════════════════════

def _sku_peringatan(teks: str):
    ss = st.session_state
    ss.sku_msg        = ("warning", teks)
    ss.pesan_toast    = teks
    ss.pesan_toast_icon = "⚠️"

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

    with st.expander("📋 Manual Input (Row by Row)",
                     expanded=bool(ss.sku_staged) or ss.sku_last_batch is not None):
        if ss.sku_last_batch is not None:
            st.success(f"✅ {ss.sku_last_n} rows saved to sku_mapping")
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
            st.button("➕ Add Row", key="sku_tambah", width="stretch",
                      on_click=_sku_tambah_baris, args=(map_raw,map_produk))

        pesan = ss.pop("sku_msg",None)
        if pesan:
            {"warning":st.warning,"error":st.error}.get(pesan[0],st.info)(pesan[1])

        if not ss.sku_staged:
            st.info("No staged rows yet."); return

        st.caption(f"Step 2 — Review staged rows ({len(ss.sku_staged)}). Edit or tick ✅ to delete.")
        df_base = pd.DataFrame(ss.sku_staged, columns=KOLOM_SKU_STAGED)
        kolom_teks = [c for c in KOLOM_SKU_STAGED if c != "Pilih"]
        df_base[kolom_teks] = df_base[kolom_teks].fillna("").astype(str)
        df_base["Pilih"]    = df_base["Pilih"].fillna(False).astype(bool)
        hasil = st.data_editor(
            df_base, key=f"sku_editor_{ss.sku_editor_key}",
            column_order=KOLOM_SKU_TAMPIL,
            column_config={
                "Pilih":                      st.column_config.CheckboxColumn("✅",width=50,default=False),
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
        if c_hapus.button("🗑️ Delete Selected", key="sku_hapus", width="stretch"):
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

        if st.button("📨 Save All to Supabase", type="primary", width="stretch",
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
                ss.pesan_toast = "Duplicate found. Nothing saved."; ss.pesan_toast_icon = "⚠️"
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

# ═════════════════════════════════════════════════════════════════════════════
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
    st.markdown(komponen.html_header("NOO & SKU Submission","Upload Failure List file to process data.","📦"),
                unsafe_allow_html=True)

    with st.container(key="kotak_unggah"):
        daftar_berkas = st.file_uploader(
            "Upload Failure List (.xlsx / .xls, max 10 MB per file)",
            type=["xlsx","xls"], key="unggah_pengajuan", accept_multiple_files=True)

    tanda_file = tuple((b.name, b.size) for b in (daftar_berkas or []))
    if ss.get("tanda_file_upload") != tanda_file:
        for k in ["sku_staged", "sku_last_batch", "sku_last_n", "sku_msg",
                  "sku_fix_on", "noo_batch_terkirim"]:
            ss.pop(k, None)
        ss.sku_editor_key = ss.get("sku_editor_key", 0) + 1
        ss.tanda_file_upload = tanda_file

    file_siap  = False
    data_sku   = pd.DataFrame(columns=KOLOM_SKU_OUTPUT)
    data_noo   = pd.DataFrame(columns=KOLOM_NOO_OUTPUT)

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
                file_siap = True
            except Exception as e:
                st.markdown(komponen.html_error_unggah(f"Failed to read file: {e}"), unsafe_allow_html=True)
    if not daftar_berkas:
        ss.sku_fix_on = False
    salah_cc = cc_salah(data_sku)
    st.markdown('<hr class="pemisah-tipis">', unsafe_allow_html=True)

    # ── SKU MAPPING ──────────────────────────────────────────────────────────
    with st.container(key="kartu_sku"):
        st.markdown(komponen.html_header_pengajuan("📦","SKU Mapping","Pivot based on Customer SKU data."),
                    unsafe_allow_html=True)
        st.markdown(komponen.html_judul_preview(len(data_sku) if file_siap else None), unsafe_allow_html=True)
        st.markdown(judul_opsi(1,"Manual","select Distributor & fill Product, Customer Product Code"),
                    unsafe_allow_html=True)
        render_manual_sku()
        st.markdown('<hr class="pemisah-opsi">', unsafe_allow_html=True)
        st.markdown(judul_opsi(2,"Upload Option","extracted from Failure List file"),
                    unsafe_allow_html=True)
        if file_siap and len(data_sku):
            st.dataframe(data_sku.head(5), hide_index=True, width="stretch")
            if salah_cc:
                contoh = ", ".join(f"{p}: {cc}→{b}…" for _, p, cc, b in salah_cc[:5])
                st.error(f"❌ {len(salah_cc)} row(s) have an invalid Customer Code prefix "
                         f"(G2G- products must use 12, all others must use 17). Examples: {contoh}. "
                         f"Submit is locked until this is fixed.")
                st.button("🔧 Fix Customer Code", key="sku_fix", on_click=_aktifkan_fix_cc)
            elif ss.get("sku_fix_on"):
                st.success("✅ Customer Code has been corrected to match the product brand.")
        else:
            st.markdown('<div class="preview-kosong">📄 No data yet, please upload a file first</div>',
                        unsafe_allow_html=True)

    c_unduh_sku, c_submit_sku = st.columns([1,2])
    c_unduh_sku.download_button("⬇️ Download Result", hasil_ke_excel(data_sku), "hasil_sku_mapping.xlsx",
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                disabled=not file_siap, width="stretch", key="unduh_sku")
    if c_submit_sku.button("📨 Submit SKU Mapping", disabled=not file_siap or bool(salah_cc),
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
            ss.pesan_toast_icon = "⚠️"
            st.rerun()
        seen, baru = set(), []
        for r in rows:
            k = _sku_kunci(r["Customer Code"], r["Product"])
            if k in dup_db or k in seen:
                continue
            seen.add(k); baru.append(r)
        if not baru:
            ss.pesan_toast = "All rows already exist in sku_mapping. Nothing saved."
            ss.pesan_toast_icon = "⚠️"
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
            st.rerun()
    st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)

    # ── NOO ──────────────────────────────────────────────────────────────────
    with st.container(key="kartu_noo"):
        lbl = "\u200b"
        st.markdown(komponen.html_header_pengajuan("🏪","NOO","Pivot based on outlet data."),
                    unsafe_allow_html=True)
        st.markdown(komponen.html_judul_preview(len(data_noo) if file_siap else None), unsafe_allow_html=True)

        # Opsi 1: Manual bulk paste
        st.markdown(judul_opsi(1,"Manual","paste Customer Store Code & Store Name"), unsafe_allow_html=True)
        with st.expander("📋 Manual Input (Paste Bulk)", expanded=bool(ss.get("bulk_data"))):
            st.caption("Step 1 — Paste Customer Store Code and Store Name (one per line)")
            c_bulk_kode, c_bulk_nama = st.columns(2)
            bulk_kode = c_bulk_kode.text_area("Customer Store Code",height=120,key="bulk_kode_noo",
                                               placeholder="DST001\nDST002\n...")
            bulk_nama = c_bulk_nama.text_area("Store Name",height=120,key="bulk_nama_noo",
                                               placeholder="STORE A\nSTORE B\n...")
            if st.button("Apply Paste", key="terapkan_bulk"):
                baris_kode = [k.strip() for k in bulk_kode.splitlines() if k.strip()]
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
                    kode_toko   = str(baris["Customer Store Code"]).strip()
                    branch_code = kode_toko[:6]
                    info        = distri_map.get(branch_code.upper())
                    distri_nama = info["distributor"] if info else ""
                    company_code = info["company_code"] if info else ""
                    if not info:
                        tidak_ketemu.append(kode_toko)
                    c1.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{kode_toko}</div>', unsafe_allow_html=True)
                    c2.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{baris["Store Name"]}</div>', unsafe_allow_html=True)
                    c3.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{distri_nama or "⚠️ not found"}</div>', unsafe_allow_html=True)
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

                if tidak_ketemu:
                    st.warning("Distributor code not found for: " + ", ".join(tidak_ketemu))

                df_bulk = pd.DataFrame(ss.bulk_data)
                for k in KOLOM_NOO_OUTPUT:
                    if k not in df_bulk.columns:
                        df_bulk[k] = ""
                df_bulk = df_bulk[KOLOM_NOO_OUTPUT]

                if st.button("📨 Submit Manual NOO", disabled=not bulk_semua_terisi,
                             type="primary", width="stretch", key="submit_bulk_noo"):
                    kode_list  = df_bulk["Customer Store Code"].astype(str).str.strip().tolist()
                    tanda_batch = ("bulk",) + tuple(sorted(kode_list))
                    if ss.get("noo_batch_terkirim") == tanda_batch:
                        set_keterangan_noo("warning","⚠️ Data already submitted.")
                    else:
                        with st.spinner("Checking duplicates..."):
                            dup_in, g_in   = sb_cari_duplikat_kode(TABLE_SUBMIT_NOO, kode_list,"Customer Store Code")
                            dup_hi, g_hi   = sb_cari_duplikat_kode(TABLE_HISTORY_ID, kode_list, KOLOM_HISTORY_ID_REF)
                            duplikat = dup_in | dup_hi
                            gagal    = g_in  | g_hi
                        if gagal:
                            set_keterangan_noo("error","❌ Submission cancelled — failed to check: " + ", ".join(sorted(gagal)))
                        else:
                            mask_dup   = df_bulk["Customer Store Code"].astype(str).str.strip().isin(duplikat)
                            data_bersih   = df_bulk[~mask_dup]
                            data_dilewati = df_bulk[mask_dup]
                            if data_bersih.empty:
                                set_keterangan_noo("error","❌ All codes already exist: " + ", ".join(sorted(duplikat)))
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
            st.markdown('<div class="preview-kosong">📄 No data yet, please upload a file first</div>',
                        unsafe_allow_html=True)
        else:
            st.markdown(komponen.html_catatan_noo(), unsafe_allow_html=True)
            LEBAR_NOO = [1.4,1.8,1.8,1.2,1.8,1.5,1.5,1.4]
            tabel_noo = st.container(key="tabel_noo_scroll")
            tabel_noo.__enter__()
            header = st.columns(LEBAR_NOO)
            for kolom, (teks, wajib) in zip(header,[
                ("Customer Store Code",True),("Store Name",False),("Customer Store Name",False),
                ("City",True),("Store Address",True),("SPV",True),("Sales Name",True),("Store Type",True)]):
                tanda = ' <b style="color:#ef4f58">*</b>' if wajib else ""
                kolom.markdown(f"<b>{teks}</b>{tanda}", unsafe_allow_html=True)
            spv_sales_lookup = muat_spv_sales_lookup()
            semua_terisi = True
            data_noo = data_noo.reset_index(drop=True)
            for _c in ["Customer Store Code","City","Store Address","SE/KAE","SPV","Store Type"]:
                if _c not in data_noo.columns:
                    data_noo[_c] = ""
                data_noo[_c] = data_noo[_c].astype("object")
            for i, baris in data_noo.iterrows():
                csn = str(baris.get("Customer Store Name",baris.get("Store Name",""))).strip().upper()
                c_kode,c_nama,c_csn,c_city,c_alamat,c_sales,c_spv,c_tipe = st.columns(LEBAR_NOO)
                kode_val = str(baris["Customer Store Code"])
                c_kode.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{kode_val}</div>', unsafe_allow_html=True)
                c_nama.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{baris["Store Name"]}</div>', unsafe_allow_html=True)
                csn_val = str(baris.get("Customer Store Name","")).strip()
                c_csn.markdown(f'<div class="catatan-kecil" style="margin-top:8px">{csn_val or "-"}</div>', unsafe_allow_html=True)
                city_noo = c_city.selectbox(lbl,[""] + CITY,label_visibility="collapsed",key=f"noo_city_{i}")
                alamat_raw = str(baris.get("Store Address","") or "")
                alamat_val = "" if alamat_raw.strip() in ("nan","None","","NaN") else alamat_raw.strip()
                alamat_noo = c_alamat.text_input(lbl,value=alamat_val,label_visibility="collapsed",
                                                  key=f"noo_alamat_{i}",placeholder="Store address...")
                spv_noo, sales_noo = input_spv_sales(csn, f"noo_{i}", c_spv, c_sales, spv_sales_lookup)
                tipe_noo = c_tipe.selectbox(lbl,[""] + JENIS_TOKO_DEFAULT,label_visibility="collapsed",key=f"noo_tipe_{i}")
                data_noo.loc[i,"Customer Store Code"] = kode_val
                data_noo.loc[i,"City"]       = city_noo
                data_noo.loc[i,"Store Address"] = alamat_noo
                data_noo.loc[i,"SE/KAE"]     = sales_noo
                data_noo.loc[i,"SPV"]        = spv_noo
                data_noo.loc[i,"Store Type"] = tipe_noo
                if not (kode_val.strip() and city_noo.strip() and tipe_noo and spv_noo.strip() and sales_noo.strip()):
                    semua_terisi = False
            tabel_noo.__exit__(None, None, None)
            noo_siap = semua_terisi

        if st.button("📨 Submit NOO", disabled=not (file_siap and noo_siap), type="primary",
                     width="stretch", key="submit_noo"):
            kode_list   = data_noo["Customer Store Code"].astype(str).str.strip().tolist()
            tanda_batch = tuple(sorted(kode_list))
            if ss.get("noo_batch_terkirim") == tanda_batch:
                set_keterangan_noo("warning","⚠️ Already submitted. Modify data or upload new file.")
            else:
                with st.spinner("Checking duplicates..."):
                    dup_in, g_in = sb_cari_duplikat_kode(TABLE_SUBMIT_NOO, kode_list,"Customer Store Code")
                    dup_hi, g_hi = sb_cari_duplikat_kode(TABLE_HISTORY_ID, kode_list, KOLOM_HISTORY_ID_REF)
                    duplikat = dup_in | dup_hi
                    gagal    = g_in  | g_hi
                if gagal:
                    set_keterangan_noo("error","❌ Cancelled — failed to verify: " + ", ".join(sorted(gagal)))
                else:
                    mask_dup      = data_noo["Customer Store Code"].astype(str).str.strip().isin(duplikat)
                    data_bersih   = data_noo[~mask_dup]
                    data_dilewati = data_noo[mask_dup]
                    if data_bersih.empty:
                        set_keterangan_noo("error","❌ All codes already exist: " + ", ".join(sorted(duplikat)))
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
LEBAR_KOLOM_SPV = [0.5,1.8,2.0,2.2,2.8,2.0,1.8,1.8,2.0,2.2]
IDX_JENIS, IDX_STORE_ID = 7, 8

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

def _tagging_dari_jenis(jenis: str) -> str:
    """Pemetaan hasil SPV Confirmation -> kolom 'tagging' di Supabase.
    'NOO'      -> STATUS_VALID  (Store Validation akan men-generate Store ID baru)
    'Existing' -> tetap 'Existing' (Store ID sudah final, diisi manual oleh SPV)
    """
    return STATUS_VALID if jenis == "NOO" else jenis

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
        nama = str(b["distributor"]).strip()   # berisi Customer Store Name
        if not kode or not nama:
            gagal.append(f"baris #{b['no']} (kode/nama kosong)")
            continue
        patch = {"tagging": _tagging_dari_jenis(b["jenis_outlet"])}
        if b["jenis_outlet"] == "Existing":
            patch["Store ID"]     = str(b["store_id_validasi"]).strip()
            patch["generated_at"] = None
        if sb_patch_by_kode_nama(TABLE_SUBMIT_NOO, kode, nama, patch):
            berhasil += 1
        else:
            gagal.append(f"{kode} / {nama}")
    return berhasil, gagal

def halaman_konfirmasi_spv():
    ss = st.session_state
    ss.setdefault("spv_tampil_error", False)
    st.markdown(komponen.html_breadcrumb("SPV Confirmation"), unsafe_allow_html=True)
    st.markdown(komponen.html_header("SPV Confirmation",
                                      "Confirm outlet type and validate data before submission.","🛡️"),
                unsafe_allow_html=True)

    c_distributor, c_region, c_refresh_spv, _ = st.columns([1.6,1.4,0.8,1.2])
    with c_distributor:
        distributor = st.selectbox("Distributor *",[""] + daftar_distributor(), key="distributor_spv")
        region_noo  = region_distributor(distributor)
    with c_region:
        st.markdown('<div style="height:1.7rem"></div>', unsafe_allow_html=True)
        st.markdown(komponen.html_info_region(region_noo), unsafe_allow_html=True)
    with c_refresh_spv:
        st.markdown('<div style="height:1.7rem"></div>', unsafe_allow_html=True)
        if st.button("🔄 Refresh", key="refresh_spv", width="stretch"):
            ambil_raw_noo.clear()
            ss.sim_tanda = None; ss.sim_hasil = {}
            ss.pesan_toast = "Data refreshed"
            st.rerun()

    semua = bangun_baris_spv(distributor)
    if distributor:
        _nk = lambda s: re.sub(r"\s+", " ", str(s or "")).strip().upper()
        semua = [b for b in semua if _nk(b["distributor"]) == _nk(distributor)]
    for n, b in enumerate(semua, 1):
        b["no"] = n                      # nomor urut ulang; "id" tetap (key widget stabil)
    isi_similarity_spv(semua, region_noo)
    ss.baris_spv = semua
    baris_tampil = [b for b in semua if b.get("tagging","") == STATUS_REVIEW]
    tempat_ringkasan = st.empty()
    
    n_sudah = len(semua) - len(baris_tampil)
    if n_sudah and baris_tampil and distributor:
        st.success(f"✅ {n_sudah} outlet(s) already confirmed, {len(baris_tampil)} remaining.")

    with st.container(key="kartu_spv"):
        st.markdown(komponen.html_catatan_spv(), unsafe_allow_html=True)
        if not baris_tampil:
            st.info(f"No NOO outlet data for {distributor}." if distributor
                    else "No NOO outlet data to display.")
        else:
          with st.container(key="tabel_spv_scroll"):
            kepala = st.columns(LEBAR_KOLOM_SPV)
            for kolom, (teks, wajib) in zip(kepala,[
                ("No.",False),("Customer Store Code",False),("Distributor",False),
                ("Store Name",False),("Store Address",False),("Similarity (Name)",False),
                ("Similarity (Code)",False),("NOO / Existing",True),
                ("Store ID (Validation)",False),("Remarks",False)]):
                kolom.markdown(komponen.html_kepala_kolom(teks, wajib), unsafe_allow_html=True)

            for b in baris_tampil:
                k = _kid(b)
                kunci_jenis, kunci_sid, kunci_ket = f"spv_jenis_{k}",f"spv_sid_{k}",f"spv_ket_{k}"
                ss.setdefault(kunci_jenis, b["jenis_outlet"])
                ss.setdefault(kunci_sid, b["store_id_validasi"])
                ss.setdefault(kunci_ket, b["keterangan"])
                with st.container(key=f"baris_spv_{k}"):
                    c_no,c_kode,c_distri,c_nama,c_alamat,c_sim_nama,c_sim_kode,c_jenis,c_sid,c_ket = \
                        st.columns(LEBAR_KOLOM_SPV)
                    c_no.markdown(komponen.html_sel_teks(b["no"]), unsafe_allow_html=True)
                    c_kode.markdown(komponen.html_sel_teks(b["store_code"],"sel-baca"), unsafe_allow_html=True)
                    c_distri.markdown(komponen.html_sel_teks(b["distributor"]), unsafe_allow_html=True)
                    c_nama.markdown(komponen.html_sel_teks(b["nama"],"sel-tebal"), unsafe_allow_html=True)
                    c_alamat.markdown(komponen.html_sel_teks(b["alamat"],"sel-baca"), unsafe_allow_html=True)
                    c_sim_nama.markdown(komponen.html_sel_teks(b.get("similarity_nama",""),"sel-baca"), unsafe_allow_html=True)
                    c_sim_kode.markdown(komponen.html_sel_teks(b.get("similarity_kode",""),"sel-baca"), unsafe_allow_html=True)
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
    c_kembali,_,c_draft,c_submit = st.columns([1.2,3.6,1.4,1.8])
    if c_kembali.button("← Kembali", key="spv_kembali"):
        tuju_halaman("pengajuan"); st.rerun()
    if c_draft.button("💾 Save as Draft", key="spv_draft", disabled=not semua):
        with st.spinner("Saving draft..."):
            time.sleep(0.7)
        catat_riwayat("SPV - Save as Draft", f"{len(semua)} rows")
        ss.pesan_toast = "SPV Confirmation draft saved"
        st.rerun()
    if c_submit.button("📨 Submit Confirmation", type="primary", key="spv_submit", disabled=not baris_tampil):
        if n_bermasalah:
            ss.spv_tampil_error = True; st.rerun()
        with st.spinner("Sending confirmation..."):
            berhasil, gagal = simpan_konfirmasi_spv(baris_tampil)
            if berhasil:
                ambil_raw_noo.clear()          # paksa reload supaya Store Validation lihat data terbaru
                ss.sim_tanda = None; ss.sim_hasil = {}
        ss.spv_tampil_error = False
        catat_riwayat("SPV - Submit Confirmation",
                      f"{berhasil} rows" + (f" (failed: {len(gagal)})" if gagal else ""))
        if gagal:
            ss.pesan_toast      = f"Submitted {berhasil} rows. Failed: {', '.join(gagal)}"
            ss.pesan_toast_icon = "⚠️"
        else:
            ss.pesan_toast = f"SPV Confirmation submitted — {berhasil} rows"
        st.rerun()

# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════
def main():
    st.set_page_config(page_title="NOO & SKU Submission", page_icon="📦", layout="wide")
    inisialisasi_state()
    ss = st.session_state
    CSS_PATH = os.path.join(os.path.dirname(__file__), "style_app.css")
    try:
        with open(CSS_PATH, encoding="utf-8") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        st.error(f"CSS tidak ketemu di: {CSS_PATH}")
    if ss.get("pesan_toast"):
        st.toast(ss.pop("pesan_toast"), icon=ss.pop("pesan_toast_icon","✅"))

    with st.sidebar:
        st.markdown('<div class="brand">📦 NOO & SKU Submission</div>'
                    '<div class="brand-sub">Submit and confirm NOO data.</div>',
                    unsafe_allow_html=True)
        for label, ikon, kunci in NAVIGASI:
            nama_tombol = f"navaktif_{kunci}" if ss.halaman_aktif == kunci else f"nav_{kunci}"
            st.button(f"{ikon}   {label}", key=nama_tombol, on_click=tuju_halaman,
                      args=(kunci,), width="stretch")
        st.markdown(f'<div class="status-update">Last updated: {ss.terakhir_update}</div>',
                    unsafe_allow_html=True)

    {"pengajuan": halaman_pengajuan_noo, "spv": halaman_konfirmasi_spv}[ss.halaman_aktif]()

if __name__ == "__main__":
    main()
