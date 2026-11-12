import datetime
import json
import math
import re
import os
import numpy as np
import pandas as pd
import bcrypt
import requests
import streamlit.noo_submission.shared as _shared
import streamlit as st
import komponen
from streamlit.noo_submission.noo_grid import noo_grid
from streamlit.noo_submission.shared import (
    # Constants
    KOLOM, KOLOM_EDIT, KOLOM_TAMPIL, WAJIB_ISI, MAPPING_SUPABASE_KE_VALIDASI,
    KOLOM_VALIDASI, STATUS_VALID, STATUS_REVIEW, WARNA_VALIDASI, warnai_validasi,
    TABLE_SUBMIT_NOO, TABLE_RIWAYAT, ZONA_WAKTU, PENGGUNA, SUPABASE_URL, _SB_HEADERS,
    PANJANG_PREFIX, PANJANG_ANGKA, JENIS_TOKO_DEFAULT, CITY,
    KOLOM_NOO_OUTPUT,
    # Supabase
    sb_insert, sb_select, sb_patch_by_kode_nama, ambil_raw_noo, sb_delete,
    # GSheets
    daftar_distributor, muat_distributor_dari_sheet, muat_prefix_per_distributor, muat_distri_code_map,
    max_angka_per_prefix, resolve_distributor, sb_max_angka_store_id,
    # Utils
    ke_wib_naive, waktu_sekarang, hitung_validasi, hasil_ke_excel, catat_riwayat, swr_cache,
)

KOLOM_ORDER_EDITOR =["#","Pilih","Validasi","Store ID"] + list(KOLOM[2:])

NAVIGASI = [
    ("Dashboard",          ":material/trending_up:", "dashboard"),
    ("Store Validation",   ":material/description:", "validasi"),
    ("Submission History", ":material/history:",     "riwayat"),
    ("Settings",           ":material/settings:",    "pengaturan"),
   # ("Input NOO", "noo"),
]

#------------------USERNAME SETTINGS-------------------------------------------
def nama_pengguna() -> str:
    user = st.session_state.get("user") or {}
    uname = str(user.get("username", "")).strip()
    if user.get("tagging") in ("RSA", "BD_SUPPORT"):
        return uname
    # username distributor = company code
    return f"Admin - {uname.upper()}" if uname else "Admin"
# GENERATOR ID
def resequence_prefix(df: pd.DataFrame, prefix: str) -> list:
    """Susun ulang nomor Store ID utk 1 prefix biar rapat tanpa gap.
    Urutan berdasar Generated At (yang paling lama dapat nomor terkecil).
    Return list index yang ID-nya berubah (perlu ditandai dirty)."""
    if not prefix:
        return []
    floor = max(
        max_angka_per_prefix().get(prefix, 0),
        sb_max_angka_store_id().get(prefix, 0),
    )
    mask = (
        df["Validasi"].eq(STATUS_VALID) &
        df["Store ID"].fillna("").astype(str).str.strip().str.startswith(prefix) &
        ~df["Store ID"].fillna("").astype(str).str.strip().isin(["", "-"])
    )
    aktif = df[mask].copy()
    if aktif.empty:
        return []
    aktif["_urut"] = pd.to_datetime(aktif["Generated At"], errors="coerce")
    aktif = aktif.sort_values("_urut", kind="stable")

    berubah = []
    for n, idx in enumerate(aktif.index, start=1):
        id_baru = f"{prefix}{floor+n:0{PANJANG_ANGKA}d}"
        if str(df.loc[idx, "Store ID"]).strip() != id_baru:
            df.loc[idx, "Store ID"] = id_baru
            berubah.append(idx)
    return berubah

def generate_store_ids(df: pd.DataFrame, semua: pd.DataFrame = None, kolom_target: str = "Store ID") -> pd.Series:
    prefix_per_distri = muat_prefix_per_distributor()
    counter = dict(max_angka_per_prefix())
    for p, a in sb_max_angka_store_id().items():
        counter[p] = max(counter.get(p, 0), a)
    if semua is not None:
        for sid in semua[kolom_target].fillna("").astype(str).str.strip():
            p, a = sid[:PANJANG_PREFIX], sid[PANJANG_PREFIX:]
            if a.isdigit():
                counter[p] = max(counter.get(p, 0), int(a))
    hasil = []
    for _, row in df.iterrows():
        distri = str(row.get("Distributor","")).strip()
        prefix = prefix_per_distri.get(distri,"")
        if not prefix:
            hasil.append("-"); continue
        angka_baru = counter.get(prefix,0) + 1
        counter[prefix] = angka_baru
        hasil.append(f"{prefix}{angka_baru:0{PANJANG_ANGKA}d}")
    return pd.Series(hasil, index=df.index)

# ═════════════════════════════════════════════════════════════════════════════
# LOAD DATA DARI SUPABASE → df_toko
# ═════════════════════════════════════════════════════════════════════════════

def muat_data_noo_dari_supabase() -> pd.DataFrame:
    rows = ambil_raw_noo()
    if not rows:
        return pd.DataFrame(columns=KOLOM_TAMPIL)
    df_raw = pd.DataFrame(rows)
    norm   = lambda s: re.sub(r"[^a-z0-9]","",str(s).lower())
    peta   = {norm(c):c for c in df_raw.columns}
    df     = pd.DataFrame(index=df_raw.index)
    for sb_col, val_col in MAPPING_SUPABASE_KE_VALIDASI.items():
        asli = peta.get(norm(sb_col))
        df[val_col] = df_raw[asli] if asli else ""
    df["Longitude"]  = pd.to_numeric(df["Longitude"],  errors="coerce")
    df["Latitude"]   = pd.to_numeric(df["Latitude"],   errors="coerce")
    df["Created At"] = ke_wib_naive(df["Created At"])
    df["Generated At"] = ke_wib_naive(df["Generated At"])
    df["_sb_id"]     = ""
    df = df.reset_index(drop=True)
    df.insert(0,"#", np.arange(1, len(df)+1))

    kolom_tagging_asli = peta.get("tagging")
    tagging_raw = (df_raw[kolom_tagging_asli].astype(str).str.strip()
                   if kolom_tagging_asli else pd.Series("",index=df_raw.index)).reset_index(drop=True)
    valid_tagging = tagging_raw.isin(KOLOM_VALIDASI)
    # tagging kosong/tidak dikenal -> Need Review (Generate ID hanya lewat aksi user, tidak otomatis)
    df["Validasi"] = np.where(valid_tagging, tagging_raw, STATUS_REVIEW)
    # distributor = Group Name (Customer Branch Name); baris lama tanpa Group Name memakai Customer Store Name
    kolom_grup = peta.get(norm("Group Name"))
    grup = (df_raw[kolom_grup].fillna("").astype(str).str.strip().tolist()
            if kolom_grup else [""] * len(df))
    kode_distri = muat_distri_code_map()      # 6 karakter awal Customer Store Code -> distributor (Distributor Mapping kol. H)
    def _cari_distri(g, csn, kota, kode):
        # 1) Group Name, 2) Customer Store Name (data lama), 3) prefix 6 karakter Customer Store Code
        #    (nama distributor di file bisa berubah, kode toko tetap)
        for kandidat in (g, csn):
            hasil = resolve_distributor(kandidat, kota) if str(kandidat).strip() else ""
            if hasil:
                return hasil
        info = kode_distri.get(str(kode).strip()[:6].upper())
        return info["distributor"].strip() if info else ""
    df["Distributor"] = [_cari_distri(g, csn, kota, kode)
                         for g, csn, kota, kode in zip(grup, df["Customer Store Name"], df["City"],
                                                       df["Customer Store Code"])]
    df = df[KOLOM_TAMPIL]
    df.index = pd.Index(df["#"].to_numpy(), name="_id")
    return df

def bangun_download_noo(df_view: pd.DataFrame) -> pd.DataFrame:
    rows = ambil_raw_noo()
    if not rows or df_view.empty:
        return pd.DataFrame(columns=KOLOM_NOO_OUTPUT)
    norm   = lambda s: re.sub(r"[^a-z0-9]","",str(s).lower())
    bersih = lambda v: str(v).strip()
    raw    = pd.DataFrame(rows)
    peta   = {norm(c):c for c in raw.columns}
    kolom_kode = peta.get(norm("Customer Store Code"))
    kolom_nama = peta.get(norm("Customer Store Name"))
    if not kolom_kode or not kolom_nama:
        return pd.DataFrame(columns=KOLOM_NOO_OUTPUT)
    raw["_kunci"] = raw[kolom_kode].map(bersih) + "||" + raw[kolom_nama].map(bersih)
    store_id_app = {
        bersih(r["Customer Store Code"]) + "||" + bersih(r["Customer Store Name"]): bersih(r["Store ID"])
        for _, r in df_view.iterrows()
        if bersih(r["Customer Store Code"]) and bersih(r["Customer Store Name"])
    }
    raw  = raw[raw["_kunci"].isin(store_id_app)]
    out  = pd.DataFrame(index=raw.index)
    for k in KOLOM_NOO_OUTPUT:
        asli = peta.get(norm(k))
        out[k] = raw[asli] if asli else ""
    out["Store ID"] = raw["_kunci"].map(store_id_app).replace("-","")
    return out.fillna("").reset_index(drop=True)

# ═════════════════════════════════════════════════════════════════════════════
# STATE MANAGEMENT
# ═════════════════════════════════════════════════════════════════════════════

def tuju_halaman(nama):
    st.session_state.halaman_aktif = nama

def atur_halaman_data(n):
    st.session_state.halaman_data = n

def reset_halaman_data():
    st.session_state.halaman_data = 1

def inisialisasi_state():
    ss = st.session_state
    ss.setdefault("baris_perlu_push", set())
    ss.setdefault("tab_validasi", "noo")
    ss.setdefault("sku_df", None)
    ss.setdefault("sku_pending", {})      # {row_key: "Done"}
    ss.setdefault("sku_ver", 0)
    ss.setdefault("sku_pilih", set())     # key baris yang dicentang
    ss.setdefault("baris_kotor", set())
    ss.setdefault("riwayat", [])
    ss.setdefault("versi_impor", 0)
    if "df_toko" not in ss or list(ss.df_toko.columns) != KOLOM_TAMPIL:
        ss.df_toko          = muat_data_noo_dari_supabase()
        ss.halaman_data     = 1
        ss.ukuran_halaman   = 15
        ss.baris_kotor      = set()
        ss.baris_perlu_push = set()
        ss.riwayat          = []
        ss.kunci_editor     = 0
        ss.versi_impor      = 0
        ss.halaman_aktif    = "validasi"
        ss.terakhir_update  = waktu_sekarang()
        ss.tanda_tangan     = None
        ss.snapshot         = None

def refresh_data_toko():
    ss = st.session_state
    ambil_raw_noo.clear()
    ss.df_toko          = muat_data_noo_dari_supabase()
    ss.halaman_data     = 1
    ss.baris_kotor      = set()
    ss.baris_perlu_push = set()
    ss.versi_impor     += 1
    ss.tanda_tangan     = None
    ss.snapshot         = None
    ss.terakhir_update  = waktu_sekarang()
    catat_riwayat("Refresh data","Manual reload from Supabase")
    ss.pesan_toast = "Data successfully refreshed from Supabase"

# Kolom grid yang boleh diedit user dan harus ikut tersimpan ke INPUT_NOO.
# (Customer Store Code/Name tidak dimasukkan: dipakai sebagai kunci pencarian baris.)
KOLOM_EDIT_KE_SUPABASE = ["Store Name", "City", "Store Address", "Longitude", "Latitude", "Store Type"]

def _patch_kolom_edit(baris: pd.Series) -> dict:
    """Nilai kolom hasil edit grid -> dict patch, memakai nama kolom ASLI di Supabase
    (huruf besar/spasi/snake_case dicocokkan lewat normalisasi). Kolom yang tidak ada di tabel dilewati."""
    norm = lambda s: re.sub(r"[^a-z0-9]","",str(s).lower())
    rows = ambil_raw_noo()
    asli = {norm(k): k for k in rows[0]} if rows else {}
    patch = {}
    for kol in KOLOM_EDIT_KE_SUPABASE:
        nama_sb = asli.get(norm(kol))
        if not nama_sb or kol not in baris.index:
            continue
        v = baris[kol]
        if pd.isna(v) or (isinstance(v, str) and not v.strip()):
            v = None
        elif kol in ("Longitude", "Latitude"):
            v = float(v)
        else:
            v = str(v).strip()
        patch[nama_sb] = v
    return patch

def simpan_perubahan():
    ss = st.session_state
    df = ss.df_toko
    kosong  = df["Store ID"].fillna("").astype(str).str.strip().isin(["","-"])
    butuh   = df["Validasi"].eq(STATUS_VALID) & kosong
    if butuh.any():
        df.loc[butuh,"Store ID"]      = generate_store_ids(df[butuh], df)
        baru_tergenerate              = [i for i in df.index[butuh] if df.loc[i,"Store ID"] != "-"]
        df.loc[baru_tergenerate,"Generated At"] = datetime.datetime.now(ZONA_WAKTU).replace(tzinfo=None)
        ss.baris_perlu_push.update(baru_tergenerate)

    sudah        = {}
    baris_target = set(ss.baris_kotor) | set(ss.baris_perlu_push)
    gagal_kunci  = []
    for idx in baris_target:
        kode  = str(df.loc[idx,"Customer Store Code"]).strip()
        nama  = str(df.loc[idx,"Customer Store Name"]).strip()
        if not kode or not nama:
            continue
        kunci   = (kode, nama)
        tagging = str(df.loc[idx,"Validasi"]).strip()
        patch   = {"tagging": tagging}
        patch.update(_patch_kolom_edit(df.loc[idx]))     # City, Address, Long/Lat, Store Type, Store Name
        if tagging == "Existing" and pd.isna(df.loc[idx,"Generated At"]):
            # Existing dari SPV Confirmation: Store ID diisi SPV -> simpan apa adanya, jangan dikosongkan
            sid_ada = str(df.loc[idx,"Store ID"]).strip()
            if sid_ada and sid_ada != "-":
                patch["Store ID"] = sid_ada
            patch["generated_at"] = None
        elif tagging != STATUS_VALID:
            patch["generated_at"] = None; patch["Store ID"] = ""
            df.loc[idx,"Store ID"] = "-"; df.loc[idx,"Generated At"] = pd.NaT
        else:
            if kunci in sudah:
                df.loc[idx,"Store ID"] = sudah[kunci]["store_id"]
                if sudah[kunci]["generated_at"]:
                    df.loc[idx,"Generated At"] = pd.Timestamp(sudah[kunci]["generated_at"])
            gid = str(df.loc[idx,"Store ID"]).strip()
            if gid and gid != "-":
                patch["Store ID"] = gid
                gen_at = df.loc[idx,"Generated At"]
                gen_at_iso = None
                if pd.notna(gen_at):
                    wt = pd.Timestamp(gen_at)
                    if wt.tzinfo is None:
                        wt = wt.tz_localize(ZONA_WAKTU)
                    gen_at_iso = wt.isoformat()
                    patch["generated_at"] = gen_at_iso
                sudah[kunci] = {"store_id":gid,"generated_at":gen_at_iso}

        if sb_patch_by_kode_nama(TABLE_SUBMIT_NOO, kode, nama, patch):
            ss.baris_kotor.discard(idx)
            ss.baris_perlu_push.discard(idx)
        else:
            gagal_kunci.append(f"{kode}/{nama}")

    jumlah = len(baris_target) - len(gagal_kunci)
    catat_riwayat("Save changes", f"{jumlah} rows updated")
    ss.tanda_tangan    = None
    ss.terakhir_update = waktu_sekarang()
    if gagal_kunci:
        detail = ss.get("err_tagging","-")
        ss.pesan_toast = f"Saved {jumlah} rows. FAILED {len(gagal_kunci)}: {', '.join(gagal_kunci)} — {detail}"
    else:
        ss.pesan_toast = f"Saved — {jumlah} rows updated" if jumlah else "No changes to save"

def selesai_impor(df, nama_file):
    ss = st.session_state
    ss.df_toko         = df
    ss.baris_kotor     = set()
    ss.halaman_data    = 1
    ss.versi_impor    += 1
    ss.tanda_tangan    = None
    ss.reset_filter    = True
    ss.halaman_aktif   = "validasi"
    ss.terakhir_update = waktu_sekarang()
    catat_riwayat("Import file", f"{nama_file} ({len(df):,} rows)")
    ss.pesan_toast = f"Successfully imported {len(df):,} rows from {nama_file}"

def normalisasi_impor(mentah: pd.DataFrame) -> pd.DataFrame:
    df = mentah[WAJIB_ISI].copy()
    for c in ["Store Name","Customer Store Code","Customer Store Name","City","Store Address","Store Type"]:
        df[c] = df[c].fillna("").astype(str).str.strip()
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df["Latitude"]  = pd.to_numeric(df["Latitude"],  errors="coerce")
    df.insert(0,"#", np.arange(1,len(df)+1))
    df["Validasi"] = STATUS_REVIEW        # Generate ID hanya lewat aksi user
    df["_sb_id"]   = ""
    df = df[KOLOM_TAMPIL]
    df.index = pd.Index(df["#"].to_numpy(), name="_id")
    return df

def template_csv() -> bytes:
    return pd.DataFrame(columns=WAJIB_ISI).to_csv(index=False).encode("utf-8")

def terapkan_filter(df, status, distributor="All distributors", rentang=None, brand="All brands"):
    m = pd.Series(True, index=df.index)
    if brand != "All brands":
        if "Customer Code" in df.columns:
            awal = df["Customer Code"].astype(str).str.strip().str[:2]
        elif "Customer Store Code" in df.columns:
            awal = df["Customer Store Code"].astype(str).str.strip().str[:2]
        else:
            awal = pd.Series("", index=df.index)
        m &= awal == ("17" if brand == "NXP" else "12")
    if rentang and "Created At" in df.columns:
        tgl = ke_wib_naive(df["Created At"]).dt.normalize()
        m  &= tgl >= pd.Timestamp(rentang[0])
        if len(rentang) == 2:
            m &= tgl <= pd.Timestamp(rentang[1])
    if status != "All statuses":
        m &= df["Validasi"] == status
    if distributor != "All distributors" and "Distributor" in df.columns:
        m &= df["Distributor"].astype(str).str.strip().str.startswith(distributor.strip())
    return df[m]

def terapkan_edit(master: pd.DataFrame, hasil_edit: pd.DataFrame):
    kolom_edit_valid = [c for c in KOLOM_EDIT if c in hasil_edit.columns]
    baru = hasil_edit[kolom_edit_valid]
    lama = master.loc[baru.index, kolom_edit_valid]
    beda = (baru.astype(object) != lama.astype(object)) & ~(baru.isna() & lama.isna())
    baris_berubah = beda.any(axis=1)
    id_berubah    = list(baris_berubah.index[baris_berubah])
    if not id_berubah:
        return []
    for c in kolom_edit_valid:
        master.loc[id_berubah, c] = baru.loc[id_berubah, c]
    return id_berubah

# ═════════════════════════════════════════════════════════════════════════════
# HALAMAN: STORE VALIDATION
# ═════════════════════════════════════════════════════════════════════════════

def tab_noo():
    ss = st.session_state
    tempat_kpi   = st.empty()

    # DEBUG SEMENTARA — hapus setelah ketemu sumbernya
#    st.write("DEBUG max BigQuery (IESM):", max_angka_per_prefix().get("IESK"))
#    st.write("DEBUG max Supabase (IESM):", sb_max_angka_store_id().get("IESK"))

    if ss.get("err_tagging"):
        st.error(ss.pop("err_tagging"))
    if ss.pop("reset_filter", False):
        ss.f_status = "All statuses"; ss.f_distri = "All distributors"; ss.f_brand = "All brands"
        ss.pop("f_tgl_awal",None); ss.pop("f_tgl_akhir",None)

    opsi_jenis  = sorted((set(JENIS_TOKO_DEFAULT) | set(ss.df_toko["Store Type"].dropna().astype(str))) - {""})
    daftar_distri = ["All distributors"] + daftar_distributor()      # hanya distributor Active
    hari_ini = datetime.datetime.now(ZONA_WAKTU).date()
    tgl_min  = pd.to_datetime(ss.df_toko.get("Created At"), errors="coerce").min() \
        if "Created At" in ss.df_toko.columns else pd.NaT
    tgl_awal = hari_ini if pd.isna(tgl_min) else tgl_min.date()

    c_tgl1,c_tgl2,c_distri,c_brand,c_status = st.columns([1.3,1.3,1.8,1.2,1.4])
    with c_tgl1:
        tgl_dari = st.date_input("Date from", value=tgl_awal, key="f_tgl_awal",
                                  format="YYYY-MM-DD", on_change=reset_halaman_data)
    with c_tgl2:
        tgl_sampai = st.date_input("Date to", value=hari_ini, key="f_tgl_akhir",
                                    format="YYYY-MM-DD", on_change=reset_halaman_data)
    if tgl_dari > tgl_sampai:
        st.warning("Start date is later than end date, range reversed automatically.")
    rentang = (min(tgl_dari,tgl_sampai), max(tgl_dari,tgl_sampai))
    with c_distri:
        distributor = st.selectbox("Distributor", daftar_distri, key="f_distri", on_change=reset_halaman_data)
    with c_brand:
        brand = st.selectbox("Brand", ["All brands", "NXP", "G2G"], key="f_brand",
                             on_change=reset_halaman_data)
    with c_status:
        status = st.selectbox("Status",["All statuses"] + KOLOM_VALIDASI, key="f_status",
                               on_change=reset_halaman_data)
    with st.container(key="noo_toolbar"):
        b1, b2, b3, info = st.columns([1, 1.4, 1.3, 6.3], gap="small", vertical_alignment="center")
        b1.button(":material/refresh: Refresh", width="stretch", key="tombol_refresh", on_click=refresh_data_toko,
                  help="Reload data from Supabase. Unsaved changes will be lost.")
        tempat_unduh = b2.empty()      # diisi setelah hasil filter dihitung
        b3.button("Save changes", type="primary", width="stretch", key="tombol_simpan",
                  on_click=simpan_perubahan)
        info.markdown(f'<div class="teks-showing">{len(ss.baris_kotor)} rows unsaved</div>'
                      if ss.baris_kotor else "", unsafe_allow_html=True)

    hasil_filter = terapkan_filter(ss.df_toko, status, distributor, tuple(rentang), brand)
    total_filter = len(hasil_filter)

    ukuran       = ss.ukuran_halaman
    total_halaman = max(1, math.ceil(total_filter / ukuran))
    ss.halaman_data = max(1, min(ss.halaman_data, total_halaman))
    mulai = (ss.halaman_data - 1) * ukuran
    akhir = min(mulai + ukuran, total_filter)

    if total_filter == 0:
        with tempat_unduh.container():
            st.download_button(":material/download: Download NOO", b"", "hasil_noo.xlsx",
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               disabled=True, width="stretch", key="unduh_noo_validasi")
        tempat_kpi.markdown(komponen.html_kpi_validasi(ss.df_toko), unsafe_allow_html=True)
        st.info("No stores match the filter.")
        return

    tanda = (status, distributor, brand, tuple(rentang), ss.halaman_data, ukuran, ss.versi_impor)
    if ss.tanda_tangan != tanda or ss.snapshot is None:
        ss.snapshot        = hasil_filter.iloc[mulai:akhir][KOLOM_TAMPIL].copy()
        ss.snapshot["Pilih"] = False
        ss.tanda_tangan    = tanda
        ss.kunci_editor   += 1

    c_select_all, c_generate, c_hapus, _ = st.columns([1.3,1.2,1.1,6], gap="small", vertical_alignment="center")
    with c_select_all:
        select_all = st.checkbox("Select All", key=f"select_all_{ss.kunci_editor}")
        ss.snapshot["Pilih"] = select_all
        ss.tampilan_kunci    = None
    with c_generate:
        tombol_generate = st.button("Generate ID", type="primary", key="tombol_generate_bulk", width="stretch")
    with c_hapus:
        tombol_hapus = st.button(":material/delete: Delete", type="primary", key="tombol_hapus_noo", width="stretch")

    # Tabel = custom component (noo_grid.py): dropdown Validation berwarna + Store ID jelas bisa/tidak diedit
    kolom_grid = [
        dict(name="#",                   label="#",                   kind="index",   width=50),
        dict(name="Pilih",               label="Select",              kind="check",   width=60),
        dict(name="Validasi",            label="Validation",          kind="status",  width=150, options=KOLOM_VALIDASI,
             help="Click to change status"),
        dict(name="Store ID",            label="Store ID (Existing: Editable)", kind="storeid", width=210,
             help="Type the Store ID here only when Validation = Existing. Otherwise it is generated automatically."),
        dict(name="Distributor",         label="Distributor",         kind="ro",      width=230),
        dict(name="Store Name",          label="Store Name",          kind="text",    width=180),
        dict(name="Customer Code",       label="Customer Code",       kind="ro",      width=150),
        dict(name="Customer Store Code", label="Customer Store Code", kind="text",    width=190),
        dict(name="Customer Store Name", label="Customer Store Name", kind="ro",      width=180),
        dict(name="City",                label="City",                kind="select",  width=150, options=[""] + CITY),
        dict(name="Store Address",       label="Store Address",       kind="text",    width=200),
        dict(name="Longitude",           label="Longitude",           kind="number",  width=110),
        dict(name="Latitude",            label="Latitude",            kind="number",  width=110),
        dict(name="Store Type",          label="Store Type",          kind="select",  width=140, options=opsi_jenis),
        dict(name="Created At",          label="Created At",          kind="ro",      width=150),
        dict(name="Generated At",        label="Generated At",        kind="ro",      width=150),
    ]
    tempat_hapus = st.empty()      # pernyataan konfirmasi hapus ditaruh DI ATAS tabel (sama seperti SKU Mapping)
    warna_css = {k: v for k, v in WARNA_VALIDASI.items()}
    # Customer Store Name ditampilkan = Store Name (bukan nama distributor). Hanya tampilan: nilai asli di df_toko
    # tetap dipakai sbg kunci Supabase (Customer Store Code + Customer Store Name), jadi dikembalikan setelah render.
    tampil_grid = ss.snapshot.copy()
    tampil_grid["Customer Store Name"] = tampil_grid["Store Name"]
    hasil_edit = noo_grid(tampil_grid, kolom_grid, key=f"noo_grid_{ss.kunci_editor}",
                          select_all=select_all, warna=warna_css)
    hasil_edit["Customer Store Name"] = ss.snapshot["Customer Store Name"]

    # Download: baris yang dicentang (kolom Select) saja; kalau tidak ada yang dicentang -> semua hasil filter
    dipilih_unduh = hasil_edit.index[hasil_edit["Pilih"] == True]
    sumber_unduh  = (hasil_filter.loc[hasil_filter.index.intersection(dipilih_unduh)]
                     if len(dipilih_unduh) else hasil_filter)
    data_unduh = bangun_download_noo(sumber_unduh)
    n_pilih_unduh = len(dipilih_unduh)
    with tempat_unduh.container():
        st.download_button(f":material/download: Download NOO{f' ({n_pilih_unduh})' if n_pilih_unduh else ''}",
                           _excel_cache(data_unduh), "hasil_noo.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           disabled=data_unduh.empty, width="stretch", key="unduh_noo_validasi")

    # Hapus baris NOO terpilih (dengan konfirmasi)
    if tombol_hapus:
        dipilih_hapus = hasil_edit[hasil_edit["Pilih"] == True].index.tolist()
        if not dipilih_hapus:
            st.warning("No rows selected.")
        else:
            ss.noo_hapus_antri = dipilih_hapus
    with tempat_hapus.container():
        if pesan_err := ss.pop("noo_hapus_error", None):
            st.error(pesan_err)
        if ss.get("noo_hapus_antri"):
            antri = [i for i in ss.noo_hapus_antri if i in ss.df_toko.index]
            st.warning(f"Permanently delete {len(antri)} NOO row(s) from Supabase? This cannot be undone.")
            k_ya, k_batal, _ = st.columns([1.3, 1, 7], gap="xxsmall")
            if k_batal.button("Cancel", key="noo_hapus_batal", width="stretch"):
                ss.noo_hapus_antri = None
                st.rerun()
            if k_ya.button("Yes, delete", type="primary", key="noo_hapus_ya", width="stretch"):
                df = ss.df_toko
                terhapus, gagal, ditolak_policy = [], [], False
                for i in antri:
                    kode = str(df.loc[i, "Customer Store Code"]).strip()
                    nama = str(df.loc[i, "Customer Store Name"]).strip()
                    n, err = sb_delete(TABLE_SUBMIT_NOO, {"Customer Store Code": kode, "Customer Store Name": nama})
                    if n > 0:
                        terhapus.append(i)
                    else:
                        gagal.append(err or "denied")
                        if not err:
                            ditolak_policy = True
                if terhapus:
                    ss.df_toko = df.drop(index=terhapus)
                    ss.baris_kotor -= set(terhapus)
                    ss.baris_perlu_push -= set(terhapus)
                    ambil_raw_noo.clear()
                    catat_riwayat("Delete NOO", f"{len(terhapus)} rows")
                ss.noo_hapus_antri = None
                ss.tanda_tangan = None; ss.snapshot = None
                ss.terakhir_update = waktu_sekarang()
                ss.pesan_toast = (f"{len(terhapus)} row(s) deleted"
                                  + (f" | {len(gagal)} failed" if gagal else ""))
                if ditolak_policy:
                    ss.noo_hapus_error = ("Supabase accepted the request but deleted 0 rows: the table has no DELETE policy "
                                          "for this API key (RLS). Add a DELETE policy on INPUT_NOO in Supabase.")
                elif gagal:
                    ss.noo_hapus_error = "Delete failed: " + "; ".join(gagal[:3])
                st.rerun()

    # Bulk Generate ID
    if tombol_generate:
        baris_dipilih = hasil_edit[hasil_edit["Pilih"] == True].index.tolist()
        if not baris_dipilih:
            st.warning("No rows selected.")
        else:
            df = ss.df_toko
            for idx in baris_dipilih:
                if df.loc[idx,"Validasi"] == "Existing":          # Existing -> Generate ID: buang Store ID lama
                    df.loc[idx,"Store ID"] = "-"; df.loc[idx,"Generated At"] = pd.NaT
                df.loc[idx,"Validasi"]      = STATUS_VALID
                hasil_edit.loc[idx,"Validasi"] = STATUS_VALID
            store_kosong = df.loc[baris_dipilih,"Store ID"].fillna("").astype(str).str.strip().isin(["","-"])
            perlu_generate = [i for i in baris_dipilih if store_kosong.loc[i]]
            baru_generate  = []
            for i in perlu_generate:
                kode  = str(df.loc[i,"Customer Store Code"]).strip()
                nama  = str(df.loc[i,"Customer Store Name"]).strip()
                cocok = df[
                    (df["Customer Store Code"].astype(str).str.strip() == kode) &
                    (df["Customer Store Name"].astype(str).str.strip() == nama) &
                    (~df["Store ID"].fillna("").astype(str).str.strip().isin(["","-"]))
                ]
                if not cocok.empty:
                    df.loc[i,"Store ID"]      = str(cocok.iloc[0]["Store ID"]).strip()
                    df.loc[i,"Generated At"]  = cocok.iloc[0]["Generated At"]
                else:
                    baru_generate.append(i)
            if baru_generate:
                df.loc[baru_generate,"Store ID"]     = generate_store_ids(df.loc[baru_generate], df, kolom_target="Store ID")
                df.loc[baru_generate,"Generated At"] = datetime.datetime.now(ZONA_WAKTU).replace(tzinfo=None)
            gagal_prefix = [i for i in baris_dipilih if df.loc[i,"Store ID"] in ("","-",None)]
            # Tidak langsung push ke Supabase: hanya ditandai belum tersimpan.
            # Baru tersimpan lewat "Save changes"; kalau tidak, hilang saat Refresh / reload.
            berhasil_idx = [i for i in baris_dipilih if i not in gagal_prefix]
            ss.baris_kotor.update(berhasil_idx)
            ss.baris_perlu_push.update(berhasil_idx)
            for i in gagal_prefix:                      # gagal generate -> kembalikan status semula
                df.loc[i,"Validasi"] = STATUS_REVIEW
                ss.baris_kotor.discard(i); ss.baris_perlu_push.discard(i)
            berhasil = len(berhasil_idx)
            pesan = f"{berhasil} rows generated — click Save changes to save to Supabase"
            if gagal_prefix:
                distri_gagal = sorted({str(df.loc[i,"Distributor"]) or "(empty)" for i in gagal_prefix})
                pesan += f" | {len(gagal_prefix)} failed (prefix not found: {', '.join(distri_gagal)})"
            ss.tanda_tangan = None; ss.snapshot = None
            ss.terakhir_update = waktu_sekarang()
            ss.pesan_toast = pesan
            st.rerun()

    # Store ID hanya boleh diketik manual kalau Validation = Existing; selain itu edit dibatalkan
    manual_sid, ditolak_sid = set(), False
    if "Store ID" in hasil_edit.columns:
        _norm = lambda s: s.fillna("").astype(str).str.strip().replace("-", "")
        lama_sid = _norm(ss.df_toko.loc[hasil_edit.index, "Store ID"])
        baru_sid = _norm(hasil_edit["Store ID"])
        for i in hasil_edit.index[baru_sid != lama_sid]:
            if str(hasil_edit.loc[i, "Validasi"]).strip() == "Existing":
                manual_sid.add(i)
            else:
                hasil_edit.loc[i, "Store ID"] = ss.df_toko.loc[i, "Store ID"]
                ditolak_sid = True
    if ditolak_sid:
        st.toast("Store ID can only be edited when Validation = Existing.", icon=":material/warning:")

    validasi_lama = ss.df_toko["Validasi"].copy()      # sebelum edit diterapkan, untuk deteksi Existing -> Generate ID
    id_berubah = terapkan_edit(ss.df_toko, hasil_edit)
    if id_berubah:
        ss.baris_kotor.update(id_berubah)
        df = ss.df_toko
        # Existing -> Generate ID: buang Store ID lama (milik Existing) supaya ID baru benar-benar di-generate
        dari_existing = [i for i in id_berubah
                         if validasi_lama.get(i) == "Existing" and df.loc[i, "Validasi"] == STATUS_VALID]
        for i in dari_existing:
            df.loc[i, "Store ID"] = "-"; df.loc[i, "Generated At"] = pd.NaT
            ss.baris_perlu_push.discard(i)
        for i in manual_sid & set(id_berubah):
            df.loc[i, "Generated At"] = pd.NaT       # Store ID manual: diperlakukan seperti Existing dari SPV
            ss.baris_perlu_push.discard(i)
        validasi_skrg = df.loc[id_berubah,"Validasi"]
        pilih = validasi_skrg.eq(STATUS_VALID)
        # Existing yang Store ID-nya dari SPV (tanpa Generated At) tidak boleh dikosongkan saat baris diedit
        existing_spv = validasi_skrg.eq("Existing") & df.loc[id_berubah,"Generated At"].isna()
        batal = [i for i in id_berubah if not pilih.loc[i] and not existing_spv.loc[i]]
        prefix_terdampak = set()
        for i in batal:
            sid_lama = str(df.loc[i,"Store ID"]).strip()
            if sid_lama and sid_lama != "-":
                prefix_terdampak.add(sid_lama[:PANJANG_PREFIX])
            df.loc[i,"Store ID"] = "-"; df.loc[i,"Generated At"] = pd.NaT
            ss.baris_perlu_push.discard(i)
        for prefix in prefix_terdampak:
            id_geser = resequence_prefix(df, prefix)
            if id_geser:
                ss.baris_kotor.update(id_geser)
                ss.baris_perlu_push.update(id_geser)
        store_kosong = df.loc[id_berubah,"Store ID"].fillna("").astype(str).str.strip().isin(["","-"])
        baru = list(pilih.index[pilih & store_kosong])
        if baru:
            perlu_generate = []
            for i in baru:
                kode = str(df.loc[i,"Customer Store Code"]).strip()
                nama = str(df.loc[i,"Customer Store Name"]).strip()
                cocok = df[
                    (df["Customer Store Code"].astype(str).str.strip() == kode) &
                    (df["Customer Store Name"].astype(str).str.strip() == nama) &
                    (~df["Store ID"].fillna("").astype(str).str.strip().isin(["","-"]))
                ]
                if not cocok.empty:
                    df.loc[i,"Store ID"]     = str(cocok.iloc[0]["Store ID"]).strip()
                    df.loc[i,"Generated At"] = cocok.iloc[0]["Generated At"]
                else:
                    perlu_generate.append(i)
            if perlu_generate:
                df.loc[perlu_generate,"Store ID"] = generate_store_ids(
                    df.loc[perlu_generate], df, kolom_target="Store ID")
                df.loc[perlu_generate,"Generated At"] = datetime.datetime.now(ZONA_WAKTU).replace(tzinfo=None)
            gagal_id  = [i for i in baru if df.loc[i,"Store ID"] == "-"]
            berhasil_id = [i for i in baru if df.loc[i,"Store ID"] != "-"]
            ss.baris_perlu_push.update(berhasil_id)
            if gagal_id:
                st.warning("Prefix not found: "
                           + ", ".join(sorted({str(df.loc[i,"Distributor"]) or "(empty)" for i in gagal_id})))
        snapshot_baru = ss.df_toko.loc[ss.snapshot.index, KOLOM_TAMPIL].copy()
        if not snapshot_baru["Validasi"].equals(ss.snapshot["Validasi"]):
            ss.snapshot = snapshot_baru; ss.kunci_editor += 1; st.rerun()

    tempat_kpi.markdown(komponen.html_kpi_validasi(ss.df_toko), unsafe_allow_html=True)
    awal_nomor    = max(1, min(ss.halaman_data-2, total_halaman-4))
    nomor_halaman = list(range(awal_nomor, min(total_halaman, awal_nomor+4)+1))
    kolom_footer  = st.columns([14]+[1]*(len(nomor_halaman)+2))
    teks_belum    = f'<span class="teks-belumsimpan">● {len(ss.baris_kotor)} rows unsaved</span>' if ss.baris_kotor else ""
    kolom_footer[0].markdown(
        f'<div class="teks-showing">Showing {mulai+1:,} – {akhir:,} of {total_filter:,} stores{teks_belum}</div>',
        unsafe_allow_html=True)
    kolom_footer[1].button("‹",key="hal_sebelum",disabled=ss.halaman_data<=1,
                            on_click=atur_halaman_data,args=(ss.halaman_data-1,),width="stretch")
    for i, n in enumerate(nomor_halaman):
        kunci = f"halaktif_{n}" if n == ss.halaman_data else f"hal_{n}"
        kolom_footer[2+i].button(str(n),key=kunci,on_click=atur_halaman_data,args=(n,),width="stretch")
    kolom_footer[-1].button("›",key="hal_lanjut",disabled=ss.halaman_data>=total_halaman,
                             on_click=atur_halaman_data,args=(ss.halaman_data+1,),width="stretch")
# ═════════════════════════════════════════════════════════════════════════════
# HALAMAN: STORE VALIDATION (wrapper) + SKU MAPPING
# ═════════════════════════════════════════════════════════════════════════════
SKU_TABLE    = "sku_mapping"
SKU_TAGGING  = ["Not Yet", "Done"]
KUNCI_NOO    = ("f_tgl_awal", "f_tgl_akhir", "f_distri", "f_brand", "f_status")
KUNCI_SKU    = ("sku_f_nama", "sku_f_brand", "sku_f_tagging", "sku_f_dari", "sku_f_sampai", "sku_select_all")

SKU_DL_HEADERS = ["Customer Code", "Customer Name", "Product", "Product Chinese Short Name",
                  "Product English Name", "Customer Product Code", "Customer Product Name",
                  "Specification", "Barcode", "Description"]
SKU_HIDE_VIEW  = {"productchineseshortname", "productenglishname",
                  "specification", "barcode", "description"}

def _bangun_download_sku(df_f):
    peta = {_norm(c): c for c in df_f.columns}
    out = pd.DataFrame(index=df_f.index)
    for h in SKU_DL_HEADERS:
        asli = peta.get(_norm(h))          
        out[h] = df_f[asli] if asli else ""
    return out.reset_index(drop=True)
_norm        = lambda s: re.sub(r"[^a-z0-9]", "", str(s).lower())

SKU_LABEL = {
    "customercode": "Customer Code", "customername": "Customer Name",
    "product": "Product", "customerproductcode": "Customer Product Code",
    "customerproductname": "Customer Product Name",
    "createdat": "Created At", "tagging": "Tagging",
}

def label_kolom(c):
    return SKU_LABEL.get(_norm(c), str(c).replace("_", " ").title())

def _kolom(df, *names):
    peta = {_norm(c): c for c in df.columns}
    for n in names:
        if _norm(n) in peta:
            return peta[_norm(n)]
    return None

def _pertahankan(kunci):
    # widget yang tidak dirender di run ini akan kehilangan state -> re-assign
    for k in kunci:
        if k in st.session_state:
            st.session_state[k] = st.session_state[k]
def _pilih_tab(nama):
    ss = st.session_state
    ss.tab_validasi = nama
    ss.snapshot = None          # bangun ulang dari df_toko (sudah berisi edit)
    ss.tanda_tangan = None

@st.cache_data(ttl=300, show_spinner=False)
def ambil_sku_mapping() -> pd.DataFrame:
    rows, start, step = [], 0, 1000
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/{SKU_TABLE}",
            headers={**_SB_HEADERS, "Prefer": "", "Range-Unit": "items",
                     "Range": f"{start}-{start+step-1}"},
            params={"select": "*", "order": "created_at.desc"}, timeout=15)
        if r.status_code == 416:      # offset melewati total baris = selesai
            break
        r.raise_for_status()
        batch = r.json()
        rows += batch
        if len(batch) < step:
            break
        start += step
    return pd.DataFrame(rows)          # urutan kolom = urutan dari Supabase

def _kolom_kunci(df):
    pk = _kolom(df, "id")
    if pk:
        return [pk]
    return [c for c in (_kolom(df, "customer_code"), _kolom(df, "customer_product_code")) if c]

def _kunci_baris(df, cols):
    return df[cols].astype(str).agg("||".join, axis=1)

def _sku_on_change():
    ss = st.session_state
    ed = ss.get(f"editor_sku_{ss.sku_ver}", {}).get("edited_rows", {})
    tag = ss.sku_tag_col
    for pos, ch in ed.items():
        k = ss.sku_view_keys[int(pos)]
        if "Pilih" in ch:
            (ss.sku_pilih.add if ch["Pilih"] else ss.sku_pilih.discard)(k)
        if tag in ch:
            if ch[tag] == ss.sku_asli.get(k):
                ss.sku_pending.pop(k, None)
            else:
                ss.sku_pending[k] = ch[tag]
    ss.sku_select_all = bool(ss.sku_view_keys) and all(k in ss.sku_pilih for k in ss.sku_view_keys)
    ss.sku_ver += 1      # remount editor supaya menampilkan overlay pending

def _sku_pilih_semua():
    ss = st.session_state
    ss.sku_pilih = set(ss.sku_view_keys) if ss.sku_select_all else set()
    ss.sku_ver += 1

def _sku_mark_done():
    ss = st.session_state
    for k in ss.sku_pilih & set(ss.sku_view_keys):
        if ss.sku_asli.get(k) == "Done":
            ss.sku_pending.pop(k, None)
        else:
            ss.sku_pending[k] = "Done"
    ss.sku_pilih = set()
    ss.sku_select_all = False
    ss.sku_ver += 1

def reset_filter_sku(tgl_min, tgl_max):
    ss = st.session_state
    ss.sku_f_nama, ss.sku_f_brand = "All customers", "All brands"
    ss.sku_f_tagging = "All tagging"
    ss.sku_f_dari, ss.sku_f_sampai = tgl_min, tgl_max

def _refresh_sku():
    ss = st.session_state
    with st.spinner("Refreshing SKU Mapping..."):
        try:
            ambil_sku_mapping.clear()
            ss.sku_df = ambil_sku_mapping()
            ss.sku_pending = {}
            ss.sku_ver += 1
            ss.sku_konfirmasi = False
            ss.sku_pesan = ("ok", "SKU Mapping refreshed from Supabase")
        except Exception as e:
            ss.sku_pesan = ("error", f"Refresh failed: {e}")
    st.rerun()

def _simpan_sku(df, kc_pk, tag, rk):
    ss, ok, gagal = st.session_state, 0, []
    for k, val in list(ss.sku_pending.items()):
        if (rk == k).sum() != 1:                       # key tidak unik -> jangan update
            gagal.append(f"{k} (key not unique)"); continue
        row = df.loc[rk == k].iloc[0]
        try:
            r = requests.patch(
                f"{SUPABASE_URL}/rest/v1/{SKU_TABLE}",
                headers={**_SB_HEADERS, "Prefer": "return=representation"},
                params={c: f"eq.{row[c]}" for c in kc_pk},
                json={tag: val}, timeout=10)
            if r.ok and len(r.json()) == 1:            # harus tepat 1 baris
                df.loc[rk == k, tag] = val
                ss.sku_pending.pop(k)
                ok += 1
            else:
                gagal.append(f"{k} (HTTP {r.status_code}: {r.text[:200]})")
        except Exception as e:
            gagal.append(f"{k} ({e})")
    ambil_sku_mapping.clear()
    ss.sku_ver += 1
    return ok, gagal

def warna_tagging(v):
    return ("background-color:#dcfce7;color:#166534;font-weight:600" if v == "Done"
            else "background-color:#fee2e2;color:#b91c1c;font-weight:600")

def _tab_sku():
    ss = st.session_state
    if ss.sku_df is None:
        try:
            ss.sku_df = ambil_sku_mapping()
            if ss.sku_df.empty:
                ambil_sku_mapping.clear()      # jangan cache hasil kosong
        except Exception as e:
            st.error(f"Failed to load SKU Mapping: {e}"); return
    df = ss.sku_df
    if df.empty:
        with st.container(key="sku_filters"):
            f1, f2, f3, f4, f5 = st.columns([2.2, 1.2, 1.2, 1.2, 0.8], vertical_alignment="bottom")
            f1.selectbox("Customer Store Name", ["All customers"], disabled=True, key="sku_f_nama_x")
            f2.selectbox("Brand", ["All brands"], disabled=True, key="sku_f_brand_x")
            f3.date_input("Date From", disabled=True, key="sku_f_dari_x", format="YYYY-MM-DD")
            f4.date_input("Date To", disabled=True, key="sku_f_sampai_x", format="YYYY-MM-DD")
            f5.button("Reset", disabled=True, key="sku_reset_x", width="stretch")
        if msg := ss.pop("sku_pesan", None):
            (st.success if msg[0] == "ok" else st.error)(msg[1])
        with st.container(key="sku_toolbar"):
            b1, b2, b3, b4, info = st.columns([1, 1.2, 1.5, 1.1, 5], vertical_alignment="center")
            if b1.button(":material/refresh: Refresh", type="primary", key="sku_refresh_empty", width="stretch"):
                _refresh_sku()
            b2.button(":material/download: Download", type="primary", disabled=True, key="sku_dl_x", width="stretch")
            b3.button(":material/save: Save changes", type="primary", disabled=True, key="sku_save_x", width="stretch")
            b4.button(":material/delete: Delete", type="primary", disabled=True, key="sku_hapus_x", width="stretch")
            info.markdown('<div class="teks-showing">Showing 0 of 0 records</div>', unsafe_allow_html=True)
        st.info("No SKU Mapping data returned from Supabase.")
        return

    tag     = _kolom(df, "tagging")
    if not tag:
        st.error("Column `tagging` not found in `sku_mapping`. Run the SQL migration in Supabase first.")
        return
    kc_nama = _kolom(df, "customer_store_name", "customer_name")
    kc_cc   = _kolom(df, "customer_code")
    kc_tgl  = _kolom(df, "created_at")
    kc_pk   = _kolom_kunci(df)
    if not kc_pk:
        st.error("Cannot determine a safe row key (need `id` or customer_code + customer_product_code)."); return
    rk = _kunci_baris(df, kc_pk)
    ss.sku_tag_col = tag
    ss.sku_asli    = dict(zip(rk, df[tag].fillna("Not Yet")))

    hari_ini = datetime.datetime.now(ZONA_WAKTU).date()
    tgl = ke_wib_naive(df[kc_tgl]).dt.normalize() if kc_tgl else None
    tgl_min = tgl.min().date() if tgl is not None and tgl.notna().any() else hari_ini
    cc2 = df[kc_cc].astype(str).str.strip().str[:2] if kc_cc else pd.Series("", index=df.index)

    try:
        _sh = muat_distributor_dari_sheet()
        _sh = _sh[_sh["Status"].str.strip().str.lower() == "active"]       # hanya distributor Active
        daftar_sheet = (_sh.iloc[:, 0].astype(str).str.strip()
                        .replace("", np.nan).dropna().unique().tolist())
    except Exception:
        daftar_sheet = []
    nama_opts  = ["All customers"] + sorted(daftar_sheet)
    brand_opts = ["All brands", "NXP", "G2G"]
    tagging_opts = ["All tagging"] + SKU_TAGGING
    ss.setdefault("sku_f_nama", "All customers"); ss.setdefault("sku_f_brand", "All brands")
    ss.setdefault("sku_f_tagging", "All tagging")
    ss.setdefault("sku_f_dari", tgl_min);         ss.setdefault("sku_f_sampai", hari_ini)
    if ss.sku_f_nama  not in nama_opts:  ss.sku_f_nama  = "All customers"
    if ss.sku_f_brand not in brand_opts: ss.sku_f_brand = "All brands"
    if ss.sku_f_tagging not in tagging_opts: ss.sku_f_tagging = "All tagging"

    # ── Filters ──
    with st.container(key="sku_filters"):
        f1, f2, f3, f4, f5, f6 = st.columns([2.2, 1.2, 1.2, 1.2, 1.2, 0.8], vertical_alignment="bottom")
        f1.selectbox("Customer Store Name", nama_opts, key="sku_f_nama")
        f2.selectbox("Brand", brand_opts, key="sku_f_brand")
        f3.selectbox("Tagging", tagging_opts, key="sku_f_tagging")
        f4.date_input("Date From", key="sku_f_dari", format="YYYY-MM-DD")
        f5.date_input("Date To",   key="sku_f_sampai", format="YYYY-MM-DD")
        f6.button("Reset", key="sku_reset", on_click=reset_filter_sku,
          args=(tgl_min, hari_ini), width="stretch")

    m = pd.Series(True, index=df.index)
    if kc_nama and ss.sku_f_nama != "All customers":
        m &= (df[kc_nama].astype(str).str.strip().str.lower()
              == ss.sku_f_nama.strip().lower())
    if ss.sku_f_brand == "NXP":
        m &= cc2 == "17"
    elif ss.sku_f_brand == "G2G":
        m &= cc2 == "12"
    if ss.sku_f_tagging != "All tagging":
        m &= df[tag].fillna("Not Yet").astype(str).str.strip() == ss.sku_f_tagging
    if tgl is not None:
        d1, d2 = sorted([ss.sku_f_dari, ss.sku_f_sampai])
        m &= (tgl >= pd.Timestamp(d1)) & (tgl <= pd.Timestamp(d2))
    df_f = df[m]

    # ── Toolbar ──
    if msg := ss.pop("sku_pesan", None):
        (st.success if msg[0] == "ok" else st.error)(msg[1])
    # Download: baris yang dicentang (Select) saja; kalau tidak ada yang dicentang -> semua hasil filter
    _keys_f   = rk[m].tolist()
    _terpilih = [k in ss.sku_pilih for k in _keys_f]
    df_unduh  = df_f[_terpilih] if any(_terpilih) else df_f
    n_pilih_sku = sum(_terpilih)
    data_dl = _bangun_download_sku(df_unduh)
    with st.container(key="sku_toolbar"):
        b1, b2, b3, info = st.columns([1, 1.2, 1.5, 6], vertical_alignment="center")
        refresh = b1.button(":material/refresh: Refresh", type="primary", key="sku_refresh", width="stretch")
        b2.download_button(
            f":material/download: Download{f' ({n_pilih_sku})' if n_pilih_sku else ''}",
            _excel_cache(data_dl, startrow=0) if not df_f.empty else b"",   # header di baris 1
            "hasil_sku_mapping.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            disabled=df_f.empty,
            type="primary",
            key="unduh_sku",
            width="stretch",
)
        simpan = b3.button(":material/save: Save changes", type="primary", key="sku_simpan",
                           disabled=not ss.sku_pending, width="stretch")
        belum = (f' <span class="teks-belumsimpan">● {len(ss.sku_pending)} unsaved</span>'
                 if ss.sku_pending else "")
        info.markdown(f'<div class="teks-showing">Showing {len(df_f):,} of {len(df):,} records{belum}</div>',
                      unsafe_allow_html=True)

    if refresh:
        if ss.sku_pending: ss.sku_konfirmasi = True
        else: _refresh_sku()
    if ss.get("sku_konfirmasi"):
        st.warning(f"{len(ss.sku_pending)} unsaved change(s) will be lost if you refresh.")
        a, b, _ = st.columns([1.6, 1, 6])
        if a.button("Discard & refresh", key="sku_discard"): _refresh_sku()
        if b.button("Cancel", key="sku_cancel"):
            ss.sku_konfirmasi = False; st.rerun()

    if simpan:
        with st.spinner("Saving changes..."):
            ok, gagal = _simpan_sku(df, kc_pk, tag, rk)
            if not gagal:
                ss.sku_df = ambil_sku_mapping()   # tarik ulang dari Supabase
        ss.sku_pesan = (("ok", f"{ok} record(s) saved") if not gagal else
                        ("error", f"{ok} saved, {len(gagal)} failed (kept as pending): " + "; ".join(gagal[:5])))
        st.rerun()

    # ── Table ──
    if df_f.empty:
        st.info("No SKU Mapping records match the filters."); return
    keys = rk[m].tolist()
    ss.sku_view_keys = keys
    view = df_f.reset_index(drop=True).copy()
    view[tag] = [ss.sku_pending.get(k, v) for k, v in zip(keys, view[tag].fillna("Not Yet"))]
    pend_pos = {i for i, k in enumerate(keys) if k in ss.sku_pending}
    view.insert(0, "Pilih", [k in ss.sku_pilih for k in keys])

    styler = (view.style
              .apply(lambda r: ["background-color:#eff6ff" if r.name in pend_pos else "" for _ in r], axis=1)
              .map(warna_tagging, subset=[tag]))
    tampil = [c for c in view.columns if _norm(c) not in SKU_HIDE_VIEW]
    if kc_cc in tampil:
        tampil.remove(kc_cc); tampil.insert(0, kc_cc)      
    if "Pilih" in tampil:
        tampil.remove("Pilih")
    tampil.insert(0, "Pilih")
    s1, s2, s3, _ = st.columns([1.3, 1.4, 1.0, 6.3], gap="small", vertical_alignment="center")
    if ss.pop("_reset_sku_select_all", False):          # reset harus SEBELUM widget dibuat (tidak boleh sesudahnya)
        ss.sku_select_all = False
    s1.checkbox("Select All", key="sku_select_all", on_change=_sku_pilih_semua)
    s2.button(":material/check_circle: Mark as Done", type="primary", key="sku_mark_done", on_click=_sku_mark_done,
              disabled=not (ss.sku_pilih & set(keys)), width="stretch")
    if s3.button(":material/delete: Delete", type="primary", key="sku_hapus", disabled=not (ss.sku_pilih & set(keys)),
                 width="stretch"):
        ss.sku_hapus_antri = sorted(ss.sku_pilih & set(keys))
    if ss.get("sku_hapus_antri"):
        st.warning(f"Permanently delete {len(ss.sku_hapus_antri)} SKU Mapping record(s) from Supabase? This cannot be undone.")
        h_ya, h_batal, _ = st.columns([1.3, 1, 7], gap="xxsmall")
        if h_batal.button("Cancel", key="sku_hapus_batal", width="stretch"):
            ss.sku_hapus_antri = None
            st.rerun()
        if h_ya.button("Yes, delete", type="primary", key="sku_hapus_ya", width="stretch"):
            terhapus, gagal = set(), []
            for k in ss.sku_hapus_antri:
                if (rk == k).sum() != 1:                      # key tidak unik -> jangan hapus
                    gagal.append(f"{k} (key not unique)"); continue
                row = df.loc[rk == k].iloc[0]
                n, err = sb_delete(SKU_TABLE, {c: row[c] for c in kc_pk})
                if n == 1:
                    terhapus.add(k)
                else:
                    gagal.append(err or "Supabase deleted 0 rows: no DELETE policy for this API key (RLS) on sku_mapping")
            if terhapus:
                ss.sku_df = df[~rk.isin(terhapus)].reset_index(drop=True)
                ss.sku_pilih -= terhapus
                for k in terhapus:
                    ss.sku_pending.pop(k, None)
                ambil_sku_mapping.clear()
                ss["_reset_sku_select_all"] = True             # dieksekusi di run berikutnya, sebelum checkbox dibuat
                ss.sku_ver += 1
                catat_riwayat("Delete SKU Mapping", f"{len(terhapus)} rows")
            ss.sku_hapus_antri = None
            ss.sku_pesan = (("ok", f"{len(terhapus)} record(s) deleted") if not gagal else
                            ("error", f"{len(terhapus)} deleted, {len(gagal)} failed: " + "; ".join(gagal[:3])))
            st.rerun()
    st.data_editor(
        styler, key=f"editor_sku_{ss.sku_ver}", on_change=_sku_on_change,
        column_order=tampil,
        column_config={
            **{c: st.column_config.TextColumn(label_kolom(c))
               for c in view.columns if c not in (tag, "Pilih")},
            "Pilih": st.column_config.CheckboxColumn("Select", width=50, default=False),
            tag: st.column_config.SelectboxColumn(label_kolom(tag), options=SKU_TAGGING,
                                                  required=True, width=130),
        },
        disabled=[c for c in view.columns if c not in (tag, "Pilih")],
        hide_index=True, num_rows="fixed", width="stretch",
        height=min(len(view) * 35 + 38, 560),
    )

def halaman_validasi_toko():
    ss = st.session_state
    st.markdown(komponen.html_header("Store Validation", "Review and update store details before submission."),
                unsafe_allow_html=True)
    _pertahankan(KUNCI_SKU if ss.tab_validasi == "noo" else KUNCI_NOO)

    n = len(ss.sku_pending)
    c1, c2, _ = st.columns([0.8, 1.8, 8])
    c1.button("NOO", key="pill_noo", width="stretch", on_click=_pilih_tab, args=("noo",),
              type="primary" if ss.tab_validasi == "noo" else "secondary")
    c2.button("SKU Mapping" + (f"  ● {n}" if n else ""), key="pill_sku", width="stretch",
              on_click=_pilih_tab, args=("sku",),
              type="primary" if ss.tab_validasi == "sku" else "secondary")

    if ss.tab_validasi == "noo":
        tab_noo()
    else:
        _tab_sku()
# ═══════════════════════════════════════
# HALAMAN: DASHBOARD
# ═══════════════════════════════════════
@swr_cache(ttl=60)
def ambil_data_dashboard() -> dict:
    rows_noo = sb_select(TABLE_SUBMIT_NOO, order="created_at.desc", limit=1000)
    df_noo   = pd.DataFrame(rows_noo) if rows_noo else pd.DataFrame()
    try:
        r      = requests.get(f"{SUPABASE_URL}/rest/v1/sku_mapping",
                               headers={**_SB_HEADERS,"Prefer":""},
                               params={"select":"*","limit":2000,"order":"created_at.desc"},timeout=10)
        rows_sku = r.json() if r.ok else []
    except Exception:
        rows_sku = []
    df_sku = pd.DataFrame(rows_sku) if rows_sku else pd.DataFrame()
    return {"noo":df_noo,"sku":df_sku}



def _brand_dari_cc(cc: str) -> str:
    s = str(cc).strip()
    if s.startswith("12"): return "G2G"
    if s.startswith("17"): return "NXP"
    return "Other"


# ═══════════════════════════════════════
# FEEDBACK (tiket Help & Support dari app submission)
# ═══════════════════════════════════════
TABLE_HELP = "help_support"
FB_PER_HALAMAN = 5
FB_FILTER = ["All", "In Progress", "Solved"]

@st.cache_data(ttl=30, show_spinner=False)
def ambil_semua_tiket_help() -> pd.DataFrame:
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{TABLE_HELP}",
                     headers={**_SB_HEADERS, "Prefer": ""},
                     params={"select": "*", "order": "generated_at.desc", "limit": 1000}, timeout=20)
    r.raise_for_status()
    return pd.DataFrame(r.json())

def _status_fb(v) -> str:
    """Status di DB -> 'Solved' atau 'In Progress' (Open dst dianggap In Progress)."""
    return "Solved" if str(v).strip().lower() in ("solved", "closed", "done", "resolved") else "In Progress"

def _tandai_solved(id_tiket) -> str:
    """Set status tiket jadi 'Solved'. Return pesan error ('' kalau sukses)."""
    try:
        r = requests.patch(f"{SUPABASE_URL}/rest/v1/{TABLE_HELP}",
                           params={"id": f"eq.{id_tiket}"}, json={"status": "Solved"},
                           headers={**_SB_HEADERS, "Prefer": "return=representation"}, timeout=15)
        if not r.ok:
            return f"HTTP {r.status_code}: {r.text[:200]}"
        if r.json() == []:
            return "No ticket was updated."
        ambil_semua_tiket_help.clear()
        return ""
    except Exception as e:
        return str(e)

def _fb_reset_halaman():
    st.session_state.fb_halaman = 1

def _fb_ke_halaman(n):
    st.session_state.fb_halaman = n

def tab_feedback():
    import html as _html
    ss = st.session_state
    ss.setdefault("fb_halaman", 1)
    ss.setdefault("fb_filter", "All")

    st.markdown('<div class="fb-judul">Feedback</div>'
                '<div class="fb-sub">Tickets submitted from Help &amp; Support </div>',
                unsafe_allow_html=True)
    try:
        df = ambil_semua_tiket_help()
    except Exception as e:
        st.error(f"Failed to load feedback: {e}")
        return

    # susun data
    if df.empty:
        items = []
    else:
        waktu = pd.to_datetime(df.get("generated_at"), errors="coerce", utc=True, format="ISO8601").dt.tz_convert(ZONA_WAKTU)
        items = []
        for i, (_, r) in enumerate(df.iterrows()):
            no = r.get("id")
            items.append({
                "id":     (no.item() if hasattr(no, "item") else no) if pd.notna(no) else None,
                "no":     str(no) if pd.notna(no) else str(len(df) - i),
                "title":  str(r.get("title", "") or "").strip(),
                "issue":  str(r.get("issue", "")).strip(),
                "user":   str(r.get("username", "")),
                "wa":     re.sub(r"\D", "", str(r.get("nomor_wa", "") or "")),   # hanya angka
                "prio":   str(r.get("priority", "")),
                "status": _status_fb(r.get("status")),
                "waktu":  waktu.iloc[i],
            })

    with st.container(key="fb_kartu"):
        c_filter, c_cari = st.columns([2, 1], vertical_alignment="center")
        c_filter.segmented_control("Status", FB_FILTER, key="fb_filter", label_visibility="collapsed",
                                   on_change=_fb_reset_halaman)
        kata = c_cari.text_input("Search", key="fb_cari", placeholder="Search feedback...",
                                 label_visibility="collapsed", on_change=_fb_reset_halaman)

        pilih = ss.get("fb_filter") or "All"
        tampil = [x for x in items
                  if (pilih == "All" or x["status"] == pilih)
                  and (not kata or kata.lower() in f'{x["title"]} {x["issue"]} {x["user"]} {x["wa"]} #{x["no"]}'.lower())]

        total = len(tampil)
        n_hal = max(1, math.ceil(total / FB_PER_HALAMAN))
        ss.fb_halaman = min(max(1, ss.fb_halaman), n_hal)
        awal = (ss.fb_halaman - 1) * FB_PER_HALAMAN
        potong = tampil[awal:awal + FB_PER_HALAMAN]

        if not potong:
            st.markdown('<div class="fb-kosong">No feedback found.</div>', unsafe_allow_html=True)
        else:
            baris = []
            for x in potong:
                isi1  = x["issue"].splitlines()[0] if x["issue"] else "(no description)"
                judul = x["title"] or isi1
                if len(judul) > 90:
                    judul = judul[:87] + "…"
                tgl = "" if pd.isna(x["waktu"]) else f'{x["waktu"].strftime("%b")} {x["waktu"].day}, {x["waktu"].year} • {x["waktu"].strftime("%H:%M")}'
                kelas = "solved" if x["status"] == "Solved" else "progress"
                baris.append(
                    f'<div class="fb-baris"><div class="fb-ikon"><span class="mi">chat_bubble_outline</span></div>'
                    f'<div class="fb-isi"><div class="fb-teks">{_html.escape(judul)}</div>'
                    f'<div class="fb-meta"><span class="fb-no">#{_html.escape(x["no"])}</span>'
                    f'<span>{_html.escape(x["user"])}</span><span>{_html.escape(x["prio"])}</span>'
                    + (f'<span>WA: <a href="https://wa.me/{x["wa"]}" target="_blank" rel="noopener" '
                       f'style="color:inherit">{x["wa"]}</a></span>' if x["wa"] else '<span>WA: -</span>')
                    + (f'<span>· {_html.escape(isi1[:70])}</span>' if x["title"] else '') +
                    '</div></div>'
                    f'<div class="fb-kanan"><span class="fb-badge {kelas}">● {x["status"]}</span>'
                    f'<div class="fb-tgl">{tgl}</div></div></div>')
            st.markdown("".join(baris), unsafe_allow_html=True)

            # ubah In Progress -> Solved (tiket di halaman ini yang punya id)
            is_bd = (ss.get("user") or {}).get("tagging") in ("BD SUPPORT", "BD_SUPPORT")
            bisa = [x for x in potong if is_bd and x["status"] == "In Progress" and x["id"] is not None]
            if bisa:
                st.markdown('<div style="height:14px"></div>', unsafe_allow_html=True)
                c_pilih, c_btn = st.columns([3, 1], vertical_alignment="bottom")
                pilih_t = c_pilih.selectbox(
                    "Mark ticket as solved", bisa, key="fb_pilih_tiket",
                    format_func=lambda x: f'#{x["no"]} — {x["title"] or (x["issue"].splitlines()[0][:60] if x["issue"] else "")}')
                if c_btn.button(":material/check: Mark as Solved", key="fb_btn_solved", width="stretch"):
                    err = _tandai_solved(pilih_t["id"])
                    if err:
                        st.error(f"Failed to update: {err}")
                    else:
                        ss.pesan_toast = f'Ticket #{pilih_t["no"]} marked as Solved.'
                        st.rerun()

        akhir = min(awal + FB_PER_HALAMAN, total)
        with st.container(key="fb_footer"):          # satu baris: info di kiri, pagination kecil di kanan (tidak boleh turun baris)
            c_info, b1, b2, b3 = st.columns([10, 1, 1, 1], gap="xxsmall", vertical_alignment="center")
            c_info.markdown(f'<div class="fb-info">Showing {awal + 1 if total else 0}-{akhir} of {total} feedback</div>',
                            unsafe_allow_html=True)
            b1.button("‹", key="fb_prev", on_click=_fb_ke_halaman, args=(ss.fb_halaman - 1,),
                      disabled=ss.fb_halaman <= 1, width="stretch")
            b2.button(str(ss.fb_halaman), key="fb_cur", disabled=True, width="stretch")   # nomor halaman: tombol seukuran ‹ ›
            b3.button("›", key="fb_next", on_click=_fb_ke_halaman, args=(ss.fb_halaman + 1,),
                      disabled=ss.fb_halaman >= n_hal, width="stretch")



# ── Grafik native (Vega/Altair, tanpa unduh library dari CDN & tanpa iframe) ──
@st.cache_data(max_entries=6, show_spinner=False)
def _excel_cache(df, startrow=1):
    return hasil_ke_excel(df, startrow=startrow)

def halaman_dashboard_tabs():
    with st.container(key="dash"):
        tab_dash, tab_fb = st.tabs([":material/trending_up: Dashboard SKU & NOO", ":material/chat: Feedback"])
        with tab_dash:
            halaman_dashboard()
        with tab_fb:
            tab_feedback()


def halaman_dashboard():
    ss = st.session_state
    st.markdown(komponen.html_header("Dashboard","Monitor NOO submission & SKU Mapping coverage.",":material/trending_up:"),
                unsafe_allow_html=True)
    with st.spinner("Loading dashboard data..."):
        data = ambil_data_dashboard()
    df_noo = data["noo"]
    df_sku = data["sku"]

    norm = lambda s: re.sub(r"[^a-z0-9]","",str(s).lower())
    peta_noo = {norm(c):c for c in df_noo.columns} if not df_noo.empty else {}
    peta_sku = {norm(c):c for c in df_sku.columns} if not df_sku.empty else {}

    col_tagging     = peta_noo.get("tagging")
    col_distri      = peta_noo.get("groupname") or peta_noo.get("customerstorename") or peta_noo.get("distributor")
    col_city        = peta_noo.get("city")
    col_created_noo = peta_noo.get("createdat") or peta_noo.get("created_at")
    col_cc_sku      = peta_sku.get("customercode") or peta_sku.get("customer_code")
    col_prod_sku    = peta_sku.get("product")
    col_cpc_sku     = peta_sku.get("customerproductcode") or peta_sku.get("customer_product_code")
    col_cpn_sku     = peta_sku.get("customerproductname") or peta_sku.get("customer_product_name")
    col_created_sku = peta_sku.get("createdat") or peta_sku.get("created_at")

    if not df_sku.empty and col_cc_sku:
        df_sku = df_sku.copy()
        df_sku["_brand"] = df_sku[col_cc_sku].astype(str).apply(_brand_dari_cc)

    st.markdown('<div style="height:4px"></div>', unsafe_allow_html=True)
    f1,f2,f3,f4,f5 = st.columns([1.4,1.4,1.4,1.4,0.8])
    distri_opts = ["All Distributor"] + daftar_distributor()          # dari Distributor Mapping, Status = Active
    f_distri   = f1.selectbox("Distributor", distri_opts, key="dash_distri")
    brand_opts = ["All Brand", "G2G", "NXP"]
    if not df_sku.empty and "Other" in df_sku["_brand"].values:
        brand_opts.append("Other")
    f_brand    = f2.selectbox("Brand (SKU)", brand_opts, key="dash_brand")
    hari_ini   = datetime.datetime.now(ZONA_WAKTU).date()
    f_tgl_dari  = f3.date_input("Period from", value=hari_ini-datetime.timedelta(days=30),
                                 key="dash_dari", format="YYYY-MM-DD")
    f_tgl_sampai = f4.date_input("Period to", value=hari_ini, key="dash_sampai", format="YYYY-MM-DD")
    with f5:
        st.markdown('<div style="height:1.7rem"></div>', unsafe_allow_html=True)
        if st.button(":material/refresh: Refresh", key="dash_refresh", width="stretch"):
            ambil_data_dashboard.clear(); st.rerun()

    df_noo_f = df_noo.copy() if not df_noo.empty else df_noo
    df_sku_f = df_sku.copy() if not df_sku.empty else df_sku
    if not df_noo_f.empty and col_created_noo:
        tgl_noo  = ke_wib_naive(df_noo_f[col_created_noo]).dt.date
        df_noo_f = df_noo_f[(tgl_noo >= f_tgl_dari) & (tgl_noo <= f_tgl_sampai)]
    if not df_noo_f.empty and col_distri and f_distri != "All Distributor":
        df_noo_f = df_noo_f[df_noo_f[col_distri].astype(str).str.strip().str.lower()
                            .str.startswith(f_distri.strip().lower())]
    if not df_sku_f.empty and col_created_sku:
        tgl_sku  = ke_wib_naive(df_sku_f[col_created_sku]).dt.date
        df_sku_f = df_sku_f[(tgl_sku >= f_tgl_dari) & (tgl_sku <= f_tgl_sampai)]
    if not df_sku_f.empty and f_brand != "All Brand":
        df_sku_f = df_sku_f[df_sku_f["_brand"] == f_brand]

    st.markdown('<hr style="margin:8px 0 16px 0;border:none;border-top:1px solid #e2e8f0">', unsafe_allow_html=True)

    # NOO 
    st.markdown('<p style="font-size:1.05rem;font-weight:700;color:#0f172a;margin-bottom:12px">NOO Submission</p>',
                unsafe_allow_html=True)
    if df_noo_f.empty:
        st.info("No NOO data in this period.")
    else:
        tagging    = df_noo_f[col_tagging].astype(str).str.strip() if col_tagging else pd.Series(dtype=str)
        total_noo  = len(df_noo_f)
        n_generate = int((tagging == "Generate ID").sum())
        n_review   = int((tagging == "Need Review").sum())
        n_existing = int((tagging == "Existing").sum())
        pct_gen    = round(n_generate/total_noo*100) if total_noo else 0
        pct_rev    = round(n_review/total_noo*100) if total_noo else 0
        pct_ext    = round(n_existing/total_noo*100) if total_noo else 0

        st.markdown(f"""
<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:8px;width:66.6%">
  <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:9px 12px;min-height:78px;box-sizing:border-box">
    <div style="font-size:0.75rem;color:#64748b;font-weight:500;margin-bottom:3px">Total Stores</div>
    <div style="font-size:1.35rem;font-weight:800;color:#0f172a;line-height:1">{total_noo:,}</div>
  </div>
  <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:9px 12px;min-height:78px;box-sizing:border-box">
    <div style="font-size:0.75rem;color:#15803d;font-weight:500;margin-bottom:3px">Generate ID</div>
    <div style="font-size:1.35rem;font-weight:800;color:#166534;line-height:1">{n_generate:,}</div>
    <div style="font-size:0.66rem;color:#16a34a;margin-top:2px">▲ {pct_gen}% dari total</div>
  </div>
  <div style="background:#fff1f2;border:1px solid #fecdd3;border-radius:10px;padding:9px 12px;min-height:78px;box-sizing:border-box">
    <div style="font-size:0.75rem;color:#be123c;font-weight:500;margin-bottom:3px">Need Review</div>
    <div style="font-size:1.35rem;font-weight:800;color:#9f1239;line-height:1">{n_review:,}</div>
    <div style="font-size:0.66rem;color:#e11d48;margin-top:2px">{pct_rev}% dari total</div>
  </div>
  <div style="background:#fefce8;border:1px solid #fde68a;border-radius:10px;padding:9px 12px;min-height:78px;box-sizing:border-box">
    <div style="font-size:0.75rem;color:#a16207;font-weight:500;margin-bottom:3px">Existing</div>
    <div style="font-size:1.35rem;font-weight:800;color:#854d0e;line-height:1">{n_existing:,}</div>
    <div style="font-size:0.66rem;color:#ca8a04;margin-top:2px">{pct_ext}% dari total</div>
  </div>
</div>
        """, unsafe_allow_html=True)
        cn1,cn2 = st.columns(2)
        with cn1:
            with st.container(key="g_tag"):
                st.markdown('<p style="font-size:0.85rem;font-weight:700;color:#334155;text-align:center;margin:0 0 6px 0">NOO by Tagging Status</p>',
                            unsafe_allow_html=True)
                if col_tagging:
                    chart_tag = tagging.value_counts()
                    labels    = chart_tag.index.tolist()
                    values    = chart_tag.values.tolist()
                    WARNA_TAG = {"Generate ID":"#475569","Need Review":"#94a3b8","Existing":"#cbd5e1"}   # abu tua -> abu muda
                    colors    = [WARNA_TAG.get(l,"#94a3b8") for l in labels]
                    st.components.v1.html(f"""
<link href="https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@400;500;600&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<canvas id="donut_noo" style="max-height:140px"></canvas>
<script>
Chart.defaults.font.family='\"Source Sans 3\",\"Source Sans Pro\",sans-serif';Chart.defaults.color='#475569';
document.fonts.load('12px \"Source Sans 3\"').finally(()=>new Chart(document.getElementById('donut_noo'),{{
  type:'doughnut',
  data:{{labels:{json.dumps(labels)},datasets:[{{data:{json.dumps(values)},backgroundColor:{json.dumps(colors)},borderWidth:1}}]}},
  options:{{responsive:true,plugins:{{legend:{{position:'right',labels:{{font:{{size:10}},boxWidth:10,padding:6}}}}}}}}
}}));
</script>""", height=150)
        with cn2:
            with st.container(key="g_distri"):
                st.markdown('<p style="font-size:0.85rem;font-weight:700;color:#334155;text-align:center;margin:0 0 6px 0">NOO by Distributor (Top 10)</p>',
                            unsafe_allow_html=True)
                if col_distri:
                    per_distri  = (df_noo_f[col_distri].replace("","(undetected)").fillna("(undetected)").value_counts().head(10))
                    labels_d    = per_distri.index.tolist()
                    values_d    = per_distri.values.tolist()
                    st.components.v1.html(f"""
<link href="https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@400;500;600&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<canvas id="bar_distri" style="max-height:140px"></canvas>
<script>
Chart.defaults.font.family='\"Source Sans 3\",\"Source Sans Pro\",sans-serif';Chart.defaults.color='#475569';
document.fonts.load('12px \"Source Sans 3\"').finally(()=>new Chart(document.getElementById('bar_distri'),{{
  type:'bar',
  data:{{labels:{json.dumps(labels_d)},datasets:[{{data:{json.dumps(values_d)},backgroundColor:'#94a3b8',borderRadius:4}}]}},
  options:{{indexAxis:'y',responsive:true,plugins:{{legend:{{display:false}}}},scales:{{x:{{grid:{{display:false}},ticks:{{font:{{size:10}}}}}},y:{{grid:{{display:false}},ticks:{{font:{{size:10}}}}}}}}}}
}}));
</script>""", height=150)
        with st.expander("Recent NOO Submissions (last 20)"):
            kolom_show = [c for c in [peta_noo.get("customerstorecode"),peta_noo.get("storename"),
                                       col_distri,col_city,col_tagging,col_created_noo] if c]
            if kolom_show:
                df_show = df_noo_f[kolom_show].head(20).copy()
                if col_created_noo in df_show.columns:
                    df_show[col_created_noo] = ke_wib_naive(df_show[col_created_noo]).dt.strftime("%d %b %Y %H:%M")
                st.dataframe(df_show, hide_index=True, width="stretch")

    st.markdown('<hr style="margin:20px 0;border:none;border-top:1px solid #e2e8f0">', unsafe_allow_html=True)

    # ── SKU
    st.markdown('<p style="font-size:1.05rem;font-weight:700;color:#0f172a;margin-bottom:12px">SKU Mapping</p>',
                unsafe_allow_html=True)
    if df_sku_f.empty:
        st.info("No SKU Mapping data in this period.")
    else:
        total_sku    = len(df_sku_f)
        n_distri_sku = df_sku_f[col_cc_sku].nunique() if col_cc_sku else 0
        sku_baru     = 0
        if col_created_sku:
            tgl_s  = ke_wib_naive(df_sku_f[col_created_sku]).dt.date
            cutoff7 = hari_ini - datetime.timedelta(days=7)
            sku_baru = int((tgl_s >= cutoff7).sum())
        brand_counts = df_sku_f["_brand"].value_counts() if "_brand" in df_sku_f.columns else pd.Series(dtype=int)
        brand_list   = brand_counts.index.tolist()
        # satu baris; lebar tiap kartu = 1/6 baris (sama dengan kartu NOO)
        n_kartu  = 3 + len(brand_list)
        kpi_cols = st.columns([1]*n_kartu + ([6 - n_kartu] if n_kartu < 6 else []), gap="small")
        kpi_cols[0].markdown(f"""
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:9px 12px;min-height:78px;box-sizing:border-box">
  <div style="font-size:0.75rem;color:#64748b;font-weight:500;margin-bottom:3px">Total SKU Mapped</div>
  <div style="font-size:1.35rem;font-weight:800;color:#0f172a;line-height:1">{total_sku:,}</div>
</div>""", unsafe_allow_html=True)
        kpi_cols[1].markdown(f"""
<div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:10px;padding:9px 12px;min-height:78px;box-sizing:border-box">
  <div style="font-size:0.75rem;color:#0369a1;font-weight:500;margin-bottom:3px">SKU Baru (7 hari)</div>
  <div style="font-size:1.35rem;font-weight:800;color:#0c4a6e;line-height:1">{sku_baru:,}</div>
</div>""", unsafe_allow_html=True)
        kpi_cols[2].markdown(f"""
<div style="background:#faf5ff;border:1px solid #e9d5ff;border-radius:10px;padding:9px 12px;min-height:78px;box-sizing:border-box">
  <div style="font-size:0.75rem;color:#7c3aed;font-weight:500;margin-bottom:3px">Distributor</div>
  <div style="font-size:1.35rem;font-weight:800;color:#4c1d95;line-height:1">{n_distri_sku:,}</div>
</div>""", unsafe_allow_html=True)
        BRAND_COLORS = {"G2G":"#6366f1","NXP":"#f59e0b","Other":"#94a3b8"}
        for idx, brand in enumerate(brand_list):
            cnt = int(brand_counts[brand])
            pct = round(cnt/total_sku*100) if total_sku else 0
            clr = BRAND_COLORS.get(brand,"#94a3b8")
            kpi_cols[3+idx].markdown(f"""
<div style="background:#fff;border:1px solid {clr}40;border-radius:10px;padding:9px 12px;min-height:78px;box-sizing:border-box">
  <div style="font-size:0.75rem;color:{clr};font-weight:600;margin-bottom:3px">{brand}</div>
  <div style="font-size:1.35rem;font-weight:800;color:#0f172a;line-height:1">{cnt:,}</div>
  <div style="font-size:0.66rem;color:#64748b;margin-top:2px">{pct}% dari total</div>
</div>""", unsafe_allow_html=True)

        st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)
        cs1,cs2 = st.columns(2)
        with cs1:
            with st.container(key="g_brand"):
                st.markdown('<p style="font-size:0.85rem;font-weight:700;color:#334155;text-align:center;margin:0 0 6px 0">SKU per Brand</p>',
                            unsafe_allow_html=True)
                if "_brand" in df_sku_f.columns:
                    labels_b = brand_counts.index.tolist()
                    values_b = brand_counts.values.tolist()
                    colors_b = (["#475569", "#94a3b8", "#cbd5e1", "#64748b", "#e2e8f0"][i % 5] for i in range(len(labels_b)))
                    colors_b = list(colors_b)
                    st.components.v1.html(f"""
<link href="https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@400;500;600&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<canvas id="donut_brand" style="max-height:140px"></canvas>
<script>
Chart.defaults.font.family='\"Source Sans 3\",\"Source Sans Pro\",sans-serif';Chart.defaults.color='#475569';
document.fonts.load('12px \"Source Sans 3\"').finally(()=>new Chart(document.getElementById('donut_brand'),{{
  type:'doughnut',
  data:{{labels:{json.dumps(labels_b)},datasets:[{{data:{json.dumps(values_b)},backgroundColor:{json.dumps(colors_b)},borderWidth:1}}]}},
  options:{{responsive:true,plugins:{{legend:{{position:'right',labels:{{font:{{size:10}},boxWidth:10,padding:6}}}}}}}}
}}));
</script>""", height=150)
        with cs2:
            with st.container(key="g_waktu"):
                st.markdown('<p style="font-size:0.85rem;font-weight:700;color:#334155;text-align:center;margin:0 0 6px 0">SKU Submitted Over Time</p>',
                            unsafe_allow_html=True)
                if col_created_sku:
                    per_tgl = ke_wib_naive(df_sku_f[col_created_sku]).dt.date.value_counts().sort_index()
                    if not per_tgl.empty:
                        labels_t = [str(d) for d in per_tgl.index.tolist()]
                        values_t = per_tgl.values.tolist()
                        st.components.v1.html(f"""
<link href="https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@400;500;600&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<canvas id="line_sku" style="max-height:140px"></canvas>
<script>
Chart.defaults.font.family='\"Source Sans 3\",\"Source Sans Pro\",sans-serif';Chart.defaults.color='#475569';
document.fonts.load('12px \"Source Sans 3\"').finally(()=>new Chart(document.getElementById('line_sku'),{{
  type:'line',
  data:{{labels:{json.dumps(labels_t)},datasets:[{{data:{json.dumps(values_t)},borderColor:'#64748b',backgroundColor:'#cbd5e155',borderWidth:2,pointRadius:3,fill:true,tension:0.3}}]}},
  options:{{responsive:true,plugins:{{legend:{{display:false}}}},scales:{{x:{{grid:{{display:false}},ticks:{{font:{{size:10}},maxRotation:0}}}},y:{{grid:{{display:false}},ticks:{{font:{{size:10}},stepSize:1}}}}}}}}
}}));
</script>""", height=150)

        with st.expander("All SKU Mapping (last 20)"):
            kolom_show_sku = [c for c in [col_cc_sku,peta_sku.get("customername") or peta_sku.get("customer_name"),
                                           col_prod_sku,col_cpc_sku,col_cpn_sku,col_created_sku] if c]
            if kolom_show_sku:
                df_show_sku = df_sku_f[kolom_show_sku].head(20).copy()
                if col_created_sku in df_show_sku.columns:
                    df_show_sku[col_created_sku] = ke_wib_naive(df_show_sku[col_created_sku]).dt.strftime("%d %b %Y %H:%M")
                st.dataframe(df_show_sku, hide_index=True, width="stretch")
        st.markdown(f'<div style="font-size:0.72rem;color:#94a3b8;margin-top:8px">Last updated: {ss.terakhir_update}</div>',
                    unsafe_allow_html=True)

# ═════════════════════════════════════════════════════════════════════════════
# HALAMAN: HISTORY
# ═════════════════════════════════════════════════════════════════════════════

@swr_cache(ttl=30)
def _riwayat_cache():
    return sb_select(TABLE_RIWAYAT, order="waktu.desc", limit=200)

def halaman_riwayat():
    st.markdown(komponen.html_header("Submission History","Log of all submission and validation activities.",":material/history:"),
                unsafe_allow_html=True)
    if st.button(":material/refresh: Refresh", key="refresh_riwayat"):
        _riwayat_cache.clear()
    rows = _riwayat_cache()
    if rows:
        df_riwayat = pd.DataFrame(rows)
        kolom_tampil = {"waktu":"Waktu","pengguna":"Pengguna","aksi":"Aksi","detail":"Detail","total":"Total"}
        kolom_ada    = [k for k in kolom_tampil if k in df_riwayat.columns]
        df_riwayat   = df_riwayat[kolom_ada].rename(columns=kolom_tampil)
        if "Waktu" in df_riwayat.columns:
            df_riwayat["Waktu"] = ke_wib_naive(df_riwayat["Waktu"]).dt.strftime("%d %b %Y %H:%M")
        st.dataframe(df_riwayat, width="stretch", hide_index=True)
    else:
        st.info("No history yet.")

# ═════════════════════════════════════════════════════════════════════════════
# HALAMAN: SETTINGS
# ═════════════════════════════════════════════════════════════════════════════

def _callback_ukuran_halaman():
    st.session_state.ukuran_halaman = st.session_state.atur_ukuran_halaman
    st.session_state.halaman_data   = 1

TABLE_USER = "users"
TAGGING_BD = "BD SUPPORT"

@swr_cache(ttl=60)
def _sb_users_list() -> list:
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{TABLE_USER}",
        headers={**_SB_HEADERS, "Prefer": ""},
        params={"select": "username,email,tagging", "order": "created_at.desc"},
        timeout=10)
    r.raise_for_status()
    return r.json()

def _sb_user_delete(username: str) -> bool:
    r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{TABLE_USER}",
        headers={**_SB_HEADERS, "Prefer": "return=representation"},
        params={"username": f"eq.{username}"}, timeout=10)
    if r.ok and len(r.json()) == 1:
        return True
    st.session_state["_err_del"] = f"HTTP {r.status_code}, rows={r.text[:150]}"
    return False

def _sb_user_reset_pw(username: str, pw_baru: str) -> bool:
    hashed = bcrypt.hashpw(pw_baru.encode(), bcrypt.gensalt()).decode()
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{TABLE_USER}",
        headers={**_SB_HEADERS, "Prefer": "return=representation"},
        params={"username": f"eq.{username}"},
        json={"password": hashed}, timeout=10)
    return r.ok and len(r.json()) == 1
@st.cache_data(ttl=20, show_spinner=False)
def _sb_riwayat_hitung() -> int:
    """Jumlah baris di tabel riwayat_noo (-1 kalau gagal dihitung)."""
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{TABLE_RIWAYAT}",
                         headers={**_SB_HEADERS, "Prefer": "count=exact", "Range-Unit": "items", "Range": "0-0"},
                         params={"select": "waktu"}, timeout=15)
        if r.status_code not in (200, 206):
            return -1
        return int(r.headers.get("Content-Range", "*/-1").split("/")[-1])
    except Exception:
        return -1

def _sb_riwayat_hapus(lebih_lama_dari_hari=None):
    """Hapus baris riwayat_noo. lebih_lama_dari_hari=None -> SEMUA baris. Return (jumlah_terhapus, pesan_error)."""
    if lebih_lama_dari_hari is None:
        params = {"waktu": "not.is.null"}
    else:
        batas = datetime.datetime.now(ZONA_WAKTU) - datetime.timedelta(days=int(lebih_lama_dari_hari))
        params = {"waktu": f"lt.{batas.isoformat(timespec='seconds')}"}
    try:
        r = requests.delete(f"{SUPABASE_URL}/rest/v1/{TABLE_RIWAYAT}",
                            headers={**_SB_HEADERS, "Prefer": "return=representation"},
                            params=params, timeout=30)
        if not r.ok:
            return 0, f"HTTP {r.status_code}: {r.text[:200]}"
        return len(r.json()), ""
    except Exception as e:
        return 0, str(e)

@st.cache_data(ttl=20, show_spinner=False)
def _sb_tabel_hitung(tabel: str, kolom: str) -> int:
    """Jumlah baris sebuah tabel Supabase (-1 kalau gagal dihitung)."""
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{tabel}",
                         headers={**_SB_HEADERS, "Prefer": "count=exact", "Range-Unit": "items", "Range": "0-0"},
                         params={"select": kolom}, timeout=15)
        if r.status_code not in (200, 206):
            return -1
        return int(r.headers.get("Content-Range", "*/-1").split("/")[-1])
    except Exception:
        return -1

def _sb_tabel_hapus(tabel: str, params: dict):
    """DELETE baris yang cocok dengan params (filter PostgREST). Return (jumlah_terhapus, pesan_error)."""
    try:
        r = requests.delete(f"{SUPABASE_URL}/rest/v1/{tabel}",
                            headers={**_SB_HEADERS, "Prefer": "return=representation"}, params=params, timeout=30)
        if not r.ok:
            return 0, f"HTTP {r.status_code}: {r.text[:200]}"
        return len(r.json()), ""
    except Exception as e:
        return 0, str(e)

def _sb_riwayat_reset_id() -> str:
    """Panggil fungsi SQL reset_riwayat_noo_id() di Supabase. Return pesan error ('' kalau sukses)."""
    try:
        r = requests.post(f"{SUPABASE_URL}/rest/v1/rpc/reset_riwayat_noo_id", headers=_SB_HEADERS, json={}, timeout=15)
        if r.status_code == 404:
            return ("Function reset_riwayat_noo_id() not found in the Supabase API "
                    f"(project: {SUPABASE_URL}). Supabase says: {r.text[:300]}")
        if not r.ok:
            return f"HTTP {r.status_code}: {r.text[:200]}"
        return ""
    except Exception as e:
        return str(e)

@st.fragment
def _tab_clear_history():
    """Isi tab Clear history. Fragment: klik centang/tombol di sini hanya me-render ulang tab ini,
    bukan seluruh halaman Settings (daftar user dll.), jadi tidak terasa lambat."""
    ss = st.session_state
    # Streamlit membuang nilai widget yang tidak ditampilkan (mis. saat pindah halaman).
    # Simpan salinan nilainya dan pulihkan saat tab ini tampil lagi.
    _keep = ss.setdefault("_keep_clear", {})
    for _k in ("riw_hari", "riw_yakin_lama", "riw_yakin_semua", "riw_sekalian_reset",
               "hs_yakin_solved", "hs_hari", "hs_yakin_lama", "hs_yakin_semua"):
        if _k in ss:
            _keep[_k] = ss[_k]
        elif _k in _keep:
            ss[_k] = _keep[_k]
    if hasil := ss.pop("riw_hasil", None):
        (st.error if hasil[0] == "error" else st.success)(hasil[1])
    jumlah = _sb_riwayat_hitung()
    st.caption(f"Table `{TABLE_RIWAYAT}` currently has **{jumlah if jumlah >= 0 else '?'}** record(s).")

    st.markdown("**Delete old records**")
    c_hari, c_btn, _sp1 = st.columns([1.1, 1.9, 3], vertical_alignment="bottom")
    hari = c_hari.number_input("Older than (days)", min_value=1, max_value=3650, value=30, step=1, key="riw_hari")
    yakin_lama = st.checkbox(f"I understand records older than {int(hari)} days will be permanently deleted",
                             key="riw_yakin_lama")
    if c_btn.button(":material/delete_sweep: Delete old records", key="riw_btn_lama", disabled=not yakin_lama,
                    width="stretch"):
        n, err = _sb_riwayat_hapus(int(hari))
        if err:
            ss.riw_hasil = ("error", f"Delete failed: {err}")
        elif n == 0:
            ss.riw_hasil = ("success", "No records older than that. If you expected some, check the Supabase "
                                       "DELETE policy on riwayat_noo (RLS).")
        else:
            # (aksi Clear history sengaja TIDAK dicatat ke riwayat_noo, supaya tabel tetap kosong & id tetap ter-reset)
            ss.riw_hasil = ("success", f"{n} record(s) deleted.")
        st.rerun()

    st.divider()
    st.markdown("**Delete ALL history**")
    yakin_semua = st.checkbox("I understand ALL history records will be permanently deleted (cannot be undone)",
                              key="riw_yakin_semua")
    sekalian_reset = st.checkbox("Also reset the id counter to 1 after deleting", value=True, key="riw_sekalian_reset")
    if st.button(":material/delete_forever: Delete all history", type="primary", key="riw_btn_semua",
                 disabled=not yakin_semua):
        n, err = _sb_riwayat_hapus(None)
        if err:
            ss.riw_hasil = ("error", f"Delete failed: {err}")
        elif n == 0 and jumlah > 0:
            ss.riw_hasil = ("error", "Supabase accepted the request but deleted 0 rows: the table has no DELETE "
                                     "policy for this API key (RLS). Add a DELETE policy on riwayat_noo.")
        else:
            pesan = f"{n} record(s) deleted."
            if sekalian_reset:
                err_r = _sb_riwayat_reset_id()
                pesan += " Id counter reset." if not err_r else f" Id reset failed: {err_r}"
            # (aksi Clear history sengaja TIDAK dicatat ke riwayat_noo, supaya tabel tetap kosong & id tetap ter-reset)
            ss.riw_hasil = ("success" if not (sekalian_reset and err_r) else "error", pesan)
        st.rerun()

    st.divider()
    st.markdown("**Reset id counter**")
    st.caption("Next new history record gets the next free number: 1 if the table is empty, otherwise "
               "(highest existing id + 1). Existing records are not touched.")
    if st.button(":material/restart_alt: Reset id counter", key="riw_btn_reset_id"):
        err_r = _sb_riwayat_reset_id()
        ss.riw_hasil = ("error", f"Reset failed: {err_r}") if err_r else ("success", "Id counter reset.")
        st.rerun()
    with st.expander(":material/code: One-time setup (run once in Supabase SQL Editor)"):
        st.code("""create or replace function public.reset_riwayat_noo_id()
returns void language plpgsql security definer as $$
begin
  perform setval(
    pg_get_serial_sequence('public.riwayat_noo', 'id'),
    coalesce((select max(id) from public.riwayat_noo), 0) + 1,
    false
  );
end $$;

grant execute on function public.reset_riwayat_noo_id() to anon, authenticated;

-- wajib: minta Supabase API memuat ulang daftar fungsi, kalau tidak fungsi baru tidak terbaca (error 404)
notify pgrst, 'reload schema';""", language="sql")
        st.caption("Assumes the table has an auto-increment column named `id`. If your column has another name, "
                   "replace `'id'` and `max(id)` above.")

    with st.expander(":material/schedule: Fully automatic cleanup (runs even when nobody opens the app)"):
        st.markdown("Streamlit cannot run background jobs, so the buttons above are manual. To clean up on a "
                    "schedule, enable the `pg_cron` extension in Supabase and run this once in the SQL Editor "
                    "(deletes records older than 30 days, every day at 02:00):")
        st.code("""select cron.schedule(
  'clean-riwayat-noo', '0 2 * * *',
  $$ delete from public.riwayat_noo where waktu < now() - interval '30 days' $$
);""", language="sql")
        st.caption("Cancel it with: select cron.unschedule('clean-riwayat-noo');")

    # ── Tiket Help & Support (tabel help_support) ──
    st.divider()
    st.markdown("#### Help & Support tickets")
    jml_hs = _sb_tabel_hitung(TABLE_HELP, "generated_at")
    st.caption(f"Table `{TABLE_HELP}` currently has **{jml_hs if jml_hs >= 0 else '?'}** ticket(s).")

    def _hs_selesai(n, err, kosong_ok=True):
        if err:
            ss.riw_hasil = ("error", f"Delete failed: {err}")
        elif n == 0 and not kosong_ok:
            ss.riw_hasil = ("error", "Supabase accepted the request but deleted 0 rows: the table has no DELETE "
                                     "policy for this API key (RLS). Add a DELETE policy on help_support.")
        elif n == 0:
            ss.riw_hasil = ("success", "No matching tickets to delete.")
        else:
            ambil_semua_tiket_help.clear()
            ss.riw_hasil = ("success", f"{n} ticket(s) deleted.")
        st.rerun()

    st.markdown("**Delete solved tickets**")
    ok_solved = st.checkbox("I understand all tickets with status Solved will be permanently deleted",
                            key="hs_yakin_solved")
    if st.button(":material/task_alt: Delete solved tickets", key="hs_btn_solved", disabled=not ok_solved):
        n, err = _sb_tabel_hapus(TABLE_HELP, {"status": "ilike.solved"})
        _hs_selesai(n, err)

    st.markdown("**Delete old tickets**")
    c_hs_hari, c_hs_btn, _sp2 = st.columns([1.1, 1.9, 3], vertical_alignment="bottom")
    hs_hari = c_hs_hari.number_input("Older than (days)", min_value=1, max_value=3650, value=90, step=1,
                                     key="hs_hari")
    ok_lama = st.checkbox(f"I understand tickets older than {int(hs_hari)} days will be permanently deleted",
                          key="hs_yakin_lama")
    if c_hs_btn.button(":material/delete_sweep: Delete old tickets", key="hs_btn_lama", disabled=not ok_lama,
                       width="stretch"):
        batas = datetime.datetime.now(ZONA_WAKTU) - datetime.timedelta(days=int(hs_hari))
        n, err = _sb_tabel_hapus(TABLE_HELP, {"generated_at": f"lt.{batas.isoformat(timespec='seconds')}"})
        _hs_selesai(n, err)

    st.markdown("**Delete ALL tickets**")
    ok_semua_hs = st.checkbox("I understand ALL tickets (including In Progress) will be permanently deleted "
                              "(cannot be undone)", key="hs_yakin_semua")
    if st.button(":material/delete_forever: Delete all tickets", type="primary", key="hs_btn_semua",
                 disabled=not ok_semua_hs):
        n, err = _sb_tabel_hapus(TABLE_HELP, {"generated_at": "not.is.null"})
        _hs_selesai(n, err, kosong_ok=not (jml_hs > 0))


def halaman_pengaturan():
    ss = st.session_state
    st.markdown(komponen.html_header("Settings","Preferences & user access management.",":material/settings:"),
                unsafe_allow_html=True)
    st.markdown("""
<style>
[data-testid="stForm"] [data-baseweb="input"],
[data-testid="stTabs"] [data-baseweb="input"]{
  background:#ffffff !important;
  border:1px solid #cbd5e1 !important;
  border-radius:10px !important;
}
[data-testid="stForm"] input,
[data-testid="stTabs"] input{
  background:#ffffff !important;
  color:#0f172a !important;
}
</style>
""", unsafe_allow_html=True)

    # ── Guard: hanya BD SUPPORT ──
    me = ss.get("user") or {}
    if me.get("tagging") != TAGGING_BD:
        st.error("Access denied. Settings is only for BD Support.")
        return

    # ── Preferences (tetap) ──
    opsi = [10,15,25,50]
    st.selectbox("Rows per page", opsi, index=opsi.index(ss.ukuran_halaman),
                 key="atur_ukuran_halaman", on_change=_callback_ukuran_halaman)
    if st.button("Reload data from Supabase"):
        ambil_raw_noo.clear()
        ss.df_toko         = muat_data_noo_dari_supabase()
        ss.terakhir_update = waktu_sekarang()
        st.rerun()

    st.divider()
    st.subheader("User Access (Input NOO & SKU)")

    try:
        users = _sb_users_list()
    except Exception as e:
        st.error(f"Failed to load users: {e}")
        return

    #   akses_users = [u for u in users if u.get("tagging") == "user"]
    hapus_opts = [u["username"] for u in users if u["username"] != me.get("username")]
    semua_opts = [u["username"] for u in users]

    tab_add, tab_del, tab_pw, tab_riw = st.tabs([":material/person_add: Add user", ":material/delete: Delete user",
                                                 ":material/key: Reset password", ":material/history: Clear history"])

    # ── Tambah user ──
    with tab_add:
        from streamlit.noo_submission.login_page import do_signup, TAGGING_USER         
        with st.form("form_add_user", clear_on_submit=True):
            n_user  = st.text_input("Username")
            n_email = st.text_input("Email")
            n_pw    = st.text_input("Temporary password (min. 8 characters)", type="password")
            ok_add  = st.form_submit_button("Add user", type="primary")
        if ok_add:
            if not all([n_user, n_email, n_pw]):
                st.error("All fields are required.")
            elif not re.fullmatch(r"[A-Za-z0-9_.]{3,20}", n_user):
                st.error("Username 3-20 characters: letters, numbers, _ or .")
            elif len(n_pw) < 8:
                st.error("Password must be at least 8 characters.")
            else:
                try:
                    do_signup(n_user, n_email, n_pw, tagging=TAGGING_USER)
                except Exception as e:
                    st.error(f"Failed to add user: {e}")
                else:
                    _sb_users_list.clear()
                    catat_riwayat("Add user", n_user.strip().lower())
                    st.success(f"User '{n_user.strip().lower()}' added.")

    # ── Hapus user ──
    with tab_del:
        pesan_del = ss.pop("_del_sukses", None)          # pesan sukses hapus, tampil sekali di bawah tombol
        if ss.pop("_reset_del_user", False):
            ss["set_del_user"] = ""                      # dropdown kembali kosong (harus diset sebelum widget dibuat)
        if not hapus_opts:
            st.info("No users available to delete.")
            if pesan_del:
                st.success(pesan_del)
        else:
            u_del = st.selectbox("Select username", [""] + list(hapus_opts), key="set_del_user",
                                 format_func=lambda x: x or "Select a user...")
            # key checkbox mengikuti username: ganti user / habis hapus -> checkbox otomatis kosong lagi,
            # jadi centang dari user sebelumnya tidak terbawa ke user berikutnya
            kunci_yakin = f"set_del_yakin_{u_del}"
            yakin = st.checkbox(f"I understand '{u_del}' will lose access" if u_del else "I understand the user will lose access",
                                key=kunci_yakin, disabled=not u_del)
            if st.button("Delete user", type="primary", disabled=not (u_del and yakin), key="set_del_btn"):
                ss.pop(kunci_yakin, None)      # centang dipakai sekali saja
                if not _sb_user_delete(u_del):
                    st.error(f"Delete failed: {ss.pop('_err_del', '-')}")
                else:
                    _sb_users_list.clear()
                    catat_riwayat("Delete user", u_del)
                    ss["_del_sukses"] = f"User '{u_del}' deleted."
                    ss["_reset_del_user"] = True
                    st.rerun()
            if pesan_del:
                st.success(pesan_del)

    # ── Reset password ──
    with tab_pw:
        u_pw = st.selectbox("Select username", semua_opts, key="set_pw_user")
        with st.form("form_reset_pw", clear_on_submit=True):
            pw1 = st.text_input("New password (min. 8 characters)", type="password")
            pw2 = st.text_input("Confirm new password", type="password")
            ok_pw = st.form_submit_button("Reset password", type="primary")
        if ok_pw:
            if len(pw1) < 8:
                st.error("Password must be at least 8 characters.")
            elif pw1 != pw2:
                st.error("Passwords do not match.")
            elif not _sb_user_reset_pw(u_pw, pw1):
                st.error("Reset failed (check Supabase RLS policy for UPDATE).")
            else:
                catat_riwayat("Reset password", u_pw)
                st.success(f"Password for '{u_pw}' has been reset.")

    # ── Bersihkan riwayat (tabel riwayat_noo) ──
    with tab_riw:
        _tab_clear_history()

# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    st.set_page_config(page_title="Store Validation", page_icon=":material/storefront:", layout="wide")
    ss = st.session_state

    # Login gate
    from streamlit.noo_submission.login_page import require_login, keluar
    # mulai memuat data berat di latar belakang selagi user masih di halaman login
    _shared.prewarm(ambil_raw_noo, ambil_data_dashboard, _riwayat_cache)
    user = require_login()

    IZIN_MASUK = {"RSA", "BD SUPPORT"}
    if user.get("tagging") not in IZIN_MASUK:
        st.error("Access denied. This app is only for RSA and BD Support.")
        if st.button("Back to login"):
            st.session_state.clear()
            st.rerun()
        st.stop()

    _shared.PENGGUNA = nama_pengguna()

    inisialisasi_state()

    CSS_PATH = os.path.join(os.path.dirname(__file__), "style_app.css")
    try:
        with open(CSS_PATH, encoding="utf-8") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        st.error(f"CSS tidak ketemu di: {CSS_PATH}")

    if ss.get("pesan_toast"):
        st.toast(ss.pop("pesan_toast"), icon=ss.pop("pesan_toast_icon",":material/check_circle:"))

    user = ss.get("user", {})

    with st.sidebar:
        st.markdown(
            '<div class="brand">'
            '  <div class="brand-icon"><span class="mi">storefront</span></div>'
            '  Store Validation'
            '</div>'
            '<div class="brand-sub">NOO & SKU Mapping Dashboard</div>',
            unsafe_allow_html=True
        )
        is_bd = user.get("tagging") == "BD SUPPORT"
        for label, ikon, kunci in NAVIGASI:
            if kunci == "pengaturan" and not is_bd:
                continue
            nama_tombol = f"navaktif_{kunci}" if ss.halaman_aktif == kunci else f"nav_{kunci}"
            st.button(label, icon=ikon, key=nama_tombol, on_click=tuju_halaman,
                      args=(kunci,), width="stretch")

        st.link_button("Input NOO & SKU", "https://noo-submission.streamlit.app/",
                icon=":material/link:", use_container_width=True, key="nav_noo")

        # st.markdown(
        #     f'<div class="status-sidebar"><span class="titik"></span> Ready</div>'
        #     f'<div class="status-update">🕐 {ss.terakhir_update}</div>',
        #     unsafe_allow_html=True
        # )

        # ── Logout (fixed bottom sidebar via CSS) ────────────────────────────
        with st.container(key="logout"):
            if st.button(":material/logout: Log Out", width="stretch"):
                keluar()
                st.rerun()

    {
        "dashboard":  halaman_dashboard_tabs,
        "validasi":   halaman_validasi_toko,
        "riwayat":    halaman_riwayat,
        "pengaturan": halaman_pengaturan,
    }[ss.halaman_aktif]()

if __name__ == "__main__":
    main()  
