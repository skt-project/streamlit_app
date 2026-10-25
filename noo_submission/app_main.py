
import datetime
import json
import math
import re
import os
import numpy as np
import pandas as pd
import requests
import streamlit as st

import komponen
from shared import (
    # Constants
    KOLOM, KOLOM_EDIT, KOLOM_TAMPIL, WAJIB_ISI, MAPPING_SUPABASE_KE_VALIDASI,
    KOLOM_VALIDASI, STATUS_VALID, STATUS_REVIEW, WARNA_VALIDASI, warnai_validasi,
    TABLE_SUBMIT_NOO, TABLE_RIWAYAT, ZONA_WAKTU, PENGGUNA, SUPABASE_URL, _SB_HEADERS,
    PANJANG_PREFIX, PANJANG_ANGKA, JENIS_TOKO_DEFAULT,
    KOLOM_NOO_OUTPUT, OPSI_RATING, label_rating,
    # Supabase
    sb_insert, sb_select, sb_patch_by_kode_nama, sb_cari_duplikat_kode, ambil_raw_noo,
    # GSheets
    daftar_distributor, muat_distributor_dari_sheet, muat_prefix_per_distributor,
    max_angka_per_prefix, resolve_distributor,
    # Utils
    ke_wib_naive, waktu_sekarang, hitung_validasi, hasil_ke_excel, catat_riwayat,
)

KOLOM_ORDER_EDITOR = ["#","Pilih","Validasi","Penanda","Store ID"] + list(KOLOM[2:])

NAVIGASI = [
    ("Dashboard",          "", "dashboard"),
    ("Store Validation",   "",   "validasi"),
    ("Submission History", "", "riwayat"),
    ("Settings",           "⚙️", "pengaturan"),
   # ("Input NOO", "noo"),
]

# ═════════════════════════════════════════════════════════════════════════════
# GENERATOR ID
# ═════════════════════════════════════════════════════════════════════════════

def generate_store_ids(df: pd.DataFrame, semua: pd.DataFrame = None, kolom_target: str = "Store ID") -> pd.Series:
    prefix_per_distri = muat_prefix_per_distributor()
    counter = dict(max_angka_per_prefix())
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
    df["Validasi"] = np.where(valid_tagging, tagging_raw, hitung_validasi(df))
    df["Distributor"] = df.apply(
        lambda r: resolve_distributor(r["Customer Store Name"], r.get("City","")), axis=1)
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
        if tagging != STATUS_VALID:
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
    df["Validasi"] = hitung_validasi(df)
    df["_sb_id"]   = ""
    df = df[KOLOM_TAMPIL]
    df.index = pd.Index(df["#"].to_numpy(), name="_id")
    return df

def template_csv() -> bytes:
    return pd.DataFrame(columns=WAJIB_ISI).to_csv(index=False).encode("utf-8")

def terapkan_filter(df, status, distributor="All distributors", rentang=None):
    m = pd.Series(True, index=df.index)
    if rentang and "Created At" in df.columns:
        tgl = ke_wib_naive(df["Created At"]).dt.normalize()
        m  &= tgl >= pd.Timestamp(rentang[0])
        if len(rentang) == 2:
            m &= tgl <= pd.Timestamp(rentang[1])
    if status != "All statuses":
        m &= df["Validasi"] == status
    if distributor != "All distributors" and "Customer Store Name" in df.columns:
        m &= df["Customer Store Name"] == distributor
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

def halaman_validasi_toko():
    ss = st.session_state
    st.markdown(komponen.html_header("Store Validation","Review and update store details before submission."),
                unsafe_allow_html=True)
    tempat_unduh = st.empty()
    tempat_kpi   = st.empty()

    if ss.get("err_tagging"):
        st.error(ss.pop("err_tagging"))
    if ss.pop("reset_filter", False):
        ss.f_status = "All statuses"; ss.f_distri = "All distributors"
        ss.pop("f_tgl_awal",None); ss.pop("f_tgl_akhir",None)

    opsi_jenis  = sorted(set(JENIS_TOKO_DEFAULT) | set(ss.df_toko["Store Type"].dropna().astype(str)) - {""})
    daftar_distri = ["All distributors"] + sorted(
        muat_distributor_dari_sheet()["distributor_company_name"].str.strip().replace("",np.nan).dropna().unique()
    )
    hari_ini = datetime.datetime.now(ZONA_WAKTU).date()
    tgl_min  = pd.to_datetime(ss.df_toko.get("Created At"), errors="coerce").min() \
        if "Created At" in ss.df_toko.columns else pd.NaT
    tgl_awal = hari_ini if pd.isna(tgl_min) else tgl_min.date()

    c_tgl1,c_tgl2,c_distri,c_status,c_refresh,c_simpan = st.columns([1.3,1.3,1.6,1.4,1.1,1.5])
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
    with c_status:
        status = st.selectbox("Status",["All statuses"] + KOLOM_VALIDASI, key="f_status",
                               on_change=reset_halaman_data)
    with c_refresh:
        st.markdown('<div style="height:1.7rem"></div>', unsafe_allow_html=True)
        st.button("🔄 Refresh", width="stretch", key="tombol_refresh", on_click=refresh_data_toko,
                   help="Reload data from Supabase. Unsaved changes will be lost.")
    with c_simpan:
        st.markdown('<div style="height:1.7rem"></div>', unsafe_allow_html=True)
        st.button("Save changes", type="primary", width="stretch", key="tombol_simpan",
                   on_click=simpan_perubahan)

    hasil_filter = terapkan_filter(ss.df_toko, status, distributor, tuple(rentang))
    total_filter = len(hasil_filter)

    data_unduh = bangun_download_noo(hasil_filter)
    with tempat_unduh.container():
        _, c_unduh = st.columns([5,1.6])
        c_unduh.download_button("⬇️ Download NOO", hasil_ke_excel(data_unduh), "hasil_noo.xlsx",
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                disabled=data_unduh.empty, width="stretch", key="unduh_noo_validasi")

    ukuran       = ss.ukuran_halaman
    total_halaman = max(1, math.ceil(total_filter / ukuran))
    ss.halaman_data = max(1, min(ss.halaman_data, total_halaman))
    mulai = (ss.halaman_data - 1) * ukuran
    akhir = min(mulai + ukuran, total_filter)

    if total_filter == 0:
        tempat_kpi.markdown(komponen.html_kpi_validasi(ss.df_toko), unsafe_allow_html=True)
        st.info("No stores match the filter.")
        return

    tanda = (status, distributor, tuple(rentang), ss.halaman_data, ukuran, ss.versi_impor)
    if ss.tanda_tangan != tanda or ss.snapshot is None:
        ss.snapshot        = hasil_filter.iloc[mulai:akhir][KOLOM_TAMPIL].copy()
        ss.snapshot["Pilih"] = False
        ss.tanda_tangan    = tanda
        ss.kunci_editor   += 1

    c_select_all, c_generate, _ = st.columns([1,1.6,5], vertical_alignment="center")
    with c_select_all:
        select_all = st.checkbox("Select All", key=f"select_all_{ss.kunci_editor}")
        ss.snapshot["Pilih"] = select_all
        ss.tampilan_kunci    = None
    with c_generate:
        tombol_generate = st.button("Generate ID", type="primary", key="tombol_generate_bulk")

    konfigurasi_kolom = {
        "#":                     st.column_config.NumberColumn("#",format="%d",width=50),
        "Pilih":                 st.column_config.CheckboxColumn("✅",width=50,default=False),
        "Validasi":              st.column_config.SelectboxColumn("Validation ▾",options=KOLOM_VALIDASI,width=140,required=True),
        "Penanda":               st.column_config.TextColumn("Marker",width=110),
        "Store ID":              st.column_config.TextColumn("Store ID",width=150,disabled=True),
        "Generated At":          st.column_config.TextColumn("Generated At",width=150,disabled=True),
        "Distributor":           st.column_config.SelectboxColumn("Distributor",options=[""] + daftar_distributor(),width=220),
        "Store Name":            st.column_config.TextColumn("Store Name",width=170,required=True),
        "Customer Store Code":   st.column_config.TextColumn("Customer Store Code",width=200),
        "Customer Store Name":   st.column_config.TextColumn("Customer Store Name",width=170),
        "City":                  st.column_config.TextColumn("City",width=110),
        "Store Address":         st.column_config.TextColumn("Store Address",width=190),
        "Longitude":             st.column_config.NumberColumn("Longitude",format="%.4f",min_value=-180.0,max_value=180.0,step=0.0001,width=100),
        "Latitude":              st.column_config.NumberColumn("Latitude",format="%.4f",min_value=-90.0,max_value=90.0,step=0.0001,width=100),
        "Store Type":            st.column_config.SelectboxColumn("Store Type",options=opsi_jenis,width=130),
    }
    if ss.get("tampilan_kunci") != ss.kunci_editor:
        tampil = ss.snapshot.copy()
        tampil["Penanda"] = tampil["Validasi"]
        ss.tampilan      = tampil.style.map(warnai_validasi, subset=["Penanda"])
        ss.tampilan_kunci = ss.kunci_editor

    hasil_edit = st.data_editor(
        ss.tampilan, key=f"editor_{ss.kunci_editor}",
        column_config=konfigurasi_kolom, column_order=KOLOM_ORDER_EDITOR,
        disabled=["#","Penanda","Created At","Generated At"],
        hide_index=True, num_rows="fixed", width="stretch",
        height=len(ss.snapshot)*35+38,
    )

    # Bulk Generate ID
    if tombol_generate:
        baris_dipilih = hasil_edit[hasil_edit["Pilih"] == True].index.tolist()
        if not baris_dipilih:
            st.warning("No rows selected.")
        else:
            df = ss.df_toko
            for idx in baris_dipilih:
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
            gagal_push   = []
            for idx in baris_dipilih:
                if idx in gagal_prefix:
                    continue
                kode = str(df.loc[idx,"Customer Store Code"]).strip()
                nama = str(df.loc[idx,"Customer Store Name"]).strip()
                if not kode or not nama:
                    continue
                gen_at = df.loc[idx,"Generated At"]
                gen_at_iso = None
                if pd.notna(gen_at):
                    wt = pd.Timestamp(gen_at)
                    if wt.tzinfo is None:
                        wt = wt.tz_localize(ZONA_WAKTU)
                    gen_at_iso = wt.isoformat()
                patch = {"tagging":STATUS_VALID,"Store ID":str(df.loc[idx,"Store ID"]).strip(),"generated_at":gen_at_iso}
                if not sb_patch_by_kode_nama(TABLE_SUBMIT_NOO, kode, nama, patch):
                    gagal_push.append(kode)
            berhasil = len(baris_dipilih) - len(gagal_prefix) - len(gagal_push)
            pesan = f"✅ {berhasil} rows successfully generated"
            if gagal_prefix:
                distri_gagal = sorted({str(df.loc[i,"Distributor"]) or "(empty)" for i in gagal_prefix})
                pesan += f" | ⚠️ {len(gagal_prefix)} failed (prefix not found: {', '.join(distri_gagal)})"
            if gagal_push:
                pesan += f" | ❌ {len(gagal_push)} failed to push"
            catat_riwayat("Bulk Generate ID", f"{berhasil} rows")
            ss.baris_kotor -= set(baris_dipilih)
            ss.baris_perlu_push -= set(baris_dipilih)
            ss.tanda_tangan = None; ss.snapshot = None
            ss.terakhir_update = waktu_sekarang()
            ss.pesan_toast = pesan
            st.rerun()

    id_berubah = terapkan_edit(ss.df_toko, hasil_edit)
    if id_berubah:
        ss.baris_kotor.update(id_berubah)
        df = ss.df_toko
        validasi_skrg = df.loc[id_berubah,"Validasi"]
        pilih = validasi_skrg.eq(STATUS_VALID)
        batal = [i for i in id_berubah if not pilih.loc[i]]
        for i in batal:
            df.loc[i,"Store ID"] = "-"; df.loc[i,"Generated At"] = pd.NaT
            ss.baris_perlu_push.discard(i)
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
# HALAMAN: DASHBOARD
# ═════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=60, show_spinner=False)
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

def halaman_dashboard():
    ss = st.session_state
    st.markdown(komponen.html_header("Dashboard","Monitor NOO submission & SKU Mapping coverage.","📊"),
                unsafe_allow_html=True)
    with st.spinner("Loading dashboard data..."):
        data = ambil_data_dashboard()
    df_noo = data["noo"]
    df_sku = data["sku"]

    norm = lambda s: re.sub(r"[^a-z0-9]","",str(s).lower())
    peta_noo = {norm(c):c for c in df_noo.columns} if not df_noo.empty else {}
    peta_sku = {norm(c):c for c in df_sku.columns} if not df_sku.empty else {}

    col_tagging     = peta_noo.get("tagging")
    col_distri      = peta_noo.get("customerstorename") or peta_noo.get("distributor")
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
    distri_opts = ["All Distributor"]
    if not df_noo.empty and col_distri:
        distri_opts += sorted(df_noo[col_distri].dropna().unique().tolist())
    f_distri   = f1.selectbox("Distributor", distri_opts, key="dash_distri")
    brand_opts = ["All Brand"]
    if not df_sku.empty:
        brand_opts += sorted(df_sku["_brand"].unique().tolist())
    f_brand    = f2.selectbox("Brand (SKU)", brand_opts, key="dash_brand")
    hari_ini   = datetime.datetime.now(ZONA_WAKTU).date()
    f_tgl_dari  = f3.date_input("Period from", value=hari_ini-datetime.timedelta(days=30),
                                 key="dash_dari", format="YYYY-MM-DD")
    f_tgl_sampai = f4.date_input("Period to", value=hari_ini, key="dash_sampai", format="YYYY-MM-DD")
    with f5:
        st.markdown('<div style="height:1.7rem"></div>', unsafe_allow_html=True)
        if st.button("🔄 Refresh", key="dash_refresh", width="stretch"):
            ambil_data_dashboard.clear(); st.rerun()

    df_noo_f = df_noo.copy() if not df_noo.empty else df_noo
    df_sku_f = df_sku.copy() if not df_sku.empty else df_sku
    if not df_noo_f.empty and col_created_noo:
        tgl_noo  = ke_wib_naive(df_noo_f[col_created_noo]).dt.date
        df_noo_f = df_noo_f[(tgl_noo >= f_tgl_dari) & (tgl_noo <= f_tgl_sampai)]
    if not df_noo_f.empty and col_distri and f_distri != "All Distributor":
        df_noo_f = df_noo_f[df_noo_f[col_distri].astype(str).str.strip() == f_distri]
    if not df_sku_f.empty and col_created_sku:
        tgl_sku  = ke_wib_naive(df_sku_f[col_created_sku]).dt.date
        df_sku_f = df_sku_f[(tgl_sku >= f_tgl_dari) & (tgl_sku <= f_tgl_sampai)]
    if not df_sku_f.empty and f_brand != "All Brand":
        df_sku_f = df_sku_f[df_sku_f["_brand"] == f_brand]

    st.markdown('<hr style="margin:8px 0 16px 0;border:none;border-top:1px solid #e2e8f0">', unsafe_allow_html=True)

    # ── NOO ──────────────────────────────────────────────────────────────────
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
<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px">
  <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:18px 16px">
    <div style="font-size:0.75rem;color:#64748b;font-weight:500;margin-bottom:6px">Total Stores</div>
    <div style="font-size:2rem;font-weight:800;color:#0f172a;line-height:1">{total_noo:,}</div>
  </div>
  <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:12px;padding:18px 16px">
    <div style="font-size:0.75rem;color:#15803d;font-weight:500;margin-bottom:6px">Generate ID</div>
    <div style="font-size:2rem;font-weight:800;color:#166534;line-height:1">{n_generate:,}</div>
    <div style="font-size:0.72rem;color:#16a34a;margin-top:4px">▲ {pct_gen}% dari total</div>
  </div>
  <div style="background:#fff1f2;border:1px solid #fecdd3;border-radius:12px;padding:18px 16px">
    <div style="font-size:0.75rem;color:#be123c;font-weight:500;margin-bottom:6px">Need Review</div>
    <div style="font-size:2rem;font-weight:800;color:#9f1239;line-height:1">{n_review:,}</div>
    <div style="font-size:0.72rem;color:#e11d48;margin-top:4px">{pct_rev}% dari total</div>
  </div>
  <div style="background:#fefce8;border:1px solid #fde68a;border-radius:12px;padding:18px 16px">
    <div style="font-size:0.75rem;color:#a16207;font-weight:500;margin-bottom:6px">Existing</div>
    <div style="font-size:2rem;font-weight:800;color:#854d0e;line-height:1">{n_existing:,}</div>
    <div style="font-size:0.72rem;color:#ca8a04;margin-top:4px">{pct_ext}% dari total</div>
  </div>
</div>
        """, unsafe_allow_html=True)
        cn1,cn2 = st.columns(2)
        with cn1:
            st.markdown('<p style="font-size:0.82rem;color:#64748b;margin-bottom:4px">NOO by Tagging Status</p>',
                        unsafe_allow_html=True)
            if col_tagging:
                chart_tag = tagging.value_counts()
                labels    = chart_tag.index.tolist()
                values    = chart_tag.values.tolist()
                WARNA_TAG = {"Generate ID":"#22c55e","Need Review":"#f43f5e","Existing":"#eab308"}
                colors    = [WARNA_TAG.get(l,"#94a3b8") for l in labels]
                st.components.v1.html(f"""
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<canvas id="donut_noo" style="max-height:200px"></canvas>
<script>
new Chart(document.getElementById('donut_noo'),{{
  type:'doughnut',
  data:{{labels:{json.dumps(labels)},datasets:[{{data:{json.dumps(values)},backgroundColor:{json.dumps(colors)},borderWidth:1}}]}},
  options:{{responsive:true,plugins:{{legend:{{position:'right',labels:{{font:{{size:11}},boxWidth:12,padding:10}}}}}}}}
}});
</script>""", height=220)
        with cn2:
            st.markdown('<p style="font-size:0.82rem;color:#64748b;margin-bottom:4px">NOO by Distributor (Top 10)</p>',
                        unsafe_allow_html=True)
            if col_distri:
                per_distri  = (df_noo_f[col_distri].replace("","(undetected)").fillna("(undetected)").value_counts().head(10))
                labels_d    = per_distri.index.tolist()
                values_d    = per_distri.values.tolist()
                st.components.v1.html(f"""
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<canvas id="bar_distri" style="max-height:200px"></canvas>
<script>
new Chart(document.getElementById('bar_distri'),{{
  type:'bar',
  data:{{labels:{json.dumps(labels_d)},datasets:[{{data:{json.dumps(values_d)},backgroundColor:'#22c55e',borderRadius:4}}]}},
  options:{{indexAxis:'y',responsive:true,plugins:{{legend:{{display:false}}}},scales:{{x:{{ticks:{{font:{{size:10}}}}}},y:{{ticks:{{font:{{size:10}}}}}}}}}}
}});
</script>""", height=220)
        with st.expander("Recent NOO Submissions (last 20)"):
            kolom_show = [c for c in [peta_noo.get("customerstorecode"),peta_noo.get("storename"),
                                       col_distri,col_city,col_tagging,col_created_noo] if c]
            if kolom_show:
                df_show = df_noo_f[kolom_show].head(20).copy()
                if col_created_noo in df_show.columns:
                    df_show[col_created_noo] = ke_wib_naive(df_show[col_created_noo]).dt.strftime("%d %b %Y %H:%M")
                st.dataframe(df_show, hide_index=True, width="stretch")

    st.markdown('<hr style="margin:20px 0;border:none;border-top:1px solid #e2e8f0">', unsafe_allow_html=True)

    # ── SKU ──────────────────────────────────────────────────────────────────
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
        kpi_cols     = st.columns(3+len(brand_list))
        kpi_cols[0].markdown(f"""
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:18px 16px">
  <div style="font-size:0.75rem;color:#64748b;font-weight:500;margin-bottom:6px">Total SKU Mapped</div>
  <div style="font-size:2rem;font-weight:800;color:#0f172a;line-height:1">{total_sku:,}</div>
</div>""", unsafe_allow_html=True)
        kpi_cols[1].markdown(f"""
<div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:12px;padding:18px 16px">
  <div style="font-size:0.75rem;color:#0369a1;font-weight:500;margin-bottom:6px">SKU Baru (7 hari)</div>
  <div style="font-size:2rem;font-weight:800;color:#0c4a6e;line-height:1">{sku_baru:,}</div>
</div>""", unsafe_allow_html=True)
        kpi_cols[2].markdown(f"""
<div style="background:#faf5ff;border:1px solid #e9d5ff;border-radius:12px;padding:18px 16px">
  <div style="font-size:0.75rem;color:#7c3aed;font-weight:500;margin-bottom:6px">Distributor</div>
  <div style="font-size:2rem;font-weight:800;color:#4c1d95;line-height:1">{n_distri_sku:,}</div>
</div>""", unsafe_allow_html=True)
        BRAND_COLORS = {"G2G":"#6366f1","NXP":"#f59e0b","Other":"#94a3b8"}
        for idx, brand in enumerate(brand_list):
            cnt = int(brand_counts[brand])
            pct = round(cnt/total_sku*100) if total_sku else 0
            clr = BRAND_COLORS.get(brand,"#94a3b8")
            kpi_cols[3+idx].markdown(f"""
<div style="background:#fff;border:1px solid {clr}40;border-radius:12px;padding:18px 16px">
  <div style="font-size:0.75rem;color:{clr};font-weight:600;margin-bottom:6px">{brand}</div>
  <div style="font-size:2rem;font-weight:800;color:#0f172a;line-height:1">{cnt:,}</div>
  <div style="font-size:0.72rem;color:#64748b;margin-top:4px">{pct}% dari total</div>
</div>""", unsafe_allow_html=True)

        st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)
        cs1,cs2 = st.columns(2)
        with cs1:
            st.markdown('<p style="font-size:0.82rem;color:#64748b;margin-bottom:4px">SKU per Brand</p>',
                        unsafe_allow_html=True)
            if "_brand" in df_sku_f.columns:
                labels_b = brand_counts.index.tolist()
                values_b = brand_counts.values.tolist()
                colors_b = [BRAND_COLORS.get(b,"#94a3b8") for b in labels_b]
                st.components.v1.html(f"""
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<canvas id="donut_brand" style="max-height:180px"></canvas>
<script>
new Chart(document.getElementById('donut_brand'),{{
  type:'doughnut',
  data:{{labels:{json.dumps(labels_b)},datasets:[{{data:{json.dumps(values_b)},backgroundColor:{json.dumps(colors_b)},borderWidth:1}}]}},
  options:{{responsive:true,plugins:{{legend:{{position:'right',labels:{{font:{{size:11}},boxWidth:12,padding:10}}}}}}}}
}});
</script>""", height=200)
        with cs2:
            st.markdown('<p style="font-size:0.82rem;color:#64748b;margin-bottom:4px">SKU Submitted Over Time</p>',
                        unsafe_allow_html=True)
            if col_created_sku:
                per_tgl = ke_wib_naive(df_sku_f[col_created_sku]).dt.date.value_counts().sort_index()
                if not per_tgl.empty:
                    labels_t = [str(d) for d in per_tgl.index.tolist()]
                    values_t = per_tgl.values.tolist()
                    st.components.v1.html(f"""
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<canvas id="line_sku" style="max-height:180px"></canvas>
<script>
new Chart(document.getElementById('line_sku'),{{
  type:'line',
  data:{{labels:{json.dumps(labels_t)},datasets:[{{data:{json.dumps(values_t)},borderColor:'#6366f1',backgroundColor:'#6366f120',borderWidth:2,pointRadius:3,fill:true,tension:0.3}}]}},
  options:{{responsive:true,plugins:{{legend:{{display:false}}}},scales:{{x:{{ticks:{{font:{{size:10}},maxRotation:0}}}},y:{{ticks:{{font:{{size:10}},stepSize:1}}}}}}}}
}});
</script>""", height=200)

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

def halaman_riwayat():
    st.markdown(komponen.html_header("Submission History","Log of all submission and validation activities.","🕒"),
                unsafe_allow_html=True)
    if st.button("🔄 Refresh", key="refresh_riwayat"):
        pass
    rows = sb_select(TABLE_RIWAYAT, order="waktu.desc", limit=200)
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

def halaman_pengaturan():
    ss = st.session_state
    st.markdown(komponen.html_header("Settings","Preferences for this session.","⚙️"), unsafe_allow_html=True)
    opsi = [10,15,25,50]
    st.selectbox("Rows per page", opsi, index=opsi.index(ss.ukuran_halaman),
                 key="atur_ukuran_halaman", on_change=_callback_ukuran_halaman)
    if st.button("Reload data from Supabase"):
        ambil_raw_noo.clear()
        ss.df_toko         = muat_data_noo_dari_supabase()
        ss.terakhir_update = waktu_sekarang()
        st.rerun()

# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    st.set_page_config(page_title="Store Validation", page_icon="🏪", layout="wide")
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
        st.markdown(
            '<div class="brand">'
            '  <div class="brand-icon">🏪</div>'
            '  Store Validation'
            '</div>'
            '<div class="brand-sub">NOO & SKU Mapping Dashboard</div>',
            unsafe_allow_html=True
        )
        for label, ikon, kunci in NAVIGASI:
            nama_tombol = f"navaktif_{kunci}" if ss.halaman_aktif == kunci else f"nav_{kunci}"
            st.button(f"{ikon}   {label}", key=nama_tombol, on_click=tuju_halaman,
                      args=(kunci,), width="stretch")
            st.markdown(f'<div class="status-sidebar"><span class="titik"></span> Ready</div>'
            f'<div class="status-update">🕐 {ss.terakhir_update}</div>',
            unsafe_allow_html=True)
        st.link_button("🔍 Input NOO", "https://noo-submission.streamlit.app/",
                use_container_width=True, key="nav_noo")

    {
        "dashboard":  halaman_dashboard,
        "validasi":   halaman_validasi_toko,
        "riwayat":    halaman_riwayat,
        "pengaturan": halaman_pengaturan,
    }[ss.halaman_aktif]()

if __name__ == "__main__":
    main()
