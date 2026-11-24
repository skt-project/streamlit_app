import contextlib
import datetime
import time
import json
import math
import re
import os
import numpy as np
import pandas as pd
import bcrypt
import requests
import shared as _shared
import streamlit as st
import komponen
from noo_grid import noo_grid
from feedback import TABLE_FEEDBACK, KOLOM_USER as KOLOM_FB_USER
from shared import (
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
    if not prefix or _shared.PAKAI_COUNTER_PUSAT:
        # Dengan counter pusat nomor TIDAK boleh digeser/dirapatkan: nomor di antaranya bisa sudah diambil Apps Script.
        return []
    floor = max(
        max_angka_per_prefix().get(prefix, 0),
        sb_max_angka_store_id().get(prefix, 0),
        _shared.max_angka_data_merge().get(prefix, 0),     # ID yang sudah dipakai di sheet (data merge)
    )
    mask = (
        df["Validasi"].eq(STATUS_VALID) &
        df.index.isin(list(st.session_state.get("baris_perlu_push", ()))) &      # hanya yang belum di-save
        df["Store ID"].fillna("").astype(str).str.strip().str.startswith(prefix) &
        ~df["Store ID"].fillna("").astype(str).str.strip().isin(["", "-"])
    )
    aktif = df[mask].copy()
    if aktif.empty:
        return []
    # Urutan nomor = urutan BARIS di tabel (bukan waktu generate), supaya bolak-balik Generate ID <-> Need Review
    # selalu kembali ke nomor yang sama: baris yang lebih atas selalu dapat nomor lebih kecil.
    aktif = aktif.sort_index(kind="stable")

    berubah = []
    for n, idx in enumerate(aktif.index, start=1):
        id_baru = f"{prefix}{floor+n:0{PANJANG_ANGKA}d}"
        if str(df.loc[idx, "Store ID"]).strip() != id_baru:
            df.loc[idx, "Store ID"] = id_baru
            berubah.append(idx)
    return berubah

def generate_store_ids(df: pd.DataFrame, semua: pd.DataFrame = None, kolom_target: str = "Store ID") -> pd.Series:
    # TIMER loading (sementara): waktu tiap langkah dicatat -> ditampilkan di tab NOO (ss.waktu_generate)
    wkt, t_awal = {}, time.perf_counter()
    @contextlib.contextmanager
    def ukur(nama):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            wkt[nama] = wkt.get(nama, 0) + time.perf_counter() - t0
    with ukur("Distributor mapping (Sheets)"):
        prefix_per_distri = muat_prefix_per_distributor()
    with ukur("BigQuery max/prefix"):
        counter = dict(max_angka_per_prefix())
    with ukur("Supabase max/prefix"):
        for p, a in sb_max_angka_store_id().items():
            counter[p] = max(counter.get(p, 0), a)
    # DATA MERGE: ID yang sudah dipakai di sheet (DATA GENERATOR, id dms) tapi mungkin belum masuk BigQuery.
    # Max efektif per prefix = MAX(BigQuery, Supabase, data merge) -- selalu dipakai, bukan hanya saat bentrok.
    _shared.muat_id_data_merge.clear()      # baca ulang sheet tanpa cache: ID terbaru dari Apps Script harus terlihat
    _shared.muat_max_pivot.clear()
    with ukur("Sheet id dms + NEW Generator"):
        id_merge, err_merge = _shared.muat_id_data_merge()
    if err_merge:
        st.session_state.id_merge_err = err_merge
    with ukur("Sheet pivot DATA GENERATOR"):
        pivot_max = _shared.muat_max_pivot()
    for p, a in _shared.max_angka_data_merge().items():
        counter[p] = max(counter.get(p, 0), a)
    # Basis check (jaring pengaman): semua ID yang sudah pernah ada. ID hasil generate yang ternyata ada di sini dilompati.
    with ukur("BigQuery get_id (basis)"):
        basis = set(id_merge) | {str(s).strip().upper() for s in _shared.get_id()}
    if semua is not None:
        for sid in semua[kolom_target].fillna("").astype(str).str.strip():
            p, a = sid[:PANJANG_PREFIX], sid[PANJANG_PREFIX:]
            if a.isdigit():
                counter[p] = max(counter.get(p, 0), int(a))
            if sid and sid != "-":
                basis.add(sid.upper())
    hasil = []
    sisa = {}                       # sisa baris yang butuh ID per prefix (untuk minta jatah ke counter pusat)
    for _, row in df.iterrows():
        p_ = prefix_per_distri.get(str(row.get("Distributor","")).strip(), "")
        if p_:
            sisa[p_] = sisa.get(p_, 0) + 1
    antrian, pusat_gagal = {}, set()
    st.session_state.kunci_err = ""

    def nomor_berikut(prefix):
        while True:
            if antrian.get(prefix):
                n = antrian[prefix].pop(0)
            else:
                with ukur("Supabase counter pusat (sb_ambil_nomor)"):
                    rentang = None if prefix in pusat_gagal else _shared.sb_ambil_nomor(prefix, sisa.get(prefix, 1), counter.get(prefix, 0))
                if rentang is not None:
                    antrian[prefix] = list(range(rentang[0], rentang[1] + 1))
                    counter[prefix] = max(counter.get(prefix, 0), rentang[1])
                    continue
                pusat_gagal.add(prefix)
                n = counter.get(prefix, 0) + 1
                counter[prefix] = n
            if f"{prefix}{n:0{PANJANG_ANGKA}d}".upper() not in basis:       # sudah ada di basis -> lompat
                return n

    for _, row in df.iterrows():
        distri = str(row.get("Distributor","")).strip()
        prefix = prefix_per_distri.get(distri,"")
        if not prefix:
            hasil.append("-"); continue
        angka_baru = nomor_berikut(prefix)
        sisa[prefix] = max(sisa.get(prefix, 1) - 1, 1)
        id_baru = f"{prefix}{angka_baru:0{PANJANG_ANGKA}d}"
        basis.add(id_baru.upper())                  # ID dalam satu batch tidak boleh kembar
        hasil.append(id_baru)
    if pusat_gagal:
        st.session_state.kunci_err = "Central ID counter unavailable, used local count"
    if pusat_gagal and any(h != "-" for h in hasil):
        # Counter pusat tidak terjangkau -> pakai kunci sheet (cadangan). Beritahu Apps Script: ada ID yang belum tersimpan -> script menunggu sampai Save changes / Refresh
        ok_kunci, err_kunci = _shared.kunci_generate_pasang(nama_pengguna())
        if not ok_kunci:
            st.session_state.kunci_err += f" | Sheet lock not set: {err_kunci[:100]}"
    wkt["TOTAL generate_store_ids"] = time.perf_counter() - t_awal
    st.session_state.waktu_generate = wkt
    _lama = max((k for k in wkt if not k.startswith("TOTAL")), key=wkt.get, default="-")
    st.toast(f"⏱ Generate ID {wkt['TOTAL generate_store_ids']:.1f}s — slowest: {_lama} ({wkt.get(_lama, 0):.1f}s)")
    return pd.Series(hasil, index=df.index)

def periksa_ulang_id(df: pd.DataFrame) -> list:
    ss = st.session_state
    idx_baru = [i for i in ss.baris_perlu_push if i in df.index and df.loc[i, "Validasi"] == STATUS_VALID
                and not _id_kosong(df.loc[i, "Store ID"])]
    if not idx_baru:
        return []
    _shared.muat_id_data_merge.clear()
    _shared.muat_max_pivot.clear()
    id_sheet =_shared.muat_id_data_merge()[0]
    id_sb = _shared.sb_id_terpakai() or {}          # gagal baca Supabase: cek sheet saja, jangan menggagalkan Save
    bentrok = []
    for i in idx_baru:
        sid = str(df.loc[i, "Store ID"]).strip().upper()
        kunci = (str(df.loc[i, "Customer Store Code"]).strip(), str(df.loc[i, "Customer Store Name"]).strip())
        pemilik = id_sb.get(sid)
        if sid in id_sheet or (pemilik is not None and pemilik != kunci):
            bentrok.append(i)
    if not bentrok:
        return []
    lama = {i: str(df.loc[i, "Store ID"]).strip() for i in bentrok}
    df.loc[bentrok, "Store ID"] = "-"
    df.loc[bentrok, "Store ID"] = generate_store_ids(df.loc[bentrok], df)
    df.loc[bentrok, "Generated At"] = datetime.datetime.now(ZONA_WAKTU).replace(tzinfo=None)
    for prefix in {str(df.loc[i, "Store ID"]).strip()[:PANJANG_PREFIX] for i in bentrok}:
        ss.baris_kotor.update(resequence_prefix(df, prefix))
    ss.baris_kotor.update(bentrok); ss.baris_perlu_push.update(bentrok)
    return [(lama[i], str(df.loc[i, "Store ID"]).strip()) for i in bentrok]

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
    tagging_raw = tagging_raw.replace(_shared.ALIAS_STATUS)      # nama status lama -> nama sekarang
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
    # Store ID di file download = yang SUDAH TERSIMPAN di Supabase (hasil Save changes), bukan yang baru di layar.
    kolom_id = peta.get(norm("Store ID"))
    out["Store ID"] = (raw[kolom_id].map(lambda v: "" if v is None or pd.isna(v) else bersih(v)).replace("-","")
                       if kolom_id else "")
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
    _shared.kunci_generate_lepas()        # ID belum tersimpan dibuang saat Refresh -> lepas kunci Apps Script
    ss.versi_impor     += 1
    ss.tanda_tangan     = None
    ss.snapshot         = None
    ss.terakhir_update  = waktu_sekarang()
    ss.pesan_toast = "Data successfully refreshed from Supabase"

# Kolom grid yang boleh diedit user dan harus ikut tersimpan ke INPUT_NOO.
# (Customer Store Code/Name tidak dimasukkan: dipakai sebagai kunci pencarian baris.)
KOLOM_EDIT_KE_SUPABASE = ["Store Name", "City", "Store Address", "Longitude", "Latitude", "Store Type"]

def _patch_kolom_edit(baris: pd.Series) -> dict:
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

def _distri_pertama(df, idx_list) -> str:
    if "Distributor" not in df.columns:
        return ""
    for i in idx_list:
        if i in df.index:
            v = str(df.loc[i, "Distributor"]).strip()
            if v and v.lower() != "nan":
                return v
    return ""

def _id_kosong(v) -> bool:
    return str(v).strip() in ("", "-", "nan", "None", "NaT")

def _iso_wib(ts) -> str:
    wt = pd.Timestamp(ts)
    if wt.tzinfo is None:
        wt = wt.tz_localize(ZONA_WAKTU)
    return wt.isoformat()

def _status_normal(v) -> str:
    v = _shared.ALIAS_STATUS.get(str(v or "").strip(), str(v or "").strip())
    return v if v in KOLOM_VALIDASI else STATUS_REVIEW

def _remark_perubahan(status_lama, id_lama, status_baru, id_baru) -> dict:
    sl, sb = _status_normal(status_lama), _status_normal(status_baru)
    il = "" if _id_kosong(id_lama) else str(id_lama).strip()
    ib = "" if _id_kosong(id_baru) else str(id_baru).strip()
    hasil = {}
    if sl != sb:
        hasil["Remark_status"] = f"{sl} > {sb}"
    if il != ib:
        hasil["Remark_ID"] = f"{il or 'null'} > {ib or 'null'}"
    if hasil:
        hasil["Remark_at"] = datetime.datetime.now(ZONA_WAKTU).strftime("%Y-%m-%d %H:%M:%S")
    return hasil

def _item_sheet(df, idx, kode, nama, remark=None) -> dict:
    r = df.loc[idx]
    dibuat = r.get("Created At")
    kolom_sheet = {                       # kolom yang bisa diedit di Store Validation -> ikut disinkronkan ke sheet
        "Store Name": r.get("Store Name", ""), "City": r.get("City", ""), "Store Address": r.get("Store Address", ""),
        "Longitude": "" if pd.isna(r.get("Longitude")) else r.get("Longitude"),
        "Latitude": "" if pd.isna(r.get("Latitude")) else r.get("Latitude"),
        "Store Type": r.get("Store Type", ""),
    }
    kolom_sheet.update(remark or {})
    # Kalau barisnya belum ada di sheet, ditambahkan LENGKAP dari data Supabase (bukan cuma kolom yang bisa diedit)
    lengkap = {}
    try:
        for b in _shared.ambil_raw_noo():
            if str(b.get("Customer Store Code") or "").strip() == kode and str(b.get("Customer Store Name") or "").strip() == nama:
                lengkap = {k: ("" if v is None else v) for k, v in b.items() if k in _shared.NOO_SHEET_KOLOM}
                lengkap["Submitted At"] = str(b.get("created_at") or "")[:19].replace("T", " ")
                lengkap["Source"] = "NOO Submission"
                lengkap.pop("Store ID", None)
                break
    except Exception:
        pass
    return {
        "kode": kode, "nama": nama, "store_id": "" if _id_kosong(r["Store ID"]) else str(r["Store ID"]).strip(),
        "nilai": kolom_sheet,
        "generated_at": pd.Timestamp(r["Generated At"]).strftime("%Y-%m-%d %H:%M:%S") if pd.notna(r["Generated At"]) else "",
        # data untuk baris baru di sheet kalau barisnya belum ada (kunci = nama kolom di NOO_SHEET_KOLOM)
        "data": {
            **lengkap,
            "Submitted At": lengkap.get("Submitted At") or (pd.Timestamp(dibuat).strftime("%Y-%m-%d %H:%M:%S") if pd.notna(dibuat) else ""),
            "Source": lengkap.get("Source", "Store Validation"), "Submitted By": r.get("Distributor", ""),
            "Store Name": r.get("Store Name", ""), "Customer Code": r.get("Customer Code", ""),
            "City": r.get("City", ""), "Store Address": r.get("Store Address", ""),
            "Longitude": "" if pd.isna(r.get("Longitude")) else r.get("Longitude"),
            "Latitude": "" if pd.isna(r.get("Latitude")) else r.get("Latitude"),
            "Store Type": r.get("Store Type", ""),
        }}

def ubah_status_dan_simpan(df, idx_list, status_lama) -> list:
    masalah, item_sheet = [], []
    for i in idx_list:
        kode = str(df.loc[i, "Customer Store Code"]).strip()
        nama = str(df.loc[i, "Customer Store Name"]).strip()
        if kode and nama and sb_patch_by_kode_nama(TABLE_SUBMIT_NOO, kode, nama, {"tagging": df.loc[i, "Validasi"]}):
            remark = _remark_perubahan(status_lama.get(i), df.loc[i, "Store ID"], df.loc[i, "Validasi"], df.loc[i, "Store ID"])
            item_sheet.append(_item_sheet(df, i, kode, nama, remark))
            continue
        df.loc[i, "Validasi"] = status_lama.get(i, STATUS_REVIEW)
        masalah.append(f"{kode or '(empty code)'}: {st.session_state.get('err_tagging', 'save failed')}")
    ok_sheet, err_sheet = _shared.update_generate_id_di_sheet(item_sheet)
    return masalah, ("" if ok_sheet else f"Google Sheet not updated: {err_sheet[:100]}")

@contextlib.contextmanager
def _ukur_ke(kamus, nama):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        kamus[nama] = kamus.get(nama, 0) + time.perf_counter() - t0

def simpan_perubahan():
    ss = st.session_state
    df = ss.df_toko
    wsave, t_save = {}, time.perf_counter()
    kosong  = df["Store ID"].fillna("").astype(str).str.strip().isin(["","-"])
    butuh   = df["Validasi"].eq(STATUS_VALID) & kosong
    if butuh.any():
        with _ukur_ke(wsave, "Generate ID (saat Save)"):
            df.loc[butuh,"Store ID"]      = generate_store_ids(df[butuh], df)
        baru_tergenerate              = [i for i in df.index[butuh] if df.loc[i,"Store ID"] != "-"]
        df.loc[baru_tergenerate,"Generated At"] = datetime.datetime.now(ZONA_WAKTU).replace(tzinfo=None)
        ss.baris_perlu_push.update(baru_tergenerate)
    with _ukur_ke(wsave, "periksa_ulang_id (baca ulang sheet + Supabase)"):
        ganti_id = periksa_ulang_id(df)       # ID yang ternyata sudah dipakai di sheet / Supabase -> dibuat ulang

    baris_target = set(ss.baris_kotor) | set(ss.baris_perlu_push)
    gagal_kunci  = []
    untuk_sheet  = []     # baris yang Store ID-nya di-generate -> disalin ke Google Sheet (kolom Generate ID + generated_at)
    # Kondisi SEBELUM Save (dibaca dari Supabase, bukan layar) -> dasar jejak perubahan Remark_status / Remark_ID / Remark_at.
    # Bolak-balik sebelum Save tidak dianggap perubahan. Kalau gagal membaca, jejak dilewati (data tetap tersimpan).
    with _ukur_ke(wsave, "Supabase baca status sebelum (sb_baca_status_id)"):
        sebelum = _shared.sb_baca_status_id([(str(df.loc[i,"Customer Store Code"]).strip(), str(df.loc[i,"Customer Store Name"]).strip())
                                             for i in baris_target]) or {}
    t_loop = time.perf_counter()
    for idx in baris_target:
        kode  = str(df.loc[idx,"Customer Store Code"]).strip()
        nama  = str(df.loc[idx,"Customer Store Name"]).strip()
        if not kode or not nama:
            continue
        tagging = str(df.loc[idx,"Validasi"]).strip()
        patch   = {"tagging": tagging}
        patch.update(_patch_kolom_edit(df.loc[idx]))     # City, Address, Long/Lat, Store Type, Store Name
        if tagging == "Existing" and pd.isna(df.loc[idx,"Generated At"]):
            # Existing dari SPV Confirmation / diketik manual: Store ID diisi user -> simpan apa adanya, jangan dikosongkan
            sid_ada = str(df.loc[idx,"Store ID"]).strip()
            if not _id_kosong(sid_ada):
                patch["Store ID"] = sid_ada
            patch["generated_at"] = None
        elif tagging != STATUS_VALID and tagging not in _shared.STATUS_TERKUNCI:
            # Need Review / Existing hasil generate: tidak punya ID -> dikosongkan
            patch["generated_at"] = None; patch["Store ID"] = ""
            df.loc[idx,"Store ID"] = "-"; df.loc[idx,"Generated At"] = pd.NaT
        else:
            # Generate ID, DMS Done, Basis Done: ID dikirim apa adanya (kalau kosong, kolom Store ID tidak disentuh)
            gid = str(df.loc[idx,"Store ID"]).strip()
            if not _id_kosong(gid):
                patch["Store ID"] = gid
                if pd.notna(df.loc[idx,"Generated At"]):
                    patch["generated_at"] = _iso_wib(df.loc[idx,"Generated At"])

        if sb_patch_by_kode_nama(TABLE_SUBMIT_NOO, kode, nama, patch):
            ss.baris_kotor.discard(idx)
            ss.baris_perlu_push.discard(idx)
            lama = sebelum.get((kode, nama))
            remark = (_remark_perubahan(lama[0], lama[1], df.loc[idx,"Validasi"], df.loc[idx,"Store ID"])
                      if lama is not None else {})
            untuk_sheet.append(_item_sheet(df, idx, kode, nama, remark))   # semua yang di-Save ikut ke sheet (edit + Generate ID + jejak)
        else:
            gagal_kunci.append(f"{kode}/{nama}")

    wsave["Supabase patch per baris"] = time.perf_counter() - t_loop
    with _ukur_ke(wsave, "Tulis ke Google Sheet (update_generate_id_di_sheet)"):
        ok_sheet, err_sheet = _shared.update_generate_id_di_sheet(untuk_sheet)   # gagal sheet tidak membatalkan simpan Supabase
    ambil_raw_noo.clear()     # download NOO membaca Store ID tersimpan dari Supabase -> segarkan setelah Save
    if not ss.baris_perlu_push:
        _shared.kunci_generate_lepas()     # semua ID sudah tersimpan -> Apps Script boleh jalan lagi

    jumlah = len(baris_target) - len(gagal_kunci)
    with _ukur_ke(wsave, "Catat riwayat (Supabase)"):
        catat_riwayat("Save changes", f"{jumlah} rows updated",
                      distributor=_distri_pertama(df, sorted(baris_target)))
    wsave["TOTAL Save changes"] = time.perf_counter() - t_save
    ss.waktu_save = wsave
    ss.tanda_tangan    = None
    ss.terakhir_update = waktu_sekarang()
    if gagal_kunci:
        detail = ss.get("err_tagging","-")
        ss.pesan_toast = f"Saved {jumlah} rows. FAILED {len(gagal_kunci)}: {', '.join(gagal_kunci)} — {detail}"
    else:
        ss.pesan_toast = f"Saved — {jumlah} rows updated" if jumlah else "No changes to save"
    if ganti_id:
        ss.pesan_toast += " | ID already used, regenerated: " + ", ".join(f"{a} → {b}" for a, b in ganti_id[:5])
    if not ok_sheet:
        ss.pesan_toast += f" | Google Sheet not updated: {err_sheet[:120]}"

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
    catat_riwayat("Import file", f"{nama_file} ({len(df):,} rows)",
                  distributor=_distri_pertama(df, df.index[:1]))
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
    if ss.get("id_merge_err"):          # sumber ID tambahan (DATA GENERATOR / id dms) gagal dibaca
        st.warning(ss.pop("id_merge_err"))
    for _kunci, _judul, _tot in (("waktu_generate", "Generate ID", "TOTAL generate_store_ids"),
                                 ("waktu_save", "Save changes", "TOTAL Save changes")):
        if ss.get(_kunci):              # TIMER loading (sementara): rincian waktu langkah terakhir
            w = ss[_kunci]
            with st.expander(f"⏱ {_judul} timing — total {w.get(_tot, 0):.2f}s (from last run)"):
                st.table(pd.DataFrame({"Step": list(w), "Seconds": [round(v, 2) for v in w.values()]}).set_index("Step"))
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
        b1, b2, b3, b4, info = st.columns([1, 1.4, 1.3, 1.5, 4.8], gap="small", vertical_alignment="center")
        b1.button(":material/refresh: Refresh", width="stretch", key="tombol_refresh", on_click=refresh_data_toko,
                  help="Reload data from Supabase. Unsaved changes will be lost.")
        tempat_unduh = b2.empty()      # diisi setelah hasil filter dihitung
        b3.button("Save changes", type="primary", width="stretch", key="tombol_simpan",
                  on_click=simpan_perubahan)
        opsi_ukuran = [10, 15, 25, 50]
        b4.selectbox("Rows per page", opsi_ukuran,
                     index=opsi_ukuran.index(ss.ukuran_halaman) if ss.ukuran_halaman in opsi_ukuran else 1,
                     format_func=lambda v: f"{v} rows / page", key="atur_ukuran_halaman",
                     on_change=_callback_ukuran_halaman, label_visibility="collapsed")
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

    c_select_all, c_generate, c_review, c_dms, c_basis, c_hapus, _ = st.columns([1.3,0.95,1.05,0.85,1.1,0.8,4.25], gap="small", vertical_alignment="center")
    with c_select_all:
        select_all = st.checkbox("Select All", key=f"select_all_{ss.kunci_editor}")
        ss.snapshot["Pilih"] = select_all
        ss.tampilan_kunci    = None
    with c_generate:
        tombol_generate = st.button("Generate ID", type="primary", key="tombol_generate_bulk", width="stretch")
    with c_review:
        tombol_review = st.button("Need Review", type="primary", key="tombol_review_bulk", width="stretch")
    # Tombol DMS Done / DMS & Basis Done: aktif/nonaktifnya bergantung pada baris yang dicentang (hasil grid, dihitung di bawah),
    # jadi tempatnya disiapkan dulu dan tombolnya digambar setelah grid.
    tempat_dms, tempat_basis = c_dms.empty(), c_basis.empty()
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
                          select_all=select_all, warna=warna_css, terkunci=_shared.STATUS_TERKUNCI)
    hasil_edit["Customer Store Name"] = ss.snapshot["Customer Store Name"]

    # ── Tombol DMS Done / DMS & Basis Done ─────────────────────────────────────────────
    # NONAKTIF secara default. Aktif HANYA jika semua baris yang dicentang, menurut data yang DIBACA ULANG dari Supabase
    # (bukan session_state), sudah berstatus Generate ID DAN sudah punya Store ID. Artinya: sudah ter-generate + sudah tersimpan.
    # Klik -> hanya status yang berubah (Store ID tidak disentuh), langsung disimpan ke Supabase, lalu tabel di-refresh.
    pilih_idx = hasil_edit.index[hasil_edit["Pilih"] == True].tolist()
    pasangan_pilih = tuple((str(ss.df_toko.loc[i, "Customer Store Code"]).strip(),
                            str(ss.df_toko.loc[i, "Customer Store Name"]).strip()) for i in pilih_idx)
    tombol_aktif, tombol_info = False, "Tick rows that are already Generate ID and saved."
    if pasangan_pilih:
        status_sb = _shared.sb_baca_status_id_singkat(pasangan_pilih)
        if status_sb is None:
            tombol_info = "Could not read Supabase to verify. Try again."
        else:
            tombol_aktif = all(status_sb.get(p, ("", ""))[0] == STATUS_VALID and not _id_kosong(status_sb.get(p, ("", ""))[1])
                               for p in pasangan_pilih)
            tombol_info = "" if tombol_aktif else "Only rows saved as Generate ID with a Store ID in Supabase can be marked."
    klik_dms   = tempat_dms.button("DMS Done", type="primary", key="tombol_dms_done", width="stretch",
                                   disabled=not tombol_aktif, help=tombol_info or None)
    klik_basis = tempat_basis.button("DMS & Basis Done", type="primary", key="tombol_basis_done", width="stretch",
                                     disabled=not tombol_aktif, help=tombol_info or None)
    if klik_dms or klik_basis:
        status_baru = _shared.STATUS_DMS if klik_dms else _shared.STATUS_BASIS
        segar = _shared.sb_baca_status_id(pasangan_pilih)         # verifikasi ULANG saat klik, tanpa cache
        if segar is None:
            ss.pesan_toast = "Could not verify with Supabase — status not changed."
            ss.pesan_toast_icon = ":material/warning:"
        else:
            df = ss.df_toko
            siap_idx = [i for i, p in zip(pilih_idx, pasangan_pilih)
                        if segar.get(p, ("", ""))[0] == STATUS_VALID and not _id_kosong(segar.get(p, ("", ""))[1])]
            status_lama = {i: df.loc[i, "Validasi"] for i in siap_idx}
            for i in siap_idx:
                df.loc[i, "Validasi"] = status_baru
            masalah, _info_sheet = ubah_status_dan_simpan(df, siap_idx, status_lama)
            n_lewat = len(pilih_idx) - len(siap_idx)
            ss.pesan_toast = (f"{len(siap_idx) - len(masalah)} row(s) set to {status_baru}"
                              + (f" | {n_lewat} skipped (not Generate ID in Supabase)" if n_lewat else "")
                              + (f" | {len(masalah)} NOT saved, reverted: {'; '.join(masalah[:2])}" if masalah else ""))
            ss.pesan_toast_icon = ":material/warning:" if (n_lewat or masalah) else ":material/check_circle:"
            ambil_raw_noo.clear()
        ss.snapshot = ss.df_toko.loc[ss.snapshot.index, KOLOM_TAMPIL].copy(); ss.kunci_editor += 1
        st.rerun()

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
                terhapus, gagal, ditolak_policy, nol_baris = [], [], False, []
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
                            nol_baris.append((kode, nama))
                # 0 baris terhapus tanpa error HTTP: bisa karena policy RLS menolak, atau barisnya memang tidak ada
                # (kode/nama beda). Cek ulang (baca saja) supaya pesan error-nya menunjuk penyebab yang benar.
                detail_nol = ""
                if nol_baris:
                    ada = _shared.sb_baca_status_id(nol_baris)
                    if ada is None:
                        detail_nol = " (could not re-check the rows)"
                    else:
                        ketemu = {f"{k} [status: {ada[(k, n)][0] or '-'}]" for k, n in nol_baris if (k, n) in ada}
                        hilang = [k for k, n in nol_baris if (k, n) not in ada]
                        if ketemu:
                            detail_nol += " Rows exist but were NOT deleted (blocked by Supabase): " + ", ".join(sorted(ketemu)[:5]) + "."
                        if hilang:
                            detail_nol += " Not found in Supabase (code/name differ): " + ", ".join(hilang[:5]) + "."
                    ditolak_policy = bool(nol_baris) and (ada is None or any((k, n) in ada for k, n in nol_baris))
                err_sheet = ""
                if terhapus:
                    # ikut hapus baris yang sama di Google Sheet (gagal sheet tidak membatalkan delete Supabase)
                    ok_sheet, err_sheet = _shared.hapus_noo_di_sheet([
                        {"kode": str(df.loc[i, "Customer Store Code"]).strip(),
                         "nama": str(df.loc[i, "Customer Store Name"]).strip()} for i in terhapus])
                    distri_hapus = _distri_pertama(df, terhapus)     # ambil SEBELUM baris di-drop
                    ss.df_toko = df.drop(index=terhapus)
                    ss.baris_kotor -= set(terhapus)
                    ss.baris_perlu_push -= set(terhapus)
                    ambil_raw_noo.clear()
                    catat_riwayat("Delete NOO", f"{len(terhapus)} rows", distributor=distri_hapus)
                ss.noo_hapus_antri = None
                ss.tanda_tangan = None; ss.snapshot = None
                ss.terakhir_update = waktu_sekarang()
                ss.pesan_toast = (f"{len(terhapus)} row(s) deleted"
                                  + (f" | {len(gagal)} failed" if gagal else "")
                                  + (f" | Google Sheet not updated: {err_sheet[:400]}" if err_sheet else ""))
                if ditolak_policy:
                    ss.noo_hapus_error = ("Supabase accepted the request but deleted 0 rows: the DELETE policy (RLS) on INPUT_NOO "
                                          "does not allow deleting these rows with this API key." + detail_nol)
                elif nol_baris:
                    ss.noo_hapus_error = "Delete failed:" + detail_nol
                elif gagal:
                    ss.noo_hapus_error = "Delete failed: " + "; ".join(gagal[:3])
                st.rerun()

    # Bulk Generate ID
    if tombol_generate:
        baris_dipilih = hasil_edit[hasil_edit["Pilih"] == True].index.tolist()
        # baris berstatus final (DMS Done / DMS & Basis Done) terkunci -> dilewati
        baris_final = [i for i in baris_dipilih if ss.df_toko.loc[i, "Validasi"] in _shared.STATUS_TERKUNCI]
        if baris_final:
            baris_dipilih = [i for i in baris_dipilih if i not in baris_final]
            st.warning(f"{len(baris_final)} locked row(s) skipped (status already DMS Done / DMS & Basis Done).")
        if not baris_dipilih:
            if not baris_final:
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
            # Tidak langsung push ke Supabase: hanya ditandai belum tersimpan (ID sementara, berurutan).
            # Baru tersimpan lewat "Save changes"; kalau tidak, hilang saat Refresh / reload.
            berhasil_idx = [i for i in baris_dipilih if i not in gagal_prefix]
            ss.baris_kotor.update(berhasil_idx)
            ss.baris_perlu_push.update(berhasil_idx)
            # rapikan nomor ID sementara per prefix sesuai urutan baris di tabel
            for prefix in {str(df.loc[i,"Store ID"]).strip()[:PANJANG_PREFIX] for i in berhasil_idx}:
                id_geser = resequence_prefix(df, prefix)
                ss.baris_kotor.update(id_geser); ss.baris_perlu_push.update(id_geser)
            for i in gagal_prefix:                      # gagal generate -> kembalikan status semula
                df.loc[i,"Validasi"] = STATUS_REVIEW
                ss.baris_kotor.discard(i); ss.baris_perlu_push.discard(i)
            berhasil = len(berhasil_idx)
            pesan = f"{berhasil} rows generated — click Save changes to save to Supabase"
            if ss.get("kunci_err"):
                pesan += f" | {ss.kunci_err}"
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

    # Bulk Need Review: baris yang dicentang diubah ke Need Review lewat jalur edit biasa di bawah (Store ID dibuang +
    # nomor ID lain di-resequence). Baris terkunci (DMS Done / DMS & Basis Done) dilewati.
    if tombol_review:
        baris_dipilih = hasil_edit.index[hasil_edit["Pilih"] == True].tolist()
        baris_final = [i for i in baris_dipilih if validasi_lama.get(i) in _shared.STATUS_TERKUNCI]
        baris_ubah = [i for i in baris_dipilih if i not in baris_final]
        if baris_final:
            st.warning(f"{len(baris_final)} locked row(s) skipped (status already DMS Done / DMS & Basis Done).")
        if not baris_dipilih:
            st.warning("No rows selected.")
        elif baris_ubah:
            hasil_edit.loc[baris_ubah, "Validasi"] = STATUS_REVIEW

    # Baris berstatus final (DMS Done / DMS & Basis Done) DIKUNCI: semua kolom selain Validation dikembalikan ke nilai
    # semula (pengaman server, selain input yang sudah dinonaktifkan di grid).
    baris_kunci = [i for i in hasil_edit.index if validasi_lama.get(i) in _shared.STATUS_TERKUNCI]
    if baris_kunci:
        for c in [c for c in KOLOM_EDIT if c in hasil_edit.columns and c != "Validasi"]:
            hasil_edit.loc[baris_kunci, c] = ss.df_toko.loc[baris_kunci, c]
    # Baris yang sudah DMS Done / DMS & Basis Done tidak boleh turun ke Need Review / Existing (ID-nya sudah dipakai di DMS).
    ditolak_status = []
    for i in hasil_edit.index[hasil_edit["Validasi"] != validasi_lama.loc[hasil_edit.index]]:
        if validasi_lama.get(i) in _shared.STATUS_TERKUNCI and hasil_edit.loc[i, "Validasi"] in (STATUS_REVIEW, "Existing"):
            hasil_edit.loc[i, "Validasi"] = validasi_lama.get(i)
            ditolak_status.append(f"#{ss.df_toko.loc[i, '#']}")
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
        final_ = validasi_skrg.isin(_shared.STATUS_TERKUNCI)       # DMS Done / DMS & Basis Done: Store ID dipertahankan (tidak digenerate)
        batal = [i for i in id_berubah if not pilih.loc[i] and not existing_spv.loc[i] and not final_.loc[i]]
        # Baris yang salah satu prefix-nya diubah ke Need Review / Existing: ID-nya dibuang, lalu nomor ID lain dengan
        # prefix sama yang BELUM di-save dirapikan lagi supaya tetap berurutan (resequence_prefix).
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
            # rapikan nomor ID sementara per prefix sesuai urutan baris di tabel
            for prefix in {str(df.loc[i,"Store ID"]).strip()[:PANJANG_PREFIX] for i in berhasil_id}:
                id_geser = resequence_prefix(df, prefix)
                ss.baris_kotor.update(id_geser); ss.baris_perlu_push.update(id_geser)
            if gagal_id:
                st.warning("Prefix not found: "
                           + ", ".join(sorted({str(df.loc[i,"Distributor"]) or "(empty)" for i in gagal_id})))
        snapshot_baru = ss.df_toko.loc[ss.snapshot.index, KOLOM_TAMPIL].copy()
        if not snapshot_baru["Validasi"].equals(ss.snapshot["Validasi"]):
            ss.snapshot = snapshot_baru; ss.kunci_editor += 1; st.rerun()
    if ditolak_status:          # turun dari DMS Done / DMS & Basis Done ditolak: gambar ulang grid + beri tahu user
        ss.pesan_toast = f"Status not changed for {', '.join(ditolak_status[:5])} — already DMS Done / DMS & Basis Done, cannot go back."
        ss.pesan_toast_icon = ":material/warning:"
        ss.kunci_editor += 1
        st.rerun()

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
    # tanpa kolom id: pakai customer_code + product + customer_product_code. `product` wajib ikut, karena satu
    # Customer Product Code (mis. "test") bisa dipakai beberapa product berbeda di customer code yang sama
    return [c for c in (_kolom(df, "customer_code"), _kolom(df, "product"), _kolom(df, "customer_product_code")) if c]

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

def _klik_refresh_sku():
    ss = st.session_state
    if ss.get("sku_pending"):
        ss.sku_konfirmasi = True
    else:
        _refresh_sku()

def _aksi_simpan_sku(df, kc_pk, tag, rk):
    ss = st.session_state
    with st.spinner("Saving changes..."):
        ok, gagal = _simpan_sku(df, kc_pk, tag, rk)
        if not gagal:
            ss.sku_df = ambil_sku_mapping()   # tarik ulang dari Supabase
    ss.sku_pesan = (("ok", f"{ok} record(s) saved") if not gagal else
                    ("error", f"{ok} saved, {len(gagal)} failed (kept as pending): " + "; ".join(gagal[:5])))

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
            b1.button(":material/refresh: Refresh", type="primary", key="sku_refresh_empty", width="stretch",
                      on_click=_refresh_sku)
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
        b1.button(":material/refresh: Refresh", type="primary", key="sku_refresh", width="stretch",
                  on_click=_klik_refresh_sku)
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
        b3.button(":material/save: Save changes", type="primary", key="sku_simpan",
                  disabled=not ss.sku_pending, width="stretch",
                  on_click=_aksi_simpan_sku, args=(df, kc_pk, tag, rk))
        belum = (f' <span class="teks-belumsimpan">● {len(ss.sku_pending)} unsaved</span>'
                 if ss.sku_pending else "")
        info.markdown(f'<div class="teks-showing">Showing {len(df_f):,} of {len(df):,} records{belum}</div>',
                      unsafe_allow_html=True)

    if ss.get("sku_konfirmasi"):
        st.warning(f"{len(ss.sku_pending)} unsaved change(s) will be lost if you refresh.")
        a, b, _ = st.columns([1.6, 1, 6])
        a.button("Discard & refresh", key="sku_discard", on_click=_refresh_sku)
        b.button("Cancel", key="sku_cancel", on_click=lambda: ss.update(sku_konfirmasi=False))

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
                distri_hapus = (next((str(v).strip() for v in df.loc[rk.isin(terhapus), kc_nama]
                                      if str(v).strip() and str(v).lower() != "nan"), "")
                                if kc_nama else "")           # ambil SEBELUM baris di-drop
                ss.sku_df = df[~rk.isin(terhapus)].reset_index(drop=True)
                ss.sku_pilih -= terhapus
                for k in terhapus:
                    ss.sku_pending.pop(k, None)
                ambil_sku_mapping.clear()
                ss["_reset_sku_select_all"] = True             # dieksekusi di run berikutnya, sebelum checkbox dibuat
                ss.sku_ver += 1
                catat_riwayat("Delete SKU Mapping", f"{len(terhapus)} rows", distributor=distri_hapus)
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
    return "Solved" if str(v).strip().lower() in ("solved", "closed", "done", "resolved") else "In Progress"

def _tandai_solved(id_tiket) -> str:
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

    st.markdown('<div class="fb-judul">Help &amp; Support</div>'
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
        try:
            peta_distri = muat_distri_code_map()          # kode (username) -> {"distributor": nama, ...} dari sheet Distributor Mapping
        except Exception:
            peta_distri = {}
        for i, (_, r) in enumerate(df.iterrows()):
            no = r.get("id")
            kode_user = str(r.get("username", "") or "").strip().upper()      # selalu tampil sebagai kode user (huruf besar)
            items.append({
                "id":     (no.item() if hasattr(no, "item") else no) if pd.notna(no) else None,
                "no":     str(no) if pd.notna(no) else str(len(df) - i),
                "title":  str(r.get("title", "") or "").strip(),
                "issue":  str(r.get("issue", "")).strip(),
                "user":   kode_user,
                "distri": (peta_distri.get(kode_user) or {}).get("distributor", ""),
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
                  and (not kata or kata.lower() in f'{x["title"]} {x["issue"]} {x["user"]} {x["distri"]} {x["wa"]} #{x["no"]}'.lower())]

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
                    f'<span>{_html.escape(x["user"])}'
                    + (f' · {_html.escape(x["distri"])}' if x["distri"] else '') +
                    f'</span><span>{_html.escape(x["prio"])}</span>'
                    + (f'<span>WA: <a href="https://wa.me/{x["wa"]}" target="_blank" rel="noopener" '
                       f'style="color:inherit">{x["wa"]}</a></span>' if x["wa"] else '<span>WA: -</span>')
                    + '</div>'
                    # penjelasan LENGKAP (semua baris, tanpa dipotong) di baris tersendiri di bawah meta
                    + (f'<div class="fb-detail">{_html.escape(x["issue"])}</div>'
                       if x["issue"] and x["issue"] != judul else '') +
                    '</div>'
                    f'<div class="fb-kanan"><span class="fb-badge {kelas}">● {x["status"]}</span>'
                    f'<div class="fb-tgl">{tgl}</div></div></div>')
            st.markdown("".join(baris), unsafe_allow_html=True)

            # ubah In Progress -> Solved (tiket di halaman ini yang punya id)
            is_bd = (ss.get("user") or {}).get("tagging") in ("RSA", "BD SUPPORT", "BD_SUPPORT")
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


_HASIL_FB_CSS = """<style>
.rf-hero, .rf-hero *, .rf-item, .rf-item *, .rf-kosong {
  font-family: "Source Sans", "Source Sans Pro", ui-sans-serif, system-ui, sans-serif; }
.rf-hero{display:grid;grid-template-columns:260px 1.2fr 0.9fr;gap:12px;margin:4px 0 12px}
@media (max-width:900px){.rf-hero{grid-template-columns:1fr}}
.rf-card{background:#fff;border:1px solid #f3dfe8;border-radius:14px;padding:10px 14px;
  box-shadow:0 6px 18px rgba(255,79,139,.07)}
.rf-skor{background:linear-gradient(135deg,#ff4f8b,#ff8a5c);color:#fff;border:none;display:flex;flex-direction:column}
.rf-skor .n{font-size:42px;font-weight:800;line-height:1}
.rf-skor .n small{font-size:16px;font-weight:600;opacity:.85}
.rf-skor .s{font-size:18px;letter-spacing:2px;margin:3px 0 1px}
.rf-skor .t:first-child{font-size:13.5px;font-weight:700;opacity:1}
.rf-skor .t{font-size:11.5px;opacity:.9}
.rf-dist{flex:1;display:flex;flex-direction:column;justify-content:center;margin-top:8px;padding-top:8px;border-top:1px solid rgba(255,255,255,.35)}
.rf-dist .b{display:flex;align-items:center;gap:7px;font-size:11px;color:#fff;margin:3px 0}
.rf-dist .b span.l{width:24px;font-weight:700}
.rf-dist .tr{flex:1;height:7px;background:rgba(255,255,255,.3);border-radius:9px;overflow:hidden}
.rf-dist .tr i{display:block;height:100%;border-radius:9px;background:#fff}
.rf-dist .c{width:24px;text-align:right}
.rf-ct{font-size:13.5px;font-weight:700;color:#1c1633;padding-bottom:6px;border-bottom:1px solid #f1e9ee;margin-bottom:6px}
.rf-ct span{font-weight:500;color:#b0a9c0;font-size:11px;margin-left:6px}
.rf-s3{display:grid;grid-template-columns:repeat(3,1fr);align-items:center;height:calc(100% - 32px);min-height:100px}
.rf-si{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:8px;padding:0 4px}
.rf-si + .rf-si{border-left:1px solid #f1e9ee}
.rf-sn{font-size:20px;font-weight:300;color:#3a3550;display:flex;align-items:baseline;gap:10px;line-height:1}
.rf-sn span{font-size:12px;font-weight:500}
.rf-sn .pos{color:#2fae78}.rf-sn .net{color:#d99a00}.rf-sn .neg{color:#e8202a}
.rf-cap{font-size:10.5px;color:#7d7690;text-align:center}
.rf-tot{display:flex;flex-direction:column}
.rf-grafik{margin-top:auto;padding-top:6px}
.rf-gt{font-size:10.5px;color:#7d7690;font-weight:600;margin-bottom:4px}
.rf-gx{display:flex;justify-content:space-between;font-size:9.5px;color:#b0a9c0}
.rf-bigrow{display:flex;align-items:center;justify-content:space-between;gap:10px}
.rf-ikon{flex:0 0 auto;display:flex;align-items:center;justify-content:center}
.rf-gauge{text-align:center;line-height:1}
.rf-gl{font-size:12px;font-weight:800;margin-top:-2px}
.rf-gp{font-size:11.5px;font-weight:700;margin-top:4px}
.rf-gp span{font-size:10px;font-weight:500;opacity:.8}
.rf-big{font-size:32px;font-weight:300;color:#1c1633;line-height:1.1;margin-top:0}
.rf-naik{color:#17a34a;font-weight:600;font-size:11.5px;margin-top:2px}
.rf-naik span{display:block;color:#7d7690;font-weight:400;font-size:10.5px;margin-top:2px}
.rf-item{display:flex;gap:12px;padding:12px 14px;border:1px solid #f3dfe8;border-radius:16px;margin-bottom:12px;
  background:#fff;transition:box-shadow .2s,transform .2s}
.rf-item:hover{box-shadow:0 8px 22px rgba(255,79,139,.12);transform:translateY(-1px)}
.rf-av{flex:0 0 44px;height:44px;border-radius:50%;display:flex;align-items:center;justify-content:center;
  font-size:24px;line-height:1;border:3px solid #fff;box-shadow:0 3px 10px rgba(0,0,0,.15)}
.rf-body{flex:1;min-width:0}
.rf-top{display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:center}
.rf-nama{font-weight:700;color:#1c1633;font-size:13px}
.rf-nama span{font-weight:500;color:#7d7690;font-size:11.5px}
.rf-tgl{font-size:10.5px;color:#9a93ad}
.rf-bintang{font-size:15px;letter-spacing:2px;margin:3px 0}
.rf-label{display:inline-block;font-size:10.5px;font-weight:700;padding:2px 10px;border-radius:20px;margin-left:8px;
  letter-spacing:0;vertical-align:middle}
.rf-chip{display:inline-block;font-size:10.5px;padding:2px 9px;border-radius:20px;background:#fff0f5;
  color:#d6336c;border:1px solid #ffd3e2;margin:4px 6px 0 0}
.rf-quote{margin-top:6px;padding:8px 12px;background:#faf8fc;border-left:3px solid #ff4f8b;border-radius:0 10px 10px 0;
  font-size:12px;line-height:1.45;color:#4a4560;white-space:pre-wrap;overflow-wrap:anywhere}
[class*="st-key-hf_row_"]{position:relative}
[class*="st-key-hf_del_"]{position:absolute;bottom:26px;right:14px;width:auto !important;z-index:2}
[class*="st-key-hf_del_"] button{min-height:30px;padding:2px 8px;border:1px solid #f3dfe8;background:#fff;color:#c2556f}
[class*="st-key-hf_del_"] button:hover{background:#fff0f5;border-color:#ff4f8b;color:#e8202a}
.rf-tut{display:flex;align-items:center;gap:16px;background:#fff;border:1px solid #f3dfe8;border-radius:14px;padding:10px 16px;
  margin:0 0 12px;box-shadow:0 6px 18px rgba(255,79,139,.07)}
.rf-tut .ik{flex:0 0 46px;height:46px;border-radius:50%;background:#fff0f5;display:flex;align-items:center;justify-content:center}
.rf-tut .bd{flex:1;min-width:0}
.rf-tut .tt{font-size:13px;font-weight:700;color:#1c1633}
.rf-tut .tt span{font-weight:500;color:#b0a9c0;font-size:11px;margin-left:6px}
.rf-tut .tr{height:7px;background:#f4eef2;border-radius:7px;overflow:hidden;margin:6px 0 4px}
.rf-tut .tr i{display:block;height:100%;background:linear-gradient(90deg,#ff7aa2,#ff9f6b);border-radius:7px}
.rf-tut .ct{font-size:11px;color:#7d7690}
.rf-tut .pc{font-size:26px;font-weight:300;color:#e8508a;white-space:nowrap}
.rf-tut .pc small{font-size:11px;color:#9a93ad;margin-left:4px}
.rf-tut2{background:#fff;border:1px solid #f3dfe8;border-radius:14px;padding:14px 18px;margin:0 0 12px;box-shadow:0 6px 18px rgba(255,79,139,.07)}
.rf-tut2 .tt{font-size:14px;font-weight:700;color:#1c1633;margin-bottom:10px}
.rf-tut2 .tt span{font-weight:500;color:#9a93ad;font-size:11px;margin-left:6px}
.rf-tut2 .tiles{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:6px}
.rf-tut2 .tile{border-radius:12px;padding:10px 14px;display:flex;align-items:center;gap:12px}
.rf-tut2 .tile.ya{background:#eefaf4}.rf-tut2 .tile.bl{background:#fff4ea}
.rf-tut2 .tile .em{line-height:0;display:flex}
.rf-tut2 .tp .nb span{display:inline-flex;align-items:center;gap:4px}
.rf-tut2 .tile .no{font-size:30px;font-weight:700;line-height:1}
.rf-tut2 .tile.ya .no{color:#2fae78}.rf-tut2 .tile.bl .no{color:#d9822b}
.rf-tut2 .tile .lb{font-size:11px;color:#7d7690}
.rf-tut2 .tp{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:7px 2px;border-top:1px solid #f7eef2}
.rf-tut2 .tp .nm{font-size:12.5px;color:#1c1633;font-weight:600}
.rf-tut2 .tp .nb{display:flex;gap:14px;font-size:13px;font-weight:700;white-space:nowrap}
.rf-tut2 .tp .ya{color:#2fae78}.rf-tut2 .tp .bl{color:#d9822b}
.rf-tc{border-left:3px solid #ffb3c9;background:#fffafb;border-radius:0 8px 8px 0;padding:6px 10px;margin-bottom:6px;font-size:12px;color:#4a4560}
.rf-tc b{color:#1c1633}.rf-tc .ya{color:#2fae78;font-weight:700}.rf-tc .bl{color:#d9822b;font-weight:700}.rf-tc .tg{color:#9a93ad;font-size:10.5px;margin-left:6px}
.rf-kosong{text-align:center;color:#9a93ad;padding:34px 0}
</style>"""

_WARNA_RATING = {1: "#e5484d", 2: "#f76b15", 3: "#f5b400", 4: "#46a758", 5: "#12a594"}
_HASIL_FB_PER_HAL = 6


# Avatar hewan bergaya kawaii (SVG orisinal, digambar sendiri). Satu hewan per user, dipilih dari hash nama.
_WAJAH = ('<circle cx="24" cy="38" r="2.6" fill="#2b2233"/><circle cx="40" cy="38" r="2.6" fill="#2b2233"/>'
          '<circle cx="25" cy="37" r="0.9" fill="#fff"/><circle cx="41" cy="37" r="0.9" fill="#fff"/>'
          '<ellipse cx="19" cy="44" rx="3.6" ry="2.2" fill="#ff9fb8" opacity=".8"/>'
          '<ellipse cx="45" cy="44" rx="3.6" ry="2.2" fill="#ff9fb8" opacity=".8"/>'
          '<path d="M29 44q3 3 6 0" stroke="#2b2233" stroke-width="1.6" fill="none" stroke-linecap="round"/>')
_HEWAN_SVG = {
    "cat": '<path d="M13 28L15 8L28 19Z" fill="#ffc98a"/><path d="M51 28L49 8L36 19Z" fill="#ffc98a"/>'
           '<path d="M16 24L17 14L24 20Z" fill="#ffadc2"/><path d="M48 24L47 14L40 20Z" fill="#ffadc2"/>'
           '<circle cx="32" cy="38" r="21" fill="#ffd9a8"/>' + _WAJAH +
           '<path d="M6 40h9M7 46l8-2M58 40h-9M57 46l-8-2" stroke="#b08a6a" stroke-width="1.2" stroke-linecap="round"/>',
    "bear": '<circle cx="14" cy="21" r="8" fill="#b9825a"/><circle cx="50" cy="21" r="8" fill="#b9825a"/>'
            '<circle cx="14" cy="21" r="4" fill="#e8b48f"/><circle cx="50" cy="21" r="4" fill="#e8b48f"/>'
            '<circle cx="32" cy="38" r="21" fill="#c99468"/><ellipse cx="32" cy="45" rx="9" ry="7" fill="#f1d3b3"/>'
            + _WAJAH.replace('<path d="M29 44q3 3 6 0"', '<ellipse cx="32" cy="42" rx="2.4" ry="1.8" fill="#3b2a2a"/><path d="M29 46q3 3 6 0"'),
    "rabbit": '<ellipse cx="22" cy="14" rx="6" ry="14" fill="#fff"/><ellipse cx="42" cy="14" rx="6" ry="14" fill="#fff"/>'
              '<ellipse cx="22" cy="15" rx="3" ry="9" fill="#ffc2d4"/><ellipse cx="42" cy="15" rx="3" ry="9" fill="#ffc2d4"/>'
              '<circle cx="32" cy="40" r="20" fill="#fff" stroke="#f1e6ee" stroke-width="1"/>' + _WAJAH,
    "panda": '<circle cx="14" cy="21" r="8" fill="#2b2233"/><circle cx="50" cy="21" r="8" fill="#2b2233"/>'
             '<circle cx="32" cy="38" r="21" fill="#fff" stroke="#eee" stroke-width="1"/>'
             '<ellipse cx="23" cy="37" rx="6" ry="7" fill="#2b2233" transform="rotate(-20 23 37)"/>'
             '<ellipse cx="41" cy="37" rx="6" ry="7" fill="#2b2233" transform="rotate(20 41 37)"/>'
             '<circle cx="24" cy="37" r="2.4" fill="#fff"/><circle cx="40" cy="37" r="2.4" fill="#fff"/>'
             '<circle cx="24.4" cy="37.2" r="1.2" fill="#2b2233"/><circle cx="39.6" cy="37.2" r="1.2" fill="#2b2233"/>'
             '<ellipse cx="32" cy="44" rx="2.4" ry="1.8" fill="#2b2233"/>'
             '<path d="M29 47q3 3 6 0" stroke="#2b2233" stroke-width="1.6" fill="none" stroke-linecap="round"/>'
             '<ellipse cx="18" cy="47" rx="3.4" ry="2" fill="#ff9fb8" opacity=".7"/><ellipse cx="46" cy="47" rx="3.4" ry="2" fill="#ff9fb8" opacity=".7"/>',
    "dog": '<ellipse cx="11" cy="32" rx="7" ry="13" fill="#9a6b45" transform="rotate(15 11 32)"/>'
           '<ellipse cx="53" cy="32" rx="7" ry="13" fill="#9a6b45" transform="rotate(-15 53 32)"/>'
           '<circle cx="32" cy="38" r="20" fill="#f3d3a8"/><ellipse cx="32" cy="46" rx="9" ry="7" fill="#fff3e0"/>'
           + _WAJAH.replace('<path d="M29 44q3 3 6 0"', '<ellipse cx="32" cy="42" rx="3" ry="2.2" fill="#2b2233"/><path d="M29 47q3 3 6 0"'),
    "frog": '<circle cx="19" cy="20" r="9" fill="#7ed37e"/><circle cx="45" cy="20" r="9" fill="#7ed37e"/>'
            '<circle cx="19" cy="20" r="5" fill="#fff"/><circle cx="45" cy="20" r="5" fill="#fff"/>'
            '<circle cx="19" cy="20.5" r="2.6" fill="#2b2233"/><circle cx="45" cy="20.5" r="2.6" fill="#2b2233"/>'
            '<ellipse cx="32" cy="42" rx="22" ry="17" fill="#8fdc8f"/>'
            '<ellipse cx="17" cy="45" rx="3.6" ry="2.2" fill="#ff9fb8" opacity=".8"/><ellipse cx="47" cy="45" rx="3.6" ry="2.2" fill="#ff9fb8" opacity=".8"/>'
            '<path d="M24 46q8 7 16 0" stroke="#2b2233" stroke-width="1.8" fill="none" stroke-linecap="round"/>'
            '<circle cx="29" cy="38" r="1" fill="#4a9a4a"/><circle cx="35" cy="38" r="1" fill="#4a9a4a"/>',
    "penguin": '<circle cx="32" cy="36" r="23" fill="#3a3550"/><ellipse cx="32" cy="42" rx="16" ry="15" fill="#fff"/>'
               '<circle cx="24" cy="36" r="2.6" fill="#2b2233"/><circle cx="40" cy="36" r="2.6" fill="#2b2233"/>'
               '<circle cx="25" cy="35" r="0.9" fill="#fff"/><circle cx="41" cy="35" r="0.9" fill="#fff"/>'
               '<path d="M27 41h10l-5 6Z" fill="#ffb347"/>'
               '<ellipse cx="19" cy="42" rx="3.4" ry="2" fill="#ff9fb8" opacity=".8"/><ellipse cx="45" cy="42" rx="3.4" ry="2" fill="#ff9fb8" opacity=".8"/>',
    "chick": '<path d="M32 10q-5 -3 -6 2q4 0 6 4q2 -4 6 -4q-1 -5 -6 -2Z" fill="#ffd23f"/>'
             '<circle cx="32" cy="38" r="22" fill="#ffe066"/>' + _WAJAH.replace('<path d="M29 44q3 3 6 0" stroke="#2b2233" stroke-width="1.6" fill="none" stroke-linecap="round"/>', '<path d="M28 42h8l-4 5Z" fill="#ff9f43"/>') +
             '<path d="M9 36q-4 8 3 11M55 36q4 8 -3 11" stroke="#f5c518" stroke-width="3" fill="none" stroke-linecap="round"/>',
}
_HEWAN_URUT = list(_HEWAN_SVG)


def _hewan_avatar(nama):
    # hewan kawaii acak tapi KONSISTEN per user (dipilih dari hash nama), jadi user yang sama selalu dapat hewan yang sama
    import zlib
    h = _HEWAN_URUT[zlib.crc32(str(nama).encode("utf-8")) % len(_HEWAN_URUT)]
    return f'<svg width="34" height="34" viewBox="0 0 64 64" xmlns="http://www.w3.org/2000/svg">{_HEWAN_SVG[h]}</svg>'


def _smiley_svg(persen, jenis):
    # lingkaran wajah terisi dari bawah sesuai persen: "pos" hijau senyum, "net" kuning datar, "neg" merah cemberut
    warna, muda, gelap, mulut = {
        "pos": ("#5fcf9a", "#d6f5e6", "#1f6b4a", "M13 28q7 8 14 0"),
        "net": ("#f5b400", "#fbe3a0", "#7a5500", "M14 29h12"),
        "neg": ("#e8202a", "#f9b9bd", "#8f1018", "M13 31q7 -7 14 0"),
    }[jenis]
    return (f'<svg width="56" height="56" viewBox="0 0 40 40"><defs><linearGradient id="sm{jenis}" x1="0" y1="0" x2="0" y2="1">'
            f'<stop offset="{100 - persen:.1f}%" stop-color="{muda}"/><stop offset="{100 - persen:.1f}%" stop-color="{warna}"/></linearGradient></defs>'
            f'<circle cx="20" cy="20" r="19.4" fill="url(#sm{jenis})" stroke="{warna}" stroke-width="1.2"/>'
            f'<circle cx="13" cy="15" r="2.2" fill="{gelap}"/><circle cx="27" cy="15" r="2.2" fill="{gelap}"/>'
            f'<path d="{mulut}" stroke="{gelap}" stroke-width="2.4" fill="none" stroke-linecap="round"/></svg>')


def _garis_harian_svg(nilai):
    # line chart kecil (area + garis + titik) jumlah feedback per hari
    w, h, pad = 220, 62, 6
    mx = max(max(nilai), 1)
    n = len(nilai)
    xs = [pad + i * (w - 2 * pad) / (n - 1) for i in range(n)]
    ys = [h - pad - (v / mx) * (h - 2 * pad) for v in nilai]
    # kurva halus (bezier dengan tangen horizontal di tiap titik): tidak ada lonjakan / overshoot di bawah garis dasar
    d = f"M{xs[0]:.1f} {ys[0]:.1f}"
    for i in range(1, n):
        mx_ = (xs[i - 1] + xs[i]) / 2
        d += f" C{mx_:.1f} {ys[i - 1]:.1f} {mx_:.1f} {ys[i]:.1f} {xs[i]:.1f} {ys[i]:.1f}"
    area = f"{d} L{xs[-1]:.1f} {h - pad} L{xs[0]:.1f} {h - pad} Z"
    # titik dibuat dari garis pendek ber-ujung bulat dengan vector-effect non-scaling-stroke: tetap BULAT walau svg diregangkan
    titik = "".join(f'<path d="M{x:.1f} {y:.1f}h0" stroke="#ff4f8b" stroke-width="9" stroke-linecap="round" vector-effect="non-scaling-stroke"/>'
                    f'<path d="M{x:.1f} {y:.1f}h0" stroke="#fff" stroke-width="4.5" stroke-linecap="round" vector-effect="non-scaling-stroke"/>'
                    for x, y, v in zip(xs, ys, nilai) if v > 0)
    return (f'<svg viewBox="0 0 {w} {h}" width="100%" height="{h}" preserveAspectRatio="none">'
            '<defs><linearGradient id="gh" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#ff4f8b" stop-opacity=".30"/>'
            '<stop offset="1" stop-color="#ff4f8b" stop-opacity="0"/></linearGradient></defs>'
            f'<line x1="{pad}" y1="{h - pad}" x2="{w - pad}" y2="{h - pad}" stroke="#f1e9ee"/>'
            f'<path d="{area}" fill="url(#gh)"/>'
            f'<path d="{d}" fill="none" stroke="#ff4f8b" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round" vector-effect="non-scaling-stroke"/>'
            f'{titik}</svg>')


def _gauge_rating_html(rata):
    # gauge busur (terisi sesuai rata-rata rating / 5) + wajah di tengah + label di bawah
    import math as _m
    if rata >= 4.5:   warna, label, mood = "#17c98f", "Superb!", "senang"
    elif rata >= 3.5: warna, label, mood = "#46a758", "Great", "senang"
    elif rata >= 2.5: warna, label, mood = "#f5b400", "Okay", "datar"
    elif rata >= 1.5: warna, label, mood = "#f76b15", "Poor", "sedih"
    else:             warna, label, mood = "#e5484d", "Bad", "sedih"
    cx, cy, r, sapuan = 60, 54, 46, 240
    frac = max(0.0, min(1.0, rata / 5))

    def titik(f):
        t = _m.radians(210 - sapuan * f)
        return cx + r * _m.cos(t), cy - r * _m.sin(t)
    x0, y0 = titik(0); x1, y1 = titik(1); kx, ky = titik(frac)
    busur = f"M{x0:.1f} {y0:.1f} A{r} {r} 0 1 1 {x1:.1f} {y1:.1f}"
    mulut = {"senang": "M50 56q10 11 20 0Z", "datar": "M52 60h16", "sedih": "M52 63q8-8 16 0"}[mood]
    mulut_el = (f'<path d="{mulut}" fill="#fff"/>' if mood == "senang"
                else f'<path d="{mulut}" stroke="#fff" stroke-width="2.6" fill="none" stroke-linecap="round"/>')
    mata = ('<path d="M48 47q4-5 8 0M64 47q4-5 8 0" stroke="#fff" stroke-width="2.6" fill="none" stroke-linecap="round"/>'
            if mood == "senang" else '<circle cx="52" cy="48" r="2.6" fill="#fff"/><circle cx="68" cy="48" r="2.6" fill="#fff"/>')
    kilau = "".join(f'<path d="M{x} {y-3}v6M{x-3} {y}h6" stroke="{warna}" stroke-width="1.3" stroke-linecap="round"/>'
                    for x, y in ((38, 30), (84, 34), (88, 66)))
    return (f'<div class="rf-gauge"><svg viewBox="0 0 120 100" width="66" height="55">'
            f'<path d="{busur}" fill="none" stroke="#e7e3ea" stroke-width="5" stroke-linecap="round"/>'
            f'<path d="{busur}" fill="none" stroke="{warna}" stroke-width="5" stroke-linecap="round" pathLength="100" '
            f'stroke-dasharray="{frac * 100:.1f} 100"/>'
            f'<circle cx="{kx:.1f}" cy="{ky:.1f}" r="5" fill="#fff" stroke="{warna}" stroke-width="2.5"/>'
            f'<circle cx="{cx}" cy="{cy}" r="21" fill="{warna}"/>{mata}{mulut_el}{kilau}</svg>'
            f'<div class="rf-gl" style="color:{warna}">{label}</div>'
            f'<div class="rf-gp" style="color:{warna}">{frac * 100:.0f}% <span>of max score</span></div></div>')


def _bintang_html(n):
    n = int(n or 0)
    return (f'<span style="color:{_WARNA_RATING.get(n, "#ccc")}">' + "★" * n + "</span>"
            f'<span style="color:#e3dde8">' + "★" * (5 - n) + "</span>")


@st.cache_data(ttl=30, show_spinner=False)
def _ambil_hasil_feedback():
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{TABLE_FEEDBACK}",
                     headers={**_SB_HEADERS, "Prefer": ""},
                     params={"select": "*", "order": "created_at.desc", "limit": 2000}, timeout=20)
    r.raise_for_status()
    return r.json()


@st.cache_data(ttl=30, show_spinner=False)
def _ambil_tutorial_feedback():
    r = requests.get(f"{SUPABASE_URL}/rest/v1/tutorial_feedback",
                     headers={**_SB_HEADERS, "Prefer": ""},
                     params={"select": "*", "order": "created_at.desc", "limit": 2000}, timeout=20)
    r.raise_for_status()
    return r.json()


@st.dialog("Delete tutorial feedback", width="small")
def _konfirmasi_hapus_tutorial_fb(tid, nama):
    st.markdown(f"Are you sure you want to delete the tutorial feedback from **{nama}**?")
    st.warning("This feedback will be permanently removed. This cannot be undone.")
    c_ya, c_batal = st.columns(2)
    if c_batal.button("Cancel", key="dlg_tf_batal", width="stretch"):
        st.rerun()
    if c_ya.button("Yes, delete", type="primary", key="dlg_tf_ya", width="stretch"):
        n, err = _sb_tabel_hapus("tutorial_feedback", {"id": f"eq.{tid}"})
        if err:
            st.error(f"Delete failed: {err}")
            return
        if n == 0:
            st.error("Supabase accepted the request but deleted 0 rows: tutorial_feedback has no DELETE policy for this API key (RLS).")
            return
        _ambil_tutorial_feedback.clear()
        st.session_state.pesan_toast = "Tutorial feedback deleted."
        st.rerun()


def _kartu_tutorial():
    # feedback tutorial (tabel tutorial_feedback) ditampilkan terpisah dari penilaian aplikasi
    import html as _h
    try:
        rows = _ambil_tutorial_feedback()
    except Exception as e:
        st.caption(f"Tutorial feedback belum bisa dimuat: {e}")
        return
    if not rows:
        st.markdown('<div class="rf-tut2"><div class="tt">Tutorial feedback <span>0 jawaban</span></div>'
                    '<div class="ct">No response yet. Displayed after the user completes the tutorial and submits their feedback.</div></div>',
                    unsafe_allow_html=True)
        return
    n = len(rows)
    ya = sum(1 for x in rows if x.get("membantu"))
    bl = n - ya
    from tutorial import _wajah_kawaii      # wajah kawaii yang sama dengan pilihan di popup feedback tutorial
    f_ya, f_bl = _wajah_kawaii("ya", 44), _wajah_kawaii("belum", 44)
    k_ya, k_bl = _wajah_kawaii("ya", 22), _wajah_kawaii("belum", 22)

    # ringkasan per tutorial (judul diambil dari baris terbaru supaya ikut kalau judulnya diganti)
    per = {}
    for x in rows:
        k = str(x.get("tutorial_key") or "-")
        d = per.setdefault(k, {"judul": x.get("tutorial_judul") or k, "ya": 0, "bl": 0})
        d["ya" if x.get("membantu") else "bl"] += 1
    baris_html = ""
    for k, d in sorted(per.items(), key=lambda kv: -(kv[1]["ya"] + kv[1]["bl"])):
        baris_html += (f'<div class="tp"><div class="nm">{_h.escape(str(d["judul"]))}</div>'
                       f'<div class="nb"><span class="ya">{k_ya}{d["ya"]}</span><span class="bl">{k_bl}{d["bl"]}</span></div></div>')
    st.markdown(
        f'<div class="rf-tut2"><div class="tt">Tutorial feedback <span>{n} jawaban</span></div>'
        f'<div class="tiles"><div class="tile ya"><div class="em">{f_ya}</div><div class="no">{ya}</div><div class="lb">Membantu</div></div>'
        f'<div class="tile bl"><div class="em">{f_bl}</div><div class="no">{bl}</div><div class="lb">Belum membantu</div></div></div>'
        f'{baris_html}</div>', unsafe_allow_html=True)

    komentar = [x for x in rows if str(x.get("komentar") or "").strip()]
    if komentar:
        with st.expander(f"Tutorial comments ({len(komentar)})", expanded=False):
            hanya_bl = st.toggle("Belum membantu saja", key="tf_hanya_bl")
            tampil = [x for x in komentar if not (hanya_bl and x.get("membantu"))]
            bisa_hapus = (st.session_state.get("user") or {}).get("tagging") in ("RSA", "BD SUPPORT", "BD_SUPPORT")   # sama dengan hapus feedback aplikasi
            for x in tampil[:30]:
                ok = bool(x.get("membantu"))
                kartu = (f'<div class="rf-tc"><b>{_h.escape(str(x.get("user") or "-"))}</b> '
                         f'<span class="{"ya" if ok else "bl"}">{"Membantu" if ok else "Belum membantu"}</span>'
                         f'<span class="tg">{_h.escape(str(x.get("tutorial_judul") or x.get("tutorial_key") or ""))}</span>'
                         f'<div>{_h.escape(str(x.get("komentar") or ""))}</div></div>')
                tid = x.get("id")
                if bisa_hapus and tid is not None:
                    c_k, c_h = st.columns([20, 1], vertical_alignment="center", gap="small")
                    c_k.markdown(kartu, unsafe_allow_html=True)
                    if c_h.button(":material/delete:", key=f"tf_del_{tid}", help="Delete this comment"):
                        _konfirmasi_hapus_tutorial_fb(tid, str(x.get("user") or "-"))
                else:
                    st.markdown(kartu, unsafe_allow_html=True)
            if not tampil:
                st.caption("Tidak ada komentar.")


def tab_hasil_feedback():
    import html as _html
    ss = st.session_state
    ss.setdefault("hf_halaman", 1)
    st.markdown(_HASIL_FB_CSS, unsafe_allow_html=True)
    st.markdown('<div class="fb-judul">Feedback</div>'
                '<div class="fb-sub">What users say about the NOO &amp; SKU Mapping submission app</div>',
                unsafe_allow_html=True)
    try:
        data = _ambil_hasil_feedback()
    except Exception as e:
        st.error(f"Failed to load feedback: {e}")
        return
    # baris lama kategori "Tutorial" (sebelum ada tabel tutorial_feedback) tidak ikut statistik/rating aplikasi,
    # tapi tetap ditampilkan di daftar di bawah supaya bisa dihapus dari sini.
    lama_tutorial = [x for x in data if str(x.get("kategori") or "").strip() == "Tutorial"]
    data = [x for x in data if str(x.get("kategori") or "").strip() != "Tutorial"]
    if not data:
        _kartu_tutorial()
        st.markdown('<div class="rf-kosong">No app feedback yet.</div>', unsafe_allow_html=True)
        return
    try:
        peta = muat_distri_code_map()
    except Exception:
        peta = {}

    def _rating(x):
        try:
            return int(x.get("rating") or 0)
        except Exception:
            return 0

    total = len(data)
    rata = sum(_rating(x) for x in data) / total
    puas = sum(1 for x in data if _rating(x) >= 4) / total * 100
    jml = {n: sum(1 for x in data if _rating(x) == n) for n in range(1, 6)}
    bulat = int(round(rata))
    n_pos = jml[4] + jml[5]
    n_neg = jml[1] + jml[2]
    n_net = jml[3]
    pos_p, neg_p, net_p = n_pos / total * 100, n_neg / total * 100, n_net / total * 100
    harian, tgl_awal, tgl_akhir = [0] * 14, "", ""
    try:
        waktu_fb = pd.to_datetime(pd.Series([x.get("created_at") for x in data]), utc=True, errors="coerce").dt.tz_convert(ZONA_WAKTU)
        hari_ini = pd.Timestamp.now(tz=ZONA_WAKTU).normalize()
        selisih = (hari_ini - waktu_fb.dt.normalize()).dt.days
        harian = [int((selisih == 13 - k).sum()) for k in range(14)]      # 14 hari terakhir, lama -> baru
        tgl_awal = (hari_ini - pd.Timedelta(days=13)).strftime("%d %b")
        tgl_akhir = hari_ini.strftime("%d %b")
    except Exception:
        pass
    try:
        sekarang = pd.Timestamp.now(tz="UTC")
        minggu = int((pd.to_datetime(pd.Series([x.get("created_at") for x in data]), utc=True, errors="coerce")
                      >= sekarang - pd.Timedelta(days=7)).sum())
    except Exception:
        minggu = 0
    dist = "".join(
        f'<div class="b"><span class="l">{n}★</span><div class="tr"><i style="width:{jml[n] / total * 100:.0f}%"></i></div>'
        f'<span class="c">{jml[n]}</span></div>' for n in (5, 4, 3, 2, 1))

    def _s_item(n, p, jenis, cap):
        return (f'<div class="rf-si">{_smiley_svg(p, jenis)}<div class="rf-sn">{n}<span class="{jenis}">{p:.0f}%</span></div>'
                f'<div class="rf-cap">{cap}</div></div>')
    st.markdown(
        '<div class="rf-hero">'
        f'<div class="rf-card rf-skor"><div class="t">Average rating</div><div class="n">{rata:.1f}<small> / 5</small></div>'
        f'<div class="s">{"★" * bulat}{"☆" * (5 - bulat)}</div><div class="t">from {total} response(s)</div>'
        f'<div class="rf-dist">{dist}</div></div>'
        f'<div class="rf-card rf-sent"><div class="rf-ct">Sentiment <span>All time</span></div><div class="rf-s3">'
        + _s_item(n_pos, pos_p, "pos", "Positive (4–5★)") + _s_item(n_net, net_p, "net", "Neutral (3★)")
        + _s_item(n_neg, neg_p, "neg", "Negative (1–2★)") +
        '</div></div>'
        f'<div class="rf-card rf-tot"><div class="rf-ct">Feedback <span>All time</span></div>'
        f'<div class="rf-bigrow"><div class="rf-big">{total}</div><div class="rf-ikon">{_gauge_rating_html(rata)}</div></div>'
        f'<div class="rf-naik">▲ +{minggu} <span>received in the last 7 days</span></div>'
        f'<div class="rf-grafik"><div class="rf-gt">Last 14 days</div>{_garis_harian_svg(harian)}'
        f'<div class="rf-gx"><span>{tgl_awal}</span><span>{tgl_akhir}</span></div></div></div></div>', unsafe_allow_html=True)

    _kartu_tutorial()
    opsi = ["All", "5★", "4★", "3★", "2★", "1★"]
    c_f, c_c = st.columns([3, 2], vertical_alignment="center")
    pilih = c_f.segmented_control("Rating", opsi, key="hf_filter", label_visibility="collapsed",
                                  on_change=lambda: ss.update(hf_halaman=1)) or "All"
    kata = c_c.text_input("Search", key="hf_cari", placeholder="Search user, category, comment...",
                          label_visibility="collapsed", on_change=lambda: ss.update(hf_halaman=1))

    def _cocok(x):
        if pilih != "All" and _rating(x) != int(pilih[0]):
            return False
        if kata:
            u = str(x.get(KOLOM_FB_USER) or "").upper()
            gab = f'{u} {(peta.get(u) or {}).get("distributor", "")} {x.get("kategori") or ""} ' \
                  f'{x.get("keterangan") or ""} {x.get("other_feedback") or ""}'
            return kata.lower() in gab.lower()
        return True

    semua_daftar = sorted(data + lama_tutorial, key=lambda x: str(x.get("created_at") or ""), reverse=True)
    tampil = [x for x in semua_daftar if _cocok(x)]
    bisa_hapus = (ss.get("user") or {}).get("tagging") in ("RSA", "BD SUPPORT", "BD_SUPPORT")
    n_hal = max(1, math.ceil(len(tampil) / _HASIL_FB_PER_HAL))
    ss.hf_halaman = min(max(1, ss.hf_halaman), n_hal)
    awal = (ss.hf_halaman - 1) * _HASIL_FB_PER_HAL
    if not tampil:
        st.markdown('<div class="rf-kosong">No feedback matches.</div>', unsafe_allow_html=True)
    for x in tampil[awal:awal + _HASIL_FB_PER_HAL]:
        r = _rating(x)
        kode = str(x.get(KOLOM_FB_USER) or "-").strip()
        kode = kode if kode.upper().startswith("RSA - ") else kode.upper()
        distri = (peta.get(kode) or {}).get("distributor", "")
        warna = _WARNA_RATING.get(r, "#999")
        tgl = ""
        try:
            w = pd.to_datetime(x.get("created_at"), utc=True).tz_convert(ZONA_WAKTU)
            tgl = f'{w.strftime("%b")} {w.day}, {w.year} • {w.strftime("%H:%M")}'
        except Exception:
            pass
        chips = "".join(f'<span class="rf-chip">{_html.escape(k.strip())}</span>'
                        for k in str(x.get("kategori") or "").split(",") if k.strip())
        if x.get("other_feedback"):
            chips += f'<span class="rf-chip">Other: {_html.escape(str(x["other_feedback"]))}</span>'
        fid = x.get("id")
        with st.container(key=f"hf_row_{fid}" if fid is not None else None):
          st.markdown(
            f'<div class="rf-item"><div class="rf-av" style="background:{warna}">{_hewan_avatar(kode)}</div>'
            f'<div class="rf-body"><div class="rf-top"><div class="rf-nama">{_html.escape(kode)}'
            + (f' <span>· {_html.escape(distri)}</span>' if distri else '') +
            f'</div><div class="rf-tgl">{tgl}</div></div>'
            f'<div class="rf-bintang">{_bintang_html(r)}<span class="rf-label" style="background:{warna}22;color:{warna}">'
            f'{_html.escape(str(x.get("interpretasi") or ""))}</span></div>'
            f'{chips}'
            + (f'<div class="rf-quote">{_html.escape(str(x["keterangan"]))}</div>' if x.get("keterangan") else '') +
            '</div></div>', unsafe_allow_html=True)
          if fid is not None and bisa_hapus:
            if st.button(":material/delete:", key=f"hf_del_{fid}", help="Delete this feedback"):
                _konfirmasi_hapus_feedback(fid, kode)

    with st.container(key="fb_footer"):              # CSS yang sama dengan pagination Help & Support (1 baris, tombol kecil)
        b0, b1, b2, b3 = st.columns([10, 1, 1, 1], gap="xxsmall", vertical_alignment="center")
        b0.markdown(f'<div class="fb-info">Showing {awal + 1 if tampil else 0}-'
                    f'{min(awal + _HASIL_FB_PER_HAL, len(tampil))} of {len(tampil)} feedback</div>',
                    unsafe_allow_html=True)
        b1.button("‹", key="fb_prev", on_click=lambda: ss.update(hf_halaman=ss.hf_halaman - 1),
                  disabled=ss.hf_halaman <= 1, width="stretch")
        b2.button(str(ss.hf_halaman), key="fb_cur", disabled=True, width="stretch")
        b3.button("›", key="fb_next", on_click=lambda: ss.update(hf_halaman=ss.hf_halaman + 1),
                  disabled=ss.hf_halaman >= n_hal, width="stretch")


# ── Grafik native (Vega/Altair, tanpa unduh library dari CDN & tanpa iframe) ──
@st.cache_data(max_entries=6, show_spinner=False)
def _excel_cache(df, startrow=1):
    return hasil_ke_excel(df, startrow=startrow)

def halaman_dashboard_tabs():
    # st.tabs selalu kembali ke tab pertama (Dashboard) setiap halaman dirender ulang penuh (st.rerun, toast, dll.).
    # Pilihan tampilan disimpan di session_state, jadi Feedback tetap di Feedback.
    ss = st.session_state
    opsi = [":material/trending_up: Dashboard SKU & NOO", ":material/chat: Help & Support", ":material/star: Feedback"]
    # key widget HARUS beda dari key container "dash_view" (yang dipakai CSS), kalau sama -> DuplicateElementKey
    if ss.get("dash_view_pilih") not in opsi:
        ss["dash_view_pilih"] = ss.get("_dash_view_terakhir", opsi[0])      # klik ulang pilihan aktif = None -> pertahankan
    with st.container(key="dash"):
        with st.container(key="dash_view"):
            st.segmented_control("View", opsi, key="dash_view_pilih", label_visibility="collapsed")
        ss["_dash_view_terakhir"] = ss["dash_view_pilih"]
        idx = opsi.index(ss["dash_view_pilih"])
        # Anti "jejak" tampilan tab lain: st.empty() langsung mengosongkan isi tab sebelumnya (tidak menunggu skrip selesai),
        # dan container ber-key per tab membuat tiap tab punya elemen sendiri, jadi tidak ada elemen lama yang tercampur/pudar.
        slot = st.empty()
        with slot.container(key=f"dash_isi_{idx}"):
            if idx == 1:
                tab_feedback()
            elif idx == 2:
                tab_hasil_feedback()
            else:
                halaman_dashboard()


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
    st.session_state.halaman_data = 1          # ukuran halaman berubah -> kembali ke halaman 1
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

@st.dialog("Delete feedback", width="small")
def _konfirmasi_hapus_feedback(fid, nama):
    st.markdown(f"Are you sure you want to delete the feedback from **{nama}**?")
    st.warning("This feedback will be permanently removed. This cannot be undone.")
    c_ya, c_batal = st.columns(2)
    if c_batal.button("Cancel", key="dlg_fb_batal", width="stretch"):
        st.rerun()
    if c_ya.button("Yes, delete", type="primary", key="dlg_fb_ya", width="stretch"):
        n, err = _sb_tabel_hapus(TABLE_FEEDBACK, {"id": f"eq.{fid}"})
        if err:
            st.error(f"Delete failed: {err}")
            return
        if n == 0:
            st.error("Supabase accepted the request but deleted 0 rows: the table has no DELETE policy for this API key (RLS).")
            return
        _ambil_hasil_feedback.clear()
        st.session_state.pesan_toast = "Feedback deleted."
        st.rerun()


@st.dialog("Delete user", width="small")
def _konfirmasi_hapus_user(username: str):
    ss = st.session_state
    st.markdown(f"Are you sure you want to delete **{username}**?")
    st.warning("This user will permanently lose access. This cannot be undone.")
    c_ya, c_batal = st.columns(2)
    if c_batal.button("Cancel", key="dlg_del_batal", width="stretch"):
        st.rerun()
    if c_ya.button("Yes, delete", type="primary", key="dlg_del_ya", width="stretch"):
        if not _sb_user_delete(username):
            st.error(f"Delete failed: {ss.pop('_err_del', '-')}")
            return
        _sb_users_list.clear()
        catat_riwayat("Delete user", username)
        ss["_del_sukses"] = f"User '{username}' deleted."
        ss["_reset_del_user"] = True
        st.rerun()

@st.dialog("Reset password", width="small")
def _konfirmasi_reset_pw(username: str, pw_baru: str):
    ss = st.session_state
    st.markdown(f"Are you sure you want to reset the password of **{username}**?")
    st.warning("The current password will stop working immediately. Share the new password with the user.")
    c_ya, c_batal = st.columns(2)
    if c_batal.button("Cancel", key="dlg_pw_batal", width="stretch"):
        st.rerun()
    if c_ya.button("Yes, reset", type="primary", key="dlg_pw_ya", width="stretch"):
        if not _sb_user_reset_pw(username, pw_baru):
            st.error("Reset failed (check Supabase RLS policy for UPDATE).")
            return
        catat_riwayat("Reset password", username)
        ss["_pw_sukses"] = f"Password for '{username}' has been reset."
        st.rerun()

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
    try:
        r = requests.delete(f"{SUPABASE_URL}/rest/v1/{tabel}",
                            headers={**_SB_HEADERS, "Prefer": "return=representation"}, params=params, timeout=30)
        if not r.ok:
            return 0, f"HTTP {r.status_code}: {r.text[:200]}"
        return len(r.json()), ""
    except Exception as e:
        return 0, str(e)

def _sb_riwayat_reset_id() -> str:
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

def _sb_feedback_reset_id() -> str:
    try:
        r = requests.post(f"{SUPABASE_URL}/rest/v1/rpc/reset_feedback_id", headers=_SB_HEADERS, json={}, timeout=15)
        if r.status_code == 404:
            return ("Function reset_feedback_id() not found in the Supabase API. Run the one-time setup SQL "
                    f"below first. Supabase says: {r.text[:200]}")
        if not r.ok:
            return f"HTTP {r.status_code}: {r.text[:200]}"
        return ""
    except Exception as e:
        return str(e)

def _selesai_clear():
    # hitungan tabel di-cache 20 dtk: kosongkan supaya angka langsung benar, lalu render ulang HANYA bagian ini
    # (st.rerun() penuh membuat halaman Settings kembali ke tab/posisi awal)
    _sb_riwayat_hitung.clear(); _sb_tabel_hitung.clear()
    st.rerun(scope="fragment")


def _bagian_clear_riwayat():
    ss = st.session_state
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
        _selesai_clear()

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
        _selesai_clear()

    st.divider()
    st.markdown("**Reset id counter**")
    st.caption("Next new history record gets the next free number: 1 if the table is empty, otherwise "
               "(highest existing id + 1). Existing records are not touched.")
    if st.button(":material/restart_alt: Reset id counter", key="riw_btn_reset_id"):
        err_r = _sb_riwayat_reset_id()
        ss.riw_hasil = ("error", f"Reset failed: {err_r}") if err_r else ("success", "Id counter reset.")
        _selesai_clear()
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


def _bagian_clear_help():
    ss = st.session_state
    # ── Tiket Help & Support (tabel help_support) ──
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
        _selesai_clear()

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


def _bagian_clear_feedback():
    ss = st.session_state
    # ── Feedback bintang (tabel feedback) ──
    st.markdown("#### Feedback")
    jml_fb = _sb_tabel_hitung(TABLE_FEEDBACK, "created_at")
    st.caption(f"Table `{TABLE_FEEDBACK}` currently has **{jml_fb if jml_fb >= 0 else '?'}** feedback(s).")

    st.markdown("**Delete ALL feedback**")
    ok_fb_semua = st.checkbox("I understand ALL feedback will be permanently deleted (cannot be undone)",
                              key="fb_yakin_semua")
    fb_reset = st.checkbox("Also reset the id counter to 1 after deleting", value=True, key="fb_sekalian_reset")
    if st.button(":material/delete_forever: Delete all feedback", type="primary", key="fb_btn_semua",
                 disabled=not ok_fb_semua):
        n, err = _sb_tabel_hapus(TABLE_FEEDBACK, {"created_at": "not.is.null"})
        if err:
            ss.riw_hasil = ("error", f"Delete failed: {err}")
        elif n == 0 and jml_fb > 0:
            ss.riw_hasil = ("error", "Supabase accepted the request but deleted 0 rows: the table has no DELETE "
                                     "policy for this API key (RLS). Add a DELETE policy on feedback.")
        else:
            pesan, ada_err = f"{n} feedback(s) deleted.", False
            if fb_reset:
                err_r = _sb_feedback_reset_id()
                ada_err = bool(err_r)
                pesan += " Id counter reset." if not err_r else f" Id reset failed: {err_r}"
            ss.riw_hasil = ("error" if ada_err else "success", pesan)
        _selesai_clear()

    st.markdown("**Reset id counter**")
    st.caption("Next new feedback gets the next free number: 1 if the table is empty, otherwise "
               "(highest existing id + 1). Existing feedback is not touched.")
    if st.button(":material/restart_alt: Reset feedback id counter", key="fb_btn_reset_id"):
        err_r = _sb_feedback_reset_id()
        ss.riw_hasil = ("error", f"Reset failed: {err_r}") if err_r else ("success", "Feedback id counter reset.")
        _selesai_clear()
    with st.expander(":material/code: One-time setup for feedback (run once in Supabase SQL Editor)"):
        st.code("""create or replace function public.reset_feedback_id()
returns void language plpgsql security definer as $$
begin
  perform setval(
    pg_get_serial_sequence('public.feedback', 'id'),
    coalesce((select max(id) from public.feedback), 0) + 1,
    false
  );
end $$;

grant execute on function public.reset_feedback_id() to anon, authenticated;
notify pgrst, 'reload schema';

-- kalau RLS aktif, tambahkan policy ini supaya app bisa baca & hapus feedback:
-- create policy "feedback_select" on public.feedback for select using (true);
-- create policy "feedback_delete" on public.feedback for delete using (true);""", language="sql")


@st.fragment
def _tab_clear_history():
    ss = st.session_state
    # Streamlit membuang nilai widget yang tidak ditampilkan (mis. saat pindah halaman).
    # Simpan salinan nilainya dan pulihkan saat tab ini tampil lagi.
    _keep = ss.setdefault("_keep_clear", {})
    for _k in ("riw_hari", "riw_yakin_lama", "riw_yakin_semua", "riw_sekalian_reset",
               "hs_yakin_solved", "hs_hari", "hs_yakin_lama", "hs_yakin_semua", "clear_bagian",
               "fb_yakin_semua", "fb_sekalian_reset"):
        if _k in ss:
            _keep[_k] = ss[_k]
        elif _k in _keep:
            ss[_k] = _keep[_k]
    if hasil := ss.pop("riw_hasil", None):
        (st.error if hasil[0] == "error" else st.success)(hasil[1])
    bagian = st.selectbox("Select what to clear",
                          ["History", "Help & Support tickets", "Feedback"], key="clear_bagian")
    {"History": _bagian_clear_riwayat, "Help & Support tickets": _bagian_clear_help,
     "Feedback": _bagian_clear_feedback}[bagian]()


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
        from login_page import do_signup, TAGGING_USER         
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
            if st.button("Delete user", type="primary", disabled=not u_del, key="set_del_btn"):
                _konfirmasi_hapus_user(u_del)      # popup konfirmasi dulu sebelum benar-benar dihapus
            if pesan_del:
                st.success(pesan_del)

    # ── Reset password ──
    with tab_pw:
        pesan_pw = ss.pop("_pw_sukses", None)            # pesan sukses reset, tampil sekali
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
            else:
                _konfirmasi_reset_pw(u_pw, pw1)    # popup konfirmasi dulu sebelum password diganti
        if pesan_pw:
            st.success(pesan_pw)

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
    from login_page import require_login, keluar
    # mulai memuat data berat di latar belakang selagi user masih di halaman login
    _shared.prewarm(ambil_raw_noo, ambil_data_dashboard, _riwayat_cache)
    user = require_login("validation")

    IZIN_MASUK = {"RSA", "BD SUPPORT"}
    if user.get("tagging") not in IZIN_MASUK:
        st.error("Access denied. This app is only for RSA and BD Support.")
        if st.button("Back to login"):
            keluar()          # hapus juga cookie login, kalau tidak sesi dipulihkan lagi dari cookie -> tetap Access denied
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
