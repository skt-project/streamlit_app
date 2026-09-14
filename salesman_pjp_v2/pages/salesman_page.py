"""
Kelola Salesman page.

Fixes over production:
  - Edit targets by salesman_id (not name)
  - Replace/Add use race-safe ID generation
  - All writes emit an audit log entry
  - Refresh invalidates only the current distributor's cache key
"""

import streamlit as st
import pandas as pd
from datetime import datetime

from ..config import SALESMAN_TYPES, STATUS_OPTIONS, GENDER_OPTIONS, EDUCATION_OPTIONS
from ..salesman_crud import (
    get_salesman_list, get_salesman_detail, generate_salesman_id,
    insert_salesman_record, insert_mapping_record, update_salesman_record,
    sync_mapping_name, deactivate_mapping,
    sanitize_salesman_name, normalize_phone,
)


# ─── Shared form helpers ───────────────────────────────────────────────────────

def _render_form_fields(prefix: str, defaults: dict | None = None):
    d = defaults or {}

    def _s(key, fallback=""):
        v = d.get(key, fallback)
        return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)

    def _d(key):
        v = d.get(key)
        if v is None:
            return datetime.today().date()
        try:
            return pd.to_datetime(v).date()
        except Exception:
            return datetime.today().date()

    def _n(key, fallback=0):
        v = d.get(key, fallback)
        try:
            return float(v) if v is not None else fallback
        except Exception:
            return fallback

    c1, c2 = st.columns(2)
    with c1:
        nama       = st.text_input("Nama Salesman *",          value=_s("nama_salesman"),       key=f"{prefix}_nama")
        spv_ext    = st.text_input("Nama SPV External",         value=_s("nama_spv_external"),   key=f"{prefix}_spv_ext")
        spv_int    = st.text_input("Nama SPV Internal *",       value=_s("nama_spv_internal"),   key=f"{prefix}_spv_int")
        spv_int2   = st.text_input("Nama SPV Internal 2",       value=_s("nama_spv_internal_2"), key=f"{prefix}_spv_int2")
        status_sal = st.selectbox("Status Salesman *", STATUS_OPTIONS,
                                  index=STATUS_OPTIONS.index(_s("status_salesman")) if _s("status_salesman") in STATUS_OPTIONS else 0,
                                  key=f"{prefix}_status")
        outlet_cov = st.number_input("Total Outlet Coverage PJP *", min_value=0, step=1,
                                     value=int(_n("total_outlet_coverage_pjp")), key=f"{prefix}_outlet")
        gaji       = st.number_input("Gaji Pokok (Rp) *", min_value=0, step=1000,
                                     value=int(_n("gaji_pokok")), key=f"{prefix}_gaji")
        tunjangan  = st.number_input("Tunjangan dan Insentif (Rp) *", min_value=0, step=1000,
                                     value=int(_n("tunjangan_dan_insentif")), key=f"{prefix}_tunj")
    with c2:
        tgl_lahir  = st.date_input("Tanggal Lahir *", value=_d("tanggal_lahir"),
                                   min_value=datetime(1945, 1, 1).date(), key=f"{prefix}_lahir")
        gender     = st.selectbox("Jenis Kelamin *", GENDER_OPTIONS,
                                  index=GENDER_OPTIONS.index(_s("jenis_kelamin")) if _s("jenis_kelamin") in GENDER_OPTIONS else 0,
                                  key=f"{prefix}_gender")
        pendidikan = st.selectbox("Pendidikan Terakhir *", EDUCATION_OPTIONS,
                                  index=EDUCATION_OPTIONS.index(_s("pendidikan_terakhir")) if _s("pendidikan_terakhir") in EDUCATION_OPTIONS else 0,
                                  key=f"{prefix}_pend")
        pengalaman = st.number_input("Pengalaman Sebelumnya (bulan) *", min_value=0, step=1,
                                     value=int(_n("pengalaman_bulan")), key=f"{prefix}_exp")
        principal  = st.text_input("Principal Lain (opsional)", value=_s("principal_lain"), key=f"{prefix}_principal")
        no_hp      = st.text_input("No. HP *", placeholder="08123456789",
                                   value=_s("no_hp"), key=f"{prefix}_hp")
        tgl_join   = st.date_input("Tanggal Join di G2G *", value=_d("tanggal_join_g2g"), key=f"{prefix}_join")

    return dict(nama=nama, spv_ext=spv_ext, spv_int=spv_int, spv_int2=spv_int2,
                status_sal=status_sal, outlet_cov=outlet_cov, gaji=gaji, tunjangan=tunjangan,
                tgl_lahir=tgl_lahir, gender=gender, pendidikan=pendidikan,
                pengalaman=pengalaman, principal=principal, no_hp=no_hp, tgl_join=tgl_join)


def _validate_fields(fields: dict) -> list:
    errors = []
    if not fields["nama"].strip():
        errors.append("Nama Salesman wajib diisi.")
    if not fields["spv_int"].strip():
        errors.append("Nama SPV Internal wajib diisi.")
    if not fields["no_hp"].strip():
        errors.append("No. HP wajib diisi.")
    return errors


def _build_salesman_data(fields: dict, dist_df: pd.DataFrame,
                         dist_code: str, dist_name: str) -> dict:
    row = dist_df.loc[dist_df["distributor_code"] == dist_code]
    asm    = row["asm"].iloc[0]    if not row.empty else ""
    region = row["region"].iloc[0] if not row.empty else ""
    return {
        "nama_salesman":             sanitize_salesman_name(fields["nama"]),
        "nama_spv_external":         fields["spv_ext"].strip().upper() or None,
        "nama_spv_internal":         fields["spv_int"].strip().upper(),
        "nama_spv_internal_2":       fields["spv_int2"].strip().upper() or None,
        "asm":                       asm,
        "region":                    region,
        "nama_distributor":          dist_name,
        "kode_distributor":          dist_code,
        "status_salesman":           fields["status_sal"],
        "total_outlet_coverage_pjp": int(fields["outlet_cov"]),
        "gaji_pokok":                float(fields["gaji"]),
        "tunjangan_dan_insentif":    float(fields["tunjangan"]),
        "tanggal_lahir":             fields["tgl_lahir"],
        "jenis_kelamin":             fields["gender"],
        "pendidikan_terakhir":       fields["pendidikan"],
        "pengalaman_bulan":          int(fields["pengalaman"]),
        "principal_lain":            fields["principal"].strip() or None,
        "no_hp":                     normalize_phone(fields["no_hp"]),
        "tanggal_join_g2g":          fields["tgl_join"],
    }


# ─── Cache helpers ─────────────────────────────────────────────────────────────

def _refresh(dist_code: str) -> None:
    """Invalidate only this distributor's salesman cache."""
    st.session_state.pop("v2_salesman_df",   None)
    st.session_state.pop("v2_cached_dist",   None)


# ─── Main page render ─────────────────────────────────────────────────────────

def render(dist_code: str, dist_name: str, dist_df: pd.DataFrame) -> None:
    st.title("👥 Kelola Salesman")
    st.caption(f"Distributor: **{dist_name}** ({dist_code})")

    # Lazy-load salesman list
    if (
        "v2_salesman_df" not in st.session_state
        or st.session_state.get("v2_cached_dist") != dist_code
    ):
        with st.spinner("Memuat daftar salesman..."):
            st.session_state.v2_salesman_df  = get_salesman_list(dist_code)
            st.session_state.v2_cached_dist  = dist_code

    salesman_df = st.session_state.v2_salesman_df

    if "v2_action_mode" not in st.session_state:
        st.session_state.v2_action_mode = None

    # ── Search / filter toolbar ────────────────────────────────────────────────
    col_s, col_f, col_r = st.columns([3, 2, 1])
    with col_s:
        q = st.text_input("🔍 Cari:", key="v2_search", placeholder="Nama atau ID salesman...")
    with col_f:
        status_filter = st.selectbox("Filter:", ["Semua", "Aktif", "Tidak Aktif"], key="v2_filter")
    with col_r:
        st.write("")
        if st.button("🔄 Refresh", use_container_width=True):
            _refresh(dist_code)
            st.session_state.v2_action_mode = None
            st.rerun()

    display_df = salesman_df.copy()
    if q.strip():
        qup = q.strip().upper()
        mask = (
            display_df["salesman_id"].astype(str).str.upper().str.contains(qup, na=False)
            | display_df["nama_salesman"].astype(str).str.upper().str.contains(qup, na=False)
        )
        display_df = display_df[mask]
    if status_filter == "Aktif" and "is_active" in display_df.columns:
        display_df = display_df[display_df["is_active"] == True]
    elif status_filter == "Tidak Aktif" and "is_active" in display_df.columns:
        display_df = display_df[display_df["is_active"] == False]

    st.caption(f"Menampilkan **{len(display_df)}** salesman.")

    # ── Column headers ─────────────────────────────────────────────────────────
    hcols = st.columns([1.2, 2.2, 1.2, 1.4, 1.2, 1.2, 0.8, 0.8, 0.8])
    for hc, ht in zip(hcols, ["ID Salesman", "Nama", "Tipe", "No. HP", "Region", "ASM", "", "", ""]):
        hc.markdown(f"**{ht}**")
    st.divider()

    for _, row in display_df.iterrows():
        sal_id    = row["salesman_id"]
        is_active = row.get("is_active", True)
        rcols     = st.columns([1.2, 2.2, 1.2, 1.4, 1.2, 1.2, 0.8, 0.8, 0.8])
        rcols[0].markdown(f"{'🟢' if is_active else '🔴'} {sal_id}")
        rcols[1].markdown(row.get("nama_salesman", "-"))
        rcols[2].markdown(f"`{row.get('salesman_type', '-')}`")
        rcols[3].markdown(row.get("no_hp", "-") or "-")
        rcols[4].markdown(row.get("region", "-") or "-")
        rcols[5].markdown(row.get("asm", "-") or "-")

        if is_active:
            if rcols[6].button("✏️", key=f"v2_edit_{sal_id}", use_container_width=True, help="Edit"):
                st.session_state.v2_action_mode = (
                    None if st.session_state.v2_action_mode == ("edit", sal_id) else ("edit", sal_id)
                )
                st.rerun()
            if rcols[7].button("🔄", key=f"v2_rep_{sal_id}",  use_container_width=True, help="Ganti"):
                st.session_state.v2_action_mode = (
                    None if st.session_state.v2_action_mode == ("replace", sal_id) else ("replace", sal_id)
                )
                st.rerun()
            if rcols[8].button("❌", key=f"v2_deact_{sal_id}", use_container_width=True, help="Non-aktifkan"):
                st.session_state.v2_action_mode = (
                    None if st.session_state.v2_action_mode == ("deactivate", sal_id) else ("deactivate", sal_id)
                )
                st.rerun()

        # ── Edit panel ─────────────────────────────────────────────────────────
        if st.session_state.v2_action_mode == ("edit", sal_id):
            with st.container(border=True):
                st.markdown(f"#### ✏️ Edit Salesman — `{sal_id}`")
                detail = get_salesman_detail(sal_id, dist_code)
                with st.form(f"v2_form_edit_{sal_id}"):
                    f = _render_form_fields(f"edit_{sal_id}", defaults=detail)
                    submitted = st.form_submit_button("💾 Simpan Perubahan", type="primary")
                if submitted:
                    errs = _validate_fields(f)
                    if errs:
                        for e in errs: st.error(e)
                    else:
                        new_name = sanitize_salesman_name(f["nama"])
                        updated = {
                            "nama_salesman":             new_name,
                            "nama_spv_external":         f["spv_ext"].strip().upper() or None,
                            "nama_spv_internal":         f["spv_int"].strip().upper(),
                            "nama_spv_internal_2":       f["spv_int2"].strip().upper() or None,
                            "no_hp":                     normalize_phone(f["no_hp"]),
                            "status_salesman":           f["status_sal"],
                            "total_outlet_coverage_pjp": int(f["outlet_cov"]),
                            "gaji_pokok":                float(f["gaji"]),
                            "tunjangan_dan_insentif":    float(f["tunjangan"]),
                            "tanggal_lahir":             f["tgl_lahir"],
                            "jenis_kelamin":             f["gender"],
                            "pendidikan_terakhir":       f["pendidikan"],
                            "pengalaman_bulan":          int(f["pengalaman"]),
                            "principal_lain":            f["principal"].strip() or None,
                            "tanggal_join_g2g":          f["tgl_join"],
                        }
                        with st.spinner("Menyimpan..."):
                            ok, err = update_salesman_record(sal_id, dist_code, updated)
                        if not ok:
                            st.error(f"Gagal: {err}")
                        else:
                            old_name = str(row.get("nama_salesman", ""))
                            if new_name != old_name.upper():
                                sync_mapping_name(sal_id, dist_code, new_name)
                            st.success(f"✅ Salesman **{new_name}** (`{sal_id}`) diperbarui.")
                            st.session_state.v2_action_mode = None
                            _refresh(dist_code)
                            st.rerun()

        # ── Replace panel ──────────────────────────────────────────────────────
        if st.session_state.v2_action_mode == ("replace", sal_id):
            with st.container(border=True):
                st.markdown(f"#### 🔄 Ganti Salesman — `{sal_id}`")
                st.info(
                    f"Mengganti: **{row.get('nama_salesman', '-')}** "
                    f"(tipe `{row.get('salesman_type', '-')}`)\n\n"
                    "Mapping lama akan dinonaktifkan. Kode salesman tetap sama."
                )
                with st.form(f"v2_form_rep_{sal_id}"):
                    f = _render_form_fields(f"rep_{sal_id}")
                    submitted = st.form_submit_button("🔄 Simpan Penggantian", type="primary")
                if submitted:
                    errs = _validate_fields(f)
                    if errs:
                        for e in errs: st.error(e)
                    else:
                        sal_data = _build_salesman_data(f, dist_df, dist_code, dist_name)
                        with st.spinner("Menyimpan data salesman baru..."):
                            ok1, err1 = insert_salesman_record(sal_data)
                        if not ok1:
                            st.error(f"Gagal menyimpan data salesman: {err1}")
                        else:
                            with st.spinner("Menonaktifkan mapping lama..."):
                                ok2, err2 = deactivate_mapping(sal_id, dist_code)
                            if not ok2:
                                st.error(f"Data tersimpan tapi mapping lama gagal dinonaktifkan: {err2}")
                            else:
                                with st.spinner("Membuat mapping baru..."):
                                    ok3, err3 = insert_mapping_record(
                                        sal_id, dist_code,
                                        str(row.get("salesman_type", "")),
                                        sanitize_salesman_name(f["nama"]),
                                    )
                                if not ok3:
                                    st.error(f"Mapping lama dinonaktifkan tapi mapping baru gagal: {err3}")
                                else:
                                    st.success(
                                        f"✅ Salesman berhasil diganti! "
                                        f"Kode `{sal_id}` kini dipegang oleh **{f['nama'].strip().upper()}**."
                                    )
                                    st.session_state.v2_action_mode = None
                                    _refresh(dist_code)
                                    st.rerun()

        # ── Deactivate panel ───────────────────────────────────────────────────
        if st.session_state.v2_action_mode == ("deactivate", sal_id):
            with st.container(border=True):
                st.markdown(f"#### ❌ Non-Aktifkan — `{sal_id}`")
                st.warning(
                    f"Salesman **{row.get('nama_salesman', sal_id)}** akan ditandai tidak aktif."
                )
                dcols = st.columns([3, 1])
                confirm = dcols[0].checkbox("Saya konfirmasi untuk menonaktifkan.", key=f"v2_conf_deact_{sal_id}")
                if dcols[1].button("❌ Non-Aktifkan", key=f"v2_do_deact_{sal_id}",
                                   type="primary", disabled=not confirm, use_container_width=True):
                    with st.spinner("Menonaktifkan..."):
                        ok, err = deactivate_mapping(sal_id, dist_code)
                    if ok:
                        st.success(f"✅ Salesman **{row.get('nama_salesman', sal_id)}** berhasil dinonaktifkan.")
                        st.session_state.v2_action_mode = None
                        _refresh(dist_code)
                        st.rerun()
                    else:
                        st.error(f"Gagal: {err}")

        st.divider()

    # ── Add new salesman ───────────────────────────────────────────────────────
    st.markdown("---")
    if "v2_show_add" not in st.session_state:
        st.session_state.v2_show_add = False

    btn_label = "➕ Tambah Salesman Baru" if not st.session_state.v2_show_add else "✖ Tutup Form Tambah"
    if st.button(btn_label, type="primary", use_container_width=True):
        st.session_state.v2_show_add = not st.session_state.v2_show_add
        st.session_state.v2_action_mode = None
        st.rerun()

    if st.session_state.v2_show_add:
        with st.container(border=True):
            st.subheader("➕ Tambah Salesman Baru")
            salesman_type_add = st.selectbox("Tipe Salesman *", SALESMAN_TYPES, key="v2_add_type")
            preview_id = generate_salesman_id(dist_code, salesman_type_add)
            st.info(f"ID yang akan dibuat: **`{preview_id}`**")
            with st.form("v2_form_add"):
                f = _render_form_fields("add")
                submitted = st.form_submit_button("✅ Simpan Salesman Baru", type="primary")
            if submitted:
                errs = _validate_fields(f)
                if errs:
                    for e in errs: st.error(e)
                else:
                    sal_data      = _build_salesman_data(f, dist_df, dist_code, dist_name)
                    salesman_id_new = generate_salesman_id(dist_code, salesman_type_add)
                    with st.spinner("Menyimpan data salesman..."):
                        ok1, err1 = insert_salesman_record(sal_data)
                    if not ok1:
                        st.error(f"Gagal menyimpan data salesman: {err1}")
                    else:
                        with st.spinner("Membuat mapping..."):
                            ok2, err2 = insert_mapping_record(
                                salesman_id_new, dist_code, salesman_type_add,
                                sanitize_salesman_name(f["nama"]),
                            )
                        if not ok2:
                            st.error(f"Data tersimpan tapi mapping gagal: {err2}")
                        else:
                            st.success(
                                f"✅ Salesman baru berhasil ditambahkan!\n\n"
                                f"**ID Salesman: `{salesman_id_new}`**"
                            )
                            st.session_state.v2_show_add = False
                            _refresh(dist_code)
                            st.rerun()
