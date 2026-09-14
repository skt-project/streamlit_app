"""
PJP Template page — three tabs:
  Tab 1: Download Template
  Tab 2: Update PJP  (atomic: stage → BEGIN DELETE+INSERT → COMMIT)
  Tab 3: PJP Efektif (live coalesce view: PJP rows + Basis fallback for uncovered stores)
"""

import streamlit as st
import pandas as pd
from datetime import datetime

from ..config import PJP_COLS
from ..excel_engine import create_pjp_excel
from ..validators import read_template_sheet, validate_pjp_df
from ..pjp_crud import get_pjp_list, commit_pjp_upload
from ..pjp_fallback import get_effective_pjp, get_coverage_summary


# ─── Cached Excel generator ────────────────────────────────────────────────────

@st.cache_data(show_spinner="Menyiapkan template Excel...", ttl=3600)
def _cached_excel(_dist_df, _store_df, _distributor_map):
    return create_pjp_excel(
        pd.DataFrame(columns=[c for c, _, _ in PJP_COLS]),
        _distributor_map,
        _dist_df,
        _store_df,
    )


# ─── Main page render ─────────────────────────────────────────────────────────

def render(
    dist_code: str,
    dist_name: str,
    dist_df: pd.DataFrame,
    store_df: pd.DataFrame,
    distributor_map: dict,
) -> None:
    st.title("🗓️ PJP Template")
    st.caption(f"Distributor: **{dist_name}** ({dist_code})")

    tab_dl, tab_up, tab_eff = st.tabs([
        "📥 Download Template",
        "🔄 Update PJP",
        "📋 PJP Efektif",
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — Download Template
    # ══════════════════════════════════════════════════════════════════════════
    with tab_dl:
        st.subheader("📥 Download PJP Template")
        with st.expander("📖 Panduan Pengisian", expanded=False):
            st.markdown("""
            ### ⚡ ATURAN DASAR:
            - **1 file = 1 distributor** (tidak boleh campur)
            - **Semua kolom "Wajib Diisi" harus terisi**
            - **Jangan edit kolom "Kode Distributor" dan "Nama Toko"** (otomatis)

            ### 🔄 URUTAN DROPDOWN BERTINGKAT (WAJIB!):
            1. **ASM** → 2. **Region** → 3. **Nama Distributor** → 4. **Kode Toko**

            ### ✅ FORMAT DATA YANG BENAR:
            - **Frekuensi PJP**: F4+ / F4 / F2 / F1
            - **Hari**: Pilih dari dropdown
            - **Minggu**: Pilih Ganjil / Genap / Ganjil+Genap
            """)

        pjp_excel = _cached_excel(dist_df, store_df, distributor_map)
        st.download_button(
            "⬇️ Download PJP Template",
            data=pjp_excel.getvalue(),
            file_name=f"PJP_Template_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary",
        )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — Update PJP (atomic)
    # ══════════════════════════════════════════════════════════════════════════
    with tab_up:
        st.subheader("🔄 Update Daftar PJP")
        st.info(
            "Data PJP lama untuk scope yang dipilih akan **dihapus** dan diganti secara **atomik** — "
            "jika proses insert gagal, data lama tidak akan hilang."
        )

        st.markdown("### 1️⃣ Pilih Scope Update")
        update_scope = st.radio(
            "Update berdasarkan:",
            ["Distributor (semua salesman)", "Salesman tertentu"],
            key="v2_pjp_scope",
            horizontal=True,
        )

        scope_dist_code = dist_code
        scope_salesman  = None

        if update_scope == "Salesman tertentu":
            sal_df = st.session_state.get("v2_salesman_df", pd.DataFrame())
            if sal_df.empty:
                st.warning("Muat halaman Kelola Salesman terlebih dahulu.")
                st.stop()
            active_sal = sal_df[sal_df.get("is_active", True) == True] if "is_active" in sal_df.columns else sal_df
            sal_options = sorted(active_sal["nama_salesman"].dropna().unique().tolist())
            scope_salesman = st.selectbox("Pilih Salesman:", sal_options, key="v2_pjp_sal_select")
        else:
            st.markdown(
                f"Scope: semua PJP milik distributor **{dist_name}** (`{dist_code}`) akan diganti."
            )

        st.markdown("### 2️⃣ Upload File PJP Baru")
        st.warning("Gunakan template resmi (download dari Tab 1).")

        uploaded = st.file_uploader(
            "Pilih file Excel PJP baru (.xlsx)", type=["xlsx"], key="v2_pjp_uploader"
        )

        if uploaded:
            try:
                xl = pd.ExcelFile(uploaded)
            except Exception as e:
                st.error(f"Gagal membaca file: {e}")
                st.stop()

            if "PJP Template" not in xl.sheet_names:
                st.error("Sheet 'PJP Template' tidak ditemukan. Gunakan template resmi.")
                st.stop()

            try:
                pjp_df = read_template_sheet(uploaded, distributor_map, store_df)
                pjp_df = pjp_df[
                    pjp_df["Nama Distributor"].notna() & (pjp_df["Nama Distributor"] != "")
                ].reset_index(drop=True)
            except Exception as e:
                st.error(f"Gagal membaca sheet: {e}")
                st.stop()

            if pjp_df.empty:
                st.warning("Tidak ada data di file yang diupload.")
                st.stop()

            errors, warnings = validate_pjp_df(pjp_df, distributor_map, store_df)

            if errors:
                st.error(f"**❌ {len(errors)} ERROR:**")
                for err in errors[:20]:
                    st.markdown(f"- {err}")
                if len(errors) > 20:
                    st.caption(f"...dan {len(errors)-20} error lainnya.")
            if warnings:
                st.warning(f"**⚠️ {len(warnings)} PERINGATAN:**")
                for w in warnings[:10]:
                    st.markdown(f"- {w}")

            if errors:
                st.error("Perbaiki semua error di atas sebelum melanjutkan.")
            else:
                st.success(f"✅ Validasi berhasil — {len(pjp_df)} baris siap diupload.")

                scope_label = (
                    f"semua PJP distributor **{dist_name}**"
                    if scope_salesman is None
                    else f"PJP salesman **{scope_salesman}**"
                )
                st.markdown("### 3️⃣ Konfirmasi & Eksekusi")
                st.warning(
                    f"⚠️ Proses ini akan **menghapus** {scope_label} dari database, "
                    f"lalu memasukkan **{len(pjp_df)} baris baru** secara atomik. "
                    "Tindakan ini **tidak dapat dibatalkan**."
                )

                confirm = st.checkbox(
                    "Saya memahami bahwa data PJP lama akan dihapus dan diganti.",
                    key="v2_confirm_update",
                )
                if st.button("🔄 Update PJP", key="v2_exec_update",
                             type="primary", disabled=not confirm):
                    with st.spinner("Memproses (delete lama → insert baru secara atomik)..."):
                        ok, msg = commit_pjp_upload(pjp_df, dist_code, scope_salesman)
                    if ok:
                        st.success(f"✅ {msg}")
                        # Invalidate effective PJP cache for this distributor
                        get_effective_pjp.clear() if hasattr(get_effective_pjp, "clear") else None
                    else:
                        st.error(
                            f"❌ Gagal: {msg}\n\n"
                            "Data lama **tidak dihapus** karena operasi dibatalkan otomatis."
                        )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — PJP Efektif (Basis fallback view)
    # ══════════════════════════════════════════════════════════════════════════
    with tab_eff:
        st.subheader("📋 PJP Efektif")
        st.markdown(
            "Menampilkan **PJP aktual** untuk toko yang sudah terdaftar di PJP, "
            "dan **data Basis** (fallback) untuk toko yang belum ada di PJP. "
            "Toko dengan sumber **Basis** perlu di-assign ke PJP agar jadwal kunjungan terdefinisi."
        )

        if st.button("🔄 Muat / Refresh PJP Efektif", key="v2_load_eff", type="secondary"):
            with st.spinner("Memuat data efektif..."):
                eff_df = get_effective_pjp(dist_code, dist_name)
            st.session_state["v2_eff_df"] = eff_df
            st.session_state["v2_eff_dist"] = dist_code

        eff_df = st.session_state.get("v2_eff_df")
        if eff_df is None or st.session_state.get("v2_eff_dist") != dist_code:
            st.info("Klik tombol di atas untuk memuat data PJP efektif.")
            st.stop()

        if eff_df.empty:
            st.warning("Tidak ada data untuk distributor ini (PJP kosong dan tidak ada data basis).")
            st.stop()

        # Coverage summary
        summary = get_coverage_summary(eff_df)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Toko (Basis)", f"{summary['total_stores']:,}")
        c2.metric("Toko dengan PJP",   f"{summary['pjp_stores']:,}")
        c3.metric("Toko Basis Saja",   f"{summary['basis_stores']:,}")
        c4.metric("% Tidak Ter-PJP",   f"{summary['basis_pct']}%")

        if summary["basis_stores"] > 0:
            st.markdown(
                f"**Toko tanpa PJP per brand:** "
                f"SKT: {summary['skt_missing']} | "
                f"G2G: {summary['g2g_missing']} | "
                f"TPH: {summary['tph_missing']}"
            )

        # Filter
        source_filter = st.selectbox(
            "Filter sumber:", ["Semua", "PJP saja", "Basis saja"],
            key="v2_eff_filter"
        )
        if source_filter == "PJP saja":
            view_df = eff_df[eff_df["assignment_source"] == "PJP"]
        elif source_filter == "Basis saja":
            view_df = eff_df[eff_df["assignment_source"] == "Basis"]
        else:
            view_df = eff_df

        search = st.text_input("🔍 Cari toko / salesman:", key="v2_eff_search")
        if search.strip():
            su = search.strip().upper()
            mask = (
                view_df["kode_toko"].astype(str).str.upper().str.contains(su, na=False)
                | view_df["store_name"].astype(str).str.upper().str.contains(su, na=False)
                | view_df["nama_salesman"].astype(str).str.upper().str.contains(su, na=False)
            )
            view_df = view_df[mask]

        st.caption(f"Menampilkan **{len(view_df)}** baris.")

        # Colour-code source
        def _row_style(row):
            if row["assignment_source"] == "Basis":
                return ["background-color: #FFF9E6"] * len(row)
            return [""] * len(row)

        display_cols = ["kode_toko", "store_name", "brand", "nama_salesman",
                        "hari", "frekuensi", "assignment_source"]
        display_cols = [c for c in display_cols if c in view_df.columns]
        st.dataframe(
            view_df[display_cols].style.apply(_row_style, axis=1),
            use_container_width=True,
            height=480,
        )
        st.caption("🟡 Baris kuning = sumber Basis (belum ada di PJP).")
