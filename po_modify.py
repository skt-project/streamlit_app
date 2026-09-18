import streamlit as st
import pandas as pd
import openpyxl
import io
import re
import zipfile
from datetime import datetime

st.set_page_config(page_title="Modify Qty per SKU", layout="wide", page_icon="✏️")

# ---------- Global font size override ----------
st.markdown("""
    <style>
    html, body, [class*="css"]  {
        font-size: 13px !important;
    }
    h1 { font-size: 3rem !important; }
    h2 { font-size: 1.3rem !important; }
    h3, h4 { font-size: 2 rem !important; }
    .stButton button { font-size: 15px !important; }
    .stTextArea textarea, .stTextInput input, .stNumberInput input { font-size: 13px !important; }
    .stSelectbox div, .stMultiSelect div { font-size: 13px !important; }
    .stDataFrame, .stDataFrame * { font-size: 10px !important; }
    .stCaption, .st-emotion-cache-* p { font-size: 12px !important; }
    </style>
""", unsafe_allow_html=True)


st.title("Modify Quantity per Product Code")
st.caption("Upload file PO, pilih SKU yang mau diubah/dihapus qty-nya.")


# ---------- Helper functions ----------

def _convert_to_xlsx(fname: str, fbytes: bytes):
    ext = fname.rsplit(".", 1)[-1].lower()
    base = fname.rsplit(".", 1)[0]
    if ext == "xlsx":
        return fname, fbytes
    if ext == "csv":
        df = pd.read_csv(io.BytesIO(fbytes), dtype=str, encoding_errors="replace")
        buf = io.BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        return base + ".xlsx", buf.getvalue()
    if ext == "xls":
        df = pd.read_excel(io.BytesIO(fbytes), sheet_name=0, dtype=str)
        buf = io.BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        return base + ".xlsx", buf.getvalue()
    return fname, fbytes


@st.cache_data(show_spinner=False)
def _get_sheet_names(file_bytes: bytes) -> list:
    try:
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True, read_only=True)
        sheets = [ws.title for ws in wb.worksheets if ws.sheet_state == "visible"]
        wb.close()
        return sheets
    except Exception:
        return []


@st.cache_data(show_spinner=False)
def detect_header_row(file_bytes: bytes, sheet_name, max_scan: int = 15) -> int:
    df_raw = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, header=None,
                            engine="openpyxl", dtype=str, nrows=max_scan)
    best_row, best_score = 0, -1
    for i in range(len(df_raw)):
        vals = [str(v).strip() for v in df_raw.iloc[i].values if pd.notna(v) and str(v).strip()]
        text_count = sum(1 for v in vals if not v.replace(".", "", 1).replace(",", "", 1).lstrip("-").isdigit())
        score = text_count * 10 + len(vals)
        if score > best_score:
            best_score, best_row = score, i
    return best_row


@st.cache_data(show_spinner=False)
def _read_df(file_bytes: bytes, sheet_name, header_row: int) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, header=header_row,
                        engine="openpyxl", dtype=str)
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")].dropna(how="all")
    return df.reset_index(drop=True)


@st.cache_data(show_spinner=False)
def _get_qty_value_map(file_bytes: bytes, sheet_name: str, header_row: int, sku_col: str, qty_col: str) -> dict:
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True, read_only=True)
    ws = wb[sheet_name] if sheet_name in wb.sheetnames else wb.active
    hdr_row = header_row + 1
    headers = {}
    for c in range(1, ws.max_column + 1):
        v = ws.cell(row=hdr_row, column=c).value
        if v is not None:
            headers[str(v).strip()] = c
    sku_ci = headers.get(sku_col)
    qty_ci = headers.get(qty_col)
    result = {}
    if sku_ci and qty_ci:
        for r in range(hdr_row + 1, ws.max_row + 1):
            sv = ws.cell(row=r, column=sku_ci).value
            if sv is None:
                continue
            sv = str(sv).strip()
            result[sv] = ws.cell(row=r, column=qty_ci).value
    wb.close()
    return result


def _detect_col(df, keywords):
    return next((c for c in df.columns if any(k in c.lower() for k in keywords)), None)


_SKU_KEYWORDS       = ["sku", "product code", "kode", "code"]
_QTY_KEYWORDS       = ["qty", "quantity"]
_DESC_KEYWORDS      = ["description", "deskripsi", "nama barang", "nama produk"]
_DPP_KEYWORDS       = ["dpp"]
_TOTAL_PRICE_KEYWORDS = ["total price", "total harga", "jumlah harga"]
_DISTRIBUTOR_KEYWORDS = ["distributor"]


def _detect_extra_cols(df) -> dict:
    return {
        "desc_col":  _detect_col(df, _DESC_KEYWORDS),
        "dpp_col":   _detect_col(df, _DPP_KEYWORDS),
        "total_col": _detect_col(df, _TOTAL_PRICE_KEYWORDS),
    }


@st.cache_data(show_spinner=False)
def _pick_template_sheet(file_bytes: bytes, sheets: list):
    for sh in sheets:
        try:
            hrow   = detect_header_row(file_bytes, sh)
            df_try = _read_df(file_bytes, sh, hrow)
            dist_c  = _detect_col(df_try, _DISTRIBUTOR_KEYWORDS)
            sku_c   = _detect_col(df_try, _SKU_KEYWORDS)
            desc_c  = _detect_col(df_try, _DESC_KEYWORDS)
            qty_c   = _detect_col(df_try, _QTY_KEYWORDS)
            dpp_c   = _detect_col(df_try, _DPP_KEYWORDS)
            total_c = _detect_col(df_try, _TOTAL_PRICE_KEYWORDS)
            if not all([dist_c, sku_c, desc_c, qty_c, dpp_c, total_c]):
                continue
            if len(df_try) == 0 or not df_try[sku_c].notna().any():
                continue
            return sh
        except Exception:
            continue
    return None


def _parse_sku_lines(raw_text: str) -> list:
    skus = []
    for line in raw_text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("--") or line.startswith("#"):
            continue
        skus.append(line)
    return skus


def _parse_distributor_blocks(raw_text: str) -> dict:
    blocks = {}
    current_key = None
    for raw_line in raw_text.strip().splitlines():
        line = raw_line.strip()
        if not line:
            continue
        header_match = re.match(r"^===\s*(.+?)\s*===$", line)
        if header_match:
            current_key = header_match.group(1).strip()
            blocks.setdefault(current_key, [])
            continue
        if line.startswith("--") or line.startswith("#"):
            continue
        if current_key is None:
            continue
        blocks[current_key].append(line)
    return blocks


def _apply_qty_changes(file_bytes: bytes, sheet_name: str, header_row: int,
                        sku_col: str, qty_col: str, value_map: dict, mode: str):
    wb_vals = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True, read_only=True)
    ws_vals = wb_vals[sheet_name] if sheet_name in wb_vals.sheetnames else wb_vals.active
    hdr_row = header_row + 1
    headers_vals = {ws_vals.cell(row=hdr_row, column=c).value: c for c in range(1, ws_vals.max_column + 1)}
    sku_ci_vals = headers_vals.get(sku_col)
    row_map = {}
    if sku_ci_vals:
        for r in range(hdr_row + 1, ws_vals.max_row + 1):
            sv = str(ws_vals.cell(row=r, column=sku_ci_vals).value or "").strip()
            if sv in value_map:
                row_map[sv] = r
    wb_vals.close()

    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=False)
    ws = wb[sheet_name] if sheet_name in wb.sheetnames else wb.active
    headers = {ws.cell(row=hdr_row, column=c).value: c for c in range(1, ws.max_column + 1)}
    qty_ci = headers.get(qty_col)
    changed = 0
    if qty_ci:
        for sv, r in row_map.items():
            ws.cell(row=r, column=qty_ci).value = None if mode == "delete" else value_map[sv]
            changed += 1
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue(), changed


def _make_zip(results: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname, data in results.items():
            zf.writestr(fname, data)
    return buf.getvalue()


# ---------- UI ----------

st.markdown("#### 1. Pilih File untuk Modifikasi")
uploaded_files = st.file_uploader(
    "Upload file (.xlsx / .xls / .csv)", type=["xlsx", "xls", "csv"],
    accept_multiple_files=True,
)

if not uploaded_files:
    st.info("Upload file dulu ya 😊")
    st.stop()

st.markdown("#### 2. Cek Sheet & Kolom")
st.caption(
    "File yang tidak punya sheet dengan header lengkap sesuai template "
    "(DISTRIBUTOR, PRODUCT CODE, DESCRIPTION, QTY, DPP, TOTAL PRICE) "
    "ditampilkan di sini untuk pemilihan manual."
)

file_meta    = []
excluded_meta = []

# ---------- Process uploaded files in 3 columns ----------
cols_per_row = 3

for row_start in range(0, len(uploaded_files), cols_per_row):

    row_files = uploaded_files[row_start:row_start + cols_per_row]
    cols = st.columns(cols_per_row)

    for col, uf in zip(cols, row_files):

        fi = uploaded_files.index(uf)

        with col:

            fname, fbytes = _convert_to_xlsx(uf.name, uf.read())

            sheets = _get_sheet_names(fbytes)

            if not sheets:
                excluded_meta.append({
                    "fname": fname,
                    "reason": "tidak ada sheet yang bisa dibaca"
                })
                continue

            auto_sheet = _pick_template_sheet(fbytes, sheets)

      
            if auto_sheet is not None:

                auto_hrow = detect_header_row(
                    fbytes, auto_sheet
                )

                auto_df = _read_df(
                    fbytes, auto_sheet, auto_hrow
                )

                auto_sku = _detect_col(
                    auto_df, _SKU_KEYWORDS
                )

                auto_qty = _detect_col(
                    auto_df, _QTY_KEYWORDS
                )

                file_meta.append({
                    "fi": fi,
                    "fname": fname,
                    "fbytes": fbytes,
                    "sheet": auto_sheet,
                    "header_row": auto_hrow,
                    "df": auto_df,
                    "sku_col": auto_sku,
                    "qty_col": auto_qty,
                    **_detect_extra_cols(auto_df),
                })

                with st.container(border=True):
                    st.markdown(f"**#{fi+1} {fname}** — sheet template terdeteksi otomatis")

                    col_sheet, col_hrow, col_badge = st.columns([2, 1, 2.2])

                    with col_sheet:
                        st.selectbox(
                            "Sheet:",
                            options=[auto_sheet],
                            index=0,
                            key=f"check_sheet_{fi}",
                            disabled=True,
                        )

                    with col_hrow:
                        st.number_input(
                            "Header row",
                            min_value=1,
                            value=int(auto_hrow) + 1,
                            step=1,
                            key=f"check_hrow_{fi}",
                            disabled=True,
                        )


                    with col_badge:
                        st.success(f"✅ OK")

                    with st.expander("👁 Preview data", expanded=False):
                        st.dataframe(auto_df, use_container_width=True, hide_index=True)

                continue
            
            with st.container(border=True):

                st.markdown(
                    f"**#{fi+1} {fname}** — "
                    "sheet template tidak terdeteksi otomatis"
                )

                col_sheet, col_hrow,col_badge = st.columns([2, 1, 2.2])

                with col_sheet:
                    sheet_sel = st.selectbox(
                        "Sheet:",
                        options=sheets,
                        index=0,
                        key=f"check_sheet_{fi}"
                    )

                hrow_default = detect_header_row(
                    fbytes, sheet_sel
                )

                with col_hrow:
                    hrow_input = st.number_input(
                        "Header row",
                        min_value=1,
                        value=int(hrow_default) + 1,
                        step=1,
                        key=f"check_hrow_{fi}"
                    )

                header_row = int(hrow_input) - 1

                try:

                    df = _read_df(
                        fbytes,
                        sheet_sel,
                        header_row
                    )

                    sku_col = _detect_col(
                        df, _SKU_KEYWORDS
                    )

                    qty_col = _detect_col(
                        df, _QTY_KEYWORDS
                    )


                    missing = []
                    if not sku_col:
                        missing.append("SKU")
                    if not qty_col:
                        missing.append("QTY")

                    with col_badge:
                        if missing:
                            st.warning(
                                f"⚠️"
                                f"{' & '.join(missing)} "
                                "MISSING"
                            )
                        else:
                            st.success(
                                f"✅OK"
                            )

                    if missing:
                        excluded_meta.append({
                            "fname": fname,
                            "reason": (
                                f"kolom {' & '.join(missing)} "
                                f"tidak terdeteksi"
                            )
                        })
                        continue

                    with st.expander(
                        "👁 Preview data",
                        expanded=False
                    ):
                        st.dataframe(
                            df,
                            use_container_width=True,
                            hide_index=True
                        )

                    file_meta.append({
                        "fi": fi,
                        "fname": fname,
                        "fbytes": fbytes,
                        "sheet": sheet_sel,
                        "header_row": header_row,
                        "df": df,
                        "sku_col": sku_col,
                        "qty_col": qty_col,
                        **_detect_extra_cols(df),
                    })

                except Exception as e:
                    with col_badge:
                        st.error(f"❌ Gagal: {e}")

                    excluded_meta.append({
                        "fname": fname,
                        "reason": f"gagal membaca file: {e}"
                    })

st.markdown("#### 3. Modifikasi")
tab1, tab2, tab3 = st.tabs([
    "🔴 Hapus per File",
    "❌ Hapus SKU di Semua File Sekaligus",
    "🗂️ Modifikasi Semua Sheet dalam 1 File",
])

with tab1:
    if not file_meta:
        st.info("Belum ada file dengan kolom SKU/QTY yang berhasil terdeteksi.")
    for fm in file_meta:
        fi         = fm["fi"]
        fname      = fm["fname"]
        fbytes     = fm["fbytes"]
        sheet_sel  = fm["sheet"]
        header_row = fm["header_row"]
        sku_col    = fm["sku_col"]
        qty_col    = fm["qty_col"]
        desc_col   = fm.get("desc_col")

        with st.container(border=True):
            st.markdown(f"**{fname}**")
            st.caption(f"SKU: **{sku_col}** · Quantity: **{qty_col}**")

            raw_codes = st.text_area(
                "Daftar Product Code",
                height=150, key=f"codes_{fi}",
                placeholder="SKU1\nSKU2\nSKU3",
            )

            b1, b2 = st.columns(2)
            with b1:
                do_edit   = st.button("Modifikasi QTY",  use_container_width=True, key=f"btn_edit_{fi}")
            with b2:
                do_delete = st.button("Auto Hapus SKU",  use_container_width=True, key=f"btn_del_{fi}")

            skus = _parse_sku_lines(raw_codes)

            if do_delete:
                if not skus:
                    st.warning("⚠️ Tidak ada SKU yang valid di daftar.")
                else:
                    out_bytes, cnt = _apply_qty_changes(
                        fbytes, sheet_sel, header_row, sku_col, qty_col,
                        {s: None for s in skus}, mode="delete"
                    )
                    st.session_state[f"result_{fi}"] = {
                        "fname": fname, "bytes": out_bytes, "cnt": cnt, "mode": "Auto Hapus"
                    }

            if do_edit:
                if not skus:
                    st.warning("⚠️ Tidak ada SKU yang valid di daftar.")
                else:
                    st.session_state[f"edit_skus_{fi}"] = skus

            edit_skus = st.session_state.get(f"edit_skus_{fi}")
            if edit_skus:
                st.markdown("**Atur quantity baru per Product Code:**")
                sku_qty_map = _get_qty_value_map(fbytes, sheet_sel, header_row, sku_col, qty_col)
                desc_map = {}
                if desc_col:
                    desc_map = dict(zip(fm["df"][sku_col].astype(str).str.strip(), fm["df"][desc_col]))
                new_values = {}
                for s in edit_skus:
                    cur_q = sku_qty_map.get(s, None)
                    try:
                        cur_q_int = int(float(cur_q)) if cur_q not in (None, "") else 0
                    except Exception:
                        cur_q_int = 0
                    rc1, rc2, rc3 = st.columns([3, 2, 3])
                    with rc1:
                        st.markdown(f"**{s}**")
                        if desc_col:
                            desc_val = desc_map.get(s)
                            st.caption(desc_val if desc_val not in (None, "", "nan") else "-")
                    with rc2:
                        st.caption("QTY saat ini")
                        st.markdown(f"**{cur_q if cur_q is not None else '-'}**")
                    with rc3:
                        new_values[s] = st.number_input(
                            "Quantity baru", min_value=0, step=1, value=cur_q_int,
                            key=f"newqty_{fi}_{s}", label_visibility="collapsed"
                        )

                if st.button("Simpan Perubahan QTY", use_container_width=True, key=f"apply_edit_{fi}"):
                    out_bytes, cnt = _apply_qty_changes(
                        fbytes, sheet_sel, header_row, sku_col, qty_col, new_values, mode="edit"
                    )
                    st.session_state[f"result_{fi}"] = {
                        "fname": fname, "bytes": out_bytes, "cnt": cnt, "mode": "Modifikasi QTY"
                    }
                    st.session_state.pop(f"edit_skus_{fi}", None)

            result = st.session_state.get(f"result_{fi}")
            if result:
                st.success(f"✅ {result['mode']} — {result['cnt']} baris berhasil diubah.")
                st.download_button(
                    label=f"⬇️ Download {fname} ({result['cnt']} baris diubah)",
                    data=result["bytes"],
                    file_name=f"Modified_{fname.rsplit('.',1)[0]}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    key=f"dl_single_{fi}",
                )


with tab2:
    st.markdown("#### Hapus Semua SKU Sekaligus per Distributor/File")
    st.caption(
        "Paste daftar SKU dikelompokkan per distributor pakai header `=== NAMA DISTRIBUTOR ===`. "
        "Sistem hanya akan hapus SKU dari blok distributor yang cocok untuk file tersebut."
    )

    mass_codes = st.text_area(
        "Daftar Product Code per distributor",
        height=150, key="mass_codes",
        placeholder=(
            "=== PASTE NAMA DISTRI FROM PO SIMULATOR ===\n"
            "G2G-SKU\n"
        ),
    )

    dist_blocks_preview = _parse_distributor_blocks(mass_codes) if mass_codes.strip() else {}
    if dist_blocks_preview:
        st.caption(
            "Distributor terdeteksi: "
            + ", ".join(f"**{k}** ({len(v)} SKU)" for k, v in dist_blocks_preview.items())
        )

    st.markdown("#### Tentukan Distributor per File")
    st.caption(f"{len(file_meta)} dari {len(uploaded_files)} file berhasil dibaca.")

    if not file_meta:
        st.info("Belum ada file yang berhasil dibaca kolom SKU/QTY-nya.")
    else:
        distributor_options = list(dist_blocks_preview.keys())
        if not distributor_options:
            st.info("ℹ️ Paste daftar SKU per distributor di atas dulu (format `=== NAMA ===`).")

        # Render file dalam 3 kolom per baris
        cols_per_row = 3

        for row_start in range(0, len(file_meta), cols_per_row):
            row_items = file_meta[row_start:row_start + cols_per_row]
            cols = st.columns(cols_per_row)

            for col, fm in zip(cols, row_items):
                options = ["-- pilih distributor --"] + distributor_options
                state_key = f"mass_dist_{fm['fi']}"

                if st.session_state.get(state_key) not in options:
                    st.session_state[state_key] = options[0]

                with col:
                    with st.container(border=True):
                        st.markdown(f"**{fm['fname']}**")

                        st.selectbox(
                            "Distributor:",
                            options=options,
                            key=state_key,
                        )

    if st.button("Modify Semua File", use_container_width=True):
        dist_blocks = _parse_distributor_blocks(mass_codes)
        if not dist_blocks:
            st.warning("⚠️ Tidak ada blok distributor yang valid. Format: `=== NAMA DISTRIBUTOR ===`.")
        elif not file_meta:
            st.warning("⚠️ Belum ada file yang berhasil dibaca kolom SKU/QTY-nya.")
        else:
            summary = []
            skipped = []
            for fm in file_meta:
                dist_name = st.session_state.get(f"mass_dist_{fm['fi']}")
                if not dist_name or dist_name == "-- pilih distributor --":
                    skipped.append(f"{fm['fname']} — belum pilih distributor")
                    continue
                skus_for_file = dist_blocks.get(dist_name)
                if not skus_for_file:
                    skipped.append(f"{fm['fname']} — tidak ada blok SKU untuk '{dist_name}'")
                    continue
                out_bytes, cnt = _apply_qty_changes(
                    fm["fbytes"], fm["sheet"], fm["header_row"],
                    fm["sku_col"], fm["qty_col"],
                    {s: None for s in skus_for_file}, mode="delete"
                )
                st.session_state[f"result_{fm['fi']}"] = {
                    "fname": fm["fname"], "bytes": out_bytes, "cnt": cnt, "mode": "Auto Hapus"
                }
                summary.append((fm["fname"], dist_name, cnt))

            if summary:
                st.success("✅ Selesai:")
                for fname, dist_name, cnt in summary:
                    st.markdown(f"- **{fname}** ({dist_name}) — {cnt} baris dihapus")
            #if skipped:
            #    st.warning("⚠️ File berikut dilewati:")
            #    for s in skipped:
            #        st.markdown(f"- {s}")

    #Download semua hasil
    st.divider()
    st.markdown("#### Download Hasil")

    results = {}
    for fm in file_meta:
        r = st.session_state.get(f"result_{fm['fi']}")
        if r:
            results[r.get("fname", fm["fname"])] = r["bytes"]

    if not results:
        st.info("Belum ada file yang dimodifikasi.")

    elif len(results) == 1:
        # Kalau hanya 1 file → download Excel langsung
        fname, file_bytes = next(iter(results.items()))

        st.caption("1 file siap didownload.")

        st.download_button(
            label=f"⬇️ Download {fname}",
            data=file_bytes,
            file_name=f"Modified_{fname.rsplit('.', 1)[0]}_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_single_mass",
        )

    else:

        st.caption(f"{len(results)} file siap didownload.")

        zip_bytes = _make_zip(results)

        st.download_button(
            label=f"⬇️ Download Semua File ({len(results)} file, .zip)",
            data=zip_bytes,
            file_name=f"Modified_Files_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
            mime="application/zip",
            use_container_width=True,
            key="dl_all_zip",
        )
with tab3:
    st.markdown("#### Modifikasi QTY di Semua Sheet dalam Satu File")
    st.caption(
        "Upload 1 file Excel, sistem akan detect semua sheet yang punya kolom SKU & QTY, "
        "lalu terapkan perubahan ke semua sheet sekaligus."
    )

    # ── 1. File picker (reuse dari uploaded_files, atau upload ulang khusus tab ini) ──
    if not uploaded_files:
        st.info("Upload file di bagian atas dulu ya 😊")
    else:
        # Pilih 1 file dari yang sudah diupload
        file_options = {uf.name: idx for idx, uf in enumerate(uploaded_files)}
        selected_fname = st.selectbox(
            "Pilih file yang mau diproses:",
            options=list(file_options.keys()),
            key="tab3_file_select",
        )
        sel_idx = file_options[selected_fname]
        sel_uf  = uploaded_files[sel_idx]
        tab3_meta_match = next(
            (fm for fm in file_meta if fm["fname"] == _convert_to_xlsx(sel_uf.name, b"")[0]),
            None,
        )

        if tab3_meta_match is None:
            st.warning(
                "⚠️ File ini belum berhasil dibaca di section 2 (mungkin kolom SKU/QTY tidak terdeteksi). "
                "Pastikan file muncul sebagai ✅ OK di bagian Cek Sheet & Kolom."
            )
            st.stop()

        tab3_fname = tab3_meta_match["fname"]
        tab3_bytes = tab3_meta_match["fbytes"]   # ← bytes yang sudah valid & ter-cache

        # ── 2. Detect semua sheet ──────────────────────────────────────────
        all_sheets = _get_sheet_names(tab3_bytes)
        if not all_sheets:
            st.error("❌ Tidak ada sheet yang bisa dibaca dari file ini.")
            st.stop()

        st.markdown(f"**Sheet terdeteksi: {len(all_sheets)} sheet**")

        # Scan tiap sheet, cek apakah punya SKU + QTY
        sheet_scan_results = []
        for sh in all_sheets:
            try:
                hrow   = detect_header_row(tab3_bytes, sh)
                df_sh  = _read_df(tab3_bytes, sh, hrow)
                sku_c  = _detect_col(df_sh, _SKU_KEYWORDS)
                qty_c  = _detect_col(df_sh, _QTY_KEYWORDS)
                desc_c = _detect_col(df_sh, _DESC_KEYWORDS)
                sheet_scan_results.append({
                    "sheet":      sh,
                    "header_row": hrow,
                    "df":         df_sh,
                    "sku_col":    sku_c,
                    "qty_col":    qty_c,
                    "desc_col":   desc_c,
                    "valid":      bool(sku_c and qty_c),
                    "row_count":  len(df_sh),
                })
            except Exception as e:
                sheet_scan_results.append({
                    "sheet": sh, "valid": False,
                    "sku_col": None, "qty_col": None,
                    "error": str(e),
                })

        valid_sheets   = [s for s in sheet_scan_results if s["valid"]]
        invalid_sheets = [s for s in sheet_scan_results if not s["valid"]]

        # Tabel ringkasan sheet
        summary_rows = []
        for s in sheet_scan_results:
            summary_rows.append({
                "Sheet":     s["sheet"],
                "SKU Col":   s.get("sku_col") or "❌ Tidak ditemukan",
                "QTY Col":   s.get("qty_col") or "❌ Tidak ditemukan",
                "Rows":      s.get("row_count", "-"),
                "Status":    "✅ Valid" if s["valid"] else "⚠️ Dilewati",
            })
        st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)

        if not valid_sheets:
            st.warning("⚠️ Tidak ada sheet dengan kolom SKU & QTY yang terdeteksi.")
            st.stop()

        st.caption(
            f"{len(valid_sheets)} sheet akan dimodifikasi · "
            f"{len(invalid_sheets)} sheet dilewati (tidak ada kolom SKU/QTY)"
        )

        # ── 3. Checkbox pilih sheet mana yang diproses ────────────────────
        with st.expander("⚙️ Pilih Sheet yang Akan Dimodifikasi", expanded=False):
            sheet_enabled = {}
            for s in valid_sheets:
                sheet_enabled[s["sheet"]] = st.checkbox(
                    f"{s['sheet']}  (SKU: {s['sku_col']} · QTY: {s['qty_col']} · {s.get('row_count',0)} rows)",
                    value=True,
                    key=f"tab3_enable_{s['sheet']}",
                )
            selected_valid_sheets = [s for s in valid_sheets if sheet_enabled.get(s["sheet"])]
        
        # ── 4. Input SKU per sheet pakai format === NAMA SHEET === ─────────
        st.markdown("---")
        st.markdown("**Daftar Product Code per Sheet:**")
        st.caption(
            "Kelompokkan SKU pakai header `=== NAMA SHEET ===` — "
            "nama harus cocok dengan nama sheet di file. "
            "SKU di bawah tiap header akan dihapus QTY-nya di sheet yang sesuai."
        )

        tab3_raw_blocks = st.text_area(
            "SKU per sheet",
            height=200,
            key="tab3_sku_input",
            placeholder=(
                "=== PT USAHA INDAH JAYA - BANJARBARU ===\n"
                "-- STOP PO (2 SKU)\n"
                "NJM101001\n"
                "NJM104001\n\n"
                "=== NAMA SHEET LAIN ===\n"
                "SKU001\n"
                "SKU002\n"
            ),
        )

        # Parse blok — reuse fungsi yang sama dengan Tab 2
        tab3_sheet_blocks = _parse_distributor_blocks(tab3_raw_blocks) if tab3_raw_blocks.strip() else {}

        if tab3_sheet_blocks:
            st.caption(
                "Distributor/blok terdeteksi dari input: "
                + ", ".join(
                    f"**{k}** ({len(v)} SKU)" for k, v in tab3_sheet_blocks.items()
                )
            )

        # ── 5. Mapping: blok distributor → sheet di file ───────────────────
        st.markdown("---")
        st.markdown("**Mapping Distributor → Sheet di File:**")
        st.caption(
            "Tentukan blok distributor mana yang akan dihapus SKU-nya di sheet mana. "
            "Satu sheet bisa menerima SKU dari beberapa blok distributor sekaligus."
        )

        valid_sheet_names_list = ["-- lewati --"] + [s["sheet"] for s in valid_sheets]
        tab3_mapping = {}  # {dist_key: sheet_name or None}

        if not tab3_sheet_blocks:
            st.info("ℹ️ Paste daftar SKU per distributor di atas dulu (format `=== NAMA ===`).")
        else:
            map_cols_per_row = 2
            block_items = list(tab3_sheet_blocks.items())

            for row_start in range(0, len(block_items), map_cols_per_row):
                row_items = block_items[row_start:row_start + map_cols_per_row]
                map_cols = st.columns(map_cols_per_row)

                for col, (dist_key, skus) in zip(map_cols, row_items):
                    with col:
                        with st.container(border=True):
                            st.markdown(f"**{dist_key}**")
                            st.caption(f"{len(skus)} SKU akan dihapus")

                            sel = st.selectbox(
                                "Apply ke sheet:",
                                options=valid_sheet_names_list,
                                key=f"tab3_map_{dist_key}",
                            )
                            tab3_mapping[dist_key] = sel if sel != "-- lewati --" else None

            # Ringkasan mapping
            mapped_count  = sum(1 for v in tab3_mapping.values() if v)
            skipped_count = sum(1 for v in tab3_mapping.values() if not v)
            if mapped_count:
                st.success(f"✅ {mapped_count} blok siap diproses · {skipped_count} dilewati")

        # ── 6. Tombol Proses ───────────────────────────────────────────────
        st.markdown("---")
        can_run = bool(
            tab3_sheet_blocks and
            any(v for v in tab3_mapping.values())
        )

        if st.button(
            "Proceed All",
            use_container_width=True,
            key="tab3_run_btn",
            type="primary",
            disabled=not can_run,
        ):
            with st.spinner("Memproses semua sheet..."):
                wb_final = openpyxl.load_workbook(
                    io.BytesIO(tab3_bytes), data_only=False
                )
                sheet_meta_map = {s["sheet"]: s for s in valid_sheets}

                # Gabungkan SKU per sheet dari semua blok yang dimapping ke sheet yang sama
                # {sheet_name: set of SKUs}
                sheet_sku_map = {}
                for dist_key, sheet_name in tab3_mapping.items():
                    if not sheet_name:
                        continue
                    skus = tab3_sheet_blocks.get(dist_key, [])
                    if sheet_name not in sheet_sku_map:
                        sheet_sku_map[sheet_name] = set()
                    sheet_sku_map[sheet_name].update(skus)

                sheet_results = []

                for sheet_name, sku_set in sheet_sku_map.items():
                    if sheet_name not in sheet_meta_map:
                        sheet_results.append({
                            "Sheet":   sheet_name,
                            "SKU dihapus": 0,
                            "Status":  "⚠️ Sheet tidak ditemukan",
                        })
                        continue

                    meta        = sheet_meta_map[sheet_name]
                    hdr_row_idx = meta["header_row"] + 1
                    sku_col     = meta["sku_col"]
                    qty_col     = meta["qty_col"]

                    ws = wb_final[sheet_name]

                    headers = {
                        ws.cell(row=hdr_row_idx, column=c).value: c
                        for c in range(1, ws.max_column + 1)
                    }
                    sku_ci = headers.get(sku_col)
                    qty_ci = headers.get(qty_col)

                    changed = 0
                    if sku_ci and qty_ci:
                        for r in range(hdr_row_idx + 1, ws.max_row + 1):
                            cell_val = ws.cell(row=r, column=sku_ci).value
                            if cell_val is None:
                                continue
                            if str(cell_val).strip() in sku_set:
                                ws.cell(row=r, column=qty_ci).value = None
                                changed += 1

                    sheet_results.append({
                        "Sheet":       sheet_name,
                        "SKU dihapus": changed,
                        "Status":      "✅ Diproses" if changed > 0 else "ℹ️ Tidak ada SKU cocok",
                    })

                out_buf = io.BytesIO()
                wb_final.save(out_buf)
                out_bytes_final = out_buf.getvalue()

            st.session_state["tab3_result"] = {
                "bytes":         out_bytes_final,
                "fname":         tab3_fname,
                "sheet_results": sheet_results,
                "mode":          "Hapus QTY",
                "sku_count":     sum(len(v) for v in sheet_sku_map.values()),
            }
        # ── 8. Hasil & Download ────────────────────────────────────────────
        tab3_result = st.session_state.get("tab3_result")
        if tab3_result:
            total_changed = sum(
                r.get("SKU dihapus", 0)
                for r in tab3_result["sheet_results"]
            )

            st.success(
                f"✅ Selesai ({tab3_result['mode']}) — "
                f"total **{total_changed} baris** diubah di "
                f"**{len(tab3_result['sheet_results'])} sheet**."
            )

            result_rows = [
                {
                    "Sheet": r.get("Sheet", "-"),
                    "Baris Diubah": r.get("SKU dihapus", 0),
                    "Status": r.get(
                        "Status",
                        "ℹ️ Tidak ada SKU cocok"
                    ),
                }
                for r in tab3_result["sheet_results"]
            ]

            st.dataframe(
                pd.DataFrame(result_rows),
                use_container_width=True,
                hide_index=True
            ) 
            st.dataframe(pd.DataFrame(result_rows), use_container_width=True, hide_index=True)

            out_name = (
                f"AllSheets_Modified_{tab3_result['fname'].rsplit('.',1)[0]}"
                f"_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            )
            st.download_button(
                label=f"⬇️ Download File Hasil ({out_name})",
                data=tab3_result["bytes"],
                file_name=out_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="tab3_dl_btn",
            )
