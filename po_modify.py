import streamlit as st
import pandas as pd
import openpyxl
import io
import re
import zipfile
from datetime import datetime

st.set_page_config(page_title="Modify Qty per SKU", layout="wide", page_icon="✏️")

st.title("Modify Quantity per Product Code")
st.caption("Upload file PO, pilih SKU yang mau diubah/dihapus qty-nya.")


# ---------- Helper functions (ringan, cuma openpyxl + pandas) ----------

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
    """Ambil QTY apa adanya (hasil kalkulasi kalau formula, bukan teks formula) pakai data_only=True."""
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
            result[sv] = ws.cell(row=r, column=qty_ci).value  # angka hasil, bukan formula
    wb.close()
    return result


def _detect_col(df, keywords):
    return next((c for c in df.columns if any(k in c.lower() for k in keywords)), None)


def _parse_sku_lines(raw_text: str) -> list:
    """Baris kosong / diawali '--' atau '#' dianggap komentar/label, bukan SKU."""
    skus = []
    for line in raw_text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("--") or line.startswith("#"):
            continue
        skus.append(line)
    return skus


def _apply_qty_changes(file_bytes: bytes, sheet_name: str, header_row: int,
                        sku_col: str, qty_col: str, value_map: dict, mode: str):
    """mode='delete' -> qty dikosongkan. mode='edit' -> qty diganti sesuai value_map."""
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=False)
    ws = wb[sheet_name] if sheet_name in wb.sheetnames else wb.active
    hdr_row = header_row + 1
    headers = {ws.cell(row=hdr_row, column=c).value: c for c in range(1, ws.max_column + 1)}
    sku_ci = headers.get(sku_col)
    qty_ci = headers.get(qty_col)
    changed = 0
    if sku_ci and qty_ci:
        for r in range(hdr_row + 1, ws.max_row + 1):
            sv = str(ws.cell(row=r, column=sku_ci).value or "").strip()
            if sv not in value_map:
                continue
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

file_meta = []  # simpan info tiap file: fname, fbytes, sheet, header_row, sku_col, qty_col

tab1, tab2 = st.tabs(["📁 Modifikasi per File", "🗑️ Hapus SKU Massal & Download"])

with tab1:
  for fi, uf in enumerate(uploaded_files):
    fname, fbytes = _convert_to_xlsx(uf.name, uf.read())

    with st.container(border=True):
        st.markdown(f"**#{fi+1}&nbsp; {fname}**")

        sheets = _get_sheet_names(fbytes)
        if not sheets:
            st.warning("⚠️ Tidak ada sheet yang bisa dibaca.")
            continue

        c1, c2 = st.columns([2, 1])
        with c1:
            sheet_sel = st.selectbox("Sheet:", options=sheets, key=f"sheet_{fi}") if len(sheets) > 1 else sheets[0]
            if len(sheets) == 1:
                st.caption(f"📄 Sheet: **{sheet_sel}**")
        with c2:
            auto_hrow = detect_header_row(fbytes, sheet_sel)
            hrow_input = st.number_input("Header row", min_value=1, value=int(auto_hrow) + 1,
                                          step=1, key=f"hrow_{fi}")
        header_row = int(hrow_input) - 1

        try:
            df = _read_df(fbytes, sheet_sel, header_row)
        except Exception as e:
            st.error(f"❌ Gagal membaca file: {e}")
            continue

        st.caption(f"{len(df):,} baris · {len(df.columns)} kolom")
        with st.expander("👁 Preview data", expanded=False):
            st.dataframe(df, use_container_width=True, hide_index=True)

        sku_col = _detect_col(df, ["sku", "product code", "kode", "code"])
        qty_col = _detect_col(df, ["qty", "quantity"])

        if not sku_col or not qty_col:
            st.info("ℹ️ Kolom SKU / QTY tidak terdeteksi otomatis.")
            continue

        file_meta.append({"fi": fi, "fname": fname, "fbytes": fbytes, "sheet": sheet_sel,
                           "header_row": header_row, "df": df, "sku_col": sku_col, "qty_col": qty_col})

        st.markdown("#### 2. Modifikasi Quantity per Product Code (file ini saja)")
        st.caption(f"SKU: **{sku_col}** · Quantity: **{qty_col}**")

        raw_codes = st.text_area(
            "Daftar Product Code (satu per baris — baris diawali '--' diabaikan)",
            height=150, key=f"codes_{fi}",
            placeholder="G2G-2884\n\n-- STOP PO (2 SKU)\nG2G-216\nG2G-842",
        )

        b1, b2 = st.columns(2)
        with b1:
            do_edit = st.button("Modifikasi QTY", use_container_width=True, key=f"btn_edit_{fi}")
        with b2:
            do_delete = st.button("Auto Hapus SKU", use_container_width=True, key=f"btn_del_{fi}")

        skus = _parse_sku_lines(raw_codes)

        if do_delete:
            if not skus:
                st.warning("⚠️ Tidak ada SKU yang valid di daftar.")
            else:
                out_bytes, cnt = _apply_qty_changes(fbytes, sheet_sel, header_row, sku_col, qty_col,
                                                     {s: None for s in skus}, mode="delete")
                st.session_state[f"result_{fi}"] = {"fname": fname, "bytes": out_bytes, "cnt": cnt, "mode": "Auto Hapus"}

        if do_edit:
            if not skus:
                st.warning("⚠️ Tidak ada SKU yang valid di daftar.")
            else:
                st.session_state[f"edit_skus_{fi}"] = skus

        edit_skus = st.session_state.get(f"edit_skus_{fi}")
        if edit_skus:
            st.markdown("**Atur quantity baru per Product Code:**")
            sku_qty_map = _get_qty_value_map(fbytes, sheet_sel, header_row, sku_col, qty_col)
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
                with rc2:
                    st.caption("QTY saat ini")
                    st.markdown(f"**{cur_q if cur_q is not None else '-'}**")
                with rc3:
                    new_values[s] = st.number_input("Quantity baru", min_value=0, step=1,
                                                     value=cur_q_int, key=f"newqty_{fi}_{s}",
                                                     label_visibility="collapsed")

            if st.button("Simpan Perubahan QTY", use_container_width=True, key=f"apply_edit_{fi}"):
                out_bytes, cnt = _apply_qty_changes(fbytes, sheet_sel, header_row, sku_col, qty_col,
                                                     new_values, mode="edit")
                st.session_state[f"result_{fi}"] = {"fname": fname, "bytes": out_bytes, "cnt": cnt, "mode": "Modifikasi QTY"}
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
    # ---------- Hapus SKU Massal (semua file) ----------

    st.markdown("#### Hapus SKU Massal (Semua File Sekaligus)")
    st.caption("Paste daftar SKU sekali — sistem cari & hapus qty SKU tersebut di semua file yang sudah diupload (kalau ketemu).")

    mass_codes = st.text_area(
        "Daftar Product Code untuk dihapus di semua file",
        height=150, key="mass_codes",
        placeholder="G2G-2884\nG2G-216\nG2G-842",
    )

    if st.button("Modify Semua File", use_container_width=True):
        mass_skus = _parse_sku_lines(mass_codes)
        if not mass_skus:
            st.warning("⚠️ Tidak ada SKU yang valid.")
        elif not file_meta:
            st.warning("⚠️ Belum ada file yang berhasil dibaca kolom SKU/QTY-nya.")
        else:
            summary = []
            for fm in file_meta:
                out_bytes, cnt = _apply_qty_changes(fm["fbytes"], fm["sheet"], fm["header_row"],
                                                     fm["sku_col"], fm["qty_col"],
                                                     {s: None for s in mass_skus}, mode="delete")
                st.session_state[f"result_{fm['fi']}"] = {"fname": fm["fname"], "bytes": out_bytes,
                                                            "cnt": cnt, "mode": "Auto Hapus"}
                summary.append((fm["fname"], cnt))
            st.success("✅ Selesai diterapkan ke semua file:")
            for fname, cnt in summary:
                st.markdown(f"- **{fname}** — {cnt} baris dihapus")

    # ---------- Download semua hasil jadi 1 ZIP ----------

    st.divider()
    st.markdown("#### Download Semua File")

    results = {}
    for fm in file_meta:
        r = st.session_state.get(f"result_{fm['fi']}")
        if r:
            results[r.get("fname", fm["fname"])] = r["bytes"]

    if not results:
        st.info("Belum ada file yang dimodifikasi.")
    else:
        st.caption(f"{len(results)} file siap didownload.")
        zip_bytes = _make_zip(results)
        st.download_button(
            label=f"⬇️ Download Semua File ({len(results)} file, .zip)",
            data=zip_bytes,
            file_name=f"Modified_Files_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
            mime="application/zip",
            use_container_width=True,
        )
