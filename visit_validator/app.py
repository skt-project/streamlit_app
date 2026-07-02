"""
app.py — Salesman Store Visit Validator
Skintific Field Operations | GPS-based visit verification
"""

from io import BytesIO
from typing import Optional

import streamlit as st

from components.styles import (
    STATUS_COLORS,
    PRIMARY, SUCCESS, DANGER, WARNING, ORANGE, INFO, TEXT_MUTED,
    apply_styles,
    download_card_header,
    mapping_group_header,
    metric_card_html,
    render_metric_row,
    render_step_indicator,
    status_badge,
)
from utils.file_utils import generate_template, get_sheet_names, read_file, to_excel_bytes
from utils.geo_utils import VALID_VISIT_THRESHOLD_KM
from utils.validation_utils import detect_column_mapping, validate_and_calculate

# ── Page config (must be first Streamlit call) ────────────────────────────────
st.set_page_config(
    page_title="Visit Validator — Skintific",
    page_icon="📍",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_styles()

# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## 📍 Visit Validator")
    st.caption("Skintific — GPS Store Visit Verification")
    st.divider()

    # ── Configurable threshold slider ─────────────────────────────────────────
    st.markdown("**Validation Threshold**")
    threshold_km: float = st.slider(
        "Valid Visit Radius (KM)",
        min_value=0.1,
        max_value=5.0,
        value=VALID_VISIT_THRESHOLD_KM,
        step=0.1,
        format="%.1f KM",
        help=(
            "Maximum distance between salesman GPS and store GPS to be "
            "classified as a VALID VISIT.\n\n"
            "Default: 1.0 KM (standard Skintific field ops policy).\n"
            "Adjust if your region uses a different allowance."
        ),
    )
    st.caption(
        f"≤ **{threshold_km:.1f} KM** → VALID VISIT  \n"
        f"> **{threshold_km:.1f} KM** → INVALID VISIT"
    )
    st.divider()

    # ── Compact status legend ─────────────────────────────────────────────────
    st.markdown("**Status Legend**")
    _STATUS_TIPS = {
        "VALID VISIT":              f"Salesman ≤ {threshold_km:.1f} KM from store.",
        "INVALID VISIT":            f"Salesman > {threshold_km:.1f} KM from store.",
        "MISSING GPS":              "Salesman GPS null or (0, 0) — device failed.",
        "INVALID COORDINATE":       "GPS outside valid range (-90–90 lat, -180–180 lon).",
        "STORE LOCATION NOT FOUND": "Store reference coordinates are missing or invalid.",
    }
    for status, (bg, _) in STATUS_COLORS.items():
        tip = _STATUS_TIPS.get(status, "")
        st.markdown(
            f'<span title="{tip}" style="background:#{bg};color:#111111 !important;'
            f'border-radius:12px;padding:3px 12px;font-size:0.78em;font-weight:600;'
            f'display:inline-block;margin:3px 0;cursor:default;">{status}</span>',
            unsafe_allow_html=True,
        )
    st.divider()

    # ── Template download ─────────────────────────────────────────────────────
    st.markdown("**Download Template**")
    st.download_button(
        label="📄 Excel Template (.xlsx)",
        data=generate_template(),
        file_name="visit_validator_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        help=(
            "Two-sheet Excel workbook:\n"
            "• Sheet 1 — Instructions & example row.\n"
            "• Sheet 2 — Empty template with required headers."
        ),
    )

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE HEADER
# ═══════════════════════════════════════════════════════════════════════════════
st.title("📍 Salesman Store Visit Validator")
st.markdown(
    "Verify GPS-based store visits using the **Haversine formula**. "
    "Upload a visit matrix, map the GPS columns, and get instant validation results."
)

with st.expander("📖 How to use this app", expanded=False):
    st.markdown(
        f"""
**Quick start (3 steps):**

1. **Upload** your visit matrix file — Excel (.xlsx / .xls) or CSV.
   The app will auto-detect GPS columns. Standard DMS export format
   (`Gps latitude`, `Gps longitude`, `Store Lat`, `Store Long`) is detected automatically.

2. **Review** the column mapping. Confirm each GPS field points to the correct column.
   You can override auto-detected values with the dropdowns.

3. **Run Validation** — the app calculates the Haversine distance for every row
   and classifies each visit. Visits within **{threshold_km:.1f} KM** are **VALID**.

---

**Output columns appended:**

| Column | Description |
|---|---|
| `Distance_KM` | Great-circle distance between salesman and store GPS |
| `Visit_Status` | VALID VISIT · INVALID VISIT · MISSING GPS · INVALID COORDINATE · STORE LOCATION NOT FOUND |
| `Validation_Remark` | Plain-English explanation |

---

**Coordinate requirements:**
- Decimal degrees: e.g. `-6.2088` (not `6°12'31.7"S`).
- Decimal separator: period `.` not comma.
- Indonesia: latitude **-11 to +6**, longitude **95 to 141**.
- GPS `(0, 0)` → auto-flagged as **MISSING GPS** (device failed to acquire fix).
        """
    )

# ── Determine current step for the progress indicator ─────────────────────────
_has_file   = bool(st.session_state.get("_file_sig"))
_has_result = "df_result" in st.session_state

if _has_result:
    _current_step = 4
elif _has_file:
    _current_step = 2
else:
    _current_step = 1

render_step_indicator(_current_step)
st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1 — Upload
# ═══════════════════════════════════════════════════════════════════════════════
st.subheader("Step 1 — Upload Visit Data")
st.caption(
    "Upload your visit matrix file. "
    "The file must contain at least four GPS columns: "
    "salesman latitude, salesman longitude, store latitude, and store longitude."
)

uploaded_file = st.file_uploader(
    "upload",
    type=["xlsx", "xls", "csv"],
    help=(
        "Accepted formats:\n"
        "• .xlsx / .xls — Excel workbook (multi-sheet supported)\n"
        "• .csv — Comma-separated values\n\n"
        "If your file has multiple sheets you will be asked to choose one after upload."
    ),
    label_visibility="collapsed",
)

if not uploaded_file:
    st.info(
        "📂 **Upload a file to get started.**  \n"
        "Your file should include columns for salesman GPS coordinates and store reference "
        "coordinates. If you need a template, download it from the sidebar."
    )
    st.stop()

# ── Invalidate results when a new file is loaded ──────────────────────────────
_file_sig = f"{uploaded_file.name}|{uploaded_file.size}"
if st.session_state.get("_file_sig") != _file_sig:
    for k in ("df_result", "active_mapping", "_reset_mapping"):
        st.session_state.pop(k, None)
    st.session_state["_file_sig"] = _file_sig

file_bytes: bytes = uploaded_file.read()
is_excel: bool = uploaded_file.name.lower().endswith((".xlsx", ".xls"))

# ── Sheet selection (Excel only) ──────────────────────────────────────────────
selected_sheet: Optional[str] = None

if is_excel:
    sheet_names = get_sheet_names(BytesIO(file_bytes))
    if not sheet_names:
        st.error(
            "Could not read sheet names from the uploaded Excel file. "
            "The file may be corrupted or password-protected."
        )
        st.stop()
    if len(sheet_names) > 1:
        default_idx = sheet_names.index("Submissions") if "Submissions" in sheet_names else 0
        selected_sheet = st.selectbox(
            "Select Sheet",
            sheet_names,
            index=default_idx,
            help=(
                "Choose the sheet that contains your visit rows.  \n"
                "**'Submissions'** is pre-selected when found — matches the standard DMS export."
            ),
        )
    else:
        selected_sheet = sheet_names[0]

# ── Parse file ────────────────────────────────────────────────────────────────
with st.spinner("Reading file…"):
    df_raw, read_error = read_file(
        BytesIO(file_bytes),
        filename=uploaded_file.name,
        sheet_name=selected_sheet,
    )

if read_error:
    st.error(
        f"Failed to read file: `{read_error}`  \n"
        "Common causes: password protection, unsupported format, or corrupted data."
    )
    st.stop()

if df_raw.empty:
    st.warning(
        "The selected sheet is empty. "
        "Choose a different sheet or upload a file that contains data rows."
    )
    st.stop()

# ── File info card ────────────────────────────────────────────────────────────
size_kb = round(len(file_bytes) / 1024, 1)
sheet_label = f" · Sheet: **{selected_sheet}**" if selected_sheet else ""
st.markdown(
    f'<div style="background:#FFFFFF;border:1px solid #E0E6ED;border-radius:10px;'
    f'padding:12px 18px;margin-bottom:4px;display:flex;gap:28px;align-items:center;">'
    f'<div><span style="font-size:0.75rem;color:{TEXT_MUTED};font-weight:500;">FILE</span>'
    f'<div style="font-weight:600;color:#1E3A4A;margin-top:2px;">{uploaded_file.name}</div></div>'
    f'<div><span style="font-size:0.75rem;color:{TEXT_MUTED};font-weight:500;">SIZE</span>'
    f'<div style="font-weight:600;color:#1E3A4A;margin-top:2px;">{size_kb} KB</div></div>'
    f'<div><span style="font-size:0.75rem;color:{TEXT_MUTED};font-weight:500;">ROWS</span>'
    f'<div style="font-weight:600;color:#1E3A4A;margin-top:2px;">{len(df_raw):,}</div></div>'
    f'<div><span style="font-size:0.75rem;color:{TEXT_MUTED};font-weight:500;">COLUMNS</span>'
    f'<div style="font-weight:600;color:#1E3A4A;margin-top:2px;">{len(df_raw.columns)}</div></div>'
    f'</div>',
    unsafe_allow_html=True,
)

with st.expander(f"📊 Dataset Preview (first 10 rows)", expanded=False):
    st.dataframe(df_raw.head(10), use_container_width=True, hide_index=True)
    if len(df_raw) > 10:
        st.caption(f"Showing first 10 of {len(df_raw):,} rows.")

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Column Mapping
# ═══════════════════════════════════════════════════════════════════════════════
st.divider()
st.subheader("Step 2 — Map GPS Columns")

# "Modify Mapping" reset: clear cached result so user can re-map
if st.session_state.get("_reset_mapping"):
    st.session_state.pop("df_result", None)
    st.session_state.pop("active_mapping", None)
    st.session_state.pop("_reset_mapping", None)

auto_mapping = detect_column_mapping(df_raw.columns.tolist())
n_detected   = sum(1 for v in auto_mapping.values() if v is not None)

if n_detected == 4:
    st.success("✅ **All 4 GPS columns were automatically detected.** Review below and proceed.")
elif n_detected > 0:
    st.warning(f"⚠️ **{n_detected}/4 GPS columns detected.** Select the missing columns below.")
else:
    st.error(
        "❌ **No GPS columns auto-detected.** "
        "Column names don't match expected patterns — map all four manually."
    )

col_options = ["— Select Column —"] + df_raw.columns.tolist()


def _default_index(detected: Optional[str]) -> int:
    if detected and detected in col_options:
        return col_options.index(detected)
    return 0


def _detection_badge(field: str) -> str:
    detected = auto_mapping.get(field)
    if detected:
        return (
            f'<span style="background:#D5F5E3;color:#1E8449;border-radius:10px;'
            f'padding:1px 9px;font-size:0.72em;font-weight:600;">AUTO</span>'
        )
    return (
        f'<span style="background:#FADBD8;color:#922B21;border-radius:10px;'
        f'padding:1px 9px;font-size:0.72em;font-weight:600;">MANUAL</span>'
    )


# Grouped mapping — Salesman GPS | Store GPS
grp_left, grp_right = st.columns(2)

with grp_left:
    st.markdown(
        mapping_group_header("🔵", "Salesman GPS", "Coordinates captured by the salesman's device", PRIMARY),
        unsafe_allow_html=True,
    )
    st.markdown(_detection_badge("salesman_lat"), unsafe_allow_html=True)
    sal_lat_col = st.selectbox(
        "Salesman Latitude",
        col_options,
        index=_default_index(auto_mapping.get("salesman_lat")),
        key="sel_sal_lat",
        help=(
            "Column holding the **salesman's GPS latitude** at check-in.  \n"
            "• Decimal degrees, e.g. `-6.2088`.  \n"
            "• Valid range: **-90 to 90**.  \n"
            "• Zero or null → **MISSING GPS**."
        ),
    )
    st.markdown(_detection_badge("salesman_lon"), unsafe_allow_html=True)
    sal_lon_col = st.selectbox(
        "Salesman Longitude",
        col_options,
        index=_default_index(auto_mapping.get("salesman_lon")),
        key="sel_sal_lon",
        help=(
            "Column holding the **salesman's GPS longitude** at check-in.  \n"
            "• Decimal degrees, e.g. `106.8456`.  \n"
            "• Valid range: **-180 to 180**.  \n"
            "• Zero or null → **MISSING GPS**."
        ),
    )

with grp_right:
    st.markdown(
        mapping_group_header("🟢", "Store GPS", "Fixed reference coordinates of the store location", SUCCESS),
        unsafe_allow_html=True,
    )
    st.markdown(_detection_badge("store_lat"), unsafe_allow_html=True)
    store_lat_col = st.selectbox(
        "Store Latitude",
        col_options,
        index=_default_index(auto_mapping.get("store_lat")),
        key="sel_store_lat",
        help=(
            "Column holding the **store's reference latitude**.  \n"
            "• Decimal degrees, e.g. `-7.7747`.  \n"
            "• Valid range: **-90 to 90**.  \n"
            "• Missing → **STORE LOCATION NOT FOUND**."
        ),
    )
    st.markdown(_detection_badge("store_lon"), unsafe_allow_html=True)
    store_lon_col = st.selectbox(
        "Store Longitude",
        col_options,
        index=_default_index(auto_mapping.get("store_lon")),
        key="sel_store_lon",
        help=(
            "Column holding the **store's reference longitude**.  \n"
            "• Decimal degrees, e.g. `110.7758`.  \n"
            "• Valid range: **-180 to 180**.  \n"
            "• Missing → **STORE LOCATION NOT FOUND**."
        ),
    )

# ── Duplicate column guard ────────────────────────────────────────────────────
selected_cols = [sal_lat_col, sal_lon_col, store_lat_col, store_lon_col]
non_placeholder = [c for c in selected_cols if c != "— Select Column —"]
has_duplicates  = len(non_placeholder) != len(set(non_placeholder))

if has_duplicates:
    st.error(
        "⚠️ **Duplicate column detected.** "
        "Two or more GPS fields are mapped to the same source column — "
        "please select a unique column for each GPS field."
    )

mapping_complete = (
    all(c != "— Select Column —" for c in selected_cols)
    and not has_duplicates
)

# ── Inline mapping checklist ──────────────────────────────────────────────────
fields_check = [
    ("Salesman Latitude",  sal_lat_col),
    ("Salesman Longitude", sal_lon_col),
    ("Store Latitude",     store_lat_col),
    ("Store Longitude",    store_lon_col),
]
check_items = "".join(
    f'<span style="margin-right:14px;font-size:0.82rem;">'
    f'{"✅" if col != "— Select Column —" else "⬜"} {label}</span>'
    for label, col in fields_check
)
st.markdown(
    f'<div style="background:#FAFBFC;border:1px solid #E0E6ED;border-radius:8px;'
    f'padding:10px 16px;margin-top:8px;">{check_items}</div>',
    unsafe_allow_html=True,
)

if mapping_complete:
    st.info(
        f"**Active mapping:**  \n"
        f"🔵 Salesman → `{sal_lat_col}` (lat) · `{sal_lon_col}` (lon)  \n"
        f"🟢 Store → `{store_lat_col}` (lat) · `{store_lon_col}` (lon)"
    )

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3 — Run Validation
# ═══════════════════════════════════════════════════════════════════════════════
st.divider()
st.subheader("Step 3 — Run Validation")
st.caption(
    f"The app will calculate the Haversine distance for every row and assign a status.  \n"
    f"Visits within **{threshold_km:.1f} KM** of the store are **VALID VISIT**; "
    f"anything beyond is **INVALID VISIT**."
)

if not mapping_complete and not has_duplicates:
    st.warning(
        "⚠️ All four GPS columns must be selected before running validation."
    )

run_btn = st.button(
    "🚀 Run Validation",
    type="primary",
    disabled=not mapping_complete,
    help=(
        f"Calculates the Haversine great-circle distance between each salesman GPS "
        f"and its store GPS.  \n"
        f"• ≤ {threshold_km:.1f} KM → **VALID VISIT**  \n"
        f"• > {threshold_km:.1f} KM → **INVALID VISIT**  \n"
        f"Missing, zero, or out-of-range coordinates get a special error status."
    ),
)

if run_btn and mapping_complete:
    mapping = {
        "salesman_lat": sal_lat_col,
        "salesman_lon": sal_lon_col,
        "store_lat":    store_lat_col,
        "store_lon":    store_lon_col,
    }
    progress = st.progress(0, text="Initialising…")
    with st.spinner("Calculating distances using Haversine formula…"):
        df_result = validate_and_calculate(df_raw, mapping, progress, threshold_km=threshold_km)
    progress.empty()
    st.session_state["df_result"]      = df_result
    st.session_state["active_mapping"] = mapping
    st.session_state["threshold_used"] = threshold_km

# ═══════════════════════════════════════════════════════════════════════════════
# RESULTS — shown only after a successful validation run
# ═══════════════════════════════════════════════════════════════════════════════
if "df_result" not in st.session_state:
    st.stop()

df_result      = st.session_state["df_result"]
threshold_used = st.session_state.get("threshold_used", threshold_km)

st.divider()

# ── Section header + modify-mapping button ────────────────────────────────────
res_title, res_btn = st.columns([5, 1])
with res_title:
    st.subheader("📊 Validation Summary")
with res_btn:
    if st.button(
        "↩ Modify Mapping",
        key="modify_mapping",
        help="Clear the current results and go back to adjust the column mapping.",
    ):
        st.session_state["_reset_mapping"] = True
        st.rerun()

# ── Counts ────────────────────────────────────────────────────────────────────
total       = len(df_result)
n_valid     = int((df_result["Visit_Status"] == "VALID VISIT").sum())
n_invalid   = int((df_result["Visit_Status"] == "INVALID VISIT").sum())
n_missing   = int((df_result["Visit_Status"] == "MISSING GPS").sum())
n_bad_coord = int((df_result["Visit_Status"] == "INVALID COORDINATE").sum())
n_no_store  = int((df_result["Visit_Status"] == "STORE LOCATION NOT FOUND").sum())

valid_pct   = f"{n_valid / total * 100:.1f}%" if total else "—"
invalid_pct = f"{n_invalid / total * 100:.1f}%" if total else "—"
error_pct   = f"{(n_missing + n_bad_coord + n_no_store) / total * 100:.1f}%" if total else "—"

# ── Custom metric cards row ───────────────────────────────────────────────────
render_metric_row([
    ("Total Submissions",  f"{total:,}",     "",          PRIMARY),
    ("Valid Visits",       f"{n_valid:,}",   valid_pct,   SUCCESS),
    ("Invalid Visits",     f"{n_invalid:,}", invalid_pct, DANGER),
    ("Missing GPS",        f"{n_missing:,}", "",          WARNING),
    ("Invalid Coordinates",f"{n_bad_coord:,}","",         ORANGE),
])

if n_no_store:
    st.caption(
        f"ℹ️ **{n_no_store:,}** record(s) flagged as **STORE LOCATION NOT FOUND** "
        f"(store reference coordinates missing or invalid)."
    )

# ── Distance stats as metric row ──────────────────────────────────────────────
df_calc = df_result[df_result["Distance_KM"].notna()]
if not df_calc.empty:
    avg_d = df_calc["Distance_KM"].mean()
    min_d = df_calc["Distance_KM"].min()
    max_d = df_calc["Distance_KM"].max()
    render_metric_row([
        ("Min Distance (KM)",  f"{min_d:.3f}", f"across {len(df_calc):,} records", INFO),
        ("Avg Distance (KM)",  f"{avg_d:.3f}", "",                                  INFO),
        ("Max Distance (KM)",  f"{max_d:.3f}", "",                                  INFO),
    ])

# ── Results table ─────────────────────────────────────────────────────────────
st.divider()
st.subheader("📋 Results Table")
st.caption(
    "Your original data with `Distance_KM`, `Visit_Status`, and `Validation_Remark` appended. "
    "Filter and sort to focus on specific groups."
)

all_statuses = sorted(df_result["Visit_Status"].dropna().unique().tolist())

f_col, s_col = st.columns([3, 1])
with f_col:
    status_filter = st.multiselect(
        "Filter by Status",
        options=all_statuses,
        default=all_statuses,
        key="status_filter",
        help="Select one or more statuses to display. All shown by default.",
    )
with s_col:
    sort_ascending = st.checkbox(
        "Sort by Distance ↑",
        value=True,
        help="Sort shortest-to-longest (valid visits first). Uncheck to reverse.",
    )

df_display = df_result[df_result["Visit_Status"].isin(status_filter)].copy()

if "Distance_KM" in df_display.columns:
    df_display = df_display.sort_values("Distance_KM", ascending=sort_ascending, na_position="last")

if df_display.empty:
    st.info(
        "No records match the selected filter. "
        "Try selecting additional statuses from the **Filter by Status** dropdown above."
    )
else:
    st.caption(f"Showing **{len(df_display):,}** of **{total:,}** records")
    st.dataframe(df_display, use_container_width=True, hide_index=True, height=440)

# ── Download section ──────────────────────────────────────────────────────────
st.divider()
st.subheader("📥 Download Results")
st.caption(
    "All exports include your original columns plus the three validation columns appended."
)

df_invalid_only = df_result[df_result["Visit_Status"] == "INVALID VISIT"]
df_errors_only  = df_result[
    df_result["Visit_Status"].isin(
        ["MISSING GPS", "INVALID COORDINATE", "STORE LOCATION NOT FOUND"]
    )
]

d1, d2, d3 = st.columns(3)

with d1:
    st.markdown(
        download_card_header("ARCHIVE", "All records — original data + validation columns.", PRIMARY),
        unsafe_allow_html=True,
    )
    st.download_button(
        label=f"📥 Full Results ({total:,} rows)",
        data=to_excel_bytes(df_result, "All Results"),
        file_name="visit_validation_full.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        help=(
            "Downloads every row with `Distance_KM`, `Visit_Status`, `Validation_Remark` appended.  \n"
            "Use as the master output for archiving or sharing."
        ),
    )

with d2:
    st.markdown(
        download_card_header(
            "ACTION REQUIRED",
            f"Salesman GPS was more than {threshold_used:.1f} KM from store.",
            DANGER,
        ),
        unsafe_allow_html=True,
    )
    st.download_button(
        label=f"❌ Invalid Visits ({len(df_invalid_only):,} rows)",
        data=to_excel_bytes(df_invalid_only, "Invalid Visits"),
        file_name="visit_validation_invalid.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        help=(
            f"Only rows classified as **INVALID VISIT** — "
            f"salesman GPS was more than {threshold_used:.1f} KM from the store.  \n"
            "Use for follow-up investigation or disciplinary review."
        ),
    )

with d3:
    st.markdown(
        download_card_header(
            "DATA QUALITY",
            "GPS missing, out-of-range coordinates, or store not found.",
            WARNING,
        ),
        unsafe_allow_html=True,
    )
    st.download_button(
        label=f"⚠️ Error Records ({len(df_errors_only):,} rows)",
        data=to_excel_bytes(df_errors_only, "Error Records"),
        file_name="visit_validation_errors.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        help=(
            "Rows that could not be distance-validated:  \n"
            "• **MISSING GPS** — salesman coordinates null or (0, 0).  \n"
            "• **INVALID COORDINATE** — salesman GPS outside valid global range.  \n"
            "• **STORE LOCATION NOT FOUND** — store reference coordinates missing.  \n\n"
            "Share with the data team to investigate and correct source data."
        ),
    )
