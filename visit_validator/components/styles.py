"""
styles.py — Global CSS and reusable UI component helpers.

Matches the Skintific internal app theme (PO Portal / Salesman PJP v2).
Call apply_styles() once at the top of app.py.
"""

from typing import List, Tuple, Optional
import streamlit as st

# ── Palette ────────────────────────────────────────────────────────────────────
PRIMARY       = "#1E6B8A"
PRIMARY_DARK  = "#155875"
PRIMARY_LIGHT = "#E8F4FD"
SUCCESS       = "#27AE60"
SUCCESS_LIGHT = "#D5F5E3"
DANGER        = "#E74C3C"
DANGER_LIGHT  = "#FADBD8"
WARNING       = "#F39C12"
WARNING_LIGHT = "#FEF9E7"
INFO          = "#3498DB"
ORANGE        = "#E67E22"
ORANGE_LIGHT  = "#FDEBD0"
TEAL          = "#1A5276"
TEAL_LIGHT    = "#EBF5FB"
TEXT_DARK     = "#1E3A4A"
TEXT_MUTED    = "#5A6C7D"   # WCAG-AA-compliant on white (#F5F7FA)
BG_PAGE       = "#F5F7FA"
BG_CARD       = "#FFFFFF"
BG_SECTION    = "#FAFBFC"
SIDEBAR_BG    = "#1E3A4A"

# ── Status colours (bg_hex, fg_hex) ───────────────────────────────────────────
# DUPLICATE SUBMISSION intentionally excluded — not yet implemented in logic
STATUS_COLORS: dict = {
    "VALID VISIT":              ("D5F5E3", "1E8449"),
    "INVALID VISIT":            ("FADBD8", "922B21"),
    "MISSING GPS":              ("FEF9E7", "9A7D0A"),
    "INVALID COORDINATE":       ("FDEBD0", "784212"),
    "STORE LOCATION NOT FOUND": ("EBF5FB", "1A5276"),
}

# Left-border accent colour per status (for cards and download containers)
STATUS_ACCENT: dict = {
    "VALID VISIT":              SUCCESS,
    "INVALID VISIT":            DANGER,
    "MISSING GPS":              WARNING,
    "INVALID COORDINATE":       ORANGE,
    "STORE LOCATION NOT FOUND": INFO,
}


# ── Global stylesheet ──────────────────────────────────────────────────────────
def apply_styles() -> None:
    """Inject the global Skintific stylesheet into the Streamlit app."""
    st.markdown(
        f"""
        <style>
        /* ── Page ─────────────────────────────────────────────── */
        .stApp {{ background-color: {BG_PAGE}; }}

        /* ── Sidebar ───────────────────────────────────────────── */
        [data-testid="stSidebar"] {{ background-color: {SIDEBAR_BG}; }}
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] span,
        [data-testid="stSidebar"] div,
        [data-testid="stSidebar"] small {{ color: #ECF0F1 !important; }}
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3 {{ color: #ECF0F1 !important; }}
        [data-testid="stSidebar"] .stDownloadButton button {{
            background-color: {PRIMARY} !important; color: white !important;
            border: none; border-radius: 8px; font-weight: 600;
        }}
        [data-testid="stSidebar"] .stDownloadButton button:hover {{
            background-color: {PRIMARY_DARK} !important;
        }}
        /* Slider in sidebar */
        [data-testid="stSidebar"] [data-testid="stSlider"] label {{ color: #ECF0F1 !important; }}
        [data-testid="stSidebar"] [data-testid="stSlider"] p {{ color: #B2C8D4 !important; }}
        [data-testid="stSidebar"] [data-testid="stSlider"] [data-testid="stTickBar"] span {{
            color: #B2C8D4 !important;
        }}

        /* ── Primary action button ─────────────────────────────── */
        .stButton > button[kind="primary"] {{
            background-color: {PRIMARY}; color: white;
            border-radius: 8px; border: none; font-weight: 600;
            padding: 0.55rem 2.5rem; font-size: 1rem;
            transition: background-color 0.2s;
        }}
        .stButton > button[kind="primary"]:hover {{ background-color: {PRIMARY_DARK}; }}
        .stButton > button[kind="primary"]:disabled {{ opacity: 0.38; cursor: not-allowed; }}

        /* ── Secondary / tertiary buttons ──────────────────────── */
        .stButton > button[kind="secondary"] {{
            border: 1.5px solid {PRIMARY}; color: {PRIMARY};
            border-radius: 8px; font-weight: 500; background: transparent;
        }}
        .stButton > button[kind="secondary"]:hover {{
            background: {PRIMARY_LIGHT};
        }}

        /* ── Download buttons ──────────────────────────────────── */
        .stDownloadButton > button {{
            border-radius: 8px; font-weight: 500;
            width: 100%; transition: opacity 0.15s;
        }}

        /* ── Metric cards ──────────────────────────────────────── */
        [data-testid="stMetricValue"] {{
            font-size: 1.85rem; font-weight: 700; color: {TEXT_DARK};
        }}
        [data-testid="stMetricLabel"] {{
            color: {TEXT_MUTED}; font-size: 0.82rem; font-weight: 500;
        }}

        /* ── File uploader drop zone ───────────────────────────── */
        [data-testid="stFileUploadDropzone"] {{
            border: 2px dashed {PRIMARY}; border-radius: 10px;
            background: {PRIMARY_LIGHT};
        }}

        /* ── Dataframe ─────────────────────────────────────────── */
        [data-testid="stDataFrame"] {{
            border-radius: 8px; overflow: hidden;
            border: 1px solid #E0E6ED;
        }}

        /* ── Expander ──────────────────────────────────────────── */
        [data-testid="stExpander"] {{
            border-radius: 8px; border: 1px solid #E0E6ED;
            background: {BG_CARD}; margin-bottom: 10px;
        }}

        /* ── Alert banners ─────────────────────────────────────── */
        [data-testid="stAlert"] {{
            border-radius: 8px; margin-bottom: 10px;
        }}

        /* ── Progress bar fill ─────────────────────────────────── */
        [data-testid="stProgressBar"] > div > div {{
            background-color: {PRIMARY};
        }}

        /* ── Selectbox / multiselect ───────────────────────────── */
        [data-testid="stSelectbox"] > div > div {{ border-radius: 6px; }}
        [data-testid="stMultiSelect"] > div > div {{ border-radius: 6px; }}

        /* ── Captions (WCAG-AA contrast) ───────────────────────── */
        [data-testid="stCaptionContainer"] p {{ color: {TEXT_MUTED} !important; }}

        /* ── Headings ──────────────────────────────────────────── */
        h1, h2, h3 {{ color: {TEXT_DARK}; }}

        /* ── HR ────────────────────────────────────────────────── */
        hr {{ border-color: #D8E0E8; margin: 1.2rem 0; }}

        /* ── Tabs ─────────────────────────────────────────────── */
        .stTabs [aria-selected="true"] {{
            background-color: {PRIMARY} !important;
            color: white !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


# ── Reusable UI components ─────────────────────────────────────────────────────

def status_badge(status: str) -> str:
    """Return an HTML <span> badge for a visit status."""
    bg, fg = STATUS_COLORS.get(status, ("EEEEEE", "555555"))
    return (
        f'<span style="background:#{bg};color:#{fg};border-radius:20px;'
        f'padding:3px 12px;font-size:0.82em;font-weight:600;">{status}</span>'
    )


def render_step_indicator(current_step: int) -> None:
    """
    Render a horizontal step progress bar.
    current_step: 1=Upload, 2=Map Columns, 3=Validate, 4=Results
    """
    steps = ["Upload Data", "Map Columns", "Validate", "Results"]
    parts: List[str] = []

    for i, label in enumerate(steps, 1):
        if i < current_step:
            circle_bg, circle_text = SUCCESS, "✓"
            label_color, label_weight = SUCCESS, "600"
        elif i == current_step:
            circle_bg, circle_text = PRIMARY, str(i)
            label_color, label_weight = TEXT_DARK, "700"
        else:
            circle_bg, circle_text = "#BDC3C7", str(i)
            label_color, label_weight = "#95A5A6", "400"

        parts.append(
            f'<div style="display:flex;align-items:center;gap:8px;">'
            f'<div style="background:{circle_bg};color:white;width:28px;height:28px;'
            f'border-radius:50%;display:flex;align-items:center;justify-content:center;'
            f'font-weight:700;font-size:0.78rem;flex-shrink:0;">{circle_text}</div>'
            f'<span style="color:{label_color};font-weight:{label_weight};'
            f'font-size:0.85rem;white-space:nowrap;">{label}</span>'
            f'</div>'
        )

    connector = (
        '<div style="flex:1;height:2px;background:#E0E6ED;margin:0 6px;min-width:16px;"></div>'
    )
    inner = connector.join(parts)
    st.markdown(
        f'<div style="display:flex;align-items:center;background:{BG_SECTION};'
        f'border:1px solid #E0E6ED;border-radius:10px;padding:12px 20px;'
        f'margin-bottom:4px;">{inner}</div>',
        unsafe_allow_html=True,
    )


def metric_card_html(
    label: str,
    value: str,
    delta: str = "",
    color: str = PRIMARY,
    delta_color: Optional[str] = None,
) -> str:
    """Return HTML string for a coloured metric card (border-top accent)."""
    dc = delta_color or color
    delta_html = (
        f'<div style="font-size:0.8rem;color:{dc};font-weight:600;margin-top:5px;">{delta}</div>'
        if delta else '<div style="height:19px;"></div>'
    )
    return (
        f'<div style="background:{BG_CARD};border-top:4px solid {color};border-radius:10px;'
        f'padding:16px 20px;box-shadow:0 1px 5px rgba(0,0,0,0.07);height:112px;">'
        f'<div style="font-size:0.78rem;color:{TEXT_MUTED};font-weight:500;margin-bottom:5px;">{label}</div>'
        f'<div style="font-size:2rem;font-weight:700;color:{TEXT_DARK};line-height:1.1;">{value}</div>'
        f'{delta_html}'
        f'</div>'
    )


def render_metric_row(cards: List[Tuple[str, str, str, str]]) -> None:
    """
    Render a horizontal row of custom metric cards.
    cards: list of (label, value, delta, color)
    """
    cols = st.columns(len(cards))
    for col, (label, value, delta, color) in zip(cols, cards):
        with col:
            st.markdown(metric_card_html(label, value, delta, color), unsafe_allow_html=True)
    st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)


def mapping_group_header(icon: str, title: str, subtitle: str, color: str) -> str:
    """Return HTML for a column-mapping group header card."""
    return (
        f'<div style="background:{color}18;border-left:4px solid {color};'
        f'border-radius:0 8px 8px 0;padding:10px 14px;margin-bottom:10px;">'
        f'<div style="font-weight:700;color:{TEXT_DARK};font-size:0.95rem;">{icon} {title}</div>'
        f'<div style="font-size:0.78rem;color:{TEXT_MUTED};margin-top:2px;">{subtitle}</div>'
        f'</div>'
    )


def download_card_header(badge_label: str, description: str, color: str) -> str:
    """Return HTML for a styled download card header."""
    return (
        f'<div style="border-left:4px solid {color};background:{color}0F;'
        f'border-radius:0 8px 8px 0;padding:10px 14px;margin-bottom:8px;">'
        f'<div style="font-size:0.68rem;font-weight:700;color:{color};'
        f'text-transform:uppercase;letter-spacing:0.08em;">{badge_label}</div>'
        f'<div style="font-size:0.82rem;color:{TEXT_MUTED};margin-top:2px;">{description}</div>'
        f'</div>'
    )


def inline_badge(text: str, bg: str, fg: str) -> str:
    """Return a small inline HTML chip/badge."""
    return (
        f'<span style="background:#{bg};color:#{fg};border-radius:12px;'
        f'padding:2px 10px;font-size:0.75em;font-weight:600;'
        f'display:inline-block;margin:2px 0;">{text}</span>'
    )
