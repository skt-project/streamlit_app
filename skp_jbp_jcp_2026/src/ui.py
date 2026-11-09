"""Shared look & feel: CSS, wordmark, small presentational helpers."""
from __future__ import annotations

import html
from pathlib import Path

import streamlit as st

# Resolved from the app folder, not the working directory: Streamlit Cloud runs
# apps in a repo subfolder with the repo root as the working directory.
APP_DIR = Path(__file__).resolve().parent.parent
LOGO_CANDIDATES = (APP_DIR / "assets" / "skintific_logo.png", APP_DIR / "assets" / "logo.png")

CSS = """
<style>
:root {
  --skp-ink: #111111;
  --skp-muted: #5f6368;
  --skp-line: #e3e3e3;
  --skp-soft: #f6f6f6;
}
/* Top padding clears Streamlit's header bar (~3.75rem; opaque on Streamlit Cloud). */
.block-container { max-width: 1240px; padding-top: 5rem; padding-bottom: 4rem; }
html, body, [class*="css"] { font-size: 16px; }
h1, h2, h3 { letter-spacing: -0.02em; }

/* Big, touch-friendly buttons */
.stButton > button, .stDownloadButton > button, .stFormSubmitButton > button, .stLinkButton > a {
  min-height: 3rem; border-radius: 12px; font-weight: 600; font-size: 1rem;
}
.stTextInput input { min-height: 2.9rem; font-size: 1rem; }

/* Quarter pills: make the selected state unmistakable */
button[data-variant="pills"] { min-height: 2.6rem; padding: 0 1rem; }
button[data-variant="pills"][aria-pressed="true"] { background: var(--skp-ink) !important; border-color: var(--skp-ink) !important; }
button[data-variant="pills"][aria-pressed="true"] * { color: #ffffff !important; }

.skp-wordmark { display: flex; align-items: baseline; gap: .75rem; margin-bottom: .5rem; }
.skp-wordmark b { font-size: 1.15rem; letter-spacing: .32em; color: var(--skp-ink); }
.skp-wordmark span { font-size: .72rem; letter-spacing: .12em; color: var(--skp-muted); text-transform: uppercase; }

.skp-hero h1 { font-size: clamp(2rem, 6vw, 3.4rem); line-height: 1.05; margin: .25rem 0 .5rem; }
.skp-hero p { color: var(--skp-muted); font-size: 1.05rem; max-width: 720px; margin: 0; }

.skp-steps { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: .6rem; margin: 1.25rem 0 1.5rem; }
.skp-step { border: 1px solid var(--skp-line); border-radius: 14px; padding: .8rem .9rem; background: #fff; }
.skp-step b { display: inline-flex; width: 1.6rem; height: 1.6rem; border-radius: 999px; align-items: center;
  justify-content: center; background: var(--skp-ink); color: #fff; font-size: .85rem; margin-bottom: .35rem; }
.skp-step div { font-size: .95rem; color: var(--skp-ink); }

.skp-card { border: 1px solid var(--skp-line); border-radius: 18px; padding: 1.4rem 1.4rem 1rem; background: #fff; min-height: 150px; }
.skp-card .icon { font-size: 2rem; }
.skp-card h3 { margin: .35rem 0 .25rem; font-size: 1.3rem; }
.skp-card p { color: var(--skp-muted); margin: 0 0 .5rem; }

.skp-section { display: flex; align-items: center; gap: .6rem; margin: 1.6rem 0 .4rem; }
.skp-section b { display: inline-flex; width: 1.9rem; height: 1.9rem; border-radius: 999px; align-items: center;
  justify-content: center; background: var(--skp-ink); color: #fff; font-size: .95rem; flex: 0 0 auto; }
.skp-section h3 { margin: 0; font-size: 1.25rem; }

.skp-facts { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: .5rem 1rem; }
.skp-facts dt { color: var(--skp-muted); font-size: .8rem; text-transform: uppercase; letter-spacing: .06em; }
.skp-facts dd { margin: 0 0 .4rem; font-weight: 600; word-break: break-word; }

.skp-chip { display: inline-block; border: 1px solid var(--skp-line); background: var(--skp-soft); border-radius: 999px;
  padding: .15rem .65rem; margin: 0 .35rem .35rem 0; font-size: .85rem; }

.skp-scroll { overflow-x: auto; -webkit-overflow-scrolling: touch; }
.skp-scroll table { border-collapse: collapse; min-width: 560px; width: 100%; font-size: .92rem; }
.skp-scroll th, .skp-scroll td { border-bottom: 1px solid var(--skp-line); padding: .45rem .6rem; text-align: left; white-space: nowrap; }
.skp-scroll th { color: var(--skp-muted); font-weight: 600; background: var(--skp-soft); }

.skp-filename { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: .85rem;
  background: var(--skp-soft); border-radius: 8px; padding: .2rem .45rem; word-break: break-all; }

@media (max-width: 760px) {
  .block-container { padding: 4.5rem 1rem 3rem; }
  .skp-steps { grid-template-columns: 1fr; }
  .skp-facts { grid-template-columns: 1fr 1fr; }
  .skp-wordmark span { display: none; }
  .skp-card { min-height: unset; }
}
</style>
"""


def inject_css() -> None:
    st.markdown(CSS, unsafe_allow_html=True)


def esc(value: object) -> str:
    return html.escape(str(value if value is not None else ""))


def wordmark() -> None:
    logo = next((p for p in LOGO_CANDIDATES if p.exists()), None)
    if logo:
        st.image(str(logo), width=170)
        st.markdown('<div class="skp-wordmark"><span>SKP Document Center · 2026</span></div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="skp-wordmark"><b>SKINTIFIC</b><span>SKP Document Center · 2026</span></div>',
                    unsafe_allow_html=True)


def section(number: int | str, title: str) -> None:
    st.markdown(f'<div class="skp-section"><b>{esc(number)}</b><h3>{esc(title)}</h3></div>', unsafe_allow_html=True)


def facts(items: list[tuple[str, str]]) -> None:
    body = "".join(f"<div><dt>{esc(k)}</dt><dd>{esc(v) or '—'}</dd></div>" for k, v in items)
    st.markdown(f'<dl class="skp-facts">{body}</dl>', unsafe_allow_html=True)


def chips(values: list[str]) -> str:
    return "".join(f'<span class="skp-chip">{esc(v)}</span>' for v in values)


def scroll_table(headers: list[str], rows: list[list[str]]) -> None:
    head = "".join(f"<th>{esc(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{esc(c)}</td>" for c in r) + "</tr>" for r in rows)
    st.markdown(f'<div class="skp-scroll"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>',
                unsafe_allow_html=True)


def mode_banner(mode, source_label: str, storage_label: str, log_label: str) -> None:
    """Explain clearly when the app is not writing to the real GCS / spreadsheet."""
    if mode.mock:
        st.warning(
            "🧪 **Mode lokal (mock).** Dokumen dan log disimpan di komputer ini, **bukan** di Google Cloud Storage / "
            f"spreadsheet.  \nSumber toko: {source_label} · Penyimpanan: {storage_label} · Log: {log_label}"
        )
    for problem in mode.problems:
        st.error(problem)
