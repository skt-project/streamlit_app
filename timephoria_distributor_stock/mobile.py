"""Penyesuaian tampilan untuk HP.

- Kolom bertanda `keep_row()` tetap berdampingan di layar sempit.
- Font input 16 px agar iOS tidak men-zoom, tombol cukup tinggi untuk jari.
- Input Stock memakai keypad angka (`inputmode=numeric`) lewat `st.html`.
- Tombol kamera "Take Photo" tampil sebagai "Ambil gambar" (merah, putih, tebal).
"""
from __future__ import annotations

import streamlit as st

_CSS = """
<style>
[data-testid="stElementContainer"]:has(.keep-row) { display: none !important; }
[data-testid="stHtml"] { display: none !important; height: 0 !important; margin: 0 !important; }
[data-testid="stHorizontalBlock"]:has(.keep-row) { flex-wrap: nowrap !important; gap: .5rem !important; }
[data-testid="stHorizontalBlock"]:has(.keep-row) > [data-testid="stColumn"] {
    min-width: 0 !important; flex: 1 1 0 !important; width: auto !important;
}
[data-testid="stHorizontalBlock"]:has(.keep-row.last-icon) > [data-testid="stColumn"]:last-child {
    flex: 0 0 3rem !important;
}
[data-testid="stHorizontalBlock"]:has(.keep-row.last-button) > [data-testid="stColumn"]:last-child {
    flex: 0 0 5.5rem !important;
}
input[aria-label="Stock"]::placeholder { font-size: .8rem; }
[data-testid="stCameraInputButton"] {
    background: #d32f2f !important; border: 1px solid #d32f2f !important; color: #fff !important;
    font-size: 0 !important; min-height: 3rem; border-radius: .5rem;
}
[data-testid="stCameraInputButton"] * { font-size: 0 !important; color: transparent !important; }
[data-testid="stCameraInputButton"]::after {
    content: "Ambil gambar"; font-size: 1rem; font-weight: 700; color: #fff;
}
[data-testid="stCameraInputButton"]:hover:not(:disabled), [data-testid="stCameraInputButton"]:active:not(:disabled) {
    background: #b71c1c !important; border-color: #b71c1c !important;
}
[data-testid="stCameraInputButton"]:disabled { opacity: .55; }
@media (max-width: 640px) {
  [data-testid="stMainBlockContainer"] { padding: 1.25rem .75rem 5rem !important; }
  h1 { font-size: 1.55rem !important; line-height: 1.25 !important; padding: 0 !important; }
  h2, h3 { font-size: 1.2rem !important; }
  input, textarea, [data-baseweb="select"] input { font-size: 16px !important; }
  .stButton > button, .stDownloadButton > button, [data-testid="stBaseButton-secondaryFormSubmit"],
  [data-testid="stBaseButton-primaryFormSubmit"] { min-height: 2.9rem; }
  [data-testid="stMetricValue"] { font-size: 1.45rem !important; }
  [data-testid="stMetricLabel"] { font-size: .8rem !important; }
  [data-testid="stTabs"] button { padding: .5rem .75rem; }
}
</style>
"""

_NUMERIC_JS = """
<script>
(function () {
  try {
    const root = window.parent && window.parent.document ? window.parent.document : document;
    const flagHost = window.parent || window;
    if (flagHost.__tphNumericKeypad) return;
    flagHost.__tphNumericKeypad = true;
    const patch = () => root.querySelectorAll("input").forEach((el) => {
      if (el.getAttribute("aria-label") === "Stock") {
        el.setAttribute("inputmode", "numeric");
        el.setAttribute("pattern", "[0-9]*");
        el.setAttribute("autocomplete", "off");
      }
    });
    patch();
    new MutationObserver(patch).observe(root.body, { childList: true, subtree: true });
  } catch (e) { /* tanpa keypad angka pun form tetap bisa diisi */ }
})();
</script>
"""


def apply_mobile_layout() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)
    # Script lewat st.html (komponen html iframe lama dihapus setelah 2026-06-01).
    st.html(_NUMERIC_JS, unsafe_allow_javascript=True)


def keep_row(last: str | None = None) -> None:
    """Tandai kolom pertama agar `st.columns` tidak menumpuk di layar sempit.

    `last="icon"` kolom terakhir 3rem. `last="button"` kolom terakhir 5.5rem.
    """
    cls = {"icon": "keep-row last-icon", "button": "keep-row last-button"}.get(last or "", "keep-row")
    st.markdown(f'<span class="{cls}"></span>', unsafe_allow_html=True)
