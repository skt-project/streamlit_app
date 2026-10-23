"""Penyesuaian tampilan untuk HP (mayoritas pemakai membuka aplikasi dari ponsel).

- Kolom bertanda `keep_row()` tetap BERDAMPINGAN di layar sempit (Streamlit biasanya menumpuk semua kolom
  di bawah 640 px) → Sell Out | Stock, metrik 2x2, dan galeri 2 kolom tidak jadi satu kolom panjang.
- Judul/padding lebih kecil, tombol dan input cukup besar untuk jari, font input >= 16 px (mencegah iOS
  men-zoom otomatis saat kolom difokuskan).
- Kolom Sell Out / Stock memunculkan keypad ANGKA (inputmode=numeric).
- Tombol kamera "Take Photo" bawaan Streamlit ditampilkan sebagai "Ambil gambar" (merah, putih, tebal).
"""
from __future__ import annotations

import streamlit as st
import streamlit.components.v1 as components

_CSS = """
<style>
/* penanda: sembunyikan elemen pembawanya agar tidak menambah jarak */
[data-testid="stElementContainer"]:has(.keep-row) { display: none !important; }
/* blok kolom yang berisi penanda tidak boleh membungkus di layar sempit */
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
/* tombol "Delete" SKU: tulisan putih tebal di latar merah (Streamlit memberi class st-key-<key>) */
[class*="st-key-del_"] button {
    background: #d32f2f !important; color: #fff !important; border: 1.5px solid #d32f2f !important;
    font-weight: 700 !important; padding: .25rem .5rem !important; white-space: nowrap; border-radius: .5rem;
}
[class*="st-key-del_"] button p { font-weight: 700 !important; color: inherit !important; }
[class*="st-key-del_"] button:hover, [class*="st-key-del_"] button:active {
    background: #b71c1c !important; border-color: #b71c1c !important; color: #fff !important;
}
/* placeholder "isi quantity disini" harus muat di dua kolom sempit di HP */
input[aria-label="Sell Out"]::placeholder, input[aria-label="Stock"]::placeholder { font-size: .8rem; }
/* tombol kamera: "Take Photo" (teks bawaan Streamlit, tidak bisa diganti lewat Python) → "Ambil gambar",
   latar merah, huruf putih tebal. Tombol "Clear photo" adalah komponen lain dan tidak ikut berubah. */
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

# Keypad angka untuk kolom jumlah. Memakai dokumen induk (iframe komponen same-origin) dan MutationObserver
# karena Streamlit membuat ulang input saat rerun.
_NUMERIC_JS = """
<script>
(function () {
  try {
    const doc = window.parent.document;
    if (window.parent.__numericKeypad) return;
    window.parent.__numericKeypad = true;
    const labels = ["Sell Out", "Stock"];
    const patch = () => doc.querySelectorAll("input").forEach((el) => {
      if (labels.includes(el.getAttribute("aria-label"))) {
        el.setAttribute("inputmode", "numeric");
        el.setAttribute("pattern", "[0-9]*");
        el.setAttribute("autocomplete", "off");
      }
    });
    patch();
    new MutationObserver(patch).observe(doc.body, { childList: true, subtree: true });
  } catch (e) { /* tanpa keypad angka pun aplikasi tetap berfungsi */ }
})();
</script>
"""


def apply_mobile_layout() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)
    components.html(_NUMERIC_JS, height=0)


def keep_row(last: str | None = None) -> None:
    """Letakkan di dalam kolom PERTAMA sebuah `st.columns(...)` agar kolomnya tetap berdampingan di HP.

    `last="icon"`  : kolom terakhir sempit (tombol ikon, 3 rem).
    `last="button"`: kolom terakhir untuk tombol bertulisan (5,5 rem). Kolom lain berbagi sisa lebar.
    """
    cls = {"icon": "keep-row last-icon", "button": "keep-row last-button"}.get(last or "", "keep-row")
    st.markdown(f'<span class="{cls}"></span>', unsafe_allow_html=True)
