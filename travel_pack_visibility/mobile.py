"""Tampilan untuk HP (ASM membuka aplikasi dari ponsel).

- Padding dan judul lebih kecil di layar sempit.
- Font input >= 16 px supaya iOS tidak men-zoom otomatis saat kolom difokuskan.
- Tombol cukup besar untuk jari; tombol pilih file bertuliskan "Pilih foto" dan teks "Drag and drop"
  disembunyikan di HP (tidak relevan di layar sentuh).
"""
from __future__ import annotations

import streamlit as st

_CSS = """
<style>
/* hanya tombol "Upload" (sebelum ada file); tombol hapus/tambah file sesudah upload tidak diubah */
[data-testid="stFileUploaderDropzone"] [data-testid="stBaseButton-secondary"] {
    min-height: 2.9rem; font-size: 0 !important; white-space: nowrap; justify-content: center !important;
}
[data-testid="stFileUploaderDropzone"] [data-testid="stBaseButton-secondary"] * { font-size: 0 !important; }
[data-testid="stFileUploaderDropzone"] [data-testid="stBaseButton-secondary"]::after {
    content: "📷 Pilih foto"; font-size: 1rem; font-weight: 600;
}
.tp-card { border: 1px solid rgba(128,128,128,.35); border-radius: .6rem; padding: .7rem .85rem; margin: .25rem 0 .75rem; }
.tp-card .tp-name { font-weight: 700; font-size: 1.05rem; line-height: 1.3; }
.tp-card .tp-meta { opacity: .8; font-size: .9rem; margin-top: .2rem; }
@media (max-width: 640px) {
  [data-testid="stMainBlockContainer"] { padding: 3.5rem .85rem 4rem !important; }
  h1 { font-size: 1.4rem !important; line-height: 1.25 !important; padding: 0 0 .25rem !important; }
  h2, h3 { font-size: 1.15rem !important; }
  input, textarea, [data-baseweb="select"] input { font-size: 16px !important; }
  .stButton > button { min-height: 3rem; font-size: 1.05rem; }
  [data-testid="stFileUploaderDropzoneInstructions"] { display: none !important; }
  [data-testid="stFileUploaderDropzone"] { padding: .5rem !important; justify-content: center; }
  [data-testid="stFileUploaderDropzone"] [data-testid="stBaseButton-secondary"] { width: 100%; }
}
</style>
"""


def apply_mobile_layout() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)
