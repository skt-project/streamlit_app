"""Skrip server untuk uji beban: aplikasi ASLI (app.main) dengan Google Sheets dan daftar SKU tiruan. Tidak menyentuh data asli.

Dijalankan oleh perf/run_load_test.py:  streamlit run perf/harness.py   (lingkungan: PERF_OUT, PERF_STORES)
Setiap eksekusi script dicatat ke $PERF_OUT/runs.log: durasi dinding, CPU thread, jumlah thread aktif.
"""
import os
import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import app  # noqa: E402
import fakegoogle  # noqa: E402
import master_product as mp  # noqa: E402
import sheets_store as store  # noqa: E402

OUT = Path(os.environ["PERF_OUT"])
app.STORE_FILE = Path(os.environ["PERF_STORES"])
app.load_config = lambda: app.Config(worksheet_title="PERF", sheet_id="FAKE",
                                     template_path=ROOT / "template" / "Contoh Excel Response.xlsx")
app.get_gspread_client = lambda: MagicMock()
app.load_products = lambda cfg: ([mp.Product(f"PERF-{i:03d}", f"PRODUK PERF NOMOR {i}", "10ml") for i in range(150)], 0, None)
store.open_spreadsheet = fakegoogle.open_spreadsheet      # menghitung berapa kali spreadsheet/worksheet dibuka
store.ensure_worksheet = fakegoogle.ensure_worksheet

_wall, _cpu = time.perf_counter(), time.thread_time()
try:
    app.main()
finally:
    with open(OUT / "runs.log", "a", encoding="utf-8") as f:
        f.write(f"{time.perf_counter() - _wall:.4f} {time.thread_time() - _cpu:.4f} {threading.active_count()}\n")
