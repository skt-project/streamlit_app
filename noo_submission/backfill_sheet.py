"""Isi ulang tab RAW (Google Sheet NOO) dengan baris INPUT_NOO di Supabase yang belum ada di sheet.

Default = DRY RUN (hanya menampilkan baris yang akan ditambahkan). Tambahkan --apply untuk benar-benar menulis.
Kunci baris = (Customer Store Code, Customer Store Name). Baris yang sudah ada di sheet tidak disentuh / tidak diduplikasi.

Contoh:
  python backfill_sheet.py                      # simulasi, semua baris sejak 2026-11-25
  python backfill_sheet.py --since 2026-11-20   # ubah tanggal awal (created_at, WIB)
  python backfill_sheet.py --apply              # tulis ke sheet
"""
import argparse
import os
import tomllib
from datetime import datetime

import gspread
import requests
from google.oauth2 import service_account

AP = argparse.ArgumentParser()
AP.add_argument("--since", default="2026-11-25")
AP.add_argument("--apply", action="store_true")
ARGS = AP.parse_args()

AKAR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SECRETS = tomllib.load(open(os.path.join(AKAR, ".streamlit", "secrets.toml"), "rb"))
flat = {}
def _rata(d):
    for k, v in d.items():
        if isinstance(v, dict):
            _rata(v)
        else:
            flat[k] = v
_rata(SECRETS)
SB_URL = flat["SUPABASE_URL"]
SB_KEY = next(v for k, v in flat.items() if "supabase" in k.lower() and "key" in k.lower())

SHEET_ID, TAB = "1EMgQSe9V15HtOg8qyjT-pjfWw1vEjiDd9D0aRz5IKhc", "RAW"
creds = service_account.Credentials.from_service_account_file(
    os.path.join(AKAR, "noo-spreadsheet.json"), scopes=["https://www.googleapis.com/auth/spreadsheets"])
ws = gspread.authorize(creds).open_by_key(SHEET_ID).worksheet(TAB)

norm = lambda v: str(v or "").strip().upper()
semua = ws.get_all_values()
header = semua[0]
i_kode, i_nama = header.index("Customer Store Code"), header.index("Customer Store Name")
ada = {(norm(r[i_kode]), norm(r[i_nama])) for r in semua[1:] if len(r) > max(i_kode, i_nama)}

h = {"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}"}
rows, start = [], 0
while True:
    r = requests.get(f"{SB_URL}/rest/v1/INPUT_NOO", timeout=30,
                     headers={**h, "Range-Unit": "items", "Range": f"{start}-{start + 999}"},
                     params={"select": "*", "created_at": f"gte.{ARGS.since}T00:00:00+07:00", "order": "created_at.asc"})
    if r.status_code == 416:
        break
    r.raise_for_status()
    b = r.json()
    rows += b
    if len(b) < 1000:
        break
    start += 1000

def wib(s):
    if not s:
        return ""
    return datetime.fromisoformat(str(s)).astimezone().strftime("%Y-%m-%d %H:%M:%S") if False else str(s)[:19].replace("T", " ")

baru = []
for x in rows:
    kunci = (norm(x.get("Customer Store Code")), norm(x.get("Customer Store Name")))
    if kunci in ada:
        continue
    d = {k: ("" if v is None else v) for k, v in x.items()}
    d["Submitted At"] = wib(x.get("created_at"))
    d["Source"] = "NOO Submission"
    d["Submitted By"] = x.get("Group Name") or ""
    d["Generate ID"] = x.get("Store ID") or ""     # sama seperti jalur Save changes: ID masuk kolom Generate ID
    d["Store ID"] = ""
    d["generated_at"] = wib(x.get("generated_at"))
    baru.append([str(d.get(k, "")) for k in header])
    ada.add(kunci)

print(f"{len(rows)} baris di Supabase sejak {ARGS.since}, {len(baru)} belum ada di sheet:")
for r in baru:
    print("  ", r[i_kode], "|", r[i_nama], "|", r[header.index("Generate ID")])
if ARGS.apply and baru:
    ws.append_rows(baru, value_input_option="USER_ENTERED", table_range="A1")
    print("DITULIS ke sheet.")
elif baru:
    print("Dry run, belum ada yang ditulis. Jalankan lagi dengan --apply.")
