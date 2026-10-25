"""Periksa dan pasang data dari BD: daftar store dan/atau daftar SKU + nomor BPOM.

    python tools/prepare_reference.py --stores "C:\\...\\Daftar Store.xlsx"              # hanya memeriksa
    python tools/prepare_reference.py --stores X.xlsx --catalogue Y.xlsx --install       # memeriksa lalu memasang

Tanpa --install tidak ada file yang ditulis. Pemasangan hanya terjadi bila SEMUA pemeriksaan lolos (tanpa ERROR).
- Daftar store -> reference/Daftar Store.xlsx (disalin apa adanya setelah lolos).
- Daftar SKU   -> Sheet1 pada template/Contoh Excel Response.xlsx (katalog SKU + BPOM; sheet lain tidak disentuh).
  Format yang diterima: (a) kolom PRODUCT ID, PRODUCT NAME, ISI (GR / ML), BPOM NUMBER (urutan bebas), atau
  (b) format BD: SKU, BPOM Number [, BPOM Product Name, Expired Date] - PRODUCT NAME dan ISI diisi dari katalog saat ini,
  lalu dari master_product (--bq, memakai perintah `bq` milik akun Anda, hanya SELECT).
"""
from __future__ import annotations

import argparse
import difflib
import json
import re
import shutil
import subprocess
import sys
from collections import Counter
from copy import copy
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import openpyxl  # noqa: E402

import master_product  # noqa: E402
import reference_data  # noqa: E402
import validation as val  # noqa: E402

STORE_TARGET = ROOT / "reference" / "Daftar Store.xlsx"
DIST_FILE = ROOT / "reference" / "Daftar Distributor.xlsx"
TEMPLATE_TARGET = ROOT / "template" / "Contoh Excel Response.xlsx"
SEARCH_THRESHOLD = 1500          # sama dengan app.STORE_SEARCH_THRESHOLD (app.py mengimpor streamlit, jadi tidak diimpor di sini)
_BPOM_RE = re.compile(r"^N[A-Z]\d{11}$")


@dataclass
class Report:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    info: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def print(self) -> None:
        for title, items in (("INFO", self.info), ("PERINGATAN", self.warnings), ("ERROR", self.errors)):
            for line in items:
                print(f"[{title}] {line}")


def _clean(v) -> str:
    return "" if v is None else str(v).strip()


# ── daftar store ────────────────────────────────────────────────────────────

def check_stores(path: Path, distributors: list[tuple[str, str]]) -> tuple[Report, list]:
    rep, stores = Report(), []
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        sheets = [(w.title, w.sheet_state) for w in wb.worksheets]
        wb.close()
    except Exception as exc:
        rep.errors.append(f"File tidak dapat dibaca: {exc}")
        return rep, stores
    if len(sheets) > 1:
        rep.warnings.append(f"File punya {len(sheets)} sheet {sheets}; HANYA sheet pertama ('{sheets[0][0]}') yang dibaca.")
    try:
        stores = reference_data.load_stores(path, distributors, max_listed=10_000)
    except reference_data.ReferenceDataError as exc:
        message = str(exc)
        # satu per masalah supaya mudah dibaca; tambahkan saran nama distributor yang mirip
        for part in message.split("; "):
            rep.errors.append(part)
        by_name = [n for _, n in distributors]
        for name in re.findall(r"'([^']+)' \(baris \d+\)", message):
            near = difflib.get_close_matches(name, by_name, n=1, cutoff=0.6)
            if near:
                rep.errors.append(f"   saran untuk '{name}': mungkin '{near[0]}'")
        return rep, stores
    per_company = Counter(s.company for s in stores)
    rep.info.append(f"{len(stores)} store valid dari {len(per_company)} distributor company.")
    biggest = per_company.most_common(3)
    rep.info.append("Company terbesar: " + ", ".join(f"{c} ({n})" for c, n in biggest))
    over = [c for c, n in per_company.items() if n > SEARCH_THRESHOLD]
    rep.info.append(f"{len(over)} company memakai mode kotak-cari di aplikasi (>{SEARCH_THRESHOLD} store)." if over
                    else f"Semua company <= {SEARCH_THRESHOLD} store: dropdown biasa.")
    zero = sum(1 for s in stores if s.store_id.startswith("0"))
    if zero:
        rep.info.append(f"{zero} Store ID berawalan 0 (dipertahankan sebagai teks).")
    names = Counter(s.store_name.upper() for s in stores)
    same_name = sum(1 for n in names.values() if n > 1)
    if same_name:
        rep.warnings.append(f"{same_name} Store Name muncul lebih dari sekali (Store ID berbeda) - label di dropdown tetap unik "
                            "karena memuat Store ID.")
    no_dist_store = sorted({n for _, n in distributors} - {s.distributor for s in stores})
    rep.info.append(f"{len(no_dist_store)} dari {len(distributors)} distributor belum punya store di daftar ini.")
    return rep, stores


# ── daftar SKU + BPOM ───────────────────────────────────────────────────────

_CAT_ALIASES = {"PRODUCT ID": ("PRODUCT ID", "SKU"), "PRODUCT NAME": ("PRODUCT NAME",),
                "ISI (GR / ML)": ("ISI (GR / ML)", "ISI", "PACK SIZE"),
                "BPOM NUMBER": ("BPOM NUMBER",)}


def _find_columns(header: list[str]) -> dict[str, int] | None:
    upper = [h.upper() for h in header]
    cols = {}
    for key, aliases in _CAT_ALIASES.items():
        for alias in aliases:
            if alias in upper:
                cols[key] = upper.index(alias)
                break
    return cols if "PRODUCT ID" in cols and "BPOM NUMBER" in cols else None


def fetch_master_via_bq(table: str = master_product.DEFAULT_TABLE) -> list[master_product.Product]:
    """SELECT sku, product_name, pack_size lewat CLI `bq` milik akun Anda (read-only)."""
    exe = shutil.which("bq")
    if not exe:
        raise RuntimeError("perintah `bq` tidak ditemukan di PATH")
    sql = f"SELECT sku, product_name, pack_size FROM `{table}`"
    out = subprocess.run([exe, "query", "--use_legacy_sql=false", "--format=json", "--max_rows=100000", sql],
                         capture_output=True, text=True, check=True, timeout=120).stdout
    products, _ = master_product.products_from_rows(json.loads(out))
    return products


def read_catalogue_input(path: Path, current: list[val.Sku], master: list | None = None) -> tuple[Report, list[val.Sku]]:
    rep, skus = Report(), []
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        ws = wb.worksheets[0]
        ws.reset_dimensions()
        all_rows = list(ws.iter_rows(values_only=True))
        title = ws.title
        wb.close()
    except Exception as exc:
        rep.errors.append(f"File tidak dapat dibaca: {exc}")
        return rep, skus
    header_no, cols = None, None
    for no, cells in enumerate(all_rows[: val.HEADER_SEARCH_ROWS], start=1):
        cols = _find_columns([_clean(c) for c in cells])
        if cols:
            header_no = no
            break
    if not cols:
        rep.errors.append("Header tidak ditemukan. Perlu kolom (PRODUCT ID atau SKU) dan BPOM NUMBER (atau 'BPOM Number').")
        return rep, skus
    rep.info.append(f"Sheet '{title}', header di baris {header_no}, kolom terbaca: {sorted(cols)}.")
    by_id_current = {s.product_id.upper(): s for s in current}
    by_id_master = {p.sku.upper(): p for p in (master or [])}
    seen, unresolved = {}, []
    for no, cells in enumerate(all_rows[header_no:], start=header_no + 1):
        get = lambda k: _clean(cells[cols[k]]) if k in cols and cols[k] < len(cells) else ""  # noqa: E731
        pid, bpom = get("PRODUCT ID"), get("BPOM NUMBER")
        name, isi = get("PRODUCT NAME"), get("ISI (GR / ML)")
        if not (pid or bpom or name or isi):
            continue
        if not pid or not bpom:
            rep.errors.append(f"baris {no}: PRODUCT ID/SKU dan BPOM NUMBER wajib diisi.")
            continue
        if not (name and isi):          # format BD: lengkapi dari katalog saat ini, lalu master_product
            src = by_id_current.get(pid.upper())
            if src:
                name, isi = name or src.product_name, isi or src.isi
            elif pid.upper() in by_id_master:
                m = by_id_master[pid.upper()]
                name, isi = name or m.product_name, isi or m.pack_size
            if not (name and isi):
                unresolved.append((no, pid))
                continue
        key = val.row_key(pid, bpom)
        if key in seen:
            rep.errors.append(f"baris {no}: {pid} + {bpom} ganda (juga di baris {seen[key]}).")
            continue
        seen[key] = no
        if not _BPOM_RE.match(bpom.upper()):
            rep.warnings.append(f"baris {no}: nomor BPOM '{bpom}' tidak berpola NA + 11 angka (periksa salah ketik).")
        skus.append(val.Sku(pid, name, bpom, isi))
    if unresolved:
        rep.errors.append(f"{len(unresolved)} baris tanpa PRODUCT NAME/ISI yang bisa dilengkapi (SKU tidak ada di katalog saat "
                          f"ini{'' if master else ' dan master_product tidak dipakai; coba --bq'}): "
                          + ", ".join(f"baris {n} {p}" for n, p in unresolved[:15]))
    if not skus and not rep.errors:
        rep.errors.append("Tidak ada baris data.")
    # perbandingan dengan katalog yang berlaku sekarang
    if skus and current:
        new_k, old_k = {s.key: s for s in skus}, {s.key: s for s in current}
        added, removed = new_k.keys() - old_k.keys(), old_k.keys() - new_k.keys()
        changed = [k for k in new_k.keys() & old_k.keys()
                   if (new_k[k].product_name, new_k[k].isi) != (old_k[k].product_name, old_k[k].isi)]
        rep.info.append(f"Dibanding katalog saat ini ({len(current)} baris): +{len(added)} baru, -{len(removed)} hilang, "
                        f"{len(changed)} nama/ISI berubah; total baru {len(skus)} baris.")
        if removed:
            rep.warnings.append("Hilang dari katalog baru: " + ", ".join(f"{k[0]}/{k[1]}" for k in sorted(removed)[:10])
                                + (" ..." if len(removed) > 10 else ""))
        if changed:
            rep.warnings.append("Nama/ISI berubah untuk: " + ", ".join(f"{k[0]}/{k[1]}" for k in sorted(changed)[:10]))
        new_ids = {s.product_id.upper() for s in skus}
        gone_ids = {s.product_id.upper() for s in current} - new_ids
        if gone_ids:
            rep.warnings.append(f"SKU yang seluruh BPOM-nya hilang: {sorted(gone_ids)}")
    return rep, skus


def write_catalogue(template_path: Path, skus: list[val.Sku]) -> None:
    """Ganti baris data Sheet1 pada template (header, judul, gaya, dan sheet lain tetap)."""
    wb = openpyxl.load_workbook(template_path)
    ws = wb.worksheets[0]
    header_row = next((no for no, cells in enumerate(ws.iter_rows(max_row=val.HEADER_SEARCH_ROWS, values_only=True), start=1)
                       if tuple(_clean(c) for c in cells[: len(val.EXPECTED_HEADERS)]) == val.EXPECTED_HEADERS), None)
    if header_row is None:
        raise ValueError("Header template tidak ditemukan.")
    first = header_row + 1
    styles = [copy(ws.cell(first, c)._style) for c in range(1, len(val.EXPECTED_HEADERS) + 1)]
    if ws.max_row >= first:
        ws.delete_rows(first, ws.max_row - first + 1)
    for i, sku in enumerate(skus):
        for c, value in enumerate((None, sku.product_id, sku.product_name, sku.isi, sku.bpom_number, None), start=1):
            cell = ws.cell(first + i, c, value)
            cell._style = copy(styles[c - 1])
    wb.save(template_path)


# ── CLI ─────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stores", type=Path, help="file daftar store dari BD (.xlsx)")
    ap.add_argument("--catalogue", type=Path, help="file daftar SKU + BPOM dari BD (.xlsx)")
    ap.add_argument("--bq", action="store_true", help="lengkapi PRODUCT NAME/ISI dari gt_schema.master_product (perintah bq, SELECT saja)")
    ap.add_argument("--install", action="store_true", help="pasang ke reference/ dan template/ bila semua pemeriksaan lolos")
    ap.add_argument("--root", type=Path, default=ROOT, help=argparse.SUPPRESS)
    args = ap.parse_args(argv)
    if not (args.stores or args.catalogue):
        ap.error("isi --stores dan/atau --catalogue")
    root = args.root
    store_target, template_target = root / "reference" / "Daftar Store.xlsx", root / "template" / "Contoh Excel Response.xlsx"
    all_ok, actions = True, []

    if args.stores:
        print(f"== Daftar store: {args.stores}")
        distributors = reference_data.load_distributors(root / "reference" / "Daftar Distributor.xlsx")
        rep, _ = check_stores(args.stores, distributors)
        rep.print()
        all_ok &= rep.ok
        if rep.ok:
            actions.append(lambda: (store_target.parent.mkdir(exist_ok=True), shutil.copyfile(args.stores, store_target),
                                    print(f"Dipasang: {store_target}")))
    if args.catalogue:
        print(f"== Daftar SKU + BPOM: {args.catalogue}")
        current = reference_data.load_catalogue(template_target)
        master = None
        if args.bq:
            try:
                master = fetch_master_via_bq()
                print(f"[INFO] master_product dibaca: {len(master)} SKU.")
            except Exception as exc:
                print(f"[PERINGATAN] master_product tidak terbaca ({exc}); hanya katalog saat ini yang dipakai.")
        rep, skus = read_catalogue_input(args.catalogue, current, master)
        rep.print()
        all_ok &= rep.ok
        if rep.ok:
            actions.append(lambda: (write_catalogue(template_target, skus), print(f"Dipasang: Sheet1 di {template_target} ({len(skus)} baris)")))
    print()
    if not all_ok:
        print("HASIL: ada ERROR - tidak ada yang dipasang. Perbaiki file sumber lalu jalankan lagi.")
        return 1
    if not args.install:
        print("HASIL: semua pemeriksaan lolos. Tambahkan --install untuk memasang.")
        return 0
    for action in actions:
        action()
    print("HASIL: terpasang. Jalankan `python -m pytest tests` lalu cek aplikasi.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
