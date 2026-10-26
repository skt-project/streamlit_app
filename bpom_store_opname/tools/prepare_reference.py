"""Periksa dan pasang data dari BD: daftar toko, daftar SKU (dropdown), dan/atau katalog SKU + nomor BPOM (template).

    python tools/prepare_reference.py --stores "C:\\...\\Daftar Store.xlsx"              # hanya memeriksa
    python tools/prepare_reference.py --stores X.xlsx --skus Z.xlsx --catalogue Y.xlsx --install   # memeriksa lalu memasang

Tanpa --install tidak ada file yang ditulis. Pemasangan hanya terjadi bila SEMUA pemeriksaan lolos (tanpa ERROR).
- Daftar toko  -> reference/Daftar Store.xlsx (disalin apa adanya setelah lolos). Kolom: Region, Nama Group Toko (opsional),
  Nama Toko; baris dengan grup menjadi SATU pilihan atas nama grup.
- Daftar SKU   (--skus) -> reference/Daftar SKU.xlsx: kolom SKU, Product Name, Pack Size; sumber dropdown form "Tambah SKU langsung".
- Katalog SKU+BPOM (--catalogue) -> Sheet1 pada template/Contoh Excel Response.xlsx (katalog SKU + BPOM; sheet lain tidak disentuh).
  Format yang diterima: (a) kolom PRODUCT ID, PRODUCT NAME, ISI (GR / ML), BPOM NUMBER (urutan bebas), atau
  (b) format BD: SKU, BPOM Number [, BPOM Product Name, Expired Date] - PRODUCT NAME dan ISI diisi dari katalog saat ini,
  lalu dari daftar SKU (--skus atau reference/Daftar SKU.xlsx yang sudah terpasang).
"""
from __future__ import annotations

import argparse
import re
import shutil
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

def check_stores(path: Path) -> tuple[Report, list]:
    rep, tokos = Report(), []
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        sheets = [(w.title, w.sheet_state) for w in wb.worksheets]
        wb.close()
    except Exception as exc:
        rep.errors.append(f"File tidak dapat dibaca: {exc}")
        return rep, tokos
    if len(sheets) > 1:
        rep.warnings.append(f"File punya {len(sheets)} sheet {sheets}; HANYA sheet pertama ('{sheets[0][0]}') yang dibaca.")
    try:
        tokos = reference_data.load_stores(path, max_listed=10_000)
    except reference_data.ReferenceDataError as exc:
        for part in str(exc).split("; "):          # satu baris per masalah
            rep.errors.append(part)
        return rep, tokos
    per_region = Counter(t.region for t in tokos)
    groups = [t for t in tokos if t.is_group]
    rep.info.append(f"{len(tokos)} pilihan toko valid di {len(per_region)} region "
                    f"({len(groups)} grup menggabungkan {sum(t.outlets for t in groups)} baris toko; "
                    f"{len(tokos) - len(groups)} toko tanpa grup).")
    rep.info.append("Region terbesar: " + ", ".join(f"{r} ({n})" for r, n in per_region.most_common(3)))
    over = [r for r, n in per_region.items() if n > SEARCH_THRESHOLD]
    rep.info.append(f"{len(over)} region memakai mode kotak-cari di aplikasi (>{SEARCH_THRESHOLD} pilihan)." if over
                    else f"Semua region <= {SEARCH_THRESHOLD} pilihan: dropdown biasa.")
    dup = [t for t in tokos if not t.is_group and t.outlets > 1]
    if dup:
        rep.warnings.append(f"{len(dup)} nama toko tanpa grup muncul lebih dari sekali di region yang sama (digabung jadi satu pilihan): "
                            + ", ".join(f"{t.name} ({t.region})" for t in dup[:8]) + (" ..." if len(dup) > 8 else ""))
    names = Counter(t.name.casefold() for t in tokos)
    shared = sorted(n for n, c in names.items() if c > 1)
    if shared:
        rep.info.append(f"{len(shared)} nama muncul di lebih dari satu region (dibedakan oleh Region), mis. {shared[:3]}.")
    return rep, tokos


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
        if not (name and isi):          # format BD: lengkapi dari katalog saat ini, lalu daftar SKU
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
                          f"ini{'' if master else ' dan daftar SKU tidak diberikan; coba --skus'}): "
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
                       if reference_data._header_matches(cells)), None)
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


# ── daftar SKU untuk dropdown ───────────────────────────────────────────────

def check_skus(path: Path) -> Report:
    rep = Report()
    try:
        products, skipped = master_product.load_products(path)
    except (master_product.SkuListMissing, master_product.SkuListError) as exc:
        rep.errors.append(str(exc))
        return rep
    rep.info.append(f"{len(products)} SKU unik terbaca.")
    if skipped:
        rep.warnings.append(f"{skipped} baris dilewati karena SKU atau Product Name kosong.")
    no_pack = [p.sku for p in products if not p.pack_size]
    if no_pack:
        rep.warnings.append(f"{len(no_pack)} SKU tanpa Pack Size (tidak bisa ditambah lewat form kecuali ada di template): "
                            + ", ".join(no_pack[:8]) + (" ..." if len(no_pack) > 8 else ""))
    current = {s.product_id.upper() for s in reference_data.load_catalogue(ROOT / "template" / "Contoh Excel Response.xlsx")}
    missing = sorted(current - {p.sku.upper() for p in products})
    if missing:
        rep.warnings.append(f"{len(missing)} SKU di template tidak ada di daftar ini: {missing[:8]}")
    return rep


# ── CLI ─────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stores", type=Path, help="file daftar toko dari BD (.xlsx): Region, Nama Group Toko, Nama Toko")
    ap.add_argument("--catalogue", type=Path, help="file daftar SKU + BPOM dari BD (.xlsx)")
    ap.add_argument("--skus", type=Path, help="file daftar SKU dari BD (.xlsx): SKU, Product Name, Pack Size")
    ap.add_argument("--install", action="store_true", help="pasang ke reference/ dan template/ bila semua pemeriksaan lolos")
    ap.add_argument("--root", type=Path, default=ROOT, help=argparse.SUPPRESS)
    args = ap.parse_args(argv)
    if not (args.stores or args.catalogue or args.skus):
        ap.error("isi --stores, --skus, dan/atau --catalogue")
    root = args.root
    store_target, template_target = root / "reference" / "Daftar Store.xlsx", root / "template" / "Contoh Excel Response.xlsx"
    sku_target = root / "reference" / "Daftar SKU.xlsx"
    all_ok, actions = True, []

    if args.stores:
        print(f"== Daftar toko: {args.stores}")
        rep, _ = check_stores(args.stores)
        rep.print()
        all_ok &= rep.ok
        if rep.ok:
            actions.append(lambda: (store_target.parent.mkdir(exist_ok=True), shutil.copyfile(args.stores, store_target),
                                    print(f"Dipasang: {store_target}")))
    if args.skus:
        print(f"== Daftar SKU (dropdown): {args.skus}")
        rep = check_skus(args.skus)
        rep.print()
        all_ok &= rep.ok
        if rep.ok:
            actions.append(lambda: (sku_target.parent.mkdir(exist_ok=True), shutil.copyfile(args.skus, sku_target),
                                    print(f"Dipasang: {sku_target}")))
    if args.catalogue:
        print(f"== Daftar SKU + BPOM: {args.catalogue}")
        current = reference_data.load_catalogue(template_target)
        master = None
        sku_source = args.skus or (sku_target if sku_target.is_file() else None)
        if sku_source:
            try:
                master, _ = master_product.load_products(sku_source)
                print(f"[INFO] daftar SKU dibaca dari {sku_source.name}: {len(master)} SKU.")
            except (master_product.SkuListMissing, master_product.SkuListError) as exc:
                print(f"[PERINGATAN] daftar SKU tidak terbaca ({exc}); hanya katalog saat ini yang dipakai.")
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
