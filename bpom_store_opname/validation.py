"""Validasi form dan file Excel BPOM Stock Opname.

Modul ini murni (tanpa Streamlit / Google API) supaya mudah dites. Semua
validasi dijalankan SEBELUM ada data yang dikirim ke Google Sheets: satu
masalah saja pada file = seluruh upload ditolak.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from datetime import date
from io import BytesIO
from typing import Any, Sequence

import openpyxl

# Header template resmi. Harus persis sama (case-sensitive, urutan sama).
EXPECTED_HEADERS: tuple[str, ...] = (
    "Distributor Branch",
    "PRODUCT ID",
    "PRODUCT NAME",
    "ISI (GR / ML)",
    "BPOM NUMBER",
    "QTY",
)
COL_ISI = EXPECTED_HEADERS[3]
_HEADERS_FOLDED = {h.casefold() for h in EXPECTED_HEADERS}

INSTRUCTION_SHEET = "Cara Pengisian"   # sheet panduan di template; tidak dibaca saat upload
HEADER_SEARCH_ROWS = 10      # baris judul/title boleh ada di atas header
MAX_DATA_ROWS = 5000
MAX_BLANK_RUN = 2000         # berhenti membaca setelah sekian baris kosong berturut-turut
MAX_FILE_BYTES = 5 * 1024 * 1024
MAX_TEXT_LEN = 200
MAX_ABS_QTY = 2**53 - 1      # batas bilangan bulat yang eksak di Google Sheets (double)

_PUNCT_NOISE = re.compile(r"[.,;:]")
_DASH = re.compile(r"\s*[-–—]\s*")
_WS = re.compile(r"\s+")
_INT_TEXT = re.compile(r"^[+-]?\d+$")


@dataclass(frozen=True)
class Issue:
    """Satu masalah validasi. `row` = nomor baris Excel (None = level file)."""
    row: int | None
    column: str
    message: str


@dataclass(frozen=True)
class Sku:
    """Satu kombinasi SKU + nomor BPOM resmi dari prinsipal."""
    product_id: str
    product_name: str
    bpom_number: str
    isi: str = ""                  # mis. "30g", "20ml"
    expired: date | None = None

    @property
    def key(self) -> tuple[str, str]:
        return row_key(self.product_id, self.bpom_number)


@dataclass(frozen=True)
class ExcelRow:
    excel_row: int
    distributor_name: str      # nama kanonik dari daftar BD (bukan ketikan user)
    product_id: str
    product_name: str
    isi: str
    bpom_number: str
    qty: int
    additional: bool = False   # BPOM tambahan: tidak ada di daftar resmi prinsipal

    @property
    def key(self) -> tuple[str, str, str]:
        """Distributor + PRODUCT ID + BPOM NUMBER (tidak peduli huruf besar/kecil)."""
        return (normalize_name(self.distributor_name), *row_key(self.product_id, self.bpom_number))


@dataclass
class ParseResult:
    rows: list[ExcelRow] = field(default_factory=list)
    issues: list[Issue] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    missing: list[tuple[str, Sku]] = field(default_factory=list)   # (distributor, kombinasi resmi) yang tidak ada di file

    @property
    def ok(self) -> bool:
        return not self.issues and bool(self.rows)


def normalize_name(name: Any) -> str:
    """Bentuk pembanding nama distributor: huruf besar, tanda baca dibuang, spasi/strip dirapikan."""
    if name is None:
        return ""
    text = _PUNCT_NOISE.sub("", str(name).strip().upper())
    text = _DASH.sub(" - ", text)
    return _WS.sub(" ", text).strip()


BULAN = ("Januari", "Februari", "Maret", "April", "Mei", "Juni", "Juli", "Agustus",
         "September", "Oktober", "November", "Desember")


def periode_label(d: date) -> str:
    """Tampilan periode dari tanggal, mis. 'Oktober 2026' (disimpan di sheet sebagai 'YYYY-MM')."""
    return f"{BULAN[d.month - 1]} {d.year}"


def row_key(product_id: str, bpom_number: str) -> tuple[str, str]:
    """Kunci kombinasi PRODUCT ID + BPOM NUMBER (tidak peduli huruf besar/kecil)."""
    return (product_id.strip().upper(), bpom_number.strip().upper())


def validate_form(*, company, store, tanggal, pic, has_file: bool) -> list[str]:
    """Validasi field form. Mengembalikan daftar pesan error (kosong = valid)."""
    errors = []
    if not company:
        errors.append("Distributor Company wajib dipilih.")
    if not store:
        errors.append("Store wajib dipilih.")
    if not tanggal:
        errors.append("Tanggal Stock Opname wajib diisi.")
    if not (pic and pic.strip()):
        errors.append("Nama PIC wajib diisi.")
    elif len(pic.strip()) > 100:
        errors.append("Nama PIC maksimal 100 karakter.")
    if not has_file:
        errors.append("File Excel wajib di-upload.")
    return errors


# ── Parsing Excel ──────────────────────────────────────────────────────────

def _is_blank(cells) -> bool:
    return all(c is None or (isinstance(c, str) and not c.strip()) for c in cells)


def _looks_like_header(cells) -> bool:
    return any(isinstance(c, str) and c.strip().casefold() in _HEADERS_FOLDED for c in cells)


def _check_header(cells) -> list[Issue]:
    actual = list(cells)
    while actual and (actual[-1] is None or (isinstance(actual[-1], str) and not actual[-1].strip())):
        actual.pop()
    expected = list(EXPECTED_HEADERS)
    if actual == expected:
        return []

    issues = []
    missing = [h for h in expected if h not in actual]
    extra = [c for c in actual if c not in expected and not (c is None or c == "")]
    if missing:
        hints = []
        for h in missing:
            near = [c for c in extra if isinstance(c, str) and c.strip().casefold() == h.casefold()]
            if near:
                hints.append(f"{near[0]!r} tidak sama dengan {h!r} (huruf besar/kecil dan spasi harus persis)")
        msg = "Kolom hilang atau nama kolom berbeda: " + ", ".join(repr(h) for h in missing) + "."
        if hints:
            msg += " " + "; ".join(hints) + "."
        issues.append(Issue(None, "Header", msg))
    if extra:
        issues.append(Issue(None, "Header", "Kolom tambahan yang tidak ada di template: "
                            + ", ".join(repr(c) for c in extra) + "."))
    if not missing and not extra:
        issues.append(Issue(None, "Header", "Urutan kolom berbeda dari template: "
                            + " | ".join(repr(c) for c in actual) + "."))
    return issues


def _text(value: Any) -> tuple[str, str | None]:
    """Nilai sel -> teks. Mengembalikan (teks, pesan_error)."""
    if value is None:
        return "", None
    if isinstance(value, bool):
        return "", "format tidak valid (TRUE/FALSE)"
    if isinstance(value, str):
        return value.strip(), None
    if isinstance(value, int):
        return str(value), None
    if isinstance(value, float):
        if math.isfinite(value) and value.is_integer():
            return str(int(value)), None
        return "", "format tidak valid (angka desimal)"
    return "", f"format tidak valid ({type(value).__name__})"


def parse_qty(value: Any) -> tuple[int | None, str | None]:
    """QTY harus bilangan bulat dan tidak boleh negatif (0 atau lebih)."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None, "QTY wajib diisi."
    if isinstance(value, bool):
        return None, "QTY harus berupa bilangan bulat."
    if isinstance(value, int):
        qty = value
    elif isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            return None, f"QTY harus bilangan bulat, ditemukan {value!r}."
        qty = int(value)
    elif isinstance(value, str):
        text = value.strip()
        if not _INT_TEXT.match(text):
            return None, f"QTY harus bilangan bulat, ditemukan {value!r}."
        qty = int(text)
    else:
        return None, f"QTY harus bilangan bulat, ditemukan {value!r}."
    if qty < 0:
        return None, f"QTY tidak boleh negatif (ditemukan {qty}). Isi 0 bila tidak ada stok."
    if qty > MAX_ABS_QTY:
        return None, "QTY terlalu besar."
    return qty, None


def _name_list(names: Sequence[str], limit: int = 3) -> str:
    shown = ", ".join(f"'{n}'" for n in names[:limit])
    return shown + (f", ... ({len(names)} distributor)" if len(names) > limit else "")


def parse_excel(data: bytes, expected_names: Sequence[str],
                skus: Sequence[Sku] | None = None) -> ParseResult:
    """Baca + validasi file Excel. Tidak ada efek samping; hasilnya `ParseResult`.

    Baris judul di atas header diabaikan. Baris yang sepenuhnya kosong dilewati.
    Semua baris data lain dicek; `result.ok` hanya True bila TIDAK ada masalah.

    `expected_names` = Distributor Name milik store yang dipilih. Kolom A tiap baris harus salah
    satunya; nama kanonik disimpan di `ExcelRow.distributor_name`.

    Bila `skus` diberikan: PRODUCT ID wajib salah satu SKU resmi; nomor BPOM boleh di luar daftar
    resmi (ditandai `additional=True`, BPOM tambahan); untuk pasangan resmi, PRODUCT NAME harus
    sama dengan daftar. Kombinasi resmi (per distributor) yang tidak ada di file dilaporkan di `result.missing`.
    """
    master = {k.key: k for k in skus} if skus is not None else None
    master_ids = {k.product_id.strip().upper() for k in skus} if skus is not None else None
    result = ParseResult()
    if len(data) > MAX_FILE_BYTES:
        result.issues.append(Issue(None, "File", f"Ukuran file melebihi {MAX_FILE_BYTES // (1024 * 1024)} MB."))
        return result
    try:
        wb = openpyxl.load_workbook(BytesIO(data), read_only=True, data_only=True)
    except Exception:
        result.issues.append(Issue(None, "File", "File tidak dapat dibaca. Pastikan formatnya .xlsx yang valid."))
        return result

    try:
        if not wb.worksheets:
            result.issues.append(Issue(None, "File", "File tidak memiliki worksheet."))
            return result
        others = [w.title for w in wb.worksheets[1:] if w.title != INSTRUCTION_SHEET]
        if others:
            result.warnings.append(
                f"File punya sheet lain ({', '.join(others)}); hanya sheet pertama ('{wb.worksheets[0].title}') yang dibaca."
            )
        ws = wb.worksheets[0]
        ws.reset_dimensions()
        rows_iter = enumerate(ws.iter_rows(values_only=True), start=1)

        header_cells = None
        for no, cells in rows_iter:
            if no > HEADER_SEARCH_ROWS:
                break
            if _looks_like_header(cells):
                header_cells = cells
                break
        if header_cells is None:
            result.issues.append(Issue(
                None, "Header",
                f"Baris header tidak ditemukan di {HEADER_SEARCH_ROWS} baris pertama. Header yang diharapkan: "
                + " | ".join(EXPECTED_HEADERS),
            ))
            return result
        header_issues = _check_header(header_cells)
        if header_issues:
            result.issues.extend(header_issues)
            return result

        names_by_norm = {normalize_name(n): n for n in expected_names}
        seen: dict[tuple[str, str, str], int] = {}
        blank_run = 0
        data_rows = 0
        for no, cells in rows_iter:
            cells = tuple(cells) + (None,) * max(0, len(EXPECTED_HEADERS) - len(cells))
            if _is_blank(cells):
                blank_run += 1
                if blank_run >= MAX_BLANK_RUN:
                    break
                continue
            blank_run = 0
            data_rows += 1
            if data_rows > MAX_DATA_ROWS:
                result.issues.append(Issue(no, "-", f"Jumlah baris data melebihi batas {MAX_DATA_ROWS}."))
                break

            before = len(result.issues)
            if not _is_blank(cells[len(EXPECTED_HEADERS):]):
                result.issues.append(Issue(no, "-", "Ada data di kolom tambahan di luar template."))

            name_cell, pid_cell, pname_cell, isi_cell, bpom_cell, qty_cell = cells[: len(EXPECTED_HEADERS)]

            dist, err = _text(name_cell)
            if err:
                result.issues.append(Issue(no, EXPECTED_HEADERS[0], err.capitalize() + "."))
            elif not dist:
                result.issues.append(Issue(no, EXPECTED_HEADERS[0],
                                           f"Wajib diisi dengan nama distributor store ini: {_name_list(list(expected_names))}."))
            elif normalize_name(dist) not in names_by_norm:
                result.issues.append(Issue(
                    no, EXPECTED_HEADERS[0],
                    f"'{dist}' bukan distributor milik store yang dipilih. Seharusnya: {_name_list(list(expected_names))}."))

            texts = {}
            for label, cell in (("PRODUCT ID", pid_cell), ("PRODUCT NAME", pname_cell), (COL_ISI, isi_cell),
                                ("BPOM NUMBER", bpom_cell)):
                text, err = _text(cell)
                if err:
                    result.issues.append(Issue(no, label, err.capitalize() + "."))
                elif not text:
                    result.issues.append(Issue(no, label, "Wajib diisi."))
                elif len(text) > MAX_TEXT_LEN:
                    result.issues.append(Issue(no, label, f"Maksimal {MAX_TEXT_LEN} karakter."))
                texts[label] = text

            qty, err = parse_qty(qty_cell)
            if err:
                result.issues.append(Issue(no, "QTY", err))

            key = row_key(texts["PRODUCT ID"], texts["BPOM NUMBER"])
            known = master.get(key) if master is not None else None
            if master is not None and len(result.issues) == before:
                if key[0] not in master_ids:
                    result.issues.append(Issue(
                        no, "PRODUCT ID",
                        f"'{texts['PRODUCT ID']}' bukan SKU yang ditetapkan prinsipal. BPOM tambahan hanya "
                        "boleh untuk SKU yang sudah ada."))
                elif known is not None and normalize_name(known.product_name) != normalize_name(texts["PRODUCT NAME"]):
                    result.issues.append(Issue(
                        no, "PRODUCT NAME",
                        f"Tidak sesuai daftar prinsipal; seharusnya '{known.product_name}'."))
                elif known is not None and known.isi and normalize_name(known.isi) != normalize_name(texts[COL_ISI]):
                    result.issues.append(Issue(
                        no, COL_ISI, f"Tidak sesuai daftar prinsipal; seharusnya '{known.isi}'."))

            if len(result.issues) == before:  # baris ini bersih -> cek duplikat
                canonical = names_by_norm[normalize_name(dist)]
                row_id = (normalize_name(canonical), *key)
                if row_id in seen:
                    result.issues.append(Issue(
                        no, "PRODUCT ID + BPOM NUMBER",
                        f"Duplikat dengan baris {seen[row_id]} ({canonical} / {texts['PRODUCT ID']} / "
                        f"{texts['BPOM NUMBER']})."))
                else:
                    seen[row_id] = no
                    result.rows.append(ExcelRow(no, canonical, texts["PRODUCT ID"], texts["PRODUCT NAME"],
                                                texts[COL_ISI], texts["BPOM NUMBER"], qty,
                                                additional=master is not None and known is None))

        if data_rows == 0:
            result.issues.append(Issue(None, "File", "File tidak berisi baris data di bawah header."))
        elif master is not None and not result.issues:
            result.missing = [(n, k) for n in expected_names for k in skus
                              if (normalize_name(n), *k.key) not in seen]
    finally:
        wb.close()
    return result


# ── SKU / BPOM tambahan yang diinput langsung di aplikasi ───────────────────

@dataclass(frozen=True)
class ExtraSku:
    """Satu baris yang ditambahkan user lewat form (bukan dari file Excel)."""
    product_id: str
    product_name: str
    isi: str
    bpom_number: str
    qty: int


def build_extra(product_id: str, bpom_number: str, qty_raw: Any, products, catalogue: Sequence[Sku]):
    """Validasi satu SKU + BPOM tambahan. Mengembalikan (ExtraSku | None, pesan_error | None).

    `products` = daftar master_product (objek dengan `.sku`, `.product_name`, `.pack_size`). Nama produk dan ISI
    memakai katalog resmi bila SKU itu ada di template (konsisten dengan baris template); jika tidak, dari master_product.
    """
    product_id, bpom = (product_id or "").strip(), (bpom_number or "").strip()
    master = {p.sku.strip().upper(): p for p in products}
    if not product_id:
        return None, "SKU wajib dipilih."
    product = master.get(product_id.upper())
    if product is None:
        return None, f"SKU '{product_id}' tidak ada di master product."
    if not bpom:
        return None, "Nomor BPOM wajib diisi."
    if len(bpom) > MAX_TEXT_LEN:
        return None, f"Nomor BPOM maksimal {MAX_TEXT_LEN} karakter."
    qty, err = parse_qty(qty_raw)
    if err:
        return None, err
    official = next((s for s in catalogue if s.product_id.strip().upper() == product_id.upper()), None)
    name, isi = (official.product_name, official.isi) if official else (product.product_name, product.pack_size)
    if not isi:
        return None, f"ISI (GR / ML) untuk SKU '{product.sku}' tidak tersedia di master product; hubungi BD."
    return ExtraSku(product.sku, name, isi, bpom, qty), None


def merge_extras(parsed: ParseResult, extras: Sequence[ExtraSku], distributor: str,
                 skus: Sequence[Sku]) -> ParseResult:
    """Gabungkan baris tambahan ke hasil parse file. Satu masalah = seluruh input batal (all-or-nothing).

    Kombinasi SKU + BPOM yang sama dengan baris file atau baris tambahan lain ditolak. Baris tambahan bertanda
    `additional=True` bila pasangannya tidak ada di katalog resmi; `excel_row` = 0 (bukan dari file).
    """
    if not extras:
        return parsed
    result = ParseResult(rows=list(parsed.rows), issues=list(parsed.issues), warnings=list(parsed.warnings),
                         missing=list(parsed.missing))
    if parsed.issues:
        return result
    official = {s.key for s in skus}
    taken = {r.key: f"baris {r.excel_row} file Excel" for r in parsed.rows}
    for n, extra in enumerate(extras, start=1):
        row = ExcelRow(0, distributor, extra.product_id, extra.product_name, extra.isi, extra.bpom_number,
                       extra.qty, additional=row_key(extra.product_id, extra.bpom_number) not in official)
        if row.key in taken:
            result.issues.append(Issue(None, f"SKU tambahan #{n}",
                                       f"{extra.product_id} + {extra.bpom_number} sudah ada ({taken[row.key]}). "
                                       "Hapus dari daftar tambahan atau ubah isi file."))
            continue
        taken[row.key] = f"SKU tambahan #{n}"
        result.rows.append(row)
    added = {r.key for r in result.rows[len(parsed.rows):]}
    result.missing = [(n, k) for n, k in parsed.missing if (normalize_name(n), *k.key) not in added]
    return result
