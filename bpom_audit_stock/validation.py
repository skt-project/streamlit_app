"""Validasi form dan file Excel BPOM Audit Stock.

Modul ini murni (tanpa Streamlit / Google API) supaya mudah dites. Semua
validasi dijalankan SEBELUM ada data yang dikirim ke Google Sheets: satu
masalah saja pada file = seluruh upload ditolak.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any

import openpyxl

# Header template resmi. Harus persis sama (case-sensitive, urutan sama).
EXPECTED_HEADERS: tuple[str, ...] = (
    "Distributor Name (Company - Region)",
    "PRODUCT ID",
    "PRODUCT NAME",
    "BPOM NUMBER",
    "QTY",
)
_HEADERS_FOLDED = {h.casefold() for h in EXPECTED_HEADERS}

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
class ExcelRow:
    excel_row: int
    product_id: str
    product_name: str
    bpom_number: str
    qty: int

    @property
    def key(self) -> tuple[str, str]:
        return row_key(self.product_id, self.bpom_number)


@dataclass
class ParseResult:
    rows: list[ExcelRow] = field(default_factory=list)
    issues: list[Issue] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

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


def row_key(product_id: str, bpom_number: str) -> tuple[str, str]:
    """Kunci kombinasi PRODUCT ID + BPOM NUMBER (tidak peduli huruf besar/kecil)."""
    return (product_id.strip().upper(), bpom_number.strip().upper())


def validate_form(
    *, company, distributor, periode, tanggal, pic, has_file: bool
) -> list[str]:
    """Validasi field form. Mengembalikan daftar pesan error (kosong = valid)."""
    errors = []
    if not company:
        errors.append("Distributor Company wajib dipilih.")
    if not distributor:
        errors.append("Distributor Name wajib dipilih.")
    if not periode:
        errors.append("Periode Audit (bulan dan tahun) wajib dipilih.")
    if not tanggal:
        errors.append("Tanggal Audit wajib diisi.")
    if not (pic and pic.strip()):
        errors.append("Nama PIC Distributor wajib diisi.")
    elif len(pic.strip()) > 100:
        errors.append("Nama PIC Distributor maksimal 100 karakter.")
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
    """QTY harus bilangan bulat (boleh positif, nol, atau negatif)."""
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
    if abs(qty) > MAX_ABS_QTY:
        return None, "QTY terlalu besar."
    return qty, None


def parse_excel(data: bytes, expected_distributor_name: str) -> ParseResult:
    """Baca + validasi file Excel. Tidak ada efek samping; hasilnya `ParseResult`.

    Baris judul di atas header diabaikan. Baris yang sepenuhnya kosong dilewati.
    Semua baris data lain dicek; `result.ok` hanya True bila TIDAK ada masalah.
    """
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
        if len(wb.worksheets) > 1:
            result.warnings.append(
                f"File punya {len(wb.worksheets)} sheet; hanya sheet pertama ('{wb.worksheets[0].title}') yang dibaca."
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

        expected_norm = normalize_name(expected_distributor_name)
        seen: dict[tuple[str, str], int] = {}
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

            name_cell, pid_cell, pname_cell, bpom_cell, qty_cell = cells[:5]

            dist, err = _text(name_cell)
            if err:
                result.issues.append(Issue(no, EXPECTED_HEADERS[0], err.capitalize() + "."))
            elif not dist:
                result.issues.append(Issue(no, EXPECTED_HEADERS[0],
                                           f"Wajib diisi dengan '{expected_distributor_name}'."))
            elif normalize_name(dist) != expected_norm:
                result.issues.append(Issue(
                    no, EXPECTED_HEADERS[0],
                    f"'{dist}' tidak sesuai distributor yang dipilih; seharusnya '{expected_distributor_name}'."))

            texts = {}
            for label, cell in (("PRODUCT ID", pid_cell), ("PRODUCT NAME", pname_cell), ("BPOM NUMBER", bpom_cell)):
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

            if len(result.issues) == before:  # baris ini bersih -> cek duplikat
                key = row_key(texts["PRODUCT ID"], texts["BPOM NUMBER"])
                if key in seen:
                    result.issues.append(Issue(
                        no, "PRODUCT ID + BPOM NUMBER",
                        f"Duplikat dengan baris {seen[key]} ({texts['PRODUCT ID']} / {texts['BPOM NUMBER']})."))
                else:
                    seen[key] = no
                    result.rows.append(ExcelRow(no, texts["PRODUCT ID"], texts["PRODUCT NAME"],
                                                texts["BPOM NUMBER"], qty))

        if data_rows == 0:
            result.issues.append(Issue(None, "File", "File tidak berisi baris data di bawah header."))
    finally:
        wb.close()
    return result
