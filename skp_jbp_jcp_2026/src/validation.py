"""Pure validation and naming rules (no Streamlit, no Google imports).

Everything here is deterministic so it can be unit-tested and so that a
re-upload of the same combination always lands on the same GCS object.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field

YEAR = 2026
BRANDS: tuple[str, ...] = ("SKINTIFIC", "TIMEPHORIA", "FACERINNA")
PROGRAMS: tuple[str, ...] = ("JBP", "JCP")
QUARTERS: tuple[str, ...] = tuple(f"{YEAR} Q{i}" for i in range(1, 5))
# Kuartal label -> header kolom di sheet sumber.
QUARTER_SOURCE_COLUMNS: dict[str, str] = {q: f"SKP Q{q[-1]} {YEAR}" for q in QUARTERS}

MAX_FILE_BYTES = 1024 * 1024  # 1 MB
MAX_FILENAME_LENGTH = 180
PDF_MAGIC = b"%PDF-"
EMPTY_REGION_FOLDER = "TANPA REGION"
PIC_MAX_LENGTH = 80

# Nilai sel yang dianggap kosong (hasil error formula di spreadsheet).
BLANK_MARKERS = {"", "#N/A", "N/A", "#REF!", "#VALUE!", "#ERROR!", "#NAME?", "#DIV/0!", "-"}

# Karakter yang tidak aman untuk nama file Windows / object GCS / URL.
_ILLEGAL_CHARS = re.compile(r'[\\:*?"<>|#\[\]\x00-\x1f\x7f]')
_WHITESPACE = re.compile(r"\s+")
_TOKEN = re.compile(r"[A-Z0-9]+")


def clean_cell(value: object) -> str:
    """Normalise one spreadsheet cell: NFKC (turns NBSP into a space), trim,
    collapse whitespace, and map spreadsheet error markers to ''."""
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    text = _WHITESPACE.sub(" ", text).strip()
    return "" if text.upper() in BLANK_MARKERS else text


def sanitize_component(text: object) -> str:
    """Make a string safe as one path component: '/' and '\\' become '_',
    illegal characters are removed, whitespace collapsed, dots/spaces trimmed."""
    value = unicodedata.normalize("NFKC", str(text or ""))
    value = value.replace("/", "_").replace("\\", "_")
    value = _ILLEGAL_CHARS.sub("", value)
    value = _WHITESPACE.sub(" ", value).strip(" .")
    return value


def normalize_filename(name: str, max_length: int = MAX_FILENAME_LENGTH) -> str:
    """Return a safe file name that always ends with a lowercase '.pdf'."""
    stem = str(name or "").strip()
    if stem.lower().endswith(".pdf"):
        stem = stem[:-4]
    stem = sanitize_component(stem)[: max_length - 4].rstrip(" .")
    return f"{stem or 'dokumen'}.pdf"


def fallback_skp_id(brand: str, program: str, quarter: str) -> str:
    """Deterministic stand-in when the spreadsheet has no SKP number."""
    return f"TANPA-NOMOR_{brand}_{program}_{quarter.replace(' ', '_')}"


def build_target_filename(store_name: str, skp_number: str, brand: str, program: str, quarter: str) -> str:
    """'SKP - {NAMA TOKO} - {NOMOR SKP}.pdf' (slashes in the SKP number become '_')."""
    skp = clean_cell(skp_number) or fallback_skp_id(brand, program, quarter)
    return normalize_filename(f"SKP - {store_name} - {skp}")


def region_folder(region: str) -> str:
    return sanitize_component(clean_cell(region)) or EMPTY_REGION_FOLDER


def build_object_path(quarter: str, program: str, region: str, filename: str) -> str:
    """'2026 Q1/jbp/{region}/{file}.pdf' — the GCS object name and ZIP entry."""
    if quarter not in QUARTERS:
        raise ValueError(f"Kuartal tidak dikenal: {quarter!r}")
    if program.upper() not in PROGRAMS:
        raise ValueError(f"Program tidak dikenal: {program!r}")
    return f"{quarter}/{program.lower()}/{region_folder(region)}/{normalize_filename(filename)}"


def format_size(num_bytes: int) -> str:
    if num_bytes < 1024:
        return f"{num_bytes} B"
    if num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:.0f} KB"
    return f"{num_bytes / (1024 * 1024):.2f} MB"


@dataclass
class FileCheck:
    """Result of checking one uploaded file. Errors block, warnings do not."""

    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    @property
    def label(self) -> str:
        if self.errors:
            return "❌ " + self.errors[0]
        if self.warnings:
            return "⚠️ " + self.warnings[0]
        return "✅ Siap diupload"


def validate_pdf(filename: str, data: bytes, max_bytes: int = MAX_FILE_BYTES) -> FileCheck:
    """Blocking checks: PDF extension, PDF signature, size <= 1 MB, not empty."""
    check = FileCheck()
    if not str(filename or "").lower().endswith(".pdf"):
        check.errors.append("Hanya file PDF (.pdf) yang diterima.")
    size = len(data or b"")
    if size == 0:
        check.errors.append("File kosong.")
    elif size > max_bytes:
        check.errors.append(
            f"Ukuran file {format_size(size)} melebihi batas {format_size(max_bytes)}. "
            "Kompres PDF terlebih dahulu lalu upload ulang."
        )
    if size and PDF_MAGIC not in (data or b"")[:1024]:
        check.errors.append("Isi file bukan PDF yang valid.")
    return check


def _number_key(token: str) -> str:
    return token.lstrip("0") or "0"


def filename_resembles_skp(filename: str, skp_number: str) -> bool:
    """True when the user's file name looks like it belongs to this SKP number.

    Rule: the running number of the SKP (first numeric token) must appear, and at
    least 60% of the SKP tokens must be present — or the whole SKP number appears
    with punctuation ignored.
    """
    skp_tokens = _TOKEN.findall(clean_cell(skp_number).upper())
    if not skp_tokens:
        return True
    stem = str(filename or "").upper()
    if stem.endswith(".PDF"):
        stem = stem[:-4]
    compact_name = re.sub(r"[^A-Z0-9]", "", stem)
    if "".join(skp_tokens) in compact_name:
        return True
    name_tokens = set(_TOKEN.findall(stem))
    name_numbers = {_number_key(t) for t in name_tokens if t.isdigit()}

    def found(token: str) -> bool:
        if token.isdigit():
            return _number_key(token) in name_numbers
        return token in name_tokens or (len(token) >= 3 and token in compact_name)

    running_number = next((t for t in skp_tokens if t.isdigit()), None)
    if running_number and not found(running_number):
        return False
    return sum(found(t) for t in skp_tokens) / len(skp_tokens) >= 0.6


def filename_warning(filename: str, skp_number: str) -> str | None:
    """Non-blocking warning text, or None when nothing to warn about."""
    if not clean_cell(skp_number):
        return "Nomor SKP tidak tersedia di spreadsheet; nama file tidak dapat dicocokkan."
    if not filename_resembles_skp(filename, skp_number):
        return f"Nama file tidak mirip nomor SKP {clean_cell(skp_number)}. Pastikan dokumennya benar (tetap boleh dilanjutkan)."
    return None


def validate_pic(value: str) -> str | None:
    """Return an error message, or None when the PIC name is acceptable."""
    pic = clean_cell(value)
    if not pic:
        return "PIC Submit wajib diisi."
    if len(pic) > PIC_MAX_LENGTH:
        return f"PIC Submit maksimal {PIC_MAX_LENGTH} karakter."
    return None
