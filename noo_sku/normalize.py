"""Normalisation, hashing and date helpers.

Pure functions only — no Streamlit, no network. Everything here is unit-tested
without credentials.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from datetime import date, datetime
from zoneinfo import ZoneInfo

from . import config

_WS = re.compile(r"\s+")
_PUNCT = re.compile(r"[^\w\s]")

# Values that Excel/pandas produce for an empty cell. All must collapse to "",
# otherwise the same logical row hashes differently across two uploads.
_BLANKS = {"", "nan", "none", "null", "nat", "#n/a", "-"}


def clean(value) -> str:
    """Trim, collapse internal whitespace, and map every blank spelling to ''."""
    if value is None:
        return ""
    text = str(value)
    # Excel round-trips numeric-looking codes as floats: "123.0" -> "123".
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    text = text.replace(" ", " ")
    text = unicodedata.normalize("NFKC", text)
    text = _WS.sub(" ", text).strip()
    return "" if text.lower() in _BLANKS else text


def norm_key(value) -> str:
    """Uppercase comparison form used for identity keys and lookups."""
    return clean(value).upper()


def norm_header(value) -> str:
    """Header comparison form: lowercase, no punctuation, single spaces.

    Tolerates the real templates' quirks — a trailing space in 'Customer Code ',
    and parenthetical hints like 'Customer Product Code ( Di isi oleh
    Distributor)'.
    """
    text = clean(value).lower()
    text = re.sub(r"\(.*?\)", " ", text)
    text = _PUNCT.sub(" ", text)
    return _WS.sub(" ", text).strip()


def row_hash(values) -> str:
    """Stable content hash for exact-duplicate detection.

    Order matters, so callers must always pass fields in the same sequence.
    """
    joined = "\x1f".join(norm_key(v) for v in values)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def now_business() -> datetime:
    """Current time in the business timezone (WIB, UTC+7) — never UTC."""
    return datetime.now(ZoneInfo(config.BUSINESS_TIMEZONE))


def format_date_sku(value: date) -> str:
    """M/D/YYYY, matching the 5,347 existing rows in the SKU MAPPING tab."""
    return f"{value.month}/{value.day}/{value.year}"


def format_date_noo(value: date) -> str:
    """DD-Mmm-YYYY, matching the SKINTIFIC NEW tab."""
    return value.strftime(config.DATE_FORMAT_NOO)


def format_input_time(value: datetime) -> str:
    """The value written to the pools' `input_time` column.

    Always rendered from a business-timezone datetime. Never derived from the
    workbook, the browser, or the user's machine clock.
    """
    return value.strftime(config.INPUT_TIME_FORMAT)


def excel_row_number(index: int, header_row: int, skipped: int = 0) -> int:
    """Translate a 0-based data index into the row number the admin sees.

    `index` counts data rows only; `header_row` is the 1-based sheet row the
    header sits on; `skipped` is how many non-data rows (e.g. the CONTOH example)
    follow the header.
    """
    return header_row + skipped + index + 1
