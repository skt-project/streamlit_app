"""Per-store PDF naming and bulk ZIP assembly for the PO Suggestion Tool.

Pure logic: no Streamlit and no BigQuery, so every rule here can be tested
without credentials. PDF rendering is injected (``pdf_generator``) instead of
imported, which keeps this module free of the Streamlit side effects that
``po_buffer.py`` has at import time.

Rules implemented here
----------------------
* One store -> one PDF. A bulk download is one ZIP holding exactly one PDF per
  store that has valid data. Nothing but PDFs goes into the ZIP.
* File name: ``<nama_toko>_<YYYYMMDD>.pdf`` with the date in Asia/Jakarta.
  Characters that are illegal on Windows/macOS/Linux are replaced. When two
  stores would get the same name, ``store_code`` is added to *both* so the
  result does not depend on row order.
* A store that cannot produce a PDF is never dropped silently: every store in
  the filter gets a :class:`StoreFilePlan`, and the ones that are excluded carry
  a human-readable ``skip_reason``.
"""
from __future__ import annotations

import io
import re
import unicodedata
import zipfile
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

import pandas as pd
import pytz

JAKARTA_TZ = "Asia/Jakarta"

# Characters that break file names on at least one OS, plus control characters.
# A run of them (and the spaces around it) collapses to a single "-":
#   "PT Maju/Jaya" -> "PT Maju-Jaya",  'Toko "A": Pusat' -> "Toko-A-Pusat".
# Windows device names (CON, NUL, ...) need no special case: the final name is
# always "<name>_<date>.pdf", which is not a reserved name.
_ILLEGAL_RUN = re.compile(r'\s*[\\/:*?"<>|\x00-\x1f\x7f]+\s*')
_WHITESPACE = re.compile(r"\s+")
_REPEATED_DASH = re.compile(r"-{2,}")

MAX_NAME_LENGTH = 100  # leaves ample room under the 255-byte file-name limit
FALLBACK_NAME = "toko"

SKIP_NO_CODE = "Kode toko kosong"
SKIP_NO_DETAIL = "Tidak ada baris inventory untuk toko ini"
SKIP_NO_PLAN = "Tidak ada SKU dengan saran qty (buffer_plan kosong)"

PdfGenerator = Callable[[Dict[str, Any], pd.DataFrame], Any]


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------

def jakarta_date_str(now: Optional[datetime] = None) -> str:
    """``YYYYMMDD`` for *now* in Asia/Jakarta (a naive *now* is taken as Jakarta)."""
    tz = pytz.timezone(JAKARTA_TZ)
    if now is None:
        now = datetime.now(tz)
    elif now.tzinfo is None:
        now = tz.localize(now)
    else:
        now = now.astimezone(tz)
    return now.strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# File names
# ---------------------------------------------------------------------------

def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return str(value).strip() == ""


def sanitize_filename_component(value: Any, fallback: str = FALLBACK_NAME) -> str:
    """Make *value* safe to use as part of a file name on any OS."""
    text = "" if _is_blank(value) else str(value)
    text = unicodedata.normalize("NFC", text)
    text = _ILLEGAL_RUN.sub("-", text)
    text = _REPEATED_DASH.sub("-", _WHITESPACE.sub(" ", text))
    # Windows silently drops trailing dots/spaces, which would change the name.
    text = text.strip(" .-")
    if len(text) > MAX_NAME_LENGTH:
        text = text[:MAX_NAME_LENGTH].rstrip(" .-")
    return text or fallback


def build_pdf_filename(
    store_name: Any, date_str: str, store_code: Optional[Any] = None
) -> str:
    """``<nama_toko>_<YYYYMMDD>.pdf``; with *store_code* ``<nama>_<kode>_<tanggal>.pdf``."""
    stem = sanitize_filename_component(store_name)
    if store_code is not None:
        stem = f"{stem}_{sanitize_filename_component(store_code, 'kode')}"
    return f"{stem}_{date_str}.pdf"


def build_zip_filename(store_count: int, date_str: str) -> str:
    """``PO_Bulk_<jumlah_toko>_<YYYYMMDD>.zip``."""
    return f"PO_Bulk_{int(store_count)}_{date_str}.zip"


def build_unique_filenames(
    stores: Sequence[Mapping[str, Any]], date_str: str
) -> Dict[str, str]:
    """Map ``store_code`` -> unique PDF file name for the given stores.

    Stores whose plain ``<nama>_<tanggal>.pdf`` collides (compared
    case-insensitively, as Windows and macOS do) all get their ``store_code``
    added. A final counter suffix guards against the unlikely case that even
    those still collide.
    """
    codes: List[str] = []
    names: Dict[str, Any] = {}
    for store in stores:
        code = store.get("store_code")
        if _is_blank(code):
            continue
        code = str(code).strip()
        if code not in names:
            codes.append(code)
            names[code] = store.get("store_name")

    plain = {code: build_pdf_filename(names[code], date_str) for code in codes}
    counts: Dict[str, int] = {}
    for filename in plain.values():
        counts[filename.casefold()] = counts.get(filename.casefold(), 0) + 1

    result: Dict[str, str] = {}
    used: set = set()
    for code in codes:
        filename = plain[code]
        if counts[filename.casefold()] > 1:
            filename = build_pdf_filename(names[code], date_str, store_code=code)
        suffix = 2
        while filename.casefold() in used:
            stem = filename[: -len(f"_{date_str}.pdf")]
            filename = f"{stem}-{suffix}_{date_str}.pdf"
            suffix += 1
        used.add(filename.casefold())
        result[code] = filename
    return result


# ---------------------------------------------------------------------------
# Store info / detail
# ---------------------------------------------------------------------------

def build_store_info(store_row: Mapping[str, Any]) -> Dict[str, Any]:
    """Store metadata dict in the shape ``generate_po_pdf`` expects.

    Used by both the individual download button and the bulk ZIP so the two
    cannot drift apart.
    """
    return {
        "store_code": store_row["store_code"],
        "store_name": store_row["store_name"],
        "distributor_g2g": store_row.get("distributor", "-"),
        "region": store_row.get("region", "-"),
        "est_order_value": store_row.get("est_order_value"),
        "address": "-",
    }


def get_store_detail(df_inventory: pd.DataFrame, store_code: str) -> pd.DataFrame:
    """Inventory rows of exactly one store (matched on ``store_code``)."""
    return df_inventory[df_inventory["store_code"] == store_code].copy()


# ---------------------------------------------------------------------------
# Bulk plan
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StoreFilePlan:
    """What will happen to one store of the filter in a bulk download."""

    store_code: str
    store_name: str
    region: str
    distributor: str
    file_name: str
    skip_reason: Optional[str] = None

    @property
    def will_generate(self) -> bool:
        return self.skip_reason is None

    def as_store_record(self) -> Dict[str, str]:
        """The shape the download tracker stores for this store."""
        return {
            "store_code": self.store_code,
            "store_name": self.store_name,
            "region": self.region,
            "distributor": self.distributor,
            "file_name": self.file_name,
        }


def _text(value: Any, default: str = "") -> str:
    return default if _is_blank(value) else str(value).strip()


def plan_bulk_download(
    filtered_stores: pd.DataFrame, df_inventory: pd.DataFrame, date_str: str
) -> List[StoreFilePlan]:
    """One :class:`StoreFilePlan` per distinct store of the filter.

    A store is excluded (with a reason) when it has no ``store_code``, no rows
    in the inventory data, or no row with a ``buffer_plan`` - the same test the
    PDF itself applies, so a store marked as generable never yields an empty PDF.
    """
    stores = filtered_stores.to_dict("records")
    filenames = build_unique_filenames(stores, date_str)

    has_plan: Dict[str, bool] = {}
    has_rows: set = set()
    if not df_inventory.empty:
        codes = df_inventory["store_code"].astype("string").str.strip()
        has_rows = set(codes.dropna().unique())
        planned = df_inventory["buffer_plan"].notna() & codes.notna()
        has_plan = {code: True for code in codes[planned].unique()}

    plan: List[StoreFilePlan] = []
    seen: set = set()
    for store in stores:
        code = _text(store.get("store_code"))
        if code and code in seen:
            continue  # defensive: the filter already de-duplicates on store_code
        seen.add(code)

        if not code:
            reason = SKIP_NO_CODE
        elif code not in has_rows:
            reason = SKIP_NO_DETAIL
        elif not has_plan.get(code):
            reason = SKIP_NO_PLAN
        else:
            reason = None

        name = _text(store.get("store_name"), default=code or "-")
        plan.append(
            StoreFilePlan(
                store_code=code,
                store_name=name,
                region=_text(store.get("region"), "-"),
                distributor=_text(store.get("distributor"), "-"),
                file_name=filenames.get(code) or build_pdf_filename(name, date_str),
                skip_reason=reason,
            )
        )
    return plan


# ---------------------------------------------------------------------------
# Bulk ZIP
# ---------------------------------------------------------------------------

@dataclass
class BulkZipResult:
    """Outcome of :func:`build_bulk_zip`.

    ``included`` and ``excluded`` together cover every store of the filter, so
    ``len(included) + len(excluded)`` always equals the number of stores asked
    for. ``zip_bytes`` is ``None`` when no store produced a PDF.
    """

    zip_bytes: Optional[bytes]
    zip_name: str
    included: List[StoreFilePlan]
    excluded: List[StoreFilePlan]

    @property
    def file_count(self) -> int:
        return len(self.included)


def _split_by_store(df_inventory: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """``store_code`` -> that store's inventory rows, in a single pass."""
    if df_inventory.empty:
        return {}
    codes = df_inventory["store_code"].astype("string").str.strip()
    return {
        code: frame
        for code, frame in df_inventory.groupby(codes, sort=False)
    }


def _pdf_bytes(generated: Any) -> bytes:
    data = generated.getvalue() if hasattr(generated, "getvalue") else bytes(generated)
    if not data.startswith(b"%PDF"):
        raise ValueError("hasil bukan file PDF yang valid")
    return data


def build_bulk_zip(
    plan: Sequence[StoreFilePlan],
    df_inventory: pd.DataFrame,
    pdf_generator: PdfGenerator,
    date_str: str,
    *,
    store_rows: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> BulkZipResult:
    """Build one ZIP with one PDF for every generable store of *plan*.

    ``pdf_generator(store_info, store_detail)`` returns the PDF as bytes or a
    ``BytesIO``. A store whose PDF fails is moved to ``excluded`` with the error
    as its reason - the other stores still go into the ZIP. The archive is built
    in memory (no temporary files) and holds only the PDFs.

    ``store_rows`` optionally maps ``store_code`` to the filter row so the PDF
    uses the same metadata as the on-screen list; without it the metadata comes
    from the plan.
    """
    detail_by_store = _split_by_store(df_inventory)

    included: List[StoreFilePlan] = []
    excluded: List[StoreFilePlan] = []
    buffer = io.BytesIO()
    now = datetime.now(pytz.timezone(JAKARTA_TZ))
    # ZIP timestamps cannot be earlier than 1980; Jakarta "now" is always fine.
    stamp = now.timetuple()[:6]

    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for item in plan:
            if not item.will_generate:
                excluded.append(item)
                continue
            try:
                row = (store_rows or {}).get(item.store_code) or {
                    "store_code": item.store_code,
                    "store_name": item.store_name,
                    "region": item.region,
                    "distributor": item.distributor,
                }
                detail = detail_by_store[item.store_code].copy()
                data = _pdf_bytes(pdf_generator(build_store_info(row), detail))
            except Exception as exc:  # noqa: BLE001 - reported per store, never silent
                excluded.append(
                    replace(item, skip_reason=f"Gagal membuat PDF: {exc}")
                )
                continue

            info = zipfile.ZipInfo(item.file_name, date_time=stamp)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, data)
            included.append(item)

    zip_bytes = buffer.getvalue() if included else None
    return BulkZipResult(
        zip_bytes=zip_bytes,
        zip_name=build_zip_filename(len(included), date_str),
        included=included,
        excluded=excluded,
    )
