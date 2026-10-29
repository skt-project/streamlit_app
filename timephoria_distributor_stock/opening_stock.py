"""Stok versi perusahaan (Head Office) dari tab Opening Stock. Hanya dibaca."""
from __future__ import annotations

from typing import Any, Mapping, Sequence

REQUIRED_COLUMNS = ("DB Name", "SPU", "Ending Stock Oct")


class HoStockError(Exception):
    """Tab stok Head Office tidak dapat dibaca. Pesan aman ditampilkan ke user."""


def norm_name(value: Any) -> str:
    return " ".join(str(value or "").split()).casefold()


def parse_qty_cell(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value != value:
            return None
        return int(value) if value.is_integer() else None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    if number != number or not number.is_integer():
        return None
    return int(number)


def parse_opening_rows(rows: Sequence[Mapping[str, Any]]) -> tuple[dict[str, dict[str, int]], list[str], dict[str, str]]:
    """Kembalikan (stok per nama distributor dan SPU, urutan SPU, nama SPU). Baris terakhir menang."""
    stocks: dict[str, dict[str, int]] = {}
    spus: list[str] = []
    names: dict[str, str] = {}
    seen: set[str] = set()
    for row in rows:
        spu = str(row.get("SPU") or "").strip()
        if spu and spu not in seen:
            seen.add(spu)
            spus.append(spu)
        spu_name = str(row.get("SPU NAME") or "").strip()
        if spu and spu_name:
            names[spu] = spu_name
        name = norm_name(row.get("DB Name"))
        if not name or not spu:
            continue
        qty = parse_qty_cell(row.get("Ending Stock Oct"))
        if qty is None:
            continue
        stocks.setdefault(name, {})[spu] = qty
    return stocks, spus, names


def spu_for_code(code: str, spu_codes: Sequence[str]) -> str | None:
    """SPU adalah awalan kode SKU. Bila lebih dari satu cocok, yang terpanjang dipakai."""
    text = str(code or "").strip()
    if not text:
        return None
    matches = [spu for spu in spu_codes if text == spu or text.startswith(spu)]
    if not matches:
        return None
    return max(matches, key=len)


def sheet_url(sheet_id: str, gid: int) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit?gid={gid}#gid={gid}"


def load_ho_stock(gc, sheet_id: str, worksheet_name: str) -> dict[str, Any]:
    try:
        worksheet = gc.open_by_key(sheet_id).worksheet(worksheet_name)
        header = [str(cell).strip() for cell in worksheet.row_values(1)]
        missing = [column for column in REQUIRED_COLUMNS if column not in header]
        if missing:
            raise HoStockError(
                "Header tab stok Head Office tidak lengkap: " + ", ".join(missing)
            )
        rows = worksheet.get_all_records()
    except HoStockError:
        raise
    except Exception as exc:
        raise HoStockError(f"Stok Head Office gagal dibaca ({exc.__class__.__name__}).") from exc
    stocks, spus, names = parse_opening_rows(rows)
    return {"url": sheet_url(sheet_id, worksheet.id), "stocks": stocks, "spus": spus, "spu_names": names}


def lookup_ho_qty(
    stocks: Mapping[str, Mapping[str, int]], distributor_name: str, sku_code: str, spu_codes: Sequence[str],
) -> int | None:
    spu = spu_for_code(sku_code, spu_codes)
    if not spu:
        return None
    return stocks.get(norm_name(distributor_name), {}).get(spu)
