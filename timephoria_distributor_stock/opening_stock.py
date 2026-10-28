"""Stok versi perusahaan (Head Office) dari tab Opening Stock. Hanya dibaca."""
from __future__ import annotations

from typing import Any, Mapping, Sequence

REQUIRED_COLUMNS = ("DB Name", "SKU List", "Opening stock Oct")


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


def parse_opening_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, int]]:
    """Nama distributor (dinormalisasi) → kode SKU → opening stock. Baris terakhir menang."""
    stocks: dict[str, dict[str, int]] = {}
    for row in rows:
        name = norm_name(row.get("DB Name"))
        sku = str(row.get("SKU List") or "").strip()
        if not name or not sku:
            continue
        qty = parse_qty_cell(row.get("Opening stock Oct"))
        if qty is None:
            continue
        stocks.setdefault(name, {})[sku] = qty
    return stocks


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
    return {"url": sheet_url(sheet_id, worksheet.id), "stocks": parse_opening_rows(rows)}


def lookup_ho_qty(stocks: Mapping[str, Mapping[str, int]], distributor_name: str, sku_code: str) -> int | None:
    return stocks.get(norm_name(distributor_name), {}).get(sku_code)
