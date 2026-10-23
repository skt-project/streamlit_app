"""Reporting mirror: upsert baris transaksi ke worksheet `Store Sales`.

BigQuery adalah source of truth; modul ini hanya mencerminkan. Key upsert:
tanggal_penjualan + store_id + sku_code.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Mapping, Sequence

from gspread.exceptions import WorksheetNotFound

from timeutil import fmt_wib

log = logging.getLogger("store_sales.sheets")

OUTPUT_HEADERS: list[str] = [
    "transaction_id", "tanggal_penjualan", "store_id", "store_name", "region", "store_group",
    "sku_code", "sku_name", "sell_out_qty", "stock_qty", "supervisor_name", "evidence_file_id", "evidence_file_url",
    "evidence_file_name", "evidence_uploaded_at", "created_at", "updated_at",
]
_KEY_COLS = [OUTPUT_HEADERS.index(c) for c in ("tanggal_penjualan", "store_id", "sku_code")]
_LAST_COL = chr(ord("A") + len(OUTPUT_HEADERS) - 1)     # 17 kolom: A..Q


class MirrorError(Exception):
    """Mirror Google Sheets gagal. Pesan aman ditampilkan ke user."""


def _fmt(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return fmt_wib(value)
    if isinstance(value, date):
        return value.isoformat()
    return value


def row_values(record: Mapping[str, Any]) -> list[Any]:
    return [_fmt(record.get(h)) for h in OUTPUT_HEADERS]


def _key_of(row: Sequence[Any]) -> tuple[str, str, str]:
    return tuple(str(row[i]).strip() if i < len(row) else "" for i in _KEY_COLS)  # type: ignore[return-value]


class SheetsMirror:
    def __init__(self, spreadsheet, worksheet_name: str = "Store Sales"):
        """`spreadsheet`: objek gspread Spreadsheet, atau fungsi tanpa argumen yang membukanya
        (lazy, agar masalah akses ke sheet output tidak menghalangi aplikasi start)."""
        self._spreadsheet = spreadsheet
        self.worksheet_name = worksheet_name

    @property
    def spreadsheet(self):
        if callable(self._spreadsheet):
            self._spreadsheet = self._spreadsheet()
        return self._spreadsheet

    def _worksheet(self):
        """Ambil worksheet; buat (beserta header) bila belum ada. Header salah → MirrorError, tanpa menulis."""
        try:
            ws = self.spreadsheet.worksheet(self.worksheet_name)
        except WorksheetNotFound:
            ws = self.spreadsheet.add_worksheet(title=self.worksheet_name, rows=1000, cols=len(OUTPUT_HEADERS))
            ws.update(range_name=f"A1:{_LAST_COL}1", values=[OUTPUT_HEADERS], value_input_option="RAW")
            try:
                ws.freeze(rows=1)
            except Exception:                       # kosmetik saja
                log.warning("Gagal freeze header worksheet baru", exc_info=True)
            return ws
        header = [str(h).strip() for h in ws.row_values(1)]
        if not header:                              # worksheet ada tapi kosong
            ws.update(range_name=f"A1:{_LAST_COL}1", values=[OUTPUT_HEADERS], value_input_option="RAW")
        elif header != OUTPUT_HEADERS:
            raise MirrorError(
                f"Header worksheet '{self.worksheet_name}' tidak sesuai; tidak ada data yang ditulis. "
                "Harus persis: " + ", ".join(OUTPUT_HEADERS)
            )
        return ws

    def upsert(self, records: Sequence[Mapping[str, Any]]) -> tuple[int, int]:
        """Upsert; kembalikan (jumlah_insert, jumlah_update)."""
        if not records:
            return 0, 0
        try:
            ws = self._worksheet()
            existing = ws.get(f"A:{_LAST_COL}") or []
            index: dict[tuple[str, str, str], int] = {}
            for n, row in enumerate(existing[1:], start=2):      # n = nomor baris sheet
                index.setdefault(_key_of(row), n)

            updates: list[dict] = []
            inserts: list[list[Any]] = []
            pending: dict[tuple[str, str, str], int] = {}       # key baru → posisi di `inserts`
            for rec in records:
                vals = row_values(rec)
                key = _key_of(vals)
                if key in index:
                    n = index[key]
                    updates.append({"range": f"A{n}:{_LAST_COL}{n}", "values": [vals]})
                elif key in pending:
                    inserts[pending[key]] = vals                # key sama dalam satu batch → yang terakhir menang
                else:
                    pending[key] = len(inserts)
                    inserts.append(vals)

            if updates:
                ws.batch_update(updates, value_input_option="RAW")
            if inserts:
                ws.append_rows(inserts, value_input_option="RAW", table_range="A1")
            return len(inserts), len(updates)
        except MirrorError:
            raise
        except Exception as exc:
            log.exception("Mirror Google Sheets gagal")
            raise MirrorError(f"{exc.__class__.__name__}: {exc}") from exc
