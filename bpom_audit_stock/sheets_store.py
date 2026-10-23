"""Penyimpanan hasil stock opname ke satu Google Spreadsheet utama (satu worksheet).

Penulisan dikirim sebagai SATU request `spreadsheets.batchUpdate`, atomik di sisi Google:
kalau gagal, tidak ada baris yang masuk sebagian.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import gspread

from validation import ExcelRow, normalize_name, row_key

HEADERS: list[str] = [
    "Distributor Company",
    "Distributor Name",
    "Periode Audit",
    "Tanggal Audit",
    "Nama PIC Distributor",
    "PRODUCT ID",
    "PRODUCT NAME",
    "BPOM NUMBER",
    "QTY",
    "created_date",
    "created_by",
    "updated_date",
    "updated_by",
    "ISI (GR / ML)",
]
_LEGACY_HEADERS = HEADERS[:-1]   # 13 kolom, sebelum kolom ISI ditambahkan di ujung
_IDX = {name: i for i, name in enumerate(HEADERS)}
_WIB = timezone(timedelta(hours=7))  # Indonesia tidak memakai DST


class SheetsError(Exception):
    """Masalah yang bisa dijelaskan ke user (skema sheet salah, dsb)."""


class DuplicateDataError(SheetsError):
    """Input ditolak karena kombinasi Distributor Name + PRODUCT ID + BPOM NUMBER sudah ada pada periode ini."""

    def __init__(self, conflicts: list[ExcelRow], periode: str):
        self.conflicts = conflicts
        super().__init__(
            f"Data duplikat: {len(conflicts)} baris di file sudah pernah dikirim "
            f"(periode {periode}). "
            "Tidak ada data yang disimpan. Revisi tidak tersedia di form ini; jika ada kesalahan data, "
            "hubungi BD."
        )


@dataclass(frozen=True)
class OpnameMeta:
    """Metadata dari form Streamlit, ikut disimpan di setiap baris."""
    company: str
    tanggal: str            # YYYY-MM-DD
    pic: str

    @property
    def periode(self) -> str:
        """Periode (YYYY-MM) diturunkan dari tanggal stock opname."""
        return self.tanggal[:7]


@dataclass(frozen=True)
class SaveSummary:
    inserted: int
    additional: int = 0
    distributors: int = 0


def now_str() -> str:
    return datetime.now(_WIB).strftime("%Y-%m-%d %H:%M:%S")


# ── Spreadsheet / worksheet ────────────────────────────────────────────────

def open_spreadsheet(
    gc: gspread.Client,
    title: str,
    *,
    sheet_id: str | None = None,
    share_with: tuple[str, ...] = (),
) -> tuple[gspread.Spreadsheet, bool]:
    """Buka spreadsheet utama. Mengembalikan (spreadsheet, baru_dibuat).

    Urutan: `sheet_id` bila diset, lalu cari berdasarkan judul; bila tidak ada, buat satu yang baru.
    """
    try:
        if sheet_id:
            return gc.open_by_key(sheet_id), False
        return gc.open(title), False
    except gspread.SpreadsheetNotFound:
        if sheet_id:
            raise SheetsError(f"Spreadsheet dengan id '{sheet_id}' tidak ditemukan atau belum di-share "
                              "ke service account.") from None
    try:
        sh = gc.create(title)
    except gspread.exceptions.APIError as exc:
        raise SheetsError(
            f"Spreadsheet '{title}' tidak ditemukan dan service account tidak bisa membuat yang baru ({exc}). "
            "Service account tidak punya kuota Drive, jadi spreadsheet harus dibuat manual, di-share sebagai Editor "
            "ke service account, lalu ID-nya diisi sebagai `google_sheet_id` di bagian `[app]` pada Secrets."
        ) from None
    for email in share_with:
        sh.share(email, perm_type="user", role="writer", notify=False)
    return sh, True


def _check_header(ws: gspread.Worksheet, current: list[str]) -> None:
    current = list(current)
    while current and not str(current[-1]).strip():
        current.pop()
    if current != HEADERS:
        raise SheetsError(
            f"Header worksheet '{ws.title}' tidak sesuai skema, sehingga tidak ditimpa. "
            f"Diharapkan: {HEADERS}. Ditemukan: {current}."
        )


def ensure_worksheet(sh: gspread.Spreadsheet, title: str, *, fresh: bool = False) -> gspread.Worksheet:
    """Ambil worksheet utama; buat bila belum ada dan tulis header bila kosong.

    Header yang sudah ada tapi berbeda dari skema TIDAK ditimpa (error).
    """
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        if fresh:  # spreadsheet baru: pakai tab bawaan daripada meninggalkan "Sheet1" kosong
            ws = sh.sheet1
            ws.update_title(title)
        else:
            ws = sh.add_worksheet(title, rows=1000, cols=len(HEADERS))
    first_row = ws.row_values(1)
    if first_row == _LEGACY_HEADERS:      # tab lama 13 kolom: tambahkan kolom ISI di ujung (data tidak disentuh)
        ws.update_cell(1, len(HEADERS), HEADERS[-1])
        first_row = HEADERS
    if not any(str(c).strip() for c in first_row):
        ws.update([HEADERS], f"A1:{chr(ord('A') + len(HEADERS) - 1)}1", value_input_option="RAW")
        ws.freeze(rows=1)
    else:
        _check_header(ws, first_row)
    return ws


def _pad(row: list[str]) -> list[str]:
    return list(row) + [""] * (len(HEADERS) - len(row))


# ── Tulis ──────────────────────────────────────────────────────────────────

def _cell(value) -> dict:
    # numberValue / stringValue = nilai literal: tidak pernah dianggap rumus atau tanggal oleh Sheets.
    if isinstance(value, int) and not isinstance(value, bool):
        return {"userEnteredValue": {"numberValue": value}}
    return {"userEnteredValue": {"stringValue": str(value)}}


def _row_values(meta: OpnameMeta, row: ExcelRow, ts: str) -> list:
    return [
        meta.company, row.distributor_name, meta.periode, meta.tanggal, meta.pic,
        row.product_id, row.product_name, row.bpom_number, row.qty,
        ts, meta.pic, ts, meta.pic,
        row.isi,
    ]


def insert_rows(sh: gspread.Spreadsheet, ws: gspread.Worksheet, meta: OpnameMeta,
                rows: list[ExcelRow], *, check_duplicates: bool = True) -> SaveSummary:
    """Tambahkan semua baris dalam satu request.

    Hanya setelah file lolos validasi, data yang ada dibaca sekali untuk memastikan tidak ada baris yang
    sudah pernah mengirim kombinasi Distributor Name + PRODUCT ID + BPOM NUMBER yang sama pada periode itu; bila ada, seluruh input ditolak
    dengan `DuplicateDataError`. `check_duplicates=False` untuk spreadsheet yang baru dibuat.
    """
    if check_duplicates:
        values = ws.get_all_values()
        if values and values[0][: len(HEADERS)] != HEADERS:
            raise SheetsError(f"Header worksheet '{ws.title}' tidak sesuai skema.")
        existing = {
            (normalize_name(r[_IDX["Distributor Name"]]),
             *row_key(r[_IDX["PRODUCT ID"]], r[_IDX["BPOM NUMBER"]]))
            for r in map(_pad, values[1:])
            if r[_IDX["Periode Audit"]].strip() == meta.periode
        }
        conflicts = [r for r in rows if r.key in existing]
        if conflicts:
            raise DuplicateDataError(conflicts, meta.periode)
    ts = now_str()
    data = [{"values": [_cell(v) for v in _row_values(meta, r, ts)]} for r in rows]
    sh.batch_update({"requests": [
        {"appendCells": {"sheetId": ws.id, "rows": data, "fields": "userEnteredValue"}}
    ]})
    return SaveSummary(inserted=len(rows), additional=sum(r.additional for r in rows),
                       distributors=len({r.distributor_name for r in rows}))
