"""Penyimpanan hasil stock opname ke satu Google Spreadsheet utama (satu worksheet).

Semua penulisan dikirim sebagai SATU request `spreadsheets.batchUpdate`, yang
atomik di sisi Google: kalau gagal, tidak ada perubahan sebagian yang masuk.
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
]
_IDX = {name: i for i, name in enumerate(HEADERS)}
_WIB = timezone(timedelta(hours=7))  # Indonesia tidak memakai DST


class SheetsError(Exception):
    """Masalah yang bisa dijelaskan ke user (skema sheet salah, data lama tidak ada, dsb)."""


class DuplicateDataError(SheetsError):
    """Mode Input ditolak karena kombinasi key yang sama sudah ada (harus lewat Revisi)."""

    def __init__(self, conflicts: list[ExcelRow], distributor_name: str, periode: str):
        self.conflicts = conflicts
        super().__init__(
            f"Data duplikat: {len(conflicts)} baris di file sudah ada di spreadsheet untuk "
            f"'{distributor_name}' periode {periode}. Tidak ada data yang disimpan. "
            "Pilih mode **Revisi** pada radio button Mode untuk memperbarui data yang sudah ada."
        )


@dataclass(frozen=True)
class AuditMeta:
    """Metadata dari form Streamlit, ikut disimpan di setiap baris."""
    company: str
    distributor_name: str   # nama kanonik "Company - Branch"
    periode: str            # YYYY-MM
    tanggal_audit: str      # YYYY-MM-DD
    pic: str


@dataclass(frozen=True)
class SaveSummary:
    inserted: int
    updated: int
    duplicate_matches: int = 0   # baris lama kembar untuk satu key (ikut diupdate semua)
    spreadsheet_url: str = ""
    spreadsheet_created: bool = False


def now_str() -> str:
    return datetime.now(_WIB).strftime("%Y-%m-%d %H:%M:%S")


# ── Spreadsheet / worksheet ────────────────────────────────────────────────

def open_spreadsheet(
    gc: gspread.Client,
    title: str,
    *,
    sheet_id: str | None = None,
    create: bool = False,
    share_with: tuple[str, ...] = (),
) -> tuple[gspread.Spreadsheet | None, bool]:
    """Buka spreadsheet utama. Mengembalikan (spreadsheet, baru_dibuat).

    Urutan: `sheet_id` bila diset, lalu cari berdasarkan judul. Bila belum ada dan
    `create=True`, buat satu spreadsheet baru. `create=False` mengembalikan (None, False).
    """
    try:
        if sheet_id:
            return gc.open_by_key(sheet_id), False
        return gc.open(title), False
    except gspread.SpreadsheetNotFound:
        if sheet_id:
            raise SheetsError(f"Spreadsheet dengan id '{sheet_id}' tidak ditemukan atau belum di-share "
                              "ke service account.") from None
        if not create:
            return None, False
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
    if not any(str(c).strip() for c in first_row):
        ws.update([HEADERS], f"A1:{chr(ord('A') + len(HEADERS) - 1)}1", value_input_option="RAW")
        ws.freeze(rows=1)
    else:
        _check_header(ws, first_row)
    return ws


def _pad(row: list[str]) -> list[str]:
    return list(row) + [""] * (len(HEADERS) - len(row))


def _matches(row: list[str], distributor_name: str, periode: str) -> bool:
    return (normalize_name(row[_IDX["Distributor Name"]]) == normalize_name(distributor_name)
            and row[_IDX["Periode Audit"]].strip() == periode)


# ── Baca (hanya mode Revisi) ───────────────────────────────────────────────

def read_existing(
    gc: gspread.Client, title: str, worksheet_title: str, distributor_name: str, periode: str,
    *, sheet_id: str | None = None,
) -> list[dict[str, str]]:
    """Baca baris lama untuk Distributor Name + Periode Audit. Tidak membuat apa pun."""
    sh, _ = open_spreadsheet(gc, title, sheet_id=sheet_id, create=False)
    if sh is None:
        return []
    try:
        ws = sh.worksheet(worksheet_title)
    except gspread.WorksheetNotFound:
        return []
    values = ws.get_all_values()
    if not values:
        return []
    if [c for c in values[0]][: len(HEADERS)] != HEADERS:
        raise SheetsError(f"Header worksheet '{worksheet_title}' tidak sesuai skema.")
    return [dict(zip(HEADERS, _pad(r))) for r in values[1:]
            if _matches(_pad(r), distributor_name, periode)]


# ── Tulis ──────────────────────────────────────────────────────────────────

def _cell(value) -> dict:
    # numberValue / stringValue = nilai literal: tidak pernah dianggap rumus atau tanggal oleh Sheets.
    if isinstance(value, int) and not isinstance(value, bool):
        return {"userEnteredValue": {"numberValue": value}}
    return {"userEnteredValue": {"stringValue": str(value)}}


def _row_values(meta: AuditMeta, row: ExcelRow, created_date: str, created_by: str,
                updated_date: str, updated_by: str) -> list:
    return [
        meta.company, meta.distributor_name, meta.periode, meta.tanggal_audit, meta.pic,
        row.product_id, row.product_name, row.bpom_number, row.qty,
        created_date, created_by, updated_date, updated_by,
    ]


def _row_data(values: list) -> dict:
    return {"values": [_cell(v) for v in values]}


def insert_rows(sh: gspread.Spreadsheet, ws: gspread.Worksheet, meta: AuditMeta,
                rows: list[ExcelRow], *, check_duplicates: bool = True) -> SaveSummary:
    """Mode Input: tambahkan semua baris dalam satu request.

    Hanya setelah file lolos validasi, data yang ada dibaca sekali untuk memastikan tidak ada
    key (PRODUCT ID + BPOM NUMBER) yang sama untuk distributor + periode ini; bila ada, seluruh
    input ditolak dengan `DuplicateDataError`. `check_duplicates=False` untuk spreadsheet baru.
    """
    if check_duplicates:
        values = ws.get_all_values()
        if values and values[0][: len(HEADERS)] != HEADERS:
            raise SheetsError(f"Header worksheet '{ws.title}' tidak sesuai skema.")
        existing = {
            row_key(r[_IDX["PRODUCT ID"]], r[_IDX["BPOM NUMBER"]])
            for r in map(_pad, values[1:])
            if _matches(r, meta.distributor_name, meta.periode)
        }
        conflicts = [r for r in rows if r.key in existing]
        if conflicts:
            raise DuplicateDataError(conflicts, meta.distributor_name, meta.periode)
    ts = now_str()
    data = [_row_data(_row_values(meta, r, ts, meta.pic, ts, meta.pic)) for r in rows]
    sh.batch_update({"requests": [
        {"appendCells": {"sheetId": ws.id, "rows": data, "fields": "userEnteredValue"}}
    ]})
    return SaveSummary(inserted=len(rows), updated=0, spreadsheet_url=sh.url)


def upsert_rows(sh: gspread.Spreadsheet, ws: gspread.Worksheet, meta: AuditMeta,
                rows: list[ExcelRow]) -> SaveSummary:
    """Mode Revisi: update baris dengan key sama, insert yang baru, dalam satu request atomik.

    Key = Distributor Name + Periode Audit + PRODUCT ID + BPOM NUMBER.
    created_date / created_by baris lama dipertahankan; updated_* diperbarui.
    Baris lama yang tidak ada di file tidak dihapus.
    """
    values = ws.get_all_values()
    if not values or values[0][: len(HEADERS)] != HEADERS:
        raise SheetsError(f"Header worksheet '{ws.title}' tidak sesuai skema.")

    index: dict[tuple[str, str], list[int]] = {}
    scope_rows = 0
    for i in range(1, len(values)):
        current = _pad(values[i])
        if not _matches(current, meta.distributor_name, meta.periode):
            continue
        scope_rows += 1
        key = row_key(current[_IDX["PRODUCT ID"]], current[_IDX["BPOM NUMBER"]])
        index.setdefault(key, []).append(i)
    if scope_rows == 0:
        raise SheetsError(f"Tidak ada data lama untuk '{meta.distributor_name}' periode {meta.periode}. "
                          "Gunakan mode Input untuk data pertama.")

    ts = now_str()
    requests, new_rows = [], []
    updated = duplicate_matches = 0
    for r in rows:
        hits = index.get(r.key)
        if not hits:
            new_rows.append(_row_data(_row_values(meta, r, ts, meta.pic, ts, meta.pic)))
            continue
        duplicate_matches += len(hits) - 1
        for idx in hits:
            old = _pad(values[idx])
            new = _row_values(meta, r, old[_IDX["created_date"]], old[_IDX["created_by"]], ts, meta.pic)
            requests.append({"updateCells": {
                "start": {"sheetId": ws.id, "rowIndex": idx, "columnIndex": 0},
                "rows": [_row_data(new)],
                "fields": "userEnteredValue",
            }})
            updated += 1
    if new_rows:
        requests.append({"appendCells": {"sheetId": ws.id, "rows": new_rows, "fields": "userEnteredValue"}})

    sh.batch_update({"requests": requests})
    return SaveSummary(inserted=len(new_rows), updated=updated,
                       duplicate_matches=duplicate_matches, spreadsheet_url=sh.url)
