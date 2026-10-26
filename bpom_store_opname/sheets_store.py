"""Penyimpanan hasil stock opname ke satu Google Spreadsheet utama (satu worksheet).

Penulisan dikirim sebagai SATU request `spreadsheets.batchUpdate`, atomik di sisi Google:
kalau gagal, tidak ada baris yang masuk sebagian.

Dirancang untuk banyak pengguna bersamaan (kuota Sheets API: 60 baca + 60 tulis per menit per service account):
- `OpnameStore` dibuat SEKALI per proses (spreadsheet/worksheet/header diperiksa sekali, bukan tiap submit);
- cek duplikat memakai indeks di memori (dimuat dengan SATU `batchGet` 4 kolom, diperbarui tiap 2 menit dan setiap
  penulisan sendiri) - tidak membaca seluruh sheet pada setiap submit;
- penulisan digabung (group commit): submit yang datang bersamaan dikirim dalam SATU `batchUpdate` oleh satu thread
  penulis, diberi jarak di bawah kuota, dan 429 dicoba ulang dengan backoff. 100 submit serentak = beberapa request,
  bukan 100;
- satu lock per (store, periode) mencegah dua submit store yang sama lolos bersamaan.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import gspread

from validation import ExcelRow, normalize_name, row_key

HEADERS: list[str] = [
    "Region",
    "Nama Toko (By Group)",
    "Periode Stock Opname",
    "Tanggal Stock Opname",
    "Nama PIC",
    "PRODUCT ID",
    "PRODUCT NAME",
    "ISI (GR / ML)",
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
    """Masalah yang bisa dijelaskan ke user (skema sheet salah, dsb)."""


class DuplicateDataError(SheetsError):
    """Input ditolak karena kombinasi Region + Nama Toko + PRODUCT ID + BPOM NUMBER sudah ada pada periode ini."""

    def __init__(self, conflicts: list[ExcelRow], periode: str):
        self.conflicts = conflicts
        super().__init__(
            f"Data duplikat: {len(conflicts)} baris di file sudah pernah dikirim untuk toko ini "
            f"(periode {periode}). "
            "Tidak ada data yang disimpan. Revisi tidak tersedia di form ini; jika ada kesalahan data, "
            "hubungi BD."
        )


def _toko_key(region: str, toko: str) -> str:
    return f"{normalize_name(region)}|{normalize_name(toko)}"


@dataclass(frozen=True)
class OpnameMeta:
    """Metadata dari form Streamlit, ikut disimpan di setiap baris."""
    region: str
    toko: str               # nama toko (By Group)
    tanggal: str            # YYYY-MM-DD
    pic: str

    @property
    def periode(self) -> str:
        """Periode (YYYY-MM) diturunkan dari tanggal stock opname."""
        return self.tanggal[:7]

    @property
    def key(self) -> str:
        return _toko_key(self.region, self.toko)


@dataclass(frozen=True)
class SaveSummary:
    inserted: int
    additional: int = 0
    distributors: int = 0


def now_str() -> str:
    return datetime.now(_WIB).strftime("%Y-%m-%d %H:%M:%S")


# ── Spreadsheet / worksheet ────────────────────────────────────────────────

def open_spreadsheet(gc: gspread.Client, sheet_id: str | None) -> gspread.Spreadsheet:
    """Buka spreadsheet output HANYA lewat ID (`open_by_key`).

    Sengaja tidak ada pencarian berdasarkan judul, pembuatan spreadsheet baru, atau share lewat Drive: aplikasi ini tidak boleh
    berdampak di luar spreadsheet output yang ditunjuk `google_sheet_id`.
    """
    if not sheet_id:
        raise SheetsError("`google_sheet_id` belum diatur di Secrets (bagian `[app]`), jadi hasil tidak bisa disimpan.")
    try:
        return gc.open_by_key(sheet_id)
    except (gspread.SpreadsheetNotFound, PermissionError):     # gspread: HTTP 403 -> PermissionError
        raise SheetsError(f"Spreadsheet dengan id '{sheet_id}' tidak ditemukan atau service account belum di-share "
                          "sebagai Editor.") from None


def _check_header(ws: gspread.Worksheet, current: list[str]) -> None:
    current = list(current)
    while current and not str(current[-1]).strip():
        current.pop()
    if current != HEADERS:
        raise SheetsError(
            f"Header worksheet '{ws.title}' tidak sesuai skema, sehingga tidak ditimpa. "
            f"Diharapkan: {HEADERS}. Ditemukan: {current}."
        )


def ensure_worksheet(sh: gspread.Spreadsheet, title: str) -> gspread.Worksheet:
    """Ambil worksheet output; satu-satunya perubahan selain penambahan baris data, SEMUA di dalam spreadsheet output:
    membuat tab `title` bila belum ada, melebarkan kolom tab yang terlalu sempit, dan menulis header (+ membekukan baris 1)
    bila baris 1 kosong. Header yang sudah ada tapi berbeda dari skema TIDAK ditimpa (error).
    """
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title, rows=1000, cols=len(HEADERS))
    first_row = ws.row_values(1)
    if not any(str(c).strip() for c in first_row):
        if ws.col_count < len(HEADERS):   # batas kolom grid tab: lebarkan sebelum menulis header
            ws.add_cols(len(HEADERS) - ws.col_count)
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
        meta.region, row.toko_name, meta.periode, meta.tanggal, meta.pic,
        row.product_id, row.product_name, row.isi, row.bpom_number, row.qty,
        ts, meta.pic, ts, meta.pic,
    ]


class _Pacer:
    """Beri jarak minimal antar permintaan (aman antar-thread) supaya tidak melewati kuota per menit."""

    def __init__(self, interval: float, clock=time.monotonic, sleep=time.sleep):
        self.interval, self._clock, self._sleep = interval, clock, sleep
        self._lock, self._next = threading.Lock(), 0.0

    def wait(self) -> None:
        with self._lock:
            now = self._clock()
            start = max(now, self._next)
            self._next = start + self.interval
        if start > now:
            self._sleep(start - now)


def _status(exc: gspread.exceptions.APIError) -> int:
    try:
        return int(exc.response.status_code)
    except Exception:
        return 0


def _retry(call, *, retry_on: tuple[int, ...], delays: tuple[float, ...], sleep=time.sleep):
    """Jalankan `call`; ulangi bila status HTTP ada di `retry_on` (backoff). Error lain langsung dilempar."""
    for delay in (*delays, None):
        try:
            return call()
        except gspread.exceptions.APIError as exc:
            if delay is None or _status(exc) not in retry_on:
                raise
            sleep(delay)


# Penulisan: HANYA 429 yang aman diulang (permintaan ditolak sebelum dijalankan). 5xx/timeout bisa berarti
# sudah tertulis, jadi tidak diulang otomatis.
_WRITE_RETRY, _READ_RETRY = (429,), (429, 500, 502, 503, 504)
_BACKOFF = (2.0, 4.0, 8.0, 16.0, 30.0)
INDEX_TTL = 120.0            # detik; indeks duplikat dimuat ulang dari sheet bila lebih tua dari ini
WRITE_INTERVAL = 1.1         # detik antar penulisan (~54/menit, di bawah kuota 60/menit)
_KEY_COLUMNS = ("Region", "Nama Toko (By Group)", "Periode Stock Opname", "PRODUCT ID", "BPOM NUMBER")


class OpnameStore:
    """Handle tunggal ke worksheet utama; aman dipakai banyak thread (satu instance per proses)."""

    def __init__(self, sh: gspread.Spreadsheet, ws: gspread.Worksheet, *, assume_empty: bool = False,
                 pacer: _Pacer | None = None, index_ttl: float = INDEX_TTL, clock=time.monotonic,
                 sleep=time.sleep):
        self.sh, self.ws = sh, ws
        self._pacer = pacer or _Pacer(WRITE_INTERVAL, clock, sleep)
        self._ttl, self._clock, self._sleep = index_ttl, clock, sleep
        self._index: dict[tuple[str, str], set[tuple[str, str]]] = {}
        self._loaded_at = clock() if assume_empty else None     # spreadsheet baru: tidak ada yang perlu dibaca
        self._index_lock = threading.Lock()
        self._key_locks: dict[tuple[str, str], threading.Lock] = {}
        self._key_locks_guard = threading.Lock()
        self._pending: list[dict] = []
        self._cv = threading.Condition()
        self._writer: threading.Thread | None = None

    # -- indeks duplikat -----------------------------------------------------
    def _load_index(self) -> None:
        ranges = [f"{chr(ord('A') + _IDX[h])}2:{chr(ord('A') + _IDX[h])}" for h in _KEY_COLUMNS]
        columns = _retry(lambda: self.ws.batch_get(ranges), retry_on=_READ_RETRY, delays=_BACKOFF, sleep=self._sleep)

        def cell(col, i):
            return str(col[i][0]).strip() if i < len(col) and col[i] else ""

        index: dict[tuple[str, str], set[tuple[str, str]]] = {}
        n = max((len(c) for c in columns), default=0)
        for i in range(n):
            region, toko, periode, pid, bpom = (cell(c, i) for c in columns)
            if region or toko or periode or pid or bpom:
                index.setdefault((_toko_key(region, toko), periode), set()).add(row_key(pid, bpom))
        self._index, self._loaded_at = index, self._clock()

    def _ensure_index(self) -> None:
        with self._index_lock:      # satu thread memuat; yang lain menunggu lalu memakai hasilnya
            if self._loaded_at is None or self._clock() - self._loaded_at > self._ttl:
                self._load_index()

    def invalidate(self) -> None:
        """Paksa baca ulang dari sheet pada pengecekan berikutnya (mis. setelah penulisan yang hasilnya tidak pasti)."""
        with self._index_lock:
            self._loaded_at = None

    def _key_lock(self, key: tuple[str, str]) -> threading.Lock:
        with self._key_locks_guard:
            return self._key_locks.setdefault(key, threading.Lock())

    # -- tulis (group commit) ------------------------------------------------
    def _writer_loop(self) -> None:
        while True:
            with self._cv:
                while not self._pending:
                    self._cv.wait()
            self._pacer.wait()                 # selama menunggu jatah, submit lain menumpuk dan ikut batch berikutnya
            with self._cv:
                batch, self._pending = self._pending, []
            body = {"requests": [item["request"] for item in batch]}
            error = None
            try:
                _retry(lambda: self.sh.batch_update(body), retry_on=_WRITE_RETRY, delays=_BACKOFF, sleep=self._sleep)
            except BaseException as exc:       # noqa: BLE001 - diteruskan ke setiap submit di batch ini
                error = exc
            for item in batch:
                item["error"] = error
                item["done"].set()

    def _commit(self, request: dict) -> None:
        """Antrikan satu permintaan appendCells dan tunggu sampai tertulis (atau gagal). Satu thread penulis per instance."""
        item = {"request": request, "done": threading.Event(), "error": None}
        with self._cv:
            if self._writer is None:
                self._writer = threading.Thread(target=self._writer_loop, name="sheets-writer", daemon=True)
                self._writer.start()
            self._pending.append(item)
            self._cv.notify()
        item["done"].wait()
        if item["error"] is not None:
            raise item["error"]

    def insert_rows(self, meta: "OpnameMeta", rows: list[ExcelRow]) -> "SaveSummary":
        """Cek duplikat (indeks) lalu tambahkan semua baris dalam satu request atomik.

        Bila Region + Nama Toko + periode + PRODUCT ID + BPOM NUMBER sudah ada -> `DuplicateDataError`, tidak ada yang ditulis.
        """
        key = (meta.key, meta.periode)
        with self._key_lock(key):
            self._ensure_index()
            existing = self._index.get(key, set())
            conflicts = [r for r in rows if row_key(r.product_id, r.bpom_number) in existing]
            if conflicts:
                raise DuplicateDataError(conflicts, meta.periode)
            ts = now_str()
            data = [{"values": [_cell(v) for v in _row_values(meta, r, ts)]} for r in rows]
            request = {"appendCells": {"sheetId": self.ws.id, "rows": data, "fields": "userEnteredValue"}}
            try:
                self._commit(request)
            except Exception:
                self.invalidate()   # hasil tidak pasti: baca ulang sheet sebelum submit berikutnya
                raise
            with self._index_lock:
                self._index.setdefault(key, set()).update(row_key(r.product_id, r.bpom_number) for r in rows)
        return SaveSummary(inserted=len(rows), additional=sum(r.additional for r in rows), distributors=1)
