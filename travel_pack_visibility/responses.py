"""Tab log hasil (format sama dengan log upload SKP JBP/JCP): satu baris per submit, tidak pernah diubah
atau dihapus.

13 kolom A–M (HEADER_SPEC). Tab dibuat otomatis bila belum ada: header tebal abu-abu, dibekukan, lebar
kolom, dan catatan (note) penjelasan di tiap sel header. Kolom di kanan M boleh ditambah tim (mis. kolom
cek/approval); aplikasi hanya menulis A–M.

Semua nilai ditulis RAW (isian user tidak pernah dijalankan sebagai formula). Setelah itu sel `GCS Path`
yang baru ditulis diubah menjadi =HYPERLINK(...) agar bisa diklik; teks yang tampil tetap gs://...
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from gspread.exceptions import WorksheetNotFound
from gspread.utils import a1_to_rowcol, rowcol_to_a1

from evidence import object_url

log = logging.getLogger("travel_pack.responses")

STATUS_FIRST = "Uploaded"
STATUS_AGAIN = "Upload ulang"

# (judul kolom, lebar px, catatan di sel header)
HEADER_SPEC: list[tuple[str, int, str]] = [
    ("Timestamp", 150, "Waktu submit (WIB), diisi otomatis oleh aplikasi. Jangan ubah judul kolom A–M: "
                       "aplikasi berhenti menulis bila berubah. Kolom tambahan boleh di kanan M."),
    ("Nama ASM", 170, "ASM yang submit (dipilih di aplikasi; daftar nama dari daftar toko Travel Pack)."),
    ("KODE ST TOKO", 115, "Kode toko dari daftar toko Travel Pack Distribution."),
    ("NAMA TOKO", 230, "Nama toko dari daftar toko saat submit."),
    ("Region", 130, "Region toko dari daftar toko (kosong bila daftar toko tidak punya kolom Region)."),
    ("NAMA SPV", 160, "Diisi ASM di aplikasi; awalnya terisi dari daftar toko bila ada."),
    ("Ach SKU (MOQ)", 105, "Nilai Ach SKU (MOQ) kolom K daftar toko saat submit. Hanya toko dengan nilai >= 4 "
                           "yang bisa submit."),
    ("Nama File", 280, "Nama file foto di bucket."),
    ("GCS Path", 340, "Lokasi foto (gs://bucket/...). Klik untuk membuka; perlu akses Storage Object Viewer "
                      "pada bucket."),
    ("Status", 110, f"{STATUS_FIRST} = foto pertama untuk toko ini. {STATUS_AGAIN} = toko ini sudah pernah "
                    "upload; foto lama tetap tersimpan."),
    ("Waktu Foto (EXIF)", 150, "Waktu pengambilan dari metadata foto, bila ada. Hanya info, tidak dipakai "
                               "menolak foto."),
    ("Lokasi Foto (EXIF)", 175, "Koordinat dari metadata foto (lat, lon), bila ada. Banyak HP membuang data "
                                "ini; bukti utama tetap cap GPS yang tercetak di foto."),
    ("Submission ID", 270, "ID unik per submit (juga tersimpan di metadata file foto)."),
]
OUTPUT_HEADERS: list[str] = [h for h, _, _ in HEADER_SPEC]
LAST_COL = chr(ord("A") + len(OUTPUT_HEADERS) - 1)          # 13 kolom: A..M
HEADER_BG = {"red": 0.851, "green": 0.851, "blue": 0.851}   # abu-abu #D9D9D9
_CODE_COL = OUTPUT_HEADERS.index("KODE ST TOKO")
_TS_COL = OUTPUT_HEADERS.index("Timestamp")
_GCS_COL = OUTPUT_HEADERS.index("GCS Path")


class ResponseSheetError(Exception):
    """Tab hasil tidak bisa dibaca / ditulis. Pesan aman ditampilkan ke user."""


@dataclass(frozen=True)
class UploadStatus:
    count: int
    last_at: str


def row_values(record: Mapping[str, Any]) -> list[str]:
    return ["" if record.get(h) is None else str(record.get(h)) for h in OUTPUT_HEADERS]


def build_index(values: Sequence[Sequence[Any]]) -> dict[str, UploadStatus]:
    """Baris tab hasil -> {KODE TOKO: jumlah upload + timestamp terakhir}. Baris 1 = header."""
    out: dict[str, UploadStatus] = {}
    for row in values[1:]:
        code = str(row[_CODE_COL]).strip().upper() if len(row) > _CODE_COL else ""
        if not code:
            continue
        ts = str(row[_TS_COL]).strip() if len(row) > _TS_COL else ""
        prev = out.get(code)
        if prev is None:
            out[code] = UploadStatus(1, ts)
        else:
            out[code] = UploadStatus(prev.count + 1, max(prev.last_at, ts))
    return out


def hyperlink_formula(gs_uri: str) -> str | None:
    """gs://bucket/objek -> =HYPERLINK("https://storage.cloud.google.com/...","gs://bucket/objek")."""
    text = str(gs_uri or "").strip()
    if not text.startswith("gs://") or "/" not in text[5:]:
        return None
    bucket, _, name = text[5:].partition("/")
    if not bucket or not name:
        return None
    url = object_url(bucket, name).replace('"', '""')
    return f'=HYPERLINK("{url}","{text.replace(chr(34), chr(34) * 2)}")'


def header_format_requests(sheet_id: int) -> list[dict]:
    """Permintaan batchUpdate: header tebal abu-abu + dibekukan + lebar kolom + catatan per sel header."""
    n = len(HEADER_SPEC)
    reqs: list[dict] = [
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0,
                      "endColumnIndex": n},
            "cell": {"userEnteredFormat": {"backgroundColor": HEADER_BG, "textFormat": {"bold": True},
                                           "verticalAlignment": "MIDDLE", "wrapStrategy": "WRAP"}},
            "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment,wrapStrategy)"}},
        {"updateSheetProperties": {"properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                                   "fields": "gridProperties.frozenRowCount"}},
        {"updateCells": {"start": {"sheetId": sheet_id, "rowIndex": 0, "columnIndex": 0},
                         "rows": [{"values": [{"note": note} for _, _, note in HEADER_SPEC]}],
                         "fields": "note"}},
    ]
    for i, (_, width, _) in enumerate(HEADER_SPEC):
        reqs.append({"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": width}, "fields": "pixelSize"}})
    return reqs


def first_appended_row(response: Any) -> int | None:
    """Baris pertama yang ditulis append_rows ("'Tab'!A3:M3" -> 3), atau None."""
    try:
        updated = response["updates"]["updatedRange"]
        return a1_to_rowcol(updated.split("!")[-1].split(":")[0])[0]
    except Exception:
        return None


class ResponseSheet:
    def __init__(self, spreadsheet: Any | Callable[[], Any], worksheet_name: str):
        """`spreadsheet`: objek gspread Spreadsheet, atau fungsi yang membukanya (lazy)."""
        self._spreadsheet = spreadsheet
        self.worksheet_name = worksheet_name

    @property
    def spreadsheet(self):
        if callable(self._spreadsheet):
            try:
                self._spreadsheet = self._spreadsheet()
            except Exception as exc:
                raise ResponseSheetError("Spreadsheet hasil tidak dapat dibuka. Pastikan sudah di-share ke "
                                         f"service account sebagai Editor. ({exc.__class__.__name__})") from exc
        return self._spreadsheet

    def _write_header(self, ws) -> None:
        ws.update(range_name=f"A1:{LAST_COL}1", values=[OUTPUT_HEADERS], value_input_option="RAW")
        try:
            self.spreadsheet.batch_update({"requests": header_format_requests(ws.id)})
        except Exception:                      # tampilan saja; data tetap aman
            log.warning("Gagal memformat header tab hasil", exc_info=True)
            try:
                ws.freeze(rows=1)
            except Exception:
                pass

    def _worksheet(self, create: bool):
        try:
            ws = self.spreadsheet.worksheet(self.worksheet_name)
        except WorksheetNotFound:
            if not create:
                return None
            ws = self.spreadsheet.add_worksheet(title=self.worksheet_name, rows=1000, cols=len(OUTPUT_HEADERS))
            self._write_header(ws)
            return ws
        header = [str(h).strip() for h in ws.row_values(1)]
        if not any(header):
            if create:
                self._write_header(ws)
        elif header[:len(OUTPUT_HEADERS)] != OUTPUT_HEADERS:
            raise ResponseSheetError(
                f"Header tab '{self.worksheet_name}' berubah; tidak ada data yang ditulis. Kolom A-{LAST_COL} "
                "harus persis: " + " | ".join(OUTPUT_HEADERS))
        return ws

    def append(self, record: Mapping[str, Any]) -> None:
        row = row_values(record)
        try:
            ws = self._worksheet(create=True)
            response = ws.append_rows([row], value_input_option="RAW", insert_data_option="INSERT_ROWS",
                                      table_range=f"A1:{LAST_COL}1")
        except ResponseSheetError:
            raise
        except Exception as exc:
            log.exception("Gagal menulis tab hasil")
            raise ResponseSheetError(f"Gagal menulis ke spreadsheet hasil ({exc.__class__.__name__}).") from exc
        self._link_gcs_path(ws, response, row[_GCS_COL])

    @staticmethod
    def _link_gcs_path(ws, response: Any, gs_uri: str) -> None:
        """Sel GCS Path baris baru -> =HYPERLINK(...). Best effort: bila gagal, sel tetap berisi teks gs://."""
        formula = hyperlink_formula(gs_uri)
        sheet_row = first_appended_row(response)
        if not formula or not sheet_row:
            return
        try:
            ws.update(range_name=rowcol_to_a1(sheet_row, _GCS_COL + 1), values=[[formula]],
                      value_input_option="USER_ENTERED")
        except Exception as exc:
            log.warning("GCS Path tidak bisa dijadikan link: %s", exc.__class__.__name__)

    def upload_index(self) -> dict[str, UploadStatus]:
        try:
            ws = self._worksheet(create=False)
            if ws is None:
                return {}
            return build_index(ws.get(f"A:{LAST_COL}") or [])
        except ResponseSheetError:
            raise
        except Exception as exc:
            log.exception("Gagal membaca tab hasil")
            raise ResponseSheetError(f"Gagal membaca status upload ({exc.__class__.__name__}).") from exc
