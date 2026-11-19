"""Fake Google Sheets / GCS (tanpa jaringan) + data contoh. Semua nama & kode toko di sini FIKTIF."""
from __future__ import annotations

import io
from fractions import Fraction

import pytest
from google.api_core.exceptions import PreconditionFailed
from gspread.exceptions import WorksheetNotFound
from gspread.utils import a1_to_rowcol
from PIL import Image

# Header di baris 3 (baris 1-2 judul), kolom K = Ach SKU (MOQ).
STORE_VALUES = [
    ["TRAVEL PACK DISTRIBUTION Q4'26"],
    [""],
    ["No", "Region", "ASM", "SPV", "Kode ST Toko", "Nama Toko", "SKU A", "SKU B", "SKU C", "SKU D",
     "Ach SKU (MOQ)", "Remark"],
    ["1", "Region Satu", "Asep Contoh", "Sari Spv", "ST001", "Toko Satu", "1", "1", "1", "1", "4", ""],
    ["2", "Region Satu", "Asep Contoh", "Sari Spv", "ST002", "Toko Dua", "1", "1", "1", "0", "3", ""],
    ["3", "Region Satu", "Asep Contoh", "", "st003", "Toko Tiga", "1", "1", "1", "1", "5", ""],
    ["4", "Region Satu", " asep  contoh ", "Sari Spv", "ST004", "Toko Empat", "", "", "", "", "4,0", ""],
    ["5", "Region Dua", "Budi Contoh", "Rina Spv", "ST005", "Toko Lima", "", "", "", "", "#N/A", ""],
    ["6", "Region Dua", "Budi Contoh", "Rina Spv", "ST006", "Toko Enam", "", "", "", "", "", ""],
    ["7", "Region Dua", "Budi Contoh", "Rina Spv", "ST007", "Toko Tujuh", "", "", "", "", "80%", ""],
    ["8", "Region Dua", "", "Rina Spv", "ST008", "Toko Delapan", "", "", "", "", "6", ""],
    ["9", "Region Satu", "Asep Contoh", "Sari Spv", "ST001", "Toko Satu Dobel", "", "", "", "", "9", ""],
    ["10", "Region Dua", "Budi Contoh", "Rina Spv", "ST009", "Toko Sembilan", "", "", "", "", "3.99", ""],
    ["11", "Region Dua", "Budi Contoh", "Rina Spv", "ST010", "Toko Sepuluh", "", "", "", "", "4.00", ""],
    ["12", "Region Tiga", "Cici Contoh", "Tono Spv", "ST011", "Toko Sebelas", "", "", "", "", "2", ""],
    [""],
]


class FakeWorksheet:
    _next_id = 100

    def __init__(self, title, values=None):
        self.title = title
        self.values = [list(r) for r in (values or [])]
        self.frozen = None
        self.calls = []
        self.fail_link = False
        FakeWorksheet._next_id += 1
        self.id = FakeWorksheet._next_id

    def get_all_values(self):
        return [list(r) for r in self.values]

    def row_values(self, n):
        return list(self.values[n - 1]) if len(self.values) >= n else []

    def update(self, range_name, values, value_input_option=None):
        """Tulis grid nilai mulai dari sel kiri-atas range (A1 / I5 / A1:M1)."""
        self.calls.append(("update", range_name, value_input_option))
        if value_input_option == "USER_ENTERED" and self.fail_link:
            raise RuntimeError("link gagal")
        row, col = a1_to_rowcol(range_name.split(":")[0])
        for r_off, vals in enumerate(values):
            while len(self.values) < row + r_off:
                self.values.append([])
            target = self.values[row + r_off - 1]
            for c_off, v in enumerate(vals):
                while len(target) < col + c_off:
                    target.append("")
                target[col + c_off - 1] = v

    def freeze(self, rows=None, cols=None):
        self.frozen = rows

    def append_rows(self, rows, value_input_option=None, insert_data_option=None, table_range=None):
        self.calls.append(("append", value_input_option, insert_data_option, table_range))
        first = len(self.values) + 1
        for r in rows:
            self.values.append(list(r))
        return {"updates": {"updatedRange": f"'{self.title}'!A{first}:M{len(self.values)}"}}

    def get(self, range_name):
        self.calls.append(("get", range_name))
        return [list(r) for r in self.values]


class FakeSpreadsheet:
    def __init__(self, sheets=None):
        self.sheets = {ws.title: ws for ws in (sheets or [])}
        self.fail_append = False
        self.fail_format = False
        self.batch_requests = []

    def batch_update(self, body):
        if self.fail_format:
            raise RuntimeError("format gagal")
        self.batch_requests.extend(body["requests"])
        return {}

    def worksheet(self, title):
        if title not in self.sheets:
            raise WorksheetNotFound(title)
        return self.sheets[title]

    def worksheets(self):
        return list(self.sheets.values())

    def add_worksheet(self, title, rows, cols):
        ws = FakeWorksheet(title)
        self.sheets[title] = ws
        return ws


class FakeGC:
    def __init__(self, books):
        self.books = books

    def open_by_key(self, key):
        if key not in self.books:
            raise PermissionError(key)
        return self.books[key]


class FakeBlob:
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name
        self.metadata = None

    def upload_from_string(self, data, content_type=None, if_generation_match=None):
        if self.bucket.fail:
            raise self.bucket.fail
        if if_generation_match == 0 and self.name in self.bucket.objects:
            raise PreconditionFailed("exists")
        self.bucket.objects[self.name] = {"data": data, "content_type": content_type, "metadata": self.metadata,
                                          "if_generation_match": if_generation_match}


class FakeBucket:
    def __init__(self):
        self.objects = {}
        self.fail = None

    def blob(self, name):
        return FakeBlob(self, name)


class FakeStorageClient:
    def __init__(self):
        self.buckets = {}

    def bucket(self, name):
        return self.buckets.setdefault(name, FakeBucket())


def make_jpeg(size=(320, 200), gps=None, taken=None, fmt="JPEG") -> bytes:
    img = Image.new("RGB", size, (30, 120, 200))
    buf = io.BytesIO()
    kwargs = {}
    if gps or taken:
        exif = Image.Exif()
        if taken:
            exif.get_ifd(0x8769)[36867] = taken
        if gps:
            lat, lon = gps

            def dms(v):
                v = abs(v)
                d = int(v)
                m = int((v - d) * 60)
                s = Fraction(round(((v - d) * 60 - m) * 60 * 1000), 1000)
                return (Fraction(d), Fraction(m), s)

            exif.get_ifd(0x8825).update({1: "S" if lat < 0 else "N", 2: dms(lat),
                                         3: "W" if lon < 0 else "E", 4: dms(lon)})
        kwargs["exif"] = exif.tobytes()
    img.save(buf, format=fmt, **kwargs)
    return buf.getvalue()


@pytest.fixture
def store_values():
    return [list(r) for r in STORE_VALUES]
