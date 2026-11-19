from datetime import datetime, timezone

import pytest
from google.api_core.exceptions import Forbidden

from conftest import FakeSpreadsheet, FakeStorageClient, FakeWorksheet, make_jpeg
from evidence import GcsEvidence, object_url
from responses import OUTPUT_HEADERS, ResponseSheet, hyperlink_formula
from service import SubmitFailed, SubmitRejected, Submitter
from stores import parse_store_list

AT = datetime(2026, 11, 19, 3, 5, 9, tzinfo=timezone.utc)      # 10:05:09 WIB


@pytest.fixture
def env(store_values):
    storage = FakeStorageClient()
    sh = FakeSpreadsheet()
    evidence = GcsEvidence(storage, "bkt", "Travel Pack Visibility BCD Q4'26")
    responses = ResponseSheet(sh, "TRAVEL_PACK_UPLOAD_LOG")
    sl = parse_store_list(store_values, ach_index=10, ach_column="K", min_ach=4, worksheet="Data")
    return {"submitter": Submitter(evidence, responses, clock=lambda: AT), "bucket": storage.bucket("bkt"),
            "sheet": sh, "sl": sl}


def submit(env, **kw):
    args = dict(store_list=env["sl"], asm="Asep Contoh", store_code="ST001", spv=" Sari  Spv ",
                photo=make_jpeg(gps=(-6.2, 106.8), taken="2026:11:19 09:59:00"))
    args.update(kw)
    return env["submitter"].submit(**args)


def test_success_uploads_photo_then_appends_row(env):
    res = submit(env)
    objs = env["bucket"].objects
    assert len(objs) == 1
    name, obj = next(iter(objs.items()))
    assert name.startswith("Travel Pack Visibility BCD Q4'26/Asep Contoh/ST001 - Toko Satu - 20261119_100509_")
    assert name.endswith(".jpg") and obj["content_type"] == "image/jpeg" and obj["if_generation_match"] == 0
    assert obj["metadata"]["store_code"] == "ST001" and obj["metadata"]["spv"] == "Sari Spv"
    row = dict(zip(OUTPUT_HEADERS, env["sheet"].sheets["TRAVEL_PACK_UPLOAD_LOG"].values[1]))
    assert row["Timestamp"] == "2026-11-19 10:05:09"
    assert row["Nama ASM"] == "Asep Contoh" and row["KODE ST TOKO"] == "ST001" and row["NAMA TOKO"] == "Toko Satu"
    assert row["NAMA SPV"] == "Sari Spv"
    assert row["Ach SKU (MOQ)"] == "4" and row["Region"] == "Region Satu"
    assert row["Nama File"] == name.rsplit("/", 1)[-1] and row["Status"] == "Uploaded"
    assert row["GCS Path"] == hyperlink_formula(f"gs://bkt/{name}")
    assert object_url("bkt", name) in row["GCS Path"]
    assert row["Waktu Foto (EXIF)"] == "2026-11-19 09:59:00" and row["Lokasi Foto (EXIF)"].startswith("-6.2")
    assert row["Submission ID"] == res.submission_id and res.photo.gs_uri == f"gs://bkt/{name}"
    assert res.submitted_at == "2026-11-19 10:05:09"


def test_store_asm_spelling_from_sheet_is_used(env):
    submit(env, asm="ASEP CONTOH", store_code="st004")
    row = env["sheet"].sheets["TRAVEL_PACK_UPLOAD_LOG"].values[1]
    assert row[1] == "asep contoh" and row[2] == "ST004"


def test_png_keeps_png_extension(env):
    submit(env, photo=make_jpeg(fmt="PNG"))
    name = next(iter(env["bucket"].objects))
    assert name.endswith(".png")


def test_two_submits_make_two_objects_and_rows(env):
    submit(env)
    submit(env)
    assert len(env["bucket"].objects) == 2
    rows = env["sheet"].sheets["TRAVEL_PACK_UPLOAD_LOG"].values
    assert len(rows) == 3
    status = OUTPUT_HEADERS.index("Status")
    assert [rows[1][status], rows[2][status]] == ["Uploaded", "Upload ulang"]
    submit(env, store_code="ST003")                 # toko lain tetap "Uploaded"
    assert env["sheet"].sheets["TRAVEL_PACK_UPLOAD_LOG"].values[3][status] == "Uploaded"


@pytest.mark.parametrize("kw,msg", [
    ({"asm": ""}, "Pilih Nama ASM"),
    ({"store_code": "NOPE"}, "Pilih toko"),
    ({"store_code": "ST010"}, "bukan toko ASM"),
    ({"store_code": "ST002"}, ">= 4"),
    ({"photo": None}, "wajib"),
    ({"photo": b"xx"}, "bukan foto"),
])
def test_rejections_store_nothing(env, kw, msg):
    with pytest.raises(SubmitRejected, match=msg):
        submit(env, **kw)
    assert env["bucket"].objects == {} and env["sheet"].sheets == {}


def test_gcs_failure_writes_no_row(env):
    env["bucket"].fail = Forbidden("denied")
    with pytest.raises(SubmitFailed, match="ditolak") as ei:
        submit(env)
    assert ei.value.photo_saved is False and env["sheet"].sheets == {}


def test_changed_header_stops_before_photo_upload(env):
    bad = list(OUTPUT_HEADERS)
    bad[0] = "Waktu"
    env["sheet"].sheets["TRAVEL_PACK_UPLOAD_LOG"] = FakeWorksheet("TRAVEL_PACK_UPLOAD_LOG", [bad])
    with pytest.raises(SubmitFailed, match="Foto belum dikirim") as ei:
        submit(env)
    assert ei.value.photo_saved is False and env["bucket"].objects == {}


class _AppendFails(FakeWorksheet):
    def append_rows(self, *a, **kw):
        raise RuntimeError("quota")


def test_sheet_failure_after_upload_keeps_photo_and_says_so(env):
    env["sheet"].sheets["TRAVEL_PACK_UPLOAD_LOG"] = _AppendFails("TRAVEL_PACK_UPLOAD_LOG", [OUTPUT_HEADERS])
    with pytest.raises(SubmitFailed, match="submit lagi") as ei:
        submit(env)
    assert ei.value.photo_saved is True and len(env["bucket"].objects) == 1
