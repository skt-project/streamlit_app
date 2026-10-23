from datetime import date, datetime, timezone

import pytest
from google.api_core.exceptions import Forbidden, NotFound, PreconditionFailed

import gcs_evidence
from gcs_evidence import EvidenceStorageError, GcsEvidence, object_url, safe_name


class FakeBlob:
    def __init__(self, bucket, name):
        self.bucket, self.name, self.metadata = bucket, name, None

    def generate_signed_url(self, version=None, expiration=None, method=None):
        if self.bucket.sign_error:
            raise self.bucket.sign_error
        self.bucket.signed.append((self.name, version, expiration, method))
        return f"https://signed/{self.name}?X-Goog-Signature=abc"

    def download_as_bytes(self):
        if self.name not in self.bucket.objects:
            raise NotFound("x")
        return self.bucket.objects[self.name]

    def upload_from_string(self, data, content_type=None, if_generation_match=None):
        self.bucket.calls.append((self.name, content_type, if_generation_match, self.metadata))
        if self.bucket.error:
            raise self.bucket.error
        if if_generation_match == 0 and self.name in self.bucket.objects:
            raise PreconditionFailed("exists")
        self.bucket.objects[self.name] = data


class FakeBucket:
    def __init__(self, error=None):
        self.objects, self.calls, self.error = {}, [], error
        self.signed, self.sign_error = [], None

    def blob(self, name):
        return FakeBlob(self, name)


class FakeClient:
    def __init__(self, bucket):
        self._b = bucket

    def bucket(self, name):
        assert name == "bkt"
        return self._b


def make(error=None):
    b = FakeBucket(error)
    return b, GcsEvidence(FakeClient(b), "bkt", "Store Sales Evidence")


def up(g, **kw):
    args = dict(data=b"jpg", sales_date=date(2026, 10, 23), store_id="ST000001",
                store_name="Toko Contoh 3", sku_code="SKU000001", sku_name="Cream",
                supervisor_name="Budi", transaction_id="tx1")
    args.update(kw)
    return g.upload(**args)


def test_object_path_name_url_and_content_type():
    b, g = make()
    ev = up(g)
    folder = "Store Sales Evidence/2026-10/ST000001 - Toko Contoh 3/"
    assert ev.file_id.startswith(folder + "2026-10-23_ST000001_SKU000001_tx1_") and ev.file_id.endswith(".jpg")
    assert ev.name == ev.file_id[len(folder):]
    assert ev.url == object_url("bkt", ev.file_id)
    assert ev.url.startswith("https://storage.cloud.google.com/bkt/Store%20Sales%20Evidence/2026-10/")
    assert b.calls[0][1] == "image/jpeg"


def test_metadata_has_all_evidence_fields_in_wib():
    b, g = make()
    up(g)
    md = b.calls[0][3]
    assert md["store_id"] == "ST000001" and md["store_name"] == "Toko Contoh 3"
    assert md["sku_code"] == "SKU000001" and md["sku_name"] == "Cream" and md["transaction_id"] == "tx1"
    assert md["tanggal_penjualan"] == "2026-10-23" and md["supervisor_name"] == "Budi"
    assert md["uploaded_at"].endswith(" WIB")


def test_upload_never_overwrites_and_class_has_no_delete_api():
    b, g = make()
    up(g)
    assert b.calls[0][2] == 0                            # if_generation_match=0 → tidak boleh menimpa
    for name in ("trash", "delete", "remove"):
        assert not hasattr(g, name)


def test_name_collision_is_rejected_not_overwritten(monkeypatch):
    fixed = datetime(2026, 10, 23, 1, 0, 0, tzinfo=timezone.utc)

    class FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed
    monkeypatch.setattr(gcs_evidence, "datetime", FakeDT)
    b, g = make()
    first = up(g, data=b"original")
    with pytest.raises(EvidenceStorageError, match="tidak menimpa"):
        up(g, data=b"new")                               # detik yang sama → nama sama → ditolak
    assert b.objects[first.file_id] == b"original"


def test_two_uploads_for_same_key_get_distinct_objects(monkeypatch):
    times = iter([datetime(2026, 10, 23, 1, 0, 0, tzinfo=timezone.utc),
                  datetime(2026, 10, 23, 1, 0, 5, tzinfo=timezone.utc)])

    class FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return next(times)
    monkeypatch.setattr(gcs_evidence, "datetime", FakeDT)
    b, g = make()
    assert up(g).file_id != up(g).file_id and len(b.objects) == 2


@pytest.mark.parametrize("err,msg", [(Forbidden("no"), "Object Creator"), (NotFound("x"), "tidak ditemukan")])
def test_errors_are_explained(err, msg):
    b, g = make(err)
    with pytest.raises(EvidenceStorageError, match=msg):
        up(g)


def test_safe_name_strips_separators():
    assert "/" not in safe_name("a/b\\c") and safe_name("  ") == "_"


def test_view_url_is_v4_get_with_expiry():
    from datetime import timedelta
    b, g = make()
    url = g.view_url("a/b c.jpg", minutes=30)
    assert url.startswith("https://signed/a/b c.jpg")
    assert b.signed == [("a/b c.jpg", "v4", timedelta(minutes=30), "GET")]


def test_view_url_returns_none_when_credentials_cannot_sign():
    b, g = make()
    b.sign_error = AttributeError("you need a private key to sign credentials")
    assert g.view_url("x.jpg") is None


def test_read_bytes_and_still_no_delete_api():
    b, g = make()
    b.objects["x.jpg"] = b"data"
    assert g.read_bytes("x.jpg") == b"data"
    with pytest.raises(EvidenceStorageError, match="tidak ditemukan"):
        g.read_bytes("missing.jpg")
    for name in ("trash", "delete", "remove"):
        assert not hasattr(g, name)
