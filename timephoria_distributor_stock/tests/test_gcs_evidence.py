from datetime import date, datetime, timedelta, timezone

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
        self._bucket = bucket

    def bucket(self, name):
        assert name == "bkt"
        return self._bucket


def make(error=None):
    bucket = FakeBucket(error)
    return bucket, GcsEvidence(FakeClient(bucket), "bkt", "Timephoria Distributor Stock Evidence")


def upload(store, **kwargs):
    args = dict(
        data=b"jpg", tanggal_stock=date(2026, 10, 23), distributor_code="D000001",
        distributor_name="Gudang Contoh", sku_code="TST000001", sku_name="Produk Contoh",
        supervisor_name="Ani Contoh", transaction_id="tx1",
    )
    args.update(kwargs)
    return store.upload(**args)


def test_object_path_metadata_and_no_overwrite():
    bucket, store = make()
    evidence = upload(store)
    folder = "Timephoria Distributor Stock Evidence/2026-10/D000001 - Gudang Contoh/"
    assert evidence.file_id.startswith(folder + "2026-10-23_D000001_TST000001_tx1_")
    assert evidence.file_id.endswith(".jpg")
    assert evidence.url == object_url("bkt", evidence.file_id)
    assert evidence.url.startswith("https://storage.cloud.google.com/bkt/Timephoria%20Distributor%20Stock%20Evidence/")
    name, content_type, generation, meta = bucket.calls[0]
    assert content_type == "image/jpeg" and generation == 0
    assert meta["distributor_code"] == "D000001" and meta["sku_code"] == "TST000001"
    assert meta["tanggal_stock"] == "2026-10-23" and meta["uploaded_at"].endswith(" WIB")
    assert meta["transaction_id"] == "tx1"
    for attr in ("trash", "delete", "remove"):
        assert not hasattr(store, attr)


def test_collision_rejected_and_later_upload_is_a_new_object(monkeypatch):
    times = iter([
        datetime(2026, 10, 23, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 10, 23, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 10, 23, 1, 0, 5, tzinfo=timezone.utc),
    ])

    class FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return next(times)

    monkeypatch.setattr(gcs_evidence, "datetime", FakeDT)
    bucket, store = make()
    first = upload(store, data=b"original")
    assert "20261023080000" in first.file_id
    with pytest.raises(EvidenceStorageError, match="sudah ada"):
        upload(store, data=b"new")
    assert bucket.objects[first.file_id] == b"original"
    second = upload(store)
    assert second.file_id != first.file_id and len(bucket.objects) == 2


@pytest.mark.parametrize("err,msg", [(Forbidden("no"), "Object Creator"), (NotFound("x"), "tidak ditemukan")])
def test_errors_are_explained(err, msg):
    _, store = make(err)
    with pytest.raises(EvidenceStorageError, match=msg):
        upload(store)


def test_safe_name_removes_separators_and_limits_length():
    assert "/" not in safe_name("a/b\\c") and "\\" not in safe_name("a/b\\c")
    assert safe_name("  ") == "_"
    assert len(safe_name("a" * 400)) == 150
    assert safe_name("a\x00b") == "ab"


def test_signed_url_v4_and_fallback_none():
    bucket, store = make()
    url = store.view_url("a/b c.jpg", minutes=60)
    assert url.startswith("https://signed/a/b c.jpg")
    assert bucket.signed == [("a/b c.jpg", "v4", timedelta(minutes=60), "GET")]
    bucket.sign_error = AttributeError("you need a private key to sign credentials")
    assert store.view_url("x.jpg") is None
    bucket.objects["x.jpg"] = b"data"
    assert store.read_bytes("x.jpg") == b"data"
