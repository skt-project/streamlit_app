from datetime import date, datetime, timezone

import pytest

from gcs_evidence import EvidenceFile, EvidenceStorageError
from service import SalesSubmitter, SubmissionFailed, SubmissionRejected, plan_actions
from validation import FinalLine, make_transaction_id

D = date(2026, 10, 23)
T0 = datetime(2026, 10, 23, 1, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 10, 23, 5, 0, tzinfo=timezone.utc)


class FakeBQ:
    def __init__(self, fail_upsert=False, dups=None):
        self.rows = {}
        self.fail_upsert = fail_upsert
        self.dups = list(dups or [])
        self.upsert_calls = 0
        self.touched = 0

    def fetch_existing(self, tanggal, code):
        self.touched += 1
        return [r for k, r in self.rows.items() if k[0] == tanggal and k[1] == code]

    def upsert(self, records):
        self.touched += 1
        self.upsert_calls += 1
        if self.fail_upsert:
            raise RuntimeError("merge failed")
        for rec in records:
            key = (rec.tanggal_stock, rec.distributor_code, rec.spu_code)
            old = self.rows.get(key)
            row = dict(
                transaction_id=rec.transaction_id, tanggal_stock=rec.tanggal_stock,
                distributor_code=rec.distributor_code, distributor_name=rec.distributor_name,
                region=rec.region, supervisor_name=rec.supervisor_name,
                spu_code=rec.spu_code, spu_name=rec.spu_name, stock_qty=rec.stock_qty,
                evidence_file_id=rec.evidence_file_id, evidence_file_url=rec.evidence_file_url,
                evidence_file_name=rec.evidence_file_name, evidence_uploaded_at=rec.evidence_uploaded_at,
                created_at=rec.created_at, updated_at=rec.updated_at,
            )
            if old:
                row["created_at"] = old["created_at"]
                row["transaction_id"] = old["transaction_id"]
                if not rec.has_new_evidence:
                    for col in ("evidence_file_id", "evidence_file_url", "evidence_file_name", "evidence_uploaded_at"):
                        row[col] = old[col]
            self.rows[key] = row

    def fetch_by_date_range(self, start, end):
        return [r for k, r in sorted(self.rows.items(), key=lambda kv: kv[0]) if start <= k[0] <= end]

    def fetch_by_ids(self, ids):
        return [r for r in self.rows.values() if r["transaction_id"] in ids]

    def fetch_previous(self, code, tanggal):
        dates = [k[0] for k in self.rows if k[1] == code and k[0] < tanggal]
        if not dates:
            return []
        prev = max(dates)
        return [r for k, r in self.rows.items() if k[0] == prev and k[1] == code]

    def find_duplicate_keys(self, tanggal, code):
        return list(self.dups)


class FakeStorage:
    def __init__(self, fail_on=None):
        self.uploaded, self.fail_on, self.n = [], fail_on, 0

    def upload(self, **kw):
        if kw["spu_code"] == self.fail_on:
            raise EvidenceStorageError("akses ditolak")
        self.n += 1
        self.uploaded.append(kw)
        return EvidenceFile(f"F{self.n}", f"https://store/F{self.n}", f"n{self.n}.jpg", T1)

    def view_url(self, file_id, minutes=60):
        return f"https://signed.example/{file_id}"

    def read_bytes(self, file_id):
        return b"jpg-bytes"


class FakeMirror:
    def __init__(self, fail=False):
        self.fail, self.calls = fail, []

    def upsert(self, rows):
        if self.fail:
            raise RuntimeError("sheets 500")
        self.calls.append(list(rows))


def stock_line(sku, qty, photo=None):
    return FinalLine(1, sku, "Name " + sku, qty, photo)


def submitter(bq=None, storage=None, mirror=None, now=T1):
    bq, storage, mirror = bq or FakeBQ(), storage or FakeStorage(), mirror or FakeMirror()
    return SalesSubmitter(bq, storage, mirror, clock=lambda: now), bq, storage, mirror


def test_insert_then_update_keeps_created_at_and_old_evidence_without_new_photo(distributors):
    bq = FakeBQ()
    dist = distributors["D1"]
    s1, *_ = submitter(bq, now=T0)
    first = s1.submit(distributor=dist, tanggal_stock=D, supervisor_name="Ani", lines=[stock_line("K1", 2, b"j")])
    assert (first.inserted, first.updated) == (1, 0)
    s2, *_ = submitter(bq, now=T1)
    second = s2.submit(
        distributor=dist, tanggal_stock=D, supervisor_name="Budi",
        lines=[stock_line("K1", 7), stock_line("K2", 0, b"j2")],
    )
    assert (second.inserted, second.updated) == (1, 1)
    row = bq.rows[(D, "D1", "K1")]
    assert row["stock_qty"] == 7 and row["supervisor_name"] == "Budi"
    assert row["created_at"] == T0 and row["updated_at"] == T1
    assert row["evidence_file_id"] == "F1"
    assert row["transaction_id"] == make_transaction_id(D, "D1", "K1")
    assert second.lines[0].evidence_url == "https://store/F1"


def test_zero_total_without_photo_is_saved_and_positive_without_photo_is_not(distributors):
    dist = distributors["D1"]
    s, bq, storage, mirror = submitter()
    with pytest.raises(SubmissionRejected, match="Foto evidence wajib"):
        s.submit(distributor=dist, tanggal_stock=D, supervisor_name="Ani", lines=[stock_line("K1", 3)])
    with pytest.raises(SubmissionRejected):
        s.submit(distributor=dist, tanggal_stock=D, supervisor_name="Ani", lines=[])
    assert (bq.touched, storage.uploaded, mirror.calls) == (0, [], [])
    saved = s.submit(distributor=dist, tanggal_stock=D, supervisor_name="Ani",
                     lines=[stock_line("K1", 0), stock_line("K2", 0)])
    assert saved.inserted == 2 and storage.uploaded == [] and len(bq.rows) == 2


def test_storage_failure_writes_nothing_and_bq_failure_keeps_orphan_photos(distributors):
    dist = distributors["D1"]
    bq, storage, mirror = FakeBQ(), FakeStorage(fail_on="K2"), FakeMirror()
    s, *_ = submitter(bq, storage, mirror)
    with pytest.raises(SubmissionFailed, match="akses ditolak"):
        s.submit(distributor=dist, tanggal_stock=D, supervisor_name="Ani",
                 lines=[stock_line("K1", 1, b"a"), stock_line("K2", 1, b"b")])
    assert bq.upsert_calls == 0 and bq.rows == {} and len(storage.uploaded) == 1
    assert not hasattr(storage, "delete") and not hasattr(storage, "trash") and not hasattr(storage, "remove")

    bq, storage, mirror = FakeBQ(fail_upsert=True), FakeStorage(), FakeMirror()
    s, *_ = submitter(bq, storage, mirror)
    with pytest.raises(SubmissionFailed, match="BigQuery"):
        s.submit(distributor=dist, tanggal_stock=D, supervisor_name="Ani",
                 lines=[stock_line("K1", 1, b"a"), stock_line("K2", 1, b"b")])
    assert bq.rows == {} and mirror.calls == [] and len(storage.uploaded) == 2


def test_sheets_failure_is_a_warning_after_bigquery_success(distributors):
    dist = distributors["D1"]
    bq, mirror = FakeBQ(), FakeMirror(fail=True)
    s, *_ = submitter(bq, mirror=mirror)
    res = s.submit(distributor=dist, tanggal_stock=D, supervisor_name="Ani", lines=[stock_line("K1", 1, b"a")])
    assert len(bq.rows) == 1 and res.bigquery_status == "OK" and res.sheets_status.startswith("GAGAL")
    assert any("BigQuery SUDAH berhasil" in w for w in res.warnings)


def test_mirror_receives_preserved_created_at(distributors):
    dist = distributors["D1"]
    bq, mirror = FakeBQ(), FakeMirror()
    submitter(bq, mirror=mirror, now=T0)[0].submit(
        distributor=dist, tanggal_stock=D, supervisor_name="Ani", lines=[stock_line("K1", 1, b"a")])
    submitter(bq, mirror=mirror, now=T1)[0].submit(
        distributor=dist, tanggal_stock=D, supervisor_name="Ani", lines=[stock_line("K1", 4, b"b")])
    assert mirror.calls[-1][0]["created_at"] == T0 and mirror.calls[-1][0]["stock_qty"] == 4


def test_plan_actions_and_duplicate_warning(distributors):
    assert plan_actions([stock_line("K1", 1)], [{"spu_code": "K1"}]) == {"K1": "UPDATE"}
    bq = FakeBQ(dups=["K1"])
    res = submitter(bq)[0].submit(
        distributor=distributors["D1"], tanggal_stock=D, supervisor_name="Ani", lines=[stock_line("K1", 0)])
    assert any("K1" in w for w in res.warnings)
