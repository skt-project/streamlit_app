from datetime import date, datetime, timezone

import pytest

from gcs_evidence import EvidenceFile, EvidenceStorageError
from service import SalesSubmitter, SubmissionFailed, SubmissionRejected, plan_actions
from validation import FinalLine, LineInput, Store, make_transaction_id, validate_submission

D = date(2026, 10, 23)
T0 = datetime(2026, 10, 23, 1, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 10, 23, 5, 0, tzinfo=timezone.utc)
STORE = Store("S1", "Toko Satu", "JATENG", "GT", "Non BA")


class FakeBQ:
    def __init__(self, fail_upsert=False, fail_fetch=False):
        self.rows = {}                      # (date, store, sku) -> dict
        self.fail_upsert, self.fail_fetch = fail_upsert, fail_fetch
        self.upsert_calls = 0
        self.touched = 0

    def fetch_existing(self, d, store_id):
        self.touched += 1
        if self.fail_fetch:
            raise RuntimeError("bq read down")
        return [r for k, r in self.rows.items() if k[0] == d and k[1] == store_id]

    def upsert(self, records):
        self.touched += 1
        self.upsert_calls += 1
        if self.fail_upsert:
            raise RuntimeError("merge failed")
        for r in records:                  # meniru MERGE
            key = (r.tanggal_penjualan, r.store_id, r.sku_code)
            old = self.rows.get(key)
            row = dict(transaction_id=r.transaction_id, tanggal_penjualan=r.tanggal_penjualan, store_id=r.store_id,
                       store_name=r.store_name, region=r.region, store_group=r.store_group, sku_code=r.sku_code,
                       sku_name=r.sku_name, sell_out_qty=r.sell_out_qty, stock_qty=r.stock_qty,
                       supervisor_name=r.supervisor_name,
                       evidence_file_id=r.evidence_file_id, evidence_file_url=r.evidence_file_url,
                       evidence_file_name=r.evidence_file_name, evidence_uploaded_at=r.evidence_uploaded_at,
                       created_at=r.created_at, updated_at=r.updated_at)
            if old:
                row["created_at"] = old["created_at"]
                if not r.has_new_evidence:
                    for c in ("evidence_file_id", "evidence_file_url", "evidence_file_name", "evidence_uploaded_at"):
                        row[c] = old[c]
            self.rows[key] = row

    def fetch_by_date_range(self, start, end):
        return [r for k, r in sorted(self.rows.items(), key=lambda kv: kv[0]) if start <= k[0] <= end]

    def fetch_by_ids(self, ids):
        return [r for r in self.rows.values() if r["transaction_id"] in ids]

    def find_duplicate_keys(self, d, store_id):
        return []


class FakeStorage:
    """Storage palsu. Sengaja TIDAK punya trash/delete: service tidak boleh menghapus evidence."""

    def __init__(self, fail_on=None):
        self.uploaded, self.fail_on, self.n = [], fail_on, 0

    def upload(self, **kw):
        if kw["sku_code"] == self.fail_on:
            raise EvidenceStorageError("akses ditolak")
        self.n += 1
        self.uploaded.append(kw)
        return EvidenceFile(f"F{self.n}", f"https://store/F{self.n}", f"n{self.n}.jpg", T1)

    def view_url(self, file_id, minutes=60):
        return f"https://signed.example/{file_id}"

    def read_bytes(self, file_id):
        return b"jpg-bytes"


FakeDrive = FakeStorage     # nama lama dipakai test lain


class FakeMirror:
    def __init__(self, fail=False):
        self.fail, self.calls = fail, []

    def upsert(self, rows):
        if self.fail:
            raise RuntimeError("sheets 500")
        self.calls.append(list(rows))


def line(sku, qty, photo=None, stock=10):
    return FinalLine(1, sku, "Name " + sku, qty, stock, photo)


def submitter(bq=None, storage=None, mirror=None, now=T1):
    bq, storage, mirror = bq or FakeBQ(), storage or FakeStorage(), mirror or FakeMirror()
    return SalesSubmitter(bq, storage, mirror, clock=lambda: now), bq, storage, mirror


def test_insert_then_update_same_key_no_duplicate_and_created_at_kept():
    bq = FakeBQ()
    s1, *_ = submitter(bq, now=T0)
    r1 = s1.submit(store=STORE, sales_date=D, supervisor_name="Budi", lines=[line("K1", 2, b"j")])
    assert (r1.inserted, r1.updated) == (1, 0)
    s2, *_ = submitter(bq, now=T1)
    r2 = s2.submit(store=STORE, sales_date=D, supervisor_name="Sari", lines=[line("K1", 7, b"j2"), line("K2", 0)])
    assert (r2.inserted, r2.updated) == (1, 1)
    assert len(bq.rows) == 2
    row = bq.rows[(D, "S1", "K1")]
    assert row["sell_out_qty"] == 7 and row["stock_qty"] == 10 and row["supervisor_name"] == "Sari"
    assert row["created_at"] == T0 and row["updated_at"] == T1
    assert row["transaction_id"] == make_transaction_id(D, "S1", "K1")


def test_new_evidence_replaces_pointer_but_old_file_is_never_deleted():
    bq, storage = FakeBQ(), FakeStorage()
    s, *_ = submitter(bq, storage)
    s.submit(store=STORE, sales_date=D, supervisor_name="B", lines=[line("K1", 1, b"a")])
    first = bq.rows[(D, "S1", "K1")]["evidence_file_id"]
    s.submit(store=STORE, sales_date=D, supervisor_name="B", lines=[line("K1", 1, b"b")])
    second = bq.rows[(D, "S1", "K1")]["evidence_file_id"]
    assert first != second and len(storage.uploaded) == 2       # kedua foto tetap ada; BigQuery menunjuk yang terbaru
    assert not hasattr(storage, "trash") and not hasattr(storage, "delete")


def test_resubmit_without_photo_keeps_existing_evidence():
    bq, storage = FakeBQ(), FakeStorage()
    s, *_ = submitter(bq, storage)
    s.submit(store=STORE, sales_date=D, supervisor_name="B", lines=[line("K1", 1, b"a"), line("K2", 1)])
    s.submit(store=STORE, sales_date=D, supervisor_name="B", lines=[line("K1", 4), line("K3", 1, b"c")])
    assert bq.rows[(D, "S1", "K1")]["evidence_file_id"] == "F1"


def test_result_reports_actions_ids_urls():
    s, bq, *_ = submitter()
    res = s.submit(store=STORE, sales_date=D, supervisor_name="B", lines=[line("K1", 1, b"a"), line("K2", 0)])
    assert [ln.action for ln in res.lines] == ["INSERT", "INSERT"]
    assert [(ln.sell_out_qty, ln.stock_qty) for ln in res.lines] == [(1, 10), (0, 10)]
    assert res.lines[0].evidence_url == "https://store/F1" and res.lines[1].evidence_url is None
    assert res.lines[0].transaction_id == make_transaction_id(D, "S1", "K1")
    assert res.bigquery_status == "OK" and res.sheets_status == "OK" and not res.warnings


def test_sheets_failure_after_bq_success_is_warning_not_failure():
    s, bq, _, _ = submitter(mirror=FakeMirror(fail=True))
    res = s.submit(store=STORE, sales_date=D, supervisor_name="B", lines=[line("K1", 1, b"a")])
    assert len(bq.rows) == 1
    assert res.bigquery_status == "OK" and res.sheets_status.startswith("GAGAL")
    assert any("BigQuery SUDAH berhasil" in w for w in res.warnings)


def test_mirror_receives_bigquery_rows_with_original_created_at():
    bq, mirror = FakeBQ(), FakeMirror()
    submitter(bq, mirror=mirror, now=T0)[0].submit(store=STORE, sales_date=D, supervisor_name="B", lines=[line("K1", 1, b"a")])
    submitter(bq, mirror=mirror, now=T1)[0].submit(store=STORE, sales_date=D, supervisor_name="B", lines=[line("K1", 2, b"a")])
    assert mirror.calls[-1][0]["created_at"] == T0 and mirror.calls[-1][0]["sell_out_qty"] == 2


def test_bigquery_failure_leaves_no_transaction_and_does_not_delete_photos():
    bq, storage, mirror = FakeBQ(fail_upsert=True), FakeStorage(), FakeMirror()
    s, *_ = submitter(bq, storage, mirror)
    with pytest.raises(SubmissionFailed, match="BigQuery"):
        s.submit(store=STORE, sales_date=D, supervisor_name="B", lines=[line("K1", 1, b"a"), line("K2", 1, b"b")])
    assert bq.rows == {} and mirror.calls == []
    assert len(storage.uploaded) == 2                           # foto yatim dibiarkan (tidak dihapus)


def test_storage_failure_aborts_before_bigquery():
    bq, storage, mirror = FakeBQ(), FakeStorage(fail_on="K2"), FakeMirror()
    s, *_ = submitter(bq, storage, mirror)
    with pytest.raises(SubmissionFailed, match="akses ditolak"):
        s.submit(store=STORE, sales_date=D, supervisor_name="B", lines=[line("K1", 1, b"a"), line("K2", 1, b"b")])
    assert bq.upsert_calls == 0 and bq.rows == {} and mirror.calls == []


def test_validation_failure_makes_zero_external_calls(stores, skus):
    """Validasi form gagal → submit tidak dipanggil; submit() sendiri juga menolak input yang melanggar aturan evidence."""
    res = validate_submission(store_id="S1", sales_date=D, supervisor_name="B", stores={"S1": STORE}, skus=skus,
                              lines=[LineInput(1, "SKU000001", "-1", "1", b"x"), LineInput(2, "SKU000002", "2", "1")])
    assert not res.ok
    s, bq, storage, mirror = submitter()
    with pytest.raises(SubmissionRejected):
        s.submit(store=STORE, sales_date=D, supervisor_name="B", lines=[line("K1", 3)])     # tanpa foto
    with pytest.raises(SubmissionRejected):
        s.submit(store=STORE, sales_date=D, supervisor_name="B", lines=[])
    assert (bq.touched, storage.uploaded, mirror.calls) == (0, [], [])


def test_photo_is_mandatory_even_when_all_quantities_are_zero():
    s, bq, storage, mirror = submitter()
    with pytest.raises(SubmissionRejected, match="Foto evidence wajib"):
        s.submit(store=STORE, sales_date=D, supervisor_name="B", lines=[line("K1", 0, stock=0), line("K2", 0, stock=0)])
    assert bq.touched == 0 and mirror.calls == []
    res = s.submit(store=STORE, sales_date=D, supervisor_name="B",
                   lines=[line("K1", 0, b"p", stock=0), line("K2", 0, stock=0)])
    assert res.inserted == 2 and len(storage.uploaded) == 1


def test_plan_actions():
    existing = [{"sku_code": "K1"}]
    assert plan_actions([line("K1", 1), line("K2", 1)], existing) == {"K1": "UPDATE", "K2": "INSERT"}
