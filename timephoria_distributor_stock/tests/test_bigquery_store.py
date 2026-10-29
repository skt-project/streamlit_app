from datetime import date, datetime, timezone

import pytest
from google.cloud import bigquery

from bigquery_store import COLUMNS, BigQueryError, BigQueryStore, TransactionRecord, create_table_sql

FQN = "proj.gt_schema.timephoria_smart_coverage_tester_feedback"
NOW = datetime(2026, 10, 23, 4, 0, tzinfo=timezone.utc)
EVIL = "x'; DROP TABLE t; --"


class FakeJob:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class FakeClient:
    def __init__(self, rows=()):
        self.calls = []
        self.rows = list(rows)

    def query(self, sql, job_config=None):
        self.calls.append((sql, job_config))
        return FakeJob(self.rows)


class Row(dict):
    pass


def rec(sku="K1", evidence=True):
    return TransactionRecord(
        transaction_id="tx-" + sku, tanggal_stock=date(2026, 10, 23), distributor_code=EVIL,
        distributor_name=EVIL, region="R", supervisor_name=EVIL, spu_code=sku, spu_name="Name",
        stock_qty=11, evidence_file_id="fid" if evidence else None,
        evidence_file_url="url" if evidence else None, evidence_file_name="n.jpg" if evidence else None,
        evidence_uploaded_at=NOW if evidence else None, created_at=NOW, updated_at=NOW,
    )


def test_invalid_table_name_rejected():
    with pytest.raises(BigQueryError):
        BigQueryStore(FakeClient(), "proj.ds.t`; DROP")


def test_queries_are_parameterized_and_evil_input_stays_out_of_sql():
    client = FakeClient()
    store = BigQueryStore(client, FQN)
    store.fetch_existing(date(2026, 10, 23), EVIL)
    store.fetch_by_date_range(date(2026, 10, 17), date(2026, 10, 23))
    store.fetch_by_ids(["a", EVIL])
    store.fetch_previous(EVIL, date(2026, 10, 23))
    store.find_duplicate_keys(date(2026, 10, 23), EVIL)
    for sql, cfg in client.calls:
        assert EVIL not in sql and "DROP" not in sql
        assert cfg is not None and cfg.query_parameters
    existing = client.calls[0][1]
    assert {p.name: p.value for p in existing.query_parameters} == {
        "tanggal": date(2026, 10, 23), "distributor_code": EVIL,
    }
    range_sql, range_cfg = client.calls[1]
    assert "tanggal_stock BETWEEN @mulai AND @selesai" in range_sql
    assert {p.name: p.value for p in range_cfg.query_parameters} == {
        "mulai": date(2026, 10, 17), "selesai": date(2026, 10, 23),
    }
    prev_sql = client.calls[3][0]
    assert "MAX(tanggal_stock)" in prev_sql and "tanggal_stock < @tanggal" in prev_sql


def test_merge_semantics_and_struct_parameter():
    client = FakeClient()
    BigQueryStore(client, FQN).upsert([rec("K1"), rec("K2", evidence=False)])
    sql, cfg = client.calls[0]
    assert sql.startswith(f"MERGE `{FQN}` T")
    assert EVIL not in sql and "K1" not in sql
    assert "ON T.tanggal_stock = S.tanggal_stock AND T.distributor_code = S.distributor_code AND T.spu_code = S.spu_code" in sql
    update = sql.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
    assert "created_at" not in update and "transaction_id" not in update
    assert "updated_at = S.updated_at" in update and "stock_qty = S.stock_qty" in update
    assert "IF(S.has_new_evidence, S.evidence_file_id, T.evidence_file_id)" in update
    insert = sql.split("WHEN NOT MATCHED THEN INSERT")[1]
    for col in COLUMNS:
        assert col in insert
    (param,) = cfg.query_parameters
    assert isinstance(param, bigquery.ArrayQueryParameter) and param.name == "rows" and len(param.values) == 2
    first = dict(param.values[0].struct_values)
    assert first["supervisor_name"] == EVIL and first["stock_qty"] == 11 and first["has_new_evidence"] is True


def test_duplicate_keys_rejected_before_query():
    client = FakeClient()
    with pytest.raises(BigQueryError):
        BigQueryStore(client, FQN).upsert([rec("K1"), rec("K1")])
    assert client.calls == []


def test_query_failure_wrapped_and_ddl_matches_columns():
    class Boom(FakeClient):
        def query(self, *args, **kwargs):
            raise RuntimeError("quota")

    with pytest.raises(BigQueryError, match="quota"):
        BigQueryStore(Boom(), FQN).fetch_existing(date(2026, 10, 23), "D1")
    ddl = create_table_sql(FQN)
    for col in COLUMNS:
        assert f"  {col} " in ddl
    assert "stock_qty INT64 NOT NULL" in ddl and "tanggal_stock DATE NOT NULL" in ddl
    assert "sell_out" not in ddl
    assert "PARTITION BY tanggal_stock" in ddl and "CLUSTER BY distributor_code, spu_code" in ddl
