from datetime import date, datetime, timezone

import pytest
from google.cloud import bigquery

from bigquery_store import COLUMNS, BigQueryError, BigQueryStore, TransactionRecord, create_table_sql

FQN = "proj.gt_schema.store_sales_transaction"
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


def rec(sku="K1", evidence=True, store_name=EVIL):
    return TransactionRecord(
        transaction_id="tx-" + sku, tanggal_penjualan=date(2026, 10, 23), store_id="S1", store_name=store_name,
        region="R", store_group="G", sku_code=sku, sku_name="Name", sell_out_qty=3, stock_qty=11, supervisor_name=EVIL,
        evidence_file_id="fid" if evidence else None, evidence_file_url="url" if evidence else None,
        evidence_file_name="n.jpg" if evidence else None, evidence_uploaded_at=NOW if evidence else None,
        created_at=NOW, updated_at=NOW)


def test_invalid_table_name_rejected():
    with pytest.raises(BigQueryError):
        BigQueryStore(FakeClient(), "proj.ds.t`; DROP")


def test_select_uses_parameters_only():
    c = FakeClient()
    BigQueryStore(c, FQN).fetch_existing(date(2026, 10, 23), EVIL)
    sql, cfg = c.calls[0]
    assert EVIL not in sql and "DROP" not in sql
    params = {p.name: p.value for p in cfg.query_parameters}
    assert params == {"tanggal": date(2026, 10, 23), "store_id": EVIL}
    assert "@tanggal" in sql and "@store_id" in sql


def test_merge_is_parameterized_with_struct_array_and_never_embeds_input():
    c = FakeClient()
    BigQueryStore(c, FQN).upsert([rec("K1"), rec("K2", evidence=False)])
    sql, cfg = c.calls[0]
    assert sql.startswith("MERGE `proj.gt_schema.store_sales_transaction` T")
    assert EVIL not in sql and "K1" not in sql and "S1" not in sql
    (param,) = cfg.query_parameters
    assert isinstance(param, bigquery.ArrayQueryParameter) and param.name == "rows" and len(param.values) == 2
    first = dict(param.values[0].struct_values)
    assert first["supervisor_name"] == EVIL and first["sell_out_qty"] == 3 and first["stock_qty"] == 11 \
        and first["has_new_evidence"] is True


def test_merge_semantics_in_sql():
    sql = BigQueryStore(FakeClient(), FQN).merge_sql()
    assert "ON T.tanggal_penjualan = S.tanggal_penjualan AND T.store_id = S.store_id AND T.sku_code = S.sku_code" in sql
    update = sql.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
    assert "created_at" not in update and "transaction_id" not in update      # dipertahankan saat UPDATE
    assert "updated_at = S.updated_at" in update
    assert "sell_out_qty = S.sell_out_qty" in update and "stock_qty = S.stock_qty" in update
    assert "supervisor_name = S.supervisor_name" in update
    assert "IF(S.has_new_evidence, S.evidence_file_id, T.evidence_file_id)" in update
    insert = sql.split("WHEN NOT MATCHED THEN INSERT")[1]
    for col in COLUMNS:
        assert col in insert


def test_duplicate_keys_in_one_submit_rejected_before_query():
    c = FakeClient()
    with pytest.raises(BigQueryError):
        BigQueryStore(c, FQN).upsert([rec("K1"), rec("K1")])
    assert c.calls == []


def test_fetch_by_ids_and_duplicates_use_params():
    c = FakeClient([Row(sku_code="K9", n=2)])
    st = BigQueryStore(c, FQN)
    assert st.find_duplicate_keys(date(2026, 10, 23), "S1") == ["K9"]
    st.fetch_by_ids(["a", EVIL])
    sql, cfg = c.calls[1]
    assert EVIL not in sql and cfg.query_parameters[0].values == ["a", EVIL]


def test_fetch_by_date_is_parameterized_and_scoped_to_one_date():
    c = FakeClient()
    BigQueryStore(c, FQN).fetch_by_date(date(2026, 10, 23))
    sql, cfg = c.calls[0]
    assert "WHERE tanggal_penjualan = @tanggal" in sql and "2026-10-23" not in sql
    assert {p.name: p.value for p in cfg.query_parameters} == {"tanggal": date(2026, 10, 23)}


def test_query_failure_wrapped():
    class Boom(FakeClient):
        def query(self, *a, **k):
            raise RuntimeError("quota")
    with pytest.raises(BigQueryError, match="quota"):
        BigQueryStore(Boom(), FQN).fetch_existing(date(2026, 10, 23), "S1")


def test_create_table_sql_has_all_columns_and_types():
    ddl = create_table_sql(FQN)
    for col in COLUMNS:
        assert f"  {col} " in ddl
    assert "sell_out_qty INT64 NOT NULL" in ddl and "stock_qty INT64" in ddl and "tanggal_penjualan DATE" in ddl and "updated_at TIMESTAMP" in ddl
