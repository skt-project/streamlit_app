"""BigQuery sebagai source of truth transaksi.

Semua nilai dari user dikirim sebagai query parameter (tidak pernah disambung ke SQL).
Satu-satunya yang di-interpolasi adalah nama tabel dari secrets, yang sudah divalidasi
karakternya di `config.load_config`.

Upsert memakai satu pernyataan MERGE → atomik: semua baris masuk atau tidak sama sekali.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Sequence

from google.cloud import bigquery

log = logging.getLogger("store_sales.bigquery")

COLUMNS = [
    "transaction_id", "tanggal_penjualan", "store_id", "store_name", "region", "store_group",
    "sku_code", "sku_name", "sell_out_qty", "stock_qty", "supervisor_name", "evidence_file_id", "evidence_file_url",
    "evidence_file_name", "evidence_uploaded_at", "created_at", "updated_at",
]

_FQN_RE = re.compile(r"^[A-Za-z0-9_:.-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$")


class BigQueryError(Exception):
    """Operasi BigQuery gagal. Pesan aman ditampilkan ke user."""


@dataclass(frozen=True)
class TransactionRecord:
    transaction_id: str
    tanggal_penjualan: date
    store_id: str
    store_name: str
    region: str
    store_group: str
    sku_code: str
    sku_name: str
    sell_out_qty: int
    stock_qty: int
    supervisor_name: str
    evidence_file_id: str | None
    evidence_file_url: str | None
    evidence_file_name: str | None
    evidence_uploaded_at: datetime | None
    created_at: datetime            # dipakai hanya saat INSERT; UPDATE mempertahankan created_at lama
    updated_at: datetime

    @property
    def has_new_evidence(self) -> bool:
        return self.evidence_file_id is not None


def create_table_sql(fqn: str) -> str:
    """DDL tabel (sama dengan yang ada di README)."""
    return f"""CREATE TABLE IF NOT EXISTS `{fqn}` (
  transaction_id STRING NOT NULL,
  tanggal_penjualan DATE NOT NULL,
  store_id STRING NOT NULL,
  store_name STRING,
  region STRING,
  store_group STRING,
  sku_code STRING NOT NULL,
  sku_name STRING,
  sell_out_qty INT64 NOT NULL,
  stock_qty INT64,
  supervisor_name STRING,
  evidence_file_id STRING,
  evidence_file_url STRING,
  evidence_file_name STRING,
  evidence_uploaded_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
)
PARTITION BY tanggal_penjualan
CLUSTER BY store_id, sku_code"""


def _struct_param(rec: TransactionRecord) -> bigquery.StructQueryParameter:
    S = bigquery.ScalarQueryParameter
    return bigquery.StructQueryParameter(
        "row",
        S("transaction_id", "STRING", rec.transaction_id),
        S("tanggal_penjualan", "DATE", rec.tanggal_penjualan),
        S("store_id", "STRING", rec.store_id),
        S("store_name", "STRING", rec.store_name),
        S("region", "STRING", rec.region),
        S("store_group", "STRING", rec.store_group),
        S("sku_code", "STRING", rec.sku_code),
        S("sku_name", "STRING", rec.sku_name),
        S("sell_out_qty", "INT64", rec.sell_out_qty),
        S("stock_qty", "INT64", rec.stock_qty),
        S("supervisor_name", "STRING", rec.supervisor_name),
        S("has_new_evidence", "BOOL", rec.has_new_evidence),
        S("evidence_file_id", "STRING", rec.evidence_file_id),
        S("evidence_file_url", "STRING", rec.evidence_file_url),
        S("evidence_file_name", "STRING", rec.evidence_file_name),
        S("evidence_uploaded_at", "TIMESTAMP", rec.evidence_uploaded_at),
        S("created_at", "TIMESTAMP", rec.created_at),
        S("updated_at", "TIMESTAMP", rec.updated_at),
    )


class BigQueryStore:
    def __init__(self, client: bigquery.Client, table_fqn: str):
        if not _FQN_RE.match(table_fqn):
            raise BigQueryError("Nama tabel BigQuery tidak valid.")
        self.client = client
        self.fqn = table_fqn

    # ── Query builders (dipisah agar bisa diuji tanpa koneksi) ──────────────

    def select_existing_sql(self) -> str:
        return (f"SELECT {', '.join(COLUMNS)} FROM `{self.fqn}` "
                "WHERE tanggal_penjualan = @tanggal AND store_id = @store_id ORDER BY sku_code")

    def select_by_ids_sql(self) -> str:
        return (f"SELECT {', '.join(COLUMNS)} FROM `{self.fqn}` "
                "WHERE transaction_id IN UNNEST(@ids) ORDER BY tanggal_penjualan, store_id, sku_code")

    def select_by_range_sql(self) -> str:
        return (f"SELECT {', '.join(COLUMNS)} FROM `{self.fqn}` "
                "WHERE tanggal_penjualan BETWEEN @mulai AND @selesai "
                "ORDER BY tanggal_penjualan, store_id, sku_code")

    def duplicates_sql(self) -> str:
        return (f"SELECT sku_code, COUNT(*) AS n FROM `{self.fqn}` "
                "WHERE tanggal_penjualan = @tanggal AND store_id = @store_id "
                "GROUP BY sku_code HAVING COUNT(*) > 1")

    def merge_sql(self) -> str:
        cols = ", ".join(COLUMNS)
        src_cols = ", ".join(f"S.{c}" for c in COLUMNS)
        return f"""MERGE `{self.fqn}` T
USING (SELECT * FROM UNNEST(@rows)) S
ON T.tanggal_penjualan = S.tanggal_penjualan AND T.store_id = S.store_id AND T.sku_code = S.sku_code
WHEN MATCHED THEN UPDATE SET
  store_name = S.store_name,
  region = S.region,
  store_group = S.store_group,
  sku_name = S.sku_name,
  sell_out_qty = S.sell_out_qty,
  stock_qty = S.stock_qty,
  supervisor_name = S.supervisor_name,
  evidence_file_id = IF(S.has_new_evidence, S.evidence_file_id, T.evidence_file_id),
  evidence_file_url = IF(S.has_new_evidence, S.evidence_file_url, T.evidence_file_url),
  evidence_file_name = IF(S.has_new_evidence, S.evidence_file_name, T.evidence_file_name),
  evidence_uploaded_at = IF(S.has_new_evidence, S.evidence_uploaded_at, T.evidence_uploaded_at),
  updated_at = S.updated_at
WHEN NOT MATCHED THEN INSERT ({cols})
  VALUES ({src_cols})"""

    # ── Operasi ─────────────────────────────────────────────────────────────

    def _run(self, sql: str, params: Sequence[Any]) -> list[dict]:
        try:
            job = self.client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=list(params)))
            return [dict(row.items()) for row in job.result()]
        except Exception as exc:
            log.exception("BigQuery query gagal")
            raise BigQueryError(f"BigQuery gagal: {exc.__class__.__name__}: {exc}") from exc

    def fetch_existing(self, sales_date: date, store_id: str) -> list[dict]:
        return self._run(self.select_existing_sql(), [
            bigquery.ScalarQueryParameter("tanggal", "DATE", sales_date),
            bigquery.ScalarQueryParameter("store_id", "STRING", store_id),
        ])

    def fetch_by_date_range(self, start: date, end: date) -> list[dict]:
        """Seluruh transaksi pada rentang tanggal penjualan, batas ikut dihitung (untuk tab Admin)."""
        return self._run(self.select_by_range_sql(), [
            bigquery.ScalarQueryParameter("mulai", "DATE", start),
            bigquery.ScalarQueryParameter("selesai", "DATE", end),
        ])

    def fetch_by_ids(self, transaction_ids: Sequence[str]) -> list[dict]:
        return self._run(self.select_by_ids_sql(), [
            bigquery.ArrayQueryParameter("ids", "STRING", list(transaction_ids)),
        ])

    def find_duplicate_keys(self, sales_date: date, store_id: str) -> list[str]:
        """SKU yang punya >1 baris untuk tanggal + store (BigQuery tidak punya unique constraint)."""
        rows = self._run(self.duplicates_sql(), [
            bigquery.ScalarQueryParameter("tanggal", "DATE", sales_date),
            bigquery.ScalarQueryParameter("store_id", "STRING", store_id),
        ])
        return [r["sku_code"] for r in rows]

    def upsert(self, records: Sequence[TransactionRecord]) -> None:
        """MERGE semua record dalam satu statement atomik."""
        if not records:
            return
        keys = {(r.tanggal_penjualan, r.store_id, r.sku_code) for r in records}
        if len(keys) != len(records):
            raise BigQueryError("Duplikat key (tanggal + store + SKU) dalam satu submit.")
        self._run(self.merge_sql(), [
            bigquery.ArrayQueryParameter("rows", "STRUCT", [_struct_param(r) for r in records]),
        ])
