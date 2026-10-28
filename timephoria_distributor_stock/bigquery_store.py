"""BigQuery sebagai source of truth stok distributor.

Semua nilai dari user dikirim sebagai query parameter. Satu-satunya yang di-interpolasi
adalah nama tabel dari secrets, yang sudah divalidasi karakternya.

Upsert memakai satu pernyataan MERGE → atomik: semua baris masuk atau tidak sama sekali.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Sequence

from google.cloud import bigquery

log = logging.getLogger("timephoria.bigquery")

COLUMNS = [
    "transaction_id", "tanggal_stock", "distributor_code", "distributor_name", "region",
    "supervisor_name", "sku_code", "sku_name", "stock_qty", "evidence_file_id", "evidence_file_url",
    "evidence_file_name", "evidence_uploaded_at", "created_at", "updated_at",
]

_FQN_RE = re.compile(r"^[A-Za-z0-9_:.-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$")


class BigQueryError(Exception):
    """Operasi BigQuery gagal. Pesan aman ditampilkan ke user."""


@dataclass(frozen=True)
class TransactionRecord:
    transaction_id: str
    tanggal_stock: date
    distributor_code: str
    distributor_name: str
    region: str
    supervisor_name: str
    sku_code: str
    sku_name: str
    stock_qty: int
    evidence_file_id: str | None
    evidence_file_url: str | None
    evidence_file_name: str | None
    evidence_uploaded_at: datetime | None
    created_at: datetime
    updated_at: datetime

    @property
    def has_new_evidence(self) -> bool:
        return self.evidence_file_id is not None


def create_table_sql(fqn: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS `{fqn}` (
  transaction_id STRING NOT NULL,
  tanggal_stock DATE NOT NULL,
  distributor_code STRING NOT NULL,
  distributor_name STRING,
  region STRING,
  supervisor_name STRING,
  sku_code STRING NOT NULL,
  sku_name STRING,
  stock_qty INT64 NOT NULL,
  evidence_file_id STRING,
  evidence_file_url STRING,
  evidence_file_name STRING,
  evidence_uploaded_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
)
PARTITION BY tanggal_stock
CLUSTER BY distributor_code, sku_code"""


def _struct_param(rec: TransactionRecord) -> bigquery.StructQueryParameter:
    scalar = bigquery.ScalarQueryParameter
    return bigquery.StructQueryParameter(
        "row",
        scalar("transaction_id", "STRING", rec.transaction_id),
        scalar("tanggal_stock", "DATE", rec.tanggal_stock),
        scalar("distributor_code", "STRING", rec.distributor_code),
        scalar("distributor_name", "STRING", rec.distributor_name),
        scalar("region", "STRING", rec.region),
        scalar("supervisor_name", "STRING", rec.supervisor_name),
        scalar("sku_code", "STRING", rec.sku_code),
        scalar("sku_name", "STRING", rec.sku_name),
        scalar("stock_qty", "INT64", rec.stock_qty),
        scalar("has_new_evidence", "BOOL", rec.has_new_evidence),
        scalar("evidence_file_id", "STRING", rec.evidence_file_id),
        scalar("evidence_file_url", "STRING", rec.evidence_file_url),
        scalar("evidence_file_name", "STRING", rec.evidence_file_name),
        scalar("evidence_uploaded_at", "TIMESTAMP", rec.evidence_uploaded_at),
        scalar("created_at", "TIMESTAMP", rec.created_at),
        scalar("updated_at", "TIMESTAMP", rec.updated_at),
    )


class BigQueryStore:
    def __init__(self, client: bigquery.Client, table_fqn: str):
        if not _FQN_RE.match(table_fqn):
            raise BigQueryError("Nama tabel BigQuery tidak valid.")
        self.client = client
        self.fqn = table_fqn

    def select_existing_sql(self) -> str:
        return (
            f"SELECT {', '.join(COLUMNS)} FROM `{self.fqn}` "
            "WHERE tanggal_stock = @tanggal AND distributor_code = @distributor_code ORDER BY sku_code"
        )

    def select_by_ids_sql(self) -> str:
        return (
            f"SELECT {', '.join(COLUMNS)} FROM `{self.fqn}` "
            "WHERE transaction_id IN UNNEST(@ids) ORDER BY tanggal_stock, distributor_code, sku_code"
        )

    def select_by_range_sql(self) -> str:
        return (
            f"SELECT {', '.join(COLUMNS)} FROM `{self.fqn}` "
            "WHERE tanggal_stock BETWEEN @mulai AND @selesai "
            "ORDER BY tanggal_stock, distributor_code, sku_code"
        )

    def duplicates_sql(self) -> str:
        return (
            f"SELECT sku_code, COUNT(*) AS n FROM `{self.fqn}` "
            "WHERE tanggal_stock = @tanggal AND distributor_code = @distributor_code "
            "GROUP BY sku_code HAVING COUNT(*) > 1"
        )

    def select_previous_sql(self) -> str:
        """Kunjungan terakhir sebelum suatu tanggal, untuk distributor yang sama (boleh di luar rentang admin)."""
        return (
            f"SELECT {', '.join(COLUMNS)} FROM `{self.fqn}` "
            "WHERE distributor_code = @distributor_code AND tanggal_stock = ("
            f"SELECT MAX(tanggal_stock) FROM `{self.fqn}` "
            "WHERE distributor_code = @distributor_code AND tanggal_stock < @tanggal) "
            "ORDER BY sku_code"
        )

    def merge_sql(self) -> str:
        cols = ", ".join(COLUMNS)
        src_cols = ", ".join(f"S.{c}" for c in COLUMNS)
        return f"""MERGE `{self.fqn}` T
USING (SELECT * FROM UNNEST(@rows)) S
ON T.tanggal_stock = S.tanggal_stock AND T.distributor_code = S.distributor_code AND T.sku_code = S.sku_code
WHEN MATCHED THEN UPDATE SET
  distributor_name = S.distributor_name,
  region = S.region,
  supervisor_name = S.supervisor_name,
  sku_name = S.sku_name,
  stock_qty = S.stock_qty,
  evidence_file_id = IF(S.has_new_evidence, S.evidence_file_id, T.evidence_file_id),
  evidence_file_url = IF(S.has_new_evidence, S.evidence_file_url, T.evidence_file_url),
  evidence_file_name = IF(S.has_new_evidence, S.evidence_file_name, T.evidence_file_name),
  evidence_uploaded_at = IF(S.has_new_evidence, S.evidence_uploaded_at, T.evidence_uploaded_at),
  updated_at = S.updated_at
WHEN NOT MATCHED THEN INSERT ({cols})
  VALUES ({src_cols})"""

    def _run(self, sql: str, params: Sequence[Any]) -> list[dict]:
        try:
            job = self.client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=list(params)))
            return [dict(row.items()) for row in job.result()]
        except Exception as exc:
            log.exception("BigQuery query gagal")
            raise BigQueryError(f"BigQuery gagal: {exc.__class__.__name__}: {exc}") from exc

    def fetch_existing(self, tanggal_stock: date, distributor_code: str) -> list[dict]:
        return self._run(self.select_existing_sql(), [
            bigquery.ScalarQueryParameter("tanggal", "DATE", tanggal_stock),
            bigquery.ScalarQueryParameter("distributor_code", "STRING", distributor_code),
        ])

    def fetch_by_date_range(self, start: date, end: date) -> list[dict]:
        """Transaksi pada rentang tanggal stock. BETWEEN inklusif di kedua ujung."""
        return self._run(self.select_by_range_sql(), [
            bigquery.ScalarQueryParameter("mulai", "DATE", start),
            bigquery.ScalarQueryParameter("selesai", "DATE", end),
        ])

    def fetch_by_ids(self, transaction_ids: Sequence[str]) -> list[dict]:
        return self._run(self.select_by_ids_sql(), [
            bigquery.ArrayQueryParameter("ids", "STRING", list(transaction_ids)),
        ])

    def fetch_previous(self, distributor_code: str, tanggal_stock: date) -> list[dict]:
        """Baris pada tanggal stock terakhir sebelum `tanggal_stock` untuk distributor yang sama."""
        return self._run(self.select_previous_sql(), [
            bigquery.ScalarQueryParameter("distributor_code", "STRING", distributor_code),
            bigquery.ScalarQueryParameter("tanggal", "DATE", tanggal_stock),
        ])

    def find_duplicate_keys(self, tanggal_stock: date, distributor_code: str) -> list[str]:
        rows = self._run(self.duplicates_sql(), [
            bigquery.ScalarQueryParameter("tanggal", "DATE", tanggal_stock),
            bigquery.ScalarQueryParameter("distributor_code", "STRING", distributor_code),
        ])
        return [r["sku_code"] for r in rows]

    def upsert(self, records: Sequence[TransactionRecord]) -> None:
        if not records:
            return
        keys = {(r.tanggal_stock, r.distributor_code, r.sku_code) for r in records}
        if len(keys) != len(records):
            raise BigQueryError("Duplikat key (tanggal stock + distributor + SKU) dalam satu submit.")
        self._run(self.merge_sql(), [
            bigquery.ArrayQueryParameter("rows", "STRUCT", [_struct_param(r) for r in records]),
        ])
