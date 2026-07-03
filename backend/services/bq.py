"""
BigQuery client singleton with in-memory TTL cache for reference data.
Reads use the service account from config; writes go only to sfa_step tables.
"""
from __future__ import annotations

import threading
import time
from typing import Any

from google.cloud import bigquery
from google.oauth2 import service_account

from config import settings


class _TTLCache:
    """Thread-safe in-memory cache with per-key TTL."""

    def __init__(self, default_ttl: int = 300):
        self._store: dict[str, tuple[Any, float]] = {}
        self._lock = threading.Lock()
        self._default_ttl = default_ttl

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._store.get(key)
            if entry and time.monotonic() < entry[1]:
                return entry[0]
            return None

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        with self._lock:
            self._store[key] = (value, time.monotonic() + (ttl or self._default_ttl))

    def invalidate(self, prefix: str = "") -> None:
        with self._lock:
            keys = [k for k in self._store if k.startswith(prefix)]
            for k in keys:
                del self._store[k]


class BQClient:
    _instance: "BQClient | None" = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        if settings.bq_sa_key_path:
            creds = service_account.Credentials.from_service_account_file(
                settings.bq_sa_key_path,
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
            self._client = bigquery.Client(project=settings.bq_project, credentials=creds)
        else:
            # Application Default Credentials (Cloud Run Workload Identity)
            self._client = bigquery.Client(project=settings.bq_project)
        self.cache = _TTLCache(default_ttl=300)

    @classmethod
    def get(cls) -> "BQClient":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def query(
        self,
        sql: str,
        params: list[bigquery.ScalarQueryParameter] | None = None,
    ) -> list[dict]:
        job_config = bigquery.QueryJobConfig(query_parameters=params or [])
        rows = self._client.query(sql, job_config=job_config).result()
        return [dict(row) for row in rows]

    def query_one(
        self,
        sql: str,
        params: list[bigquery.ScalarQueryParameter] | None = None,
    ) -> dict | None:
        results = self.query(sql, params)
        return results[0] if results else None

    def execute(
        self,
        sql: str,
        params: list[bigquery.ScalarQueryParameter] | None = None,
    ) -> None:
        """Run DML (INSERT / UPDATE / DELETE / MERGE) against sfa_step."""
        job_config = bigquery.QueryJobConfig(query_parameters=params or [])
        self._client.query(sql, job_config=job_config).result()

    def insert_rows(self, table_id: str, rows: list[dict]) -> None:
        """Streaming insert — use for single-row audit-log writes."""
        full_id = f"{settings.bq_project}.{settings.bq_dataset}.{table_id}"
        errors = self._client.insert_rows_json(full_id, rows)
        if errors:
            raise RuntimeError(f"BigQuery streaming insert errors: {errors}")

    # ------------------------------------------------------------------
    # Convenience: parameterize a string/date/bool/int
    # ------------------------------------------------------------------

    @staticmethod
    def p(name: str, bq_type: str, value: Any) -> bigquery.ScalarQueryParameter:
        return bigquery.ScalarQueryParameter(name, bq_type, value)
