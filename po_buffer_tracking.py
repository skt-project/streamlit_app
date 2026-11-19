"""Persistent per-store download tracking for the PO Suggestion Tool.

Pure logic: no Streamlit, so it can be tested without a running app. The
BigQuery client is injected (``client_factory``); ``po_buffer.py`` passes its
own cached client.

Storage
-------
Table ``skintific-data-warehouse.gt_schema.po_buffer_download_log`` - one row per
*store per download action* (a bulk ZIP of 5 stores writes 5 rows that share one
``event_id``). Columns:

=================  =========  ================================================
column             type       meaning
=================  =========  ================================================
event_id           STRING     id of the download action; all stores of one bulk
                              ZIP share it, which is also how "which stores were
                              in this bulk" is answered
store_code         STRING     store downloaded
store_name         STRING     store name at download time
region             STRING     region at download time
distributor        STRING     distributor at download time
download_type      STRING     ``individual`` or ``bulk``
file_name          STRING     PDF file name of this store in the download
bulk_file_count    INT64      PDFs in the ZIP (NULL for individual downloads)
downloaded_at      TIMESTAMP  UTC moment of the click
=================  =========  ================================================

Counting rules: a store's counter is ``COUNT(*)`` of its rows; individual and bulk
are the same query split on ``download_type``. Generating a PDF never writes a
row - only the click on a download button does (see ``po_buffer.py``).

Idempotency: rows are written with ``INSERT ... WHERE NOT EXISTS`` on
``(event_id, store_code)``, so a retried or replayed event can never double a
counter.

Fallback: when BigQuery cannot be used (no permission to create/write the table,
outage) events go to a local JSON-lines file instead and are pushed to BigQuery
the next time it works. The file lives in the server's temp dir - on Streamlit
Cloud that is ephemeral and per container, so events kept only there are lost on
restart. The dashboard says so whenever it is serving from the fallback.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableSet, Optional, Sequence

import pandas as pd
import pytz
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TRACKING_PROJECT = "skintific-data-warehouse"
TRACKING_DATASET = "gt_schema"
TRACKING_TABLE = "po_buffer_download_log"
TRACKING_TABLE_ID = f"{TRACKING_PROJECT}.{TRACKING_DATASET}.{TRACKING_TABLE}"

FALLBACK_PATH_ENV = "PO_BUFFER_TRACKING_FALLBACK_PATH"
DEFAULT_FALLBACK_FILENAME = "po_buffer_download_log.jsonl"

DOWNLOAD_INDIVIDUAL = "individual"
DOWNLOAD_BULK = "bulk"
DOWNLOAD_TYPES = (DOWNLOAD_INDIVIDUAL, DOWNLOAD_BULK)

TIMEZONE = "Asia/Jakarta"

TRACKING_COLUMNS = [
    "event_id",
    "store_code",
    "store_name",
    "region",
    "distributor",
    "download_type",
    "file_name",
    "bulk_file_count",
    "downloaded_at",
]

# How long BigQuery is skipped after a failure before it is tried again.
BQ_RETRY_AFTER_SECONDS = 300


def create_table_ddl(table_id: str = TRACKING_TABLE_ID) -> str:
    """DDL for the tracking table. Mirrored in ``sql/po_buffer_download_log.sql``."""
    return f"""CREATE TABLE IF NOT EXISTS `{table_id}` (
  event_id STRING NOT NULL OPTIONS(description="One id per download action; shared by every store of one bulk ZIP"),
  store_code STRING NOT NULL OPTIONS(description="Store downloaded"),
  store_name STRING OPTIONS(description="Store name at download time"),
  region STRING OPTIONS(description="Region at download time"),
  distributor STRING OPTIONS(description="Distributor at download time"),
  download_type STRING NOT NULL OPTIONS(description="individual or bulk"),
  file_name STRING OPTIONS(description="PDF file name of this store in the download"),
  bulk_file_count INT64 OPTIONS(description="PDFs in the ZIP; NULL for individual downloads"),
  downloaded_at TIMESTAMP NOT NULL OPTIONS(description="UTC time of the click")
)
OPTIONS(description="PO Suggestion Tool: one row per store per download click (po_buffer.py)")"""


def insert_events_sql(table_id: str = TRACKING_TABLE_ID) -> str:
    """Idempotent insert of the ``@rows`` struct array."""
    return f"""INSERT INTO `{table_id}`
  (event_id, store_code, store_name, region, distributor,
   download_type, file_name, bulk_file_count, downloaded_at)
SELECT r.event_id, r.store_code, r.store_name, r.region, r.distributor,
       r.download_type, r.file_name, r.bulk_file_count, r.downloaded_at
FROM UNNEST(@rows) AS r
WHERE NOT EXISTS (
  SELECT 1 FROM `{table_id}` AS t
  WHERE t.event_id = r.event_id AND t.store_code = r.store_code
)"""


def select_events_sql(table_id: str = TRACKING_TABLE_ID) -> str:
    columns = ", ".join(TRACKING_COLUMNS)
    return f"SELECT {columns} FROM `{table_id}` ORDER BY downloaded_at DESC"


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------

def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text or None


def build_download_rows(
    download_type: str,
    stores: Iterable[Mapping[str, Any]],
    event_id: str,
    downloaded_at: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Rows to store for one download action: one per distinct store.

    ``stores`` items need ``store_code`` and may carry ``store_name``,
    ``region``, ``distributor`` and ``file_name``. Stores without a code cannot
    be tracked and are ignored; repeats of a code within the action are counted
    once, so a ZIP of N PDFs always yields exactly N rows.
    """
    if download_type not in DOWNLOAD_TYPES:
        raise ValueError(f"download_type must be one of {DOWNLOAD_TYPES}, got {download_type!r}")
    if not _clean(event_id):
        raise ValueError("event_id is required")

    moment = downloaded_at or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    moment = moment.astimezone(timezone.utc)

    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for store in stores:
        code = _clean(store.get("store_code"))
        if code is None or code in seen:
            continue
        seen.add(code)
        rows.append(
            {
                "event_id": str(event_id),
                "store_code": code,
                "store_name": _clean(store.get("store_name")),
                "region": _clean(store.get("region")),
                "distributor": _clean(store.get("distributor")),
                "download_type": download_type,
                "file_name": _clean(store.get("file_name")),
                "bulk_file_count": None,
                "downloaded_at": moment,
            }
        )
    if not rows:
        raise ValueError("no store with a store_code to record")

    if download_type == DOWNLOAD_BULK:
        for row in rows:
            row["bulk_file_count"] = len(rows)
    return rows


# ---------------------------------------------------------------------------
# Storage backends
# ---------------------------------------------------------------------------

@dataclass
class RecordResult:
    """What happened to a recorded event."""

    rows: int
    backend: str  # "bigquery", "local_file" or "skipped"
    warning: Optional[str] = None
    duplicate: bool = False


@dataclass
class FetchResult:
    events: pd.DataFrame
    source: str  # "bigquery" or "local_file"
    warning: Optional[str] = None


def default_fallback_path() -> str:
    return os.environ.get(FALLBACK_PATH_ENV) or os.path.join(
        tempfile.gettempdir(), DEFAULT_FALLBACK_FILENAME
    )


def _empty_events() -> pd.DataFrame:
    frame = pd.DataFrame({column: pd.Series(dtype="object") for column in TRACKING_COLUMNS})
    return _normalise_events(frame)


def _normalise_events(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    for column in TRACKING_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame = frame[TRACKING_COLUMNS]
    frame["downloaded_at"] = pd.to_datetime(frame["downloaded_at"], utc=True)
    frame["bulk_file_count"] = pd.to_numeric(frame["bulk_file_count"]).astype("Int64")
    return frame.reset_index(drop=True)


class TrackingStore:
    """Reads and writes download events; BigQuery first, local file as fallback."""

    def __init__(
        self,
        client_factory: Optional[Callable[[], Any]] = None,
        *,
        table_id: str = TRACKING_TABLE_ID,
        fallback_path: Optional[str] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client_factory = client_factory
        self.table_id = table_id
        self.fallback_path = fallback_path or default_fallback_path()
        self._clock = clock
        self._lock = threading.RLock()
        self._table_ready = False
        self._retry_at = 0.0
        self.last_error: Optional[str] = None

    # -- BigQuery ---------------------------------------------------------

    def _bq_enabled(self) -> bool:
        return self._client_factory is not None and self._clock() >= self._retry_at

    def _bq_failed(self, exc: Exception) -> str:
        """Stop using BigQuery for a while; return a message safe to show."""
        self._retry_at = self._clock() + BQ_RETRY_AFTER_SECONDS
        self._table_ready = False
        self.last_error = type(exc).__name__
        logger.warning("Download tracking: BigQuery unavailable (%s): %s",
                       type(exc).__name__, str(exc)[:300])
        return type(exc).__name__

    def ensure_table(self) -> None:
        """Create the tracking table if it does not exist (once per process)."""
        if self._table_ready:
            return
        client = self._client_factory()
        try:
            client.query(create_table_ddl(self.table_id)).result()
        except Exception as create_error:  # noqa: BLE001
            # The service account may lack CREATE but still be able to use a
            # table an admin created from sql/po_buffer_download_log.sql.
            try:
                client.get_table(self.table_id)
            except Exception:  # noqa: BLE001
                raise create_error from None
        self._table_ready = True

    def _bq_insert(self, rows: Sequence[Mapping[str, Any]]) -> None:
        structs = []
        for row in rows:
            structs.append(
                bigquery.StructQueryParameter(
                    None,
                    bigquery.ScalarQueryParameter("event_id", "STRING", row["event_id"]),
                    bigquery.ScalarQueryParameter("store_code", "STRING", row["store_code"]),
                    bigquery.ScalarQueryParameter("store_name", "STRING", row["store_name"]),
                    bigquery.ScalarQueryParameter("region", "STRING", row["region"]),
                    bigquery.ScalarQueryParameter("distributor", "STRING", row["distributor"]),
                    bigquery.ScalarQueryParameter("download_type", "STRING", row["download_type"]),
                    bigquery.ScalarQueryParameter("file_name", "STRING", row["file_name"]),
                    bigquery.ScalarQueryParameter("bulk_file_count", "INT64", row["bulk_file_count"]),
                    bigquery.ScalarQueryParameter("downloaded_at", "TIMESTAMP", row["downloaded_at"]),
                )
            )
        config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("rows", "STRUCT", structs)]
        )
        self._client_factory().query(insert_events_sql(self.table_id), job_config=config).result()

    def _bq_fetch(self) -> pd.DataFrame:
        return self._client_factory().query(select_events_sql(self.table_id)).to_dataframe()

    # -- Local fallback ---------------------------------------------------

    def _append_local(self, rows: Sequence[Mapping[str, Any]]) -> None:
        lines = []
        for row in rows:
            record = dict(row)
            record["downloaded_at"] = row["downloaded_at"].isoformat()
            lines.append(json.dumps(record, ensure_ascii=False))
        with self._lock:
            directory = os.path.dirname(self.fallback_path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            with open(self.fallback_path, "a", encoding="utf-8") as handle:
                handle.write("\n".join(lines) + "\n")

    def _read_local(self) -> List[Dict[str, Any]]:
        with self._lock:
            try:
                with open(self.fallback_path, "r", encoding="utf-8") as handle:
                    raw = handle.read().splitlines()
            except FileNotFoundError:
                return []
        records = []
        for line in raw:
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                logger.warning("Download tracking: skipped a corrupt line in the fallback file")
        return records

    def _flush_local(self) -> None:
        """Push events held in the fallback file to BigQuery, then drop the file.

        Best effort: any failure leaves the file untouched, and the idempotent
        insert makes a repeated flush harmless.
        """
        with self._lock:
            records = self._read_local()
            if not records:
                return
            rows = []
            for record in records:
                record["downloaded_at"] = datetime.fromisoformat(record["downloaded_at"])
                rows.append(record)
            self._bq_insert(rows)
            try:
                os.remove(self.fallback_path)
            except FileNotFoundError:
                pass
            logger.info("Download tracking: flushed %d fallback rows to BigQuery", len(rows))

    # -- Public API -------------------------------------------------------

    def record(self, rows: Sequence[Mapping[str, Any]]) -> RecordResult:
        """Persist *rows*; falls back to the local file when BigQuery fails."""
        if not rows:
            raise ValueError("nothing to record")
        warning = None
        if self._client_factory is None:
            warning = "BigQuery tidak dikonfigurasi."
        elif not self._bq_enabled():
            warning = "BigQuery sedang tidak tersedia."
        else:
            try:
                with self._lock:
                    self.ensure_table()
                    self._bq_insert(rows)
                try:
                    self._flush_local()
                except Exception as exc:  # noqa: BLE001 - the event itself is already safe
                    logger.warning("Download tracking: flush of fallback file failed: %s", exc)
                return RecordResult(rows=len(rows), backend="bigquery")
            except Exception as exc:  # noqa: BLE001
                warning = f"Penyimpanan BigQuery gagal ({self._bq_failed(exc)})."

        self._append_local(rows)
        return RecordResult(
            rows=len(rows),
            backend="local_file",
            warning=(
                f"{warning} Download dicatat sementara di file lokal server; "
                "data ini hilang jika aplikasi restart."
            ),
        )

    def fetch(self) -> FetchResult:
        """All stored events as a DataFrame (BigQuery rows plus any local rows)."""
        frames: List[pd.DataFrame] = []
        source = "bigquery"
        warning: Optional[str] = None

        if self._client_factory is None:
            source, warning = "local_file", "BigQuery tidak dikonfigurasi."
        elif not self._bq_enabled():
            source, warning = "local_file", "BigQuery sedang tidak tersedia."
        else:
            try:
                with self._lock:
                    self.ensure_table()
                try:
                    self._flush_local()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Download tracking: flush of fallback file failed: %s", exc)
                frames.append(self._bq_fetch())
            except Exception as exc:  # noqa: BLE001
                source = "local_file"
                warning = f"Gagal membaca BigQuery ({self._bq_failed(exc)})."

        local = self._read_local()
        if local:
            frames.append(pd.DataFrame(local))
            if source == "bigquery":
                warning = (
                    "Sebagian event masih di file lokal server dan belum masuk BigQuery."
                )
        if source == "local_file":
            warning = (
                f"{warning} Menampilkan hanya data file lokal server, yang hilang "
                "jika aplikasi restart."
            )

        frames = [f for f in frames if not f.empty]
        if not frames:
            return FetchResult(_empty_events(), source, warning)
        events = _normalise_events(pd.concat([_normalise_events(f) for f in frames]))
        events = events.drop_duplicates(["event_id", "store_code"], keep="first")
        return FetchResult(events.reset_index(drop=True), source, warning)


def record_download_event(
    store: TrackingStore,
    download_type: str,
    stores: Iterable[Mapping[str, Any]],
    event_id: str,
    *,
    downloaded_at: Optional[datetime] = None,
    seen_event_ids: Optional[MutableSet[str]] = None,
) -> RecordResult:
    """Record one download click.

    ``event_id`` identifies the click. If it is already in ``seen_event_ids``
    (the session's set of recorded ids) nothing is written, so a Streamlit rerun
    that re-delivers the same click cannot count it twice; the database enforces
    the same rule across sessions.
    """
    if seen_event_ids is not None and event_id in seen_event_ids:
        return RecordResult(rows=0, backend="skipped", duplicate=True)
    rows = build_download_rows(download_type, stores, event_id, downloaded_at)
    result = store.record(rows)
    if seen_event_ids is not None:
        seen_event_ids.add(event_id)
    return result


def fetch_tracking_data(store: TrackingStore) -> FetchResult:
    """Read all tracking events (thin wrapper kept for symmetry with record)."""
    return store.fetch()


# ---------------------------------------------------------------------------
# Analysis for the admin dashboard
# ---------------------------------------------------------------------------

STORE_SUMMARY_COLUMNS = [
    "store_code",
    "store_name",
    "region",
    "distributor",
    "individual_downloads",
    "bulk_downloads",
    "total_downloads",
    "last_download_at",
]


def _jakarta(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True).dt.tz_convert(pytz.timezone(TIMEZONE))


def filter_events(
    events: pd.DataFrame,
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    store_codes: Optional[Sequence[str]] = None,
    regions: Optional[Sequence[str]] = None,
    distributors: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """Filter events; dates are inclusive and compared in Asia/Jakarta."""
    if events.empty:
        return events
    mask = pd.Series(True, index=events.index)
    if start_date is not None or end_date is not None:
        local_date = _jakarta(events["downloaded_at"]).dt.date
        if start_date is not None:
            mask &= local_date >= start_date
        if end_date is not None:
            mask &= local_date <= end_date
    if store_codes:
        mask &= events["store_code"].isin(list(store_codes))
    if regions:
        mask &= events["region"].isin(list(regions))
    if distributors:
        mask &= events["distributor"].isin(list(distributors))
    return events[mask]


def summarize_totals(events: pd.DataFrame) -> Dict[str, int]:
    """Headline numbers. Downloads are counted per store, as the counters are."""
    if events.empty:
        return {
            "total_downloads": 0,
            "individual_downloads": 0,
            "bulk_downloads": 0,
            "bulk_actions": 0,
            "stores_downloaded": 0,
        }
    is_bulk = events["download_type"] == DOWNLOAD_BULK
    return {
        "total_downloads": int(len(events)),
        "individual_downloads": int((events["download_type"] == DOWNLOAD_INDIVIDUAL).sum()),
        "bulk_downloads": int(is_bulk.sum()),
        "bulk_actions": int(events.loc[is_bulk, "event_id"].nunique()),
        "stores_downloaded": int(events["store_code"].nunique()),
    }


def aggregate_by_store(events: pd.DataFrame) -> pd.DataFrame:
    """One row per store with its individual/bulk/total counters, newest first."""
    if events.empty:
        return pd.DataFrame({column: pd.Series(dtype="object") for column in STORE_SUMMARY_COLUMNS})

    ordered = events.sort_values("downloaded_at")
    latest = ordered.groupby("store_code").tail(1).set_index("store_code")
    counts = (
        events.groupby(["store_code", "download_type"]).size().unstack(fill_value=0)
        .reindex(columns=list(DOWNLOAD_TYPES), fill_value=0)
    )
    summary = pd.DataFrame(
        {
            "store_name": latest["store_name"],
            "region": latest["region"],
            "distributor": latest["distributor"],
            "individual_downloads": counts[DOWNLOAD_INDIVIDUAL],
            "bulk_downloads": counts[DOWNLOAD_BULK],
            "last_download_at": _jakarta(latest["downloaded_at"]),
        }
    )
    summary["total_downloads"] = summary["individual_downloads"] + summary["bulk_downloads"]
    summary = summary.reset_index()[STORE_SUMMARY_COLUMNS]
    return summary.sort_values(
        ["total_downloads", "last_download_at"], ascending=[False, False]
    ).reset_index(drop=True)


def bulk_event_details(events: pd.DataFrame) -> pd.DataFrame:
    """One row per bulk download: when, how many stores, which stores and files."""
    columns = ["event_id", "downloaded_at", "store_count", "stores", "files"]
    bulk = events[events["download_type"] == DOWNLOAD_BULK] if not events.empty else events
    if bulk.empty:
        return pd.DataFrame({column: pd.Series(dtype="object") for column in columns})

    def _join(values: pd.Series) -> str:
        return "; ".join(str(v) for v in values if _clean(v))

    rows = []
    for event_id, group in bulk.groupby("event_id", sort=False):
        group = group.sort_values("store_name")
        rows.append(
            {
                "event_id": event_id,
                "downloaded_at": _jakarta(group["downloaded_at"]).max(),
                "store_count": int(len(group)),
                "stores": _join(group["store_code"] + " - " + group["store_name"].fillna("")),
                "files": _join(group["file_name"]),
            }
        )
    details = pd.DataFrame(rows, columns=columns)
    return details.sort_values("downloaded_at", ascending=False).reset_index(drop=True)


_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


def _csv_safe(value: Any) -> Any:
    """Stop spreadsheet apps from running a text cell as a formula."""
    if isinstance(value, str) and value.startswith(_FORMULA_PREFIXES):
        return "'" + value
    return value


def to_csv_bytes(frame: pd.DataFrame) -> bytes:
    """UTF-8 (with BOM, so Excel reads it right) CSV; timestamps in Asia/Jakarta."""
    export = frame.copy()
    for column in export.columns:
        if pd.api.types.is_datetime64_any_dtype(export[column]):
            export[column] = (
                _jakarta(export[column]).dt.strftime("%Y-%m-%d %H:%M:%S")
            )
        elif not pd.api.types.is_numeric_dtype(export[column]):
            # text columns: object dtype on pandas 2, "str" dtype on pandas 3
            export[column] = export[column].map(_csv_safe)
    return export.to_csv(index=False).encode("utf-8-sig")
