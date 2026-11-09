"""Wires settings -> backends (Google or local) with Streamlit caching, and
turns Google API errors into messages a non-technical user can act on."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import streamlit as st

from src.config import SCOPES, RuntimeMode, Settings, load_settings, resolve_mode
from src.sample_data import SAMPLE_SOURCE_VALUES
from src.sheets import (
    LocalLogStore,
    LogStore,
    SheetsLogStore,
    SheetsSource,
    SourceReader,
    StaticSource,
    build_store_index,
    logs_to_frame,
    parse_source_values,
)
from src.storage import DocumentStorage, GCSStorage, LocalStorage

log = logging.getLogger("skp.runtime")

SOURCE_TTL_SECONDS = 300
LOG_TTL_SECONDS = 30


class DataUnavailable(RuntimeError):
    """Raised with a user-facing message when a data source cannot be read."""


@dataclass
class Backends:
    settings: Settings
    mode: RuntimeMode
    source: SourceReader | None
    logs: LogStore | None
    storage: DocumentStorage | None
    error: str = ""


def friendly_google_error(exc: Exception, settings: Settings) -> str:
    email = settings.service_account_email or "service account"
    name = type(exc).__name__
    status = getattr(getattr(exc, "response", None), "status_code", None)
    try:
        import gspread

        if isinstance(exc, gspread.exceptions.WorksheetNotFound):
            return f"Sheet '{settings.source_sheet_name}' tidak ditemukan di spreadsheet sumber."
        if isinstance(exc, gspread.exceptions.SpreadsheetNotFound) or status == 404:
            return "Spreadsheet tidak ditemukan. Periksa SOURCE_SPREADSHEET_ID."
    except ImportError:
        pass
    if status == 403:
        return f"Spreadsheet belum dibagikan ke {email} sebagai Editor."
    if status == 429:
        return "Batas kuota Google Sheets tercapai sementara. Tunggu 1 menit lalu muat ulang."
    if "RefreshError" in name or "TransportError" in name:
        return "Kredensial service account ditolak atau koneksi ke Google gagal. Periksa GCP_SERVICE_ACCOUNT_JSON."
    return f"Gagal membaca Google Sheets ({name}). Coba muat ulang halaman."


@st.cache_resource(show_spinner=False)
def _google_clients(client_email: str, project_id: str):
    """One credential + gspread client + storage client per process."""
    import gspread
    from google.cloud import storage
    from google.oauth2.service_account import Credentials

    settings = load_settings()
    creds = Credentials.from_service_account_info(settings.service_account_info, scopes=list(SCOPES))
    return gspread.authorize(creds), storage.Client(project=project_id or None, credentials=creds)


@st.cache_resource(show_spinner=False)
def _spreadsheet(client_email: str, project_id: str, spreadsheet_id: str):
    gc, _ = _google_clients(client_email, project_id)
    return gc.open_by_key(spreadsheet_id)


def get_backends() -> Backends:
    settings = load_settings()
    mode = resolve_mode(settings)
    local_root = Path(settings.local_data_dir)
    source: SourceReader | None = None
    logs: LogStore | None = None
    storage: DocumentStorage | None = None
    error = ""

    spreadsheet = None
    if mode.source_live:
        try:
            spreadsheet = _spreadsheet(settings.service_account_email, settings.gcp_project_id,
                                       settings.source_spreadsheet_id)
        except Exception as exc:  # noqa: BLE001
            log.warning("Spreadsheet open failed: %s", type(exc).__name__)
            error = friendly_google_error(exc, settings)

    if spreadsheet is not None:
        source = SheetsSource(spreadsheet, settings.source_sheet_name)
    elif mode.mock and not error:
        source = StaticSource(SAMPLE_SOURCE_VALUES)

    if mode.mock:
        logs = LocalLogStore(local_root / f"{settings.log_sheet_name}.csv")
        storage = LocalStorage(local_root / "gcs")
    else:
        if spreadsheet is not None:
            logs = SheetsLogStore(spreadsheet, settings.log_sheet_name)
        if settings.service_account_info and settings.gcs_bucket:
            _, storage_client = _google_clients(settings.service_account_email, settings.gcp_project_id)
            storage = GCSStorage(storage_client, settings.gcs_bucket)
    return Backends(settings, mode, source, logs, storage, error)


@st.cache_data(ttl=SOURCE_TTL_SECONDS, show_spinner="Memuat daftar toko…")
def _source_values(cache_key: str, _source: SourceReader) -> list[list[str]]:
    return _source.read_values()


@st.cache_data(ttl=LOG_TTL_SECONDS, show_spinner=False)
def _log_values(cache_key: str, _logs: LogStore) -> list[list[str]]:
    return _logs.read_values()


def load_source(backends: Backends) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(clean source rows, store index). Raises DataUnavailable with a friendly message."""
    if backends.source is None:
        raise DataUnavailable(backends.error or "Sumber data toko belum dikonfigurasi.")
    try:
        values = _source_values(backends.source.label + backends.settings.source_spreadsheet_id, backends.source)
        df = parse_source_values(values)
    except ValueError as exc:  # SourceFormatError
        raise DataUnavailable(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise DataUnavailable(friendly_google_error(exc, backends.settings)) from exc
    return df, build_store_index(df)


def load_logs(backends: Backends) -> pd.DataFrame:
    if backends.logs is None:
        raise DataUnavailable(backends.error or "Log upload belum dapat diakses.")
    try:
        return logs_to_frame(_log_values(backends.logs.label, backends.logs))
    except Exception as exc:  # noqa: BLE001
        raise DataUnavailable(friendly_google_error(exc, backends.settings)) from exc


def clear_log_cache() -> None:
    _log_values.clear()


def clear_all_caches() -> None:
    _log_values.clear()
    _source_values.clear()
